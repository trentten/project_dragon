from __future__ import annotations

import argparse
import itertools
import json
import hashlib
import logging
import os
import socket
import time
from collections import Counter, OrderedDict
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from project_dragon.backtest_core import DataSettings, run_single_backtest
from project_dragon.data_online import get_candles_with_cache, load_ccxt_candles
from project_dragon.domain import Candle
from project_dragon.storage import (
    build_backtest_run_rows,
    bulk_enqueue_backtest_run_jobs,
    claim_job_with_lease,
    compute_run_key,
    finalize_sweep_if_complete,
    get_backtest_run_job_counts_by_sweep,
    get_job,
    get_parent_job_flags,
    get_sweep_row,
    init_db,
    open_db_connection,
    reclaim_stale_job,
    renew_job_lease,
    upsert_backtest_run_by_run_key,
    update_job,
    update_sweep_status,
)

logger = logging.getLogger(__name__)

POLL_SECONDS = float(os.environ.get("DRAGON_WORKER_POLL_S", "1.0") or 1.0)
JOB_LEASE_S = float(os.environ.get("DRAGON_JOB_LEASE_S", "30") or 30.0)
JOB_LEASE_RENEW_EVERY_S = float(os.environ.get("DRAGON_JOB_LEASE_RENEW_EVERY_S", "10") or 10.0)

# Sweep planning chunk size (limits memory and allows pause/cancel to take effect quickly).
SWEEP_PLAN_BATCH_SIZE = int(os.environ.get("DRAGON_SWEEP_PLAN_BATCH_SIZE", "250") or 250)

# Worker-local candle LRU cache (bounded, in-memory)
_CANDLE_CACHE_MAX_ENTRIES = int(os.environ.get("DRAGON_CANDLE_CACHE_MAX_ENTRIES", "32") or 32)
_CANDLE_CACHE_MAX_TOTAL_CANDLES = int(os.environ.get("DRAGON_CANDLE_CACHE_MAX_TOTAL_CANDLES", "200000") or 200_000)
_CANDLE_CACHE_PREFETCH = int(os.environ.get("DRAGON_CANDLE_CACHE_PREFETCH", "1") or 1)
_CANDLE_CACHE_PREFETCH_TOP_N = int(os.environ.get("DRAGON_CANDLE_CACHE_PREFETCH_TOP_N", "2") or 2)

_CANDLE_CACHE_PREFETCH_QUERY_LIMIT = 50
_CANDLE_CACHE_PREFETCH_BUDGET_S = 2.0

# In-process lease tracking (like live_worker).
_JOB_EXPECTED_LEASE_VERSION: Dict[int, int] = {}
_JOB_LAST_LEASE_RENEW_S: Dict[int, float] = {}


class CandleCache:
    """Worker-local bounded LRU candle cache.

    Stores lists of Candle objects in memory.
    Eviction is by LRU and bounded by both entry count and total candle count.
    """

    def __init__(self, *, max_entries: int, max_total_candles: int):
        self.max_entries = max(1, int(max_entries))
        self.max_total_candles = max(1, int(max_total_candles))
        self._od: "OrderedDict[tuple, list[Candle]]" = OrderedDict()
        self._total_candles = 0

    @property
    def total_candles(self) -> int:
        return int(self._total_candles)

    @property
    def entries(self) -> int:
        return int(len(self._od))

    def get(self, key: tuple) -> Optional[list[Candle]]:
        try:
            v = self._od.get(key)
        except Exception:
            v = None
        if v is None:
            return None
        try:
            self._od.move_to_end(key, last=True)
        except Exception:
            pass
        return v

    def set(self, key: tuple, candles: list[Candle]) -> None:
        if candles is None:
            return
        try:
            n_new = int(len(candles))
        except Exception:
            n_new = 0

        # Replace existing.
        if key in self._od:
            try:
                old = self._od.pop(key)
                self._total_candles -= int(len(old))
            except Exception:
                pass

        self._od[key] = candles
        self._total_candles += int(n_new)
        try:
            self._od.move_to_end(key, last=True)
        except Exception:
            pass

        self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        # Evict until within both bounds.
        while True:
            if len(self._od) <= int(self.max_entries) and int(self._total_candles) <= int(self.max_total_candles):
                return
            try:
                k, v = self._od.popitem(last=False)
                self._total_candles -= int(len(v))
            except KeyError:
                self._total_candles = 0
                return
            except Exception:
                return


_CANDLE_CACHE = CandleCache(max_entries=_CANDLE_CACHE_MAX_ENTRIES, max_total_candles=_CANDLE_CACHE_MAX_TOTAL_CANDLES)
_CANDLE_CACHE_HITS = 0
_CANDLE_CACHE_MISSES = 0
_CANDLE_CACHE_LOG_EVERY = 50
_LAST_PREFETCH_GROUP_KEY: Optional[str] = None


def _to_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except Exception:
            return None
    if isinstance(value, datetime):
        try:
            return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # ISO timestamps
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            # numeric-as-string
            try:
                return int(float(s))
            except Exception:
                return None
    return None


def _candle_cache_key_from_settings(ds: DataSettings) -> Optional[tuple]:
    try:
        if str(ds.data_source).lower() == "synthetic":
            return None
        params = ds.range_params or {}
        exchange_id = str(ds.exchange_id or "binance").strip().lower()
        market_type = str(ds.market_type or "unknown").strip().lower()
        symbol = str(ds.symbol or "BTC/USDT").strip()
        timeframe = str(ds.timeframe or "1h").strip()
        range_mode = str(ds.range_mode or "bars").strip().lower()
        limit = params.get("limit")
        try:
            limit_i = int(limit) if limit is not None else None
        except Exception:
            limit_i = None
        since_ms = _to_ms(params.get("since"))
        until_ms = _to_ms(params.get("until"))
        return (exchange_id, market_type, symbol, timeframe, range_mode, limit_i, since_ms, until_ms)
    except Exception:
        return None


def _load_candles_worker_cached(ds: DataSettings) -> list[Candle]:
    global _CANDLE_CACHE_HITS, _CANDLE_CACHE_MISSES

    key = _candle_cache_key_from_settings(ds)
    if key is not None:
        hit = _CANDLE_CACHE.get(key)
        if hit is not None:
            _CANDLE_CACHE_HITS += 1
            if (_CANDLE_CACHE_HITS + _CANDLE_CACHE_MISSES) % _CANDLE_CACHE_LOG_EVERY == 0:
                logger.info(
                    "candle_cache stats worker=%s hits=%s misses=%s entries=%s total_candles=%s",
                    "?",
                    int(_CANDLE_CACHE_HITS),
                    int(_CANDLE_CACHE_MISSES),
                    int(_CANDLE_CACHE.entries),
                    int(_CANDLE_CACHE.total_candles),
                )
            return hit

    _CANDLE_CACHE_MISSES += 1
    params = ds.range_params or {}
    exchange_id = ds.exchange_id or "binance"
    market_type = ds.market_type or "unknown"
    symbol = ds.symbol or "BTC/USDT"
    timeframe = ds.timeframe or "1h"
    range_mode = (ds.range_mode or "bars").lower()
    limit = params.get("limit")
    since = params.get("since")
    until = params.get("until")

    try:
        candles = get_candles_with_cache(
            exchange_id=exchange_id,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            since=since,
            until=until,
            range_mode=range_mode,
            market_type=market_type,
        )
    except Exception as exc:
        msg = str(exc).lower()
        if "database is locked" in msg or "locked" in msg:
            candles = load_ccxt_candles(
                exchange_id,
                symbol,
                timeframe,
                limit=limit,
                since=since,
                until=until,
                range_mode=range_mode,
                market_type=market_type,
            )
        else:
            raise

    if key is not None:
        try:
            _CANDLE_CACHE.set(key, list(candles))
        except Exception:
            pass
    return list(candles)


def _prefetch_group_candles(*, conn, group_key: str, worker_id: str) -> None:
    global _LAST_PREFETCH_GROUP_KEY

    if int(_CANDLE_CACHE_PREFETCH or 0) != 1:
        return
    gk = str(group_key or "").strip()
    if not gk:
        return
    if _LAST_PREFETCH_GROUP_KEY == gk:
        return

    start_s = time.time()
    _LAST_PREFETCH_GROUP_KEY = gk

    try:
        rows = conn.execute(
            """
            SELECT payload_json
            FROM jobs
            WHERE group_key = %s
              AND job_type = 'backtest_run'
              AND status = 'queued'
            ORDER BY COALESCE(created_at, updated_at) ASC
            LIMIT %s
            """,
            (gk, int(_CANDLE_CACHE_PREFETCH_QUERY_LIMIT)),
        ).fetchall()
    except Exception:
        return

    keys: list[tuple] = []
    for r in rows or []:
        if (time.time() - start_s) > float(_CANDLE_CACHE_PREFETCH_BUDGET_S):
            break
        try:
            payload_raw = r[0] if not isinstance(r, dict) else r.get("payload_json")
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) and payload_raw.strip() else {}
            ds = DataSettings(**(payload.get("data_settings") or {}))
            k = _candle_cache_key_from_settings(ds)
            if k is not None:
                keys.append(k)
        except Exception:
            continue

    if not keys:
        return

    top_n = max(0, int(_CANDLE_CACHE_PREFETCH_TOP_N or 0))
    if top_n <= 0:
        return

    common = Counter(keys).most_common(top_n)
    prefetched = 0
    for k, _count in common:
        if (time.time() - start_s) > float(_CANDLE_CACHE_PREFETCH_BUDGET_S):
            break
        if _CANDLE_CACHE.get(k) is not None:
            continue
        try:
            exchange_id, market_type, symbol, timeframe, range_mode, limit_i, since_ms, until_ms = k
            # Use the exact same fetch path as a real run (DB cache + fallback).
            try:
                candles = get_candles_with_cache(
                    exchange_id=str(exchange_id),
                    symbol=str(symbol),
                    timeframe=str(timeframe),
                    limit=limit_i,
                    since=since_ms,
                    until=until_ms,
                    range_mode=str(range_mode),
                    market_type=str(market_type),
                )
            except Exception as exc:
                msg = str(exc).lower()
                if "database is locked" in msg or "locked" in msg:
                    candles = load_ccxt_candles(
                        str(exchange_id),
                        str(symbol),
                        str(timeframe),
                        limit=limit_i,
                        since=since_ms,
                        until=until_ms,
                        range_mode=str(range_mode),
                        market_type=str(market_type),
                    )
                else:
                    continue
            _CANDLE_CACHE.set(k, list(candles))
            prefetched += 1
        except Exception:
            continue

    if prefetched > 0:
        logger.info(
            "candle_cache prefetch worker=%s group=%s prefetched=%s entries=%s total_candles=%s",
            str(worker_id),
            str(gk),
            int(prefetched),
            int(_CANDLE_CACHE.entries),
            int(_CANDLE_CACHE.total_candles),
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _exc_text() -> str:
    import traceback

    return "".join(traceback.format_exception(*traceback.sys.exc_info()))


def _maybe_renew_job_lease(*, conn, job: Dict[str, Any], worker_id: str, lease_s: float, renew_every_s: float) -> bool:
    try:
        job_id = int(job.get("id"))
    except Exception:
        return False

    expected = _JOB_EXPECTED_LEASE_VERSION.get(job_id)
    if expected is None:
        try:
            expected = int(job.get("lease_version") or 0)
        except Exception:
            expected = 0
        _JOB_EXPECTED_LEASE_VERSION[job_id] = int(expected)

    last_s = float(_JOB_LAST_LEASE_RENEW_S.get(job_id) or 0.0)
    now_s = time.time()
    if float(renew_every_s) > 0 and (now_s - last_s) < float(renew_every_s):
        return True

    ok = renew_job_lease(
        conn,
        job_id=job_id,
        worker_id=str(worker_id),
        lease_s=float(lease_s),
        expected_lease_version=int(expected or 0),
    )
    _JOB_LAST_LEASE_RENEW_S[job_id] = float(now_s)
    if not ok:
        try:
            update_job(conn, job_id, status="error", error_text="job_lease_lost", finished_at=_now_iso())
        except Exception:
            pass
    return bool(ok)


def _write_with_retry(fn, *, timeout_s: float = 5.0) -> None:
    """Retry short DB write blocks on 'database is locked'."""

    start = time.time()
    delay = 0.05
    while True:
        try:
            fn()
            return
        except Exception as exc:
            msg = str(exc).lower()
            if "database is locked" not in msg and "locked" not in msg:
                raise
            if (time.time() - start) >= float(timeout_s):
                raise
            time.sleep(delay)
            delay = min(delay * 1.5, 0.5)


def _write_tx_with_retry(fn, *, timeout_s: float = 8.0) -> None:
    """Retry a short transaction block on transient DB errors."""

    start = time.time()
    delay = 0.05
    while True:
        try:
            fn()
            return
        except Exception as exc:
            msg = str(exc).lower()
            if "database is locked" not in msg and "locked" not in msg and "busy" not in msg:
                raise
            if (time.time() - start) >= float(timeout_s):
                raise
            time.sleep(delay)
            delay = min(delay * 1.7, 0.7)


def _sweep_combo_key(keys: list[str], combo: list[object]) -> str:
    """Stable identifier for a sweep combo (sha1 over canonical overrides JSON)."""

    try:
        overrides = dict(zip(list(keys), list(combo)))
    except Exception:
        overrides = {}
    payload = json.dumps(overrides, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"sha1:{digest}"


def _coerce_sweep_value(type_name: str, raw: object) -> object:
    """Best-effort parse sweep values stored as strings."""

    t = str(type_name or "").strip().lower()
    if raw is None:
        return None
    if t in {"int", "integer"}:
        return int(float(str(raw).strip()))
    if t in {"float", "number"}:
        return float(str(raw).strip())
    if t in {"bool", "boolean"}:
        s = str(raw).strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
        return bool(s)
    # default: string
    return str(raw)


def _set_nested(d: dict, path: list[str], value: object) -> None:
    cur: object = d
    for key in path[:-1]:
        if not isinstance(cur, dict):
            return
        if key not in cur or not isinstance(cur.get(key), dict):
            cur[key] = {}
        cur = cur[key]
    if isinstance(cur, dict) and path:
        cur[path[-1]] = value


def execute_sweep_parent_job(*, conn, job: Dict[str, Any], worker_id: str) -> None:
    job_id = int(job.get("id"))

    payload_raw = job.get("payload_json") or job.get("payload")
    payload: Dict[str, Any] = {}
    if isinstance(payload_raw, str) and payload_raw.strip():
        payload = json.loads(payload_raw)
    elif isinstance(payload_raw, dict):
        payload = payload_raw

    sweep_id = str(payload.get("sweep_id") or job.get("sweep_id") or "").strip()
    if not sweep_id:
        update_job(conn, job_id, status="failed", error_text="missing_sweep_id", finished_at=_now_iso())
        return

    update_job(conn, job_id, status="running", started_at=job.get("started_at") or _now_iso(), message="Planning sweep")

    sweep_row = get_sweep_row(conn, sweep_id)
    if not sweep_row:
        update_job(conn, job_id, status="failed", error_text="sweep_not_found", finished_at=_now_iso())
        return

    # Ensure a stable group_key for this sweep (used for bulk cancel + dashboards).
    group_key = f"sweep:{sweep_id}"
    try:
        conn.execute(
            "UPDATE jobs SET group_key = COALESCE(NULLIF(group_key,''), %s) WHERE id = %s",
            (group_key, int(job_id)),
        )
        conn.commit()
    except Exception:
        pass

    # Respect pause/cancel immediately.
    try:
        flags0 = get_parent_job_flags(conn, int(job_id))
        if int(flags0.get("cancel_requested") or 0) == 1:
            update_job(conn, job_id, status="cancelled", message="Sweep cancelled", finished_at=_now_iso())
            try:
                update_sweep_status(str(sweep_id), "cancelled", error_message="cancel_requested")
            except Exception:
                pass
            return
        if int(flags0.get("pause_requested") or 0) == 1:
            update_job(conn, job_id, status="paused", message="Sweep paused")
            return
    except Exception:
        pass

    try:
        sweep_def = json.loads(sweep_row.get("sweep_definition_json") or "{}")
    except Exception:
        sweep_def = {}

    params = sweep_def.get("params") if isinstance(sweep_def.get("params"), dict) else {}
    field_meta = sweep_def.get("field_meta") if isinstance(sweep_def.get("field_meta"), dict) else {}
    metadata_common = sweep_def.get("metadata_common") if isinstance(sweep_def.get("metadata_common"), dict) else {}

    keys = [str(k).strip() for k in params.keys() if str(k).strip()]
    value_lists: list[list[object]] = []
    for k in keys:
        vals = params.get(k)
        if not isinstance(vals, list) or not vals:
            value_lists.append([None])
            continue
        value_lists.append(list(vals))

    # Base snapshots
    try:
        base_config = json.loads(sweep_row.get("base_config_json") or "{}")
    except Exception:
        base_config = sweep_def.get("base_config") if isinstance(sweep_def.get("base_config"), dict) else {}
    if not isinstance(base_config, dict):
        base_config = {}

    base_data_settings = sweep_def.get("data_settings") if isinstance(sweep_def.get("data_settings"), dict) else None
    if not isinstance(base_data_settings, dict):
        try:
            range_params = json.loads(sweep_row.get("range_params_json") or "{}")
        except Exception:
            range_params = {}
        base_data_settings = {
            "data_source": sweep_row.get("data_source"),
            "exchange_id": sweep_row.get("exchange_id"),
            "symbol": sweep_row.get("symbol"),
            "timeframe": sweep_row.get("timeframe"),
            "range_mode": sweep_row.get("range_mode"),
            "range_params": range_params,
        }

    # Sweep assets
    sweep_assets: list[str] = []
    try:
        assets_raw = sweep_row.get("sweep_assets_json")
        if isinstance(assets_raw, str) and assets_raw.strip():
            parsed = json.loads(assets_raw)
            if isinstance(parsed, list):
                sweep_assets = [str(s).strip() for s in parsed if str(s).strip()]
    except Exception:
        sweep_assets = []
    if not sweep_assets:
        sym0 = str(sweep_row.get("symbol") or "").strip()
        if sym0:
            sweep_assets = [sym0]

    if not value_lists:
        value_lists = [[None]]

    jobs_to_enqueue: list[tuple[str, Dict[str, Any], Optional[str]]] = []
    total_seen = 0

    def _flush() -> bool:
        nonlocal total_seen
        if not jobs_to_enqueue:
            return True
        if not _maybe_renew_job_lease(conn=conn, job=job, worker_id=worker_id, lease_s=JOB_LEASE_S, renew_every_s=0.0):
            return False
        res = bulk_enqueue_backtest_run_jobs(
            conn,
            jobs=list(jobs_to_enqueue),
            parent_job_id=int(job_id),
            group_key=str(group_key),
        )
        created = int((res or {}).get("created") or 0)
        existing = int((res or {}).get("existing") or 0)
        total_seen += int(created + existing)
        jobs_to_enqueue.clear()
        try:
            update_job(conn, job_id, message=f"Planning sweep… queued {int(total_seen)}")
        except Exception:
            pass
        return True

    for sym in (sweep_assets or [""]):
        sym_norm = str(sym or "").strip()
        for combo_index, combo in enumerate(itertools.product(*value_lists), start=1):
            # Periodically honor pause/cancel.
            if not jobs_to_enqueue or (len(jobs_to_enqueue) % max(1, min(25, int(SWEEP_PLAN_BATCH_SIZE))) == 0):
                try:
                    flags = get_parent_job_flags(conn, int(job_id))
                    if int(flags.get("cancel_requested") or 0) == 1:
                        _flush()
                        update_job(conn, job_id, status="cancelled", message="Sweep cancelled", finished_at=_now_iso())
                        try:
                            update_sweep_status(str(sweep_id), "cancelled", error_message="cancel_requested")
                        except Exception:
                            pass
                        return
                    if int(flags.get("pause_requested") or 0) == 1:
                        _flush()
                        update_job(conn, job_id, status="paused", message="Sweep paused")
                        return
                except Exception:
                    pass

            combo_list = list(combo)
            param_key = _sweep_combo_key(keys, combo_list)
            pair_key = f"{sym_norm}|{param_key}" if sym_norm else param_key

            cfg_snapshot = json.loads(json.dumps(base_config))  # deep copy via JSON
            ds_snapshot = json.loads(json.dumps(base_data_settings))
            if not isinstance(cfg_snapshot, dict):
                cfg_snapshot = {}
            if not isinstance(ds_snapshot, dict):
                ds_snapshot = {}

            if sym_norm:
                ds_snapshot["symbol"] = sym_norm

            overrides: dict[str, object] = {}
            for field_key, raw_val in zip(keys, combo_list):
                meta = field_meta.get(field_key) if isinstance(field_meta, dict) else None
                if not isinstance(meta, dict):
                    continue
                target = str(meta.get("target") or "config").strip()
                path_raw = meta.get("path")
                if isinstance(path_raw, (list, tuple)):
                    path = [str(p).strip() for p in path_raw if str(p).strip()]
                else:
                    path = []
                if not path:
                    continue
                v = _coerce_sweep_value(str(meta.get("type") or "str"), raw_val)
                overrides[str(field_key)] = v

                if target == "config":
                    _set_nested(cfg_snapshot, path, v)
                elif target == "data_settings":
                    _set_nested(ds_snapshot, path, v)
                elif target == "range_params":
                    rp = ds_snapshot.get("range_params")
                    if not isinstance(rp, dict):
                        rp = {}
                    rp[str(path[-1])] = v
                    ds_snapshot["range_params"] = rp

            md = dict(metadata_common)
            md.update(
                {
                    "strategy_name": sweep_row.get("strategy_name"),
                    "strategy_version": sweep_row.get("strategy_version"),
                    "user_id": sweep_row.get("user_id"),
                    "sweep_combo_index": int(combo_index),
                    "sweep_combo_key": str(pair_key),
                    "sweep_overrides": overrides,
                    "symbol": str(ds_snapshot.get("symbol") or "").strip() or None,
                    "timeframe": str(ds_snapshot.get("timeframe") or "").strip() or None,
                }
            )

            job_payload = {
                "sweep_id": str(sweep_id),
                "config": cfg_snapshot,
                "data_settings": ds_snapshot,
                "metadata": md,
                "parent_job_id": int(job_id),
            }

            job_key = (
                f"backtest_run:sweep:{str(sweep_id)}:{pair_key}:"
                f"{str(ds_snapshot.get('data_source') or '')}:"
                f"{str(ds_snapshot.get('exchange_id') or '')}:"
                f"{str(ds_snapshot.get('market_type') or '')}:"
                f"{str(ds_snapshot.get('symbol') or '')}:"
                f"{str(ds_snapshot.get('timeframe') or '')}"
            )
            jobs_to_enqueue.append((job_key, job_payload, str(sweep_id)))

            if len(jobs_to_enqueue) >= max(1, int(SWEEP_PLAN_BATCH_SIZE)):
                if not _flush():
                    return

    _flush()

    if total_seen <= 0:
        update_job(conn, job_id, status="failed", error_text="no_child_jobs", finished_at=_now_iso())
        try:
            update_sweep_status(str(sweep_id), "failed", error_message="no_child_jobs")
        except Exception:
            pass
        return

    update_job(
        conn,
        job_id,
        status="done",
        progress=1.0,
        message=f"Planned sweep: total children {int(total_seen)}",
        finished_at=_now_iso(),
    )


def execute_backtest_run_job(*, conn, job: Dict[str, Any], worker_id: str) -> None:
    job_id = int(job.get("id"))

    payload_raw = job.get("payload_json") or job.get("payload")
    payload: Dict[str, Any] = {}
    if isinstance(payload_raw, str) and payload_raw.strip():
        payload = json.loads(payload_raw)
    elif isinstance(payload_raw, dict):
        payload = payload_raw

    # Fill in traceability keys in-memory.
    job_key = job.get("job_key") or payload.get("job_key")
    if job_key:
        payload.setdefault("job_key", str(job_key))

    # Mark started.
    update_job(conn, job_id, status="running", started_at=job.get("started_at") or _now_iso(), message="Running")

    # If this child belongs to a paused/cancelled sweep, stop early (v1).
    try:
        parent_id = payload.get("parent_job_id")
        if parent_id is None and job.get("parent_job_id") is not None:
            parent_id = job.get("parent_job_id")
        if parent_id is not None:
            flags = get_parent_job_flags(conn, int(parent_id))
            if int(flags.get("cancel_requested") or 0) == 1:
                update_job(conn, job_id, status="cancelled", message="Cancelled (sweep cancel requested)", finished_at=_now_iso())
                return
            if int(flags.get("pause_requested") or 0) == 1:
                update_job(conn, job_id, status="queued", message="Paused (sweep paused)")
                return
    except Exception:
        pass

    sweep_id = payload.get("sweep_id")
    if sweep_id is not None:
        try:
            update_sweep_status(str(sweep_id), "running")
        except Exception:
            pass

    config_snapshot = payload.get("config") or {}
    data_settings = DataSettings(**(payload.get("data_settings") or {}))

    # Best-effort prefetch for new sweep groups.
    try:
        group_key = job.get("group_key") or payload.get("group_key")
        if group_key:
            _prefetch_group_candles(conn=conn, group_key=str(group_key), worker_id=str(worker_id))
    except Exception:
        pass

    # Worker-local candle cache (in-memory) to reduce DB contention under concurrency.
    candles_override: Optional[list[Candle]] = None
    label_override: Optional[str] = None
    storage_symbol_override: Optional[str] = None
    storage_timeframe_override: Optional[str] = None
    try:
        if str(getattr(data_settings, "data_source", "") or "").lower() != "synthetic":
            candles_override = _load_candles_worker_cached(data_settings)
            try:
                exchange_id = data_settings.exchange_id or "binance"
                symbol = data_settings.symbol or "BTC/USDT"
                timeframe = data_settings.timeframe or "1h"
                label_override = f"Crypto (CCXT) – {exchange_id} {symbol} {timeframe}, {len(candles_override)} candles"
                storage_symbol_override = str(symbol)
                storage_timeframe_override = str(timeframe)
            except Exception:
                label_override = None
    except Exception:
        candles_override = None

    # Run compute without holding a transaction.
    cfg_for_storage, result, _label, storage_symbol, storage_timeframe = run_single_backtest(
        config_snapshot,
        data_settings,
        candles_override=candles_override,
        label_override=label_override,
        storage_symbol_override=storage_symbol_override,
        storage_timeframe_override=storage_timeframe_override,
    )

    # Always renew before the final write.
    if not _maybe_renew_job_lease(conn=conn, job=job, worker_id=worker_id, lease_s=JOB_LEASE_S, renew_every_s=0.0):
        return

    metadata_payload = dict(payload.get("metadata") or {})
    metadata_payload.setdefault("symbol", storage_symbol)
    metadata_payload.setdefault("timeframe", storage_timeframe)
    metadata_payload.setdefault(
        "start_time",
        result.start_time.isoformat() if getattr(result, "start_time", None) else None,
    )
    metadata_payload.setdefault(
        "end_time",
        result.end_time.isoformat() if getattr(result, "end_time", None) else None,
    )

    # Deterministic run_key for exactly-once persistence.
    run_key = payload.get("run_key")
    if not run_key:
        try:
            run_key = compute_run_key(payload)
        except Exception:
            # Fallback: compute using the updated metadata fields.
            payload2 = dict(payload)
            payload2["metadata"] = dict(metadata_payload)
            run_key = compute_run_key(payload2)
    run_key = str(run_key).strip()
    payload["run_key"] = run_key

    # Commit run + job completion atomically in one short transaction (exactly-once by run_key).
    run_id: Optional[str] = None

    run_row_fields, details_fields = build_backtest_run_rows(
        cfg_for_storage,
        result.metrics,
        metadata_payload,
        sweep_id=str(sweep_id) if sweep_id is not None else None,
        result=result,
    )
    # Embed keys into metadata for traceability.
    meta_obj = run_row_fields.get("metadata_json")
    if not isinstance(meta_obj, dict):
        meta_obj = {}
    meta_obj = dict(meta_obj)
    meta_obj["run_key"] = run_key
    if job_key:
        meta_obj["job_key"] = str(job_key)
    run_row_fields["metadata_json"] = meta_obj
    details_fields["metadata_json"] = dict(meta_obj)

    def _tx() -> None:
        nonlocal run_id
        # BEGIN ensures write serialization so two workers can't both insert a new run for the same run_key.
        conn.execute("BEGIN")
        try:
            run_id = upsert_backtest_run_by_run_key(
                conn,
                run_key=run_key,
                run_row_fields=run_row_fields,
                details_fields=details_fields,
            )

            # Update job payload with result linkage.
            payload2 = dict(payload)
            payload2["run_key"] = run_key
            payload2["result_run_id"] = str(run_id)
            if job_key:
                payload2["job_key"] = str(job_key)

            now_iso = _now_iso()
            conn.execute(
                """
                UPDATE jobs
                SET status = %s, finished_at = %s, progress = %s, message = %s, run_id = %s, payload_json = %s, updated_at = %s
                WHERE id = %s
                """,
                (
                    "done",
                    now_iso,
                    1.0,
                    f"Done ({storage_symbol} {storage_timeframe})",
                    str(run_id),
                    json.dumps(payload2),
                    now_iso,
                    int(job_id),
                ),
            )

            if sweep_id is not None:
                finalize_sweep_if_complete(conn, str(sweep_id))

            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    try:
        _write_tx_with_retry(_tx, timeout_s=8.0)
    except Exception as exc:
        err = _exc_text()
        logger.exception("Backtest run job failed")

        def _tx_fail() -> None:
            conn.execute("BEGIN")
            try:
                now_iso = _now_iso()
                conn.execute(
                    "UPDATE jobs SET status = %s, error_text = %s, finished_at = %s, updated_at = %s WHERE id = %s",
                    ("failed", err[-4000:], now_iso, now_iso, int(job_id)),
                )
                if sweep_id is not None:
                    finalize_sweep_if_complete(conn, str(sweep_id))
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise

        try:
            _write_tx_with_retry(_tx_fail, timeout_s=8.0)
        except Exception:
            pass

        # Preserve prior best-effort behavior of surfacing sweep error message.
        if sweep_id is not None:
            try:
                update_sweep_status(str(sweep_id), "failed", error_message=str(exc))
            except Exception:
                pass


def _poll_once(worker_id: str) -> int:
    """Process at most one job per tick. Returns number of jobs processed."""

    with open_db_connection() as conn:
        init_db()  # ensure schema
        now_iso = _now_iso()

                # Reclaim one stale running job (planner or run).
        try:
            stale = conn.execute(
                """
                SELECT *
                FROM jobs
                                WHERE job_type IN ('sweep_parent','backtest_run')
                  AND status = 'running'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < %s
                ORDER BY lease_expires_at ASC
                LIMIT 1
                """,
                (now_iso,),
            ).fetchone()
            if stale:
                stale_job = dict(stale)
                stale_job_id = int(stale_job.get("id"))
                ok = reclaim_stale_job(conn, job_id=stale_job_id, new_worker_id=str(worker_id), lease_s=float(JOB_LEASE_S))
                if ok:
                    reclaimed = get_job(conn, stale_job_id)
                    if reclaimed and reclaimed.get("id") is not None:
                        jid = int(reclaimed.get("id"))
                        _JOB_EXPECTED_LEASE_VERSION[jid] = int(reclaimed.get("lease_version") or 0)
                        _JOB_LAST_LEASE_RENEW_S[jid] = 0.0
        except Exception:
            pass

        # Claim one queued job. Prefer sweep planners to fan out quickly.
        claimed = None
        try:
            claimed = claim_job_with_lease(conn, worker_id=str(worker_id), lease_s=float(JOB_LEASE_S), job_types=("sweep_parent",))
        except Exception:
            claimed = None
        if not claimed:
            try:
                claimed = claim_job_with_lease(conn, worker_id=str(worker_id), lease_s=float(JOB_LEASE_S), job_types=("backtest_run",))
            except Exception:
                claimed = None

        if claimed and claimed.get("id") is not None:
            try:
                jid = int(claimed.get("id"))
                _JOB_EXPECTED_LEASE_VERSION[jid] = int(claimed.get("lease_version") or 0)
                _JOB_LAST_LEASE_RENEW_S[jid] = 0.0
            except Exception:
                pass

        if not claimed:
            return 0

        # Process it.
        try:
            if not _maybe_renew_job_lease(conn=conn, job=claimed, worker_id=worker_id, lease_s=JOB_LEASE_S, renew_every_s=0.0):
                return 0
            if str(claimed.get("job_type") or "").strip() == "sweep_parent":
                execute_sweep_parent_job(conn=conn, job=claimed, worker_id=worker_id)
            else:
                execute_backtest_run_job(conn=conn, job=claimed, worker_id=worker_id)
            return 1
        except Exception:
            err = _exc_text()
            try:
                update_job(conn, int(claimed.get("id")), status="failed", error_text=err[-4000:], finished_at=_now_iso())
            except Exception:
                pass
            return 1


def main(argv: Optional[list[str]] = None) -> int:
    global POLL_SECONDS, JOB_LEASE_S, JOB_LEASE_RENEW_EVERY_S

    parser = argparse.ArgumentParser(description="Project Dragon backtest worker")
    parser.add_argument(
        "--worker-id",
        default=os.environ.get("DRAGON_WORKER_ID")
        or f"backtest:{socket.gethostname()}:{os.getpid()}:{int(time.time())}",
    )
    parser.add_argument("--poll-s", type=float, default=POLL_SECONDS)
    parser.add_argument("--lease-s", type=float, default=JOB_LEASE_S)
    parser.add_argument("--renew-every-s", type=float, default=JOB_LEASE_RENEW_EVERY_S)
    parser.add_argument("--max-jobs", type=int, default=0, help="Exit after processing N jobs (0=run forever)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=os.environ.get("DRAGON_LOG_LEVEL", "INFO"))

    # Allow CLI overrides.
    POLL_SECONDS = float(args.poll_s)
    JOB_LEASE_S = float(args.lease_s)
    JOB_LEASE_RENEW_EVERY_S = float(args.renew_every_s)

    processed = 0
    while True:
        n = _poll_once(str(args.worker_id))
        processed += int(n)
        if args.max_jobs and processed >= int(args.max_jobs):
            return 0
        time.sleep(max(0.05, float(POLL_SECONDS)))


if __name__ == "__main__":
    raise SystemExit(main())
