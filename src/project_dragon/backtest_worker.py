from __future__ import annotations

import argparse
import itertools
import json
import hashlib
import logging
import os
import socket
import time
import threading
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Iterable, Tuple

from project_dragon.backtest_core import DataSettings, compute_analysis_window_from_snapshot, run_single_backtest
from project_dragon.candle_cache import get_candles_analysis_window, enqueue_prefetch_analysis_window
from project_dragon.data_online import get_candles_with_cache, load_ccxt_candles, timeframe_to_milliseconds
from project_dragon.domain import Candle
from project_dragon.storage import (
    build_backtest_run_rows,
    bulk_enqueue_backtest_run_jobs,
    bulk_mark_jobs_cancelled,
    claim_job_with_lease,
    compute_run_key,
    ensure_sweep_parent_jobs,
    finalize_sweep_if_complete,
    get_backtest_run_job_counts_by_sweep,
    get_job,
    get_parent_job_flags,
    get_sweep_row,
    init_db,
    open_db_connection,
    requeue_orphaned_running_jobs,
    requeue_expired_lease_jobs,
    renew_job_lease,
    upsert_backtest_run_by_run_key,
    update_job,
    update_sweep_status,
    get_cached_coverage,
)

logger = logging.getLogger(__name__)

POLL_SECONDS = float(os.environ.get("DRAGON_WORKER_POLL_S", "1.0") or 1.0)
JOB_LEASE_S = float(os.environ.get("DRAGON_JOB_LEASE_S", "30") or 30.0)
JOB_LEASE_RENEW_EVERY_S = float(os.environ.get("DRAGON_JOB_LEASE_RENEW_EVERY_S", "10") or 10.0)

# Sweep planning chunk size (limits memory and allows pause/cancel to take effect quickly).
SWEEP_PLAN_BATCH_SIZE = int(os.environ.get("DRAGON_SWEEP_PLAN_BATCH_SIZE", "250") or 250)

# In-process lease tracking (like live_worker).
_JOB_EXPECTED_LEASE_VERSION: Dict[int, int] = {}
_JOB_LAST_LEASE_RENEW_S: Dict[int, float] = {}
_CANDLE_CACHE_PREFETCH_TOP_N = int(os.environ.get("DRAGON_CANDLE_CACHE_PREFETCH_TOP_N", "2") or 2)
_CANDLE_CACHE_PREFETCH_QUERY_LIMIT = 50
_CANDLE_CACHE_PREFETCH_BUDGET_S = 2.0
_LAST_PREFETCH_GROUP_KEY: Optional[str] = None
_CACHE_PREFLIGHT_LOCKS: Dict[tuple[str, str, str, str], threading.Lock] = {}
_CANDLE_CACHE_END_TOLERANCE_BARS = int(os.environ.get("DRAGON_CANDLE_CACHE_END_TOLERANCE_BARS", "2") or 2)
_CANDLE_CACHE_END_TOLERANCE_RECENT_BARS = int(
    os.environ.get("DRAGON_CANDLE_CACHE_END_TOLERANCE_RECENT_BARS", "12") or 12
)
_CANDLE_CACHE_END_TOLERANCE_RECENT_WINDOW_H = int(
    os.environ.get("DRAGON_CANDLE_CACHE_END_TOLERANCE_RECENT_WINDOW_H", "24") or 24
)


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


def _load_candles_worker_cached(ds: DataSettings) -> list[Candle]:
    params = ds.range_params or {}
    exchange_id = ds.exchange_id or "binance"
    symbol = ds.symbol or "BTC/USDT"
    timeframe = ds.timeframe or "1h"
    market_type = ds.market_type or "unknown"
    range_mode = (ds.range_mode or "bars").lower()
    try:
        _analysis_start, _display_start, _display_end, candles_analysis, _candles_display = get_candles_analysis_window(
            data_settings=ds,
            analysis_info=None,
            display_start_ms=_to_ms(params.get("since")),
            display_end_ms=_to_ms(params.get("until")),
            fetch_fn=get_candles_with_cache,
        )
        return list(candles_analysis)
    except Exception as exc:
        msg = str(exc).lower()
        if "database is locked" in msg or "locked" in msg:
            return list(
                load_ccxt_candles(
                    exchange_id,
                    symbol,
                    timeframe,
                    limit=params.get("limit"),
                    since=params.get("since"),
                    until=params.get("until"),
                    range_mode=range_mode,
                    market_type=market_type,
                )
            )
        raise


def _prefetch_group_candles(*, conn, group_key: str, worker_id: str) -> None:
    global _LAST_PREFETCH_GROUP_KEY

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
    key_to_payload: Dict[tuple, tuple[DataSettings, Dict[str, Any]]] = {}
    for r in rows or []:
        if (time.time() - start_s) > float(_CANDLE_CACHE_PREFETCH_BUDGET_S):
            break
        try:
            payload_raw = r[0] if not isinstance(r, dict) else r.get("payload_json")
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) and payload_raw.strip() else {}
            ds = DataSettings(**(payload.get("data_settings") or {}))
            config_snapshot = payload.get("config") or {}
            try:
                analysis_info = compute_analysis_window_from_snapshot(config_snapshot, ds)
            except Exception:
                analysis_info = {}
            params = getattr(ds, "range_params", None) or {}
            analysis_start_ms = analysis_info.get("analysis_start_ms") or _to_ms(params.get("since"))
            analysis_end_ms = analysis_info.get("analysis_end_ms") or _to_ms(params.get("until"))
            try:
                analysis_limit = analysis_info.get("analysis_limit")
                if analysis_limit is None and params.get("limit") is not None:
                    analysis_limit = int(params.get("limit"))
            except Exception:
                analysis_limit = None
            k = (
                str(getattr(ds, "exchange_id", None) or "binance").lower(),
                str(getattr(ds, "market_type", None) or "unknown").lower(),
                str(getattr(ds, "symbol", None) or "BTC/USDT"),
                str(getattr(ds, "timeframe", None) or "1h"),
                str(getattr(ds, "range_mode", None) or "bars").lower(),
                int(analysis_start_ms or 0),
                int(analysis_end_ms or 0),
                int(analysis_limit or 0),
            )
            keys.append(k)
            if k not in key_to_payload:
                key_to_payload[k] = (ds, analysis_info)
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
        try:
            ds, analysis_info = key_to_payload.get(k, (None, None))
            if ds is None:
                continue
            enqueue_prefetch_analysis_window(data_settings=ds, analysis_info=analysis_info or {}, reason=f"group:{gk}")
            prefetched += 1
        except Exception:
            continue

    if prefetched > 0:
        logger.info(
            "candle_cache prefetch worker=%s group=%s prefetched=%s",
            str(worker_id),
            str(gk),
            int(prefetched),
        )


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _exc_text() -> str:
    import traceback

    return "".join(traceback.format_exception(*traceback.sys.exc_info()))


def _commit_quietly(conn: Any) -> None:
    try:
        conn.commit()
    except Exception:
        pass


def _start_lease_heartbeat(
    *,
    job_id: int,
    worker_id: str,
    lease_s: float,
    interval_s: float,
) -> tuple[threading.Event, Dict[str, Any]]:
    stop_event = threading.Event()
    status: Dict[str, Any] = {"lost": False}
    expected = _JOB_EXPECTED_LEASE_VERSION.get(job_id)
    if expected is None:
        expected = 0
    expected = int(expected or 0)

    def _runner() -> None:
        try:
            with open_db_connection() as hb_conn:
                while not stop_event.wait(float(interval_s)):
                    ok = renew_job_lease(
                        hb_conn,
                        job_id=int(job_id),
                        worker_id=str(worker_id),
                        lease_s=float(lease_s),
                        expected_lease_version=int(expected),
                    )
                    _JOB_LAST_LEASE_RENEW_S[int(job_id)] = float(time.time())
                    if not ok:
                        status["lost"] = True
                        break
        except Exception:
            status["lost"] = True

    thread = threading.Thread(target=_runner, name=f"lease_hb_{job_id}", daemon=True)
    thread.start()
    status["thread"] = thread
    return stop_event, status


def _parse_time_ms(value: Any) -> Optional[int]:
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
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            try:
                return int(float(s))
            except Exception:
                return None
    return None


def _align_range_to_timeframe(
    *,
    start_ms: Optional[int],
    end_ms: Optional[int],
    timeframe: str,
) -> Tuple[Optional[int], Optional[int], int]:
    if end_ms is None:
        return start_ms, end_ms, 0
    try:
        ms_per_bar = int(timeframe_to_milliseconds(str(timeframe or "1h")))
    except Exception:
        ms_per_bar = 0
    if ms_per_bar <= 0:
        return start_ms, end_ms, 0
    # Align to the previous closed candle (use candle open timestamps).
    aligned_end = int(end_ms) - (int(end_ms) % int(ms_per_bar)) - int(ms_per_bar)
    if aligned_end < 0:
        aligned_end = 0
    if aligned_end == int(end_ms):
        return start_ms, end_ms, 0
    delta = int(end_ms) - int(aligned_end)
    aligned_start = int(start_ms) - delta if start_ms is not None else None
    return aligned_start, aligned_end, int(delta)


def _cache_lock_key(exchange_id: str, market_type: str, symbol: str, timeframe: str) -> tuple[str, str, str, str]:
    return (
        str(exchange_id or "").strip().lower(),
        str(market_type or "").strip().lower(),
        str(symbol or "").strip(),
        str(timeframe or "").strip(),
    )


def _get_cache_lock(key: tuple[str, str, str, str]) -> threading.Lock:
    lock = _CACHE_PREFLIGHT_LOCKS.get(key)
    if lock is None:
        lock = threading.Lock()
        _CACHE_PREFLIGHT_LOCKS[key] = lock
    return lock


def _ensure_candle_cache(
    *,
    conn,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
    limit: Optional[int],
    job_id: int,
    label: str,
) -> Tuple[bool, str]:
    lock_key = _cache_lock_key(exchange_id, market_type, symbol, timeframe)
    lock = _get_cache_lock(lock_key)
    with lock:
        try:
            min_ts, max_ts = get_cached_coverage(conn, exchange_id, market_type, symbol, timeframe)
        except Exception:
            min_ts, max_ts = None, None
        _commit_quietly(conn)

        coverage_ok = False
        if start_ms is not None and end_ms is not None:
            coverage_ok = min_ts is not None and max_ts is not None and min_ts <= start_ms and max_ts >= end_ms
        elif start_ms is None and end_ms is not None:
            coverage_ok = max_ts is not None and max_ts >= end_ms
        elif start_ms is not None and end_ms is None:
            coverage_ok = min_ts is not None and min_ts <= start_ms

        if coverage_ok:
            return True, "coverage_ok"

        logger.info(
            "cache_missing_ranges job_id=%s label=%s symbol=%s timeframe=%s start_ms=%s end_ms=%s",
            int(job_id),
            str(label),
            str(symbol),
            str(timeframe),
            int(start_ms or 0) if start_ms is not None else None,
            int(end_ms or 0) if end_ms is not None else None,
        )
        _commit_quietly(conn)
        try:
            if limit is None and start_ms is not None and end_ms is not None:
                ms_per_bar = int(timeframe_to_milliseconds(str(timeframe or "1h")) or 0)
                if ms_per_bar > 0:
                    span_ms = max(0, int(end_ms) - int(start_ms))
                    est = int(span_ms // ms_per_bar) + 2
                    limit = min(max(est, 1), 250_000)
            candles = get_candles_with_cache(
                exchange_id=exchange_id,
                symbol=symbol,
                timeframe=timeframe,
                limit=limit,
                since=start_ms,
                until=end_ms,
                range_mode="date range" if start_ms is not None or end_ms is not None else "bars",
                market_type=market_type,
            )
            logger.info(
                "cache_fetch_progress job_id=%s label=%s fetched=%s",
                int(job_id),
                str(label),
                int(len(candles or [])),
            )
        except Exception as exc:
            return False, f"fetch_failed:{exc}"

        if start_ms is None and end_ms is None:
            return True, "fetched_bars"

        try:
            min_ts, max_ts = get_cached_coverage(conn, exchange_id, market_type, symbol, timeframe)
        except Exception:
            min_ts, max_ts = None, None
        _commit_quietly(conn)
        if min_ts is None or max_ts is None:
            return False, "coverage_missing"
        if start_ms is not None and min_ts > start_ms:
            return False, "coverage_start_missing"
        if end_ms is not None and max_ts < end_ms:
            try:
                ms_per_bar = timeframe_to_milliseconds(str(timeframe or "1h"))
                tolerance_ms = max(0, int(_CANDLE_CACHE_END_TOLERANCE_BARS)) * int(ms_per_bar)
            except Exception:
                tolerance_ms = 0
            if tolerance_ms and max_ts + tolerance_ms >= end_ms:
                return True, "coverage_end_tolerated"
            try:
                recent_window_ms = max(0, int(_CANDLE_CACHE_END_TOLERANCE_RECENT_WINDOW_H)) * 3600 * 1000
            except Exception:
                recent_window_ms = 0
            if recent_window_ms:
                now_ms = int(time.time() * 1000)
                if int(end_ms) >= (now_ms - int(recent_window_ms)):
                    try:
                        recent_tolerance_ms = max(
                            int(tolerance_ms),
                            int(_CANDLE_CACHE_END_TOLERANCE_RECENT_BARS) * int(ms_per_bar),
                        )
                    except Exception:
                        recent_tolerance_ms = int(tolerance_ms or 0)
                    if recent_tolerance_ms and max_ts + recent_tolerance_ms >= end_ms:
                        return True, "coverage_end_recent_tolerated"
            return False, "coverage_end_missing"
        return True, "coverage_ok"


def _cancel_if_requested(
    *,
    conn,
    job_id: int,
    reason: str,
    sweep_id: Optional[str] = None,
) -> bool:
    try:
        job_row = get_job(conn, int(job_id))
    except Exception:
        job_row = None
    if not isinstance(job_row, dict):
        return False
    status = str(job_row.get("status") or "").strip().lower()
    try:
        cancel_flag = int(job_row.get("cancel_requested") or 0)
    except Exception:
        cancel_flag = 0
    if status != "cancel_requested" and cancel_flag != 1:
        return False

    logger.info("cancel_detected job_id=%s status=%s", int(job_id), status)
    update_job(
        conn,
        int(job_id),
        status="cancelled",
        message=str(reason or "cancel_requested"),
        finished_at=_now_iso(),
    )
    if sweep_id:
        try:
            update_sweep_status(str(sweep_id), "cancelled", error_message="cancel_requested")
        except Exception:
            pass
    logger.info("cancel_finalized job_id=%s status=cancelled", int(job_id))
    return True


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
            try:
                bulk_mark_jobs_cancelled(conn, group_key)
            except Exception:
                pass
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

    # Sweep assets (needed for preflight symbol union)
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

    # --- Sweep candle cache preflight (union of symbol/timeframe pairs) ---
    try:
        data_source_key = str(base_data_settings.get("data_source") or "ccxt").strip().lower()
    except Exception:
        data_source_key = "ccxt"
    if data_source_key == "ccxt":
        base_exchange = str(base_data_settings.get("exchange_id") or "").strip()
        base_symbol = str(base_data_settings.get("symbol") or "").strip()
        base_timeframe = str(base_data_settings.get("timeframe") or "").strip() or "1h"
        base_market_type = str(base_data_settings.get("market_type") or "unknown").strip().lower() or "unknown"
        base_range_params = base_data_settings.get("range_params") if isinstance(base_data_settings.get("range_params"), dict) else {}

        symbols: set[str] = set()
        timeframes: set[str] = set()
        if sweep_assets:
            symbols.update([str(s).strip() for s in sweep_assets if str(s).strip()])
        if base_symbol:
            symbols.add(base_symbol)
        if base_timeframe:
            timeframes.add(base_timeframe)

        # Include swept symbols/timeframes from data_settings meta.
        for field_key, vals in (params or {}).items():
            meta = field_meta.get(field_key) if isinstance(field_meta, dict) else None
            if not isinstance(meta, dict):
                continue
            target = str(meta.get("target") or "").strip().lower()
            path_raw = meta.get("path")
            path = [str(p).strip() for p in path_raw] if isinstance(path_raw, (list, tuple)) else []
            if target == "data_settings" and path:
                if path[-1] == "symbol":
                    symbols.update([str(v).strip() for v in (vals or []) if str(v).strip()])
                if path[-1] == "timeframe":
                    timeframes.update([str(v).strip() for v in (vals or []) if str(v).strip()])

        symbols = {s for s in symbols if s}
        timeframes = {t for t in timeframes if t}
        if not symbols:
            symbols = {base_symbol} if base_symbol else set()
        if not timeframes:
            timeframes = {base_timeframe} if base_timeframe else set()

        # Derive union time window (min start, max end) if available.
        start_candidates: list[int] = []
        end_candidates: list[int] = []
        if isinstance(base_range_params, dict):
            s0 = _parse_time_ms(base_range_params.get("since"))
            e0 = _parse_time_ms(base_range_params.get("until"))
            if s0 is not None:
                start_candidates.append(int(s0))
            if e0 is not None:
                end_candidates.append(int(e0))

        for field_key, vals in (params or {}).items():
            meta = field_meta.get(field_key) if isinstance(field_meta, dict) else None
            if not isinstance(meta, dict):
                continue
            target = str(meta.get("target") or "").strip().lower()
            path_raw = meta.get("path")
            path = [str(p).strip() for p in path_raw] if isinstance(path_raw, (list, tuple)) else []
            if target == "range_params" and path:
                if path[-1] == "since":
                    for v in (vals or []):
                        ts = _parse_time_ms(v)
                        if ts is not None:
                            start_candidates.append(int(ts))
                if path[-1] == "until":
                    for v in (vals or []):
                        ts = _parse_time_ms(v)
                        if ts is not None:
                            end_candidates.append(int(ts))

        union_start = min(start_candidates) if start_candidates else None
        union_end = max(end_candidates) if end_candidates else None
        union_limit = None
        try:
            union_limit = int(base_range_params.get("limit")) if isinstance(base_range_params, dict) and base_range_params.get("limit") is not None else None
        except Exception:
            union_limit = None

        logger.info(
            "sweep_preflight_start job_id=%s sweep_id=%s symbols=%s timeframes=%s",
            int(job_id),
            str(sweep_id),
            int(len(symbols)),
            int(len(timeframes)),
        )

        for sym in sorted(symbols):
            for tf in sorted(timeframes):
                ds_snapshot = DataSettings(
                    **{
                        **base_data_settings,
                        "exchange_id": base_exchange,
                        "symbol": sym,
                        "timeframe": tf,
                    }
                )
                try:
                    analysis_info = compute_analysis_window_from_snapshot(base_config, ds_snapshot)
                except Exception:
                    analysis_info = {}

                start_ms = analysis_info.get("analysis_start_ms")
                end_ms = analysis_info.get("analysis_end_ms")
                limit_ms = analysis_info.get("analysis_limit")
                start_ms = start_ms if start_ms is not None else union_start
                end_ms = end_ms if end_ms is not None else union_end
                limit_val = int(limit_ms) if limit_ms is not None else union_limit

                start_ms, end_ms, _delta = _align_range_to_timeframe(
                    start_ms=start_ms,
                    end_ms=end_ms,
                    timeframe=tf,
                )

                ok, reason = _ensure_candle_cache(
                    conn=conn,
                    exchange_id=base_exchange,
                    market_type=base_market_type,
                    symbol=sym,
                    timeframe=tf,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    limit=limit_val,
                    job_id=job_id,
                    label=f"sweep:{sweep_id}",
                )
                if not ok:
                    update_job(conn, job_id, status="failed", error_text=f"sweep_preflight_failed:{sym}:{tf}:{reason}", finished_at=_now_iso())
                    try:
                        update_sweep_status(str(sweep_id), "failed", error_message=f"preflight_failed:{sym}:{tf}")
                    except Exception:
                        pass
                    logger.error(
                        "sweep_preflight_failed job_id=%s sweep_id=%s symbol=%s timeframe=%s reason=%s",
                        int(job_id),
                        str(sweep_id),
                        str(sym),
                        str(tf),
                        str(reason),
                    )
                    return

        logger.info("sweep_preflight_done job_id=%s sweep_id=%s", int(job_id), str(sweep_id))

    if not value_lists:
        value_lists = [[None]]

    jobs_to_enqueue: list[tuple[str, Dict[str, Any], Optional[str]]] = []
    total_seen = 0
    prefetch_candidates: list[tuple[DataSettings, Dict[str, Any]]] = []
    try:
        prefetch_top_n = max(0, int(_CANDLE_CACHE_PREFETCH_TOP_N or 0))
    except Exception:
        prefetch_top_n = 0

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
                        try:
                            bulk_mark_jobs_cancelled(conn, group_key)
                        except Exception:
                            pass
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

            # Best-effort prefetch candidates during planning (non-blocking enqueue).
            try:
                if prefetch_top_n > 0 and len(prefetch_candidates) < prefetch_top_n:
                    ds_obj = DataSettings(**(ds_snapshot or {}))
                    ai = compute_analysis_window_from_snapshot(cfg_snapshot, ds_obj)
                    prefetch_candidates.append((ds_obj, dict(ai or {})))
            except Exception:
                pass

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

    # Kick prefetch for a few representative runs as soon as the sweep is planned.
    try:
        for ds_obj, ai in prefetch_candidates:
            enqueue_prefetch_analysis_window(data_settings=ds_obj, analysis_info=ai, reason=f"sweep_plan:{sweep_id}")
    except Exception:
        pass

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

    # Honor cancel before starting.
    if _cancel_if_requested(conn=conn, job_id=job_id, reason="Cancelled before start", sweep_id=payload.get("sweep_id")):
        return

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

    analysis_info: Dict[str, Any] = {}
    try:
        analysis_info = compute_analysis_window_from_snapshot(config_snapshot, data_settings)
    except Exception:
        analysis_info = {}

    # Align range to last completed bar if end_ms is not on a candle boundary.
    try:
        display_start_ms = analysis_info.get("display_start_ms")
        display_end_ms = analysis_info.get("display_end_ms")
        aligned_display_start, aligned_display_end, delta = _align_range_to_timeframe(
            start_ms=display_start_ms,
            end_ms=display_end_ms,
            timeframe=str(getattr(data_settings, "timeframe", "1h") or "1h"),
        )
        if delta:
            analysis_info["display_start_ms"] = aligned_display_start
            analysis_info["display_end_ms"] = aligned_display_end
            if analysis_info.get("analysis_start_ms") is not None:
                analysis_info["analysis_start_ms"] = int(analysis_info.get("analysis_start_ms") or 0) - int(delta)
            if analysis_info.get("analysis_end_ms") is not None:
                analysis_info["analysis_end_ms"] = int(analysis_info.get("analysis_end_ms") or 0) - int(delta)
            rp = dict(getattr(data_settings, "range_params", None) or {})
            if aligned_display_end is not None:
                rp["until"] = int(aligned_display_end)
            if aligned_display_start is not None:
                rp["since"] = int(aligned_display_start)
            data_settings.range_params = rp
    except Exception:
        pass

    if str(getattr(data_settings, "data_source", "") or "").lower() != "synthetic":
        try:
            ok, reason = _ensure_candle_cache(
                conn=conn,
                exchange_id=str(getattr(data_settings, "exchange_id", "") or ""),
                market_type=str(getattr(data_settings, "market_type", "unknown") or "unknown"),
                symbol=str(getattr(data_settings, "symbol", "") or ""),
                timeframe=str(getattr(data_settings, "timeframe", "") or ""),
                start_ms=analysis_info.get("analysis_start_ms"),
                end_ms=analysis_info.get("analysis_end_ms"),
                limit=analysis_info.get("analysis_limit"),
                job_id=job_id,
                label="single_run",
            )
            if not ok:
                update_job(conn, job_id, status="failed", error_text=f"preflight_failed:{reason}", finished_at=_now_iso())
                logger.error("preflight_failed job_id=%s reason=%s", int(job_id), str(reason))
                return
        except Exception as exc:
            update_job(conn, job_id, status="failed", error_text=f"preflight_error:{exc}", finished_at=_now_iso())
            logger.exception("preflight_error job_id=%s", int(job_id))
            return
        _commit_quietly(conn)

    # Best-effort prefetch for new sweep groups.
    try:
        group_key = job.get("group_key") or payload.get("group_key")
        if group_key:
            _prefetch_group_candles(conn=conn, group_key=str(group_key), worker_id=str(worker_id))
    except Exception:
        pass
    _commit_quietly(conn)

    # Worker-local candle cache (in-memory) to reduce DB contention under concurrency.
    candles_override: Optional[list[Candle]] = None
    label_override: Optional[str] = None
    storage_symbol_override: Optional[str] = None
    storage_timeframe_override: Optional[str] = None
    try:
        if str(getattr(data_settings, "data_source", "") or "").lower() != "synthetic":
            params = dict(data_settings.range_params or {})
            if analysis_info.get("analysis_start_ms") is not None:
                params["since"] = analysis_info.get("analysis_start_ms")
            if analysis_info.get("analysis_end_ms") is not None:
                params["until"] = analysis_info.get("analysis_end_ms")
            if analysis_info.get("analysis_limit") is not None:
                params["limit"] = analysis_info.get("analysis_limit")
            fetch_range_mode = analysis_info.get("effective_range_mode") or data_settings.range_mode
            data_settings_fetch = DataSettings(
                **{**data_settings.__dict__, "range_params": params, "range_mode": fetch_range_mode}
            )
            candles_override = _load_candles_worker_cached(data_settings_fetch)
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
    _commit_quietly(conn)

    # Keep the lease alive during long compute.
    lease_stop, lease_status = _start_lease_heartbeat(
        job_id=int(job_id),
        worker_id=str(worker_id),
        lease_s=float(JOB_LEASE_S),
        interval_s=max(5.0, float(JOB_LEASE_RENEW_EVERY_S or 10.0)),
    )

    # Run compute without holding a transaction.
    cfg_for_storage, result, _label, storage_symbol, storage_timeframe = run_single_backtest(
        config_snapshot,
        data_settings,
        candles_override=candles_override,
        label_override=label_override,
        storage_symbol_override=storage_symbol_override,
        storage_timeframe_override=storage_timeframe_override,
    )

    lease_stop.set()
    try:
        thread = lease_status.get("thread")
        if thread is not None:
            thread.join(timeout=2.0)
    except Exception:
        pass
    if lease_status.get("lost"):
        try:
            update_job(conn, job_id, status="error", error_text="job_lease_lost", finished_at=_now_iso())
        except Exception:
            pass
        return

    # Honor cancel before persisting results.
    if _cancel_if_requested(conn=conn, job_id=job_id, reason="Cancelled after run", sweep_id=payload.get("sweep_id")):
        return

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

        # Ensure running/paused sweeps have a planner job after restarts.
        try:
            ensure_sweep_parent_jobs(conn)
        except Exception:
            pass

        # Requeue legacy running jobs without leases (resume after restart).
        try:
            requeue_orphaned_running_jobs(
                conn,
                job_types=("sweep_parent", "backtest_run"),
                older_than_s=float(JOB_LEASE_S or 30.0),
            )
        except Exception:
            pass

        # Requeue jobs with expired leases so they can be claimed again.
        try:
            requeue_expired_lease_jobs(
                conn,
                job_types=("sweep_parent", "backtest_run"),
            )
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
