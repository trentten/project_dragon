from __future__ import annotations

import logging
import math
import os
import threading
import queue
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List, Callable

from project_dragon.domain import Candle
from project_dragon.data_online import get_candles_with_cache

logger = logging.getLogger(__name__)

# Cache sizing knobs
_CANDLE_CACHE_MAX_ITEMS = int(os.environ.get("DRAGON_CANDLE_CACHE_MAX_ITEMS", "64") or 64)
_CANDLE_CACHE_MAX_MB = float(os.environ.get("DRAGON_CANDLE_CACHE_MB", "256") or 256)
_CANDLE_CACHE_CHUNK_SEC = int(os.environ.get("DRAGON_CANDLE_CACHE_CHUNK_SEC", "21600") or 21600)  # 6h
_CANDLE_CACHE_PREFETCH = int(os.environ.get("DRAGON_CANDLE_CACHE_PREFETCH", "1") or 1)
_CANDLE_CACHE_PREFETCH_QUEUE_SIZE = int(os.environ.get("DRAGON_CANDLE_CACHE_PREFETCH_QUEUE_SIZE", "64") or 64)

_BYTES_PER_CANDLE_EST = 80  # rough estimate; keeps memory bounded without heavy introspection

_PREFETCH_Q: "queue.Queue[tuple[Any, Dict[str, Any], str]]" = queue.Queue(maxsize=max(1, _CANDLE_CACHE_PREFETCH_QUEUE_SIZE))
_PREFETCH_WORKER_STARTED = False
_PREFETCH_WORKER_LOCK = threading.Lock()


class LruCandleCache:
    def __init__(self, *, max_items: int, max_bytes: int) -> None:
        self.max_items = max(1, int(max_items))
        self.max_bytes = max(1, int(max_bytes))
        self._od: "OrderedDict[tuple, list[Candle]]" = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()

    def _estimate_bytes(self, candles: list[Candle]) -> int:
        try:
            return int(len(candles)) * int(_BYTES_PER_CANDLE_EST)
        except Exception:
            return 0

    def get(self, key: tuple) -> Optional[list[Candle]]:
        with self._lock:
            if key not in self._od:
                return None
            candles = self._od.pop(key)
            self._od[key] = candles
            return candles

    def set(self, key: tuple, candles: list[Candle]) -> None:
        if candles is None:
            return
        with self._lock:
            if key in self._od:
                old = self._od.pop(key)
                self._bytes -= self._estimate_bytes(old)
            self._od[key] = candles
            self._bytes += self._estimate_bytes(candles)
            self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        while len(self._od) > self.max_items or self._bytes > self.max_bytes:
            try:
                _k, v = self._od.popitem(last=False)
                self._bytes -= self._estimate_bytes(v)
            except KeyError:
                self._bytes = 0
                break

    @property
    def bytes(self) -> int:
        with self._lock:
            return int(self._bytes)

    @property
    def entries(self) -> int:
        with self._lock:
            return int(len(self._od))


_GLOBAL_CACHE = LruCandleCache(
    max_items=_CANDLE_CACHE_MAX_ITEMS,
    max_bytes=int(_CANDLE_CACHE_MAX_MB * 1024 * 1024),
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


def _chunk_range(start_ms: Optional[int], end_ms: Optional[int], *, chunk_sec: int) -> Tuple[Optional[int], Optional[int]]:
    if start_ms is None or end_ms is None:
        return None, None
    chunk_ms = max(60, int(chunk_sec)) * 1000
    c_start = int(start_ms // chunk_ms) * chunk_ms
    c_end = int(math.ceil(end_ms / float(chunk_ms)) * chunk_ms)
    return c_start, c_end


def _cache_key(
    *,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    range_mode: str,
    analysis_start_ms: Optional[int],
    analysis_end_ms: Optional[int],
    analysis_limit: Optional[int],
    chunk_sec: int,
) -> tuple:
    if analysis_start_ms is not None and analysis_end_ms is not None:
        c_start, c_end = _chunk_range(analysis_start_ms, analysis_end_ms, chunk_sec=chunk_sec)
        return (
            str(exchange_id).lower(),
            str(market_type).lower(),
            str(symbol),
            str(timeframe),
            str(range_mode).lower(),
            "range",
            int(c_start or 0),
            int(c_end or 0),
        )
    return (
        str(exchange_id).lower(),
        str(market_type).lower(),
        str(symbol),
        str(timeframe),
        str(range_mode).lower(),
        "bars",
        int(analysis_limit or 0),
    )


def get_candles_analysis_window(
    *,
    data_settings: Any,
    analysis_info: Optional[Dict[str, Any]] = None,
    display_start_ms: Optional[int] = None,
    display_end_ms: Optional[int] = None,
    fetch_fn: Optional[Callable[..., List[Candle]]] = None,
    cache: Optional[LruCandleCache] = None,
    chunk_sec: Optional[int] = None,
    allow_cache: bool = True,
) -> Tuple[Optional[int], Optional[int], Optional[int], List[Candle], List[Candle]]:
    """Return analysis candles + display view with warmup-aware windowing."""

    ds = data_settings
    params = getattr(ds, "range_params", None) or {}
    exchange_id = getattr(ds, "exchange_id", None) or "binance"
    symbol = getattr(ds, "symbol", None) or "BTC/USDT"
    timeframe = getattr(ds, "timeframe", None) or "1h"
    market_type = (getattr(ds, "market_type", None) or "unknown")
    range_mode = str(getattr(ds, "range_mode", "bars") or "bars").lower()

    analysis_info = analysis_info or {}
    display_start_ms = display_start_ms if display_start_ms is not None else analysis_info.get("display_start_ms")
    display_end_ms = display_end_ms if display_end_ms is not None else analysis_info.get("display_end_ms")
    display_limit = analysis_info.get("display_limit")
    analysis_start_ms = analysis_info.get("analysis_start_ms")
    analysis_end_ms = analysis_info.get("analysis_end_ms")
    analysis_limit = analysis_info.get("analysis_limit")

    if display_start_ms is None:
        display_start_ms = _to_ms(params.get("since"))
    if display_end_ms is None:
        display_end_ms = _to_ms(params.get("until"))
    if analysis_start_ms is None and display_start_ms is not None:
        analysis_start_ms = display_start_ms
    if analysis_end_ms is None and display_end_ms is not None:
        analysis_end_ms = display_end_ms

    if analysis_limit is None:
        try:
            analysis_limit = int(params.get("limit")) if params.get("limit") is not None else None
        except Exception:
            analysis_limit = None

    fetch_fn = fetch_fn or get_candles_with_cache
    cache = cache or _GLOBAL_CACHE
    chunk_sec = int(chunk_sec or _CANDLE_CACHE_CHUNK_SEC)

    key = _cache_key(
        exchange_id=exchange_id,
        market_type=market_type,
        symbol=symbol,
        timeframe=timeframe,
        range_mode=range_mode,
        analysis_start_ms=analysis_start_ms,
        analysis_end_ms=analysis_end_ms,
        analysis_limit=analysis_limit,
        chunk_sec=chunk_sec,
    )

    candles_analysis: List[Candle] = []
    if allow_cache:
        hit = cache.get(key)
        if hit is not None:
            candles_analysis = list(hit)

    if not candles_analysis:
        candles_analysis = fetch_fn(
            exchange_id=exchange_id,
            symbol=symbol,
            timeframe=timeframe,
            limit=analysis_limit,
            since=analysis_start_ms,
            until=analysis_end_ms,
            range_mode=range_mode,
            market_type=market_type,
        )
        if allow_cache:
            cache.set(key, list(candles_analysis))

    # Build display view from analysis candles.
    candles_display: List[Candle] = []
    if display_start_ms is not None:
        for c in candles_analysis:
            ts = getattr(c, "timestamp", None)
            ts_ms = _to_ms(ts)
            if ts_ms is None:
                continue
            if ts_ms < int(display_start_ms):
                continue
            if display_end_ms is not None and ts_ms > int(display_end_ms):
                continue
            candles_display.append(c)
    elif display_limit is not None:
        candles_display = list(candles_analysis)[-int(display_limit) :]
    else:
        candles_display = list(candles_analysis)

    return analysis_start_ms, display_start_ms, display_end_ms, list(candles_analysis), list(candles_display)


def _ensure_prefetch_worker() -> None:
    global _PREFETCH_WORKER_STARTED
    if _PREFETCH_WORKER_STARTED:
        return
    with _PREFETCH_WORKER_LOCK:
        if _PREFETCH_WORKER_STARTED:
            return

        def _loop() -> None:
            while True:
                ds, info, reason = _PREFETCH_Q.get()
                try:
                    get_candles_analysis_window(
                        data_settings=ds,
                        analysis_info=info or {},
                        allow_cache=True,
                    )
                except Exception as exc:
                    logger.debug("candle_cache prefetch worker skipped (%s): %s", str(reason or ""), str(exc))
                finally:
                    try:
                        _PREFETCH_Q.task_done()
                    except Exception:
                        pass

        t = threading.Thread(target=_loop, name="dragon-candle-prefetch", daemon=True)
        t.start()
        _PREFETCH_WORKER_STARTED = True


def enqueue_prefetch_analysis_window(
    *,
    data_settings: Any,
    analysis_info: Optional[Dict[str, Any]] = None,
    reason: str = "",
) -> None:
    """Best-effort non-blocking prefetch.

    Drops tasks if prefetch is disabled or the queue is full.
    """

    if int(_CANDLE_CACHE_PREFETCH or 0) != 1:
        return
    try:
        _ensure_prefetch_worker()
        _PREFETCH_Q.put_nowait((data_settings, dict(analysis_info or {}), str(reason or "")))
    except queue.Full:
        logger.debug("candle_cache prefetch queue full (%s)", str(reason or ""))
    except Exception as exc:
        logger.debug("candle_cache prefetch enqueue failed (%s): %s", str(reason or ""), str(exc))


def get_run_details_candles_analysis_window(
    *,
    data_settings: Any,
    run_context: Optional[Dict[str, Any]] = None,
    fetch_fn: Optional[Callable[..., List[Candle]]] = None,
    cache: Optional[LruCandleCache] = None,
    chunk_sec: Optional[int] = None,
    allow_cache: bool = True,
) -> Tuple[Optional[int], Optional[int], Optional[int], List[Candle], List[Candle]]:
    """Convenience wrapper for run-details pages.

    Uses the run's persisted `run_context` as `analysis_info` so warmup windows are respected.
    """

    return get_candles_analysis_window(
        data_settings=data_settings,
        analysis_info=dict(run_context or {}),
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=chunk_sec,
        allow_cache=allow_cache,
    )


def prefetch_analysis_window(
    *,
    data_settings: Any,
    analysis_info: Optional[Dict[str, Any]] = None,
    reason: str = "",
) -> None:
    # Back-compat shim: enqueue prefetch to avoid blocking.
    enqueue_prefetch_analysis_window(data_settings=data_settings, analysis_info=analysis_info, reason=reason)
