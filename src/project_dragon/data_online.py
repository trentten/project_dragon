from __future__ import annotations

from datetime import datetime
import time
from typing import Any, List, Optional, Tuple

import ccxt
import requests

from .domain import Candle
from .storage import (
    get_cached_coverage,
    list_cached_coverages,
    load_cached_range,
    open_db_connection,
    upsert_candles,
)
from .exchange_normalization import (
    canonical_exchange_id,
    canonical_symbol,
    exchange_id_for_ccxt,
)


def timeframe_to_milliseconds(timeframe: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
    try:
        value = int(timeframe[:-1])
        unit = timeframe[-1]
        return value * units[unit]
    except (ValueError, KeyError):
        return 60_000


def infer_market_type(symbol: str, market_type: Optional[str]) -> str:
    if market_type:
        mt = str(market_type).strip().lower()
        if mt in {"perp", "perps", "futures"}:
            return "perps"
        if mt in {"spot"}:
            return "spot"
        return mt
    return "perps" if ":" in str(symbol) else "spot"


def ccxt_symbol_to_woox_symbol(symbol: str, market_type: str) -> str:
    """
    ccxt spot:  BTC/USDT          -> SPOT_BTC_USDT
    ccxt perps: BTC/USDT:USDT     -> PERP_BTC_USDT   (ignore settle suffix)
    """
    s = str(symbol).strip()
    left = s.split(":", 1)[0]  # drop settle segment
    if "/" not in left:
        raise ValueError(f"Invalid ccxt symbol format: {symbol!r}")
    base, quote = left.split("/", 1)
    base_u, quote_u = base.strip().upper(), quote.strip().upper()

    mt = (market_type or "").lower()
    prefix = "PERP" if mt in {"perp", "perps"} else "SPOT"
    return f"{prefix}_{base_u}_{quote_u}"


def woox_symbol_to_ccxt_symbol(woox_symbol: str) -> Tuple[str, str]:
    """Convert WooX symbol format to our internal ccxt-style symbol.

    SPOT_BTC_USDT -> BTC/USDT
    PERP_BTC_USDT -> BTC/USDT:USDT (settle = quote)
    """
    s = str(woox_symbol or "").strip().upper()
    parts = [p for p in s.split("_") if p]
    if len(parts) < 3:
        raise ValueError(f"Invalid WooX symbol format: {woox_symbol!r}")

    kind = parts[0]
    base = parts[1]
    quote = parts[2]

    if kind == "SPOT":
        return f"{base}/{quote}", "spot"
    if kind == "PERP":
        return f"{base}/{quote}:{quote}", "perps"
    raise ValueError(f"Unsupported WooX symbol kind: {kind!r}")


_WOOX_MARKETS_CACHE: dict[str, tuple[float, dict[str, dict], list[str]]] = {}


def _woox_get(path: str, params: dict) -> dict:
    base_url = "https://api.woox.io"
    url = f"{base_url}{path}"
    resp = requests.get(url, params=params or {}, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("success") is False:
        raise RuntimeError(f"WooX public API error for {path}: {payload!r}")
    return payload


def fetch_woox_instruments() -> list[dict[str, Any]]:
    """Fetch WooX market instruments using the V3 public endpoint.

    Endpoint: GET /v3/public/instruments
    Expected shape: {"success": true, "data": ...}
    """
    payload = _woox_get("/v3/public/instruments", {})
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise RuntimeError(f"Unexpected WooX instruments response: {payload!r}")

    data = payload.get("data")
    # Parse defensively: API may evolve.
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        rows = data.get("rows")
        if isinstance(rows, list):
            return [x for x in rows if isinstance(x, dict)]
        instruments = data.get("instruments")
        if isinstance(instruments, list):
            return [x for x in instruments if isinstance(x, dict)]

    raise RuntimeError(f"Unexpected WooX instruments data shape: {payload!r}")


def get_woox_market_catalog(exchange_id: str, *, ttl_sec: int = 600) -> tuple[dict, list[str]]:
    """Return a ccxt-like markets dict for WooX without using ccxt.

    - Keys are internal ccxt-style symbols (BTC/USDT or BTC/USDT:USDT)
    - Values mimic a small subset of ccxt market objects used by Streamlit.
    """
    key = str(exchange_id or "").strip().lower() or "woo"
    now = time.monotonic()
    cached = _WOOX_MARKETS_CACHE.get(key)
    if cached is not None:
        expires_at, markets, timeframes = cached
        if now < expires_at:
            return markets, list(timeframes)

    instruments = fetch_woox_instruments()
    markets: dict[str, dict] = {}

    def _pick_str(obj: dict, keys: tuple[str, ...]) -> Optional[str]:
        for k in keys:
            v = obj.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return None

    for ins in instruments:
        wsym = _pick_str(ins, ("symbol", "instrument", "instrumentId", "name"))
        if not wsym:
            continue
        wsym_u = wsym.strip().upper()
        if not (wsym_u.startswith("SPOT_") or wsym_u.startswith("PERP_")):
            continue

        try:
            ccxt_sym, mt = woox_symbol_to_ccxt_symbol(wsym_u)
        except Exception:
            continue

        # Parse base/quote from the WooX symbol itself (authoritative for our conversion).
        parts = [p for p in wsym_u.split("_") if p]
        base = parts[1] if len(parts) >= 2 else ""
        quote = parts[2] if len(parts) >= 3 else ""

        status = _pick_str(ins, ("status", "state"))
        active_raw = ins.get("active") if isinstance(ins, dict) else None
        if isinstance(active_raw, bool):
            active = active_raw
        else:
            st = (status or "").strip().upper()
            active = st in {"TRADING", "ONLINE", "ENABLED", "ACTIVE"} if st else True

        is_perp = mt == "perps"

        precision: dict[str, str] = {}
        for k in ("priceTick", "price_tick", "tickSize", "priceStep", "priceIncrement"):
            if k in ins and ins.get(k) is not None:
                precision["price"] = str(ins.get(k))
                break
        for k in ("quantityTick", "qtyStep", "sizeIncrement", "quantityStep", "quantityIncrement", "baseTick"):
            if k in ins and ins.get(k) is not None:
                precision["amount"] = str(ins.get(k))
                break

        limits: dict[str, dict] = {}
        min_amount = _pick_str(ins, ("minQty", "minQuantity", "minTradeSize", "minSize", "baseMin", "minBase"))
        if min_amount is not None:
            limits["amount"] = {"min": min_amount}
        min_cost = _pick_str(ins, ("minNotional", "minCost", "quoteMin"))
        if min_cost is not None:
            limits["cost"] = {"min": min_cost}

        markets[ccxt_sym] = {
            "symbol": ccxt_sym,
            "base": base or None,
            "quote": quote or None,
            "swap": bool(is_perp),
            "future": bool(is_perp),
            "active": bool(active),
            "status": status,
            "precision": precision,
            "limits": limits,
            "info": ins,
        }

    # Streamlit already has a fallback list; return a stable set here.
    timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]
    _WOOX_MARKETS_CACHE[key] = (now + float(ttl_sec), markets, timeframes)
    return markets, list(timeframes)


def _extract_rows(payload: dict) -> list[dict]:
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected WooX response type: {type(payload)}")
    if isinstance(payload.get("rows"), list):
        return payload["rows"]
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("rows"), list):
        return data["rows"]
    raise RuntimeError(f"Unexpected WooX response shape: {payload!r}")


def _fetch_woox_kline_latest(woox_symbol: str, timeframe: str, limit: int) -> list[dict]:
    lim = max(1, min(int(limit), 1000))
    payload = _woox_get(
        "/v3/public/kline",
        {"symbol": woox_symbol, "type": str(timeframe), "limit": lim},
    )
    return _extract_rows(payload)


def _fetch_woox_kline_history(
    woox_symbol: str,
    timeframe: str,
    *,
    before: int | None,
    after: int | None,
    limit: int,
) -> list[dict]:
    lim = max(1, min(int(limit), 1000))
    params: dict = {"symbol": woox_symbol, "type": str(timeframe), "limit": lim}
    # Docs: if both provided, before wins; cursor candle excluded.
    if before is not None:
        params["before"] = int(before)
    elif after is not None:
        params["after"] = int(after)

    payload = _woox_get("/v3/public/klineHistory", params)
    return _extract_rows(payload)


def _woox_rows_to_candles(rows: list[dict]) -> list[Candle]:
    candles: list[Candle] = []
    for r in rows or []:
        try:
            ts_ms = int(r.get("startTimestamp"))
        except Exception:
            continue
        ts = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=None)
        candles.append(
            Candle(
                timestamp=ts,
                open=float(r.get("open", 0.0)),
                high=float(r.get("high", 0.0)),
                low=float(r.get("low", 0.0)),
                close=float(r.get("close", 0.0)),
                volume=float(r.get("volume", 0.0)),
            )
        )
    candles.sort(key=lambda c: c.timestamp)
    return candles


def load_ccxt_candles(
    exchange_id: str,
    symbol: str,
    timeframe: str,
    limit: Optional[int] = None,
    since: Optional[float] = None,
    until: Optional[float] = None,
    range_mode: str = "Bars",
    market_type: Optional[str] = None,
) -> List[Candle]:
    """
    Fetch OHLCV and convert into Candle objects.

    For WooX: uses WooX V3 public klines (no ccxt network calls).
    """
    ex_norm = canonical_exchange_id(exchange_id)
    is_woox = ex_norm == "woox"

    sym_in = str(symbol or "").strip()
    if is_woox:
        sym_in = canonical_symbol(ex_norm, sym_in)

    ms_per_bar = timeframe_to_milliseconds(timeframe)

    # Keep existing "infer limit for explicit ranges" behavior (cap at 250k).
    fetch_limit: int
    if limit is not None:
        fetch_limit = int(limit)
    else:
        inferred = None
        try:
            if since is not None and until is not None and ms_per_bar > 0:
                span_ms = max(0, int(until) - int(since))
                inferred = int(span_ms // ms_per_bar) + 2
        except Exception:
            inferred = None
        fetch_limit = min(max(int(inferred or 1000), 1), 250_000)

    if not is_woox:
        # ...existing code...
        # (leave non-WooX behavior as-is for now)
        ccxt_exchange_id = exchange_id_for_ccxt(exchange_id)
        exchange_class = getattr(ccxt, ccxt_exchange_id)
        exchange = exchange_class()

        ohlcv: List[List[float]] = []
        if since is None:
            data = exchange.fetch_ohlcv(symbol, timeframe, limit=fetch_limit)
            ohlcv.extend(data or [])
        else:
            remaining = fetch_limit
            cursor = int(since)
            while remaining > 0:
                batch = min(remaining, 1000)
                data = exchange.fetch_ohlcv(symbol, timeframe, since=cursor, limit=batch)
                if not data:
                    break
                ohlcv.extend(data)
                cursor = data[-1][0] + ms_per_bar
                remaining -= len(data)
                if until is not None and data[-1][0] >= until:
                    break

        if until is not None:
            cutoff = int(until)
            ohlcv = [row for row in ohlcv if row[0] <= cutoff]

        candles: List[Candle] = []
        for ts, open_, high, low, close, volume in ohlcv:
            candles.append(
                Candle(
                    timestamp=datetime.utcfromtimestamp(ts / 1000.0).replace(tzinfo=None),
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            )
        return candles

    # WooX path (public REST)
    sym_up = str(sym_in or "").strip().upper()
    if sym_up.startswith("PERP_") or sym_up.startswith("SPOT_"):
        woox_symbol = sym_up
        mt = "perps" if sym_up.startswith("PERP_") else "spot"
    else:
        mt = infer_market_type(sym_in, market_type)
        woox_symbol = ccxt_symbol_to_woox_symbol(sym_in, mt)

    since_ms = _to_ms(since)
    until_ms = _to_ms(until)

    seen: dict[int, dict] = {}

    def _add_rows(rows: list[dict]) -> None:
        for r in rows or []:
            try:
                ts = int(r.get("startTimestamp"))
            except Exception:
                continue
            if ts not in seen:
                seen[ts] = r

    def _sorted_rows() -> list[dict]:
        return [seen[k] for k in sorted(seen.keys())]

    max_pages_guard = max(1, (fetch_limit // 1000) + 10)

    if since_ms is None:
        # Latest-N bars, optionally "up to until"
        remaining = fetch_limit
        pages = 0

        if until_ms is None and remaining <= 1000:
            _add_rows(_fetch_woox_kline_latest(woox_symbol, timeframe, remaining))
        else:
            # Page backwards via history
            before = (until_ms + ms_per_bar) if until_ms is not None and ms_per_bar > 0 else None
            while remaining > 0 and pages < max_pages_guard:
                pages += 1
                batch = min(remaining, 1000)
                rows = _fetch_woox_kline_history(
                    woox_symbol,
                    timeframe,
                    before=before,
                    after=None,
                    limit=batch,
                )
                if not rows:
                    break
                _add_rows(rows)

                starts = [int(r["startTimestamp"]) for r in rows if "startTimestamp" in r]
                if not starts:
                    break
                new_before = min(starts)
                if before is not None and new_before >= before:
                    break
                before = new_before
                remaining = fetch_limit - len(seen)

        rows = _sorted_rows()
        if until_ms is not None:
            rows = [r for r in rows if int(r.get("startTimestamp", 0)) <= int(until_ms)]
        # If caller asked for bars, keep most recent N bars semantics.
        if fetch_limit is not None and len(rows) > fetch_limit:
            rows = rows[-fetch_limit:]
        return _woox_rows_to_candles(rows)

    # Explicit range / forward scan (or forward "since + limit")
    remaining = fetch_limit
    pages = 0
    after = int(since_ms - ms_per_bar) if ms_per_bar > 0 else int(since_ms)

    while remaining > 0 and pages < max_pages_guard:
        pages += 1
        batch = min(remaining, 1000)
        rows = _fetch_woox_kline_history(
            woox_symbol,
            timeframe,
            before=None,
            after=after,
            limit=batch,
        )
        if not rows:
            break

        # Ensure deterministic cursor progress
        rows_sorted = sorted(
            (r for r in rows if r.get("startTimestamp") is not None),
            key=lambda r: int(r["startTimestamp"]),
        )
        if not rows_sorted:
            break

        max_ts = int(rows_sorted[-1]["startTimestamp"])
        if max_ts <= after:
            break

        for r in rows_sorted:
            ts = int(r["startTimestamp"])
            if ts < int(since_ms):
                continue
            if until_ms is not None and ts > int(until_ms):
                continue
            if ts not in seen:
                seen[ts] = r

        after = max_ts
        remaining = fetch_limit - len(seen)

        if until_ms is not None and after >= int(until_ms):
            break

    rows = _sorted_rows()
    if until_ms is not None:
        rows = [r for r in rows if int(r.get("startTimestamp", 0)) <= int(until_ms)]
    if fetch_limit is not None and len(rows) > fetch_limit:
        rows = rows[:fetch_limit]
    return _woox_rows_to_candles(rows)


def load_ccxt_ticker(exchange_id: str, symbol: str) -> dict:
    """Fetch ticker via ccxt.

    Returns the raw ccxt ticker dict. Callers should be defensive about shape.
    """
    ccxt_exchange_id = exchange_id_for_ccxt(exchange_id)
    exchange_class = getattr(ccxt, ccxt_exchange_id)
    exchange = exchange_class()
    sym = str(symbol or "").strip()
    try:
        if canonical_exchange_id(exchange_id) == "woox" and sym.upper().startswith(("PERP_", "SPOT_")):
            sym, _mt = woox_symbol_to_ccxt_symbol(sym)
    except Exception:
        pass
    return exchange.fetch_ticker(sym)


def _to_ms(value: Optional[float | int | datetime]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def get_candles_with_cache(
    exchange_id: str,
    symbol: str,
    timeframe: str,
    *,
    limit: Optional[int] = None,
    since: Optional[float] = None,
    until: Optional[float] = None,
    range_mode: str = "Bars",
    market_type: str = "perps",
) -> List[Candle]:
    """
    Fetch candles with a simple cache layer. If requested range is not fully covered, fetch from the
    upstream venue (WooX public klines for WooX; ccxt for others), upsert into cache, then reload
    the requested slice from cache.
    """
    range_mode = (range_mode or "bars").lower()
    start_ms = _to_ms(since)
    end_ms = _to_ms(until)
    market_type = (market_type or "unknown").lower()
    ex_norm = canonical_exchange_id(exchange_id)
    sym_norm = canonical_symbol(ex_norm, str(symbol or "").strip())

    # Fast path: bar-count fetches without explicit range -> fetch fresh and cache.
    if range_mode == "bars" and start_ms is None and end_ms is None:
        fetched = load_ccxt_candles(
            ex_norm,
            sym_norm,
            timeframe,
            limit=limit,
            since=None,
            until=None,
            range_mode=range_mode,
            market_type=market_type,
        )
        with open_db_connection() as conn:
            upsert_candles(conn, ex_norm, market_type, sym_norm, timeframe, fetched)
        return fetched

    with open_db_connection() as conn:
        min_ts, max_ts = get_cached_coverage(conn, ex_norm, market_type, sym_norm, timeframe)
        coverage_ok = False
        if start_ms is not None and end_ms is not None:
            coverage_ok = min_ts is not None and max_ts is not None and min_ts <= start_ms and max_ts >= end_ms
        elif start_ms is None and end_ms is not None:
            coverage_ok = max_ts is not None and max_ts >= end_ms
        elif start_ms is not None and end_ms is None:
            coverage_ok = min_ts is not None and min_ts <= start_ms

        def _expected_count(start_ms_val: Optional[int], end_ms_val: Optional[int]) -> Optional[int]:
            if start_ms_val is None or end_ms_val is None:
                return None
            try:
                ms_per_bar = timeframe_to_milliseconds(timeframe)
                if ms_per_bar <= 0:
                    return None
                span_ms = max(0, int(end_ms_val) - int(start_ms_val))
                # Inclusive endpoints: +1, then a tiny cushion.
                return int(span_ms // ms_per_bar) + 1
            except Exception:
                return None

        def _min_required_rows() -> Optional[int]:
            if limit is None:
                return None
            exp = _expected_count(start_ms, end_ms)
            target = int(min(int(limit), int(exp)) if exp is not None else int(limit))
            # If we have significantly fewer rows than requested/expected, treat cache as incomplete.
            return max(1, int(target * 0.90))

        def _fetch_and_upsert(fetch_limit: Optional[int]) -> None:
            fetched = load_ccxt_candles(
                ex_norm,
                sym_norm,
                timeframe,
                limit=fetch_limit,
                since=start_ms,
                until=end_ms,
                range_mode=range_mode,
                market_type=market_type,
            )
            upsert_candles(conn, ex_norm, market_type, sym_norm, timeframe, fetched)

        if not coverage_ok:
            _fetch_and_upsert(limit)

        rows = load_cached_range(conn, ex_norm, market_type, sym_norm, timeframe, start_ms, end_ms)

        # Repair path: earlier versions could populate the cache with a truncated slice (e.g. default 1000 bars)
        # while still having min/max timestamps that make coverage appear OK.
        min_required = _min_required_rows()
        if min_required is not None and len(rows) < min_required:
            # Best-effort: infer a better fetch limit for this explicit range.
            exp = _expected_count(start_ms, end_ms)
            desired = None
            try:
                desired = int(exp) + 10 if exp is not None else None
            except Exception:
                desired = None
            if desired is not None:
                desired = min(max(desired, int(limit or 1)), 250_000)
            _fetch_and_upsert(desired)
            rows = load_cached_range(conn, ex_norm, market_type, sym_norm, timeframe, start_ms, end_ms)

    candles: List[Candle] = []
    for row in rows:
        ts_ms = row.get("timestamp_ms")
        if ts_ms is None:
            continue
        ts = datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=None)
        candles.append(
            Candle(
                timestamp=ts,
                open=row.get("open", 0.0),
                high=row.get("high", 0.0),
                low=row.get("low", 0.0),
                close=row.get("close", 0.0),
                volume=row.get("volume", 0.0),
            )
        )
    return candles
