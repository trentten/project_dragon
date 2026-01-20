from __future__ import annotations

from datetime import datetime, timezone

import pytest

from project_dragon.backtest_core import DataSettings, compute_analysis_window_from_snapshot
from project_dragon.candle_cache import LruCandleCache, get_candles_analysis_window, get_run_details_candles_analysis_window
from project_dragon.domain import Candle


def _dt_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def _candles_for_range(start_ms: int, end_ms: int, step_ms: int = 10_000) -> list[Candle]:
    out: list[Candle] = []
    t = int(start_ms)
    while t <= int(end_ms):
        out.append(Candle(timestamp=_dt_ms(t), open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0))
        t += int(step_ms)
    return out


def test_cache_hit_miss_with_chunked_keys() -> None:
    ds = DataSettings(
        data_source="ccxt",
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        range_mode="range",
        range_params={"since": 1000, "until": 55_000},
    )

    cache = LruCandleCache(max_items=8, max_bytes=10_000_000)

    calls: dict[str, int] = {"n": 0}

    def fetch_fn(**kwargs):
        calls["n"] += 1
        since = int(kwargs.get("since") or 0)
        until = int(kwargs.get("until") or 0)
        return _candles_for_range(since, until)

    # First request populates cache.
    a1 = {
        "analysis_start_ms": 1_000,
        "analysis_end_ms": 55_000,
        "display_start_ms": 10_000,
        "display_end_ms": 55_000,
    }
    _analysis_start, _display_start, _display_end, candles_analysis_1, _candles_display_1 = get_candles_analysis_window(
        data_settings=ds,
        analysis_info=a1,
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=60,  # 60s chunk => both windows map to [0, 60000]
    )
    assert calls["n"] == 1
    assert candles_analysis_1

    # Second request is a different per-run window, but same chunked key => cache hit.
    a2 = {
        "analysis_start_ms": 2_000,
        "analysis_end_ms": 50_000,
        "display_start_ms": 10_000,
        "display_end_ms": 50_000,
    }
    _analysis_start, _display_start, _display_end, candles_analysis_2, _candles_display_2 = get_candles_analysis_window(
        data_settings=ds,
        analysis_info=a2,
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=60,
    )
    assert calls["n"] == 1
    assert candles_analysis_2


def test_warmup_analysis_window_for_multi_timeframe_ma() -> None:
    ds = DataSettings(
        data_source="ccxt",
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        range_mode="range",
        range_params={
            "since": "2020-01-01T00:00:00Z",
            "until": "2020-01-03T00:00:00Z",
        },
    )

    # 6H MA on 1H base => requires warmup history beyond display_start.
    config_snapshot = {
        "general": {},
        "exits": {},
        "dca": {},
        "trend": {"ma_interval_min": 360, "ma_len": 10, "ma_type": "Sma"},
        "bbands": {},
        "macd": {},
        "rsi": {},
    }

    analysis_info = compute_analysis_window_from_snapshot(config_snapshot, ds)
    assert analysis_info.get("display_start_ms") is not None
    assert analysis_info.get("analysis_start_ms") is not None
    assert int(analysis_info["analysis_start_ms"]) < int(analysis_info["display_start_ms"])

    captured: dict[str, int | None] = {"since": None}

    def fetch_fn(**kwargs):
        captured["since"] = int(kwargs.get("since") or 0)
        since = int(kwargs.get("since") or 0)
        until = int(kwargs.get("until") or 0)
        return _candles_for_range(since, until)

    cache = LruCandleCache(max_items=8, max_bytes=10_000_000)
    get_candles_analysis_window(
        data_settings=ds,
        analysis_info=analysis_info,
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=3600,
    )

    assert captured["since"] == int(analysis_info["analysis_start_ms"])


def test_run_details_uses_cache_second_open(monkeypatch: pytest.MonkeyPatch) -> None:
    ds = DataSettings(
        data_source="ccxt",
        exchange_id="binance",
        market_type="spot",
        symbol="BTC/USDT",
        timeframe="1h",
        range_mode="range",
        range_params={"since": 10_000, "until": 80_000},
    )

    run_context = {
        "analysis_start_ms": 10_000,
        "analysis_end_ms": 80_000,
        "display_start_ms": 30_000,
        "display_end_ms": 80_000,
    }

    cache = LruCandleCache(max_items=8, max_bytes=10_000_000)

    calls: dict[str, int] = {"n": 0}

    def fetch_fn(**kwargs):
        calls["n"] += 1
        since = int(kwargs.get("since") or 0)
        until = int(kwargs.get("until") or 0)
        return _candles_for_range(since, until)

    get_run_details_candles_analysis_window(
        data_settings=ds,
        run_context=run_context,
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=60,
    )
    assert calls["n"] == 1

    # Second open should hit worker-local LRU and not invoke fetch_fn.
    get_run_details_candles_analysis_window(
        data_settings=ds,
        run_context=run_context,
        fetch_fn=fetch_fn,
        cache=cache,
        chunk_sec=60,
    )
    assert calls["n"] == 1
