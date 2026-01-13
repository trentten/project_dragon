import math

from project_dragon.data_online import timeframe_to_milliseconds


def _infer_limit(timeframe: str, since_ms: int, until_ms: int) -> int:
    ms_per_bar = timeframe_to_milliseconds(timeframe)
    span_ms = max(0, int(until_ms) - int(since_ms))
    return int(span_ms // ms_per_bar) + 2


def test_one_month_15m_needs_more_than_default_1000() -> None:
    # 30 days of 15m candles ~ 2880 bars. We add +2 cushion.
    since_ms = 0
    until_ms = 30 * 24 * 60 * 60 * 1000
    limit = _infer_limit("15m", since_ms, until_ms)
    assert limit > 1000
    assert math.isclose(limit, (30 * 24 * 4) + 2, rel_tol=0, abs_tol=0)


def test_one_month_1h_within_default_1000() -> None:
    # 30 days of 1h candles ~ 720 bars (+2 cushion) which is <= 1000.
    since_ms = 0
    until_ms = 30 * 24 * 60 * 60 * 1000
    limit = _infer_limit("1h", since_ms, until_ms)
    assert limit <= 1000
    assert limit == (30 * 24) + 2
