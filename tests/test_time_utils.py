from __future__ import annotations

from datetime import datetime, timezone

import pytest

from project_dragon.time_utils import (
    DEFAULT_DISPLAY_TZ_NAME,
    fmt_dt,
    parse_db_ts,
    to_local_display,
    fmt_date,
)

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


@pytest.mark.parametrize(
    "value, expected_utc_iso_prefix",
    [
        ("2026-01-01T00:00:00", "2026-01-01T00:00:00+00:00"),  # naive treated as UTC
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00+00:00"),
        ("2026-01-01T10:00:00+10:00", "2026-01-01T00:00:00+00:00"),
        (1700000000, None),  # seconds epoch -> just ensure parses
        (1700000000000, None),  # ms epoch -> just ensure parses
    ],
)
def test_parse_db_ts(value, expected_utc_iso_prefix):
    dt = parse_db_ts(value)
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.tzinfo.utcoffset(dt) == timezone.utc.utcoffset(dt)
    if expected_utc_iso_prefix is not None:
        assert dt.isoformat().startswith(expected_utc_iso_prefix)


def test_to_local_display_brisbane_offset():
    if ZoneInfo is None:
        pytest.skip("zoneinfo not available")

    tz = ZoneInfo(DEFAULT_DISPLAY_TZ_NAME)
    # Brisbane is UTC+10 year-round
    dt_local = to_local_display("2026-01-01T00:00:00Z", tz)
    assert dt_local is not None
    assert dt_local.tzinfo is not None
    # 00:00 UTC -> 10:00 local
    assert dt_local.strftime("%Y-%m-%d %H:%M") == "2026-01-01 10:00"


def test_fmt_dt_none():
    if ZoneInfo is None:
        pytest.skip("zoneinfo not available")
    tz = ZoneInfo(DEFAULT_DISPLAY_TZ_NAME)
    assert fmt_dt(None, tz) == "—"

def test_fmt_date_brisbane_rollover() -> None:
    if ZoneInfo is None:
        pytest.skip("zoneinfo not available")
    tz = ZoneInfo("Australia/Brisbane")
    # 2023-12-31 18:30Z is 2024-01-01 04:30 in Brisbane (UTC+10)
    assert fmt_date("2023-12-31T18:30:00Z", tz) == "2024-01-01"


def test_parse_datetime_naive_is_utc():
    dt = datetime(2026, 1, 1, 0, 0, 0)
    out = parse_db_ts(dt)
    assert out is not None
    assert out.tzinfo is not None
    assert out.isoformat().startswith("2026-01-01T00:00:00")
