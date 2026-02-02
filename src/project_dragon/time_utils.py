from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
import math

from project_dragon.ui_formatters import format_duration_dhm
from typing import Any, Optional, Union

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DEFAULT_DISPLAY_TZ_NAME = "Australia/Brisbane"


def get_display_tz_name(conn: Any, user_id: str) -> str:
    """Return the per-user display timezone name (IANA tz database).

    Presentation-only. Timestamps remain stored in UTC in the DB.
    """

    try:
        from project_dragon.storage import get_user_setting  # local import to avoid cycles

        tz_name = get_user_setting(conn, user_id, "display_timezone", default=DEFAULT_DISPLAY_TZ_NAME)
        tz_name_s = str(tz_name or "").strip()
        return tz_name_s or DEFAULT_DISPLAY_TZ_NAME
    except Exception:
        return DEFAULT_DISPLAY_TZ_NAME


def get_display_tz(conn: Any, user_id: str) -> tzinfo:
    name = get_display_tz_name(conn, user_id)
    return _safe_zoneinfo(name)


def _safe_zoneinfo(name: str) -> tzinfo:
    # ZoneInfo should exist on Python 3.9+; but be defensive.
    if ZoneInfo is None:  # pragma: no cover
        return timezone.utc  # type: ignore[return-value]

    try:
        return ZoneInfo(str(name))
    except Exception:
        try:
            return ZoneInfo(DEFAULT_DISPLAY_TZ_NAME)
        except Exception:
            return timezone.utc  # type: ignore[return-value]


def parse_db_ts(value: Any) -> Optional[datetime]:
    """Parse a DB timestamp into a tz-aware UTC datetime.

    Accepts:
    - datetime (naive treated as UTC)
    - ISO-8601 string (naive treated as UTC; offset parsed and converted to UTC)
    - numeric seconds/ms timestamps

    Returns:
    - tz-aware datetime in UTC, or None.
    """

    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt

    if isinstance(value, (int, float)):
        try:
            v = float(value)
        except Exception:
            return None
        if not (v == v) or v in (float("inf"), float("-inf")):
            return None
        # Heuristic: >1e12 is ms since epoch
        if v > 1e12:
            v = v / 1000.0
        try:
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except Exception:
            return None

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # Common suffixes.
        if s.endswith(" UTC"):
            s = s[: -len(" UTC")].strip()

        # Handle Zulu.
        if s.endswith("Z") and "+" not in s and "-" not in s[10:]:
            s = s[:-1] + "+00:00"

        # Fast path: fromisoformat handles offsets.
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            pass

        # Fallback: tolerate common non-ISO formats.
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue

        return None

    # Unknown type
    return None


def to_local_display(value: Any, tz: tzinfo) -> Optional[datetime]:
    dt_utc = parse_db_ts(value)
    if dt_utc is None:
        return None
    try:
        return dt_utc.astimezone(tz)
    except Exception:
        return dt_utc


def fmt_dt(value: Any, tz: tzinfo, *, with_seconds: bool = False) -> str:
    dt_local = to_local_display(value, tz)
    if dt_local is None:
        return "—"
    fmt = "%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M"
    try:
        return dt_local.strftime(fmt)
    except Exception:
        return "—"


def fmt_dt_short(value: Any, tz: tzinfo) -> str:
    dt_local = to_local_display(value, tz)
    if dt_local is None:
        return "—"
    try:
        return dt_local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def fmt_date(value: Any, tz: tzinfo) -> str:
    """Return a tz-aware local date string (YYYY-MM-DD)."""

    dt_local = to_local_display(value, tz)
    if dt_local is None:
        return "—"
    try:
        return dt_local.date().isoformat()
    except Exception:
        return "—"
    # Still lexicographically sortable for same year if prefixed with year.
    try:
        return dt_local.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "—"


def format_duration(seconds: Optional[float]) -> str:
    """Format a duration in seconds for UI display.

    Rules:
    - None/NaN/<=0 -> ""
    - < 60s -> "{n}s"
    - < 3600s -> "{m}m {s}s"
    - < 86400s -> "{h}h {m}m"
    - >= 86400s -> "{d}d {h}h"
    """



    if seconds is None:
        return ""
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return ""
    if not math.isfinite(s) or s <= 0:
        return ""

    return format_duration_dhm(s, empty_for_none=True)


def fmt_age(value: Any, now: Any, tz: tzinfo) -> str:
    """Return a compact relative age like '3m ago'."""

    dt_local = to_local_display(value, tz)
    now_local = to_local_display(now, tz)
    if dt_local is None or now_local is None:
        return "—"

    try:
        seconds = (now_local - dt_local).total_seconds()
    except Exception:
        return "—"

    if seconds < 0:
        seconds = abs(seconds)
        suffix = "from now"
    else:
        suffix = "ago"

    if seconds < 60:
        return f"{int(seconds)}s {suffix}"
    if seconds < 3600:
        return f"{int(seconds // 60)}m {suffix}"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h {suffix}"
    return f"{int(seconds // 86400)}d {suffix}"
