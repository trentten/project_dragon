from __future__ import annotations

import math
from typing import Optional


def format_duration_dhm(seconds: Optional[float | int], *, empty_for_none: bool = False) -> str:
    """Format a duration in seconds into compact d/h/m/s.

    Matches Results grid formatting:
    - None/NaN/invalid/negative -> "—" (or "" when empty_for_none=True)
    - >= 1d -> "{d}d {h}h"
    - >= 1h -> "{h}h {m}m"
    - >= 1m -> "{m}m {s}s"
    - else -> "{s}s"
    """

    if seconds is None:
        return "" if empty_for_none else "—"
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return "" if empty_for_none else "—"
    if not math.isfinite(s) or s < 0:
        return "" if empty_for_none else "—"

    total = int(round(s))
    days = total // 86400
    rem = total % 86400
    hours = rem // 3600
    rem = rem % 3600
    minutes = rem // 60
    secs = rem % 60

    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"
