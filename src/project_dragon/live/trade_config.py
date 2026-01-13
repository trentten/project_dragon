from __future__ import annotations

from typing import Optional


def normalize_primary_position_side(value: object) -> str:
    """Normalize a primary hedge leg marker to 'LONG' or 'SHORT'.

    Accepts strings like 'long'/'short' and returns 'LONG' or 'SHORT'.
    Defaults to 'LONG' for invalid inputs.
    """
    s = str(value or "").strip().upper()
    if s in {"LONG", "SHORT"}:
        return s
    return "LONG"


def derive_primary_position_side_from_allow_flags(*, allow_long: bool, allow_short: bool) -> Optional[str]:
    """Derive primary hedge leg from allow flags.

    Returns:
      - 'LONG' if only long allowed
      - 'SHORT' if only short allowed
      - 'BOTH' if both allowed (requires user choice / multi-bot creation)
      - None if neither allowed (invalid)
    """
    allow_long = bool(allow_long)
    allow_short = bool(allow_short)
    if allow_long and not allow_short:
        return "LONG"
    if allow_short and not allow_long:
        return "SHORT"
    if allow_long and allow_short:
        return "BOTH"
    return None
