from __future__ import annotations

import base64
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_spothq_root() -> Path:
    return _repo_root() / "app" / "assets" / "crypto_icons" / "spothq" / "cryptocurrency-icons"


def _normalize_ticker(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    # Common aliases / wrappers
    if t == "XBT":
        t = "BTC"
    if t.startswith("W") and len(t) > 1 and t[1:].isalpha():
        # best-effort: WETH -> ETH
        t = t[1:]
    return t


def resolve_crypto_icon(
    base_asset: str,
    *,
    size_preference: Sequence[str] = ("svg", "32", "128"),
    style: str = "color",
    icons_root: Optional[Path] = None,
) -> Optional[Tuple[str, bytes]]:
    """Resolve a crypto icon from the local spothq/cryptocurrency-icons pack.

    Returns (mime, bytes) or None.

    Looks for:
    - <root>/svg/<style>/<ticker>.svg
    - <root>/<size>/<style>/<ticker>.png  for size in {32,128,32@2x,...}

    Notes:
    - `base_asset` is normalized to uppercase; filenames are typically lowercase.
    - No network calls; purely local file lookup.
    """

    root = icons_root or default_spothq_root()
    t = _normalize_ticker(base_asset)
    if not t:
        return None

    ticker = t.lower()
    style_norm = (style or "color").strip().lower() or "color"

    for pref in list(size_preference or []):
        p = (str(pref or "").strip().lower())
        if not p:
            continue
        if p == "svg":
            candidate = root / "svg" / style_norm / f"{ticker}.svg"
            if candidate.exists():
                return ("image/svg+xml", candidate.read_bytes())
        else:
            candidate = root / p / style_norm / f"{ticker}.png"
            if candidate.exists():
                return ("image/png", candidate.read_bytes())

    return None


def icon_bytes_to_data_uri(mime: str, b: bytes) -> str:
    m = (mime or "").strip() or "image/png"
    raw = b or b""
    encoded = base64.b64encode(raw).decode("ascii")
    return f"data:{m};base64,{encoded}"
