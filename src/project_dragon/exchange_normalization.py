from __future__ import annotations

import re
from typing import Iterable, List, Set


_WOOX_CANONICAL = "woox"
_WOOX_ALIASES = {"woo", "woox", "woo-x", "woo_x"}


def canonical_exchange_id(exchange_id: str) -> str:
    ex = (str(exchange_id or "").strip().lower() or "")
    if ex in _WOOX_ALIASES:
        return _WOOX_CANONICAL
    return ex


def exchange_aliases(exchange_id: str) -> Set[str]:
    """Return known aliases for the canonical exchange id (including itself)."""

    ex = canonical_exchange_id(exchange_id)
    if ex == _WOOX_CANONICAL:
        return set(_WOOX_ALIASES)
    return {ex}


def exchange_ids_for_query(exchange_id: str) -> List[str]:
    """Stable list of exchange ids to query for backwards compatibility."""

    ex = canonical_exchange_id(exchange_id)
    aliases = exchange_aliases(ex)
    # Prefer canonical first, then others for stable SQL placeholder order.
    out = [ex] + sorted([a for a in aliases if a != ex])
    # Drop empties defensively.
    return [x for x in out if x]


def exchange_id_for_ccxt(exchange_id: str) -> str:
    ex = canonical_exchange_id(exchange_id)
    return "woo" if ex == _WOOX_CANONICAL else ex


def canonical_symbol(exchange_id: str, symbol: str) -> str:
    sym = str(symbol or "").strip()
    if not sym:
        return ""
    ex = canonical_exchange_id(exchange_id)
    if ex == _WOOX_CANONICAL:
        return canonical_woox_symbol(sym)
    return sym


def canonical_woox_symbol(symbol: str) -> str:
    """Normalize Woo/WooX symbol strings into a canonical internal format.

    Canonical format:
    - Spot:  SPOT_{BASE}_{QUOTE}
    - Perps: PERP_{BASE}_{QUOTE}

    Accepted inputs (best-effort):
    - PERP_BTC_USDT / SPOT_BTC_USDT
    - BTC/USDT / BTC/USDT:USDT
    - BTC-USDT / BTC_USDT
    - ETH-PERP (assumes USDT)
    """

    raw = str(symbol or "").strip()
    if not raw:
        return ""

    s = raw.strip().upper()

    # Already canonical.
    m = re.match(r"^(PERP|SPOT)_(?P<base>[A-Z0-9]+)_(?P<quote>[A-Z0-9]+)$", s)
    if m:
        kind = m.group(1)
        base = m.group("base")
        quote = m.group("quote")
        return f"{kind}_{base}_{quote}"

    # CCXT-like: BTC/USDT or BTC/USDT:USDT
    if "/" in s:
        left, right = s.split("/", 1)
        base = left.strip().upper()
        right = right.strip().upper()
        quote = right.split(":", 1)[0].strip().upper()
        kind = "PERP" if ":" in right else "SPOT"
        if base and quote:
            return f"{kind}_{base}_{quote}"

    # ETH-PERP => PERP_ETH_USDT
    m = re.match(r"^(?P<base>[A-Z0-9]+)[-_]PERP$", s)
    if m:
        base = (m.group("base") or "").strip().upper()
        if base:
            return f"PERP_{base}_USDT"

    # PERP-ETH-USDT / PERP_ETH_USDT
    m = re.match(r"^PERP[-_](?P<base>[A-Z0-9]+)[-_](?P<quote>[A-Z0-9]+)$", s)
    if m:
        base = (m.group("base") or "").strip().upper()
        quote = (m.group("quote") or "").strip().upper()
        if base and quote:
            return f"PERP_{base}_{quote}"

    # SPOT-ETH-USDT / SPOT_ETH_USDT
    m = re.match(r"^SPOT[-_](?P<base>[A-Z0-9]+)[-_](?P<quote>[A-Z0-9]+)$", s)
    if m:
        base = (m.group("base") or "").strip().upper()
        quote = (m.group("quote") or "").strip().upper()
        if base and quote:
            return f"SPOT_{base}_{quote}"

    # Simple separators: BTC-USDT / BTC_USDT
    if "-" in s or "_" in s:
        parts = [p.strip().upper() for p in re.split(r"[-_]", s) if p.strip()]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
            if base and quote:
                return f"SPOT_{base}_{quote}"

    # Fallback: best-effort preserve.
    return s
