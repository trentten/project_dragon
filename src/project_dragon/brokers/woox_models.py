from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from project_dragon.domain import PositionSide


@dataclass
class WooXPosition:
    symbol: str
    position_side: str
    # Raw signed holding as reported by venue. Can be negative.
    holding_raw: float
    # Absolute holding size for convenience (strategy/broker sizing).
    holding_abs: float
    # Back-compat alias used by older code paths (treated as absolute).
    holding: float
    average_open_price: float
    mark_price: Optional[float] = None
    timestamp_ms: Optional[int] = None
    pnl_24h: Optional[float] = None
    fee_24h: Optional[float] = None
    pnl: float = 0.0
    leverage: Optional[float] = None

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            f = float(value)
        except (TypeError, ValueError):
            return None
        if f != f:  # NaN
            return None
        return f

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            i = int(float(value))
        except (TypeError, ValueError):
            return None
        return i

    @classmethod
    def from_api(cls, payload: Dict[str, Any]) -> "WooXPosition":
        symbol = str(payload.get("symbol", "") or "")
        position_side = str(payload.get("positionSide", "") or "")

        holding_raw = cls._safe_float(payload.get("holding"))
        holding_raw = float(holding_raw) if holding_raw is not None else 0.0
        holding_abs = abs(float(holding_raw))

        avg_open = cls._safe_float(payload.get("averageOpenPrice"))
        average_open_price = float(avg_open) if avg_open is not None else 0.0

        mark = cls._safe_float(payload.get("markPrice"))
        ts_ms = cls._safe_int(payload.get("timestamp"))
        pnl_24h = cls._safe_float(payload.get("pnl24H"))
        fee_24h = cls._safe_float(payload.get("fee24H"))
        leverage_val = cls._safe_float(payload.get("leverage"))

        pnl = cls._safe_float(payload.get("pnl"))
        pnl = float(pnl) if pnl is not None else 0.0

        return cls(
            symbol=symbol,
            position_side=position_side,
            holding_raw=holding_raw,
            holding_abs=holding_abs,
            holding=holding_abs,
            average_open_price=average_open_price,
            mark_price=mark,
            timestamp_ms=ts_ms,
            pnl_24h=pnl_24h,
            fee_24h=fee_24h,
            pnl=pnl,
            leverage=leverage_val,
        )

    def to_position_side_enum(self) -> PositionSide:
        side_upper = (self.position_side or "").upper()
        if side_upper == "LONG":
            return PositionSide.LONG
        if side_upper == "SHORT":
            return PositionSide.SHORT
        return PositionSide.FLAT
