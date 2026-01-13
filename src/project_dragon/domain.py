from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional
from datetime import datetime


class Side(Enum):
    LONG = auto()
    SHORT = auto()


class PositionSide(Enum):
    FLAT = auto()
    LONG = auto()
    SHORT = auto()


class OrderType(Enum):
    MARKET = auto()
    LIMIT = auto()


class OrderStatus(Enum):
    OPEN = auto()
    FILLED = auto()
    CANCELLED = auto()
    PARTIAL = auto()
    REJECTED = auto()


class OrderActivationState(Enum):
    PARKED = auto()
    ACTIVE = auto()


@dataclass
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Order:
    id: int
    side: Side
    type: OrderType
    price: Optional[float]  # None for market
    size: float
    status: OrderStatus = OrderStatus.OPEN
    filled_size: float = 0.0
    note: str = ""
    activation_band_pct: Optional[float] = None
    target_price: Optional[float] = None
    activation_state: OrderActivationState = OrderActivationState.ACTIVE
    last_activation_price: Optional[float] = None


@dataclass
class Trade:
    index: Optional[int]
    timestamp: Optional[datetime]
    side: Side
    price: float
    size: float
    pnl: float = 0.0
    note: str = ""

    @property
    def realized_pnl(self) -> float:
        return self.pnl

    def to_dict(self) -> Dict[str, Optional[object]]:
        return {
            "index": self.index,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "side": self.side.name if isinstance(self.side, Enum) else str(self.side),
            "price": self.price,
            "size": self.size,
            "pnl": self.pnl,
            "realized_pnl": self.realized_pnl,
            "note": self.note,
        }


@dataclass
class Position:
    side: PositionSide = PositionSide.FLAT
    size: float = 0.0
    avg_price: float = 0.0

    def reset(self) -> None:
        self.side = PositionSide.FLAT
        self.size = 0.0
        self.avg_price = 0.0


@dataclass
class BacktestResult:
    candles: List[Candle]
    equity_curve: List[float]
    trades: List[Trade]
    metrics: Dict[str, float] = field(default_factory=dict)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    equity_timestamps: List[datetime] = field(default_factory=list)
    params: Dict[str, float] = field(default_factory=dict)
    run_context: Dict[str, float] = field(default_factory=dict)
    computed_metrics: Dict[str, float] = field(default_factory=dict)
    extra_series: Dict[str, List[Optional[float]]] = field(default_factory=dict)
