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
    timestamp: Optional[datetime]
    side: Side
    price: float
    size: float
    pnl: float = 0.0
    note: str = ""


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
    trades: List[Trade]
    equity_curve: List[float]
    equity_timestamps: List[datetime] = field(default_factory=list)
    candles: List[Candle] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    params: Dict[str, float] = field(default_factory=dict)
