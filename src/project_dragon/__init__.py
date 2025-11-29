"""
Project Dragon core package.

This package contains:
- Domain models (Candle, Order, Position, Trade, BacktestResult)
- A simple broker simulator (BrokerSim)
- A backtest engine (BacktestEngine)
- The Dragon DCA ATR strategy skeleton
"""

from .domain import Candle, Trade, Position, BacktestResult  # noqa: F401
from .broker_sim import BrokerSim  # noqa: F401
from .engine import BacktestEngine, BacktestConfig  # noqa: F401
