from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Protocol

from .domain import BacktestResult, Candle
from .broker_sim import BrokerSim


class Strategy(Protocol):
    def on_bar(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        ...

    def on_finalize(self, broker: BrokerSim) -> None:
        ...


@dataclass
class BacktestConfig:
    initial_balance: float = 1_000.0
    fee_rate: float = 0.0004


class BacktestEngine:
    def __init__(self, bt_config: BacktestConfig) -> None:
        self.bt_config = bt_config

    def run(
        self,
        candles: Iterable[Candle],
        strategy: Strategy,
    ) -> BacktestResult:
        broker = BrokerSim(
            balance=self.bt_config.initial_balance,
            fee_rate=self.bt_config.fee_rate,
        )

        equity_curve: List[float] = []
        candle_list = list(candles)

        for i, candle in enumerate(candle_list):
            # 1) Let broker process pending limit orders
            broker.process_candle(candle)

            # 2) Strategy logic for this bar
            strategy.on_bar(i, candle, broker)

            # 3) Reprocess orders that may have been placed during this bar
            broker.process_candle(candle)

            # 4) Track equity: balance + mark-to-market
            equity = broker.balance
            if broker.position.size > 0:
                if broker.position.side.name == "LONG":
                    equity += broker.position.size * candle.close
                # later: handle shorts
            equity_curve.append(equity)

        # Final hook
        strategy.on_finalize(broker)

        result = BacktestResult(
            trades=broker.trades,
            equity_curve=equity_curve,
            metrics={},
            params={},
        )
        return result
