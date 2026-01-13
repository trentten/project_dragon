from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Protocol

from .domain import BacktestResult, Candle
from .broker_sim import BrokerSim
from .metrics import compute_metrics


class Strategy(Protocol):
    def on_bar(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        ...

    def on_finalize(self, broker: BrokerSim) -> None:
        ...


@dataclass
class BacktestConfig:
    initial_balance: float = 1_000.0
    fee_rate: float = 0.0001


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
            broker.update_bar_context(i, candle)
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
                elif broker.position.side.name == "SHORT":
                    equity -= broker.position.size * candle.close
            equity_curve.append(equity)

        # Final hook
        strategy.on_finalize(broker)

        extra_series: Dict[str, List[Optional[float]]] = {}
        if hasattr(strategy, "debug_avg_entry_prices"):
            extra_series["avg_entry_price"] = list(strategy.debug_avg_entry_prices)
        if hasattr(strategy, "debug_next_dca_prices"):
            extra_series["next_dca_price"] = list(strategy.debug_next_dca_prices)
        if hasattr(strategy, "debug_tp_levels"):
            extra_series["tp_level"] = list(strategy.debug_tp_levels)
        if hasattr(strategy, "debug_sl_levels"):
            extra_series["sl_level"] = list(strategy.debug_sl_levels)

        start_time = candle_list[0].timestamp if candle_list else None
        end_time = candle_list[-1].timestamp if candle_list else None

        result = BacktestResult(
            candles=candle_list,
            equity_curve=equity_curve,
            trades=broker.trades,
            metrics={},
            start_time=start_time,
            end_time=end_time,
            equity_timestamps=[],
            params={},
            extra_series=extra_series,
        )
        result.metrics = compute_metrics(equity_curve, broker.trades, self.bt_config.initial_balance)
        try:
            result.metrics["fees_total"] = float(getattr(broker, "fees_total", 0.0) or 0.0)
        except Exception:
            result.metrics["fees_total"] = 0.0
        return result
