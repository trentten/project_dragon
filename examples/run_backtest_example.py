from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

from project_dragon.domain import Candle
from project_dragon.config_dragon import dragon_config_example
from project_dragon.engine import BacktestEngine, BacktestConfig
from project_dragon.strategy_dragon import DragonDcaAtrStrategy


def generate_dummy_candles(n: int = 200) -> List[Candle]:
    """
    Creates a simple synthetic price series so the pipeline can run.
    Replace this with real OHLCV data from CSV or an exchange later.
    """
    candles: List[Candle] = []
    t = datetime.utcnow()
    price = 100.0

    for i in range(n):
        t = t + timedelta(minutes=1)
        # simple random-ish walk without using random module
        delta = ((i % 10) - 5) * 0.01  # deterministic wobble
        open_ = price
        close = price * (1 + delta / 100)
        high = max(open_, close) * 1.002
        low = min(open_, close) * 0.998
        volume = 1.0

        candles.append(
            Candle(
                timestamp=t,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
        )
        price = close

    return candles


def main() -> None:
    candles = generate_dummy_candles(300)

    bt_config = BacktestConfig(
        initial_balance=1_000.0,
        fee_rate=0.0004,
    )
    engine = BacktestEngine(bt_config)

    strategy = DragonDcaAtrStrategy(config=dragon_config_example)

    result = engine.run(candles, strategy)

    print(f"Backtest finished. Equity points: {len(result.equity_curve)}")
    if result.trades:
        print(f"Number of trades: {len(result.trades)}")
        print(f"First trade: {result.trades[0]}")
    else:
        print("No trades yet (strategy logic is still stubbed).")


if __name__ == "__main__":
    main()
