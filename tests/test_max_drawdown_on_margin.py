from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from project_dragon.storage import get_backtest_run_details, init_db, open_db_connection, save_backtest_run


@dataclass
class _FakeResult:
    equity_curve: List[float]
    metrics: Dict[str, Any]
    trades: Optional[list] = None
    start_time: Any = None
    end_time: Any = None


def test_save_backtest_run_sets_max_drawdown_from_equity_curve(tmp_path) -> None:
    db_path = tmp_path / "test_dd_margin.sqlite"
    init_db(db_path)

    # Equity curve (mark-to-market account equity).
    equity_curve = [1000.0, 900.0, 1100.0, 800.0]

    # Max drawdown on equity:
    # peak 1100 -> trough 800 => 300 / 1100
    fake_result = _FakeResult(
        equity_curve=equity_curve,
        metrics={
            "net_profit": equity_curve[-1] - equity_curve[0],
            # include a different value to ensure it's overridden
            "max_drawdown_pct": 0.2,
            "max_drawdown": 0.2,
        },
    )

    run_id = "run_dd_margin_1"
    save_backtest_run(
        config={
            "general": {"initial_entry_balance_pct": 100.0},
            "_futures": {"leverage": 10},
        },
        metrics=fake_result.metrics,
        metadata={
            "id": run_id,
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
            "initial_balance": 1000.0,
            "market_type": "perps",
        },
        sweep_id=None,
        result=fake_result,
    )

    with open_db_connection(db_path) as conn:
        row = conn.execute(
            "SELECT max_drawdown_pct FROM backtest_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        assert row is not None
        assert float(row[0]) == (300.0 / 1100.0)

        details = get_backtest_run_details(run_id, conn=conn)

    assert details is not None
    metrics = details.get("metrics")
    assert isinstance(metrics, dict)
    assert float(metrics.get("max_drawdown_pct")) == (300.0 / 1100.0)
