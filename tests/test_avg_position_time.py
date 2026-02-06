from __future__ import annotations

from project_dragon.storage import (
    init_db,
    list_runs_server_side,
    load_backtest_runs_explorer_rows,
    open_db_connection,
    save_backtest_run,
)
from project_dragon.time_utils import format_duration


def test_format_duration_rules() -> None:
    assert format_duration(None) == ""
    assert format_duration(0) == ""
    assert format_duration(30) == "30s"
    assert format_duration(65) == "1m 5s"
    assert format_duration(3600) == "1h 0m"
    assert format_duration(3661) == "1h 1m"
    assert format_duration(86399) == "23h 59m"
    assert format_duration(86400) == "1d 0h"
    assert format_duration(90061) == "1d 1h"


def test_runs_queries_return_avg_position_time_seconds() -> None:
    init_db()
    run_id = "run_avg_pos_1"
    save_backtest_run(
        config={},
        metrics={"net_profit": 1.0, "avg_position_time_seconds": 90.0},
        metadata={
            "id": run_id,
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=None,
        result=None,
    )

    with open_db_connection() as conn:
        rows, total = list_runs_server_side(
            conn,
            page=1,
            page_size=50,
            visible_columns=["run_id", "avg_position_time_seconds"],
        )

    assert total == 1
    assert len(rows) == 1
    row = rows[0]
    assert row.get("run_id") == run_id
    assert row.get("avg_position_time_seconds") == 90.0

    rows_explorer = load_backtest_runs_explorer_rows(user_id=None, limit=10, filters={})
    assert len(rows_explorer) == 1
    assert rows_explorer[0].get("avg_position_time_seconds") == 90.0
