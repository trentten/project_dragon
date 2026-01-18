from __future__ import annotations

from project_dragon import storage
from project_dragon.storage import init_db, open_db_connection, save_backtest_run


def test_save_backtest_run_writes_summary_and_details_atomically(monkeypatch):
    init_db()

    run_id = "run_atomic_1"

    # Force the details write to fail for this run_id.
    def _fail_details(*_args, **_kwargs):
        raise RuntimeError("forced details insert failure")

    monkeypatch.setattr(storage, "upsert_backtest_run_details", _fail_details)

    try:
        save_backtest_run(
            config={"x": 1},
            metrics={"net_profit": 1.0},
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
        assert False, "expected save_backtest_run to raise when details insert fails"
    except RuntimeError:
        pass

    # Must be fully rolled back: no summary row and no details row.
    with open_db_connection() as conn:
        row = conn.execute("SELECT COUNT(1) FROM backtest_runs WHERE id = %s", (run_id,)).fetchone()
        assert int(row[0]) == 0
        row = conn.execute("SELECT COUNT(1) FROM backtest_run_details WHERE run_id = %s", (run_id,)).fetchone()
        assert int(row[0]) == 0


def test_save_backtest_run_success_writes_both():
    init_db()

    run_id = "run_ok_1"
    save_backtest_run(
        config={"x": 1},
        metrics={"net_profit": 1.0},
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
        row = conn.execute("SELECT COUNT(1) FROM backtest_runs WHERE id = %s", (run_id,)).fetchone()
        assert int(row[0]) == 1
        row = conn.execute("SELECT COUNT(1) FROM backtest_run_details WHERE run_id = %s", (run_id,)).fetchone()
        assert int(row[0]) == 1
