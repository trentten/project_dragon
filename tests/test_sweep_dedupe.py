from __future__ import annotations

from project_dragon.storage import dedupe_sweep_runs, init_db, open_db_connection, save_backtest_run


def test_dedupe_sweep_runs_removes_config_duplicates():
    init_db()

    sweep_id = "sweep_1"

    save_backtest_run(
        config={"a": 1, "b": {"x": 2, "y": 3}},
        metrics={"net_profit": 1.0},
        metadata={
            "id": "r1",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=sweep_id,
        result=None,
    )

    # Same logical config, different key order.
    save_backtest_run(
        config={"b": {"y": 3, "x": 2}, "a": 1},
        metrics={"net_profit": 2.0},
        metadata={
            "id": "r2",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=sweep_id,
        result=None,
    )

    dd = dedupe_sweep_runs(sweep_id)
    assert int(dd.get("total") or 0) == 2
    assert int(dd.get("removed") or 0) == 1

    with open_db_connection() as conn:
        row = conn.execute("SELECT COUNT(1) FROM backtest_runs WHERE sweep_id = %s", (sweep_id,)).fetchone()
        assert int(row[0]) == 1
        row = conn.execute(
            "SELECT COUNT(1) FROM backtest_run_details WHERE run_id IN ('r1','r2')"
        ).fetchone()
        assert int(row[0]) == 1


def test_dedupe_sweep_runs_does_not_cross_symbols():
    init_db()

    sweep_id = "sweep_syms"

    for rid, sym in [("r1", "BTC/USDT"), ("r2", "ETH/USDT")]:
        save_backtest_run(
            config={"a": 1, "b": 2},
            metrics={"net_profit": 1.0},
            metadata={
                "id": rid,
                "symbol": sym,
                "timeframe": "1h",
                "strategy_name": "Test",
                "strategy_version": "0",
            },
            sweep_id=sweep_id,
            result=None,
        )

    dd = dedupe_sweep_runs(sweep_id)
    assert int(dd.get("total") or 0) == 2
    assert int(dd.get("removed") or 0) == 0


def test_dedupe_sweep_runs_prefers_bot_mapped_run():
    init_db()

    sweep_id = "sweep_botmap"

    save_backtest_run(
        config={"a": 1, "b": 2},
        metrics={"net_profit": 1.0},
        metadata={
            "id": "r1",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=sweep_id,
        result=None,
    )

    save_backtest_run(
        config={"b": 2, "a": 1},
        metrics={"net_profit": 2.0},
        metadata={
            "id": "r2",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=sweep_id,
        result=None,
    )

    # Create a bot and map it to r2, so r2 should be the survivor.
    with open_db_connection() as conn:
        conn.execute(
            """
            INSERT INTO bots(name, exchange_id, symbol, timeframe, status, desired_status, config_json, created_at, updated_at)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            ("b1", "binance", "BTC/USDT", "1h", "paused", "paused", "{}", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"),
        )
        bot_id = int(conn.execute("SELECT id FROM bots ORDER BY id DESC LIMIT 1").fetchone()[0])
        conn.execute(
            """
            INSERT INTO bot_run_map(bot_id, run_id, created_at)
            VALUES(%s, %s, %s)
            ON CONFLICT (bot_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                created_at = EXCLUDED.created_at
            """,
            (bot_id, "r2", "2025-01-01T00:00:00Z"),
        )
        conn.commit()

    dd = dedupe_sweep_runs(sweep_id)
    assert int(dd.get("total") or 0) == 2
    assert int(dd.get("removed") or 0) == 1

    with open_db_connection() as conn:
        row = conn.execute(
            "SELECT id FROM backtest_runs WHERE sweep_id = %s ORDER BY created_at ASC LIMIT 1",
            (sweep_id,),
        ).fetchone()
        assert row and str(row[0]) == "r2"
