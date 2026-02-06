from __future__ import annotations

import json

from project_dragon.storage import SweepMeta, create_job, create_sweep, init_db, open_db_connection, reconcile_sweep_statuses


def test_reconcile_sweep_running_to_done():
    init_db()

    sweep_id = create_sweep(
        SweepMeta(
            user_id="admin@local",
            name="reconcile sweep",
            strategy_name="DragonDcaAtr",
            strategy_version="0.1.0",
            data_source="synthetic",
            exchange_id=None,
            symbol=None,
            timeframe=None,
            range_mode="bars",
            range_params_json=json.dumps({"count": 5}),
            base_config_json=json.dumps({"general": {"max_entries": 1}}),
            sweep_definition_json=json.dumps({"params": {}, "base_config": {}}),
            sweep_scope="single_asset",
            sweep_assets_json=None,
            status="running",
        )
    )

    with open_db_connection() as conn:
        create_job(conn, "backtest_run", {"x": 1}, sweep_id=sweep_id)
        create_job(conn, "backtest_run", {"x": 2}, sweep_id=sweep_id)
        conn.execute("UPDATE jobs SET status = 'done' WHERE sweep_id = %s", (sweep_id,))
        conn.commit()

    with open_db_connection() as conn:
        out = reconcile_sweep_statuses(conn, sweep_ids=[sweep_id])
        assert int(out.get("updated") or 0) == 1

    with open_db_connection() as conn:
        row = conn.execute("SELECT status FROM sweeps WHERE id = %s", (sweep_id,)).fetchone()
        assert row is not None
        assert str(row[0]) == "done"
