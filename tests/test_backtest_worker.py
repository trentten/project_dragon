from __future__ import annotations

import json
from uuid import uuid5, NAMESPACE_DNS

import project_dragon.backtest_worker as backtest_worker
from project_dragon.storage import (
    SweepMeta,
    bulk_enqueue_backtest_run_jobs,
    claim_job_with_lease,
    compute_run_key,
    create_sweep,
    create_job,
    get_job,
    init_db,
    open_db_connection,
    reclaim_stale_job,
    set_job_cancel_requested,
    set_job_pause_requested,
)


def _create_toy_sweep(*, name: str = "toy sweep") -> str:
    base_config = {
        "general": {"max_entries": 1, "allow_long": True, "allow_short": False},
        "exits": {},
        "dca": {},
        "trend": {},
        "bbands": {},
        "macd": {},
        "rsi": {},
    }
    data_settings = {
        "data_source": "synthetic",
        "range_mode": "bars",
        "range_params": {"count": 30},
        "initial_balance": 1000.0,
        "fee_rate": 0.0004,
    }

    sweep_def = {
        "params": {"general.max_entries": ["1", "2"]},
        "params_mode": {"general.max_entries": "range"},
        "field_meta": {
            "general.max_entries": {"target": "config", "path": ["general", "max_entries"], "type": "int"}
        },
        "metadata_common": {"strategy_name": "DragonDcaAtr", "strategy_version": "0.1.0"},
        "base_config": base_config,
        "data_settings": data_settings,
    }

    return create_sweep(
        SweepMeta(
            user_id="admin@local",
            name=name,
            strategy_name="DragonDcaAtr",
            strategy_version="0.1.0",
            data_source="synthetic",
            exchange_id=None,
            symbol=None,
            timeframe=None,
            range_mode="bars",
            range_params_json=json.dumps({"count": 30}),
            base_config_json=json.dumps(base_config),
            sweep_definition_json=json.dumps(sweep_def),
            sweep_scope="single_asset",
            sweep_assets_json=None,
            status="queued",
        )
    )


def test_bulk_enqueue_idempotent(tmp_path):
    db_path = tmp_path / "idempotent.db"
    init_db(db_path)

    jobs = []
    sweep_id = "sweep_123"
    for i in range(10):
        key = f"sweep:{sweep_id}:combo:{i}:symbol:BTC/USDT:tf:1h"
        payload = {"config": {"general": {"max_entries": 1}}, "data_settings": {"data_source": "synthetic", "range_params": {"count": 10}}}
        jobs.append((key, payload, sweep_id))

    with open_db_connection(db_path) as conn:
        r1 = bulk_enqueue_backtest_run_jobs(conn, jobs=jobs)
        r2 = bulk_enqueue_backtest_run_jobs(conn, jobs=jobs)

        assert int(r1["created"]) == 10
        assert int(r2["created"]) == 0
        assert int(r2["existing"]) == 10

        rows = conn.execute("SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run'").fetchone()
        assert int(rows[0]) == 10


def test_lease_reclaim_for_backtest_job(tmp_path):
    db_path = tmp_path / "lease_backtest.db"
    init_db(db_path)

    with open_db_connection(db_path) as conn:
        _ = create_job(conn, "backtest_run", {"x": 1})
        job = claim_job_with_lease(conn, worker_id="w1", lease_s=1.0, job_types=("backtest_run",))
        assert job is not None
        job_id = int(job["id"])

        # Force expiry.
        conn.execute("UPDATE jobs SET lease_expires_at = '2000-01-01T00:00:00+00:00' WHERE id = %s", (job_id,))
        conn.commit()

        ok = reclaim_stale_job(conn, job_id=job_id, new_worker_id="w2", lease_s=30)
        assert ok is True
        after = get_job(conn, job_id)
        assert after is not None
        assert after.get("claimed_by") == "w2"


def test_worker_executes_synthetic_backtest(tmp_path, monkeypatch):
    db_path = tmp_path / "worker_run.db"
    init_db(db_path)

    # Point worker at temp db.
    monkeypatch.setattr(backtest_worker, "open_db_connection", lambda: open_db_connection(db_path))
    monkeypatch.setattr(backtest_worker, "init_db", lambda: init_db(db_path))

    sweep_id = "sweep_toy"
    job_key = f"sweep:{sweep_id}:combo:toy:symbol:SYNTH:tf:synthetic"

    payload = {
        "sweep_id": sweep_id,
        "config": {
            "general": {"max_entries": 1, "allow_long": True, "allow_short": False},
            "exits": {},
            "dca": {},
            "trend": {},
            "bbands": {},
            "macd": {},
            "rsi": {},
        },
        "data_settings": {
            "data_source": "synthetic",
            "range_mode": "bars",
            "range_params": {"count": 50},
            "initial_balance": 1000.0,
            "fee_rate": 0.0004,
        },
        "metadata": {"strategy_name": "DragonDcaAtr", "strategy_version": "0.1.0"},
    }

    run_key = compute_run_key(payload)
    payload["run_key"] = run_key
    payload["job_key"] = job_key

    with open_db_connection(db_path) as conn:
        create_job(conn, "backtest_run", payload, sweep_id=sweep_id, run_id=None, job_key=job_key)

    # One poll should claim + process.
    n = backtest_worker._poll_once("test_worker")
    assert int(n) == 1

    with open_db_connection(db_path) as conn:
        row = conn.execute("SELECT status, run_id FROM jobs WHERE job_key = %s", (job_key,)).fetchone()
        assert row is not None
        assert str(row[0]) == "done"
        job_run_id = str(row[1])
        assert job_run_id.strip()

        # Exactly-once persistence by run_key.
        # Use details-table metadata_json for run_key lookup.
        run_row = conn.execute(
            "SELECT run_id FROM backtest_run_details WHERE metadata_json::json->>'run_key' = %s",
            (run_key,),
        ).fetchall()
        assert len(run_row) == 1
        assert str(run_row[0][0]) == job_run_id

        # Simulate a retry/reclaim by re-queuing the same job.
        conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                claimed_by = NULL,
                worker_id = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                last_lease_renew_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                error_text = ''
            WHERE job_key = %s
            """,
            (job_key,),
        )
        conn.commit()

    n2 = backtest_worker._poll_once("test_worker")
    assert int(n2) == 1

    with open_db_connection(db_path) as conn:
        # Still only one run row for this combo.
        run_row2 = conn.execute(
            "SELECT COUNT(*) FROM backtest_run_details WHERE metadata_json::json->>'run_key' = %s",
            (run_key,),
        ).fetchone()
        assert int(run_row2[0]) == 1


def test_sweep_parent_plans_children_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "sweep_parent.db"
    init_db(db_path)

    # Point worker at temp db.
    monkeypatch.setattr(backtest_worker, "open_db_connection", lambda: open_db_connection(db_path))
    monkeypatch.setattr(backtest_worker, "init_db", lambda: init_db(db_path))

    base_config = {
        "general": {"max_entries": 1, "allow_long": True, "allow_short": False},
        "exits": {},
        "dca": {},
        "trend": {},
        "bbands": {},
        "macd": {},
        "rsi": {},
    }
    data_settings = {
        "data_source": "synthetic",
        "range_mode": "bars",
        "range_params": {"count": 30},
        "initial_balance": 1000.0,
        "fee_rate": 0.0004,
    }

    sweep_def = {
        "params": {"general.max_entries": ["1", "2"]},
        "params_mode": {"general.max_entries": "range"},
        "field_meta": {
            "general.max_entries": {"target": "config", "path": ["general", "max_entries"], "type": "int"}
        },
        "metadata_common": {"strategy_name": "DragonDcaAtr", "strategy_version": "0.1.0"},
        "base_config": base_config,
        "data_settings": data_settings,
    }

    sweep_id = create_sweep(
        SweepMeta(
            user_id="admin@local",
            name="toy sweep",
            strategy_name="DragonDcaAtr",
            strategy_version="0.1.0",
            data_source="synthetic",
            exchange_id=None,
            symbol=None,
            timeframe=None,
            range_mode="bars",
            range_params_json=json.dumps({"count": 30}),
            base_config_json=json.dumps(base_config),
            sweep_definition_json=json.dumps(sweep_def),
            sweep_scope="single_asset",
            sweep_assets_json=None,
            status="queued",
        )
    )

    parent_job_key = f"sweep_parent:{sweep_id}"
    with open_db_connection(db_path) as conn:
        parent_job_id = create_job(
            conn,
            "sweep_parent",
            {"sweep_id": sweep_id},
            sweep_id=sweep_id,
            job_key=parent_job_key,
        )

    n = backtest_worker._poll_once("planner")
    assert int(n) == 1

    with open_db_connection(db_path) as conn:
        parent_row = conn.execute("SELECT id, status FROM jobs WHERE job_key = %s", (parent_job_key,)).fetchone()
        assert parent_row is not None
        assert int(parent_row[0]) == int(parent_job_id)
        assert str(parent_row[1]) == "done"

        # Two child combos; linked to parent_job_id.
        c0 = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s",
            (int(parent_job_id),),
        ).fetchone()
        assert int(c0[0]) == 2

        # Requeue the parent and rerun planning: should not duplicate children.
        conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                claimed_by = NULL,
                worker_id = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                last_lease_renew_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                error_text = ''
            WHERE job_key = %s
            """,
            (parent_job_key,),
        )
        conn.commit()

    n2 = backtest_worker._poll_once("planner")
    assert int(n2) == 1

    with open_db_connection(db_path) as conn:
        c = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s",
            (int(parent_job_id),),
        ).fetchone()
        assert int(c[0]) == 2


def test_pause_blocks_claiming_children_and_resume_unblocks(tmp_path, monkeypatch):
    db_path = tmp_path / "sweep_pause_claim.db"
    init_db(db_path)

    # Point worker at temp db.
    monkeypatch.setattr(backtest_worker, "open_db_connection", lambda: open_db_connection(db_path))
    monkeypatch.setattr(backtest_worker, "init_db", lambda: init_db(db_path))

    sweep_id = _create_toy_sweep(name="pause-claim")
    parent_job_key = f"sweep_parent:{sweep_id}"
    with open_db_connection(db_path) as conn:
        parent_job_id = create_job(conn, "sweep_parent", {"sweep_id": sweep_id}, sweep_id=sweep_id, job_key=parent_job_key)

    # Plan children.
    n = backtest_worker._poll_once("planner")
    assert int(n) == 1

    with open_db_connection(db_path) as conn:
        # Pause the parent (even if planning is done) should block child claims.
        set_job_pause_requested(conn, int(parent_job_id), True)
        claimed = claim_job_with_lease(conn, worker_id="w1", lease_s=30.0, job_types=("backtest_run",))
        assert claimed is None

        # Resume should allow claiming again.
        set_job_pause_requested(conn, int(parent_job_id), False)
        claimed2 = claim_job_with_lease(conn, worker_id="w1", lease_s=30.0, job_types=("backtest_run",))
        assert claimed2 is not None
        assert str(claimed2.get("job_type")) == "backtest_run"
        assert int(claimed2.get("parent_job_id") or 0) == int(parent_job_id)


def test_cancel_marks_queued_children_cancelled_and_blocks_claiming(tmp_path, monkeypatch):
    db_path = tmp_path / "sweep_cancel.db"
    init_db(db_path)

    # Point worker at temp db.
    monkeypatch.setattr(backtest_worker, "open_db_connection", lambda: open_db_connection(db_path))
    monkeypatch.setattr(backtest_worker, "init_db", lambda: init_db(db_path))

    sweep_id = _create_toy_sweep(name="cancel-claim")
    parent_job_key = f"sweep_parent:{sweep_id}"
    with open_db_connection(db_path) as conn:
        parent_job_id = create_job(conn, "sweep_parent", {"sweep_id": sweep_id}, sweep_id=sweep_id, job_key=parent_job_key)

    # Plan children.
    n = backtest_worker._poll_once("planner")
    assert int(n) == 1

    with open_db_connection(db_path) as conn:
        # Cancel should bulk-cancel queued children (v1).
        set_job_cancel_requested(conn, int(parent_job_id), True)

        cancelled = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s AND status = 'cancelled'",
            (int(parent_job_id),),
        ).fetchone()
        assert int(cancelled[0]) == 2

        # And claiming should yield nothing.
        claimed = claim_job_with_lease(conn, worker_id="w1", lease_s=30.0, job_types=("backtest_run",))
        assert claimed is None


def test_pause_then_resume_allows_planning_without_duplicates(tmp_path, monkeypatch):
    db_path = tmp_path / "sweep_pause_resume_plan.db"
    init_db(db_path)

    # Point worker at temp db.
    monkeypatch.setattr(backtest_worker, "open_db_connection", lambda: open_db_connection(db_path))
    monkeypatch.setattr(backtest_worker, "init_db", lambda: init_db(db_path))

    sweep_id = _create_toy_sweep(name="pause-resume-plan")
    parent_job_key = f"sweep_parent:{sweep_id}"
    with open_db_connection(db_path) as conn:
        parent_job_id = create_job(conn, "sweep_parent", {"sweep_id": sweep_id}, sweep_id=sweep_id, job_key=parent_job_key)
        set_job_pause_requested(conn, int(parent_job_id), True)

    # Planner should observe pause and not enqueue children.
    n = backtest_worker._poll_once("planner")
    assert int(n) == 1

    with open_db_connection(db_path) as conn:
        c0 = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s",
            (int(parent_job_id),),
        ).fetchone()
        assert int(c0[0]) == 0

        # Resume should requeue parent and allow planning.
        set_job_pause_requested(conn, int(parent_job_id), False)

    n2 = backtest_worker._poll_once("planner")
    assert int(n2) == 1

    with open_db_connection(db_path) as conn:
        c1 = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s",
            (int(parent_job_id),),
        ).fetchone()
        assert int(c1[0]) == 2

        # Requeue parent and re-run planning: should not duplicate children.
        conn.execute(
            """
            UPDATE jobs
            SET status = 'queued',
                claimed_by = NULL,
                worker_id = NULL,
                claimed_at = NULL,
                lease_expires_at = NULL,
                last_lease_renew_at = NULL,
                started_at = NULL,
                finished_at = NULL,
                error_text = ''
            WHERE id = %s
            """,
            (int(parent_job_id),),
        )
        conn.commit()

    n3 = backtest_worker._poll_once("planner")
    assert int(n3) == 1

    with open_db_connection(db_path) as conn:
        c2 = conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_type='backtest_run' AND parent_job_id = %s",
            (int(parent_job_id),),
        ).fetchone()
        assert int(c2[0]) == 2
