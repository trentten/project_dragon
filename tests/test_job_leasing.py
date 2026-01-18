from __future__ import annotations

from datetime import datetime, timedelta, timezone

import project_dragon.live_worker as live_worker
from project_dragon.storage import (
    claim_job_with_lease,
    create_job,
    get_job,
    open_db_connection,
    reclaim_stale_job,
    renew_job_lease,
)


def test_claim_sets_lease_fields_and_increments_version():
    with open_db_connection() as conn:
        job_id = int(create_job(conn, "live_bot", {"x": 1}, bot_id=123))
        job = claim_job_with_lease(conn, worker_id="workerA", lease_s=30)
        assert job is not None
        assert int(job["id"]) == job_id
        assert str(job["status"]) == "running"
        assert job.get("claimed_by") == "workerA"
        assert job.get("claimed_at")
        assert job.get("lease_expires_at")
        assert job.get("last_lease_renew_at")
        assert int(job.get("lease_version") or 0) >= 1

        # Legacy compatibility
        assert job.get("worker_id") == "workerA"


def test_renew_requires_expected_version_and_owner():
    with open_db_connection() as conn:
        job_id = int(create_job(conn, "live_bot", {"x": 1}, bot_id=123))
        job = claim_job_with_lease(conn, worker_id="workerA", lease_s=30)
        assert job is not None
        v = int(job.get("lease_version") or 0)

        ok = renew_job_lease(conn, job_id=job_id, worker_id="workerA", lease_s=30, expected_lease_version=v)
        assert ok is True

        ok_wrong_owner = renew_job_lease(conn, job_id=job_id, worker_id="workerB", lease_s=30, expected_lease_version=v)
        assert ok_wrong_owner is False

        ok_wrong_ver = renew_job_lease(conn, job_id=job_id, worker_id="workerA", lease_s=30, expected_lease_version=v + 999)
        assert ok_wrong_ver is False


def test_reclaim_only_when_expired():
    with open_db_connection() as conn:
        job_id = int(create_job(conn, "live_bot", {"x": 1}, bot_id=123))
        job = claim_job_with_lease(conn, worker_id="workerA", lease_s=30)
        assert job is not None

        # Not expired yet
        ok = reclaim_stale_job(conn, job_id=job_id, new_worker_id="workerB", lease_s=30)
        assert ok is False

        # Force expiry
        past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        conn.execute("UPDATE jobs SET lease_expires_at = %s WHERE id = %s", (past, job_id))
        conn.commit()

        ok = reclaim_stale_job(conn, job_id=job_id, new_worker_id="workerB", lease_s=30)
        assert ok is True

        after = get_job(conn, job_id)
        assert after is not None
        assert after.get("claimed_by") == "workerB"
        assert after.get("worker_id") == "workerB"
        assert int(after.get("stale_reclaims") or 0) >= 1
        assert int(after.get("lease_version") or 0) >= int(job.get("lease_version") or 0) + 1


def test_worker_helper_stops_when_lease_renewal_fails():
    events = []

    def emit(level: str, event_type: str, message: str, payload=None):
        events.append((level, event_type, message, payload or {}))

    with open_db_connection() as conn:
        job_id = int(create_job(conn, "live_bot", {"x": 1}, bot_id=123))
        job = claim_job_with_lease(conn, worker_id="workerA", lease_s=30)
        assert job is not None

        # Seed expected lease version cache like the worker does.
        live_worker._JOB_EXPECTED_LEASE_VERSION.pop(job_id, None)
        live_worker._JOB_LAST_LEASE_RENEW_S.pop(job_id, None)

        # Bump lease_version to simulate another worker reclaiming it.
        conn.execute("UPDATE jobs SET lease_version = COALESCE(lease_version, 0) + 1 WHERE id = %s", (job_id,))
        conn.commit()

        ok = live_worker._maybe_renew_job_lease(
            conn=conn,
            job=job,
            worker_id="workerA",
            lease_s=30,
            renew_every_s=0.0,
            emit=emit,
        )
        assert ok is False
        assert any(et == "job_lease_lost" for _, et, _, _ in events)
