import os
import pytest

from project_dragon.storage import (
    _get_or_create_user_insert_sql,
    apply_migrations,
    claim_job_with_lease,
    create_job,
    get_or_create_user,
    is_postgres_dsn,
    open_db_connection,
    psycopg,
    psycopg2,
)


def test_get_or_create_user_sql_variants() -> None:
    pg_sql = _get_or_create_user_insert_sql("postgres")
    assert "ON CONFLICT" in pg_sql
    assert "VALUES" in pg_sql


@pytest.mark.skipif(
    not is_postgres_dsn(os.getenv("DRAGON_DATABASE_URL", "")),
    reason="DRAGON_DATABASE_URL not set to Postgres",
)
@pytest.mark.skipif(
    psycopg is None and psycopg2 is None,
    reason="Postgres driver missing. Install psycopg[binary] or psycopg2-binary.",
)
def test_postgres_migrations_smoke() -> None:
    with open_db_connection() as conn:
        apply_migrations(conn)
        user_id = get_or_create_user(conn, "pg-smoke@example.com")
        assert user_id
        job_id = create_job(conn, "live_bot", {"foo": "bar"})
        assert job_id > 0
        claimed = claim_job_with_lease(conn, worker_id="pytest", lease_s=5.0, job_types=("live_bot",))
        assert claimed is not None
