from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

import pytest


# Ensure imports resolve to the canonical src/ tree when running pytest from the repo root.
_repo_root = Path(__file__).resolve().parent
_src_path = _repo_root / "src"
if _src_path.exists():
    sys.path.insert(0, str(_src_path))


def _with_search_path(dsn: str, schema: str) -> str:
    parsed = urlparse(dsn)
    params = parse_qs(parsed.query, keep_blank_values=True)
    opts = params.get("options", [""])[0]
    opts = (opts + " ") if opts else ""
    opts += f"-c search_path={schema}"
    params["options"] = [opts]
    query = urlencode(params, doseq=True, quote_via=quote)
    return urlunparse(parsed._replace(query=query))


def _truncate_tables(conn) -> None:
    rows = conn.execute(
        """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = current_schema()
        """
    ).fetchall()
    tables = [r[0] for r in rows if r and r[0] and str(r[0]) != "schema_migrations"]
    if not tables:
        return
    table_sql = ", ".join([f'"{t}"' for t in tables])
    conn.execute(f"TRUNCATE TABLE {table_sql} CASCADE")
    conn.commit()


@pytest.fixture(scope="session", autouse=True)
def postgres_test_schema() -> Iterable[str]:
    dsn = (os.getenv("DRAGON_DATABASE_URL") or "").strip()
    if not dsn:
        raise RuntimeError("DRAGON_DATABASE_URL must be set for tests.")

    schema = f"test_{uuid.uuid4().hex[:8]}"

    from project_dragon.storage import open_postgres_connection, open_db_connection, apply_migrations

    base_conn = open_postgres_connection(dsn)
    try:
        base_conn.autocommit = True
    except Exception:
        pass
    cur = base_conn.cursor()
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    base_conn.commit()
    base_conn.close()

    os.environ["DRAGON_DATABASE_URL"] = _with_search_path(dsn, schema)

    with open_db_connection() as conn:
        apply_migrations(conn)
        conn.commit()

    yield schema

    base_conn = open_postgres_connection(dsn)
    cur = base_conn.cursor()
    cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    base_conn.commit()
    base_conn.close()


@pytest.fixture(autouse=True)
def truncate_tables_between_tests(postgres_test_schema: str) -> Iterable[None]:
    from project_dragon.storage import open_db_connection

    with open_db_connection() as conn:
        _truncate_tables(conn)
    yield
