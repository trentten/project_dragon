from __future__ import annotations

import sqlite3
from pathlib import Path

from project_dragon.storage import open_db_connection

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "backtests.sqlite"

def assert_column(conn: sqlite3.Connection, table: str, column: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    cols = {r[1] for r in rows}
    assert column in cols, f"Missing column {column} in {table}"


def assert_table(conn: sqlite3.Connection, table: str) -> None:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchall()
    assert rows, f"Missing table {table}"


def run_checks() -> None:
    with open_db_connection(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row

        assert_table(conn, "schema_migrations")

        assert_table(conn, "jobs")
        assert_column(conn, "jobs", "bot_id")
        assert_column(conn, "jobs", "updated_at")

        assert_table(conn, "bot_run_map")
        assert_column(conn, "bot_run_map", "bot_id")
        assert_column(conn, "bot_run_map", "run_id")
        assert_column(conn, "bot_run_map", "created_at")

        # updated_at backfill should not be null
        rows = conn.execute("SELECT COUNT(1) AS cnt FROM jobs WHERE updated_at IS NULL OR updated_at = ''").fetchone()
        assert rows["cnt"] == 0, "jobs.updated_at not backfilled"

    print("Schema checks passed.")


if __name__ == "__main__":
    run_checks()
