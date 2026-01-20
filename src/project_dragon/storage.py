from __future__ import annotations

import base64
import hashlib
import time
import json
import math
import mimetypes
import os
import re
import threading
import requests
import uuid
import logging
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
import hashlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, TYPE_CHECKING, Tuple, Union, Iterator
from uuid import uuid4

from project_dragon.observability import profile_span

logger = logging.getLogger(__name__)

try:
    import psycopg  # type: ignore
except Exception:  # optional dependency
    psycopg = None

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
    psycopg2_extras = psycopg2.extras
except Exception:  # optional dependency
    psycopg2 = None
    psycopg2_extras = None

_DB_OPERATIONAL_ERRORS: tuple[type[BaseException], ...] = tuple(
    err
    for err in (
        getattr(psycopg, "OperationalError", None),
        getattr(psycopg2, "OperationalError", None),
    )
    if err is not None
 ) or (Exception,)
_DB_INTEGRITY_ERRORS: tuple[type[BaseException], ...] = tuple(
    err
    for err in (
        getattr(psycopg, "IntegrityError", None),
        getattr(psycopg2, "IntegrityError", None),
    )
    if err is not None
) or (Exception,)


class DbErrors:
    OperationalError = _DB_OPERATIONAL_ERRORS
    IntegrityError = _DB_INTEGRITY_ERRORS


def _rollback_quietly(conn: Any) -> None:
    try:
        conn.rollback()
    except Exception:
        pass

if TYPE_CHECKING:
    from project_dragon.domain import BacktestResult, Candle

_DB_INIT_LOCK = threading.Lock()
_DB_INITIALIZED_FOR: Optional[str] = None
_DB_INITIALIZED_VERSION: Optional[int] = None
_AUTO_MIGRATE_WARNED: bool = False
_SCHEMA_DDL_ALLOWED: bool = False


DEFAULT_USER_EMAIL = "admin@local"

# Sentinel used for optional update helpers where None means "set NULL".
_UNSET: object = object()


def get_database_url() -> Optional[str]:
    raw = (os.getenv("DRAGON_DATABASE_URL") or "").strip()
    return raw or None


def _truthy_env(var_name: str) -> bool:
    return (os.getenv(var_name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _auto_migrate_enabled() -> bool:
    global _AUTO_MIGRATE_WARNED
    env = (os.getenv("DRAGON_ENV") or "prod").strip().lower()
    auto = _truthy_env("DRAGON_AUTO_MIGRATE")
    if env == "prod" and auto:
        if not _AUTO_MIGRATE_WARNED:
            logger.warning("DRAGON_AUTO_MIGRATE ignored in prod")
            _AUTO_MIGRATE_WARNED = True
        return False
    return env == "dev" and auto


@contextmanager
def _allow_schema_ddl() -> Iterator[None]:
    global _SCHEMA_DDL_ALLOWED
    prev = _SCHEMA_DDL_ALLOWED
    _SCHEMA_DDL_ALLOWED = True
    try:
        yield
    finally:
        _SCHEMA_DDL_ALLOWED = prev


def _try_acquire_migration_lock(conn: Any) -> bool:
    try:
        row = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext(%s))",
            ("project_dragon_migrations",),
        ).fetchone()
        return bool(row[0]) if row else False
    except Exception:
        return False


def _release_migration_lock(conn: Any) -> None:
    try:
        conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("project_dragon_migrations",))
    except Exception:
        pass


def is_postgres_dsn(dsn: Optional[str]) -> bool:
    if not dsn:
        return False
    return dsn.startswith("postgres://") or dsn.startswith("postgresql://")


class PostgresConnectionAdapter:
    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._is_postgres = True

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        cur = self._conn.cursor()
        cur.execute(sql, params or None)
        return cur

    def executemany(self, sql: str, seq_params: Sequence[Sequence[Any]]):
        cur = self._conn.cursor()
        cur.executemany(sql, seq_params)
        return cur

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        _log_tx_state(self._conn, "tx_enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            try:
                self.commit()
            except Exception:
                pass
        try:
            self.close()
        except Exception:
            pass
        _log_tx_state(self._conn, "tx_exit")


class PostgresConnectionAdapterV3:
    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._is_postgres = True

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        cur = self._conn.cursor()
        cur.execute(sql, params or None)
        return cur

    def executemany(self, sql: str, seq_params: Sequence[Sequence[Any]]):
        cur = self._conn.cursor()
        cur.executemany(sql, seq_params)
        return cur

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    def __enter__(self):
        _log_tx_state(self._conn, "tx_enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.rollback()
        else:
            try:
                self.commit()
            except Exception:
                pass
        try:
            self.close()
        except Exception:
            pass
        _log_tx_state(self._conn, "tx_exit")


def is_postgres(conn: Any) -> bool:
    if conn is None:
        return False
    return bool(getattr(conn, "_is_postgres", False))


def _row_to_dict(cursor: Any, row: Any) -> Dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    if isinstance(row, Mapping):
        return dict(row)
    try:
        desc = cursor.description or []
    except Exception:
        desc = []
    if not desc:
        return {"value": row}
    keys = [d[0] for d in desc]
    if isinstance(row, (list, tuple)):
        return {k: row[idx] if idx < len(row) else None for idx, k in enumerate(keys)}
    return {keys[0]: row}


def fetchone_dict(cursor: Any) -> Optional[Dict[str, Any]]:
    row = cursor.fetchone()
    if row is None:
        return None
    return _row_to_dict(cursor, row)


def fetchall_dicts(cursor: Any) -> List[Dict[str, Any]]:
    rows = cursor.fetchall()
    if not rows:
        return []
    return [_row_to_dict(cursor, row) for row in rows]


def execute_fetchone(conn: Any, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Dict[str, Any]]:
    cur = db_execute(conn, sql, params)
    return fetchone_dict(cur)


def execute_fetchall(conn: Any, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
    cur = db_execute(conn, sql, params)
    return fetchall_dicts(cur)


def _raw_conn_execute(raw_conn: Any, sql: str) -> None:
    try:
        if hasattr(raw_conn, "execute"):
            raw_conn.execute(sql)
            return
    except Exception:
        raise
    cur = raw_conn.cursor()
    cur.execute(sql)


def _configure_session_timeouts(raw_conn: Any) -> None:
    try:
        _raw_conn_execute(raw_conn, "SET statement_timeout = '30s'")
        _raw_conn_execute(raw_conn, "SET idle_in_transaction_session_timeout = '30s'")
    except Exception:
        pass


def _configure_read_only(raw_conn: Any) -> None:
    try:
        _raw_conn_execute(raw_conn, "SET default_transaction_read_only = on")
    except Exception:
        pass


def _tx_status_label(raw_conn: Any) -> str:
    try:
        if psycopg is not None and isinstance(raw_conn, getattr(psycopg, "Connection", object)):
            status = getattr(raw_conn, "info", None)
            if status is not None:
                return str(getattr(status, "transaction_status", "unknown"))
    except Exception:
        pass
    try:
        if psycopg2 is not None:
            status = raw_conn.get_transaction_status()
            return str(status)
    except Exception:
        pass
    return "unknown"


def _log_tx_state(raw_conn: Any, event: str) -> None:
    if (os.environ.get("DRAGON_DB_TX_DEBUG") or os.environ.get("DRAGON_DEBUG") or "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    try:
        logger.debug("db_tx.%s status=%s", event, _tx_status_label(raw_conn))
    except Exception:
        pass


def open_postgres_connection(dsn: str) -> Any:
    if psycopg is not None:
        conn = psycopg.connect(str(dsn))
        try:
            conn.autocommit = False
        except Exception:
            pass
        try:
            _configure_session_timeouts(conn)
        except Exception:
            pass
        return conn
    if psycopg2 is not None:
        conn = psycopg2.connect(str(dsn))
        try:
            conn.autocommit = False
        except Exception:
            pass
        try:
            _configure_session_timeouts(conn)
        except Exception:
            pass
        return conn
    raise RuntimeError(
        "Postgres support requires 'psycopg' (recommended) or 'psycopg2'. Install one of:\n"
        "pip install psycopg[binary]\n"
        "pip install psycopg2-binary"
    )


def sql_placeholder(conn: Any) -> str:
    return "%s"


def sql_placeholders(conn: Any, count: int) -> str:
    total = max(0, int(count))
    if total <= 0:
        return ""
    return ",".join([sql_placeholder(conn)] * total)


def db_execute(conn: Any, sql: str, params: Optional[Sequence[Any]] = None):
    try:
        if params is None:
            return conn.execute(sql)
        return conn.execute(sql, params)
    except Exception:
        _rollback_quietly(conn)
        raise


def execute_optional(conn: Any, sql: str, params: Optional[Sequence[Any]] = None) -> None:
    sp = f"sp_{uuid.uuid4().hex[:8]}"
    try:
        conn.execute(f"SAVEPOINT {sp}")
        if params is None:
            conn.execute(sql)
        else:
            conn.execute(sql, params)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass


def execute_optional_logged(
    conn: Any,
    sql: str,
    params: Optional[Sequence[Any]] = None,
    *,
    label: str,
) -> None:
    sp = f"sp_{uuid.uuid4().hex[:8]}"
    try:
        conn.execute(f"SAVEPOINT {sp}")
        if params is None:
            conn.execute(sql)
        else:
            conn.execute(sql, params)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception as exc:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        print(f"{label}: {exc}")


def get_table_columns(conn: Any, table: str) -> set[str]:
    table_name = str(table or "").strip()
    if not table_name:
        return set()
    try:
        rows = conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        ).fetchall()
        return {str(r[0]) for r in rows or [] if r and r[0] is not None}
    except Exception:
        return set()


def _table_exists(conn: Any, table: str) -> bool:
    name = str(table or "").strip()
    if not name:
        return False
    try:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _backfill_jsonb_column(
    conn: Any,
    *,
    table: str,
    pk: str,
    text_col: str,
    jsonb_col: str,
    batch_size: int = 5000,
) -> None:
    tbl = str(table or "").strip()
    if not tbl:
        return
    while True:
        rows = execute_fetchall(
            conn,
            f"""
            SELECT {pk} AS pk, {text_col} AS txt
            FROM {tbl}
            WHERE {jsonb_col} IS NULL
              AND {text_col} IS NOT NULL
              AND TRIM({text_col}) != ''
            LIMIT {int(batch_size)}
            """,
        )
        if not rows:
            break
        updates: list[tuple[Any, Any]] = []
        failures = 0
        for r in rows:
            raw = r.get("txt")
            obj: Any
            try:
                obj = json.loads(raw) if isinstance(raw, str) else raw
                if obj is None:
                    obj = {}
            except Exception:
                obj = {}
                failures += 1
            updates.append((to_jsonb(obj), r.get("pk")))
        conn.executemany(
            f"UPDATE {tbl} SET {jsonb_col} = %s WHERE {pk} = %s",
            updates,
        )
        if failures:
            print(f"jsonb_backfill_parse_failed table={tbl} column={jsonb_col} count={failures}")


def _auto_pk(conn: Any) -> str:
    return "SERIAL PRIMARY KEY"


def _run_key_expr(conn: Any) -> str:
    return "metadata_jsonb ->> 'run_key'"


def _get_or_create_user_insert_sql(backend: str) -> str:
    return "INSERT INTO users(id, email, created_at) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING"


def insert_ignore(conn: Any, table: str, columns: Sequence[str], values: Sequence[Any]) -> Any:
    cols_sql = ", ".join(columns)
    placeholders = sql_placeholders(conn, len(columns))
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    return conn.execute(sql, tuple(values))


def upsert_backtest_run_details(conn: Any, columns: Sequence[str], values: Sequence[Any]) -> Any:
    cols_sql = ", ".join(columns)
    placeholders = sql_placeholders(conn, len(columns))
    update_cols = [c for c in columns if c != "run_id"]
    updates = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    sql = (
        f"INSERT INTO backtest_run_details ({cols_sql}) VALUES ({placeholders}) "
        f"ON CONFLICT (run_id) DO UPDATE SET {updates}"
    )
    return conn.execute(sql, tuple(values))


def _ensure_schema_migrations(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at TEXT NOT NULL
        )
        """
    )


def _migration_0001_core_schema(conn: Any) -> None:
    """Initial schema + additive upgrades.

    This migration is intentionally idempotent (CREATE IF NOT EXISTS + safe ALTERs).
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_runs (
            id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            metadata_json TEXT DEFAULT '{}',
            start_time TEXT,
            end_time TEXT,
            net_profit REAL,
            net_return_pct REAL,
            max_drawdown_pct DOUBLE PRECISION,
            sharpe REAL,
            sortino REAL,
            win_rate REAL,
            profit_factor REAL,
            cpc_index REAL,
            common_sense_ratio REAL,
            trades_json TEXT,
            sweep_id TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS jobs (
            id {_auto_pk(conn)},
            job_type TEXT NOT NULL,
            job_key TEXT,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            sweep_id TEXT,
            run_id TEXT,
            bot_id INTEGER,
            progress REAL DEFAULT 0.0,
            message TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            error_text TEXT DEFAULT ''
        )
        """
    )
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bots (
            id {_auto_pk(conn)},
            name TEXT,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            status TEXT,
            desired_status TEXT,
            config_json TEXT NOT NULL,
            heartbeat_at TEXT,
            heartbeat_msg TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_run_map (
            bot_id INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_id, run_id)
        )
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_run_map_bot_id_unique ON bot_run_map(bot_id)")

    # Ensure additive columns / indices exist.
    _ensure_metadata_column(conn)
    _ensure_schema(conn)

    # Useful indexes for list views and common filters.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_at ON backtest_runs(created_at)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_tf_created ON backtest_runs(symbol, timeframe, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_created_at ON backtest_runs(sweep_id, created_at)"
    )
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_sweeps_created_at ON sweeps(created_at)")

    # Optional multi-user performance index for sweeps list.
    # (Safe even if the column doesn't exist yet; `_ensure_schema` will add it.)
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_sweeps_user_created ON sweeps(user_id, created_at)")


def _migration_0002_backtest_run_details(conn: Any) -> None:
    """Create backtest_run_details table for large JSON blobs."""

    # Base table (older DBs may already have this with fewer columns).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_run_details (
            run_id TEXT PRIMARY KEY,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            trades_json TEXT,
            metadata_json TEXT
        )
        """
    )

    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "metadata_json",
        "ALTER TABLE backtest_run_details ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}' ",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS metadata_json TEXT NOT NULL DEFAULT '{}'",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "created_at",
        "ALTER TABLE backtest_run_details ADD COLUMN created_at TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS created_at TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "updated_at",
        "ALTER TABLE backtest_run_details ADD COLUMN updated_at TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS updated_at TEXT",
    )

    # Chart artifacts (avoid re-running backtests just to render charts)
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "equity_curve_json",
        "ALTER TABLE backtest_run_details ADD COLUMN equity_curve_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS equity_curve_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "equity_timestamps_json",
        "ALTER TABLE backtest_run_details ADD COLUMN equity_timestamps_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS equity_timestamps_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "extra_series_json",
        "ALTER TABLE backtest_run_details ADD COLUMN extra_series_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS extra_series_json TEXT",
    )

    # Candle + run-context artifacts (render + analysis without network/re-run)
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "candles_json",
        "ALTER TABLE backtest_run_details ADD COLUMN candles_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS candles_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "params_json",
        "ALTER TABLE backtest_run_details ADD COLUMN params_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS params_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "run_context_json",
        "ALTER TABLE backtest_run_details ADD COLUMN run_context_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS run_context_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_run_details",
        "computed_metrics_json",
        "ALTER TABLE backtest_run_details ADD COLUMN computed_metrics_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS computed_metrics_json TEXT",
    )

    cols = get_table_columns(conn, "backtest_run_details")
    if "updated_at" in cols:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_run_details_updated_at ON backtest_run_details(updated_at)"
        )


def _migration_0003_exchange_state_snapshots(conn: Any) -> None:
    # bot_state_snapshots additions
    try:
        cols = get_table_columns(conn, "bot_state_snapshots")
    except Exception:
        cols = set()
    _db_add_column_if_missing(
        conn,
        "bot_state_snapshots",
        "exchange_state",
        "ALTER TABLE bot_state_snapshots ADD COLUMN exchange_state TEXT",
        "ALTER TABLE bot_state_snapshots ADD COLUMN IF NOT EXISTS exchange_state TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "bot_state_snapshots",
        "last_exchange_error_at",
        "ALTER TABLE bot_state_snapshots ADD COLUMN last_exchange_error_at TEXT",
        "ALTER TABLE bot_state_snapshots ADD COLUMN IF NOT EXISTS last_exchange_error_at TEXT",
    )

    # account_state_snapshots additions
    try:
        cols2 = get_table_columns(conn, "account_state_snapshots")
    except Exception:
        cols2 = set()
    _db_add_column_if_missing(
        conn,
        "account_state_snapshots",
        "exchange_state",
        "ALTER TABLE account_state_snapshots ADD COLUMN exchange_state TEXT",
        "ALTER TABLE account_state_snapshots ADD COLUMN IF NOT EXISTS exchange_state TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "account_state_snapshots",
        "last_exchange_error_at",
        "ALTER TABLE account_state_snapshots ADD COLUMN last_exchange_error_at TEXT",
        "ALTER TABLE account_state_snapshots ADD COLUMN IF NOT EXISTS last_exchange_error_at TEXT",
    )


def _migration_0004_backtest_run_details_artifacts(conn: Any) -> None:
    """Ensure `backtest_run_details` has artifact columns.

        Rationale:
        - Earlier DBs may have applied migration v2 before artifact columns existed.
        - This migration is additive and idempotent.
    """

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_run_details (
            run_id TEXT PRIMARY KEY,
            config_json TEXT NOT NULL,
            metrics_json TEXT NOT NULL,
            trades_json TEXT,
            metadata_json TEXT
        )
        """
    )

    def _add_col(name: str, ddl: str, ddl_pg: str) -> None:
        _db_add_column_if_missing(conn, "backtest_run_details", name, ddl, ddl_pg)

    _add_col(
        "metadata_json",
        "ALTER TABLE backtest_run_details ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}' ",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS metadata_json TEXT NOT NULL DEFAULT '{}'",
    )
    _add_col(
        "created_at",
        "ALTER TABLE backtest_run_details ADD COLUMN created_at TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS created_at TEXT",
    )
    _add_col(
        "updated_at",
        "ALTER TABLE backtest_run_details ADD COLUMN updated_at TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS updated_at TEXT",
    )

    # Chart artifacts
    _add_col(
        "equity_curve_json",
        "ALTER TABLE backtest_run_details ADD COLUMN equity_curve_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS equity_curve_json TEXT",
    )
    _add_col(
        "equity_timestamps_json",
        "ALTER TABLE backtest_run_details ADD COLUMN equity_timestamps_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS equity_timestamps_json TEXT",
    )
    _add_col(
        "extra_series_json",
        "ALTER TABLE backtest_run_details ADD COLUMN extra_series_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS extra_series_json TEXT",
    )

    # Candle + run-context artifacts
    _add_col(
        "candles_json",
        "ALTER TABLE backtest_run_details ADD COLUMN candles_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS candles_json TEXT",
    )
    _add_col(
        "params_json",
        "ALTER TABLE backtest_run_details ADD COLUMN params_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS params_json TEXT",
    )
    _add_col(
        "run_context_json",
        "ALTER TABLE backtest_run_details ADD COLUMN run_context_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS run_context_json TEXT",
    )
    _add_col(
        "computed_metrics_json",
        "ALTER TABLE backtest_run_details ADD COLUMN computed_metrics_json TEXT",
        "ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS computed_metrics_json TEXT",
    )

    try:
        cols = get_table_columns(conn, "backtest_run_details")
    except Exception:
        cols = set()
    if "updated_at" in cols:
        execute_optional(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_backtest_run_details_updated_at ON backtest_run_details(updated_at)",
        )


def _migration_0005_jsonb_columns(conn: Any) -> None:
    """Add JSONB columns for JSON blobs and backfill from legacy TEXT columns."""

    if _table_exists(conn, "backtest_runs"):
        for name in ("config_jsonb", "metrics_jsonb", "metadata_jsonb", "trades_jsonb"):
            _db_add_column_if_missing(
                conn,
                "backtest_runs",
                name,
                f"ALTER TABLE backtest_runs ADD COLUMN {name} JSONB",
                f"ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS {name} JSONB",
            )
        _backfill_jsonb_column(
            conn,
            table="backtest_runs",
            pk="id",
            text_col="config_json",
            jsonb_col="config_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_runs",
            pk="id",
            text_col="metrics_json",
            jsonb_col="metrics_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_runs",
            pk="id",
            text_col="metadata_json",
            jsonb_col="metadata_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_runs",
            pk="id",
            text_col="trades_json",
            jsonb_col="trades_jsonb",
        )

        execute_optional(conn, "DROP INDEX IF EXISTS idx_backtest_runs_run_key")
        execute_optional_logged(
            conn,
            "CREATE INDEX IF NOT EXISTS idx_backtest_runs_run_key "
            "ON backtest_runs ((metadata_jsonb ->> 'run_key'))",
            label="migration_index_create_failed",
        )

    if _table_exists(conn, "backtest_run_details"):
        for name in (
            "config_jsonb",
            "metrics_jsonb",
            "metadata_jsonb",
            "trades_jsonb",
            "candles_jsonb",
            "params_jsonb",
            "run_context_jsonb",
            "computed_metrics_jsonb",
            "equity_curve_jsonb",
            "equity_timestamps_jsonb",
            "extra_series_jsonb",
        ):
            _db_add_column_if_missing(
                conn,
                "backtest_run_details",
                name,
                f"ALTER TABLE backtest_run_details ADD COLUMN {name} JSONB",
                f"ALTER TABLE backtest_run_details ADD COLUMN IF NOT EXISTS {name} JSONB",
            )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="config_json",
            jsonb_col="config_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="metrics_json",
            jsonb_col="metrics_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="metadata_json",
            jsonb_col="metadata_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="trades_json",
            jsonb_col="trades_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="candles_json",
            jsonb_col="candles_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="params_json",
            jsonb_col="params_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="run_context_json",
            jsonb_col="run_context_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="computed_metrics_json",
            jsonb_col="computed_metrics_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="equity_curve_json",
            jsonb_col="equity_curve_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="equity_timestamps_json",
            jsonb_col="equity_timestamps_jsonb",
        )
        _backfill_jsonb_column(
            conn,
            table="backtest_run_details",
            pk="run_id",
            text_col="extra_series_json",
            jsonb_col="extra_series_jsonb",
        )

    if _table_exists(conn, "bot_events"):
        _db_add_column_if_missing(
            conn,
            "bot_events",
            "json_payload_jsonb",
            "ALTER TABLE bot_events ADD COLUMN json_payload_jsonb JSONB",
            "ALTER TABLE bot_events ADD COLUMN IF NOT EXISTS json_payload_jsonb JSONB",
        )
        _backfill_jsonb_column(
            conn,
            table="bot_events",
            pk="id",
            text_col="json_payload",
            jsonb_col="json_payload_jsonb",
        )

    if _table_exists(conn, "bot_state_snapshots"):
        for name, text_col in (
            ("health_jsonb", "health_json"),
            ("positions_summary_jsonb", "positions_summary_json"),
            ("exchange_state_jsonb", "exchange_state"),
        ):
            _db_add_column_if_missing(
                conn,
                "bot_state_snapshots",
                name,
                f"ALTER TABLE bot_state_snapshots ADD COLUMN {name} JSONB",
                f"ALTER TABLE bot_state_snapshots ADD COLUMN IF NOT EXISTS {name} JSONB",
            )
            _backfill_jsonb_column(
                conn,
                table="bot_state_snapshots",
                pk="bot_id",
                text_col=text_col,
                jsonb_col=name,
            )

    if _table_exists(conn, "account_state_snapshots"):
        for name, text_col in (
            ("positions_summary_jsonb", "positions_summary_json"),
            ("exchange_state_jsonb", "exchange_state"),
        ):
            _db_add_column_if_missing(
                conn,
                "account_state_snapshots",
                name,
                f"ALTER TABLE account_state_snapshots ADD COLUMN {name} JSONB",
                f"ALTER TABLE account_state_snapshots ADD COLUMN IF NOT EXISTS {name} JSONB",
            )
            _backfill_jsonb_column(
                conn,
                table="account_state_snapshots",
                pk="account_id",
                text_col=text_col,
                jsonb_col=name,
            )


def _migration_0006_candles_cache_timestamp_bigint(conn: Any) -> None:
    if not _table_exists(conn, "candles_cache"):
        return
    try:
        conn.execute("ALTER TABLE candles_cache ALTER COLUMN timestamp_ms TYPE BIGINT")
    except Exception:
        pass


def _migration_0007_backtest_runs_indexes(conn: Any) -> None:
    if not _table_exists(conn, "backtest_runs"):
        return
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_backtest_runs_created_at ON backtest_runs(created_at DESC)")
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol_tf_created ON backtest_runs(symbol, timeframe, created_at DESC)",
    )
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy_created ON backtest_runs(strategy_name, created_at DESC)",
    )
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_created ON backtest_runs(sweep_id, created_at DESC)",
    )
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_id ON backtest_runs(sweep_id)",
    )
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_backtest_runs_net_profit ON backtest_runs(net_profit)")
    # Expression index for run_key (jsonb)
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_run_key ON backtest_runs ((metadata_jsonb->>'run_key'))",
    )


def _migration_0008_jobs_indexes(conn: Any) -> None:
    if not _table_exists(conn, "jobs"):
        return
    execute_optional(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_jobs_sweep_type_status ON jobs(sweep_id, job_type, status)",
    )


def _migration_0009_sweeps_indexes(conn: Any) -> None:
    if not _table_exists(conn, "sweeps"):
        return
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_sweeps_created_at ON sweeps(created_at)")
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_sweeps_user_created ON sweeps(user_id, created_at)")


def _migration_0010_assets_indexes(conn: Any) -> None:
    if not _table_exists(conn, "assets"):
        return
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_assets_updated_at ON assets(updated_at)")
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_assets_symbol ON assets(symbol)")
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_assets_base_asset ON assets(base_asset)")


_MIGRATIONS: list[tuple[int, str, Any]] = [
    (1, "core_schema", _migration_0001_core_schema),
    (2, "backtest_run_details", _migration_0002_backtest_run_details),
    (3, "exchange_state_snapshots", _migration_0003_exchange_state_snapshots),
    (4, "backtest_run_details_artifacts", _migration_0004_backtest_run_details_artifacts),
    (5, "jsonb_columns", _migration_0005_jsonb_columns),
    (6, "candles_cache_timestamp_bigint", _migration_0006_candles_cache_timestamp_bigint),
    (7, "backtest_runs_indexes", _migration_0007_backtest_runs_indexes),
    (8, "jobs_indexes", _migration_0008_jobs_indexes),
    (9, "sweeps_indexes", _migration_0009_sweeps_indexes),
    (10, "assets_indexes", _migration_0010_assets_indexes),
]


def apply_migrations(conn: Any, *, use_lock: bool = True) -> None:
    """Apply any pending schema migrations."""
    def _tx_context() -> Any:
        tx = getattr(conn, "transaction", None)
        if callable(tx):
            return tx()

        @contextmanager
        def _fallback() -> Iterator[None]:
            try:
                yield
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        return _fallback()

    _ensure_schema_migrations(conn)
    if use_lock:
        conn.execute("SELECT pg_advisory_lock(hashtext(%s))", ("project_dragon_migrations",))
    try:
        with _allow_schema_ddl():
            applied = {
                int(r[0])
                for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
            }
            for version, name, fn in _MIGRATIONS:
                if int(version) in applied:
                    continue
                with _tx_context():
                    fn(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations(version, name, applied_at) VALUES (%s, %s, %s)",
                        (int(version), str(name), now_utc_iso()),
                    )
            try:
                row = conn.execute(
                    "SELECT COUNT(1) AS c, MAX(version) AS max_v FROM schema_migrations"
                ).fetchone()
            except Exception as exc:
                raise RuntimeError("schema_migrations check failed") from exc
            if not row:
                raise RuntimeError("schema_migrations check failed: no rows")
            try:
                count = int(row[0] or 0)
                max_v = int(row[1] or 0)
            except Exception as exc:
                raise RuntimeError("schema_migrations check failed: unreadable rows") from exc
            expected_max = max(v for v, _name, _fn in _MIGRATIONS)
            if count < len(_MIGRATIONS) or max_v < expected_max:
                raise RuntimeError(
                    f"schema_migrations check failed: count={count} max={max_v} expected_max={expected_max}"
                )
    finally:
        if use_lock:
            conn.execute("SELECT pg_advisory_unlock(hashtext(%s))", ("project_dragon_migrations",))


def open_db_connection(*_args: Any, read_only: bool = False, **_kwargs: Any) -> Any:
    """Open a configured Postgres connection for Project Dragon."""

    dsn = get_database_url()
    if not is_postgres_dsn(dsn):
        raise RuntimeError(
            "DRAGON_DATABASE_URL must be set to a postgres:// or postgresql:// URL."
        )
    _init_postgres_db(str(dsn))
    raw_conn = open_postgres_connection(str(dsn))
    try:
        _configure_session_timeouts(raw_conn)
        if read_only:
            try:
                raw_conn.autocommit = True
            except Exception:
                pass
            _configure_read_only(raw_conn)
    except Exception:
        pass
    if psycopg is not None and isinstance(raw_conn, getattr(psycopg, "Connection", object)):
        return PostgresConnectionAdapterV3(raw_conn)
    return PostgresConnectionAdapter(raw_conn)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _file_bytes_to_data_uri(file_bytes: bytes, filename: str) -> str:
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = "image/svg+xml" if filename.lower().endswith(".svg") else "image/png"
    b64 = base64.b64encode(file_bytes).decode("ascii")
    return f"data:{mime_type};base64,{b64}"


def _parse_exchange_symbol_assets(exchange_symbol: str) -> Tuple[Optional[str], Optional[str]]:
    """Best-effort parse base/quote from common symbol formats.

    This is intentionally heuristic; it is used only for icon lookup and metadata display.
    """

    s = (exchange_symbol or "").strip()
    if not s:
        return None, None

    # Common ccxt format: BTC/USDT or BTC/USDT:USDT
    if "/" in s:
        left, right = s.split("/", 1)
        base = left.strip().upper() or None
        quote = right.split(":", 1)[0].strip().upper() or None
        return base, quote

    # Common WooX perps format: PERP_BTC_USDT
    m = re.match(r"^(?:PERP|SPOT)_(?P<base>[A-Z0-9]+)_(?P<quote>[A-Z0-9]+)$", s.strip().upper())
    if m:
        return (m.group("base") or "").strip() or None, (m.group("quote") or "").strip() or None

    # Simple dash format: BTC-USDT
    if "-" in s:
        parts = [p.strip().upper() for p in s.split("-") if p.strip()]
        if len(parts) >= 2:
            return parts[0], parts[1]

    return s.strip().upper(), None


def upsert_symbol(
    conn: Any,
    *,
    exchange_symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    market_type: Optional[str] = None,
    icon_uri: Optional[str] = None,
) -> None:
    exchange_symbol = (exchange_symbol or "").strip()
    if not exchange_symbol:
        return

    if base_asset is None or quote_asset is None:
        base_guess, quote_guess = _parse_exchange_symbol_assets(exchange_symbol)
        base_asset = base_asset or base_guess
        quote_asset = quote_asset or quote_guess

    market_type_norm = (str(market_type).strip().lower() if market_type is not None else None)
    if market_type_norm in {"perp", "perps", "futures"}:
        market_type_norm = "perps"
    elif market_type_norm == "spot":
        market_type_norm = "spot"
    elif market_type_norm is not None and not market_type_norm:
        market_type_norm = None

    updated_at = now_utc_iso()
    conn.execute(
        """
        INSERT INTO symbols (
            exchange_symbol, base_asset, quote_asset, market_type, icon_uri, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT(exchange_symbol) DO UPDATE SET
            base_asset = COALESCE(excluded.base_asset, symbols.base_asset),
            quote_asset = COALESCE(excluded.quote_asset, symbols.quote_asset),
            market_type = COALESCE(excluded.market_type, symbols.market_type),
            icon_uri = COALESCE(excluded.icon_uri, symbols.icon_uri),
            updated_at = excluded.updated_at
        """,
        (
            exchange_symbol,
            (base_asset.strip().upper() if isinstance(base_asset, str) and base_asset.strip() else None),
            (quote_asset.strip().upper() if isinstance(quote_asset, str) and quote_asset.strip() else None),
            market_type_norm,
            icon_uri,
            updated_at,
        ),
    )


def ensure_symbols_for_exchange_symbols(conn: Any, exchange_symbols: Iterable[str]) -> int:
    """Ensure a `symbols` row exists for each exchange_symbol (best-effort).

    Performance note:
    - Callers may invoke this on every UI rerun (e.g. Results grids).
    - Avoid rewriting existing rows (especially `updated_at`) since that turns a
      lightweight metadata check into thousands of writes.
    """

    # Normalize + de-dup while preserving order.
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in exchange_symbols:
        sym = str(raw or "").strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        normalized.append(sym)

    if not normalized:
        return 0

    # Fetch existing symbols in chunks to keep the IN clause bounded.
    existing: set[str] = set()
    chunk_size = 500
    try:
        for i in range(0, len(normalized), chunk_size):
            chunk = normalized[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            rows = fetchall_dicts(
                conn.execute(
                    f"SELECT exchange_symbol FROM symbols WHERE exchange_symbol IN ({placeholders})",
                    chunk,
                )
            )
            for r in rows:
                if r.get("exchange_symbol") is not None:
                    existing.add(str(r.get("exchange_symbol")))
    except Exception:
        # If the symbols table doesn't exist yet, fall back to best-effort upserts.
        existing = set()

    missing = [sym for sym in normalized if sym not in existing]
    if not missing:
        return 0

    n = 0
    for sym in missing:
        try:
            upsert_symbol(conn, exchange_symbol=sym)
            n += 1
        except Exception:
            continue
    return n


def sync_symbol_icons_from_manifest(
    conn: Any,
    *,
    manifest_path: str,
    icons_root: str,
    force: bool = False,
    max_updates: int = 500,
) -> int:
    """Sync local icon pack (manifest + files) into `symbols.icon_uri`.

    `manifest_path` must be a JSON object mapping asset ticker -> relative file path.
    Updates all rows matching `symbols.base_asset = <ticker>`.
    """
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(manifest_path)
    if not os.path.isdir(icons_root):
        raise FileNotFoundError(icons_root)

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    if not isinstance(manifest, dict):
        raise ValueError("Icon manifest must be a JSON object")

    updates = 0
    for raw_key, rel_path in manifest.items():
        if updates >= max_updates:
            break
        if not isinstance(raw_key, str) or not isinstance(rel_path, str):
            continue
        asset = raw_key.strip().upper()
        rel_path_norm = rel_path.strip().lstrip("/")
        if not asset or not rel_path_norm:
            continue
        full_path = os.path.join(icons_root, rel_path_norm)
        if not os.path.exists(full_path):
            continue

        # Skip if already set (unless force)
        if not force:
            row = execute_fetchone(
                conn,
                "SELECT 1 FROM symbols WHERE base_asset = %s AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                (asset,),
            )
            if row:
                continue

        with open(full_path, "rb") as f:
            icon_bytes = f.read()
        icon_uri = _file_bytes_to_data_uri(icon_bytes, filename=os.path.basename(full_path))

        cur = conn.execute(
            "UPDATE symbols SET icon_uri = %s, updated_at = %s WHERE base_asset = %s",
            (icon_uri, now_utc_iso(), asset),
        )
        if cur.rowcount:
            updates += 1

    return updates


def sync_symbol_icons_from_spothq_pack(
    conn: Any,
    *,
    icons_root: str,
    base_assets: Optional[Iterable[str]] = None,
    style: str = "color",
    size_preference: Optional[Iterable[str]] = None,
    force: bool = False,
    max_updates: int = 500,
) -> int:
    """Sync spothq/cryptocurrency-icons into `symbols.icon_uri`.

    This resolver does NOT require a manifest and does not perform network calls.
    It looks for files in the spothq layout:

    - <root>/svg/<style>/<ticker>.svg
    - <root>/<size>/<style>/<ticker>.png

    Rows are updated by `symbols.base_asset` (so multiple exchange_symbol rows for the
    same asset get the same icon).
    """

    from project_dragon.ui.crypto_icons import icon_bytes_to_data_uri, resolve_crypto_icon

    root = (icons_root or "").strip()
    if not root or not os.path.isdir(root):
        raise FileNotFoundError(root or icons_root)

    prefs = list(size_preference) if size_preference is not None else ["svg", "32", "128"]
    style_norm = (style or "color").strip().lower() or "color"

    assets: list[str] = []
    if base_assets is not None:
        for a in base_assets:
            s = (str(a or "").strip().upper())
            if s and s not in assets:
                assets.append(s)
    else:
        try:
            rows = execute_fetchall(
                conn,
                "SELECT DISTINCT base_asset FROM symbols WHERE base_asset IS NOT NULL AND TRIM(base_asset) != '' ORDER BY base_asset",
            )
            assets = [str(r.get("base_asset")).strip().upper() for r in rows or [] if str(r.get("base_asset") or "").strip()]
        except Exception:
            assets = []

    updates = 0
    for asset in assets:
        if updates >= max_updates:
            break

        # Skip if already set (unless force)
        if not force:
            row = execute_fetchone(
                conn,
                "SELECT 1 FROM symbols WHERE base_asset = %s AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                (asset,),
            )
            if row:
                continue

        resolved = resolve_crypto_icon(asset, size_preference=prefs, style=style_norm, icons_root=Path(root))
        if not resolved:
            continue
        mime, b = resolved
        icon_uri = icon_bytes_to_data_uri(mime, b)

        cur = conn.execute(
            "UPDATE symbols SET icon_uri = %s, updated_at = %s WHERE base_asset = %s",
            (icon_uri, now_utc_iso(), asset),
        )
        if cur.rowcount:
            updates += 1

    return updates


def get_current_user_email() -> str:
    """Resolve current user identity.

    Priority order:
    1) Reverse-proxy auth header (Streamlit): DRAGON_AUTH_HEADER (default X-Forwarded-User)
    2) Env var DRAGON_USER_EMAIL
    3) Fallback DEFAULT_USER_EMAIL

    Notes:
    - Header lookup is best-effort and safe to call outside Streamlit.
    - Value is normalized (strip + lower); empty falls back.
    """

    header_name = (os.environ.get("DRAGON_AUTH_HEADER") or "X-Forwarded-User").strip() or "X-Forwarded-User"

    def _extract_first_email(value: str) -> str:
        raw = (value or "").strip()
        if not raw:
            return ""
        # Handle comma-separated values by picking the first email-ish token.
        matches = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", raw, flags=re.IGNORECASE)
        if matches:
            return matches[0]
        first_part = raw.split(",", 1)[0].strip()
        if "<" in first_part and ">" in first_part:
            inner = first_part.split("<", 1)[1].split(">", 1)[0].strip()
            matches = re.findall(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", inner, flags=re.IGNORECASE)
            return matches[0] if matches else inner
        # As a final fallback, take the first whitespace token that looks like an email.
        for token in first_part.replace(";", " ").split():
            if "@" in token and "." in token:
                return token.strip("<>\"' ")
        return first_part

    email = ""
    try:
        import streamlit as st  # type: ignore
        try:
            from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx  # type: ignore
        except Exception:
            get_script_run_ctx = None

        headers = {}
        try:
            if get_script_run_ctx is not None and get_script_run_ctx() is not None:
                headers = st.context.headers  # type: ignore[attr-defined]
        except Exception:
            headers = {}
        if isinstance(headers, dict):
            v = headers.get(header_name) or headers.get(header_name.lower()) or ""
            email = _extract_first_email(str(v))
    except Exception:
        # Streamlit not available or headers unavailable.
        email = ""

    if not email:
        email = (os.environ.get("DRAGON_USER_EMAIL") or "").strip()
    email = (email or "").strip().lower()
    return email or DEFAULT_USER_EMAIL


def get_or_create_user_id(email: str, conn: Optional[Any] = None) -> str:
    """Ensure a user row exists and return a stable user_id.

    v0 uses the email itself as the user_id (TEXT) for migration safety.
    Accepts an optional open connection for callers that already have one.
    """

    resolved = (email or "").strip().lower() or DEFAULT_USER_EMAIL
    if conn is not None:
        return get_or_create_user(conn, resolved)
    with _get_conn() as c:
        return get_or_create_user(c, resolved)


def get_or_create_user(conn: Any, email: str) -> str:
    """Ensure a user row exists and return user_id.

    For v0 we use email itself as the stable user_id.
    """

    user_id = (email or "").strip() or DEFAULT_USER_EMAIL
    try:
        conn.execute(
            _get_or_create_user_insert_sql("postgres"),
            (user_id, user_id, now_utc_iso()),
        )
        conn.commit()
    except Exception:
        # Schema may not be migrated yet; caller is expected to run init_db().
        pass
    return user_id


def init_db(*_args: Any, **_kwargs: Any) -> str:
    dsn = get_database_url()
    if not is_postgres_dsn(dsn):
        raise RuntimeError(
            "DRAGON_DATABASE_URL must be set to a postgres:// or postgresql:// URL."
        )
    return _init_postgres_db(str(dsn))


def _init_postgres_db(dsn: str) -> str:
    global _DB_INITIALIZED_FOR
    global _DB_INITIALIZED_VERSION
    key = f"postgres:{dsn}"
    latest_version = 0
    try:
        latest_version = max(int(v) for (v, _name, _fn) in _MIGRATIONS)
    except Exception:
        latest_version = 0

    if not _auto_migrate_enabled():
        return dsn
    if (
        _DB_INITIALIZED_FOR is not None
        and _DB_INITIALIZED_FOR == key
        and _DB_INITIALIZED_VERSION is not None
        and int(_DB_INITIALIZED_VERSION) >= int(latest_version)
    ):
        return dsn

    with _DB_INIT_LOCK:
        if not _auto_migrate_enabled():
            return dsn
        if (
            _DB_INITIALIZED_FOR is not None
            and _DB_INITIALIZED_FOR == key
            and _DB_INITIALIZED_VERSION is not None
            and int(_DB_INITIALIZED_VERSION) >= int(latest_version)
        ):
            return dsn

        raw_conn = open_postgres_connection(str(dsn))
        if psycopg is not None and isinstance(raw_conn, getattr(psycopg, "Connection", object)):
            conn = PostgresConnectionAdapterV3(raw_conn)
        else:
            conn = PostgresConnectionAdapter(raw_conn)
        try:
            if not _try_acquire_migration_lock(conn):
                logger.debug("Auto-migrate skipped: lock unavailable")
                return dsn
            apply_migrations(conn, use_lock=False)
            _release_migration_lock(conn)
            try:
                row = conn.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
                _DB_INITIALIZED_VERSION = int(row[0] or 0) if row else 0
            except Exception:
                _DB_INITIALIZED_VERSION = latest_version
        finally:
            try:
                conn.close()
            except Exception:
                pass
        _DB_INITIALIZED_FOR = key
        return dsn


def _ensure_metadata_column(conn: Any) -> None:
    _db_add_column_if_missing(
        conn,
        "backtest_runs",
        "metadata_json",
        "ALTER TABLE backtest_runs ADD COLUMN metadata_json TEXT DEFAULT '{}' ",
        "ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS metadata_json TEXT DEFAULT '{}'",
    )
    _db_add_column_if_missing(
        conn,
        "backtest_runs",
        "metadata_jsonb",
        "ALTER TABLE backtest_runs ADD COLUMN metadata_jsonb JSONB",
        "ALTER TABLE backtest_runs ADD COLUMN IF NOT EXISTS metadata_jsonb JSONB",
    )


def _db_add_column_if_missing(
    conn: Any,
    table: str,
    col: str,
    ddl_base: str,
    ddl_postgres: str,
) -> None:
    try:
        conn.execute(ddl_postgres)
    except Exception as exc:
        try:
            if psycopg is not None:
                dup = getattr(psycopg.errors, "DuplicateColumn", None)
                if dup is not None and isinstance(exc, dup):
                    return
        except Exception:
            pass
        try:
            if psycopg2 is not None:
                dup2 = getattr(psycopg2.errors, "DuplicateColumn", None)
                if dup2 is not None and isinstance(exc, dup2):
                    return
        except Exception:
            pass
        raise


def _is_tx_read_only(conn: Any) -> bool:
    try:
        cur = conn.execute("SHOW transaction_read_only")
        row = cur.fetchone() if cur is not None else None
        if row is None:
            return False
        try:
            val = row[0]
        except Exception:
            val = row
        return str(val or "").strip().lower() in {"on", "true", "1"}
    except Exception:
        return False


def _ensure_schema(conn: Any) -> None:
    if not _auto_migrate_enabled() and not _SCHEMA_DDL_ALLOWED:
        return
    if _is_tx_read_only(conn):
        logger.debug("Skipping schema ensure for read-only transaction")
        return
    if not _try_acquire_migration_lock(conn):
        logger.debug("Skipping schema ensure: lock unavailable")
        return
    ph = sql_placeholder(conn)
    # Core identity + credentials tables.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )

    # Per-user settings (UI preferences, etc).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, key),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_settings_user ON user_settings(user_id)")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS exchange_credentials (
            id {_auto_pk(conn)},
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            label TEXT,
            api_key TEXT NOT NULL,
            api_secret_enc TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_used_at TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user
        ON exchange_credentials(user_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user_exchange
        ON exchange_credentials(user_id, exchange_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_exchange_credentials_user_id
        ON exchange_credentials(user_id, id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candles_cache (
            exchange_id TEXT NOT NULL,
            market_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            timestamp_ms BIGINT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (exchange_id, market_type, symbol, timeframe, timestamp_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_candles_cache_symbol_tf_ts
        ON candles_cache(exchange_id, symbol, timeframe, timestamp_ms)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sweeps (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            created_at TEXT NOT NULL,
            name TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            data_source TEXT NOT NULL,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            range_mode TEXT NOT NULL,
            range_params_json TEXT NOT NULL,
            base_config_json TEXT NOT NULL,
            sweep_definition_json TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT
        )
        """
    )

    # Older DBs may have sweeps without user_id; add it safely.
    _db_add_column_if_missing(
        conn,
        "sweeps",
        "user_id",
        "ALTER TABLE sweeps ADD COLUMN user_id TEXT",
        "ALTER TABLE sweeps ADD COLUMN IF NOT EXISTS user_id TEXT",
    )

    # Multi-asset sweep support (additive columns).
    _db_add_column_if_missing(
        conn,
        "sweeps",
        "sweep_scope",
        "ALTER TABLE sweeps ADD COLUMN sweep_scope TEXT NOT NULL DEFAULT 'single_asset'",
        "ALTER TABLE sweeps ADD COLUMN IF NOT EXISTS sweep_scope TEXT NOT NULL DEFAULT 'single_asset'",
    )
    _db_add_column_if_missing(
        conn,
        "sweeps",
        "sweep_assets_json",
        "ALTER TABLE sweeps ADD COLUMN sweep_assets_json TEXT",
        "ALTER TABLE sweeps ADD COLUMN IF NOT EXISTS sweep_assets_json TEXT",
    )
    _db_add_column_if_missing(
        conn,
        "sweeps",
        "sweep_category_id",
        "ALTER TABLE sweeps ADD COLUMN sweep_category_id INTEGER",
        "ALTER TABLE sweeps ADD COLUMN IF NOT EXISTS sweep_category_id INTEGER",
    )

    # Index for per-user sweeps list (safe even if column missing).
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_sweeps_user_created ON sweeps(user_id, created_at)")
    try:
        columns = get_table_columns(conn, "backtest_runs")
    except Exception:
        columns = set()
    added_avg_position_time_s = False
    desired_columns = {
        "sweep_id": "ALTER TABLE backtest_runs ADD COLUMN sweep_id TEXT",
        "market_type": "ALTER TABLE backtest_runs ADD COLUMN market_type TEXT",
        "start_time": "ALTER TABLE backtest_runs ADD COLUMN start_time TEXT",
        "end_time": "ALTER TABLE backtest_runs ADD COLUMN end_time TEXT",
        "net_profit": "ALTER TABLE backtest_runs ADD COLUMN net_profit REAL",
        "net_return_pct": "ALTER TABLE backtest_runs ADD COLUMN net_return_pct REAL",
        "roi_pct_on_margin": "ALTER TABLE backtest_runs ADD COLUMN roi_pct_on_margin REAL",
        "max_drawdown_pct": "ALTER TABLE backtest_runs ADD COLUMN max_drawdown_pct DOUBLE PRECISION",
        "sharpe": "ALTER TABLE backtest_runs ADD COLUMN sharpe REAL",
        "sortino": "ALTER TABLE backtest_runs ADD COLUMN sortino REAL",
        "win_rate": "ALTER TABLE backtest_runs ADD COLUMN win_rate REAL",
        "profit_factor": "ALTER TABLE backtest_runs ADD COLUMN profit_factor REAL",
        "cpc_index": "ALTER TABLE backtest_runs ADD COLUMN cpc_index REAL",
        "common_sense_ratio": "ALTER TABLE backtest_runs ADD COLUMN common_sense_ratio REAL",
        "trades_json": "ALTER TABLE backtest_runs ADD COLUMN trades_json TEXT",
        "avg_position_time_s": "ALTER TABLE backtest_runs ADD COLUMN avg_position_time_s REAL",
    }
    for name, ddl in desired_columns.items():
        if name not in columns:
            ddl_pg = ddl.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
            _db_add_column_if_missing(conn, "backtest_runs", name, ddl, ddl_pg)
            if name == "avg_position_time_s":
                added_avg_position_time_s = True

    # One-time best-effort backfill for avg_position_time_s from stored trades_json.
    # Runs only when the column is first added in this DB.
    if added_avg_position_time_s:
        try:
            import math
            from datetime import timezone

            def _parse_iso_dt(v: Any) -> Optional[datetime]:
                if v is None:
                    return None
                s = str(v).strip()
                if not s:
                    return None
                try:
                    if s.endswith("Z"):
                        s2 = s[:-1] + "+00:00"
                    else:
                        s2 = s
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    return None

            def _compute_avg_pos_time_s_from_trades(trades_obj: Any) -> Optional[float]:
                if not isinstance(trades_obj, list) or not trades_obj:
                    return None
                net_qty = 0.0
                open_ts: Optional[datetime] = None
                durs: list[float] = []
                for t in trades_obj:
                    if not isinstance(t, dict):
                        continue
                    ts = _parse_iso_dt(t.get("timestamp"))
                    if ts is None:
                        continue
                    try:
                        qty = float(t.get("size") or 0.0)
                    except Exception:
                        qty = 0.0
                    if qty <= 0:
                        continue
                    side = str(t.get("side") or "").strip().upper()
                    prev = net_qty
                    if side == "LONG":
                        net_qty += qty
                    else:
                        net_qty -= qty
                    if prev == 0.0 and net_qty != 0.0 and open_ts is None:
                        open_ts = ts
                    if prev != 0.0 and net_qty == 0.0 and open_ts is not None:
                        try:
                            dt_s = float((ts - open_ts).total_seconds())
                        except Exception:
                            dt_s = float("nan")
                        if math.isfinite(dt_s) and dt_s >= 0:
                            durs.append(dt_s)
                        open_ts = None
                if not durs:
                    return None
                try:
                    return float(sum(durs) / len(durs))
                except Exception:
                    return None

            rows = conn.execute(
                "SELECT id, trades_json FROM backtest_runs WHERE avg_position_time_s IS NULL AND trades_json IS NOT NULL AND TRIM(trades_json) != ''"
            ).fetchall()
            for r in rows or []:
                try:
                    run_id = str(r[0])
                    raw = r[1]
                    trades_obj = json.loads(raw) if isinstance(raw, str) and raw.strip() else None
                    avg_s = _compute_avg_pos_time_s_from_trades(trades_obj)
                    if avg_s is None:
                        continue
                    execute_optional(
                        conn,
                        f"UPDATE backtest_runs SET avg_position_time_s = {ph} WHERE id = {ph}",
                        (float(avg_s), run_id),
                    )
                except Exception:
                    continue
            conn.commit()
        except Exception:
            pass

    # Backfill market_type from existing stored config/metadata for older rows.
    # (Best-effort; no JSON1 dependency.)
    try:
        cols = get_table_columns(conn, "backtest_runs")
        if "market_type" in cols:
            rows = conn.execute(
                "SELECT id, market_type, metadata_json, config_json FROM backtest_runs WHERE market_type IS NULL OR TRIM(market_type) = ''"
            ).fetchall()
            for r in rows:
                try:
                    run_id = r[0]
                    meta_raw = r[2]
                    cfg_raw = r[3]
                    meta = json.loads(meta_raw) if isinstance(meta_raw, str) and meta_raw else {}
                    cfg = json.loads(cfg_raw) if isinstance(cfg_raw, str) and cfg_raw else {}
                except Exception:
                    meta = {}
                    cfg = {}
                    run_id = r[0]

                mkt = None
                if isinstance(meta, dict):
                    mkt = meta.get("market_type") or meta.get("market") or meta.get("market_type_hint")
                if not mkt and isinstance(cfg, dict):
                    ds = cfg.get("data_settings") if isinstance(cfg.get("data_settings"), dict) else None
                    if isinstance(ds, dict):
                        mkt = ds.get("market_type")
                    if not mkt:
                        mkt = cfg.get("market_type")
                mkt_norm = (str(mkt).strip().lower() if mkt is not None else "")
                if mkt_norm in {"perp", "perps", "futures"}:
                    mkt_norm = "perps"
                elif mkt_norm in {"spot"}:
                    mkt_norm = "spot"
                if mkt_norm:
                    execute_optional(
                        conn,
                        f"UPDATE backtest_runs SET market_type = {ph} WHERE id = {ph}",
                        (mkt_norm, str(run_id)),
                    )
            conn.commit()
    except Exception:
        pass

    # Index (safe even if column missing on very old DBs)
    execute_optional(conn, "CREATE INDEX IF NOT EXISTS idx_runs_roi_pct_on_margin ON backtest_runs(roi_pct_on_margin)")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_id ON backtest_runs(sweep_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_sweep_created_at ON backtest_runs(sweep_id, created_at)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_net_return_pct ON backtest_runs(net_return_pct)")

    # Deterministic backtest run idempotency key.
    execute_optional_logged(
        conn,
        "CREATE INDEX IF NOT EXISTS idx_backtest_runs_run_key "
        "ON backtest_runs ((metadata_jsonb ->> 'run_key'))",
        label="migration_index_create_failed",
    )

    execute_optional(
        conn,
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_run_map_bot_id_unique ON bot_run_map(bot_id)",
    )

    # Symbols metadata + cached icons (local icon pack, stored as data URIs)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbols (
            exchange_symbol TEXT PRIMARY KEY,
            base_asset TEXT NOT NULL,
            quote_asset TEXT,
            market_type TEXT,
            icon_uri TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_base_asset ON symbols(base_asset)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbols_market_type ON symbols(market_type)")

    # --- Assets + categories (per-user grouping) -----------------------
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS assets (
            id {_auto_pk(conn)},
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            base_asset TEXT,
            quote_asset TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(exchange_id, symbol)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_status ON assets(exchange_id, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_symbol ON assets(exchange_id, symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_exchange_base ON assets(exchange_id, base_asset)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS asset_categories (
            id {_auto_pk(conn)},
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            name TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'user',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, exchange_id, name)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_asset_categories_user_exchange ON asset_categories(user_id, exchange_id)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS asset_category_membership (
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (user_id, exchange_id, category_id, symbol)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_asset_cat_membership_cat ON asset_category_membership(user_id, exchange_id, category_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_asset_cat_membership_symbol ON asset_category_membership(user_id, exchange_id, symbol)"
    )

    # Best-effort backfill: seed assets from existing symbols table (or runs/bots) once.
    try:
        row = execute_fetchone(conn, "SELECT COUNT(*) AS c FROM assets")
        assets_count = int(row.get("c") if row is not None else 0)
    except Exception:
        assets_count = 0
    if assets_count <= 0:
        try:
            _seed_assets_from_existing_tables(conn)
        except Exception:
            pass

    # Per-user shortlists for runs explorer.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS run_shortlists (
            user_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            shortlisted INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (user_id, run_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_run_shortlists_user_updated ON run_shortlists(user_id, updated_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_bot_id ON jobs(bot_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at)")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bots (
            id {_auto_pk(conn)},
            name TEXT,
            exchange_id TEXT,
            symbol TEXT,
            timeframe TEXT,
            status TEXT,
            desired_status TEXT,
            config_json TEXT NOT NULL,
            heartbeat_at TEXT,
            heartbeat_msg TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    try:
        bot_columns = get_table_columns(conn, "bots")
    except Exception:
        bot_columns = set()
    bot_new_columns = {
        "user_id": "ALTER TABLE bots ADD COLUMN user_id TEXT",
        "credentials_id": "ALTER TABLE bots ADD COLUMN credentials_id INTEGER",
        "account_id": "ALTER TABLE bots ADD COLUMN account_id INTEGER",
        "realized_pnl": "ALTER TABLE bots ADD COLUMN realized_pnl REAL DEFAULT 0",
        "unrealized_pnl": "ALTER TABLE bots ADD COLUMN unrealized_pnl REAL DEFAULT 0",
        "fees_total": "ALTER TABLE bots ADD COLUMN fees_total REAL DEFAULT 0",
        "funding_total": "ALTER TABLE bots ADD COLUMN funding_total REAL DEFAULT 0",
        "roi_pct_on_margin": "ALTER TABLE bots ADD COLUMN roi_pct_on_margin REAL",
        "dca_fills_current": "ALTER TABLE bots ADD COLUMN dca_fills_current INTEGER DEFAULT 0",
        "position_json": "ALTER TABLE bots ADD COLUMN position_json TEXT DEFAULT '{}'",
        "positions_snapshot_json": "ALTER TABLE bots ADD COLUMN positions_snapshot_json TEXT DEFAULT '{}'",
        "positions_normalized_json": "ALTER TABLE bots ADD COLUMN positions_normalized_json TEXT",
        "open_orders_json": "ALTER TABLE bots ADD COLUMN open_orders_json TEXT DEFAULT '[]'",
        "mark_price": "ALTER TABLE bots ADD COLUMN mark_price REAL",
        "desired_action": "ALTER TABLE bots ADD COLUMN desired_action TEXT",
        "blocked_actions_count": "ALTER TABLE bots ADD COLUMN blocked_actions_count INTEGER DEFAULT 0",
    }
    for name, ddl in bot_new_columns.items():
        if name not in bot_columns:
            ddl_pg = ddl.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
            _db_add_column_if_missing(conn, "bots", name, ddl, ddl_pg)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user_id ON bots(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_credentials_id ON bots(credentials_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_user_account_id ON bots(user_id, account_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_status ON bots(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_desired_status ON bots(desired_status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_desired_action ON bots(desired_action)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_blocked_actions_count ON bots(blocked_actions_count)")
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bots_roi_pct_on_margin ON bots(roi_pct_on_margin)")
    except DbErrors.OperationalError:
        pass

    # Multi-account model (trading_accounts)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS trading_accounts (
            id {_auto_pk(conn)},
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            label TEXT NOT NULL,
            credential_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            risk_block_new_entries INTEGER DEFAULT 1,
            risk_max_daily_loss_usd REAL,
            risk_max_total_loss_usd REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(user_id, exchange_id, label)
        )
        """
    )
    # Additive account-risk columns for older DBs (idempotent)
    try:
        try:
            acct_cols = get_table_columns(conn, "trading_accounts")
        except Exception:
            acct_cols = set()
        acct_new_columns = {
            "risk_block_new_entries": "ALTER TABLE trading_accounts ADD COLUMN risk_block_new_entries INTEGER DEFAULT 1",
            "risk_max_daily_loss_usd": "ALTER TABLE trading_accounts ADD COLUMN risk_max_daily_loss_usd REAL",
            "risk_max_total_loss_usd": "ALTER TABLE trading_accounts ADD COLUMN risk_max_total_loss_usd REAL",
        }
        for name, ddl in acct_new_columns.items():
            if name not in acct_cols:
                ddl_pg = ddl.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
                _db_add_column_if_missing(conn, "trading_accounts", name, ddl, ddl_pg)
    except Exception:
        pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_user_exchange ON trading_accounts(user_id, exchange_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_credential_id ON trading_accounts(credential_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trading_accounts_status ON trading_accounts(status)")

    # Backfill bots.account_id for legacy bots that used bots.credentials_id.
    try:
        # Ensure bots.user_id is non-empty before backfill.
        db_execute(
            conn,
            "UPDATE bots SET user_id = COALESCE(user_id, %s) WHERE user_id IS NULL OR TRIM(user_id) = ''",
            (DEFAULT_USER_EMAIL,),
        )
        legacy_rows = db_execute(
            conn,
            """
            SELECT id,
                   COALESCE(NULLIF(TRIM(user_id), ''), %s) AS user_id,
                   COALESCE(NULLIF(TRIM(exchange_id), ''), 'woox') AS exchange_id,
                   credentials_id
            FROM bots
            WHERE (account_id IS NULL OR account_id = 0)
              AND credentials_id IS NOT NULL
            """,
            (DEFAULT_USER_EMAIL,),
        ).fetchall()
        now = now_utc_iso()
        for bot_id_val, uid_raw, ex_raw, cred_id_raw in legacy_rows:
            try:
                cred_id = int(cred_id_raw)
            except (TypeError, ValueError):
                continue
            if cred_id <= 0:
                continue
            uid = str(uid_raw or "").strip() or DEFAULT_USER_EMAIL
            ex = str(ex_raw or "woox").strip().lower() or "woox"
            # Prefer reusing any existing account for (user, exchange, credential).
            row = conn.execute(
                f"""
                SELECT id
                FROM trading_accounts
                WHERE user_id = {ph} AND exchange_id = {ph} AND credential_id = {ph}
                ORDER BY id ASC
                LIMIT 1
                """,
                (uid, ex, int(cred_id)),
            ).fetchone()
            if row:
                account_id = int(row[0])
            else:
                label = f"Legacy {ex.upper()} (cred #{cred_id})"
                insert_ignore(
                    conn,
                    "trading_accounts",
                    [
                        "user_id",
                        "exchange_id",
                        "label",
                        "credential_id",
                        "status",
                        "created_at",
                        "updated_at",
                    ],
                    (uid, ex, label, int(cred_id), "active", now, now),
                )
                row2 = conn.execute(
                    f"""
                    SELECT id
                    FROM trading_accounts
                    WHERE user_id = {ph} AND exchange_id = {ph} AND credential_id = {ph}
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (uid, ex, int(cred_id)),
                ).fetchone()
                if not row2:
                    continue
                account_id = int(row2[0])

            conn.execute(
                f"UPDATE bots SET account_id = {ph} WHERE id = {ph} AND (account_id IS NULL OR account_id = 0)",
                (int(account_id), int(bot_id_val)),
            )
        conn.commit()
    except DbErrors.OperationalError:
        # Older DBs may not have the columns yet; keep schema init idempotent.
        pass

    # Backfill null blocked_actions_count values
    try:
        conn.execute(
            "UPDATE bots SET blocked_actions_count = COALESCE(blocked_actions_count, 0) WHERE blocked_actions_count IS NULL"
        )
    except DbErrors.OperationalError:
        pass

    # Backfill user_id for legacy bots.
    try:
        conn.execute(
            f"UPDATE bots SET user_id = COALESCE(user_id, {ph}) WHERE user_id IS NULL OR TRIM(user_id) = ''",
            (DEFAULT_USER_EMAIL,),
        )
    except DbErrors.OperationalError:
        pass

    # Ensure default user exists.
    try:
        get_or_create_user(conn, DEFAULT_USER_EMAIL)
    except Exception:
        pass
    try:
        job_columns = get_table_columns(conn, "jobs")
    except Exception:
        job_columns = set()
    for name, ddl in {
        "job_key": "ALTER TABLE jobs ADD COLUMN job_key TEXT",
        "sweep_id": "ALTER TABLE jobs ADD COLUMN sweep_id TEXT",
        "run_id": "ALTER TABLE jobs ADD COLUMN run_id TEXT",
        "bot_id": "ALTER TABLE jobs ADD COLUMN bot_id INTEGER",
        "worker_id": "ALTER TABLE jobs ADD COLUMN worker_id TEXT",
    }.items():
        if name not in job_columns:
            ddl_pg = ddl.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
            _db_add_column_if_missing(conn, "jobs", name, ddl, ddl_pg)

    # Parent/group linkage for sweep planning (additive / ALTER-only).
    for name, ddl in {
        "parent_job_id": "ALTER TABLE jobs ADD COLUMN parent_job_id INTEGER",
        "group_key": "ALTER TABLE jobs ADD COLUMN group_key TEXT",
        "priority": "ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 100",
        "is_interactive": "ALTER TABLE jobs ADD COLUMN is_interactive INTEGER NOT NULL DEFAULT 0",
        "pause_requested": "ALTER TABLE jobs ADD COLUMN pause_requested INTEGER NOT NULL DEFAULT 0",
        "cancel_requested": "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0",
        "paused_at": "ALTER TABLE jobs ADD COLUMN paused_at TEXT",
        "cancelled_at": "ALTER TABLE jobs ADD COLUMN cancelled_at TEXT",
        "claimed_by": "ALTER TABLE jobs ADD COLUMN claimed_by TEXT",
        "claimed_at": "ALTER TABLE jobs ADD COLUMN claimed_at TEXT",
        "lease_expires_at": "ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT",
        "last_lease_renew_at": "ALTER TABLE jobs ADD COLUMN last_lease_renew_at TEXT",
        "lease_version": "ALTER TABLE jobs ADD COLUMN lease_version INTEGER DEFAULT 0",
        "stale_reclaims": "ALTER TABLE jobs ADD COLUMN stale_reclaims INTEGER DEFAULT 0",
        "updated_at": "ALTER TABLE jobs ADD COLUMN updated_at TEXT",
    }.items():
        if name not in job_columns:
            ddl_pg = ddl.replace("ADD COLUMN", "ADD COLUMN IF NOT EXISTS")
            _db_add_column_if_missing(conn, "jobs", name, ddl, ddl_pg)
    # Backfill updated_at where missing
    try:
        conn.execute("UPDATE jobs SET updated_at = COALESCE(updated_at, created_at) WHERE updated_at IS NULL OR updated_at = ''")
    except DbErrors.OperationalError:
        pass

    # Backfill lease_version/stale_reclaims NULLs for legacy rows.
    try:
        conn.execute(
            "UPDATE jobs SET lease_version = COALESCE(lease_version, 0) WHERE lease_version IS NULL"
        )
    except DbErrors.OperationalError:
        pass
    try:
        conn.execute(
            "UPDATE jobs SET stale_reclaims = COALESCE(stale_reclaims, 0) WHERE stale_reclaims IS NULL"
        )
    except DbErrors.OperationalError:
        pass

    # Backfill pause/cancel flags NULLs for legacy rows.
    try:
        conn.execute(
            "UPDATE jobs SET pause_requested = COALESCE(pause_requested, 0) WHERE pause_requested IS NULL"
        )
    except DbErrors.OperationalError:
        pass
    try:
        conn.execute(
            "UPDATE jobs SET cancel_requested = COALESCE(cancel_requested, 0) WHERE cancel_requested IS NULL"
        )
    except DbErrors.OperationalError:
        pass

    # Backfill scheduling fields NULLs for legacy rows.
    try:
        conn.execute("UPDATE jobs SET priority = COALESCE(priority, 100) WHERE priority IS NULL")
    except DbErrors.OperationalError:
        pass
    try:
        conn.execute("UPDATE jobs SET is_interactive = COALESCE(is_interactive, 0) WHERE is_interactive IS NULL")
    except DbErrors.OperationalError:
        pass

    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs(worker_id)")

    # Idempotent enqueue for planner/UIs (NULLs are allowed; duplicates are prevented when job_key is set).
    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_job_key_unique ON jobs(job_key)")
    except DbErrors.OperationalError:
        pass

    # Fast sweep progress / dashboards.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_sweep_type_status ON jobs(sweep_id, job_type, status)")

    # Fast parent/group progress aggregation.
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_parent_type_status ON jobs(parent_job_id, job_type, status)")
    except DbErrors.OperationalError:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_group_type_status ON jobs(group_key, job_type, status)")
    except DbErrors.OperationalError:
        pass

    # Control plane + dashboards.
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_type_status_group ON jobs(job_type, status, group_key)")
    except DbErrors.OperationalError:
        pass
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_group_pause_cancel_status ON jobs(group_key, pause_requested, cancel_requested, status)"
        )
    except DbErrors.OperationalError:
        pass

    # Lease lookup / stale reclaim helpers.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_lease_expires ON jobs(status, lease_expires_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_claimed_by_status ON jobs(claimed_by, status)")

    # Claim ordering for backtest workers.
    # Prefer interactive single runs over sweep children via is_interactive/priority.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_claim_backtest ON jobs(job_type, status, is_interactive, priority, created_at)"
        )
    except DbErrors.OperationalError:
        try:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_claim_backtest ON jobs(job_type, status, priority, created_at)"
            )
        except DbErrors.OperationalError:
            pass

    # bot_run_map for traceability
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_run_map (
            bot_id INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (bot_id, run_id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_run_map_run_id ON bot_run_map(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_run_map_bot_id ON bot_run_map(bot_id)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bot_events (
            id {_auto_pk(conn)},
            bot_id INTEGER NOT NULL,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            json_payload TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_events_bot_id_ts ON bot_events(bot_id, ts DESC)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bot_fills (
            id {_auto_pk(conn)},
            bot_id INTEGER NOT NULL,
            run_id TEXT,
            symbol TEXT NOT NULL,
            exchange_id TEXT,
            position_side TEXT,
            order_action TEXT,
            client_order_id TEXT,
            external_order_id TEXT,
            filled_qty REAL NOT NULL,
            avg_fill_price REAL NOT NULL,
            fee_paid REAL,
            fee_asset TEXT,
            is_reduce_only INTEGER DEFAULT 0,
            is_dca INTEGER DEFAULT 0,
            note TEXT,
            event_ts TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_bot_ts ON bot_fills(bot_id, event_ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_run_id ON bot_fills(run_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_fills_client_oid ON bot_fills(bot_id, client_order_id)")

    # Unified ledger for fills/fees/funding adjustments
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bot_ledger (
            id {_auto_pk(conn)},
            bot_id INTEGER NOT NULL,
            event_ts TEXT NOT NULL,
            kind TEXT NOT NULL,
            symbol TEXT,
            side TEXT,
            position_side TEXT,
            qty REAL,
            price REAL,
            fee REAL,
            funding REAL,
            pnl REAL,
            ref_id TEXT,
            meta_json TEXT DEFAULT '{{}}'
        )
        """
    )
    # Unique idempotency only when ref_id is present
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bot_ledger_unique_ref
        ON bot_ledger(bot_id, kind, ref_id)
        WHERE ref_id IS NOT NULL
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_ledger_bot_ts ON bot_ledger(bot_id, event_ts DESC)")

    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bot_positions_history (
            id {_auto_pk(conn)},
            bot_id INTEGER NOT NULL,
            run_id TEXT,
            symbol TEXT NOT NULL,
            exchange_id TEXT,
            position_side TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT NOT NULL,
            max_size REAL,
            entry_avg_price REAL,
            exit_avg_price REAL,
            realized_pnl REAL,
            fees_paid REAL,
            num_fills INTEGER,
            num_dca_fills INTEGER,
            metadata_json TEXT DEFAULT '{{}}',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_positions_closed ON bot_positions_history(bot_id, closed_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_positions_run_id ON bot_positions_history(run_id)")

    # Bot order intents (restart-safe dynamic order tracking)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS bot_order_intents (
            id {_auto_pk(conn)},
            bot_id INTEGER NOT NULL,
            intent_key TEXT NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'parked',
            local_id INTEGER NOT NULL,
            note TEXT,
            position_side TEXT,
            activation_pct REAL,
            target_price REAL,
            qty REAL,
            reduce_only INTEGER DEFAULT 0,
            post_only INTEGER DEFAULT 0,
            client_order_id TEXT,
            external_order_id TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(bot_id, intent_key)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_bot_status ON bot_order_intents(bot_id, status, updated_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_bot_local ON bot_order_intents(bot_id, local_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_order_intents_client_oid ON bot_order_intents(bot_id, client_order_id)"
    )

    # Durable state snapshots (bot/account) for UI + multi-worker readiness.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state_snapshots (
            bot_id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            worker_id TEXT,
            status TEXT,
            desired_status TEXT,
            desired_action TEXT,
            next_action TEXT,
            health_status TEXT,
            health_json TEXT,
            pos_status TEXT,
            positions_summary_json TEXT,
            open_orders_count INTEGER,
            open_intents_count INTEGER,
            account_id INTEGER,
            risk_blocked INTEGER,
            risk_reason TEXT,
            last_error TEXT,
            last_event_ts TEXT,
            last_event_level TEXT,
            last_event_type TEXT,
            last_event_message TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_user_updated ON bot_state_snapshots(user_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_account_updated ON bot_state_snapshots(account_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_bot_state_snapshots_exchange_symbol_tf ON bot_state_snapshots(exchange_id, symbol, timeframe)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS account_state_snapshots (
            account_id INTEGER PRIMARY KEY,
            user_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            worker_id TEXT,
            status TEXT,
            risk_blocked INTEGER,
            risk_reason TEXT,
            positions_summary_json TEXT,
            open_orders_count INTEGER,
            margin_ratio REAL,
            wallet_balance REAL,
            last_error TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_user_updated ON account_state_snapshots(user_id, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_state_snapshots_exchange_updated ON account_state_snapshots(exchange_id, updated_at DESC)"
    )
    _release_migration_lock(conn)


def _get_conn() -> Any:
    return open_db_connection()


@dataclass
class SweepMeta:
    name: str
    strategy_name: str
    strategy_version: str
    data_source: str
    exchange_id: Optional[str]
    symbol: Optional[str]
    timeframe: Optional[str]
    range_mode: str
    range_params_json: str
    base_config_json: str
    sweep_definition_json: str
    sweep_scope: str = "single_asset"
    sweep_assets_json: Optional[str] = None
    sweep_category_id: Optional[int] = None
    user_id: Optional[str] = None
    status: str = "pending"
    error_message: Optional[str] = None
    id: Optional[str] = None


def create_sweep(meta: SweepMeta) -> str:
    sweep_id = meta.id or str(uuid.uuid4())
    payload = asdict(meta)
    payload["id"] = sweep_id
    payload["created_at"] = now_utc_iso()
    with _get_conn() as conn:
        _ensure_schema(conn)

        # Backwards-compatible: older DBs may not have sweeps.user_id.
        try:
            sweep_cols = get_table_columns(conn, "sweeps")
        except Exception:
            sweep_cols = set()

        # Refresh sweeps columns after any ALTERs.
        try:
            sweep_cols = get_table_columns(conn, "sweeps")
        except Exception:
            sweep_cols = set()

        has_user_id = "user_id" in sweep_cols
        has_scope = "sweep_scope" in sweep_cols
        has_assets = "sweep_assets_json" in sweep_cols
        has_cat = "sweep_category_id" in sweep_cols

        if has_user_id:
            payload["user_id"] = (payload.get("user_id") or "").strip() or DEFAULT_USER_EMAIL

        cols: list[str] = [
            "id",
            "created_at",
            "name",
            "strategy_name",
            "strategy_version",
            "data_source",
            "exchange_id",
            "symbol",
            "timeframe",
            "range_mode",
            "range_params_json",
            "base_config_json",
            "sweep_definition_json",
            "status",
            "error_message",
        ]
        if has_user_id:
            cols.insert(1, "user_id")
        if has_scope:
            cols.append("sweep_scope")
        if has_assets:
            cols.append("sweep_assets_json")
        if has_cat:
            cols.append("sweep_category_id")

        placeholders = sql_placeholders(conn, len(cols))
        cols_sql = ", ".join(cols)
        values = tuple(payload.get(c) for c in cols)

        conn.execute(
            f"""
            INSERT INTO sweeps ({cols_sql})
            VALUES ({placeholders})
            """,
            values,
        )
    return sweep_id


def _infer_exchange_id_for_symbol_seed(exchange_symbol: str) -> str:
    s = (exchange_symbol or "").strip().upper()
    if s.startswith("PERP_") or s.startswith("SPOT_"):
        return "woox"
    # CCXT default used in the app is "woo".
    return "woo"


def _seed_assets_from_existing_tables(conn: Any) -> int:
    """Seed `assets` once from existing tables (best-effort).

    Priority:
    1) symbols table (if present)
    2) sweeps/bots distinct symbols with exchange_id
    3) backtest_runs metadata_json (best-effort, limited scan)
    """

    now = now_utc_iso()
    rows: list[tuple[str, str, Optional[str], Optional[str], str, str]] = []

    try:
        sym_rows = execute_fetchall(
            conn,
            "SELECT exchange_symbol, base_asset, quote_asset FROM symbols WHERE exchange_symbol IS NOT NULL AND TRIM(exchange_symbol) != ''",
        )
    except Exception:
        sym_rows = []

    for r in sym_rows or []:
        ex_sym = str(r.get("exchange_symbol") or "").strip()
        if not ex_sym:
            continue
        ex_id = _infer_exchange_id_for_symbol_seed(ex_sym)
        base = (str(r.get("base_asset")).strip().upper() if r.get("base_asset") is not None else None)
        quote = (str(r.get("quote_asset")).strip().upper() if r.get("quote_asset") is not None else None)
        rows.append((ex_id, ex_sym, base, quote, now, now))

    if not rows:
        try:
            sw = execute_fetchall(
                conn,
                "SELECT DISTINCT exchange_id, symbol FROM sweeps WHERE exchange_id IS NOT NULL AND TRIM(exchange_id) != '' AND symbol IS NOT NULL AND TRIM(symbol) != ''",
            )
        except Exception:
            sw = []
        for r in sw or []:
            ex_id = str(r[0] or "").strip()
            sym = str(r[1] or "").strip()
            if not ex_id or not sym:
                continue
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

        try:
            bt = conn.execute(
                "SELECT DISTINCT exchange_id, symbol FROM bots WHERE exchange_id IS NOT NULL AND TRIM(exchange_id) != '' AND symbol IS NOT NULL AND TRIM(symbol) != ''"
            ).fetchall()
        except Exception:
            bt = []
        for r in bt or []:
            ex_id = str(r[0] or "").strip()
            sym = str(r[1] or "").strip()
            if not ex_id or not sym:
                continue
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

    if not rows:
        # Last resort: scan a limited number of runs for metadata.exchange_id.
        try:
            run_rows = conn.execute(
                "SELECT symbol, metadata_json FROM backtest_runs WHERE symbol IS NOT NULL AND TRIM(symbol) != '' ORDER BY created_at DESC LIMIT 5000"
            ).fetchall()
        except Exception:
            run_rows = []
        for r in run_rows or []:
            sym = str(r[0] or "").strip()
            if not sym:
                continue
            ex_id = ""
            try:
                meta_raw = r[1]
                meta = json.loads(meta_raw) if isinstance(meta_raw, str) and meta_raw.strip() else {}
                if isinstance(meta, dict):
                    ex_id = str(meta.get("exchange_id") or "").strip()
            except Exception:
                ex_id = ""
            if not ex_id:
                ex_id = _infer_exchange_id_for_symbol_seed(sym)
            base, quote = _parse_exchange_symbol_assets(sym)
            rows.append((ex_id, sym, base, quote, now, now))

    if not rows:
        return 0

    # Deduplicate before insert.
    dedup: dict[tuple[str, str], tuple[str, str, Optional[str], Optional[str], str, str]] = {}
    for ex_id, sym, base, quote, ca, ua in rows:
        key = (str(ex_id), str(sym))
        if key not in dedup:
            dedup[key] = (str(ex_id), str(sym), base, quote, ca, ua)

    cur = conn.executemany(
        f"""
        INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, created_at, updated_at)
        VALUES ({sql_placeholders(conn, 6)})
        ON CONFLICT (exchange_id, symbol) DO NOTHING
        """,
        list(dedup.values()),
    )
    try:
        return int(cur.rowcount or 0)
    except Exception:
        return 0


def _upsert_asset(
    conn: Any,
    *,
    exchange_id: str,
    symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    status: Optional[str] = None,
) -> None:
    ex_id = (exchange_id or "").strip()
    sym = (symbol or "").strip()
    if not ex_id or not sym:
        return
    if base_asset is None or quote_asset is None:
        b, q = _parse_exchange_symbol_assets(sym)
        base_asset = base_asset or b
        quote_asset = quote_asset or q
    base_norm = (base_asset.strip().upper() if isinstance(base_asset, str) and base_asset.strip() else None)
    quote_norm = (quote_asset.strip().upper() if isinstance(quote_asset, str) and quote_asset.strip() else None)
    status_norm = (str(status).strip().lower() if status is not None else None)
    if status_norm is not None and status_norm not in {"active", "disabled"}:
        status_norm = None

    created_at = now_utc_iso()
    updated_at = created_at
    conn.execute(
        """
        INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, status, created_at, updated_at)
        VALUES (%s, %s, %s, %s, COALESCE(%s, 'active'), %s, %s)
        ON CONFLICT(exchange_id, symbol) DO UPDATE SET
            base_asset = COALESCE(excluded.base_asset, assets.base_asset),
            quote_asset = COALESCE(excluded.quote_asset, assets.quote_asset),
            status = COALESCE(excluded.status, assets.status),
            updated_at = excluded.updated_at
        """,
        (
            ex_id,
            sym,
            base_norm,
            quote_norm,
            status_norm,
            created_at,
            updated_at,
        ),
    )


def upsert_asset(
    exchange_id: str,
    symbol: str,
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
) -> None:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            _upsert_asset(
                conn,
                exchange_id=str(exchange_id or "").strip(),
                symbol=str(symbol or "").strip(),
                base_asset=base_asset,
                quote_asset=quote_asset,
            )


def upsert_assets_bulk(exchange_id: str, rows: Iterable[tuple[str, Optional[str], Optional[str]]]) -> int:
    """Bulk upsert assets for an exchange.

    `rows` is an iterable of (symbol, base_asset, quote_asset).
    Returns the number of non-empty symbols processed.
    """

    ex_id = str(exchange_id or "").strip()
    if not ex_id:
        raise ValueError("exchange_id is required")

    now = now_utc_iso()
    payload: list[tuple[str, str, Optional[str], Optional[str], str, str]] = []
    for sym_raw, base_raw, quote_raw in (rows or []):
        sym = str(sym_raw or "").strip()
        if not sym:
            continue
        b = str(base_raw or "").strip().upper() or None
        q = str(quote_raw or "").strip().upper() or None
        payload.append((ex_id, sym, b, q, now, now))

    if not payload:
        return 0

    # Deduplicate before insert (executemany does not dedupe automatically).
    dedup: dict[tuple[str, str], tuple[str, str, Optional[str], Optional[str], str, str]] = {}
    for ex, sym, b, q, ca, ua in payload:
        key = (ex, sym)
        if key not in dedup:
            dedup[key] = (ex, sym, b, q, ca, ua)

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.executemany(
                """
                INSERT INTO assets(exchange_id, symbol, base_asset, quote_asset, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, 'active', %s, %s)
                ON CONFLICT(exchange_id, symbol) DO UPDATE SET
                    base_asset = COALESCE(excluded.base_asset, assets.base_asset),
                    quote_asset = COALESCE(excluded.quote_asset, assets.quote_asset),
                    updated_at = excluded.updated_at
                """,
                list(dedup.values()),
            )

    return int(len(dedup))


def list_assets(
    user_id: str,
    exchange_id: str,
    status: str = "active",
) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    st_norm = (status or "").strip().lower() or "active"
    if st_norm not in {"active", "disabled", "all"}:
        st_norm = "active"

    with open_db_connection(read_only=True) as conn:

        # One query: assets + a comma-separated list of category names for this user.
        rows = execute_fetchall(
            conn,
            f"""
            SELECT
                a.id,
                a.exchange_id,
                a.symbol,
                a.base_asset,
                a.quote_asset,
                a.status,
                a.created_at,
                a.updated_at,
                COALESCE(STRING_AGG(DISTINCT c.name, ','), '') AS categories_csv
            FROM assets a
            LEFT JOIN asset_category_membership m
                ON m.exchange_id = a.exchange_id
                AND m.symbol = a.symbol
                AND m.user_id = %s
            LEFT JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            WHERE a.exchange_id = %s
            {('AND a.status = %s' if st_norm != 'all' else '')}
            GROUP BY a.id
            ORDER BY a.base_asset ASC, a.symbol ASC
            """,
            tuple([uid, ex_id] + ([st_norm] if st_norm != "all" else [])),
        )

    out: list[Dict[str, Any]] = []
    for r in rows or []:
        d = dict(r)
        cats = [c.strip() for c in str(d.get("categories_csv") or "").split(",") if c.strip()]
        d["categories"] = cats
        out.append(d)
    return out


def list_asset_symbols(
    exchange_id: str,
    *,
    status: str = "all",
    conn: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    ex_id = (exchange_id or "").strip()
    if not ex_id:
        return []
    st_norm = (status or "").strip().lower() or "all"
    if st_norm not in {"active", "disabled", "all"}:
        st_norm = "all"

    owns_conn = conn is None
    connection = conn or open_db_connection(read_only=True)
    try:
        params: list[Any] = [ex_id]
        where = "WHERE exchange_id = %s"
        if st_norm != "all":
            where += " AND status = %s"
            params.append(st_norm)
        rows = execute_fetchall(
            connection,
            f"SELECT symbol, status FROM assets {where} ORDER BY symbol ASC",
            tuple(params),
        )
        return [dict(r) for r in rows or []]
    finally:
        if owns_conn:
            try:
                connection.close()
            except Exception:
                pass


def list_assets_server_side(
    *,
    user_id: str,
    exchange_id: str,
    page: int,
    page_size: int,
    sort_model: Optional[List[Dict[str, Any]]] = None,
    filter_model: Optional[Dict[str, Any]] = None,
    category_id: Optional[int] = None,
    category_name: Optional[str] = None,
    search_text: Optional[str] = None,
    status: str = "all",
    conn: Optional[Any] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    st_norm = (status or "").strip().lower() or "all"
    if st_norm not in {"active", "disabled", "all"}:
        st_norm = "all"

    if not ex_id:
        return [], 0

    def _order_sql(model: Optional[List[Dict[str, Any]]]) -> str:
        allowed_sort = {
            "id": {"sql": "a.id"},
            "exchange_id": {"sql": "a.exchange_id"},
            "symbol": {"sql": "a.symbol"},
            "base_asset": {"sql": "a.base_asset"},
            "quote_asset": {"sql": "a.quote_asset"},
            "status": {"sql": "a.status"},
            "created_at": {"sql": "a.created_at"},
            "updated_at": {"sql": "a.updated_at"},
            "categories": {"sql": "categories_csv"},
        }
        if not isinstance(model, list) or not model:
            return "ORDER BY a.updated_at DESC, a.id ASC"
        order_parts: list[str] = []
        for item in model:
            if not isinstance(item, dict):
                continue
            col = item.get("colId") or item.get("field")
            if col not in allowed_sort:
                continue
            sql_expr = allowed_sort[col].get("sql")
            if not sql_expr:
                continue
            direction = str(item.get("sort") or "desc").lower()
            if direction not in {"asc", "desc"}:
                direction = "desc"
            order_parts.append(f"{sql_expr} {direction.upper()}")
        if not order_parts:
            return "ORDER BY a.updated_at DESC, a.id ASC"
        return "ORDER BY " + ", ".join(order_parts)

    owns_conn = conn is None
    connection = conn or open_db_connection(read_only=True)
    try:
        allowed_filters = {
            "id": {"type": "number", "sql": "a.id"},
            "exchange_id": {"type": "text", "sql": "a.exchange_id"},
            "symbol": {"type": "text", "sql": "a.symbol"},
            "base_asset": {"type": "text", "sql": "a.base_asset"},
            "quote_asset": {"type": "text", "sql": "a.quote_asset"},
            "status": {"type": "text", "sql": "a.status"},
            "created_at": {"type": "date", "sql": "a.created_at"},
            "updated_at": {"type": "date", "sql": "a.updated_at"},
            "categories": {"type": "text", "sql": "c.name"},
            "category": {"type": "text", "sql": "c.name"},
            "category_name": {"type": "text", "sql": "c.name"},
            "name": {"type": "text", "sql": "COALESCE(a.base_asset, a.symbol)"},
        }

        where_parts: list[str] = ["a.exchange_id = %s"]
        params: list[Any] = [ex_id]

        if st_norm != "all":
            where_parts.append("a.status = %s")
            params.append(st_norm)

        if category_id is not None:
            where_parts.append("m.category_id = %s")
            params.append(int(category_id))

        if category_name:
            where_parts.append("LOWER(c.name) = LOWER(%s)")
            params.append(str(category_name))

        if search_text:
            s = str(search_text).strip()
            if s:
                like = f"%{s}%"
                where_parts.append(
                    "(a.symbol ILIKE %s OR a.base_asset ILIKE %s OR a.quote_asset ILIKE %s OR c.name ILIKE %s)"
                )
                params.extend([like, like, like, like])

        filt_sql, filt_params = translate_filter_model(filter_model, allowed_filters)
        if filt_sql:
            where_parts.append(f"({filt_sql})")
            params.extend(filt_params)

        where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        order_sql = _order_sql(sort_model)

        page_val = max(1, int(page or 1))
        size_val = max(1, min(int(page_size or 200), 500))
        offset_val = (page_val - 1) * size_val

        sql = f"""
            SELECT
                a.id,
                a.exchange_id,
                a.symbol,
                a.base_asset,
                a.quote_asset,
                a.status,
                a.created_at,
                a.updated_at,
                COALESCE(STRING_AGG(DISTINCT c.name, ','), '') AS categories_csv
            FROM assets a
            LEFT JOIN asset_category_membership m
                ON m.exchange_id = a.exchange_id
                AND m.symbol = a.symbol
                AND m.user_id = %s
            LEFT JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            {where_clause}
            GROUP BY a.id
            {order_sql}
            LIMIT %s OFFSET %s
        """

        data_params = [uid] + list(params) + [size_val, offset_val]
        rows = execute_fetchall(connection, sql, tuple(data_params))

        count_sql = f"""
            SELECT COUNT(DISTINCT a.id)
            FROM assets a
            LEFT JOIN asset_category_membership m
                ON m.exchange_id = a.exchange_id
                AND m.symbol = a.symbol
                AND m.user_id = %s
            LEFT JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            {where_clause}
        """
        total_row = connection.execute(count_sql, tuple([uid] + list(params))).fetchone()
        total_count = int(total_row[0] or 0) if total_row else 0

        out: list[Dict[str, Any]] = []
        for r in rows or []:
            d = dict(r)
            cats = [c.strip() for c in str(d.get("categories_csv") or "").split(",") if c.strip()]
            d["categories"] = cats
            out.append(d)
        return out, int(total_count)
    finally:
        if owns_conn:
            try:
                connection.close()
            except Exception:
                pass


def list_categories(user_id: str, exchange_id: str) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    with open_db_connection(read_only=True) as conn:
        rows = execute_fetchall(
            conn,
            """
            SELECT
                c.*, 
                COALESCE(mc.member_count, 0) AS member_count
            FROM asset_categories c
            LEFT JOIN (
                SELECT user_id, exchange_id, category_id, COUNT(*) AS member_count
                FROM asset_category_membership
                GROUP BY user_id, exchange_id, category_id
            ) mc
                ON mc.user_id = c.user_id
                AND mc.exchange_id = c.exchange_id
                AND mc.category_id = c.id
            WHERE c.user_id = %s AND c.exchange_id = %s
            ORDER BY c.name ASC
            """,
            (uid, ex_id),
        )
    return [dict(r) for r in rows or []]


def set_assets_status(exchange_id: str, symbols: Iterable[str], status: str) -> int:
    """Bulk set assets.status for an exchange.

    Returns the number of rows updated (best-effort).
    """
    ex_id = (exchange_id or "").strip()
    st_norm = (status or "").strip().lower()
    if st_norm not in {"active", "disabled"}:
        raise ValueError("status must be 'active' or 'disabled'")
    syms = [str(s or "").strip() for s in (symbols or []) if str(s or "").strip()]
    if not ex_id or not syms:
        return 0
    now = now_utc_iso()

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            updated = 0
            chunk = 300
            for i in range(0, len(syms), chunk):
                part = syms[i : i + chunk]
                placeholders = ",".join(["%s"] * len(part))
                cur = conn.execute(
                    f"UPDATE assets SET status = %s, updated_at = %s WHERE exchange_id = %s AND symbol IN ({placeholders})",
                    tuple([st_norm, now, ex_id] + part),
                )
                try:
                    updated += int(cur.rowcount or 0)
                except Exception:
                    pass
            return int(updated)


def create_category(user_id: str, exchange_id: str, name: str, source: str = "user") -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    nm = (name or "").strip()
    if not ex_id or not nm:
        raise ValueError("exchange_id and name are required")
    src = (source or "user").strip().lower() or "user"
    if src not in {"user", "auto"}:
        src = "user"
    now = now_utc_iso()

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                """
                INSERT INTO asset_categories(user_id, exchange_id, name, source, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(user_id, exchange_id, name) DO UPDATE SET
                    updated_at = excluded.updated_at
                """,
                (uid, ex_id, nm, src, now, now),
            )
            row = execute_fetchone(
                conn,
                "SELECT id FROM asset_categories WHERE user_id = %s AND exchange_id = %s AND name = %s",
                (uid, ex_id, nm),
            )
            return int(row.get("id")) if row else 0


def get_category_symbols(user_id: str, exchange_id: str, category_id: int) -> List[str]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    with open_db_connection(read_only=True) as conn:
        rows = execute_fetchall(
            conn,
            """
            SELECT symbol
            FROM asset_category_membership
            WHERE user_id = %s AND exchange_id = %s AND category_id = %s
            ORDER BY symbol ASC
            """,
            (uid, ex_id, cid),
        )
        return [str(r.get("symbol")) for r in rows or [] if r and str(r.get("symbol") or "").strip()]


def set_category_membership(
    user_id: str,
    exchange_id: str,
    category_id: int,
    symbols_to_add: Iterable[str],
    symbols_to_remove: Iterable[str],
) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    now = now_utc_iso()

    add_rows: list[tuple[str, str, int, str, str]] = []
    for s in symbols_to_add or []:
        sym = (str(s or "").strip())
        if not sym:
            continue
        add_rows.append((uid, ex_id, cid, sym, now))

    rem_rows: list[str] = []
    for s in symbols_to_remove or []:
        sym = (str(s or "").strip())
        if not sym:
            continue
        rem_rows.append(sym)

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            if add_rows:
                conn.executemany(
                    """
                    INSERT INTO asset_category_membership(user_id, exchange_id, category_id, symbol, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    add_rows,
                )
            if rem_rows:
                # Chunk to stay under parameter limits.
                chunk = 300
                for i in range(0, len(rem_rows), chunk):
                    part = rem_rows[i : i + chunk]
                    placeholders = ",".join(["%s"] * len(part))
                    conn.execute(
                        f"DELETE FROM asset_category_membership WHERE user_id = %s AND exchange_id = %s AND category_id = %s AND symbol IN ({placeholders})",
                        tuple([uid, ex_id, cid] + part),
                    )
            conn.execute(
                "UPDATE asset_categories SET updated_at = %s WHERE user_id = %s AND exchange_id = %s AND id = %s",
                (now, uid, ex_id, cid),
            )


def delete_category(user_id: str, exchange_id: str, category_id: int) -> int:
    """Delete a category and all its memberships for a user/exchange."""

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    cid = int(category_id)
    if not ex_id:
        raise ValueError("exchange_id is required")

    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                "DELETE FROM asset_category_membership WHERE user_id = %s AND exchange_id = %s AND category_id = %s",
                (uid, ex_id, cid),
            )
            cur = conn.execute(
                "DELETE FROM asset_categories WHERE user_id = %s AND exchange_id = %s AND id = %s",
                (uid, ex_id, cid),
            )
            return int(cur.rowcount or 0)


def seed_asset_categories_if_empty(user_id: str, exchange_id: str, *, force: bool = False) -> Dict[str, Any]:
    """One-time best-effort category seeding.

    Never overwrites user edits: when categories exist, default is a no-op.
    If `force=True`, this runs in merge mode (adds missing auto categories/memberships via ON CONFLICT DO NOTHING).
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    if not ex_id:
        raise ValueError("exchange_id is required")

    with _get_conn() as conn:
        _ensure_schema(conn)
        row = execute_fetchone(
            conn,
            "SELECT COUNT(*) AS c FROM asset_categories WHERE user_id = %s AND exchange_id = %s",
            (uid, ex_id),
        )
        existing = int(row.get("c") if row is not None else 0)
        if existing > 0 and not bool(force):
            return {"status": "skipped", "reason": "categories_exist", "created_categories": 0, "created_memberships": 0}

    # Create base categories regardless (source=auto).
    base_categories: list[tuple[str, str]] = [
        ("L1", "layer-1"),
        ("L2", "layer-2"),
        ("Meme", "meme-token"),
        ("Stable", "stablecoins"),
        ("DeFi", "decentralized-finance-defi"),
    ]

    created_categories = 0
    created_memberships = 0
    cat_name_to_id: dict[str, int] = {}
    for name, _slug in base_categories:
        try:
            cid = create_category(uid, ex_id, name, source="auto")
            if cid:
                cat_name_to_id[name] = cid
                created_categories += 1
        except Exception:
            continue

    # Best-effort online fetch from CoinGecko.
    ticker_map: dict[str, set[str]] = {name: set() for name, _ in base_categories}
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "project-dragon/1.0 (category-seed)"})

        for name, slug in base_categories:
            if not slug:
                continue
            url = "https://api.coingecko.com/api/v3/coins/markets"
            params = {
                "vs_currency": "usd",
                "category": slug,
                "order": "market_cap_desc",
                "per_page": 250,
                "page": 1,
                "sparkline": "false",
            }
            resp = session.get(url, params=params, timeout=8)
            if resp.status_code != 200:
                continue
            payload = resp.json()
            if not isinstance(payload, list):
                continue
            for item in payload:
                if not isinstance(item, dict):
                    continue
                sym = (str(item.get("symbol") or "").strip().upper())
                if sym:
                    ticker_map[name].add(sym)
    except Exception:
        ticker_map = {name: set() for name, _ in base_categories}

    # Map tickers -> exchange symbols using assets.base_asset.
    with _get_conn() as conn:
        _ensure_schema(conn)
        for cat_name, tickers in ticker_map.items():
            cid = cat_name_to_id.get(cat_name)
            if not cid or not tickers:
                continue
            tick_list = sorted(list(tickers))
            chunk = 300
            symbols: list[str] = []
            for i in range(0, len(tick_list), chunk):
                part = tick_list[i : i + chunk]
                placeholders = ",".join([sql_placeholder(conn)] * len(part))
                rows = execute_fetchall(
                    conn,
                    f"SELECT symbol FROM assets WHERE exchange_id = {sql_placeholder(conn)} AND base_asset IN ({placeholders}) AND status = 'active'",
                    tuple([ex_id] + part),
                )
                for r in rows or []:
                    s = str(r.get("symbol") or "").strip()
                    if s:
                        symbols.append(s)
            if symbols:
                before = execute_fetchone(
                    conn,
                    f"SELECT COUNT(*) AS c FROM asset_category_membership WHERE user_id = {sql_placeholder(conn)} AND exchange_id = {sql_placeholder(conn)} AND category_id = {sql_placeholder(conn)}",
                    (uid, ex_id, int(cid)),
                )
                before_n = int(before.get("c") if before is not None else 0)

                # Insert (ignore duplicates).
                now = now_utc_iso()
                conn.executemany(
                    f"""
                    INSERT INTO asset_category_membership(user_id, exchange_id, category_id, symbol, created_at)
                    VALUES ({sql_placeholders(conn, 5)})
                    ON CONFLICT DO NOTHING
                    """,
                    [(uid, ex_id, int(cid), s, now) for s in sorted(set(symbols))],
                )

                after = execute_fetchone(
                    conn,
                    f"SELECT COUNT(*) AS c FROM asset_category_membership WHERE user_id = {sql_placeholder(conn)} AND exchange_id = {sql_placeholder(conn)} AND category_id = {sql_placeholder(conn)}",
                    (uid, ex_id, int(cid)),
                )
                after_n = int(after.get("c") if after is not None else 0)
                created_memberships += max(0, after_n - before_n)

        conn.commit()

    return {
        "status": "ok",
        "created_categories": int(created_categories),
        "created_memberships": int(created_memberships),
    }


def get_symbols_primary_category_map(
    conn: Any,
    *,
    user_id: str,
    exchange_id: str,
    symbols: Sequence[str],
) -> Dict[str, str]:
    """Return symbol -> (first category name) mapping for a user/exchange.

    Uses a single query; suitable for result grids.
    """
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    ex_id = (exchange_id or "").strip()
    syms = [str(s or "").strip() for s in (symbols or []) if str(s or "").strip()]
    if not ex_id or not syms:
        return {}
    out: Dict[str, str] = {}
    chunk = 300
    for i in range(0, len(syms), chunk):
        part = syms[i : i + chunk]
        placeholders = ",".join(["%s"] * len(part))
        rows = execute_fetchall(
            conn,
            f"""
            SELECT m.symbol, MIN(c.name) AS cat
            FROM asset_category_membership m
            JOIN asset_categories c
                ON c.id = m.category_id
                AND c.user_id = m.user_id
                AND c.exchange_id = m.exchange_id
            WHERE m.user_id = %s AND m.exchange_id = %s AND m.symbol IN ({placeholders})
            GROUP BY m.symbol
            """,
            tuple([uid, ex_id] + part),
        )
        for r in rows or []:
            sym = str(r.get("symbol") or "").strip()
            cat = str(r.get("cat") or "").strip()
            if sym and cat:
                out[sym] = cat
    return out


def update_sweep_status(sweep_id: str, status: str, error_message: Optional[str] = None) -> None:
    with _get_conn() as conn:
        _ensure_schema(conn)
        with conn:
            conn.execute(
                "UPDATE sweeps SET status = %s, error_message = %s WHERE id = %s",
                (status, error_message, sweep_id),
            )


def delete_sweep_and_runs(*, user_id: str, sweep_id: str) -> Dict[str, int]:
    """Delete a sweep and all runs inside it.

    Best-effort cleanup:
    - `backtest_run_details` for those runs
    - `run_shortlists` rows for those runs (scoped to the requesting user)
    - `bot_run_map` rows for those runs (if present)
    - `jobs` rows referencing sweep/run ids when possible
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    sid = str(sweep_id or "").strip()
    if not sid:
        raise ValueError("sweep_id is required")

    with _get_conn() as conn:
        _ensure_schema(conn)
        # Ensure the sweep exists and is in-scope for this user (legacy DBs may have NULL/empty user_id).
        try:
            sweep_cols = get_table_columns(conn, "sweeps")
        except Exception:
            sweep_cols = set()
        if "user_id" in sweep_cols:
            row = execute_fetchone(
                conn,
                "SELECT id FROM sweeps WHERE id = %s AND (user_id = %s OR user_id IS NULL OR TRIM(user_id) = '')",
                (sid, uid),
            )
        else:
            row = execute_fetchone(conn, "SELECT id FROM sweeps WHERE id = %s", (sid,))
        if row is None:
            raise ValueError(f"Sweep not found (or not accessible): {sid}")

        run_rows = execute_fetchall(conn, "SELECT id FROM backtest_runs WHERE sweep_id = %s", (sid,))
        run_ids = [str(r.get("id") or "").strip() for r in run_rows or [] if r and str(r.get("id") or "").strip()]

        details_deleted = 0
        shortlist_deleted = 0
        bot_run_map_deleted = 0
        jobs_deleted = 0
        runs_deleted = 0
        sweeps_deleted = 0

        with conn:
            if run_ids:
                placeholders = ",".join(["%s"] * len(run_ids))

                # backtest_run_details
                try:
                    cur = conn.execute(f"DELETE FROM backtest_run_details WHERE run_id IN ({placeholders})", tuple(run_ids))
                    details_deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    details_deleted = 0

                # per-user run shortlist cleanup
                try:
                    cur = conn.execute(
                        f"DELETE FROM run_shortlists WHERE user_id = %s AND run_id IN ({placeholders})",
                        tuple([uid] + run_ids),
                    )
                    shortlist_deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    shortlist_deleted = 0

                # bot_run_map cleanup (best-effort)
                try:
                    cur = conn.execute(f"DELETE FROM bot_run_map WHERE run_id IN ({placeholders})", tuple(run_ids))
                    bot_run_map_deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    bot_run_map_deleted = 0

                # jobs cleanup (best-effort)
                try:
                    # sweep-level jobs
                    cur = conn.execute(
                        "DELETE FROM jobs WHERE sweep_id = %s OR payload_json LIKE %s",
                        (sid, f'%"sweep_id": "{sid}"%'),
                    )
                    jobs_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    pass
                try:
                    # run-level jobs: payload_json is more reliable than the (legacy) integer run_id column.
                    for rid in run_ids:
                        cur = conn.execute(
                            "DELETE FROM jobs WHERE payload_json LIKE %s",
                            (f'%"run_id": "{rid}"%',),
                        )
                        jobs_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    pass

            # Delete runs, then sweep.
            cur = conn.execute("DELETE FROM backtest_runs WHERE sweep_id = %s", (sid,))
            runs_deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)

            cur = conn.execute("DELETE FROM sweeps WHERE id = %s", (sid,))
            sweeps_deleted = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)

        return {
            "sweeps_deleted": sweeps_deleted,
            "runs_deleted": runs_deleted,
            "details_deleted": details_deleted,
            "shortlist_deleted": shortlist_deleted,
            "bot_run_map_deleted": bot_run_map_deleted,
            "jobs_deleted": jobs_deleted,
        }


def _is_db_locked_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return "database is locked" in msg or "database table is locked" in msg or "locked" in msg


def _run_write_with_retry(fn, *, retries: int = 3, base_sleep_s: float = 0.4) -> Any:
    """Run a DB write with brief retry/backoff for transient locks."""

    last_exc: Optional[Exception] = None
    for attempt in range(max(1, int(retries))):
        try:
            return fn()
        except DbErrors.OperationalError as exc:
            last_exc = exc
            if not _is_db_locked_error(exc):
                raise
            sleep_s = float(base_sleep_s) * (2 ** attempt)
            time.sleep(sleep_s)
        except Exception as exc:
            last_exc = exc
            raise
    if last_exc is not None:
        raise last_exc
    return None


def _build_runs_filter_sql(
    *,
    older_than_days: Optional[int] = None,
    sweep_ids: Optional[Sequence[str]] = None,
    max_profit: Optional[float] = None,
    roi_threshold_pct: Optional[float] = None,
    only_completed: bool = False,
) -> tuple[str, list[Any]]:
    """Return WHERE SQL + params for backtest_runs filters."""

    where_parts: list[str] = []
    params: list[Any] = []

    if older_than_days is not None and int(older_than_days) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
        where_parts.append("created_at < %s")
        params.append(cutoff.isoformat())

    if sweep_ids:
        sweep_ids_clean = [str(s).strip() for s in sweep_ids if str(s).strip()]
        if sweep_ids_clean:
            placeholders = ",".join(["%s"] * len(sweep_ids_clean))
            where_parts.append(f"sweep_id IN ({placeholders})")
            params.extend(sweep_ids_clean)

    if max_profit is not None:
        where_parts.append("net_profit IS NOT NULL AND net_profit <= %s")
        params.append(float(max_profit))

    if roi_threshold_pct is not None:
        roi_expr = (
            "COALESCE(roi_pct_on_margin, "
            "CASE WHEN net_return_pct BETWEEN -1 AND 1 THEN net_return_pct * 100.0 ELSE net_return_pct END)"
        )
        where_parts.append(f"{roi_expr} IS NOT NULL AND {roi_expr} <= %s")
        params.append(float(roi_threshold_pct))

    if only_completed:
        where_parts.append("end_time IS NOT NULL AND TRIM(COALESCE(end_time, '')) <> ''")

    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    return where_sql, params


def count_runs_matching_filters(
    *,
    older_than_days: Optional[int] = None,
    sweep_ids: Optional[Sequence[str]] = None,
    max_profit: Optional[float] = None,
    roi_threshold_pct: Optional[float] = None,
    only_completed: bool = False,
    conn: Optional[Any] = None,
) -> int:
    """Count backtest_runs matching filter criteria."""

    where_sql, params = _build_runs_filter_sql(
        older_than_days=older_than_days,
        sweep_ids=sweep_ids,
        max_profit=max_profit,
        roi_threshold_pct=roi_threshold_pct,
        only_completed=only_completed,
    )

    connection = conn or open_db_connection()
    try:
        _ensure_schema(connection)
        row = execute_fetchone(
            connection,
            f"SELECT COUNT(*) AS c FROM backtest_runs{where_sql}",
            tuple(params),
        )
        return int(row.get("c") if row is not None else 0)
    finally:
        if conn is None:
            try:
                connection.close()
            except Exception:
                pass


def clear_run_details_payloads(
    *,
    older_than_days: Optional[int] = None,
    sweep_ids: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    """Clear large payload columns while preserving run basics + metrics/config."""

    where_sql, params = _build_runs_filter_sql(older_than_days=older_than_days, sweep_ids=sweep_ids)
    if not where_sql:
        where_sql = " WHERE 1=1"

    details_updated = 0
    runs_updated = 0

    with open_db_connection() as conn:
        _ensure_schema(conn)

        try:
            detail_cols = get_table_columns(conn, "backtest_run_details")
        except Exception:
            detail_cols = set()

        clear_cols = [
            c
            for c in (
                "trades_json",
                "equity_curve_json",
                "equity_timestamps_json",
                "extra_series_json",
                "candles_json",
                "params_json",
                "run_context_json",
                "computed_metrics_json",
            )
            if c in detail_cols
        ]

        def _do_update() -> None:
            nonlocal details_updated, runs_updated
            now = now_utc_iso()
            if clear_cols:
                set_bits = [f"{c}=NULL" for c in clear_cols]
                params_update: list[Any] = []
                if "updated_at" in detail_cols:
                    set_bits.append("updated_at = %s")
                    params_update.append(now)

                sql = (
                    "UPDATE backtest_run_details SET "
                    + ", ".join(set_bits)
                    + f" WHERE run_id IN (SELECT id FROM backtest_runs{where_sql})"
                )
                cur = conn.execute(sql, tuple(params_update + params))
                details_updated = int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)

            # Legacy: clear trades_json on backtest_runs (if present)
            try:
                run_cols = get_table_columns(conn, "backtest_runs")
            except Exception:
                run_cols = set()
            if "trades_json" in run_cols:
                cur2 = conn.execute(
                    f"UPDATE backtest_runs SET trades_json = NULL WHERE id IN (SELECT id FROM backtest_runs{where_sql})",
                    tuple(params),
                )
                runs_updated = int(cur2.rowcount if cur2.rowcount is not None and cur2.rowcount >= 0 else 0)

            conn.commit()

        try:
            _run_write_with_retry(_do_update)
        except Exception as exc:
            if _is_db_locked_error(exc):
                raise RuntimeError("Database is locked. Please retry in a moment.") from exc
            raise

    return {"details_updated": int(details_updated), "runs_updated": int(runs_updated)}


def delete_runs_and_children(
    *,
    older_than_days: Optional[int] = None,
    sweep_ids: Optional[Sequence[str]] = None,
    max_profit: Optional[float] = None,
    roi_threshold_pct: Optional[float] = None,
    only_completed: bool = False,
    batch_size: int = 500,
) -> Dict[str, int]:
    """Delete runs and child tables in batches for safety."""

    where_sql, params = _build_runs_filter_sql(
        older_than_days=older_than_days,
        sweep_ids=sweep_ids,
        max_profit=max_profit,
        roi_threshold_pct=roi_threshold_pct,
        only_completed=only_completed,
    )

    total_deleted = 0
    details_deleted = 0
    shortlists_deleted = 0
    bot_run_map_deleted = 0

    with open_db_connection() as conn:
        _ensure_schema(conn)

        batch_size_i = max(1, int(batch_size or 500))

        while True:
            rows = execute_fetchall(
                conn,
                f"SELECT id FROM backtest_runs{where_sql} ORDER BY created_at ASC LIMIT %s",
                tuple(params + [batch_size_i]),
            )
            if not rows:
                break

            run_ids = [str(r.get("id") or "").strip() for r in rows if r and str(r.get("id") or "").strip()]
            if not run_ids:
                break

            placeholders = ",".join(["%s"] * len(run_ids))

            def _delete_batch() -> None:
                nonlocal total_deleted, details_deleted, shortlists_deleted, bot_run_map_deleted

                # backtest_run_details
                try:
                    cur = conn.execute(
                        f"DELETE FROM backtest_run_details WHERE run_id IN ({placeholders})",
                        tuple(run_ids),
                    )
                    details_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    pass

                # run_shortlists
                try:
                    cur = conn.execute(
                        f"DELETE FROM run_shortlists WHERE run_id IN ({placeholders})",
                        tuple(run_ids),
                    )
                    shortlists_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    pass

                # bot_run_map
                try:
                    cur = conn.execute(
                        f"DELETE FROM bot_run_map WHERE run_id IN ({placeholders})",
                        tuple(run_ids),
                    )
                    bot_run_map_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)
                except DbErrors.OperationalError:
                    pass

                # backtest_runs
                cur = conn.execute(
                    f"DELETE FROM backtest_runs WHERE id IN ({placeholders})",
                    tuple(run_ids),
                )
                total_deleted += int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0)

                conn.commit()

            try:
                _run_write_with_retry(_delete_batch)
            except Exception as exc:
                if _is_db_locked_error(exc):
                    raise RuntimeError("Database is locked. Please retry in a moment.") from exc
                raise

    return {
        "runs_deleted": int(total_deleted),
        "details_deleted": int(details_deleted),
        "shortlists_deleted": int(shortlists_deleted),
        "bot_run_map_deleted": int(bot_run_map_deleted),
    }


def list_sweeps(limit: int = 100, offset: int = 0, *, conn: Optional[Any] = None) -> List[Dict]:
    def _is_db_conn(obj: Any) -> bool:
        return bool(obj is not None and hasattr(obj, "execute"))

    connection = conn if _is_db_conn(conn) else open_db_connection(read_only=True)
    owns_conn = connection is not conn
    try:
        with profile_span("db.list_sweeps", meta={"limit": int(limit), "offset": int(offset)}):
            rows = execute_fetchall(
                connection,
                """
                SELECT * FROM sweeps
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
        return [dict(r) for r in rows]
    finally:
        if not owns_conn:
            try:
                connection.commit()
            except Exception:
                pass
        if owns_conn:
            try:
                connection.close()
            except Exception:
                pass


def list_sweeps_with_run_counts(
    *,
    user_id: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    offset: int = 0,
    conn: Optional[Any] = None,
) -> List[Dict]:
    """List sweeps with a DB-derived run_count per sweep.

    Performance: one query using a LEFT JOIN to an aggregated subquery.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    _ = filters  # reserved for future SQL WHERE clauses

    owns_conn = conn is None
    connection = conn or open_db_connection(read_only=True)
    try:
        try:
            sweep_cols = get_table_columns(connection, "sweeps")
        except Exception:
            sweep_cols = set()

        where_clause = ""
        params: list[Any] = []
        if "user_id" in sweep_cols:
            where_clause = "WHERE (s.user_id = %s OR s.user_id IS NULL OR TRIM(s.user_id) = '')"
            params.append(uid)

        params.extend([int(limit), int(offset)])

        with profile_span(
            "db.list_sweeps_with_run_counts",
            meta={"limit": int(limit), "offset": int(offset), "user_scoped": bool(where_clause)},
        ):
            rows = execute_fetchall(
                connection,
                f"""
                SELECT
                    s.*,
                    COALESCE(rc.run_count, 0) AS run_count
                FROM sweeps s
                LEFT JOIN (
                    SELECT sweep_id, COUNT(*) AS run_count
                    FROM backtest_runs
                    WHERE sweep_id IS NOT NULL AND TRIM(sweep_id) != ''
                    GROUP BY sweep_id
                ) rc
                    ON rc.sweep_id = s.id
                {where_clause}
                ORDER BY s.created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
        return [dict(r) for r in rows]
    finally:
        if not owns_conn:
            try:
                connection.commit()
            except Exception:
                pass
        if owns_conn:
            try:
                connection.close()
            except Exception:
                pass


def list_sweep_best_metrics(*, user_id: str) -> List[Dict[str, Any]]:
    """Compute per-sweep best metrics via SQL aggregation.

    This avoids loading all run summaries into memory (which can be slow and can
    make the Sweeps page feel like it's hanging).

    Returns rows like:
      {"sweep_id": "...", "best_roi_pct_on_margin": 12.3, "best_cpc_index": 1.8, ...}

    Only includes metrics that exist as columns in `backtest_runs`.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    with open_db_connection(read_only=True) as conn:

        try:
            run_cols = get_table_columns(conn, "backtest_runs")
        except Exception:
            run_cols = set()

        metric_cols: list[tuple[str, str]] = []
        if "net_return_pct" in run_cols:
            metric_cols.append(("best_net_return_pct", "net_return_pct"))
        if "sharpe" in run_cols:
            metric_cols.append(("best_sharpe", "sharpe"))
        if "roi_pct_on_margin" in run_cols:
            metric_cols.append(("best_roi_pct_on_margin", "roi_pct_on_margin"))
        if "cpc_index" in run_cols:
            metric_cols.append(("best_cpc_index", "cpc_index"))

        # Nothing to do if sweep_id missing or no metrics exist.
        if "sweep_id" not in run_cols or not metric_cols:
            return []

        select_bits = ["sweep_id"] + [f"MAX({src}) AS {alias}" for alias, src in metric_cols]
        where_bits: list[str] = ["sweep_id IS NOT NULL", "TRIM(COALESCE(sweep_id,'')) <> ''"]
        params: list[Any] = []
        if "user_id" in run_cols:
            where_bits.append("(user_id = %s OR user_id IS NULL OR TRIM(user_id) = '')")
            params.append(uid)

        sql = (
            "SELECT "
            + ", ".join(select_bits)
            + " FROM backtest_runs WHERE "
            + " AND ".join(where_bits)
            + " GROUP BY sweep_id"
        )

        with profile_span("db.list_sweep_best_metrics", meta={"user_id": uid}):
            rows = execute_fetchall(conn, sql, params)
        return [dict(r) for r in rows]


def get_sweep_run_count(*, user_id: str, sweep_id: str) -> int:
    """Fallback helper to count runs for a given sweep.

    Prefer using the run_count already returned by list_sweeps_with_run_counts.
    """

    _ = (user_id or "").strip() or DEFAULT_USER_EMAIL
    sid = str(sweep_id or "").strip()
    if not sid:
        return 0

    with open_db_connection(read_only=True) as conn:
        row = execute_fetchone(
            conn,
            "SELECT COUNT(*) AS c FROM backtest_runs WHERE sweep_id = %s",
            (sid,),
        )
    try:
        return int(row.get("c") if row is not None else 0)
    except Exception:
        return 0


def get_sweep(sweep_id: str) -> Optional[Dict]:
    with open_db_connection(read_only=True) as conn:
        row = execute_fetchone(conn, "SELECT * FROM sweeps WHERE id = %s", (sweep_id,))
    return dict(row) if row else None


def list_runs_for_sweep(sweep_id: str) -> List[Dict]:
    with open_db_connection(read_only=True) as conn:
        with profile_span("db.list_runs_for_sweep", meta={"sweep_id": str(sweep_id)}):
            rows = execute_fetchall(
                conn,
                """
                SELECT * FROM backtest_runs
                WHERE sweep_id = %s
                ORDER BY created_at ASC
                """,
                (sweep_id,),
            )
    return [dict(r) for r in rows]


def _canonicalize_json_for_hash(raw: Any) -> str:
    """Return a stable string representation for hash-based comparisons.

    Used for deduplication. This is intentionally tolerant of legacy / malformed
    payloads: when JSON parsing fails, it falls back to a normalized string.
    """

    if raw is None:
        return "null"

    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return ""
        try:
            obj = json.loads(s)
        except Exception:
            return s
    else:
        obj = raw

    try:
        # Ensure deterministic formatting independent of dict insertion order.
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return ""


def dedupe_sweep_runs(sweep_id: str) -> Dict[str, Any]:
    """Remove duplicate runs within a sweep.

    Duplicates are defined as runs that share the same sweep_id and the same
    canonicalized config payload, *scoped by* (strategy_name, strategy_version,
    symbol, timeframe) to avoid deleting legitimate multi-asset sweeps.

    Returns a small summary dict suitable for logging/UI.
    """

    sid = str(sweep_id or "").strip()
    if not sid:
        return {"sweep_id": sid, "total": 0, "deduped_groups": 0, "removed": 0}

    with _get_conn() as conn:
        _ensure_schema(conn)
        with profile_span("db.dedupe_sweep_runs", meta={"sweep_id": sid}):
            rows = execute_fetchall(
                conn,
                """
                SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version, config_json
                FROM backtest_runs
                WHERE sweep_id = %s
                ORDER BY created_at ASC
                """,
                (sid,),
            )

            total = len(rows or [])
            if total <= 1:
                return {"sweep_id": sid, "total": total, "deduped_groups": 0, "removed": 0}

            groups: Dict[str, List[Dict[str, Any]]] = {}
            for r in rows:
                cfg_canon = _canonicalize_json_for_hash(r["config_json"])
                cfg_hash = hashlib.sha256(cfg_canon.encode("utf-8", errors="ignore")).hexdigest()
                key = "|".join(
                    [
                        str(r["strategy_name"] or ""),
                        str(r["strategy_version"] or ""),
                        str(r["symbol"] or ""),
                        str(r["timeframe"] or ""),
                        cfg_hash,
                    ]
                )
                groups.setdefault(key, []).append(r)

            to_remove: List[str] = []
            deduped_groups = 0
            for _k, bucket in groups.items():
                if len(bucket) <= 1:
                    continue

                deduped_groups += 1
                bucket_ids = [str(b["id"]) for b in bucket]

                # Prefer keeping a run that is already referenced by a live bot.
                keep_id: Optional[str] = None
                try:
                    placeholders = ",".join(["%s"] * len(bucket_ids))
                    ref = execute_fetchone(
                        conn,
                        f"SELECT run_id FROM bot_run_map WHERE run_id IN ({placeholders}) LIMIT 1",
                        tuple(bucket_ids),
                    )
                    if ref:
                        keep_id = str(ref.get("run_id") or "")
                except DbErrors.OperationalError:
                    keep_id = None

                if not keep_id:
                    keep_id = str(bucket[0]["id"])  # earliest created_at

                for rid in bucket_ids:
                    if rid != keep_id:
                        to_remove.append(rid)

            removed = 0
            if to_remove:
                placeholders = ",".join(["%s"] * len(to_remove))
                with conn:
                    try:
                        conn.execute(
                            f"DELETE FROM backtest_run_details WHERE run_id IN ({placeholders})",
                            tuple(to_remove),
                        )
                    except Exception:
                        pass
                    cur = conn.execute(
                        f"DELETE FROM backtest_runs WHERE id IN ({placeholders})",
                        tuple(to_remove),
                    )
                    removed = int(cur.rowcount or 0)

            return {
                "sweep_id": sid,
                "total": total,
                "deduped_groups": deduped_groups,
                "removed": removed,
            }


def get_setting(conn: Any, key: str, default: Any = None) -> Any:
    row = execute_fetchone(conn, "SELECT value_json FROM app_settings WHERE key = %s", (key,))
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return default


def get_user_setting(conn: Any, user_id: str, key: str, default: Any = None) -> Any:
    _ensure_schema(conn)
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = execute_fetchone(
        conn,
        "SELECT value_json FROM user_settings WHERE user_id = %s AND key = %s",
        (uid, str(key)),
    )
    if not row:
        return default
    try:
        return json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return default


def set_user_setting(conn: Any, user_id: str, key: str, value: Any) -> None:
    _ensure_schema(conn)
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    payload = json.dumps(value)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO user_settings(user_id, key, value_json, updated_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(user_id, key) DO UPDATE
        SET value_json = excluded.value_json, updated_at = excluded.updated_at
        """,
        (uid, str(key), payload, now),
    )
    conn.commit()


def set_setting(conn: Any, key: str, value: Any) -> None:
    payload = json.dumps(value)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO app_settings(key, value_json, updated_at)
        VALUES (%s, %s, %s)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
        """,
        (key, payload, now),
    )
    conn.commit()


def get_settings(conn: Any) -> Dict[str, Any]:
    rows = execute_fetchall(conn, "SELECT key, value_json FROM app_settings")
    out: Dict[str, Any] = {}
    for row in rows:
        try:
            out[row["key"]] = json.loads(row["value_json"])
        except (TypeError, json.JSONDecodeError):
            continue
    return out


def get_app_settings(conn: Any) -> Dict[str, Any]:
    defaults = {
        "initial_balance_default": 1000.0,
        "fee_spot_pct": 0.10,
        "fee_perps_pct": 0.06,
        "fee_stocks_pct": 0.10,
        "min_net_return_pct": 20.0,
        "jobs_refresh_seconds": 2.0,
        "prefer_bbo_maker": True,
        "bbo_queue_level": 1,
        "live_kill_switch_enabled": False,
        "live_trading_enabled": False,
    }
    current = get_settings(conn)
    merged = {**defaults, **current}

    # Backfill missing keys for new installs/upgrades.
    try:
        if "live_trading_enabled" not in current:
            set_setting(conn, "live_trading_enabled", bool(defaults["live_trading_enabled"]))
    except Exception:
        pass

    # Backfill Settings-page keys so they persist across restarts even if the
    # user hasn't opened the Settings page yet.
    for k in (
        "jobs_refresh_seconds",
        "prefer_bbo_maker",
        "bbo_queue_level",
    ):
        try:
            if k not in current:
                set_setting(conn, k, defaults[k])
        except Exception:
            pass

    # Backward compatibility: if old fraction-based keys exist, map them to pct defaults when pct is missing.
    for old_key, pct_key in (
        ("fee_spot", "fee_spot_pct"),
        ("fee_perps", "fee_perps_pct"),
        ("fee_stocks", "fee_stocks_pct"),
    ):
        if pct_key not in merged and old_key in current:
            try:
                merged[pct_key] = float(current[old_key]) * 100.0
            except (TypeError, ValueError):
                continue
    return merged


def get_backtest_run_chart_artifacts(
    run_id: str,
    *,
    conn: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """Load stored chart artifacts for a run (if present).

    These are stored on `backtest_run_details` to avoid re-running backtests for chart rendering.
    Returns None when not available.
    """

    rid = str(run_id)
    connection = conn or _get_conn()

    # Guard for legacy DBs that don't have the columns yet.
    try:
        cols = get_table_columns(connection, "backtest_run_details")
    except Exception:
        cols = set()
    required = {"equity_curve_json", "equity_timestamps_json", "extra_series_json"}
    if not required.issubset(cols):
        if conn is None:
            connection.close()
        return None

    row = None
    try:
        select_cols = ["equity_curve_json", "equity_timestamps_json", "extra_series_json"]
        for opt in ("candles_json", "params_json", "run_context_json", "computed_metrics_json"):
            if opt in cols:
                select_cols.append(opt)
        row = execute_fetchone(
            connection,
            f"SELECT {', '.join(select_cols)} FROM backtest_run_details WHERE run_id = %s",
            (rid,),
        )
    except DbErrors.OperationalError:
        row = None

    if conn is None:
        connection.close()

    if row is None:
        return None

    eq = None
    ts = None
    extra = None
    candles = None
    params = None
    run_context = None
    computed_metrics = None
    try:
        eq = json.loads(row["equity_curve_json"]) if row["equity_curve_json"] else None
    except (TypeError, json.JSONDecodeError):
        eq = None
    try:
        ts = json.loads(row["equity_timestamps_json"]) if row["equity_timestamps_json"] else None
    except (TypeError, json.JSONDecodeError):
        ts = None
    try:
        extra = json.loads(row["extra_series_json"]) if row["extra_series_json"] else None
    except (TypeError, json.JSONDecodeError):
        extra = None

    # Optional artifacts
    try:
        if "candles_json" in row.keys():
            candles = json.loads(row["candles_json"]) if row["candles_json"] else None
    except (TypeError, json.JSONDecodeError):
        candles = None
    try:
        if "params_json" in row.keys():
            params = json.loads(row["params_json"]) if row["params_json"] else None
    except (TypeError, json.JSONDecodeError):
        params = None
    try:
        if "run_context_json" in row.keys():
            run_context = json.loads(row["run_context_json"]) if row["run_context_json"] else None
    except (TypeError, json.JSONDecodeError):
        run_context = None
    try:
        if "computed_metrics_json" in row.keys():
            computed_metrics = json.loads(row["computed_metrics_json"]) if row["computed_metrics_json"] else None
    except (TypeError, json.JSONDecodeError):
        computed_metrics = None

    if not isinstance(eq, list) or not eq:
        return None
    if ts is not None and not isinstance(ts, list):
        ts = None
    if extra is not None and not isinstance(extra, dict):
        extra = None

    if candles is not None and not isinstance(candles, list):
        candles = None
    if params is not None and not isinstance(params, dict):
        params = None
    if run_context is not None and not isinstance(run_context, dict):
        run_context = None
    if computed_metrics is not None and not isinstance(computed_metrics, dict):
        computed_metrics = None

    return {
        "equity_curve": eq,
        "equity_timestamps": ts,
        "extra_series": extra or {},
        "candles": candles or [],
        "params": params or {},
        "run_context": run_context or {},
        "computed_metrics": computed_metrics or {},
    }


def set_backtest_run_chart_artifacts(
    conn: Any,
    run_id: str,
    *,
    equity_curve: Optional[list],
    equity_timestamps: Optional[list],
    extra_series: Optional[dict],
) -> None:
    """Upsert chart artifacts onto backtest_run_details for an existing run."""

    _ensure_schema(conn)
    rid = str(run_id)

    try:
        cols = get_table_columns(conn, "backtest_run_details")
    except Exception:
        cols = set()
    required = {"equity_curve_json", "equity_timestamps_json", "extra_series_json", "updated_at"}
    if not required.issubset(cols):
        return

    now = datetime.now(timezone.utc).isoformat()
    eq_payload = json.dumps(_sanitize_for_json(equity_curve)) if equity_curve is not None else None
    ts_payload = json.dumps(_sanitize_for_json(equity_timestamps)) if equity_timestamps is not None else None
    extra_payload = json.dumps(_sanitize_for_json(extra_series)) if extra_series is not None else None

    conn.execute(
        """
        UPDATE backtest_run_details
        SET equity_curve_json = COALESCE(%s, equity_curve_json),
            equity_timestamps_json = COALESCE(%s, equity_timestamps_json),
            extra_series_json = COALESCE(%s, extra_series_json),
            updated_at = %s
        WHERE run_id = %s
        """,
        (eq_payload, ts_payload, extra_payload, now, rid),
    )
    conn.commit()


def get_setting_bool(conn: Any, key: str, default: bool = False) -> bool:
    val = get_setting(conn, key, default)
    if isinstance(val, bool):
        return val
    if val is None:
        return bool(default)
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        v = val.strip().lower()
        if v in {"1", "true", "yes", "on"}:
            return True
        if v in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def set_setting_bool(conn: Any, key: str, value: bool) -> None:
    set_setting(conn, key, bool(value))


def create_job(
    conn: Any,
    job_type: str,
    payload: Dict[str, Any],
    bot_id: Optional[int] = None,
    *,
    sweep_id: Optional[str] = None,
    run_id: Optional[str] = None,
    job_key: Optional[str] = None,
    parent_job_id: Optional[int] = None,
    group_key: Optional[str] = None,
    priority: Optional[int] = None,
    is_interactive: Optional[int] = None,
) -> int:
    cur = None
    try:
        now = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload)

        # Backwards compatible: older DBs may not have newer columns.
        try:
            job_cols = get_table_columns(conn, "jobs")
        except Exception:
            job_cols = set()

        cols = ["job_type", "status", "payload_json", "progress", "message", "created_at", "updated_at"]
        vals: list[Any] = [str(job_type), "queued", payload_json, 0.0, "", now, now]

        if "job_key" in job_cols:
            cols.insert(1, "job_key")
            vals.insert(1, (str(job_key).strip() if job_key is not None else None))

        if "sweep_id" in job_cols:
            cols.insert(4 if "job_key" in job_cols else 3, "sweep_id")
            vals.insert(4 if "job_key" in job_cols else 3, (str(sweep_id) if sweep_id is not None else None))
        if "run_id" in job_cols:
            cols.insert(5 if "job_key" in job_cols else 4, "run_id")
            vals.insert(5 if "job_key" in job_cols else 4, (str(run_id) if run_id is not None else None))
        if "bot_id" in job_cols:
            cols.insert(6 if "job_key" in job_cols else 5, "bot_id")
            vals.insert(6 if "job_key" in job_cols else 5, bot_id)

        # Optional planner/group linkage (older DBs may not have these columns).
        if "parent_job_id" in job_cols:
            cols.append("parent_job_id")
            vals.append(int(parent_job_id) if parent_job_id is not None else None)
        if "group_key" in job_cols:
            cols.append("group_key")
            vals.append(str(group_key).strip() if group_key is not None else None)

        # Optional job scheduling fields.
        if priority is not None and "priority" in job_cols:
            cols.append("priority")
            vals.append(int(priority))
        if is_interactive is not None and "is_interactive" in job_cols:
            cols.append("is_interactive")
            vals.append(1 if int(is_interactive) else 0)

        cols_sql = ", ".join(cols)
        placeholders = sql_placeholders(conn, len(cols))

        if is_postgres(conn):
            if job_key is not None and "job_key" in job_cols:
                cur = conn.execute(
                    f"""
                    INSERT INTO jobs ({cols_sql})
                    VALUES ({placeholders})
                    ON CONFLICT (job_key) DO NOTHING
                    RETURNING id
                    """,
                    tuple(vals),
                )
            else:
                cur = conn.execute(
                    f"""
                    INSERT INTO jobs ({cols_sql})
                    VALUES ({placeholders})
                    RETURNING id
                    """,
                    tuple(vals),
                )
            row = cur.fetchone()
            conn.commit()
            if row is not None:
                return int(row[0])
        # If ignored due to UNIQUE(job_key), return existing id.
        if cur is not None and cur.rowcount == 0 and job_key is not None and "job_key" in job_cols:
            row = conn.execute(
                f"SELECT id FROM jobs WHERE job_key = {sql_placeholder(conn)} LIMIT 1",
                (str(job_key).strip(),),
            ).fetchone()
            if row is not None:
                try:
                    return int(row[0])
                except Exception:
                    pass
        if cur is not None:
            try:
                return int(cur.lastrowid)
            except Exception:
                pass
        raise ValueError("Failed to create job")
    except Exception:
        _rollback_quietly(conn)
        logger.exception("create_job failed for job_type=%s job_key=%s", job_type, job_key)
        raise


def create_bot(
    conn: Any,
    *,
    name: Optional[str],
    exchange_id: Optional[str],
    symbol: Optional[str],
    timeframe: Optional[str],
    config: Any,
    status: str = "created",
    desired_status: str = "running",
    heartbeat_msg: Optional[str] = None,
    last_error: Optional[str] = None,
    user_id: Optional[str] = None,
    credentials_id: Optional[int] = None,
    account_id: Optional[int] = None,
) -> int:
    now = now_utc_iso()
    resolved_user_id = (user_id or "").strip() or DEFAULT_USER_EMAIL
    try:
        get_or_create_user(conn, resolved_user_id)
    except Exception:
        pass
    payload = {
        "name": name,
        "exchange_id": exchange_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "status": status,
        "desired_status": desired_status,
        "config_json": json.dumps(_sanitize_for_json(config) if config is not None else {}),
        "heartbeat_at": now,
        "heartbeat_msg": heartbeat_msg,
        "last_error": last_error,
        "created_at": now,
        "updated_at": now,
        "user_id": resolved_user_id,
        "credentials_id": credentials_id,
        "account_id": account_id,
    }
    bot_cols = get_table_columns(conn, "bots")
    cols = [
        "name",
        "exchange_id",
        "symbol",
        "timeframe",
        "status",
        "desired_status",
        "config_json",
        "heartbeat_at",
        "heartbeat_msg",
        "last_error",
        "created_at",
        "updated_at",
        "user_id",
        "credentials_id",
    ]
    if "account_id" in bot_cols:
        cols.append("account_id")

    values = [payload.get(c) for c in cols]
    placeholders = sql_placeholders(conn, len(cols))
    sql = f"INSERT INTO bots ({', '.join(cols)}) VALUES ({placeholders})"
    if is_postgres(conn):
        sql += " RETURNING id"
        cur = conn.execute(sql, tuple(values))
        row = cur.fetchone()
        conn.commit()
        if row is not None:
            return int(row[0])
        raise ValueError("Failed to create bot")

    cur = conn.execute(sql, tuple(values))
    conn.commit()
    return int(cur.lastrowid)


def list_trading_accounts(
    conn: Any,
    user_id: str,
    exchange_id: Optional[str] = None,
    *,
    include_deleted: bool = False,
) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    where = ["user_id = %s"]
    params: list[Any] = [uid]
    if exchange_id is not None:
        where.append("exchange_id = %s")
        params.append(str(exchange_id).strip().lower())
    if not include_deleted:
        where.append("status != 'deleted'")
    rows = execute_fetchall(
        conn,
        f"SELECT * FROM trading_accounts WHERE {' AND '.join(where)} ORDER BY updated_at DESC, id DESC",
        tuple(params),
    )
    return [dict(r) for r in rows]


def create_trading_account(
    conn: Any,
    *,
    user_id: str,
    exchange_id: str,
    label: str,
    credential_id: int,
) -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    get_or_create_user(conn, uid)
    now = now_utc_iso()
    ex = str(exchange_id or "").strip().lower()
    lbl = (label or "").strip()
    if not ex:
        raise ValueError("exchange_id is required")
    if not lbl:
        raise ValueError("label is required")
    cred_id = int(credential_id)
    cur = conn.execute(
        f"""
        INSERT INTO trading_accounts(
            user_id, exchange_id, label, credential_id, status, created_at, updated_at
        ) VALUES ({sql_placeholders(conn, 7)})
        ON CONFLICT (user_id, exchange_id, label)
        DO UPDATE SET updated_at = EXCLUDED.updated_at
        RETURNING id
        """,
        (uid, ex, lbl, cred_id, "active", now, now),
    )
    row = cur.fetchone()
    conn.commit()
    if row is None:
        raise ValueError("Failed to create trading account")
    return int(row[0])


def get_trading_account(conn: Any, user_id: str, account_id: int) -> Optional[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = execute_fetchone(
        conn,
        "SELECT * FROM trading_accounts WHERE user_id = %s AND id = %s",
        (uid, int(account_id)),
    )
    return dict(row) if row else None


def set_trading_account_status(conn: Any, user_id: str, account_id: int, status: str) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    st = str(status or "").strip().lower()
    if st not in {"active", "disabled", "deleted"}:
        raise ValueError("Invalid status")
    now = now_utc_iso()
    conn.execute(
        "UPDATE trading_accounts SET status = %s, updated_at = %s WHERE user_id = %s AND id = %s",
        (st, now, uid, int(account_id)),
    )
    conn.commit()


def set_trading_account_credential(conn: Any, user_id: str, account_id: int, credential_id: int) -> None:
    """Rotate/update which credential a trading account points to."""
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    conn.execute(
        "UPDATE trading_accounts SET credential_id = %s, updated_at = %s WHERE user_id = %s AND id = %s",
        (int(credential_id), now, uid, int(account_id)),
    )
    conn.commit()


def count_bots_for_account(conn: Any, user_id: str, account_id: int) -> int:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = execute_fetchone(
        conn,
        "SELECT COUNT(1) AS c FROM bots WHERE user_id = %s AND account_id = %s",
        (uid, int(account_id)),
    )
    try:
        return int(row.get("c") if row else 0)
    except Exception:
        return 0


def get_bot_context(conn: Any, user_id: str, bot_id: int) -> Dict[str, Any]:
    """Fetch bot + its trading account (new source of truth).

    Returns {"bot": {...}, "account": {...}|None}.
    """
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = execute_fetchone(
        conn,
        """
        SELECT
            b.*, 
            a.id AS a_id,
            a.user_id AS a_user_id,
            a.exchange_id AS a_exchange_id,
            a.label AS a_label,
            a.credential_id AS a_credential_id,
            a.status AS a_status,
            a.risk_block_new_entries AS a_risk_block_new_entries,
            a.risk_max_daily_loss_usd AS a_risk_max_daily_loss_usd,
            a.risk_max_total_loss_usd AS a_risk_max_total_loss_usd,
            a.created_at AS a_created_at,
            a.updated_at AS a_updated_at
        FROM bots b
        LEFT JOIN trading_accounts a ON a.id = b.account_id
        WHERE b.id = %s AND b.user_id = %s
        """,
        (int(bot_id), uid),
    )
    if not row:
        return {"bot": None, "account": None}

    data = dict(row)
    account = None
    if data.get("a_id") is not None:
        account = {
            "id": data.get("a_id"),
            "user_id": data.get("a_user_id"),
            "exchange_id": data.get("a_exchange_id"),
            "label": data.get("a_label"),
            "credential_id": data.get("a_credential_id"),
            "status": data.get("a_status"),
            "risk_block_new_entries": data.get("a_risk_block_new_entries"),
            "risk_max_daily_loss_usd": data.get("a_risk_max_daily_loss_usd"),
            "risk_max_total_loss_usd": data.get("a_risk_max_total_loss_usd"),
            "created_at": data.get("a_created_at"),
            "updated_at": data.get("a_updated_at"),
        }

    for k in [
        "a_id",
        "a_user_id",
        "a_exchange_id",
        "a_label",
        "a_credential_id",
        "a_status",
        "a_created_at",
        "a_updated_at",
        "a_risk_block_new_entries",
        "a_risk_max_daily_loss_usd",
        "a_risk_max_total_loss_usd",
    ]:
        data.pop(k, None)

    return {"bot": data, "account": account}


def create_credential(
    conn: Any,
    *,
    user_id: str,
    exchange_id: str,
    label: str,
    api_key: str,
    api_secret_plain: str,
) -> int:
    from project_dragon.crypto import encrypt_str

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    get_or_create_user(conn, uid)
    now = now_utc_iso()
    secret_enc = encrypt_str(api_secret_plain)
    cur = conn.execute(
        """
        INSERT INTO exchange_credentials(
            user_id, exchange_id, label, api_key, api_secret_enc,
            created_at, updated_at, last_used_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL)
        RETURNING id
        """,
        (uid, (exchange_id or "").strip(), (label or "").strip(), (api_key or "").strip(), secret_enc, now, now),
    )
    row = cur.fetchone()
    conn.commit()
    if row is None:
        raise ValueError("Failed to create credential")
    return int(row[0])


def update_trading_account_risk(
    conn: Any,
    user_id: str,
    account_id: int,
    *,
    risk_block_new_entries: Any = _UNSET,
    risk_max_daily_loss_usd: Any = _UNSET,
    risk_max_total_loss_usd: Any = _UNSET,
) -> None:
    """Update account-level risk settings.

    All fields are optional; only provided fields will be updated.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    fields: Dict[str, Any] = {}

    if risk_block_new_entries is not _UNSET:
        if risk_block_new_entries is not None:
            fields["risk_block_new_entries"] = 1 if bool(risk_block_new_entries) else 0

    if risk_max_daily_loss_usd is not _UNSET:
        if risk_max_daily_loss_usd is None:
            fields["risk_max_daily_loss_usd"] = None
        else:
            fields["risk_max_daily_loss_usd"] = float(risk_max_daily_loss_usd)

    if risk_max_total_loss_usd is not _UNSET:
        if risk_max_total_loss_usd is None:
            fields["risk_max_total_loss_usd"] = None
        else:
            fields["risk_max_total_loss_usd"] = float(risk_max_total_loss_usd)
    if not fields:
        return
    fields["updated_at"] = now_utc_iso()
    sets = ", ".join([f"{k} = %s" for k in fields.keys()])
    params = list(fields.values()) + [uid, int(account_id)]
    conn.execute(
        f"UPDATE trading_accounts SET {sets} WHERE user_id = %s AND id = %s",
        params,
    )
    conn.commit()


def sum_account_net_pnl(
    conn: Any,
    user_id: str,
    account_id: int,
    *,
    since_ts: Optional[str] = None,
) -> float:
    """Return net PnL for an account across all its bots.

    net = SUM(pnl) + SUM(funding) - SUM(fee)
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    where = ["b.user_id = %s", "b.account_id = %s"]
    params: List[Any] = [uid, int(account_id)]
    if since_ts:
        where.append("l.event_ts >= %s")
        params.append(str(since_ts))
    row = execute_fetchone(
        conn,
        f"""
        SELECT
            COALESCE(SUM(l.pnl), 0) + COALESCE(SUM(l.funding), 0) - COALESCE(SUM(l.fee), 0) AS net
        FROM bots b
        JOIN bot_ledger l ON l.bot_id = b.id
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    )
    try:
        return float(row.get("net") if row and row.get("net") is not None else 0.0)
    except Exception:
        return 0.0


def request_flatten_all_bots_in_account(
    conn: Any,
    user_id: str,
    account_id: int,
    *,
    include_stopped: bool = False,
) -> int:
    """Set desired_action='flatten_now' for all bots under an account.

    Returns number of bots updated.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    where = "user_id = %s AND account_id = %s"
    params: List[Any] = [uid, int(account_id)]
    if not include_stopped:
        where += " AND COALESCE(desired_status, 'running') != 'stopped'"
    cur = conn.execute(
        f"UPDATE bots SET desired_action = 'flatten_now', updated_at = %s WHERE {where}",
        tuple([now] + params),
    )
    conn.commit()
    try:
        return int(cur.rowcount or 0)
    except Exception:
        return 0


def list_credentials(conn: Any, user_id: str) -> List[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    rows = execute_fetchall(
        conn,
        """
        SELECT id, user_id, exchange_id, label, api_key, created_at, updated_at, last_used_at
        FROM exchange_credentials
        WHERE user_id = %s
        ORDER BY updated_at DESC
        """,
        (uid,),
    )
    return [dict(r) for r in rows]


def get_credential(conn: Any, user_id: str, cred_id: int) -> Optional[Dict[str, Any]]:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    row = execute_fetchone(
        conn,
        """
        SELECT * FROM exchange_credentials
        WHERE user_id = %s AND id = %s
        """,
        (uid, int(cred_id)),
    )
    return dict(row) if row else None


def delete_credential(conn: Any, user_id: str, cred_id: int) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    conn.execute(
        "DELETE FROM exchange_credentials WHERE user_id = %s AND id = %s",
        (uid, int(cred_id)),
    )
    conn.commit()


def touch_credential_last_used(conn: Any, user_id: str, cred_id: int) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    now = now_utc_iso()
    conn.execute(
        """
        UPDATE exchange_credentials
        SET last_used_at = %s, updated_at = %s
        WHERE user_id = %s AND id = %s
        """,
        (now, now, uid, int(cred_id)),
    )
    conn.commit()


def set_bot_credentials(conn: Any, bot_id: int, *, user_id: str, credentials_id: Optional[int]) -> None:
    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    # Ensure bots.user_id is set for legacy rows.
    update_bot(conn, int(bot_id), user_id=uid, credentials_id=credentials_id)


def update_job(conn: Any, job_id: int, **fields: Any) -> None:
    if not fields:
        fields = {}
    try:
        now = datetime.now(timezone.utc).isoformat()
        fields = {**fields, "updated_at": now}
        columns = []
        params: list[Any] = []
        ph = sql_placeholder(conn)
        for key, value in fields.items():
            columns.append(f"{key} = {ph}")
            params.append(value)
        params.append(job_id)
        conn.execute(f"UPDATE jobs SET {', '.join(columns)} WHERE id = {ph}", tuple(params))
        conn.commit()
    except Exception:
        _rollback_quietly(conn)
        logger.exception("update_job failed for job_id=%s", job_id)
        raise


def list_jobs(conn: Any, limit: int = 200) -> List[Dict[str, Any]]:
    rows = execute_fetchall(
        conn,
        f"""
        SELECT * FROM jobs
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT {sql_placeholder(conn)}
        """,
        (limit,),
    )
    jobs: List[Dict[str, Any]] = []
    for r in rows:
        data = dict(r)
        if data.get("run_id") is not None:
            data["run_id"] = str(data["run_id"])
        jobs.append(data)
    return jobs


def get_job(conn: Any, job_id: int) -> Optional[Dict[str, Any]]:
    row = execute_fetchone(conn, f"SELECT * FROM jobs WHERE id = {sql_placeholder(conn)}", (job_id,))
    if not row:
        return None
    data = dict(row)
    if data.get("run_id") is not None:
        data["run_id"] = str(data["run_id"])
    return data


def claim_job(conn: Any, job_id: int, worker_id: str) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    # Atomic claim: only one worker can transition queued -> running.
    ph = sql_placeholder(conn)
    cur = conn.execute(
        f"""
        UPDATE jobs
        SET status='running', worker_id={ph}, started_at=COALESCE(started_at, {ph}), updated_at={ph}
        WHERE id = {ph} AND status = 'queued' AND (worker_id IS NULL OR worker_id = '')
        """,
        (worker_id, now, now, job_id),
    )
    conn.commit()
    return cur.rowcount > 0


def claim_job_with_lease(
    conn: Any,
    *,
    worker_id: str,
    lease_s: float,
    job_types: Sequence[str] = ("live_bot",),
) -> Optional[Dict[str, Any]]:
    """Claim the next queued job using a time-based lease.

    Conservative and minimal:
    - Claims one job of the allowed `job_types`
    - Transitions `queued` -> `running`
    - Sets both legacy `worker_id` and lease fields (`claimed_by`, `lease_expires_at`, ...)
    """

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    types = [str(t).strip() for t in (job_types or []) if str(t).strip()]
    if not types:
        types = ["live_bot"]

    placeholders = ",".join([sql_placeholder(conn)] * len(types))

    # Best-effort: if pause/cancel columns exist, avoid claiming backtest_run jobs
    # whose parent sweep job has pause/cancel requested.
    try:
        job_cols = get_table_columns(conn, "jobs")
    except Exception:
        job_cols = set()
    has_control_plane = {"pause_requested", "cancel_requested", "parent_job_id"}.issubset(job_cols)

    has_priority = "priority" in job_cols
    has_interactive = "is_interactive" in job_cols

    # Prefer interactive jobs (single runs) first, then lower priority, then oldest.
    order_parts: list[str] = []
    if has_interactive:
        order_parts.append("COALESCE(j.is_interactive, 0) DESC")
    if has_priority:
        order_parts.append("COALESCE(j.priority, 100) ASC")
    order_parts.append("COALESCE(j.created_at, j.updated_at) ASC")
    order_parts.append("j.id ASC")
    order_sql = ", ".join(order_parts)

    # Best-effort retry loop to handle races between multiple workers.
    for _ in range(3):

        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat()
        lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

        if has_control_plane and "backtest_run" in set(types):
            select_sql = (
                "SELECT j.id "
                "FROM jobs j "
                "WHERE j.job_type IN ("
                + placeholders
                + ") "
                "  AND j.status = 'queued' "
                "  AND ( "
                "    j.job_type != 'backtest_run' "
                "    OR j.parent_job_id IS NULL "
                "    OR NOT EXISTS ( "
                "      SELECT 1 FROM jobs p "
                "      WHERE p.id = j.parent_job_id "
                "        AND (COALESCE(p.pause_requested, 0) = 1 OR COALESCE(p.cancel_requested, 0) = 1) "
                "    ) "
                "  ) "
                "ORDER BY "
                + order_sql
                + " LIMIT 1"
            )
        else:
            select_sql = (
                "SELECT j.id "
                "FROM jobs j "
                "WHERE j.job_type IN ("
                + placeholders
                + ") "
                "  AND j.status = 'queued' "
                "ORDER BY "
                + order_sql
                + " LIMIT 1"
            )

        try:
            ph = sql_placeholder(conn)
            update_sql = f"""
                UPDATE jobs
                SET
                  status = 'running',
                  worker_id = {ph},
                  claimed_by = {ph},
                  claimed_at = {ph},
                  lease_expires_at = {ph},
                  last_lease_renew_at = {ph},
                  lease_version = COALESCE(lease_version, 0) + 1,
                  started_at = COALESCE(started_at, {ph}),
                  updated_at = {ph}
                WHERE id = ({select_sql})
                  AND status = 'queued'
                RETURNING id
            """
            cur = conn.execute(
                update_sql,
                (
                    str(worker_id),
                    str(worker_id),
                    now_iso,
                    lease_expires_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                    *tuple(types),
                ),
            )
            row = cur.fetchone()
            conn.commit()
            if row is not None:
                try:
                    return get_job(conn, int(row[0]))
                except Exception:
                    return None
        except DbErrors.OperationalError as exc:
            _rollback_quietly(conn)
            logger.debug("claim_job_with_lease RETURNING failed; falling back: %s", exc)
        except Exception:
            _rollback_quietly(conn)
            logger.exception("claim_job_with_lease failed during initial claim")
            return None

        # Fallback (no RETURNING): select then update.
        try:
            row2 = conn.execute(select_sql, tuple(types)).fetchone()
        except Exception:
            _rollback_quietly(conn)
            logger.exception("claim_job_with_lease failed selecting queued job")
            return None
        if not row2:
            return None
        try:
            job_id = int(row2[0])
        except Exception:
            return None

        ph = sql_placeholder(conn)
        try:
            cur2 = conn.execute(
                f"""
                UPDATE jobs
                SET
                  status = 'running',
                  worker_id = {ph},
                  claimed_by = {ph},
                  claimed_at = {ph},
                  lease_expires_at = {ph},
                  last_lease_renew_at = {ph},
                  lease_version = COALESCE(lease_version, 0) + 1,
                  started_at = COALESCE(started_at, {ph}),
                  updated_at = {ph}
                WHERE id = {ph}
                  AND status = 'queued'
                """,
                (
                    str(worker_id),
                    str(worker_id),
                    now_iso,
                    lease_expires_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                    job_id,
                ),
            )
            conn.commit()
        except Exception:
            _rollback_quietly(conn)
            logger.exception("claim_job_with_lease failed updating job %s", job_id)
            return None
        if cur2.rowcount > 0:
            return get_job(conn, job_id)

    return None


def bulk_enqueue_backtest_run_jobs(
    conn: Any,
    *,
    jobs: Sequence[Tuple[str, Dict[str, Any], Optional[str]]],
    parent_job_id: Optional[int] = None,
    group_key: Optional[str] = None,
) -> Dict[str, int]:
    """Bulk enqueue `backtest_run` jobs idempotently.

    Args:
      jobs: sequence of (job_key, payload_dict, sweep_id)

    Returns: {"created": int, "existing": int}

    Notes:
    - Uses a single SELECT to discover existing keys, then one executemany INSERT.
    - Requires `jobs.job_key` (added via schema migration). If missing, falls back
      to non-idempotent inserts.
    """

    try:
        try:
            job_cols = get_table_columns(conn, "jobs")
        except Exception:
            job_cols = set()

        created = 0
        existing = 0

        rows_in = [(str(k).strip(), p, (str(s) if s is not None else None)) for (k, p, s) in (jobs or []) if str(k).strip()]
        if not rows_in:
            return {"created": 0, "existing": 0}

        now = now_utc_iso()

        # No job_key support -> non-idempotent (legacy DB); still enqueue.
        if "job_key" not in job_cols:
            for k, payload, sid in rows_in:
                _ = create_job(
                    conn,
                    "backtest_run",
                    payload,
                    sweep_id=sid,
                    job_key=None,
                    priority=100,
                    is_interactive=0,
                )
                created += 1
            return {"created": int(created), "existing": 0}

        keys = [k for k, _p, _sid in rows_in]
        existing_keys: set[str] = set()
        chunk = 500
        for i in range(0, len(keys), chunk):
            part = keys[i : i + chunk]
            placeholders = ",".join(["%s"] * len(part))
            found = execute_fetchall(
                conn,
                f"SELECT job_key FROM jobs WHERE job_key IN ({placeholders})",
                tuple(part),
            )
            for r in found or []:
                try:
                    existing_keys.add(str(r.get("job_key") or "").strip())
                except Exception:
                    continue

        to_insert = [(k, payload, sid) for (k, payload, sid) in rows_in if k not in existing_keys]
        existing = len(rows_in) - len(to_insert)

        if not to_insert:
            return {"created": 0, "existing": int(existing)}

        has_parent = "parent_job_id" in job_cols
        has_group = "group_key" in job_cols
        has_priority = "priority" in job_cols
        has_interactive = "is_interactive" in job_cols

        payload_rows: list[tuple[Any, ...]] = []
        for k, payload, sid in to_insert:
            # Store job_key + deterministic run_key inside the payload for traceability.
            # (Safe to add; workers treat payload as opaque.)
            try:
                if not isinstance(payload, dict):
                    payload = {}
            except Exception:
                payload = {}
            payload = dict(payload)
            payload.setdefault("job_key", str(k))
            if sid is not None:
                payload.setdefault("sweep_id", str(sid))
            try:
                payload.setdefault("run_key", compute_run_key(payload))
            except Exception:
                # If the payload contains non-serializable values, worker will compute.
                pass
            row: list[Any] = [
                "backtest_run",
                str(k),
                "queued",
                json.dumps(payload),
                (str(sid) if sid is not None else None),
                None,
                None,
                0.0,
                "",
                now,
                now,
            ]
            if has_parent:
                row.append(int(parent_job_id) if parent_job_id is not None else None)
            if has_group:
                row.append(str(group_key).strip() if group_key is not None else None)
            if has_interactive:
                row.append(0)
            if has_priority:
                row.append(100)
            payload_rows.append(tuple(row))

        # Insert only missing keys.
        cols_sql = "job_type, job_key, status, payload_json, sweep_id, run_id, bot_id, progress, message, created_at, updated_at"
        placeholders = ", ".join(["%s"] * 11)
        if has_parent:
            cols_sql += ", parent_job_id"
            placeholders += ", %s"
        if has_group:
            cols_sql += ", group_key"
            placeholders += ", %s"
        if has_interactive:
            cols_sql += ", is_interactive"
            placeholders += ", %s"
        if has_priority:
            cols_sql += ", priority"
            placeholders += ", %s"

        conn.executemany(
            f"""
            INSERT INTO jobs ({cols_sql})
            VALUES ({placeholders})
            """,
            payload_rows,
        )
        conn.commit()
        created = len(to_insert)
        return {"created": int(created), "existing": int(existing)}
    except Exception:
        _rollback_quietly(conn)
        logger.exception("bulk_enqueue_backtest_run_jobs failed")
        raise


def get_sweep_row(conn: Any, sweep_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a sweep row using an existing connection (worker-safe)."""

    sid = str(sweep_id or "").strip()
    if not sid:
        return None
    row = execute_fetchone(conn, "SELECT * FROM sweeps WHERE id = %s", (sid,))
    return dict(row) if row else None


def get_sweep_parent_jobs_by_sweep(
    conn: Any,
    *,
    sweep_ids: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    """Return mapping sweep_id -> parent sweep_parent job info.

    One query for all given sweep IDs.
    """

    sids = [str(s).strip() for s in (sweep_ids or []) if str(s).strip()]
    if not sids:
        return {}
    placeholders = ",".join(["%s"] * len(sids))
    rows = execute_fetchall(
        conn,
        f"""
                SELECT sweep_id,
                             id AS parent_job_id,
                             status,
                             COALESCE(pause_requested, 0) AS pause_requested,
                             COALESCE(cancel_requested, 0) AS cancel_requested,
                             paused_at,
                             cancelled_at,
                             group_key
        FROM jobs
        WHERE job_type = 'sweep_parent'
          AND sweep_id IN ({placeholders})
        """,
        tuple(sids),
        )

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        try:
            sid = str(r["sweep_id"])
            out[sid] = {
                "parent_job_id": int(r["parent_job_id"]),
                "status": str(r["status"] or ""),
                "pause_requested": int(r["pause_requested"] or 0),
                "cancel_requested": int(r["cancel_requested"] or 0),
                "paused_at": r["paused_at"],
                "cancelled_at": r["cancelled_at"],
                "group_key": r["group_key"],
            }
        except Exception:
            continue
    return out


def get_parent_job_flags(conn: Any, parent_job_id: int) -> Dict[str, Any]:
    """Return pause/cancel flags + timestamps for a parent job id."""

    
    try:
        pid = int(parent_job_id)
    except Exception:
        return {"pause_requested": 0, "cancel_requested": 0, "paused_at": None, "cancelled_at": None}
    if pid <= 0:
        return {"pause_requested": 0, "cancel_requested": 0, "paused_at": None, "cancelled_at": None}

    row = execute_fetchone(
        conn,
        """
        SELECT COALESCE(pause_requested, 0) AS pause_requested,
               COALESCE(cancel_requested, 0) AS cancel_requested,
               paused_at,
               cancelled_at,
               status,
               group_key
        FROM jobs
        WHERE id = %s
        LIMIT 1
        """,
        (pid,),
    )
    if not row:
        return {"pause_requested": 0, "cancel_requested": 0, "paused_at": None, "cancelled_at": None}

    try:
        return {
            "pause_requested": int(row["pause_requested"] or 0),
            "cancel_requested": int(row["cancel_requested"] or 0),
            "paused_at": row["paused_at"],
            "cancelled_at": row["cancelled_at"],
            "status": str(row["status"] or ""),
            "group_key": row["group_key"],
        }
    except Exception:
        return {"pause_requested": 0, "cancel_requested": 0, "paused_at": None, "cancelled_at": None}


def set_job_pause_requested(conn: Any, job_id: int, paused: bool) -> None:
    """Request pause/resume for a job (typically sweep_parent).

    - paused=True: sets pause_requested=1 and stamps paused_at if not set.
    - paused=False (resume): sets pause_requested=0; if status=='paused' moves back to queued.
    """

    _ensure_schema(conn)
    jid = int(job_id)
    now = now_utc_iso()
    if paused:
        conn.execute(
            """
            UPDATE jobs
            SET pause_requested = 1,
                paused_at = COALESCE(paused_at, %s),
                updated_at = %s
            WHERE id = %s
            """,
            (now, now, jid),
        )
    else:
        conn.execute(
            """
            UPDATE jobs
            SET pause_requested = 0,
                updated_at = %s,
                status = CASE WHEN status = 'paused' THEN 'queued' ELSE status END
            WHERE id = %s
            """,
            (now, jid),
        )
    conn.commit()


def bulk_mark_jobs_cancelled(conn: Any, group_key: str) -> int:
    """Mark queued jobs in a group as cancelled (best-effort v1)."""

    _ensure_schema(conn)
    gk = str(group_key or "").strip()
    if not gk:
        return 0
    now = now_utc_iso()
    cur = conn.execute(
        """
        UPDATE jobs
        SET status = 'cancelled',
            cancel_requested = COALESCE(cancel_requested, 0),
            cancelled_at = COALESCE(cancelled_at, %s),
            updated_at = %s
        WHERE group_key = %s
          AND status = 'queued'
        """,
        (now, now, gk),
    )
    conn.commit()
    return int(cur.rowcount or 0)


def set_job_cancel_requested(conn: Any, job_id: int, cancelled: bool) -> None:
    """Request cancel for a job (typically sweep_parent).

    cancelled=True: sets cancel_requested=1 and stamps cancelled_at.
    cancelled=False: clears cancel_requested (not typically used).
    """

    _ensure_schema(conn)
    jid = int(job_id)
    now = now_utc_iso()
    if cancelled:
        # Ensure a group_key exists for sweep parents where possible.
        row = execute_fetchone(conn, "SELECT group_key, sweep_id FROM jobs WHERE id = %s", (jid,))
        gk = None
        sid = None
        if row is not None:
            try:
                gk = str(row.get("group_key") or "").strip()
            except Exception:
                gk = None
            try:
                sid = str(row.get("sweep_id") or "").strip()
            except Exception:
                sid = None
        if not gk and sid:
            gk = f"sweep:{sid}"
            try:
                conn.execute(
                    "UPDATE jobs SET group_key = COALESCE(NULLIF(group_key,''), %s) WHERE id = %s",
                    (gk, jid),
                )
            except Exception:
                pass

        conn.execute(
            """
            UPDATE jobs
            SET cancel_requested = 1,
                cancelled_at = COALESCE(cancelled_at, %s),
                pause_requested = 0,
                updated_at = %s
            WHERE id = %s
            """,
            (now, now, jid),
        )
        conn.commit()
        if gk:
            _ = bulk_mark_jobs_cancelled(conn, gk)
    else:
        conn.execute(
            """
            UPDATE jobs
            SET cancel_requested = 0,
                updated_at = %s
            WHERE id = %s
            """,
            (now, jid),
        )
        conn.commit()


def count_jobs_by_status_for_group(conn: Any, group_key: str) -> Dict[str, int]:
    """Aggregate job status counts for a group_key (single query)."""

    gk = str(group_key or "").strip()
    if not gk:
        return {}
    rows = execute_fetchall(
        conn,
        """
        SELECT status, COUNT(*) AS c
        FROM jobs
        WHERE group_key = %s
        GROUP BY status
        """,
        (gk,),
    )
    out: Dict[str, int] = {}
    total = 0
    for r in rows or []:
        try:
            st = str(r["status"])
            c = int(r["c"])
        except Exception:
            continue
        out[st] = int(c)
        total += int(c)
    out["total"] = int(total)
    return out


def get_backtest_run_job_counts_by_parent(
    conn: Any,
    *,
    parent_job_ids: Sequence[int],
    job_type: str = "backtest_run",
) -> Dict[int, Dict[str, int]]:
    """Return per-parent job status counts.

    One query (grouped by parent_job_id,status).
    Returns mapping: parent_job_id -> {"total": n, "queued": n, "running": n, "done": n, ...}
    """

    pids: list[int] = []
    for v in (parent_job_ids or []):
        try:
            pids.append(int(v))
        except Exception:
            continue
    pids = [p for p in pids if p > 0]
    if not pids:
        return {}
    placeholders = ",".join(["%s"] * len(pids))
    rows = execute_fetchall(
        conn,
        f"""
        SELECT parent_job_id, status, COUNT(*) AS c
        FROM jobs
        WHERE job_type = %s
          AND parent_job_id IN ({placeholders})
        GROUP BY parent_job_id, status
        """,
        tuple([str(job_type)] + pids),
    )

    out: Dict[int, Dict[str, int]] = {}
    for r in rows or []:
        try:
            pid = int(r.get("parent_job_id"))
            st = str(r.get("status"))
            c = int(r.get("c"))
        except Exception:
            continue
        d = out.setdefault(pid, {})
        d[st] = int(c)

    for pid in pids:
        d = out.setdefault(pid, {})
        total = 0
        for k, v in d.items():
            if k == "total":
                continue
            try:
                total += int(v or 0)
            except Exception:
                continue
        d["total"] = int(total)
    return out


def _canonical_json_dumps(obj: Any) -> str:
    """Canonical JSON for hashing/idempotency.

    - Stable key ordering
    - Compact separators
    - Sanitizes dataclasses/enums/other non-JSON types via `_sanitize_for_json`
    """

    return json.dumps(_sanitize_for_json(obj), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_run_key(payload: Dict[str, Any]) -> str:
    """Compute deterministic run_key for a backtest_run job payload.

    The output is a SHA256 hex digest of a canonical JSON blob built from stable fields.
    """

    if not isinstance(payload, dict):
        payload = {}

    data_settings = payload.get("data_settings") if isinstance(payload.get("data_settings"), dict) else {}
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}

    user_id = payload.get("user_id")
    if user_id is None:
        user_id = metadata.get("user_id")

    sweep_id = payload.get("sweep_id")
    if sweep_id is None:
        sweep_id = metadata.get("sweep_id")

    exchange_id = data_settings.get("exchange_id") or metadata.get("exchange_id")
    market_type = data_settings.get("market_type") or metadata.get("market_type") or metadata.get("market")
    symbol = data_settings.get("symbol") or metadata.get("symbol")
    timeframe = data_settings.get("timeframe") or metadata.get("timeframe")

    strategy_name = metadata.get("strategy_name") or payload.get("strategy_name")
    strategy_version = metadata.get("strategy_version") or payload.get("strategy_version")

    # Normalize config by parsing/re-dumping with sorted keys.
    cfg = payload.get("config")
    if isinstance(cfg, str) and cfg.strip():
        try:
            cfg_obj = json.loads(cfg)
        except Exception:
            cfg_obj = cfg
    else:
        cfg_obj = cfg
    cfg_norm = _canonical_json_dumps(cfg_obj)
    try:
        cfg_norm_obj = json.loads(cfg_norm)
    except Exception:
        cfg_norm_obj = cfg_norm

    range_mode = data_settings.get("range_mode")
    range_params = data_settings.get("range_params") if isinstance(data_settings.get("range_params"), dict) else {}

    # Include data-source flags that affect results.
    stable_blob = {
        "user_id": (str(user_id).strip() if user_id is not None else None),
        "sweep_id": (str(sweep_id).strip() if sweep_id is not None else None),
        "exchange_id": (str(exchange_id).strip() if exchange_id is not None else None),
        "market_type": (str(market_type).strip().lower() if market_type is not None else None),
        "symbol": (str(symbol).strip() if symbol is not None else None),
        "timeframe": (str(timeframe).strip() if timeframe is not None else None),
        "strategy_name": (str(strategy_name).strip() if strategy_name is not None else None),
        "strategy_version": (str(strategy_version).strip() if strategy_version is not None else None),
        "config": cfg_norm_obj,
        "range_mode": (str(range_mode).strip() if range_mode is not None else None),
        "range_params": range_params,
        "data_source": (str(data_settings.get("data_source") or "").strip() or None),
        "initial_balance": data_settings.get("initial_balance"),
        "fee_rate": data_settings.get("fee_rate"),
    }

    canonical = _canonical_json_dumps(stable_blob)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_backtest_run_id_by_run_key(conn: Any, run_key: str) -> Optional[str]:
    """Find existing backtest_runs.id by deterministic run_key stored in metadata_jsonb."""

    rk = str(run_key or "").strip()
    if not rk:
        return None
    try:
        row = execute_fetchone(
            conn,
            f"SELECT id FROM backtest_runs WHERE {_run_key_expr(conn)} = {sql_placeholder(conn)} LIMIT 1",
            (rk,),
        )
    except Exception:
        return None
    if not row:
        return None
    try:
        return str(row.get("id"))
    except Exception:
        return None


def upsert_backtest_run_by_run_key(
    conn: Any,
    *,
    run_key: str,
    run_row_fields: Dict[str, Any],
    details_fields: Dict[str, Any],
) -> str:
    """Upsert backtest run summary+details exactly-once by run_key.

    Important: this helper does NOT commit; call it inside your own transaction.
    """

    rk = str(run_key or "").strip()
    if not rk:
        raise ValueError("run_key is required")

    # Pull any existing row (for metadata merge + stable created_at).
    existing = None
    try:
        existing = execute_fetchone(
            conn,
            f"""
            SELECT id, created_at, metadata_json, metadata_jsonb
            FROM backtest_runs
            WHERE {_run_key_expr(conn)} = {sql_placeholder(conn)}
            LIMIT 1
            """,
            (rk,),
        )
    except Exception:
        existing = None

    incoming_meta_raw = run_row_fields.get("metadata_json")
    incoming_meta = _load_json_field(
        {"metadata_json": incoming_meta_raw, "metadata_jsonb": run_row_fields.get("metadata_jsonb")},
        "metadata",
    )
    if not isinstance(incoming_meta, dict):
        incoming_meta = {}

    # Always include run_key (+ job_key if present).
    incoming_meta["run_key"] = rk
    job_key = None
    try:
        job_key = incoming_meta.get("job_key")
    except Exception:
        job_key = None
    if not job_key:
        try:
            job_key = run_row_fields.get("job_key") or details_fields.get("job_key")
        except Exception:
            job_key = None
    if job_key:
        incoming_meta["job_key"] = str(job_key)

    if existing:
        run_id = str(existing.get("id") or "")
        created_at_existing = str(existing.get("created_at") or "")
        existing_meta = _load_json_field(existing, "metadata")
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        merged_meta = {**existing_meta, **incoming_meta}
        merged_meta["run_key"] = rk

        # Update summary row (keep created_at stable).
        # Only update known columns; ignore unknown keys.
        ph = sql_placeholder(conn)
        conn.execute(
            f"""
            UPDATE backtest_runs
            SET symbol = {ph}, timeframe = {ph}, strategy_name = {ph}, strategy_version = {ph},
                config_json = {ph}, config_jsonb = {ph},
                metrics_json = {ph}, metrics_jsonb = {ph},
                metadata_json = {ph}, metadata_jsonb = {ph},
                start_time = {ph}, end_time = {ph},
                net_profit = {ph}, net_return_pct = {ph}, roi_pct_on_margin = {ph}, max_drawdown_pct = {ph},
                sharpe = {ph}, sortino = {ph}, win_rate = {ph}, profit_factor = {ph},
                cpc_index = {ph}, common_sense_ratio = {ph}, avg_position_time_s = {ph}, trades_json = {ph}, trades_jsonb = {ph},
                sweep_id = {ph}, market_type = {ph}
            WHERE id = {ph}
            """,
            (
                run_row_fields.get("symbol"),
                run_row_fields.get("timeframe"),
                run_row_fields.get("strategy_name"),
                run_row_fields.get("strategy_version"),
                run_row_fields.get("config_json"),
                to_jsonb(run_row_fields.get("config_jsonb")),
                run_row_fields.get("metrics_json"),
                to_jsonb(run_row_fields.get("metrics_jsonb")),
                json.dumps(_sanitize_for_json(merged_meta)),
                to_jsonb(merged_meta),
                run_row_fields.get("start_time"),
                run_row_fields.get("end_time"),
                run_row_fields.get("net_profit"),
                run_row_fields.get("net_return_pct"),
                run_row_fields.get("roi_pct_on_margin"),
                run_row_fields.get("max_drawdown_pct"),
                run_row_fields.get("sharpe"),
                run_row_fields.get("sortino"),
                run_row_fields.get("win_rate"),
                run_row_fields.get("profit_factor"),
                run_row_fields.get("cpc_index"),
                run_row_fields.get("common_sense_ratio"),
                run_row_fields.get("avg_position_time_s"),
                run_row_fields.get("trades_json"),
                to_jsonb(run_row_fields.get("trades_jsonb")),
                run_row_fields.get("sweep_id"),
                run_row_fields.get("market_type"),
                run_id,
            ),
        )

        # Keep details table in sync.
        now_iso = now_utc_iso()
        details_meta_obj = _load_json_field(
            {"metadata_json": details_fields.get("metadata_json"), "metadata_jsonb": details_fields.get("metadata_jsonb")},
            "metadata",
        )
        if isinstance(details_meta_obj, dict):
            details_meta = {**existing_meta, **details_meta_obj, "run_key": rk}
            if job_key:
                details_meta["job_key"] = str(job_key)
            details_meta_json = json.dumps(_sanitize_for_json(details_meta))
        else:
            details_meta = dict(merged_meta)
            details_meta_json = json.dumps(_sanitize_for_json(details_meta))

        try:
            upsert_backtest_run_details(
                conn,
                (
                    "run_id",
                    "config_json",
                    "config_jsonb",
                    "metrics_json",
                    "metrics_jsonb",
                    "metadata_json",
                    "metadata_jsonb",
                    "trades_json",
                    "trades_jsonb",
                    "equity_curve_json",
                    "equity_curve_jsonb",
                    "equity_timestamps_json",
                    "equity_timestamps_jsonb",
                    "extra_series_json",
                    "extra_series_jsonb",
                    "candles_json",
                    "candles_jsonb",
                    "params_json",
                    "params_jsonb",
                    "run_context_json",
                    "run_context_jsonb",
                    "computed_metrics_json",
                    "computed_metrics_jsonb",
                    "created_at",
                    "updated_at",
                ),
                (
                    run_id,
                    details_fields.get("config_json"),
                    to_jsonb(details_fields.get("config_jsonb") or details_fields.get("config_json")),
                    details_fields.get("metrics_json"),
                    to_jsonb(details_fields.get("metrics_jsonb") or details_fields.get("metrics_json")),
                    details_meta_json,
                    to_jsonb(details_meta),
                    details_fields.get("trades_json"),
                    to_jsonb(details_fields.get("trades_jsonb") or details_fields.get("trades_json")),
                    details_fields.get("equity_curve_json"),
                    to_jsonb(details_fields.get("equity_curve_jsonb") or details_fields.get("equity_curve_json")),
                    details_fields.get("equity_timestamps_json"),
                    to_jsonb(details_fields.get("equity_timestamps_jsonb") or details_fields.get("equity_timestamps_json")),
                    details_fields.get("extra_series_json"),
                    to_jsonb(details_fields.get("extra_series_jsonb") or details_fields.get("extra_series_json")),
                    details_fields.get("candles_json"),
                    to_jsonb(details_fields.get("candles_jsonb") or details_fields.get("candles_json")),
                    details_fields.get("params_json"),
                    to_jsonb(details_fields.get("params_jsonb") or details_fields.get("params_json")),
                    details_fields.get("run_context_json"),
                    to_jsonb(details_fields.get("run_context_jsonb") or details_fields.get("run_context_json")),
                    details_fields.get("computed_metrics_json"),
                    to_jsonb(details_fields.get("computed_metrics_jsonb") or details_fields.get("computed_metrics_json")),
                    created_at_existing,
                    now_iso,
                ),
            )
        except Exception:
            try:
                upsert_backtest_run_details(
                    conn,
                    (
                        "run_id",
                        "config_json",
                        "metrics_json",
                        "metadata_json",
                        "trades_json",
                        "equity_curve_json",
                        "equity_timestamps_json",
                        "extra_series_json",
                        "candles_json",
                        "params_json",
                        "run_context_json",
                        "computed_metrics_json",
                        "created_at",
                        "updated_at",
                    ),
                    (
                        run_id,
                        details_fields.get("config_json"),
                        details_fields.get("metrics_json"),
                        details_meta_json,
                        details_fields.get("trades_json"),
                        details_fields.get("equity_curve_json"),
                        details_fields.get("equity_timestamps_json"),
                        details_fields.get("extra_series_json"),
                        details_fields.get("candles_json"),
                        details_fields.get("params_json"),
                        details_fields.get("run_context_json"),
                        details_fields.get("computed_metrics_json"),
                        created_at_existing,
                        now_iso,
                    ),
                )
            except Exception:
                upsert_backtest_run_details(
                    conn,
                    (
                        "run_id",
                        "config_json",
                        "metrics_json",
                        "metadata_json",
                        "trades_json",
                        "created_at",
                        "updated_at",
                    ),
                    (
                        run_id,
                        details_fields.get("config_json"),
                        details_fields.get("metrics_json"),
                        details_meta_json,
                        details_fields.get("trades_json"),
                        created_at_existing,
                        now_iso,
                    ),
                )

        return run_id

    # Insert new run.
    run_id = str(uuid4())
    created_at = str(run_row_fields.get("created_at") or now_utc_iso())
    merged_meta = dict(incoming_meta)
    merged_meta["run_key"] = rk
    if job_key:
        merged_meta["job_key"] = str(job_key)

    conn.execute(
        f"""
        INSERT INTO backtest_runs (
            id, created_at, symbol, timeframe,
            strategy_name, strategy_version,
            config_json, config_jsonb, metrics_json, metrics_jsonb, metadata_json, metadata_jsonb,
            market_type,
            start_time, end_time,
            net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
            sharpe, sortino, win_rate, profit_factor,
            cpc_index, common_sense_ratio, avg_position_time_s, trades_json, trades_jsonb,
            sweep_id
        )
        VALUES (
            {sql_placeholders(conn, 29)}
        )
        """,
        (
            run_id,
            created_at,
            run_row_fields.get("symbol"),
            run_row_fields.get("timeframe"),
            run_row_fields.get("strategy_name"),
            run_row_fields.get("strategy_version"),
            run_row_fields.get("config_json"),
            to_jsonb(run_row_fields.get("config_jsonb")),
            run_row_fields.get("metrics_json"),
            to_jsonb(run_row_fields.get("metrics_jsonb")),
            json.dumps(_sanitize_for_json(merged_meta)),
            to_jsonb(merged_meta),
            run_row_fields.get("market_type"),
            run_row_fields.get("start_time"),
            run_row_fields.get("end_time"),
            run_row_fields.get("net_profit"),
            run_row_fields.get("net_return_pct"),
            run_row_fields.get("roi_pct_on_margin"),
            run_row_fields.get("max_drawdown_pct"),
            run_row_fields.get("sharpe"),
            run_row_fields.get("sortino"),
            run_row_fields.get("win_rate"),
            run_row_fields.get("profit_factor"),
            run_row_fields.get("cpc_index"),
            run_row_fields.get("common_sense_ratio"),
            run_row_fields.get("avg_position_time_s"),
            run_row_fields.get("trades_json"),
            to_jsonb(run_row_fields.get("trades_jsonb")),
            run_row_fields.get("sweep_id"),
        ),
    )

    now_iso = now_utc_iso()
    details_meta_obj = _load_json_field(
        {"metadata_json": details_fields.get("metadata_json"), "metadata_jsonb": details_fields.get("metadata_jsonb")},
        "metadata",
    )
    if isinstance(details_meta_obj, dict):
        details_meta = dict(details_meta_obj)
        details_meta["run_key"] = rk
        if job_key:
            details_meta["job_key"] = str(job_key)
        details_meta_json = json.dumps(_sanitize_for_json(details_meta))
    else:
        details_meta = dict(merged_meta)
        details_meta_json = json.dumps(_sanitize_for_json(details_meta))

    try:
        upsert_backtest_run_details(
            conn,
            (
                "run_id",
                "config_json",
                "config_jsonb",
                "metrics_json",
                "metrics_jsonb",
                "metadata_json",
                "metadata_jsonb",
                "trades_json",
                "trades_jsonb",
                "equity_curve_json",
                "equity_curve_jsonb",
                "equity_timestamps_json",
                "equity_timestamps_jsonb",
                "extra_series_json",
                "extra_series_jsonb",
                "candles_json",
                "candles_jsonb",
                "params_json",
                "params_jsonb",
                "run_context_json",
                "run_context_jsonb",
                "computed_metrics_json",
                "computed_metrics_jsonb",
                "created_at",
                "updated_at",
            ),
            (
                run_id,
                details_fields.get("config_json"),
                to_jsonb(details_fields.get("config_jsonb") or details_fields.get("config_json")),
                details_fields.get("metrics_json"),
                to_jsonb(details_fields.get("metrics_jsonb") or details_fields.get("metrics_json")),
                details_meta_json,
                to_jsonb(details_meta),
                details_fields.get("trades_json"),
                to_jsonb(details_fields.get("trades_jsonb") or details_fields.get("trades_json")),
                details_fields.get("equity_curve_json"),
                to_jsonb(details_fields.get("equity_curve_jsonb") or details_fields.get("equity_curve_json")),
                details_fields.get("equity_timestamps_json"),
                to_jsonb(details_fields.get("equity_timestamps_jsonb") or details_fields.get("equity_timestamps_json")),
                details_fields.get("extra_series_json"),
                to_jsonb(details_fields.get("extra_series_jsonb") or details_fields.get("extra_series_json")),
                details_fields.get("candles_json"),
                to_jsonb(details_fields.get("candles_jsonb") or details_fields.get("candles_json")),
                details_fields.get("params_json"),
                to_jsonb(details_fields.get("params_jsonb") or details_fields.get("params_json")),
                details_fields.get("run_context_json"),
                to_jsonb(details_fields.get("run_context_jsonb") or details_fields.get("run_context_json")),
                details_fields.get("computed_metrics_json"),
                to_jsonb(details_fields.get("computed_metrics_jsonb") or details_fields.get("computed_metrics_json")),
                created_at,
                now_iso,
            ),
        )
    except Exception:
        try:
            upsert_backtest_run_details(
                conn,
                (
                    "run_id",
                    "config_json",
                    "metrics_json",
                    "metadata_json",
                    "trades_json",
                    "equity_curve_json",
                    "equity_timestamps_json",
                    "extra_series_json",
                    "candles_json",
                    "params_json",
                    "run_context_json",
                    "computed_metrics_json",
                    "created_at",
                    "updated_at",
                ),
                (
                    run_id,
                    details_fields.get("config_json"),
                    details_fields.get("metrics_json"),
                    details_meta_json,
                    details_fields.get("trades_json"),
                    details_fields.get("equity_curve_json"),
                    details_fields.get("equity_timestamps_json"),
                    details_fields.get("extra_series_json"),
                    details_fields.get("candles_json"),
                    details_fields.get("params_json"),
                    details_fields.get("run_context_json"),
                    details_fields.get("computed_metrics_json"),
                    created_at,
                    now_iso,
                ),
            )
        except Exception:
            upsert_backtest_run_details(
                conn,
                (
                    "run_id",
                    "config_json",
                    "metrics_json",
                    "metadata_json",
                    "trades_json",
                    "created_at",
                    "updated_at",
                ),
                (
                    run_id,
                    details_fields.get("config_json"),
                    details_fields.get("metrics_json"),
                    details_meta_json,
                    details_fields.get("trades_json"),
                    created_at,
                    now_iso,
                ),
            )

    return run_id


def finalize_sweep_if_complete(conn: Any, sweep_id: str) -> None:
    """Mark sweep done/failed when all child backtest_run jobs are terminal.

    Race-safe: updates only if the sweep is not already terminal.
    Does not commit.
    """

    sid = str(sweep_id or "").strip()
    if not sid:
        return
    rows = execute_fetchall(
        conn,
        """
        SELECT status, COUNT(*) AS c
        FROM jobs
        WHERE job_type = 'backtest_run'
            AND sweep_id = %s
        GROUP BY status
        """,
        (sid,),
    )

    counts: Dict[str, int] = {}
    total = 0
    for r in rows or []:
        st = str(r.get("status"))
        c = int(r.get("c"))
        counts[st] = int(c)
        total += int(c)

    if total <= 0:
        return

    queued = int(counts.get("queued") or 0)
    running = int(counts.get("running") or 0)
    if queued > 0 or running > 0:
        return

    failed = int(counts.get("failed") or 0) + int(counts.get("error") or 0)
    status = "failed" if failed > 0 else "done"
    err_msg = "one_or_more_child_jobs_failed" if failed > 0 else None

    conn.execute(
        """
        UPDATE sweeps
                SET status = %s, error_message = COALESCE(error_message, %s)
                WHERE id = %s
          AND status NOT IN ('done','failed','cancelled','canceled')
        """,
        (status, err_msg, sid),
    )


def get_backtest_run_job_counts_by_sweep(
    conn: Any,
    *,
    sweep_ids: Sequence[str],
    job_type: str = "backtest_run",
) -> Dict[str, Dict[str, int]]:
    """Return per-sweep job status counts for a set of sweep IDs.

    One query (grouped by sweep_id,status).
    Returns mapping: sweep_id -> {"total": n, "queued": n, "running": n, "done": n, "failed": n, ...}
    """

    sids = [str(s).strip() for s in (sweep_ids or []) if str(s).strip()]
    if not sids:
        return {}

    placeholders = ",".join(["%s"] * len(sids))
    rows = execute_fetchall(
        conn,
        f"""
        SELECT sweep_id, status, COUNT(*) AS c
        FROM jobs
        WHERE job_type = %s
          AND sweep_id IN ({placeholders})
        GROUP BY sweep_id, status
        """,
        tuple([str(job_type)] + sids),
    )

    out: Dict[str, Dict[str, int]] = {}
    for r in rows or []:
        sid = str(r.get("sweep_id") or "")
        st = str(r.get("status") or "")
        c = int(r.get("c") or 0) if r is not None else 0
        d = out.setdefault(sid, {})
        d[st] = int(c)

    # Add totals in-memory without additional queries.
    for sid in sids:
        d = out.setdefault(sid, {})
        total = 0
        for k, v in d.items():
            if k == "total":
                continue
            try:
                total += int(v or 0)
            except Exception:
                continue
        d["total"] = int(total)
    return out


def renew_job_lease(
    conn: Any,
    *,
    job_id: int,
    worker_id: str,
    lease_s: float,
    expected_lease_version: int,
) -> bool:
    """Renew a running job lease only if we still own it (version + owner check)."""

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

    cur = conn.execute(
        """
        UPDATE jobs
        SET
                    lease_expires_at = %s,
                    last_lease_renew_at = %s,
                    updated_at = %s,
                    worker_id = COALESCE(worker_id, %s),
                    claimed_by = COALESCE(claimed_by, %s)
                WHERE id = %s
          AND status = 'running'
                    AND claimed_by = %s
                    AND lease_version = %s
        """,
        (
            lease_expires_iso,
            now_iso,
            now_iso,
            str(worker_id),
            str(worker_id),
            int(job_id),
            str(worker_id),
            int(expected_lease_version),
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def reclaim_stale_job(
    conn: Any,
    *,
    job_id: int,
    new_worker_id: str,
    lease_s: float,
) -> bool:
    """Reclaim a stale (lease-expired) running job for this worker."""

    lease_seconds = float(lease_s or 0.0)
    if lease_seconds <= 0:
        lease_seconds = 30.0

    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()
    lease_expires_iso = (now_dt + timedelta(seconds=lease_seconds)).isoformat()

        # Only reclaim when lease_expires_at < now (ISO strings are comparable in Postgres).
    cur = conn.execute(
        """
        UPDATE jobs
        SET
                    worker_id = %s,
                    claimed_by = %s,
                    claimed_at = %s,
                    lease_expires_at = %s,
                    last_lease_renew_at = %s,
          lease_version = COALESCE(lease_version, 0) + 1,
          stale_reclaims = COALESCE(stale_reclaims, 0) + 1,
                    updated_at = %s
                WHERE id = %s
          AND status = 'running'
          AND lease_expires_at IS NOT NULL
                    AND lease_expires_at < %s
        """,
        (
            str(new_worker_id),
            str(new_worker_id),
            now_iso,
            lease_expires_iso,
            now_iso,
            now_iso,
            int(job_id),
            now_iso,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def update_bot(conn: Any, bot_id: int, **fields: Any) -> None:
    if not fields:
        return
    now = datetime.now(timezone.utc).isoformat()
    fields = {**fields, "updated_at": now}
    columns = []
    params: list[Any] = []
    ph = sql_placeholder(conn)
    for key, value in fields.items():
        columns.append(f"{key} = {ph}")
        params.append(value)
    params.append(bot_id)
    conn.execute(f"UPDATE bots SET {', '.join(columns)} WHERE id = {ph}", tuple(params))
    conn.commit()


def set_bot_desired_action(bot_id: int, desired_action: Optional[str]) -> None:
    """Convenience wrapper for Streamlit/UI code.

    Sets `bots.desired_action` and updates `updated_at`.
    """
    with open_db_connection() as conn:
        update_bot(conn, int(bot_id), desired_action=desired_action)


def increment_bot_blocked_actions(bot_id: int, delta: int = 1) -> None:
    """Increment bots.blocked_actions_count by delta (default 1).

    Must update updated_at.
    """
    try:
        delta_int = int(delta)
    except (TypeError, ValueError):
        delta_int = 1
    if delta_int == 0:
        return
    now = datetime.now(timezone.utc).isoformat()
    with open_db_connection() as conn:
        conn.execute(
            "UPDATE bots SET blocked_actions_count = COALESCE(blocked_actions_count, 0) + %s, updated_at = %s WHERE id = %s",
            (delta_int, now, int(bot_id)),
        )
        conn.commit()


def set_bot_status(
    conn: Any,
    bot_id: int,
    status: str,
    desired_status: Optional[str] = None,
    last_error: Optional[str] = None,
    heartbeat_msg: Optional[str] = None,
    heartbeat_at: Optional[str] = None,
) -> None:
    fields: Dict[str, Any] = {"status": status}
    if desired_status is not None:
        fields["desired_status"] = desired_status
    if last_error is not None:
        fields["last_error"] = last_error
    if heartbeat_msg is not None:
        fields["heartbeat_msg"] = heartbeat_msg
    fields["heartbeat_at"] = heartbeat_at or datetime.now(timezone.utc).isoformat()
    update_bot(conn, bot_id, **fields)


def get_bot(conn: Any, bot_id: int) -> Optional[Dict[str, Any]]:
    row = execute_fetchone(conn, f"SELECT * FROM bots WHERE id = {sql_placeholder(conn)}", (bot_id,))
    return dict(row) if row else None


def list_bots(conn: Any, limit: int = 200, status: Optional[str] = None) -> List[Dict[str, Any]]:
    query = "SELECT * FROM bots"
    params: list[Any] = []
    if status is not None:
        query += f" WHERE status = {sql_placeholder(conn)}"
        params.append(status)
    query += f" ORDER BY created_at DESC LIMIT {sql_placeholder(conn)}"
    params.append(limit)
    rows = execute_fetchall(conn, query, tuple(params))
    return [dict(r) for r in rows]

def list_bots_with_accounts(
    conn: Any,
    user_id: str,
    *,
    limit: int = 200,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List bots for a user with account label/status joined.

    This is a UI/helper query (bots → trading_accounts) so Live Bots can display account metadata.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    base = (
        "SELECT b.*, a.label AS account_label, a.status AS account_status "
        "FROM bots b "
        "LEFT JOIN trading_accounts a ON a.id = b.account_id "
        f"WHERE b.user_id = {sql_placeholder(conn)}"
    )
    params: List[Any] = [uid]
    if status:
        base += f" AND b.status = {sql_placeholder(conn)}"
        params.append(str(status))
    base += f" ORDER BY b.updated_at DESC LIMIT {sql_placeholder(conn)}"
    params.append(int(limit))
    rows = execute_fetchall(conn, base, params)
    return [dict(r) for r in rows]


def upsert_bot_snapshot(conn: Any, snapshot: Dict[str, Any]) -> None:
    """Upsert a durable bot snapshot.

    Must be safe to call frequently. Callers should throttle and treat failures as best-effort.
    """

    bot_id = snapshot.get("bot_id")
    if bot_id is None:
        raise ValueError("snapshot missing bot_id")

    fields = {
        "bot_id": int(bot_id),
        "user_id": str(snapshot.get("user_id") or "").strip() or DEFAULT_USER_EMAIL,
        "exchange_id": str(snapshot.get("exchange_id") or "").strip().lower() or "woox",
        "symbol": str(snapshot.get("symbol") or "").strip() or "",
        "timeframe": str(snapshot.get("timeframe") or "").strip() or "",
        "updated_at": str(snapshot.get("updated_at") or now_utc_iso()),
        "worker_id": snapshot.get("worker_id"),
        "status": snapshot.get("status"),
        "desired_status": snapshot.get("desired_status"),
        "desired_action": snapshot.get("desired_action"),
        "next_action": snapshot.get("next_action"),
        "health_status": snapshot.get("health_status"),
        "health_json": snapshot.get("health_json"),
        "pos_status": snapshot.get("pos_status"),
        "positions_summary_json": snapshot.get("positions_summary_json"),
        "open_orders_count": snapshot.get("open_orders_count"),
        "open_intents_count": snapshot.get("open_intents_count"),
        "account_id": snapshot.get("account_id"),
        "risk_blocked": snapshot.get("risk_blocked"),
        "risk_reason": snapshot.get("risk_reason"),
        "last_error": snapshot.get("last_error"),
        "last_event_ts": snapshot.get("last_event_ts"),
        "last_event_level": snapshot.get("last_event_level"),
        "last_event_type": snapshot.get("last_event_type"),
        "last_event_message": snapshot.get("last_event_message"),
        "exchange_state": snapshot.get("exchange_state"),
        "last_exchange_error_at": snapshot.get("last_exchange_error_at"),
    }

    try:
        cols = get_table_columns(conn, "bot_state_snapshots")
    except Exception:
        cols = set()
    if "health_jsonb" in cols:
        fields["health_jsonb"] = to_jsonb(_coerce_json_obj(snapshot.get("health_json")))
    if "positions_summary_jsonb" in cols:
        fields["positions_summary_jsonb"] = to_jsonb(_coerce_json_obj(snapshot.get("positions_summary_json")))
    if "exchange_state_jsonb" in cols:
        fields["exchange_state_jsonb"] = to_jsonb(_coerce_json_obj(snapshot.get("exchange_state")))

    cols = ",".join(fields.keys())
    placeholders = ",".join([sql_placeholder(conn)] * len(fields))
    update_clause = ",".join([f"{k}=excluded.{k}" for k in fields.keys() if k != "bot_id"])
    conn.execute(
        f"""
        INSERT INTO bot_state_snapshots ({cols})
        VALUES ({placeholders})
        ON CONFLICT(bot_id) DO UPDATE SET {update_clause}
        """,
        tuple(fields.values()),
    )
    conn.commit()


def get_bot_snapshot(conn: Any, bot_id: int) -> Optional[Dict[str, Any]]:
    """Get a bot row joined with its latest durable snapshot (if present)."""
    row = execute_fetchone(
        conn,
        """
        SELECT
            b.*,
            a.label AS account_label,
            a.status AS account_status,
            s.updated_at AS snapshot_updated_at,
            s.worker_id AS snapshot_worker_id,
            s.status AS snapshot_status,
            s.desired_status AS snapshot_desired_status,
            s.desired_action AS snapshot_desired_action,
            s.next_action AS snapshot_next_action,
            s.health_status AS snapshot_health_status,
            COALESCE(s.health_jsonb::text, s.health_json) AS snapshot_health_json,
            s.pos_status AS snapshot_pos_status,
            COALESCE(s.positions_summary_jsonb::text, s.positions_summary_json) AS snapshot_positions_summary_json,
            s.open_orders_count AS snapshot_open_orders_count,
            s.open_intents_count AS snapshot_open_intents_count,
            s.risk_blocked AS snapshot_risk_blocked,
            s.risk_reason AS snapshot_risk_reason,
            s.last_error AS snapshot_last_error,
            s.last_event_ts AS snapshot_last_event_ts,
            s.last_event_level AS snapshot_last_event_level,
            s.last_event_type AS snapshot_last_event_type,
            s.last_event_message AS snapshot_last_event_message,
            COALESCE(s.exchange_state_jsonb::text, s.exchange_state) AS snapshot_exchange_state,
            s.last_exchange_error_at AS snapshot_last_exchange_error_at
        FROM bots b
        LEFT JOIN trading_accounts a ON a.id = b.account_id
        LEFT JOIN bot_state_snapshots s ON s.bot_id = b.id
        WHERE b.id = {sql_placeholder(conn)}
        LIMIT 1
        """,
        (int(bot_id),),
    )
    return dict(row) if row else None


def get_bot_snapshots(
    conn: Any,
    user_id: str,
    *,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 500,
    status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Single-query bot overview for UI.

    Returns bots joined with accounts and latest snapshot fields.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    _ = filters  # reserved for future SQL WHERE clauses

    base = (
        "SELECT "
        "  b.*, "
        "  a.label AS account_label, a.status AS account_status, "
        "  s.updated_at AS snapshot_updated_at, "
        "  s.worker_id AS snapshot_worker_id, "
        "  s.next_action AS snapshot_next_action, "
        "  s.health_status AS snapshot_health_status, "
        "  s.pos_status AS snapshot_pos_status, "
        "  s.open_orders_count AS snapshot_open_orders_count, "
        "  s.open_intents_count AS snapshot_open_intents_count, "
        "  s.risk_blocked AS snapshot_risk_blocked, "
        "  s.risk_reason AS snapshot_risk_reason, "
        "  s.last_error AS snapshot_last_error, "
        "  s.last_event_ts AS snapshot_last_event_ts, "
        "  s.last_event_level AS snapshot_last_event_level, "
        "  s.last_event_type AS snapshot_last_event_type, "
        "  s.last_event_message AS snapshot_last_event_message, "
        "  COALESCE(s.exchange_state_jsonb::text, s.exchange_state) AS snapshot_exchange_state, "
        "  s.last_exchange_error_at AS snapshot_last_exchange_error_at "
        "FROM bots b "
        "LEFT JOIN trading_accounts a ON a.id = b.account_id "
        "LEFT JOIN bot_state_snapshots s ON s.bot_id = b.id "
        f"WHERE b.user_id = {sql_placeholder(conn)}"
    )
    params: List[Any] = [uid]
    if status:
        base += f" AND b.status = {sql_placeholder(conn)}"
        params.append(str(status))
    base += f" ORDER BY b.updated_at DESC LIMIT {sql_placeholder(conn)}"
    params.append(int(limit))
    rows = execute_fetchall(conn, base, params)
    return [dict(r) for r in rows]


def upsert_account_snapshot(conn: Any, snapshot: Dict[str, Any]) -> None:
    """Upsert a durable trading account snapshot."""

    account_id = snapshot.get("account_id")
    if account_id is None:
        raise ValueError("snapshot missing account_id")

    fields = {
        "account_id": int(account_id),
        "user_id": str(snapshot.get("user_id") or "").strip() or DEFAULT_USER_EMAIL,
        "exchange_id": str(snapshot.get("exchange_id") or "").strip().lower() or "woox",
        "updated_at": str(snapshot.get("updated_at") or now_utc_iso()),
        "worker_id": snapshot.get("worker_id"),
        "status": snapshot.get("status"),
        "risk_blocked": snapshot.get("risk_blocked"),
        "risk_reason": snapshot.get("risk_reason"),
        "positions_summary_json": snapshot.get("positions_summary_json"),
        "open_orders_count": snapshot.get("open_orders_count"),
        "margin_ratio": snapshot.get("margin_ratio"),
        "wallet_balance": snapshot.get("wallet_balance"),
        "last_error": snapshot.get("last_error"),
          "exchange_state": snapshot.get("exchange_state"),
          "last_exchange_error_at": snapshot.get("last_exchange_error_at"),
    }

    try:
        cols = get_table_columns(conn, "account_state_snapshots")
    except Exception:
        cols = set()
    if "positions_summary_jsonb" in cols:
        fields["positions_summary_jsonb"] = to_jsonb(_coerce_json_obj(snapshot.get("positions_summary_json")))
    if "exchange_state_jsonb" in cols:
        fields["exchange_state_jsonb"] = to_jsonb(_coerce_json_obj(snapshot.get("exchange_state")))

    cols = ",".join(fields.keys())
    placeholders = ",".join([sql_placeholder(conn)] * len(fields))
    update_clause = ",".join([f"{k}=excluded.{k}" for k in fields.keys() if k != "account_id"])
    conn.execute(
        f"""
        INSERT INTO account_state_snapshots ({cols})
        VALUES ({placeholders})
        ON CONFLICT(account_id) DO UPDATE SET {update_clause}
        """,
        tuple(fields.values()),
    )
    conn.commit()


def get_account_snapshots(
    conn: Any,
    user_id: str,
    *,
    include_deleted: bool = True,
    exchange_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Single-query account overview joined with latest durable snapshot."""

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL

    base = (
        "SELECT "
        "  a.*, "
        "  s.updated_at AS snapshot_updated_at, "
        "  s.worker_id AS snapshot_worker_id, "
        "  s.status AS snapshot_status, "
        "  s.risk_blocked AS snapshot_risk_blocked, "
        "  s.risk_reason AS snapshot_risk_reason, "
                "  COALESCE(s.positions_summary_jsonb::text, s.positions_summary_json) AS snapshot_positions_summary_json, "
        "  s.open_orders_count AS snapshot_open_orders_count, "
        "  s.margin_ratio AS snapshot_margin_ratio, "
        "  s.wallet_balance AS snapshot_wallet_balance, "
                    "  s.last_error AS snapshot_last_error, "
                    "  COALESCE(s.exchange_state_jsonb::text, s.exchange_state) AS snapshot_exchange_state, "
                    "  s.last_exchange_error_at AS snapshot_last_exchange_error_at "
        "FROM trading_accounts a "
        "LEFT JOIN account_state_snapshots s ON s.account_id = a.id "
        "WHERE a.user_id = %s"
    )
    params: List[Any] = [uid]
    if not include_deleted:
        base += " AND COALESCE(NULLIF(TRIM(a.status), ''), 'active') != 'deleted'"
    if exchange_id:
        base += " AND a.exchange_id = %s"
        params.append(str(exchange_id).strip().lower())
    base += " ORDER BY a.updated_at DESC, a.id DESC"
    rows = execute_fetchall(conn, base, params)
    return [dict(r) for r in rows]

def set_job_link(
    conn: Any,
    job_id: int,
    sweep_id: Optional[str] = None,
    run_id: Optional[str] = None,
) -> None:
    fields: Dict[str, Any] = {}
    if sweep_id is not None:
        fields["sweep_id"] = sweep_id
    if run_id is not None:
        fields["run_id"] = str(run_id)
    if not fields:
        return
    update_job(conn, job_id, **fields)


def request_cancel_job(conn: Any, job_id: int) -> None:
    row = execute_fetchone(conn, "SELECT status FROM jobs WHERE id = %s", (job_id,))
    if not row:
        return
    status = row.get("status")
    if status in {"queued", "running"}:
        # Use update_job so updated_at is refreshed.
        update_job(conn, job_id, status="cancel_requested")


def reconcile_inprocess_jobs_after_restart(
    conn: Any,
    *,
    current_worker_id: str,
    job_types: Sequence[str] = ("backtest", "sweep"),
    min_age_s_unowned_running: float = 10.0,
) -> Dict[str, int]:
    """Reconcile Streamlit in-process jobs after a restart.

    Project Dragon runs backtests/sweeps in-process (ThreadPoolExecutor). When
    Streamlit restarts, any previously-running in-process jobs are stranded in
    DB as status='running' with no worker thread.

    Policy:
    - If a job is `cancel_requested` (queued/running), mark it `cancelled`.
    - If a job is `running` but belongs to a different Streamlit worker_id,
      re-queue it for re-execution.
    - If a job is `running` with no worker_id and it's older than a small
      threshold, treat it as orphaned and re-queue it.

    Returns counts: {"requeued": int, "cancelled": int}
    """

    types = [str(t).strip() for t in (job_types or []) if str(t).strip()]
    if not types:
        return {"requeued": 0, "cancelled": 0}

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(seconds=float(min_age_s_unowned_running))).isoformat()
    now_iso = now.isoformat()
    placeholders = ",".join(["%s"] * len(types))

    # 1) Finalize cancel requests (no worker should keep them alive after restart).
    cur_cancel = conn.execute(
        f"""
        UPDATE jobs
                SET status = 'cancelled', finished_at = COALESCE(finished_at, %s), message = COALESCE(NULLIF(message, ''), 'Cancelled'), updated_at = %s
        WHERE job_type IN ({placeholders})
          AND status = 'cancel_requested'
        """,
        tuple([now_iso, now_iso] + types),
    )

    # 2) Requeue orphaned running jobs.
    cur_requeue = conn.execute(
        f"""
        UPDATE jobs
        SET status = 'queued', worker_id = NULL, progress = 0.0,
                        message = 'Resumed after restart', updated_at = %s
        WHERE job_type IN ({placeholders})
          AND status = 'running'
          AND (
                        (worker_id LIKE 'streamlit:%' AND worker_id != %s)
                        OR ((worker_id IS NULL OR worker_id = '') AND COALESCE(updated_at, created_at) < %s)
          )
        """,
        tuple([now_iso] + types + [str(current_worker_id), cutoff]),
    )

    conn.commit()
    try:
        cancelled = int(cur_cancel.rowcount or 0)
    except Exception:
        cancelled = 0
    try:
        requeued = int(cur_requeue.rowcount or 0)
    except Exception:
        requeued = 0
    return {"requeued": requeued, "cancelled": cancelled}


def link_bot_to_run(conn: Any, bot_id: int, run_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    insert_ignore(
        conn,
        "bot_run_map",
        ["bot_id", "run_id", "created_at"],
        (bot_id, str(run_id), now),
    )
    conn.commit()


def add_bot_event(
    conn: Any,
    bot_id: int,
    level: str,
    event_type: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> int:
    ts = datetime.now(timezone.utc).isoformat()
    payload_obj = _sanitize_for_json(payload or {})
    payload_json = json.dumps(payload_obj)
    cols = get_table_columns(conn, "bot_events") if is_postgres(conn) else set()
    use_jsonb = "json_payload_jsonb" in cols
    if use_jsonb:
        placeholders = sql_placeholders(conn, 7)
        sql = (
            "INSERT INTO bot_events (bot_id, ts, level, event_type, message, json_payload, json_payload_jsonb) "
            f"VALUES ({placeholders})"
        )
        if is_postgres(conn):
            sql += " RETURNING id"
            cur = conn.execute(sql, (bot_id, ts, level, event_type, message, payload_json, to_jsonb(payload_obj)))
            row = cur.fetchone()
            conn.commit()
            if row is None:
                raise ValueError("Failed to insert bot_event")
            return int(row[0])
        cur = conn.execute(sql, (bot_id, ts, level, event_type, message, payload_json, payload_json))
        conn.commit()
        return int(cur.lastrowid)

    placeholders = sql_placeholders(conn, 6)
    sql = (
        "INSERT INTO bot_events (bot_id, ts, level, event_type, message, json_payload) "
        f"VALUES ({placeholders})"
    )
    if is_postgres(conn):
        sql += " RETURNING id"
        cur = conn.execute(sql, (bot_id, ts, level, event_type, message, payload_json))
        row = cur.fetchone()
        conn.commit()
        if row is None:
            raise ValueError("Failed to insert bot_event")
        return int(row[0])
    cur = conn.execute(sql, (bot_id, ts, level, event_type, message, payload_json))
    conn.commit()
    return int(cur.lastrowid)


def add_ledger_row(
    conn: Any,
    *,
    bot_id: int,
    event_ts: str,
    kind: str,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
    position_side: Optional[str] = None,
    qty: Optional[float] = None,
    price: Optional[float] = None,
    fee: Optional[float] = None,
    funding: Optional[float] = None,
    pnl: Optional[float] = None,
    ref_id: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> bool:
    """Insert a ledger row, idempotent when ref_id is provided.

    Uses ON CONFLICT DO NOTHING against the unique index (bot_id, kind, ref_id) where ref_id is not null.
    """
    meta_json = json.dumps(meta or {})
    cur = insert_ignore(
        conn,
        "bot_ledger",
        [
            "bot_id",
            "event_ts",
            "kind",
            "symbol",
            "side",
            "position_side",
            "qty",
            "price",
            "fee",
            "funding",
            "pnl",
            "ref_id",
            "meta_json",
        ],
        (
            int(bot_id),
            str(event_ts),
            str(kind),
            symbol,
            side,
            position_side,
            qty,
            price,
            fee,
            funding,
            pnl,
            ref_id,
            meta_json,
        ),
    )
    conn.commit()
    return cur.rowcount > 0


def list_ledger(conn: Any, bot_id: int, limit: int = 500) -> List[Dict[str, Any]]:
    rows = execute_fetchall(
        conn,
        """
        SELECT id, bot_id, event_ts, kind, symbol, side, position_side, qty, price, fee, funding, pnl, ref_id, meta_json
        FROM bot_ledger
        WHERE bot_id = %s
        ORDER BY event_ts DESC, id DESC
        LIMIT %s
        """,
        (int(bot_id), int(limit)),
    )
    out: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        if data.get("meta_json"):
            try:
                data["meta"] = json.loads(data["meta_json"])
            except (TypeError, json.JSONDecodeError):
                data["meta"] = {}
        out.append(data)
    return out


def sum_ledger(conn: Any, bot_id: int) -> Dict[str, float]:
    row = execute_fetchone(
        conn,
        """
        SELECT
            COALESCE(SUM(COALESCE(fee, 0.0)), 0.0) AS fees_total,
            COALESCE(SUM(COALESCE(funding, 0.0)), 0.0) AS funding_total,
            COALESCE(SUM(COALESCE(pnl, 0.0)), 0.0) AS realized_total
        FROM bot_ledger
        WHERE bot_id = %s
        """,
        (int(bot_id),),
    )
    if not row:
        return {"fees_total": 0.0, "funding_total": 0.0, "realized_total": 0.0}
    return {
        "fees_total": float(row["fees_total"] or 0.0),
        "funding_total": float(row["funding_total"] or 0.0),
        "realized_total": float(row["realized_total"] or 0.0),
    }


def list_bot_events(conn: Any, bot_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    rows = None
    try:
        rows = execute_fetchall(
            conn,
            """
            SELECT id, bot_id, ts, level, event_type, message,
                   COALESCE(json_payload_jsonb::text, json_payload) AS json_payload
            FROM bot_events
            WHERE bot_id = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (bot_id, limit),
        )
    except Exception:
        rows = execute_fetchall(
            conn,
            """
            SELECT id, bot_id, ts, level, event_type, message, json_payload
            FROM bot_events
            WHERE bot_id = %s
            ORDER BY ts DESC
            LIMIT %s
            """,
            (bot_id, limit),
        )
    out: List[Dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        if data.get("json_payload"):
            try:
                data["payload"] = json.loads(data.get("json_payload"))
            except json.JSONDecodeError:
                data["payload"] = {}
        out.append(data)
    return out


def add_bot_fill(conn: Any, fill: Dict[str, Any]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "bot_id": fill.get("bot_id"),
        "run_id": fill.get("run_id"),
        "symbol": fill.get("symbol"),
        "exchange_id": fill.get("exchange_id"),
        "position_side": fill.get("position_side"),
        "order_action": fill.get("order_action"),
        "client_order_id": fill.get("client_order_id"),
        "external_order_id": fill.get("external_order_id"),
        "filled_qty": float(fill.get("filled_qty") or 0.0),
        "avg_fill_price": float(fill.get("avg_fill_price") or 0.0),
        "fee_paid": fill.get("fee_paid"),
        "fee_asset": fill.get("fee_asset"),
        "is_reduce_only": 1 if fill.get("is_reduce_only") else 0,
        "is_dca": 1 if fill.get("is_dca") else 0,
        "note": fill.get("note"),
        "event_ts": fill.get("event_ts") or now,
        "created_at": now,
    }
    cur = conn.execute(
        """
        INSERT INTO bot_fills (
            bot_id, run_id, symbol, exchange_id, position_side, order_action,
            client_order_id, external_order_id, filled_qty, avg_fill_price,
            fee_paid, fee_asset, is_reduce_only, is_dca, note, event_ts, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING id
        """,
        (
            payload.get("bot_id"),
            payload.get("run_id"),
            payload.get("symbol"),
            payload.get("exchange_id"),
            payload.get("position_side"),
            payload.get("order_action"),
            payload.get("client_order_id"),
            payload.get("external_order_id"),
            payload.get("filled_qty"),
            payload.get("avg_fill_price"),
            payload.get("fee_paid"),
            payload.get("fee_asset"),
            payload.get("is_reduce_only"),
            payload.get("is_dca"),
            payload.get("note"),
            payload.get("event_ts"),
            payload.get("created_at"),
        ),
    )
    conn.commit()
    row = cur.fetchone()
    if row is None:
        raise ValueError("Failed to insert bot_fill")
    return int(row[0])


def upsert_bot_order_intent(
    conn: Any,
    *,
    bot_id: int,
    intent_key: str,
    kind: str,
    status: str,
    local_id: int,
    note: Optional[str] = None,
    position_side: Optional[str] = None,
    activation_pct: Optional[float] = None,
    target_price: Optional[float] = None,
    qty: Optional[float] = None,
    reduce_only: bool = False,
    post_only: bool = False,
    client_order_id: Optional[str] = None,
    external_order_id: Optional[str] = None,
    last_error: Optional[str] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO bot_order_intents(
            bot_id, intent_key, kind, status, local_id, note, position_side,
            activation_pct, target_price, qty, reduce_only, post_only,
            client_order_id, external_order_id, last_error, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT(bot_id, intent_key) DO UPDATE SET
            status=excluded.status,
            local_id=excluded.local_id,
            note=excluded.note,
            position_side=excluded.position_side,
            activation_pct=excluded.activation_pct,
            target_price=excluded.target_price,
            qty=excluded.qty,
            reduce_only=excluded.reduce_only,
            post_only=excluded.post_only,
            client_order_id=COALESCE(excluded.client_order_id, bot_order_intents.client_order_id),
            external_order_id=COALESCE(excluded.external_order_id, bot_order_intents.external_order_id),
            last_error=excluded.last_error,
            updated_at=excluded.updated_at
        """,
        (
            int(bot_id),
            str(intent_key),
            str(kind),
            str(status),
            int(local_id),
            note,
            position_side,
            activation_pct,
            target_price,
            qty,
            1 if reduce_only else 0,
            1 if post_only else 0,
            client_order_id,
            external_order_id,
            last_error,
            now,
            now,
        ),
    )
    conn.commit()


def list_bot_order_intents(
    conn: Any,
    bot_id: int,
    *,
    statuses: Optional[List[str]] = None,
    kind: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    clauses = ["bot_id = %s"]
    params: List[Any] = [int(bot_id)]
    if kind:
        clauses.append("kind = %s")
        params.append(str(kind))
    if statuses:
        placeholders = ",".join(["%s"] * len(statuses))
        clauses.append(f"status IN ({placeholders})")
        params.extend([str(s) for s in statuses])
    sql = (
        "SELECT * FROM bot_order_intents WHERE "
        + " AND ".join(clauses)
        + " ORDER BY updated_at DESC, id DESC LIMIT %s"
    )
    params.append(int(limit))
    rows = execute_fetchall(conn, sql, tuple(params))
    return [dict(r) for r in rows]


def get_bot_order_intent(conn: Any, bot_id: int, intent_key: str) -> Optional[Dict[str, Any]]:
    row = execute_fetchone(
        conn,
        "SELECT * FROM bot_order_intents WHERE bot_id = %s AND intent_key = %s",
        (int(bot_id), str(intent_key)),
    )
    return dict(row) if row else None


def get_bot_order_intent_by_local_id(
    conn: Any,
    bot_id: int,
    local_id: int,
    *,
    statuses: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    params: List[Any] = [int(bot_id), int(local_id)]
    sql = "SELECT * FROM bot_order_intents WHERE bot_id = %s AND local_id = %s"
    if statuses:
        placeholders = ",".join(["%s"] * len(statuses))
        sql += f" AND status IN ({placeholders})"
        params.extend([str(s) for s in statuses])
    sql += " ORDER BY updated_at DESC, id DESC LIMIT 1"
    row = execute_fetchone(conn, sql, tuple(params))
    return dict(row) if row else None


def update_bot_order_intent(
    conn: Any,
    *,
    bot_id: int,
    intent_key: str,
    **fields: Any,
) -> None:
    if not fields:
        return
    now = datetime.now(timezone.utc).isoformat()
    fields = dict(fields)
    fields["updated_at"] = now
    sets = ", ".join([f"{k} = %s" for k in fields.keys()])
    params = list(fields.values()) + [int(bot_id), str(intent_key)]
    conn.execute(
        f"UPDATE bot_order_intents SET {sets} WHERE bot_id = %s AND intent_key = %s",
        params,
    )
    conn.commit()


def list_bot_fills(conn: Any, bot_id: int, limit: int = 200, since_ts: Optional[str] = None) -> List[Dict[str, Any]]:
    params: List[Any] = [bot_id]
    query = [
        "SELECT * FROM bot_fills WHERE bot_id = %s",
    ]
    if since_ts:
        query.append("AND event_ts >= %s")
        params.append(since_ts)
    query.append("ORDER BY event_ts DESC LIMIT %s")
    params.append(limit)
    rows = execute_fetchall(conn, "\n".join(query), tuple(params))
    return [dict(r) for r in rows]


def create_position_history(conn: Any, row: Dict[str, Any]) -> int:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "bot_id": row.get("bot_id"),
        "run_id": row.get("run_id"),
        "symbol": row.get("symbol"),
        "exchange_id": row.get("exchange_id"),
        "position_side": row.get("position_side"),
        "opened_at": row.get("opened_at"),
        "closed_at": row.get("closed_at"),
        "max_size": row.get("max_size"),
        "entry_avg_price": row.get("entry_avg_price"),
        "exit_avg_price": row.get("exit_avg_price"),
        "realized_pnl": row.get("realized_pnl"),
        "fees_paid": row.get("fees_paid"),
        "num_fills": row.get("num_fills"),
        "num_dca_fills": row.get("num_dca_fills"),
        "metadata_json": json.dumps(row.get("metadata", {})),
        "created_at": now,
    }
    cur = conn.execute(
        """
        INSERT INTO bot_positions_history (
            bot_id, run_id, symbol, exchange_id, position_side, opened_at, closed_at,
            max_size, entry_avg_price, exit_avg_price, realized_pnl, fees_paid,
            num_fills, num_dca_fills, metadata_json, created_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        RETURNING id
        """,
        (
            payload.get("bot_id"),
            payload.get("run_id"),
            payload.get("symbol"),
            payload.get("exchange_id"),
            payload.get("position_side"),
            payload.get("opened_at"),
            payload.get("closed_at"),
            payload.get("max_size"),
            payload.get("entry_avg_price"),
            payload.get("exit_avg_price"),
            payload.get("realized_pnl"),
            payload.get("fees_paid"),
            payload.get("num_fills"),
            payload.get("num_dca_fills"),
            payload.get("metadata_json"),
            payload.get("created_at"),
        ),
    )
    conn.commit()
    row_id = cur.fetchone()
    if row_id is None:
        raise ValueError("Failed to insert position history")
    return int(row_id[0])


def list_positions_history(conn: Any, bot_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    rows = execute_fetchall(
        conn,
        """
        SELECT * FROM bot_positions_history
        WHERE bot_id = %s
        ORDER BY closed_at DESC
        LIMIT %s
        """,
        (bot_id, limit),
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        data = dict(r)
        meta_raw = data.get("metadata_json")
        if isinstance(meta_raw, str):
            try:
                data["metadata"] = json.loads(meta_raw)
            except json.JSONDecodeError:
                data["metadata"] = {}
        out.append(data)
    return out


def list_bots_for_run(conn: Any, run_id: str) -> List[Dict[str, Any]]:
    rows = execute_fetchall(
        conn,
        f"""
        SELECT bot_id, run_id, created_at
        FROM bot_run_map
        WHERE run_id = {sql_placeholder(conn)}
        ORDER BY created_at DESC
        """,
        (run_id,),
    )
    return [dict(r) for r in rows]


def list_runs_for_bot(conn: Any, bot_id: int) -> List[Dict[str, Any]]:
    rows = execute_fetchall(
        conn,
        f"""
        SELECT bot_id, run_id, created_at
        FROM bot_run_map
        WHERE bot_id = {sql_placeholder(conn)}
        ORDER BY created_at DESC
        """,
        (bot_id,),
    )
    return [dict(r) for r in rows]


def _ts_to_ms(ts: Any) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, datetime):
        return int(ts.timestamp() * 1000)
    return None


def upsert_candles(
    conn: Any,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    candles: List["Candle" | Any],
) -> None:
    if not candles:
        return
    market_type = (market_type or "unknown").lower()
    rows = []
    for c in candles:
        ts_ms = _ts_to_ms(getattr(c, "timestamp", None))
        if ts_ms is None:
            continue
        rows.append(
            (
                exchange_id,
                market_type,
                symbol,
                timeframe,
                ts_ms,
                float(getattr(c, "open", 0.0)),
                float(getattr(c, "high", 0.0)),
                float(getattr(c, "low", 0.0)),
                float(getattr(c, "close", 0.0)),
                float(getattr(c, "volume", 0.0)),
            )
        )
    if not rows:
        return
    conn.executemany(
        f"""
        INSERT INTO candles_cache (
            exchange_id, market_type, symbol, timeframe, timestamp_ms,
            open, high, low, close, volume
        ) VALUES ({sql_placeholders(conn, 10)})
        ON CONFLICT (exchange_id, market_type, symbol, timeframe, timestamp_ms)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """,
        rows,
    )
    conn.commit()


def load_cached_range(
    conn: Any,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> List[Dict[str, Any]]:
    market_type = (market_type or "unknown").lower()
    start_ms = _ts_to_ms(start_ms)
    end_ms = _ts_to_ms(end_ms)
    ph = sql_placeholder(conn)
    params: list[Any] = [exchange_id, market_type, symbol, timeframe]
    conditions = [
        f"exchange_id = {ph}",
        f"market_type = {ph}",
        f"symbol = {ph}",
        f"timeframe = {ph}",
    ]
    if start_ms is not None:
        conditions.append(f"timestamp_ms >= {ph}")
        params.append(int(start_ms))
    if end_ms is not None:
        conditions.append(f"timestamp_ms <= {ph}")
        params.append(int(end_ms))
    query = (
        "SELECT timestamp_ms, open, high, low, close, volume FROM candles_cache "
        + " WHERE "
        + " AND ".join(conditions)
        + " ORDER BY timestamp_ms ASC"
    )
    rows = fetchall_dicts(conn.execute(query, tuple(params)))
    return rows


def get_cached_coverage(
    conn: Any,
    exchange_id: str,
    market_type: str,
    symbol: str,
    timeframe: str,
) -> tuple[Optional[int], Optional[int]]:
    market_type = (market_type or "unknown").lower()
    row = fetchone_dict(
        conn.execute(
            f"""
            SELECT MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts
            FROM candles_cache
            WHERE exchange_id = {sql_placeholder(conn)} AND market_type = {sql_placeholder(conn)}
              AND symbol = {sql_placeholder(conn)} AND timeframe = {sql_placeholder(conn)}
            """,
            (exchange_id, market_type, symbol, timeframe),
        )
    )
    if not row:
        return None, None
    return row.get("min_ts"), row.get("max_ts")


def purge_old_candles(conn: Any, keep_months: int) -> int:
    if keep_months <= 0:
        return 0
    cutoff = datetime.now(timezone.utc) - timedelta(days=30 * keep_months)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    cur = conn.execute("DELETE FROM candles_cache WHERE timestamp_ms < %s", (cutoff_ms,))
    conn.commit()
    return cur.rowcount


def list_cached_coverages(conn: Any) -> List[Dict[str, Any]]:
    rows = fetchall_dicts(
        conn.execute(
            """
            SELECT exchange_id, market_type, symbol, timeframe,
                   MIN(timestamp_ms) AS min_ts, MAX(timestamp_ms) AS max_ts, COUNT(*) AS rows_count
            FROM candles_cache
            GROUP BY exchange_id, market_type, symbol, timeframe
            ORDER BY symbol, timeframe
            """
        )
    )
    return rows


def _extract_initial_entry_notional_usd(config_obj: Any, metadata: Dict[str, Any]) -> Optional[float]:
    """Best-effort: infer initial entry notional from Dragon config + metadata."""

    # Try dict (most common)
    cfg = config_obj
    if is_dataclass(config_obj):
        try:
            cfg = asdict(config_obj)
        except Exception:
            cfg = config_obj
    if not isinstance(cfg, dict):
        return None

    general = cfg.get("general") if isinstance(cfg.get("general"), dict) else {}
    mode_raw = general.get("initial_entry_sizing_mode")
    mode = str(mode_raw).strip().lower() if mode_raw is not None else ""
    if mode in {"fixed_usd", "fixed", "usd"}:
        v = _safe_float(general.get("initial_entry_fixed_usd"))
        return v if (v is not None and v > 0) else None

    # Default: pct balance
    pct = _safe_float(general.get("initial_entry_balance_pct"))
    if pct is None or pct <= 0:
        return None
    initial_balance = _safe_float(metadata.get("initial_balance") or metadata.get("initial_equity"))
    if initial_balance is None or initial_balance <= 0:
        return None
    return initial_balance * (pct / 100.0)


def build_backtest_run_rows(
    config: Any,
    metrics: Dict[str, Any],
    metadata: Dict[str, Any],
    *,
    sweep_id: Optional[str] = None,
    result: Optional["BacktestResult"] = None,
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Build DB-ready summary/details fields for a backtest run.

    Returns (run_row_fields, details_fields). Does not write to DB.
    `run_row_fields["metadata_json"]` is a dict (not a JSON string) to enable merge-by-run_key.
    """

    created_at = metadata.get("created_at") or datetime.now(timezone.utc).isoformat()
    symbol = metadata.get("symbol", "SYNTH")
    timeframe = metadata.get("timeframe", "1m")
    strategy_name = metadata.get("strategy_name", "DragonDcaAtr")
    strategy_version = metadata.get("strategy_version", "0.0.1")

    market_type_raw = metadata.get("market_type") or metadata.get("market") or metadata.get("market_type_hint")
    market_type = (str(market_type_raw).strip().lower() if market_type_raw is not None else "")
    if market_type in {"perp", "perps", "futures"}:
        market_type = "perps"
    elif market_type == "spot":
        market_type = "spot"
    elif not market_type:
        # Best-effort fallback: some callers store it in config payload.
        try:
            if isinstance(config, dict):
                ds = config.get("data_settings") if isinstance(config.get("data_settings"), dict) else None
                market_type = (str((ds or {}).get("market_type") or config.get("market_type") or "").strip().lower())
        except Exception:
            market_type = ""

    metrics_source = (result.metrics if result is not None else metrics) or {}
    start_dt = getattr(result, "start_time", None) if result is not None else metadata.get("start_time")
    end_dt = getattr(result, "end_time", None) if result is not None else metadata.get("end_time")

    # Best-effort: ROI% on margin for futures/perps runs.
    roi_pct_on_margin: Optional[float] = None
    try:
        from project_dragon.metrics import compute_max_drawdown, compute_roi_pct_on_margin, get_effective_leverage

        lev = get_effective_leverage(config) or get_effective_leverage(metadata)
        notional = _extract_initial_entry_notional_usd(config, metadata)
        net_pnl = _safe_float(metrics_source.get("net_profit"))
        roi_pct_on_margin = compute_roi_pct_on_margin(net_pnl=net_pnl, notional=notional, leverage=lev)

        equity_curve = getattr(result, "equity_curve", None) if result is not None else None
        if isinstance(equity_curve, list) and equity_curve:
            try:
                dd_abs_e, dd_pct_e = compute_max_drawdown([float(e) for e in equity_curve])
                if isinstance(metrics_source, dict):
                    metrics_source = dict(metrics_source)
                    metrics_source["max_drawdown_abs"] = float(dd_abs_e)
                    metrics_source["max_drawdown_pct"] = float(dd_pct_e)
                    metrics_source["max_drawdown"] = float(dd_pct_e)
            except Exception:
                pass

        if isinstance(metrics_source, dict) and roi_pct_on_margin is not None:
            metrics_source = dict(metrics_source)
            metrics_source["roi_pct_on_margin"] = roi_pct_on_margin
    except Exception:
        roi_pct_on_margin = None

    start_time = _to_iso8601(start_dt)
    end_time = _to_iso8601(end_dt)

    config_obj = _sanitize_for_json(config)
    metrics_obj = _sanitize_for_json(metrics_source)
    config_payload = json.dumps(config_obj)
    metrics_payload = json.dumps(metrics_obj)
    # Keep metadata as a dict for merge-by-run_key at write time.
    metadata_dict = dict(metadata) if isinstance(metadata, dict) else {}
    metadata_obj = _sanitize_for_json(metadata_dict)

    trades_json: Optional[str] = None
    trades_obj: Optional[Any] = None
    if result is not None and getattr(result, "trades", None) is not None:
        trades_obj = [t.to_dict() if hasattr(t, "to_dict") else _sanitize_for_json(t) for t in result.trades]
        trades_json = json.dumps(trades_obj)

    # Artifacts
    equity_curve_json: Optional[str] = None
    equity_timestamps_json: Optional[str] = None
    extra_series_json: Optional[str] = None
    candles_json: Optional[str] = None
    params_json: Optional[str] = None
    run_context_json: Optional[str] = None
    computed_metrics_json: Optional[str] = None
    equity_curve_obj: Optional[Any] = None
    equity_timestamps_obj: Optional[Any] = None
    extra_series_obj: Optional[Any] = None
    candles_obj: Optional[Any] = None
    params_obj: Optional[Any] = None
    run_context_obj: Optional[Any] = None
    computed_metrics_obj: Optional[Any] = None
    if result is not None:
        try:
            eq = getattr(result, "equity_curve", None)
            if isinstance(eq, list) and eq:
                equity_curve_obj = _sanitize_for_json(eq)
                equity_curve_json = json.dumps(equity_curve_obj)
        except Exception:
            equity_curve_json = None
            equity_curve_obj = None
        try:
            eq_ts = getattr(result, "equity_timestamps", None)
            if isinstance(eq_ts, list) and eq_ts:
                ts_list = []
                for ts in eq_ts:
                    if ts is None:
                        ts_list.append(None)
                    elif hasattr(ts, "isoformat"):
                        ts_list.append(ts.isoformat())
                    else:
                        ts_list.append(str(ts))
                equity_timestamps_obj = _sanitize_for_json(ts_list)
                equity_timestamps_json = json.dumps(equity_timestamps_obj)
            else:
                candles = getattr(result, "candles", None)
                if isinstance(candles, list) and candles:
                    ts_list = []
                    for c in candles:
                        ts = getattr(c, "timestamp", None)
                        if ts is None:
                            ts_list.append(None)
                        elif hasattr(ts, "isoformat"):
                            ts_list.append(ts.isoformat())
                        else:
                            ts_list.append(str(ts))
                    equity_timestamps_obj = _sanitize_for_json(ts_list)
                    equity_timestamps_json = json.dumps(equity_timestamps_obj)
        except Exception:
            equity_timestamps_json = None
            equity_timestamps_obj = None
        try:
            extra = getattr(result, "extra_series", None)
            if isinstance(extra, dict) and extra:
                extra_series_obj = _sanitize_for_json(extra)
                extra_series_json = json.dumps(extra_series_obj)
        except Exception:
            extra_series_json = None
            extra_series_obj = None
        try:
            candles = getattr(result, "candles", None)
            if isinstance(candles, list) and candles:
                packed = []
                for c in candles:
                    ts = getattr(c, "timestamp", None)
                    if ts is None:
                        ts_s = None
                    elif hasattr(ts, "isoformat"):
                        ts_s = ts.isoformat()
                    else:
                        ts_s = str(ts)
                    packed.append(
                        [
                            ts_s,
                            _safe_float(getattr(c, "open", None)),
                            _safe_float(getattr(c, "high", None)),
                            _safe_float(getattr(c, "low", None)),
                            _safe_float(getattr(c, "close", None)),
                            _safe_float(getattr(c, "volume", None)),
                        ]
                    )
                candles_obj = _sanitize_for_json(packed)
                candles_json = json.dumps(candles_obj)
        except Exception:
            candles_json = None
            candles_obj = None

        try:
            params = getattr(result, "params", None)
            if isinstance(params, dict) and params:
                params_obj = _sanitize_for_json(params)
                params_json = json.dumps(params_obj)
        except Exception:
            params_json = None
            params_obj = None
        try:
            rc = getattr(result, "run_context", None)
            if isinstance(rc, dict) and rc:
                run_context_obj = _sanitize_for_json(rc)
                run_context_json = json.dumps(run_context_obj)
        except Exception:
            run_context_json = None
            run_context_obj = None
        try:
            cm = getattr(result, "computed_metrics", None)
            if isinstance(cm, dict) and cm:
                computed_metrics_obj = _sanitize_for_json(cm)
                computed_metrics_json = json.dumps(computed_metrics_obj)
        except Exception:
            computed_metrics_json = None
            computed_metrics_obj = None

    run_row_fields: Dict[str, Any] = {
        "created_at": created_at,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "strategy_version": strategy_version,
        "config_json": config_payload,
        "config_jsonb": config_obj,
        "metrics_json": metrics_payload,
        "metrics_jsonb": metrics_obj,
        "metadata_json": metadata_dict,
        "metadata_jsonb": metadata_obj,
        "sweep_id": sweep_id,
        "market_type": market_type or None,
        "start_time": start_time,
        "end_time": end_time,
        "net_profit": _safe_float(metrics_source.get("net_profit")),
        "net_return_pct": _safe_float(metrics_source.get("net_return_pct", metrics_source.get("return_pct"))),
        "roi_pct_on_margin": _safe_float(metrics_source.get("roi_pct_on_margin", roi_pct_on_margin)),
        "max_drawdown_pct": _safe_float(metrics_source.get("max_drawdown_pct", metrics_source.get("max_drawdown"))),
        "sharpe": _safe_float(metrics_source.get("sharpe", metrics_source.get("sharpe_ratio"))),
        "sortino": _safe_float(metrics_source.get("sortino", metrics_source.get("sortino_ratio"))),
        "win_rate": _safe_float(metrics_source.get("win_rate")),
        "profit_factor": _safe_float(metrics_source.get("profit_factor")),
        "cpc_index": _safe_float(metrics_source.get("cpc_index")),
        "common_sense_ratio": _safe_float(metrics_source.get("common_sense_ratio")),
        "avg_position_time_s": _safe_float(metrics_source.get("avg_position_time_s")),
        "trades_json": trades_json,
        "trades_jsonb": trades_obj,
    }

    details_fields: Dict[str, Any] = {
        "config_json": config_payload,
        "config_jsonb": config_obj,
        "metrics_json": metrics_payload,
        "metrics_jsonb": metrics_obj,
        "metadata_json": metadata_dict,
        "metadata_jsonb": metadata_obj,
        "trades_json": trades_json,
        "trades_jsonb": trades_obj,
        "equity_curve_json": equity_curve_json,
        "equity_curve_jsonb": equity_curve_obj,
        "equity_timestamps_json": equity_timestamps_json,
        "equity_timestamps_jsonb": equity_timestamps_obj,
        "extra_series_json": extra_series_json,
        "extra_series_jsonb": extra_series_obj,
        "candles_json": candles_json,
        "candles_jsonb": candles_obj,
        "params_json": params_json,
        "params_jsonb": params_obj,
        "run_context_json": run_context_json,
        "run_context_jsonb": run_context_obj,
        "computed_metrics_json": computed_metrics_json,
        "computed_metrics_jsonb": computed_metrics_obj,
        "created_at": created_at,
    }

    return run_row_fields, details_fields

def save_backtest_run(
    config: Any,
    metrics: Dict[str, Any],
    metadata: Dict[str, Any],
    sweep_id: Optional[str] = None,
    result: Optional["BacktestResult"] = None,
) -> str:
    run_id = metadata.get("id") or str(uuid4())
    run_row_fields, details_fields = build_backtest_run_rows(
        config,
        metrics,
        metadata,
        sweep_id=sweep_id,
        result=result,
    )
    created_at = str(run_row_fields.get("created_at") or datetime.now(timezone.utc).isoformat())

    payload = dict(run_row_fields)
    payload["id"] = run_id
    payload["created_at"] = created_at
    payload["metadata_json"] = json.dumps(_sanitize_for_json(run_row_fields.get("metadata_json") or {}))
    payload["metadata_jsonb"] = to_jsonb(run_row_fields.get("metadata_jsonb") or run_row_fields.get("metadata_json"))
    payload["config_jsonb"] = to_jsonb(run_row_fields.get("config_jsonb"))
    payload["metrics_jsonb"] = to_jsonb(run_row_fields.get("metrics_jsonb"))
    payload["trades_jsonb"] = to_jsonb(run_row_fields.get("trades_jsonb"))

    with _get_conn() as conn:
        _ensure_schema(conn)
        cols = [
            "id",
            "created_at",
            "symbol",
            "timeframe",
            "strategy_name",
            "strategy_version",
            "config_json",
            "config_jsonb",
            "metrics_json",
            "metrics_jsonb",
            "metadata_json",
            "metadata_jsonb",
            "market_type",
            "start_time",
            "end_time",
            "net_profit",
            "net_return_pct",
            "roi_pct_on_margin",
            "max_drawdown_pct",
            "sharpe",
            "sortino",
            "win_rate",
            "profit_factor",
            "cpc_index",
            "common_sense_ratio",
            "avg_position_time_s",
            "trades_json",
            "trades_jsonb",
            "sweep_id",
        ]
        values = [payload.get(c) for c in cols]
        placeholders = sql_placeholders(conn, len(cols))
        conn.execute(
            f"INSERT INTO backtest_runs ({', '.join(cols)}) VALUES ({placeholders})",
            tuple(values),
        )

        # Keep details table in sync for newer DBs. This enables summary-only list queries.
        try:
            try:
                upsert_backtest_run_details(
                    conn,
                    (
                        "run_id",
                        "config_json",
                        "config_jsonb",
                        "metrics_json",
                        "metrics_jsonb",
                        "metadata_json",
                        "metadata_jsonb",
                        "trades_json",
                        "trades_jsonb",
                        "equity_curve_json",
                        "equity_curve_jsonb",
                        "equity_timestamps_json",
                        "equity_timestamps_jsonb",
                        "extra_series_json",
                        "extra_series_jsonb",
                        "candles_json",
                        "candles_jsonb",
                        "params_json",
                        "params_jsonb",
                        "run_context_json",
                        "run_context_jsonb",
                        "computed_metrics_json",
                        "computed_metrics_jsonb",
                        "created_at",
                        "updated_at",
                    ),
                    (
                        run_id,
                        details_fields.get("config_json"),
                        to_jsonb(details_fields.get("config_jsonb")),
                        details_fields.get("metrics_json"),
                        to_jsonb(details_fields.get("metrics_jsonb")),
                        json.dumps(_sanitize_for_json(details_fields.get("metadata_json") or {})),
                        to_jsonb(details_fields.get("metadata_jsonb")),
                        details_fields.get("trades_json"),
                        to_jsonb(details_fields.get("trades_jsonb")),
                        details_fields.get("equity_curve_json"),
                        to_jsonb(details_fields.get("equity_curve_jsonb")),
                        details_fields.get("equity_timestamps_json"),
                        to_jsonb(details_fields.get("equity_timestamps_jsonb")),
                        details_fields.get("extra_series_json"),
                        to_jsonb(details_fields.get("extra_series_jsonb")),
                        details_fields.get("candles_json"),
                        to_jsonb(details_fields.get("candles_jsonb")),
                        details_fields.get("params_json"),
                        to_jsonb(details_fields.get("params_jsonb")),
                        details_fields.get("run_context_json"),
                        to_jsonb(details_fields.get("run_context_jsonb")),
                        details_fields.get("computed_metrics_json"),
                        to_jsonb(details_fields.get("computed_metrics_jsonb")),
                        created_at,
                        created_at,
                    ),
                )
            except Exception:
                # Legacy DB without the new artifact columns.
                upsert_backtest_run_details(
                    conn,
                    (
                        "run_id",
                        "config_json",
                        "metrics_json",
                        "metadata_json",
                        "trades_json",
                        "created_at",
                        "updated_at",
                    ),
                    (
                        run_id,
                        details_fields.get("config_json"),
                        details_fields.get("metrics_json"),
                        json.dumps(_sanitize_for_json(details_fields.get("metadata_json") or {})),
                        details_fields.get("trades_json"),
                        created_at,
                        created_at,
                    ),
                )
        except Exception as e:
            # Allow legacy DBs that haven't been migrated yet.
            # Otherwise, surface the error so the caller gets an atomic rollback.
            msg = str(e).lower()
            if "no such table: backtest_run_details" in msg:
                pass
            else:
                raise
    return run_id


def save_backtest_run_details_for_existing_run(
    *,
    run_id: str,
    config: Any,
    metrics: Dict[str, Any],
    metadata: Dict[str, Any],
    result: Optional["BacktestResult"],
) -> None:
    """Upsert heavy detail payloads for an existing run.

    This updates backtest_run_details only (no changes to backtest_runs summary).
    """

    rid = str(run_id or "").strip()
    if not rid:
        raise ValueError("run_id is required")

    run_row_fields, details_fields = build_backtest_run_rows(
        config,
        metrics,
        metadata,
        sweep_id=metadata.get("sweep_id"),
        result=result,
    )
    now_iso = now_utc_iso()

    def _do_write() -> None:
        with open_db_connection() as conn:
            _ensure_schema(conn)

            try:
                cols = get_table_columns(conn, "backtest_run_details")
            except Exception:
                cols = set()
            if not cols:
                return

            created_at = None
            try:
                row = execute_fetchone(
                    conn,
                    f"SELECT created_at FROM backtest_run_details WHERE run_id = {sql_placeholder(conn)}",
                    (rid,),
                )
                if row is not None:
                    created_at = row.get("created_at")
            except Exception:
                created_at = None
            if not created_at:
                try:
                    row2 = execute_fetchone(
                        conn,
                        f"SELECT created_at FROM backtest_runs WHERE id = {sql_placeholder(conn)}",
                        (rid,),
                    )
                    if row2 is not None:
                        created_at = row2.get("created_at")
                except Exception:
                    created_at = None
            created_at = created_at or str(details_fields.get("created_at") or now_iso)

            def _json_or_none(val: Any) -> Optional[str]:
                if val is None:
                    return None
                if isinstance(val, str):
                    return val
                try:
                    return json.dumps(_sanitize_for_json(val))
                except Exception:
                    return None

            details_meta = details_fields.get("metadata_json")
            details_meta_json = _json_or_none(details_meta) or "{}"
            has_jsonb = "config_jsonb" in cols

            if {"equity_curve_json", "equity_timestamps_json", "extra_series_json"}.issubset(cols):
                if has_jsonb:
                    upsert_backtest_run_details(
                        conn,
                        (
                            "run_id",
                            "config_json",
                            "config_jsonb",
                            "metrics_json",
                            "metrics_jsonb",
                            "metadata_json",
                            "metadata_jsonb",
                            "trades_json",
                            "trades_jsonb",
                            "equity_curve_json",
                            "equity_curve_jsonb",
                            "equity_timestamps_json",
                            "equity_timestamps_jsonb",
                            "extra_series_json",
                            "extra_series_jsonb",
                            "candles_json",
                            "candles_jsonb",
                            "params_json",
                            "params_jsonb",
                            "run_context_json",
                            "run_context_jsonb",
                            "computed_metrics_json",
                            "computed_metrics_jsonb",
                            "created_at",
                            "updated_at",
                        ),
                        (
                            rid,
                            details_fields.get("config_json"),
                            to_jsonb(details_fields.get("config_jsonb") or details_fields.get("config_json")),
                            details_fields.get("metrics_json"),
                            to_jsonb(details_fields.get("metrics_jsonb") or details_fields.get("metrics_json")),
                            details_meta_json,
                            to_jsonb(details_fields.get("metadata_jsonb") or details_fields.get("metadata_json")),
                            details_fields.get("trades_json"),
                            to_jsonb(details_fields.get("trades_jsonb") or details_fields.get("trades_json")),
                            details_fields.get("equity_curve_json"),
                            to_jsonb(details_fields.get("equity_curve_jsonb") or details_fields.get("equity_curve_json")),
                            details_fields.get("equity_timestamps_json"),
                            to_jsonb(details_fields.get("equity_timestamps_jsonb") or details_fields.get("equity_timestamps_json")),
                            details_fields.get("extra_series_json"),
                            to_jsonb(details_fields.get("extra_series_jsonb") or details_fields.get("extra_series_json")),
                            details_fields.get("candles_json"),
                            to_jsonb(details_fields.get("candles_jsonb") or details_fields.get("candles_json")),
                            details_fields.get("params_json"),
                            to_jsonb(details_fields.get("params_jsonb") or details_fields.get("params_json")),
                            details_fields.get("run_context_json"),
                            to_jsonb(details_fields.get("run_context_jsonb") or details_fields.get("run_context_json")),
                            details_fields.get("computed_metrics_json"),
                            to_jsonb(details_fields.get("computed_metrics_jsonb") or details_fields.get("computed_metrics_json")),
                            created_at,
                            now_iso,
                        ),
                    )
                else:
                    upsert_backtest_run_details(
                        conn,
                        (
                            "run_id",
                            "config_json",
                            "metrics_json",
                            "metadata_json",
                            "trades_json",
                            "equity_curve_json",
                            "equity_timestamps_json",
                            "extra_series_json",
                            "candles_json",
                            "params_json",
                            "run_context_json",
                            "computed_metrics_json",
                            "created_at",
                            "updated_at",
                        ),
                        (
                            rid,
                            details_fields.get("config_json"),
                            details_fields.get("metrics_json"),
                            details_meta_json,
                            details_fields.get("trades_json"),
                            details_fields.get("equity_curve_json"),
                            details_fields.get("equity_timestamps_json"),
                            details_fields.get("extra_series_json"),
                            details_fields.get("candles_json"),
                            details_fields.get("params_json"),
                            details_fields.get("run_context_json"),
                            details_fields.get("computed_metrics_json"),
                            created_at,
                            now_iso,
                        ),
                    )
            else:
                if has_jsonb:
                    upsert_backtest_run_details(
                        conn,
                        (
                            "run_id",
                            "config_json",
                            "config_jsonb",
                            "metrics_json",
                            "metrics_jsonb",
                            "metadata_json",
                            "metadata_jsonb",
                            "trades_json",
                            "trades_jsonb",
                            "created_at",
                            "updated_at",
                        ),
                        (
                            rid,
                            details_fields.get("config_json"),
                            to_jsonb(details_fields.get("config_jsonb") or details_fields.get("config_json")),
                            details_fields.get("metrics_json"),
                            to_jsonb(details_fields.get("metrics_jsonb") or details_fields.get("metrics_json")),
                            details_meta_json,
                            to_jsonb(details_fields.get("metadata_jsonb") or details_fields.get("metadata_json")),
                            details_fields.get("trades_json"),
                            to_jsonb(details_fields.get("trades_jsonb") or details_fields.get("trades_json")),
                            created_at,
                            now_iso,
                        ),
                    )
                else:
                    upsert_backtest_run_details(
                        conn,
                        (
                            "run_id",
                            "config_json",
                            "metrics_json",
                            "metadata_json",
                            "trades_json",
                            "created_at",
                            "updated_at",
                        ),
                        (
                            rid,
                            details_fields.get("config_json"),
                            details_fields.get("metrics_json"),
                            details_meta_json,
                            details_fields.get("trades_json"),
                            created_at,
                            now_iso,
                        ),
                    )
            conn.commit()

    try:
        _run_write_with_retry(_do_write)
    except Exception as exc:
        if _is_db_locked_error(exc):
            raise RuntimeError("Database is locked. Please retry in a moment.") from exc
        raise


def count_backtest_runs_missing_details(conn: Any) -> int:
    """Count backtest_runs that don't have a matching backtest_run_details row."""

    row = execute_fetchone(
        conn,
        """
        SELECT COUNT(1) AS c
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        """,
    )
    try:
        return int(row.get("c") if row else 0)
    except Exception:
        return 0


def sample_backtest_runs_missing_details(conn: Any, limit: int = 5) -> List[str]:
    """Return up to `limit` run IDs missing details rows."""

    lim = max(1, int(limit))
    rows = execute_fetchall(
        conn,
        """
        SELECT r.id
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        ORDER BY r.created_at DESC
        LIMIT %s
        """,
        (lim,),
    )
    out: List[str] = []
    for r in rows or []:
        try:
            out.append(str(r.get("id")))
        except Exception:
            continue
    return out


def load_backtest_run_summaries(
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    conn: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Load summary rows for list views (no blob JSON parsing).

    Returns only index-friendly columns from backtest_runs.
    """

    query = """
         SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
             start_time, end_time, net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
             sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
             sweep_id
        FROM backtest_runs
    """
    where_clauses: list[str] = []
    params_list: list[Any] = []
    filters = filters or {}
    min_ret = filters.get("min_net_return_pct")
    if min_ret is not None:
        where_clauses.append("net_return_pct IS NOT NULL AND net_return_pct >= %s")
        params_list.append(min_ret)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY created_at DESC"

    if limit is not None:
        query += " LIMIT %s"
        params_list.append(limit)

    connection = conn or _get_conn()
    with profile_span(
        "db.load_backtest_run_summaries",
        meta={"limit": limit if limit is not None else None, "has_filters": bool(filters)},
    ):
        rows = execute_fetchall(connection, query, tuple(params_list))
    if conn is None:
        connection.close()
    return [dict(r) for r in rows]


def get_backtest_run_details(
    run_id: str,
    *,
    conn: Optional[Any] = None,
) -> Optional[Dict[str, Any]]:
    """Load the large JSON payloads for a run.

    Prefers backtest_run_details; falls back to legacy columns in backtest_runs.
    """

    rid = str(run_id)
    connection = conn or _get_conn()

    row = None
    try:
        with profile_span("db.get_backtest_run_details", meta={"run_id": rid}):
            row = execute_fetchone(
                connection,
                """
                SELECT run_id,
                       config_json, config_jsonb,
                       metrics_json, metrics_jsonb,
                       metadata_json, metadata_jsonb,
                       trades_json, trades_jsonb
                FROM backtest_run_details
                WHERE run_id = %s
                """,
                (rid,),
            )
    except Exception:
        row = None

    if row is None:
        # Legacy fallback.
        with profile_span("db.get_backtest_run_details_legacy", meta={"run_id": rid}):
            row = execute_fetchone(
                connection,
                """
                SELECT id AS run_id,
                       config_json, config_jsonb,
                       metrics_json, metrics_jsonb,
                       metadata_json, metadata_jsonb,
                       trades_json, trades_jsonb
                FROM backtest_runs
                WHERE id = %s
                """,
                (rid,),
            )

    if conn is None:
        connection.close()

    if row is None:
        return None

    config_obj: Any = _coerce_json_obj(row.get("config_jsonb"))
    if config_obj is None:
        config_obj = _coerce_json_obj(row.get("config_json")) or {}
    metrics_obj: Any = _coerce_json_obj(row.get("metrics_jsonb"))
    if metrics_obj is None:
        metrics_obj = _coerce_json_obj(row.get("metrics_json")) or {}
    metadata_obj: Any = _coerce_json_obj(row.get("metadata_jsonb"))
    if metadata_obj is None:
        metadata_obj = _coerce_json_obj(row.get("metadata_json")) or {}
    trades_payload = row.get("trades_jsonb") if row.get("trades_jsonb") is not None else row.get("trades_json")

    return {
        "run_id": row["run_id"],
        "config": config_obj,
        "metrics": metrics_obj,
        "metadata": metadata_obj,
        "trades_json": trades_payload,
    }


def load_backtest_runs(
    limit: Optional[int] = None,
    filters: Optional[Dict[str, Any]] = None,
    conn: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    query = """
         SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
             config_json, config_jsonb, metrics_json, metrics_jsonb, metadata_json, metadata_jsonb,
             start_time, end_time, net_profit, net_return_pct, roi_pct_on_margin, max_drawdown_pct,
             sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
             trades_json, trades_jsonb, sweep_id
        FROM backtest_runs
    """
    where_clauses: list[str] = []
    params_list: list[Any] = []
    filters = filters or {}
    min_ret = filters.get("min_net_return_pct")
    if min_ret is not None:
        where_clauses.append("net_return_pct IS NOT NULL AND net_return_pct >= %s")
        params_list.append(min_ret)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY created_at DESC"

    if limit is not None:
        query += " LIMIT %s"
        params_list.append(limit)

    connection = conn or _get_conn()
    with profile_span(
        "db.load_backtest_runs",
        meta={"limit": limit if limit is not None else None, "has_filters": bool(filters)},
    ):
        rows = execute_fetchall(connection, query, tuple(params_list))
    if conn is None:
        connection.close()

    return [
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "strategy_name": row["strategy_name"],
            "strategy_version": row["strategy_version"],
            "config": _load_json_field(row, "config"),
            "metrics": _load_json_field(row, "metrics"),
            "metadata": _load_json_field(row, "metadata"),
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "net_profit": row["net_profit"],
            "net_return_pct": row["net_return_pct"],
            "roi_pct_on_margin": row["roi_pct_on_margin"],
            "max_drawdown_pct": row["max_drawdown_pct"],
            "sharpe": row["sharpe"],
            "sortino": row["sortino"],
            "win_rate": row["win_rate"],
            "profit_factor": row["profit_factor"],
            "cpc_index": row["cpc_index"],
            "common_sense_ratio": row["common_sense_ratio"],
            "trades_json": row["trades_jsonb"] if row.get("trades_jsonb") is not None else row["trades_json"],
            "sweep_id": row["sweep_id"],
        }
        for row in rows
    ]


def get_user_run_shortlist_map(user_id: str) -> Dict[str, Dict[str, Any]]:
    """Return mapping run_id -> {shortlisted, note, updated_at} for a user."""

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    with _get_conn() as conn:
        _ensure_schema(conn)
        try:
            rows = execute_fetchall(
                conn,
                """
                SELECT run_id, shortlisted, note, updated_at
                FROM run_shortlists
                WHERE user_id = %s AND shortlisted = 1
                ORDER BY updated_at DESC
                """,
                (uid,),
            )
        except DbErrors.OperationalError:
            return {}
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows or []:
        rid = str(r.get("run_id"))
        out[rid] = {
            "shortlisted": bool(r.get("shortlisted")),
            "note": (r.get("note") or ""),
            "updated_at": r.get("updated_at"),
        }
    return out


def set_user_run_shortlists(
    user_id: str,
    run_id_to_shortlisted: Mapping[str, bool],
    *,
    run_id_to_note: Optional[Mapping[str, str]] = None,
) -> None:
    """Bulk upsert run shortlist rows for a user.

    Does not delete rows; sets shortlisted to 0 when false.
    """

    uid = (user_id or "").strip() or DEFAULT_USER_EMAIL
    run_id_to_note = run_id_to_note or {}
    now = now_utc_iso()
    rows: List[Tuple[Any, ...]] = []
    for rid_raw, flag in run_id_to_shortlisted.items():
        rid = (str(rid_raw or "").strip())
        if not rid:
            continue
        note = run_id_to_note.get(rid, "")
        note_s = "" if note is None else str(note)
        rows.append((uid, rid, 1 if bool(flag) else 0, note_s, now))
    if not rows:
        return

    with _get_conn() as conn:
        _ensure_schema(conn)
        try:
            conn.executemany(
                """
                INSERT INTO run_shortlists(user_id, run_id, shortlisted, note, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT(user_id, run_id) DO UPDATE SET
                    shortlisted = excluded.shortlisted,
                    note = excluded.note,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
        except DbErrors.OperationalError:
            # Table may not exist on legacy DBs.
            return


def load_backtest_runs_explorer_rows(
    *,
    user_id: Optional[str] = None,
    limit: int = 2000,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Load rows for the Runs Explorer grid.

    - Uses stored `backtest_runs.market_type` (never derived from symbol).
    - Joins `symbols.icon_uri` by exact symbol match.
    - Joins per-user shortlist (optional).
    """

    lim = max(1, int(limit or 2000))
    uid = (str(user_id).strip() if user_id is not None else "")
    filters = filters or {}

    with _get_conn() as conn:
        _ensure_schema(conn)
        shortlist_join = ""
        shortlist_cols = "0 AS shortlisted, '' AS shortlist_note"
        filter_params: List[Any] = []
        params: List[Any] = []
        if uid:
            shortlist_join = "LEFT JOIN run_shortlists rs ON rs.run_id = r.id AND rs.user_id = %s"
            shortlist_cols = "COALESCE(rs.shortlisted, 0) AS shortlisted, COALESCE(rs.note, '') AS shortlist_note"
            params.append(uid)

        where_clauses: list[str] = []
        if filters.get("sweep_id") is not None:
            where_clauses.append("r.sweep_id = %s")
            filter_params.append(filters.get("sweep_id"))
        if filters.get("min_net_return_pct") is not None:
            where_clauses.append("r.net_return_pct IS NOT NULL AND r.net_return_pct >= %s")
            filter_params.append(filters.get("min_net_return_pct"))
        if filters.get("net_return_pct_gt") is not None:
            where_clauses.append("r.net_return_pct IS NOT NULL AND r.net_return_pct > %s")
            filter_params.append(filters.get("net_return_pct_gt"))

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Ensure symbol rows exist for the symbols we are about to display.
        # Doing a full-table scan each time makes the Results page feel very slow.
        try:
            sym_rows = conn.execute(
                f"SELECT r.symbol FROM backtest_runs r{where_sql} ORDER BY r.created_at DESC LIMIT %s",
                tuple(list(filter_params) + [lim]),
            ).fetchall()
            symbols = []
            for r in sym_rows or []:
                try:
                    v = r[0]
                except Exception:
                    v = None
                s = ("" if v is None else str(v)).strip()
                if s:
                    symbols.append(s)
            if symbols:
                ensure_symbols_for_exchange_symbols(conn, list(dict.fromkeys(symbols)))
        except Exception:
            pass

        params.extend(filter_params)

        sql = f"""
            SELECT
                r.id AS run_id,
                r.created_at,
                r.symbol,
                r.timeframe,
                r.strategy_name,
                r.strategy_version,
                r.market_type,
                r.config_json,
                r.start_time,
                r.end_time,
                r.net_profit,
                r.net_return_pct,
                r.roi_pct_on_margin,
                r.max_drawdown_pct,
                r.sharpe,
                r.sortino,
                r.win_rate,
                r.profit_factor,
                r.cpc_index,
                r.common_sense_ratio,
                r.avg_position_time_s,
                r.sweep_id,
                sw.name AS sweep_name,
                sw.exchange_id AS sweep_exchange_id,
                sw.sweep_scope AS sweep_scope,
                sw.sweep_assets_json AS sweep_assets_json,
                sw.sweep_category_id AS sweep_category_id,
                s.base_asset,
                s.quote_asset,
                s.icon_uri,
                {shortlist_cols}
            FROM backtest_runs r
            LEFT JOIN sweeps sw ON sw.id = r.sweep_id
            LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
            {shortlist_join}
            {where_sql}
            ORDER BY r.created_at DESC
            LIMIT %s
        """
        params.append(lim)

        rows = execute_fetchall(conn, sql, tuple(params))
        return [dict(r) for r in rows]


def _runs_allowed_filter_map(run_cols: set[str]) -> Dict[str, Dict[str, str]]:
    allow: Dict[str, Dict[str, str]] = {
        "run_id": {"type": "text", "sql": "r.id"},
        "created_at": {"type": "date", "sql": "r.created_at"},
        "symbol": {"type": "text", "sql": "r.symbol"},
        "timeframe": {"type": "text", "sql": "r.timeframe"},
        "strategy_name": {"type": "text", "sql": "r.strategy_name"},
        "strategy_version": {"type": "text", "sql": "r.strategy_version"},
        "strategy": {"type": "text", "sql": "r.strategy_name"},
        "market_type": {"type": "text", "sql": "r.market_type"},
        "market": {"type": "text", "sql": "r.market_type"},
        "net_profit": {"type": "number", "sql": "r.net_profit"},
        "net_return_pct": {"type": "number", "sql": "r.net_return_pct"},
        "roi_pct_on_margin": {"type": "number", "sql": "r.roi_pct_on_margin"},
        "max_drawdown_pct": {"type": "number", "sql": "r.max_drawdown_pct"},
        "sharpe": {"type": "number", "sql": "r.sharpe"},
        "sortino": {"type": "number", "sql": "r.sortino"},
        "win_rate": {"type": "number", "sql": "r.win_rate"},
        "profit_factor": {"type": "number", "sql": "r.profit_factor"},
        "cpc_index": {"type": "number", "sql": "r.cpc_index"},
        "common_sense_ratio": {"type": "number", "sql": "r.common_sense_ratio"},
        "sweep_id": {"type": "text", "sql": "r.sweep_id"},
        "sweep_name": {"type": "text", "sql": "sw.name"},
        "user_id": {"type": "text", "sql": "r.user_id"},
    }
    if "metadata_jsonb" in run_cols:
        allow["run_key"] = {"type": "text", "sql": "(r.metadata_jsonb->>'run_key')"}
    return allow


def _runs_allowed_select_map(run_cols: set[str]) -> Dict[str, str]:
    select: Dict[str, str] = {
        "run_id": "r.id AS run_id",
        "id": "r.id",
        "created_at": "r.created_at",
        "symbol": "r.symbol",
        "timeframe": "r.timeframe",
        "strategy_name": "r.strategy_name",
        "strategy_version": "r.strategy_version",
        "market_type": "r.market_type",
        "config_json": "r.config_json",
        "metrics_json": "r.metrics_json",
        "metadata_json": "r.metadata_json",
        "start_time": "r.start_time",
        "end_time": "r.end_time",
        "net_profit": "r.net_profit",
        "net_return_pct": "r.net_return_pct",
        "roi_pct_on_margin": "r.roi_pct_on_margin",
        "max_drawdown_pct": "r.max_drawdown_pct",
        "sharpe": "r.sharpe",
        "sortino": "r.sortino",
        "win_rate": "r.win_rate",
        "profit_factor": "r.profit_factor",
        "cpc_index": "r.cpc_index",
        "common_sense_ratio": "r.common_sense_ratio",
        "avg_position_time_s": "r.avg_position_time_s",
        "sweep_id": "r.sweep_id",
        "sweep_name": "sw.name AS sweep_name",
        "sweep_exchange_id": "sw.exchange_id AS sweep_exchange_id",
        "sweep_scope": "sw.sweep_scope AS sweep_scope",
        "sweep_assets_json": "sw.sweep_assets_json AS sweep_assets_json",
        "sweep_category_id": "sw.sweep_category_id AS sweep_category_id",
        "base_asset": "s.base_asset",
        "quote_asset": "s.quote_asset",
        "icon_uri": "s.icon_uri",
        "shortlisted": "COALESCE(rs.shortlisted, 0) AS shortlisted",
        "shortlist_note": "COALESCE(rs.note, '') AS shortlist_note",
    }
    if "metadata_jsonb" in run_cols:
        select["run_key"] = "(r.metadata_jsonb->>'run_key') AS run_key"
    # Drop selections for missing columns.
    for key in list(select.keys()):
        if key in {"run_id", "id", "sweep_name", "sweep_exchange_id", "sweep_scope", "sweep_assets_json", "sweep_category_id", "base_asset", "quote_asset", "icon_uri", "shortlisted", "shortlist_note", "run_key"}:
            continue
        col = select[key].split(" AS ")[0].strip()
        if col.startswith("r.") and col[2:] not in run_cols:
            select.pop(key, None)
    return select


def _runs_duration_seconds_sql() -> str:
    return "EXTRACT(EPOCH FROM (r.end_time::timestamptz - r.start_time::timestamptz))"


def _build_runs_where(
    *,
    filter_model: Optional[Dict[str, Any]],
    allowed_filters: Dict[str, Dict[str, str]],
    extra_where: Optional[Dict[str, Any]],
    user_id: Optional[str],
    run_cols: set[str],
    exclude_keys: Optional[set[str]] = None,
) -> Tuple[str, List[Any]]:
    exclude_keys = exclude_keys or set()
    where_parts: List[str] = []
    where_params: List[Any] = []

    where_sql, where_from_filters = translate_filter_model(filter_model, allowed_filters)
    if where_sql:
        where_parts.append(where_sql)
        where_params.extend(where_from_filters)

    if isinstance(extra_where, dict):
        for key, value in extra_where.items():
            if key in exclude_keys:
                continue
            if key == "min_net_return_pct":
                where_parts.append("r.net_return_pct IS NOT NULL AND r.net_return_pct >= %s")
                where_params.append(value)
                continue
            if key == "sweep_id":
                where_parts.append("r.sweep_id = %s")
                where_params.append(value)
                continue
            if key == "search":
                term = str(value or "").strip()
                if term:
                    like = f"%{term}%"
                    where_parts.append(
                        "(r.id ILIKE %s OR r.symbol ILIKE %s OR r.strategy_name ILIKE %s OR sw.name ILIKE %s)"
                    )
                    where_params.extend([like, like, like, like])
                continue
            if key == "symbol_contains":
                term = str(value or "").strip()
                if term:
                    like = f"%{term}%"
                    where_parts.append("(r.symbol ILIKE %s OR s.base_asset ILIKE %s OR s.quote_asset ILIKE %s)")
                    where_params.extend([like, like, like])
                continue
            if key == "strategy_contains":
                term = str(value or "").strip()
                if term:
                    where_parts.append("r.strategy_name ILIKE %s")
                    where_params.append(f"%{term}%")
                continue
            if key == "timeframe_contains":
                term = str(value or "").strip()
                if term:
                    where_parts.append("r.timeframe ILIKE %s")
                    where_params.append(f"%{term}%")
                continue
            if key == "sweep_name_contains":
                term = str(value or "").strip()
                if term:
                    where_parts.append("sw.name ILIKE %s")
                    where_params.append(f"%{term}%")
                continue
            if key == "created_at_from":
                where_parts.append("r.created_at >= %s")
                where_params.append(value)
                continue
            if key == "created_at_to":
                where_parts.append("r.created_at <= %s")
                where_params.append(value)
                continue
            if key == "net_profit_min":
                where_parts.append("r.net_profit >= %s")
                where_params.append(value)
                continue
            if key == "net_profit_max":
                where_parts.append("r.net_profit <= %s")
                where_params.append(value)
                continue
            if key == "net_return_pct_min":
                where_parts.append("r.net_return_pct >= %s")
                where_params.append(value)
                continue
            if key == "net_return_pct_gt":
                where_parts.append("r.net_return_pct > %s")
                where_params.append(value)
                continue
            if key == "net_return_pct_max":
                where_parts.append("r.net_return_pct <= %s")
                where_params.append(value)
                continue
            if key == "roi_pct_on_margin_min":
                where_parts.append("r.roi_pct_on_margin >= %s")
                where_params.append(value)
                continue
            if key == "roi_pct_on_margin_max":
                where_parts.append("r.roi_pct_on_margin <= %s")
                where_params.append(value)
                continue
            if key == "max_drawdown_pct_min":
                where_parts.append("r.max_drawdown_pct >= %s")
                where_params.append(value)
                continue
            if key == "max_drawdown_pct_max":
                where_parts.append("r.max_drawdown_pct <= %s")
                where_params.append(value)
                continue
            if key == "duration_min_hours":
                where_parts.append(
                    f"r.start_time IS NOT NULL AND r.end_time IS NOT NULL AND {_runs_duration_seconds_sql()} >= %s"
                )
                where_params.append(float(value) * 3600.0)
                continue
            if key == "duration_max_hours":
                where_parts.append(
                    f"r.start_time IS NOT NULL AND r.end_time IS NOT NULL AND {_runs_duration_seconds_sql()} <= %s"
                )
                where_params.append(float(value) * 3600.0)
                continue
            if key == "run_type":
                rt = str(value or "").strip().lower()
                if rt in {"single", "single_run", "single-run"}:
                    where_parts.append("r.sweep_id IS NULL")
                elif rt in {"sweep", "sweep_run", "sweep-run"}:
                    where_parts.append("r.sweep_id IS NOT NULL")
                continue
            if key == "symbol" and isinstance(value, (list, tuple, set)):
                vals = [v for v in value if v is not None and str(v).strip() != ""]
                if vals:
                    placeholders = ",".join(["%s"] * len(vals))
                    where_parts.append(
                        f"(r.symbol IN ({placeholders}) OR s.base_asset IN ({placeholders}))"
                    )
                    where_params.extend(list(vals) + list(vals))
                continue
            if key in allowed_filters:
                sql_expr = allowed_filters[key]["sql"]
                if isinstance(value, (list, tuple, set)):
                    vals = [v for v in value if v is not None and str(v).strip() != ""]
                    if vals:
                        placeholders = ",".join(["%s"] * len(vals))
                        where_parts.append(f"{sql_expr} IN ({placeholders})")
                        where_params.extend(vals)
                else:
                    where_parts.append(f"{sql_expr} = %s")
                    where_params.append(value)

    if user_id and "user_id" in run_cols:
        where_parts.append("r.user_id = %s")
        where_params.append(str(user_id))

    where_clause = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    return where_clause, where_params


def translate_filter_model(
    filter_model: Optional[Dict[str, Any]],
    allowed_filters: Dict[str, Dict[str, str]],
) -> Tuple[str, List[Any]]:
    if not isinstance(filter_model, dict) or not filter_model:
        return "", []

    clauses: List[str] = []
    params: List[Any] = []

    def _apply_simple(model: Dict[str, Any], sql_expr: str, ftype: str) -> Optional[str]:
        op = str(model.get("type") or "").strip()
        if ftype == "set":
            values = model.get("values") or []
            if not isinstance(values, list) or not values:
                return None
            placeholders = ",".join(["%s"] * len(values))
            if any(isinstance(v, str) for v in values):
                params.extend([str(v).lower() for v in values])
                return f"LOWER({sql_expr}) IN ({placeholders})"
            params.extend([v for v in values])
            return f"{sql_expr} IN ({placeholders})"

        if op == "blank":
            if ftype == "number":
                return f"{sql_expr} IS NULL"
            return f"({sql_expr} IS NULL OR {sql_expr} = '')"
        if op == "notBlank":
            if ftype == "number":
                return f"{sql_expr} IS NOT NULL"
            return f"({sql_expr} IS NOT NULL AND {sql_expr} <> '')"

        val = model.get("filter")
        val_to = model.get("filterTo") or model.get("dateTo")
        if ftype == "date":
            val = model.get("dateFrom") or val

        if op in {"equals", "notEqual"}:
            params.append(val)
            return f"{sql_expr} {'!=' if op == 'notEqual' else '='} %s"
        if op == "contains":
            params.append(f"%{val}%")
            return f"{sql_expr} ILIKE %s"
        if op == "startsWith":
            params.append(f"{val}%")
            return f"{sql_expr} ILIKE %s"
        if op == "endsWith":
            params.append(f"%{val}")
            return f"{sql_expr} ILIKE %s"
        if op == "lessThan":
            params.append(val)
            return f"{sql_expr} < %s"
        if op == "greaterThan":
            params.append(val)
            return f"{sql_expr} > %s"
        if op == "lessThanOrEqual":
            params.append(val)
            return f"{sql_expr} <= %s"
        if op == "greaterThanOrEqual":
            params.append(val)
            return f"{sql_expr} >= %s"
        if op == "inRange":
            params.extend([val, val_to])
            return f"{sql_expr} BETWEEN %s AND %s"
        return None

    def _walk(model: Dict[str, Any], sql_expr: str, ftype: str) -> Optional[str]:
        if not isinstance(model, dict):
            return None
        ftype_local = ftype
        filter_type = str(model.get("filterType") or "").strip().lower()
        if filter_type in {"text", "number", "date", "set"}:
            ftype_local = filter_type
        if model.get("operator") and model.get("condition1") and model.get("condition2"):
            cond1 = _walk(model.get("condition1") or {}, sql_expr, ftype_local)
            cond2 = _walk(model.get("condition2") or {}, sql_expr, ftype_local)
            if cond1 and cond2:
                op = str(model.get("operator") or "AND").upper()
                if op not in {"AND", "OR"}:
                    op = "AND"
                return f"({cond1} {op} {cond2})"
            return cond1 or cond2
        return _apply_simple(model, sql_expr, ftype_local)

    for col, model in filter_model.items():
        if col not in allowed_filters:
            continue
        meta = allowed_filters[col]
        sql_expr = meta.get("sql")
        ftype = meta.get("type")
        if not sql_expr or not ftype:
            continue
        clause = _walk(model if isinstance(model, dict) else {}, sql_expr, str(ftype))
        if clause:
            clauses.append(clause)

    if not clauses:
        return "", []
    return " AND ".join(clauses), params


def translate_sort_model(
    sort_model: Optional[List[Dict[str, Any]]],
    allowed_filters: Dict[str, Dict[str, str]],
) -> str:
    if not isinstance(sort_model, list) or not sort_model:
        return "ORDER BY r.created_at DESC"
    order_parts: List[str] = []
    for item in sort_model:
        if not isinstance(item, dict):
            continue
        col = item.get("colId") or item.get("field")
        if col not in allowed_filters:
            continue
        sql_expr = allowed_filters[col].get("sql")
        if not sql_expr:
            continue
        direction = str(item.get("sort") or "desc").lower()
        if direction not in {"asc", "desc"}:
            direction = "desc"
        order_parts.append(f"{sql_expr} {direction.upper()}")
    if not order_parts:
        return "ORDER BY r.created_at DESC"
    return "ORDER BY " + ", ".join(order_parts)


def list_runs_server_side(
    conn: Any,
    page: int,
    page_size: int,
    filter_model: Optional[Dict[str, Any]] = None,
    sort_model: Optional[List[Dict[str, Any]]] = None,
    visible_columns: Optional[List[str]] = None,
    user_id: Optional[str] = None,
    extra_where: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    try:
        run_cols = get_table_columns(conn, "backtest_runs")
        allowed_filters = _runs_allowed_filter_map(run_cols)
        allowed_select = _runs_allowed_select_map(run_cols)

        req_cols = {"run_id", "created_at", "symbol", "timeframe", "strategy_name"}
        if visible_columns:
            selected = [c for c in visible_columns if c in allowed_select]
        else:
            selected = []
        for col in req_cols:
            if col in allowed_select and col not in selected:
                selected.append(col)
        if "config_json" in allowed_select and "config_json" not in selected:
            selected.append("config_json")
        if "start_time" in allowed_select and "start_time" not in selected:
            selected.append("start_time")
        if "end_time" in allowed_select and "end_time" not in selected:
            selected.append("end_time")

        select_sql = ", ".join([allowed_select[c] for c in selected])
        if not select_sql:
            select_sql = "r.id AS run_id, r.created_at"

        where_clause, where_params = _build_runs_where(
            filter_model=filter_model,
            allowed_filters=allowed_filters,
            extra_where=extra_where,
            user_id=user_id,
            run_cols=run_cols,
        )

        order_sql = translate_sort_model(sort_model, allowed_filters)

        page_val = max(1, int(page or 1))
        size_val = max(1, min(int(page_size or 50), 500))
        offset_val = (page_val - 1) * size_val

        shortlist_join = ""
        select_params: List[Any] = list(where_params)
        if user_id:
            shortlist_join = "LEFT JOIN run_shortlists rs ON rs.run_id = r.id AND rs.user_id = %s"
            select_params = [str(user_id)] + select_params

        sql = f"""
            SELECT {select_sql}
            FROM backtest_runs r
            LEFT JOIN sweeps sw ON sw.id = r.sweep_id
            LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
            {shortlist_join}
            {where_clause}
            {order_sql}
            LIMIT %s OFFSET %s
        """
        data_params = list(select_params) + [size_val, offset_val]

        rows = execute_fetchall(conn, sql, tuple(data_params))

        count_sql = (
            "SELECT COUNT(1) "
            "FROM backtest_runs r "
            "LEFT JOIN sweeps sw ON sw.id = r.sweep_id "
            "LEFT JOIN symbols s ON s.exchange_symbol = r.symbol "
            f"{where_clause}"
        )
        total_row = conn.execute(count_sql, tuple(where_params)).fetchone()
        total_count = int(total_row[0] or 0) if total_row else 0

        # Ensure symbols for current page (icon lookup) without full table scan.
        try:
            symbols = []
            for r in rows or []:
                s = str((r.get("symbol") if isinstance(r, dict) else None) or "").strip()
                if s:
                    symbols.append(s)
            if symbols:
                ensure_symbols_for_exchange_symbols(conn, list(dict.fromkeys(symbols)))
        except Exception:
            pass

        return [dict(r) for r in rows], int(total_count)
    except Exception:
        _rollback_quietly(conn)
        logger.exception("list_runs_server_side failed")
        raise


def list_backtest_run_symbols(
    conn: Any,
    *,
    filter_model: Optional[Dict[str, Any]] = None,
    extra_where: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    limit: int = 500,
) -> List[str]:
    run_cols = get_table_columns(conn, "backtest_runs")
    allowed_filters = _runs_allowed_filter_map(run_cols)
    where_clause, params = _build_runs_where(
        filter_model=filter_model,
        allowed_filters=allowed_filters,
        extra_where=extra_where,
        user_id=user_id,
        run_cols=run_cols,
        exclude_keys={"symbol", "symbol_contains"},
    )
    lim = max(1, min(int(limit or 500), 5000))
    sql = f"""
        SELECT DISTINCT COALESCE(s.base_asset, r.symbol) AS symbol
        FROM backtest_runs r
        LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        {where_clause}
        ORDER BY 1
        LIMIT %s
    """
    rows = execute_fetchall(conn, sql, tuple(list(params) + [lim]))
    out: List[str] = []
    for r in rows or []:
        v = r.get("symbol") if isinstance(r, dict) else None
        s = ("" if v is None else str(v)).strip()
        if s:
            out.append(s)
    return out


def list_backtest_run_timeframes(
    conn: Any,
    *,
    filter_model: Optional[Dict[str, Any]] = None,
    extra_where: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    limit: int = 200,
) -> List[str]:
    run_cols = get_table_columns(conn, "backtest_runs")
    allowed_filters = _runs_allowed_filter_map(run_cols)
    where_clause, params = _build_runs_where(
        filter_model=filter_model,
        allowed_filters=allowed_filters,
        extra_where=extra_where,
        user_id=user_id,
        run_cols=run_cols,
        exclude_keys={"timeframe", "timeframe_contains"},
    )
    lim = max(1, min(int(limit or 200), 2000))
    sql = f"""
        SELECT DISTINCT r.timeframe
        FROM backtest_runs r
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        {where_clause}
        ORDER BY r.timeframe
        LIMIT %s
    """
    rows = execute_fetchall(conn, sql, tuple(list(params) + [lim]))
    out: List[str] = []
    for r in rows or []:
        v = r.get("timeframe") if isinstance(r, dict) else None
        s = ("" if v is None else str(v)).strip()
        if s:
            out.append(s)
    return out


def list_backtest_run_strategies_filtered(
    conn: Any,
    *,
    filter_model: Optional[Dict[str, Any]] = None,
    extra_where: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
    limit: int = 500,
) -> List[str]:
    run_cols = get_table_columns(conn, "backtest_runs")
    allowed_filters = _runs_allowed_filter_map(run_cols)
    where_clause, params = _build_runs_where(
        filter_model=filter_model,
        allowed_filters=allowed_filters,
        extra_where=extra_where,
        user_id=user_id,
        run_cols=run_cols,
        exclude_keys={"strategy", "strategy_name", "strategy_contains"},
    )
    lim = max(1, min(int(limit or 500), 5000))
    sql = f"""
        SELECT DISTINCT r.strategy_name
        FROM backtest_runs r
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        {where_clause}
        ORDER BY r.strategy_name
        LIMIT %s
    """
    rows = execute_fetchall(conn, sql, tuple(list(params) + [lim]))
    out: List[str] = []
    for r in rows or []:
        v = r.get("strategy_name") if isinstance(r, dict) else None
        s = ("" if v is None else str(v)).strip()
        if s:
            out.append(s)
    return out


def get_runs_numeric_bounds(
    conn: Any,
    *,
    filter_model: Optional[Dict[str, Any]] = None,
    extra_where: Optional[Dict[str, Any]] = None,
    user_id: Optional[str] = None,
) -> Dict[str, Optional[float]]:
    run_cols = get_table_columns(conn, "backtest_runs")
    allowed_filters = _runs_allowed_filter_map(run_cols)
    where_clause, params = _build_runs_where(
        filter_model=filter_model,
        allowed_filters=allowed_filters,
        extra_where=extra_where,
        user_id=user_id,
        run_cols=run_cols,
        exclude_keys={
            "net_profit_min",
            "net_profit_max",
            "net_return_pct_min",
            "net_return_pct_max",
            "roi_pct_on_margin_min",
            "roi_pct_on_margin_max",
            "max_drawdown_pct_min",
            "max_drawdown_pct_max",
            "duration_min_hours",
            "duration_max_hours",
        },
    )
    sql = f"""
        SELECT
            MIN(r.net_profit) AS net_profit_min,
            MAX(r.net_profit) AS net_profit_max,
            MIN(r.net_return_pct) AS net_return_pct_min,
            MAX(r.net_return_pct) AS net_return_pct_max,
            MIN({_runs_duration_seconds_sql()}) AS duration_sec_min,
            MAX({_runs_duration_seconds_sql()}) AS duration_sec_max
        FROM backtest_runs r
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        {where_clause}
    """
    row = execute_fetchone(conn, sql, tuple(params)) or {}
    out: Dict[str, Optional[float]] = {
        "net_profit_min": float(row.get("net_profit_min")) if row.get("net_profit_min") is not None else None,
        "net_profit_max": float(row.get("net_profit_max")) if row.get("net_profit_max") is not None else None,
        "net_return_pct_min": float(row.get("net_return_pct_min")) if row.get("net_return_pct_min") is not None else None,
        "net_return_pct_max": float(row.get("net_return_pct_max")) if row.get("net_return_pct_max") is not None else None,
        "duration_hours_min": float(row.get("duration_sec_min")) / 3600.0 if row.get("duration_sec_min") is not None else None,
        "duration_hours_max": float(row.get("duration_sec_max")) / 3600.0 if row.get("duration_sec_max") is not None else None,
    }
    return out


def list_backtest_run_strategies(*, limit: int = 5000) -> List[str]:
    """List distinct strategy_name values seen in backtest_runs.

    This is intended for UI filter dropdowns.
    """

    lim = max(1, min(int(limit or 5000), 50000))
    with _get_conn() as conn:
        _ensure_schema(conn)
        rows = execute_fetchall(
            conn,
            "SELECT DISTINCT strategy_name FROM backtest_runs WHERE strategy_name IS NOT NULL ORDER BY strategy_name LIMIT %s",
            (lim,),
        )
    out: List[str] = []
    for r in rows or []:
        try:
            s = str(r[0] or "").strip()
        except Exception:
            s = ""
        if s:
            out.append(s)
    return out


def get_runs_explorer_rows(
    user_id: str,
    filters: Dict[str, Any] | None,
    *,
    limit: int = 500,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], int]:
    """Runs Explorer read API.

    Returns (rows, total_count).

    - Uses a single SQL query for rows+total_count via a window function.
    - No per-row DB calls.
    - Returns config_json and metrics_json (preferring backtest_run_details when available).

    Filters (all optional):
    - strategy_name: str
    - symbols: list[str]
    - sweep_id: str|int
    - created_from: ISO date/time string (inclusive)
    - created_to: ISO date/time string (exclusive)
    """

    _ = (user_id or "").strip()  # reserved for future per-user scoping
    lim = max(1, min(int(limit or 500), 5000))
    off = max(0, int(offset or 0))
    filters = filters or {}

    where: List[str] = []
    params: List[Any] = []

    strategy_name = filters.get("strategy_name")
    if strategy_name:
        where.append("r.strategy_name = %s")
        params.append(str(strategy_name))

    sweep_id = filters.get("sweep_id")
    if sweep_id is not None and str(sweep_id).strip() != "":
        where.append("r.sweep_id = %s")
        params.append(sweep_id)

    created_from = filters.get("created_from")
    if created_from:
        where.append("r.created_at >= %s")
        params.append(str(created_from))

    created_to = filters.get("created_to")
    if created_to:
        where.append("r.created_at < %s")
        params.append(str(created_to))

    symbols = filters.get("symbols")
    if isinstance(symbols, list) and symbols:
        syms = [str(s).strip() for s in symbols if str(s).strip()]
        if syms:
            where.append("r.symbol IN (" + ",".join(["%s"] * len(syms)) + ")")
            params.extend(syms)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
        SELECT
            r.id AS run_id,
            r.created_at,
            r.start_time,
            r.end_time,
            r.symbol,
            r.timeframe,
            r.sweep_id,
            r.strategy_name,
            r.strategy_version,
            r.market_type,
            r.net_profit,
            r.net_return_pct,
            r.roi_pct_on_margin,
            r.max_drawdown_pct,
            r.sharpe,
            r.sortino,
            r.win_rate,
            r.profit_factor,
            r.cpc_index,
            r.common_sense_ratio,
            COALESCE(d.config_jsonb, d.config_json::jsonb, r.config_jsonb, r.config_json::jsonb)::text AS config_json,
            COALESCE(d.metrics_jsonb, d.metrics_json::jsonb, r.metrics_jsonb, r.metrics_json::jsonb)::text AS metrics_json,
            COALESCE(d.metadata_jsonb, d.metadata_json::jsonb, r.metadata_jsonb, r.metadata_json::jsonb)::text AS metadata_json,
            sw.name AS sweep_name,
            sw.exchange_id AS sweep_exchange_id,
            s.base_asset,
            s.quote_asset,
            s.icon_uri,
            COUNT(1) OVER() AS total_count
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        LEFT JOIN sweeps sw ON sw.id = r.sweep_id
        LEFT JOIN symbols s ON s.exchange_symbol = r.symbol
        {where_sql}
        ORDER BY r.created_at DESC
        LIMIT %s OFFSET %s
    """
    params2 = list(params)
    params2.extend([lim, off])

    with _get_conn() as conn:
        _ensure_schema(conn)
        with profile_span(
            "db.get_runs_explorer_rows",
            meta={
                "limit": lim,
                "offset": off,
                "has_filters": bool(filters),
            },
        ):
            rows = execute_fetchall(conn, sql, tuple(params2))

    if not rows:
        return [], 0

    total = 0
    try:
        total = int(rows[0].get("total_count") or 0)
    except Exception:
        try:
            total = int(dict(rows[0]).get("total_count") or 0)
        except Exception:
            total = 0

    out: List[Dict[str, Any]] = []
    for r in rows:
        drow = dict(r)
        drow.pop("total_count", None)
        out.append(drow)
    return out, total


def get_backtest_run(run_id: str) -> Optional[Dict[str, Any]]:
    with _get_conn() as conn:
        row = execute_fetchone(
            conn,
            """
                 SELECT id, created_at, symbol, timeframe, strategy_name, strategy_version,
                     start_time, end_time, net_profit, net_return_pct, max_drawdown_pct,
                     sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
                     sweep_id
            FROM backtest_runs
            WHERE id = %s
            """,
            (run_id,),
        )

        if row is None:
            return None

        details = get_backtest_run_details(str(run_id), conn=conn) or {
            "config": {},
            "metrics": {},
            "metadata": {},
            "trades_json": None,
        }

        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "symbol": row["symbol"],
            "timeframe": row["timeframe"],
            "strategy_name": row["strategy_name"],
            "strategy_version": row["strategy_version"],
            "config": details.get("config") or {},
            "metrics": details.get("metrics") or {},
            "metadata": details.get("metadata") or {},
            "start_time": row["start_time"],
            "end_time": row["end_time"],
            "net_profit": row["net_profit"],
            "net_return_pct": row["net_return_pct"],
            "max_drawdown_pct": row["max_drawdown_pct"],
            "sharpe": row["sharpe"],
            "sortino": row["sortino"],
            "win_rate": row["win_rate"],
            "profit_factor": row["profit_factor"],
            "cpc_index": row["cpc_index"],
            "common_sense_ratio": row["common_sense_ratio"],
            "trades_json": details.get("trades_json"),
            "sweep_id": row["sweep_id"],
        }


def _sanitize_for_json(value: Any) -> Any:
    if is_dataclass(value):
        return _sanitize_for_json(asdict(value))
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _coerce_json_obj(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None
    return value


def _json_text_from_obj(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(_sanitize_for_json(value))
    except Exception:
        return None


def to_jsonb(value: Any) -> Any:
    if value is None:
        return None
    obj = _sanitize_for_json(value)
    try:
        if psycopg is not None:
            from psycopg.types.json import Jsonb  # type: ignore

            return Jsonb(obj)
    except Exception:
        pass
    try:
        if psycopg2_extras is not None:
            return psycopg2_extras.Json(obj, dumps=json.dumps)
    except Exception:
        pass
    return obj


def _load_json_field(row: Mapping[str, Any], base: str, default: Any = None) -> Any:
    if default is None:
        default = {}
    jsonb_key = f"{base}_jsonb"
    text_key = f"{base}_json"
    if jsonb_key in row and row.get(jsonb_key) is not None:
        value = row.get(jsonb_key)
        obj = _coerce_json_obj(value)
        return obj if obj is not None else default
    if text_key in row and row.get(text_key) is not None:
        value = row.get(text_key)
        obj = _coerce_json_obj(value)
        return obj if obj is not None else default
    return default


def _safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_iso8601(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    return None
