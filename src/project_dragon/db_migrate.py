from __future__ import annotations

import sys
from urllib.parse import urlparse

from project_dragon.storage import (
    apply_migrations,
    get_database_url,
    is_postgres_dsn,
    open_postgres_connection,
    PostgresConnectionAdapter,
    PostgresConnectionAdapterV3,
    psycopg,
)


def _describe_target(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
    except Exception:
        return "host=unknown db=unknown"
    host = parsed.hostname or ""
    port = parsed.port
    db = (parsed.path or "").lstrip("/")
    if port:
        return f"host={host}:{port} db={db}"
    return f"host={host} db={db}"


def _role_from_dsn(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        return (parsed.username or "dragon").strip() or "dragon"
    except Exception:
        return "dragon"


def main() -> int:
    dsn = str(get_database_url() or "").strip()
    if not dsn:
        print("DB migrate failed: DRAGON_DATABASE_URL is not set.", file=sys.stderr)
        return 2
    if not is_postgres_dsn(dsn):
        print("DB migrate failed: DRAGON_DATABASE_URL must be postgres:// or postgresql://", file=sys.stderr)
        return 2

    conn = None
    try:
        print(f"DB migrate target: {_describe_target(dsn)}")
        raw = open_postgres_connection(dsn)
        if psycopg is not None and isinstance(raw, getattr(psycopg, "Connection", object)):
            conn = PostgresConnectionAdapterV3(raw)
        else:
            conn = PostgresConnectionAdapter(raw)
        apply_migrations(conn)
        try:
            role = _role_from_dsn(dsn)
            conn.execute(
                f"ALTER ROLE {role} SET idle_in_transaction_session_timeout = '30s'"
            )
            conn.execute(
                f"ALTER ROLE {role} SET statement_timeout = '30s'"
            )
            conn.commit()
            print(f"DB role timeouts applied for {role}")
        except Exception as exc:
            print(f"DB role timeout setup skipped: {exc}")
        print("DB migrations ok")
        return 0
    except Exception as exc:
        print(f"DB migrate failed: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
