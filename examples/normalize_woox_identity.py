from __future__ import annotations

import os
import sys

from project_dragon.storage import get_database_url, is_postgres_dsn, open_db_connection, normalize_woox_identity


def main() -> int:
    dsn = str(get_database_url() or "").strip()
    if not is_postgres_dsn(dsn):
        print("DRAGON_DATABASE_URL must be set to a postgres:// or postgresql:// URL", file=sys.stderr)
        return 2

    with open_db_connection() as conn:
        stats = normalize_woox_identity(conn)
        try:
            conn.commit()
        except Exception:
            pass

    summary = ", ".join([f"{k}={v}" for k, v in sorted(stats.items())]) or "no changes"
    print(f"WooX normalization complete: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
