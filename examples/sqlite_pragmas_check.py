"""Print key SQLite PRAGMA settings.

This is a small debug utility to confirm our connection hardening is active.

Usage:
  python examples/sqlite_pragmas_check.py
  python examples/sqlite_pragmas_check.py --db data/backtests.sqlite
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from project_dragon.storage import get_sqlite_pragmas, open_db_connection


def main() -> int:
    parser = argparse.ArgumentParser(description="Print key SQLite PRAGMAs")
    parser.add_argument("--db", type=str, default=None, help="Path to sqlite db")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve() if args.db else None

    with open_db_connection(db_path) as conn:
        pragmas = get_sqlite_pragmas(conn)
        print("SQLite PRAGMAs:")
        for k in ("journal_mode", "busy_timeout", "foreign_keys", "synchronous"):
            print(f"- {k}: {pragmas.get(k)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
