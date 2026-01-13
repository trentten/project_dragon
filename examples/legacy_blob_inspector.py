"""Legacy blob inspector/backfill tool.

This script is intentionally the *only* place in the repo allowed to read the
legacy JSON blob columns directly from `backtest_runs`:
- config_json
- metrics_json
- trades_json
- metadata_json

It exists to help with auditing and migrating older DBs after introducing
`backtest_run_details` as the source-of-truth for run blobs.

Usage examples:
  python examples/legacy_blob_inspector.py report
  python examples/legacy_blob_inspector.py list-missing --limit 50
  python examples/legacy_blob_inspector.py backfill --batch 500
  python examples/legacy_blob_inspector.py verify

Optional DB selection:
  python examples/legacy_blob_inspector.py report --db data/backtests.sqlite
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import sqlite3

from project_dragon.storage import open_db_connection


def _print_rows(rows: Iterable[sqlite3.Row], *, limit: int = 20) -> None:
    n = 0
    for r in rows:
        if n >= limit:
            break
        run_id = r["id"] if "id" in r.keys() else r[0]
        missing_reason = r.get("missing_reason") if hasattr(r, "get") else None
        print(f"- {run_id}" + (f" ({missing_reason})" if missing_reason else ""))
        n += 1


def _count(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def report(conn: sqlite3.Connection) -> int:
    total_runs = _count(conn, "SELECT COUNT(1) FROM backtest_runs")
    total_details = _count(conn, "SELECT COUNT(1) FROM backtest_run_details")
    missing_details = _count(
        conn,
        """
        SELECT COUNT(1)
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        """,
    )

    print("Backtest run blobs migration status")
    print(f"- backtest_runs rows:        {total_runs}")
    print(f"- backtest_run_details rows: {total_details}")
    print(f"- missing details rows:      {missing_details}")

    # Spot-check whether legacy columns look non-trivial on missing rows.
    nontrivial_legacy_missing = _count(
        conn,
        """
        SELECT COUNT(1)
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
          AND (
            LENGTH(COALESCE(r.config_json,'')) > 4
            OR LENGTH(COALESCE(r.metrics_json,'')) > 4
            OR LENGTH(COALESCE(r.metadata_json,'')) > 4
            OR LENGTH(COALESCE(r.trades_json,'')) > 4
          )
        """,
    )
    print(f"- missing w/ non-trivial legacy blobs: {nontrivial_legacy_missing}")

    return 0


def list_missing(conn: sqlite3.Connection, *, limit: int) -> int:
    rows = conn.execute(
        """
        SELECT
          r.id,
          CASE
            WHEN d.run_id IS NULL THEN 'no_details_row'
            ELSE 'unknown'
          END AS missing_reason
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        ORDER BY r.created_at DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    if not rows:
        print("No missing details rows.")
        return 0

    print(f"Missing details rows (showing up to {limit}):")
    _print_rows(rows, limit=limit)
    return 0


def backfill(conn: sqlite3.Connection, *, batch: int, dry_run: bool) -> int:
    # NOTE: This is intentionally the only place allowed to read these legacy columns.
    rows = conn.execute(
        """
        SELECT r.id, r.config_json, r.metrics_json, r.trades_json, r.metadata_json
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        ORDER BY r.created_at DESC
        LIMIT ?
        """,
        (int(batch),),
    ).fetchall()

    if not rows:
        print("No missing details rows to backfill.")
        return 0

    # Filter out rows where legacy blobs are only placeholders.
    payloads: list[tuple[str, str, str, Optional[str], str]] = []
    skipped_placeholders = 0
    for r in rows:
        rid = str(r["id"])
        cfg = r["config_json"] or "{}"
        met = r["metrics_json"] or "{}"
        md = r["metadata_json"] or "{}"
        trades = r["trades_json"]

        # Treat "{}" (and empty/None) as placeholder.
        legacy_has_data = (
            (isinstance(cfg, str) and len(cfg.strip()) > 4)
            or (isinstance(met, str) and len(met.strip()) > 4)
            or (isinstance(md, str) and len(md.strip()) > 4)
            or (isinstance(trades, str) and len(trades.strip()) > 4)
        )
        if not legacy_has_data:
            skipped_placeholders += 1
            continue

        # Basic sanity: keep valid JSON strings when possible.
        for blob in (cfg, met, md):
            try:
                json.loads(blob)
            except Exception:
                # Still store it; inspector is for rescue, not strict validation.
                pass

        payloads.append((rid, cfg, met, trades, md))

    print(f"Found {len(rows)} missing details rows")
    print(f"- backfillable (non-placeholder legacy blobs): {len(payloads)}")
    print(f"- skipped placeholder-only legacy blobs:      {skipped_placeholders}")

    if dry_run:
        print("Dry-run: not writing anything.")
        return 0

    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO backtest_run_details(run_id, config_json, metrics_json, trades_json, metadata_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            payloads,
        )

    print("Backfill done.")
    return 0


def verify(conn: sqlite3.Connection) -> int:
    missing = _count(
        conn,
        """
        SELECT COUNT(1)
        FROM backtest_runs r
        LEFT JOIN backtest_run_details d ON d.run_id = r.id
        WHERE d.run_id IS NULL
        """,
    )
    if missing:
        print(f"Verification: still missing {missing} details rows.")
        print("Tip: run `backfill` or check why legacy blobs are placeholders.")
        return 1

    # Lightweight integrity: ensure required columns are non-empty JSON-ish.
    bad = _count(
        conn,
        """
        SELECT COUNT(1)
        FROM backtest_run_details
        WHERE TRIM(COALESCE(config_json,'')) = ''
           OR TRIM(COALESCE(metrics_json,'')) = ''
        """,
    )

    if bad:
        print(f"Verification: found {bad} details rows with empty required blobs.")
        return 1

    print("Verification OK: all backtest_runs have a backtest_run_details row.")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect/backfill legacy backtest run blob columns")
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Path to SQLite DB (defaults to Project Dragon data/backtests.sqlite)",
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("report", help="Print high-level migration status")

    p_list = sub.add_parser("list-missing", help="List run_ids missing details rows")
    p_list.add_argument("--limit", type=int, default=50)

    p_backfill = sub.add_parser("backfill", help="Backfill missing details rows from legacy columns")
    p_backfill.add_argument("--batch", type=int, default=500)
    p_backfill.add_argument("--dry-run", action="store_true")

    sub.add_parser("verify", help="Verify all runs have details rows")

    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db).expanduser().resolve() if args.db else None

    with open_db_connection(db_path) as conn:
        # Ensure we can index by column name.
        conn.row_factory = sqlite3.Row

        if args.cmd == "report":
            return report(conn)
        if args.cmd == "list-missing":
            return list_missing(conn, limit=int(args.limit))
        if args.cmd == "backfill":
            return backfill(conn, batch=int(args.batch), dry_run=bool(args.dry_run))
        if args.cmd == "verify":
            return verify(conn)

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
