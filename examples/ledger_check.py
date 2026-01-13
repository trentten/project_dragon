"""Quick sanity checks for bot_ledger.

Runs locally against a throwaway SQLite db in /tmp.
Validates:
- Schema creates fine
- add_ledger_row is idempotent for (bot_id, kind, ref_id)
- sum_ledger aggregates correctly

Usage:
  python -m examples.ledger_check
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from project_dragon.storage import add_ledger_row, open_db_connection, sum_ledger


def main() -> None:
    fd, path = tempfile.mkstemp(prefix="dragon_ledger_", suffix=".sqlite")
    os.close(fd)
    try:
        with open_db_connection(Path(path)) as conn:
            conn.row_factory = sqlite3.Row

            bot_id = 1
            # funding idempotency
            inserted_1 = add_ledger_row(
                conn,
                bot_id=bot_id,
                event_ts="2025-01-01T00:00:00+00:00",
                kind="funding",
                symbol="BTC-PERP",
                funding=1.25,
                ref_id="funding-1",
                meta={"note": "first"},
            )
            inserted_2 = add_ledger_row(
                conn,
                bot_id=bot_id,
                event_ts="2025-01-01T00:00:00+00:00",
                kind="funding",
                symbol="BTC-PERP",
                funding=1.25,
                ref_id="funding-1",
                meta={"note": "duplicate"},
            )

            # fee + realized
            add_ledger_row(
                conn,
                bot_id=bot_id,
                event_ts="2025-01-01T00:01:00+00:00",
                kind="fee",
                symbol="BTC-PERP",
                fee=0.10,
                ref_id="fee-1",
                meta=None,
            )
            add_ledger_row(
                conn,
                bot_id=bot_id,
                event_ts="2025-01-01T00:02:00+00:00",
                kind="adjustment",
                symbol="BTC-PERP",
                pnl=5.00,
                ref_id="pnl-1",
                meta={"source": "test"},
            )

            totals = sum_ledger(conn, bot_id)

            print(f"DB: {path}")
            print(f"inserted_1={inserted_1} inserted_2={inserted_2} (should be True then False)")
            print("totals:", totals)

            assert inserted_1 is True
            assert inserted_2 is False
            assert abs(float(totals["funding_total"]) - 1.25) < 1e-9
            assert abs(float(totals["fees_total"]) - 0.10) < 1e-9
            assert abs(float(totals["realized_total"]) - 5.00) < 1e-9

            print("OK")
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
