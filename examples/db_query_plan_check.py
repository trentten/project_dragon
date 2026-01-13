"""SQLite EXPLAIN QUERY PLAN sanity checks.

This prints the query plans for the key list/detail queries that should be
index-friendly, especially after:
- storing created_at as ISO8601
- avoiding ORDER BY datetime(created_at)
- keeping list views summary-only

Usage:
  python examples/db_query_plan_check.py
  python examples/db_query_plan_check.py --db data/backtests.sqlite

Machine-readable mode:
    python examples/db_query_plan_check.py --json
    python examples/db_query_plan_check.py --json --fail

Optional filters:
  python examples/db_query_plan_check.py --symbol BTC/USDT --timeframe 1h
  python examples/db_query_plan_check.py --sweep-id <uuid>
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import sqlite3

from project_dragon.storage import open_db_connection


def _collect_plan(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> List[str]:
    rows = conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall()
    details: List[str] = []
    for r in rows:
        detail = r[3] if len(r) >= 4 else str(r)
        details.append(str(detail))
    return details


def _has_any(details: Sequence[str], patterns: Sequence[str]) -> bool:
    if not patterns:
        return True
    hay = "\n".join(details).upper()
    for p in patterns:
        if p is None:
            continue
        if str(p).upper() in hay:
            return True
    return False


def _has_any_regex(details: Sequence[str], patterns: Sequence[str]) -> bool:
    if not patterns:
        return True
    hay = "\n".join(details)
    for p in patterns:
        if not p:
            continue
        if re.search(p, hay, flags=re.IGNORECASE):
            return True
    return False


def _find_forbidden(details: Sequence[str], forbidden_patterns: Sequence[str]) -> List[str]:
    hay = "\n".join(details)
    hits: List[str] = []
    for p in forbidden_patterns or []:
        if not p:
            continue
        if re.search(p, hay, flags=re.IGNORECASE):
            hits.append(p)
    return hits


def _check_plan(
    *,
    conn: sqlite3.Connection,
    title: str,
    sql: str,
    params: tuple[Any, ...] = (),
    require_any: Sequence[str] = (),
    require_any_regex: Sequence[str] = (),
    forbid_regex: Sequence[str] = (),
) -> Dict[str, Any]:
    plan_details = _collect_plan(conn, sql, params)
    ok_required = _has_any(plan_details, require_any) and _has_any_regex(plan_details, require_any_regex)
    forbidden_hits = _find_forbidden(plan_details, forbid_regex)
    ok = bool(ok_required) and not forbidden_hits
    return {
        "title": title,
        "sql": " ".join(line.strip() for line in sql.strip().splitlines()),
        "params": list(params),
        "plan": plan_details,
        "ok": ok,
        "require_any": list(require_any),
        "require_any_regex": list(require_any_regex),
        "forbid_regex": list(forbid_regex),
        "forbidden_hits": forbidden_hits,
    }


def _fails_full_scan_plus_temp_sort(details: Sequence[str], *, table: str) -> bool:
    """Return True if the plan indicates a full scan AND a temp sort for ORDER BY."""

    hay = "\n".join(details)
    scan = re.search(rf"\bSCAN\b.*\b{re.escape(table)}\b", hay, flags=re.IGNORECASE) is not None
    using_index = re.search(r"\bUSING\s+INDEX\b", hay, flags=re.IGNORECASE) is not None
    temp_sort = re.search(r"USE\s+TEMP\s+B-TREE\s+FOR\s+ORDER\s+BY", hay, flags=re.IGNORECASE) is not None
    return bool(scan and (not using_index) and temp_sort)


def _check_no_full_scan_temp_sort(
    *,
    conn: sqlite3.Connection,
    title: str,
    sql: str,
    params: tuple[Any, ...] = (),
    table: str,
) -> Dict[str, Any]:
    plan_details = _collect_plan(conn, sql, params)
    bad = _fails_full_scan_plus_temp_sort(plan_details, table=table)
    return {
        "title": title,
        "sql": " ".join(line.strip() for line in sql.strip().splitlines()),
        "params": list(params),
        "plan": plan_details,
        "ok": not bad,
        "require_any": [],
        "require_any_regex": [],
        "forbid_regex": ["full-scan+temp-sort"],
        "forbidden_hits": ["full-scan+temp-sort"] if bad else [],
    }


def _first_value(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Optional[str]:
    row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    v = row[0]
    return str(v) if v is not None else None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print SQLite query plans for key Project Dragon queries")
    p.add_argument("--db", type=str, default=None, help="Path to SQLite DB")
    p.add_argument("--symbol", type=str, default=None)
    p.add_argument("--timeframe", type=str, default=None)
    p.add_argument("--sweep-id", type=str, default=None)
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    p.add_argument("--fail", action="store_true", help="Exit non-zero if any plan check fails")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    db_path = Path(args.db).expanduser().resolve() if args.db else None

    with open_db_connection(db_path) as conn:
        conn.row_factory = None  # raw tuples for explain output

        symbol = args.symbol
        timeframe = args.timeframe
        sweep_id = args.sweep_id

        if not symbol:
            symbol = _first_value(conn, "SELECT symbol FROM backtest_runs WHERE symbol IS NOT NULL LIMIT 1")
        if not timeframe:
            timeframe = _first_value(conn, "SELECT timeframe FROM backtest_runs WHERE timeframe IS NOT NULL LIMIT 1")
        if not sweep_id:
            sweep_id = _first_value(conn, "SELECT id FROM sweeps ORDER BY created_at DESC LIMIT 1")

        results: List[Dict[str, Any]] = []

        results.append(
            _check_no_full_scan_temp_sort(
                conn=conn,
                title="Run summaries (no filters)",
                sql="""
            SELECT
              id, created_at, symbol, timeframe, strategy_name, strategy_version,
              trade_direction,
              start_time, end_time,
              net_profit, net_return_pct, max_drawdown_pct,
              sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
              sweep_id
            FROM backtest_runs
            ORDER BY created_at DESC
            LIMIT ?
            """,
                params=(5000,),
                table="backtest_runs",
            )
        )

        if symbol and timeframe:
            results.append(
                _check_no_full_scan_temp_sort(
                    conn=conn,
                    title="Run summaries (symbol+timeframe)",
                    sql="""
                SELECT
                  id, created_at, symbol, timeframe, strategy_name, strategy_version,
                  trade_direction,
                  start_time, end_time,
                  net_profit, net_return_pct, max_drawdown_pct,
                  sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
                  sweep_id
                FROM backtest_runs
                WHERE symbol = ? AND timeframe = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                    params=(symbol, timeframe, 5000),
                    table="backtest_runs",
                )
            )

        if sweep_id:
            results.append(
                _check_plan(
                    conn=conn,
                    title="Runs for sweep (sweep_id, created_at)",
                    sql="""
                SELECT
                  id, created_at, symbol, timeframe, strategy_name, strategy_version,
                  trade_direction,
                  start_time, end_time,
                  net_profit, net_return_pct, max_drawdown_pct,
                  sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
                  sweep_id
                FROM backtest_runs
                WHERE sweep_id = ?
                ORDER BY created_at ASC
                """,
                    params=(sweep_id,),
                    forbid_regex=(r"USE TEMP B-TREE\s+FOR\s+ORDER BY",),
                    require_any_regex=(r"USING\s+INDEX\s+IDX_BACKTEST_RUNS_SWEEP_CREATED_AT",),
                )
            )

        results.append(
            _check_no_full_scan_temp_sort(
                conn=conn,
                title="Sweeps list (created_at desc)",
                sql="""
            SELECT id, created_at, name, strategy_name, strategy_version, data_source,
                   exchange_id, symbol, timeframe, range_mode,
                   range_params_json, base_config_json, sweep_definition_json,
                   status, error_message
            FROM sweeps
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
                params=(100, 0),
                table="sweeps",
            )
        )

        run_id_for_detail = _first_value(conn, "SELECT id FROM backtest_runs ORDER BY created_at DESC LIMIT 1") or ""
        results.append(
            _check_plan(
                conn=conn,
                title="Run details join (by id)",
                sql="""
            SELECT r.id, r.created_at, r.symbol, r.timeframe, r.strategy_name, r.strategy_version,
                   r.trade_direction,
                   r.start_time, r.end_time,
                   r.net_profit, r.net_return_pct, r.max_drawdown_pct,
                   r.sharpe, r.sortino, r.win_rate, r.profit_factor, r.cpc_index, r.common_sense_ratio,
                   r.sweep_id,
                   d.config_json, d.metrics_json, d.trades_json, d.metadata_json
            FROM backtest_runs r
            LEFT JOIN backtest_run_details d ON d.run_id = r.id
            WHERE r.id = ?
            """,
                params=(run_id_for_detail,),
                # Must not devolve into a full scan of backtest_runs for a single-id lookup.
                forbid_regex=(r"SCAN\s+.*BACKTEST_RUNS\b",),
                require_any_regex=(r"SEARCH\s+.*BACKTEST_RUNS\b", r"USING\s+INDEX\s+SQLITE_AUTOINDEX_BACKTEST_RUNS_1"),
            )
        )

    if args.json:
        payload = {
            "ok": all(bool(r.get("ok")) for r in results),
            "checks": results,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        # Human-readable output
        for r in results:
            print("\n==", r["title"])
            print("SQL:", r["sql"])
            if r.get("params"):
                print("params:", tuple(r["params"]))
            for line in r.get("plan") or []:
                print(" ", line)
            if r.get("ok"):
                print(" OK")
            else:
                hits = r.get("forbidden_hits") or []
                if hits:
                    print(" ! FAIL: forbidden patterns matched:", hits)
                else:
                    print(" ! FAIL: required index usage not detected")

    if args.fail and not all(bool(r.get("ok")) for r in results):
        return 2

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
