"""Deterministic benchmark for backtest run list/detail performance.

Creates N synthetic runs with realistic JSON blob sizes, then measures:
- Summary list query (no blob parsing)
- Single-run details fetch

Usage:
  PYTHONPATH=src python examples/bench_runs_list.py --reset --n 10000

By default writes to: data/bench_runs_list.sqlite
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from project_dragon.storage import (
    apply_migrations,
    open_db_connection,
    load_backtest_run_summaries,
    get_backtest_run_details,
)


def _make_json_blob(*, kb: int, seed_text: str) -> str:
    # Produce valid JSON of roughly kb kilobytes.
    target = max(0, int(kb)) * 1024
    pad = (seed_text + "-") * max(1, (target // (len(seed_text) + 1) + 1))
    pad = pad[:target]
    return json.dumps({"blob": pad, "kb": kb})


def _populate(
    conn,
    *,
    n: int,
    config_kb: int,
    metrics_kb: int,
    trades_kb: int,
    base_dt: datetime,
) -> None:
    base_dt = base_dt.astimezone(timezone.utc)
    base = base_dt - timedelta(seconds=n)

    config_json = _make_json_blob(kb=config_kb, seed_text="cfg")
    metrics_json = _make_json_blob(kb=metrics_kb, seed_text="m")
    trades_json = _make_json_blob(kb=trades_kb, seed_text="t")
    metadata_json = json.dumps({"bench": True})

    rows_runs: list[tuple[Any, ...]] = []
    rows_details: list[tuple[Any, ...]] = []

    for i in range(int(n)):
        run_id = f"bench-{i:06d}"
        created_at = (base + timedelta(seconds=i)).isoformat()
        symbol = "BTC/USDT:USDT" if i % 2 == 0 else "ETH/USDT:USDT"
        timeframe = "1h" if i % 3 == 0 else "15m"
        net_return_pct = (i % 200) / 10.0 - 5.0

        rows_runs.append(
            (
                run_id,
                created_at,
                symbol,
                timeframe,
                "DragonDcaAtr",
                "bench",
                config_json,
                metrics_json,
                metadata_json,
                None,
                None,
                None,
                float(net_return_pct),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                trades_json,
                None,
            )
        )
        rows_details.append(
            (
                run_id,
                config_json,
                metrics_json,
                metadata_json,
                trades_json,
                created_at,
                created_at,
            )
        )

    conn.execute("BEGIN")
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO backtest_runs (
                id, created_at, symbol, timeframe, strategy_name, strategy_version,
                config_json, metrics_json, metadata_json,
                start_time, end_time, net_profit, net_return_pct, max_drawdown_pct,
                sharpe, sortino, win_rate, profit_factor, cpc_index, common_sense_ratio,
                trades_json, sweep_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows_runs,
        )
        conn.executemany(
            """
            INSERT OR REPLACE INTO backtest_run_details(
                run_id, config_json, metrics_json, metadata_json, trades_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows_details,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _timeit(fn, *, iters: int = 1) -> float:
    start = time.perf_counter()
    for _ in range(int(iters)):
        fn()
    return (time.perf_counter() - start) * 1000.0 / max(1, int(iters))


def main() -> int:
    p = argparse.ArgumentParser(description="Benchmark run list/detail performance")
    p.add_argument("--db", default=str(ROOT / "data" / "bench_runs_list.sqlite"))
    p.add_argument("--reset", action="store_true", help="Delete the DB file before running")
    p.add_argument("--n", type=int, default=10_000)
    p.add_argument("--config-kb", type=int, default=10)
    p.add_argument("--metrics-kb", type=int, default=5)
    p.add_argument("--trades-kb", type=int, default=50)
    p.add_argument(
        "--base-iso",
        type=str,
        default=None,
        help="Optional fixed ISO8601 timestamp to make the synthetic DB deterministic (e.g. 2020-01-01T00:00:00+00:00)",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    if args.reset and db_path.exists():
        db_path.unlink()

    # Open via factory (applies pragmas) and ensure migrations.
    with open_db_connection(db_path) as conn:
        apply_migrations(conn)

        runs_count = conn.execute("SELECT COUNT(1) FROM backtest_runs").fetchone()[0]
        if int(runs_count or 0) < int(args.n):
            print(f"Populating {args.n} runs into {db_path} …")
            base_dt = datetime.now(timezone.utc)
            if args.base_iso:
                base_dt = datetime.fromisoformat(str(args.base_iso))
                if base_dt.tzinfo is None:
                    base_dt = base_dt.replace(tzinfo=timezone.utc)
            _populate(
                conn,
                n=args.n,
                config_kb=args.config_kb,
                metrics_kb=args.metrics_kb,
                trades_kb=args.trades_kb,
                base_dt=base_dt,
            )

        # Pick a stable run id to fetch details for.
        target_id = "bench-000000"

    # Benchmark using the storage APIs (new connections each call)
    avg_ms_summary = _timeit(lambda: load_backtest_run_summaries(limit=200), iters=5)
    avg_ms_details = _timeit(lambda: get_backtest_run_details(target_id), iters=20)

    size_bytes = db_path.stat().st_size if db_path.exists() else 0

    print("\nBenchmark results")
    print(f"- db: {db_path} ({size_bytes/1024/1024:.1f} MiB)")
    print(f"- runs: {args.n}")
    print(f"- summary list (limit=200): {avg_ms_summary:.2f} ms avg")
    print(f"- details fetch (run_id={target_id}): {avg_ms_details:.2f} ms avg")

    print("\nTip: set DRAGON_PROFILE=1 DRAGON_SLOW_MS=1 to log slow spans.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
