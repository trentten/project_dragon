# DB tools

This project is SQLite-first and performance-sensitive. To keep list views fast and migrations safe, the codebase supports a summary/details split for backtest runs:

- `backtest_runs`: summary columns (small, index-friendly)
- `backtest_run_details`: large blobs (config/metrics/trades/metadata)

The `backtest_run_details` table is created via automatic schema migrations.

## Utilities

### 0) SQLite PRAGMAs check

File: [examples/sqlite_pragmas_check.py](../examples/sqlite_pragmas_check.py)

Purpose:
- Verify key per-connection PRAGMAs are applied (`journal_mode=WAL`, `busy_timeout`, `foreign_keys=ON`).

Usage:
- `python examples/sqlite_pragmas_check.py`

### 1) Legacy blob inspector

File: [examples/legacy_blob_inspector.py](../examples/legacy_blob_inspector.py)

Purpose:
- Audit the migration state (are all `backtest_runs.id` present in `backtest_run_details.run_id`?)
- Backfill missing `backtest_run_details` rows from legacy blob columns (older DBs)
- Verify that details rows are complete

Commands:

- Report overall status:
  - `python examples/legacy_blob_inspector.py report`

- List recent runs missing details rows:
  - `python examples/legacy_blob_inspector.py list-missing --limit 50`

- Backfill missing details rows (from legacy columns):
  - `python examples/legacy_blob_inspector.py backfill --batch 500`
  - Dry run: `python examples/legacy_blob_inspector.py backfill --batch 500 --dry-run`

- Verify all runs have a details row:
  - `python examples/legacy_blob_inspector.py verify`

Optional DB selection:
- `python examples/legacy_blob_inspector.py report --db data/backtests.sqlite`

### 2) Query plan checker

File: [examples/db_query_plan_check.py](../examples/db_query_plan_check.py)

Purpose:
- Run `EXPLAIN QUERY PLAN` for the primary list/detail queries
- Confirm indexes are being used (especially for `ORDER BY created_at`)

Usage:
- `python examples/db_query_plan_check.py`
- Optional DB selection: `python examples/db_query_plan_check.py --db data/backtests.sqlite`
- Optional filters: `python examples/db_query_plan_check.py --symbol BTC/USDT:USDT --timeframe 1h`

CI-friendly:
- JSON output: `python examples/db_query_plan_check.py --json`
- Fail on regression: `python examples/db_query_plan_check.py --json --fail`

## Project rule: legacy run blobs

Legacy blob columns (historical) in `backtest_runs`:
- `config_json`
- `metrics_json`
- `trades_json`
- `metadata_json`

Rule:
- Only [examples/legacy_blob_inspector.py](../examples/legacy_blob_inspector.py) may read these legacy blob columns *directly* from `backtest_runs`.
- All other code must:
  - Use `load_backtest_run_summaries()` for list views (summary-only; no blob parsing)
  - Use `get_backtest_run_details(run_id)` to load config/metrics/trades/metadata

Rationale:
- Keeps list views fast (avoids loading/parsing huge JSON per rerun)
- Makes the DB schema evolvable without table rebuilds

## Troubleshooting

### Details rows are missing

Symptoms:
- You see missing `backtest_run_details` rows (typically on older DBs)
- UI detail views may show empty config/metrics (depending on fallback behavior)

Fix:
1) Backfill:
- `python examples/legacy_blob_inspector.py backfill --batch 500`

2) Verify:
- `python examples/legacy_blob_inspector.py verify`

If backfill reports “skipped placeholder-only legacy blobs”, those runs likely already had their legacy columns replaced with placeholders; in that case the original blobs are not recoverable from `backtest_runs`.

## Schema migrations

The DB schema is managed by a simple migrations table:
- `schema_migrations(version, name, applied_at)`

On startup, `open_db_connection()` / `init_db()` will apply any pending migrations.

To add a migration:
1) Add a new `_migration_XXXX_*` function in [src/project_dragon/storage.py](../src/project_dragon/storage.py)
2) Append it to `_MIGRATIONS` with the next integer version
3) Keep migrations idempotent (CREATE IF NOT EXISTS + safe ALTERs)

## Profiling / slow spans

Storage and worker hot paths can emit slow-span logs when enabled:
- Enable: `DRAGON_PROFILE=1`
- Threshold: `DRAGON_SLOW_MS=100` (default 100ms)

Logs go to the `project_dragon.profile` logger and never include large JSON payloads.

## "database is locked" playbook

Project Dragon is SQLite + WAL-first. In normal usage, we rely on:

- Per-connection PRAGMAs: `journal_mode=WAL`, `busy_timeout=5000`, `foreign_keys=ON` (and `synchronous=NORMAL`).
- Short, bounded transactions (avoid holding write transactions over network calls).

If you see intermittent `sqlite3.OperationalError: database is locked` under load:

1) Confirm PRAGMAs are active: `python examples/sqlite_pragmas_check.py`.
2) Check for long-running transactions (a write transaction held open across network calls).
3) Reduce write frequency / batch updates (e.g., heartbeat cadence, bot_events writes).
4) As a last resort, add small targeted retry + jitter around *specific* write operations.

We intentionally do **not** add global worker retry/jitter until there is concrete evidence of lock contention in the target deployment.

## Benchmark

File: [examples/bench_runs_list.py](../examples/bench_runs_list.py)

Purpose:
- Populate a deterministic synthetic DB (N runs + realistic blob sizes)
- Measure summary listing vs details fetch

Usage:
- `PYTHONPATH=src python examples/bench_runs_list.py --reset --n 10000`

For fully deterministic data (recommended for CI), pin the base timestamp:

- `PYTHONPATH=src python examples/bench_runs_list.py --db data/bench_runs_list.sqlite --reset --n 10000 --base-iso 2020-01-01T00:00:00+00:00`

## CI example (GitHub Actions)

This is a snippet only (not wired into this repo). You would paste this into a workflow file like `.github/workflows/ci.yml` under a job’s `steps:`.

It:
- sets up Python
- generates a deterministic synthetic SQLite DB (via the existing benchmark/populator)
- runs the query-plan regression guard in machine-readable mode
- prints the JSON output on failure for debugging

```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: '3.11'

- name: Install deps
  run: |
    python -m pip install --upgrade pip
    pip install -r requirements.txt

- name: Generate deterministic synthetic DB
  run: |
    PYTHONPATH=src python examples/bench_runs_list.py \
      --db data/ci_bench.sqlite \
      --reset \
      --n 10000 \
      --base-iso 2020-01-01T00:00:00+00:00

- name: Query plan regression guard
  shell: bash
  run: |
    set -euo pipefail
    out="$(mktemp)"
    if ! PYTHONPATH=src python examples/db_query_plan_check.py --db data/ci_bench.sqlite --json --fail > "$out"; then
      echo "db_query_plan_check failed; JSON output:" >&2
      cat "$out" >&2
      exit 1
    fi
```
