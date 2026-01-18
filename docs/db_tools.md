# Database tools (Postgres)

Project Dragon uses Postgres via DRAGON_DATABASE_URL. The schema is normalized into summary and detail tables:

- `backtest_runs`: summary columns (index-friendly)
- `backtest_run_details`: larger JSON blobs (charts, trades, config, metrics)

## Migrations

Migrations are applied automatically on startup by the UI and workers. For manual runs:

```
PYTHONPATH=src python -c "from project_dragon.storage import open_db_connection, apply_migrations;\nconn = open_db_connection();\napply_migrations(conn);\nconn.commit()"
```

## Bench list/detail performance

```
PYTHONPATH=src python examples/bench_runs_list.py --n 10000 --base-iso 2020-01-01T00:00:00+00:00
```

