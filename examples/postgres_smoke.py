import json
import os
import sys
from uuid import uuid4

from project_dragon.storage import (
    apply_migrations,
    build_backtest_run_rows,
    claim_job_with_lease,
    create_job,
    get_or_create_user,
    is_postgres_dsn,
    now_utc_iso,
    open_db_connection,
    sql_placeholders,
    upsert_backtest_run_details,
)


def main() -> int:
    dsn = (os.getenv("DRAGON_DATABASE_URL") or "").strip()
    if not is_postgres_dsn(dsn):
        print("DRAGON_DATABASE_URL must be set to a postgres:// or postgresql:// URL")
        return 1

    open_db = open_db_connection

    with open_db() as conn:
        apply_migrations(conn)
        user_id = get_or_create_user(conn, "postgres-smoke@example.com")

        job_payload = {
            "action": "backtest_run",
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy": "DragonDcaAtr",
        }
        job_id = create_job(conn, "backtest_run", job_payload)
        claimed = claim_job_with_lease(conn, worker_id="postgres-smoke", lease_s=30.0, job_types=("backtest_run",))
        claimed_id = int(claimed["id"]) if claimed else job_id

        now = now_utc_iso()
        update_job_fields = {"status": "running", "started_at": now, "message": "running"}
        from project_dragon.storage import update_job

        update_job(conn, claimed_id, **update_job_fields)
        update_job(
            conn,
            claimed_id,
            status="finished",
            finished_at=now_utc_iso(),
            progress=1.0,
            message="finished",
        )

        run_id = str(uuid4())
        metadata = {
            "id": run_id,
            "created_at": now_utc_iso(),
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "DragonDcaAtr",
            "strategy_version": "0.0.1",
            "market_type": "perps",
            "user_id": user_id,
        }
        config = {
            "data_settings": {
                "exchange_id": "woox",
                "symbol": "PERP_BTC_USDT",
                "timeframe": "1h",
                "data_source": "ccxt",
                "market_type": "perps",
                "range_mode": "recent",
                "range_params": {"lookback_bars": 100},
            }
        }
        metrics = {"net_profit": 0.0}

        run_row_fields, details_fields = build_backtest_run_rows(
            config,
            metrics,
            metadata,
            sweep_id=None,
            result=None,
        )
        created_at = str(run_row_fields.get("created_at") or now_utc_iso())
        payload = dict(run_row_fields)
        payload["id"] = run_id
        payload["created_at"] = created_at
        payload["metadata_json"] = json.dumps(run_row_fields.get("metadata_json") or {})

        cols = [
            "id",
            "created_at",
            "symbol",
            "timeframe",
            "strategy_name",
            "strategy_version",
            "config_json",
            "metrics_json",
            "metadata_json",
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
            "sweep_id",
        ]
        values = [payload.get(c) for c in cols]
        conn.execute(
            f"INSERT INTO backtest_runs ({', '.join(cols)}) VALUES ({sql_placeholders(conn, len(cols))})",
            tuple(values),
        )

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
                json.dumps(details_fields.get("metadata_json") or {}),
                details_fields.get("trades_json"),
                details_fields.get("equity_curve_json"),
                details_fields.get("equity_timestamps_json"),
                details_fields.get("extra_series_json"),
                details_fields.get("candles_json"),
                details_fields.get("params_json"),
                details_fields.get("run_context_json"),
                details_fields.get("computed_metrics_json"),
                created_at,
                now_utc_iso(),
            ),
        )
        conn.commit()

    print(f"Inserted run_id={run_id} job_id={claimed_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
