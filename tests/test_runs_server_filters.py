from __future__ import annotations

from datetime import datetime, timezone

from project_dragon.storage import (
    _build_runs_where,
    _runs_allowed_filter_map,
    get_table_columns,
    init_db,
    list_runs_server_side,
    open_db_connection,
    save_backtest_run,
)


def test_list_runs_server_side_config_keys() -> None:
    init_db()
    run_id = "run_cfg_keys_1"
    save_backtest_run(
        config={"trend": {"ma_len": 21}, "exits": {"tp_atr_multiple": 2.5}},
        metrics={"net_profit": 1.0},
        metadata={
            "id": run_id,
            "symbol": "BTC/USDT",
            "timeframe": "1h",
            "strategy_name": "Test",
            "strategy_version": "0",
        },
        sweep_id=None,
        result=None,
    )

    with open_db_connection() as conn:
        rows, total = list_runs_server_side(
            conn,
            page=1,
            page_size=50,
            config_keys=["trend.ma_len", "exits.tp_atr_multiple"],
        )

    assert total == 1
    assert len(rows) == 1
    row = rows[0]
    assert row.get("run_id") == run_id
    assert row.get("cfg__trend.ma_len") == "21"
    assert row.get("cfg__exits.tp_atr_multiple") == "2.5"


def test_build_runs_where_duration_roi_run_type() -> None:
    init_db()
    start_dt = datetime(2024, 2, 1, tzinfo=timezone.utc).isoformat()
    end_dt = datetime(2024, 2, 29, 23, 59, 59, tzinfo=timezone.utc).isoformat()
    extra = {
        "created_at_from": start_dt,
        "created_at_to": end_dt,
        "roi_pct_on_margin_min": 10.0,
        "run_type": "sweep",
    }

    with open_db_connection() as conn:
        run_cols = get_table_columns(conn, "backtest_runs")
    allowed_filters = _runs_allowed_filter_map(run_cols)

    where_clause, params = _build_runs_where(
        filter_model=None,
        allowed_filters=allowed_filters,
        extra_where=extra,
        user_id=None,
        run_cols=run_cols,
    )

    assert "r.created_at >= %s" in where_clause
    assert "r.created_at <= %s" in where_clause
    assert "r.roi_pct_on_margin >= %s" in where_clause
    assert "r.sweep_id IS NOT NULL" in where_clause
    assert start_dt in params
    assert end_dt in params
    assert 10.0 in params
