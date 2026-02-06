from __future__ import annotations

import project_dragon.streamlit_app as ui


def test_ui_page_dependencies_are_defined() -> None:
    required_callables = [
        "list_backtest_run_symbols",
        "list_backtest_run_timeframes",
        "list_backtest_run_strategies_filtered",
        "get_runs_numeric_bounds",
        "get_account_snapshots",
        "list_credentials",
        "list_saved_periods",
        "execute_fetchall",
        "list_cached_coverages",
        "get_cached_coverage",
        "purge_old_candles",
        "get_user_setting",
        "set_user_setting",
        "get_database_url",
        "get_current_user_email",
        "get_or_create_user_id",
        "list_bots",
    ]
    for name in required_callables:
        assert hasattr(ui, name), f"Missing {name} in streamlit_app"
        assert callable(getattr(ui, name)), f"{name} should be callable"


def test_ui_grid_dependencies_are_defined() -> None:
    # These are optional at runtime but must be defined to avoid NameError.
    for name in ("AgGrid", "GridOptionsBuilder", "GridUpdateMode", "DataReturnMode", "JsCode"):
        assert hasattr(ui, name), f"Missing {name} in streamlit_app"
