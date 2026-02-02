from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import html
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import random
import re
import threading
import time
import traceback
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, MutableMapping

import pandas as pd

import plotly.graph_objects as go
import logging
import streamlit as st

try:
    from st_aggrid import AgGrid, DataReturnMode, GridOptionsBuilder, GridUpdateMode, JsCode  # type: ignore
except Exception:  # pragma: no cover
    AgGrid = None  # type: ignore
    DataReturnMode = None  # type: ignore
    GridOptionsBuilder = None  # type: ignore
    GridUpdateMode = None  # type: ignore
    JsCode = None  # type: ignore

try:
    from project_dragon._version import __version__  # type: ignore
except Exception:  # pragma: no cover
    __version__ = "0"  # type: ignore


# Friendly column headings used across multiple AGGrid tables.
# Keep this lightweight: only override where a generic snake_case title
# isn't good enough.
COL_LABELS: dict[str, str] = {
    "id": "ID",
    "run_id": "Run ID",
    "bot_id": "Bot ID",
    "created_at": "Run Date",
    "start_date": "Start",
    "end_date": "End",
    "duration": "Duration",
    "symbol": "Asset",
    "symbol_full": "Symbol (raw)",
    "icon_uri": "Icon URI",
    "market": "Market",
    "category": "Category",
    "timeframe": "Timeframe",
    "direction": "Direction",
    "strategy": "Strategy",
    "sweep_name": "Sweep",
    "start_time": "Start Time",
    "end_time": "End Time",
    "net_return_pct": "Net Return",
    "roi_pct_on_margin": "ROI",
    "max_drawdown_pct": "Max Drawdown",
    "win_rate": "Win Rate",
    "net_profit": "Net Profit",
    "sharpe": "Sharpe",
    "sortino": "Sortino",
    "profit_factor": "Profit Factor",
    "cpc_index": "CPC Index",
    "common_sense_ratio": "Common Sense Ratio",
    "avg_position_time": "Avg Position Time",
    "avg_position_time_seconds": "Avg Position Time (s)",
    "shortlist": "Shortlist",
    "shortlist_note": "Shortlist Note",
    "actions": "Actions",
}


_ACRONYMS: dict[str, str] = {
    "api": "API",
    "atr": "ATR",
    "bb": "BB",
    "bbo": "BBO",
    "cpc": "CPC",
    "dca": "DCA",
    "ema": "EMA",
    "id": "ID",
    "ma": "MA",
    "macd": "MACD",
    "pnl": "PnL",
    "roi": "ROI",
    "rsi": "RSI",
    "sma": "SMA",
    "tp": "TP",
    "sl": "SL",
    "usd": "USD",
    "usdt": "USDT",
}


def snake_to_title(name: Any) -> str:
    s = str(name)
    if not s:
        return ""

    parts = [p for p in s.replace("-", "_").split("_") if p]
    out: list[str] = []
    for p in parts:
        key = p.lower()
        if key == "pct":
            out.append("%")
        elif key in _ACRONYMS:
            out.append(_ACRONYMS[key])
        else:
            out.append(p[:1].upper() + p[1:])
    return " ".join(out)


def col_label(field: Any) -> str:
    s = str(field)
    return COL_LABELS.get(s, snake_to_title(s))


from project_dragon.runs_explorer import (
    STRATEGY_CONFIG_KEYS,
    compute_config_diff_flags,
    flatten_config,
    top_config_keys_by_frequency,
)
from project_dragon.ui.icons import ICONS as TABLER_ICONS
from project_dragon.ui.icons import (
    get_tabler_svg,
    render_tabler_icon,
    st_tabler_icon,
    tabler_svg_to_data_uri,
    tabler_assets_available,
    tabler_icon_inventory,
)
from project_dragon.ui.ui_filters import render_table_filters
from project_dragon.ui.components import (
    aggrid_pill_style,
    inject_trade_stream_css,
    render_active_filter_chips,
    render_card,
    render_kpi_tile,
)
from project_dragon.ui.asset_icons import asset_market_getter_js, asset_renderer_js
from project_dragon.ui.grid_columns import (
    apply_columns,
    col_avg_position_time,
    col_date_ddmmyy,
    col_direction_pill,
    col_shortlist,
    col_shortlist_note,
    col_timeframe_pill,
)
from project_dragon.ui_formatters import format_duration_dhm
from project_dragon.ui.global_filters import apply_global_filters, ensure_global_filters_initialized, get_global_filters, set_global_filters
from project_dragon.storage import (
    count_backtest_runs_missing_details,
    count_bots_for_account,
    create_job,
    create_sweep,
    SweepMeta,
    update_run_shortlist_fields,
    update_job,
    execute_fetchall,
    get_table_columns,
    get_backtest_run_chart_artifacts,
    get_backtest_run,
    get_backtest_run_job_counts_by_parent,
    get_backtest_run_job_counts_by_sweep,
    get_app_settings,
    get_cached_coverage,
    get_current_user_email,
    get_database_url,
    get_account_snapshots,
    get_or_create_user_id,
    get_user_setting,
    init_db,
    is_postgres_dsn,
    list_backtest_run_symbols,
    list_backtest_run_timeframes,
    list_backtest_run_strategies_filtered,
    list_runs_server_side,
    load_backtest_runs_explorer_rows,
    get_runs_numeric_bounds,
    list_saved_periods,
    list_credentials,
    list_cached_coverages,
    list_bots,
    get_sweep_parent_jobs_by_sweep,
    open_db_connection,
    purge_old_candles,
    sample_backtest_runs_missing_details,
    set_setting,
    set_user_setting,
    create_saved_period,
    update_saved_period,
    delete_saved_period,
    upsert_asset,
    seed_asset_categories_if_empty,
    upsert_assets_bulk,
    create_category,
    set_assets_status,
    sync_symbol_icons_from_spothq_pack,
)
from project_dragon.config_dragon import (
    DcaSettings,
    DynamicActivationConfig,
    DragonAlgoConfig,
    BBandsConfig,
    ExitSettings,
    GeneralSettings,
    InitialEntrySizingMode,
    MACDConfig,
    OrderStyle,
    RSIConfig,
    StopLossMode,
    TakeProfitMode,
    TrendFilterConfig,
)
from project_dragon.domain import BacktestResult, Candle, Trade
from project_dragon.data_online import get_candles_with_cache, get_woox_market_catalog
from project_dragon.strategy_dragon import DragonDcaAtrStrategy

from project_dragon.time_utils import (
    DEFAULT_DISPLAY_TZ_NAME,
    fmt_date,
    fmt_dt,
    fmt_dt_short,
    format_duration,
    get_display_tz_name,
    to_local_display,
)

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _ui_display_tz():
    """Global UI display timezone (presentation-only).

    Stored per-user via user_settings key 'display_timezone'.
    Falls back to DEFAULT_DISPLAY_TZ_NAME.
    """

    name = str(st.session_state.get("display_timezone_name") or "").strip() or DEFAULT_DISPLAY_TZ_NAME
    if ZoneInfo is None:  # pragma: no cover
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        try:
            return ZoneInfo(DEFAULT_DISPLAY_TZ_NAME)
        except Exception:
            return timezone.utc


def _ui_local_date_iso(value: Any, tz: Any) -> Optional[str]:
    """Convert a timestamp to a local ISO date (YYYY-MM-DD) for UI display."""

    try:
        dt_local = to_local_display(value, tz)
        return dt_local.date().isoformat() if dt_local is not None else None
    except Exception:
        return None


def _ui_local_date_dmy(value: Any, tz: Any) -> Optional[str]:
    """Convert a timestamp to a local date string (DD/MM/YY) for UI display."""

    try:
        dt_local = to_local_display(value, tz)
        if dt_local is None:
            return None
        return dt_local.strftime("%d/%m/%y")
    except Exception:
        return None


def _ui_local_dt_ampm(value: Any, tz: Any) -> Optional[str]:
    """Run-date display format used across run grids: DD/MM hh:mm AM/PM (local tz)."""

    try:
        dt_local = to_local_display(value, tz)
        if dt_local is None:
            return None
        return dt_local.strftime("%d/%m %I:%M %p")
    except Exception:
        return None


def _build_live_bots_overview_df(bots: List[Dict[str, Any]], user_id: str | int) -> pd.DataFrame:
    """Build the Live Bots overview dataframe (shared by Live Bots page + Dashboard)."""

    # Tabler health icons as inline HTML (colored) for AGGrid.
    try:
        _health_ok_html = render_tabler_icon(TABLER_ICONS["ok"], size_px=16, color="#7ee787", variant="filled")
        _health_warn_html = render_tabler_icon(TABLER_ICONS["warn"], size_px=16, color="#facc15", variant="filled")
        _health_err_html = render_tabler_icon(TABLER_ICONS["error"], size_px=16, color="#ff7b72", variant="filled")
    except Exception:
        _health_ok_html = "OK"
        _health_warn_html = "WARN"
        _health_err_html = "ERR"

    icon_map: Dict[str, str] = {}
    with _job_conn() as conn:
        app_settings = get_app_settings(conn)
        # Best-effort icon map (matches Results grid behavior).
        try:
            symbols = sorted({str(b.get("symbol") or "").strip() for b in (bots or []) if str(b.get("symbol") or "").strip()})
            if symbols:
                placeholders = ",".join(["?"] * len(symbols))
                cur = conn.execute(
                    f"SELECT exchange_symbol, icon_uri FROM symbols WHERE exchange_symbol IN ({placeholders})",
                    tuple(symbols),
                )
                for exchange_symbol, icon_uri in cur.fetchall() or []:
                    k = str(exchange_symbol or "").strip()
                    if k:
                        icon_map[k] = str(icon_uri or "").strip()
        except Exception:
            icon_map = {}
    global_kill = bool((app_settings or {}).get("live_kill_switch_enabled", False))
    try:
        fast_interval = float(
            os.environ.get("DRAGON_WORKER_FAST_INTERVAL_S")
            or (app_settings or {}).get("worker_fast_interval_s")
            or 3.0
        )
    except Exception:
        fast_interval = 3.0
    stale_threshold_s = max(2.0 * float(fast_interval) + 2.0, 6.0)
    now = datetime.now(timezone.utc)

    rows: List[Dict[str, Any]] = []
    for b in bots or []:
        bot_id_val = b.get("id")
        if bot_id_val is None:
            continue
        try:
            bot_id_int = int(bot_id_val)
        except (TypeError, ValueError):
            continue

        name = b.get("name") or f"Bot {bot_id_int}"
        exchange = b.get("exchange_id") or ""
        symbol = b.get("symbol") or ""
        icon_uri = str(b.get("icon_uri") or "").strip() or icon_map.get(str(symbol).strip(), "")

        def _base_from_symbol(sym: str) -> str:
            s = str(sym or "").strip()
            if not s:
                return ""
            if "/" in s:
                return s.split("/", 1)[0].strip().upper()
            up = s.upper()
            if up.startswith("PERP_") or up.startswith("SPOT_"):
                parts = [p for p in up.split("_") if p]
                return parts[1].strip().upper() if len(parts) >= 2 else up
            if "-" in s:
                return s.split("-", 1)[0].strip().upper()
            return up

        base_asset = str(b.get("base_asset") or "").strip().upper() or _base_from_symbol(str(symbol))
        tf = b.get("timeframe") or ""
        status = b.get("status") or ""
        desired = b.get("desired_status") or ""
        realized = _safe_float(b.get("realized_pnl"))
        unrealized = _safe_float(b.get("unrealized_pnl"))
        total_pnl = (realized or 0.0) + (unrealized or 0.0)

        runtime_health: Dict[str, Any] = {}
        bot_kill = False
        raw_config = b.get("config_json")
        if isinstance(raw_config, str) and raw_config:
            try:
                cfg = json.loads(raw_config)
                bot_risk_cfg = (cfg or {}).get("_risk") if isinstance(cfg, dict) else None
                if isinstance(bot_risk_cfg, dict):
                    bot_kill = bool(bot_risk_cfg.get("kill_switch", False))
                rt = (cfg or {}).get("_runtime") if isinstance(cfg, dict) else None
                if isinstance(rt, dict):
                    runtime_health = (rt.get("health") if isinstance(rt.get("health"), dict) else {}) or {}
            except Exception:
                pass

        # Position summary (best-effort)
        long_qty = 0.0
        short_qty = 0.0
        try:
            pos_raw = b.get("position_json")
            pos_obj = json.loads(pos_raw) if isinstance(pos_raw, str) and pos_raw else (pos_raw or {})
            if isinstance(pos_obj, dict):
                long_leg = pos_obj.get("LONG") or {}
                short_leg = pos_obj.get("SHORT") or {}
                if isinstance(long_leg, dict):
                    long_qty = float(long_leg.get("size") or 0.0)
                if isinstance(short_leg, dict):
                    short_qty = float(short_leg.get("size") or 0.0)
        except Exception:
            long_qty = 0.0
            short_qty = 0.0
        _ = "FLAT" if abs(long_qty) < 1e-12 and abs(short_qty) < 1e-12 else f"L {long_qty:.4f} / S {short_qty:.4f}"

        # Heartbeat staleness
        hb_at = b.get("heartbeat_at")
        snap_at = b.get("snapshot_updated_at")
        hb_stale = False
        try:
            ref_ts = hb_at or snap_at
            if ref_ts:
                hb_dt = datetime.fromisoformat(str(ref_ts))
                if hb_dt.tzinfo is None:
                    hb_dt = hb_dt.replace(tzinfo=timezone.utc)
                hb_stale = (now - hb_dt).total_seconds() > stale_threshold_s
            else:
                hb_stale = True
        except Exception:
            hb_stale = True

        acct_risk = runtime_health.get("account_risk") if isinstance(runtime_health, dict) else None
        acct_blocked = bool(acct_risk.get("blocked")) if isinstance(acct_risk, dict) else False
        acct_reason = (acct_risk.get("reason") if isinstance(acct_risk, dict) else None) or None
        if b.get("snapshot_risk_blocked") is not None:
            try:
                acct_blocked = bool(int(b.get("snapshot_risk_blocked") or 0))
                acct_reason = (b.get("snapshot_risk_reason") or acct_reason) or None
            except Exception:
                pass

        next_action = b.get("snapshot_next_action")
        if not next_action:
            if isinstance(runtime_health, dict):
                next_action = runtime_health.get("next_action")
        if not next_action:
            next_action = b.get("heartbeat_msg") or "Waiting"
        if acct_blocked:
            next_action = "Risk blocked" + (f" ({acct_reason})" if acct_reason else "")
        elif global_kill or bot_kill:
            next_action = "Kill switch"

        last_error = (b.get("snapshot_last_error") or b.get("last_error") or "").strip()
        status_l = str(status or "").strip().lower()
        health_state = "ok"
        health_html = _health_ok_html
        snap_health = (str(b.get("snapshot_health_status") or "")).strip().lower()
        if snap_health == "error" or status_l == "error" or last_error:
            health_state = "error"
            health_html = _health_err_html
        elif snap_health == "warn" or hb_stale or acct_blocked or global_kill or bot_kill:
            health_state = "warn"
            health_html = _health_warn_html

        exchange_state = None
        try:
            if isinstance(runtime_health, dict):
                exchange_state = runtime_health.get("exchange_state")
        except Exception:
            exchange_state = None
        exchange_state_s = (str(exchange_state or "")).strip().upper() or None

        flags: List[str] = []
        if acct_blocked:
            flags.append("RISK")
        if exchange_state_s and exchange_state_s != "CLOSED":
            flags.append(exchange_state_s)
        elif bool(isinstance(runtime_health, dict) and runtime_health.get("exchange_degraded")):
            flags.append("DEG")
        hedge = runtime_health.get("hedge_mode") if isinstance(runtime_health, dict) else None
        if isinstance(hedge, dict) and hedge.get("ok") is False:
            flags.append("HEDGE")
        if global_kill or bot_kill:
            flags.append("KILL")
        if hb_stale:
            flags.append("STALE")
        flags_disp = " ".join(flags) if flags else ""
        flags_tip_parts: List[str] = []
        if acct_blocked:
            flags_tip_parts.append("Account risk blocked" + (f": {acct_reason}" if acct_reason else ""))
        if exchange_state_s:
            flags_tip_parts.append(f"Exchange state: {exchange_state_s}")
        elif bool(isinstance(runtime_health, dict) and runtime_health.get("exchange_degraded")):
            flags_tip_parts.append("Exchange degraded")
        if isinstance(hedge, dict) and hedge.get("ok") is False:
            flags_tip_parts.append("Hedge mode check failed" + (f": {hedge.get('reason')}" if hedge.get("reason") else ""))
        if global_kill:
            flags_tip_parts.append("Global kill switch")
        if bot_kill:
            flags_tip_parts.append("Bot kill switch")
        if hb_stale:
            flags_tip_parts.append("Heartbeat stale")
        if last_error:
            flags_tip_parts.append(f"Last error: {last_error}")
        flags_tip = " • ".join([p for p in flags_tip_parts if p])

        status_disp = str(status or "")
        desired_disp = str(desired or "")
        if desired_disp and desired_disp.strip() and desired_disp.strip().lower() != str(status_disp).strip().lower():
            status_disp = f"{status_disp} → {desired_disp}"

        rows.append(
            {
                "bot_id": bot_id_int,
                "account_id": b.get("account_id"),
                "exchange_id": exchange,
                "market_type": ("PERPS" if str(symbol).upper().startswith("PERP_") else "SPOT") if symbol else "",
                "market": ("PERPS" if str(symbol).upper().startswith("PERP_") else "SPOT") if symbol else "",
                "icon_uri": icon_uri,
                "base_asset": base_asset,
                "health_state": health_state,
                "Health": health_html,
                "Name": name,
                "Symbol": symbol,
                "TF": tf,
                "Status": status_disp,
                "PnL": float(total_pnl),
                "Next": str(next_action),
                "Account": (
                    (
                        (str(b.get("account_label") or "")).strip()
                        or (f"#{int(b.get('account_id'))}" if b.get("account_id") is not None else "")
                    )
                    + (
                        f" ({str(b.get('account_status') or '').strip().lower()})"
                        if (b.get("account_label") is not None or b.get("account_id") is not None)
                        and str(b.get("account_status") or "").strip()
                        else ""
                    )
                )
                if (b.get("account_label") is not None or b.get("account_id") is not None)
                else "–",
                "Risk/Degraded": flags_disp,
                "_flags_tip": flags_tip,
                "_last_error": last_error,
            }
        )

    df = pd.DataFrame(rows)

    # Normalize market label used by Results-style Asset renderer.
    if "market_type" in df.columns and "market" not in df.columns:
        df["market"] = df["market_type"]
    if "market" in df.columns:
        try:
            m = df["market"].fillna("").astype(str).str.strip().str.lower()
            m = m.replace({"perp": "perps", "futures": "perps", "future": "perps"})
            df["market"] = m.map({"perps": "PERPS", "spot": "SPOT"}).fillna(m.str.upper())
        except Exception:
            pass

    df = apply_global_filters(df, get_global_filters())
    return df


def _render_live_bots_aggrid(df: pd.DataFrame, *, grid_key: str = "live_bots_overview_grid") -> None:
    if df is None or df.empty:
        st.info("No bots found.")
        return

    if AgGrid is None or GridOptionsBuilder is None or JsCode is None or GridUpdateMode is None or DataReturnMode is None:
        st.dataframe(df, hide_index=True, width="stretch")
        return

    # Keep stable column order for the overview grid.
    ordered_cols = [
        "Health",
        "Name",
        "Symbol",
        "TF",
        "Status",
        "PnL",
        "Next",
        "Account",
        "Risk/Degraded",
        "icon_uri",
        "base_asset",
        "market",
        "bot_id",
        "health_state",
        "account_id",
        "exchange_id",
        "market_type",
        "_flags_tip",
        "_last_error",
    ]
    df = df[[c for c in ordered_cols if c in df.columns]]

    # Results-style Asset renderer for the bots Symbol column (icon + label).
    asset_market_getter = asset_market_getter_js(
        symbol_field="Symbol",
        market_field="market",
        base_asset_field="base_asset",
        asset_field="asset",
    )
    asset_renderer = asset_renderer_js(icon_field="icon_uri", size_px=18, text_color="#FFFFFF")

    health_renderer = JsCode(
        """
        class HealthRenderer {
            init(params) {
                const wrap = document.createElement('div');
                wrap.style.display = 'flex';
                wrap.style.alignItems = 'center';
                wrap.style.justifyContent = 'center';
                wrap.style.width = '100%';
                const v = (params && params.value !== undefined && params.value !== null) ? params.value.toString() : '';
                wrap.innerHTML = v;
                this.eGui = wrap;
            }
            getGui() { return this.eGui; }
            refresh(params) { return false; }
        }
        """
    )

    timeframe_badge_style = aggrid_pill_style("timeframe")
    status_badge_style = aggrid_pill_style("status")
    flags_style = aggrid_pill_style("risk")

    status_cap_formatter = JsCode(
        """
        function(params) {
            try {
                const v = (params && params.value !== undefined && params.value !== null) ? params.value.toString() : '';
                const s = v.trim();
                if (!s) return '';

                function cap1(x) {
                    const t = (x || '').toString().trim();
                    if (!t) return '';
                    return t.charAt(0).toUpperCase() + t.slice(1);
                }

                // Preserve desired-state arrows while capitalizing each part.
                if (s.indexOf('→') >= 0) {
                    const parts = s.split('→').map(p => cap1(p));
                    return parts.join(' → ');
                }
                return cap1(s);
            } catch (e) {
                return (params && params.value !== undefined && params.value !== null) ? params.value : '';
            }
        }
        """
    )

    pnl_style = JsCode(
        r"""
        function(params) {
            const v = Number(params.value);
            if (!isFinite(v)) { return { color: '#94a3b8', textAlign: 'right' }; }
            if (v > 0) { return { color: '#7ee787', fontWeight: '800', textAlign: 'right' }; }
            if (v < 0) { return { color: '#ff7b72', fontWeight: '800', textAlign: 'right' }; }
            return { color: '#94a3b8', textAlign: 'right' };
        }
        """
    )

    pnl_formatter = JsCode(
        """
        function(params) {
            const v = Number(params.value);
            if (!isFinite(v)) { return ''; }
            return v.toFixed(2);
        }
        """
    )

    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_grid_options(
        headerHeight=50,
        rowHeight=38,
        suppressRowHoverHighlight=False,
        animateRows=False,
        suppressCellFocus=True,
    )

    if "Health" in df.columns:
        gb.configure_column(
            "Health",
            pinned="left",
            width=60,
            cellRenderer=health_renderer,
            cellClass="ag-center-aligned-cell",
            sortable=False,
            filter=False,
            headerName="",
        )
    if "Name" in df.columns:
        gb.configure_column(
            "Name",
            pinned="left",
            width=220,
            wrapHeaderText=True,
            autoHeaderHeight=True,
        )
    if "Symbol" in df.columns:
        gb.configure_column(
            "Symbol",
            width=170,
            wrapHeaderText=True,
            autoHeaderHeight=True,
            cellRenderer=asset_renderer,
            valueGetter=asset_market_getter,
        )
    if "TF" in df.columns:
        gb.configure_column(
            "TF",
            headerName="Timeframe",
            width=95,
            cellStyle=timeframe_badge_style,
            cellClass="ag-center-aligned-cell",
            wrapHeaderText=True,
            autoHeaderHeight=True,
        )
    if "Status" in df.columns:
        gb.configure_column(
            "Status",
            width=150,
            cellStyle=status_badge_style,
            valueFormatter=status_cap_formatter,
            cellClass="ag-center-aligned-cell",
            wrapHeaderText=True,
            autoHeaderHeight=True,
        )
    if "PnL" in df.columns:
        gb.configure_column(
            "PnL",
            width=110,
            type=["numericColumn"],
            valueFormatter=pnl_formatter,
            cellStyle=pnl_style,
        )
    if "Next" in df.columns:
        gb.configure_column("Next", width=260, wrapHeaderText=True, autoHeaderHeight=True)
    if "Account" in df.columns:
        gb.configure_column("Account", width=220, wrapHeaderText=True, autoHeaderHeight=True)
    if "Risk/Degraded" in df.columns:
        gb.configure_column(
            "Risk/Degraded",
            width=140,
            cellStyle=flags_style,
            cellClass="ag-center-aligned-cell",
            tooltipField="_flags_tip",
            wrapHeaderText=True,
            autoHeaderHeight=True,
        )

    for hidden in (
        "bot_id",
        "health_state",
        "account_id",
        "exchange_id",
        "market_type",
        "market",
        "icon_uri",
        "base_asset",
        "_flags_tip",
        "_last_error",
    ):
        if hidden in df.columns:
            gb.configure_column(hidden, hide=True)

    grid_options = gb.build()

    try:
        grid_options["getRowId"] = JsCode(
            """
            function(params) {
                try {
                    return (params && params.data && params.data.bot_id !== undefined && params.data.bot_id !== null)
                        ? params.data.bot_id.toString()
                        : undefined;
                } catch (e) { return undefined; }
            }
            """
        )
    except Exception:
        pass

    pre_selected_rows: List[int] = []
    try:
        current_selected = st.session_state.get("selected_bot_id")
        if current_selected not in (None, "") and "bot_id" in df.columns:
            target = int(current_selected)
            matches = df.index[df["bot_id"].astype(int) == target].tolist()
            if matches:
                pre_selected_rows = [int(matches[0])]
    except Exception:
        pre_selected_rows = []

    grid = AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        data_return_mode=DataReturnMode.AS_INPUT,
        enable_enterprise_modules=False,
        allow_unsafe_jscode=True,
        height=620,
        theme="dark",
        custom_css=_aggrid_dark_custom_css(),
        key=str(grid_key or "live_bots_overview_grid"),
        pre_selected_rows=pre_selected_rows,
    )

    try:
        sel_raw = grid.get("selected_rows")
        if sel_raw is None:
            sel: list[dict[str, Any]] = []
        elif isinstance(sel_raw, list):
            sel = sel_raw
        elif isinstance(sel_raw, pd.DataFrame):
            sel = sel_raw.to_dict("records")
        else:
            try:
                sel = list(sel_raw)
            except Exception:
                sel = []

        if sel:
            row0 = sel[0] if isinstance(sel[0], dict) else {}
            bot_id_selected = row0.get("bot_id")

            # Some streamlit-aggrid versions drop hidden fields from selected_rows.
            if bot_id_selected in (None, ""):
                try:
                    info = row0.get("_selectedRowNodeInfo") if isinstance(row0, dict) else None
                    if isinstance(info, dict):
                        idx = info.get("rowIndex")
                        if idx is None:
                            idx = info.get("nodeRowIndex")
                        if idx is not None:
                            idx_i = int(idx)
                            if 0 <= idx_i < len(df) and "bot_id" in df.columns:
                                bot_id_selected = df.iloc[idx_i]["bot_id"]
                except Exception:
                    bot_id_selected = None

            if bot_id_selected not in (None, ""):
                bot_id_int = int(bot_id_selected)
                if st.session_state.get("selected_bot_id") != bot_id_int:
                    _set_selected_bot(bot_id_int)
                    st.rerun()
    except Exception:
        pass


def _aggrid_dark_custom_css() -> dict:
    """Centralized AG Grid dark theme CSS.

    Keep this aligned with the styling used in Results so pages look consistent.
    """

    return {
        ".ag-root-wrapper": {
            "--ag-font-family": "Roboto,-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif",
            "--ag-font-size": "13px",
            "--ag-header-font-size": "14px",
            "--ag-row-height": "36px",
            "--ag-header-height": "40px",
            "--ag-background-color": "#262627",
            "--ag-odd-row-background-color": "#242425",
            "--ag-header-background-color": "#2d2d2e",
            "--ag-header-foreground-color": "rgba(242,242,242,0.96)",
            "--ag-foreground-color": "rgba(242,242,242,0.96)",
            "--ag-secondary-foreground-color": "rgba(242,242,242,0.70)",
            "--ag-border-color": "#323233",
            "--ag-row-border-color": "#2f2f30",
            "--ag-cell-horizontal-padding": "10px",
            "--ag-border-radius": "6px",
            "--ag-row-hover-color": "rgba(242, 140, 40, 0.10)",
            "--ag-selected-row-background-color": "rgba(242, 140, 40, 0.14)",
            "--ag-control-panel-background-color": "#262627",
            "--ag-menu-background-color": "#262627",
            "--ag-popup-background-color": "#262627",
            "--ag-popup-shadow": "0 8px 24px rgba(0,0,0,0.35)",
            "--ag-input-background-color": "#333334",
            "--ag-input-border-color": "#323233",
            "border": "1px solid #323233",
            "border-radius": "6px",
            "background-color": "#262627",
            "color": "rgba(242,242,242,0.96)",
            "font-family": "Roboto,-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif",
        },
        ".ag-root-wrapper-body": {"background-color": "#262627"},
        ".ag-body-viewport": {"background-color": "#262627"},
        ".ag-body": {"background-color": "#262627"},
        ".ag-center-cols-viewport": {"background-color": "#262627"},
        ".ag-center-cols-container": {"background-color": "#262627"},
        ".ag-header": {
            "border-bottom": "1px solid rgba(255,255,255,0.10)",
            "background-color": "#2d2d2e",
        },
        ".ag-header-cell-label": {
            "font-weight": "500",
            "letter-spacing": "0.00em",
            "color": "rgba(242,242,242,0.96)",
            "white-space": "normal",
            "line-height": "1.1",
        },
        ".ag-header-cell-text": {"color": "rgba(242,242,242,0.96)", "white-space": "normal"},
        ".ag-header-cell": {"align-items": "center"},
        ".ag-row:nth-child(even) .ag-cell": {"background-color": "#242425"},
        ".ag-cell": {
            "padding-left": "10px",
            "padding-right": "10px",
            "font-variant-numeric": "tabular-nums",
            "color": "rgba(242,242,242,0.96)",
        },
        ".ag-row-hover .ag-cell": {"background-color": "rgba(242, 140, 40, 0.10)"},
        ".ag-row-hover.ag-row-selected .ag-cell": {"background-color": "rgba(242, 140, 40, 0.14)"},
        # Actions column: tighter padding so icon buttons fit without a huge column.
        ".ag-cell[col-id='actions']": {"padding-left": "2px", "padding-right": "2px"},
        ".ag-cell-wrapper": {"align-items": "center", "height": "100%"},
        ".ag-cell-value": {"display": "flex", "align-items": "center", "height": "100%"},
        ".ag-row": {"background-color": "#262627"},
        ".ag-body": {"background-color": "#262627"},
        ".ag-body-viewport": {"color-scheme": "dark", "background-color": "#262627"},
        ".ag-center-cols-viewport": {"background-color": "#262627"},
        ".ag-center-cols-container": {"background-color": "#262627"},
        ".ag-overlay-no-rows-center": {"background-color": "#262627", "color": "rgba(242,242,242,0.7)"},
        ".ag-overlay-loading-center": {"background-color": "#262627", "color": "rgba(242,242,242,0.7)"},
        ".ag-popup": {"color-scheme": "dark"},
        ".ag-popup-child": {
            "background-color": "#262627",
            "border": "1px solid #323233",
            "box-shadow": "0 8px 24px rgba(0,0,0,0.35)",
            "color": "rgba(242,242,242,0.96)",
        },
        ".ag-menu": {
            "background-color": "#262627",
            "color": "rgba(242,242,242,0.96)",
            "border": "1px solid #323233",
            "color-scheme": "dark",
        },
        ".ag-menu-option": {"color": "rgba(242,242,242,0.96)"},
        ".ag-menu-option:hover": {"background-color": "rgba(242, 242, 242, 0.06)"},
        ".ag-filter": {"background-color": "#262627", "color": "rgba(242,242,242,0.96)"},
        ".ag-filter-body": {"background-color": "#262627"},
        ".ag-filter-apply-panel": {"background-color": "#262627"},
        ".ag-input-field-input": {
            "background-color": "#333334",
            "border": "1px solid #323233",
            "color": "rgba(242,242,242,0.96)",
        },
        ".ag-picker-field-wrapper": {"background-color": "#333334", "border": "1px solid #323233"},
        ".ag-select": {"background-color": "#333334", "color": "rgba(242,242,242,0.96)"},
        ".ag-right-aligned-cell": {"font-variant-numeric": "tabular-nums"},
        ".ag-center-aligned-cell": {"justify-content": "center"},
    }


def _extract_aggrid_models(grid: Any) -> tuple[Optional[dict], Optional[list]]:
    try:
        if isinstance(grid, dict):
            state = grid.get("grid_state") or grid.get("gridState")
            if isinstance(state, dict):
                filt = state.get("filterModel") or state.get("filter_model")
                sort = state.get("sortModel") or state.get("sort_model")
                if filt or sort:
                    return filt if isinstance(filt, dict) else None, sort if isinstance(sort, list) else None
            opts = grid.get("gridOptions") or grid.get("grid_options")
            if isinstance(opts, dict):
                filt = opts.get("filterModel") or opts.get("filter_model")
                sort = opts.get("sortModel") or opts.get("sort_model")
                if filt or sort:
                    return filt if isinstance(filt, dict) else None, sort if isinstance(sort, list) else None
    except Exception:
        pass
    return None, None


def _extract_aggrid_columns_state(grid: Any) -> Optional[list]:
    try:
        if hasattr(grid, "columns_state"):
            cols = getattr(grid, "columns_state")
            if isinstance(cols, list) and cols:
                return cols
        if isinstance(grid, dict):
            cols = grid.get("columns_state") or grid.get("columnsState") or grid.get("column_state") or grid.get("columnState")
            if isinstance(cols, list) and cols:
                return cols
            state = grid.get("grid_state") or grid.get("gridState")
            if isinstance(state, dict):
                cols = (
                    state.get("columnState")
                    or state.get("columnsState")
                    or state.get("column_state")
                    or state.get("columns_state")
                )
                if isinstance(cols, list) and cols:
                    return cols
    except Exception:
        pass
    return None

def _extract_ma_periods_from_config_dict(config_dict: Optional[Dict[str, Any]]) -> List[int]:
    if not isinstance(config_dict, dict):
        return []
    trend = config_dict.get("trend") if isinstance(config_dict.get("trend"), dict) else {}
    raw = None
    for key in ("ma_len", "ma_lens", "ma_period", "ma_periods"):
        if key in trend:
            raw = trend.get(key)
            break
    if raw is None:
        return []

    periods: List[int] = []
    if isinstance(raw, (list, tuple)):
        values = list(raw)
    else:
        values = [raw]

    for v in values:
        try:
            n = int(v)
        except (TypeError, ValueError):
            continue
        if n > 0:
            periods.append(n)

    # Deduplicate while preserving order.
    out: List[int] = []
    seen: set[int] = set()
    for n in periods:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out


def _extract_trend_ma_settings_from_config_dict(config_dict: Optional[Dict[str, Any]]) -> Tuple[int, str, List[int]]:
    """Extract trend MA settings from the strategy snapshot.

    Returns: (ma_interval_min, ma_type, ma_lens)
    - ma_lens supports int or list in config.
    """

    if not isinstance(config_dict, dict):
        return 1, "sma", []
    trend = config_dict.get("trend") if isinstance(config_dict.get("trend"), dict) else {}
    try:
        interval_min = int(trend.get("ma_interval_min") or 1)
    except (TypeError, ValueError):
        interval_min = 1
    ma_type = str(trend.get("ma_type") or "sma")
    ma_lens = _extract_ma_periods_from_config_dict(config_dict)
    return max(1, interval_min), ma_type, ma_lens


def _estimate_warmup_bars_for_trend_ma(timeframe: str, ma_interval_min: int, ma_periods: List[int]) -> int:
    """Estimate how many *base timeframe* candles are needed so the trend MA has a full window at the first displayed bar."""

    if not ma_periods:
        return 0
    base_min = timeframe_to_minutes(timeframe) or 1
    try:
        step = max(1, int(round(max(int(ma_interval_min or 1), 1) / float(base_min))))
    except Exception:
        step = 1
    try:
        max_len = max(int(p) for p in ma_periods if int(p) > 0)
    except Exception:
        max_len = 0
    if max_len <= 0:
        return 0
    # Strategy uses resampled closes + current close; having step*len bars before the first displayed bar
    # is a good approximation for a stable full-window MA from the start.
    return int(step * max_len)


@st.cache_data(show_spinner=False)
def _cached_strategy_trend_ma_overlays(
    scope_key: str,
    timeframe: str,
    calc_ts_ns: Tuple[int, ...],
    calc_closes: Tuple[float, ...],
    display_ts_ns: Tuple[int, ...],
    ma_lens: Tuple[int, ...],
    ma_interval_min: int,
    ma_type: str,
) -> Dict[int, List[float]]:
    """Compute MA overlays like DragonDcaAtrStrategy trend MA (HTF close -> MA -> ffill to base)."""

    out: Dict[int, List[float]] = {}
    if not calc_closes or not ma_lens:
        return out

    calc_ts = pd.to_datetime(list(calc_ts_ns), utc=True)
    display_ts = pd.to_datetime(list(display_ts_ns), utc=True)
    calc_series = pd.Series(list(calc_closes), index=calc_ts).sort_index()

    for period in ma_lens:
        p = int(period)
        if p <= 0:
            continue
        _, _, _, mapped_ma = htf_ma_mapping(
            calc_series.index,
            calc_series.values,
            int(ma_interval_min or 1),
            p,
            str(ma_type or "sma"),
        )
        if mapped_ma.empty:
            out[p] = [math.nan] * len(display_ts)
            continue
        mapped_display = mapped_ma.reindex(display_ts, method="ffill")
        out[p] = [float(v) if pd.notna(v) else math.nan for v in mapped_display.values]

    return out


def _add_ma_overlays_to_candles_fig(
    fig: go.Figure,
    *,
    x: pd.Series,
    closes: pd.Series,
    ts: pd.Series,
    scope_key: str,
    timeframe: str,
    ma_periods: List[int],
    ma_interval_min: int,
    ma_type: str,
    warmup_ts: Optional[pd.Series] = None,
    warmup_closes: Optional[pd.Series] = None,
    ma_calc_ts: Optional[pd.Series] = None,
    ma_calc_closes: Optional[pd.Series] = None,
) -> None:
    if not ma_periods:
        return

    # Combine warmup + calculation series for MA computation.
    base_ts = pd.to_datetime(ma_calc_ts if ma_calc_ts is not None else ts, utc=True)
    base_closes = pd.to_numeric(ma_calc_closes if ma_calc_closes is not None else closes, errors="coerce")
    if warmup_ts is not None and warmup_closes is not None and len(warmup_ts) and len(warmup_closes):
        ts_all = pd.concat([pd.to_datetime(warmup_ts, utc=True), base_ts], ignore_index=True)
        closes_all = pd.concat([pd.to_numeric(warmup_closes, errors="coerce"), base_closes], ignore_index=True)
    else:
        ts_all = base_ts
        closes_all = base_closes

    try:
        close_tuple = tuple(float(v) for v in closes_all.tolist())
    except Exception:
        return
    try:
        # Use int64 nanoseconds for stable hashing in cache.
        ts_ns_tuple = tuple(int(v) for v in pd.to_datetime(ts_all, utc=True).astype("int64").tolist())
        display_ns_tuple = tuple(int(v) for v in pd.to_datetime(ts, utc=True).astype("int64").tolist())
    except Exception:
        return
    ma_periods_tuple = tuple(int(p) for p in ma_periods if int(p) > 0)
    if not ma_periods_tuple:
        return
    series_map = _cached_strategy_trend_ma_overlays(
        str(scope_key or ""),
        str(timeframe or ""),
        ts_ns_tuple,
        close_tuple,
        display_ns_tuple,
        ma_periods_tuple,
        int(ma_interval_min),
        str(ma_type or "sma"),
    )
    for period in ma_periods:
        y_full = series_map.get(int(period))
        if not y_full:
            continue
        y = y_full
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y,
                mode="lines",
                name=f"MA {int(period)}",
                connectgaps=False,
            )
        )


def test_woox_credential(user_id: str, credential_id: int, symbol: str) -> Dict[str, Any]:
    """Connectivity check using stored credentials (no env keys).

    Returns a dict safe for UI display (no secrets).
    """

    def _truncate(v: Any, n: int = 200) -> Optional[str]:
        if v is None:
            return None
        s = str(v)
        s = s.replace("\n", " ").replace("\r", " ").strip()
        return s[:n] if len(s) > n else s

    def _safe_error(exc: Exception) -> Dict[str, Any]:
        # Never echo raw response payloads; keep error details short.
        if isinstance(exc, WooXAPIError):
            payload = getattr(exc, "payload", {}) or {}
            return {
                "type": "WooXAPIError",
                "status_code": getattr(exc, "status_code", None),
                "message": _truncate(str(exc)),
                "payload_message": _truncate(payload.get("message")),
            }
        return {"type": exc.__class__.__name__, "message": _truncate(str(exc))}

    symbol = (symbol or "").strip() or "PERP_ETH_USDT"
    with _job_conn() as conn:
        cred = get_credential(conn, user_id, int(credential_id))

    if not cred:
        return {
            "ok": False,
            "error": "Credential not found for this user",
            "status_code": None,
            "payload_message": None,
        }

    exchange_id = str(cred.get("exchange_id") or "").strip().lower()
    if exchange_id and exchange_id != "woox":
        return {
            "ok": False,
            "error": f"Selected credential is for exchange '{exchange_id}', not 'woox'",
            "status_code": None,
            "payload_message": None,
        }

    api_key = str(cred.get("api_key") or "").strip()
    api_secret_enc = str(cred.get("api_secret_enc") or "").strip()
    if not api_key or not api_secret_enc:
        return {
            "ok": False,
            "error": "Credential record is incomplete (missing api_key/api_secret)",
            "status_code": None,
            "payload_message": None,
        }

    # Decrypt secret locally; never log or return it.
    try:
        api_secret = decrypt_str(api_secret_enc)
    except CryptoConfigError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "status_code": None,
            "payload_message": None,
        }

    base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")
    client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)

    # Prefer safe private endpoint: positions.
    try:
        data = client.get_positions(symbol)
        rows = data.get("rows") if isinstance(data, dict) else None
        rows = rows if isinstance(rows, list) else []
        sample = rows[0] if rows else {}
        return {
            "ok": True,
            "endpoint": "get_positions",
            "symbol": symbol,
            "positions_count": len(rows),
            "sample": {
                "positionSide": sample.get("positionSide"),
                "holding": sample.get("holding"),
                "markPrice": sample.get("markPrice"),
            }
            if isinstance(sample, dict)
            else {},
            "api_key_masked": mask_api_key(api_key),
        }
    except WooXAPIError as exc:
        # Fallbacks: orders, then public orderbook.
        try:
            data = client.get_orders(symbol, status="INCOMPLETE")
            rows = data.get("rows") if isinstance(data, dict) else None
            rows = rows if isinstance(rows, list) else []
            sample = rows[0] if rows else {}
            return {
                "ok": True,
                "endpoint": "get_orders",
                "symbol": symbol,
                "open_orders_count": len(rows),
                "sample": {
                    "status": sample.get("status"),
                    "side": sample.get("side"),
                    "positionSide": sample.get("positionSide"),
                    "price": sample.get("price"),
                    "quantity": sample.get("quantity"),
                }
                if isinstance(sample, dict)
                else {},
                "api_key_masked": mask_api_key(api_key),
                "warn": {
                    "primary_endpoint": "get_positions",
                    **_safe_error(exc),
                },
            }
        except Exception:
            pass
        try:
            data = client.get_orderbook(symbol, depth=1)
            bids = data.get("bids") if isinstance(data, dict) else None
            asks = data.get("asks") if isinstance(data, dict) else None
            best_bid = bids[0] if isinstance(bids, list) and bids else None
            best_ask = asks[0] if isinstance(asks, list) and asks else None
            return {
                "ok": True,
                "endpoint": "get_orderbook",
                "symbol": symbol,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "api_key_masked": mask_api_key(api_key),
                "warn": {
                    "primary_endpoint": "get_positions",
                    **_safe_error(exc),
                },
            }
        except WooXAPIError as exc2:
            return {
                "ok": False,
                "error": "WooX connectivity failed",
                "details": _safe_error(exc2),
                "api_key_masked": mask_api_key(api_key),
            }
        except Exception as exc2:
            return {
                "ok": False,
                "error": "WooX connectivity failed",
                "details": _safe_error(exc2),
                "api_key_masked": mask_api_key(api_key),
            }
    except Exception as exc:
        return {
            "ok": False,
            "error": "WooX connectivity failed",
            "details": _safe_error(exc),
            "api_key_masked": mask_api_key(api_key),
        }

DURATION_CHOICES = {
    "1 week": timedelta(weeks=1),
    "1 month": timedelta(days=30),
    "3 months": timedelta(days=90),
    "6 months": timedelta(days=180),
    "12 months": timedelta(days=365),
}
    
def _aggrid_metrics_gradient_style_js() -> str:
    """Shared metric gradient style (Results + Sweeps Runs)."""

    return """
            function(params) {
                const col = (params && params.colDef && params.colDef.field) ? params.colDef.field.toString() : '';
                const v = Number(params.value);
                if (!isFinite(v)) { return {}; }

                const greenText = '#7ee787';
                const redText = '#ff7b72';
                const neutralText = '#cbd5e1';
                const greenRgb = '46,160,67';
                const redRgb = '248,81,73';

                function clamp01(x) { return Math.max(0.0, Math.min(1.0, x)); }
                function alphaFromT(t) { return 0.10 + (0.22 * clamp01(t)); }
                function styleGoodBad(isGood, t) {
                    const a = alphaFromT(t);
                    if (isGood === true) return { color: greenText, backgroundColor: `rgba(${greenRgb},${a})`, textAlign: 'center' };
                    if (isGood === false) return { color: redText, backgroundColor: `rgba(${redRgb},${a})`, textAlign: 'center' };
                    return { color: neutralText, textAlign: 'center' };
                }

                function asRatio(x) {
                    if (!isFinite(x)) { return x; }
                    return (Math.abs(x) <= 1.0) ? x : (x / 100.0);
                }

                if (col === 'max_drawdown_pct') {
                    const r = asRatio(v);
                    const t = clamp01(r / 0.30);
                    return styleGoodBad(false, t);
                }
                if (col === 'net_return_pct') {
                    const r = asRatio(v);
                    const t = clamp01(Math.abs(r) / 0.50);
                    if (r > 0) return styleGoodBad(true, t);
                    if (r < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'roi_pct_on_margin') {
                    // v is already a percent (e.g. 100.0 means +100%)
                    const t = clamp01(Math.abs(v) / 200.0);
                    if (v > 0) return styleGoodBad(true, t);
                    if (v < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'win_rate') {
                    const r = asRatio(v);
                    const dist = Math.abs(r - 0.5);
                    const t = clamp01(dist / 0.25);
                    return styleGoodBad(r >= 0.5, t);
                }
                if (['profit_factor', 'cpc_index', 'common_sense_ratio'].includes(col)) {
                    const dist = Math.abs(v - 1.0);
                    const t = clamp01(dist / 1.0);
                    return styleGoodBad(v >= 1.0, t);
                }
                if (['sharpe', 'sortino'].includes(col)) {
                    if (v >= 1.0) {
                        const t = clamp01((v - 1.0) / 2.0);
                        return styleGoodBad(true, t);
                    }
                    if (v < 0.0) {
                        const t = clamp01(Math.abs(v) / 1.5);
                        return styleGoodBad(false, t);
                    }
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'net_profit') {
                    const t = clamp01(Math.abs(v) / 1000.0);
                    if (v > 0) return styleGoodBad(true, t);
                    if (v < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                return {};
            }
            """

def _sanitize_widget_choice(
    state: MutableMapping[str, Any],
    key: str,
    options: Sequence[Any],
    *,
    default: Any = None,
) -> Optional[Any]:
    """Normalize invalid widget values before Streamlit renders controls."""

    if not options:
        return None
    if key not in state:
        return None
    current = state.get(key)
    if current in options:
        return current

    # Case-insensitive/normalized match
    cur_norm = str(current).strip().lower()
    for opt in options:
        if str(opt).strip().lower() == cur_norm:
            state[key] = opt
            return opt

    fallback = default if default in options else options[0]
    state[key] = fallback
    return fallback

ORDER_STYLE_OPTIONS = {
    "Market": OrderStyle.MARKET,
    "Limit": OrderStyle.LIMIT,
    "Maker or Cancel": OrderStyle.MAKER_OR_CANCEL,
}


def _segmented_or_radio(
    label: str,
    options: list[Any] | tuple[Any, ...],
    *,
    key: str,
    index: int = 0,
    format_func: Any = None,
    help: str | None = None,
    on_change: Any = None,
    args: tuple[Any, ...] = (),
) -> Any:
    """Use segmented control when available; fallback to radio.

    Streamlit versions vary; this keeps the Backtest UX consistent without
    requiring a minimum Streamlit version.
    """

    opts = list(options or [])
    if not opts:
        return None

    idx = int(index) if isinstance(index, int) else 0
    if idx < 0 or idx >= len(opts):
        idx = 0
    default = opts[idx]

    sc = getattr(st, "segmented_control", None)
    if callable(sc):
        try:
            return sc(
                label,
                options=opts,
                default=default,
                key=key,
                format_func=format_func,
                help=help,
                on_change=on_change,
                args=args,
            )
        except TypeError:
            # Fall back to radio if the runtime Streamlit signature differs.
            pass

    return st.radio(
        label,
        options=opts,
        index=idx,
        key=key,
        format_func=format_func,
        help=help,
        on_change=on_change,
        args=args,
        horizontal=True,
    )


def init_state_defaults(settings: Dict[str, Any]) -> None:
    """Seed Streamlit session_state defaults for UI widgets.

    Rules:
    - session_state is the source of truth
    - defaults apply only when the key is missing
    - must not overwrite deep-link or selection keys
    """

    def _seed(key: str, value: Any) -> None:
        if key not in st.session_state:
            st.session_state[key] = value

    # UI schema versioning (for safe one-time migrations)
    schema_key = "dragon_ui_schema_version"
    if schema_key not in st.session_state:
        st.session_state[schema_key] = 1
    try:
        schema_version = int(st.session_state.get(schema_key) or 0)
    except (TypeError, ValueError):
        schema_version = 0

    # --- Backtesting tab -------------------------------------------------
    init_balance_default = float((settings or {}).get("initial_balance_default", 1000.0))
    _seed("bt_initial_balance", init_balance_default)
    _seed("bt_fee_rate", fee_fraction_from_pct((settings or {}).get("fee_spot_pct", 0.10)))

    _seed("bt_run_mode", "Single backtest")
    _seed("bt_sweep_name", "Dragon sweep")
    _seed("bt_sweep_fields", [])

    # Data selection
    _seed("data_source_mode", "Crypto (CCXT)")

    # Strategy config
    _seed("bt_max_entries", 5)
    _seed("bt_initial_entry_sizing_mode", InitialEntrySizingMode.PCT_BALANCE.value)
    _seed("bt_initial_entry_balance_pct", 10.0)
    _seed("bt_initial_entry_fixed_usd", 100.0)
    _seed("bt_entry_cooldown_min", 10)
    _seed("bt_entry_timeout_min", 10)
    _seed("bt_allow_long", True)
    _seed("bt_allow_short", False)
    # New UI: Trade direction radio; canonical source for allow_long/allow_short.
    # We keep bt_allow_long/bt_allow_short for payload compatibility.
    _seed("bt_trade_direction", "Long only")
    _seed("bt_use_indicator_consensus", True)
    _seed("bt_lock_atr_on_entry", False)
    _seed("bt_use_avg_entry_for_dca_base", True)
    _seed("bt_backtest_leverage", 1)
    _seed("bt_global_dyn_pct", 0.0)
    _seed("bt_dyn_entry_pct", 0.0)
    _seed("bt_dyn_dca_pct", 0.0)
    _seed("bt_dyn_tp_pct", 0.0)

    _seed("bt_entry_order_style_label", "Maker or Cancel")
    _seed("bt_exit_order_style_label", "Maker or Cancel")

    _seed("bt_prefer_bbo_maker", bool((settings or {}).get("prefer_bbo_maker", True)))
    _seed("bt_bbo_queue_level", int((settings or {}).get("bbo_queue_level", 1) or 1))

    # Optional MA direction gate for entries
    _seed("bt_use_ma_direction", True)

    # Stop Loss mode + fields
    _seed("bt_sl_mode", StopLossMode.PCT.value)
    _seed("bt_sl_pct", 5.0)
    _seed("bt_atr_period", 14)
    _seed("bt_sl_atr_mult", 3.0)
    _seed("bt_trail_distance_pct", 7.0)
    _seed("bt_trail_activation_atr_mult", 1.0)
    _seed("bt_trail_distance_atr_mult", 2.0)

    # Take Profit mode + fields
    _seed("bt_tp_mode", TakeProfitMode.ATR.value)
    _seed("bt_tp_pct", 1.0)
    _seed("bt_tp_atr_mult", 10.0)

    _seed("bt_fixed_sl_pct", 5.0)
    _seed("bt_fixed_tp_pct", 1.0)
    _seed("bt_use_atr_tp", True)
    _seed("bt_tp_atr_multiple", 10.0)
    _seed("bt_tp_atr_period", 14)
    _seed("bt_tp_replace_threshold_pct", 0.05)
    _seed("bt_trail_activation_pct", 5.0)
    _seed("bt_trail_stop_pct", 7.0)

    _seed("bt_base_deviation_pct", 1.0)
    _seed("bt_deviation_multiplier", 2.0)
    _seed("bt_volume_multiplier", 2.0)

    _seed("bt_ma_interval_min", 120)
    _seed("bt_ma_length", 200)
    _seed("bt_ma_type", "Sma")
    _seed("bt_bb_interval_min", 60)
    _seed("bt_macd_interval_min", 360)
    _seed("bt_rsi_interval_min", 1_440)
    _seed("bt_bb_length", 20)
    _seed("bt_rsi_length", 14)

    _seed("bt_bb_dev_up", 2.0)
    _seed("bt_bb_dev_down", 2.0)
    _seed("bt_bb_ma_type", "Sma")
    _seed("bt_bb_deviation", 0.2)
    _seed("bt_bb_require_fcc", False)
    _seed("bt_bb_reset_middle", False)
    _seed("bt_bb_allow_mid_sells", False)

    _seed("bt_macd_fast", 12)
    _seed("bt_macd_slow", 26)
    _seed("bt_macd_signal", 7)

    _seed("bt_rsi_buy_level", 30.0)
    _seed("bt_rsi_sell_level", 70.0)

    # Migration: a prior refactor briefly allowed numeric widgets to initialize
    # to their Streamlit defaults (often 0/1). If we detect that "everything"
    # is still at those sentinel values, reset backtest defaults once.
    if schema_version < 2:
        suspect_keys = [
            "bt_max_entries",
            "bt_entry_cooldown_min",
            "bt_entry_timeout_min",
            "bt_ma_interval_min",
            "bt_ma_length",
            "bt_bb_length",
            "bt_rsi_length",
        ]
        vals = [st.session_state.get(k) for k in suspect_keys]
        def _is_suspect(v: Any) -> bool:
            try:
                return float(v) in (0.0, 1.0)
            except (TypeError, ValueError):
                return True
        if vals and all(_is_suspect(v) for v in vals):
            # Clear only backtest-related keys so the _seed() defaults apply.
            for k in list(st.session_state.keys()):
                if k.startswith("bt_"):
                    st.session_state.pop(k, None)
            st.session_state.pop("data_source_mode", None)

            # Re-seed after clearing.
            init_balance_default = float((settings or {}).get("initial_balance_default", 1000.0))
            _seed("bt_initial_balance", init_balance_default)
            _seed("bt_fee_rate", fee_fraction_from_pct((settings or {}).get("fee_spot_pct", 0.10)))
            _seed("bt_run_mode", "Single backtest")
            _seed("bt_sweep_name", "Dragon sweep")
            _seed("bt_sweep_fields", [])
            _seed("data_source_mode", "Crypto (CCXT)")
            _seed("bt_max_entries", 5)
            _seed("bt_initial_entry_sizing_mode", InitialEntrySizingMode.PCT_BALANCE.value)
            _seed("bt_initial_entry_balance_pct", 10.0)
            _seed("bt_initial_entry_fixed_usd", 100.0)
            _seed("bt_entry_cooldown_min", 10)
            _seed("bt_entry_timeout_min", 10)
            _seed("bt_allow_long", True)
            _seed("bt_allow_short", False)
            _seed("bt_trade_direction", "Long only")
            _seed("bt_use_indicator_consensus", True)
            _seed("bt_lock_atr_on_entry", False)
            _seed("bt_use_avg_entry_for_dca_base", True)
            _seed("bt_backtest_leverage", 1)
            _seed("bt_global_dyn_pct", 0.0)
            _seed("bt_entry_order_style_label", "Maker or Cancel")
            _seed("bt_exit_order_style_label", "Maker or Cancel")
            _seed("bt_prefer_bbo_maker", bool((settings or {}).get("prefer_bbo_maker", True)))
            _seed("bt_bbo_queue_level", int((settings or {}).get("bbo_queue_level", 1) or 1))
            _seed("bt_fixed_sl_pct", 5.0)
            _seed("bt_fixed_tp_pct", 1.0)
            _seed("bt_use_atr_tp", True)
            _seed("bt_tp_atr_multiple", 10.0)
            _seed("bt_tp_atr_period", 14)
            _seed("bt_trail_activation_pct", 5.0)
            _seed("bt_trail_stop_pct", 7.0)
            _seed("bt_base_deviation_pct", 1.0)
            _seed("bt_deviation_multiplier", 2.0)
            _seed("bt_volume_multiplier", 2.0)
            _seed("bt_ma_interval_min", 120)
            _seed("bt_ma_length", 200)
            _seed("bt_bb_interval_min", 60)
            _seed("bt_macd_interval_min", 360)
            _seed("bt_rsi_interval_min", 1_440)
            _seed("bt_bb_length", 20)
            _seed("bt_rsi_length", 14)

        st.session_state[schema_key] = 2




def _reset_backtest_ui_state() -> None:
    """Clear only Backtest UI keys so defaults can re-seed on next rerun."""
    for k in list(st.session_state.keys()):
        if k.startswith("bt_"):
            st.session_state.pop(k, None)
    st.session_state.pop("data_source_mode", None)
JOB_LOCK = threading.Lock()

_STREAMLIT_WORKER_ID = f"streamlit:{os.getpid()}:{uuid.uuid4().hex[:10]}"
_JOB_TICK_LOCK = threading.Lock()
_LAST_JOB_TICK_TS = 0.0


def _truthy_env(var_name: str) -> bool:
    return (os.environ.get(var_name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _profile_enabled() -> bool:
    # Opt-in only. We keep it separate from DRAGON_DEBUG so a debug run doesn't
    # accidentally spam timings.
    if _truthy_env("DRAGON_PROFILE"):
        try:
            return bool(st.session_state.get("_profile_enabled", True))
        except Exception:
            return True
    try:
        return bool(st.session_state.get("_profile_enabled", False))
    except Exception:
        return False


def _perf_reset() -> None:
    if not _profile_enabled():
        return
    try:
        st.session_state["_perf_records"] = []
    except Exception:
        pass


def _runs_global_filters_to_extra_where(global_filters: Dict[str, Any]) -> Dict[str, Any]:
    gf = global_filters or {}
    if not bool(gf.get("enabled", True)):
        return {}
    extra: Dict[str, Any] = {}

    preset = str(gf.get("created_at_preset") or "").strip()
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    if preset and preset != "All Time":
        now = datetime.now(timezone.utc)
        today = now.date()
        if preset == "Last Week":
            start_d = today - timedelta(days=7)
        elif preset == "Last Month":
            start_d = today - timedelta(days=30)
        elif preset == "Last 3 Months":
            start_d = today - timedelta(days=90)
        elif preset == "Last 6 Months":
            start_d = today - timedelta(days=180)
        else:
            start_d = None
        if start_d is not None:
            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)

    if start_dt is None or end_dt is None:
        dr = gf.get("date_range")
        if isinstance(dr, (list, tuple)) and len(dr) == 2 and dr[0] and dr[1]:
            try:
                start_d = dr[0]
                end_d = dr[1]
                start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
                end_dt = datetime.combine(end_d, datetime.max.time(), tzinfo=timezone.utc)
            except Exception:
                start_dt = None
                end_dt = None

    if start_dt is not None:
        extra["created_at_from"] = start_dt.isoformat()
    if end_dt is not None:
        extra["created_at_to"] = end_dt.isoformat()

    symbols = gf.get("symbols")
    if isinstance(symbols, (list, tuple)) and symbols:
        extra["symbol"] = [str(s).strip() for s in symbols if str(s).strip()]

    strategies = gf.get("strategies")
    if isinstance(strategies, (list, tuple)) and strategies:
        extra["strategy_name"] = [str(s).strip() for s in strategies if str(s).strip()]

    timeframes = gf.get("timeframes")
    if isinstance(timeframes, (list, tuple)) and timeframes:
        extra["timeframe"] = [str(s).strip() for s in timeframes if str(s).strip()]

    market_types = gf.get("market_types")
    if isinstance(market_types, (list, tuple)) and market_types:
        extra["market_type"] = [str(s).strip() for s in market_types if str(s).strip()]

    if bool(gf.get("profitable_only", False)):
        extra["net_return_pct_gt"] = 0.0

    return extra


def _runs_server_filter_controls(
    prefix: str,
    *,
    include_run_type: bool,
    on_change: Optional[Callable[[], None]] = None,
    symbol_options: Optional[List[str]] = None,
    strategy_options: Optional[List[str]] = None,
    timeframe_options: Optional[List[str]] = None,
    bounds: Optional[Dict[str, Optional[float]]] = None,
) -> Dict[str, Any]:
    def _kw() -> Dict[str, Any]:
        return {"on_change": on_change} if on_change else {}

    with st.container(border=True):
        st.markdown("#### Filters")
        row1 = st.columns(4)
        search = row1[0].text_input(
            "Search",
            help="Matches run id, symbol, strategy, or sweep name",
            key=f"{prefix}_search",
            **_kw(),
        )

        symbol_vals: List[str] = []
        if isinstance(symbol_options, list) and symbol_options:
            symbol_vals = row1[1].multiselect(
                "Symbol",
                options=symbol_options,
                default=st.session_state.get(f"{prefix}_symbol_multi", []),
                key=f"{prefix}_symbol_multi",
            )
        else:
            _ = row1[1].text_input("Symbol contains", key=f"{prefix}_symbol_contains", **_kw())

        timeframe_vals: List[str] = []
        if isinstance(timeframe_options, list) and timeframe_options:
            timeframe_vals = row1[2].multiselect(
                "Timeframe",
                options=timeframe_options,
                default=st.session_state.get(f"{prefix}_timeframe_multi", []),
                key=f"{prefix}_timeframe_multi",
            )
        else:
            _ = row1[2].text_input("Timeframe contains", key=f"{prefix}_timeframe_contains", **_kw())

        direction_val = row1[3].selectbox(
            "Direction",
            options=["All", "Long", "Short"],
            index=["All", "Long", "Short"].index(
                str(st.session_state.get(f"{prefix}_direction", "All") or "All")
                if str(st.session_state.get(f"{prefix}_direction", "All") or "All") in {"All", "Long", "Short"}
                else "All"
            ),
            key=f"{prefix}_direction",
        )

        row2 = st.columns(4)
        strategy_vals: List[str] = []
        if isinstance(strategy_options, list) and strategy_options:
            strategy_vals = row2[0].multiselect(
                "Strategy",
                options=strategy_options,
                default=st.session_state.get(f"{prefix}_strategy_multi", []),
                key=f"{prefix}_strategy_multi",
            )
        else:
            _ = row2[0].text_input("Strategy contains", key=f"{prefix}_strategy_contains", **_kw())

        bounds = bounds or {}
        roi_min_bound = bounds.get("roi_pct_on_margin_min")
        roi_max_bound = bounds.get("roi_pct_on_margin_max")
        if roi_min_bound is not None and roi_max_bound is not None:
            roi_min = row2[1].slider(
                "ROI % (min)",
                min_value=float(roi_min_bound),
                max_value=float(roi_max_bound),
                value=float(st.session_state.get(f"{prefix}_roi_min", roi_min_bound)),
                step=1.0,
                key=f"{prefix}_roi_min",
            )
        else:
            roi_min = row2[1].text_input("ROI % (min)", key=f"{prefix}_roi_min", **_kw())

        duration_choice = row2[2].selectbox(
            "Duration",
            options=["All", "1W", "1M", "3M", "6M", "12M"],
            index=["All", "1W", "1M", "3M", "6M", "12M"].index(
                str(st.session_state.get(f"{prefix}_duration_preset", "All") or "All")
                if str(st.session_state.get(f"{prefix}_duration_preset", "All") or "All")
                in {"All", "1W", "1M", "3M", "6M", "12M"}
                else "All"
            ),
            key=f"{prefix}_duration_preset",
        )

        run_type = "All"
        if include_run_type:
            run_type = row2[3].radio(
                "Run type",
                options=["All", "Single", "Sweep"],
                index=["All", "Single", "Sweep"].index(
                    str(st.session_state.get(f"{prefix}_run_type", "All") or "All")
                    if str(st.session_state.get(f"{prefix}_run_type", "All") or "All") in {"All", "Single", "Sweep"}
                    else "All"
                ),
                key=f"{prefix}_run_type",
                horizontal=True,
            )
        else:
            row2[3].markdown("&nbsp;", unsafe_allow_html=True)

        def _clear_filters() -> None:
            for key_suffix in (
                "search",
                "symbol_contains",
                "symbol_multi",
                "strategy_contains",
                "strategy_multi",
                "timeframe_contains",
                "timeframe_multi",
                "direction",
                "roi_min",
                "duration_preset",
                "run_type",
            ):
                st.session_state.pop(f"{prefix}_{key_suffix}", None)
            st.session_state.pop(f"{prefix}_filter_model", None)
            st.session_state.pop(f"{prefix}_sort_model", None)
            if on_change:
                on_change()

        reset_cols = st.columns([1, 3])
        with reset_cols[0]:
            st.button("Clear filters", key=f"{prefix}_clear_filters", on_click=_clear_filters)

    def _as_float(raw: Any) -> Optional[float]:
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None

    extra: Dict[str, Any] = {}
    if str(search or "").strip():
        extra["search"] = str(search).strip()
    symbol_text = str(st.session_state.get(f"{prefix}_symbol_contains", "") or "").strip()
    if symbol_text:
        extra["symbol_contains"] = symbol_text
    if symbol_vals:
        extra["symbol"] = symbol_vals

    strategy_text = str(st.session_state.get(f"{prefix}_strategy_contains", "") or "").strip()
    if strategy_text:
        extra["strategy_contains"] = strategy_text
    if strategy_vals:
        extra["strategy_name"] = strategy_vals

    timeframe_text = str(st.session_state.get(f"{prefix}_timeframe_contains", "") or "").strip()
    if timeframe_text:
        extra["timeframe_contains"] = timeframe_text
    if timeframe_vals:
        extra["timeframe"] = timeframe_vals
    if str(direction_val or "").strip() and str(direction_val).lower() != "all":
        extra["direction"] = str(direction_val).strip()

    if include_run_type:
        if str(run_type).lower() == "single":
            extra["run_type"] = "single"
        elif str(run_type).lower() == "sweep":
            extra["run_type"] = "sweep"

    rmin = _as_float(roi_min)
    if rmin is not None:
        extra["roi_pct_on_margin_min"] = rmin

    if str(duration_choice or "").strip() and str(duration_choice) != "All":
        now = datetime.now(timezone.utc)
        delta_days = 7
        if duration_choice == "1M":
            delta_days = 30
        elif duration_choice == "3M":
            delta_days = 90
        elif duration_choice == "6M":
            delta_days = 180
        elif duration_choice == "12M":
            delta_days = 365
        start_dt = datetime.combine((now - timedelta(days=delta_days)).date(), datetime.min.time(), tzinfo=timezone.utc)
        end_dt = datetime.combine(now.date(), datetime.max.time(), tzinfo=timezone.utc)
        extra["created_at_from"] = start_dt.isoformat()
        extra["created_at_to"] = end_dt.isoformat()

    return extra


def _safe_json_for_display(payload: Any) -> Any:
    try:
        return json.loads(json.dumps(payload, default=str))
    except Exception:
        try:
            return {"value": str(payload)}
        except Exception:
            return ""


@contextmanager
def _perf(label: str):
    if not _profile_enabled():
        yield
        return
    start = time.perf_counter()
    try:
        yield
    finally:
        dur_ms = (time.perf_counter() - start) * 1000.0
        try:
            recs = st.session_state.get("_perf_records")
            if not isinstance(recs, list):
                recs = []
            recs.append({"label": str(label), "ms": float(dur_ms)})
            st.session_state["_perf_records"] = recs
        except Exception:
            pass


def _render_perf_panel() -> None:
    if not _profile_enabled():
        return
    try:
        recs = st.session_state.get("_perf_records")
    except Exception:
        recs = None
    if not isinstance(recs, list) or not recs:
        return
    try:
        total_ms = sum(float(r.get("ms") or 0.0) for r in recs if isinstance(r, dict))
    except Exception:
        total_ms = 0.0
    with st.sidebar.expander(f"Perf (this rerun) – {total_ms/1000.0:.2f}s", expanded=False):
        dfp = pd.DataFrame(recs)
        if not dfp.empty and {"label", "ms"}.issubset(set(dfp.columns)):
            dfp = dfp.sort_values("ms", ascending=False)
        st.dataframe(dfp, use_container_width=True, hide_index=True)


def _sweep_combo_key(keys: Sequence[str], combo: Sequence[Any]) -> str:
    """Stable identifier for a sweep combo.

    Used to resume sweeps across restarts without candle-by-candle checkpointing.
    """

    try:
        overrides = dict(zip(list(keys), list(combo)))
    except Exception:
        overrides = {}
    payload = json.dumps(overrides, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()
    return f"sha1:{digest}"


def _job_scheduler_tick(*, force: bool = False) -> Dict[str, int]:
    """Legacy in-process job recovery helper (deprecated).

    Backtests/sweeps are now executed by the dedicated `backtest_worker` process.
    This function remains only to reconcile legacy in-process jobs (requeue/cancel)
    after a Streamlit restart.
    """

    global _LAST_JOB_TICK_TS
    now_ts = time.time()
    # Throttle to avoid extra DB scans on fast reruns.
    if (not force) and (now_ts - float(_LAST_JOB_TICK_TS) < 1.0):
        return {"requeued": 0, "cancelled": 0, "submitted": 0}
    with _JOB_TICK_LOCK:
        now_ts = time.time()
        if (not force) and (now_ts - float(_LAST_JOB_TICK_TS) < 1.0):
            return {"requeued": 0, "cancelled": 0, "submitted": 0}
        _LAST_JOB_TICK_TS = now_ts

    try:
        with _job_conn() as conn:
            reconcile_counts = reconcile_inprocess_jobs_after_restart(conn, current_worker_id=_STREAMLIT_WORKER_ID)
    except Exception:
        return {"requeued": 0, "cancelled": 0, "submitted": 0}

    return {
        "requeued": int((reconcile_counts or {}).get("requeued") or 0),
        "cancelled": int((reconcile_counts or {}).get("cancelled") or 0),
        "submitted": 0,
    }


@dataclass
class DataSettings:
    data_source: str
    exchange_id: Optional[str] = None
    market_type: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    range_mode: str = "bars"
    range_params: Dict[str, Any] = field(default_factory=dict)
    initial_balance: float = 0.0
    fee_rate: float = 0.0


@dataclass
class BacktestArtifacts:
    result: BacktestResult
    candles: List[Candle]
    data_label: str
    storage_symbol: str
    storage_timeframe: str


def generate_dummy_candles(n: int = 300, start_price: float = 100.0) -> List[Candle]:
    candles: List[Candle] = []
    price = start_price
    for i in range(n):
        ts = datetime.now(timezone.utc) - timedelta(minutes=(n - i))
        drift = math.sin(i / 15.0) * 0.5
        noise = random.uniform(-0.5, 0.5)
        change = drift + noise
        open_price = price
        close_price = max(0.1, price + change)
        high_price = max(open_price, close_price) + random.uniform(0.0, 0.5)
        low_price = min(open_price, close_price) - random.uniform(0.0, 0.5)
        volume_val = random.uniform(10.0, 100.0)
        candles.append(
            Candle(
                timestamp=ts,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume_val,
            )
        )
        price = close_price
    return candles


def load_candles_for_settings(data_settings: DataSettings) -> Tuple[List[Candle], str, str, str]:
    if data_settings.data_source == "synthetic":
        count = int(data_settings.range_params.get("count", 300))
        candles = generate_dummy_candles(n=count)
        label = f"Synthetic – {len(candles)} candles"
        return candles, label, "SYNTH", "synthetic"

    params = data_settings.range_params
    candles = get_candles_with_cache(
        exchange_id=data_settings.exchange_id or "binance",
        symbol=data_settings.symbol or "BTC/USDT",
        timeframe=data_settings.timeframe or "1h",
        limit=params.get("limit"),
        since=params.get("since"),
        until=params.get("until"),
        range_mode=data_settings.range_mode,
        market_type=(data_settings.market_type or "unknown"),
    )
    label = (
        f"Crypto (CCXT) – {data_settings.exchange_id or 'binance'} "
        f"{data_settings.symbol or 'BTC/USDT'} {data_settings.timeframe or '1h'}, {len(candles)} candles"
    )
    return candles, label, data_settings.symbol or "", data_settings.timeframe or ""


def run_single_backtest(
    config: DragonAlgoConfig,
    data_settings: DataSettings,
    candles_override: Optional[List[Candle]] = None,
    metadata_override: Optional[Tuple[str, str, str]] = None,
) -> BacktestArtifacts:
    analysis_info = compute_analysis_window(config, data_settings)

    if candles_override is None:
        if data_settings.data_source == "synthetic":
            count = int(data_settings.range_params.get("count", 300))
            candles = generate_dummy_candles(n=count)
            label = f"Synthetic – {len(candles)} candles"
            storage_symbol = "SYNTH"
            storage_timeframe = "synthetic"
        else:
            params = data_settings.range_params or {}
            exchange_id = data_settings.exchange_id or "binance"
            symbol = data_settings.symbol or "BTC/USDT"
            timeframe = data_settings.timeframe or "1h"
            market_type = (data_settings.market_type or "unknown")
            limit = analysis_info.get("analysis_limit") or params.get("limit")
            try:
                _analysis_start, _display_start, _display_end, candles, _candles_display = get_run_details_candles_analysis_window(
                    data_settings=data_settings,
                    run_context=analysis_info,
                    fetch_fn=get_candles_with_cache,
                )
            except Exception as exc:
                msg = str(exc).lower()
                if "database is locked" in msg or "locked" in msg:
                    _analysis_start, _display_start, _display_end, candles, _candles_display = get_run_details_candles_analysis_window(
                        data_settings=data_settings,
                        run_context=analysis_info,
                        fetch_fn=load_ccxt_candles,
                    )
                else:
                    raise
            label = (
                f"Crypto (CCXT) – {exchange_id} "
                f"{symbol} {timeframe}, {len(candles)} candles"
            )
            storage_symbol = symbol
            storage_timeframe = timeframe
    else:
        candles = candles_override
        if metadata_override:
            label, storage_symbol, storage_timeframe = metadata_override
        else:
            label = ""
            storage_symbol = data_settings.symbol or "SYNTH"
            storage_timeframe = data_settings.timeframe or "synthetic"

    warmup_candles: List[Candle] = []
    candles_main: List[Candle] = []
    try:
        display_start_ms = analysis_info.get("display_start_ms")
        display_end_ms = analysis_info.get("display_end_ms")
        display_limit = analysis_info.get("display_limit")
        if display_start_ms is not None:
            for c in candles:
                ts = _normalize_timestamp(getattr(c, "timestamp", None))
                if ts is None:
                    continue
                ts_ms = int(ts.timestamp() * 1000)
                if ts_ms < int(display_start_ms):
                    warmup_candles.append(c)
                else:
                    if display_end_ms is None or ts_ms <= int(display_end_ms):
                        candles_main.append(c)
        elif display_limit is not None and display_limit > 0:
            candles_main = list(candles)[-int(display_limit):]
            warmup_candles = list(candles)[: max(0, len(candles) - len(candles_main))]
        if not candles_main:
            candles_main = list(candles)
    except Exception:
        warmup_candles = []
        candles_main = list(candles)

    engine = BacktestEngine(
        BacktestConfig(initial_balance=data_settings.initial_balance, fee_rate=data_settings.fee_rate)
    )
    strategy = DragonDcaAtrStrategy(config=config)
    try:
        if warmup_candles:
            strategy.prime_history(warmup_candles)
    except Exception:
        pass

    result = engine.run(candles_main, strategy)
    if not result.candles:
        result.candles = candles_main
    try:
        result.run_context = {
            **(result.run_context or {}),
            "display_start_ms": analysis_info.get("display_start_ms"),
            "display_end_ms": analysis_info.get("display_end_ms"),
            "display_limit": analysis_info.get("display_limit"),
            "analysis_start_ms": analysis_info.get("analysis_start_ms"),
            "analysis_end_ms": analysis_info.get("analysis_end_ms"),
            "analysis_limit": analysis_info.get("analysis_limit"),
            "warmup_seconds": analysis_info.get("warmup_seconds"),
            "warmup_bars": analysis_info.get("warmup_bars"),
            "base_timeframe_sec": analysis_info.get("base_timeframe_sec"),
            "analysis_range_mode": analysis_info.get("effective_range_mode"),
        }
    except Exception:
        pass
    return BacktestArtifacts(
        result=result,
        candles=candles_main,
        data_label=label,
        storage_symbol=storage_symbol,
        storage_timeframe=storage_timeframe,
    )


def fee_fraction_from_pct(pct: float) -> float:
    try:
        return max(0.0, float(pct)) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _extract_query_param(params: Dict[str, Any], key: str) -> Optional[str]:
    val = params.get(key)
    if isinstance(val, list):
        return val[0] if val else None
    return val


def _update_query_params(**kwargs: Any) -> None:
    try:
        params = st.query_params
        for k, v in kwargs.items():
            if v is None:
                if k in params:
                    del params[k]
            else:
                params[k] = str(v)
    except Exception:
        return


def _clear_selected_bot() -> None:
    st.session_state.pop("selected_bot_id", None)
    _update_query_params(page="Live Bots", view="overview", bot_id=None)


def _set_selected_bot(bot_id: Optional[int]) -> None:
    if bot_id is None:
        _clear_selected_bot()
        return
    st.session_state["selected_bot_id"] = bot_id
    _update_query_params(page="Live Bots", view="bot", bot_id=bot_id, run_id=None, sweep_id=None)


def _wkey(model_key: str) -> str:
    """Derive a widget key from a persistent model key.

    Streamlit may drop widget state for widgets that are not rendered in a run.
    We keep a persistent model key (source-of-truth) and a separate widget key
    that is rehydrated from the model when the widget re-appears.
    """

    return f"w__{model_key}"


def _sync_model_from_widget(model_key: str) -> None:
    widget_key = _wkey(model_key)
    st.session_state[model_key] = st.session_state.get(widget_key)
    if model_key.endswith("_value"):
        base_key = model_key[: -len("_value")]
        if base_key:
            st.session_state[base_key] = st.session_state.get(widget_key)


def _sync_sweep_mode_from_bool(mode_key: str, bool_key: str) -> None:
    """Sync a sweep-mode checkbox (bool) into the string mode key.

    We keep `{base}_mode` as the canonical string value ('fixed'|'sweep')
    to avoid touching downstream logic that expects strings.
    """

    widget_key = _wkey(bool_key)
    is_sweep = bool(st.session_state.get(widget_key))
    st.session_state[bool_key] = is_sweep
    st.session_state[mode_key] = "sweep" if is_sweep else "fixed"


def _seed_state(key: str, default: Any) -> None:
    if key not in st.session_state:
        st.session_state[key] = default


def _init_sweepable_state(base_key: str, *, default_value: Any, sweep_field_key: str) -> None:
    """Initialize per-field sweep UI state.

    Stored keys:
      - {base_key}_mode: 'fixed' | 'sweep'
      - {base_key}_value: fixed value
      - {base_key}_min/_max/_step OR {base_key}_list when in sweep mode

    base_key itself may exist from older UI versions; we migrate that into
    {base_key}_value for back-compat.
    """

    mode_key = f"{base_key}_mode"
    value_key = f"{base_key}_value"
    min_key = f"{base_key}_min"
    max_key = f"{base_key}_max"
    step_key = f"{base_key}_step"
    list_key = f"{base_key}_list"
    sweep_kind_key = f"{base_key}_sweep_kind"

    _seed_state(mode_key, "fixed")
    _seed_state(sweep_kind_key, "range")

    if value_key not in st.session_state:
        if base_key in st.session_state and st.session_state.get(base_key) is not None:
            st.session_state[value_key] = st.session_state.get(base_key)
        else:
            st.session_state[value_key] = default_value

    meta = SWEEPABLE_FIELDS.get(sweep_field_key) if "SWEEPABLE_FIELDS" in globals() else None
    default_values = (meta or {}).get("default_values", "") if isinstance(meta, dict) else ""

    # Seed sweep defaults.
    if meta and default_values:
        try:
            parsed_default = _parse_sweep_values(str(default_values), meta)
        except Exception:
            parsed_default = []
    else:
        parsed_default = []

    if isinstance(parsed_default, list) and parsed_default:
        if isinstance(parsed_default[0], (int, float)):
            try:
                vals = [float(v) for v in parsed_default]
                vmin = float(min(vals))
                vmax = float(max(vals))
                _seed_state(min_key, vmin)
                _seed_state(max_key, vmax)
                if len(vals) >= 2:
                    approx_step = abs(vals[1] - vals[0]) or 1.0
                else:
                    approx_step = 1.0
                _seed_state(step_key, approx_step)
                # Still seed list input so the user can switch to list mode.
                _seed_state(list_key, str(default_values or ""))
            except Exception:
                pass
        else:
            _seed_state(sweep_kind_key, "list")
            _seed_state(list_key, str(default_values))
    else:
        _seed_state(list_key, str(default_values or ""))


def render_sweepable_number(
    base_key: str,
    label: str,
    *,
    default_value: Any,
    sweep_field_key: str,
    help: Optional[str] = None,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
    step: Optional[float] = None,
    fmt: Optional[str] = None,
) -> tuple[Any, Optional[list[Any]]]:
    """Render a number input with an inline Sweep toggle."""

    _init_sweepable_state(base_key, default_value=default_value, sweep_field_key=sweep_field_key)

    mode_key = f"{base_key}_mode"
    mode_bool_key = f"{base_key}_mode__is_sweep"
    value_key = f"{base_key}_value"
    min_key = f"{base_key}_min"
    max_key = f"{base_key}_max"
    step_key = f"{base_key}_step"
    list_key = f"{base_key}_list"
    sweep_kind_key = f"{base_key}_sweep_kind"

    # Keep a stable bool alongside the canonical string mode.
    mode_val = str(st.session_state.get(mode_key) or "fixed").strip().lower()
    st.session_state[mode_bool_key] = (mode_val == "sweep")

    # Keep the sweep toggle close to the sweep inputs.
    cols = st.columns([6, 1, 11], gap="small")
    with cols[0]:
        kwargs: Dict[str, Any] = {
            "label": label,
            "value": st.session_state.get(value_key),
            "key": _wkey(value_key),
            "on_change": _sync_model_from_widget,
            "args": (value_key,),
        }
        if help is not None:
            kwargs["help"] = help
        if min_value is not None:
            kwargs["min_value"] = min_value
        if max_value is not None:
            kwargs["max_value"] = max_value
        if step is not None:
            kwargs["step"] = step
        if fmt is not None:
            kwargs["format"] = fmt
        st.number_input(**kwargs)

    with cols[1]:
        st.checkbox(
            "Toggle sweep",
            value=bool(st.session_state.get(mode_bool_key)),
            key=_wkey(mode_bool_key),
            on_change=_sync_sweep_mode_from_bool,
            args=(mode_key, mode_bool_key),
            label_visibility="collapsed",
        )

    sweep_values: Optional[list[Any]] = None
    if str(mode_val).strip().lower() == "sweep":
        with cols[2]:
            sweep_kind_options = ["Range", "List"]
            widget_key = _wkey(sweep_kind_key)
            _sanitize_widget_choice(
                st.session_state,
                widget_key,
                sweep_kind_options,
                default="Range",
            )
            _sanitize_widget_choice(
                st.session_state,
                sweep_kind_key,
                sweep_kind_options,
                default="Range",
            )
            selected_kind = st.session_state.get(widget_key) if widget_key in st.session_state else st.session_state.get(sweep_kind_key)
            kind_index = 0
            try:
                if selected_kind in sweep_kind_options:
                    kind_index = sweep_kind_options.index(selected_kind)
                elif str(selected_kind or "").strip().lower() == "list":
                    kind_index = 1
            except Exception:
                kind_index = 0

            kind = st.radio(
                "Sweep type",
                options=sweep_kind_options,
                index=kind_index,
                horizontal=True,
                key=widget_key,
                on_change=_sync_model_from_widget,
                args=(sweep_kind_key,),
                label_visibility="collapsed",
                help="Use a stepped range (min/max/step) or an explicit comma-separated list.",
            )
            kind_norm = str(kind or "Range").strip().lower()
            st.session_state[sweep_kind_key] = "list" if kind_norm == "list" else "range"

            meta = SWEEPABLE_FIELDS.get(sweep_field_key)
            if kind_norm == "list":
                raw = st.text_input(
                    "Sweep values",
                    value=str(st.session_state.get(list_key) or ""),
                    key=_wkey(list_key),
                    on_change=_sync_model_from_widget,
                    args=(list_key,),
                    label_visibility="collapsed",
                    help="Comma-separated values (e.g., 1, 2.5, 5)",
                )
                if meta:
                    sweep_values = _parse_sweep_values(str(raw or ""), meta)
                else:
                    sweep_values = [x.strip() for x in str(raw or "").split(",") if x.strip()]
            else:
                mini = st.columns(3, gap="small")
                mini[0].caption("Low")
                mini[1].caption("High")
                mini[2].caption("Step")
                mini[0].number_input(
                    "Min",
                    value=float(st.session_state.get(min_key) or 0.0),
                    key=_wkey(min_key),
                    on_change=_sync_model_from_widget,
                    args=(min_key,),
                    label_visibility="collapsed",
                )
                mini[1].number_input(
                    "Max",
                    value=float(st.session_state.get(max_key) or 0.0),
                    key=_wkey(max_key),
                    on_change=_sync_model_from_widget,
                    args=(max_key,),
                    label_visibility="collapsed",
                )
                mini[2].number_input(
                    "Step",
                    min_value=0.0,
                    value=float(st.session_state.get(step_key) or 0.0),
                    key=_wkey(step_key),
                    on_change=_sync_model_from_widget,
                    args=(step_key,),
                    label_visibility="collapsed",
                )

                try:
                    vmin = float(st.session_state.get(min_key))
                    vmax = float(st.session_state.get(max_key))
                    vstep = float(st.session_state.get(step_key))
                except (TypeError, ValueError):
                    vmin, vmax, vstep = 0.0, 0.0, 0.0

                value_type = (meta or {}).get("type", float) if isinstance(meta, dict) else float

                if vstep <= 0 or vmax < vmin:
                    sweep_values = []
                else:
                    vals: list[Any] = []
                    cur = vmin
                    # inclusive range with float tolerance
                    guard = 0
                    while cur <= vmax + 1e-12 and guard < 200_000:
                        guard += 1
                        try:
                            if value_type is int:
                                vals.append(int(round(cur)))
                            elif value_type is bool:
                                vals.append(bool(cur))
                            else:
                                vals.append(float(cur))
                        except Exception:
                            vals.append(cur)
                        cur += vstep
                    sweep_values = vals

    fixed_value = st.session_state.get(value_key)
    return fixed_value, sweep_values


def render_sweepable_select(
    base_key: str,
    label: str,
    *,
    options: list[Any],
    default_value: Any,
    sweep_field_key: str,
    help: Optional[str] = None,
    format_func: Optional[Callable[[Any], str]] = None,
) -> tuple[Any, Optional[list[Any]]]:
    """Render a selectbox with an inline Sweep toggle and comma-list sweep."""

    _init_sweepable_state(base_key, default_value=default_value, sweep_field_key=sweep_field_key)

    mode_key = f"{base_key}_mode"
    mode_bool_key = f"{base_key}_mode__is_sweep"
    value_key = f"{base_key}_value"
    list_key = f"{base_key}_list"

    mode_val = str(st.session_state.get(mode_key) or "fixed").strip().lower()
    st.session_state[mode_bool_key] = (mode_val == "sweep")

    cols = st.columns([6, 2, 10])
    with cols[0]:
        current_val = st.session_state.get(value_key)
        if current_val not in options:
            current_val = default_value if default_value in options else (options[0] if options else None)

        st.selectbox(
            label,
            options=options,
            index=(options.index(current_val) if current_val in options else 0),
            key=_wkey(value_key),
            on_change=_sync_model_from_widget,
            args=(value_key,),
            format_func=format_func,
            help=help,
        )

    with cols[1]:
        st.checkbox(
            "Toggle sweep",
            value=bool(st.session_state.get(mode_bool_key)),
            key=_wkey(mode_bool_key),
            on_change=_sync_sweep_mode_from_bool,
            args=(mode_key, mode_bool_key),
            label_visibility="collapsed",
        )

    sweep_values: Optional[list[Any]] = None
    if str(mode_val).strip().lower() == "sweep":
        with cols[2]:
            raw = st.text_input(
                "Sweep values",
                value=str(st.session_state.get(list_key) or ""),
                key=_wkey(list_key),
                on_change=_sync_model_from_widget,
                args=(list_key,),
                label_visibility="collapsed",
                help="Comma-separated values",
            )

        meta = SWEEPABLE_FIELDS.get(sweep_field_key)
        if meta:
            sweep_values = _parse_sweep_values(raw, meta)
        else:
            sweep_values = [x.strip() for x in str(raw or "").split(",") if x.strip()]

    fixed_value = st.session_state.get(value_key)
    return fixed_value, sweep_values


def _sidebar_active_button_css(active_button_key: str) -> str:
        # Deprecated: we now rely on Streamlit's built-in button styling via
        # type="primary" for reliability across browsers.
        return ""


def timeframe_to_minutes(tf: str) -> int:
    if not tf:
        return 0
    tf = tf.strip().lower()
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except ValueError:
        return 0
    if unit == "m":
        return value
    if unit == "h":
        return value * 60
    if unit == "d":
        return value * 60 * 24
    return 0


def _job_conn() -> Any:
    return open_db_connection()


def _parse_positions(raw: Any) -> Dict[str, Dict[str, Any]]:
    try:
        positions = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        positions = {}
    if not isinstance(positions, dict):
        positions = {}

    def _default_leg() -> Dict[str, Any]:
        return {
            "size": 0.0,
            "avg_entry": 0.0,
            "realized": 0.0,
            "fees": 0.0,
            "dca_fills": 0,
            "opened_at": None,
            "max_size": 0.0,
        }

    for key in ("LONG", "SHORT"):
        leg = positions.get(key) or {}
        if not isinstance(leg, dict):
            leg = {}
        default_leg = _default_leg()
        for k, v in default_leg.items():
            leg.setdefault(k, v)
        positions[key] = leg
    return positions


def _parse_positions_snapshot(raw: Any) -> Dict[str, Dict[str, Any]]:
    """Normalize WooX get_positions() payload into LONG/SHORT summary rows.

    Stores best-effort numeric fields used by the UI:
    - qty, avg_entry, mark, unrealized_pnl, realized_pnl
    """
    try:
        payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    rows = payload.get("rows") or payload.get("data") or payload.get("result") or []
    if not isinstance(rows, list):
        rows = []

    out: Dict[str, Dict[str, Any]] = {"LONG": {}, "SHORT": {}}

    def _side_norm(val: Any) -> Optional[str]:
        if val is None:
            return None
        text = str(val).strip().upper()
        if text in {"LONG", "SHORT"}:
            return text
        # WooX sometimes uses "BOTH" / empty
        return None

    for row in rows:
        if not isinstance(row, dict):
            continue
        side = _side_norm(row.get("positionSide") or row.get("position_side") or row.get("side"))
        if side not in {"LONG", "SHORT"}:
            continue

        qty = (
            _safe_float(row.get("holding"))
            or _safe_float(row.get("qty"))
            or _safe_float(row.get("quantity"))
            or _safe_float(row.get("positionQty"))
            or 0.0
        )
        avg_entry = (
            _safe_float(row.get("averageOpenPrice"))
            or _safe_float(row.get("avgOpenPrice"))
            or _safe_float(row.get("entryPrice"))
            or _safe_float(row.get("openPrice"))
            or _safe_float(row.get("avgPrice"))
        )
        mark = _safe_float(row.get("markPrice")) or _safe_float(row.get("mark"))
        unreal = (
            _safe_float(row.get("unrealizedPnl"))
            or _safe_float(row.get("unrealizedProfit"))
            or _safe_float(row.get("unrealized"))
        )
        realized = _safe_float(row.get("realizedPnl")) or _safe_float(row.get("realized"))

        out[side] = {
            "qty": float(qty or 0.0),
            "avg_entry": avg_entry,
            "mark": mark,
            "unrealized_pnl": unreal,
            "realized_pnl": realized,
            "raw": row,
        }

    return out


def _extract_raw_positions_list(snapshot: Any) -> List[Dict[str, Any]]:
    """Extract WooX get_positions rows for UI rendering.

    Supports:
      - {"positions": [...]}
      - {"rows": [...]}
      - {"data": [...]} (already list)
      - [...] (snapshot itself is list)
    """
    if snapshot is None:
        return []
    if isinstance(snapshot, str):
        try:
            snapshot = json.loads(snapshot)
        except json.JSONDecodeError:
            return []
    if isinstance(snapshot, list):
        return [r for r in snapshot if isinstance(r, dict)]
    if isinstance(snapshot, dict):
        rows = snapshot.get("positions") or snapshot.get("rows") or snapshot.get("data") or []
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def _parse_positions_normalized(raw: Any) -> Dict[str, Any]:
    try:
        payload = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except json.JSONDecodeError:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _parse_open_orders(raw: Any) -> List[Dict[str, Any]]:
    try:
        orders = json.loads(raw) if isinstance(raw, str) else (raw or [])
    except json.JSONDecodeError:
        orders = []
    if not isinstance(orders, list):
        return []
    return orders


def _find_order_price(open_orders: List[Dict[str, Any]], keywords: Tuple[str, ...]) -> Optional[float]:
    for order in open_orders:
        fields = [order.get(k) for k in ("intent", "bucket", "label", "note", "type", "order_type", "tag", "client_order_id")]
        text = " ".join([str(f).lower() for f in fields if f])
        if any(kw.lower() in text for kw in keywords):
            price = order.get("price") or order.get("limit_price") or order.get("px")
            try:
                price_val = float(price)
            except (TypeError, ValueError):
                price_val = None
            if price_val is not None and not math.isnan(price_val):
                return price_val
    return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


@st.cache_data(ttl=30)
def _load_recent_candles_for_bot(bot_row: Dict[str, Any], bars: int, *, warmup_bars: int = 0) -> pd.DataFrame:
    exchange = bot_row.get("exchange_id") or "woox"
    symbol = bot_row.get("symbol") or "PERP_BTC_USDT"
    timeframe = bot_row.get("timeframe") or "1m"
    market_type = (bot_row.get("market_type") or "perps").lower()

    try:
        limit = int(bars)
    except (TypeError, ValueError):
        limit = 500

    try:
        warm = int(warmup_bars or 0)
    except (TypeError, ValueError):
        warm = 0

    requested = max(1, limit + max(0, warm))
    # Keep UI snappy by default, but allow larger loads when warmup is explicitly requested.
    if warm <= 0:
        requested = max(300, min(requested, 800))
    else:
        # Allow larger warmups for higher-timeframe MAs (e.g., 6h MA computed from 1m candles).
        requested = max(300, min(requested, 250_000))
    limit = requested

    candles: List[Candle] = []
    ms_per_bar = max(int(timeframe_to_minutes(timeframe)) * 60_000, 60_000)

    # Prefer local cache when present; only hit ccxt if cache is empty/insufficient.
    try:
        with open_db_connection() as conn:
            _, max_ts = get_cached_coverage(conn, exchange, market_type, symbol, timeframe)
            if max_ts is not None:
                start_ms = int(max_ts) - int((limit - 1) * ms_per_bar)
                rows = load_cached_range(conn, exchange, market_type, symbol, timeframe, start_ms, int(max_ts))
                if rows and len(rows) >= min(limit, 200):
                    candles = [
                        Candle(
                            timestamp=datetime.utcfromtimestamp(r["timestamp_ms"] / 1000.0).replace(tzinfo=None),
                            open=float(r.get("open", 0.0) or 0.0),
                            high=float(r.get("high", 0.0) or 0.0),
                            low=float(r.get("low", 0.0) or 0.0),
                            close=float(r.get("close", 0.0) or 0.0),
                            volume=float(r.get("volume", 0.0) or 0.0),
                        )
                        for r in rows
                        if r.get("timestamp_ms") is not None
                    ]
    except Exception:
        candles = []

    if not candles:
        try:
            candles = load_ccxt_candles(exchange, symbol, timeframe, limit=limit, since=None, until=None, range_mode="bars")
            with open_db_connection() as conn:
                upsert_candles(conn, exchange, market_type, symbol, timeframe, candles)
        except Exception:
            candles = []

    if not candles:
        return pd.DataFrame()

    records = []
    for c in candles:
        ts = getattr(c, "timestamp", None) or getattr(c, "ts", None) or getattr(c, "time", None)
        if ts is None and isinstance(c, dict):
            ts = c.get("timestamp") or c.get("ts") or c.get("time")
        records.append(
            {
                "ts": pd.to_datetime(ts, utc=True),
                "open": float(getattr(c, "open", c.get("open", 0.0) if isinstance(c, dict) else 0.0)),
                "high": float(getattr(c, "high", c.get("high", 0.0) if isinstance(c, dict) else 0.0)),
                "low": float(getattr(c, "low", c.get("low", 0.0) if isinstance(c, dict) else 0.0)),
                "close": float(getattr(c, "close", c.get("close", 0.0) if isinstance(c, dict) else 0.0)),
                "volume": float(getattr(c, "volume", c.get("volume", 0.0) if isinstance(c, dict) else 0.0)),
            }
        )
    df = pd.DataFrame(records).dropna(subset=["ts"]).sort_values("ts")
    return df


def _build_bot_price_chart(
    bot_row: Dict[str, Any],
    candles_df: pd.DataFrame,
    fills: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
    positions: Dict[str, Dict[str, Any]],
    *,
    overlay_prices: Optional[Dict[str, Optional[float]]] = None,
    autozoom_y_on_x_zoom: bool = True,
    warmup_candles_df: Optional[pd.DataFrame] = None,
) -> Optional[go.Figure]:
    if candles_df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=candles_df["ts"],
            open=candles_df["open"],
            high=candles_df["high"],
            low=candles_df["low"],
            close=candles_df["close"],
            name="Price",
            increasing_line_color="#26a69a",
            decreasing_line_color="#ef5350",
            showlegend=False,
        )
    )

    # Strategy MA overlays (from bot config).
    try:
        cfg_dict: Optional[Dict[str, Any]] = None
        raw_cfg = bot_row.get("config_json")
        if isinstance(raw_cfg, str) and raw_cfg.strip():
            cfg_dict = json.loads(raw_cfg)
        elif isinstance(raw_cfg, dict):
            cfg_dict = raw_cfg
        ma_interval_min, ma_type, ma_periods = _extract_trend_ma_settings_from_config_dict(cfg_dict)
        bot_id_val = bot_row.get("id")
        bot_id_str = str(int(bot_id_val)) if bot_id_val is not None else "unknown"
        tf = str(bot_row.get("timeframe") or "")
        _add_ma_overlays_to_candles_fig(
            fig,
            x=candles_df["ts"],
            closes=candles_df["close"],
            ts=candles_df["ts"],
            scope_key=f"bot:{bot_id_str}",
            timeframe=tf,
            ma_periods=ma_periods,
            ma_interval_min=ma_interval_min,
            ma_type=ma_type,
            warmup_ts=(warmup_candles_df["ts"] if warmup_candles_df is not None and "ts" in warmup_candles_df.columns else None),
            warmup_closes=(
                warmup_candles_df["close"]
                if warmup_candles_df is not None and "close" in warmup_candles_df.columns
                else None
            ),
        )
    except Exception:
        pass

    if fills:
        df_fills = pd.DataFrame(fills)
        df_fills["event_ts"] = pd.to_datetime(df_fills["event_ts"], errors="coerce", utc=True)
        if "order_action" in df_fills:
            buys = df_fills[df_fills["order_action"].astype(str).str.lower() == "buy"]
            sells = df_fills[df_fills["order_action"].astype(str).str.lower() == "sell"]
        else:
            buys = pd.DataFrame()
            sells = pd.DataFrame()
        if not buys.empty:
            fig.add_trace(
                go.Scatter(
                    x=buys["event_ts"],
                    y=buys["avg_fill_price"],
                    mode="markers",
                    marker=dict(color="#2e7d32", symbol="triangle-up", size=10),
                    name="Buys",
                )
            )
        if not sells.empty:
            fig.add_trace(
                go.Scatter(
                    x=sells["event_ts"],
                    y=sells["avg_fill_price"],
                    mode="markers",
                    marker=dict(color="#c62828", symbol="triangle-down", size=10),
                    name="Sells",
                )
            )

    if open_orders:
        order_points = []
        for order in open_orders:
            price = order.get("price") or order.get("limit_price") or order.get("px")
            price_val = _safe_float(price)
            # Ignore NaN / BBO-style orders without a numeric price.
            if price_val is None:
                continue
            ts = order.get("updated_at") or order.get("created_at") or candles_df["ts"].iloc[-1]
            order_points.append(
                {
                    "ts": pd.to_datetime(ts, errors="coerce", utc=True),
                    "price": price_val,
                    "label": order.get("client_order_id") or order.get("external_order_id") or "order",
                }
            )
        if order_points:
            df_orders = pd.DataFrame(order_points).dropna(subset=["ts", "price"])
            fig.add_trace(
                go.Scatter(
                    x=df_orders["ts"],
                    y=df_orders["price"],
                    mode="markers",
                    marker=dict(color="#757575", symbol="x", size=9),
                    name="Open orders",
                    text=df_orders["label"],
                    hovertemplate="Order %{text}<br>Price %{y}<extra></extra>",
                )
            )

    overlay_prices = overlay_prices or {}
    avg_entry_price = overlay_prices.get("avg_entry_price")
    next_dca_price = overlay_prices.get("next_dca_price")
    take_profit_price = overlay_prices.get("take_profit_price")
    stop_loss_price = overlay_prices.get("stop_loss_price")
    if avg_entry_price is not None:
        fig.add_hline(y=avg_entry_price, line=dict(color="#0277bd", width=1, dash="dash"), annotation_text="Avg Entry", annotation_position="top left")
    if next_dca_price is not None:
        fig.add_hline(y=next_dca_price, line=dict(color="#8e24aa", width=1, dash="dot"), annotation_text="Next DCA", annotation_position="bottom right")
    if take_profit_price is not None:
        fig.add_hline(y=take_profit_price, line=dict(color="#2e7d32", width=1, dash="dot"), annotation_text="TP", annotation_position="top right")
    if stop_loss_price is not None:
        fig.add_hline(y=stop_loss_price, line=dict(color="#d32f2f", width=1, dash="dot"), annotation_text="SL", annotation_position="bottom left")

    fig.update_layout(
        height=520,
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
        yaxis_autorange=True if autozoom_y_on_x_zoom else False,
        margin=dict(l=10, r=10, t=10, b=10),
    )
    fig.update_yaxes(fixedrange=False)
    return fig


def render_live_bots_overview(bots: List[Dict[str, Any]], user_id: int) -> None:
    if not bots:
        st.info("No bots found.")
        return

    df = _build_live_bots_overview_df(bots, user_id=user_id)

    total_rows = int(len(df))
    filter_spec = {
        "search": {"type": "text", "label": "Search", "columns": ["Name", "Symbol", "Account"]},
        "Symbol": "multiselect",
        "TF": "multiselect",
        "Status": "multiselect",
        "Account": "multiselect",
        "PnL": "range",
        "Risk/Degraded": "multiselect",
    }
    with open_db_connection() as conn:
        fr = render_table_filters(
            df,
            filter_spec,
            scope_key="live_bots:overview",
            dataset_cache_key=f"bots:{total_rows}",
            presets_conn=conn,
            presets_user_id=user_id,
        )
    df = fr.df
    st.caption(f"{int(len(df))} rows shown / {total_rows} total")

    _render_live_bots_aggrid(df)


def render_live_bot_detail(bot_id: int) -> None:
    with _job_conn() as conn:
        user_id = get_or_create_user_id(get_current_user_email(), conn=conn)
        ctx = get_bot_context(conn, user_id, int(bot_id))
        bot_row = get_bot_snapshot(conn, int(bot_id))
        if not bot_row:
            bot_row = ctx.get("bot") if isinstance(ctx, dict) else None
        account_row = ctx.get("account") if isinstance(ctx, dict) else None
        if not bot_row:
            bot_row = next((b for b in list_bots(conn, limit=500) if str(b.get("id")) == str(bot_id)), None)
        app_settings = get_app_settings(conn)

    if not bot_row:
        st.warning("Bot not found. Returning to overview.")
        _clear_selected_bot()
        st.rerun()
        return

    tz = _ui_display_tz()

    def _df_local_times(df_in: pd.DataFrame) -> pd.DataFrame:
        if df_in is None or df_in.empty:
            return df_in
        df = df_in.copy()
        # Common timestamp columns across bot tables.
        for col in (
            "ts",
            "timestamp",
            "event_ts",
            "created_at",
            "updated_at",
            "opened_at",
            "closed_at",
            "heartbeat_at",
            "snapshot_updated_at",
            "lease_expires_at",
            "last_used_at",
        ):
            if col in df.columns:
                try:
                    df[col] = df[col].apply(lambda v: fmt_dt_short(v, tz))
                except Exception:
                    pass
        return df

    _set_selected_bot(bot_id)

    config_dict = None
    raw_config = bot_row.get("config_json")
    if isinstance(raw_config, str):
        try:
            config_dict = json.loads(raw_config)
        except json.JSONDecodeError:
            config_dict = None

    trade_cfg = ((config_dict or {}).get("_trade") or {}) if isinstance(config_dict, dict) else {}
    primary_leg = normalize_primary_position_side((trade_cfg or {}).get("primary_position_side") or "LONG")

    runtime_health: Dict[str, Any] = {}
    try:
        snap_health_raw = bot_row.get("snapshot_health_json")
        if isinstance(snap_health_raw, str) and snap_health_raw:
            try:
                runtime_health = json.loads(snap_health_raw) if isinstance(json.loads(snap_health_raw), dict) else {}
            except Exception:
                runtime_health = {}
        rt = (config_dict or {}).get("_runtime") if isinstance(config_dict, dict) else None
        if isinstance(rt, dict):
            if not runtime_health:
                runtime_health = (rt.get("health") if isinstance(rt.get("health"), dict) else {}) or {}
    except Exception:
        runtime_health = {}

    global_kill = bool((app_settings or {}).get("live_kill_switch_enabled", False))
    bot_risk_cfg = ((config_dict or {}).get("_risk") or {}) if isinstance(config_dict, dict) else {}
    bot_kill = bool((bot_risk_cfg or {}).get("kill_switch", False))

    positions = _parse_positions(bot_row.get("position_json"))
    positions_snapshot = _parse_positions_snapshot(bot_row.get("positions_snapshot_json"))
    positions_normalized = _parse_positions_normalized(bot_row.get("positions_normalized_json"))
    open_orders = _parse_open_orders(bot_row.get("open_orders_json"))
    desired_action = bot_row.get("desired_action")
    try:
        blocked_count = int(bot_row.get("blocked_actions_count") or 0)
    except (TypeError, ValueError):
        blocked_count = 0

    with _job_conn() as conn:
        fills = list_bot_fills(conn, int(bot_id), limit=500)
        history_rows = list_positions_history(conn, int(bot_id), limit=100)
        events = list_bot_events(conn, int(bot_id), limit=200)
        jobs_for_bot = [j for j in list_jobs(conn, limit=200) if j.get("bot_id") == bot_id]

    worker_id = None
    job_claimed_by = None
    job_lease_expires_at = None
    job_stale_reclaims = 0
    if jobs_for_bot:
        jobs_sorted = sorted(jobs_for_bot, key=lambda j: j.get("updated_at") or j.get("created_at") or "", reverse=True)
        worker_id = jobs_sorted[0].get("worker_id") or jobs_sorted[0].get("id")
        try:
            job_claimed_by = jobs_sorted[0].get("claimed_by") or jobs_sorted[0].get("worker_id")
            job_lease_expires_at = jobs_sorted[0].get("lease_expires_at")
            job_stale_reclaims = int(jobs_sorted[0].get("stale_reclaims") or 0)
        except Exception:
            job_claimed_by = job_claimed_by
            job_lease_expires_at = job_lease_expires_at
            job_stale_reclaims = 0

    realized = bot_row.get("realized_pnl", 0.0)
    unrealized = bot_row.get("unrealized_pnl", 0.0)
    total_pnl = (realized or 0.0) + (unrealized or 0.0)
    # Primary mark source is bots.mark_price (worker-persisted).
    mark_price = bot_row.get("mark_price")
    initial_equity = None
    try:
        initial_equity = float(((config_dict or {}).get("general", {}) or {}).get("initial_balance"))
    except (TypeError, ValueError):
        initial_equity = None
    roi_pct = (total_pnl / initial_equity * 100.0) if initial_equity and initial_equity != 0 else None
    roi_pct_on_margin = _safe_float(bot_row.get("roi_pct_on_margin"))

    # --- Header -------------------------------------------------------
    header = st.container()
    with header:
        left, right = st.columns([3, 2])
        with left:
            # Health icon + name
            # Determine an overall health state similar to the overview.
            now = datetime.now(timezone.utc)
            hb_at = bot_row.get("heartbeat_at")
            snap_at = bot_row.get("snapshot_updated_at")
            hb_stale = False
            try:
                with _job_conn() as _conn:
                    app_settings_local = get_app_settings(_conn)
                fast_interval = float(
                    os.environ.get("DRAGON_WORKER_FAST_INTERVAL_S")
                    or (app_settings_local or {}).get("worker_fast_interval_s")
                    or 3.0
                )
            except Exception:
                fast_interval = 3.0
            stale_threshold_s = max(2.0 * float(fast_interval) + 2.0, 6.0)
            try:
                ref_ts = hb_at or snap_at
                if ref_ts:
                    hb_dt = datetime.fromisoformat(str(ref_ts))
                    if hb_dt.tzinfo is None:
                        hb_dt = hb_dt.replace(tzinfo=timezone.utc)
                    hb_stale = (now - hb_dt).total_seconds() > stale_threshold_s
                else:
                    hb_stale = True
            except Exception:
                hb_stale = True

            last_error = (bot_row.get("snapshot_last_error") or bot_row.get("last_error") or "").strip()
            status_l = str(bot_row.get("status") or "").strip().lower()
            snap_health = (str(bot_row.get("snapshot_health_status") or "")).strip().lower()

            acct_risk = runtime_health.get("account_risk") if isinstance(runtime_health, dict) else None
            acct_blocked = bool(acct_risk.get("blocked")) if isinstance(acct_risk, dict) else False
            exchange_state_s = None
            try:
                exchange_state_s = (str((runtime_health or {}).get("exchange_state") or "")).strip().upper() or None
            except Exception:
                exchange_state_s = None
            exchange_degraded = bool((runtime_health or {}).get("exchange_degraded")) if isinstance(runtime_health, dict) else False

            health_state = "ok"
            if snap_health == "error" or status_l == "error" or last_error:
                health_state = "error"
            elif snap_health == "warn" or hb_stale or acct_blocked or exchange_degraded or global_kill or bot_kill:
                health_state = "warn"

            icon_name = TABLER_ICONS["ok"] if health_state == "ok" else (TABLER_ICONS["warn"] if health_state == "warn" else TABLER_ICONS["error"])
            icon_color = "#7ee787" if health_state == "ok" else ("#facc15" if health_state == "warn" else "#ff7b72")

            bot_title = str(bot_row.get("name") or f"Bot {bot_id}")
            try:
                icon_html = render_tabler_icon(icon_name, size_px=20, color=icon_color, variant="filled", as_img=True)
            except Exception:
                icon_html = ""
            escaped_title = html.escape(bot_title)
            st.markdown(
                """
                <style>
                  .dragon-bot-title-row { display:flex; align-items:center; gap:0.5rem; }
                  .dragon-bot-title-row svg { display:block; }
                  .dragon-bot-title { font-size: 1.35rem; font-weight: 700; line-height: 1.15; }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div class='dragon-bot-title-row'><div>{icon_html}</div><div class='dragon-bot-title'>{escaped_title}</div></div>",
                unsafe_allow_html=True,
            )
            st.caption(
                f"{bot_row.get('exchange_id') or 'n/a'} • {bot_row.get('symbol') or 'n/a'} • {bot_row.get('timeframe') or 'n/a'}"
            )
        with right:
            # Status pills (compact)
            status_val = bot_row.get("status") or ""
            desired_status_val = bot_row.get("desired_status") or ""
            kill_state = "GLOBAL" if global_kill else ("BOT" if bot_kill else "OFF")
            desired_action_disp = str(desired_action) if desired_action else "None"

            # Compact badge line (status / desired / account / risk / degraded)
            acct_label = None
            acct_status = None
            if account_row:
                acct_label = (account_row.get("label") or "").strip() or f"#{account_row.get('id')}"
                acct_status = (account_row.get("status") or "").strip().lower() or "n/a"

            badge_parts: List[str] = []
            badge_parts.append(f"Status: <b>{status_val}</b>")
            if desired_status_val and str(desired_status_val).strip().lower() != str(status_val).strip().lower():
                badge_parts.append(f"Desired: <b>{desired_status_val}</b>")
            if acct_label:
                badge_parts.append(f"Account: <b>{acct_label}</b> ({acct_status})")
            if acct_blocked:
                reason = acct_risk.get("reason") if isinstance(acct_risk, dict) else None
                reason_suffix = f" ({reason})" if reason else ""
                badge_parts.append(f"<span style='color:#facc15; font-weight:800;'>RISK BLOCKED</span>{reason_suffix}")
            if exchange_state_s and exchange_state_s != "CLOSED":
                badge_parts.append(f"<span style='color:#facc15; font-weight:800;'>EXCHANGE {exchange_state_s}</span>")
            if global_kill or bot_kill:
                badge_parts.append(f"<span style='color:#ff7b72; font-weight:800;'>KILL {kill_state}</span>")
            if hb_stale:
                badge_parts.append("<span style='color:#facc15; font-weight:800;'>HEARTBEAT STALE</span>")
            badge_parts.append(f"Leg: <b>{primary_leg}</b>")

            st.markdown("<br>".join(badge_parts), unsafe_allow_html=True)
            # Lease info (best-effort; useful for multi-worker correctness debugging).
            if job_claimed_by or job_lease_expires_at or (job_stale_reclaims or 0) > 0:
                st.write(f"Claimed by: **{job_claimed_by or 'n/a'}**")
                st.write(f"Lease expires: **{fmt_dt_short(job_lease_expires_at, tz)}**")
                st.write(f"Stale reclaims: **{job_stale_reclaims}**")

            # Health signals from worker runtime
            acct_risk = runtime_health.get("account_risk") if isinstance(runtime_health, dict) else None
            if isinstance(acct_risk, dict) and bool(acct_risk.get("blocked")):
                reason = acct_risk.get("reason")
                st.write(f"Account risk: **BLOCKED**{f' ({reason})' if reason else ''}")
            else:
                st.write("Account risk: **ACTIVE**")

            hedge = runtime_health.get("hedge_mode") if isinstance(runtime_health, dict) else None
            if isinstance(hedge, dict):
                ok = bool(hedge.get("ok"))
                reason = str(hedge.get("reason") or "unknown")
                checked_at = str(hedge.get("checked_at") or "")
                checked_disp = fmt_dt_short(checked_at, tz) if checked_at else ""
                st.write(
                    f"Hedge mode: **{'PASS' if ok else reason.upper()}**" + (f" • {checked_disp}" if checked_disp else "")
                )
            st.write(f"Desired action: **{desired_action_disp}**")

            try:
                if events and isinstance(events, list):
                    last_ev = events[0]
                    if isinstance(last_ev, dict) and (last_ev.get("event_type") == "pnl_reconcile_warn"):
                        st.warning("Reconciliation: WARN")
            except Exception:
                pass

        st.button("Back to bots", on_click=_clear_selected_bot, key="detail_back_to_bots")

    st.markdown("#### Bot Chart")
    chart_controls = st.columns([1, 1, 2])
    bars_label = chart_controls[0].selectbox(
        "Bars",
        options=["300", "500", "800"],
        index=1,
        key="bot_chart_bars",
    )
    autozoom_y = chart_controls[1].toggle(
        "Auto-zoom Y on X zoom",
        value=True,
        key="bot_chart_autozoom_y",
    )
    bar_count = 500
    try:
        bar_count = int(bars_label)
    except (TypeError, ValueError):
        bar_count = 500

    # Overlay lines (best-effort): prefer explicit snapshot fields; otherwise infer.
    avg_entry_price = _safe_float(bot_row.get("avg_entry_price"))
    long_leg = positions.get("LONG") if isinstance(positions, dict) else None
    if isinstance(long_leg, dict):
        if _safe_float(long_leg.get("size")) not in (None, 0.0):
            avg_entry_price = avg_entry_price or _safe_float(long_leg.get("avg_entry"))

    next_dca_snapshot = _safe_float(bot_row.get("next_dca_price"))
    tp_snapshot = _safe_float(bot_row.get("take_profit_price"))
    sl_snapshot = _safe_float(bot_row.get("stop_loss_price"))

    overlay_prices = {
        "avg_entry_price": avg_entry_price,
        "next_dca_price": next_dca_snapshot or _safe_float(_find_order_price(open_orders, ("dca",))),
        "take_profit_price": tp_snapshot or _safe_float(_find_order_price(open_orders, ("tp", "takeprofit", "take profit"))),
        "stop_loss_price": sl_snapshot or _safe_float(_find_order_price(open_orders, ("sl", "stop", "stoploss", "stop loss"))),
    }

    # Warmup candles so MA overlays are correct from the left edge of the displayed window.
    warmup_bars = 0
    try:
        ma_interval_min, ma_type, ma_periods = _extract_trend_ma_settings_from_config_dict(config_dict)
        warmup_bars = _estimate_warmup_bars_for_trend_ma(str(bot_row.get("timeframe") or ""), ma_interval_min, ma_periods)
    except Exception:
        warmup_bars = 0

    candles_all_df = _load_recent_candles_for_bot(bot_row, bar_count, warmup_bars=warmup_bars)
    if not candles_all_df.empty and len(candles_all_df) > bar_count:
        warmup_df = candles_all_df.iloc[: max(0, len(candles_all_df) - bar_count)].copy()
        candles_df = candles_all_df.iloc[-bar_count:].copy()
    else:
        warmup_df = None
        candles_df = candles_all_df
    price_fig = _build_bot_price_chart(
        bot_row,
        candles_df,
        fills,
        open_orders,
        positions,
        overlay_prices=overlay_prices,
        autozoom_y_on_x_zoom=autozoom_y,
        warmup_candles_df=warmup_df,
    )
    if price_fig:
        st.plotly_chart(price_fig, width="stretch", key="bot_price_chart")
    else:
        st.info("Bot Chart: no candles available.")

    # --- KPIs (organized) --------------------------------------------
    fees_total = _safe_float(bot_row.get("fees_total")) or 0.0
    funding_total = _safe_float(bot_row.get("funding_total")) or 0.0
    net_after_costs = (float(realized or 0.0) + float(unrealized or 0.0)) - fees_total + funding_total
    roi_base = net_after_costs
    try:
        roi_pct = (roi_base / initial_equity * 100.0) if initial_equity and initial_equity != 0 else None
    except Exception:
        roi_pct = None

    kpi_cols = st.columns(3)
    with kpi_cols[0]:
        st.markdown("##### PnL")
        st.metric("Realized PnL", _fmt_num(realized, 2))
        st.metric("Unrealized PnL", _fmt_num(unrealized, 2))
        st.metric("Net After Costs", _fmt_num(net_after_costs, 2))
    with kpi_cols[1]:
        st.markdown("##### Costs")
        st.metric("Fees", _fmt_num(fees_total, 2))
        st.metric("Funding", _fmt_num(funding_total, 2))
        st.metric("Total Costs", _fmt_num(fees_total + funding_total, 2))
    with kpi_cols[2]:
        st.markdown("##### State")
        # Position size summary
        long_size = _safe_float(((positions or {}).get("LONG") or {}).get("size")) if isinstance(positions, dict) else None
        short_size = _safe_float(((positions or {}).get("SHORT") or {}).get("size")) if isinstance(positions, dict) else None
        if (long_size or 0.0) != 0.0 or (short_size or 0.0) != 0.0:
            st.metric("Position", f"L {long_size or 0.0:.4f} / S {short_size or 0.0:.4f}")
        else:
            st.metric("Position", "Flat")
        st.metric("Avg Entry", _fmt_num(overlay_prices.get("avg_entry_price"), 4) if overlay_prices.get("avg_entry_price") is not None else "n/a")
        st.metric("Mark", _fmt_num(mark_price, 4) if _safe_float(mark_price) is not None else "n/a")
        st.metric("ROI % (equity, net)", _fmt_num(roi_pct, 2) if roi_pct is not None else "n/a")
        st.metric("ROI % (margin, net)", _fmt_num(roi_pct_on_margin, 2) if roi_pct_on_margin is not None else "n/a")
        dca_fills = bot_row.get("dca_fills_current")
        st.caption(f"DCA fills: {str(dca_fills) if dca_fills is not None else 'n/a'}")
        st.caption(
            f"Next DCA: {_fmt_num(overlay_prices.get('next_dca_price'), 4) if overlay_prices.get('next_dca_price') is not None else 'n/a'} • "
            f"TP: {_fmt_num(overlay_prices.get('take_profit_price'), 4) if overlay_prices.get('take_profit_price') is not None else 'n/a'} • "
            f"SL: {_fmt_num(overlay_prices.get('stop_loss_price'), 4) if overlay_prices.get('stop_loss_price') is not None else 'n/a'}"
        )

    # --- Compact controls bar ----------------------------------------
    st.markdown("---")
    controls = st.columns([1, 1, 1, 3])
    if controls[0].button("Resume", key="detail_resume"):
        with _job_conn() as conn:
            update_bot(conn, int(bot_id), desired_status="running")
        st.rerun()
    if controls[1].button("Pause", key="detail_pause"):
        with _job_conn() as conn:
            update_bot(conn, int(bot_id), desired_status="paused")
        st.rerun()
    if controls[2].button("Stop", key="detail_stop"):
        with _job_conn() as conn:
            update_bot(conn, int(bot_id), desired_status="stopped")
        st.rerun()
    hb_at = bot_row.get("heartbeat_at")
    snap_at = bot_row.get("snapshot_updated_at")
    lease_note = ""
    try:
        if int(job_stale_reclaims or 0) > 0:
            lease_note = f" • ⚠ reclaimed {int(job_stale_reclaims)}x"
    except Exception:
        lease_note = ""
    controls[3].caption(
        f"Worker: {worker_id or 'n/a'} • Heartbeat: {fmt_dt_short(hb_at, tz)} • Snapshot: {fmt_dt_short(snap_at, tz)}{lease_note}"
    )

    # --- Tabs ---------------------------------------------------------
    tab_overview, tab_positions, tab_orders_fills, tab_ledger, tab_events, tab_config = st.tabs(
        ["Overview", "Positions", "Orders & Fills", "Ledger", "Events", "Config"]
    )

    with tab_overview:
        st.markdown("#### Snapshot")
        snap = st.columns(4)
        snap[0].write(f"Status: **{bot_row.get('status')}**")
        snap[0].write(f"Desired: **{bot_row.get('desired_status')}**")
        snap[1].write(f"Exchange: **{bot_row.get('exchange_id') or 'n/a'}**")
        snap[1].write(f"Symbol: **{bot_row.get('symbol') or 'n/a'}**")
        snap[2].write(f"TF: **{bot_row.get('timeframe') or 'n/a'}**")
        snap[2].write(f"Mark: **{_fmt_num(mark_price, 4)}**" if _safe_float(mark_price) is not None else "Mark: n/a")
        snap[3].write(f"Leg: **{primary_leg}**")
        snap[3].write(f"Desired action: **{desired_action or 'None'}**")
        snap[3].write(f"Blocked: **{blocked_count}**")

        st.markdown("#### Account")
        with _job_conn() as conn:
            user_id = get_or_create_user_id(get_current_user_email(), conn=conn)
            bot_user_id = (str(bot_row.get("user_id") or "")).strip() or user_id
            bot_exchange = (str(bot_row.get("exchange_id") or "woox")).strip().lower() or "woox"
            accounts_for_user = get_account_snapshots(conn, bot_user_id, exchange_id=bot_exchange, include_deleted=False)
            account_id_raw = bot_row.get("account_id")
            try:
                bot_account_id = int(account_id_raw) if account_id_raw is not None else None
            except (TypeError, ValueError):
                bot_account_id = None

        current_account = next((a for a in accounts_for_user if int(a.get("id")) == int(bot_account_id)) , None) if bot_account_id is not None else None
        if current_account:
            acct_label = (current_account.get("label") or "").strip() or f"#{current_account.get('id')}"
            st.write(f"Account: **{acct_label}**")
            st.caption(f"Status: {current_account.get('status')} • Credential ID: {current_account.get('credential_id')}")
        else:
            st.warning("No trading account set for this bot (or account not found).")
            legacy_cred = bot_row.get("credentials_id")
            if legacy_cred is not None:
                st.caption(f"Legacy credential_id on bot: {legacy_cred} (should be backfilled to an account)")

        active_accounts = [a for a in accounts_for_user if str(a.get("status") or "").strip().lower() == "active"]
        if not active_accounts:
            st.warning("No active accounts available. Create one in Tools → Accounts.")
        else:
            acct_ids = [int(a.get("id")) for a in active_accounts if a.get("id") is not None]

            def _fmt_acct(aid: int) -> str:
                row = next((a for a in active_accounts if int(a.get("id")) == int(aid)), None)
                if not row:
                    return f"#{aid}"
                label = (row.get("label") or "").strip() or f"#{aid}"
                ex = (row.get("exchange_id") or "").strip()
                return f"{label} ({ex})"

            selected_acct = st.selectbox(
                "Set/Change account",
                options=acct_ids,
                format_func=_fmt_acct,
                index=(acct_ids.index(bot_account_id) if bot_account_id in acct_ids else 0),
                key=f"bot_account_select_{bot_id}",
            )

            needs_confirm = (bot_account_id is not None) and (int(selected_acct) != int(bot_account_id))
            confirm = True
            if needs_confirm:
                confirm = st.checkbox(
                    "Confirm account change",
                    value=False,
                    key=f"bot_account_confirm_{bot_id}",
                    help="Changing account changes which stored credential the worker uses for live trading.",
                )

            if st.button("Save account", key=f"bot_account_save_{bot_id}", disabled=needs_confirm and not confirm):
                with _job_conn() as conn:
                    update_bot(conn, int(bot_id), user_id=bot_user_id, account_id=int(selected_acct), credentials_id=None)
                    add_bot_event(
                        conn,
                        int(bot_id),
                        "info",
                        "account_changed",
                        "Bot trading account updated",
                        {
                            "old_account_id": bot_account_id,
                            "new_account_id": int(selected_acct),
                            "user_id": bot_user_id,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                st.success("Account updated.")
                st.rerun()

        st.markdown("#### Cycle")
        mark_val = _safe_float(mark_price)
        next_dca = _safe_float(overlay_prices.get("next_dca_price"))
        tp_val = _safe_float(overlay_prices.get("take_profit_price"))
        sl_val = _safe_float(overlay_prices.get("stop_loss_price"))

        def _pct_from_mark(target: Optional[float]) -> Optional[float]:
            if mark_val is None or mark_val == 0 or target is None:
                return None
            return (float(target) - float(mark_val)) / float(mark_val) * 100.0

        # Open orders: active vs parked (dynamic activation).
        active_n = 0
        parked_n = 0
        for o in (open_orders or []):
            if not isinstance(o, dict):
                continue
            activation = str(o.get("activation_state") or "").strip().upper()
            if activation == "PARKED":
                parked_n += 1
            else:
                active_n += 1

        # Position summary (prefer engine snapshot fields already used elsewhere).
        long_leg = positions.get("LONG") if isinstance(positions, dict) else None
        short_leg = positions.get("SHORT") if isinstance(positions, dict) else None
        long_qty = _safe_float((long_leg or {}).get("size")) if isinstance(long_leg, dict) else None
        short_qty = _safe_float((short_leg or {}).get("size")) if isinstance(short_leg, dict) else None
        long_avg = _safe_float((long_leg or {}).get("avg_entry")) if isinstance(long_leg, dict) else None
        short_avg = _safe_float((short_leg or {}).get("avg_entry")) if isinstance(short_leg, dict) else None

        if (long_qty or 0.0) != 0.0 and (short_qty or 0.0) != 0.0:
            side_disp = "BOTH"
            qty_disp = f"L {long_qty or 0.0:.4f} / S {short_qty or 0.0:.4f}"
            avg_disp = f"L {_fmt_num(long_avg, 4)} / S {_fmt_num(short_avg, 4)}"
        elif (long_qty or 0.0) != 0.0:
            side_disp = "LONG"
            qty_disp = f"{long_qty or 0.0:.4f}"
            avg_disp = _fmt_num(long_avg, 4)
        elif (short_qty or 0.0) != 0.0:
            side_disp = "SHORT"
            qty_disp = f"{short_qty or 0.0:.4f}"
            avg_disp = _fmt_num(short_avg, 4)
        else:
            side_disp = "FLAT"
            qty_disp = "0.0000"
            avg_disp = "n/a"

        cycle_top = st.columns(4)
        cycle_top[0].metric("Side", side_disp)
        cycle_top[1].metric("Qty", qty_disp)
        cycle_top[2].metric("Avg Entry", avg_disp)
        cycle_top[3].metric("Orders", f"A {active_n} / P {parked_n}")

        cycle_targets = st.columns(3)
        cycle_targets[0].metric(
            "Next DCA",
            _fmt_num(next_dca, 4) if next_dca is not None else "n/a",
            delta=(f"{_pct_from_mark(next_dca):+.2f}%" if _pct_from_mark(next_dca) is not None else None),
        )
        cycle_targets[1].metric(
            "TP",
            _fmt_num(tp_val, 4) if tp_val is not None else "n/a",
            delta=(f"{_pct_from_mark(tp_val):+.2f}%" if _pct_from_mark(tp_val) is not None else None),
        )
        cycle_targets[2].metric(
            "SL",
            _fmt_num(sl_val, 4) if sl_val is not None else "n/a",
            delta=(f"{_pct_from_mark(sl_val):+.2f}%" if _pct_from_mark(sl_val) is not None else None),
        )

        st.markdown("#### Health")
        now_dt = datetime.now(timezone.utc)
        hb_dt = _normalize_timestamp(bot_row.get("heartbeat_at"))
        hb_age_s = (now_dt - hb_dt).total_seconds() if hb_dt is not None else None
        last_err = (bot_row.get("last_error") or "").strip()

        health_cols = st.columns(3)
        with health_cols[0]:
            if hb_age_s is None:
                st.error("Heartbeat: missing")
            elif hb_age_s <= 90:
                st.success(f"Heartbeat: {int(hb_age_s)}s ago")
            elif hb_age_s <= 300:
                st.warning(f"Heartbeat: {int(hb_age_s)}s ago")
            else:
                st.error(f"Heartbeat: {int(hb_age_s)}s ago")

        with health_cols[1]:
            if last_err:
                st.error("Last error: present")
            else:
                st.success("Last error: none")

        with health_cols[2]:
            window_s = 15 * 60
            recent_levels: List[str] = []
            for ev in (events or [])[:50]:
                ev_ts = _normalize_timestamp(ev.get("ts"))
                if ev_ts is None:
                    continue
                age_s = (now_dt - ev_ts).total_seconds()
                if age_s < 0 or age_s > window_s:
                    continue
                lvl = (ev.get("level") or "").strip().lower()
                if lvl:
                    recent_levels.append(lvl)
            if any(lvl == "error" for lvl in recent_levels):
                st.error("Recent events: error")
            elif any(lvl == "warn" for lvl in recent_levels):
                st.warning("Recent events: warnings")
            else:
                st.success("Recent events: ok")

        if last_err:
            st.error(last_err)
        elif bot_row.get("heartbeat_msg"):
            st.info(bot_row.get("heartbeat_msg"))

        st.markdown("#### Recent events")
        if events:
            st.dataframe(_df_local_times(pd.DataFrame(events[:10])), hide_index=True, width="stretch")
        else:
            st.info("No events yet.")

        st.markdown("---")
        with st.expander("Danger zone", expanded=False):
            st.write(f"Global kill switch: **{'ON' if global_kill else 'OFF'}**")
            bot_kill_key = f"bot_kill_switch_{bot_id}"
            bot_kill_new = st.toggle(
                "Bot kill switch",
                value=bot_kill,
                key=bot_kill_key,
                help=(
                    "When enabled, the live worker blocks ALL broker place/cancel actions for this bot. "
                    "This is a safety brake and will also block flatten execution while enabled."
                ),
            )
            if bool(bot_kill_new) != bool(bot_kill):
                cfg_to_save = config_dict if isinstance(config_dict, dict) else {}
                risk = cfg_to_save.setdefault("_risk", {})
                if not isinstance(risk, dict):
                    risk = {}
                    cfg_to_save["_risk"] = risk
                risk["kill_switch"] = bool(bot_kill_new)
                with _job_conn() as conn:
                    update_bot(conn, int(bot_id), config_json=json.dumps(cfg_to_save))
                    add_bot_event(
                        conn,
                        int(bot_id),
                        "warn" if bool(bot_kill_new) else "info",
                        "bot_kill_switch",
                        "Bot kill switch updated",
                        {"bot_id": int(bot_id), "enabled": bool(bot_kill_new), "timestamp": datetime.now(timezone.utc).isoformat()},
                    )
                st.rerun()

            reset_cols = st.columns([1, 3])
            if reset_cols[0].button(
                "Reset blocked counter",
                key=f"reset_blocked_{bot_id}",
                help="Sets blocked_actions_count back to 0 (diagnostic counter only).",
            ):
                with _job_conn() as conn:
                    update_bot(conn, int(bot_id), blocked_actions_count=0)
                    add_bot_event(
                        conn,
                        int(bot_id),
                        "info",
                        "blocked_counter_reset",
                        "Blocked actions counter reset",
                        {"bot_id": int(bot_id), "timestamp": datetime.now(timezone.utc).isoformat()},
                    )
                st.rerun()
            reset_cols[1].caption("Counts how often kill switch blocked broker actions.")

            st.markdown("##### Flatten Now (guarded)")
            desired_status_val = (bot_row.get("desired_status") or "").lower().strip()
            status_val = (bot_row.get("status") or "").lower().strip()
            if desired_status_val == "stopped":
                st.warning("Bot desired status is **stopped**. Flatten is blocked.")
            elif status_val == "error":
                st.warning("Bot is currently in **error** state. Flatten is still allowed, but use caution.")

            confirm_key = f"flatten_confirm_{bot_id}"
            confirm_text = st.text_input(
                "Type FLATTEN to confirm",
                key=confirm_key,
                help="This prevents accidental market-flatten requests.",
            )
            can_request_flatten = (confirm_text or "").strip().upper() == "FLATTEN"

            flatten_cols = st.columns([1, 3])
            if flatten_cols[0].button(
                "Flatten Now (guarded)",
                type="primary",
                disabled=not can_request_flatten,
                key=f"flatten_now_btn_{bot_id}",
                help="Queues a guarded flatten request. The worker will cancel orders and reduce-only close positions on the next cycle.",
            ):
                if desired_status_val == "stopped":
                    with _job_conn() as conn:
                        add_bot_event(
                            conn,
                            int(bot_id),
                            "warn",
                            "flatten_blocked_stopped",
                            "Flatten blocked: desired_status=stopped",
                            {"bot_id": int(bot_id), "desired_status": desired_status_val, "timestamp": datetime.now(timezone.utc).isoformat()},
                        )
                    st.error("Flatten blocked because the bot is stopped.")
                else:
                    set_bot_desired_action(int(bot_id), "flatten_now")
                    with _job_conn() as conn:
                        add_bot_event(
                            conn,
                            int(bot_id),
                            "warn",
                            "flatten_requested",
                            "Flatten requested by user",
                            {"bot_id": int(bot_id), "timestamp": datetime.now(timezone.utc).isoformat()},
                        )
                    st.session_state[confirm_key] = ""
                    st.success("Flatten requested. The live worker will execute it on the next cycle.")
                    st.rerun()
            flatten_cols[1].caption("Cancels open orders and market-closes any open position (reduce-only).")

    with tab_positions:
        st.markdown("#### Exchange Positions (normalized)")
        mark_val = _safe_float(mark_price)
        per_side = (positions_normalized or {}).get("per_side") if isinstance(positions_normalized, dict) else None
        per_side = per_side if isinstance(per_side, dict) else {}
        totals = (positions_normalized or {}).get("totals") if isinstance(positions_normalized, dict) else None
        totals = totals if isinstance(totals, dict) else {}

        rows: List[Dict[str, Any]] = []
        for side in ("LONG", "SHORT"):
            snap = per_side.get(side) or {}
            qty = _safe_float(snap.get("qty")) or 0.0
            avg_entry = _safe_float(snap.get("avg_entry"))
            mark_used = _safe_float(snap.get("mark"))
            if mark_used is None:
                mark_used = mark_val
            notional = _safe_float(snap.get("notional"))
            if notional is None and mark_used is not None:
                notional = abs(qty) * mark_used

            rows.append(
                {
                    "Side": side,
                    "Qty": float(qty),
                    "Avg Entry": avg_entry,
                    "Mark": mark_used,
                    "Notional": notional,
                    "Unrealized PnL": _safe_float(snap.get("unrealized_pnl")),
                    "Liquidation": _safe_float(snap.get("liquidation")),
                    "Margin": _safe_float(snap.get("margin")),
                }
            )

        rows.append(
            {
                "Side": "TOTAL",
                "Qty": float((_safe_float((per_side.get('LONG') or {}).get('qty')) or 0.0) - (_safe_float((per_side.get('SHORT') or {}).get('qty')) or 0.0)),
                "Avg Entry": None,
                "Mark": mark_val,
                "Notional": _safe_float(totals.get("notional")),
                "Unrealized PnL": _safe_float(totals.get("unrealized_pnl")),
                "Liquidation": None,
                "Margin": None,
            }
        )

        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

        st.markdown("#### Raw WooX positions snapshot")
        raw_rows = _extract_raw_positions_list(positions_snapshot)
        if raw_rows:
            show_fields = ["positionSide", "holding", "averageOpenPrice", "markPrice", "leverage", "timestamp", "symbol"]
            display_rows: List[Dict[str, Any]] = []
            for r in raw_rows[:50]:
                display_rows.append({k: r.get(k) for k in show_fields})
            st.dataframe(_df_local_times(pd.DataFrame(display_rows)), hide_index=True, width="stretch")
        else:
            st.info("No raw positions in snapshot.")

        with st.expander("Reconciliation", expanded=False):
            computed = (positions_normalized or {}).get("computed") if isinstance(positions_normalized, dict) else None
            computed = computed if isinstance(computed, dict) else {}
            st.write(f"Mark (bot): **{_fmt_num(mark_val, 6) if mark_val is not None else 'n/a'}**")
            st.write(
                f"Exchange unrealized: **{_fmt_num(totals.get('unrealized_pnl_exchange'), 4)}** • "
                f"Computed unrealized: **{_fmt_num(computed.get('unrealized_pnl'), 4)}**"
            )
            st.write(
                f"Exchange realized (if any): **{_fmt_num(totals.get('realized_pnl_exchange'), 4)}** • "
                f"Ledger realized: **{_fmt_num(bot_row.get('realized_pnl'), 4)}**"
            )
            st.write(
                f"Ledger fees: **{_fmt_num(bot_row.get('fees_total'), 4)}** • "
                f"Ledger funding: **{_fmt_num(bot_row.get('funding_total'), 4)}**"
            )
            src_fields = (positions_normalized or {}).get("source_fields_present")
            if isinstance(src_fields, list) and src_fields:
                st.caption("Source fields present: " + ", ".join([str(x) for x in src_fields[:40]]) + (" …" if len(src_fields) > 40 else ""))

        st.markdown("#### Open orders by side")
        oo_counts = {"LONG": {"active": 0, "parked": 0}, "SHORT": {"active": 0, "parked": 0}, "UNKNOWN": {"active": 0, "parked": 0}}
        for o in (open_orders or []):
            if not isinstance(o, dict):
                continue
            side = str(o.get("position_side") or "").strip().upper() or "UNKNOWN"
            if side not in oo_counts:
                side = "UNKNOWN"
            activation = str(o.get("activation_state") or "").strip().upper()
            if activation == "PARKED":
                oo_counts[side]["parked"] += 1
            else:
                oo_counts[side]["active"] += 1
        oo_rows = []
        for side in ("LONG", "SHORT", "UNKNOWN"):
            oo_rows.append(
                {
                    "Side": side,
                    "Active": int(oo_counts[side]["active"]),
                    "Parked": int(oo_counts[side]["parked"]),
                    "Total": int(oo_counts[side]["active"] + oo_counts[side]["parked"]),
                }
            )
        oo_rows.append(
            {
                "Side": "TOTAL",
                "Active": int(sum(v["active"] for v in oo_counts.values())),
                "Parked": int(sum(v["parked"] for v in oo_counts.values())),
                "Total": int(sum(v["active"] + v["parked"] for v in oo_counts.values())),
            }
        )
        st.dataframe(pd.DataFrame(oo_rows), hide_index=True, width="stretch")

        with st.expander("Strategy runtime legs (engine)", expanded=False):
            st.markdown("##### Runtime snapshot")
            position_rows: List[Dict[str, Any]] = []
            if isinstance(positions, dict):
                for side in ("LONG", "SHORT"):
                    leg = positions.get(side) or {}
                    if not isinstance(leg, dict):
                        continue
                    size = _safe_float(leg.get("size")) or 0.0
                    avg_entry = _safe_float(leg.get("avg_entry"))
                    realized_leg = _safe_float(leg.get("realized"))
                    dca_fills = leg.get("dca_fills")
                    notional = (abs(size) * mark_val) if (mark_val is not None) else None
                    unreal_leg = None
                    if mark_val is not None and avg_entry is not None and size != 0:
                        if side == "LONG":
                            unreal_leg = (mark_val - avg_entry) * size
                        else:
                            unreal_leg = (avg_entry - mark_val) * abs(size)
                    position_rows.append(
                        {
                            "Side": side,
                            "Size": size,
                            "Avg Entry": avg_entry,
                            "Notional": notional,
                            "Unrealized PnL": unreal_leg,
                            "Realized": realized_leg,
                            "DCA Fills": dca_fills,
                            "Opened At": fmt_dt_short(leg.get("opened_at"), tz),
                        }
                    )
            if position_rows:
                st.dataframe(pd.DataFrame(position_rows), hide_index=True, width="stretch")
            else:
                st.info("No runtime snapshot available.")

        st.markdown("#### Previous Positions")
        if history_rows:
            st.dataframe(_df_local_times(pd.DataFrame(history_rows)), hide_index=True, width="stretch")
        else:
            st.info("No position history yet.")

    with tab_orders_fills:
        sub_a, sub_b = st.tabs(["Open Orders", "Recent Fills"])
        with sub_a:
            if open_orders:
                st.dataframe(_df_local_times(pd.DataFrame(open_orders)), hide_index=True, width="stretch")
            else:
                st.info("No open orders.")
        with sub_b:
            if fills:
                st.dataframe(_df_local_times(pd.DataFrame(fills)), hide_index=True, width="stretch")
            else:
                st.info("No fills recorded.")

    with tab_ledger:
        totals = sum_ledger(conn, bot_id)
        l1, l2, l3 = st.columns(3)
        l1.metric("Fees (ledger)", _fmt_num(totals.get("fees_total"), 4))
        l2.metric("Funding (ledger)", _fmt_num(totals.get("funding_total"), 4))
        l3.metric("Realized (ledger)", _fmt_num(totals.get("realized_total"), 4))

        rows = list_ledger(conn, bot_id, limit=500)
        if rows:
            st.dataframe(_df_local_times(pd.DataFrame(rows)), hide_index=True, width="stretch")
        else:
            st.info("No ledger rows yet.")
    with tab_events:
        if events:
            st.dataframe(_df_local_times(pd.DataFrame(events)), hide_index=True, width="stretch")
        else:
            st.info("No events yet.")

    with tab_config:
        if not config_dict:
            st.info("No config snapshot found for this bot.")
        else:
            # Runtime-safe config controls
            st.markdown("#### Sizing")
            general_cfg = config_dict.setdefault("general", {})
            if not isinstance(general_cfg, dict):
                general_cfg = {}
                config_dict["general"] = general_cfg

            mode_cur = _safe_initial_entry_sizing_mode(general_cfg.get("initial_entry_sizing_mode")).value
            mode_new = st.radio(
                "Initial entry sizing",
                options=[InitialEntrySizingMode.PCT_BALANCE.value, InitialEntrySizingMode.FIXED_USD.value],
                index=0 if mode_cur == InitialEntrySizingMode.PCT_BALANCE.value else 1,
                key=f"bot_init_entry_mode_{bot_id}",
                format_func=lambda v: "% of portfolio" if v == InitialEntrySizingMode.PCT_BALANCE.value else "Fixed $ amount",
            )

            if str(mode_new) == InitialEntrySizingMode.FIXED_USD.value:
                init_entry_usd_cur = _safe_float(general_cfg.get("initial_entry_fixed_usd"))
                init_entry_usd_new = st.number_input(
                    "Initial entry ($ notional)",
                    min_value=0.0,
                    step=10.0,
                    value=float(init_entry_usd_cur) if init_entry_usd_cur is not None else 100.0,
                    key=f"bot_init_entry_usd_{bot_id}",
                )
                init_entry_pct_new = float(_safe_float(general_cfg.get("initial_entry_balance_pct")) or 10.0)
            else:
                init_entry_pct_cur = _safe_float(general_cfg.get("initial_entry_balance_pct"))
                init_entry_pct_new = st.number_input(
                    "Initial entry (% of balance)",
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    value=float(init_entry_pct_cur) if init_entry_pct_cur is not None else 10.0,
                    key=f"bot_init_entry_pct_{bot_id}",
                    help="Strategy sizes the first entry as this % of broker.balance (live uses DRAGON_LIVE_BALANCE or _risk.balance_override).",
                )
                init_entry_usd_new = float(_safe_float(general_cfg.get("initial_entry_fixed_usd")) or 100.0)

            st.markdown("#### Futures (optional)")
            futures_cfg = config_dict.setdefault("_futures", {})
            if not isinstance(futures_cfg, dict):
                futures_cfg = {}
                config_dict["_futures"] = futures_cfg
            apply_leverage = bool(futures_cfg.get("apply_leverage_on_start", False))
            leverage_val = _safe_float(futures_cfg.get("leverage"))

            colf1, colf2 = st.columns([1, 1])
            apply_new = colf1.toggle("Apply leverage on start", value=apply_leverage, key=f"apply_leverage_{bot_id}")
            lev_new = colf2.number_input(
                "Leverage",
                min_value=1,
                max_value=200,
                value=int(leverage_val) if leverage_val is not None else 10,
                step=1,
                key=f"leverage_val_{bot_id}",
            )
            risk_cfg = config_dict.setdefault("_risk", {})
            if not isinstance(risk_cfg, dict):
                risk_cfg = {}
                config_dict["_risk"] = risk_cfg
            max_lev = int(_safe_float(risk_cfg.get("max_leverage")) or 50)
            max_lev_new = st.number_input(
                "Max leverage (risk clamp)",
                min_value=1,
                max_value=200,
                value=int(max_lev),
                step=1,
                key=f"max_leverage_{bot_id}",
            )

            if st.button("Save config (sizing/futures/risk)", key=f"save_futures_{bot_id}"):
                general_cfg["initial_entry_sizing_mode"] = str(mode_new)
                general_cfg["initial_entry_balance_pct"] = float(init_entry_pct_new)
                general_cfg["initial_entry_fixed_usd"] = float(init_entry_usd_new)
                futures_cfg["apply_leverage_on_start"] = bool(apply_new)
                futures_cfg["leverage"] = int(lev_new)
                risk_cfg["max_leverage"] = int(max_lev_new)
                with _job_conn() as conn:
                    update_bot(conn, int(bot_id), config_json=json.dumps(config_dict))
                    add_bot_event(
                        conn,
                        int(bot_id),
                        "info",
                        "config_updated",
                        "Updated sizing/futures/risk settings",
                        {
                            "bot_id": int(bot_id),
                            "initial_entry_sizing_mode": str(mode_new),
                            "initial_entry_balance_pct": float(init_entry_pct_new),
                            "initial_entry_fixed_usd": float(init_entry_usd_new),
                            "apply_leverage_on_start": bool(apply_new),
                            "leverage": int(lev_new),
                            "max_leverage": int(max_lev_new),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                    )
                st.success("Saved.")
                st.rerun()

            st.markdown("#### Full config")
            st.json(config_dict)


def enqueue_sweep_job(payload: Dict[str, Any]) -> Dict[str, int]:
    """Enqueue child `backtest_run` jobs for a sweep (planner-only).

    Streamlit no longer executes sweeps in-process.
    """

    sweep_id = payload.get("sweep_id")
    if sweep_id is None:
        raise ValueError("enqueue_sweep_job requires payload['sweep_id']")

    base_cfg = _dragon_config_from_snapshot(payload.get("base_config") or {})
    base_ds = DataSettings(**(payload.get("data_settings") or {}))
    combos = payload.get("combos") or []
    keys = payload.get("keys") or []

    sweep_assets_raw = payload.get("sweep_assets")
    sweep_assets: list[str] = []
    if isinstance(sweep_assets_raw, list):
        sweep_assets = [str(s).strip() for s in sweep_assets_raw if str(s).strip()]
    if not sweep_assets:
        sym0 = str(getattr(base_ds, "symbol", "") or "").strip()
        if sym0:
            sweep_assets = [sym0]

    metadata_common = payload.get("metadata") or {}
    if not isinstance(metadata_common, dict):
        metadata_common = {}

    jobs_to_enqueue: list[tuple[str, Dict[str, Any], Optional[str]]] = []
    for sym in (sweep_assets or [""]):
        sym_norm = str(sym or "").strip()
        for combo_index, combo in enumerate(combos or [], start=1):
            try:
                overrides = dict(zip(list(keys), list(combo)))
            except Exception:
                overrides = {}

            param_key = _sweep_combo_key(keys, combo)
            pair_key = f"{sym_norm}|{param_key}" if sym_norm else param_key

            cfg = deepcopy(base_cfg)
            ds_variant = deepcopy(base_ds)
            if sym_norm:
                ds_variant.symbol = sym_norm
            for field_key, value in (overrides or {}).items():
                _apply_sweep_override(cfg, ds_variant, str(field_key), value)

            cfg_snapshot = _jsonify(cfg)
            if not isinstance(cfg_snapshot, dict):
                try:
                    cfg_snapshot = asdict(cfg)
                except Exception:
                    cfg_snapshot = {}

            ds_snapshot = asdict(ds_variant)

            md = dict(metadata_common)
            md.update(
                {
                    "sweep_combo_index": int(combo_index),
                    "sweep_combo_key": str(pair_key),
                    "sweep_overrides": overrides,
                    "symbol": str(ds_snapshot.get("symbol") or "").strip() or None,
                    "timeframe": str(ds_snapshot.get("timeframe") or "").strip() or None,
                }
            )

            job_payload = {
                "sweep_id": str(sweep_id),
                "config": cfg_snapshot,
                "data_settings": ds_snapshot,
                "metadata": md,
            }

            job_key = (
                f"backtest_run:sweep:{str(sweep_id)}:{pair_key}:"
                f"{str(ds_snapshot.get('data_source') or '')}:"
                f"{str(ds_snapshot.get('exchange_id') or '')}:"
                f"{str(ds_snapshot.get('market_type') or '')}:"
                f"{str(ds_snapshot.get('symbol') or '')}:"
                f"{str(ds_snapshot.get('timeframe') or '')}"
            )
            jobs_to_enqueue.append((job_key, job_payload, str(sweep_id)))

    with open_db_connection() as conn:
        res = bulk_enqueue_backtest_run_jobs(conn, jobs=jobs_to_enqueue)
    created = int((res or {}).get("created") or 0)
    existing = int((res or {}).get("existing") or 0)
    return {"created": created, "existing": existing, "total": int(created + existing)}


def enqueue_sweep_parent_job(*, sweep_id: str, user_id: Optional[str] = None) -> int:
    """Enqueue a `sweep_parent` planner job.

    Streamlit only enqueues the parent; the backtest worker expands it into child
    `backtest_run` jobs.
    """

    sid = str(sweep_id or "").strip()
    if not sid:
        raise ValueError("enqueue_sweep_parent_job requires sweep_id")
    job_key = f"sweep_parent:{sid}"
    group_key = f"sweep:{sid}"
    payload = {"sweep_id": sid, "user_id": (str(user_id).strip() if user_id else None)}

    with _job_conn() as conn:
        job_id = create_job(
            conn,
            "sweep_parent",
            payload,
            sweep_id=sid,
            job_key=job_key,
            group_key=group_key,
            priority=200,
            is_interactive=0,
        )
        update_job(conn, int(job_id), status="queued", progress=0.0, message="Queued (awaiting backtest_worker)")
    return int(job_id)


def enqueue_backtest_job(
    payload: Dict[str, Any],
    *,
    existing_job_id: Optional[int] = None,
    is_interactive: bool = True,
) -> int:
    """Enqueue a `backtest_run` job for the dedicated backtest worker."""

    if existing_job_id is not None:
        return int(existing_job_id)

    with _job_conn() as conn:
        job_id = create_job(
            conn,
            "backtest_run",
            payload,
            sweep_id=None,
            is_interactive=(1 if bool(is_interactive) else 0),
            priority=(0 if bool(is_interactive) else 100),
        )
        update_job(conn, int(job_id), status="queued", progress=0.0, message="Queued (awaiting backtest_worker)")
    return int(job_id)


def enqueue_live_bot_job(payload: Dict[str, Any]) -> int:
    """Enqueue a `live_bot` job for the dedicated `live_worker` process."""

    with _job_conn() as conn:
        job_id = create_job(conn, "live_bot", payload, bot_id=payload.get("bot_id"))
        update_job(conn, int(job_id), status="queued", progress=0.0, message="Queued (awaiting live_worker)")
    return int(job_id)


def trades_to_dataframe(trades: List[Trade]) -> pd.DataFrame:
    records = []
    for idx, t in enumerate(trades):
        if isinstance(t, Trade):
            ts_val = t.timestamp.isoformat() if t.timestamp else None
            side_val = t.side.name if isinstance(t.side, Enum) else str(t.side)
            pnl_val = getattr(t, "pnl", getattr(t, "realized_pnl", 0.0))
            note_val = getattr(t, "note", "")
            records.append(
                {
                    "index": t.index if t.index is not None else idx,
                    "timestamp": ts_val,
                    "side": side_val,
                    "price": t.price,
                    "size": t.size,
                    "pnl": pnl_val,
                    "note": note_val,
                }
            )
        elif isinstance(t, dict):
            records.append(
                {
                    "index": t.get("index", idx),
                    "timestamp": t.get("timestamp"),
                    "side": t.get("side"),
                    "price": t.get("price"),
                    "size": t.get("size"),
                    "pnl": t.get("pnl", t.get("realized_pnl", 0.0)),
                    "note": t.get("note", ""),
                }
            )
    return pd.DataFrame(records)


def parse_trades_payload(trades_payload: Any) -> List[dict]:
    if trades_payload is None:
        return []
    if isinstance(trades_payload, list):
        return trades_payload
    if isinstance(trades_payload, str):
        try:
            data = json.loads(trades_payload)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            return []
    return []


def infer_trade_direction_label(config_payload: Any) -> str:
    """Infer display direction label (LONG/SHORT/...) from a stored config payload.

    Supports either a dict payload (preferred) or a JSON string payload.
    """

    cfg: dict[str, Any] = {}
    if isinstance(config_payload, dict):
        cfg = config_payload
    elif isinstance(config_payload, str) and config_payload.strip():
        try:
            parsed = json.loads(config_payload)
            if isinstance(parsed, dict):
                cfg = parsed
        except json.JSONDecodeError:
            cfg = {}

    general = cfg.get("general") if isinstance(cfg.get("general"), dict) else {}
    allow_long = bool(general.get("allow_long", True))
    allow_short = bool(general.get("allow_short", False))

    if allow_long and not allow_short:
        return "LONG"
    if allow_short and not allow_long:
        return "SHORT"
    if allow_long and allow_short:
        return "LONG+SHORT"
    return "—"


def build_config(form_inputs: Dict[str, Any]) -> DragonAlgoConfig:
    dyn_pct = float(form_inputs.get("global_dyn_pct", 0.0))
    dyn_entry = float(form_inputs.get("dyn_entry_pct", dyn_pct))
    dyn_dca = float(form_inputs.get("dyn_dca_pct", dyn_pct))
    dyn_tp = float(form_inputs.get("dyn_tp_pct", dyn_pct))
    general = GeneralSettings(
        max_entries=int(form_inputs.get("max_entries", 1)),
        initial_entry_balance_pct=float(form_inputs.get("initial_entry_balance_pct", 10.0) or 0.0),
        initial_entry_sizing_mode=_safe_initial_entry_sizing_mode(form_inputs.get("initial_entry_sizing_mode")),
        initial_entry_fixed_usd=float(form_inputs.get("initial_entry_fixed_usd", 0.0) or 0.0),
        use_indicator_consensus=bool(form_inputs.get("use_indicator_consensus", False)),
        entry_order_style=OrderStyle(form_inputs.get("entry_order_style", OrderStyle.MARKET)),
        entry_timeout_min=int(form_inputs.get("entry_timeout_min", 0)),
        entry_cooldown_min=int(form_inputs.get("entry_cooldown_min", 0)),
        exit_order_style=OrderStyle(form_inputs.get("exit_order_style", OrderStyle.MARKET)),
        exit_timeout_min=int(form_inputs.get("exit_timeout_min", 0)),
        allow_long=bool(form_inputs.get("allow_long", True)),
        allow_short=bool(form_inputs.get("allow_short", False)),
        lock_atr_on_entry_for_dca=bool(form_inputs.get("lock_atr_on_entry_for_dca", False)),
        use_avg_entry_for_dca_base=bool(form_inputs.get("use_avg_entry_for_dca_base", True)),
        plot_dca_levels=bool(form_inputs.get("plot_dca_levels", False)),
        dynamic_activation=DynamicActivationConfig(entry_pct=dyn_entry, dca_pct=dyn_dca, tp_pct=dyn_tp),
        prefer_bbo_maker=bool(form_inputs.get("prefer_bbo_maker", True)),
        bbo_queue_level=int(form_inputs.get("bbo_queue_level", 1)),
        use_ma_direction=bool(form_inputs.get("use_ma_direction", False)),
        ma_type=str(form_inputs.get("ma_gate_type", form_inputs.get("ma_type", "Ema")) or "Ema"),
        ma_length=int(form_inputs.get("ma_gate_length", 200) or 200),
        ma_source=str(form_inputs.get("ma_gate_source", "close") or "close"),
    )

    # Exit schema: SL mode controls initial SL + trailing activation + trailing distance.
    sl_mode_raw = str(form_inputs.get("sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value).strip().upper()
    try:
        sl_mode = StopLossMode(sl_mode_raw)
    except Exception:
        sl_mode = StopLossMode.PCT

    tp_mode_raw = str(form_inputs.get("tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value).strip().upper()
    try:
        tp_mode = TakeProfitMode(tp_mode_raw)
    except Exception:
        tp_mode = TakeProfitMode.ATR

    # Normalize mutually-exclusive exit fields based on sl_mode / tp_mode.
    atr_period = int(form_inputs.get("atr_period", form_inputs.get("tp_atr_period", 14)) or 14)
    if atr_period <= 0:
        atr_period = 14

    sl_pct = float(form_inputs.get("sl_pct", form_inputs.get("fixed_sl_pct", 0.0)) or 0.0)
    trail_activation_pct = float(form_inputs.get("trail_activation_pct", 0.0) or 0.0)
    trail_distance_pct = float(form_inputs.get("trail_distance_pct", form_inputs.get("trail_stop_pct", 0.0)) or 0.0)
    sl_atr_mult = float(form_inputs.get("sl_atr_mult", 0.0) or 0.0)
    trail_activation_atr_mult = float(form_inputs.get("trail_activation_atr_mult", 0.0) or 0.0)
    trail_distance_atr_mult = float(form_inputs.get("trail_distance_atr_mult", 0.0) or 0.0)

    tp_pct = float(form_inputs.get("tp_pct", form_inputs.get("fixed_tp_pct", 0.0)) or 0.0)
    tp_atr_mult = float(form_inputs.get("tp_atr_mult", form_inputs.get("tp_atr_multiple", 0.0)) or 0.0)

    if sl_mode == StopLossMode.ATR:
        # Ignore percent SL fields in ATR mode.
        sl_pct = 0.0
        trail_activation_pct = 0.0
        trail_distance_pct = 0.0
    else:
        # Ignore ATR SL fields in percent mode.
        sl_atr_mult = 0.0
        trail_activation_atr_mult = 0.0
        trail_distance_atr_mult = 0.0

    if tp_mode == TakeProfitMode.ATR:
        tp_pct = 0.0
    else:
        tp_atr_mult = 0.0

    exits = ExitSettings(
        sl_mode=sl_mode,
        sl_pct=sl_pct,
        trail_activation_pct=trail_activation_pct,
        trail_distance_pct=trail_distance_pct,
        atr_period=atr_period,
        sl_atr_mult=sl_atr_mult,
        trail_activation_atr_mult=trail_activation_atr_mult,
        trail_distance_atr_mult=trail_distance_atr_mult,
        tp_mode=tp_mode,
        tp_pct=tp_pct,
        tp_atr_mult=tp_atr_mult,
        tp_replace_threshold_pct=float(form_inputs.get("tp_replace_threshold_pct", 0.05)),
    )

    dca = DcaSettings(
        base_deviation_pct=float(form_inputs.get("base_deviation_pct", 0.0)),
        deviation_multiplier=float(form_inputs.get("deviation_multiplier", 1.0)),
        volume_multiplier=float(form_inputs.get("volume_multiplier", 1.0)),
    )

    trend = TrendFilterConfig(
        ma_interval_min=int(form_inputs.get("ma_interval_min", 60)),
        ma_len=int(form_inputs.get("ma_length", 14)),
        ma_type=str(form_inputs.get("ma_type", "Sma") or "Sma"),
    )

    bbands = BBandsConfig(
        interval_min=int(form_inputs.get("bb_interval_min", 60)),
        length=int(form_inputs.get("bb_length", 20)),
        dev_up=float(form_inputs.get("bb_dev_up", 2.0)),
        dev_down=float(form_inputs.get("bb_dev_down", 2.0)),
        ma_type=str(form_inputs.get("bb_ma_type", "Sma") or "Sma"),
        deviation=float(form_inputs.get("bbands_deviation", 0.2)),
        require_fcc=bool(form_inputs.get("bb_require_fcc", False)),
        reset_middle=bool(form_inputs.get("bb_reset_middle", False)),
        allow_mid_sells=bool(form_inputs.get("bb_allow_mid_sells", False)),
    )

    macd = MACDConfig(
        interval_min=int(form_inputs.get("macd_interval_min", 60)),
        fast=int(form_inputs.get("macd_fast", 12)),
        slow=int(form_inputs.get("macd_slow", 26)),
        signal=int(form_inputs.get("macd_signal", 7)),
    )

    rsi = RSIConfig(
        interval_min=int(form_inputs.get("rsi_interval_min", 60)),
        length=int(form_inputs.get("rsi_length", 14)),
        buy_level=float(form_inputs.get("rsi_buy_level", 30)),
        sell_level=float(form_inputs.get("rsi_sell_level", 70)),
    )

    return DragonAlgoConfig(
        general=general,
        exits=exits,
        dca=dca,
        trend=trend,
        bbands=bbands,
        macd=macd,
        rsi=rsi,
    )
def normalize_exchange_id(exchange_id: str) -> str:
    from project_dragon.exchange_normalization import canonical_exchange_id

    return canonical_exchange_id(exchange_id)


def exchange_id_for_ccxt(exchange_id: str) -> str:
    ex = normalize_exchange_id(exchange_id)
    return "woo" if ex == "woox" else ex


def normalize_symbol_to_asset_key(symbol: str) -> str:
    s = str(symbol or "").strip()
    if not s:
        return ""

    # Strip CCXT suffixes like :USDT
    if ":" in s:
        s = s.split(":", 1)[0]

    up = s.upper().strip()
    if up.startswith("PERP_") or up.startswith("SPOT_"):
        parts = [p for p in up.split("_") if p]
        if len(parts) >= 2:
            up = parts[1]
        else:
            up = up.replace("PERP_", "").replace("SPOT_", "")

    if "/" in up:
        base = up.split("/", 1)[0].strip()
    elif "-" in up:
        base = up.split("-", 1)[0].strip()
    elif "_" in up:
        base = up.split("_", 1)[0].strip()
    else:
        base = up

    # Handle concatenated symbols like ETHUSDT.
    if base == up and up.isalnum():
        quote_suffixes = [
            "USDT",
            "USDC",
            "BUSD",
            "USD",
            "BTC",
            "ETH",
            "BNB",
            "DAI",
            "EUR",
            "GBP",
            "JPY",
            "KRW",
            "TRY",
            "AUD",
            "CAD",
            "CHF",
        ]
        for suf in sorted(quote_suffixes, key=len, reverse=True):
            if up.endswith(suf) and len(up) > len(suf):
                base = up[: -len(suf)]
                break

    base = re.sub(r"^\d+", "", base)
    return base.strip().upper()


@st.cache_data(show_spinner=False, ttl=300)
def _lookup_symbol_icon_uri(symbol: str, exchange_id: Optional[str] = None) -> str:
    asset_key = normalize_symbol_to_asset_key(symbol)
    if not asset_key:
        return ""
    try:
        with open_db_connection() as conn:
            row = conn.execute(
                "SELECT icon_uri FROM symbols WHERE base_asset = ? AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                (asset_key,),
            ).fetchone()
            if row:
                return str(row[0] or "").strip()

            lookup_symbol = str(symbol or "").strip()
            if lookup_symbol and exchange_id:
                ex_norm = normalize_exchange_id(str(exchange_id))
                if ex_norm == "woox":
                    up = lookup_symbol.upper()
                    if up.startswith("PERP_") or up.startswith("SPOT_"):
                        try:
                            from project_dragon.data_online import woox_symbol_to_ccxt_symbol

                            lookup_symbol, _ = woox_symbol_to_ccxt_symbol(up)
                        except Exception:
                            pass

            if lookup_symbol:
                row = conn.execute(
                    "SELECT icon_uri FROM symbols WHERE exchange_symbol = ? AND icon_uri IS NOT NULL AND TRIM(icon_uri) != '' LIMIT 1",
                    (lookup_symbol,),
                ).fetchone()
                if row:
                    return str(row[0] or "").strip()
    except Exception:
        return ""
    try:
        from project_dragon.ui.crypto_icons import icon_bytes_to_data_uri, resolve_crypto_icon

        resolved = resolve_crypto_icon(asset_key)
        if resolved:
            mime, b = resolved
            return icon_bytes_to_data_uri(mime, b)
    except Exception:
        return ""
    return ""


def _dedupe_symbol_candidates(symbols: list[str], markets: Optional[dict] = None) -> list[str]:
    """Deduplicate symbol candidates with a stable preference order.

    Preference rule:
    - Keep the first occurrence unless a later duplicate is marked active.
    """

    out: list[str] = []
    best_score: dict[str, int] = {}
    markets = markets or {}
    for sym in symbols or []:
        s = str(sym or "").strip()
        if not s:
            continue
        m = markets.get(s) if isinstance(markets, dict) else None
        active = bool(m.get("active")) if isinstance(m, dict) else False
        score = 2 if active else 0
        if s not in best_score:
            best_score[s] = score
            out.append(s)
        elif score > best_score.get(s, -1):
            best_score[s] = score
            try:
                idx = out.index(s)
                out[idx] = s
            except ValueError:
                out.append(s)
    return out


def fetch_ccxt_metadata(exchange_id: str) -> tuple[dict, list[str]]:
    # NOTE: This is used to populate UI dropdowns. It must be fast and should
    # avoid heavyweight network calls whenever possible.

    ex_norm = normalize_exchange_id(exchange_id) or "woox"

    # WooX: prefer our public REST implementation (no ccxt.load_markets).
    if ex_norm == "woox":
        markets, timeframes = get_woox_market_catalog(ex_norm, ttl_sec=600)
        return markets, list(timeframes)

    if ccxt is None:
        raise RuntimeError("ccxt is not available")

    exchange_class = getattr(ccxt, exchange_id_for_ccxt(exchange_id))
    exchange = exchange_class()
    try:
        markets = exchange.load_markets()
        timeframes = getattr(exchange, "timeframes", None) or {}
    finally:
        if hasattr(exchange, "close"):
            try:
                exchange.close()
            except Exception:
                pass
    return markets, sorted(timeframes.keys())


def parse_values(text: str) -> List[float]:
    parts = [p.strip() for p in text.split(",") if p.strip()]
    values: List[float] = []
    for part in parts:
        try:
            values.append(float(part))
        except ValueError:
            st.warning(f"Could not parse '{part}' as a number; ignoring.")
    return values


def _bool_parser(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered in {"1", "true", "t", "yes", "y"}:
        return True
    if lowered in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Unrecognized boolean value '{text}'")


def _parse_sweep_values(raw: str, meta: Dict[str, Any]) -> List[Any]:
    entries = [part.strip() for part in raw.split(",") if part.strip()]
    values: List[Any] = []
    parser: Optional[Any] = meta.get("parser")
    value_type: Any = meta.get("type", str)
    for entry in entries:
        try:
            if parser:
                values.append(parser(entry))
            else:
                values.append(value_type(entry))
        except Exception:
            label = meta.get("label", meta.get("key", "parameter"))
            st.warning(f"Could not parse '{entry}' for {label}; ignoring this value.")
    return values


def _jsonify(value: Any) -> Any:
    if value is None:
        return None
    if is_dataclass(value):
        return {k: _jsonify(v) for k, v in asdict(value).items()}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonify(v) for v in value]
    if isinstance(value, tuple):
        return [_jsonify(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


SWEEPABLE_FIELDS: Dict[str, Dict[str, Any]] = {
    "general.initial_entry_balance_pct": {
        "label": "General – Initial entry (% of balance)",
        "path": ("general", "initial_entry_balance_pct"),
        "type": float,
        "default_values": "5,10,20",
    },
    "general.initial_entry_fixed_usd": {
        "label": "General – Initial entry ($ notional)",
        "path": ("general", "initial_entry_fixed_usd"),
        "type": float,
        "default_values": "25,50,100",
    },
    "general.dynamic_activation.entry_pct": {
        "label": "Dynamic activation – Entry (%)",
        "path": ("general", "dynamic_activation", "entry_pct"),
        "type": float,
        "default_values": "0.25,0.5,1.0",
    },
    "general.dynamic_activation.dca_pct": {
        "label": "Dynamic activation – DCA (%)",
        "path": ("general", "dynamic_activation", "dca_pct"),
        "type": float,
        "default_values": "0.25,0.5,1.0",
    },
    "general.dynamic_activation.tp_pct": {
        "label": "Dynamic activation – TP (%)",
        "path": ("general", "dynamic_activation", "tp_pct"),
        "type": float,
        "default_values": "0.25,0.5,1.0",
    },
    "exits.fixed_sl_pct": {
        "label": "Exit – Fixed SL (%)",
        "path": ("exits", "fixed_sl_pct"),
        "type": float,
        "default_values": "3,5,7",
    },
    "exits.fixed_tp_pct": {
        "label": "Exit – Fixed TP (%)",
        "path": ("exits", "fixed_tp_pct"),
        "type": float,
        "default_values": "1,1.5,2",
    },
    "exits.trail_activation_pct": {
        "label": "Exit – Trail activation (%)",
        "path": ("exits", "trail_activation_pct"),
        "type": float,
        "default_values": "3,5,7",
    },
    "exits.trail_stop_pct": {
        "label": "Exit – Trail stop (%)",
        "path": ("exits", "trail_stop_pct"),
        "type": float,
        "default_values": "5,6,7",
    },
    "exits.use_atr_tp": {
        "label": "Exit – Use ATR TP",
        "path": ("exits", "use_atr_tp"),
        "type": bool,
        "parser": _bool_parser,
        "default_values": "True,False",
    },
    "exits.tp_atr_multiple": {
        "label": "Exit – ATR multiple",
        "path": ("exits", "tp_atr_multiple"),
        "type": float,
        "default_values": "5,10,15",
    },
    "exits.tp_atr_period": {
        "label": "Exit – ATR period",
        "path": ("exits", "tp_atr_period"),
        "type": int,
        "default_values": "7,14,21",
    },
    "exits.sl_atr_mult": {
        "label": "Exit – SL ATR multiple",
        "path": ("exits", "sl_atr_mult"),
        "type": float,
        "default_values": "1,2,3",
    },
    "exits.trail_activation_atr_mult": {
        "label": "Exit – Trail activation (ATR×)",
        "path": ("exits", "trail_activation_atr_mult"),
        "type": float,
        "default_values": "0,1,2",
    },
    "exits.trail_distance_atr_mult": {
        "label": "Exit – Trail distance (ATR×)",
        "path": ("exits", "trail_distance_atr_mult"),
        "type": float,
        "default_values": "1,2,3",
    },
    "dca.base_deviation_pct": {
        "label": "DCA – Base deviation (%)",
        "path": ("dca", "base_deviation_pct"),
        "type": float,
        "default_values": "0.5,1.0,1.5",
    },
    "dca.deviation_multiplier": {
        "label": "DCA – Deviation multiplier",
        "path": ("dca", "deviation_multiplier"),
        "type": float,
        "default_values": "1.5,2.0,2.5",
    },
    "dca.volume_multiplier": {
        "label": "DCA – Volume multiplier",
        "path": ("dca", "volume_multiplier"),
        "type": float,
        "default_values": "1.5,2.0,2.5",
    },
    "trend.ma_interval_min": {
        "label": "Trend – MA interval (min)",
        "path": ("trend", "ma_interval_min"),
        "type": int,
        "default_values": "60,120,240",
    },
    "trend.ma_len": {
        "label": "Trend – MA length",
        "path": ("trend", "ma_len"),
        "type": int,
        "default_values": "100,200,300",
    },
    "bbands.interval_min": {
        "label": "BBands – Interval (min)",
        "path": ("bbands", "interval_min"),
        "type": int,
        "default_values": "30,60,120",
    },
    "bbands.length": {
        "label": "BBands – Length",
        "path": ("bbands", "length"),
        "type": int,
        "default_values": "12,20,30",
    },
    "macd.interval_min": {
        "label": "MACD – Interval (min)",
        "path": ("macd", "interval_min"),
        "type": int,
        "default_values": "120,240,360",
    },
    "macd.fast": {
        "label": "MACD – Fast",
        "path": ("macd", "fast"),
        "type": int,
        "default_values": "12,18,24",
    },
    "macd.slow": {
        "label": "MACD – Slow",
        "path": ("macd", "slow"),
        "type": int,
        "default_values": "26,32,40",
    },
    "macd.signal": {
        "label": "MACD – Signal",
        "path": ("macd", "signal"),
        "type": int,
        "default_values": "7,9,12",
    },
    "rsi.interval_min": {
        "label": "RSI – Interval (min)",
        "path": ("rsi", "interval_min"),
        "type": int,
        "default_values": "240,720,1440",
    },
    "rsi.length": {
        "label": "RSI – Length",
        "path": ("rsi", "length"),
        "type": int,
        "default_values": "7,14,21",
    },
}


def _apply_sweep_override(
    config: DragonAlgoConfig,
    data_settings: DataSettings,
    field_key: str,
    value: Any,
) -> None:
    meta = SWEEPABLE_FIELDS.get(field_key)
    if not meta:
        return
    target = meta.get("target", "config")
    path: Tuple[str, ...] = meta.get("path", tuple())
    if not path:
        return

    if target == "config":
        obj: Any = config
        for attr in path[:-1]:
            obj = getattr(obj, attr)
        setattr(obj, path[-1], value)
    elif target == "data_settings":
        obj = data_settings
        for attr in path[:-1]:
            obj = getattr(obj, attr)
        setattr(obj, path[-1], value)
    elif target == "range_params":
        range_params = dict(data_settings.range_params)
        final_key = path[-1]
        range_params[final_key] = value
        data_settings.range_params = range_params


def _extract_metrics(run_row: Dict[str, Any]) -> Dict[str, Any]:
    metrics = run_row.get("metrics")
    if isinstance(metrics, str):
        try:
            metrics = json.loads(metrics)
        except json.JSONDecodeError:
            metrics = {}
    if metrics is None and run_row.get("metrics_json"):
        try:
            metrics = json.loads(run_row["metrics_json"])
        except (TypeError, json.JSONDecodeError):
            metrics = {}
    return metrics or {}


def _extract_config_dict(run_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    config = run_row.get("config")
    if config is None and run_row.get("config_json"):
        try:
            config = json.loads(run_row["config_json"])
        except (TypeError, json.JSONDecodeError):
            config = None
    if config is not None and is_dataclass(config):
        return asdict(config)
    if isinstance(config, dict):
        return config
    return None


def _extract_metadata(run_row: Dict[str, Any]) -> Dict[str, Any]:
    metadata = run_row.get("metadata")
    if metadata is None and run_row.get("metadata_json"):
        try:
            metadata = json.loads(run_row["metadata_json"])
        except (TypeError, json.JSONDecodeError):
            metadata = None
    return metadata or {}


def _normalize_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 1_000_000_000_000 else value
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def build_price_orders_chart(
    result: BacktestResult,
    *,
    ma_periods: Optional[List[int]] = None,
    ma_interval_min: int = 1,
    ma_type: str = "sma",
    scope_key: str = "",
    timeframe: str = "",
    warmup_ts: Optional[pd.Series] = None,
    warmup_closes: Optional[pd.Series] = None,
    ma_calc_ts: Optional[pd.Series] = None,
    ma_calc_closes: Optional[pd.Series] = None,
) -> Optional[go.Figure]:
    if not result.candles:
        return None

    candles_df = pd.DataFrame(
        [
            {
                "timestamp": getattr(c, "timestamp", None),
                "open": getattr(c, "open", None),
                "high": getattr(c, "high", None),
                "low": getattr(c, "low", None),
                "close": getattr(c, "close", None),
            }
            for c in result.candles
        ]
    )
    if candles_df.empty:
        return None
    candles_df["timestamp"] = pd.to_datetime(candles_df["timestamp"], utc=True)

    # BacktestEngine emits keys like avg_entry_price/next_dca_price. Keep compatibility
    # with older names if present.
    overlays = {
        "avg_entry": result.extra_series.get("avg_entry_price") or result.extra_series.get("avg_entry"),
        "next_dca": result.extra_series.get("next_dca_price") or result.extra_series.get("next_dca"),
        "tp_level": result.extra_series.get("tp_level"),
        "sl_level": result.extra_series.get("sl_level"),
    }
    for key, series in overlays.items():
        if series and len(series) == len(candles_df):
            candles_df[key] = series
        else:
            candles_df[key] = [math.nan] * len(candles_df)

    # Ensure overlays are numeric; None -> NaN for proper line breaks.
    for col in ("avg_entry", "next_dca", "tp_level", "sl_level"):
        candles_df[col] = pd.to_numeric(candles_df[col], errors="coerce")

    trades_df = trades_to_dataframe(result.trades or [])
    if not trades_df.empty and "timestamp" in trades_df.columns:
        trades_df["timestamp"] = pd.to_datetime(trades_df["timestamp"], utc=True)
        trades_df["side_norm"] = trades_df["side"].astype(str).str.upper()

    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=candles_df["timestamp"],
            open=candles_df["open"],
            high=candles_df["high"],
            low=candles_df["low"],
            close=candles_df["close"],
            name="Price",
        )
    )

    # Strategy MA overlays (from run config snapshot).
    try:
        periods = list(ma_periods or [])
        if periods:
            _add_ma_overlays_to_candles_fig(
                fig,
                x=candles_df["timestamp"],
                closes=candles_df["close"],
                ts=candles_df["timestamp"],
                scope_key=str(scope_key or "run"),
                timeframe=str(timeframe or ""),
                ma_periods=periods,
                ma_interval_min=int(ma_interval_min or 1),
                ma_type=str(ma_type or "sma"),
                warmup_ts=warmup_ts,
                warmup_closes=warmup_closes,
                ma_calc_ts=ma_calc_ts,
                ma_calc_closes=ma_calc_closes,
            )
    except Exception:
        pass

    overlay_specs = [
        ("avg_entry", "Avg Entry", "#ff7f0e", "dash"),
        ("next_dca", "Next DCA", "#9467bd", "dot"),
        ("tp_level", "Take Profit", "#17becf", "dashdot"),
        ("sl_level", "Stop Loss", "#8c564b", "longdash"),
    ]
    for column, label, color, dash in overlay_specs:
        # Plot the full series with NaNs; Plotly will break the line across NaNs.
        if candles_df[column].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=candles_df["timestamp"],
                    y=candles_df[column],
                    mode="lines",
                    name=label,
                    line=dict(color=color, dash=dash),
                    connectgaps=False,
                )
            )

    if not trades_df.empty:
        long_trades = trades_df[trades_df["side_norm"].str.contains("LONG", na=False)]
        short_trades = trades_df[trades_df["side_norm"].str.contains("SHORT", na=False)]
        if not long_trades.empty:
            fig.add_trace(
                go.Scatter(
                    x=long_trades["timestamp"],
                    y=long_trades["price"],
                    mode="markers",
                    marker=dict(symbol="triangle-up", color="#2ca02c", size=10),
                    name="Long/Entry",
                    text=long_trades["note"],
                )
            )
        if not short_trades.empty:
            fig.add_trace(
                go.Scatter(
                    x=short_trades["timestamp"],
                    y=short_trades["price"],
                    mode="markers",
                    marker=dict(symbol="triangle-down", color="#d62728", size=10),
                    name="Exit/Short",
                    text=short_trades["note"],
                )
            )

    fig.update_xaxes(rangeslider_visible=False)
    fig.update_layout(height=500, xaxis_title="Time", yaxis_title="Price", showlegend=True)
    return fig


def build_equity_chart(result: BacktestResult) -> Optional[go.Figure]:
    equity = result.equity_curve or []
    if not equity:
        return None

    timestamps = result.equity_timestamps or []
    if not timestamps and result.candles:
        timestamps = [getattr(c, "timestamp", None) for c in result.candles]

    if timestamps:
        ts_series = pd.to_datetime(pd.Series(timestamps), utc=True)
    else:
        ts_series = pd.to_datetime(pd.Series(range(len(equity))), unit="m", utc=True)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=ts_series,
            y=equity,
            mode="lines",
            name="Equity",
            line=dict(color="#1f77b4"),
        )
    )
    fig.update_layout(height=400, xaxis_title="Time", yaxis_title="Equity", showlegend=False)
    return fig


def render_run_charts_from_result(
    result: BacktestResult,
    *,
    key_prefix: str = "",
    ma_periods: Optional[List[int]] = None,
    ma_interval_min: int = 1,
    ma_type: str = "sma",
    scope_key: str = "",
    timeframe: str = "",
    warmup_ts: Optional[pd.Series] = None,
    warmup_closes: Optional[pd.Series] = None,
    downsample_keep_frac: float = 1.0,
    max_points: int = 5000,
) -> None:
    result_for_charts = result
    try:
        k = float(downsample_keep_frac)
    except Exception:
        k = 1.0
    n_candles = len(result.candles or [])
    should_downsample = (0.0 < k < 1.0) or (int(max_points or 0) > 0 and n_candles > int(max_points))
    if should_downsample:
        try:
            result_for_charts = _downsample_backtest_result(result, keep_frac=k, max_points=int(max_points or 0))
        except Exception:
            result_for_charts = result

    ma_calc_ts = None
    ma_calc_closes = None
    try:
        if result.candles:
            ma_calc_ts = pd.Series([getattr(c, "timestamp", None) for c in result.candles])
            ma_calc_closes = pd.Series([getattr(c, "close", None) for c in result.candles])
    except Exception:
        ma_calc_ts = None
        ma_calc_closes = None

    equity_fig = build_equity_chart(result_for_charts)
    price_fig = build_price_orders_chart(
        result_for_charts,
        ma_periods=ma_periods,
        ma_interval_min=ma_interval_min,
        ma_type=ma_type,
        scope_key=scope_key,
        timeframe=timeframe,
        warmup_ts=warmup_ts,
        warmup_closes=warmup_closes,
        ma_calc_ts=ma_calc_ts,
        ma_calc_closes=ma_calc_closes,
    )

    if equity_fig:
        st.markdown("**Equity curve**")
        st.plotly_chart(equity_fig, width="stretch", key=f"{key_prefix}equity")
    else:
        st.info("No equity series available for this run.")

    if price_fig:
        st.markdown("**Price & orders**")
        st.plotly_chart(price_fig, width="stretch", key=f"{key_prefix}price")
    else:
        st.info("No candle data available for price chart.")


def _downsample_indices(n: int, *, keep_frac: float, max_points: int = 0) -> List[int]:
    if n <= 0:
        return []
    if n <= 2:
        return list(range(n))
    try:
        k = float(keep_frac)
    except Exception:
        k = 1.0
    mp = int(max_points or 0)
    if k >= 1.0:
        if mp > 0 and n > mp:
            k = 1.0
        else:
            return list(range(n))
    if k <= 0.0:
        return [0, n - 1]

    m = int(round(n * k))
    m = max(2, min(n, m))
    if mp > 0:
        m = max(2, min(m, mp))
    if m >= n:
        return list(range(n))

    step = (n - 1) / float(m - 1)
    idxs = [int(round(i * step)) for i in range(m)]
    idxs = sorted(set([max(0, min(n - 1, int(x))) for x in idxs]))
    if 0 not in idxs:
        idxs.insert(0, 0)
    if (n - 1) not in idxs:
        idxs.append(n - 1)

    # If rounding collapsed too many points, fill deterministically.
    if len(idxs) < m:
        have = set(idxs)
        for j in range(n):
            if j in have:
                continue
            idxs.append(j)
            have.add(j)
            if len(idxs) >= m:
                break
        idxs = sorted(idxs)
    return idxs


def _downsample_backtest_result(result: BacktestResult, *, keep_frac: float, max_points: int = 0) -> BacktestResult:
    candles = result.candles or []
    n = len(candles)
    idxs = _downsample_indices(n, keep_frac=keep_frac, max_points=int(max_points or 0))
    if not idxs or len(idxs) >= n:
        return result

    def _take(seq: Any) -> Any:
        try:
            if not isinstance(seq, list) or not seq:
                return seq
            return [seq[i] for i in idxs if 0 <= i < len(seq)]
        except Exception:
            return seq

    extra_series_ds: Dict[str, List[Optional[float]]] = {}
    try:
        for k, v in (result.extra_series or {}).items():
            if isinstance(v, list) and len(v) == n:
                extra_series_ds[str(k)] = [v[i] for i in idxs]
            else:
                extra_series_ds[str(k)] = v  # type: ignore[assignment]
    except Exception:
        extra_series_ds = dict(result.extra_series or {})

    eq_ds = result.equity_curve
    if isinstance(eq_ds, list) and len(eq_ds) == n:
        eq_ds = [eq_ds[i] for i in idxs]

    eq_ts_ds = result.equity_timestamps
    if isinstance(eq_ts_ds, list) and len(eq_ts_ds) == n:
        eq_ts_ds = [eq_ts_ds[i] for i in idxs]

    return BacktestResult(
        candles=[candles[i] for i in idxs],
        equity_curve=eq_ds if isinstance(eq_ds, list) else list(result.equity_curve or []),
        trades=list(result.trades or []),
        metrics=dict(result.metrics or {}),
        start_time=result.start_time,
        end_time=result.end_time,
        equity_timestamps=eq_ts_ds if isinstance(eq_ts_ds, list) else list(result.equity_timestamps or []),
        params=dict(result.params or {}),
        run_context=dict(result.run_context or {}),
        computed_metrics=dict(result.computed_metrics or {}),
        extra_series=extra_series_ds,
    )


def _fmt_num(value: Any, decimals: int = 2, pct: bool = False) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "—"
    if pct:
        return f"{num:.{decimals}f}%"
    return f"{num:.{decimals}f}"


# ----------------------------
# Run Details: UI formatters
# ----------------------------
# NOTE: Presentation only. Stored metric values remain unchanged.


def get_metric_color(metric_name: str, value: Any) -> str:
    """Return a color kind for a metric value.

    Kinds: green / red / amber / muted
    This is intentionally heuristic + UI-only (no business logic).
    """

    name = str(metric_name or "").strip().lower()
    v = _safe_float(value)
    if v is None:
        return "muted"

    def _as_ratio(x: float) -> float:
        # Accept both ratio (0..1) and already-percent (0..100).
        return (x * 100.0) if -1.0 <= x <= 1.0 else x

    # Outcome metrics
    if name in {"net_profit", "net_pnl", "pnl", "profit"} or "net_profit" in name or name.endswith("_pnl"):
        if v > 0:
            return "green"
        if v < 0:
            return "red"
        return "muted"

    if name in {"net_return_pct", "return_pct"} or "return" in name and "pct" in name:
        if v > 0:
            return "green"
        if v < 0:
            return "red"
        return "muted"

    if name in {"roi_pct_on_margin", "roi_pct", "roi"} or ("roi" in name and "pct" in name):
        if v > 0:
            return "green"
        if v < 0:
            return "red"
        return "muted"

    if "drawdown" in name or name in {"max_dd", "max_dd_pct", "max_drawdown", "max_drawdown_pct"}:
        dd_pct = abs(_as_ratio(float(v)))
        if dd_pct <= 10.0:
            return "green"
        if dd_pct <= 25.0:
            return "amber"
        return "red"

    # Quality metrics
    if name.startswith("sharpe"):
        if v < 0:
            return "red"
        if v >= 1.0:
            return "green"
        if v >= 0.3:
            return "amber"
        return "red"

    if "profit_factor" in name or name == "pf":
        if v >= 1.5:
            return "green"
        if v >= 1.1:
            return "amber"
        return "red"

    if "win_rate" in name or name in {"win_pct", "win_percent"}:
        wr_pct = _as_ratio(float(v))
        if wr_pct >= 55.0:
            return "green"
        if wr_pct >= 45.0:
            return "amber"
        return "red"

    if name in {"cpc_index", "common_sense_ratio"} or "cpc" in name or "common_sense" in name:
        if v > 0:
            return "green"
        if v < 0:
            return "red"
        return "muted"

    # Costs: lower is better (heuristic thresholds; absolute units vary by exchange)
    if any(x in name for x in ("fee", "fees", "fund", "funding", "commission", "slippage")):
        av = abs(float(v))
        if av <= 5.0:
            return "green"
        if av <= 25.0:
            return "amber"
        return "red"

    return "muted"


def format_pct(
    value: float | int | None,
    *,
    assume_ratio_if_leq_1: bool = True,
    decimals: int = 0,
) -> str:
    """Format a value as a percentage string.

    Rules:
    - None -> "—"
    - If assume_ratio_if_leq_1 and 0 <= value <= 1: treat as ratio and multiply by 100.
    - Otherwise treat value as already percent.
    """

    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    # Treat small-magnitude values as ratios (supports negative returns too).
    if assume_ratio_if_leq_1 and -1.0 <= v <= 1.0:
        v *= 100.0
    return f"{v:.{decimals}f}%"


def fmt_num(value: float | int | None, decimals: int = 2) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.{decimals}f}"
    except (TypeError, ValueError):
        return "—"


def fmt_int(value: float | int | None) -> str:
    if value is None:
        return "—"
    try:
        return f"{int(value)}"
    except (TypeError, ValueError):
        return "—"


def format_money(value: float | int | None, *, decimals: int = 2) -> str:
    if value is None:
        return "—"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "—"
    sign = "-" if v < 0 else ""
    v_abs = abs(v)
    return f"{sign}${v_abs:,.{decimals}f}"


def format_duration(seconds: float | int | None) -> str:
    return format_duration_dhm(seconds)


def get_value_color(metric_name: str, value: Any) -> str:
    kind = get_metric_color(metric_name, value)
    if kind == "green":
        return "#7ee787"
    if kind == "red":
        return "#ff7b72"
    if kind == "amber":
        return "#facc15"
    return "rgba(148,163,184,0.95)"


def render_metric_two_col_table(
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    *,
    key_prefix: str = "metrics",
) -> None:
    """Render a compact TradeStream-like 2-column metric list.

    Rows are dicts: {label, value, metric_name?, hint?}
    """

    def _render_side(rows: list[dict[str, Any]], side_key: str) -> str:
        out = ["<table class='dragon-metrics-table'>"]
        for i, r in enumerate(rows or []):
            label = str(r.get("label") or "").strip()
            value = str(r.get("value") or "—").strip() or "—"
            metric_name = str(r.get("metric_name") or label)
            raw_value = r.get("raw_value", None)
            hint = str(r.get("hint") or "").strip()
            info = (
                f"<span title='{html.escape(hint)}' style='margin-left:6px; color: rgba(148,163,184,0.95); font-weight: 900;'>ⓘ</span>"
                if hint
                else ""
            )
            bg = "dragon-row-bg" if (i % 2 == 0) else ""
            color = get_value_color(metric_name, raw_value)
            out.append(
                f"<tr class='{bg}'>"
                f"<td class='label'>{html.escape(label)}{info}</td>"
                f"<td class='value' style='color:{color};'>{html.escape(value)}</td>"
                f"</tr>"
            )
        out.append("</table>")
        return "".join(out)

    left_html = _render_side(left_rows, "L")
    right_html = _render_side(right_rows, "R")
    st.markdown(
        f"<div class='dragon-metrics-grid' id='{html.escape(key_prefix)}'>" f"<div>{left_html}</div>" f"<div>{right_html}</div>" f"</div>",
        unsafe_allow_html=True,
    )


def _render_kv_table(rows: list[tuple[str, str, Optional[str]]]) -> None:
    out = ["<table class='dragon-kv-table'>"]
    for i, (label, value, color) in enumerate(rows or []):
        bg = "dragon-row-bg" if (i % 2 == 0) else ""
        style = f" style='color:{color};'" if color else ""
        out.append(
            f"<tr class='{bg}'>"
            f"<td class='label'>{html.escape(str(label))}</td>"
            f"<td class='value'{style}>{html.escape(str(value))}</td>"
            f"</tr>"
        )
    out.append("</table>")
    st.markdown("".join(out), unsafe_allow_html=True)


_RUN_DETAILS_PCT_KEYS_RATIO = {
    # Stored as 0..1 ratios.
    "win_rate",
    "win_pct",
    "win_percent",
    "net_return_pct",
    "return_pct",
    "max_drawdown_pct",
    "max_dd_pct",
    "max_drawdown",
    "max_dd",
}

_RUN_DETAILS_PCT_KEYS_ALREADY_PERCENT = {
    # Stored as percent values (0..100).
    "roi_pct_on_margin",
    "roi_pct",
    "roi_percent",
    "pnl_percent",
}


def format_run_metric(key: str, value: Any) -> str:
    k = str(key or "").strip().lower()
    if k in _RUN_DETAILS_PCT_KEYS_RATIO:
        return format_pct(_safe_float(value), assume_ratio_if_leq_1=True, decimals=0)
    if k in _RUN_DETAILS_PCT_KEYS_ALREADY_PERCENT:
        return format_pct(_safe_float(value), assume_ratio_if_leq_1=False, decimals=0)
    if k in {"total_trades", "trades", "trade_count"}:
        return fmt_int(_safe_float(value))
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, str):
        s = value.strip()
        return s if s else "—"
    if isinstance(value, (int, float)):
        return fmt_num(_safe_float(value), 2)
    if isinstance(value, (dict, list, tuple)):
        try:
            s = json.dumps(value, sort_keys=True)
        except Exception:
            s = str(value)
        s = str(s or "").strip()
        if not s:
            return "—"
        return (s[:117] + "…") if len(s) > 120 else s
    s = str(value or "").strip()
    return s if s else "—"


def _debug_log_suspicious_win_rate(*, run_id: Any, win_rate: Any, trades: Any) -> None:
    """Dev-only sanity check for accidental % mis-scaling.

    If displayed win% would be <5% with >20 trades, log debug warning.
    This is intentionally not shown in user UI.
    """

    try:
        t = int(float(trades)) if trades is not None else 0
        wr = float(win_rate) if win_rate is not None else None
        if t > 20 and wr is not None and math.isfinite(wr):
            displayed_pct = (wr * 100.0) if 0.0 <= wr <= 1.0 else wr
            if 0.0 <= displayed_pct < 5.0:
                logging.getLogger(__name__).debug(
                    "Suspicious win-rate scaling in Run Details: run_id=%s win_rate_raw=%s trades=%s",
                    run_id,
                    wr,
                    t,
                )
    except Exception:
        return


def render_run_header(run_row: Dict[str, Any]) -> None:
    run_id = run_row.get("id")
    strategy = run_row.get("strategy_name", "DragonDcaAtr")
    symbol = run_row.get("symbol", "?")
    timeframe = run_row.get("timeframe", "?")

    # Best-effort timestamps: use stored start/end or metadata fallbacks.
    start_raw = run_row.get("start_time") or run_row.get("start_ts")
    end_raw = run_row.get("end_time") or run_row.get("end_ts")
    metadata = _extract_metadata(run_row)
    start_raw = start_raw or metadata.get("start_time") or metadata.get("start_ts")
    end_raw = end_raw or metadata.get("end_time") or metadata.get("end_ts")
    start_dt = _normalize_timestamp(start_raw)
    end_dt = _normalize_timestamp(end_raw)

    tz = _ui_display_tz()

    short_id = str(run_id)[:8] if run_id else ""
    start_txt = fmt_dt(start_dt, tz) if start_dt else "—"
    end_txt = fmt_dt(end_dt, tz) if end_dt else "—"
    dur_txt = (end_dt - start_dt) if (start_dt and end_dt) else "—"

    # Compact header line (replaces giant hero title)
    st.caption(f"Run {short_id} · {strategy} · {symbol} · {timeframe} · {start_txt} → {end_txt} · {dur_txt}")


def render_run_stats(run_row: Dict[str, Any]) -> None:
    metrics = _extract_metrics(run_row)
    if not metrics:
        st.info("No metrics available for this run.")
        return

    run_id = run_row.get("id")

    # Pull canonical keys with fallbacks.
    net_profit = _safe_float(metrics.get("net_profit"))
    roi_pct_on_margin = _safe_float(metrics.get("roi_pct_on_margin"))
    net_return_pct = _safe_float(metrics.get("net_return_pct", metrics.get("return_pct")))
    max_dd = _safe_float(metrics.get("max_drawdown_pct", metrics.get("max_drawdown", metrics.get("max_dd"))))
    sharpe = _safe_float(metrics.get("sharpe", metrics.get("sharpe_ratio")))
    sortino = _safe_float(metrics.get("sortino", metrics.get("sortino_ratio")))
    profit_factor = _safe_float(metrics.get("profit_factor"))
    trades = _safe_float(metrics.get("total_trades", metrics.get("trades")))
    win_rate = _safe_float(metrics.get("win_rate", metrics.get("win_pct")))
    avg_pos_time_seconds = _safe_float(metrics.get("avg_position_time_seconds", metrics.get("avg_position_time_s")))
    csr = _safe_float(metrics.get("common_sense_ratio"))
    cpc = _safe_float(metrics.get("cpc_index"))

    _debug_log_suspicious_win_rate(run_id=run_id, win_rate=win_rate, trades=trades)

    def _kpi(label: str, value: str) -> None:
        st.caption(label)
        st.markdown(f"**{value}**")

    # 2) Top KPI strip (6 columns)
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1:
        _kpi("Net profit", fmt_num(net_profit, 2))
    with k2:
        if roi_pct_on_margin is not None:
            _kpi("ROI% (margin)", format_pct(roi_pct_on_margin, assume_ratio_if_leq_1=False, decimals=0))
        elif net_return_pct is not None:
            _kpi("PnL%", format_pct(net_return_pct, assume_ratio_if_leq_1=True, decimals=0))
        else:
            _kpi("", "")
    with k3:
        _kpi("Max drawdown", format_pct(max_dd, assume_ratio_if_leq_1=True, decimals=0))
    with k4:
        _kpi("Sharpe", fmt_num(sharpe, 2))
    with k5:
        _kpi("Profit factor", fmt_num(profit_factor, 2))
    with k6:
        _kpi("Trades", fmt_int(trades))

    # 3) Two-column grouped blocks
    left, right = st.columns(2)

    start_raw = run_row.get("start_time") or run_row.get("start_ts")
    end_raw = run_row.get("end_time") or run_row.get("end_ts")
    metadata = _extract_metadata(run_row)
    start_raw = start_raw or metadata.get("start_time") or metadata.get("start_ts")
    end_raw = end_raw or metadata.get("end_time") or metadata.get("end_ts")
    start_dt = _normalize_timestamp(start_raw)
    end_dt = _normalize_timestamp(end_raw)

    tz = _ui_display_tz()
    start_txt = fmt_dt(start_dt, tz) if start_dt else "—"
    end_txt = fmt_dt(end_dt, tz) if end_dt else "—"
    dur_txt = str(end_dt - start_dt) if (start_dt and end_dt) else "—"

    with left:
        with st.container(border=True):
            st.markdown("**Performance**")
            st.write(f"Net profit: **{fmt_num(net_profit, 2)}**")
            st.write(f"Profit factor: **{fmt_num(profit_factor, 2)}**")
            st.write(f"Win rate: **{format_pct(win_rate, assume_ratio_if_leq_1=True, decimals=0)}**")

            # Optional secondary fields (only show if present)
            for label, key, dec in (
                ("Gross profit", "gross_profit", 2),
                ("Gross loss", "gross_loss", 2),
                ("Reward/Risk", "reward_risk", 2),
            ):
                v = _safe_float(metrics.get(key))
                if v is not None:
                    st.write(f"{label}: **{fmt_num(v, dec)}**")

        with st.container(border=True):
            st.markdown("**Risk**")
            st.write(f"Max drawdown: **{format_pct(max_dd, assume_ratio_if_leq_1=True, decimals=0)}**")
            st.write(f"Sharpe: **{fmt_num(sharpe, 2)}**")
            st.write(f"Sortino: **{fmt_num(sortino, 2)}**")
            st.write(f"Common Sense Ratio: **{fmt_num(csr, 2)}**")
            st.write(f"CPC Index: **{fmt_num(cpc, 2)}**")

    with right:
        with st.container(border=True):
            st.markdown("**Activity**")
            st.write(f"Trades: **{fmt_int(trades)}**")

            if avg_pos_time_seconds is not None:
                hold_txt = format_duration(avg_pos_time_seconds)
                if hold_txt:
                    st.write(f"Avg Pos Time: **{hold_txt}**")

            for label, key, dec in (
                ("Wins", "wins", 0),
                ("Losses", "losses", 0),
            ):
                v = _safe_float(metrics.get(key))
                if v is not None:
                    st.write(f"{label}: **{fmt_num(v, dec)}**")

        with st.container(border=True):
            st.markdown("**Shortlist**")
            shortlist_key = f"run_details_shortlist_flag_{run_id}"
            shortlist_note_key = f"run_details_shortlist_note_{run_id}"
            shortlist_default = bool(run_row.get("shortlist"))
            shortlist_note_default = str(run_row.get("shortlist_note") or "")

            def _persist_shortlist_from_state() -> None:
                rid = str(run_id or "").strip()
                if not rid:
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = "Run not available."
                    return
                try:
                    flag = bool(st.session_state.get(shortlist_key))
                    note = str(st.session_state.get(shortlist_note_key) or "")
                    update_run_shortlist_fields(
                        rid,
                        shortlist=flag,
                        shortlist_note=note,
                        user_id=user_id,
                    )
                    run_row["shortlist"] = bool(flag)
                    run_row["shortlist_note"] = str(note or "")
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = "Saved."
                except Exception as exc:
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = f"Save failed: {exc}"

            def _persist_shortlist_from_state() -> None:
                rid = str(run_id or "").strip()
                if not rid:
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = "Run not available."
                    return
                try:
                    flag = bool(st.session_state.get(shortlist_key))
                    note = str(st.session_state.get(shortlist_note_key) or "")
                    update_run_shortlist_fields(
                        rid,
                        shortlist=flag,
                        shortlist_note=note,
                        user_id=user_id,
                    )
                    run_row["shortlist"] = bool(flag)
                    run_row["shortlist_note"] = str(note or "")
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = "Saved."
                except Exception as exc:
                    st.session_state[f"run_details_shortlist_status_{run_id}"] = f"Save failed: {exc}"

            shortlist_flag = st.checkbox(
                "Shortlist this run",
                value=shortlist_default,
                key=shortlist_key,
                disabled=not bool(run_id),
                on_change=_persist_shortlist_from_state,
            )
            shortlist_note = st.text_area(
                "Note",
                value=shortlist_note_default,
                key=shortlist_note_key,
                height=90,
                placeholder="Why this run is shortlisted…",
                disabled=not bool(run_id),
                on_change=_persist_shortlist_from_state,
            )
            status_msg = st.session_state.get(f"run_details_shortlist_status_{run_id}")
            if status_msg:
                st.caption(str(status_msg))

        with st.container(border=True):
            st.markdown("**Run window**")
            st.write(f"Start: **{start_txt}**")
            st.write(f"End: **{end_txt}**")
            st.write(f"Duration: **{dur_txt}**")

    # Note: drawdown and win rate are displayed as percentages in UI.
    st.caption("Note: Win rate and drawdown are displayed as percentages.")

    # 4) Advanced metrics expander (secondary ratios / remaining fields)
    with st.expander("Advanced metrics", expanded=False):
        shown = {
            "net_profit",
            "roi_pct_on_margin",
            "net_return_pct",
            "return_pct",
            "max_drawdown_pct",
            "max_drawdown",
            "max_dd",
            "sharpe",
            "sharpe_ratio",
            "sortino",
            "sortino_ratio",
            "profit_factor",
            "total_trades",
            "trades",
            "win_rate",
            "win_pct",
            "common_sense_ratio",
            "cpc_index",
            "gross_profit",
            "gross_loss",
            "reward_risk",
            "wins",
            "losses",
        }
        remaining = [(k, v) for k, v in (metrics or {}).items() if str(k) not in shown]
        if not remaining:
            st.caption("No additional metrics.")
        else:
            for k, v in sorted(remaining, key=lambda kv: str(kv[0])):
                st.write(f"{k}: **{format_run_metric(k, v)}**")


def render_run_config(run_row: Dict[str, Any]) -> None:
    config_dict = _extract_config_dict(run_row)
    with st.expander("Config snapshot", expanded=False):
        if config_dict is not None:
            st.json(config_dict)
        else:
            st.write("No config snapshot available for this run.")


def _safe_order_style(value: Any, default: OrderStyle = OrderStyle.MARKET) -> OrderStyle:
    if isinstance(value, OrderStyle):
        return value
    if isinstance(value, str):
        try:
            return OrderStyle(value)
        except ValueError:
            try:
                return OrderStyle(value.lower())
            except ValueError:
                return default
    return default


def _safe_initial_entry_sizing_mode(value: Any) -> InitialEntrySizingMode:
    if isinstance(value, InitialEntrySizingMode):
        return value
    raw = str(value or "").strip().lower()
    if raw in {"fixed_usd", "fixed", "usd", "$"} or raw.endswith("fixed_usd"):
        return InitialEntrySizingMode.FIXED_USD
    # Default: keep existing behavior
    return InitialEntrySizingMode.PCT_BALANCE


def _dragon_config_from_snapshot(config_dict: Dict[str, Any]) -> DragonAlgoConfig:
    if not config_dict:
        raise ValueError("Missing config data")

    general_dict = config_dict.get("general", {})
    dynamic_dict = general_dict.get("dynamic_activation", {})
    general = GeneralSettings(
        max_entries=int(general_dict.get("max_entries", 1) or 1),
        initial_entry_balance_pct=float(general_dict.get("initial_entry_balance_pct") or 10.0),
        initial_entry_sizing_mode=_safe_initial_entry_sizing_mode(general_dict.get("initial_entry_sizing_mode")),
        initial_entry_fixed_usd=float(general_dict.get("initial_entry_fixed_usd") or 0.0),
        use_indicator_consensus=bool(general_dict.get("use_indicator_consensus", True)),
        entry_order_style=_safe_order_style(general_dict.get("entry_order_style", OrderStyle.MARKET.value)),
        entry_timeout_min=int(general_dict.get("entry_timeout_min", 0) or 0),
        entry_cooldown_min=int(general_dict.get("entry_cooldown_min", 0) or 0),
        exit_order_style=_safe_order_style(general_dict.get("exit_order_style", OrderStyle.MARKET.value)),
        exit_timeout_min=int(general_dict.get("exit_timeout_min", 0) or 0),
        allow_long=bool(general_dict.get("allow_long", True)),
        allow_short=bool(general_dict.get("allow_short", False)),
        lock_atr_on_entry_for_dca=bool(general_dict.get("lock_atr_on_entry_for_dca", False)),
        use_avg_entry_for_dca_base=bool(general_dict.get("use_avg_entry_for_dca_base", True)),
        plot_dca_levels=bool(general_dict.get("plot_dca_levels", False)),
        dynamic_activation=DynamicActivationConfig(
            entry_pct=float(dynamic_dict.get("entry_pct") or 0.0),
            dca_pct=float(dynamic_dict.get("dca_pct") or 0.0),
            tp_pct=float(dynamic_dict.get("tp_pct") or 0.0),
        ),
        prefer_bbo_maker=bool(general_dict.get("prefer_bbo_maker", True)),
        bbo_queue_level=int(general_dict.get("bbo_queue_level", 1) or 1),
        use_ma_direction=bool(general_dict.get("use_ma_direction", False)),
        ma_type=str(general_dict.get("ma_type") or "Ema"),
        ma_length=int(general_dict.get("ma_length") or 200),
        ma_source=str(general_dict.get("ma_source") or "close"),
    )

    exits_dict = config_dict.get("exits", {})
    # --- Back-compat mapping for exits ---------------------------------
    # New schema keys
    sl_mode_raw = str(exits_dict.get("sl_mode") or "").strip().upper()
    try:
        sl_mode = StopLossMode(sl_mode_raw) if sl_mode_raw else StopLossMode.PCT
    except Exception:
        sl_mode = StopLossMode.PCT

    tp_mode_raw = str(exits_dict.get("tp_mode") or "").strip().upper()
    if not tp_mode_raw:
        # Legacy: use_atr_tp controls TP mode.
        tp_mode_raw = TakeProfitMode.ATR.value if bool(exits_dict.get("use_atr_tp", False)) else TakeProfitMode.PCT.value
    try:
        tp_mode = TakeProfitMode(tp_mode_raw)
    except Exception:
        tp_mode = TakeProfitMode.ATR

    # Shared ATR period (legacy: tp_atr_period)
    atr_period = int(exits_dict.get("atr_period") or exits_dict.get("tp_atr_period") or 14)

    exits = ExitSettings(
        sl_mode=sl_mode,
        sl_pct=float(exits_dict.get("sl_pct") or exits_dict.get("fixed_sl_pct") or 0.0),
        trail_activation_pct=float(exits_dict.get("trail_activation_pct") or 0.0),
        trail_distance_pct=float(exits_dict.get("trail_distance_pct") or exits_dict.get("trail_stop_pct") or 0.0),
        atr_period=atr_period,
        sl_atr_mult=float(exits_dict.get("sl_atr_mult") or 0.0),
        trail_activation_atr_mult=float(exits_dict.get("trail_activation_atr_mult") or 0.0),
        trail_distance_atr_mult=float(exits_dict.get("trail_distance_atr_mult") or 0.0),
        tp_mode=tp_mode,
        tp_pct=float(exits_dict.get("tp_pct") or exits_dict.get("fixed_tp_pct") or 0.0),
        tp_atr_mult=float(exits_dict.get("tp_atr_mult") or exits_dict.get("tp_atr_multiple") or 0.0),
        tp_replace_threshold_pct=float(exits_dict.get("tp_replace_threshold_pct") or 0.0),
    )

    dca_dict = config_dict.get("dca", {})
    dca = DcaSettings(
        base_deviation_pct=float(dca_dict.get("base_deviation_pct") or 0.0),
        deviation_multiplier=float(dca_dict.get("deviation_multiplier") or 1.0),
        volume_multiplier=float(dca_dict.get("volume_multiplier") or 1.0),
    )

    trend_dict = config_dict.get("trend", {})
    trend = TrendFilterConfig(
        ma_interval_min=int(trend_dict.get("ma_interval_min") or 1),
        ma_len=int(trend_dict.get("ma_len") or 1),
        ma_type=str(trend_dict.get("ma_type") or "Sma"),
    )

    bb_dict = config_dict.get("bbands", {})
    bbands = BBandsConfig(
        interval_min=int(bb_dict.get("interval_min") or 1),
        length=int(bb_dict.get("length") or 1),
        dev_up=float(bb_dict.get("dev_up") or 0.0),
        dev_down=float(bb_dict.get("dev_down") or 0.0),
        ma_type=str(bb_dict.get("ma_type") or "Sma"),
        deviation=float(bb_dict.get("deviation") or 0.0),
        require_fcc=bool(bb_dict.get("require_fcc", False)),
        reset_middle=bool(bb_dict.get("reset_middle", False)),
        allow_mid_sells=bool(bb_dict.get("allow_mid_sells", False)),
    )

    macd_dict = config_dict.get("macd", {})
    macd = MACDConfig(
        interval_min=int(macd_dict.get("interval_min") or 1),
        fast=int(macd_dict.get("fast") or 12),
        slow=int(macd_dict.get("slow") or 26),
        signal=int(macd_dict.get("signal") or 9),
    )

    rsi_dict = config_dict.get("rsi", {})
    rsi = RSIConfig(
        interval_min=int(rsi_dict.get("interval_min") or 1),
        length=int(rsi_dict.get("length") or 14),
        buy_level=float(rsi_dict.get("buy_level") or 30.0),
        sell_level=float(rsi_dict.get("sell_level") or 70.0),
    )

    return DragonAlgoConfig(
        general=general,
        exits=exits,
        dca=dca,
        trend=trend,
        bbands=bbands,
        macd=macd,
        rsi=rsi,
    )


def _build_data_settings(run_row: Dict[str, Any]) -> Optional[DataSettings]:
    metadata = _extract_metadata(run_row)
    symbol = run_row.get("symbol") or metadata.get("symbol")
    timeframe = run_row.get("timeframe") or metadata.get("timeframe")

    data_source = (metadata.get("data_source") or ("synthetic" if (symbol or "").upper() == "SYNTH" else "ccxt")).lower()
    range_mode = (metadata.get("range_mode") or "bars").lower()
    market_type = (metadata.get("market_type") or metadata.get("market_type_hint") or "perps").lower()

    range_params: Dict[str, Any] = {}
    range_params_meta = metadata.get("range_params")
    if isinstance(range_params_meta, str):
        try:
            range_params_meta = json.loads(range_params_meta)
        except (TypeError, json.JSONDecodeError):
            range_params_meta = None
    if isinstance(range_params_meta, dict):
        range_params.update(dict(range_params_meta))
    if data_source.startswith("synthetic"):
        count = metadata.get("candle_count")
        if count is None and metadata.get("equity_curve"):
            count = len(metadata["equity_curve"])
        range_params["count"] = int(count or 300)
    else:
        limit = metadata.get("ccxt_limit") or metadata.get("limit") or range_params.get("limit")
        if limit is not None:
            range_params["limit"] = int(limit)

        since = metadata.get("since") or range_params.get("since")
        until = metadata.get("until") or range_params.get("until")
        start_ts = metadata.get("start_ts") or metadata.get("start_time")
        end_ts = metadata.get("end_ts") or metadata.get("end_time")
        if since is None and start_ts is not None:
            since = _timestamp_to_millis(start_ts)
        if until is None and end_ts is not None:
            until = _timestamp_to_millis(end_ts)
        if since is not None:
            range_params["since"] = int(since)
        if until is not None:
            range_params["until"] = int(until)

        if "limit" not in range_params:
            approx_limit = metadata.get("candle_count")
            if approx_limit is not None:
                range_params["limit"] = int(approx_limit)

        if "limit" not in range_params and since is not None and until is not None and timeframe:
            tf_minutes = timeframe_to_minutes(timeframe)
            if tf_minutes > 0:
                span_minutes = max(int((until - since) / 60000), tf_minutes)
                range_params["limit"] = max(int(span_minutes / tf_minutes) + 1, 1)

    initial_balance = float(metadata.get("initial_balance", 1_000.0))
    fee_rate = float(metadata.get("fee_rate", 0.0004))

    return DataSettings(
        data_source="synthetic" if data_source.startswith("synthetic") else "ccxt",
        exchange_id=metadata.get("exchange_id"),
        market_type=market_type,
        symbol=symbol,
        timeframe=timeframe,
        range_mode=range_mode,
        range_params=range_params,
        initial_balance=initial_balance,
        fee_rate=fee_rate,
    )


def _timestamp_to_millis(value: Any) -> Optional[int]:
    dt = _normalize_timestamp(value)
    if not dt:
        return None
    return int(dt.timestamp() * 1000)


def _hydrate_run_row(run_row: Dict[str, Any]) -> Dict[str, Any]:
    if not run_row:
        return {}
    has_config = isinstance(run_row.get("config"), dict)
    has_metrics = isinstance(run_row.get("metrics"), dict)
    has_metadata = isinstance(run_row.get("metadata"), dict)
    if has_config and has_metrics and has_metadata:
        return run_row

    run_id = run_row.get("id")
    if not run_id:
        return run_row

    stored = get_backtest_run(run_id)
    if stored:
        merged = {**stored, **run_row}
        if "metadata" not in merged and run_row.get("metadata_json"):
            try:
                merged["metadata"] = json.loads(run_row["metadata_json"])
            except (TypeError, json.JSONDecodeError):
                merged["metadata"] = {}
        return merged
    return run_row


def rebuild_backtest_result_from_run_row(
    conn: Optional[Any],
    run_row: Dict[str, Any],
) -> Optional[BacktestResult]:
    raise RuntimeError(
        "Backtest re-run/rebuild from stored runs is disabled. "
        "Runs are immutable; load charts/analysis from stored run artifacts."
    )


def pick_rows_via_checkbox(df: pd.DataFrame, key_prefix: str, clearable: bool = True) -> list[dict]:
    if df is None or df.empty:
        return []

    editor_key = f"{key_prefix}_editor"
    state_key = f"{key_prefix}_selected_ids"

    if state_key not in st.session_state:
        st.session_state[state_key] = []

    df_with_sel = df.copy()
    if "selected" not in df_with_sel.columns:
        df_with_sel["selected"] = False

    current_selection = st.session_state[state_key]
    if "id" in df_with_sel.columns:
        df_with_sel["selected"] = df_with_sel["id"].isin(current_selection)
    else:
        df_with_sel["selected"] = df_with_sel.index.isin(current_selection)

    cols = ["selected"] + [c for c in df_with_sel.columns if c != "selected"]
    df_with_sel = df_with_sel[cols]

    if clearable and st.button("Clear selection", key=f"{key_prefix}_clear"):
        st.session_state[state_key] = []
        df_with_sel["selected"] = False
        if editor_key in st.session_state:
            st.session_state.pop(editor_key)

    edited = st.data_editor(
        df_with_sel,
        key=editor_key,
        hide_index=True,
        column_config={
            "selected": st.column_config.CheckboxColumn(
                "Select",
                help="Tick to include this row in the details below",
                default=False,
            ),
        },
    )

    if "selected" not in edited.columns:
        st.session_state[state_key] = []
        return []

    selected_rows = edited[edited["selected"]]
    if selected_rows.empty:
        st.session_state[state_key] = []
        return []

    results: list[dict] = []
    if "id" in selected_rows.columns and "id" in df.columns:
        new_selection = selected_rows["id"].tolist()
        st.session_state[state_key] = new_selection
        for row_id in new_selection:
            base_row = df[df["id"] == row_id]
            if not base_row.empty:
                results.append(base_row.iloc[0].to_dict())
    else:
        new_selection_idx = list(selected_rows.index)
        st.session_state[state_key] = new_selection_idx
        for idx in new_selection_idx:
            if idx in df.index:
                results.append(df.loc[idx].to_dict())

    return results


def render_run_detail_from_db(
    conn: Optional[Any],
    run_row: dict,
    *,
    show_charts: bool = True,
    show_header: bool = True,
    show_stats: bool = True,
    chart_downsample_keep_frac: float = 0.75,
) -> None:
    if not run_row:
        st.info("No run data available.")
        return

    hydrated = _hydrate_run_row(run_row)

    # --- Helpers (Run Detail layout only) -----------------------------
    COLOR_BY_NAME = {
        "green": "#7ee787",
        "red": "#ff7b72",
        "amber": "#facc15",
        "muted": "#94a3b8",
    }

    def _present(v: Any) -> bool:
        if v is None:
            return False
        try:
            if isinstance(v, float) and (math.isnan(v) or not math.isfinite(v)):
                return False
        except Exception:
            pass
        s = str(v).strip()
        if s in {"", "—", "None", "nan", "NaN"}:
            return False
        return True

    def _metric_span(metric_name: str, raw_value: Any, formatted: str) -> str:
        kind = get_metric_color(metric_name, raw_value)
        color = COLOR_BY_NAME.get(str(kind), COLOR_BY_NAME["muted"])
        return f"<span style='color:{color}; font-weight:800;'>{html.escape(str(formatted))}</span>"

    def _pretty_key(k: str) -> str:
        key = str(k or "").strip()
        if not key:
            return ""
        # Common nicer labels
        m = {
            "net_profit": "Net Profit",
            "net_return_pct": "Net Return",
            "return_pct": "Net Return",
            "max_drawdown_pct": "Max Drawdown",
            "max_drawdown": "Max Drawdown",
            "max_dd": "Max Drawdown",
            "profit_factor": "Profit Factor",
            "win_rate": "Win Rate",
            "win_pct": "Win Rate",
            "total_trades": "Trades",
            "trades": "Trades",
            "roi_pct_on_margin": "ROI (Margin)",
            "common_sense_ratio": "Common Sense Ratio",
            "cpc_index": "CPC Index",
        }
        lk = key.lower()
        if lk in m:
            return m[lk]
        return key.replace("_", " ").strip().title()

    # Run metadata
    try:
        rid = str(hydrated.get("id") or hydrated.get("run_id") or "").strip()
    except Exception:
        rid = ""

    metadata = _extract_metadata(hydrated)
    start_raw = hydrated.get("start_time") or hydrated.get("start_ts")
    end_raw = hydrated.get("end_time") or hydrated.get("end_ts")
    start_raw = start_raw or metadata.get("start_time") or metadata.get("start_ts")
    end_raw = end_raw or metadata.get("end_time") or metadata.get("end_ts")
    start_dt = _normalize_timestamp(start_raw)
    end_dt = _normalize_timestamp(end_raw)

    # Saved metrics payload (no recompute)
    metrics = _extract_metrics(hydrated)
    metrics_dict: Dict[str, Any] = metrics if isinstance(metrics, dict) else {}

    # --- Header row ---------------------------------------------------
    if show_header:
        hleft, hright = st.columns([0.62, 0.38], vertical_alignment="center")
        with hleft:
            st.markdown("<div class='dragon-page-title'>Run Detail</div>", unsafe_allow_html=True)

        # Header actions on the right
        sweep_id_for_action = hydrated.get("sweep_id")
        if not sweep_id_for_action:
            try:
                sweep_id_for_action = (hydrated.get("metadata") or {}).get("sweep_id")
            except Exception:
                sweep_id_for_action = None

        with hright:
            b1, b2, b3 = st.columns([1, 1, 1], gap="small")

            with b1:
                if st.button(
                    "Re-Run",
                    key=f"run_actions_rerun_{rid}",
                    disabled=not bool(rid),
                    use_container_width=True,
                ):
                    try:
                        _reset_backtest_ui_state()
                    except Exception:
                        pass
                    try:
                        md = _extract_metadata(hydrated)
                        _apply_run_config_to_backtest_ui(hydrated, settings={})
                        _apply_backtest_data_settings_to_ui(run_row=hydrated, metadata=md, settings={})
                        # Make it obvious this is a new run request.
                        _set_model_and_widget("bt_name", f"Re-run {rid}")
                    except Exception:
                        pass
                    st.session_state["current_page"] = "Backtest"
                    _update_query_params(page="Backtest", run_id=str(rid).strip())
                    st.rerun()

            with b2:
                if st.button(
                    "Create bot",
                    key=f"run_actions_create_bot_{rid}",
                    disabled=not bool(rid),
                    use_container_width=True,
                ):
                    st.session_state["create_live_bot_run_id"] = rid
                    st.session_state.pop("_create_live_bot_prefilled_for", None)
                    st.session_state.pop("selected_bot_id", None)
                    st.session_state["current_page"] = "Live Bots"
                    _update_query_params(page="Live Bots", create_live_bot_run_id=rid)
                    st.rerun()

            with b3:
                open_sweep_disabled = not bool(sweep_id_for_action)
                if st.button(
                    "Open sweep",
                    key=f"run_actions_open_sweep_{rid}",
                    disabled=open_sweep_disabled,
                    use_container_width=True,
                ):
                    st.session_state["current_page"] = "Sweeps"
                    st.session_state["deep_link_sweep_id"] = str(sweep_id_for_action).strip()
                    st.session_state["deep_link_run_id"] = rid
                    _update_query_params(page="Sweeps", sweep_id=str(sweep_id_for_action).strip(), run_id=rid)
                    st.rerun()

        # Subtitle line: Strategy • Symbol • Timeframe • Direction • Market • Account
        strategy = str(hydrated.get("strategy_name") or metadata.get("strategy_name") or "").strip()
        symbol = str(hydrated.get("symbol") or metadata.get("symbol") or "").strip()
        timeframe = str(hydrated.get("timeframe") or metadata.get("timeframe") or "").strip()
        config_payload = hydrated.get("config_json") or _extract_config_dict(hydrated)
        direction = ""
        try:
            direction = str(infer_trade_direction_label(config_payload) or "").strip()
        except Exception:
            direction = ""

        market = ""
        if symbol:
            market = "PERPS" if str(symbol).upper().startswith("PERP_") else "SPOT"
        account = ""
        for k in ("account_label", "account", "account_name"):
            if hydrated.get(k):
                account = str(hydrated.get(k) or "").strip()
                break
        if not account:
            try:
                acct_id = hydrated.get("account_id") or (metadata.get("account_id") if isinstance(metadata, dict) else None)
                if acct_id not in (None, ""):
                    account = f"#{acct_id}"
            except Exception:
                account = ""

        subtitle_parts = [p for p in [strategy, symbol, timeframe, direction, market, account] if p]
        if subtitle_parts:
            st.caption(" • ".join(subtitle_parts))
        if rid:
            st.caption(f"Run ID: {rid}")

        # Run window: Start/End/Duration (only here; do not repeat elsewhere)
        if start_dt and end_dt:
            tz = _ui_display_tz()
            start_txt = fmt_dt(start_dt, tz)
            end_txt = fmt_dt(end_dt, tz)
            dur_txt = str(end_dt - start_dt)
        else:
            start_txt = end_txt = dur_txt = "—"

        trades_cnt = metrics_dict.get("total_trades", metrics_dict.get("trades"))
        bars_cnt = metrics_dict.get("bars", metrics_dict.get("n_bars", metrics_dict.get("bar_count")))
        window_parts = [f"Start: {start_txt}", f"End: {end_txt}", f"Duration: {dur_txt}"]
        if _present(trades_cnt):
            window_parts.append(f"Trades: {fmt_int(_safe_float(trades_cnt))}")
        if _present(bars_cnt):
            window_parts.append(f"Bars: {fmt_int(_safe_float(bars_cnt))}")
        st.caption(" · ".join(window_parts))

    trades_df = pd.DataFrame(columns=["timestamp", "side", "price", "size", "pnl", "note", "index"])
    trades_payload = hydrated.get("trades_json") or hydrated.get("trades")

    tf_for_ma = str(hydrated.get("timeframe") or metadata.get("timeframe") or "")
    scope_key = f"run:{hydrated.get('id') or 'unknown'}"

    # Trades table should be fast: use stored payload first.
    if trades_payload:
        trades_df = trades_to_dataframe(parse_trades_payload(trades_payload))

    if not trades_df.empty:
        trades_df = trades_df.copy()
        for col in ("price", "size", "pnl"):
            if col in trades_df.columns:
                trades_df[col] = pd.to_numeric(trades_df[col], errors="coerce").round(2)

    if show_charts:
        config_dict_for_ma = _extract_config_dict(hydrated)
        ma_interval_min, ma_type, ma_periods = _extract_trend_ma_settings_from_config_dict(config_dict_for_ma)

        # Hard cap for Run detail charts: prevents huge runs from freezing the UI.
        # (Still preserves endpoints via the downsampling index selection.)
        MAX_RUN_CHART_POINTS = 5000

        # Charts: cache per-run in session_state so reruns don't repeatedly parse payloads.
        # IMPORTANT: Runs must never be re-run once stored.
        run_id_for_key = str(hydrated.get("id") or "").strip() or "unknown"
        result_cache_key = f"run_details_result_cache__{run_id_for_key}"
        result: Optional[BacktestResult] = st.session_state.get(result_cache_key)
        if result is not None and not isinstance(result, BacktestResult):
            result = None
        recomputed_result: Optional[BacktestResult] = None

        artifacts: Optional[Dict[str, Any]] = None
        if run_id_for_key and conn is not None:
            try:
                with _perf("run.charts.load_artifacts"):
                    artifacts = get_backtest_run_chart_artifacts(run_id_for_key, conn=conn)
            except Exception:
                artifacts = None

        debug_schema = None
        debug_cols: list[str] = []
        debug_row_exists = None
        debug_required_cols = {}
        if conn is not None:
            try:
                row = conn.execute("SELECT current_schema() AS schema").fetchone()
                if row is not None:
                    debug_schema = row[0]
            except Exception:
                debug_schema = None
            try:
                cols = sorted(get_table_columns(conn, "backtest_run_details"))
                debug_cols = cols
                debug_required_cols = {
                    "equity_curve": ("equity_curve_json" in cols or "equity_curve_jsonb" in cols),
                    "equity_timestamps": ("equity_timestamps_json" in cols or "equity_timestamps_jsonb" in cols),
                    "extra_series": ("extra_series_json" in cols or "extra_series_jsonb" in cols),
                }
            except Exception:
                debug_cols = []
            try:
                row2 = conn.execute(
                    "SELECT 1 FROM backtest_run_details WHERE run_id = %s LIMIT 1",
                    (run_id_for_key,),
                ).fetchone()
                debug_row_exists = bool(row2)
            except Exception:
                debug_row_exists = None

            debug_artifact_probe = {}
            try:
                probe = conn.execute(
                    """
                    SELECT
                        length(equity_curve_json) AS eq_text_len,
                        length(equity_timestamps_json) AS ts_text_len,
                        length(extra_series_json) AS extra_text_len,
                        equity_curve_jsonb IS NULL AS eq_jsonb_null,
                        equity_timestamps_jsonb IS NULL AS ts_jsonb_null,
                        extra_series_jsonb IS NULL AS extra_jsonb_null
                    FROM backtest_run_details
                    WHERE run_id = %s
                    """,
                    (run_id_for_key,),
                ).fetchone()
                if probe is not None:
                    debug_artifact_probe = {
                        "eq_text_len": probe[0],
                        "ts_text_len": probe[1],
                        "extra_text_len": probe[2],
                        "eq_jsonb_null": probe[3],
                        "ts_jsonb_null": probe[4],
                        "extra_jsonb_null": probe[5],
                    }
            except Exception as exc:
                debug_artifact_probe = {"error": str(exc)}

        artifacts_debug = {
            "run_id": run_id_for_key,
            "artifacts_loaded": bool(artifacts),
            "equity_len": len(artifacts.get("equity_curve") or []) if isinstance(artifacts, dict) else 0,
            "timestamps_len": len(artifacts.get("equity_timestamps") or []) if isinstance(artifacts, dict) else 0,
            "candles_len": len(artifacts.get("candles") or []) if isinstance(artifacts, dict) else 0,
            "extra_keys": list((artifacts.get("extra_series") or {}).keys()) if isinstance(artifacts, dict) else [],
            "schema": debug_schema,
            "details_row_exists": debug_row_exists,
            "required_cols_present": debug_required_cols,
            "details_cols": debug_cols,
            "artifact_probe": debug_artifact_probe,
        }

        # Build a result from stored artifacts only (no network, no re-run).
        if isinstance(artifacts, dict) and artifacts.get("equity_curve"):
                equity_curve = artifacts.get("equity_curve") or []
                extra_series = artifacts.get("extra_series") or {}
                params = artifacts.get("params") or {}
                run_context = artifacts.get("run_context") or {}
                computed_metrics = artifacts.get("computed_metrics") or {}

                candles_raw = artifacts.get("candles")
                base_n: Optional[int] = None
                if isinstance(candles_raw, list) and candles_raw:
                    base_n = len(candles_raw)
                elif isinstance(equity_curve, list) and equity_curve:
                    base_n = len(equity_curve)

                ds_idxs: Optional[list[int]] = None
                if base_n is not None and base_n > 0:
                    try:
                        ds_idxs = _downsample_indices(
                            int(base_n),
                            keep_frac=float(chart_downsample_keep_frac),
                            max_points=int(MAX_RUN_CHART_POINTS),
                        )
                    except Exception:
                        ds_idxs = None

                def _ds_list(seq: Any) -> Any:
                    if ds_idxs is None:
                        return seq
                    if not isinstance(seq, list) or not seq:
                        return seq
                    if len(seq) != int(base_n or 0):
                        return seq
                    try:
                        return [seq[i] for i in ds_idxs if 0 <= i < len(seq)]
                    except Exception:
                        return seq

                def _ds_series_dict(d: Any) -> Any:
                    if ds_idxs is None:
                        return d
                    if not isinstance(d, dict) or not d:
                        return d
                    out: dict[str, Any] = {}
                    for k, v in d.items():
                        if isinstance(v, list) and base_n is not None and len(v) == int(base_n):
                            out[str(k)] = [v[i] for i in ds_idxs if 0 <= i < len(v)]
                        else:
                            out[str(k)] = v
                    return out

                equity_curve = _ds_list(equity_curve) if isinstance(equity_curve, list) else equity_curve
                extra_series = _ds_series_dict(extra_series) if isinstance(extra_series, dict) else extra_series

                candles: list[Candle] = []
                if isinstance(candles_raw, list) and candles_raw:
                    # Downsample before constructing Candle objects to keep memory/CPU bounded.
                    rows_iter: Iterable[Any]
                    if ds_idxs is not None and len(candles_raw) == int(base_n or 0):
                        try:
                            rows_iter = (candles_raw[i] for i in ds_idxs if 0 <= i < len(candles_raw))
                        except Exception:
                            rows_iter = candles_raw
                    else:
                        rows_iter = candles_raw

                    for row in rows_iter:
                        try:
                            if not isinstance(row, list) or len(row) < 6:
                                continue
                            ts = _normalize_timestamp(row[0])
                            if ts is None:
                                continue
                            o = float(row[1]) if row[1] is not None else float("nan")
                            h = float(row[2]) if row[2] is not None else float("nan")
                            l = float(row[3]) if row[3] is not None else float("nan")
                            c = float(row[4]) if row[4] is not None else float("nan")
                            v = float(row[5]) if row[5] is not None else 0.0
                            candles.append(Candle(timestamp=ts, open=o, high=h, low=l, close=c, volume=v))
                        except Exception:
                            continue

                eq_ts: list[datetime] = []
                eq_ts_raw = artifacts.get("equity_timestamps")
                if isinstance(eq_ts_raw, list) and eq_ts_raw:
                    eq_ts_raw = _ds_list(eq_ts_raw)
                    for item in (eq_ts_raw or []):
                        dt = _normalize_timestamp(item)
                        if dt is not None:
                            eq_ts.append(dt)
                if not eq_ts and candles and len(candles) == len(equity_curve):
                    try:
                        eq_ts = [c.timestamp for c in candles]
                    except Exception:
                        eq_ts = []

                result = BacktestResult(
                    candles=list(candles),
                    equity_curve=[float(x) if x is not None else float("nan") for x in equity_curve],
                    trades=parse_trades_payload(trades_payload),
                    metrics=_extract_metrics(hydrated) if isinstance(_extract_metrics(hydrated), dict) else {},
                    start_time=start_dt,
                    end_time=end_dt,
                    equity_timestamps=eq_ts,
                    params=dict(params) if isinstance(params, dict) else {},
                    run_context=dict(run_context) if isinstance(run_context, dict) else {},
                    computed_metrics=dict(computed_metrics) if isinstance(computed_metrics, dict) else {},
                    extra_series=extra_series if isinstance(extra_series, dict) else {},
                )

        st.session_state[result_cache_key] = result

        missing_trades = not bool(trades_payload)
        missing_artifacts = result is None
        if missing_artifacts:
            with st.expander("Charts debug", expanded=False):
                st.json(artifacts_debug)
        if missing_trades or missing_artifacts:
            st.warning("Detailed data was removed. Basics are preserved.")
            recompute_cols = st.columns([1, 2, 2])
            with recompute_cols[0]:
                resave_details = st.checkbox(
                    "Re-save details",
                    value=False,
                    key=f"run_recompute_resave_{run_id_for_key}",
                    help="When checked, recomputed details will be stored back into the DB.",
                )
            with recompute_cols[1]:
                if st.button(
                    "Recompute details now",
                    key=f"run_recompute_btn_{run_id_for_key}",
                    use_container_width=True,
                ):
                    with st.spinner("Recomputing details (this may take a while)..."):
                        try:
                            cfg_snapshot = _extract_config_dict(hydrated) or {}
                            cfg_obj = _dragon_config_from_snapshot(cfg_snapshot)
                            md = _extract_metadata(hydrated)
                            data_settings = _build_data_settings(hydrated)
                            if data_settings is None:
                                raise ValueError("Unable to derive data settings for this run.")

                            artifacts_out = run_single_backtest(cfg_obj, data_settings)
                            recomputed_result = artifacts_out.result

                            st.session_state[result_cache_key] = recomputed_result

                            if resave_details:
                                md_save = dict(md) if isinstance(md, dict) else {}
                                md_save.setdefault("id", run_id_for_key)
                                md_save.setdefault("created_at", hydrated.get("created_at") or md_save.get("created_at"))
                                md_save.setdefault("symbol", hydrated.get("symbol") or md_save.get("symbol"))
                                md_save.setdefault("timeframe", hydrated.get("timeframe") or md_save.get("timeframe"))
                                md_save.setdefault("strategy_name", hydrated.get("strategy_name") or md_save.get("strategy_name"))
                                md_save.setdefault("strategy_version", hydrated.get("strategy_version") or md_save.get("strategy_version"))
                                md_save.setdefault("sweep_id", hydrated.get("sweep_id") or md_save.get("sweep_id"))

                                save_backtest_run_details_for_existing_run(
                                    run_id=run_id_for_key,
                                    config=cfg_snapshot,
                                    metrics=recomputed_result.metrics or {},
                                    metadata=md_save,
                                    result=recomputed_result,
                                )
                                st.success("Details re-saved.")

                        except Exception as exc:
                            st.error(f"Recompute failed: {exc}")

            # Prefer recomputed result for display in this render pass.
            if recomputed_result is not None:
                result = recomputed_result
                if not trades_payload and getattr(recomputed_result, "trades", None):
                    trades_payload = recomputed_result.trades

        if trades_payload and trades_df.empty:
            trades_df = trades_to_dataframe(parse_trades_payload(trades_payload))

        # KPI tiles (compact 3x2)
        if show_stats:
            net_profit = _safe_float(metrics_dict.get("net_profit"))
            net_return_pct = _safe_float(metrics_dict.get("net_return_pct", metrics_dict.get("return_pct")))
            max_dd = _safe_float(metrics_dict.get("max_drawdown_pct", metrics_dict.get("max_drawdown", metrics_dict.get("max_dd"))))
            # Best-effort: if an absolute drawdown metric is present in saved metrics, show it as a subline.
            max_dd_abs_raw = _safe_float(
                metrics_dict.get(
                    "max_drawdown_abs",
                    metrics_dict.get(
                        "max_dd_abs",
                        # If both pct + max_drawdown exist, max_drawdown is often the absolute variant.
                        metrics_dict.get("max_drawdown") if (metrics_dict.get("max_drawdown_pct") is not None) else None,
                    ),
                )
            )
            max_dd_abs_txt = None
            if max_dd_abs_raw is not None:
                try:
                    max_dd_abs_txt = f"$: {fmt_num(abs(float(max_dd_abs_raw)), 2)}"
                except Exception:
                    max_dd_abs_txt = None
            sharpe = _safe_float(metrics_dict.get("sharpe", metrics_dict.get("sharpe_ratio")))
            profit_factor = _safe_float(metrics_dict.get("profit_factor"))
            win_rate = _safe_float(metrics_dict.get("win_rate", metrics_dict.get("win_pct")))
            trades_cnt = _safe_float(metrics_dict.get("total_trades", metrics_dict.get("trades")))

            r1 = st.columns(3)
            with r1[0]:
                render_kpi_tile("Net Profit ($)", _metric_span("net_profit", net_profit, fmt_num(net_profit, 2)))
            with r1[1]:
                render_kpi_tile(
                    "Net Return (%)",
                    _metric_span("net_return_pct", net_return_pct, format_pct(net_return_pct, assume_ratio_if_leq_1=True, decimals=0)),
                )
            with r1[2]:
                render_kpi_tile(
                    "Max Drawdown (%)",
                    _metric_span("max_drawdown_pct", max_dd, format_pct(max_dd, assume_ratio_if_leq_1=True, decimals=0)),
                    subtext=max_dd_abs_txt,
                )

            r2 = st.columns(3)
            with r2[0]:
                render_kpi_tile("Sharpe", _metric_span("sharpe", sharpe, fmt_num(sharpe, 2)))
            with r2[1]:
                render_kpi_tile("Profit Factor", _metric_span("profit_factor", profit_factor, fmt_num(profit_factor, 2)))
            with r2[2]:
                render_kpi_tile(
                    "Win Rate (%)",
                    _metric_span("win_rate", win_rate, format_pct(win_rate, assume_ratio_if_leq_1=True, decimals=0)),
                    subtext=(f"Trades: {fmt_int(trades_cnt)}" if trades_cnt is not None else None),
                )

        # Main body: charts left, metrics right
        body_left, body_right = st.columns([0.88, 0.12])

        def _render_metric_group(title: str, rows: list[tuple[str, str, object, str]]) -> None:
            if not rows:
                return

            def _rows() -> None:
                for label, metric_name, raw, formatted in rows:
                    val_html = _metric_span(metric_name, raw, formatted)
                    st.markdown(
                        f"<div style='display:grid; grid-template-columns: 1fr auto; column-gap:0.35rem; align-items:baseline; padding:0.15rem 0;'>"
                        f"<span style='color:{COLOR_BY_NAME['muted']}; font-weight:600; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;'>{html.escape(label)}</span>"
                        f"{val_html}"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

            render_card(title, _rows)

        def _first_present_value(keys: list[str]) -> tuple[Optional[str], object]:
            for k in keys:
                if k in (metrics_dict or {}):
                    v = (metrics_dict or {}).get(k)
                    if _present(v):
                        return str(k), v
            return None, None

        def _add_float_row(
            rows: list[tuple[str, str, object, str]],
            *,
            label: str,
            keys: list[str],
            fmt_fn,
            color_metric: Optional[str] = None,
            abs_value: bool = False,
        ) -> None:
            k, raw = _first_present_value(keys)
            if not k:
                return
            v = _safe_float(raw)
            if v is None:
                return
            if abs_value:
                try:
                    v = abs(float(v))
                except Exception:
                    pass
            try:
                formatted = str(fmt_fn(v))
            except Exception:
                formatted = str(format_run_metric(k, v))
            rows.append((label, str(color_metric or k), v, formatted))

        def _fmt_seconds_compact(seconds: float) -> str:
            try:
                s = float(seconds)
            except Exception:
                return "—"
            if not math.isfinite(s) or s < 0:
                return "—"
            secs = int(s)
            days = secs // 86400
            hours = (secs % 86400) // 3600
            mins = (secs % 3600) // 60
            if days > 0:
                return f"{days}d {hours}h" if hours else f"{days}d"
            if hours > 0:
                return f"{hours}h {mins}m" if mins else f"{hours}h"
            if mins > 0:
                return f"{mins}m"
            return "<1m"

        perf_rows: list[tuple[str, str, object, str]] = []
        _add_float_row(perf_rows, label="Net Profit", keys=["net_profit"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(
            perf_rows,
            label="Net Return",
            keys=["net_return_pct", "return_pct"],
            fmt_fn=lambda x: format_pct(x, assume_ratio_if_leq_1=True, decimals=0),
            color_metric="net_return_pct",
        )
        _add_float_row(
            perf_rows,
            label="ROI",
            keys=["roi_pct_on_margin", "roi_pct"],
            fmt_fn=lambda x: format_pct(x, assume_ratio_if_leq_1=False, decimals=(1 if abs(float(x)) < 1.0 else 0)),
            color_metric="roi_pct_on_margin",
        )
        _add_float_row(perf_rows, label="CPC Index", keys=["cpc_index"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(perf_rows, label="Common Sense Ratio", keys=["common_sense_ratio"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(perf_rows, label="Tail Ratio", keys=["tail_ratio"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(perf_rows, label="Profit Factor", keys=["profit_factor"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(perf_rows, label="Sharpe", keys=["sharpe", "sharpe_ratio"], fmt_fn=lambda x: fmt_num(x, 2), color_metric="sharpe")
        _add_float_row(
            perf_rows,
            label="Win Rate",
            keys=["win_rate", "win_pct"],
            fmt_fn=lambda x: format_pct(x, assume_ratio_if_leq_1=True, decimals=0),
            color_metric="win_rate",
        )

        risk_rows: list[tuple[str, str, object, str]] = []
        _add_float_row(risk_rows, label="Sortino", keys=["sortino", "sortino_ratio"], fmt_fn=lambda x: fmt_num(x, 2), color_metric="sortino")
        _add_float_row(risk_rows, label="Gain to Pain", keys=["gain_to_pain"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(
            risk_rows,
            label="Max Drawdown (%)",
            keys=["max_drawdown_pct", "max_dd"],
            fmt_fn=lambda x: format_pct(x, assume_ratio_if_leq_1=True, decimals=0),
            color_metric="max_drawdown_pct",
            abs_value=True,
        )
        _add_float_row(
            risk_rows,
            label="Max Drawdown ($)",
            keys=["max_drawdown_abs", "max_dd_abs"],
            fmt_fn=lambda x: fmt_num(x, 2),
            color_metric="max_drawdown_abs",
            abs_value=True,
        )

        exec_rows: list[tuple[str, str, object, str]] = []
        _add_float_row(exec_rows, label="Trades", keys=["total_trades", "trades"], fmt_fn=lambda x: fmt_int(x), color_metric="total_trades")
        _add_float_row(exec_rows, label="Final Equity", keys=["final_equity"], fmt_fn=lambda x: fmt_num(x, 2))
        _add_float_row(exec_rows, label="Fees", keys=["fees_total", "fees"], fmt_fn=lambda x: fmt_num(x, 2), color_metric="fees_total", abs_value=True)
        _add_float_row(
            exec_rows,
            label="Avg Pos Time",
            keys=["avg_position_time_seconds", "avg_position_time_s"],
            fmt_fn=format_duration,
            color_metric="avg_position_time_seconds",
        )

        grouped_fixed: list[tuple[str, list[tuple[str, str, object, str]]]] = [
            ("Performance", perf_rows),
            ("Risk", risk_rows),
            ("Execution", exec_rows),
        ]

        with body_left:
            if result is not None:
                # Build charts (same data; layout-only tweaks)
                result_for_charts = result
                try:
                    k = float(chart_downsample_keep_frac)
                except Exception:
                    k = 1.0
                n_candles = len(result.candles or [])
                should_downsample = (0.0 < k < 1.0) or (int(MAX_RUN_CHART_POINTS or 0) > 0 and n_candles > int(MAX_RUN_CHART_POINTS))
                if should_downsample:
                    try:
                        result_for_charts = _downsample_backtest_result(result, keep_frac=k, max_points=int(MAX_RUN_CHART_POINTS or 0))
                    except Exception:
                        result_for_charts = result

                warmup_ts = None
                warmup_closes = None
                try:
                    rc = result_for_charts.run_context if result_for_charts is not None else {}
                    analysis_start_ms = rc.get("analysis_start_ms") if isinstance(rc, dict) else None
                    display_start_ms = rc.get("display_start_ms") if isinstance(rc, dict) else None
                    warmup_seconds = rc.get("warmup_seconds") if isinstance(rc, dict) else None
                    if display_start_ms is None and result_for_charts is not None and result_for_charts.candles:
                        try:
                            first_ts = getattr(result_for_charts.candles[0], "timestamp", None)
                            first_dt = _normalize_timestamp(first_ts)
                            if first_dt is not None:
                                display_start_ms = int(first_dt.timestamp() * 1000)
                        except Exception:
                            display_start_ms = None
                    if analysis_start_ms is None and display_start_ms is not None and warmup_seconds:
                        try:
                            analysis_start_ms = max(0, int(display_start_ms) - int(float(warmup_seconds) * 1000))
                        except Exception:
                            analysis_start_ms = None
                    if analysis_start_ms is not None and display_start_ms is not None:
                        data_settings = None
                        try:
                            data_settings = _build_data_settings(hydrated)
                        except Exception:
                            data_settings = None
                        if data_settings is not None and str(data_settings.data_source or "").lower() != "synthetic":
                            analysis_info = dict(rc) if isinstance(rc, dict) else {}
                            try:
                                _analysis_start, _display_start, _display_end, candles_analysis, _candles_display = get_run_details_candles_analysis_window(
                                    data_settings=data_settings,
                                    run_context=analysis_info,
                                    fetch_fn=get_candles_with_cache,
                                )
                            except Exception as exc:
                                msg = str(exc).lower()
                                if "database is locked" in msg or "locked" in msg:
                                    _analysis_start, _display_start, _display_end, candles_analysis, _candles_display = get_run_details_candles_analysis_window(
                                        data_settings=data_settings,
                                        run_context=analysis_info,
                                        fetch_fn=load_ccxt_candles,
                                    )
                                else:
                                    raise

                            warmup_ts_list: list[datetime] = []
                            warmup_close_list: list[float] = []
                            for c in candles_analysis or []:
                                ts = _normalize_timestamp(getattr(c, "timestamp", None))
                                if ts is None:
                                    continue
                                ts_ms = int(ts.timestamp() * 1000)
                                if ts_ms >= int(display_start_ms):
                                    continue
                                warmup_ts_list.append(ts)
                                warmup_close_list.append(float(getattr(c, "close", 0.0) or 0.0))

                            if warmup_ts_list:
                                warmup_ts = pd.Series(warmup_ts_list)
                                warmup_closes = pd.Series(warmup_close_list)
                except Exception:
                    warmup_ts = None
                    warmup_closes = None
                eq_fig = build_equity_chart(result_for_charts)
                ma_calc_ts = None
                ma_calc_closes = None
                try:
                    if result.candles:
                        ma_calc_ts = pd.Series([getattr(c, "timestamp", None) for c in result.candles])
                        ma_calc_closes = pd.Series([getattr(c, "close", None) for c in result.candles])
                except Exception:
                    ma_calc_ts = None
                    ma_calc_closes = None

                pr_fig = build_price_orders_chart(
                    result_for_charts,
                    ma_periods=ma_periods,
                    ma_interval_min=ma_interval_min,
                    ma_type=ma_type,
                    scope_key=scope_key,
                    timeframe=tf_for_ma,
                    warmup_ts=warmup_ts,
                    warmup_closes=warmup_closes,
                    ma_calc_ts=ma_calc_ts,
                    ma_calc_closes=ma_calc_closes,
                )
                if eq_fig is not None:
                    eq_fig.update_layout(height=460, margin=dict(l=10, r=10, t=10, b=10))
                if pr_fig is not None:
                    pr_fig.update_layout(height=560, margin=dict(l=10, r=10, t=10, b=10))

                render_card(
                    "Equity curve",
                    (lambda: st.plotly_chart(eq_fig, width="stretch", key=f"run_{run_id_for_key}_equity_detail"))
                    if eq_fig is not None
                    else (lambda: st.info("No equity series available for this run.")),
                )
                render_card(
                    "Price & orders",
                    (lambda: st.plotly_chart(pr_fig, width="stretch", key=f"run_{run_id_for_key}_price_detail"))
                    if pr_fig is not None
                    else (lambda: st.info("No candle data available for price chart.")),
                )
            else:
                st.info("Charts unavailable for this run (artifacts missing).")

        with body_right:
            if show_stats:
                for title, rows in grouped_fixed:
                    _render_metric_group(title, rows)
    else:
        st.caption("Charts disabled for this view.")

    # Trades table after charts (full-width AGGrid)
    st.markdown("#### Trades")
    if not trades_df.empty:
        trades_table_df = trades_df.reset_index(drop=True).copy()
        # Remove implicit index column from the Trades grid.
        trades_table_df = trades_table_df.drop(columns=["index"], errors="ignore")

        # Presentation-only formatting: convert timestamps to display timezone and render
        # as a stable string to avoid browser/JS timezone interpretation.
        if "timestamp" in trades_table_df.columns:
            ts_utc = pd.to_datetime(trades_table_df["timestamp"], utc=True, errors="coerce")
            try:
                tz = _ui_display_tz()
                ts_local = ts_utc.dt.tz_convert(tz)
            except Exception:
                ts_local = ts_utc

            # Hidden numeric sort key (ms since epoch) for AGGrid.
            try:
                ts_utc_naive = ts_utc.dt.tz_convert("UTC").dt.tz_localize(None)
                trades_table_df["timestamp_sort_ms"] = (ts_utc_naive.astype("int64") // 1_000_000).astype("Int64")
            except Exception:
                trades_table_df["timestamp_sort_ms"] = pd.Series([pd.NA] * len(trades_table_df), dtype="Int64")

            trades_table_df["timestamp"] = ts_local.dt.strftime("%d/%m/%y %I:%M %p").fillna("—")

        if AgGrid is None or GridOptionsBuilder is None:
            st.dataframe(
                trades_table_df.drop(columns=["timestamp_sort_ms"], errors="ignore"),
                width="stretch",
                hide_index=True,
            )
        else:
            gb_t = GridOptionsBuilder.from_dataframe(trades_table_df)
            gb_t.configure_default_column(filter=True, sortable=True, resizable=True)
            if "timestamp_sort_ms" in trades_table_df.columns:
                gb_t.configure_column("timestamp_sort_ms", hide=True, sortable=True, filter=False)
            if "timestamp" in trades_table_df.columns:
                # Prevent lexicographic sorting of DD/MM/YY strings.
                gb_t.configure_column("timestamp", header_name="Time", sortable=False)
            gb_t.configure_grid_options(headerHeight=44, rowHeight=34, suppressRowHoverHighlight=False, animateRows=False)
            grid_opts_t = gb_t.build()
            AgGrid(
                trades_table_df,
                gridOptions=grid_opts_t,
                update_mode=GridUpdateMode.NO_UPDATE,
                data_return_mode=DataReturnMode.AS_INPUT,
                enable_enterprise_modules=False,
                allow_unsafe_jscode=False,
                height=420,
                theme="dark",
                custom_css=_aggrid_dark_custom_css(),
                key=f"run_trades_{rid or hydrated.get('id') or 'unknown'}",
            )
    else:
        st.caption("No trades recorded for this run.")

    # Config snapshot (collapsed by default)
    render_run_config(hydrated)


def render_run_quick_summary_from_grid_row(row: Dict[str, Any]) -> None:
    """Fast run summary using the AgGrid-selected row payload.

    Intentionally avoids DB access and heavy rebuilds. Used on Results/Sweeps to show
    immediate KPIs while charts are deferred.
    """

    if not isinstance(row, dict) or not row:
        return

    rid = str(row.get("run_id") or row.get("id") or "").strip()
    symbol = str(row.get("symbol") or row.get("symbol_full") or "?")
    timeframe = str(row.get("timeframe") or "?")
    strategy = str(row.get("strategy") or row.get("strategy_name") or "")
    icon_uri = str(row.get("icon_uri") or "").strip()

    hcols = st.columns([1, 6])
    with hcols[0]:
        if icon_uri:
            try:
                st.markdown(
                    f'<img src="{icon_uri}" width="36" style="display:block;" />',
                    unsafe_allow_html=True,
                )
            except Exception:
                pass
    with hcols[1]:
        title_parts = [f"**{symbol}**", timeframe]
        if strategy:
            title_parts.append(strategy)
        if rid:
            title_parts.append(f"run_id={rid}")
        st.write(" · ".join([p for p in title_parts if p and p != "?"]))

    kpi = st.columns(6)
    kpi[0].metric(
        "Net Return (equity)",
        format_pct(_safe_float(row.get("net_return_pct")), assume_ratio_if_leq_1=True, decimals=0),
    )
    kpi[1].metric("Net Profit", f"{float(row.get('net_profit') or 0.0):.2f}")
    kpi[2].metric(
        "Max DD",
        format_pct(_safe_float(row.get("max_drawdown_pct")), assume_ratio_if_leq_1=True, decimals=0),
    )
    kpi[3].metric("Sharpe", f"{float(row.get('sharpe') or 0.0):.2f}")
    kpi[4].metric("Sortino", f"{float(row.get('sortino') or 0.0):.2f}")
    kpi[5].metric(
        "Win Rate",
        format_pct(_safe_float(row.get("win_rate")), assume_ratio_if_leq_1=True, decimals=0),
    )

    try:
        roi_margin = row.get("roi_pct_on_margin")
        if roi_margin is not None and str(roi_margin).strip() != "":
            st.caption(
                f"ROI (margin): {format_pct(_safe_float(roi_margin), assume_ratio_if_leq_1=False, decimals=0)}"
            )
    except Exception:
        pass


# Safety: ensure header/stats helpers exist even if earlier edits failed
if "render_run_header" not in globals():
    def render_run_header(run_row: Dict[str, Any]) -> None:
        run_id = run_row.get("id")
        strategy = run_row.get("strategy_name", "DragonDcaAtr")
        version = run_row.get("strategy_version", "0.1.0")
        symbol = run_row.get("symbol", "?")
        timeframe = run_row.get("timeframe", "?")
        st.subheader(f"Run {run_id or ''} – {strategy} v{version} – {symbol} {timeframe}")

if "render_run_stats" not in globals():
    def render_run_stats(run_row: Dict[str, Any]) -> None:
        metrics = _extract_metrics(run_row)
        if not metrics:
            st.info("No metrics available for this run.")
            return

        run_id = run_row.get("id")

        cols = st.columns(4)
        cols[0].metric("Net profit", _fmt_num(metrics.get("net_profit"), 2))
        cols[1].metric("Max drawdown", _fmt_num(metrics.get("max_drawdown"), 2))
        cols[2].metric(
            "Win %",
            format_pct(_safe_float(metrics.get("win_rate", metrics.get("win_pct"))), assume_ratio_if_leq_1=True, decimals=0),
        )
        cols[3].metric("Profit factor", _fmt_num(metrics.get("profit_factor"), 2))

        cols2 = st.columns(4)
        cols2[0].metric("Sharpe", _fmt_num(metrics.get("sharpe"), 2))
        cols2[1].metric("Sortino", _fmt_num(metrics.get("sortino"), 2))
        cols2[2].metric("Trades", _fmt_num(metrics.get("total_trades", metrics.get("trades")), 0))
        cols2[3].metric(
            "Win rate",
            format_pct(_safe_float(metrics.get("win_rate", metrics.get("win_pct"))), assume_ratio_if_leq_1=True, decimals=0),
        )

        with st.container(border=True):
            st.markdown("**Shortlist**")
            shortlist_key = f"run_details_shortlist_flag_{run_id}"
            shortlist_note_key = f"run_details_shortlist_note_{run_id}"
            shortlist_default = bool(run_row.get("shortlist"))
            shortlist_note_default = str(run_row.get("shortlist_note") or "")

            shortlist_flag = st.checkbox(
                "Shortlist this run",
                value=shortlist_default,
                key=shortlist_key,
                disabled=not bool(run_id),
                on_change=_persist_shortlist_from_state,
            )
            shortlist_note = st.text_area(
                "Note",
                value=shortlist_note_default,
                key=shortlist_note_key,
                height=90,
                placeholder="Why this run is shortlisted…",
                disabled=not bool(run_id),
                on_change=_persist_shortlist_from_state,
            )
            status_msg = st.session_state.get(f"run_details_shortlist_status_{run_id}")
            if status_msg:
                st.caption(str(status_msg))


def _set_model_and_widget(key: str, value: Any) -> None:
    """Set both the model key and its widget key (w__) to keep Streamlit inputs in sync."""

    st.session_state[key] = value
    st.session_state[_wkey(key)] = value


def _apply_backtest_data_settings_to_ui(
    *,
    run_row: Dict[str, Any],
    metadata: Dict[str, Any],
    settings: Dict[str, Any],
) -> None:
    """Best-effort: apply data/range/capital settings to Backtest widgets.

    Notes:
    - Backtest uses a model/widget-key split for some inputs (via `_wkey`).
    - CCXT selectors use direct widget keys like `ccxt_symbol`.
    """

    def _to_dt(val: Any) -> Optional[datetime]:
        dt = _normalize_timestamp(val)
        if dt is not None:
            return dt
        if isinstance(val, str):
            try:
                return datetime.fromisoformat(val.replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                return None
        return None

    # Data source (radio uses widget key `_wkey('data_source_mode')`)
    data_source = str(metadata.get("data_source") or metadata.get("data_source_mode") or "ccxt").strip().lower()
    if data_source in {"ccxt", "crypto", "crypto (ccxt)"}:
        _set_model_and_widget("data_source_mode", "Crypto (CCXT)")
    else:
        _set_model_and_widget("data_source_mode", "Synthetic")

    # Capital & fees (these use model/widget split)
    init_bal_raw = metadata.get("initial_balance")
    if init_bal_raw is not None:
        try:
            _set_model_and_widget("bt_initial_balance", float(init_bal_raw))
        except Exception:
            pass

    fee_raw = metadata.get("fee_rate")
    if fee_raw is not None:
        try:
            _set_model_and_widget("bt_fee_rate", float(fee_raw))
        except Exception:
            pass

    # CCXT-specific widgets (direct widget keys)
    if str(st.session_state.get("data_source_mode") or "").strip() != "Crypto (CCXT)":
        return

    ex_id = normalize_exchange_id(str(metadata.get("exchange_id") or "woox").strip() or "woox") or "woox"
    st.session_state["ccxt_exchange_choice"] = ex_id if ex_id in {"woox", "binance", "bybit", "okx", "kraken"} else "Custom…"
    if st.session_state["ccxt_exchange_choice"] == "Custom…":
        st.session_state["ccxt_exchange_custom"] = ex_id

    mkt = str(metadata.get("market_type") or metadata.get("market") or "perps").strip().lower()
    st.session_state["ccxt_market_type"] = "Spot" if mkt == "spot" else "Perps"

    sym = str(run_row.get("symbol") or metadata.get("symbol") or "ETH/USDT:USDT").strip() or "ETH/USDT:USDT"
    tf = str(run_row.get("timeframe") or metadata.get("timeframe") or "1h").strip() or "1h"
    st.session_state["ccxt_symbol"] = sym
    st.session_state["ccxt_timeframe"] = tf

    range_mode_raw = str(metadata.get("range_mode") or "duration").strip().lower()
    if range_mode_raw in {"bars", "bar"}:
        st.session_state["ccxt_range_mode"] = "Bars"
    elif range_mode_raw in {"duration"}:
        st.session_state["ccxt_range_mode"] = "Duration"
    elif "start" in range_mode_raw or "end" in range_mode_raw:
        st.session_state["ccxt_range_mode"] = "Start/End"
    else:
        st.session_state["ccxt_range_mode"] = "Duration"

    range_params = metadata.get("range_params")
    if isinstance(range_params, str):
        try:
            range_params = json.loads(range_params)
        except Exception:
            range_params = None
    if not isinstance(range_params, dict):
        range_params = {}

    if st.session_state.get("ccxt_range_mode") == "Bars":
        limit = metadata.get("ccxt_limit")
        if limit is None:
            limit = range_params.get("limit")
        if limit is not None:
            try:
                st.session_state["ccxt_limit"] = int(limit)
            except Exception:
                pass
    elif st.session_state.get("ccxt_range_mode") == "Duration":
        dur = str(metadata.get("duration_label") or "").strip()
        if dur:
            st.session_state["ccxt_duration_label"] = dur
    else:
        start_dt = _to_dt(metadata.get("start_ts")) or _to_dt(range_params.get("since"))
        end_dt = _to_dt(metadata.get("end_ts")) or _to_dt(range_params.get("until"))
        if start_dt is not None:
            st.session_state["ccxt_start_dt"] = start_dt
        if end_dt is not None:
            st.session_state["ccxt_end_dt"] = end_dt


_SWEEP_FIELD_KEY_TO_BASE_KEY: Dict[str, str] = {
    "general.initial_entry_balance_pct": "bt_initial_entry_balance_pct",
    "general.initial_entry_fixed_usd": "bt_initial_entry_fixed_usd",
    "exits.fixed_sl_pct": "bt_fixed_sl_pct",
    "exits.fixed_tp_pct": "bt_fixed_tp_pct",
    "exits.use_atr_tp": "bt_use_atr_tp",
    "exits.tp_atr_multiple": "bt_tp_atr_multiple",
    "exits.tp_atr_period": "bt_tp_atr_period",
    "exits.trail_activation_pct": "bt_trail_activation_pct",
    "exits.trail_stop_pct": "bt_trail_stop_pct",
    "exits.sl_atr_mult": "bt_sl_atr_mult",
    "exits.trail_activation_atr_mult": "bt_trail_activation_atr_mult",
    "exits.trail_distance_atr_mult": "bt_trail_distance_atr_mult",
    "dca.base_deviation_pct": "bt_base_deviation_pct",
    "dca.deviation_multiplier": "bt_deviation_multiplier",
    "dca.volume_multiplier": "bt_volume_multiplier",
    "trend.ma_interval_min": "bt_ma_interval_min",
    "trend.ma_len": "bt_ma_length",
    "bbands.interval_min": "bt_bb_interval_min",
    "bbands.length": "bt_bb_length",
    "macd.interval_min": "bt_macd_interval_min",
    "rsi.interval_min": "bt_rsi_interval_min",
    "rsi.length": "bt_rsi_length",
}


def _apply_sweep_definition_to_backtest_ui(
    sweep_row: Dict[str, Any],
    *,
    settings: Dict[str, Any],
) -> None:
    """Prefill Backtest sweep controls from a stored sweep definition."""

    try:
        base_config = json.loads(sweep_row.get("base_config_json") or "{}")
    except Exception:
        base_config = {}

    try:
        definition = json.loads(sweep_row.get("sweep_definition_json") or "{}")
    except Exception:
        definition = {}

    try:
        range_params = json.loads(sweep_row.get("range_params_json") or "{}")
    except Exception:
        range_params = {}

    ds_from_def = definition.get("data_settings") if isinstance(definition.get("data_settings"), dict) else {}
    market_type = ds_from_def.get("market_type") if isinstance(ds_from_def, dict) else None

    fake_run_row: Dict[str, Any] = {
        "config": base_config,
        "metadata": {
            "data_source": sweep_row.get("data_source"),
            "exchange_id": sweep_row.get("exchange_id"),
            "symbol": sweep_row.get("symbol"),
            "timeframe": sweep_row.get("timeframe"),
            "market_type": market_type,
            "range_mode": sweep_row.get("range_mode"),
            "range_params": range_params,
            "initial_balance": ds_from_def.get("initial_balance") if isinstance(ds_from_def, dict) else None,
            "fee_rate": ds_from_def.get("fee_rate") if isinstance(ds_from_def, dict) else None,
        },
        "symbol": sweep_row.get("symbol"),
        "timeframe": sweep_row.get("timeframe"),
    }

    _apply_run_config_to_backtest_ui(fake_run_row, settings=settings)
    _apply_backtest_data_settings_to_ui(run_row=fake_run_row, metadata=fake_run_row.get("metadata") or {}, settings=settings)

    sweep_name = str(sweep_row.get("name") or "Dragon sweep")
    _set_model_and_widget("bt_sweep_name", sweep_name)

    params = definition.get("params") if isinstance(definition.get("params"), dict) else {}
    params_mode = definition.get("params_mode") if isinstance(definition.get("params_mode"), dict) else {}
    for field_key, raw_values in (params or {}).items():
        base_key = _SWEEP_FIELD_KEY_TO_BASE_KEY.get(str(field_key))
        if not base_key or not isinstance(raw_values, list) or not raw_values:
            continue

        mode_key = f"{base_key}_mode"
        mode_bool_key = f"{base_key}_mode__is_sweep"
        st.session_state[mode_key] = "sweep"
        st.session_state[mode_bool_key] = True
        st.session_state[_wkey(mode_bool_key)] = True

        meta = SWEEPABLE_FIELDS.get(str(field_key)) if "SWEEPABLE_FIELDS" in globals() else None
        value_type = (meta or {}).get("type", float) if isinstance(meta, dict) else float

        parsed: List[Any] = []
        for v in raw_values:
            try:
                if value_type is int:
                    parsed.append(int(float(v)))
                elif value_type is bool:
                    parsed.append(bool(str(v).strip().lower() in {"1", "true", "yes", "on"}))
                else:
                    parsed.append(float(v))
            except Exception:
                continue
        if not parsed:
            continue

        mode_hint = str((params_mode or {}).get(str(field_key)) or "").strip().lower()
        list_str = ", ".join([str(v).strip() for v in raw_values if str(v).strip()])

        if value_type in {int, float}:
            try:
                vals = sorted({float(x) for x in parsed})
                diffs = [round(vals[i + 1] - vals[i], 12) for i in range(len(vals) - 1)] if len(vals) >= 2 else []
                diffs_pos = [d for d in diffs if d > 0]
                uniform_step = False
                vstep = 1.0
                if diffs_pos:
                    vstep = min(diffs_pos) if diffs_pos else 1.0
                    uniform_step = (max(diffs_pos) - min(diffs_pos)) <= 1e-9

                wants_list = mode_hint == "list" or (mode_hint not in {"range", "list"} and not uniform_step)
                if wants_list:
                    _set_model_and_widget(f"{base_key}_sweep_kind", "list")
                    _set_model_and_widget(f"{base_key}_list", list_str)
                else:
                    vmin = float(min(vals))
                    vmax = float(max(vals))
                    _set_model_and_widget(f"{base_key}_sweep_kind", "range")
                    _set_model_and_widget(f"{base_key}_min", float(vmin))
                    _set_model_and_widget(f"{base_key}_max", float(vmax))
                    _set_model_and_widget(f"{base_key}_step", float(vstep))
            except Exception:
                pass
        else:
            # Non-numeric sweeps (select/list) use the comma list UI.
            _set_model_and_widget(f"{base_key}_list", list_str)


def _apply_run_config_to_backtest_ui(run_row: Dict[str, Any], *, settings: Dict[str, Any]) -> None:
    """Prefill Backtest page widgets from a stored backtest run row."""

    metadata = _extract_metadata(run_row) if isinstance(run_row, dict) else {}
    config_dict = _extract_config_dict(run_row) if isinstance(run_row, dict) else None
    if not isinstance(config_dict, dict):
        return

    # Strategy config -> bt_ keys
    general = config_dict.get("general") if isinstance(config_dict.get("general"), dict) else {}
    trend = config_dict.get("trend") if isinstance(config_dict.get("trend"), dict) else {}
    bbands = config_dict.get("bbands") if isinstance(config_dict.get("bbands"), dict) else {}
    macd = config_dict.get("macd") if isinstance(config_dict.get("macd"), dict) else {}
    rsi = config_dict.get("rsi") if isinstance(config_dict.get("rsi"), dict) else {}
    dca = config_dict.get("dca") if isinstance(config_dict.get("dca"), dict) else {}
    exits = config_dict.get("exits") if isinstance(config_dict.get("exits"), dict) else {}
    dynamic = {}
    try:
        dynamic = general.get("dynamic_activation") if isinstance(general.get("dynamic_activation"), dict) else {}
    except Exception:
        dynamic = {}

    def _f(d: dict, k: str, default: Any) -> Any:
        v = d.get(k, default)
        return default if v is None else v

    _set_model_and_widget("bt_max_entries", int(_f(general, "max_entries", 5)))
    _set_model_and_widget("bt_entry_cooldown_min", int(_f(general, "entry_cooldown_min", 10)))
    _set_model_and_widget("bt_entry_timeout_min", int(_f(general, "entry_timeout_min", 10)))
    _set_model_and_widget(
        "bt_initial_entry_sizing_mode",
        str(_safe_initial_entry_sizing_mode(_f(general, "initial_entry_sizing_mode", InitialEntrySizingMode.PCT_BALANCE.value)).value),
    )
    _set_model_and_widget("bt_initial_entry_balance_pct", float(_f(general, "initial_entry_balance_pct", 10.0)))
    _set_model_and_widget("bt_initial_entry_fixed_usd", float(_f(general, "initial_entry_fixed_usd", 100.0)))
    _set_model_and_widget("bt_use_indicator_consensus", bool(_f(general, "use_indicator_consensus", True)))
    _set_model_and_widget(
        "bt_lock_atr_on_entry",
        bool(_f(general, "lock_atr_on_entry_for_dca", _f(general, "lock_atr_on_entry", False))),
    )
    _set_model_and_widget("bt_use_avg_entry_for_dca_base", bool(_f(general, "use_avg_entry_for_dca_base", True)))
    # Dynamic activation (new schema uses general.dynamic_activation.{entry,dca,tp}_pct).
    dyn_entry = float(_f(dynamic, "entry_pct", _f(general, "dynamic_activation_pct", _f(general, "global_dynamic_activation_pct", 0.0))))
    dyn_dca = float(_f(dynamic, "dca_pct", dyn_entry))
    dyn_tp = float(_f(dynamic, "tp_pct", dyn_entry))
    _set_model_and_widget("bt_dyn_entry_pct", dyn_entry)
    _set_model_and_widget("bt_dyn_dca_pct", dyn_dca)
    _set_model_and_widget("bt_dyn_tp_pct", dyn_tp)
    _set_model_and_widget("bt_global_dyn_pct", dyn_entry)

    # Order style labels (Backtest UI stores label; snapshot stores OrderStyle value string)
    try:
        entry_style = _safe_order_style(_f(general, "entry_order_style", OrderStyle.MARKET.value))
        exit_style = _safe_order_style(_f(general, "exit_order_style", OrderStyle.MARKET.value))
        inv = {v: k for k, v in ORDER_STYLE_OPTIONS.items()}
        _set_model_and_widget("bt_entry_order_style_label", inv.get(entry_style, "Maker or Cancel"))
        _set_model_and_widget("bt_exit_order_style_label", inv.get(exit_style, "Maker or Cancel"))
    except Exception:
        pass

    allow_long = bool(_f(general, "allow_long", True))
    allow_short = bool(_f(general, "allow_short", False))
    st.session_state["bt_allow_long"] = allow_long
    st.session_state["bt_allow_short"] = allow_short
    td = "Short only" if (allow_short and not allow_long) else "Long only"
    # Trade direction radio uses a single session_state key (no separate widget key)
    # to avoid Streamlit's "default value + Session State API" warning.
    st.session_state["bt_trade_direction"] = td

    _set_model_and_widget("bt_prefer_bbo_maker", bool(_f(general, "prefer_bbo_maker", bool(settings.get("prefer_bbo_maker", True)))))
    _set_model_and_widget("bt_bbo_queue_level", int(_f(general, "bbo_queue_level", int(settings.get("bbo_queue_level", 1) or 1))))

    # Optional MA direction gate for entries (entry-only)
    _set_model_and_widget("bt_use_ma_direction", bool(_f(general, "use_ma_direction", False)))

    _set_model_and_widget("bt_ma_interval_min", int(_f(trend, "ma_interval_min", 120)))
    _set_model_and_widget("bt_ma_length", int(_f(trend, "ma_len", int(_f(general, "ma_length", 200) or 200))))
    _set_model_and_widget("bt_ma_type", str(_f(trend, "ma_type", str(_f(general, "ma_type", "Sma") or "Sma")) or "Sma"))
    _set_model_and_widget("bt_bb_interval_min", int(_f(bbands, "interval_min", 60)))
    _set_model_and_widget("bt_bb_length", int(_f(bbands, "length", 20)))
    _set_model_and_widget("bt_macd_interval_min", int(_f(macd, "interval_min", 360)))
    _set_model_and_widget("bt_macd_fast", int(_f(macd, "fast", 12)))
    _set_model_and_widget("bt_macd_slow", int(_f(macd, "slow", 26)))
    _set_model_and_widget("bt_macd_signal", int(_f(macd, "signal", 7)))
    _set_model_and_widget("bt_rsi_interval_min", int(_f(rsi, "interval_min", 1440)))
    _set_model_and_widget("bt_rsi_length", int(_f(rsi, "length", 14)))
    _set_model_and_widget("bt_rsi_buy_level", float(_f(rsi, "buy_level", 30)))
    _set_model_and_widget("bt_rsi_sell_level", float(_f(rsi, "sell_level", 70)))

    _set_model_and_widget("bt_bb_dev_up", float(_f(bbands, "dev_up", 2.0)))
    _set_model_and_widget("bt_bb_dev_down", float(_f(bbands, "dev_down", 2.0)))
    _set_model_and_widget("bt_bb_ma_type", str(_f(bbands, "ma_type", "Sma") or "Sma"))
    _set_model_and_widget("bt_bb_deviation", float(_f(bbands, "deviation", 0.2)))
    _set_model_and_widget("bt_bb_require_fcc", bool(_f(bbands, "require_fcc", False)))
    _set_model_and_widget("bt_bb_reset_middle", bool(_f(bbands, "reset_middle", False)))
    _set_model_and_widget("bt_bb_allow_mid_sells", bool(_f(bbands, "allow_mid_sells", False)))

    _set_model_and_widget("bt_base_deviation_pct", float(_f(dca, "base_deviation_pct", 1.0)))
    _set_model_and_widget("bt_deviation_multiplier", float(_f(dca, "deviation_multiplier", 2.0)))
    _set_model_and_widget("bt_volume_multiplier", float(_f(dca, "volume_multiplier", 2.0)))

    # Exits: hydrate both canonical keys (sl_mode/tp_mode/sl_pct/tp_pct/atr_period)
    # and legacy UI keys (fixed_* / tp_atr_* / trail_stop_pct) for migration-safe UX.
    sl_mode_raw = str(_f(exits, "sl_mode", "") or "").strip().upper()
    if sl_mode_raw not in {StopLossMode.PCT.value, StopLossMode.ATR.value}:
        sl_mode_raw = StopLossMode.PCT.value
    _set_model_and_widget("bt_sl_mode", sl_mode_raw)

    sl_pct_val = float(_f(exits, "sl_pct", _f(exits, "fixed_sl_pct", 5.0)))
    trail_act_pct_val = float(_f(exits, "trail_activation_pct", 5.0))
    trail_dist_pct_val = float(_f(exits, "trail_distance_pct", _f(exits, "trail_stop_pct", 7.0)))
    _set_model_and_widget("bt_sl_pct", sl_pct_val)
    _set_model_and_widget("bt_fixed_sl_pct", sl_pct_val)
    _set_model_and_widget("bt_trail_activation_pct", trail_act_pct_val)
    _set_model_and_widget("bt_trail_distance_pct", trail_dist_pct_val)
    _set_model_and_widget("bt_trail_stop_pct", trail_dist_pct_val)

    atr_period_val = int(_f(exits, "atr_period", _f(exits, "tp_atr_period", 14)) or 14)
    _set_model_and_widget("bt_atr_period", atr_period_val)
    _set_model_and_widget("bt_tp_atr_period", atr_period_val)
    _set_model_and_widget("bt_sl_atr_mult", float(_f(exits, "sl_atr_mult", 3.0)))
    _set_model_and_widget("bt_trail_activation_atr_mult", float(_f(exits, "trail_activation_atr_mult", 1.0)))
    _set_model_and_widget("bt_trail_distance_atr_mult", float(_f(exits, "trail_distance_atr_mult", 2.0)))

    tp_mode_raw = str(_f(exits, "tp_mode", "") or "").strip().upper()
    if tp_mode_raw not in {TakeProfitMode.PCT.value, TakeProfitMode.ATR.value}:
        tp_mode_raw = TakeProfitMode.ATR.value if bool(_f(exits, "use_atr_tp", True)) else TakeProfitMode.PCT.value
    _set_model_and_widget("bt_tp_mode", tp_mode_raw)
    tp_pct_val = float(_f(exits, "tp_pct", _f(exits, "fixed_tp_pct", 1.0)))
    tp_atr_mult_val = float(_f(exits, "tp_atr_mult", _f(exits, "tp_atr_multiple", 10.0)))
    _set_model_and_widget("bt_tp_pct", tp_pct_val)
    _set_model_and_widget("bt_fixed_tp_pct", tp_pct_val)
    _set_model_and_widget("bt_tp_atr_mult", tp_atr_mult_val)
    _set_model_and_widget("bt_tp_atr_multiple", tp_atr_mult_val)
    _set_model_and_widget("bt_use_atr_tp", tp_mode_raw == TakeProfitMode.ATR.value)

    _set_model_and_widget("bt_tp_replace_threshold_pct", float(_f(exits, "tp_replace_threshold_pct", 0.05)))

    # Backtest leverage (used for ROI-on-margin style metrics). Stored as `_futures.leverage` when != 1.
    lev_bt = None
    try:
        fut = config_dict.get("_futures") if isinstance(config_dict.get("_futures"), dict) else None
        if isinstance(fut, dict) and fut.get("leverage") is not None:
            lev_bt = int(float(fut.get("leverage")))
    except Exception:
        lev_bt = None
    if lev_bt is None:
        try:
            mlev = metadata.get("leverage")
            if mlev is not None:
                lev_bt = int(float(mlev))
        except Exception:
            lev_bt = None
    if lev_bt is None:
        lev_bt = 1
    try:
        lev_bt = max(1, int(lev_bt))
    except Exception:
        lev_bt = 1
    _set_model_and_widget("bt_backtest_leverage", int(lev_bt))

    _apply_backtest_data_settings_to_ui(run_row=run_row, metadata=metadata, settings=settings)


def _ensure_runs_grid_action_columns(df: "pd.DataFrame") -> "pd.DataFrame":
    """Ensure the runs grid (Results) has action plumbing columns."""

    try:
        if df is None or df.empty:
            return df
    except Exception:
        return df

    if "run_id" not in getattr(df, "columns", []):
        return df

    # Copy only if we need to add/overwrite columns.
    try:
        needs_copy = any(c not in df.columns for c in ("create_bot_url", "actions", "action_click"))
    except Exception:
        needs_copy = True
    if needs_copy:
        try:
            df = df.copy()
        except Exception:
            pass

    try:
        run_id_series = df["run_id"].astype(str)
    except Exception:
        run_id_series = df["run_id"].apply(lambda v: "" if v is None else str(v))

    df["create_bot_url"] = run_id_series.apply(
        lambda rid: f"?page=Live%20Bots&create_live_bot_run_id={rid}"
    )
    df["actions"] = ""
    # Hidden internal field: JS renderer writes here to trigger a Streamlit rerun.
    df["action_click"] = ""
    return df


def _parse_action_click_token(token: str) -> tuple[Optional[str], Optional[str]]:
    """Parse JS-emitted action click token.

        Expected formats:
            - create_bot:<run_id>:<epoch_ms>
            - open_sweep:<run_id>:<epoch_ms>
    """

    try:
        t = str(token or "").strip()
    except Exception:
        return None, None
    if not t:
        return None, None

    parts = t.split(":")
    if len(parts) < 2:
        return None, None
    action = (parts[0] or "").strip()
    run_id = (parts[1] or "").strip()
    if not action or not run_id:
        return None, None
    if action not in {"create_bot", "open_sweep"}:
        return None, None
    return action, run_id


def _parse_sweep_click_token(token: str) -> Optional[str]:
    """Parse JS-emitted sweep click token.

    Expected format:
      - open_sweep:<sweep_id>:<epoch_ms>
    Returns sweep_id as string.
    """

    try:
        t = str(token or "").strip()
    except Exception:
        return None
    if not t:
        return None
    parts = t.split(":")
    if len(parts) < 2:
        return None
    action = (parts[0] or "").strip()
    sweep_id = (parts[1] or "").strip()
    if action != "open_sweep" or not sweep_id:
        return None
    return sweep_id


def _parse_sweep_action_click_token(token: str) -> tuple[Optional[str], Optional[str]]:
    """Parse JS-emitted sweep action click token.

        Expected format:
            - open_sweep:<sweep_id>:<epoch_ms>
    """

    try:
        t = str(token or "").strip()
    except Exception:
        return None, None
    if not t:
        return None, None
    parts = t.split(":")
    if len(parts) < 2:
        return None, None
    action = (parts[0] or "").strip()
    sweep_id = (parts[1] or "").strip()
    if action != "open_sweep":
        return None, None
    if not sweep_id:
        return None, None
    return action, sweep_id


def _reset_create_live_bot_form_state() -> None:
    """Clear widget state for the 'Create bot from run' form.

    Streamlit widget keys are sticky; when switching between different runs, we want
    the defaults derived from the selected run to actually take effect.
    """

    for k in (
        "live_bot_strategy_name",
        "live_bot_name",
        "live_bot_exchange",
        "live_bot_symbol",
        "live_bot_timeframe",
        "live_bot_prefer_bbo",
        "live_bot_bbo_level",
        "live_bot_initial_entry_sizing_mode",
        "live_bot_initial_entry_balance_pct",
        "live_bot_initial_entry_fixed_usd",
        "live_bot_entry_cooldown_min",
        "live_bot_entry_timeout_min",
        "live_bot_exit_timeout_min",
        "live_bot_account_id",
        "live_bot_desired_status",
        "live_bot_create_mode",
        "live_bot_primary_leg",
        "live_bot_apply_leverage",
        "live_bot_leverage",
        "live_bot_max_leverage",
    ):
        st.session_state.pop(k, None)


def render_dragon_strategy_config_view(config_dict: Dict[str, Any], *, key_prefix: str = "live_view") -> None:
    """Render a read-only view of Dragon strategy config in the same shape as Backtest inputs."""

    try:
        cfg = _dragon_config_from_snapshot(config_dict)
    except Exception:
        st.warning("Unable to parse strategy config snapshot.")
        st.json(config_dict)
        return

    def _k(name: str) -> str:
        return f"{key_prefix}__{name}"

    with st.container(border=True):
        st.markdown("### Core / Position")
        row = st.columns(3)
        row[0].number_input("Max entries", value=int(cfg.general.max_entries), disabled=True, key=_k("max_entries"))
        if str(getattr(cfg.general, "initial_entry_sizing_mode", InitialEntrySizingMode.PCT_BALANCE)).lower().endswith("fixed_usd"):
            row[1].number_input(
                "Initial entry ($ notional)",
                value=float(getattr(cfg.general, "initial_entry_fixed_usd", 0.0) or 0.0),
                disabled=True,
                key=_k("initial_entry_fixed_usd"),
            )
        else:
            row[1].number_input(
                "Initial entry (% of balance)",
                value=float(cfg.general.initial_entry_balance_pct),
                disabled=True,
                key=_k("initial_entry_balance_pct"),
            )
        row[2].checkbox(
            "Use indicator consensus",
            value=bool(cfg.general.use_indicator_consensus),
            disabled=True,
            key=_k("use_indicator_consensus"),
        )

        row2 = st.columns(3)
        row2[0].checkbox(
            "Allow long",
            value=bool(cfg.general.allow_long),
            disabled=True,
            key=_k("allow_long"),
        )
        row2[1].checkbox(
            "Allow short",
            value=bool(cfg.general.allow_short),
            disabled=True,
            key=_k("allow_short"),
        )
        row2[2].checkbox(
            "Lock ATR on entry (DCA)",
            value=bool(cfg.general.lock_atr_on_entry_for_dca),
            disabled=True,
            key=_k("lock_atr"),
        )

        row3 = st.columns(3)
        row3[0].number_input(
            "Entry cooldown (min)",
            value=int(cfg.general.entry_cooldown_min),
            disabled=True,
            key=_k("entry_cooldown_min"),
        )
        row3[1].number_input(
            "Entry timeout (min)",
            value=int(cfg.general.entry_timeout_min),
            disabled=True,
            key=_k("entry_timeout_min"),
        )
        row3[2].number_input(
            "Exit timeout (min)",
            value=int(cfg.general.exit_timeout_min),
            disabled=True,
            key=_k("exit_timeout_min"),
        )

        st.markdown("#### Dynamic activation")
        dyn = cfg.general.dynamic_activation
        dyn_row = st.columns(3)
        dyn_row[0].number_input("Entry band (%)", value=float(dyn.entry_pct), disabled=True, key=_k("dyn_entry"))
        dyn_row[1].number_input("DCA band (%)", value=float(dyn.dca_pct), disabled=True, key=_k("dyn_dca"))
        dyn_row[2].number_input("TP band (%)", value=float(dyn.tp_pct), disabled=True, key=_k("dyn_tp"))

    with st.container(border=True):
        st.markdown("### Entry & Indicators")
        ma = st.columns(3)
        ma[0].number_input("MA interval (min)", value=int(cfg.trend.ma_interval_min), disabled=True, key=_k("ma_i"))
        ma[1].number_input("MA length", value=int(cfg.trend.ma_len), disabled=True, key=_k("ma_l"))
        ma[2].selectbox("MA type", options=["Sma", "Ema"], index=0 if str(cfg.trend.ma_type).lower().startswith("s") else 1, disabled=True, key=_k("ma_t"))

        bb1 = st.columns(3)
        bb1[0].number_input("BB interval (min)", value=int(cfg.bbands.interval_min), disabled=True, key=_k("bb_i"))
        bb1[1].number_input("BB length", value=int(cfg.bbands.length), disabled=True, key=_k("bb_l"))
        bb1[2].selectbox("BB MA type", options=["Sma", "Ema"], index=0 if str(cfg.bbands.ma_type).lower().startswith("s") else 1, disabled=True, key=_k("bb_mt"))
        bb2 = st.columns(3)
        bb2[0].number_input("BB dev up", value=float(cfg.bbands.dev_up), disabled=True, key=_k("bb_du"))
        bb2[1].number_input("BB dev down", value=float(cfg.bbands.dev_down), disabled=True, key=_k("bb_dd"))
        bb2[2].number_input("BB deviation", value=float(cfg.bbands.deviation), disabled=True, key=_k("bb_dev"))
        bb3 = st.columns(3)
        bb3[0].checkbox("Require FCC", value=bool(cfg.bbands.require_fcc), disabled=True, key=_k("bb_fcc"))
        bb3[1].checkbox("Reset middle", value=bool(cfg.bbands.reset_middle), disabled=True, key=_k("bb_rm"))
        bb3[2].checkbox("Allow mid sells", value=bool(cfg.bbands.allow_mid_sells), disabled=True, key=_k("bb_ms"))

        mac = st.columns(4)
        mac[0].number_input("MACD interval (min)", value=int(cfg.macd.interval_min), disabled=True, key=_k("mac_i"))
        mac[1].number_input("MACD fast", value=int(cfg.macd.fast), disabled=True, key=_k("mac_f"))
        mac[2].number_input("MACD slow", value=int(cfg.macd.slow), disabled=True, key=_k("mac_s"))
        mac[3].number_input("MACD signal", value=int(cfg.macd.signal), disabled=True, key=_k("mac_sig"))

        r = st.columns(4)
        r[0].number_input("RSI interval (min)", value=int(cfg.rsi.interval_min), disabled=True, key=_k("rsi_i"))
        r[1].number_input("RSI length", value=int(cfg.rsi.length), disabled=True, key=_k("rsi_l"))
        r[2].number_input("RSI buy level", value=float(cfg.rsi.buy_level), disabled=True, key=_k("rsi_b"))
        r[3].number_input("RSI sell level", value=float(cfg.rsi.sell_level), disabled=True, key=_k("rsi_s"))

    with st.container(border=True):
        st.markdown("### DCA")
        d = st.columns(3)
        d[0].number_input("Base deviation (%)", value=float(cfg.dca.base_deviation_pct), disabled=True, key=_k("dca_bd"))
        d[1].number_input("Deviation multiplier", value=float(cfg.dca.deviation_multiplier), disabled=True, key=_k("dca_dm"))
        d[2].number_input("Volume multiplier", value=float(cfg.dca.volume_multiplier), disabled=True, key=_k("dca_vm"))

    with st.container(border=True):
        st.markdown("### Exits")
        ex = cfg.exits

        def _get_attr(obj: Any, name: str, default: Any) -> Any:
            try:
                if hasattr(obj, name):
                    return getattr(obj, name)
            except Exception:
                return default
            return default

        # Backward-compat mapping (older configs/UI fields)
        sl_pct = float(_get_attr(ex, "sl_pct", _get_attr(ex, "fixed_sl_pct", 0.0)) or 0.0)
        trail_activation_pct = float(_get_attr(ex, "trail_activation_pct", 0.0) or 0.0)
        trail_distance_pct = float(_get_attr(ex, "trail_distance_pct", _get_attr(ex, "trail_stop_pct", 0.0)) or 0.0)

        sl_mode = _get_attr(ex, "sl_mode", StopLossMode.PCT)
        tp_mode = _get_attr(ex, "tp_mode", None)
        if tp_mode is None:
            use_atr_tp = bool(_get_attr(ex, "use_atr_tp", False))
            tp_mode = TakeProfitMode.ATR if use_atr_tp else TakeProfitMode.PCT
        tp_pct = float(_get_attr(ex, "tp_pct", _get_attr(ex, "fixed_tp_pct", 0.0)) or 0.0)
        tp_atr_mult = float(_get_attr(ex, "tp_atr_mult", _get_attr(ex, "tp_atr_multiple", 0.0)) or 0.0)

        atr_period = int(_get_attr(ex, "atr_period", _get_attr(ex, "tp_atr_period", 14)) or 14)
        sl_atr_mult = float(_get_attr(ex, "sl_atr_mult", 0.0) or 0.0)
        trail_activation_atr_mult = float(_get_attr(ex, "trail_activation_atr_mult", 0.0) or 0.0)
        trail_distance_atr_mult = float(_get_attr(ex, "trail_distance_atr_mult", 0.0) or 0.0)
        tp_replace_threshold_pct = float(_get_attr(ex, "tp_replace_threshold_pct", 0.0) or 0.0)

        top = st.columns(2)
        # sl_mode is an Enum; render as text without making it editable.
        top[0].selectbox(
            "Stop loss mode",
            options=[StopLossMode.PCT, StopLossMode.ATR],
            index=0 if str(sl_mode).endswith("PCT") else 1,
            disabled=True,
            key=_k("sl_mode"),
        )
        top[1].selectbox(
            "Take profit mode",
            options=[TakeProfitMode.PCT, TakeProfitMode.ATR],
            index=0 if str(tp_mode).endswith("PCT") else 1,
            disabled=True,
            key=_k("tp_mode"),
        )

        if str(sl_mode).endswith("ATR"):
            sl = st.columns(4)
            sl[0].number_input("ATR period", value=int(atr_period), disabled=True, key=_k("atr_period"))
            sl[1].number_input("SL ATR multiple", value=float(sl_atr_mult), disabled=True, key=_k("sl_atr_mult"))
            sl[2].number_input(
                "Trail activation (ATR)", value=float(trail_activation_atr_mult), disabled=True, key=_k("trail_act_atr")
            )
            sl[3].number_input(
                "Trail distance (ATR)", value=float(trail_distance_atr_mult), disabled=True, key=_k("trail_dist_atr")
            )
        else:
            sl = st.columns(3)
            sl[0].number_input("SL (%)", value=float(sl_pct), disabled=True, key=_k("sl_pct"))
            sl[1].number_input(
                "Trailing activation (%)", value=float(trail_activation_pct), disabled=True, key=_k("tr_a")
            )
            sl[2].number_input("Trailing distance (%)", value=float(trail_distance_pct), disabled=True, key=_k("tr_d"))

        if str(tp_mode).endswith("ATR"):
            tp = st.columns(2)
            tp[0].number_input("TP ATR multiple", value=float(tp_atr_mult), disabled=True, key=_k("tp_atr_mult"))
            tp[1].number_input(
                "TP replace threshold (%)",
                value=float(tp_replace_threshold_pct),
                disabled=True,
                key=_k("tp_r"),
            )
        else:
            tp = st.columns(2)
            tp[0].number_input("TP (%)", value=float(tp_pct), disabled=True, key=_k("tp_pct"))
            tp[1].number_input(
                "TP replace threshold (%)",
                value=float(tp_replace_threshold_pct),
                disabled=True,
                key=_k("tp_r"),
            )



def main() -> None:
    icon_path = None
    try:
        repo_root = None
        try:
            env_root = os.environ.get("PROJECT_DRAGON_ROOT") or os.environ.get("DRAGON_ASSETS_ROOT")
            if env_root:
                repo_root = Path(env_root).expanduser().resolve()
        except Exception:
            repo_root = None
        if repo_root is None:
            candidates = [Path.cwd(), Path(__file__).resolve()]
            for base in candidates:
                for parent in [base] + list(base.parents):
                    if (parent / "app" / "assets").exists():
                        repo_root = parent
                        break
                if repo_root is not None:
                    break
        if repo_root is None:
            for fallback in (Path("/app"), Path("/workspaces/project_dragon")):
                if (fallback / "app" / "assets").exists() or (fallback / "config").exists():
                    repo_root = fallback
                    break
        if repo_root is None:
            repo_root = Path(__file__).resolve().parents[2]

        config_root = repo_root / "config"
        override_candidates = [
            config_root / "favicon.png",
            config_root / "favicon.ico",
        ]
        asset_candidates = [
            repo_root / "app" / "assets" / "project_dragon_favicon.png",
            repo_root / "app" / "assets" / "project_dragon_icon.svg",
        ]
        icon_candidate = None
        for c in override_candidates + asset_candidates:
            if c.exists():
                icon_candidate = c
                break
        if icon_candidate is not None:
            try:
                from PIL import Image  # type: ignore

                icon_path = Image.open(str(icon_candidate))
            except Exception:
                icon_path = str(icon_candidate)
    except Exception:
        icon_path = None
    st.set_page_config(page_title="Project Dragon", layout="wide", page_icon=(icon_path or "🐉"))

    dsn = str(get_database_url() or "").strip()
    if not is_postgres_dsn(dsn):
        st.error("DRAGON_DATABASE_URL must be set to a postgres:// or postgresql:// URL.")
        st.stop()

    try:
        if not st.session_state.get("_tabler_assets_warned", False) and not tabler_assets_available():
            st.warning(
                "Tabler icon assets are missing. UI icons may not render correctly. "
                "Ensure app/assets/ui_icons/tabler is present in the container."
            )
            st.session_state["_tabler_assets_warned"] = True
    except Exception:
        pass
    try:
        if not st.session_state.get("_tabler_assets_logged", False):
            inv = tabler_icon_inventory()
            try:
                _ = get_tabler_svg("circle-check")
                ok_icon = True
            except Exception:
                ok_icon = False
            logging.getLogger(__name__).info(
                "tabler_assets inventory=%s ok_icon=%s",
                inv,
                ok_icon,
            )
            st.session_state["_tabler_assets_logged"] = True
    except Exception:
        pass
    inject_trade_stream_css(max_width_px=1920)

    # Optional perf profiling (dev-only): enable with DRAGON_PROFILE=1.
    if _truthy_env("DRAGON_PROFILE"):
        st.sidebar.checkbox(
            "Enable profiling",
            value=bool(st.session_state.get("_profile_enabled", True)),
            key="_profile_enabled",
        )
    _perf_reset()

    with _perf("init_db"):
        db_path = init_db()

    # Option B integrity: in debug mode, warn if any runs are missing a details row.
    # This check is intentionally opt-in since it can scan the runs table.
    if (os.environ.get("DRAGON_DEBUG") or "").strip() in {"1", "true", "yes", "on"}:
        try:
            with open_db_connection() as check_conn:
                missing = count_backtest_runs_missing_details(check_conn)
                if missing > 0:
                    sample = []
                    try:
                        sample = sample_backtest_runs_missing_details(check_conn, limit=5)
                    except Exception:
                        sample = []
                    msg = f"{missing} backtest runs are missing details rows (backtest_run_details)."
                    if sample:
                        msg += f" Sample run_ids: {', '.join(sample)}"
                    st.warning(msg)
        except Exception:
            pass
    with _perf("get_app_settings"):
        with open_db_connection() as settings_conn:
            settings = get_app_settings(settings_conn)

    with _perf("init_state_defaults"):
        init_state_defaults(settings)

    # Global Filters session state (Phase 1)
    ensure_global_filters_initialized()

    # Backtests/sweeps are executed by worker processes, not Streamlit.

    query_params = st.query_params
    qp_page = (_extract_query_param(query_params, "page") or "").strip()
    qp_view = (_extract_query_param(query_params, "view") or "").strip()
    qp_run_id = _extract_query_param(query_params, "run_id")
    qp_sweep_id = _extract_query_param(query_params, "sweep_id")
    qp_bot_id = _extract_query_param(query_params, "bot_id")
    qp_create_live_bot_run_id = _extract_query_param(query_params, "create_live_bot_run_id")

    if qp_run_id:
        st.session_state["deep_link_run_id"] = qp_run_id
    if qp_sweep_id:
        st.session_state["deep_link_sweep_id"] = qp_sweep_id

    if qp_create_live_bot_run_id:
        st.session_state["create_live_bot_run_id"] = str(qp_create_live_bot_run_id).strip()
        st.session_state.pop("selected_bot_id", None)
        st.session_state["current_page"] = "Live Bots"

    # Deep-link enforcement: only force bot detail when we're on the Live Bots page.
    # This ensures sidebar navigation away from a bot detail isn't overridden.
    qp_page_norm = (qp_page or "").strip().lower()
    is_live_page = qp_page_norm in {"", "live", "bots", "live bots"}
    if qp_bot_id and is_live_page:
        try:
            st.session_state["selected_bot_id"] = int(qp_bot_id)
        except (TypeError, ValueError):
            st.session_state["selected_bot_id"] = qp_bot_id
        st.session_state["current_page"] = "Live Bots"
        if (qp_view or "").lower() != "bot" or qp_page_norm != "live bots":
            _update_query_params(page="Live Bots", view="bot", bot_id=qp_bot_id)
            st.rerun()

    # Sidebar navigation only (grouped buttons). No run/bot settings in the sidebar.
    def _page_from_query(page_raw: str) -> str:
        p = (page_raw or "").strip().lower()
        if p in {"live", "bots", "live bots"}:
            return "Live Bots"
        if p in {"run", "run detail", "run details", "run_detail", "run_details"}:
            return "Run"
        if p in {"dashboard", "home", ""}:
            return "Dashboard"
        if p in {"dashboard", "home"}:
            return "Dashboard"
        if p in {"backtest", "backtesting"}:
            return "Backtest"
        if p in {"results"}:
            return "Results"
        if p in {"runs explorer", "runs_explorer", "runs-explorer", "run explorer", "run_explorer", "runs"}:
            return "Runs Explorer"
        if p in {"sweeps"}:
            return "Sweeps"
        if p in {"credentials", "creds"}:
            return "Credentials"
        if p in {"accounts", "account", "trading accounts", "trading_accounts"}:
            return "Accounts"
        if p in {"jobs"}:
            return "Jobs"
        if p in {"candle cache", "cache"}:
            return "Candle Cache"
        if p in {"settings"}:
            return "Settings"
        return "Backtest"

    if "current_page" not in st.session_state:
        st.session_state["current_page"] = _page_from_query(qp_page)

    # Prefer deep links when page is not explicit (unless bot deep link already forced rerun above).
    # Important: only apply this when we are still on the default landing page (Backtest).
    # Otherwise it can override explicit in-app navigation (e.g., Results → Open sweep).
    if not qp_page and not qp_bot_id and st.session_state.get("current_page") in {None, "Dashboard"}:
        if st.session_state.get("deep_link_run_id"):
            st.session_state["current_page"] = "Results"
        elif st.session_state.get("deep_link_sweep_id"):
            st.session_state["current_page"] = "Sweeps"

    # If URL specifies a page explicitly, honor it only when the URL changes.
    # Otherwise it will override sidebar navigation on every rerun.
    if qp_page and not qp_bot_id:
        desired_from_qp = _page_from_query(qp_page)
        last_qp_page = st.session_state.get("_last_qp_page")
        if last_qp_page != qp_page or "current_page" not in st.session_state:
            st.session_state["current_page"] = desired_from_qp
        st.session_state["_last_qp_page"] = qp_page

    current_page = st.session_state.get("current_page", "Dashboard")

    # Sidebar header (render once)
    st.sidebar.markdown(
        """
        <div style="margin-top: 0.25rem; margin-bottom: 0.75rem;">
            <div style="font-size: 1.15rem; font-weight: 700; line-height: 1.2;">Project Dragon</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    def _nav_button(
        label: str,
        page_param: str,
        *,
        display_label: str | None = None,
        clear_bot: bool = True,
    ) -> None:
        # Read from session_state at render-time to avoid the active indicator
        # being one click behind.
        active = (st.session_state.get("current_page", "Dashboard") == label)
        shown = display_label if display_label is not None else label
        if st.sidebar.button(
            shown,
            width="stretch",
            key=f"nav_btn_{page_param}",
            type="primary" if active else "secondary",
        ):
            st.session_state["current_page"] = label
            if clear_bot:
                st.session_state.pop("selected_bot_id", None)
            # NOTE: Do not rewrite URL on navigation clicks.
            # Some deployments/proxies treat URL changes as a full refresh,
            # which would reset Streamlit session_state and break input persistence.
            st.rerun()

    _nav_button("Dashboard", "dashboard")

    st.sidebar.markdown("### Backtesting")
    _nav_button("Backtest", "backtest")
    _nav_button("Results", "results")
    _nav_button("Runs Explorer", "runs_explorer")
    _nav_button("Sweeps", "sweeps")

    st.sidebar.markdown("### Live")
    _nav_button("Live Bots", "live", display_label="Bots")

    st.sidebar.markdown("### Tools")
    _nav_button("Accounts", "accounts")
    _nav_button("Jobs", "jobs")
    _nav_button("Candle Cache", "candle_cache")
    _nav_button("Settings", "settings")

    # Re-read current_page after sidebar clicks (button triggers a rerun already).
    current_page = st.session_state.get("current_page", "Dashboard")

    # Keep URL ingestion only: we honor query params on load, but we don't
    # automatically rewrite them during navigation to preserve session_state.
    jobs_refresh_seconds = max(0.5, float(settings.get("jobs_refresh_seconds", 2.0)))
    if "jobs_refresh_seconds" not in st.session_state:
        st.session_state["jobs_refresh_seconds"] = jobs_refresh_seconds

    user_email = get_current_user_email()
    with open_db_connection() as _id_conn:
        user_id = get_or_create_user_id(user_email, conn=_id_conn)
        # Global presentation timezone (per-user). This does not affect DB storage (UTC).
        try:
            st.session_state["display_timezone_name"] = get_display_tz_name(_id_conn, str(user_id))
        except Exception:
            st.session_state.setdefault("display_timezone_name", DEFAULT_DISPLAY_TZ_NAME)
    st.sidebar.caption(f"User: {user_email}")
    st.sidebar.caption(f"v{__version__}")

    # --- Global Filters (sidebar) -------------------------------------
    with _job_conn() as _gf_conn:
        # Options are best-effort; keep them light and derived from existing tables.
        try:
            user_id_gf = get_or_create_user_id(get_current_user_email(), conn=_gf_conn)
        except Exception:
            user_id_gf = user_id

        gf = get_global_filters()
        st.sidebar.markdown("---")
        header_cols = st.sidebar.columns([6, 1])
        header_cols[0].markdown("### Global Filters")
        enabled = header_cols[1].checkbox(
            "Enabled",
            value=bool(gf.get("enabled", True)),
            key="gf_enabled",
            label_visibility="collapsed",
        )

        # Run creation window
        preset_options = ["All Time", "Last Week", "Last Month", "Last 3 Months", "Last 6 Months"]
        preset_default = str(gf.get("created_at_preset") or "All Time").strip() or "All Time"
        if preset_default not in preset_options:
            preset_default = "All Time"

        # Auto-widen when the default time window yields no runs.
        if preset_default != "All Time":
            if not (gf.get("account_ids") or gf.get("exchanges") or gf.get("market_types") or gf.get("symbols") or gf.get("strategies") or gf.get("timeframes") or gf.get("date_range")):
                try:
                    start_dt = None
                    end_dt = None
                    now = datetime.now(timezone.utc)
                    today = now.date()
                    if preset_default == "Last Week":
                        start_d = today - timedelta(days=7)
                    elif preset_default == "Last Month":
                        start_d = today - timedelta(days=30)
                    elif preset_default == "Last 3 Months":
                        start_d = today - timedelta(days=90)
                    elif preset_default == "Last 6 Months":
                        start_d = today - timedelta(days=180)
                    else:
                        start_d = None
                    if start_d is not None:
                        start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
                        end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)
                    if start_dt is not None and end_dt is not None:
                        cur = _gf_conn.execute(
                            "SELECT COUNT(1) FROM backtest_runs WHERE created_at >= ? AND created_at <= ?",
                            (start_dt.isoformat(), end_dt.isoformat()),
                        )
                        row = cur.fetchone() if cur is not None else None
                        if row and int(row[0] or 0) == 0:
                            preset_default = "All Time"
                except Exception:
                    pass
        created_at_preset = st.sidebar.selectbox(
            "Created",
            options=preset_options,
            index=preset_options.index(preset_default),
            key="gf_created_at_preset",
        )

        profitable_only = st.sidebar.checkbox(
            "Profitable Runs",
            value=bool(gf.get("profitable_only", False)),
            key="gf_profitable_only",
        )

        # Accounts
        accounts = []
        try:
            accounts = get_account_snapshots(_gf_conn, str(user_id_gf), include_deleted=False)
        except Exception:
            accounts = []
        account_options = []
        account_id_by_label: Dict[str, int] = {}
        for a in accounts or []:
            try:
                aid = int(a.get("id"))
            except Exception:
                continue
            label = str(a.get("label") or f"#{aid}").strip() or f"#{aid}"
            ex_raw = str(a.get("exchange_id") or "").strip().lower()
            ex = (normalize_exchange_id(ex_raw) or ex_raw).upper()
            opt = f"{label} ({ex} #{aid})" if ex else f"{label} (#{aid})"
            account_options.append(opt)
            account_id_by_label[opt] = aid
        selected_accounts = st.sidebar.multiselect(
            "Account(s)",
            options=sorted(account_options),
            default=[o for o in account_options if account_id_by_label.get(o) in (gf.get("account_ids") or [])],
            key="gf_accounts",
        )
        account_ids = [account_id_by_label[o] for o in selected_accounts if o in account_id_by_label]

        # Exchanges (from accounts + bots)
        exchange_opts: List[str] = []
        try:
            exchange_opts.extend([
                normalize_exchange_id(str(a.get("exchange_id") or "").strip().lower())
                or str(a.get("exchange_id") or "").strip().lower()
                for a in accounts or []
            ])
        except Exception:
            pass
        try:
            bots_for_opts = get_bot_snapshots(_gf_conn, str(user_id_gf), limit=500)
            exchange_opts.extend([
                normalize_exchange_id(str(b.get("exchange_id") or "").strip().lower())
                or str(b.get("exchange_id") or "").strip().lower()
                for b in bots_for_opts or []
            ])
        except Exception:
            pass
        exchange_opts = sorted({e for e in exchange_opts if e})
        exchanges = st.sidebar.multiselect(
            "Exchange",
            options=[e.upper() for e in exchange_opts],
            default=[str(e).upper() for e in (gf.get("exchanges") or [])],
            key="gf_exchanges",
        )

        market_types = st.sidebar.multiselect(
            "Market type",
            options=["SPOT", "PERPS"],
            default=[str(m).upper() for m in (gf.get("market_types") or [])],
            key="gf_market_types",
        )

        # Symbols (union from bots)
        symbol_opts: List[str] = []
        try:
            bots_for_opts = bots_for_opts if "bots_for_opts" in locals() else get_bot_snapshots(_gf_conn, str(user_id_gf), limit=500)
            for b in bots_for_opts or []:
                ex_raw = str(b.get("exchange_id") or "").strip().lower()
                ex_norm = normalize_exchange_id(ex_raw) or ex_raw
                sym_raw = str(b.get("symbol") or "").strip()
                if not sym_raw:
                    continue
                try:
                    from project_dragon.exchange_normalization import canonical_symbol

                    sym_norm = canonical_symbol(ex_norm, sym_raw)
                except Exception:
                    sym_norm = sym_raw
                symbol_opts.append(str(sym_norm or sym_raw).strip().upper())
        except Exception:
            pass
        symbol_opts = sorted({s for s in symbol_opts if s})
        symbols = st.sidebar.multiselect(
            "Symbol(s)",
            options=symbol_opts,
            default=[str(s).upper() for s in (gf.get("symbols") or [])],
            key="gf_symbols",
        )

        # Strategy (from stored backtest strategies)
        try:
            strategies_opts = [str(s or "").strip() for s in (list_backtest_run_strategies(limit=5000) or [])]
            strategies_opts = sorted({s for s in strategies_opts if s})
        except Exception:
            strategies_opts = []
        strategies = st.sidebar.multiselect(
            "Strategy",
            options=strategies_opts,
            default=[str(s).strip() for s in (gf.get("strategies") or [])],
            key="gf_strategies",
        )

        timeframes = st.sidebar.multiselect(
            "Timeframe",
            options=["1m", "3m", "5m", "15m", "1h", "4h", "1d"],
            default=[str(t).strip() for t in (gf.get("timeframes") or [])],
            key="gf_timeframes",
        )

        new_gf = {
            "enabled": bool(enabled),
            "created_at_preset": str(created_at_preset or "").strip() or "All Time",
            "account_ids": account_ids,
            "exchanges": [str(e).strip().lower() for e in exchanges if str(e).strip()],
            "market_types": [str(m).strip().upper() for m in market_types if str(m).strip()],
            "symbols": [str(s).strip().upper() for s in symbols if str(s).strip()],
            "strategies": [str(s).strip() for s in strategies if str(s).strip()],
            "timeframes": [str(t).strip() for t in timeframes if str(t).strip()],
            "profitable_only": bool(profitable_only),
        }
        if new_gf != gf:
            set_global_filters(new_gf)

    if current_page == "Dashboard":
        st.subheader("Dashboard")
        render_active_filter_chips(get_global_filters())

        with open_db_connection() as conn:
            user_id_dash = get_or_create_user_id(get_current_user_email(), conn=conn)

            # Portfolio value from account snapshots (best-effort)
            accounts = []
            try:
                accounts = get_account_snapshots(conn, str(user_id_dash), include_deleted=False)
            except Exception:
                accounts = []
            portfolio_value = 0.0
            have_portfolio = False
            for a in accounts or []:
                v = _safe_float(a.get("snapshot_wallet_balance"))
                if v is not None:
                    portfolio_value += float(v)
                    have_portfolio = True

            # Bot snapshots for status counts
            try:
                bots = get_bot_snapshots(conn, str(user_id_dash), limit=500)
            except Exception:
                bots = list_bots(conn, limit=500)

            # Ledger + positions for KPIs/charts/cards (include enough shape to compute TradeStream cards)
            ledger_rows: list[Any] = []
            pos_rows: list[Any] = []

            # Best-effort join to strategy (from the run that created the bot).
            # If this fails (older DBs / missing tables), we fall back to no strategy column.
            try:
                ledger_rows = conn.execute(
                    """
                    WITH latest_bot_run AS (
                        SELECT brm.bot_id, brm.run_id
                        FROM bot_run_map brm
                        JOIN (
                            SELECT bot_id, MAX(created_at) AS max_created_at
                            FROM bot_run_map
                            GROUP BY bot_id
                        ) x
                        ON x.bot_id = brm.bot_id AND x.max_created_at = brm.created_at
                    )
                    SELECT
                        l.event_ts,
                        l.kind,
                        COALESCE(l.pnl, 0.0) AS pnl,
                        COALESCE(l.fee, 0.0) AS fee,
                        COALESCE(l.funding, 0.0) AS funding,
                        l.qty,
                        l.price,
                        l.side,
                        l.position_side,
                        l.meta_json,
                        b.exchange_id,
                        b.symbol,
                        b.timeframe,
                        b.account_id,
                        COALESCE(r.strategy_name, '') AS strategy
                    FROM bot_ledger l
                    JOIN bots b ON b.id = l.bot_id
                    LEFT JOIN latest_bot_run m ON m.bot_id = b.id
                    LEFT JOIN backtest_runs r ON CAST(r.id AS TEXT) = CAST(m.run_id AS TEXT)
                    WHERE b.user_id = ?
                    """,
                    (str(user_id_dash),),
                ).fetchall()
            except Exception:
                try:
                    ledger_rows = conn.execute(
                        """
                        SELECT
                            l.event_ts,
                            l.kind,
                            COALESCE(l.pnl, 0.0) AS pnl,
                            COALESCE(l.fee, 0.0) AS fee,
                            COALESCE(l.funding, 0.0) AS funding,
                            l.qty,
                            l.price,
                            l.side,
                            l.position_side,
                            l.meta_json,
                            b.exchange_id,
                            b.symbol,
                            b.timeframe,
                            b.account_id,
                            '' AS strategy
                        FROM bot_ledger l
                        JOIN bots b ON b.id = l.bot_id
                        WHERE b.user_id = ?
                        """,
                        (str(user_id_dash),),
                    ).fetchall()
                except Exception:
                    ledger_rows = []

            try:
                pos_rows = conn.execute(
                    """
                    WITH latest_bot_run AS (
                        SELECT brm.bot_id, brm.run_id
                        FROM bot_run_map brm
                        JOIN (
                            SELECT bot_id, MAX(created_at) AS max_created_at
                            FROM bot_run_map
                            GROUP BY bot_id
                        ) x
                        ON x.bot_id = brm.bot_id AND x.max_created_at = brm.created_at
                    )
                    SELECT
                        p.opened_at,
                        p.closed_at,
                        COALESCE(p.realized_pnl, 0.0) AS realized_pnl,
                        COALESCE(p.fees_paid, 0.0) AS fees_paid,
                        p.position_side,
                        b.exchange_id,
                        p.symbol,
                        b.timeframe,
                        b.account_id,
                        COALESCE(r.strategy_name, '') AS strategy
                    FROM bot_positions_history p
                    JOIN bots b ON b.id = p.bot_id
                    LEFT JOIN latest_bot_run m ON m.bot_id = b.id
                    LEFT JOIN backtest_runs r ON CAST(r.id AS TEXT) = CAST(m.run_id AS TEXT)
                    WHERE b.user_id = ?
                    """,
                    (str(user_id_dash),),
                ).fetchall()
            except Exception:
                try:
                    pos_rows = conn.execute(
                        """
                        SELECT
                            p.opened_at,
                            p.closed_at,
                            COALESCE(p.realized_pnl, 0.0) AS realized_pnl,
                            COALESCE(p.fees_paid, 0.0) AS fees_paid,
                            p.position_side,
                            b.exchange_id,
                            p.symbol,
                            b.timeframe,
                            b.account_id,
                            '' AS strategy
                        FROM bot_positions_history p
                        JOIN bots b ON b.id = p.bot_id
                        WHERE b.user_id = ?
                        """,
                        (str(user_id_dash),),
                    ).fetchall()
                except Exception:
                    pos_rows = []

        # DataFrames
        df_ledger = pd.DataFrame(
            ledger_rows,
            columns=[
                "event_ts",
                "kind",
                "pnl",
                "fee",
                "funding",
                "qty",
                "price",
                "side",
                "position_side",
                "meta_json",
                "exchange_id",
                "symbol",
                "timeframe",
                "account_id",
                "strategy",
            ],
        )
        df_pos = pd.DataFrame(
            pos_rows,
            columns=[
                "opened_at",
                "closed_at",
                "realized_pnl",
                "fees_paid",
                "position_side",
                "exchange_id",
                "symbol",
                "timeframe",
                "account_id",
                "strategy",
            ],
        )

        if not df_ledger.empty:
            df_ledger["event_ts"] = pd.to_datetime(df_ledger["event_ts"], errors="coerce", utc=True)
            # Fees are stored as positive costs; net should subtract fees.
            df_ledger["net"] = df_ledger["pnl"].astype(float) - df_ledger["fee"].astype(float) + df_ledger["funding"].astype(float)
            try:
                sym = df_ledger["symbol"].fillna("").astype(str).str.upper()
                df_ledger["market_type"] = sym.apply(lambda s: "PERPS" if s.startswith("PERP_") else ("SPOT" if s else ""))
                df_ledger["market"] = df_ledger["market_type"]
            except Exception:
                pass
        if not df_pos.empty:
            df_pos["opened_at"] = pd.to_datetime(df_pos["opened_at"], errors="coerce", utc=True)
            df_pos["closed_at"] = pd.to_datetime(df_pos["closed_at"], errors="coerce", utc=True)
            try:
                sym = df_pos["symbol"].fillna("").astype(str).str.upper()
                df_pos["market_type"] = sym.apply(lambda s: "PERPS" if s.startswith("PERP_") else ("SPOT" if s else ""))
                df_pos["market"] = df_pos["market_type"]
            except Exception:
                pass

        gf = get_global_filters()
        df_ledger = apply_global_filters(df_ledger, gf)
        df_pos = apply_global_filters(df_pos, gf)

        # ---- Derived frames for cards --------------------------------
        def _parse_meta(v: Any) -> dict:
            if not v:
                return {}
            if isinstance(v, dict):
                return v
            try:
                return json.loads(str(v)) if str(v).strip() else {}
            except Exception:
                return {}

        if not df_ledger.empty and "meta_json" in df_ledger.columns:
            try:
                df_ledger["meta"] = df_ledger["meta_json"].apply(_parse_meta)
            except Exception:
                df_ledger["meta"] = [{} for _ in range(len(df_ledger))]
        else:
            df_ledger["meta"] = [{} for _ in range(len(df_ledger))]

        # Closed positions (trade-level) derived stats
        if not df_pos.empty:
            df_pos["trade_net"] = df_pos["realized_pnl"].astype(float) - df_pos["fees_paid"].astype(float)
            try:
                dur_s = (df_pos["closed_at"] - df_pos["opened_at"]).dt.total_seconds()
                df_pos["hold_s"] = pd.to_numeric(dur_s, errors="coerce")
            except Exception:
                df_pos["hold_s"] = None

        # Fills / fees / funding (ledger-level)
        df_fills = df_ledger.loc[df_ledger.get("kind").fillna("").astype(str).str.lower() == "fill"].copy() if not df_ledger.empty else pd.DataFrame()
        df_fees = df_ledger.loc[df_ledger.get("kind").fillna("").astype(str).str.lower() == "fee"].copy() if not df_ledger.empty else pd.DataFrame()
        df_funding = df_ledger.loc[df_ledger.get("kind").fillna("").astype(str).str.lower() == "funding"].copy() if not df_ledger.empty else pd.DataFrame()

        if not df_fills.empty:
            df_fills["notional"] = (pd.to_numeric(df_fills.get("qty"), errors="coerce").abs() * pd.to_numeric(df_fills.get("price"), errors="coerce").abs()).fillna(0.0)

            def _is_limit_like(meta: Any) -> bool:
                if not isinstance(meta, dict):
                    return False
                coid = meta.get("client_order_id")
                return bool(str(coid).strip()) if coid is not None else False

            try:
                df_fills["is_limit_like"] = df_fills["meta"].apply(_is_limit_like)
            except Exception:
                df_fills["is_limit_like"] = False

        if not df_fees.empty:
            try:
                df_fees["is_limit_like"] = df_fees["meta"].apply(lambda m: bool(str(m.get("client_order_id")).strip()) if isinstance(m, dict) and m.get("client_order_id") is not None else False)
            except Exception:
                df_fees["is_limit_like"] = False

        # Rollup granularity: daily for shorter ranges; weekly for longer.
        def _pick_rollup_freq(df_ts: pd.Series) -> str:
            try:
                mi = pd.to_datetime(df_ts, errors="coerce", utc=True).min()
                ma = pd.to_datetime(df_ts, errors="coerce", utc=True).max()
                if mi is None or ma is None or pd.isna(mi) or pd.isna(ma):
                    return "1D"
                days = float((ma - mi).total_seconds() / 86400.0)
                return "1W" if days >= 120.0 else "1D"
            except Exception:
                return "1D"

        rollup_freq = "1D"
        if not df_fills.empty and "event_ts" in df_fills.columns:
            rollup_freq = _pick_rollup_freq(df_fills["event_ts"])
        elif not df_pos.empty and "closed_at" in df_pos.columns:
            rollup_freq = _pick_rollup_freq(df_pos["closed_at"])

        # ---- KPI summary values -------------------------------------
        lifetime_pnl = float(df_ledger["net"].sum()) if (not df_ledger.empty and "net" in df_ledger.columns) else 0.0
        total_trades = int(len(df_pos)) if not df_pos.empty else 0

        wins = int((df_pos.get("trade_net") > 0).sum()) if (not df_pos.empty and "trade_net" in df_pos.columns) else 0
        win_rate = (wins / total_trades) if total_trades > 0 else 0.0

        gross_profit = float(df_pos.loc[df_pos.get("trade_net") > 0, "trade_net"].sum()) if (not df_pos.empty and "trade_net" in df_pos.columns) else 0.0
        gross_loss = float(df_pos.loc[df_pos.get("trade_net") < 0, "trade_net"].sum()) if (not df_pos.empty and "trade_net" in df_pos.columns) else 0.0
        profit_factor = (gross_profit / abs(gross_loss)) if gross_loss < 0 else (gross_profit if gross_profit > 0 else 0.0)

        avg_win = float(df_pos.loc[df_pos.get("trade_net") > 0, "trade_net"].mean()) if (not df_pos.empty and "trade_net" in df_pos.columns and wins > 0) else None
        avg_loss = float(df_pos.loc[df_pos.get("trade_net") < 0, "trade_net"].mean()) if (not df_pos.empty and "trade_net" in df_pos.columns and (total_trades - wins) > 0) else None
        expected_value = float(df_pos["trade_net"].mean()) if (not df_pos.empty and "trade_net" in df_pos.columns) else None
        avg_hold_s = float(df_pos["hold_s"].mean()) if (not df_pos.empty and "hold_s" in df_pos.columns and df_pos["hold_s"].notna().any()) else None

        volume_traded = float(df_fills["notional"].sum()) if (not df_fills.empty and "notional" in df_fills.columns) else 0.0
        avg_trade_size = (volume_traded / float(total_trades)) if total_trades > 0 else None

        # Sharpe / Sortino from daily net (best-effort)
        sharpe = None
        sortino = None
        try:
            if not df_ledger.empty:
                daily_net = (
                    df_ledger.dropna(subset=["event_ts"]).set_index("event_ts")["net"].resample("1D").sum().sort_index()
                )
                if len(daily_net) >= 5:
                    mu = float(daily_net.mean())
                    sigma = float(daily_net.std(ddof=0))
                    if sigma > 0:
                        sharpe = (mu / sigma) * math.sqrt(252.0)
                    downside = daily_net[daily_net < 0]
                    ds = float(downside.std(ddof=0)) if len(downside) >= 2 else 0.0
                    if ds > 0:
                        sortino = (mu / ds) * math.sqrt(252.0)
        except Exception:
            sharpe = None
            sortino = None

        # Max drawdown from daily equity (net cumsum)
        max_dd = 0.0
        if not df_ledger.empty:
            daily = (
                df_ledger.dropna(subset=["event_ts"])\
                    .set_index("event_ts")["net"]\
                    .resample("1D")\
                    .sum()\
                    .sort_index()
            )
            equity = daily.cumsum()
            peak = equity.cummax()
            dd = equity - peak
            try:
                max_dd = float(dd.min())
            except Exception:
                max_dd = 0.0

        kpi_cols = st.columns(6)
        with kpi_cols[0]:
            render_kpi_tile("Portfolio Value", fmt_num(portfolio_value, 2) if have_portfolio else "–")
        with kpi_cols[1]:
            render_kpi_tile("Lifetime PnL", fmt_num(lifetime_pnl, 2))
        with kpi_cols[2]:
            render_kpi_tile("Win Rate", format_pct(win_rate, assume_ratio_if_leq_1=True, decimals=0))
        with kpi_cols[3]:
            render_kpi_tile("Total Trades", fmt_int(total_trades))
        with kpi_cols[4]:
            render_kpi_tile("Max Drawdown", fmt_num(abs(float(max_dd or 0.0)), 2))
        with kpi_cols[5]:
            render_kpi_tile("Profit Factor", fmt_num(profit_factor, 2))

        # Charts row (moved up from the old "More charts" expander)
        c1, c2, c3 = st.columns(3)
        if not df_ledger.empty:
            daily_net_series = (
                df_ledger.dropna(subset=["event_ts"])\
                    .set_index("event_ts")["net"]\
                    .resample("1D")\
                    .sum()\
                    .sort_index()
            )
            pnl_cum = daily_net_series.cumsum()
            pnl_weekly = daily_net_series.resample("W").sum()
        else:
            daily_net_series = None
            pnl_cum = None
            pnl_weekly = None

        with c1:
            render_card(
                "PnL (cumulative)",
                lambda: (
                    st.plotly_chart(
                        go.Figure(data=[go.Scatter(x=pnl_cum.index, y=pnl_cum.values, mode="lines", name="PnL")]).update_layout(
                            template="plotly_dark",
                            height=320,
                            margin=dict(l=10, r=10, t=10, b=10),
                            xaxis_title="",
                            yaxis_title="",
                        ),
                        width="stretch",
                        key="dash_pnl_cum",
                    )
                    if pnl_cum is not None and len(pnl_cum) > 0
                    else st.info("No ledger data for PnL chart yet.")
                ),
            )

        with c2:
            render_card(
                "Weekly PnL",
                lambda: (
                    st.plotly_chart(
                        go.Figure(data=[go.Bar(x=pnl_weekly.index, y=pnl_weekly.values, name="Weekly")]).update_layout(
                            template="plotly_dark",
                            height=320,
                            margin=dict(l=10, r=10, t=10, b=10),
                            xaxis_title="",
                            yaxis_title="",
                        ),
                        width="stretch",
                        key="dash_pnl_weekly",
                    )
                    if pnl_weekly is not None and len(pnl_weekly) > 0
                    else st.info("No ledger data for weekly chart yet.")
                ),
            )

        with c3:
            def _winrate_chart() -> None:
                if df_pos.empty:
                    st.info("No closed positions yet.")
                    return
                tmp = df_pos.dropna(subset=["closed_at"]).copy()
                if tmp.empty:
                    st.info("No closed positions yet.")
                    return
                # Avoid warning: converting tz-aware datetime to Period drops tz.
                try:
                    ca = tmp["closed_at"]
                    if getattr(ca.dt, "tz", None) is not None:
                        tmp["closed_at"] = ca.dt.tz_convert("UTC").dt.tz_localize(None)
                except Exception:
                    pass
                tmp["month"] = tmp["closed_at"].dt.to_period("M").dt.to_timestamp()
                g = tmp.groupby("month")["trade_net"].apply(lambda s: float((s > 0).sum()) / float(len(s)) if len(s) else 0.0)
                fig = go.Figure(data=[go.Bar(x=g.index, y=(g.values * 100.0), name="Win rate")])
                fig.update_layout(template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10), yaxis_title="Win %")
                st.plotly_chart(fig, width="stretch", key="dash_win_rate_by_month")

            render_card("Win Rate by Month", _winrate_chart)

        # --- TradeStream-style cards ----------------------------------
        # Row 1 under KPIs: Metrics (L) + Order types & Funding (R)
        mcol, ofcol = st.columns(2)

        with mcol:
            def _render_metrics_card() -> None:
                left_rows = [
                    {"label": "Total PnL ($)", "value": format_money(lifetime_pnl, decimals=2), "raw_value": lifetime_pnl, "metric_name": "pnl"},
                    {"label": "Win Rate", "value": format_pct(win_rate, assume_ratio_if_leq_1=True, decimals=0), "raw_value": win_rate, "metric_name": "win_rate"},
                    {"label": "Average Win", "value": format_money(avg_win, decimals=2) if avg_win is not None else "—", "raw_value": avg_win, "metric_name": "pnl"},
                    {"label": "Average Loss", "value": format_money(avg_loss, decimals=2) if avg_loss is not None else "—", "raw_value": avg_loss, "metric_name": "pnl"},
                    {"label": "Average MAE", "value": "—", "raw_value": None, "metric_name": "mae", "hint": "Max adverse excursion is not currently recorded for live trades."},
                    {"label": "Average MFE", "value": "—", "raw_value": None, "metric_name": "mfe", "hint": "Max favorable excursion is not currently recorded for live trades."},
                    {"label": "MFE/MAE Ratio", "value": "—", "raw_value": None, "metric_name": "mfe/mae", "hint": "Requires both MFE and MAE per trade."},
                ]
                right_rows = [
                    {"label": "Volume Traded", "value": format_money(volume_traded, decimals=0), "raw_value": volume_traded, "metric_name": "volume"},
                    {"label": "Average Trade Size", "value": format_money(avg_trade_size, decimals=0) if avg_trade_size is not None else "—", "raw_value": avg_trade_size, "metric_name": "volume"},
                    {"label": "Average Hold Time", "value": format_duration(avg_hold_s), "raw_value": None, "metric_name": "hold"},
                    {"label": "Sharpe Ratio", "value": fmt_num(sharpe, 2) if sharpe is not None else "—", "raw_value": sharpe, "metric_name": "sharpe", "hint": "Computed from daily net PnL (best-effort)."},
                    {"label": "Sortino Ratio", "value": fmt_num(sortino, 2) if sortino is not None else "—", "raw_value": sortino, "metric_name": "sortino", "hint": "Computed from daily net PnL (best-effort)."},
                    {"label": "Profit Factor", "value": fmt_num(profit_factor, 2), "raw_value": profit_factor, "metric_name": "profit_factor"},
                    {"label": "Expected Value", "value": format_money(expected_value, decimals=2) if expected_value is not None else "—", "raw_value": expected_value, "metric_name": "pnl"},
                ]
                render_metric_two_col_table(left_rows, right_rows, key_prefix="dashboard_metrics")

            render_card("Metrics", _render_metrics_card)

        with ofcol:
            def _render_order_types_and_funding() -> None:
                # Fees by limit-like (clientOrderId) vs market-like
                fees_limit = 0.0
                fees_market = 0.0
                if not df_fees.empty:
                    try:
                        fees_limit = float(df_fees.loc[df_fees["is_limit_like"] == True, "fee"].sum())
                        fees_market = float(df_fees.loc[df_fees["is_limit_like"] != True, "fee"].sum())
                    except Exception:
                        fees_limit = 0.0
                        fees_market = 0.0
                fees_total = float((fees_limit + fees_market) or 0.0)

                funding_paid = 0.0
                funding_received = 0.0
                net_funding = 0.0
                if not df_funding.empty:
                    try:
                        vals = pd.to_numeric(df_funding.get("funding"), errors="coerce").fillna(0.0)
                        net_funding = float(vals.sum())
                        funding_received = float(vals[vals > 0].sum())
                        funding_paid = float(abs(vals[vals < 0].sum()))
                    except Exception:
                        funding_paid = 0.0
                        funding_received = 0.0
                        net_funding = 0.0

                top_left, top_right = st.columns(2)
                with top_left:
                    _render_kv_table(
                        [
                            ("Fees (limit)", format_money(fees_limit, decimals=2), get_value_color("fees", fees_limit)),
                            ("Fees (market)", format_money(fees_market, decimals=2), get_value_color("fees", fees_market)),
                            ("Total Fees", format_money(fees_total, decimals=2), get_value_color("fees", fees_total)),
                        ]
                    )
                with top_right:
                    _render_kv_table(
                        [
                            ("Funding Paid", format_money(funding_paid, decimals=2), get_value_color("funding", funding_paid)),
                            ("Funding Received", format_money(funding_received, decimals=2), get_value_color("funding", funding_received)),
                            ("Net Funding", format_money(net_funding, decimals=2), get_value_color("funding", net_funding)),
                        ]
                    )

                st.markdown("<div style='height:0.45rem;'></div>", unsafe_allow_html=True)

                d1, d2 = st.columns(2)
                with d1:
                    st.caption("Limit vs Market orders by volume")
                    if df_fills.empty or float(df_fills.get("notional", pd.Series(dtype=float)).sum() or 0.0) <= 0:
                        st.markdown("<div class='dragon-muted'>No volume data.</div>", unsafe_allow_html=True)
                    else:
                        v_limit = float(df_fills.loc[df_fills.get("is_limit_like") == True, "notional"].sum())
                        v_market = float(df_fills.loc[df_fills.get("is_limit_like") != True, "notional"].sum())
                        v_total = max(0.0, v_limit + v_market)
                        if v_total <= 0:
                            st.markdown("<div class='dragon-muted'>No volume data.</div>", unsafe_allow_html=True)
                        else:
                            p_limit = (v_limit / v_total) * 100.0
                            p_market = 100.0 - p_limit
                            fig = go.Figure(
                                data=[
                                    go.Pie(
                                        labels=["Limit", "Market"],
                                        values=[v_limit, v_market],
                                        hole=0.68,
                                        sort=False,
                                        marker=dict(colors=["rgba(46,160,67,0.70)", "rgba(248,81,73,0.70)"]),
                                        textinfo="none",
                                    )
                                ]
                            )
                            fig.update_layout(
                                template="plotly_dark",
                                height=240,
                                margin=dict(l=10, r=10, t=10, b=10),
                                showlegend=False,
                            )
                            fig.add_annotation(
                                text=f"{p_limit:.0f}% / {p_market:.0f}%",
                                x=0.5,
                                y=0.5,
                                font=dict(size=16, color="rgba(226,232,240,0.95)"),
                                showarrow=False,
                            )
                            st.plotly_chart(fig, width="stretch", key="dash_donut_limit_market")
                            st.markdown("<div class='dragon-muted'>Limit-like = has clientOrderId (best-effort).</div>", unsafe_allow_html=True)

                with d2:
                    st.caption("Funding paid vs received")
                    if (funding_paid + funding_received) <= 0:
                        st.markdown("<div class='dragon-muted'>No funding data.</div>", unsafe_allow_html=True)
                    else:
                        f_total = max(0.0, funding_paid + funding_received)
                        p_paid = (funding_paid / f_total) * 100.0 if f_total > 0 else 0.0
                        p_recv = 100.0 - p_paid
                        fig = go.Figure(
                            data=[
                                go.Pie(
                                    labels=["Paid", "Received"],
                                    values=[funding_paid, funding_received],
                                    hole=0.68,
                                    sort=False,
                                    marker=dict(colors=["rgba(248,81,73,0.70)", "rgba(46,160,67,0.70)"]),
                                    textinfo="none",
                                )
                            ]
                        )
                        fig.update_layout(
                            template="plotly_dark",
                            height=240,
                            margin=dict(l=10, r=10, t=10, b=10),
                            showlegend=False,
                        )
                        fig.add_annotation(
                            text=f"{p_paid:.0f}% / {p_recv:.0f}%",
                            x=0.5,
                            y=0.5,
                            font=dict(size=16, color="rgba(226,232,240,0.95)"),
                            showarrow=False,
                        )
                        st.plotly_chart(fig, width="stretch", key="dash_donut_funding")

            render_card("Order types and Funding", _render_order_types_and_funding)

        # Row 2: Volume (cumulative), Hold time, Long vs Short ratio
        vcol, hcol, rcol = st.columns(3)

        with vcol:
            def _render_volume_cumulative() -> None:
                if df_fills.empty or "event_ts" not in df_fills.columns:
                    st.info("No fill data yet.")
                    return
                ts = df_fills.dropna(subset=["event_ts"]).set_index("event_ts")["notional"].resample(rollup_freq).sum().sort_index()
                cum = ts.cumsum()
                total_v = float(cum.iloc[-1]) if len(cum) else 0.0
                st.caption(f"Total: {format_money(total_v, decimals=0)}")
                fig = go.Figure(data=[go.Scatter(x=cum.index, y=cum.values, mode="lines", name="Volume")])
                fig.update_layout(
                    template="plotly_dark",
                    height=280,
                    margin=dict(l=10, r=10, t=10, b=10),
                    xaxis_title="",
                    yaxis_title="",
                )
                st.plotly_chart(fig, width="stretch", key="dash_volume_cum")

            render_card("Volume (Cumulative)", _render_volume_cumulative)

        with hcol:
            def _render_hold_time() -> None:
                if df_pos.empty or "hold_s" not in df_pos.columns:
                    st.markdown("<div style='font-weight:900; font-size:1.35rem;'>—</div>", unsafe_allow_html=True)
                    st.markdown("<div class='dragon-muted'>Hold time unavailable.</div>", unsafe_allow_html=True)
                    return
                tmp = df_pos.dropna(subset=["closed_at", "hold_s"]).copy()
                if tmp.empty:
                    st.info("No closed positions yet.")
                    return
                series = tmp.set_index("closed_at")["hold_s"].resample(rollup_freq).mean().sort_index()
                fig = go.Figure(data=[go.Scatter(x=series.index, y=(series.values / 3600.0), mode="lines", name="Hours")])
                fig.update_layout(
                    template="plotly_dark",
                    height=280,
                    margin=dict(l=10, r=10, t=10, b=10),
                    xaxis_title="",
                    yaxis_title="Avg hours",
                )
                st.plotly_chart(fig, width="stretch", key="dash_hold_time")

            render_card("Hold time", _render_hold_time)

        with rcol:
            def _render_long_short_ratio() -> None:
                if df_fills.empty or "position_side" not in df_fills.columns:
                    st.markdown("<div style='font-weight:900; font-size:1.35rem;'>—</div>", unsafe_allow_html=True)
                    st.markdown("<div class='dragon-muted'>Direction not available.</div>", unsafe_allow_html=True)
                    return
                tmp = df_fills.copy()
                ps = tmp["position_side"].fillna("").astype(str).str.upper().str.strip()
                tmp["ps"] = ps
                if not tmp["ps"].isin(["LONG", "SHORT"]).any():
                    st.markdown("<div style='font-weight:900; font-size:1.35rem;'>—</div>", unsafe_allow_html=True)
                    st.markdown("<div class='dragon-muted'>Direction not available.</div>", unsafe_allow_html=True)
                    return
                long_v = float(tmp.loc[tmp["ps"] == "LONG", "notional"].sum())
                short_v = float(tmp.loc[tmp["ps"] == "SHORT", "notional"].sum())
                total = long_v + short_v
                if total <= 0:
                    st.markdown("<div class='dragon-muted'>No volume data.</div>", unsafe_allow_html=True)
                    return
                long_pct = long_v / total
                short_pct = 1.0 - long_pct
                st.markdown(
                    """
                    <div class='dragon-ratio-wrap'>
                      <div class='dragon-ratio-header'>
                        <div><span class='label'>Long Ratio</span> <span class='value' style='color:#7ee787;'>"""
                    + html.escape(format_pct(long_pct, assume_ratio_if_leq_1=True, decimals=0))
                    + """</span></div>
                        <div><span class='label'>Short Ratio</span> <span class='value' style='color:#ff7b72;'>"""
                    + html.escape(format_pct(short_pct, assume_ratio_if_leq_1=True, decimals=0))
                    + """</span></div>
                      </div>
                      <div class='dragon-splitbar'>
                        <div class='long' style='width:"""
                    + f"{long_pct * 100.0:.2f}%"
                    + """; float:left;'></div>
                        <div class='short' style='width:"""
                    + f"{short_pct * 100.0:.2f}%"
                    + """; float:left;'></div>
                      </div>
                      <div class='dragon-muted' style='margin-top:0.35rem;'>"""
                    + html.escape(f"Long {format_money(long_v, decimals=0)} • Short {format_money(short_v, decimals=0)}")
                    + """</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            render_card("Long vs Short ratio", _render_long_short_ratio)

        # Bots overview (counts + table)
        def _render_bots_overview() -> None:
            df_bots = _build_live_bots_overview_df(bots, user_id=str(user_id_dash))
            if df_bots.empty:
                st.info("No bots match the current filters.")
                return
            # Counts by status (rough buckets)
            s = df_bots.get("Status")
            status_counts = {"running": 0, "stopped": 0, "paused": 0, "error": 0, "stale": 0}
            if s is not None:
                for v in s.fillna("").astype(str).str.lower().tolist():
                    if "error" in v:
                        status_counts["error"] += 1
                    elif "pause" in v:
                        status_counts["paused"] += 1
                    elif "run" in v:
                        status_counts["running"] += 1
                    elif "stop" in v:
                        status_counts["stopped"] += 1
            flags = df_bots.get("Risk/Degraded")
            if flags is not None:
                status_counts["stale"] = int(flags.fillna("").astype(str).str.upper().str.contains("STALE").sum())
            st.caption(
                f"Running {status_counts['running']} • Paused {status_counts['paused']} • Stopped {status_counts['stopped']} • "
                f"Error {status_counts['error']} • Stale {status_counts['stale']}"
            )
            _render_live_bots_aggrid(df_bots, grid_key="dashboard_bots_overview_grid")

        render_card("Bots Overview", _render_bots_overview)

        return

    if current_page == "Accounts":
        st.subheader("Accounts")
        st.caption("Accounts manage exchange keys (encrypted) and reusable bot account selection.")

        tz = _ui_display_tz()

        with _job_conn() as conn:
            accounts_all = get_account_snapshots(conn, user_id, include_deleted=True)
            creds = list_credentials(conn, user_id)

        if accounts_all:
            df = pd.DataFrame(
                [
                    {
                        "ID": a.get("id"),
                        "Exchange": normalize_exchange_id(str(a.get("exchange_id") or "").strip())
                        or str(a.get("exchange_id") or "").strip(),
                        "Label": a.get("label"),
                        "Status": a.get("status"),
                        "Credential ID": a.get("credential_id"),
                        "Last Update": fmt_dt_short(a.get("snapshot_updated_at"), tz),
                        "Risk": (
                            ("BLOCKED" + (f" ({a.get('snapshot_risk_reason')})" if a.get("snapshot_risk_reason") else ""))
                            if a.get("snapshot_risk_blocked")
                            else "OK"
                        )
                        if a.get("snapshot_updated_at")
                        else "–",
                        "Open Orders": a.get("snapshot_open_orders_count"),
                        "Wallet": a.get("snapshot_wallet_balance"),
                        "Margin": a.get("snapshot_margin_ratio"),
                        "Error": a.get("snapshot_last_error"),
                        "Updated": fmt_dt_short(a.get("updated_at"), tz),
                        "Created": fmt_dt_short(a.get("created_at"), tz),
                    }
                    for a in accounts_all
                ]
            )
            st.dataframe(df, hide_index=True, width="stretch")
        else:
            st.info("No trading accounts yet. Create one below.")

        st.markdown("#### Create account")
        # Clear sensitive fields on the next rerun (must happen before widgets are created).
        if st.session_state.get("acct_create_clear_fields"):
            st.session_state["acct_create_api_key"] = ""
            st.session_state["acct_create_api_secret"] = ""
            st.session_state.pop("acct_create_clear_fields", None)
        ex_choice = st.selectbox("Exchange", options=["woox"], index=0, key="acct_create_exchange")
        label_val = st.text_input("Label", value="Default WooX", key="acct_create_label")
        api_key_val = st.text_input("API Key", value="", type="password", key="acct_create_api_key")
        api_secret_val = st.text_input("API Secret", value="", type="password", key="acct_create_api_secret")

        can_create = bool((label_val or "").strip()) and bool((api_key_val or "").strip()) and bool((api_secret_val or "").strip())
        if st.button("Create account", type="primary", key="acct_create_btn", disabled=not can_create):
            try:
                # Avoid creating an orphan credential if the account label already exists.
                existing = [a for a in accounts_all if (str(a.get("exchange_id") or "").strip().lower() == str(ex_choice).strip().lower()) and ((a.get("label") or "").strip() == (label_val or "").strip()) and (str(a.get("status") or "").strip().lower() != "deleted")]
                if existing:
                    st.error("An account with this label already exists. Choose a different label or rotate keys on the existing account.")
                else:
                    with _job_conn() as conn:
                        try:
                            _create_account = create_trading_account
                        except NameError:
                            from project_dragon.storage import create_trading_account as _create_account
                        cred_id = create_credential(
                            conn,
                            user_id=user_id,
                            exchange_id=str(ex_choice),
                            label=str(label_val).strip(),
                            api_key=str(api_key_val).strip(),
                            api_secret_plain=str(api_secret_val),
                        )
                        acct_id = _create_account(
                            conn,
                            user_id=user_id,
                            exchange_id=str(ex_choice),
                            label=str(label_val).strip(),
                            credential_id=int(cred_id),
                        )
                    # Clear sensitive fields on next rerun (safe for widget-backed state).
                    st.session_state["acct_create_clear_fields"] = True
                    st.success(f"Account #{acct_id} created")
                    st.rerun()
            except Exception as exc:
                st.error(f"Failed to create account: {exc}")

        st.markdown("#### Manage accounts")
        if not accounts_all:
            st.info("Create an account to enable live bot creation.")
        else:
            for a in accounts_all:
                aid = a.get("id")
                label = (a.get("label") or "").strip() or f"#{aid}"
                ex_raw = (a.get("exchange_id") or "").strip() or "woox"
                ex = normalize_exchange_id(ex_raw) or ex_raw
                status = (a.get("status") or "").strip().lower()
                with st.expander(f"{label} • {ex} • {status}", expanded=False):
                    st.write(f"Account ID: **{aid}**")
                    cred_row = None
                    try:
                        with _job_conn() as conn:
                            cred_row = get_credential(conn, user_id, int(a.get("credential_id"))) if a.get("credential_id") is not None else None
                    except Exception:
                        cred_row = None
                    if cred_row:
                        st.write(f"Credential ID: **{cred_row.get('id')}**")
                        st.caption(
                            f"API key: {mask_api_key(cred_row.get('api_key'))} • Last used: {fmt_dt_short(cred_row.get('last_used_at'), tz)}"
                        )
                    else:
                        st.write(f"Credential ID: **{a.get('credential_id')}**")
                    st.caption(
                        f"Created: {fmt_dt_short(a.get('created_at'), tz)} • Updated: {fmt_dt_short(a.get('updated_at'), tz)}"
                    )
                    with _job_conn() as conn:
                        bot_count = count_bots_for_account(conn, user_id, int(aid)) if aid is not None else 0
                    st.caption(f"Bots using this account: {bot_count}")

                    st.markdown("##### Account risk (blocks new entries)")
                    try:
                        day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
                    except Exception:
                        day_start = None

                    daily_net = None
                    total_net = None
                    try:
                        with _job_conn() as conn:
                            daily_net = sum_account_net_pnl(conn, user_id, int(aid), since_ts=day_start) if (aid is not None and day_start) else None
                            total_net = sum_account_net_pnl(conn, user_id, int(aid)) if aid is not None else None
                    except Exception:
                        daily_net = None
                        total_net = None

                    if daily_net is not None:
                        st.caption(f"Net PnL today (ledger): {daily_net:.2f}")
                    if total_net is not None:
                        st.caption(f"Net PnL total (ledger): {total_net:.2f}")

                    risk_block = bool(int(a.get("risk_block_new_entries") or 1))
                    risk_daily_existing = a.get("risk_max_daily_loss_usd")
                    risk_total_existing = a.get("risk_max_total_loss_usd")
                    try:
                        risk_daily_existing_f = float(risk_daily_existing) if risk_daily_existing is not None else 0.0
                    except (TypeError, ValueError):
                        risk_daily_existing_f = 0.0
                    try:
                        risk_total_existing_f = float(risk_total_existing) if risk_total_existing is not None else 0.0
                    except (TypeError, ValueError):
                        risk_total_existing_f = 0.0

                    rb_key = f"acct_risk_block_{aid}"
                    daily_key = f"acct_risk_daily_{aid}"
                    total_key = f"acct_risk_total_{aid}"
                    risk_block_new = st.toggle("Block new entries when limits tripped", value=risk_block, key=rb_key)
                    daily_limit_new = st.number_input(
                        "Max daily loss (USD, >0 enables)",
                        min_value=0.0,
                        value=float(risk_daily_existing_f),
                        step=10.0,
                        key=daily_key,
                    )
                    total_limit_new = st.number_input(
                        "Max total loss (USD, >0 enables)",
                        min_value=0.0,
                        value=float(risk_total_existing_f),
                        step=10.0,
                        key=total_key,
                    )
                    if st.button("Save risk settings", key=f"acct_risk_save_{aid}"):
                        try:
                            daily_arg = None if float(daily_limit_new or 0.0) <= 0 else float(daily_limit_new)
                            total_arg = None if float(total_limit_new or 0.0) <= 0 else float(total_limit_new)
                            with _job_conn() as conn:
                                update_trading_account_risk(
                                    conn,
                                    user_id,
                                    int(aid),
                                    risk_block_new_entries=bool(risk_block_new),
                                    risk_max_daily_loss_usd=daily_arg,
                                    risk_max_total_loss_usd=total_arg,
                                )
                            st.success("Account risk settings updated")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed to update risk settings: {exc}")

                    st.markdown("##### Flatten ALL bots in account (guarded)")
                    st.caption("Sets desired_action=flatten_now for all bots under this account (worker will execute per-bot).")
                    confirm_all = st.text_input("Type FLATTEN to confirm", key=f"acct_flatten_confirm_{aid}")
                    can_flatten_all = (confirm_all or "").strip().upper() == "FLATTEN"
                    if st.button(
                        "Flatten ALL bots",
                        key=f"acct_flatten_all_btn_{aid}",
                        disabled=not can_flatten_all,
                        type="primary",
                    ):
                        try:
                            with _job_conn() as conn:
                                n = request_flatten_all_bots_in_account(conn, user_id, int(aid), include_stopped=False)
                            st.success(f"Flatten requested for {n} bot(s).")
                        except Exception as exc:
                            st.error(f"Flatten-all failed: {exc}")

                    cols = st.columns([1, 1, 2])
                    if status == "active":
                        if cols[0].button("Disable", key=f"acct_disable_{aid}"):
                            with _job_conn() as conn:
                                set_trading_account_status(conn, user_id, int(aid), "disabled")
                            st.rerun()
                    elif status == "disabled":
                        if cols[0].button("Enable", key=f"acct_enable_{aid}"):
                            with _job_conn() as conn:
                                set_trading_account_status(conn, user_id, int(aid), "active")
                            st.rerun()

                    if cols[1].button("Delete", key=f"acct_delete_{aid}"):
                        if bot_count > 0:
                            st.error("Cannot delete: bots are still linked to this account. Disable it instead.")
                        else:
                            with _job_conn() as conn:
                                set_trading_account_status(conn, user_id, int(aid), "deleted")
                            st.success("Account deleted (soft-delete)")
                            st.rerun()

                    st.markdown("##### Rotate keys")
                    st.caption("Creates a new encrypted credential record and points this account to it.")
                    new_key = st.text_input("New API Key", value="", type="password", key=f"acct_rotate_key_{aid}")
                    new_secret = st.text_input("New API Secret", value="", type="password", key=f"acct_rotate_secret_{aid}")
                    if st.button("Rotate keys", key=f"acct_rotate_btn_{aid}", disabled=not (new_key.strip() and new_secret.strip())):
                        try:
                            with _job_conn() as conn:
                                new_cred_id = create_credential(
                                    conn,
                                    user_id=user_id,
                                    exchange_id=str(ex),
                                    label=f"{label} (rotated {datetime.now(timezone.utc).date().isoformat()})",
                                    api_key=str(new_key).strip(),
                                    api_secret_plain=str(new_secret),
                                )
                                set_trading_account_credential(conn, user_id, int(aid), int(new_cred_id))
                            # Clear sensitive fields.
                            st.session_state[f"acct_rotate_key_{aid}"] = ""
                            st.session_state[f"acct_rotate_secret_{aid}"] = ""
                            st.success("Keys rotated.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Key rotation failed: {exc}")

                    st.markdown("##### Connectivity")
                    test_symbol = st.text_input("Test symbol", value="PERP_ETH_USDT", key=f"acct_test_symbol_{aid}")
                    if st.button("Test connectivity", key=f"acct_test_btn_{aid}"):
                        try:
                            cred_id = int(a.get("credential_id"))
                            result = test_woox_credential(user_id, cred_id, str(test_symbol))
                            if result.get("ok"):
                                st.success(f"WooX connectivity OK via {result.get('endpoint')}")
                            else:
                                st.error("WooX connectivity failed")
                            st.json(result)
                        except Exception as exc:
                            st.error(f"Connectivity test failed: {exc}")

    if current_page == "Credentials":
        st.subheader("Credentials")
        st.warning("Credentials are now managed via Tools → Accounts. This page is kept for backward compatibility.")
        st.caption("Read-only view. Create/rotate keys via Tools → Accounts.")

        tz = _ui_display_tz()

        with _job_conn() as conn:
            creds = list_credentials(conn, user_id)

        if creds:
            df = pd.DataFrame(
                [
                    {
                        "ID": c.get("id"),
                        "Exchange": c.get("exchange_id"),
                        "Label": c.get("label"),
                        "API Key": mask_api_key(c.get("api_key")),
                        "Updated": fmt_dt_short(c.get("updated_at"), tz),
                        "Last Used": fmt_dt_short(c.get("last_used_at"), tz),
                    }
                    for c in creds
                ]
            )
            st.dataframe(df, hide_index=True, width="stretch")
        else:
            st.info("No credentials found for this user.")
        return

    if current_page in {"Runs Explorer", "Backtest"}:
        if current_page == "Runs Explorer":
            st.subheader("Runs Explorer")
            st.caption("Compare run metrics and strategy config side-by-side (with diff highlighting).")

            # Reuse global filter chips for consistency with Results.
            try:
                render_active_filter_chips(get_global_filters())
            except Exception:
                pass

            if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
                st.error("Runs Explorer requires `streamlit-aggrid`. Install dependencies and restart the app.")
                return

            # Match Results page headings + formatters.
            header_map = {
                "created_at": "Run Date",
                "symbol": "Asset",
                "timeframe": "Timeframe",
                "direction": "Direction",
                "strategy": "Strategy",
                "sweep": "Sweep",
                "start_date": "Start",
                "end_date": "End",
                "duration": "Duration",
                "net_return_pct": "Net Return",
                "roi_pct_on_margin": "ROI",
                "max_drawdown_pct": "Max Drawdown",
                "win_rate": "Win Rate",
                "net_profit": "Net Profit",
                "sharpe": "Sharpe",
                "sortino": "Sortino",
                "profit_factor": "Profit Factor",
                "cpc_index": "CPC Index",
                "common_sense_ratio": "Common Sense Ratio",
                "avg_position_time": "Avg Position Time",
                "shortlist": "Shortlist",
                "shortlist_note": "Shortlist Note",
            }

            roi_pct_formatter = JsCode(
                """
                function(params) {
                    if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                    const v = Number(params.value);
                    if (!isFinite(v)) { return ''; }
                    return Math.round(v).toString() + '%';
                }
                """
            )
            pct_formatter = JsCode(
                """
                function(params) {
                  if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                  const v = Number(params.value);
                  if (!isFinite(v)) { return ''; }
                                    const shown = (Math.abs(v) <= 1.0) ? (v * 100.0) : v;
                                    return shown.toFixed(0) + '%';
                }
                """
            )
            num2_formatter = JsCode(
                """
                function(params) {
                  if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                  const v = Number(params.value);
                  if (!isFinite(v)) { return ''; }
                  return v.toFixed(2);
                }
                """
            )
            # Match Results/Runs Explorer pills for Timeframe + Direction.
            metrics_gradient_style = JsCode(
                """
                function(params) {
                    const col = (params && params.colDef && params.colDef.field) ? params.colDef.field.toString() : '';
                    const v = Number(params.value);
                    if (!isFinite(v)) { return {}; }
                    function asRatio(x) {
                        if (!isFinite(x)) { return x; }
                        return (Math.abs(x) <= 1.0) ? x : (x / 100.0);
                    }
                    const greenText = '#7ee787';
                    const redText = '#ff7b72';
                    const neutralText = '#cbd5e1';
                    const greenRgb = '46,160,67';
                    const redRgb = '248,81,73';
                    function clamp01(x) { return Math.max(0.0, Math.min(1.0, x)); }
                    function alphaFromT(t) { return 0.10 + (0.22 * clamp01(t)); }
                    function styleGoodBad(isGood, t) {
                        const a = alphaFromT(t);
                        if (isGood === true) return { color: greenText, backgroundColor: `rgba(${greenRgb},${a})`, textAlign: 'center' };
                        if (isGood === false) return { color: redText, backgroundColor: `rgba(${redRgb},${a})`, textAlign: 'center' };
                        return { color: neutralText, textAlign: 'center' };
                    }
                    if (col === 'max_drawdown_pct') {
                        const r = asRatio(v);
                        const t = clamp01(r / 0.30);
                        return styleGoodBad(false, t);
                    }
                    if (col === 'net_return_pct') {
                        const r = asRatio(v);
                        const t = clamp01(Math.abs(r) / 0.50);
                        if (r > 0) return styleGoodBad(true, t);
                        if (r < 0) return styleGoodBad(false, t);
                        return { color: neutralText, textAlign: 'center' };
                    }
                    if (col === 'roi_pct_on_margin') {
                        const t = clamp01(Math.abs(v) / 200.0);
                        if (v > 0) return styleGoodBad(true, t);
                        if (v < 0) return styleGoodBad(false, t);
                        return { color: neutralText, textAlign: 'center' };
                    }
                    if (col === 'win_rate') {
                        const r = asRatio(v);
                        const dist = Math.abs(r - 0.5);
                        const t = clamp01(dist / 0.25);
                        return styleGoodBad(r >= 0.5, t);
                    }
                    if (['profit_factor','cpc_index','common_sense_ratio'].includes(col)) {
                        const dist = Math.abs(v - 1.0);
                        const t = clamp01(dist / 1.0);
                        return styleGoodBad(v >= 1.0, t);
                    }
                    if (['sharpe','sortino'].includes(col)) {
                        if (v >= 1.0) {
                            const t = clamp01((v - 1.0) / 2.0);
                            return styleGoodBad(true, t);
                        }
                        if (v < 0.0) {
                            const t = clamp01(Math.abs(v) / 1.5);
                            return styleGoodBad(false, t);
                        }
                        return { color: neutralText, textAlign: 'center' };
                    }
                    if (col === 'net_profit') {
                        const t = clamp01(Math.abs(v) / 1000.0);
                        if (v > 0) return styleGoodBad(true, t);
                        if (v < 0) return styleGoodBad(false, t);
                        return { color: neutralText, textAlign: 'center' };
                    }
                    return {};
                }
                """
            )
            dd_style = JsCode(
                """
                function(params) {
                    const v = Number(params.value);
                    if (!isFinite(v)) { return {}; }
                    const r = (Math.abs(v) <= 1.0) ? v : (v / 100.0);
                    const t = Math.max(0.0, Math.min(1.0, r / 0.30));
                    const alpha = 0.10 + (0.25 * t);
                    return { color: '#ff7b72', backgroundColor: `rgba(248,81,73,${alpha})`, textAlign: 'center' };
                }
                """
            )
            def _pretty_label(s: str) -> str:
                raw = (s or "").strip()
                if not raw:
                    return raw
                # Known acronyms
                acr = {"roi", "atr", "ma", "rsi", "bbands", "macd", "sl", "tp", "dca", "pnl"}
                parts = []
                for tok in raw.replace("_", " ").replace(".", " ").split():
                    if tok.isdigit():
                        parts.append(f"[{tok}]")
                    else:
                        lo = tok.lower()
                        parts.append(tok.upper() if lo in acr else tok.capitalize())
                return " ".join(parts)

            def _metric_label(k: str) -> str:
                kk = str(k)
                return header_map.get(kk, _pretty_label(kk))

            def _config_label(k: str) -> str:
                kk = str(k or "").strip()
                # Nicer labels for common interval keys.
                if kk.endswith(".interval_min"):
                    base = kk.rsplit(".", 1)[0]
                    return _pretty_label(f"{base}.interval")
                if kk.endswith("ma_interval_min"):
                    return _pretty_label(kk.replace("ma_interval_min", "ma_interval"))
                return _pretty_label(kk)

            # --- Persisted preferences (per-user) ------------------------
            # These are lightweight settings stored via user_settings.
            if not st.session_state.get("runs_explorer_compare_prefs_loaded", False):
                try:
                    with open_db_connection() as _prefs_conn:
                        saved_strategy = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.strategy", default=None
                        )
                        saved_show_only = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.show_only_diff", default=None
                        )
                        saved_symbols = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.symbols", default=None
                        )
                        saved_sweeps = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.sweeps", default=None
                        )
                        saved_metric_cols = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.metric_cols", default=None
                        )
                        saved_cfg_cols = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.config_cols", default=None
                        )
                        saved_visible_cols = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.visible_cols", default=None
                        )
                        saved_col_state = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer_compare.columns_state", default=None
                        )
                    if isinstance(saved_show_only, bool):
                        st.session_state["runs_explorer_show_only_diff_cols"] = bool(saved_show_only)
                    if isinstance(saved_symbols, list):
                        st.session_state["runs_explorer_filter_symbols"] = [str(x) for x in saved_symbols if str(x).strip()]
                    if isinstance(saved_sweeps, list):
                        st.session_state["runs_explorer_filter_sweep_names"] = [str(x) for x in saved_sweeps if str(x).strip()]
                    if isinstance(saved_metric_cols, list):
                        st.session_state["runs_explorer_metric_cols"] = [str(x) for x in saved_metric_cols if str(x).strip()]
                    if isinstance(saved_cfg_cols, list):
                        st.session_state["runs_explorer_config_cols"] = [str(x) for x in saved_cfg_cols if str(x).strip()]
                    if isinstance(saved_visible_cols, list):
                        st.session_state["runs_explorer_compare_visible_cols"] = [
                            str(x) for x in saved_visible_cols if str(x).strip()
                        ]
                    if isinstance(saved_col_state, list) and saved_col_state:
                        st.session_state["runs_explorer_compare_columns_state"] = saved_col_state
                        st.session_state["runs_explorer_compare_columns_state_last_persisted"] = saved_col_state
                    # Strategy is applied after we know available strategies (below).
                    st.session_state["_runs_explorer_saved_strategy"] = saved_strategy
                except Exception:
                    pass
                st.session_state["runs_explorer_compare_prefs_loaded"] = True

            # Load persisted layout state for the Runs Explorer grid once per session.
            if not st.session_state.get("runs_explorer_columns_state_loaded", False):
                try:
                    with open_db_connection() as _prefs_conn:
                        saved_state = get_user_setting(
                            _prefs_conn, user_id, "ui.runs_explorer.columns_state", default=None
                        )
                    if isinstance(saved_state, list) and saved_state:
                        st.session_state["runs_explorer_columns_state"] = saved_state
                        st.session_state["runs_explorer_columns_state_last_persisted"] = saved_state
                except Exception:
                    pass
                st.session_state["runs_explorer_columns_state_loaded"] = True

            # --- Results-style filters (top bar) -------------------------
            try:
                all_strategies = list_backtest_run_strategies()
            except Exception:
                all_strategies = []

            def _reset_runs_explorer_page() -> None:
                st.session_state["runs_explorer_page"] = 1

            if "runs_explorer_page" not in st.session_state:
                st.session_state["runs_explorer_page"] = 1
            if "runs_explorer_page_size" not in st.session_state:
                st.session_state["runs_explorer_page_size"] = 100
            if "runs_explorer_filter_model" not in st.session_state:
                st.session_state["runs_explorer_filter_model"] = None
            if "runs_explorer_sort_model" not in st.session_state:
                st.session_state["runs_explorer_sort_model"] = None

            page_size_options = [25, 50, 100, 200]
            page_size = int(st.session_state.get("runs_explorer_page_size") or page_size_options[2])
            if page_size not in page_size_options:
                page_size_options.append(page_size)
                page_size_options = sorted(set(page_size_options))
            page = int(st.session_state.get("runs_explorer_page") or 1)

            with open_db_connection() as _filter_conn:
                options_extra = _runs_global_filters_to_extra_where(get_global_filters())
                symbol_opts = list_backtest_run_symbols(
                    _filter_conn,
                    extra_where=options_extra if options_extra else None,
                    user_id=user_id,
                )
                if not symbol_opts:
                    symbol_opts = list_backtest_run_symbols(_filter_conn, extra_where=None, user_id=user_id)
                tf_opts = list_backtest_run_timeframes(
                    _filter_conn,
                    extra_where=options_extra if options_extra else None,
                    user_id=user_id,
                )
                if not tf_opts:
                    tf_opts = list_backtest_run_timeframes(_filter_conn, extra_where=None, user_id=user_id)
                strat_opts = list_backtest_run_strategies_filtered(
                    _filter_conn,
                    extra_where=options_extra if options_extra else None,
                    user_id=user_id,
                )
                if not strat_opts:
                    strat_opts = list_backtest_run_strategies_filtered(_filter_conn, extra_where=None, user_id=user_id)
                bounds = get_runs_numeric_bounds(
                    _filter_conn,
                    extra_where=options_extra if options_extra else None,
                    user_id=user_id,
                )
                if not any(v is not None for v in bounds.values()):
                    bounds = get_runs_numeric_bounds(_filter_conn, extra_where=None, user_id=user_id)

            ui_filters = _runs_server_filter_controls(
                "runs_explorer",
                include_run_type=True,
                on_change=_reset_runs_explorer_page,
                symbol_options=symbol_opts,
                strategy_options=strat_opts,
                timeframe_options=tf_opts,
                bounds=bounds,
            )

            runs_query_cols = [
                "run_id",
                "created_at",
                "symbol",
                "timeframe",
                "strategy_name",
                "strategy_version",
                "market_type",
                "config_json",
                "metrics_json",
                "metadata_json",
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
                "avg_position_time_seconds",
                "shortlist",
                "shortlist_note",
                "sweep_id",
                "sweep_name",
                "sweep_exchange_id",
                "sweep_scope",
                "sweep_assets_json",
                "sweep_category_id",
                "base_asset",
                "quote_asset",
                "icon_uri",
            ]

            def _normalize_cfg_keys(raw: Any) -> List[str]:
                out: List[str] = []
                if isinstance(raw, (list, tuple)):
                    for v in raw:
                        s = str(v).strip()
                        if s:
                            out.append(s)
                elif isinstance(raw, str) and raw.strip():
                    out.append(raw.strip())
                return out

            config_keys_for_query = _normalize_cfg_keys(st.session_state.get("runs_explorer_config_cols"))
            st.session_state["runs_explorer_config_keys_query"] = list(config_keys_for_query)

            extra_where = _runs_global_filters_to_extra_where(get_global_filters())
            if ui_filters:
                extra_where.update(ui_filters)

            with _perf("runs_explorer_compare.load_rows"):
                with open_db_connection() as _runs_conn:
                    rows, total_count = list_runs_server_side(
                        conn=_runs_conn,
                        page=page,
                        page_size=page_size,
                        filter_model=st.session_state.get("runs_explorer_filter_model"),
                        sort_model=st.session_state.get("runs_explorer_sort_model"),
                        visible_columns=runs_query_cols,
                        config_keys=config_keys_for_query,
                        user_id=user_id,
                        extra_where=extra_where if extra_where else None,
                    )

            total_count = int(total_count or 0)
            max_page = max(1, math.ceil(total_count / float(page_size))) if total_count else 1
            if page > max_page:
                st.session_state["runs_explorer_page"] = max_page
                st.rerun()

            if st.checkbox("Show active filters", value=False, key="runs_explorer_show_active_filters"):
                st.json(_safe_json_for_display({"global": _runs_global_filters_to_extra_where(get_global_filters()), "ui": ui_filters}))

            if st.checkbox("Show grid state (debug)", value=False, key="runs_explorer_debug_grid_state"):
                st.info("Showing raw grid state for debugging filter/sort extraction.")

            if not rows:
                st.info("No runs found for the current filters.")
                return

            # Build a lightweight filter dataframe first (avoid parsing config/metrics JSON for rows we filter out).
            with _perf("runs_explorer_compare.build_filter_df"):
                df_filter = pd.DataFrame(rows)

            # Keep raw exchange symbol in `symbol` and track it in `symbol_full` for display/debug.
            if "symbol" in df_filter.columns:
                try:
                    df_filter["symbol_full"] = df_filter["symbol"].fillna("").astype(str)
                except Exception:
                    df_filter["symbol_full"] = df_filter["symbol"].astype(str)
            else:
                df_filter["symbol_full"] = ""

            if "symbol" not in df_filter.columns:
                df_filter["symbol"] = ""

            if "strategy_name" in df_filter.columns and "strategy" not in df_filter.columns:
                df_filter["strategy"] = df_filter["strategy_name"]
            if "market_type" in df_filter.columns and "market" not in df_filter.columns:
                df_filter["market"] = df_filter["market_type"]
            if "market" in df_filter.columns:
                try:
                    m = df_filter["market"].fillna("").astype(str).str.strip().str.lower()
                    m = m.replace({"perp": "perps", "futures": "perps", "future": "perps"})
                    df_filter["market"] = m.map({"perps": "PERPS", "spot": "SPOT"}).fillna(m.str.upper())
                except Exception:
                    pass

            if "sweep_name" in df_filter.columns and "sweep" not in df_filter.columns:
                df_filter["sweep"] = df_filter["sweep_name"].fillna("").astype(str)

            if "direction" not in df_filter.columns:
                try:
                    df_filter["direction"] = df_filter.get("config_json").apply(infer_trade_direction_label)
                except Exception:
                    df_filter["direction"] = ""

            st.caption(f"{len(rows)} rows shown (page {page})")

            # Parse JSON once per run row (after filtering).
            parsed: List[Dict[str, Any]] = []
            flat_cfgs: List[Dict[str, Any]] = []
            metrics_dicts: List[Dict[str, Any]] = []
            for r in rows:
                config_obj: Dict[str, Any] = {}
                metrics_obj: Dict[str, Any] = {}
                try:
                    raw = r.get("config_json")
                    config_obj = json.loads(raw) if isinstance(raw, str) and raw.strip() else {}
                    if not isinstance(config_obj, dict):
                        config_obj = {}
                except Exception:
                    config_obj = {}
                try:
                    rawm = r.get("metrics_json")
                    metrics_obj = json.loads(rawm) if isinstance(rawm, str) and rawm.strip() else {}
                    if not isinstance(metrics_obj, dict):
                        metrics_obj = {}
                except Exception:
                    metrics_obj = {}

                strat = str(r.get("strategy_name") or "").strip()
                flat = flatten_config(config_obj, strat)

                parsed.append({"_base": r, "metrics": metrics_obj, "config": config_obj, "config_flat": flat})
                flat_cfgs.append(flat)
                metrics_dicts.append(metrics_obj)

            if not parsed:
                st.info("No runs match the current filters.")
                return

            # --- Column selection ---------------------------------------
            metric_keys: List[str] = []
            try:
                mset = set()
                for m in metrics_dicts:
                    if isinstance(m, dict):
                        mset.update(str(k) for k in m.keys())
                metric_keys = sorted(mset)
            except Exception:
                metric_keys = []

            cfg_keys_all = sorted({k for cfg in flat_cfgs for k in (cfg.keys() if isinstance(cfg, dict) else [])})

            try:
                saved_cfg = st.session_state.get("runs_explorer_config_cols")
                if isinstance(saved_cfg, (list, tuple)) and saved_cfg:
                    for k in saved_cfg:
                        ks = str(k).strip()
                        if ks and ks not in cfg_keys_all:
                            cfg_keys_all.append(ks)
            except Exception:
                pass

            metric_defaults = [
                k
                for k in ["net_profit", "roi_pct_on_margin", "net_return_pct", "max_drawdown_pct", "sharpe"]
                if k in metric_keys
            ]
            if not metric_defaults:
                metric_defaults = metric_keys[:5]

            strat_for_defaults = ""
            try:
                picked_strategies = st.session_state.get("runs_explorer:compare__strategy")
                if isinstance(picked_strategies, str):
                    picked_strategies = [picked_strategies]
                if isinstance(picked_strategies, (list, tuple)) and len(picked_strategies) == 1:
                    strat_for_defaults = str(picked_strategies[0]).strip()
            except Exception:
                strat_for_defaults = ""
            cfg_defaults: List[str] = []
            # Requested defaults: indicator intervals (when available).
            wanted_cfg = [
                "trend.ma_interval_min",
                "rsi.interval_min",
                "bbands.interval_min",
                "macd.interval_min",
            ]
            cfg_defaults = [k for k in wanted_cfg if k in cfg_keys_all]
            if not cfg_defaults and strat_for_defaults and strat_for_defaults in STRATEGY_CONFIG_KEYS:
                cfg_defaults = [k for k in STRATEGY_CONFIG_KEYS[strat_for_defaults] if k in cfg_keys_all]
            if not cfg_defaults:
                cfg_defaults = [k for k in top_config_keys_by_frequency(flat_cfgs, max_keys=10) if k in cfg_keys_all]

            sel_cols = st.columns([1, 1, 1])
            with sel_cols[0]:
                try:
                    if "runs_explorer_metric_cols" in st.session_state and isinstance(
                        st.session_state.get("runs_explorer_metric_cols"), list
                    ):
                        st.session_state["runs_explorer_metric_cols"] = [
                            str(x) for x in st.session_state["runs_explorer_metric_cols"] if str(x) in set(metric_keys)
                        ]
                except Exception:
                    pass

                if "runs_explorer_metric_cols" in st.session_state:
                    selected_metric_keys = st.multiselect(
                        "Metrics columns",
                        options=metric_keys,
                        key="runs_explorer_metric_cols",
                        format_func=_metric_label,
                    )
                else:
                    selected_metric_keys = st.multiselect(
                        "Metrics columns",
                        options=metric_keys,
                        default=metric_defaults,
                        key="runs_explorer_metric_cols",
                        format_func=_metric_label,
                    )
            with sel_cols[1]:
                if "runs_explorer_config_cols" in st.session_state:
                    selected_cfg_keys = st.multiselect(
                        "Config columns",
                        options=cfg_keys_all,
                        key="runs_explorer_config_cols",
                        format_func=_config_label,
                    )
                else:
                    selected_cfg_keys = st.multiselect(
                        "Config columns",
                        options=cfg_keys_all,
                        default=cfg_defaults,
                        key="runs_explorer_config_cols",
                        format_func=_config_label,
                    )
            with sel_cols[2]:
                if "runs_explorer_show_only_diff_cols" in st.session_state:
                    show_only_diff_cols = st.checkbox(
                        "Show only differing config columns",
                        key="runs_explorer_show_only_diff_cols",
                    )
                else:
                    show_only_diff_cols = st.checkbox(
                        "Show only differing config columns",
                        value=True,
                        key="runs_explorer_show_only_diff_cols",
                    )

            try:
                prev_cfg = st.session_state.get("runs_explorer_compare_last_cfg_keys")
                prev_metric = st.session_state.get("runs_explorer_compare_last_metric_keys")
                if prev_cfg != list(selected_cfg_keys or []) or prev_metric != list(selected_metric_keys or []):
                    st.session_state.pop("runs_explorer_compare_columns_state_runtime", None)
                st.session_state["runs_explorer_compare_last_cfg_keys"] = list(selected_cfg_keys or [])
                st.session_state["runs_explorer_compare_last_metric_keys"] = list(selected_metric_keys or [])
            except Exception:
                pass

            # If selected config columns changed, rerun once so SQL extractions include them.
            try:
                query_keys_snapshot = st.session_state.get("runs_explorer_config_keys_query") or []
                if list(selected_cfg_keys or []) != list(query_keys_snapshot or []):
                    st.session_state["runs_explorer_config_cols"] = list(selected_cfg_keys or [])
                    st.session_state["runs_explorer_config_keys_query"] = list(selected_cfg_keys or [])
                    st.rerun()
            except Exception:
                pass

            # --- Build dataframe ----------------------------------------
            base_rows: List[Dict[str, Any]] = []
            cfg_field_by_key: Dict[str, str] = {k: f"cfg__{k}" for k in selected_cfg_keys}

            def _coerce_dt(v: Any) -> Optional[datetime]:
                try:
                    if v is None:
                        return None
                    if isinstance(v, (int, float)):
                        # assume ms if it looks like ms, else seconds
                        x = float(v)
                        if x > 10_000_000_000:
                            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
                        return datetime.fromtimestamp(x, tz=timezone.utc)
                    s = str(v).strip()
                    if not s:
                        return None
                    # ISO or other parseable
                    try:
                        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                    except Exception:
                        return None
                except Exception:
                    return None

            def _extract_range_from_cfg(cfg: Any) -> tuple[Optional[datetime], Optional[datetime]]:
                if not isinstance(cfg, dict):
                    return (None, None)
                # Common places
                for obj in (
                    cfg,
                    cfg.get("data_settings") if isinstance(cfg.get("data_settings"), dict) else None,
                    cfg.get("range_params") if isinstance(cfg.get("range_params"), dict) else None,
                    cfg.get("metadata") if isinstance(cfg.get("metadata"), dict) else None,
                ):
                    if not isinstance(obj, dict):
                        continue
                    s = obj.get("start_ts") or obj.get("start_time") or obj.get("since")
                    e = obj.get("end_ts") or obj.get("end_time") or obj.get("until")
                    sd = _coerce_dt(s)
                    ed = _coerce_dt(e)
                    if sd or ed:
                        return (sd, ed)
                return (None, None)

            def _fmt_duration(sd: Optional[datetime], ed: Optional[datetime]) -> str:
                if not sd or not ed:
                    return ""
                try:
                    delta = ed - sd
                    if delta.total_seconds() <= 0:
                        return ""
                    days = int(delta.total_seconds() // 86400)
                    hours = int((delta.total_seconds() % 86400) // 3600)
                    if days > 0:
                        return f"{days}d {hours}h" if hours else f"{days}d"
                    mins = int((delta.total_seconds() % 3600) // 60)
                    return f"{hours}h {mins}m" if hours else f"{mins}m"
                except Exception:
                    return ""

            for p in parsed:
                b = dict(p.get("_base") or {})
                sweep_name = str(b.get("sweep_name") or "").strip()
                if not sweep_name:
                    sid = b.get("sweep_id")
                    sweep_name = f"#{sid}" if sid is not None and str(sid).strip() else ""
                # Prefer canonical DB run timings; fall back to config-derived range.
                cfg_obj = p.get("config") if isinstance(p.get("config"), dict) else {}
                sd = _coerce_dt(b.get("start_time"))
                ed = _coerce_dt(b.get("end_time"))
                if not sd and not ed:
                    sd, ed = _extract_range_from_cfg(cfg_obj)
                avg_pos_seconds = b.get("avg_position_time_seconds")
                if avg_pos_seconds is None:
                    avg_pos_seconds = b.get("avg_position_time_s")
                row: Dict[str, Any] = {
                    "run_id": b.get("run_id"),
                    "created_at": b.get("created_at"),
                    "symbol": b.get("symbol"),
                    "asset_display": b.get("asset_display"),
                    "icon_uri": b.get("icon_uri"),
                    "base_asset": b.get("base_asset"),
                    "quote_asset": b.get("quote_asset"),
                    "market_type": b.get("market_type"),
                    "timeframe": b.get("timeframe"),
                    "direction": infer_trade_direction_label(cfg_obj),
                    "strategy": b.get("strategy_name"),
                    "sweep": sweep_name,
                    "start_date": sd.date().isoformat() if sd else None,
                    "end_date": ed.date().isoformat() if ed else None,
                    "duration": _fmt_duration(sd, ed),
                    "avg_position_time_seconds": avg_pos_seconds,
                    "avg_position_time": format_duration(avg_pos_seconds),
                    "shortlist": bool(b.get("shortlist")) if b.get("shortlist") is not None else False,
                    "shortlist_note": str(b.get("shortlist_note") or ""),
                    # Keep common metric fields available for filtering/formatting even if not selected.
                    "net_return_pct": b.get("net_return_pct"),
                    "roi_pct_on_margin": b.get("roi_pct_on_margin"),
                    "max_drawdown_pct": b.get("max_drawdown_pct"),
                    "cpc_index": b.get("cpc_index"),
                    "sharpe": b.get("sharpe"),
                }
                m = p.get("metrics") or {}
                for k in selected_metric_keys:
                    row[str(k)] = m.get(k) if isinstance(m, dict) else None
                cf = p.get("config_flat") or {}
                for k in selected_cfg_keys:
                    col = cfg_field_by_key.get(k)
                    if not col:
                        continue
                    if isinstance(b, dict) and col in b:
                        row[col] = b.get(col)
                    else:
                        row[col] = cf.get(k) if isinstance(cf, dict) else None
                base_rows.append(row)

            df = pd.DataFrame(base_rows)

            if "asset_display" not in df.columns:
                try:
                    df["asset_display"] = df["symbol"].fillna("").astype(str)
                except Exception:
                    df["asset_display"] = df.get("symbol", "")
            try:
                from project_dragon.data_online import woox_symbol_to_ccxt_symbol

                def _rx_normalize_asset_display(v: Any) -> str:
                    s = str(v or "").strip()
                    if not s:
                        return ""
                    if s.upper().startswith(("PERP_", "SPOT_")):
                        try:
                            display, _ = woox_symbol_to_ccxt_symbol(s)
                            return str(display or s).strip()
                        except Exception:
                            return s
                    return s

                df["asset_display"] = df["asset_display"].apply(_rx_normalize_asset_display)
            except Exception:
                pass

            # Default ordering: CPC Index desc.
            try:
                if "cpc_index" in df.columns:
                    df = df.sort_values(by="cpc_index", ascending=False, na_position="last")
            except Exception:
                pass

            # Match Results grid: normalize market label used by Asset renderer.
            if "market_type" in df.columns and "market" not in df.columns:
                df["market"] = df["market_type"]
            if "market" in df.columns:
                try:
                    m = df["market"].fillna("").astype(str).str.strip().str.lower()
                    m = m.replace({"perp": "perps", "futures": "perps", "future": "perps"})
                    df["market"] = m.map({"perps": "PERPS", "spot": "SPOT"}).fillna(m.str.upper())
                except Exception:
                    pass

            # Compute diff flags for selected config keys.
            _, cfg_flags = compute_config_diff_flags(flat_cfgs, selected_cfg_keys)
            cfg_key_to_diff_field: Dict[str, str] = {}
            for k in selected_cfg_keys:
                cfg_col = cfg_field_by_key.get(k)
                if not cfg_col:
                    continue
                diff_col = f"__diff__{cfg_col}"
                cfg_key_to_diff_field[k] = diff_col
                try:
                    df[diff_col] = [bool(f.get(k)) for f in cfg_flags]
                except Exception:
                    df[diff_col] = False

            differing_cfg_keys: List[str] = []
            for k in selected_cfg_keys:
                diff_col = cfg_key_to_diff_field.get(k)
                if not diff_col or diff_col not in df.columns:
                    continue
                try:
                    if bool(df[diff_col].any()):
                        differing_cfg_keys.append(k)
                except Exception:
                    continue

            effective_cfg_keys = differing_cfg_keys if show_only_diff_cols else list(selected_cfg_keys)
            if show_only_diff_cols and not differing_cfg_keys and selected_cfg_keys:
                # If nothing differs on this page, still show explicitly selected config columns.
                effective_cfg_keys = list(selected_cfg_keys)
            hidden_cols = [v for v in cfg_key_to_diff_field.values() if v in df.columns]

            ordered_cols: List[str] = [
                "run_id",
                "created_at",
                "start_date",
                "end_date",
                "duration",
                "avg_position_time",
                "symbol",
                "timeframe",
                "direction",
                "strategy",
                "sweep",
            ]

            # Visible columns picker (non-metric, non-config only) — matches Results UX.
            base_identity_cols = [c for c in ordered_cols if c in df.columns]
            always_visible_cols = [c for c in ("symbol",) if c in base_identity_cols]
            # Run ID is an internal identifier for this view; keep it available for export
            # but do not show it as a visible/toggleable column.
            candidate_cols = [c for c in base_identity_cols if c not in {"market", "run_id"}]
            for c in ("shortlist", "shortlist_note"):
                if c in df.columns and c not in candidate_cols:
                    candidate_cols.append(c)

            # Migrate legacy Avg Position Time column key.
            try:
                persisted = st.session_state.get("runs_explorer_compare_visible_cols")
                if isinstance(persisted, list) and "avg_position_time_s" in persisted:
                    st.session_state["runs_explorer_compare_visible_cols"] = [
                        ("avg_position_time" if c == "avg_position_time_s" else c) for c in persisted
                    ]
                persisted = st.session_state.get("runs_explorer_compare_visible_cols")
            except Exception:
                pass

            # If the user has interacted with the Visible columns popover, the per-column checkbox
            # widget values are the authoritative source (they persist across reruns).
            try:
                any_checkbox = any(
                    (f"runs_explorer_compare_col_{c}" in st.session_state) for c in (candidate_cols or [])
                )
            except Exception:
                any_checkbox = False

            if any_checkbox:
                chosen_visible = [
                    c for c in candidate_cols if bool(st.session_state.get(f"runs_explorer_compare_col_{c}", False))
                ]
            else:
                default_visible = st.session_state.get("runs_explorer_compare_visible_cols")
                if isinstance(default_visible, list) and default_visible:
                    chosen_visible = [c for c in default_visible if c in candidate_cols]
                else:
                    chosen_visible = list(candidate_cols)
            for c in always_visible_cols:
                if c not in chosen_visible:
                    chosen_visible.append(c)
            visible_cols = chosen_visible
            st.session_state["runs_explorer_compare_visible_cols"] = list(visible_cols)

            ordered_cols += [str(k) for k in selected_metric_keys]
            ordered_cols += [cfg_field_by_key[k] for k in effective_cfg_keys if cfg_field_by_key.get(k) in df.columns]
            ordered_cols += [c for c in ("shortlist", "shortlist_note") if c in df.columns]

            # Keep internal identity fields even if not displayed so cell renderers/valueGetters
            # can build the same composite labels as the Results grid (e.g., Asset = "BTC - PERPS").
            internal_keep = [
                c
                for c in (
                    "icon_uri",
                    "market",
                    "market_type",
                    "base_asset",
                    "quote_asset",
                    "symbol_full",
                    "asset_display",
                )
                if c in df.columns
            ]
            df = df[[c for c in (ordered_cols + internal_keep) if c in df.columns] + hidden_cols]

            # --- Results-style Asset renderer (icon + label) -----------
            asset_market_getter = asset_market_getter_js(
                symbol_field="symbol",
                market_field="market",
                base_asset_field="asset_display",
                asset_field="asset_display",
            )
            asset_renderer = asset_renderer_js(icon_field="icon_uri", size_px=18, text_color="#FFFFFF")

            # --- Grid ----------------------------------------------------
            # Presentation-only: convert date columns to local strings before AGGrid.
            try:
                tz = _ui_display_tz()
                if "created_at" in df.columns:
                    df["created_at"] = [_ui_local_dt_ampm(v, tz) for v in df["created_at"].tolist()]
                for _c in ("start_date", "end_date"):
                    if _c in df.columns:
                        df[_c] = [_ui_local_date_iso(v, tz) for v in df[_c].tolist()]
            except Exception:
                pass
            gb = GridOptionsBuilder.from_dataframe(df)
            gb.configure_default_column(filter=True, sortable=True, resizable=True)
            gb.configure_selection("multiple", use_checkbox=True)
            gb.configure_grid_options(headerHeight=58, rowHeight=38, suppressRowHoverHighlight=False, animateRows=False)

            runtime_state = st.session_state.get("runs_explorer_compare_columns_state_runtime")
            saved_state = st.session_state.get("runs_explorer_compare_columns_state")
            state_source = runtime_state if isinstance(runtime_state, list) and runtime_state else saved_state
            _rx_widths: dict[str, int] = {}
            try:
                if isinstance(state_source, list) and state_source:
                    for item in state_source:
                        if not isinstance(item, dict):
                            continue
                        col_id = item.get("colId") or item.get("col_id") or item.get("field")
                        if not col_id:
                            continue
                        width_val = item.get("width") or item.get("actualWidth")
                        if width_val is None:
                            continue
                        try:
                            _rx_widths[str(col_id)] = int(width_val)
                        except Exception:
                            continue
            except Exception:
                pass

            def _rx_w(col: str, default: int) -> int:
                try:
                    return max(60, int(_rx_widths.get(str(col), default)))
                except Exception:
                    return default

            shared_col_defs: list[dict[str, Any]] = []
            if "start_date" in df.columns:
                shared_col_defs.append(
                    col_date_ddmmyy(
                        "start_date",
                        header_map.get("start_date", "Start"),
                        pinned="left",
                        width=_rx_w("start_date", 110),
                        hide=("start_date" not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                )
            if "end_date" in df.columns:
                shared_col_defs.append(
                    col_date_ddmmyy(
                        "end_date",
                        header_map.get("end_date", "End"),
                        pinned="left",
                        width=_rx_w("end_date", 110),
                        hide=("end_date" not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                )
            if "timeframe" in df.columns:
                shared_col_defs.append(
                    col_timeframe_pill(
                        field="timeframe",
                        header=header_map.get("timeframe", "Timeframe"),
                        pinned="left",
                        width=_rx_w("timeframe", 90),
                        hide=("timeframe" not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                )
            if "direction" in df.columns:
                shared_col_defs.append(
                    col_direction_pill(
                        field="direction",
                        header=header_map.get("direction", "Direction"),
                        pinned="left",
                        width=_rx_w("direction", 100),
                        hide=("direction" not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                )
            if "avg_position_time" in df.columns:
                shared_col_defs.append(
                    col_avg_position_time(
                        field="avg_position_time",
                        header=header_map.get("avg_position_time", "Avg Position Time"),
                        width=_rx_w("avg_position_time", 150),
                        hide=("avg_position_time" not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                )
            apply_columns(gb, shared_col_defs, saved_widths=_rx_widths)

            # Identity columns (match Results page formatting)
            if "run_id" in df.columns:
                gb.configure_column(
                    "run_id",
                    headerName=header_map.get("run_id", "Run ID"),
                    hide=True,
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            if "created_at" in df.columns:
                gb.configure_column(
                    "created_at",
                    headerName=header_map.get("created_at", "Run Date"),
                    pinned="left",
                    width=_rx_w("created_at", 120),
                    hide=("created_at" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            if "duration" in df.columns:
                gb.configure_column(
                    "duration",
                    headerName=header_map.get("duration", "Duration"),
                    pinned="left",
                    width=_rx_w("duration", 105),
                    hide=("duration" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                    cellClass="ag-center-aligned-cell",
                )
            if "symbol" in df.columns:
                gb.configure_column(
                    "symbol",
                    headerName=header_map.get("symbol", "Asset"),
                    pinned="left",
                    width=_rx_w("symbol", 140),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                    hide=("symbol" not in visible_cols),
                    cellRenderer=asset_renderer,
                    valueGetter=asset_market_getter,
                    checkboxSelection=True,
                    headerCheckboxSelection=True,
                )
            if "strategy" in df.columns:
                gb.configure_column(
                    "strategy",
                    headerName=header_map.get("strategy", "Strategy"),
                    pinned="left",
                    width=_rx_w("strategy", 160),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                    hide=("strategy" not in visible_cols),
                )
            if "sweep" in df.columns:
                gb.configure_column(
                    "sweep",
                    headerName=header_map.get("sweep", "Sweep"),
                    width=_rx_w("sweep", 200),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                    hide=("sweep" not in visible_cols),
                )

            # Hide Results-style helper columns
            for hidden in ("icon_uri", "base_asset", "quote_asset", "market_type", "market", "asset_display"):
                if hidden in df.columns:
                    gb.configure_column(hidden, hide=True)

            # Hide helper diff columns
            for hc in hidden_cols:
                gb.configure_column(hc, hide=True)

            # Metrics columns (use Results formatters/styles)
            for k in selected_metric_keys:
                mk = str(k)
                if mk not in df.columns:
                    continue
                fmt = None
                style = None
                if mk in {"net_return_pct", "win_rate"}:
                    fmt = pct_formatter
                    style = metrics_gradient_style
                elif mk == "roi_pct_on_margin":
                    fmt = roi_pct_formatter
                    style = metrics_gradient_style
                elif mk in {"max_drawdown_pct", "max_drawdown", "max_dd_pct", "max_dd"}:
                    fmt = pct_formatter
                    style = dd_style
                elif mk in {"net_profit", "sharpe", "sortino", "profit_factor", "cpc_index", "common_sense_ratio"}:
                    fmt = num2_formatter
                    style = metrics_gradient_style
                gb.configure_column(
                    mk,
                    headerName=_metric_label(mk),
                    width=_rx_w(mk, 130),
                    type=["numericColumn"],
                    valueFormatter=fmt,
                    cellStyle=style,
                    cellClass="ag-center-aligned-cell",
                )

            # Guardrail: ensure core % columns keep % formatting even when the metric-selection
            # UI changes (e.g., adding config columns causes reruns).
            for core_pct_col, core_style in (
                ("net_return_pct", metrics_gradient_style),
                ("max_drawdown_pct", dd_style),
                ("max_drawdown", dd_style),
                ("win_rate", metrics_gradient_style),
            ):
                if core_pct_col in df.columns:
                    gb.configure_column(
                        core_pct_col,
                        headerName=header_map.get(core_pct_col, core_pct_col.replace("_", " ").title()),
                        width=_rx_w(core_pct_col, 130),
                        type=["numericColumn"],
                        valueFormatter=pct_formatter,
                        cellStyle=core_style,
                        cellClass="ag-center-aligned-cell",
                    )

            # Config columns: label headings + diff highlighting
            for k in effective_cfg_keys:
                cfg_col = cfg_field_by_key.get(k)
                diff_field = cfg_key_to_diff_field.get(k)
                if not cfg_col or cfg_col not in df.columns or not diff_field:
                    continue
                style_js = JsCode(
                    (
                        """
                        function(params) {
                            try {
                                const d = params && params.data ? params.data : null;
                                if (d && d['__DIFF__'] === true) {
                                    return { 'backgroundColor': 'rgba(250, 204, 21, 0.18)', 'fontWeight': '700' };
                                }
                            } catch (e) {}
                            return null;
                        }
                        """
                    ).replace("__DIFF__", diff_field)
                )
                gb.configure_column(
                    cfg_col,
                    headerName=_config_label(str(k)),
                    width=_rx_w(cfg_col, 170),
                    cellStyle=style_js,
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )

            # Restore persisted column sizing/order for this compare grid.
            columns_state = None
            if isinstance(state_source, list) and state_source:
                try:
                    sanitized: list[dict] = []
                    valid_cols = set(df.columns.tolist())
                    for item in state_source:
                        if not isinstance(item, dict):
                            continue
                        d = dict(item)
                        # Visible columns picker is authoritative; don't persist `hide` here.
                        d.pop("hide", None)
                        col_id = d.get("colId") or d.get("col_id") or d.get("field")
                        if not col_id:
                            continue
                        if str(col_id) not in valid_cols:
                            continue
                        sanitized.append(d)
                    columns_state = sanitized or None
                except Exception:
                    columns_state = None

            def _cols_signature(cols: Sequence[str]) -> str:
                try:
                    payload = "|".join([str(c) for c in cols if c])
                    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
                except Exception:
                    return "default"

            grid_key = f"runs_explorer_compare_grid__{_cols_signature(list(selected_metric_keys or []) + list(selected_cfg_keys or []))}"

            grid = AgGrid(
                df,
                gridOptions=gb.build(),
                # Sorting/filtering should be client-side (no rerun). Only selection should rerun
                # (for CSV selection/export) to keep the UI responsive.
                update_mode=(
                    GridUpdateMode.SELECTION_CHANGED
                    | GridUpdateMode.MODEL_CHANGED
                ),
                data_return_mode=DataReturnMode.AS_INPUT,
                enable_enterprise_modules=False,
                allow_unsafe_jscode=True,
                height=620,
                theme="dark",
                custom_css=_aggrid_dark_custom_css(),
                key=grid_key,
                columns_state=columns_state,
            )

            # Persist runtime column sizing so filtering/toggling doesn't reset widths.
            try:
                runtime_cols = None
                if hasattr(grid, "columns_state"):
                    runtime_cols = getattr(grid, "columns_state")
                if not runtime_cols and hasattr(grid, "get"):
                    runtime_cols = grid.get("columns_state")
                if isinstance(runtime_cols, list) and runtime_cols:
                    sanitized_runtime: list[dict] = []
                    valid_cols = set(df.columns.tolist())
                    for item in runtime_cols:
                        if not isinstance(item, dict):
                            continue
                        d = dict(item)
                        d.pop("hide", None)
                        col_id = d.get("colId") or d.get("col_id") or d.get("field")
                        if not col_id or str(col_id) not in valid_cols:
                            continue
                        sanitized_runtime.append(d)
                    st.session_state["runs_explorer_compare_columns_state_runtime"] = sanitized_runtime
            except Exception:
                pass

            grid_filter_model, grid_sort_model = _extract_aggrid_models(grid)
            if grid_filter_model is not None or grid_sort_model is not None:
                if grid_filter_model != st.session_state.get("runs_explorer_filter_model") or grid_sort_model != st.session_state.get("runs_explorer_sort_model"):
                    st.session_state["runs_explorer_filter_model"] = grid_filter_model
                    st.session_state["runs_explorer_sort_model"] = grid_sort_model
                    st.session_state["runs_explorer_page"] = 1
                    st.rerun()

            if st.session_state.get("runs_explorer_debug_grid_state"):
                try:
                    st.json(grid.get("grid_state") or grid.get("gridState") or grid)
                except Exception:
                    st.json(grid)

            # Open run detail reliably when exactly one row is selected.
            # (If multiple rows are selected we assume the user is doing CSV export.)
            try:
                sel_raw = grid.get("selected_rows")
                sel: list[dict[str, Any]] = []
                if sel_raw is None:
                    sel = []
                elif isinstance(sel_raw, list):
                    sel = sel_raw
                elif isinstance(sel_raw, pd.DataFrame):
                    sel = sel_raw.to_dict("records")
                else:
                    sel = list(sel_raw)
                if isinstance(sel, list) and len(sel) == 1:
                    rid = str((sel[0] or {}).get("run_id") or "").strip()
                    if rid:
                        last_opened = str(st.session_state.get("_runs_explorer_last_opened_run_id") or "").strip()
                        if rid != last_opened:
                            st.session_state["_runs_explorer_last_opened_run_id"] = rid
                            st.session_state["deep_link_run_id"] = rid
                            st.session_state["current_page"] = "Run"
                            _update_query_params(page="Run", run_id=rid)
                            st.rerun()
            except Exception:
                pass

            export_cols = [c for c in df.columns if not c.startswith("__diff__")]
            export_df = df[export_cols].copy()
            try:
                sel = grid.get("selected_rows")
                if isinstance(sel, list) and sel:
                    export_df = pd.DataFrame(sel)
            except Exception:
                pass

            csv_bytes = export_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                "Download CSV (selected or all)",
                data=csv_bytes,
                file_name="runs_explorer.csv",
                mime="text/csv",
                key="runs_explorer_download_csv",
            )

            # Bottom controls (Visible columns + Save/Reset layout), like the Results page.
            bottom_bar = st.columns([2, 3], gap="large")
            with bottom_bar[0]:
                st.caption("Columns")
                picker_cols = list(candidate_cols)

                persisted = st.session_state.get("runs_explorer_compare_visible_cols")
                if isinstance(persisted, list) and persisted:
                    persisted_base = [c for c in persisted if c in picker_cols]
                else:
                    persisted_base = [c for c in visible_cols if c in picker_cols]

                chosen_base: list[str] = []
                with st.popover("Visible columns", use_container_width=False):
                    st.caption("Toggle visible columns")
                    for c in picker_cols:
                        label = header_map.get(c, str(c).replace("_", " ").title())
                        wkey = f"runs_explorer_compare_col_{c}"
                        desired = bool(c in persisted_base)
                        # Initialize from persisted visibility only if the widget key isn't already set.
                        if wkey not in st.session_state:
                            st.session_state[wkey] = desired
                        checked = st.checkbox(label, key=wkey)
                        if checked:
                            chosen_base.append(c)

                chosen_visible_ui = [c for c in picker_cols if c in chosen_base]
                for c in always_visible_cols:
                    if c not in chosen_visible_ui:
                        chosen_visible_ui.append(c)

                last_persisted = st.session_state.get("runs_explorer_compare_visible_cols_last_persisted")
                if isinstance(chosen_visible_ui, list) and chosen_visible_ui and chosen_visible_ui != last_persisted:
                    try:
                        with open_db_connection() as _prefs_conn:
                            set_user_setting(
                                _prefs_conn,
                                user_id,
                                "ui.runs_explorer_compare.visible_cols",
                                list(chosen_visible_ui),
                            )
                        st.session_state["runs_explorer_compare_visible_cols"] = list(chosen_visible_ui)
                        st.session_state["runs_explorer_compare_visible_cols_last_persisted"] = list(chosen_visible_ui)
                    except Exception:
                        pass
            with bottom_bar[1]:
                st.caption("Layout")

                def _request_save_layout() -> None:
                    st.session_state["runs_explorer_compare_save_layout_requested"] = True

                st.button("Save layout", key="runs_explorer_compare_save_layout", on_click=_request_save_layout)

                if st.button("Reset layout", key="runs_explorer_compare_reset_layout"):
                    try:
                        with open_db_connection() as _prefs_conn:
                            set_user_setting(_prefs_conn, user_id, "ui.runs_explorer_compare.visible_cols", None)
                            set_user_setting(_prefs_conn, user_id, "ui.runs_explorer_compare.columns_state", None)
                    except Exception:
                        pass
                    for k in (
                        "runs_explorer_compare_visible_cols",
                        "runs_explorer_compare_visible_cols_last_persisted",
                        "runs_explorer_compare_columns_state",
                        "runs_explorer_compare_columns_state_last_persisted",
                        "runs_explorer_compare_prefs_loaded",
                    ):
                        st.session_state.pop(k, None)
                    st.rerun()

            # Persist column layout only on explicit save (prevents resize loops and matches Results).
            if st.session_state.get("runs_explorer_compare_save_layout_requested"):
                try:
                    new_state = _extract_aggrid_columns_state(grid)
                    if isinstance(new_state, list) and new_state:
                        sanitized_state: list[dict] = []
                        for item in new_state:
                            if not isinstance(item, dict):
                                continue
                            d = dict(item)
                            d.pop("hide", None)
                            col_id = d.get("colId") or d.get("col_id") or d.get("field")
                            if not col_id:
                                continue
                            if "width" not in d:
                                try:
                                    d["width"] = int(d.get("actualWidth") or 0) or d.get("width")
                                except Exception:
                                    pass
                            sanitized_state.append(d)
                        with open_db_connection() as _prefs_conn:
                            set_user_setting(
                                _prefs_conn,
                                user_id,
                                "ui.runs_explorer_compare.columns_state",
                                sanitized_state,
                            )
                        st.session_state["runs_explorer_compare_columns_state"] = sanitized_state
                        st.session_state["runs_explorer_compare_columns_state_last_persisted"] = sanitized_state
                        st.success("Layout saved.")
                except Exception as exc:
                    st.error(f"Failed to save layout: {exc}")
                finally:
                    st.session_state.pop("runs_explorer_compare_save_layout_requested", None)

            # Persist updated preferences (best-effort; only when values change).
            def _persist_pref(pref_key: str, value: Any, last_state_key: str) -> None:
                try:
                    if st.session_state.get(last_state_key) == value:
                        return
                    with open_db_connection() as _prefs_conn:
                        set_user_setting(_prefs_conn, user_id, pref_key, value)
                    st.session_state[last_state_key] = value
                except Exception:
                    return

            try:
                picked_strategies = st.session_state.get("runs_explorer:compare__strategy")
                if isinstance(picked_strategies, str):
                    picked_strategies = [picked_strategies]
                if not isinstance(picked_strategies, (list, tuple)):
                    picked_strategies = []
                picked_strategies = [str(s).strip() for s in picked_strategies if str(s).strip()]
            except Exception:
                picked_strategies = []

            try:
                picked_symbols = st.session_state.get("runs_explorer:compare__symbol")
                if isinstance(picked_symbols, str):
                    picked_symbols = [picked_symbols]
                if not isinstance(picked_symbols, (list, tuple)):
                    picked_symbols = []
                picked_symbols = [str(s).strip() for s in picked_symbols if str(s).strip()]
            except Exception:
                picked_symbols = []

            try:
                picked_sweeps = st.session_state.get("runs_explorer:compare__sweep")
                if isinstance(picked_sweeps, str):
                    picked_sweeps = [picked_sweeps]
                if not isinstance(picked_sweeps, (list, tuple)):
                    picked_sweeps = []
                picked_sweeps = [str(s).strip() for s in picked_sweeps if str(s).strip()]
            except Exception:
                picked_sweeps = []

            _persist_pref("ui.runs_explorer_compare.strategy", picked_strategies, "_rx_last_strategy")
            _persist_pref("ui.runs_explorer_compare.show_only_diff", bool(show_only_diff_cols), "_rx_last_show_only")
            _persist_pref("ui.runs_explorer_compare.symbols", picked_symbols, "_rx_last_symbols")
            _persist_pref(
                "ui.runs_explorer_compare.sweeps",
                picked_sweeps,
                "_rx_last_sweeps",
            )
            _persist_pref(
                "ui.runs_explorer_compare.metric_cols",
                [str(k) for k in (selected_metric_keys or []) if str(k).strip()],
                "_rx_last_metric_cols",
            )
            _persist_pref(
                "ui.runs_explorer_compare.config_cols",
                [str(k) for k in (selected_cfg_keys or []) if str(k).strip()],
                "_rx_last_cfg_cols",
            )
            return

        # Backtest page (UI body)
        st.subheader("Backtesting")

        def _render_backtest_job_status(job_id: Optional[int], label: str) -> None:
            if not job_id:
                return
            try:
                with open_db_connection(read_only=True) as _job_conn:
                    job_row = get_job(_job_conn, int(job_id))
            except Exception:
                job_row = None
            if not isinstance(job_row, dict):
                st.warning(f"{label}: job #{job_id} not found")
                return
            status = str(job_row.get("status") or "").strip() or "unknown"
            message = str(job_row.get("message") or "").strip()
            error_text = str(job_row.get("error_text") or "").strip()
            progress = job_row.get("progress")
            msg = f"{label}: #{job_id} • {status}"
            if message:
                msg = f"{msg} • {message}"
            if status.lower() == "failed":
                st.error(msg)
            else:
                st.caption(msg)
            try:
                if progress is not None:
                    st.progress(float(progress), text=None)
            except Exception:
                pass
            if error_text:
                with st.expander(f"{label} error details", expanded=False):
                    st.code(error_text)

        _render_backtest_job_status(
            st.session_state.get("last_backtest_job_id"),
            "Last backtest job",
        )
        _render_backtest_job_status(
            st.session_state.get("last_sweep_parent_job_id"),
            "Last sweep planner job",
        )

        # Stored runs/sweeps are immutable: no deep-link "re-run" actions.

        if st.button("Reset Backtest defaults", key="bt_reset_defaults"):
            _reset_backtest_ui_state()
            init_state_defaults(settings)
            st.rerun()

        # Two-column layout.
        run_feedback = None
        left, right = st.columns([2, 1], gap="large")

        # --- One-time migration: legacy allow_long/allow_short -> Trade direction ---
        # We keep bt_allow_long/bt_allow_short for payload compatibility.
        if not bool(st.session_state.get("bt_trade_direction_migrated_v1", False)):
            allow_l = bool(st.session_state.get("bt_allow_long", True))
            allow_s = bool(st.session_state.get("bt_allow_short", False))
            if allow_l and not allow_s:
                st.session_state["bt_trade_direction"] = "Long only"
            elif allow_s and not allow_l:
                st.session_state["bt_trade_direction"] = "Short only"
            else:
                # If both/neither were set historically, default to Long only.
                st.session_state["bt_trade_direction"] = "Long only"
            st.session_state["bt_trade_direction_migrated_v1"] = True

        def _sync_trade_direction_to_allow_flags() -> None:
            # Persist the radio choice and keep legacy flags synced.
            choice = str(st.session_state.get("bt_trade_direction") or "Long only")
            if choice == "Short only":
                st.session_state["bt_allow_long"] = False
                st.session_state["bt_allow_short"] = True
            else:
                st.session_state["bt_allow_long"] = True
                st.session_state["bt_allow_short"] = False

        # Collect per-field sweep specs (keyed by SWEEPABLE_FIELDS keys)
        sweep_specs: Dict[str, List[Any]] = {}
        sweep_param_modes: Dict[str, str] = {}
        sweep_mode_active = False
        swept_missing: List[str] = []

        def _track_sweep(base_key: str, sweep_field_key: str, sweep_values: Optional[List[Any]]) -> None:
            nonlocal sweep_mode_active
            if str(st.session_state.get(f"{base_key}_mode") or "fixed").strip().lower() == "sweep":
                sweep_mode_active = True
                try:
                    kind_key = f"{base_key}_sweep_kind"
                    if kind_key in st.session_state:
                        kind_val = str(st.session_state.get(kind_key) or "range").strip().lower()
                        sweep_param_modes[sweep_field_key] = "list" if kind_val == "list" else "range"
                    else:
                        # Select-type sweeps are list-based.
                        sweep_param_modes[sweep_field_key] = "list"
                except Exception:
                    sweep_param_modes[sweep_field_key] = "range"
                if sweep_values:
                    sweep_specs[sweep_field_key] = list(sweep_values)

        with left:
            with st.container(border=True):
                st.markdown("### Core / Position")

                row_size = st.columns(2)
                with row_size[0]:
                    init_entry_mode = _segmented_or_radio(
                        "Initial entry sizing",
                        options=[InitialEntrySizingMode.PCT_BALANCE.value, InitialEntrySizingMode.FIXED_USD.value],
                        index=0
                        if str(st.session_state.get("bt_initial_entry_sizing_mode") or InitialEntrySizingMode.PCT_BALANCE.value)
                        == InitialEntrySizingMode.PCT_BALANCE.value
                        else 1,
                        key="bt_initial_entry_sizing_mode",
                        format_func=lambda v: "% of portfolio" if v == InitialEntrySizingMode.PCT_BALANCE.value else "Fixed $ amount",
                        help="Choose whether the initial entry is sized as a % of balance or as a fixed $ notional amount.",
                    )
                    # Ensure the hidden field doesn't accidentally remain in sweep mode.
                    if str(init_entry_mode) == InitialEntrySizingMode.FIXED_USD.value:
                        st.session_state["bt_initial_entry_balance_pct_mode"] = "fixed"
                    else:
                        st.session_state["bt_initial_entry_fixed_usd_mode"] = "fixed"
                with row_size[1]:
                    if str(st.session_state.get("bt_initial_entry_sizing_mode") or InitialEntrySizingMode.PCT_BALANCE.value) == InitialEntrySizingMode.FIXED_USD.value:
                        init_entry_usd, init_entry_usd_sweep = render_sweepable_number(
                            "bt_initial_entry_fixed_usd",
                            "Initial entry ($ notional)",
                            default_value=float(st.session_state.get("bt_initial_entry_fixed_usd", 100.0)),
                            sweep_field_key="general.initial_entry_fixed_usd",
                            help="Size of the first entry as a fixed $ notional amount (USD/USDT).",
                            min_value=0.0,
                            step=10.0,
                        )
                        init_entry_pct = float(st.session_state.get("bt_initial_entry_balance_pct", 10.0))
                        _track_sweep("bt_initial_entry_fixed_usd", "general.initial_entry_fixed_usd", init_entry_usd_sweep)
                    else:
                        init_entry_pct, init_entry_pct_sweep = render_sweepable_number(
                            "bt_initial_entry_balance_pct",
                            "Initial entry (% of balance)",
                            default_value=float(st.session_state.get("bt_initial_entry_balance_pct", 10.0)),
                            sweep_field_key="general.initial_entry_balance_pct",
                            help="Size of the first entry as % of balance/equity (backtest uses initial balance; live uses DRAGON_LIVE_BALANCE or _risk.balance_override).",
                            min_value=0.0,
                            step=1.0,
                        )
                        init_entry_usd = float(st.session_state.get("bt_initial_entry_fixed_usd", 100.0))
                        _track_sweep("bt_initial_entry_balance_pct", "general.initial_entry_balance_pct", init_entry_pct_sweep)

                row2 = st.columns(2)
                with row2[0]:
                    trade_direction = _segmented_or_radio(
                        "Trade direction",
                        options=["Long only", "Short only"],
                        index=["Long only", "Short only"].index(
                            str(st.session_state.get("bt_trade_direction") or "Long only")
                            if str(st.session_state.get("bt_trade_direction") or "Long only") in {"Long only", "Short only"}
                            else "Long only"
                        ),
                        key="bt_trade_direction",
                        on_change=_sync_trade_direction_to_allow_flags,
                        help="Restrict the strategy to a single direction for this run.",
                    )
                # Ensure allow flags are always consistent even if something else mutated them.
                if str(trade_direction) == "Short only":
                    allow_long, allow_short = False, True
                    st.session_state["bt_allow_long"] = False
                    st.session_state["bt_allow_short"] = True
                else:
                    allow_long, allow_short = True, False
                    st.session_state["bt_allow_long"] = True
                    st.session_state["bt_allow_short"] = False

                row3 = st.columns(2)
                lock_atr_on_entry = row3[0].checkbox(
                    "Lock ATR on entry (DCA)",
                    value=bool(st.session_state.get("bt_lock_atr_on_entry", False)),
                    key=_wkey("bt_lock_atr_on_entry"),
                    on_change=_sync_model_from_widget,
                    args=("bt_lock_atr_on_entry",),
                    help="When enabled, DCA distances are based on ATR captured at initial entry (more stable in fast volatility).",
                )
                use_avg_entry = row3[1].checkbox(
                    "Use avg entry for DCA base",
                    value=bool(st.session_state.get("bt_use_avg_entry_for_dca_base", True)),
                    key=_wkey("bt_use_avg_entry_for_dca_base"),
                    on_change=_sync_model_from_widget,
                    args=("bt_use_avg_entry_for_dca_base",),
                    help="Use the current average entry price (after DCAs) as the reference for next DCA levels.",
                )

            with st.container(border=True):
                st.markdown("### Orders")

                row = st.columns(3)
                with row[0]:
                    max_entries = st.number_input(
                        "Max entries",
                        value=int(st.session_state.get("bt_max_entries", 5)),
                        min_value=1,
                        step=1,
                        key=_wkey("bt_max_entries"),
                        on_change=_sync_model_from_widget,
                        args=("bt_max_entries",),
                        help="Maximum number of entries (initial + DCA) allowed per position.",
                    )
                with row[1]:
                    entry_cooldown = st.number_input(
                        "Entry cooldown (min)",
                        value=int(st.session_state.get("bt_entry_cooldown_min", 10)),
                        min_value=0,
                        step=1,
                        key=_wkey("bt_entry_cooldown_min"),
                        on_change=_sync_model_from_widget,
                        args=("bt_entry_cooldown_min",),
                        help="Minimum minutes to wait before placing another entry/DCA order.",
                    )
                with row[2]:
                    entry_timeout = st.number_input(
                        "Entry timeout (min)",
                        value=int(st.session_state.get("bt_entry_timeout_min", 10)),
                        min_value=0,
                        step=1,
                        key=_wkey("bt_entry_timeout_min"),
                        on_change=_sync_model_from_widget,
                        args=("bt_entry_timeout_min",),
                        help="Cancel/abandon an unfilled entry after this many minutes.",
                    )

                global_dyn_pct = st.number_input(
                    "Global dynamic activation band (%)",
                    min_value=0.0,
                    max_value=5.0,
                    step=0.05,
                    value=float(st.session_state.get("bt_global_dyn_pct", 0.0)),
                    key=_wkey("bt_global_dyn_pct"),
                    on_change=_sync_model_from_widget,
                    args=("bt_global_dyn_pct",),
                    help="For dynamic limit orders: only activate/submit a limit order when price is within this % band of the target.",
                )

                with st.expander("Dynamic activation (advanced)", expanded=False):
                    dyn_cols = st.columns(3)
                    dyn_entry = dyn_cols[0].number_input(
                        "Entry band (%)",
                        min_value=0.0,
                        max_value=5.0,
                        step=0.05,
                        value=float(st.session_state.get("bt_dyn_entry_pct", float(global_dyn_pct))),
                        key=_wkey("bt_dyn_entry_pct"),
                        on_change=_sync_model_from_widget,
                        args=("bt_dyn_entry_pct",),
                    )
                    dyn_dca = dyn_cols[1].number_input(
                        "DCA band (%)",
                        min_value=0.0,
                        max_value=5.0,
                        step=0.05,
                        value=float(st.session_state.get("bt_dyn_dca_pct", float(global_dyn_pct))),
                        key=_wkey("bt_dyn_dca_pct"),
                        on_change=_sync_model_from_widget,
                        args=("bt_dyn_dca_pct",),
                    )
                    dyn_tp = dyn_cols[2].number_input(
                        "TP band (%)",
                        min_value=0.0,
                        max_value=5.0,
                        step=0.05,
                        value=float(st.session_state.get("bt_dyn_tp_pct", float(global_dyn_pct))),
                        key=_wkey("bt_dyn_tp_pct"),
                        on_change=_sync_model_from_widget,
                        args=("bt_dyn_tp_pct",),
                    )

                row4 = st.columns(2)
                entry_order_style_label = row4[0].selectbox(
                    "Entry order style",
                    list(ORDER_STYLE_OPTIONS.keys()),
                    index=list(ORDER_STYLE_OPTIONS.keys()).index(
                        st.session_state.get("bt_entry_order_style_label", "Maker or Cancel")
                    ),
                    key=_wkey("bt_entry_order_style_label"),
                    on_change=_sync_model_from_widget,
                    args=("bt_entry_order_style_label",),
                    help="How entry orders are placed/fill (market vs limit vs maker-style).",
                )
                exit_order_style_label = row4[1].selectbox(
                    "Exit order style",
                    list(ORDER_STYLE_OPTIONS.keys()),
                    index=list(ORDER_STYLE_OPTIONS.keys()).index(
                        st.session_state.get("bt_exit_order_style_label", "Maker or Cancel")
                    ),
                    key=_wkey("bt_exit_order_style_label"),
                    on_change=_sync_model_from_widget,
                    args=("bt_exit_order_style_label",),
                    help="How take-profit / stop orders are placed/fill.",
                )
                entry_order_style = ORDER_STYLE_OPTIONS[entry_order_style_label]
                exit_order_style = ORDER_STYLE_OPTIONS[exit_order_style_label]
            with st.container(border=True):
                st.markdown("### Entry & Indicators")

                use_indicator_consensus = st.checkbox(
                    "Use indicator consensus",
                    value=bool(st.session_state.get("bt_use_indicator_consensus", True)),
                    key=_wkey("bt_use_indicator_consensus"),
                    on_change=_sync_model_from_widget,
                    args=("bt_use_indicator_consensus",),
                    help="Require multiple indicators to agree before entering (reduces noise but trades less).",
                )

                st.checkbox(
                    "Require MA direction for entries",
                    value=bool(st.session_state.get("bt_use_ma_direction", False)),
                    key=_wkey("bt_use_ma_direction"),
                    on_change=_sync_model_from_widget,
                    args=("bt_use_ma_direction",),
                    help="When enabled: LONG entries require close > MA; SHORT entries require close < MA. Uses the MA settings below.",
                )

                with st.container(border=True):
                    st.markdown("#### MA")

                    ma_cols = st.columns(2)
                    with ma_cols[0]:
                        ma_interval, ma_interval_sweep = render_sweepable_number(
                            "bt_ma_interval_min",
                            "MA interval (min)",
                            default_value=int(st.session_state.get("bt_ma_interval_min", 120)),
                            sweep_field_key="trend.ma_interval_min",
                            help="How often MA updates (in minutes).",
                            min_value=1,
                            step=1,
                        )
                    with ma_cols[1]:
                        ma_length, ma_length_sweep = render_sweepable_number(
                            "bt_ma_length",
                            "MA length",
                            default_value=int(st.session_state.get("bt_ma_length", 200)),
                            sweep_field_key="trend.ma_len",
                            help="Number of MA periods.",
                            min_value=1,
                            step=1,
                        )
                    _track_sweep("bt_ma_interval_min", "trend.ma_interval_min", ma_interval_sweep)
                    _track_sweep("bt_ma_length", "trend.ma_len", ma_length_sweep)

                    st.selectbox(
                        "MA type",
                        options=["Sma", "Ema"],
                        index=["Sma", "Ema"].index(
                            str(st.session_state.get("bt_ma_type", "Sma") or "Sma")
                            if str(st.session_state.get("bt_ma_type", "Sma") or "Sma") in {"Sma", "Ema"}
                            else "Sma"
                        ),
                        key=_wkey("bt_ma_type"),
                        on_change=_sync_model_from_widget,
                        args=("bt_ma_type",),
                        help="Moving average type used by the trend filter.",
                    )

                with st.container(border=True):
                    st.markdown("#### BBands")

                    bb_cols = st.columns(2)
                    with bb_cols[0]:
                        bb_interval, bb_interval_sweep = render_sweepable_number(
                            "bt_bb_interval_min",
                            "BB interval (min)",
                            default_value=int(st.session_state.get("bt_bb_interval_min", 60)),
                            sweep_field_key="bbands.interval_min",
                            help="How often Bollinger Bands update (in minutes).",
                            min_value=1,
                            step=1,
                        )
                    with bb_cols[1]:
                        bb_length, bb_length_sweep = render_sweepable_number(
                            "bt_bb_length",
                            "BB length",
                            default_value=int(st.session_state.get("bt_bb_length", 20)),
                            sweep_field_key="bbands.length",
                            help="Number of BB periods.",
                            min_value=1,
                            step=1,
                        )
                    _track_sweep("bt_bb_interval_min", "bbands.interval_min", bb_interval_sweep)
                    _track_sweep("bt_bb_length", "bbands.length", bb_length_sweep)

                    with st.expander("BBands (advanced)", expanded=False):
                        bb_adv1 = st.columns(2)
                        bb_dev_up = bb_adv1[0].number_input(
                            "BB dev up",
                            min_value=0.1,
                            step=0.1,
                            value=float(st.session_state.get("bt_bb_dev_up", 2.0)),
                            key=_wkey("bt_bb_dev_up"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_dev_up",),
                        )
                        bb_dev_down = bb_adv1[1].number_input(
                            "BB dev down",
                            min_value=0.1,
                            step=0.1,
                            value=float(st.session_state.get("bt_bb_dev_down", 2.0)),
                            key=_wkey("bt_bb_dev_down"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_dev_down",),
                        )

                        bb_adv2 = st.columns(2)
                        bb_ma_type = bb_adv2[0].selectbox(
                            "BB MA type",
                            options=["Sma", "Ema"],
                            index=["Sma", "Ema"].index(
                                str(st.session_state.get("bt_bb_ma_type", "Sma") or "Sma")
                                if str(st.session_state.get("bt_bb_ma_type", "Sma") or "Sma") in {"Sma", "Ema"}
                                else "Sma"
                            ),
                            key=_wkey("bt_bb_ma_type"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_ma_type",),
                        )
                        bb_deviation = bb_adv2[1].number_input(
                            "BB deviation",
                            min_value=0.0,
                            step=0.05,
                            value=float(st.session_state.get("bt_bb_deviation", 0.2)),
                            key=_wkey("bt_bb_deviation"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_deviation",),
                        )

                        bb_adv3 = st.columns(3)
                        bb_req_fcc = bb_adv3[0].checkbox(
                            "Require FCC",
                            value=bool(st.session_state.get("bt_bb_require_fcc", False)),
                            key=_wkey("bt_bb_require_fcc"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_require_fcc",),
                        )
                        bb_reset_mid = bb_adv3[1].checkbox(
                            "Reset middle",
                            value=bool(st.session_state.get("bt_bb_reset_middle", False)),
                            key=_wkey("bt_bb_reset_middle"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_reset_middle",),
                        )
                        bb_allow_mid_sells = bb_adv3[2].checkbox(
                            "Allow mid sells",
                            value=bool(st.session_state.get("bt_bb_allow_mid_sells", False)),
                            key=_wkey("bt_bb_allow_mid_sells"),
                            on_change=_sync_model_from_widget,
                            args=("bt_bb_allow_mid_sells",),
                        )

                with st.container(border=True):
                    st.markdown("#### MACD")
                    macd_interval, macd_interval_sweep = render_sweepable_number(
                        "bt_macd_interval_min",
                        "MACD interval (min)",
                        default_value=int(st.session_state.get("bt_macd_interval_min", 360)),
                        sweep_field_key="macd.interval_min",
                        help="How often MACD updates (in minutes).",
                        min_value=1,
                        step=1,
                    )
                    _track_sweep("bt_macd_interval_min", "macd.interval_min", macd_interval_sweep)

                    with st.expander("MACD (advanced)", expanded=False):
                        macd_adv = st.columns(3)
                        macd_fast = macd_adv[0].number_input(
                            "MACD fast",
                            min_value=1,
                            step=1,
                            value=int(st.session_state.get("bt_macd_fast", 12)),
                            key=_wkey("bt_macd_fast"),
                            on_change=_sync_model_from_widget,
                            args=("bt_macd_fast",),
                        )
                        macd_slow = macd_adv[1].number_input(
                            "MACD slow",
                            min_value=1,
                            step=1,
                            value=int(st.session_state.get("bt_macd_slow", 26)),
                            key=_wkey("bt_macd_slow"),
                            on_change=_sync_model_from_widget,
                            args=("bt_macd_slow",),
                        )
                        macd_signal = macd_adv[2].number_input(
                            "MACD signal",
                            min_value=1,
                            step=1,
                            value=int(st.session_state.get("bt_macd_signal", 7)),
                            key=_wkey("bt_macd_signal"),
                            on_change=_sync_model_from_widget,
                            args=("bt_macd_signal",),
                        )

                with st.container(border=True):
                    st.markdown("#### RSI")

                    rsi_cols = st.columns(2)
                    with rsi_cols[0]:
                        rsi_interval, rsi_interval_sweep = render_sweepable_number(
                            "bt_rsi_interval_min",
                            "RSI interval (min)",
                            default_value=int(st.session_state.get("bt_rsi_interval_min", 1440)),
                            sweep_field_key="rsi.interval_min",
                            help="Higher interval = slower signal.",
                            min_value=1,
                            step=1,
                        )
                    with rsi_cols[1]:
                        rsi_length, rsi_length_sweep = render_sweepable_number(
                            "bt_rsi_length",
                            "RSI length",
                            default_value=int(st.session_state.get("bt_rsi_length", 14)),
                            sweep_field_key="rsi.length",
                            help="Number of RSI periods.",
                            min_value=1,
                            step=1,
                        )
                    _track_sweep("bt_rsi_interval_min", "rsi.interval_min", rsi_interval_sweep)
                    _track_sweep("bt_rsi_length", "rsi.length", rsi_length_sweep)

                    with st.expander("RSI (advanced)", expanded=False):
                        rsi_levels = st.columns(2)
                        rsi_buy_level = rsi_levels[0].number_input(
                            "RSI buy level",
                            min_value=0.0,
                            max_value=100.0,
                            step=1.0,
                            value=float(st.session_state.get("bt_rsi_buy_level", 30.0)),
                            key=_wkey("bt_rsi_buy_level"),
                            on_change=_sync_model_from_widget,
                            args=("bt_rsi_buy_level",),
                        )
                        rsi_sell_level = rsi_levels[1].number_input(
                            "RSI sell level",
                            min_value=0.0,
                            max_value=100.0,
                            step=1.0,
                            value=float(st.session_state.get("bt_rsi_sell_level", 70.0)),
                            key=_wkey("bt_rsi_sell_level"),
                            on_change=_sync_model_from_widget,
                            args=("bt_rsi_sell_level",),
                        )

            with st.container(border=True):
                st.markdown("### DCA")

                row = st.columns(3)
                with row[0]:
                    base_dev, base_dev_sweep = render_sweepable_number(
                        "bt_base_deviation_pct",
                        "Base deviation (%)",
                        default_value=float(st.session_state.get("bt_base_deviation_pct", 1.0)),
                        sweep_field_key="dca.base_deviation_pct",
                        min_value=0.1,
                        step=0.1,
                    )
                with row[1]:
                    dev_multiplier, dev_multiplier_sweep = render_sweepable_number(
                        "bt_deviation_multiplier",
                        "Deviation multiplier",
                        default_value=float(st.session_state.get("bt_deviation_multiplier", 2.0)),
                        sweep_field_key="dca.deviation_multiplier",
                        min_value=1.0,
                        step=0.1,
                    )
                with row[2]:
                    vol_multiplier, vol_multiplier_sweep = render_sweepable_number(
                        "bt_volume_multiplier",
                        "Volume multiplier",
                        default_value=float(st.session_state.get("bt_volume_multiplier", 2.0)),
                        sweep_field_key="dca.volume_multiplier",
                        min_value=1.0,
                        step=0.1,
                    )

                _track_sweep("bt_base_deviation_pct", "dca.base_deviation_pct", base_dev_sweep)
                _track_sweep("bt_deviation_multiplier", "dca.deviation_multiplier", dev_multiplier_sweep)
                _track_sweep("bt_volume_multiplier", "dca.volume_multiplier", vol_multiplier_sweep)

            with st.container(border=True):
                st.markdown("### Exits")

                st.markdown("#### Stop Loss + Trailing")
                sl_mode_widget = _segmented_or_radio(
                    "Stop Loss mode",
                    options=[StopLossMode.PCT.value, StopLossMode.ATR.value],
                    index=[StopLossMode.PCT.value, StopLossMode.ATR.value].index(
                        str(st.session_state.get("bt_sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value)
                        if str(st.session_state.get("bt_sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value)
                        in {StopLossMode.PCT.value, StopLossMode.ATR.value}
                        else StopLossMode.PCT.value
                    ),
                    key=_wkey("bt_sl_mode"),
                    on_change=_sync_model_from_widget,
                    args=("bt_sl_mode",),
                    format_func=lambda v: "Percent" if v == StopLossMode.PCT.value else "ATR",
                )
                sl_mode_selected = str(st.session_state.get("bt_sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value).strip().upper()
                show_tp_atr_period_in_sl = sl_mode_selected == StopLossMode.ATR.value

                if sl_mode_selected == StopLossMode.ATR.value:
                    atr_cols = st.columns(2)
                    with atr_cols[0]:
                        tp_atr_period, tp_atr_period_sweep = render_sweepable_number(
                            "bt_tp_atr_period",
                            "ATR period",
                            default_value=int(st.session_state.get("bt_tp_atr_period", 14)),
                            sweep_field_key="exits.tp_atr_period",
                            help="Shared ATR lookback period used for ATR SL/TP.",
                            min_value=1,
                            step=1,
                        )
                    with atr_cols[1]:
                        sl_atr_mult, sl_atr_mult_sweep = render_sweepable_number(
                            "bt_sl_atr_mult",
                            "Initial SL (ATR×)",
                            default_value=float(st.session_state.get("bt_sl_atr_mult", 3.0)),
                            sweep_field_key="exits.sl_atr_mult",
                            help="Initial stop distance in ATR multiples.",
                            min_value=0.0,
                            step=0.25,
                        )
                    _track_sweep("bt_tp_atr_period", "exits.tp_atr_period", tp_atr_period_sweep)
                    _track_sweep("bt_sl_atr_mult", "exits.sl_atr_mult", sl_atr_mult_sweep)

                    trail_cols = st.columns(2)
                    with trail_cols[0]:
                        trail_activation_atr_mult, trail_activation_atr_sweep = render_sweepable_number(
                            "bt_trail_activation_atr_mult",
                            "Activation (ATR×)",
                            default_value=float(st.session_state.get("bt_trail_activation_atr_mult", 1.0)),
                            sweep_field_key="exits.trail_activation_atr_mult",
                            help="Start trailing after price moves in your favor by this ATR multiple.",
                            min_value=0.0,
                            step=0.25,
                        )
                    with trail_cols[1]:
                        trail_distance_atr_mult, trail_distance_atr_sweep = render_sweepable_number(
                            "bt_trail_distance_atr_mult",
                            "Trail distance (ATR×)",
                            default_value=float(st.session_state.get("bt_trail_distance_atr_mult", 2.0)),
                            sweep_field_key="exits.trail_distance_atr_mult",
                            help="Trailing stop distance in ATR multiples.",
                            min_value=0.0,
                            step=0.25,
                        )
                    _track_sweep("bt_trail_activation_atr_mult", "exits.trail_activation_atr_mult", trail_activation_atr_sweep)
                    _track_sweep("bt_trail_distance_atr_mult", "exits.trail_distance_atr_mult", trail_distance_atr_sweep)
                else:
                    fixed_sl_pct, fixed_sl_sweep = render_sweepable_number(
                        "bt_fixed_sl_pct",
                        "Fixed SL (%)",
                        default_value=float(st.session_state.get("bt_fixed_sl_pct", 5.0)),
                        sweep_field_key="exits.fixed_sl_pct",
                        help="Exit the position if price moves against you by this percent.",
                        min_value=0.0,
                        step=0.5,
                    )
                    _track_sweep("bt_fixed_sl_pct", "exits.fixed_sl_pct", fixed_sl_sweep)

                    trail_cols = st.columns(2)
                    with trail_cols[0]:
                        trail_activation_pct, trail_activation_sweep = render_sweepable_number(
                            "bt_trail_activation_pct",
                            "Trailing activation (%)",
                            default_value=float(st.session_state.get("bt_trail_activation_pct", 5.0)),
                            sweep_field_key="exits.trail_activation_pct",
                            help="Start trailing only after price has moved in your favor by this percent.",
                            min_value=0.0,
                            step=0.5,
                        )
                    with trail_cols[1]:
                        trail_stop_pct, trail_stop_sweep = render_sweepable_number(
                            "bt_trail_stop_pct",
                            "Trailing stop (%)",
                            default_value=float(st.session_state.get("bt_trail_stop_pct", 7.0)),
                            sweep_field_key="exits.trail_stop_pct",
                            help="Trail distance (percent). When hit, exit the position.",
                            min_value=0.0,
                            step=0.5,
                        )
                    _track_sweep("bt_trail_activation_pct", "exits.trail_activation_pct", trail_activation_sweep)
                    _track_sweep("bt_trail_stop_pct", "exits.trail_stop_pct", trail_stop_sweep)

                st.divider()

                st.markdown("#### Take Profit (TP)")
                tp_mode_widget = _segmented_or_radio(
                    "TP mode",
                    options=[TakeProfitMode.PCT.value, TakeProfitMode.ATR.value],
                    index=[TakeProfitMode.PCT.value, TakeProfitMode.ATR.value].index(
                        str(st.session_state.get("bt_tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value)
                        if str(st.session_state.get("bt_tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value)
                        in {TakeProfitMode.PCT.value, TakeProfitMode.ATR.value}
                        else TakeProfitMode.ATR.value
                    ),
                    key=_wkey("bt_tp_mode"),
                    on_change=_sync_model_from_widget,
                    args=("bt_tp_mode",),
                    format_func=lambda v: "Percent" if v == TakeProfitMode.PCT.value else "ATR",
                )
                tp_mode_selected = str(st.session_state.get("bt_tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value).strip().upper()
                # Keep legacy toggle in sync (used by older snapshots/sweep definitions)
                st.session_state["bt_use_atr_tp"] = tp_mode_selected == TakeProfitMode.ATR.value

                if tp_mode_selected == TakeProfitMode.ATR.value:
                    if show_tp_atr_period_in_sl:
                        tp_atr_multiple, tp_atr_multiple_sweep = render_sweepable_number(
                            "bt_tp_atr_multiple",
                            "TP (ATR×)",
                            default_value=float(st.session_state.get("bt_tp_atr_multiple", 10.0)),
                            sweep_field_key="exits.tp_atr_multiple",
                            help="Take profit distance in ATR multiples.",
                            min_value=0.0,
                            step=0.5,
                        )
                        st.caption("ATR period is shared with Stop Loss settings above.")
                    else:
                        atr_cols = st.columns(2)
                        with atr_cols[0]:
                            tp_atr_multiple, tp_atr_multiple_sweep = render_sweepable_number(
                                "bt_tp_atr_multiple",
                                "TP (ATR×)",
                                default_value=float(st.session_state.get("bt_tp_atr_multiple", 10.0)),
                                sweep_field_key="exits.tp_atr_multiple",
                                help="Take profit distance in ATR multiples.",
                                min_value=0.0,
                                step=0.5,
                            )
                        with atr_cols[1]:
                            tp_atr_period, tp_atr_period_sweep = render_sweepable_number(
                                "bt_tp_atr_period",
                                "ATR period",
                                default_value=int(st.session_state.get("bt_tp_atr_period", 14)),
                                sweep_field_key="exits.tp_atr_period",
                                help="Shared ATR lookback period used for ATR SL/TP.",
                                min_value=1,
                                step=1,
                            )
                        _track_sweep("bt_tp_atr_period", "exits.tp_atr_period", tp_atr_period_sweep)
                    _track_sweep("bt_tp_atr_multiple", "exits.tp_atr_multiple", tp_atr_multiple_sweep)
                else:
                    fixed_tp_pct, fixed_tp_sweep = render_sweepable_number(
                        "bt_fixed_tp_pct",
                        "TP (%)",
                        default_value=float(st.session_state.get("bt_fixed_tp_pct", 1.0)),
                        sweep_field_key="exits.fixed_tp_pct",
                        help="Percent-based take profit target.",
                        min_value=0.0,
                        step=0.5,
                    )
                    _track_sweep("bt_fixed_tp_pct", "exits.fixed_tp_pct", fixed_tp_sweep)

                tp_replace_threshold_pct = st.number_input(
                    "TP replace threshold (%)",
                    min_value=0.0,
                    max_value=5.0,
                    step=0.01,
                    value=float(st.session_state.get("bt_tp_replace_threshold_pct", 0.05)),
                    key=_wkey("bt_tp_replace_threshold_pct"),
                    on_change=_sync_model_from_widget,
                    args=("bt_tp_replace_threshold_pct",),
                    help="When using dynamic TP logic, only replace an existing TP if the new target differs by at least this percent.",
                )

        with right:
            with st.container(border=True):
                st.markdown("### Data source")
                data_source_options = ("Synthetic", "Crypto (CCXT)")
                data_source_current = str(st.session_state.get("data_source_mode", "Crypto (CCXT)") or "Crypto (CCXT)")
                if data_source_current not in set(data_source_options):
                    data_source_current = "Crypto (CCXT)"
                data_source = _segmented_or_radio(
                    "Data source",
                    data_source_options,
                    index=data_source_options.index(data_source_current),
                    key=_wkey("data_source_mode"),
                    on_change=_sync_model_from_widget,
                    args=("data_source_mode",),
                )

                # Defaults / suggestions (shown; fee_rate is user-configurable elsewhere)
                fee_settings_spot = fee_fraction_from_pct(settings.get("fee_spot_pct", 0.10))
                fee_settings_perps = fee_fraction_from_pct(settings.get("fee_perps_pct", 0.06))
                fee_settings_stocks = fee_fraction_from_pct(settings.get("fee_stocks_pct", 0.10))

                # Ensure downstream variables always exist.
                ccxt_exchange_id = ccxt_symbol = ccxt_timeframe = ""
                exchange_id = "woox"
                symbol = "ETH/USDT:USDT"
                timeframe = "1h"
                market_type = "Perps"
                ccxt_range_mode = "Duration"
                ccxt_limit_input = 500
                duration_label = "1 month"
                start_dt = None
                end_dt = None
                candle_count = int(st.session_state.get("synthetic_candle_count", 300))

                if data_source == "Crypto (CCXT)":
                    exchange_options = ["WooX", "Binance", "Bybit", "OKX", "Kraken", "Custom…"]
                    exchange_label_map = {
                        "woox": "WooX",
                        "woo": "WooX",
                        "binance": "Binance",
                        "bybit": "Bybit",
                        "okx": "OKX",
                        "kraken": "Kraken",
                    }
                    current_choice_raw = str(st.session_state.get("ccxt_exchange_choice") or "").strip()
                    if current_choice_raw in exchange_options:
                        current_choice = current_choice_raw
                    else:
                        current_norm = normalize_exchange_id(current_choice_raw) or current_choice_raw.lower()
                        current_choice = exchange_label_map.get(current_norm, "WooX")
                        if current_choice:
                            st.session_state["ccxt_exchange_choice"] = current_choice

                    exchange_choice = st.selectbox(
                        "Exchange",
                        exchange_options,
                        index=exchange_options.index(current_choice) if current_choice in exchange_options else 0,
                        key="ccxt_exchange_choice",
                    )
                    if exchange_choice == "Custom…":
                        exchange_id = st.text_input(
                            "Custom exchange id",
                            value=str(st.session_state.get("ccxt_exchange_custom") or "woox"),
                            key="ccxt_exchange_custom",
                        )
                    else:
                        exchange_id = exchange_choice
                    exchange_id = normalize_exchange_id(exchange_id) or (str(exchange_id or "").strip().lower())

                    market_type = st.selectbox(
                        "Market type",
                        options=["Perps", "Spot"],
                        index=0,
                        key="ccxt_market_type",
                    )

                    markets: dict = {}
                    exchange_timeframes: list[str] = []
                    try:
                        with _perf("backtest.metadata"):
                            markets, exchange_timeframes = fetch_ccxt_metadata(exchange_id)
                    except Exception as e:
                        st.warning(f"Could not load CCXT metadata for '{exchange_id}': {e}")

                    ex_norm = normalize_exchange_id(exchange_id) or (str(exchange_id or "").strip().lower())
                    raw_symbol_candidates = sorted(list(markets.keys())) if markets else ["ETH/USDT:USDT"]

                    # WooX: show canonical PERP_/SPOT_ symbols (no ccxt-format duplicates).
                    if ex_norm == "woox":
                        from project_dragon.exchange_normalization import canonical_symbol

                        symbol_candidates = []
                        seen: set[str] = set()
                        want_prefix = "PERP_" if str(market_type).strip().lower().startswith("perp") else "SPOT_"
                        for s in raw_symbol_candidates:
                            cs = str(canonical_symbol(ex_norm, s) or "").strip().upper()
                            if not cs or not cs.startswith(want_prefix):
                                continue
                            if cs in seen:
                                continue
                            seen.add(cs)
                            symbol_candidates.append(cs)
                        preferred_symbol = "PERP_ETH_USDT" if want_prefix == "PERP_" else "SPOT_ETH_USDT"
                        symbol_default_raw = st.session_state.get("ccxt_symbol") or preferred_symbol
                        symbol_default = str(canonical_symbol(ex_norm, symbol_default_raw) or preferred_symbol).strip().upper()
                    else:
                        symbol_candidates = _dedupe_symbol_candidates(raw_symbol_candidates, markets)
                        preferred_symbol = "ETH/USDT:USDT"
                        symbol_default = st.session_state.get("ccxt_symbol") or preferred_symbol
                    if symbol_default in symbol_candidates:
                        symbol_index = symbol_candidates.index(symbol_default)
                    elif preferred_symbol in symbol_candidates:
                        symbol_index = symbol_candidates.index(preferred_symbol)
                    else:
                        symbol_index = 0
                    mkt_label = "PERPS" if str(market_type).strip().lower().startswith("perp") else "SPOT"

                    def _format_symbol_option(exchange_symbol: str, ml: str) -> str:
                        s = (exchange_symbol or "").strip()
                        if not s:
                            return ""
                        # CCXT: BTC/USDT or BTC/USDT:USDT
                        if "/" in s:
                            left = s.split(":", 1)[0]
                            base = (left.split("/", 1)[0] or "").strip().upper()
                            quote = (left.split("/", 1)[1] or "").strip().upper() if "/" in left else ""
                        else:
                            up = s.strip().upper()
                            if up.startswith("PERP_") or up.startswith("SPOT_"):
                                parts = [p for p in up.split("_") if p]
                                base = (parts[1] if len(parts) >= 2 else up).strip().upper()
                                quote = (parts[2] if len(parts) >= 3 else "").strip().upper()
                            elif "-" in s:
                                base = (s.split("-", 1)[0] or "").strip().upper()
                                quote = (s.split("-", 1)[1] or "").strip().upper()
                            else:
                                base = up
                                quote = ""
                        if not base:
                            return ""
                        if quote:
                            return f"{base}/{quote} - {ml}"
                        return f"{base} - {ml}"

                    selected_symbol_for_defaults = str(st.session_state.get("ccxt_symbol") or symbol_default or "").strip()

                    # Sweep assets: above Symbol, and hide Symbol when multi-asset.
                    scope = None
                    show_symbol_picker = True
                    if sweep_mode_active and data_source == "Crypto (CCXT)":
                        scope = _segmented_or_radio(
                            "Sweep assets",
                            options=["Single asset", "Manual selection", "Category"],
                            index=["Single asset", "Manual selection", "Category"].index(
                                str(st.session_state.get("bt_sweep_assets_scope") or "Single asset")
                                if str(st.session_state.get("bt_sweep_assets_scope") or "Single asset")
                                in {"Single asset", "Manual selection", "Category"}
                                else "Single asset"
                            ),
                            key="bt_sweep_assets_scope",
                        )
                        show_symbol_picker = str(scope) == "Single asset"

                        # Used only for the "Estimated total runs" hint (avoid extra DB work here).
                        st.session_state["bt_sweep_assets_count_hint"] = 1

                        if scope == "Single asset":
                            st.session_state["bt_sweep_assets_count_hint"] = 1 if selected_symbol_for_defaults else 0

                        elif scope == "Manual selection":
                            options = list(symbol_candidates) if isinstance(symbol_candidates, list) else []
                            default_sel = [selected_symbol_for_defaults] if selected_symbol_for_defaults else []
                            selected = st.multiselect(
                                "Symbols",
                                options=options,
                                default=default_sel,
                                key="bt_sweep_assets_manual",
                            )
                            st.session_state["bt_sweep_assets_count_hint"] = int(len(selected or []))
                            st.caption(f"Selected assets: {int(st.session_state.get('bt_sweep_assets_count_hint') or 0)}")

                        else:
                            cats = []
                            try:
                                cats = list_categories(
                                    str(user_id or "").strip() or "admin@local",
                                    str(exchange_id or "").strip(),
                                )
                            except Exception:
                                cats = []
                            if not cats:
                                st.warning("No categories found for this exchange. Create one in Settings → Assets.")
                                st.session_state["bt_sweep_assets_count_hint"] = 0
                            else:
                                cat_labels = [
                                    str(c.get("name") or "").strip()
                                    for c in cats
                                    if str(c.get("name") or "").strip()
                                ]
                                if not cat_labels:
                                    st.warning("No named categories found for this exchange.")
                                    st.session_state["bt_sweep_assets_count_hint"] = 0
                                else:
                                    default_name = str(st.session_state.get("bt_sweep_assets_category_name") or "").strip()
                                    idx0 = cat_labels.index(default_name) if default_name in cat_labels else 0
                                    picked_name = st.selectbox(
                                        "Category",
                                        options=cat_labels,
                                        index=idx0,
                                        key="bt_sweep_assets_category_name",
                                    )
                                    picked_row = next(
                                        (
                                            c
                                            for c in cats
                                            if str(c.get("name") or "").strip() == str(picked_name or "").strip()
                                        ),
                                        None,
                                    )
                                    try:
                                        st.session_state["bt_sweep_assets_count_hint"] = int(
                                            (picked_row or {}).get("member_count") or 0
                                        )
                                    except Exception:
                                        st.session_state["bt_sweep_assets_count_hint"] = 0
                                    st.caption(
                                        f"{int(st.session_state.get('bt_sweep_assets_count_hint') or 0)} assets will be included"
                                    )

                    # Streamlit's native selectbox can't render per-option icons.
                    # Best-effort: show the selected symbol's icon inline next to the dropdown.
                    if show_symbol_picker:
                        icon_uri_selected = None
                        selected_for_icon = str(st.session_state.get("ccxt_symbol") or symbol_default or "").strip()
                        if selected_for_icon:
                            icon_uri_selected = _lookup_symbol_icon_uri(selected_for_icon, exchange_id=ex_norm) or None

                        placeholder_html = None
                        if not icon_uri_selected:
                            try:
                                placeholder_html = render_tabler_icon(
                                    "coin",
                                    size_px=18,
                                    color="#9aa0a6",
                                    variant="outline",
                                    as_img=True,
                                )
                            except Exception:
                                placeholder_html = None

                        sym_icon_col, sym_select_col = st.columns([1, 11], vertical_alignment="center")
                        with sym_icon_col:
                            if icon_uri_selected:
                                st.markdown(
                                    f'<div style="width:22px;height:22px;display:flex;align-items:center;justify-content:center;">'
                                    f'<img src="{icon_uri_selected}" width="20" height="20" style="display:block;border-radius:4px;" />'
                                    f"</div>",
                                    unsafe_allow_html=True,
                                )
                            elif placeholder_html:
                                st.markdown(
                                    '<div style="width:22px;height:22px;display:flex;align-items:center;justify-content:center;">'
                                    f"{placeholder_html}"
                                    "</div>",
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.markdown(
                                    '<div style="width:22px;height:22px;"></div>',
                                    unsafe_allow_html=True,
                                )
                        with sym_select_col:
                            symbol = st.selectbox(
                                "Symbol",
                                symbol_candidates,
                                index=symbol_index,
                                key="ccxt_symbol",
                                format_func=lambda s, ml=mkt_label: _format_symbol_option(s, ml),
                            )
                    else:
                        # Keep a deterministic base symbol for downstream data settings without showing UI.
                        symbol = selected_symbol_for_defaults or (str(symbol_default or "").strip())
                        if symbol:
                            st.session_state["ccxt_symbol"] = symbol

                    tf_candidates = exchange_timeframes if exchange_timeframes else ["1m", "5m", "15m", "1h", "4h", "1d"]
                    timeframe = st.selectbox(
                        "Timeframe",
                        tf_candidates,
                        index=tf_candidates.index("1h") if "1h" in tf_candidates else 0,
                        key="ccxt_timeframe",
                    )

                    ccxt_exchange_id = exchange_id_for_ccxt(exchange_id)
                    ccxt_symbol = symbol
                    ccxt_timeframe = timeframe

                backtest_leverage = st.slider(
                    "Leverage",
                    min_value=1,
                    max_value=20,
                    value=int(st.session_state.get("bt_backtest_leverage", 1) or 1),
                    step=1,
                    key=_wkey("bt_backtest_leverage"),
                    on_change=_sync_model_from_widget,
                    args=("bt_backtest_leverage",),
                    help="Backtest-only: used for ROI-on-margin and drawdown-on-margin metrics (perps/futures).",
                )

            with st.container(border=True):
                st.markdown("### Backtest Range")
                if data_source == "Crypto (CCXT)":
                    ccxt_range_mode = _segmented_or_radio(
                        "Range",
                        options=["Bars", "Duration", "Start/End", "Periods"],
                        index=["Bars", "Duration", "Start/End", "Periods"].index(
                            st.session_state.get("ccxt_range_mode", "Duration")
                            if st.session_state.get("ccxt_range_mode", "Duration") in {"Bars", "Duration", "Start/End", "Periods"}
                            else "Duration"
                        ),
                        key="ccxt_range_mode",
                    )

                    if ccxt_range_mode == "Bars":
                        ccxt_limit_input = st.number_input(
                            "Number of bars",
                            value=int(st.session_state.get("ccxt_limit", 500)),
                            min_value=10,
                            max_value=5_000,
                            step=10,
                            key="ccxt_limit",
                        )
                    elif ccxt_range_mode == "Duration":
                        duration_options = list(DURATION_CHOICES.keys())
                        desired_duration = str(st.session_state.get("ccxt_duration_label") or "1 month")
                        duration_index = (
                            duration_options.index(desired_duration)
                            if desired_duration in duration_options
                            else (duration_options.index("1 month") if "1 month" in duration_options else 0)
                        )
                        duration_label = st.selectbox(
                            "Duration",
                            duration_options,
                            index=duration_index,
                            key="ccxt_duration_label",
                        )
                    elif ccxt_range_mode == "Periods":
                        periods = []
                        try:
                            with open_db_connection() as _p_conn:
                                periods = list_saved_periods(_p_conn, user_id)
                        except Exception:
                            periods = []
                        period_rows_by_id: Dict[int, dict] = {}
                        period_ids: list[int] = []
                        for p in periods or []:
                            try:
                                pid = int(p.get("id"))
                            except Exception:
                                continue
                            name = str(p.get("name") or "").strip()
                            if not name:
                                continue
                            if pid not in period_rows_by_id:
                                period_rows_by_id[pid] = p
                                period_ids.append(pid)

                        # Best-effort migration from legacy "ccxt_period_label" state.
                        if "ccxt_period_id" not in st.session_state and "ccxt_period_label" in st.session_state:
                            legacy = str(st.session_state.get("ccxt_period_label") or "").strip()
                            migrated = None
                            if legacy.isdigit():
                                migrated = int(legacy)
                            else:
                                m = re.search(r"\(#(\d+)\)", legacy)
                                if m:
                                    try:
                                        migrated = int(m.group(1))
                                    except Exception:
                                        migrated = None
                            if migrated is not None:
                                st.session_state["ccxt_period_id"] = migrated

                        if not period_ids:
                            st.warning("No saved periods found. Create one in Settings → Periods.")
                            start_dt = None
                            end_dt = None
                        else:
                            # Store id as value; render only the saved name.
                            selected_id = st.selectbox(
                                "Period",
                                options=period_ids,
                                index=0,
                                key="ccxt_period_id",
                                format_func=lambda pid: str((period_rows_by_id.get(int(pid)) or {}).get("name") or "").strip(),
                            )
                            selected_row = period_rows_by_id.get(int(selected_id)) if selected_id is not None else None
                            start_dt = _normalize_timestamp((selected_row or {}).get("start_ts"))
                            end_dt = _normalize_timestamp((selected_row or {}).get("end_ts"))
                            if start_dt and end_dt:
                                st.caption(
                                    f"{start_dt.astimezone(timezone.utc).isoformat()} → {end_dt.astimezone(timezone.utc).isoformat()}"
                                )
                    else:
                        default_end = datetime.now(timezone.utc)
                        default_start = default_end - timedelta(days=7)
                        start_dt = st.datetime_input("Start (UTC)", value=default_start, key="ccxt_start_dt")
                        end_dt = st.datetime_input("End (UTC)", value=default_end, key="ccxt_end_dt")
                        if start_dt and end_dt and start_dt >= end_dt:
                            st.warning("End time must be after start time.")
                else:
                    candle_count = st.slider(
                        "Synthetic candle count",
                        min_value=100,
                        max_value=1_000,
                        value=int(st.session_state.get("synthetic_candle_count", 300)),
                        step=50,
                        key="synthetic_candle_count",
                    )

            with st.container(border=True):
                st.markdown("### Capital & Fees")
                initial_balance = st.number_input(
                    "Initial balance",
                    min_value=100.0,
                    step=100.0,
                    value=float(st.session_state.get("bt_initial_balance", 1000.0)),
                    key=_wkey("bt_initial_balance"),
                    on_change=_sync_model_from_widget,
                    args=("bt_initial_balance",),
                )
                fee_rate_input = st.number_input(
                    "Fee rate (fraction)",
                    min_value=0.0,
                    step=0.0001,
                    format="%.4f",
                    value=float(st.session_state.get("bt_fee_rate", fee_fraction_from_pct(settings.get("fee_spot_pct", 0.10)))),
                    key=_wkey("bt_fee_rate"),
                    on_change=_sync_model_from_widget,
                    args=("bt_fee_rate",),
                )

            with st.container(border=True):
                st.markdown("### Advanced")
                with st.expander("Live / Broker settings", expanded=False):
                    prefer_bbo_maker = st.checkbox(
                        "Prefer maker via BBO (live)",
                        help="When live trading on WooX, prefer BID/ASK queueing for maker fills.",
                        value=bool(st.session_state.get("bt_prefer_bbo_maker", bool(settings.get("prefer_bbo_maker", True)))),
                        key=_wkey("bt_prefer_bbo_maker"),
                        on_change=_sync_model_from_widget,
                        args=("bt_prefer_bbo_maker",),
                    )

                    bbo_level_input = st.selectbox(
                        "BBO queue level",
                        options=[1, 2, 3, 4, 5],
                        index=max(
                            0,
                            min(
                                4,
                                int(st.session_state.get("bt_bbo_queue_level", int(settings.get("bbo_queue_level", 1)) or 1))
                                - 1,
                            ),
                        ),
                        key=_wkey("bt_bbo_queue_level"),
                        on_change=_sync_model_from_widget,
                        args=("bt_bbo_queue_level",),
                        help="For maker execution: which level in the bid/ask queue to target.",
                    )

            # Estimate sweep run count
            lengths = [len(vals) for vals in sweep_specs.values() if vals]
            base_total_runs = math.prod(lengths) if lengths else 0
            asset_multiplier = 1
            if sweep_mode_active and data_source == "Crypto (CCXT)":
                try:
                    asset_multiplier = int(st.session_state.get("bt_sweep_assets_count_hint") or 1)
                except Exception:
                    asset_multiplier = 1
            total_runs = int(base_total_runs) * int(asset_multiplier)

            with st.container(border=True):
                st.markdown("### Run")

                if sweep_mode_active:
                    sweep_name = st.text_input(
                        "Sweep name",
                        value=str(st.session_state.get("bt_sweep_name", "Dragon sweep")),
                        key=_wkey("bt_sweep_name"),
                        on_change=_sync_model_from_widget,
                        args=("bt_sweep_name",),
                    )
                else:
                    sweep_name = str(st.session_state.get("bt_sweep_name", "Dragon sweep"))

                if sweep_mode_active:
                    st.info(f"Estimated total runs: {total_runs:,}")
                    if total_runs > 1000:
                        st.warning("Large sweep: consider reducing combinations.")

                run_as_high_priority = True
                if not sweep_mode_active:
                    run_as_high_priority = st.toggle(
                        "Run as high priority (interactive)",
                        value=bool(st.session_state.get("bt_run_high_priority", True)),
                        key=_wkey("bt_run_high_priority"),
                        help="When enabled, this single backtest run will be claimed before sweep child jobs.",
                    )

                run_button_label = "Start sweep" if sweep_mode_active else "Run backtest"
                run_clicked = st.button(run_button_label, type="primary")

                # Keep run confirmations directly under the Run section.
                run_feedback = st.empty()

        # --- Execute (payload paths preserved) ---------------------------
        fee_rate = float(fee_rate_input)

        if run_clicked:
            # Validate sweep state (if any field is in sweep mode, it must contribute values)
            if sweep_mode_active:
                sl_mode_selected = str(st.session_state.get("bt_sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value).strip().upper()
                tp_mode_selected = str(st.session_state.get("bt_tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value).strip().upper()

                sweep_field_map: Dict[str, str] = {
                    "bt_initial_entry_balance_pct": "general.initial_entry_balance_pct",
                    "bt_initial_entry_fixed_usd": "general.initial_entry_fixed_usd",
                    "bt_base_deviation_pct": "dca.base_deviation_pct",
                    "bt_deviation_multiplier": "dca.deviation_multiplier",
                    "bt_volume_multiplier": "dca.volume_multiplier",
                    "bt_ma_interval_min": "trend.ma_interval_min",
                    "bt_ma_length": "trend.ma_len",
                    "bt_bb_interval_min": "bbands.interval_min",
                    "bt_bb_length": "bbands.length",
                    "bt_macd_interval_min": "macd.interval_min",
                    "bt_rsi_interval_min": "rsi.interval_min",
                    "bt_rsi_length": "rsi.length",
                }

                # SL sweeps depend on SL mode.
                if sl_mode_selected == StopLossMode.ATR.value:
                    sweep_field_map.update(
                        {
                            "bt_tp_atr_period": "exits.tp_atr_period",
                            "bt_sl_atr_mult": "exits.sl_atr_mult",
                            "bt_trail_activation_atr_mult": "exits.trail_activation_atr_mult",
                            "bt_trail_distance_atr_mult": "exits.trail_distance_atr_mult",
                        }
                    )
                else:
                    sweep_field_map.update(
                        {
                            "bt_fixed_sl_pct": "exits.fixed_sl_pct",
                            "bt_trail_activation_pct": "exits.trail_activation_pct",
                            "bt_trail_stop_pct": "exits.trail_stop_pct",
                        }
                    )

                # TP sweeps depend on TP mode.
                if tp_mode_selected == TakeProfitMode.ATR.value:
                    sweep_field_map.update(
                        {
                            "bt_tp_atr_multiple": "exits.tp_atr_multiple",
                            "bt_tp_atr_period": "exits.tp_atr_period",
                        }
                    )
                else:
                    sweep_field_map.update({"bt_fixed_tp_pct": "exits.fixed_tp_pct"})

                missing = []
                for base_key, field_key in sweep_field_map.items():
                    if str(st.session_state.get(f"{base_key}_mode") or "fixed").strip().lower() == "sweep":
                        if not sweep_specs.get(field_key):
                            missing.append(field_key)
                if not sweep_specs or missing:
                    st.error("Sweep enabled but some fields have invalid/empty sweep values.")
                    if missing:
                        st.caption("Missing sweep values for: " + ", ".join(missing[:20]))
                    st.stop()

            sl_mode_selected = str(st.session_state.get("bt_sl_mode", StopLossMode.PCT.value) or StopLossMode.PCT.value).strip().upper()
            if sl_mode_selected not in {StopLossMode.PCT.value, StopLossMode.ATR.value}:
                sl_mode_selected = StopLossMode.PCT.value
            tp_mode_selected = str(st.session_state.get("bt_tp_mode", TakeProfitMode.ATR.value) or TakeProfitMode.ATR.value).strip().upper()
            if tp_mode_selected not in {TakeProfitMode.PCT.value, TakeProfitMode.ATR.value}:
                tp_mode_selected = TakeProfitMode.ATR.value

            use_atr_tp = tp_mode_selected == TakeProfitMode.ATR.value

            form_inputs = {
                "max_entries": int(max_entries),
                "initial_entry_sizing_mode": str(st.session_state.get("bt_initial_entry_sizing_mode") or InitialEntrySizingMode.PCT_BALANCE.value),
                "initial_entry_balance_pct": float(init_entry_pct),
                "initial_entry_fixed_usd": float(init_entry_usd),
                "use_indicator_consensus": bool(use_indicator_consensus),
                "backtest_leverage": int(backtest_leverage),
                "entry_order_style": entry_order_style,
                "entry_timeout_min": int(entry_timeout),
                "entry_cooldown_min": int(entry_cooldown),
                "exit_order_style": exit_order_style,
                "exit_timeout_min": int(entry_timeout),
                "allow_long": bool(allow_long),
                "allow_short": bool(allow_short),
                "lock_atr_on_entry_for_dca": bool(lock_atr_on_entry),
                "use_avg_entry_for_dca_base": bool(use_avg_entry),
                "use_ma_direction": bool(st.session_state.get("bt_use_ma_direction", False)),

                "sl_mode": sl_mode_selected,
                "fixed_sl_pct": float(st.session_state.get("bt_fixed_sl_pct", 0.0) or 0.0),
                "trail_activation_pct": float(st.session_state.get("bt_trail_activation_pct", 0.0) or 0.0),
                "trail_stop_pct": float(st.session_state.get("bt_trail_stop_pct", 0.0) or 0.0),
                "atr_period": int(st.session_state.get("bt_tp_atr_period", st.session_state.get("bt_atr_period", 14)) or 14),
                "sl_atr_mult": float(st.session_state.get("bt_sl_atr_mult", 0.0) or 0.0),
                "trail_activation_atr_mult": float(st.session_state.get("bt_trail_activation_atr_mult", 0.0) or 0.0),
                "trail_distance_atr_mult": float(st.session_state.get("bt_trail_distance_atr_mult", 0.0) or 0.0),

                "tp_mode": tp_mode_selected,
                "fixed_tp_pct": float(st.session_state.get("bt_fixed_tp_pct", 0.0) or 0.0),
                "use_atr_tp": bool(use_atr_tp),
                "tp_atr_multiple": float(st.session_state.get("bt_tp_atr_multiple", 0.0) or 0.0),
                "tp_atr_period": int(st.session_state.get("bt_tp_atr_period", 14) or 14),
                "tp_replace_threshold_pct": float(tp_replace_threshold_pct),
                "base_deviation_pct": float(base_dev),
                "deviation_multiplier": float(dev_multiplier),
                "volume_multiplier": float(vol_multiplier),
                "ma_interval_min": int(ma_interval),
                "ma_length": int(ma_length),
                "ma_type": str(st.session_state.get("bt_ma_type", "Sma") or "Sma"),
                "bb_interval_min": int(bb_interval),
                "bb_length": int(bb_length),
                "bb_dev_up": float(bb_dev_up),
                "bb_dev_down": float(bb_dev_down),
                "bb_ma_type": str(bb_ma_type),
                "bbands_deviation": float(bb_deviation),
                "bb_require_fcc": bool(bb_req_fcc),
                "bb_reset_middle": bool(bb_reset_mid),
                "bb_allow_mid_sells": bool(bb_allow_mid_sells),
                "macd_interval_min": int(macd_interval),
                "macd_fast": int(macd_fast),
                "macd_slow": int(macd_slow),
                "macd_signal": int(macd_signal),
                "rsi_interval_min": int(rsi_interval),
                "rsi_length": int(rsi_length),
                "rsi_buy_level": float(rsi_buy_level),
                "rsi_sell_level": float(rsi_sell_level),
                "global_dyn_pct": float(global_dyn_pct),
                "dyn_entry_pct": float(dyn_entry),
                "dyn_dca_pct": float(dyn_dca),
                "dyn_tp_pct": float(dyn_tp),
                "prefer_bbo_maker": bool(prefer_bbo_maker),
                "bbo_queue_level": int(bbo_level_input),
            }
            config = build_config(form_inputs)

            range_params: Dict[str, Optional[float]] = {}
            if data_source == "Crypto (CCXT)":
                timeframe_value = timeframe or "1h"
                timeframe_minutes = timeframe_to_minutes(timeframe_value)
                if ccxt_range_mode == "Bars":
                    range_params = {"limit": int(ccxt_limit_input or 500), "since": None, "until": None}
                elif ccxt_range_mode == "Duration":
                    delta = DURATION_CHOICES.get(duration_label or "1 week", timedelta(weeks=1))
                    end_time = datetime.now(timezone.utc)
                    start_time = end_time - delta
                    total_minutes = max(int(delta.total_seconds() // 60), timeframe_minutes)
                    range_params = {
                        "limit": max(int(total_minutes / timeframe_minutes) + 1, 1),
                        "since": start_time.timestamp() * 1000,
                        # Important: provide an explicit end bound so the cache layer can
                        # correctly detect incomplete coverage and fetch the missing tail.
                        "until": end_time.timestamp() * 1000,
                    }
                else:
                    if not start_dt or not end_dt or start_dt >= end_dt:
                        st.error("Please choose a valid start and end time for the backtest.")
                        st.stop()
                    span_minutes = max(int((end_dt - start_dt).total_seconds() // 60), timeframe_minutes)
                    range_params = {
                        "limit": max(int(span_minutes / timeframe_minutes) + 1, 1),
                        "since": start_dt.timestamp() * 1000,
                        "until": end_dt.timestamp() * 1000,
                    }
            else:
                range_params = {"count": int(candle_count)}

            def load_data() -> tuple[List[Candle], str, str, str]:
                if data_source == "Crypto (CCXT)":
                    try:
                        candles_local = get_candles_with_cache(
                            exchange_id=exchange_id,
                            symbol=symbol,
                            timeframe=timeframe,
                            limit=range_params.get("limit"),
                            since=range_params.get("since"),
                            until=range_params.get("until"),
                            range_mode=ccxt_range_mode or "Bars",
                            market_type=market_type.lower(),
                        )
                    except Exception as exc:
                        st.error(f"Failed to load CCXT data: {exc}")
                        st.stop()
                    if not candles_local:
                        st.warning("No candles returned from the exchange.")
                        st.stop()
                    label_local = (
                        f"Crypto (CCXT) – {ccxt_exchange_id} {ccxt_symbol} {ccxt_timeframe}, "
                        f"{len(candles_local)} candles"
                    )
                    return candles_local, label_local, ccxt_symbol, ccxt_timeframe
                count_local = int(range_params.get("count", candle_count))
                candles_local = generate_dummy_candles(n=count_local)
                return candles_local, f"Synthetic – {len(candles_local)} candles", "SYNTH", "synthetic"

            if not sweep_mode_active:
                data_source_key = "ccxt" if data_source == "Crypto (CCXT)" else "synthetic"
                range_mode_value = (ccxt_range_mode or "Bars") if data_source_key == "ccxt" else "Bars"

                config_payload = asdict(config)
                lev_bt = int(backtest_leverage) if int(backtest_leverage) > 0 else 1
                if lev_bt != 1:
                    config_payload["_futures"] = {"leverage": int(lev_bt)}
                payload = {
                    "config": config_payload,
                    "data_settings": asdict(
                        DataSettings(
                            data_source=data_source_key,
                            exchange_id=ccxt_exchange_id if data_source_key == "ccxt" else None,
                            market_type=market_type.lower() if data_source_key == "ccxt" else None,
                            symbol=symbol if data_source_key == "ccxt" else "SYNTH",
                            timeframe=timeframe if data_source_key == "ccxt" else "synthetic",
                            range_mode=range_mode_value.lower(),
                            range_params=range_params,
                            initial_balance=initial_balance,
                            fee_rate=fee_rate,
                        )
                    ),
                    "metadata": {
                        "strategy_name": "DragonDcaAtr",
                        "strategy_version": "0.1.0",
                        "data_source": data_source_key,
                        "exchange_id": ccxt_exchange_id if data_source_key == "ccxt" else None,
                        "market_type": market_type.lower() if data_source_key == "ccxt" else None,
                        "candle_count": range_params.get("limit", candle_count if data_source_key == "synthetic" else None),
                        "ccxt_limit": int(ccxt_limit_input or range_params.get("limit", 0))
                        if data_source_key == "ccxt" and range_mode_value.lower() == "bars"
                        else None,
                        "dynamic_activation_pct": config.general.dynamic_activation.entry_pct,
                        "range_mode": range_mode_value.lower(),
                        "range_params": range_params,
                        "duration_label": duration_label if range_mode_value.lower() == "duration" else None,
                        "start_ts": start_dt.isoformat() if start_dt else None,
                        "end_ts": end_dt.isoformat() if end_dt else None,
                        "prefer_bbo_maker": config.general.prefer_bbo_maker,
                        "bbo_queue_level": config.general.bbo_queue_level,
                    },
                }

                if lev_bt != 1:
                    payload["metadata"]["_futures"] = {"leverage": int(lev_bt)}
                    payload["metadata"]["leverage"] = int(lev_bt)

                job_id = enqueue_backtest_job(payload, is_interactive=bool(run_as_high_priority))
                st.session_state["last_backtest_job_id"] = int(job_id)
                msg = f"Backtest job queued: #{job_id}"
                if run_feedback is not None:
                    run_feedback.success(msg)
                else:
                    st.success(msg)
            else:
                candles, data_source_label, storage_symbol, storage_timeframe = load_data()
                data_source_key = "ccxt" if data_source == "Crypto (CCXT)" else "synthetic"
                range_mode_value = (ccxt_range_mode or "Bars") if data_source_key == "ccxt" else "Bars"
                range_params_copy = dict(range_params)
                base_data_settings = DataSettings(
                    data_source=data_source_key,
                    exchange_id=ccxt_exchange_id if data_source_key == "ccxt" else None,
                    market_type=market_type.lower() if data_source_key == "ccxt" else None,
                    symbol=storage_symbol if data_source_key == "ccxt" else None,
                    timeframe=storage_timeframe if data_source_key == "ccxt" else None,
                    range_mode=range_mode_value.lower(),
                    range_params=range_params_copy,
                    initial_balance=initial_balance,
                    fee_rate=fee_rate,
                )

                base_config_snapshot = _jsonify(config)
                lev_bt = int(backtest_leverage) if int(backtest_leverage) > 0 else 1
                if lev_bt != 1 and isinstance(base_config_snapshot, dict):
                    base_config_snapshot["_futures"] = {"leverage": int(lev_bt)}

                metadata_common = {
                    "strategy_name": "DragonDcaAtr",
                    "strategy_version": "0.1.0",
                    "initial_balance": initial_balance,
                    "fee_rate": fee_rate,
                    "data_source": data_source_key,
                    "exchange_id": ccxt_exchange_id if data_source_key == "ccxt" else None,
                    "market_type": market_type.lower() if data_source_key == "ccxt" else None,
                    "candle_count": len(candles),
                    "prefer_bbo_maker": config.general.prefer_bbo_maker,
                    "bbo_queue_level": config.general.bbo_queue_level,
                }
                if lev_bt != 1:
                    metadata_common["_futures"] = {"leverage": int(lev_bt)}
                    metadata_common["leverage"] = int(lev_bt)

                field_meta: dict[str, dict[str, Any]] = {}
                for k in sweep_specs.keys():
                    meta = SWEEPABLE_FIELDS.get(k)
                    if not meta:
                        continue
                    t = meta.get("type")
                    try:
                        type_name = str(getattr(t, "__name__", "str"))
                    except Exception:
                        type_name = "str"
                    path_val = meta.get("path")
                    path_list: list[str] = []
                    if isinstance(path_val, (list, tuple)):
                        path_list = [str(p).strip() for p in path_val if str(p).strip()]
                    field_meta[str(k)] = {
                        "target": str(meta.get("target") or "config"),
                        "path": path_list,
                        "type": type_name,
                    }

                sweep_definition = {
                    "params": {key: [str(v) for v in values] for key, values in sweep_specs.items()},
                    "params_mode": {key: str(sweep_param_modes.get(key) or "range") for key in sweep_specs.keys()},
                    "field_meta": field_meta,
                    "metadata_common": metadata_common,
                    "base_config": base_config_snapshot,
                    "data_settings": _jsonify(base_data_settings),
                }

                # Determine sweep asset scope + snapshot.
                sweep_scope = "single_asset"
                sweep_assets_snapshot: list[str] = []
                sweep_category_id = None
                if data_source_key == "ccxt":
                    scope_label = str(st.session_state.get("bt_sweep_assets_scope") or "Single asset")
                    if scope_label == "Manual selection":
                        sweep_scope = "multi_asset_manual"
                        sel = st.session_state.get("bt_sweep_assets_manual")
                        if isinstance(sel, list):
                            sweep_assets_snapshot = [str(s).strip() for s in sel if str(s).strip()]
                    elif scope_label == "Category":
                        sweep_scope = "multi_asset_category"
                        picked_name = str(st.session_state.get("bt_sweep_assets_category_name") or "").strip()
                        cats = []
                        try:
                            cats = list_categories(str(user_id or "").strip() or "admin@local", str(ccxt_exchange_id or "").strip())
                        except Exception:
                            cats = []
                        cid = None
                        for c in cats or []:
                            if str(c.get("name") or "").strip() == picked_name:
                                cid = c.get("id")
                                break
                        if cid is not None:
                            sweep_category_id = int(cid)
                            try:
                                sweep_assets_snapshot = get_category_symbols(
                                    str(user_id or "").strip() or "admin@local",
                                    str(ccxt_exchange_id or "").strip(),
                                    int(sweep_category_id),
                                )
                            except Exception:
                                sweep_assets_snapshot = []
                    else:
                        sweep_scope = "single_asset"
                        sweep_assets_snapshot = [str(storage_symbol or "").strip()] if str(storage_symbol or "").strip() else []

                # Validate snapshot for multi-asset modes.
                if sweep_scope in {"multi_asset_manual", "multi_asset_category"} and not sweep_assets_snapshot:
                    st.error("No assets selected for multi-asset sweep.")
                    st.stop()

                # Persist seen assets for Settings → Assets (best-effort).
                if data_source_key == "ccxt":
                    ex_for_assets = str(ccxt_exchange_id or "").strip()
                    for sym in (sweep_assets_snapshot or [str(storage_symbol or "").strip()]):
                        try:
                            if ex_for_assets and str(sym or "").strip():
                                upsert_asset(ex_for_assets, str(sym).strip(), None, None)
                        except Exception:
                            pass

                sweep_id = create_sweep(
                    SweepMeta(
                        user_id=str(user_id or "").strip() or None,
                        name=sweep_name,
                        strategy_name="DragonDcaAtr",
                        strategy_version="0.1.0",
                        data_source=data_source_key,
                        exchange_id=ccxt_exchange_id if data_source_key == "ccxt" else None,
                        symbol=(None if sweep_scope != "single_asset" else (storage_symbol if data_source_key == "ccxt" else None)),
                        timeframe=storage_timeframe if data_source_key == "ccxt" else None,
                        range_mode=range_mode_value.lower(),
                        range_params_json=json.dumps(_jsonify(range_params)),
                        base_config_json=json.dumps(base_config_snapshot),
                        sweep_definition_json=json.dumps(sweep_definition),
                        sweep_scope=str(sweep_scope),
                        sweep_assets_json=json.dumps([str(s).strip() for s in (sweep_assets_snapshot or []) if str(s).strip()])
                        if sweep_assets_snapshot
                        else json.dumps([str(storage_symbol or "").strip()])
                        if str(storage_symbol or "").strip()
                        else None,
                        sweep_category_id=int(sweep_category_id) if sweep_category_id is not None else None,
                        status="queued",
                    )
                )

                sweep_assets_job = (
                    [str(s).strip() for s in (sweep_assets_snapshot or []) if str(s).strip()]
                    if sweep_scope != "single_asset"
                    else [str(storage_symbol or "").strip()] if str(storage_symbol or "").strip() else []
                )
                total_combos = 1
                try:
                    for _k, vals in sweep_specs.items():
                        total_combos *= max(1, int(len(vals or [])))
                except Exception:
                    total_combos = 0
                total_runs = int(total_combos) * int(len(sweep_assets_job) if sweep_assets_job else 1)

                parent_job_id = enqueue_sweep_parent_job(sweep_id=str(sweep_id), user_id=str(user_id or "").strip() or None)
                st.session_state["last_sweep_parent_job_id"] = int(parent_job_id)
                st.session_state["last_sweep_id"] = str(sweep_id)
                msg = f"Sweep queued: sweep_id={sweep_id} ({total_runs} runs) – parent job_id={int(parent_job_id)}"
                if run_feedback is not None:
                    run_feedback.success(msg)
                else:
                    st.success(msg)

    if current_page == "Live Bots":
        st.subheader("Live Bots")
        render_active_filter_chips(get_global_filters())

        try:
            with open_db_connection() as _conn:
                _settings = get_app_settings(_conn)
            if not bool((_settings or {}).get("live_trading_enabled", False)):
                st.warning("Live trading is disabled globally. Worker will not process bots.")
        except Exception:
            pass

        st.markdown("#### Live Test Tools (safe – no live orders)")
        with st.expander("Connectivity check and one-bar dry-run", expanded=False):
            test_exchange = st.text_input("Exchange ID", value="woox", key="live_test_exchange")
            test_symbol = st.text_input("Symbol", value="PERP_BTC_USDT", key="live_test_symbol")
            test_timeframe = st.text_input("Timeframe", value="1m", key="live_test_timeframe")
            test_run_id = st.text_input("Config from backtest run (optional)", value="", key="live_test_run_id")

            if st.button("Connectivity check", key="live_test_connectivity"):
                api_key = os.environ.get("WOOX_API_KEY")
                api_secret = os.environ.get("WOOX_API_SECRET")
                base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")
                if not api_key or not api_secret:
                    st.warning("WOOX_API_KEY/SECRET not set; skipping real call and using stub.")
                try:
                    client = WooXClient(api_key=api_key or "stub", api_secret=api_secret or "stub", base_url=base_url)
                    ob = client.get_orderbook(test_symbol, depth=1)
                    st.success(f"Orderbook fetched for {test_symbol}: {ob}")
                except Exception as exc:
                    st.error(f"Connectivity check failed: {exc}")

            test_events: List[Dict[str, Any]] = []
            if st.button("Dry-run once (no orders)", key="live_test_dry_run"):
                config_dict = None
                if test_run_id.strip():
                    run_row = get_backtest_run(test_run_id.strip())
                    if run_row:
                        config_dict = _extract_config_dict(run_row)
                if not config_dict:
                    config_dict = asdict(dragon_config_example)
                config_obj = _dragon_config_from_snapshot(config_dict)

                try:
                    candles = load_ccxt_candles(test_exchange, test_symbol, test_timeframe, limit=2)
                except Exception:
                    candles = []
                if len(candles) < 2:
                    candles = generate_dummy_candles(n=2)
                candle = candles[-2]

                class _DryClient:
                    def __init__(self):
                        self._orders: List[Dict[str, Any]] = []

                    def set_position_mode(self, mode: str):
                        return {}

                    def get_positions(self, symbol: str):
                        return {"rows": []}

                    def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
                        return {"rows": list(self._orders)}

                    def place_order(self, body: Dict[str, Any]):
                        oid = body.get("clientOrderId", f"dry-{len(self._orders) + 1}")
                        order = {
                            "orderId": f"dry-{oid}",
                            "clientOrderId": oid,
                            "status": "INCOMPLETE",
                            "side": body.get("side"),
                            "positionSide": body.get("positionSide"),
                            "price": body.get("price"),
                            "quantity": body.get("quantity"),
                            "type": body.get("type"),
                        }
                        self._orders.append(order)
                        return {"orderId": order["orderId"], "clientOrderId": oid}

                    def cancel_order(self, order_id, symbol: str, client_order_id=None):
                        for ord_obj in self._orders:
                            if ord_obj.get("orderId") == order_id or ord_obj.get("clientOrderId") == client_order_id:
                                ord_obj["status"] = "CANCELLED"
                        return {}

                    def get_orderbook(self, symbol: str, depth: int = 1):
                        return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}

                broker = WooXBroker(
                    _DryClient(),
                    symbol=test_symbol,
                    bot_id="live-test",
                    prefer_bbo_maker=config_obj.general.prefer_bbo_maker,
                    bbo_level=config_obj.general.bbo_queue_level,
                )

                def _collect(level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
                    payload_local = payload or {}
                    payload_local.setdefault("level", level)
                    payload_local.setdefault("event_type", event_type)
                    payload_local.setdefault("message", message)
                    payload_local.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
                    test_events.append(payload_local)

                broker.set_event_logger(_collect)
                broker.sync_positions()
                broker.sync_orders()

                strategy = DragonDcaAtrStrategy(config=config_obj)
                strategy.on_bar(0, candle, broker)
                broker.sync_orders()

                st.success("Dry-run completed (no live orders placed).")
                if test_events:
                    st.dataframe(pd.DataFrame(test_events), hide_index=True, width="stretch")
                else:
                    st.info("No events emitted during dry-run.")

        selected_bot_id = st.session_state.get("selected_bot_id")
        view_mode = (_extract_query_param(st.query_params, "view") or "").strip().lower()

        with _job_conn() as conn:
            user_id = get_or_create_user_id(get_current_user_email(), conn=conn)
            try:
                bots = get_bot_snapshots(conn, user_id, limit=500)
            except Exception:
                bots = list_bots(conn, limit=500)

        try:
            selected_bot_id = int(selected_bot_id) if selected_bot_id not in (None, "") else None
        except (TypeError, ValueError):
            pass

        # Creating bots is now driven by the grid action button (+) on a run.
        target_run_id = str(st.session_state.get("create_live_bot_run_id") or "").strip()
        if target_run_id and st.session_state.get("_create_live_bot_prefilled_for") != target_run_id:
            _reset_create_live_bot_form_state()
            st.session_state["_create_live_bot_prefilled_for"] = target_run_id
            _update_query_params(create_live_bot_run_id=None)

        if not target_run_id:
            # Overview grid + selection-driven details (restores the "select row → see more details" flow).
            render_live_bots_overview(bots, user_id=user_id)
            st.info("Use the '+' action on a backtest run in Results to create a bot from that run's config.")

            # Show selected bot details below the grid.
            try:
                selected_bot_id_int = int(selected_bot_id) if selected_bot_id not in (None, "") else None
            except (TypeError, ValueError):
                selected_bot_id_int = None
            if selected_bot_id_int:
                st.markdown("---")
                render_live_bot_detail(int(selected_bot_id_int))
            return

        st.subheader(f"Create bot from run #{target_run_id}")
        run_row: Optional[Dict[str, Any]] = get_backtest_run(target_run_id)
        config_dict: Optional[Dict[str, Any]] = None
        metadata: Dict[str, Any] = {}
        if run_row:
            metadata = _extract_metadata(run_row)
            config_dict = _extract_config_dict(run_row)
            if not config_dict:
                st.error("Run found but missing config snapshot; cannot create a bot from it.")
                config_dict = None
        else:
            st.warning("Run not found. Check the run ID and try again.")
            config_dict = None

        if config_dict:
            symbol_default = run_row.get("symbol") if isinstance(run_row, dict) else None
            symbol_default = symbol_default or metadata.get("symbol") or ""
            timeframe_default = run_row.get("timeframe") if isinstance(run_row, dict) else None
            timeframe_default = timeframe_default or metadata.get("timeframe") or "1m"
            exchange_default = metadata.get("exchange_id") or "woox"
            prefer_bbo_default = bool(metadata.get("prefer_bbo_maker", st.session_state.get("prefer_bbo_maker", True)))
            bbo_level_default = int(metadata.get("bbo_queue_level", st.session_state.get("bbo_queue_level", 1)) or 1)
            bot_name_default = (
                f"Bot {run_row.get('id')}" if isinstance(run_row, dict) and run_row.get("id") is not None else "New bot"
            )

            general_cfg = (config_dict.get("general") or {}) if isinstance(config_dict, dict) else {}
            allow_long = bool(general_cfg.get("allow_long", True))
            allow_short = bool(general_cfg.get("allow_short", False))
            init_entry_mode_default = _safe_initial_entry_sizing_mode(general_cfg.get("initial_entry_sizing_mode")).value
            init_entry_pct_default = float(general_cfg.get("initial_entry_balance_pct") or 10.0)
            init_entry_usd_default = float(general_cfg.get("initial_entry_fixed_usd") or 100.0)
            entry_cd_default = int(general_cfg.get("entry_cooldown_min") or 10)
            entry_to_default = int(general_cfg.get("entry_timeout_min") or 10)
            exit_to_default = int(general_cfg.get("exit_timeout_min") or entry_to_default)
            derived = derive_primary_position_side_from_allow_flags(allow_long=allow_long, allow_short=allow_short)

            with st.expander("Strategy config (from run)", expanded=False):
                render_dragon_strategy_config_view(config_dict, key_prefix=f"run_{target_run_id}")

            st.caption("Strategy config will be reused from the backtest; only entry/timeout sizing fields below are editable.")
            strategy_default = str(run_row.get("strategy_name") if isinstance(run_row, dict) else "").strip()
            if not strategy_default:
                strategy_default = str(metadata.get("strategy_name") or "").strip()
            if not strategy_default:
                strategy_default = "DragonDcaAtr"
            strategy_name_live = st.text_input(
                "Strategy",
                value=strategy_default,
                key="live_bot_strategy_name",
                help="Stored on the bot config for display/traceability.",
            )
            bot_name = st.text_input("Bot name", value=bot_name_default, key="live_bot_name")
            exchange_val = st.text_input("Exchange ID", value=exchange_default, key="live_bot_exchange")
            symbol_val = st.text_input("Symbol", value=symbol_default or "PERP_BTC_USDT", key="live_bot_symbol")
            timeframe_val = st.text_input("Timeframe", value=timeframe_default or "1m", key="live_bot_timeframe")
            prefer_bbo_live = st.checkbox(
                "Prefer maker via BBO",
                value=prefer_bbo_default,
                help="Use BID/ASK queueing when supported (WooX).",
                key="live_bot_prefer_bbo",
            )
            bbo_level_live = st.selectbox(
                "BBO queue level",
                options=[1, 2, 3, 4, 5],
                index=max(0, min(4, bbo_level_default - 1)),
                key="live_bot_bbo_level",
            )

            init_entry_mode_live = st.radio(
                "Initial entry sizing",
                options=[InitialEntrySizingMode.PCT_BALANCE.value, InitialEntrySizingMode.FIXED_USD.value],
                index=0
                if str(st.session_state.get("live_bot_initial_entry_sizing_mode", init_entry_mode_default)) == InitialEntrySizingMode.PCT_BALANCE.value
                else 1,
                key="live_bot_initial_entry_sizing_mode",
                format_func=lambda v: "Percent of portfolio" if v == InitialEntrySizingMode.PCT_BALANCE.value else "Fixed $ amount",
            )

            if str(init_entry_mode_live) == InitialEntrySizingMode.FIXED_USD.value:
                init_entry_usd_live = st.number_input(
                    "Initial entry ($ notional)",
                    min_value=0.0,
                    step=10.0,
                    value=float(st.session_state.get("live_bot_initial_entry_fixed_usd", init_entry_usd_default)),
                    key="live_bot_initial_entry_fixed_usd",
                    help="Size of the first entry as a fixed $ notional amount (USD/USDT).",
                )
                init_entry_pct_live = float(st.session_state.get("live_bot_initial_entry_balance_pct", init_entry_pct_default))
            else:
                init_entry_pct_live = st.number_input(
                    "Initial entry (% of balance)",
                    min_value=0.0,
                    max_value=100.0,
                    step=1.0,
                    value=float(st.session_state.get("live_bot_initial_entry_balance_pct", init_entry_pct_default)),
                    key="live_bot_initial_entry_balance_pct",
                    help="Strategy sizes the first entry as this % of broker.balance (live uses DRAGON_LIVE_BALANCE or _risk.balance_override).",
                )
                init_entry_usd_live = float(st.session_state.get("live_bot_initial_entry_fixed_usd", init_entry_usd_default))

            row_timeouts = st.columns(3)
            with row_timeouts[0]:
                entry_cooldown_live = st.number_input(
                    "Entry cooldown (min)",
                    min_value=0,
                    step=1,
                    value=int(st.session_state.get("live_bot_entry_cooldown_min", entry_cd_default)),
                    key="live_bot_entry_cooldown_min",
                )
            with row_timeouts[1]:
                entry_timeout_live = st.number_input(
                    "Entry timeout (min)",
                    min_value=0,
                    step=1,
                    value=int(st.session_state.get("live_bot_entry_timeout_min", entry_to_default)),
                    key="live_bot_entry_timeout_min",
                )
            with row_timeouts[2]:
                exit_timeout_live = st.number_input(
                    "Exit timeout (min)",
                    min_value=0,
                    step=1,
                    value=int(st.session_state.get("live_bot_exit_timeout_min", exit_to_default)),
                    key="live_bot_exit_timeout_min",
                )

            with _job_conn() as conn:
                accounts_for_user = get_account_snapshots(conn, user_id, exchange_id=str(exchange_default).strip().lower(), include_deleted=False)
            active_accounts = [a for a in accounts_for_user if str(a.get("status") or "").strip().lower() == "active"]
            acct_ids = [int(a.get("id")) for a in active_accounts if a.get("id") is not None]
            if not acct_ids:
                st.warning("No active accounts available. Create one in Tools → Accounts.")
                selected_account_id = None
            else:
                def _fmt_acct_id(aid: int) -> str:
                    row = next((a for a in active_accounts if int(a.get("id")) == int(aid)), None)
                    if not row:
                        return f"#{aid}"
                    label = (row.get("label") or "").strip() or f"#{aid}"
                    ex = (row.get("exchange_id") or "").strip()
                    return f"{label} ({ex})"

                selected_account_id = st.selectbox(
                    "Account",
                    options=acct_ids,
                    format_func=_fmt_acct_id,
                    key="live_bot_account_id",
                )
                desired_status_label = st.selectbox(
                    "Desired status",
                    ["running", "paused", "stopped"],
                    index=0,
                    format_func=lambda val: val.capitalize(),
                    key="live_bot_desired_status",
                )

                st.markdown("#### Hedge leg")
                st.caption("Uses allow_long/allow_short to decide which exchange leg this bot controls.")

                create_mode = None
                primary_leg_choice = None
                if derived is None:
                    st.error("This run config has allow_long=False and allow_short=False. Enable at least one to create a bot.")
                elif derived in {"LONG", "SHORT"}:
                    st.info(f"Primary leg derived from config: **{derived}**")
                    primary_leg_choice = derived
                else:
                    create_mode = st.radio(
                        "When both long and short are allowed",
                        options=["Create TWO bots (recommended)", "Create ONE bot (pick primary leg)"],
                        index=0,
                        horizontal=False,
                        key="live_bot_create_mode",
                    )
                    if create_mode == "Create ONE bot (pick primary leg)":
                        primary_leg_choice = st.selectbox(
                            "Primary leg",
                            options=["LONG", "SHORT"],
                            index=0,
                            key="live_bot_primary_leg",
                        )

                st.markdown("#### Futures (optional)")
                fut_cfg = (config_dict.get("_futures") or {}) if isinstance(config_dict.get("_futures"), dict) else {}
                risk_cfg_base = (config_dict.get("_risk") or {}) if isinstance(config_dict.get("_risk"), dict) else {}

                lev_default = None
                try:
                    if fut_cfg.get("leverage") is not None:
                        lev_default = int(float(fut_cfg.get("leverage")))
                except Exception:
                    lev_default = None
                if lev_default is None:
                    try:
                        mlev = (metadata.get("_futures") or {}) if isinstance(metadata.get("_futures"), dict) else None
                        if isinstance(mlev, dict) and mlev.get("leverage") is not None:
                            lev_default = int(float(mlev.get("leverage")))
                    except Exception:
                        lev_default = None
                if lev_default is None:
                    try:
                        if metadata.get("leverage") is not None:
                            lev_default = int(float(metadata.get("leverage")))
                    except Exception:
                        lev_default = None
                if lev_default is None:
                    lev_default = 10
                try:
                    lev_default = max(1, int(lev_default))
                except Exception:
                    lev_default = 10

                apply_default = False
                try:
                    if fut_cfg.get("apply_leverage_on_start") is not None:
                        apply_default = bool(fut_cfg.get("apply_leverage_on_start"))
                    else:
                        apply_default = bool(int(lev_default) != 1)
                except Exception:
                    apply_default = bool(int(lev_default) != 1)

                max_lev_default = 50
                try:
                    if risk_cfg_base.get("max_leverage") is not None:
                        max_lev_default = int(float(risk_cfg_base.get("max_leverage")))
                except Exception:
                    max_lev_default = 50

                apply_lev = st.checkbox(
                    "Apply leverage on start",
                    value=bool(st.session_state.get("live_bot_apply_leverage", apply_default)),
                    key="live_bot_apply_leverage",
                )
                lev_val = st.number_input(
                    "Leverage",
                    min_value=1,
                    max_value=200,
                    value=int(st.session_state.get("live_bot_leverage", lev_default) or lev_default),
                    step=1,
                    key="live_bot_leverage",
                )
                max_lev_val = st.number_input(
                    "Max leverage (risk clamp)",
                    min_value=1,
                    max_value=200,
                    value=int(st.session_state.get("live_bot_max_leverage", max_lev_default) or max_lev_default),
                    step=1,
                    key="live_bot_max_leverage",
                )

                can_create = derived is not None and selected_account_id is not None
                if st.button("Create bot from run", type="primary", disabled=not can_create):
                    payload_config_base = deepcopy(config_dict)
                    strategy_name_clean = str(strategy_name_live or "").strip()
                    if strategy_name_clean:
                        payload_config_base["strategy_name"] = strategy_name_clean
                        md_cfg = payload_config_base.setdefault("metadata", {})
                        if not isinstance(md_cfg, dict):
                            md_cfg = {}
                            payload_config_base["metadata"] = md_cfg
                        md_cfg["strategy_name"] = strategy_name_clean
                    payload_config_base.setdefault("general", {})
                    payload_config_base["general"]["prefer_bbo_maker"] = bool(prefer_bbo_live)
                    payload_config_base["general"]["bbo_queue_level"] = int(bbo_level_live)
                    payload_config_base["general"]["initial_entry_sizing_mode"] = str(init_entry_mode_live)
                    payload_config_base["general"]["initial_entry_balance_pct"] = float(init_entry_pct_live)
                    payload_config_base["general"]["initial_entry_fixed_usd"] = float(init_entry_usd_live)
                    payload_config_base["general"]["entry_cooldown_min"] = int(entry_cooldown_live)
                    payload_config_base["general"]["entry_timeout_min"] = int(entry_timeout_live)
                    payload_config_base["general"]["exit_timeout_min"] = int(exit_timeout_live)

                    fut = payload_config_base.setdefault("_futures", {})
                    if not isinstance(fut, dict):
                        fut = {}
                        payload_config_base["_futures"] = fut
                    fut["apply_leverage_on_start"] = bool(apply_lev)
                    fut["leverage"] = int(lev_val)
                    risk_cfg = payload_config_base.setdefault("_risk", {})
                    if not isinstance(risk_cfg, dict):
                        risk_cfg = {}
                        payload_config_base["_risk"] = risk_cfg
                    risk_cfg["max_leverage"] = int(max_lev_val)

                    legs_to_create: List[str] = []
                    if derived in {"LONG", "SHORT"}:
                        legs_to_create = [str(primary_leg_choice or derived)]
                    else:
                        if create_mode == "Create ONE bot (pick primary leg)":
                            legs_to_create = [str(primary_leg_choice or "LONG")]
                        else:
                            legs_to_create = ["LONG", "SHORT"]

                    created_bot_ids: List[int] = []
                    created_job_ids: List[int] = []

                    with _job_conn() as conn:
                        for leg in legs_to_create:
                            leg_norm = normalize_primary_position_side(leg)
                            payload_config = deepcopy(payload_config_base)
                            trade_cfg = payload_config.setdefault("_trade", {})
                            if not isinstance(trade_cfg, dict):
                                trade_cfg = {}
                                payload_config["_trade"] = trade_cfg
                            trade_cfg["primary_position_side"] = leg_norm

                            name_for_leg = bot_name
                            if len(legs_to_create) > 1:
                                name_for_leg = f"{bot_name} ({leg_norm})"

                            bot_id = create_bot(
                                conn,
                                name=name_for_leg,
                                exchange_id=exchange_val,
                                symbol=symbol_val,
                                timeframe=timeframe_val,
                                config=payload_config,
                                status="created",
                                desired_status=str(desired_status_label),
                                heartbeat_msg="Bot created from run",
                                user_id=user_id,
                                account_id=int(selected_account_id) if selected_account_id is not None else None,
                                credentials_id=None,
                            )
                            created_bot_ids.append(int(bot_id))
                            link_bot_to_run(conn, bot_id, str(target_run_id).strip())
                            payload_live = {
                                "job_type": "live_bot",
                                "bot_id": bot_id,
                                "run_id": str(target_run_id).strip(),
                                "exchange_id": exchange_val,
                                "symbol": symbol_val,
                                "timeframe": timeframe_val,
                                "account_id": int(selected_account_id) if selected_account_id is not None else None,
                                "prefer_bbo_maker": bool(prefer_bbo_live),
                                "bbo_queue_level": int(bbo_level_live),
                                "config": payload_config,
                            }
                            job_id = create_job(conn, "live_bot", payload_live, bot_id=bot_id)
                            created_job_ids.append(int(job_id))
                            set_job_link(conn, job_id, run_id=str(target_run_id).strip())

                    if len(created_bot_ids) == 1:
                        st.success(f"Bot #{created_bot_ids[0]} created and live job #{created_job_ids[0]} queued.")
                        _set_selected_bot(int(created_bot_ids[0]))
                    else:
                        st.success(f"Bots created: {', '.join([f'#{x}' for x in created_bot_ids])}.")
                    st.rerun()
        else:
            st.info("Enter a valid run ID to load its config and create a bot.")

        # Live Bots page is complete; avoid falling through to Results grid logic.
        return

    # Runs Explorer page removed (Results now covers this functionality).

        good_bad_ratio_style = JsCode(
                        """
                        function(params) {
                            const v = Number(params.value);
                            if (!isFinite(v)) { return {}; }
                            if (v >= 1.0) { return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)' }; }
                            return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)' };
                        }
                        """
                )

        good_bad_win_rate_style = JsCode(
                        """
                        function(params) {
                            const v = Number(params.value);
                            if (!isFinite(v)) { return {}; }
                            if (v >= 0.5) { return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)' }; }
                            return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)' };
                        }
                        """
                )

        good_bad_sharpe_style = JsCode(
                        """
                        function(params) {
                            const v = Number(params.value);
                            if (!isFinite(v)) { return {}; }
                            if (v >= 1.0) { return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)' }; }
                            if (v < 0.0) { return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)' }; }
                            return { color: '#cbd5e1' };
                        }
                        """
                )

        dd_style = JsCode(
                        """
                        function(params) {
                            const v = Number(params.value);
                            if (!isFinite(v)) { return {}; }
                            // v is a fraction (e.g. 0.12 = 12%). Use a light->dark red ramp.
                            // Keep the same red base as elsewhere (#f8d7da) and vary opacity.
                            const t = Math.max(0.0, Math.min(1.0, v / 0.30)); // saturate at 30% DD
                            // Dark theme: red overlay on dark background.
                            const alpha = 0.10 + (0.25 * t);
                            return { color: '#ff7b72', backgroundColor: `rgba(248,81,73,${alpha})`, textAlign: 'center' };
                        }
                        """
                )

        metrics_gradient_style = JsCode(
                        """
                        function(params) {
                            const col = (params && params.colDef && params.colDef.field) ? params.colDef.field.toString() : '';
                            const v = Number(params.value);
                            if (!isFinite(v)) { return {}; }

                            // Dark theme palette (high-contrast, subtle overlays)
                            const greenText = '#7ee787';
                            const redText = '#ff7b72';
                            const neutralText = '#cbd5e1';
                            const greenRgb = '46,160,67';   // green overlay
                            const redRgb = '248,81,73';     // red overlay

                            function clamp01(x) {
                                return Math.max(0.0, Math.min(1.0, x));
                            }

                            function alphaFromT(t) {
                                const tt = clamp01(t);
                                // Dark theme: keep subtle but visible.
                                return 0.10 + (0.22 * tt);
                            }

                            function styleGoodBad(isGood, t) {
                                const a = alphaFromT(t);
                                if (isGood === true) {
                                    return { color: greenText, backgroundColor: `rgba(${greenRgb},${a})`, textAlign: 'center' };
                                }
                                if (isGood === false) {
                                    return { color: redText, backgroundColor: `rgba(${redRgb},${a})`, textAlign: 'center' };
                                }
                                return { color: neutralText, textAlign: 'center' };
                            }

                            function asRatio(x) {
                                if (!isFinite(x)) { return x; }
                                return (Math.abs(x) <= 1.0) ? x : (x / 100.0);
                            }

                            if (col === 'max_drawdown_pct') {
                                const r = asRatio(v);
                                const t = clamp01(r / 0.30);
                                return styleGoodBad(false, t);
                            }

                            if (col === 'net_return_pct') {
                                const r = asRatio(v);
                                const t = clamp01(Math.abs(r) / 0.50);
                                if (r > 0) return styleGoodBad(true, t);
                                if (r < 0) return styleGoodBad(false, t);
                                return { color: neutralText, textAlign: 'center' };
                            }

                            if (col === 'roi_pct_on_margin') {
                                // v is already a percent (e.g. 100.0 means +100%)
                                const t = clamp01(Math.abs(v) / 200.0);
                                if (v > 0) return styleGoodBad(true, t);
                                if (v < 0) return styleGoodBad(false, t);
                                return { color: neutralText, textAlign: 'center' };
                            }

                            if (col === 'win_rate') {
                                const r = asRatio(v);
                                const dist = Math.abs(r - 0.5);
                                const t = clamp01(dist / 0.25);
                                return styleGoodBad(r >= 0.5, t);
                            }

                            if (['profit_factor', 'cpc_index', 'common_sense_ratio'].includes(col)) {
                                const dist = Math.abs(v - 1.0);
                                const t = clamp01(dist / 1.0);
                                return styleGoodBad(v >= 1.0, t);
                            }

                            if (['sharpe', 'sortino'].includes(col)) {
                                if (v >= 1.0) {
                                    const t = clamp01((v - 1.0) / 2.0);
                                    return styleGoodBad(true, t);
                                }
                                if (v < 0.0) {
                                    const t = clamp01(Math.abs(v) / 1.5);
                                    return styleGoodBad(false, t);
                                }
                                return { color: neutralText, textAlign: 'center' };
                            }

                            if (col === 'net_profit') {
                                const t = clamp01(Math.abs(v) / 1000.0);
                                if (v > 0) return styleGoodBad(true, t);
                                if (v < 0) return styleGoodBad(false, t);
                                return { color: neutralText, textAlign: 'center' };
                            }

                            return {};
                        }
                        """
                )

        # Default ordering: CPC Index desc (falls back to current ordering if missing).
        # Do this before computing pre_selected row indices.
        try:
            if "cpc_index" in getattr(df, "columns", []):
                df = df.sort_values(by="cpc_index", ascending=False, na_position="last")
                df = df.reset_index(drop=True)
        except Exception:
            pass

        # Ensure this is defined before use below (avoids UnboundLocalError)
        active_run_id = str(st.session_state.get("runs_explorer_active_run_id") or "").strip()

        pre_selected: List[int] = []
        df_cols: list[str] = []
        try:
            df_cols = list(getattr(df, "columns", []))  # type: ignore[name-defined]
        except Exception:
            df_cols = []
        if active_run_id and "run_id" in df_cols:
            try:
                pre_selected = [int(i) for i in df.index[df["run_id"].astype(str) == active_run_id].tolist()]
            except Exception:
                pre_selected = []
        pct_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
                            const shown = (Math.abs(v) <= 1.0) ? (v * 100.0) : v;
                                                        return shown.toFixed(0) + '%';
            }
            """
        )
        num2_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
              return v.toFixed(2);
            }
            """
        )
        def _escape_js_template_literal2(s: str) -> str:
            return (s or "").replace("`", "\\`").replace("${", "\\${")

        def _tabler_svg_safe2(name: str, fallback: str) -> str:
            try:
                return get_tabler_svg(name)
            except Exception:
                return get_tabler_svg(fallback)

        play_svg_js2 = _escape_js_template_literal2(get_tabler_svg(TABLER_ICONS["refresh"]))
        plus_svg_js2 = _escape_js_template_literal2(_tabler_svg_safe2("robot", TABLER_ICONS["create"]))
        folder_svg_js2 = _escape_js_template_literal2(get_tabler_svg(TABLER_ICONS["sweeps"]))

        actions_js2 = (
                        """
                        class ActionsRenderer {
                                init(params) {
                                        const wrap = document.createElement('div');
                                        wrap.style.display = 'flex';
                                        wrap.style.gap = '4px';
                                        wrap.style.justifyContent = 'flex-start';
                                        wrap.style.alignItems = 'center';
                                        wrap.style.width = '100%';
                                        wrap.style.pointerEvents = 'auto';

                                        const btnSize = '34px';

                                        const playSvg = `__PLAY_SVG__`;
                                        const plusSvg = `__PLUS_SVG__`;
                                        const folderSvg = `__FOLDER_SVG__`;

                    function setClick(action) {
                        try {
                            const rid = (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null)
                                ? params.data.run_id.toString()
                                : '';
                            if (!rid) { return; }

                            // Fast-path navigation: update the parent's URL so Streamlit switches page immediately.
                            // This avoids re-running the heavy Results grid just to handle the click token.
                            try {
                                let url = '';
                                if (action === 'create_bot') {
                                    url = (params && params.data && params.data.create_bot_url) ? params.data.create_bot_url.toString() : '';
                                }
                                if (url && typeof window !== 'undefined' && window.parent && window.parent.location) {
                                    if (url.indexOf('?') !== 0) { url = '?' + url; }
                                    const base = window.parent.location.origin + window.parent.location.pathname;
                                    window.parent.location.href = base + url;
                                    return;
                                }
                            } catch (navErr) {}

                            const token = `${action}:${rid}:${Date.now()}`;
                            if (params && params.node && params.node.setDataValue) {
                                params.node.setDataValue('action_click', token);
                            }
                            // Also select the row so Streamlit reruns on SELECTION_CHANGED.
                            // This makes actions reliable even if the value-change event isn't propagated.
                            if (params && params.node && params.node.setSelected) {
                                params.node.setSelected(true, true);
                            }
                        } catch (err) {}
                    }

                    function mk(svg, title, action, bg, disabled=false) {
                        const b = document.createElement('button');
                        b.type = 'button';
                        b.innerHTML = svg;
                        b.title = title;
                        b.setAttribute('aria-label', title);
                        b.style.display = 'inline-flex';
                        b.style.alignItems = 'center';
                        b.style.justifyContent = 'center';
                        b.style.width = btnSize;
                        b.style.height = btnSize;
                        b.style.minWidth = btnSize;
                        b.style.minHeight = btnSize;
                        b.style.borderRadius = '6px';
                        b.style.border = '1px solid rgba(255,255,255,0.18)';
                        b.style.background = bg;
                        b.style.color = '#FFFFFF';
                        b.style.cursor = disabled ? 'not-allowed' : 'pointer';
                        b.style.padding = '0';
                        b.style.lineHeight = btnSize;
                        b.style.boxShadow = 'none';
                        b.style.outline = 'none';
                        b.style.userSelect = 'none';
                        if (disabled) {
                            b.style.opacity = '0.35';
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                            };
                        } else {
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                                setClick(action);
                            };
                        }
                        return b;
                    }

                    const hasRunId = (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null);
                    if (hasRunId) {
                        const sid = (params && params.data && params.data.sweep_id !== undefined && params.data.sweep_id !== null)
                            ? params.data.sweep_id.toString()
                            : '';
                        if (sid) {
                            wrap.appendChild(mk(folderSvg, 'Open sweep', 'open_sweep', 'rgba(148,163,184,0.16)'));
                        } else {
                            wrap.appendChild(mk(folderSvg, 'Not part of a sweep', 'open_sweep', 'rgba(148,163,184,0.10)', true));
                        }
                        wrap.appendChild(mk(plusSvg, 'Create bot', 'create_bot', 'rgba(46,160,67,0.18)'));
                    }

                    this.eGui = wrap;
                }
                getGui() { return this.eGui; }
                refresh(params) { return false; }
            }
            """
        )

        # Presentation-only: convert date columns to local strings before AGGrid.
        try:
            tz = _ui_display_tz()
            if "created_at" in df.columns:
                df["created_at"] = [_ui_local_dt_ampm(v, tz) for v in df["created_at"].tolist()]
            for _c in ("start_date", "end_date"):
                if _c in df.columns:
                    df[_c] = [_ui_local_date_dmy(v, tz) for v in df[_c].tolist()]
        except Exception:
            pass

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(filter=True, sortable=True, resizable=True)
        # Place selection checkboxes inside the Asset column (not as a separate left column).
        gb.configure_selection("multiple", use_checkbox=False)

        # Finance-style layout tuning
        gb.configure_grid_options(headerHeight=58, rowHeight=38, suppressRowHoverHighlight=False, animateRows=False)

        # Prefer set filters when enterprise modules are enabled
        sym_filter = "agSetColumnFilter" if use_enterprise else True

        _sweeps_runs_widths: dict[str, int] = {}
        try:
            _sweeps_runs_saved = st.session_state.get("sweeps_runs_columns_state")
            if isinstance(_sweeps_runs_saved, list) and _sweeps_runs_saved:
                for item in _sweeps_runs_saved:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        _sweeps_runs_widths[str(col_id)] = int(width_val)
                    except Exception:
                        continue
        except Exception:
            pass

        def _sweeps_runs_w(col: str, default: int) -> int:
            try:
                return max(60, int(_sweeps_runs_widths.get(str(col), default)))
            except Exception:
                return default

        shared_col_defs: list[dict[str, Any]] = []
        if "start_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_date",
                    header_map.get("start_date", "Start"),
                    width=_sweeps_runs_w("start_date", 110),
                    hide=("start_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_date",
                    header_map.get("end_date", "End"),
                    width=_sweeps_runs_w("end_date", 110),
                    hide=("end_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "timeframe" in df.columns:
            shared_col_defs.append(
                col_timeframe_pill(
                    field="timeframe",
                    header=header_map.get("timeframe", "Timeframe"),
                    width=_sweeps_runs_w("timeframe", 90),
                    filter=sym_filter,
                    hide=("timeframe" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "direction" in df.columns:
            shared_col_defs.append(
                col_direction_pill(
                    field="direction",
                    header=header_map.get("direction", "Direction"),
                    width=_sweeps_runs_w("direction", 100),
                    filter=sym_filter,
                    hide=("direction" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "avg_position_time" in df.columns:
            shared_col_defs.append(
                col_avg_position_time(
                    field="avg_position_time",
                    header=header_map.get("avg_position_time", "Avg Position Time"),
                    width=_sweeps_runs_w("avg_position_time", 150),
                    hide=("avg_position_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist" in df.columns:
            shared_col_defs.append(
                col_shortlist(
                    field="shortlist",
                    header=col_label("shortlist"),
                    editable=True,
                    width=_sweeps_runs_w("shortlist", 130),
                    hide=("shortlist" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist_note" in df.columns:
            shared_col_defs.append(
                col_shortlist_note(
                    field="shortlist_note",
                    header=col_label("shortlist_note"),
                    editable=True,
                    width=_sweeps_runs_w("shortlist_note", 200),
                    hide=("shortlist_note" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        apply_columns(gb, shared_col_defs, saved_widths=_sweeps_runs_widths)

        runtime_state = st.session_state.get("results_runs_columns_state_runtime")
        saved_state = st.session_state.get("runs_explorer_columns_state")
        state_source = runtime_state if isinstance(runtime_state, list) and runtime_state else saved_state
        _results_widths: dict[str, int] = {}
        try:
            if isinstance(state_source, list) and state_source:
                for item in state_source:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        _results_widths[str(col_id)] = int(width_val)
                    except Exception:
                        continue
        except Exception:
            pass

        def _res_w(col: str, default: int) -> int:
            try:
                return max(60, int(_results_widths.get(str(col), default)))
            except Exception:
                return default

        # Columns
        # (via “Save layout”), and we keep defaults as the true starting point.
        debug_grid = str(os.environ.get("DRAGON_DEBUG_GRID") or "").strip().lower() in {"1", "true", "yes", "on"}
        debug_logger = logging.getLogger(__name__)
        _runs_widths: dict[str, int] = {}
        try:
            _runs_runtime_state = st.session_state.get("runs_explorer_columns_state_runtime")
            _runs_saved_state = st.session_state.get("runs_explorer_columns_state")
            _runs_state_source = (
                _runs_runtime_state
                if isinstance(_runs_runtime_state, list) and _runs_runtime_state
                else _runs_saved_state
            )
            if isinstance(_runs_state_source, list) and _runs_state_source:
                for item in _runs_state_source:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        _runs_widths[str(col_id)] = int(width_val)
                    except Exception:
                        continue
        except Exception:
            pass
        if debug_grid:
            debug_logger.info(
                "runs_explorer.grid_state width_map key=%s widths=%s",
                "ui.runs_explorer.columns_state",
                len(_runs_widths),
            )

        def _w(col: str, default: int) -> int:
            try:
                base = _runs_widths.get(str(col), default)
                return max(60, int(base))
            except Exception:
                return default

        shared_col_defs: list[dict[str, Any]] = []
        if "start_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_date",
                    header_map.get("start_date", "Start"),
                    width=_w("start_date", 110),
                    hide=("start_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_date",
                    header_map.get("end_date", "End"),
                    width=_w("end_date", 110),
                    hide=("end_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "timeframe" in df.columns:
            shared_col_defs.append(
                col_timeframe_pill(
                    field="timeframe",
                    header=header_map.get("timeframe", "Timeframe"),
                    width=_w("timeframe", 90),
                    filter=sym_filter,
                    hide=("timeframe" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "direction" in df.columns:
            shared_col_defs.append(
                col_direction_pill(
                    field="direction",
                    header=header_map.get("direction", "Direction"),
                    width=_w("direction", 100),
                    filter=sym_filter,
                    hide=("direction" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "avg_position_time" in df.columns:
            shared_col_defs.append(
                col_avg_position_time(
                    field="avg_position_time",
                    header=header_map.get("avg_position_time", "Avg Position Time"),
                    width=_w("avg_position_time", 150),
                    hide=("avg_position_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist" in df.columns:
            shared_col_defs.append(
                col_shortlist(
                    field="shortlist",
                    header="Shortlist",
                    editable=True,
                    width=_w("shortlist", 130),
                    hide=("shortlist" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist_note" in df.columns:
            shared_col_defs.append(
                col_shortlist_note(
                    field="shortlist_note",
                    header="Shortlist Note",
                    editable=True,
                    width=_w("shortlist_note", 180),
                    hide=("shortlist_note" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        apply_columns(gb, shared_col_defs, saved_widths=_runs_widths)

        if "run_id" in df.columns:
            gb.configure_column(
                "run_id",
                headerName=header_map.get("run_id", "Run ID"),
                width=_w("run_id", 180),
                hide=("run_id" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        if "created_at" in df.columns:
            gb.configure_column(
                "created_at",
                headerName=header_map.get("created_at", "Run Date"),
                width=_w("created_at", 120),
                hide=("created_at" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        if "duration" in df.columns:
            gb.configure_column(
                "duration",
                headerName=header_map.get("duration", "Duration"),
                width=_w("duration", 105),
                hide=("duration" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
                cellClass="ag-center-aligned-cell",
            )

        for col, width in (("symbol", 110), ("timeframe", 90), ("direction", 100), ("strategy", 160), ("sweep_name", 180)):
            if col in df.columns:
                if col in {"timeframe", "direction"}:
                    continue
                col_filter = sym_filter if col in {"symbol", "timeframe", "direction", "strategy"} else True
                if col == "strategy":
                    gb.configure_column(
                        col,
                        headerName=header_map.get("strategy", "Strategy"),
                        width=_w(col, width),
                        filter=col_filter,
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                elif col == "sweep_name":
                    gb.configure_column(
                        col,
                        headerName=header_map.get("sweep_name", "Sweep"),
                        width=_w(col, width),
                        filter=True,
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                else:
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, str(col).replace("_", " ").title()),
                        width=_w(col, width),
                        filter=col_filter,
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                        cellRenderer=asset_renderer if col == "symbol" else None,
                        valueGetter=asset_market_getter if col == "symbol" else None,
                        checkboxSelection=True if col == "symbol" else None,
                        headerCheckboxSelection=True if col == "symbol" else None,
                    )
                        )

        for col in ("net_return_pct", "max_drawdown_pct", "max_drawdown", "win_rate"):
            if col in df.columns:
                if col in {"max_drawdown_pct", "max_drawdown"}:
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, "Max Drawdown"),
                        valueFormatter=pct_formatter,
                        width=_w(col, 130),
                        cellStyle=dd_style,
                        type=["numericColumn"],
                        cellClass="ag-center-aligned-cell",
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                elif col == "win_rate":
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, "Win Rate"),
                        valueFormatter=pct_formatter,
                        width=_w(col, 120),
                        cellStyle=metrics_gradient_style,
                        type=["numericColumn"],
                        cellClass="ag-center-aligned-cell",
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                else:
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, "Net Return"),
                        valueFormatter=pct_formatter,
                        width=_w(col, 120),
                        cellStyle=metrics_gradient_style,
                        type=["numericColumn"],
                        cellClass="ag-center-aligned-cell",
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
        for col in ("net_profit", "sharpe", "sortino", "profit_factor", "cpc_index", "common_sense_ratio"):
            if col in df.columns:
                if col == "net_profit":
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, "Net Profit"),
                        valueFormatter=num2_formatter,
                        width=_w(col, 120),
                        cellStyle=metrics_gradient_style,
                        type=["numericColumn"],
                        cellClass="ag-center-aligned-cell",
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )
                else:
                    gb.configure_column(
                        col,
                        headerName=header_map.get(col, str(col).replace("_", " ").title()),
                        valueFormatter=num2_formatter,
                        width=_w(col, 140 if col == "common_sense_ratio" else 120),
                        cellStyle=metrics_gradient_style,
                        type=["numericColumn"],
                        cellClass="ag-center-aligned-cell",
                        hide=(col not in visible_cols),
                        wrapHeaderText=True,
                        autoHeaderHeight=True,
                    )


        if "actions" in df.columns:
            gb.configure_column(
                "actions",
                headerName="Actions",
                cellRenderer=actions_renderer,
                width=_w("actions", 130),
                minWidth=130,
                pinned="right",
                filter=False,
                sortable=False,
                resizable=False,
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )

        for hidden in (
            "asset",
            "symbol_full",
            "metadata_json",
            "sweep_exchange_id",
            "_exchange_id_for_cat",
            # Market is shown inside the Asset label; never show as a separate column.
            "market",
            "market_type",
            "strategy_name",
            "icon_uri",
            "config_json",
            "strategy_version",
            "start_time",
            "end_time",
            "sweep_id",
            "base_asset",
            "quote_asset",
            "create_bot_url",
            "action_click",
        ):
            if hidden in df.columns:
                gb.configure_column(hidden, hide=True)

        # Apply saved widths after dynamic columns are configured.
        _runs_saved_state = st.session_state.get("runs_explorer_columns_state")
        if isinstance(_runs_saved_state, list) and _runs_saved_state:
            try:
                width_map: dict[str, int] = {}
                for item in _runs_saved_state:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        width_map[str(col_id)] = int(width_val)
                    except Exception:
                        continue
                for col in df.columns.tolist():
                    if col in width_map:
                        gb.configure_column(col, width=width_map[col])
            except Exception:
                pass

        grid_options = gb.build()

        # Quartz-like dark theme (approx) using AG Grid CSS variables.
        # streamlit-aggrid doesn't expose ag-grid v33+ `themeQuartz` directly; it supports a small set
        # of bundled themes (e.g., "dark"). We use theme="dark" and then tune via CSS variables.
        custom_css = {
            # Apply variables at wrapper level so they cascade regardless of the bundled theme class.
            ".ag-root-wrapper": {
                "--ag-font-family": "Roboto,-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif",
                "--ag-font-size": "13px",
                "--ag-header-font-size": "14px",
                "--ag-row-height": "36px",
                "--ag-header-height": "40px",

                # Palette (match requested params)
                "--ag-background-color": "#1D2737",
                "--ag-odd-row-background-color": "#223046",
                "--ag-header-background-color": "#2A4569",
                "--ag-header-foreground-color": "#FFFFFF",
                "--ag-foreground-color": "#FFFFFF",
                "--ag-secondary-foreground-color": "#CBD5E1",

                # Lines + sizing
                "--ag-border-color": "#3B516F",
                "--ag-row-border-color": "#2D3F5B",
                "--ag-cell-horizontal-padding": "10px",
                "--ag-border-radius": "6px",

                # Interaction
                "--ag-row-hover-color": "rgba(59, 130, 246, 0.12)",
                "--ag-selected-row-background-color": "rgba(59, 130, 246, 0.18)",

                # Controls / panels
                "--ag-control-panel-background-color": "#1D2737",
                "--ag-menu-background-color": "#1D2737",
                "--ag-popup-background-color": "#1D2737",
                "--ag-popup-shadow": "0 8px 24px rgba(0,0,0,0.35)",
                "--ag-input-background-color": "#223046",
                "--ag-input-border-color": "#3B516F",
                "--ag-input-focus-border-color": "#60a5fa",

                "border": "1px solid #3B516F",
                "border-radius": "6px",
                "background-color": "#1D2737",
                "color": "#FFFFFF",
                "font-family": "Roboto,-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif",
            },
            ".ag-root-wrapper-body": {
                "background-color": "#1D2737",
            },
            ".ag-header": {
                "border-bottom": "1px solid rgba(255,255,255,0.10)",
                "background-color": "#2A4569",
            },
            ".ag-header-cell-label": {
                "font-weight": "500",
                "letter-spacing": "0.00em",
                "color": "#FFFFFF",
                "white-space": "normal",
                "line-height": "1.1",
            },
            ".ag-header-cell-text": {
                "color": "#FFFFFF",
                "white-space": "normal",
            },
            ".ag-header-cell": {
                "align-items": "center",
            },
            # Zebra striping (keep very subtle)
            ".ag-row:nth-child(even) .ag-cell": {
                "background-color": "#1F2B3F",
            },
            # Default cell padding + numeric readability
            ".ag-cell": {
                "padding-left": "10px",
                "padding-right": "10px",
                "font-variant-numeric": "tabular-nums",
                "color": "#FFFFFF",
            },
            # Actions column: tighter padding so icon buttons fit without a huge column.
            ".ag-cell[col-id='actions']": {"padding-left": "2px", "padding-right": "2px"},
            # Vertical centering (AG Grid nests content in wrappers)
            ".ag-cell-wrapper": {"align-items": "center", "height": "100%"},
            ".ag-cell-value": {"display": "flex", "align-items": "center", "height": "100%"},
            ".ag-row": {
                "background-color": "#223046",
            },
            ".ag-body-viewport": {
                # Hint to the browser for form controls/scrollbars in dark mode.
                "color-scheme": "dark",
                "background-color": "#1D2737",
            },
            ".ag-body": {"background-color": "#1D2737"},
            ".ag-center-cols-viewport": {"background-color": "#1D2737"},
            ".ag-center-cols-container": {"background-color": "#1D2737"},
            ".ag-overlay-no-rows-center": {"background-color": "#1D2737", "color": "#CBD5E1"},
            ".ag-overlay-loading-center": {"background-color": "#1D2737", "color": "#CBD5E1"},
            # Column menu + filter popups (match dark theme; some AG Grid parts ignore vars depending on bundled theme)
            ".ag-popup": {"color-scheme": "dark"},
            ".ag-popup-child": {
                "background-color": "#1D2737",
                "border": "1px solid #3B516F",
                "box-shadow": "0 8px 24px rgba(0,0,0,0.35)",
                "color": "#FFFFFF",
            },
            ".ag-menu": {
                "background-color": "#1D2737",
                "color": "#FFFFFF",
                "border": "1px solid #3B516F",
                "color-scheme": "dark",
            },
            ".ag-menu-option": {"color": "#FFFFFF"},
            ".ag-menu-option:hover": {"background-color": "rgba(59, 130, 246, 0.16)"},
            ".ag-filter": {"background-color": "#1D2737", "color": "#FFFFFF"},
            ".ag-filter-body": {"background-color": "#1D2737"},
            ".ag-filter-apply-panel": {"background-color": "#1D2737"},
            ".ag-input-field-input": {
                "background-color": "#223046",
                "border": "1px solid #3B516F",
                "color": "#FFFFFF",
            },
            ".ag-input-field-input:focus": {"border-color": "#60a5fa"},
            ".ag-picker-field-wrapper": {"background-color": "#223046", "border": "1px solid #3B516F"},
            ".ag-select": {"background-color": "#223046", "color": "#FFFFFF"},
            ".ag-right-aligned-cell": {"font-variant-numeric": "tabular-nums"},
            ".ag-center-aligned-cell": {"justify-content": "center"},
        }

        # Restore persisted column sizing/order. We sanitize out `hide` so the dropdown remains authoritative.
        saved_state = st.session_state.get("runs_explorer_columns_state")
        runtime_state = st.session_state.get("runs_explorer_columns_state_runtime")
        state_source = runtime_state if isinstance(runtime_state, list) and runtime_state else saved_state
        columns_state = None
        if isinstance(state_source, list) and state_source:
            try:
                sanitized: list[dict] = []
                widths_applied = 0
                for item in state_source:
                    if not isinstance(item, dict):
                        continue
                    d = dict(item)
                    d.pop("hide", None)
                    d.pop("headerName", None)
                    # Ensure Actions stays right-pinned (new column may be missing in older saved layouts).
                    col_id = d.get("colId") or d.get("col_id") or d.get("field")
                    if str(col_id) == "action_click":
                        continue
                    if str(col_id) == "actions":
                        d["pinned"] = "right"
                        d.pop("flex", None)
                        try:
                            d["width"] = max(130, int(d.get("width") or d.get("actualWidth") or 0))
                        except Exception:
                            d["width"] = 150
                        d["minWidth"] = max(130, int(d.get("minWidth") or 0))
                    if d.get("width") is None and d.get("actualWidth") is not None:
                        try:
                            d["width"] = int(d.get("actualWidth"))
                        except Exception:
                            pass
                    if d.get("width") is not None:
                        widths_applied += 1
                    sanitized.append(d)
                have_ids = {
                    str((x.get("colId") or x.get("col_id") or x.get("field") or "")).strip() for x in sanitized
                }
                if "actions" not in have_ids and "actions" in df.columns:
                    sanitized.append({"colId": "actions", "pinned": "right", "width": 150, "minWidth": 150})
                columns_state = sanitized or None
                if debug_grid:
                    debug_logger.info(
                        "runs_explorer.grid_state loaded key=%s cols=%s widths_applied=%s",
                        "ui.runs_explorer.columns_state",
                        len(sanitized),
                        widths_applied,
                    )
            except Exception:
                columns_state = None
        if debug_grid:
            debug_logger.info(
                "runs_explorer.grid_state final_payload=%s",
                json.dumps(columns_state, default=str) if columns_state else "null",
            )

        fit_columns = not bool(saved_state)
        grid = AgGrid(
            df,
            gridOptions=grid_options,
            # Sorting/filtering should be client-side (no rerun). Only selection + edits should rerun.
            update_mode=(
                GridUpdateMode.SELECTION_CHANGED
                | GridUpdateMode.VALUE_CHANGED
                | GridUpdateMode.MODEL_CHANGED
                | getattr(GridUpdateMode, "COLUMN_RESIZED", 0)
                | getattr(GridUpdateMode, "COLUMN_MOVED", 0)
                | getattr(GridUpdateMode, "COLUMN_VISIBLE", 0)
            ),
            data_return_mode=DataReturnMode.AS_INPUT,
            fit_columns_on_grid_load=fit_columns,
            allow_unsafe_jscode=True,
            enable_enterprise_modules=use_enterprise,
            theme="dark",
            custom_css=custom_css,
            height=630,
            key="grid_runs_explorer",
            pre_selected_rows=pre_selected,
            columns_state=columns_state,
        )

        # Store latest grid response for Save Layout (avoid stale column state).
        st.session_state["grid_resp_runs_explorer"] = grid

        # Persist runtime column sizing/order so widths survive sorting/filtering/selection.
        try:
            runtime_cols = _extract_aggrid_columns_state(grid)
            if isinstance(runtime_cols, list) and runtime_cols:
                sanitized_runtime: list[dict] = []
                valid_ids = {str(c) for c in df.columns.tolist()}
                for item in runtime_cols:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if col_id is None or str(col_id) not in valid_ids:
                        continue
                    if str(col_id) == "action_click":
                        continue
                    d = dict(item)
                    d.pop("hide", None)
                    d.pop("headerName", None)
                    if str(col_id) == "actions":
                        d["pinned"] = "right"
                        d.pop("flex", None)
                        try:
                            d["width"] = max(130, int(d.get("width") or d.get("actualWidth") or 0))
                        except Exception:
                            d["width"] = 150
                        d["minWidth"] = max(130, int(d.get("minWidth") or 0))
                    sanitized_runtime.append(d)
                if sanitized_runtime:
                    st.session_state["runs_explorer_columns_state_runtime"] = sanitized_runtime
        except Exception:
            pass

        pager_cols = st.columns([1, 1, 2, 2, 2])
        with pager_cols[0]:
            if st.button("← Prev", key="runs_explorer_prev", disabled=page <= 1):
                st.session_state["runs_explorer_page"] = max(1, page - 1)
                st.rerun()
        with pager_cols[1]:
            if st.button("Next →", key="runs_explorer_next", disabled=page >= max_page):
                st.session_state["runs_explorer_page"] = min(max_page, page + 1)
                st.rerun()
        with pager_cols[2]:
            st.caption(f"Page {page} / {max_page}")
        with pager_cols[3]:
            st.caption(f"Total rows: {total_count}")
        with pager_cols[4]:
            st.selectbox(
                "Page size",
                options=page_size_options,
                index=page_size_options.index(int(page_size)),
                key="runs_explorer_page_size",
                on_change=_reset_runs_explorer_page,
                label_visibility="collapsed",
            )

        st.markdown("---")
        st.caption("Runs Explorer settings")
        layout_bar = st.columns([2, 3], gap="large")
        with layout_bar[1]:
            st.caption("Layout")

            def _request_save_layout_runs() -> None:
                st.session_state["runs_explorer_save_layout_requested"] = True
                try:
                    st.toast("Saving layout…")
                except Exception:
                    pass

            st.button("Save layout", key="runs_explorer_save_layout", on_click=_request_save_layout_runs)

            if st.button("Reset layout", key="runs_explorer_reset_layout"):
                try:
                    with open_db_connection() as conn:
                        set_user_setting(conn, user_id, "ui.runs_explorer.columns_state", None)
                except Exception:
                    pass
                for k in (
                    "runs_explorer_columns_state",
                    "runs_explorer_columns_state_runtime",
                    "runs_explorer_columns_state_last_persisted",
                    "runs_explorer_columns_state_loaded",
                ):
                    st.session_state.pop(k, None)
                st.rerun()

            if str(os.environ.get("DRAGON_DEBUG") or "").strip() == "1":
                resp_dbg = st.session_state.get("grid_resp_runs_explorer") or {}
                has_state = bool(
                    resp_dbg.get("columnState")
                    or resp_dbg.get("columnsState")
                    or resp_dbg.get("column_state")
                    or resp_dbg.get("columns_state")
                )
                saved_count = len(saved_state) if isinstance(saved_state, list) else 0
                st.caption(
                    f"Debug: key=grid_runs_explorer • columnState={has_state} • saved={saved_count} • fit_columns_on_grid_load={fit_columns}"
                )

        # Persist column sizing/order only on explicit save (prevents resize loops).
        if st.session_state.get("runs_explorer_save_layout_requested"):
            try:
                resp = st.session_state.get("grid_resp_runs_explorer")
                new_state = _extract_aggrid_columns_state(resp)
                if isinstance(new_state, list) and new_state:
                    # Never persist internal plumbing columns.
                    sanitized_state: list[dict] = []
                    for item in new_state:
                        if not isinstance(item, dict):
                            continue
                        col_id = item.get("colId") or item.get("col_id") or item.get("field")
                        if str(col_id) == "action_click":
                            continue
                        d = dict(item)
                        d.pop("headerName", None)
                        if str(col_id) == "actions":
                            d["pinned"] = "right"
                            d.pop("flex", None)
                            try:
                                d["width"] = max(130, int(d.get("width") or d.get("actualWidth") or 0))
                            except Exception:
                                d["width"] = 150
                            d["minWidth"] = max(130, int(d.get("minWidth") or 0))
                        if "width" not in d:
                            try:
                                d["width"] = int(d.get("actualWidth") or 0) or d.get("width")
                            except Exception:
                                pass
                        sanitized_state.append(d)
                    with open_db_connection() as conn:
                        set_user_setting(conn, user_id, "ui.runs_explorer.columns_state", sanitized_state)
                    st.session_state["runs_explorer_columns_state"] = sanitized_state
                    st.session_state["runs_explorer_columns_state_last_persisted"] = sanitized_state
                    st.success("Layout saved.")
                else:
                    st.warning("No column state returned; ensure update_mode includes column events.")
            except Exception as exc:
                st.error(f"Failed to save layout: {exc}")
            finally:
                st.session_state.pop("runs_explorer_save_layout_requested", None)

        # Persist shortlist edits (diff against baseline to avoid writes on filter/sort changes)
        grid_data = grid.get("data")
        try:
            grid_df = pd.DataFrame(grid_data)
        except Exception:
            grid_df = df

        # Handle action clicks (reliable navigation vs iframe link hacks)
        try:
            token = ""

            # Prefer reading the token from the selected row payload (most reliable transport).
            try:
                sel_raw = grid.get("selected_rows")
                sel: list[dict[str, Any]] = []
                if sel_raw is None:
                    sel = []
                elif isinstance(sel_raw, list):
                    sel = sel_raw
                elif isinstance(sel_raw, pd.DataFrame):
                    sel = sel_raw.to_dict("records")
                else:
                    sel = list(sel_raw)
                if sel:
                    tok = str((sel[0] or {}).get("action_click") or "").strip()
                    if tok:
                        token = tok
            except Exception:
                pass

            if not grid_df.empty and "action_click" in grid_df.columns:
                tok_series = grid_df["action_click"].astype(str).map(lambda s: str(s or "").strip())
                non_empty = tok_series[tok_series != ""]
                if not non_empty.empty:
                    token = str(non_empty.iloc[0]).strip()

            if token:
                last = str(st.session_state.get("last_action_click_token") or "")
                if token != last:
                    st.session_state["last_action_click_token"] = token
                    action, rid = _parse_action_click_token(token)
                    if action == "create_bot" and rid:
                        st.session_state["create_live_bot_run_id"] = str(rid).strip()
                        st.session_state.pop("_create_live_bot_prefilled_for", None)
                        st.session_state.pop("selected_bot_id", None)
                        st.session_state["current_page"] = "Live Bots"
                        _update_query_params(page="Live Bots", create_live_bot_run_id=rid)
                        st.rerun()
                    if action == "open_sweep" and rid:
                        run = get_backtest_run(str(rid).strip())
                        sweep_id = None if not isinstance(run, dict) else run.get("sweep_id")
                        if sweep_id is None:
                            st.warning("This run is not part of a sweep.")
                        else:
                            st.session_state["current_page"] = "Sweeps"
                            st.session_state["deep_link_sweep_id"] = str(sweep_id).strip()
                            st.session_state["deep_link_run_id"] = str(rid).strip()
                            _update_query_params(page="Sweeps", sweep_id=str(sweep_id), run_id=str(rid).strip())
                            st.rerun()
        except Exception:
            pass

        baseline = st.session_state.get("runs_explorer_shortlist_baseline")
        if not isinstance(baseline, dict):
            baseline = {}
            if not df.empty and "run_id" in df.columns and "shortlist" in df.columns:
                for _, r in df.iterrows():
                    rid = str(r.get("run_id") or r.get("id") or r.get("runId") or "").strip()
                    if not rid:
                        continue
                    baseline[rid] = {
                        "shortlist": bool(r.get("shortlist")),
                        "note": str(r.get("shortlist_note") or "").strip(),
                    }
            st.session_state["runs_explorer_shortlist_baseline"] = baseline

        current_map: Dict[str, Dict[str, Any]] = {}
        if not grid_df.empty and "run_id" in grid_df.columns and "shortlist" in grid_df.columns:
            for _, r in grid_df.iterrows():
                rid = str(r.get("run_id") or "").strip()
                if not rid:
                    continue
                current_map[rid] = {
                    "shortlist": bool(r.get("shortlist")),
                    "note": str(r.get("shortlist_note") or ""),
                }

        changed_flags: Dict[str, bool] = {}
        changed_notes: Dict[str, str] = {}
        for rid, cur in current_map.items():
            old = baseline.get(rid)
            if old is None:
                continue
            if bool(old.get("shortlist")) != bool(cur.get("shortlist")) or str(old.get("note") or "") != str(cur.get("note") or ""):
                changed_flags[rid] = bool(cur.get("shortlist"))
                changed_notes[rid] = str(cur.get("note") or "")

        if changed_flags:
            try:
                for rid, flag in changed_flags.items():
                    update_run_shortlist_fields(
                        rid,
                        shortlist=bool(flag),
                        shortlist_note=str(changed_notes.get(rid) or ""),
                        user_id=user_id,
                    )
                    baseline[rid] = {
                        "shortlist": bool(flag),
                        "note": str(changed_notes.get(rid) or ""),
                    }
                st.session_state["runs_explorer_shortlist_baseline"] = baseline
            except Exception as exc:
                st.error(f"Failed to persist shortlist changes: {exc}")

        selected_rows_raw = grid.get("selected_rows")
        if selected_rows_raw is None:
            selected_rows: list[dict[str, Any]] = []
        elif isinstance(selected_rows_raw, list):
            selected_rows = selected_rows_raw
        elif isinstance(selected_rows_raw, pd.DataFrame):
            try:
                selected_rows = selected_rows_raw.to_dict("records")
            except Exception:
                selected_rows = []
        else:
            # Best-effort normalization for unexpected shapes.
            try:
                selected_rows = list(selected_rows_raw)
            except Exception:
                selected_rows = []

        if selected_rows:
            try:
                new_active = str(selected_rows[0].get("run_id") or "").strip()
            except Exception:
                new_active = ""
            if new_active and new_active != active_run_id:
                st.session_state["runs_explorer_active_run_id"] = new_active
                _update_query_params(run_id=new_active)
                active_run_id = new_active

        st.markdown("---")
        st.markdown("### Active run")
        if not active_run_id:
            st.info("Select at least one run in the grid to view details.")
            return

        run = get_backtest_run(active_run_id)
        if not run:
            st.warning(f"Run not found: {active_run_id}")
            return

        # Details header + icon
        icon_uri = None
        try:
            match = df[df["run_id"].astype(str) == str(active_run_id)]
            if not match.empty:
                icon_uri = match.iloc[0].get("icon_uri")
        except Exception:
            icon_uri = None

        hcols = st.columns([1, 6])
        with hcols[0]:
            if icon_uri:
                try:
                    st.markdown(
                        f'<img src="{icon_uri}" width="36" style="display:block;" />',
                        unsafe_allow_html=True,
                    )
                except Exception:
                    pass
        with hcols[1]:
            st.write(
                f"**{run.get('symbol')}** {run.get('timeframe')} · {run.get('strategy_name')} · run_id={run.get('id')}"
            )

        kpi = st.columns(6)
        kpi[0].metric(
            "Net Return (equity)",
            format_pct(_safe_float(run.get("net_return_pct")), assume_ratio_if_leq_1=True, decimals=0),
        )
        kpi[1].metric("Net Profit", f"{float(run.get('net_profit') or 0.0):.2f}")
        kpi[2].metric(
            "Max DD",
            format_pct(_safe_float(run.get("max_drawdown_pct")), assume_ratio_if_leq_1=True, decimals=0),
        )
        kpi[3].metric("Sharpe", f"{float(run.get('sharpe') or 0.0):.2f}")
        kpi[4].metric("Sortino", f"{float(run.get('sortino') or 0.0):.2f}")
        kpi[5].metric(
            "Win Rate",
            format_pct(_safe_float(run.get("win_rate")), assume_ratio_if_leq_1=True, decimals=0),
        )

        # Optional: leverage-aware ROI on margin (perps/futures), stored in metrics JSON.
        try:
            m = run.get("metrics") or {}
            roi_margin = float(m.get("roi_pct_on_margin")) if isinstance(m, dict) and m.get("roi_pct_on_margin") is not None else None
        except Exception:
            roi_margin = None
        if roi_margin is not None:
            st.caption(f"ROI (margin): {format_pct(roi_margin, assume_ratio_if_leq_1=False, decimals=0)}")

        trades_json = run.get("trades_json")
        trade_count = None
        try:
            if isinstance(trades_json, str) and trades_json.strip():
                obj = json.loads(trades_json)
                if isinstance(obj, list):
                    trade_count = len(obj)
        except Exception:
            trade_count = None
        if trade_count is not None:
            st.caption(f"Trades: {trade_count}")

        with st.expander("Metadata", expanded=False):
            st.json(run.get("metadata") or {})
        with st.expander("Metrics (raw)", expanded=False):
            st.json(run.get("metrics") or {})
        with st.expander("Config (raw)", expanded=False):
            st.json(run.get("config") or {})
        return

    if current_page == "Results":
        st.subheader("Stored backtest runs")
        render_active_filter_chips(get_global_filters())
        deep_link_run_id_val = st.session_state.get("deep_link_run_id")
        deep_link_run_id_int: Optional[int] = None
        if deep_link_run_id_val:
            try:
                deep_link_run_id_int = int(deep_link_run_id_val)
            except (TypeError, ValueError):
                deep_link_run_id_int = None
        if deep_link_run_id_int is not None:
            st.info(f"Deep link active: focusing run #{deep_link_run_id_int}.")
            if st.button("Clear run deep link", key="clear_run_link"):
                _update_query_params(run_id=None)
                st.session_state.pop("deep_link_run_id", None)
                st.rerun()

        # Results grid (AgGrid).
        if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
            st.error("Results requires `streamlit-aggrid`. Install dependencies and restart the app.")
            return

        def _reset_results_page() -> None:
            st.session_state["results_runs_page"] = 1

        if "results_runs_page" not in st.session_state:
            st.session_state["results_runs_page"] = 1
        if "results_runs_page_size" not in st.session_state:
            st.session_state["results_runs_page_size"] = 100
        if "results_runs_filter_model" not in st.session_state:
            st.session_state["results_runs_filter_model"] = None
        if "results_runs_sort_model" not in st.session_state:
            st.session_state["results_runs_sort_model"] = None

        page_size_options = [25, 50, 100, 200]
        page_size = int(st.session_state.get("results_runs_page_size") or page_size_options[2])
        if page_size not in page_size_options:
            page_size_options.append(page_size)
            page_size_options = sorted(set(page_size_options))
        page = int(st.session_state.get("results_runs_page") or 1)

        with open_db_connection() as _filter_conn:
            options_extra = _runs_global_filters_to_extra_where(get_global_filters())
            symbol_opts = list_backtest_run_symbols(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            if not symbol_opts:
                symbol_opts = list_backtest_run_symbols(_filter_conn, extra_where=None, user_id=user_id)
            tf_opts = list_backtest_run_timeframes(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            if not tf_opts:
                tf_opts = list_backtest_run_timeframes(_filter_conn, extra_where=None, user_id=user_id)
            strat_opts = list_backtest_run_strategies_filtered(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            if not strat_opts:
                strat_opts = list_backtest_run_strategies_filtered(_filter_conn, extra_where=None, user_id=user_id)
            bounds = get_runs_numeric_bounds(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            if not any(v is not None for v in bounds.values()):
                bounds = get_runs_numeric_bounds(_filter_conn, extra_where=None, user_id=user_id)

        ui_filters = _runs_server_filter_controls(
            "results_runs",
            include_run_type=True,
            on_change=_reset_results_page,
            symbol_options=symbol_opts,
            strategy_options=strat_opts,
            timeframe_options=tf_opts,
            bounds=bounds,
        )

        runs_query_cols = [
            "run_id",
            "created_at",
            "symbol",
            "timeframe",
            "strategy_name",
            "strategy_version",
            "market_type",
            "config_json",
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
            "sweep_id",
            "sweep_name",
            "sweep_exchange_id",
            "sweep_scope",
            "sweep_assets_json",
            "sweep_category_id",
            "base_asset",
            "quote_asset",
            "icon_uri",
            "shortlist",
            "shortlist_note",
        ]

        extra_where = _runs_global_filters_to_extra_where(get_global_filters())
        if ui_filters:
            extra_where.update(ui_filters)
        with _perf("results.load_rows"):
            with open_db_connection() as _runs_conn:
                rows, total_rows = list_runs_server_side(
                    conn=_runs_conn,
                    page=page,
                    page_size=page_size,
                    filter_model=st.session_state.get("results_runs_filter_model"),
                    sort_model=st.session_state.get("results_runs_sort_model"),
                    visible_columns=runs_query_cols,
                    user_id=user_id,
                    extra_where=extra_where if extra_where else None,
                )
        total_rows = int(total_rows or 0)
        max_page = max(1, math.ceil(total_rows / float(page_size))) if total_rows else 1
        if page > max_page:
            st.session_state["results_runs_page"] = max_page
            st.rerun()

        if st.checkbox("Show active filters", value=False, key="results_runs_show_active_filters"):
            st.json(_safe_json_for_display({"global": _runs_global_filters_to_extra_where(get_global_filters()), "ui": ui_filters}))

        if st.checkbox("Show grid state (debug)", value=False, key="results_runs_debug_grid_state"):
            st.info("Showing raw grid state for debugging filter/sort extraction.")

        if not rows:
            st.info("No backtest runs stored yet. Run a backtest to populate this list.")
            return

        # Reuse the same grid UX/styling (including persisted layout + column picker).
        st.session_state["current_page"] = "Results"  # keep sidebar state stable during reruns

        # Inline the grid plumbing and keep selection -> run_id deep-link.
        # Avoid external font loads here (they can make the page look like it “hangs”
        # in environments without reliable access to fonts.googleapis.com).
        # If Roboto is available locally, the browser will still use it via fallbacks.
        st.markdown(
            """
            <style>
            html, body, [class*="css"], .stApp { font-family: Roboto, system-ui, -apple-system, Segoe UI, Arial, sans-serif; }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Load per-user persisted Results grid preferences once per session.
        if not st.session_state.get("results_prefs_loaded", False):
            try:
                with open_db_connection() as conn:
                    saved_visible = get_user_setting(conn, user_id, "ui.results.visible_cols", default=None)
                    saved_col_state = get_user_setting(conn, user_id, "ui.results.columns_state", default=None)
                if isinstance(saved_visible, list) and saved_visible:
                    st.session_state["results_visible_cols"] = saved_visible
                    st.session_state["results_visible_cols_last_persisted"] = list(saved_visible)
                if isinstance(saved_col_state, list) and saved_col_state:
                    st.session_state["results_columns_state"] = saved_col_state
                    st.session_state["results_columns_state_last_persisted"] = saved_col_state
                    # Apply saved column state only once; after that, let the grid keep its own client-side state.
                    st.session_state["results_apply_saved_columns_state"] = True
            except Exception:
                pass
            st.session_state["results_prefs_loaded"] = True

        use_enterprise = bool(os.environ.get("AGGRID_ENTERPRISE"))

        # Prefer query-param run_id as initial active run
        qp_run_id = _extract_query_param(st.query_params, "run_id")
        if qp_run_id and "runs_explorer_active_run_id" not in st.session_state:
            st.session_state["runs_explorer_active_run_id"] = str(qp_run_id)

        with _perf("results.build_df"):
            df = pd.DataFrame(rows)

        _results_log = logging.getLogger(__name__)

        # Derive Start/End/Duration columns from stored run fields if present.
        try:
            if {"start_time", "end_time"}.issubset(set(df.columns)):
                sdt = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
                edt = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
                df["start_date"] = sdt.dt.date.astype(str).where(sdt.notna(), None)
                df["end_date"] = edt.dt.date.astype(str).where(edt.notna(), None)
                dur = (edt - sdt)

                def _fmt_td(x: Any) -> str:
                    try:
                        if x is None or pd.isna(x):
                            return ""
                        secs = float(getattr(x, "total_seconds", lambda: 0.0)())
                        if secs <= 0:
                            return ""
                        days = int(secs // 86400)
                        hours = int((secs % 86400) // 3600)
                        if days > 0:
                            return f"{days}d {hours}h" if hours else f"{days}d"
                        mins = int((secs % 3600) // 60)
                        return f"{hours}h {mins}m" if hours else f"{mins}m"
                    except Exception:
                        return ""

                df["duration"] = dur.map(_fmt_td)
        except Exception:
            _results_log.warning("Results prep: failed to derive start/end/duration", exc_info=True)
            if "start_date" not in df.columns:
                df["start_date"] = ""
            if "end_date" not in df.columns:
                df["end_date"] = ""
            if "duration" not in df.columns:
                df["duration"] = ""

        if "direction" not in df.columns:
            try:
                if "config_json" in df.columns:
                    df["direction"] = df["config_json"].apply(infer_trade_direction_label)
                else:
                    df["direction"] = ""
            except Exception:
                _results_log.warning("Results prep: failed to infer direction", exc_info=True)
                df["direction"] = ""

        if "avg_position_time" not in df.columns:
            df["avg_position_time"] = "—"

        # Drop large JSON payloads early. Even if the columns are hidden in the grid,
        # AgGrid still receives the full dataset which can cause very slow browser-side
        # rendering for large run lists.
        df = df.drop(columns=["config_json", "sweep_assets_json"], errors="ignore")

        if "shortlist" in df.columns:
            df["shortlist"] = df["shortlist"].astype(bool)
        # Keep icon_uri so the Results grid can show asset icons.
        if "icon_uri" not in df.columns:
            df["icon_uri"] = ""

        if "asset" not in df.columns:
            base = df.get("base_asset") if "base_asset" in df.columns else None
            if base is not None:
                df["asset"] = base.fillna("").astype(str).str.upper().replace({"NAN": ""})
            else:
                df["asset"] = ""

        # (do not drop icon_uri)

        if "symbol" in df.columns:
            try:
                df["symbol_full"] = df["symbol"].fillna("").astype(str)
            except Exception:
                _results_log.warning("Results prep: failed to normalize symbol_full", exc_info=True)
                try:
                    df["symbol_full"] = df["symbol"].astype(str)
                except Exception:
                    df["symbol_full"] = ""
        else:
            df["symbol_full"] = ""

        if "asset_display" not in df.columns:
            try:
                df["asset_display"] = df["symbol"].fillna("").astype(str)
            except Exception:
                _results_log.warning("Results prep: failed to build asset_display", exc_info=True)
                df["asset_display"] = df.get("symbol", "")

        # Category should reflect the sweep's category (not global membership):
        # only populate for multi-asset category sweeps.
        if "category" not in df.columns:
            df["category"] = ""
        try:
            if {"sweep_scope", "sweep_category_id", "sweep_exchange_id"}.issubset(set(df.columns)):
                mask = (
                    df["sweep_scope"].fillna("").astype(str).str.strip().eq("multi_asset_category")
                    & df["sweep_category_id"].notna()
                )
                if mask.any():
                    df_cat = df.loc[mask, ["sweep_exchange_id", "sweep_category_id"]].copy()
                    pairs: list[tuple[str, int]] = []
                    for row in df_cat.itertuples(index=False):
                        ex_id = str(getattr(row, "sweep_exchange_id", "") or "").strip()
                        if not ex_id:
                            continue
                        try:
                            cat_id = int(getattr(row, "sweep_category_id", None))
                        except Exception:
                            continue
                        if cat_id <= 0:
                            continue
                        pairs.append((ex_id, cat_id))
                    pairs = sorted(set(pairs))
                    if pairs:
                        placeholders = ",".join(["(?, ?)"] * len(pairs))
                        params: list[Any] = [str(user_id or "").strip() or "admin@local"]
                        for ex_id, cat_id in pairs:
                            params.extend([ex_id, cat_id])
                        with open_db_connection() as _cat_conn:
                            rows_cat = _cat_conn.execute(
                                f"SELECT exchange_id, id, name FROM asset_categories WHERE user_id = ? AND (exchange_id, id) IN ({placeholders})",
                                tuple(params),
                            ).fetchall()
                        id_to_name: dict[tuple[str, int], str] = {}
                        for r in rows_cat or []:
                            try:
                                ex_id = str(r[0] or "").strip()
                                cat_id = int(r[1])
                                name = str(r[2] or "").strip()
                                if ex_id and cat_id > 0:
                                    id_to_name[(ex_id, cat_id)] = name
                            except Exception:
                                continue
                        if id_to_name:
                            def _lookup_category(ex_id: Any, cat_id: Any) -> str:
                                try:
                                    ex_norm = str(ex_id or "").strip()
                                    cid = int(cat_id)
                                except Exception:
                                    return ""
                                return id_to_name.get((ex_norm, cid), "")

                            df.loc[mask, "category"] = df.loc[mask].apply(
                                lambda r: _lookup_category(r.get("sweep_exchange_id"), r.get("sweep_category_id")),
                                axis=1,
                            )
        except Exception:
            _results_log.warning("Results prep: failed to map categories", exc_info=True)

        # Remove internal/raw columns from the Results page.
        # Keep them long enough to derive `category`, then drop.
        df = df.drop(columns=["metadata_json", "sweep_exchange_id", "sweep_category_id"], errors="ignore")

        st.caption(f"{total_rows} rows matched")

        if "market_type" in df.columns and "market" not in df.columns:
            df["market"] = df["market_type"]
        if "strategy_name" in df.columns and "strategy" not in df.columns:
            df["strategy"] = df["strategy_name"]
        if "market" in df.columns:
            try:
                m = df["market"].fillna("").astype(str).str.strip().str.lower()
                m = m.replace({"perp": "perps", "futures": "perps", "future": "perps"})
                df["market"] = m.map({"perps": "PERPS", "spot": "SPOT"}).fillna(m.str.upper())
            except Exception:
                pass
        if "sweep_name" in df.columns:
            try:
                df["sweep_name"] = df["sweep_name"].fillna("").astype(str)
            except Exception:
                pass

        # Remove sweep-internal columns from the Sweeps > Runs grid.
        # These can otherwise show up as default columns even if we don't include them in the column picker.
        df = df.drop(columns=["sweep_exchange_id", "sweep_scope", "sweep_category_id"], errors="ignore")

        preferred = [
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "avg_position_time",
            "symbol",
            "timeframe",
            "direction",
            "strategy",
            "sweep_name",
        ]
        ordered_cols: List[str] = []
        for c in preferred:
            if c in df.columns and c not in ordered_cols:
                ordered_cols.append(c)
        for c in df.columns.tolist():
            if c in ordered_cols:
                continue
            ordered_cols.append(c)
        df = df[ordered_cols]

        # Limit payload: only send columns we use on the Results grid.
        # AgGrid receives the full dataset even for hidden columns.
        grid_allowlist = [
            "run_id",
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "symbol",
            "symbol_full",
            "icon_uri",
            "asset_display",
            "timeframe",
            "direction",
            "strategy",
            "sweep_name",
            "category",
            "market",
            "net_return_pct",
            "roi_pct_on_margin",
            "max_drawdown_pct",
            "win_rate",
            "net_profit",
            "sharpe",
            "sortino",
            "profit_factor",
            "cpc_index",
            "common_sense_ratio",
            "avg_position_time",
            "shortlist",
            "shortlist_note",
        ]
        df = df[[c for c in grid_allowlist if c in df.columns]]

        # Belt-and-suspenders: keep sweep-internal columns out of the grid even if present
        # in the underlying query result.
        df = df.drop(columns=["sweep_exchange_id", "sweep_scope", "sweep_category_id"], errors="ignore")

        always_visible_cols = [c for c in ("symbol",) if c in df.columns]

        # Candidate columns should be the exact payload sent to AgGrid (even if hidden).
        candidate_cols = df.columns.tolist()

        # Keep internal/helper columns in the payload, but hide them by default.
        _default_hidden_cols = {"run_id", "icon_uri", "symbol_full", "market", "asset_display"}
        default_visible = st.session_state.get("results_visible_cols")
        if not isinstance(default_visible, list) or not default_visible:
            default_visible = [c for c in candidate_cols if c not in _default_hidden_cols]
        try:
            default_visible = [c for c in default_visible if c in candidate_cols]
        except Exception:
            default_visible = [c for c in candidate_cols if c not in _default_hidden_cols]
        for c in always_visible_cols:
            if c not in default_visible:
                default_visible.append(c)

        # If the user has interacted with the Visible columns popover, those checkbox values
        # live in session_state. Read them here so the grid reflects the changes immediately
        # on the same rerun (the popover UI is rendered later on the page).
        try:
            picker_cols = list(candidate_cols)
            any_picker_key = any(f"results_col_{c}" in st.session_state for c in picker_cols)
            if any_picker_key:
                chosen_from_picker = [
                    c for c in picker_cols if bool(st.session_state.get(f"results_col_{c}", False))
                ]
                for c in always_visible_cols:
                    if c not in chosen_from_picker:
                        chosen_from_picker.append(c)
                st.session_state["results_visible_cols"] = list(chosen_from_picker)
        except Exception:
            pass

        date_comparator = JsCode(
            """
            function(a, b) {
                try {
                    const at = new Date(a).getTime();
                    const bt = new Date(b).getTime();
                    if (!isFinite(at) && !isFinite(bt)) return 0;
                    if (!isFinite(at)) return -1;
                    if (!isFinite(bt)) return 1;
                    return at - bt;
                } catch (e) {
                    return 0;
                }
            }
            """
        )

        # Determine visible columns from session state; UI control is rendered at bottom.
        raw_visible = st.session_state.get("results_visible_cols")
        try:
            if isinstance(raw_visible, list) and "avg_position_time_s" in raw_visible:
                raw_visible = ["avg_position_time" if c == "avg_position_time_s" else c for c in raw_visible]
                st.session_state["results_visible_cols"] = list(raw_visible)
        except Exception:
            pass
        if isinstance(raw_visible, list) and raw_visible:
            sanitized_visible = [c for c in raw_visible if c in candidate_cols]
            if sanitized_visible != raw_visible:
                st.session_state["results_visible_cols"] = sanitized_visible
            chosen_visible = sanitized_visible
        else:
            chosen_visible = [c for c in default_visible if c in candidate_cols]
        for c in always_visible_cols:
            if c not in chosen_visible:
                chosen_visible.append(c)

        # Ensure newly-added metric columns show up even if the user has an older persisted layout.
        if "roi_pct_on_margin" in candidate_cols and "roi_pct_on_margin" not in chosen_visible:
            chosen_visible.append("roi_pct_on_margin")
        if "max_drawdown_pct" in candidate_cols and "max_drawdown_pct" not in chosen_visible:
            chosen_visible.append("max_drawdown_pct")
        visible_cols = chosen_visible

        # JS renderers/styles (shared helpers)
        asset_market_getter = asset_market_getter_js(
            symbol_field="symbol",
            market_field="market",
            base_asset_field="asset_display",
            asset_field="asset_display",
        )
        asset_renderer = asset_renderer_js(icon_field="icon_uri", size_px=18, text_color="#FFFFFF")

        roi_pct_formatter = JsCode(
            """
            function(params) {
                if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                const v = Number(params.value);
                if (!isFinite(v)) { return ''; }
                // Preserve sign for small values (e.g. -0.4%) and avoid "-0" collapsing to "0".
                if (Math.abs(v) < 1.0) {
                    return v.toFixed(1) + '%';
                }
                return Math.round(v).toString() + '%';
            }
            """
        )
        def _escape_js_template_literal_results(s: str) -> str:
            return (s or "").replace("`", "\\`").replace("${", "\\${")

        def _tabler_svg_safe_results(name: str, fallback: str) -> str:
            try:
                return get_tabler_svg(name)
            except Exception:
                return get_tabler_svg(fallback)

        # Results grid action icons:
        # - Re-Backtest: refresh
        # - Open Sweep: sweeps
        # - Create bot: robot (fallback to plus)
        play_svg_js_results = _escape_js_template_literal_results(get_tabler_svg(TABLER_ICONS["refresh"]))
        plus_svg_js_results = _escape_js_template_literal_results(
            _tabler_svg_safe_results("robot", TABLER_ICONS["create"])
        )
        folder_svg_js_results = _escape_js_template_literal_results(get_tabler_svg(TABLER_ICONS["sweeps"]))

        actions_js_results = (
            """
            class ActionsRenderer {
                init(params) {
                    const wrap = document.createElement('div');
                    wrap.style.display = 'flex';
                    wrap.style.gap = '4px';
                    wrap.style.justifyContent = 'flex-start';
                    wrap.style.alignItems = 'center';
                    wrap.style.width = '100%';
                    wrap.style.pointerEvents = 'auto';

                    const btnSize = '34px';
                    const playSvg = `__PLAY_SVG__`;
                    const plusSvg = `__PLUS_SVG__`;
                    const folderSvg = `__FOLDER_SVG__`;

                    function setClick(action) {
                        try {
                            const rid = (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null)
                                ? params.data.run_id.toString()
                                : '';
                            if (!rid) { return; }
                            const token = `${action}:${rid}:${Date.now()}`;
                            if (params && params.node && params.node.setDataValue) {
                                params.node.setDataValue('action_click', token);
                            }
                        } catch (err) {}
                    }

                    function mk(svg, title, action, bg, disabled) {
                        const b = document.createElement('button');
                        b.type = 'button';
                        b.innerHTML = svg;
                        b.title = title;
                        b.setAttribute('aria-label', title);
                        b.style.display = 'inline-flex';
                        b.style.alignItems = 'center';
                        b.style.justifyContent = 'center';
                        b.style.width = btnSize;
                        b.style.height = btnSize;
                        b.style.minWidth = btnSize;
                        b.style.minHeight = btnSize;
                        b.style.borderRadius = '6px';
                        b.style.border = '1px solid rgba(255,255,255,0.18)';
                        b.style.background = bg;
                        b.style.color = '#FFFFFF';
                        b.style.cursor = disabled ? 'not-allowed' : 'pointer';
                        b.style.padding = '0';
                        b.style.lineHeight = btnSize;
                        b.style.boxShadow = 'none';
                        b.style.outline = 'none';
                        b.style.userSelect = 'none';
                        if (disabled) {
                            b.style.opacity = '0.35';
                            b.disabled = true;
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                            };
                        } else {
                            b.disabled = false;
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                                setClick(action);
                            };
                        }
                        return b;
                    }

                    const hasRunId = (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null);
                    if (hasRunId) {
                        const sid = (params && params.data && params.data.sweep_id !== undefined && params.data.sweep_id !== null)
                            ? params.data.sweep_id.toString()
                            : '';
                        if (sid) {
                            wrap.appendChild(mk(folderSvg, 'Open sweep', 'open_sweep', 'rgba(148,163,184,0.16)', false));
                        } else {
                            wrap.appendChild(mk(folderSvg, 'Not part of a sweep', 'open_sweep', 'rgba(148,163,184,0.10)', true));
                        }
                        wrap.appendChild(mk(plusSvg, 'Create bot', 'create_bot', 'rgba(46,160,67,0.18)', false));
                    }

                    this.eGui = wrap;
                }
                getGui() { return this.eGui; }
                refresh(params) { return false; }
            }
            """
        )

        actions_js_results = actions_js_results.replace("__PLAY_SVG__", play_svg_js_results)
        actions_js_results = actions_js_results.replace("__PLUS_SVG__", plus_svg_js_results)
        actions_js_results = actions_js_results.replace("__FOLDER_SVG__", folder_svg_js_results)
        actions_renderer = JsCode(actions_js_results)

        dd_style = JsCode(
            """
            function(params) {
                const v = Number(params.value);
                if (!isFinite(v)) { return {}; }
                const t = Math.max(0.0, Math.min(1.0, v / 0.30));
                const alpha = 0.10 + (0.25 * t);
                return { color: '#ff7b72', backgroundColor: `rgba(248,81,73,${alpha})`, textAlign: 'center' };
            }
            """
        )

        metrics_gradient_style = JsCode(_aggrid_metrics_gradient_style_js())

        pct_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
                            const shown = (Math.abs(v) <= 1.0) ? (v * 100.0) : v;
                                                        return shown.toFixed(0) + '%';
            }
            """
        )
        num2_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
              return v.toFixed(2);
            }
            """
        )
        def _escape_js_template_literal3(s: str) -> str:
            return (s or "").replace("`", "\\`").replace("${", "\\${")

        def _tabler_svg_safe3(name: str, fallback: str) -> str:
            try:
                return get_tabler_svg(name)
            except Exception:
                return get_tabler_svg(fallback)

        play_svg_js3 = _escape_js_template_literal3(get_tabler_svg(TABLER_ICONS["refresh"]))
        plus_svg_js3 = _escape_js_template_literal3(_tabler_svg_safe3("robot", TABLER_ICONS["create"]))
        folder_svg_js3 = _escape_js_template_literal3(get_tabler_svg(TABLER_ICONS["sweeps"]))

        actions_js3 = (
                        """
                        class ActionsRenderer {
                                init(params) {
                                        const wrap = document.createElement('div');
                                        wrap.style.display = 'flex';
                                        wrap.style.gap = '4px';
                                        wrap.style.justifyContent = 'flex-start';
                                        wrap.style.alignItems = 'center';
                                        wrap.style.width = '100%';

                                        wrap.style.pointerEvents = 'auto';
                                        const btnSize = '34px';

                                        const playSvg = `__PLAY_SVG__`;
                                        const plusSvg = `__PLUS_SVG__`;
                                        const folderSvg = `__FOLDER_SVG__`;

                    function setClick(action) {
                        try {
        )
        actions_js3 = actions_js3.replace("__PLAY_SVG__", play_svg_js3)
        actions_js3 = actions_js3.replace("__PLUS_SVG__", plus_svg_js3)
        actions_js3 = actions_js3.replace("__FOLDER_SVG__", folder_svg_js3)
        actions_renderer = JsCode(actions_js3)
                                ? params.data.run_id.toString()
                                : '';
                            if (!rid) { return; }
                            const token = `${action}:${rid}:${Date.now()}`;
                            if (params && params.node && params.node.setDataValue) {
                                params.node.setDataValue('action_click', token);
                            }
                        } catch (err) {}
                    }

                    function mk(svg, title, action, bg, disabled) {
                        disabled = (disabled === true);
                        const b = document.createElement('button');
                        b.type = 'button';
                        b.innerHTML = svg;
                        b.title = title;
                        b.setAttribute('aria-label', title);
                        b.style.display = 'inline-flex';
                        b.style.alignItems = 'center';
                        b.style.justifyContent = 'center';
                        b.style.width = btnSize;
                        b.style.height = btnSize;
                        b.style.minWidth = btnSize;
                        b.style.minHeight = btnSize;
                        b.style.borderRadius = '6px';
                        b.style.border = '1px solid rgba(255,255,255,0.18)';
                        b.style.background = bg;
                        b.style.color = '#FFFFFF';
                        b.style.cursor = disabled ? 'not-allowed' : 'pointer';
                        b.style.padding = '0';
                        b.style.lineHeight = btnSize;
                        b.style.boxShadow = 'none';
                        b.style.outline = 'none';
                        b.style.userSelect = 'none';
                        if (disabled) {
                            b.style.opacity = '0.35';
                            b.disabled = true;
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                            };
                        } else {
                            b.disabled = false;
                            b.onclick = (e) => {
                                try { e.preventDefault(); e.stopPropagation(); } catch (err) {}
                                setClick(action);
                            };
                        }
                        return b;
                    }

                    const hasRunId = (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null);
                    if (hasRunId) {
                        const sid = (params && params.data && params.data.sweep_id !== undefined && params.data.sweep_id !== null)
                            ? params.data.sweep_id.toString()
                            : '';
                        if (sid) {
                            wrap.appendChild(mk(folderSvg, 'Open sweep', 'open_sweep', 'rgba(148,163,184,0.16)'));
                        } else {
                            wrap.appendChild(mk(folderSvg, 'Not part of a sweep', 'open_sweep', 'rgba(148,163,184,0.10)', true));
                        }
                        wrap.appendChild(mk(plusSvg, 'Create bot', 'create_bot', 'rgba(46,160,67,0.18)'));
                    }

                    this.eGui = wrap;
                }
                getGui() { return this.eGui; }
                refresh(params) { return false; }
            }
            """
        )

        # Shared AG Grid dark theme CSS
        custom_css = _aggrid_dark_custom_css()

        # Presentation-only: render date columns as local strings before sending to AGGrid.
        # This avoids JS Date parsing and keeps sorting stable.
        # Keep Start/End columns as datetime/ISO so sorting/filtering remains correct.
        try:
            tz = _ui_display_tz()
            if "created_at" in df.columns:
                df["created_at"] = [_ui_local_dt_ampm(v, tz) for v in df["created_at"].tolist()]
        except Exception:
            pass

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(filter=True, sortable=True, resizable=True)
        # IMPORTANT: `use_checkbox=True` creates a separate selection column.
        # We want the selection checkbox to live only inside the Asset column.
        gb.configure_selection("single", use_checkbox=False)
        gb.configure_grid_options(headerHeight=58, rowHeight=38, suppressRowHoverHighlight=False, animateRows=False)

        sym_filter = "agSetColumnFilter" if use_enterprise else True

        saved_state = st.session_state.get("results_columns_state")
        _results_widths: dict[str, int] = {}
        try:
            # Always seed widths from persisted state (Sweeps-style).
            if isinstance(saved_state, list) and saved_state:
                for item in saved_state:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        _results_widths[str(col_id)] = int(width_val)
                    except Exception:
                        continue
        except Exception:
            pass

        def _res_w(col: str, default: int) -> int:
            try:
                return max(60, int(_results_widths.get(str(col), default)))
            except Exception:
                return default

        shared_col_defs: list[dict[str, Any]] = []
        if "start_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_date",
                    col_label("start_date"),
                    width=_res_w("start_date", 110),
                    filter=True,
                    comparator=date_comparator,
                    hide=("start_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_date",
                    col_label("end_date"),
                    width=_res_w("end_date", 110),
                    filter=True,
                    comparator=date_comparator,
                    hide=("end_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "start_time" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_time",
                    col_label("start_time"),
                    width=_res_w("start_time", 130),
                    filter=True,
                    comparator=date_comparator,
                    hide=("start_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_time" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_time",
                    col_label("end_time"),
                    width=_res_w("end_time", 130),
                    filter=True,
                    comparator=date_comparator,
                    hide=("end_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "timeframe" in df.columns:
            shared_col_defs.append(
                col_timeframe_pill(
                    field="timeframe",
                    header=col_label("timeframe"),
                    width=_res_w("timeframe", 90),
                    filter=sym_filter,
                    hide=("timeframe" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "direction" in df.columns:
            shared_col_defs.append(
                col_direction_pill(
                    field="direction",
                    header=col_label("direction"),
                    width=_res_w("direction", 100),
                    filter=sym_filter,
                    hide=("direction" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "avg_position_time" in df.columns:
            shared_col_defs.append(
                col_avg_position_time(
                    field="avg_position_time",
                    header=col_label("avg_position_time"),
                    width=_res_w("avg_position_time", 150),
                    hide=("avg_position_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist" in df.columns:
            shared_col_defs.append(
                col_shortlist(
                    field="shortlist",
                    header=col_label("shortlist"),
                    editable=True,
                    width=_res_w("shortlist", 105),
                    hide=("shortlist" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "shortlist_note" in df.columns:
            shared_col_defs.append(
                col_shortlist_note(
                    field="shortlist_note",
                    header=col_label("shortlist_note"),
                    editable=True,
                    width=_res_w("shortlist_note", 200),
                    hide=("shortlist_note" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        apply_columns(gb, shared_col_defs, saved_widths=_results_widths)

        gb.configure_column(
            "symbol",
            headerName=col_label("symbol"),
            width=_res_w("symbol", 220),
            filter=sym_filter,
            hide=("symbol" not in visible_cols),
            wrapHeaderText=True,
            autoHeaderHeight=True,
            cellRenderer=asset_renderer,
            valueGetter=asset_market_getter,
            checkboxSelection=True,
            headerCheckboxSelection=False,
        )
        if "created_at" in df.columns:
            gb.configure_column(
                "created_at",
                headerName=col_label("created_at"),
                width=_res_w("created_at", 120),
                hide=("created_at" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )

        if "duration" in df.columns:
            gb.configure_column(
                "duration",
                headerName=col_label("duration"),
                width=_res_w("duration", 110),
                filter=False,
                hide=("duration" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
                cellClass="ag-center-aligned-cell",
            )
        if "strategy" in df.columns:
            gb.configure_column(
                "strategy",
                headerName=col_label("strategy"),
                width=_res_w("strategy", 160),
                filter=sym_filter,
                hide=("strategy" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        if "sweep_name" in df.columns:
            gb.configure_column(
                "sweep_name",
                headerName=col_label("sweep_name"),
                width=_res_w("sweep_name", 180),
                filter=True,
                hide=("sweep_name" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )

        # Numeric columns formatting/styling (best-effort)
        for col in ("net_return_pct", "max_drawdown_pct", "win_rate"):
            if col in df.columns:
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    valueFormatter=pct_formatter,
                    width=_res_w(col, 130 if col == "max_drawdown_pct" else 120),
                    cellStyle=dd_style if col == "max_drawdown_pct" else metrics_gradient_style,
                    type=["numericColumn"],
                    cellClass="ag-center-aligned-cell",
                    hide=(col not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )

        if "roi_pct_on_margin" in df.columns:
            gb.configure_column(
                "roi_pct_on_margin",
                headerName=col_label("roi_pct_on_margin"),
                valueFormatter=roi_pct_formatter,
                width=_res_w("roi_pct_on_margin", 140),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("roi_pct_on_margin" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        for col in ("net_profit", "sharpe", "sortino", "profit_factor", "cpc_index", "common_sense_ratio"):
            if col in df.columns:
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    valueFormatter=num2_formatter,
                    width=_res_w(col, 120),
                    cellStyle=metrics_gradient_style,
                    type=["numericColumn"],
                    cellClass="ag-center-aligned-cell",
                    hide=(col not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )


        # Ensure the Cols picker controls visibility for *all* candidate columns.
        # GridOptionsBuilder.from_dataframe() creates default column defs for every df column;
        # without explicitly setting `hide`, extra columns would remain visible regardless of picker state.
        _custom_configured_cols = {
            "symbol",
            "created_at",
            "start_date",
            "end_date",
            "start_time",
            "end_time",
            "duration",
            "timeframe",
            "direction",
            "strategy",
            "sweep_name",
            "net_return_pct",
            "roi_pct_on_margin",
            "max_drawdown_pct",
            "win_rate",
            "net_profit",
            "sharpe",
            "sortino",
            "profit_factor",
            "cpc_index",
            "common_sense_ratio",
            "avg_position_time",
            "shortlist",
            "shortlist_note",
        }
        try:
            for col in candidate_cols:
                if col in _custom_configured_cols:
                    continue
                if col not in df.columns:
                    continue
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    hide=(col not in visible_cols),
                    width=_res_w(col, 120),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
        except Exception:
            pass

        # Hide truly-internal columns (not part of payload allowlist, but keep defensive).
        for hidden in ("start_time", "end_time", "avg_position_time_seconds"):
            if hidden in df.columns:
                gb.configure_column(hidden, hide=True)

        # Apply saved widths after dynamic columns are configured.
        if isinstance(saved_state, list) and saved_state:
            try:
                width_map: dict[str, int] = {}
                for item in saved_state:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        width_map[str(col_id)] = int(width_val)
                    except Exception:
                        continue
                for col in df.columns.tolist():
                    if col in width_map:
                        gb.configure_column(col, width=width_map[col])
            except Exception:
                pass

        grid_options = gb.build()

        # Force friendly headers even if the client regenerates columnDefs.
        # This also prevents any stored grid state from "bringing back" raw db field names.
        try:
            header_value_map = {str(c): col_label(c) for c in df.columns.tolist()}
            if "actions" in df.columns:
                header_value_map["actions"] = col_label("actions")
            grid_options.setdefault("defaultColDef", {})
            grid_options["defaultColDef"]["headerValueGetter"] = JsCode(
                f"""
                function(params) {{
                    try {{
                        const m = {json.dumps(header_value_map)};
                        const cd = (params && params.colDef) ? params.colDef : {{}};
                        const key = (cd.field || cd.colId || '');
                        if (key && m[key]) return m[key];
                        if (cd.headerName) return cd.headerName;
                        return key || '';
                    }} catch (e) {{
                        return '';
                    }}
                }}
                """
            )
        except Exception:
            pass

        # Restore persisted column sizing/order (sanitized) on every render (Sweeps-style).
        columns_state = None
        if isinstance(saved_state, list) and saved_state:
            try:
                sanitized: list[dict] = []
                valid_ids = {str(c) for c in df.columns.tolist()}
                for item in saved_state:
                    if not isinstance(item, dict):
                        continue
                    d = dict(item)
                    d.pop("hide", None)
                    d.pop("headerName", None)
                    col_id = d.get("colId") or d.get("col_id") or d.get("field")
                    if str(col_id) in {"action_click", "actions"}:
                        continue
                    if col_id is None or str(col_id) not in valid_ids:
                        continue
                    sanitized.append(d)
                columns_state = sanitized or None
            except Exception:
                columns_state = None

        grid = AgGrid(
            df,
            gridOptions=grid_options,
            theme="dark",
            custom_css=custom_css,
            # Keep reruns tight: selection (open run) + value changes (shortlist edits).
            update_mode=(
                GridUpdateMode.SELECTION_CHANGED
                | GridUpdateMode.VALUE_CHANGED
                | GridUpdateMode.MODEL_CHANGED
                | getattr(GridUpdateMode, "COLUMN_RESIZED", 0)
                | getattr(GridUpdateMode, "COLUMN_MOVED", 0)
                | getattr(GridUpdateMode, "COLUMN_VISIBLE", 0)
            ),
            data_return_mode=DataReturnMode.AS_INPUT,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            height=630,
            key="results_runs_grid_v2",
            columns_state=columns_state,
        )

        # Store latest grid response for Save Layout (avoid stale column state).
        st.session_state["grid_resp_results"] = grid

        # Persist runtime column sizing/order so widths survive sorting/filtering/selection.
        try:
            runtime_cols = _extract_aggrid_columns_state(grid)
            if isinstance(runtime_cols, list) and runtime_cols:
                sanitized_runtime: list[dict] = []
                valid_ids = {str(c) for c in df.columns.tolist()}
                for item in runtime_cols:
                    if not isinstance(item, dict):
                        continue
                    d = dict(item)
                    d.pop("hide", None)
                    col_id = d.get("colId") or d.get("col_id") or d.get("field")
                    if col_id is None or str(col_id) not in valid_ids:
                        continue
                    sanitized_runtime.append(d)
                if sanitized_runtime:
                    st.session_state["results_runs_columns_state_runtime"] = sanitized_runtime
        except Exception:
            pass

        pager_cols = st.columns([1, 1, 2, 2, 2])
        with pager_cols[0]:
            if st.button("← Prev", key="results_runs_prev", disabled=page <= 1):
                st.session_state["results_runs_page"] = max(1, page - 1)
                st.rerun()
        with pager_cols[1]:
            if st.button("Next →", key="results_runs_next", disabled=page >= max_page):
                st.session_state["results_runs_page"] = min(max_page, page + 1)
                st.rerun()
        with pager_cols[2]:
            st.caption(f"Page {page} / {max_page}")
        with pager_cols[3]:
            st.caption(f"Total rows: {total_rows}")
        with pager_cols[4]:
            st.selectbox(
                "Page size",
                options=page_size_options,
                index=page_size_options.index(int(page_size)),
                key="results_runs_page_size",
                on_change=_reset_results_page,
                label_visibility="collapsed",
            )

        grid_filter_model, grid_sort_model = _extract_aggrid_models(grid)
        if grid_filter_model is not None or grid_sort_model is not None:
            if grid_filter_model != st.session_state.get("results_runs_filter_model") or grid_sort_model != st.session_state.get("results_runs_sort_model"):
                st.session_state["results_runs_filter_model"] = grid_filter_model
                st.session_state["results_runs_sort_model"] = grid_sort_model
                st.session_state["results_runs_page"] = 1
                st.rerun()

        if st.session_state.get("results_runs_debug_grid_state"):
            try:
                st.json(grid.get("grid_state") or grid.get("gridState") or grid)
            except Exception:
                st.json(grid)

        # Persist column sizing/order only on explicit save.
        if st.session_state.get("results_runs_save_layout_requested"):
            try:
                resp = st.session_state.get("grid_resp_results")
                new_state = _extract_aggrid_columns_state(resp)
                if isinstance(new_state, list) and new_state:
                    sanitized_state: list[dict] = []
                    for item in new_state:
                        if not isinstance(item, dict):
                            continue
                        col_id = item.get("colId") or item.get("col_id") or item.get("field")
                        if str(col_id) in {"action_click", "actions"}:
                            continue
                        d = dict(item)
                        # Visible columns are controlled separately (ui.results.visible_cols).
                        d.pop("hide", None)
                        d.pop("headerName", None)
                        if "width" not in d:
                            try:
                                d["width"] = int(d.get("actualWidth") or 0) or d.get("width")
                            except Exception:
                                pass
                        sanitized_state.append(d)
                    with open_db_connection() as conn:
                        set_user_setting(conn, user_id, "ui.results.columns_state", sanitized_state)
                    st.session_state["results_columns_state"] = sanitized_state
                    st.session_state["results_columns_state_last_persisted"] = sanitized_state
                    st.success("Layout saved.")
                else:
                    st.warning("No column state returned; ensure update_mode includes column events.")
            except Exception as exc:
                st.error(f"Failed to save layout: {exc}")
            finally:
                st.session_state.pop("results_runs_save_layout_requested", None)

        # Persist shortlist edits (avoid pandas conversion on every rerun).
        grid_data_raw = grid.get("data")
        if isinstance(grid_data_raw, pd.DataFrame):
            try:
                grid_data = grid_data_raw.to_dict("records")
            except Exception:
                grid_data = []
        elif isinstance(grid_data_raw, list):
            grid_data = grid_data_raw
        else:
            try:
                grid_data = list(grid_data_raw) if grid_data_raw is not None else []
            except Exception:
                grid_data = []

        baseline = st.session_state.get("runs_explorer_shortlist_baseline")
        if not isinstance(baseline, dict):
            baseline = {}
            if not df.empty and "run_id" in df.columns and "shortlist" in df.columns:
                for _, r in df.iterrows():
                    rid = str(r.get("run_id") or "").strip()
                    if not rid:
                        continue
                    baseline[rid] = {
                        "shortlist": bool(r.get("shortlist")),
                        "note": str(r.get("shortlist_note") or ""),
                    }
            st.session_state["runs_explorer_shortlist_baseline"] = baseline

        changed_flags: Dict[str, bool] = {}
        changed_notes: Dict[str, str] = {}
        if isinstance(grid_data, list) and baseline:
            for row in grid_data:
                if not isinstance(row, dict):
                    continue
                rid = str(row.get("run_id") or row.get("id") or row.get("runId") or "").strip()
                if not rid:
                    continue
                old = baseline.get(rid)
                if old is None:
                    continue
                cur_shortlisted = bool(row.get("shortlist") or False)
                cur_note = str(row.get("shortlist_note") or "").strip()
                old_shortlisted = bool(old.get("shortlist") or False)
                old_note = str(old.get("note") or "").strip()
                if old_shortlisted != cur_shortlisted or old_note != cur_note:
                    changed_flags[rid] = cur_shortlisted
                    changed_notes[rid] = cur_note

        if changed_flags:
            try:
                for rid, flag in changed_flags.items():
                    update_run_shortlist_fields(
                        rid,
                        shortlist=bool(flag),
                        shortlist_note=str(changed_notes.get(rid) or "").strip(),
                        user_id=user_id,
                    )
                    baseline[rid] = {
                        "shortlist": bool(flag),
                        "note": str(changed_notes.get(rid) or "").strip(),
                    }
                st.session_state["runs_explorer_shortlist_baseline"] = baseline
            except Exception as exc:
                st.error(f"Failed to persist shortlist changes: {exc}")

        selected_rows_raw = grid.get("selected_rows")
        if selected_rows_raw is None:
            selected_rows: list[dict[str, Any]] = []
        elif isinstance(selected_rows_raw, list):
            selected_rows = selected_rows_raw
        elif isinstance(selected_rows_raw, pd.DataFrame):
            try:
                selected_rows = selected_rows_raw.to_dict("records")
            except Exception:
                selected_rows = []
        else:
            try:
                selected_rows = list(selected_rows_raw)
            except Exception:
                selected_rows = []

        active_run_id = str(st.session_state.get("runs_explorer_active_run_id") or "").strip()
        if selected_rows:
            try:
                new_active = str(
                    selected_rows[0].get("run_id")
                    or selected_rows[0].get("id")
                    or selected_rows[0].get("runId")
                    or ""
                ).strip()
            except Exception:
                new_active = ""
            edited_ids = set(changed_flags.keys()) if isinstance(changed_flags, dict) else set()
            if new_active and new_active != active_run_id and (new_active not in edited_ids):
                st.session_state["runs_explorer_active_run_id"] = new_active
                st.session_state["deep_link_run_id"] = new_active
                st.session_state["current_page"] = "Run"
                _update_query_params(page="Run", run_id=new_active)
                st.rerun()

        # --- Results settings (bottom of page) ------------------------------
        st.markdown("---")
        st.caption("Results settings")
        bottom_bar = st.columns([2, 3], gap="large")
        with bottom_bar[0]:
            st.caption("Columns")
            picker_cols = list(candidate_cols)

            persisted = st.session_state.get("results_visible_cols")
            if isinstance(persisted, list) and persisted:
                persisted_base = [c for c in persisted if c in picker_cols]
            else:
                persisted_base = [c for c in visible_cols if c in picker_cols]

            chosen_base: list[str] = []
            with st.popover("Visible columns"):
                st.caption("Toggle visible columns")
                for c in picker_cols:
                    label = col_label(c)
                    wkey = f"results_col_{c}"
                    desired = bool(c in persisted_base)
                    # Initialize from persisted visibility only if the widget key isn't already set.
                    if wkey not in st.session_state:
                        st.session_state[wkey] = desired
                    checked = st.checkbox(label, key=wkey)
                    if checked:
                        chosen_base.append(c)

            chosen_visible_ui = [c for c in picker_cols if c in chosen_base]
            for c in always_visible_cols:
                if c not in chosen_visible_ui:
                    chosen_visible_ui.append(c)

            last_persisted = st.session_state.get("results_visible_cols_last_persisted")
            if isinstance(chosen_visible_ui, list) and chosen_visible_ui and chosen_visible_ui != last_persisted:
                try:
                    with open_db_connection() as conn:
                        set_user_setting(conn, user_id, "ui.results.visible_cols", list(chosen_visible_ui))
                    st.session_state["results_visible_cols_last_persisted"] = list(chosen_visible_ui)
                except Exception:
                    pass
        with bottom_bar[1]:
            st.caption("Layout")

            def _request_save_layout() -> None:
                st.session_state["results_runs_save_layout_requested"] = True
                try:
                    st.toast("Saving layout…")
                except Exception:
                    pass

            st.button("Save layout", key="results_runs_save_layout", on_click=_request_save_layout)

            if st.button("Reset layout", key="results_runs_reset_layout"):
                try:
                    with open_db_connection() as conn:
                        set_user_setting(conn, user_id, "ui.results.visible_cols", None)
                        set_user_setting(conn, user_id, "ui.results.columns_state", None)
                except Exception:
                    pass
                try:
                    for _k in list(st.session_state.keys()):
                        if str(_k).startswith("results_col_"):
                            st.session_state.pop(_k, None)
                except Exception:
                    pass
                for k in (
                    "results_visible_cols",
                    "results_visible_cols_last_persisted",
                    "results_columns_state",
                    "results_runs_columns_state_runtime",
                    "results_columns_state_last_persisted",
                    "results_apply_saved_columns_state",
                    "results_prefs_loaded",
                ):
                    st.session_state.pop(k, None)
                st.rerun()

            if str(os.environ.get("DRAGON_DEBUG") or "").strip() == "1":
                resp_dbg = st.session_state.get("grid_resp_results") or {}
                has_state = bool(
                    resp_dbg.get("columnState")
                    or resp_dbg.get("columnsState")
                    or resp_dbg.get("column_state")
                    or resp_dbg.get("columns_state")
                )
                saved_count = len(saved_state) if isinstance(saved_state, list) else 0
                st.caption(
                    f"Debug: columnState={has_state} • saved={saved_count} • fit_columns_on_grid_load=False"
                )

    if current_page == "Sweeps":
        st.subheader("Parameter sweeps")
        deep_link_sweep_val = st.session_state.get("deep_link_sweep_id")
        deep_link_sweep_id: Optional[str] = None
        if deep_link_sweep_val is not None:
            deep_link_sweep_id = str(deep_link_sweep_val).strip() or None
        if deep_link_sweep_id is not None:
            st.info(f"Deep link active: focusing sweep #{deep_link_sweep_id}.")
            if st.button("Clear sweep deep link", key="clear_sweep_link"):
                _update_query_params(sweep_id=None)
                st.session_state.pop("deep_link_sweep_id", None)
                st.session_state.pop("sweeps_list_selected_ids", None)
                sweep_run_key = f"sweep_runs_{deep_link_sweep_id}_selected_ids"
                st.session_state.pop(sweep_run_key, None)
                st.rerun()
        deep_link_run_val = st.session_state.get("deep_link_run_id")
        deep_link_run_int: Optional[int] = None
        if deep_link_run_val:
            try:
                deep_link_run_int = int(deep_link_run_val)
            except (TypeError, ValueError):
                deep_link_run_int = None

        # Canonical sweep selection for this page.
        # NOTE: sweep ids are TEXT in the DB, not integers.
        if deep_link_sweep_id is not None:
            st.session_state["sweeps_selected_sweep_id"] = str(deep_link_sweep_id)
        # Defensive: Streamlit hot-reload can occasionally get into a partially-reloaded
        # module state. Resolve list_sweeps_with_run_counts dynamically to avoid a hard NameError.
        _list_sweeps_with_counts = globals().get("list_sweeps_with_run_counts")
        if _list_sweeps_with_counts is None:
            from project_dragon.storage import list_sweeps_with_run_counts as _list_sweeps_with_counts  # type: ignore

        # Reconcile sweeps that were running/paused across restarts.
        try:
            with open_db_connection() as _recon_conn:
                reconcile_sweep_statuses(_recon_conn, user_id=user_id)
        except Exception:
            pass

        sweeps = _list_sweeps_with_counts(user_id=user_id, limit=100, offset=0)
        if not sweeps:
            st.info("No sweeps recorded yet.")
            return

        if AgGrid is None or GridOptionsBuilder is None or JsCode is None:
            st.error("Sweeps requires `streamlit-aggrid`. Install dependencies and restart the app.")
            return

        # --- Filters (applies to the runs grid below) ---------------------
        run_filters: Optional[dict] = None

        # Font for grid
        st.markdown(
            """
            <style>
            </style>
            <link rel="preconnect" href="https://fonts.googleapis.com">
            <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
            <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700;800&display=swap" rel="stylesheet">
            <style>
            </style>
            """,
            unsafe_allow_html=True,
        )

        use_enterprise = bool(os.environ.get("AGGRID_ENTERPRISE"))

        df_sweeps = pd.DataFrame(sweeps)
        if not df_sweeps.empty and "direction" not in df_sweeps.columns:
            if "base_config_json" in df_sweeps.columns:
                df_sweeps["direction"] = df_sweeps["base_config_json"].apply(infer_trade_direction_label)

        # Job-based progress (child backtest_run jobs).
        if not df_sweeps.empty and "id" in df_sweeps.columns:
            try:
                sweep_ids = [str(v).strip() for v in df_sweeps["id"].tolist() if str(v).strip()]
                counts_by_sweep: dict[str, dict[str, int]] = {}
                parent_by_sweep: dict[str, dict[str, Any]] = {}
                counts_by_parent: dict[int, dict[str, int]] = {}
                if sweep_ids:
                    with open_db_connection() as _jc_conn:
                        # Prefer parent-job-based aggregation when present.
                        try:
                            parent_by_sweep = get_sweep_parent_jobs_by_sweep(_jc_conn, sweep_ids=sweep_ids)
                        except Exception:
                            parent_by_sweep = {}

                        parent_ids: list[int] = []
                        for sid, info in (parent_by_sweep or {}).items():
                            try:
                                pid = int((info or {}).get("parent_job_id"))
                                if pid > 0:
                                    parent_ids.append(pid)
                            except Exception:
                                continue
                        parent_ids = sorted(set(parent_ids))
                        if parent_ids:
                            try:
                                counts_by_parent = get_backtest_run_job_counts_by_parent(_jc_conn, parent_job_ids=parent_ids)
                            except Exception:
                                counts_by_parent = {}

                        # Legacy fallback for older sweeps without a parent job.
                        counts_by_sweep = get_backtest_run_job_counts_by_sweep(_jc_conn, sweep_ids=sweep_ids)

                def _parent_id_for(sid: Any) -> Optional[int]:
                    try:
                        info = parent_by_sweep.get(str(sid)) if isinstance(parent_by_sweep, dict) else None
                        if not isinstance(info, dict):
                            return None
                        pid = info.get("parent_job_id")
                        return int(pid) if pid is not None else None
                    except Exception:
                        return None

                def _parent_status_for(sid: Any) -> str:
                    try:
                        info = parent_by_sweep.get(str(sid)) if isinstance(parent_by_sweep, dict) else None
                        if not isinstance(info, dict):
                            return ""
                        return str(info.get("status") or "")
                    except Exception:
                        return ""

                def _parent_pause_for(sid: Any) -> int:
                    try:
                        info = parent_by_sweep.get(str(sid)) if isinstance(parent_by_sweep, dict) else None
                        if not isinstance(info, dict):
                            return 0
                        return int(info.get("pause_requested") or 0)
                    except Exception:
                        return 0

                def _parent_cancel_for(sid: Any) -> int:
                    try:
                        info = parent_by_sweep.get(str(sid)) if isinstance(parent_by_sweep, dict) else None
                        if not isinstance(info, dict):
                            return 0
                        return int(info.get("cancel_requested") or 0)
                    except Exception:
                        return 0

                def _c(sid: Any, key: str) -> int:
                    try:
                        pid = _parent_id_for(sid)
                        if pid is not None and pid in (counts_by_parent or {}):
                            return int((counts_by_parent.get(int(pid), {}) or {}).get(key) or 0)
                        return int((counts_by_sweep.get(str(sid), {}) or {}).get(key) or 0)
                    except Exception:
                        return 0

                df_sweeps = df_sweeps.copy()
                df_sweeps["parent_job_id"] = df_sweeps["id"].apply(lambda sid: _parent_id_for(sid))
                df_sweeps["parent_job_status"] = df_sweeps["id"].apply(lambda sid: _parent_status_for(sid) or None)
                df_sweeps["parent_pause_requested"] = df_sweeps["id"].apply(lambda sid: _parent_pause_for(sid) or 0)
                df_sweeps["parent_cancel_requested"] = df_sweeps["id"].apply(lambda sid: _parent_cancel_for(sid) or 0)
                df_sweeps["jobs_total"] = df_sweeps["id"].apply(lambda sid: _c(sid, "total"))
                df_sweeps["jobs_done"] = df_sweeps["id"].apply(lambda sid: _c(sid, "done"))
                df_sweeps["jobs_failed"] = df_sweeps["id"].apply(lambda sid: _c(sid, "failed"))
                df_sweeps["jobs_cancelled"] = df_sweeps["id"].apply(lambda sid: _c(sid, "cancelled"))
                df_sweeps["jobs_running"] = df_sweeps["id"].apply(lambda sid: _c(sid, "running"))
                df_sweeps["jobs_queued"] = df_sweeps["id"].apply(lambda sid: _c(sid, "queued"))

                def _pct(terminal: Any, total: Any) -> Optional[float]:
                    try:
                        t = int(total or 0)
                        d = int(terminal or 0)
                        if t <= 0:
                            return None
                        return float(d) * 100.0 / float(t)
                    except Exception:
                        return None

                def _terminal(r: dict) -> int:
                    try:
                        return int(r.get("jobs_done") or 0) + int(r.get("jobs_failed") or 0) + int(r.get("jobs_cancelled") or 0)
                    except Exception:
                        return 0

                df_sweeps["jobs_progress_pct"] = df_sweeps.apply(lambda r: _pct(_terminal(dict(r)), r.get("jobs_total")), axis=1)
            except Exception:
                pass

        # Keep job progress columns stable even when counts are unavailable.
        if not df_sweeps.empty:
            for _col in (
                "jobs_total",
                "jobs_done",
                "jobs_failed",
                "jobs_cancelled",
                "jobs_running",
                "jobs_queued",
                "jobs_progress_pct",
            ):
                if _col not in df_sweeps.columns:
                    df_sweeps[_col] = None

        # Status pills + capitalization for the Sweeps grid.
        # Prefer a derived label when pause/cancel is requested on the parent job.
        try:
            if not df_sweeps.empty and "status" in df_sweeps.columns:
                df_sweeps = df_sweeps.copy()

                def _cap1(s: Any) -> str:
                    raw = str(s or "").strip()
                    if not raw:
                        return ""
                    return raw[:1].upper() + raw[1:]

                def _status_label(row: dict) -> str:
                    try:
                        raw = str(row.get("status") or "").strip().lower()
                        parent_status = str(row.get("parent_job_status") or "").strip().lower()
                        # Parent control-plane signals are more immediate than sweep.status.
                        try:
                            pause = int(row.get("parent_pause_requested") or 0) == 1
                        except Exception:
                            pause = False
                        try:
                            cancel = int(row.get("parent_cancel_requested") or 0) == 1
                        except Exception:
                            cancel = False

                        try:
                            jobs_total = int(row.get("jobs_total") or 0)
                        except Exception:
                            jobs_total = 0
                        try:
                            jobs_done = int(row.get("jobs_done") or 0)
                        except Exception:
                            jobs_done = 0
                        try:
                            jobs_failed = int(row.get("jobs_failed") or 0)
                        except Exception:
                            jobs_failed = 0
                        try:
                            jobs_cancelled = int(row.get("jobs_cancelled") or 0)
                        except Exception:
                            jobs_cancelled = 0
                        terminal_children = jobs_done + jobs_failed + jobs_cancelled

                        terminal = raw in {"done", "failed", "cancelled", "canceled"}
                        if cancel:
                            # If cancellation is effectively complete, show Cancelled.
                            if parent_status in {"cancelled", "canceled"}:
                                return "Cancelled"
                            if jobs_total > 0 and terminal_children >= jobs_total and jobs_cancelled > 0:
                                return "Cancelled"
                            # Parent job may finish as done/failed even when cancellation was requested;
                            # if any children were cancelled, treat the sweep as Cancelled.
                            if parent_status in {"done", "failed", "error"} and jobs_cancelled > 0:
                                return "Cancelled"
                            if not terminal:
                                return "Cancelling"
                        if pause and not terminal:
                            return "Paused"
                        if raw in {"cancelled", "canceled"}:
                            return "Cancelled"
                        if raw in {"done", "complete", "completed", "success", "succeeded"}:
                            return "Done"
                        if raw in {"failed", "failure", "error"}:
                            return "Failed"
                        if raw in {"running", "planning", "processing"}:
                            return "Running"
                        if raw in {"queued", "pending"}:
                            return "Queued"
                        return _cap1(raw)
                    except Exception:
                        return _cap1(row.get("status"))

                df_sweeps["status"] = df_sweeps.apply(lambda r: _status_label(dict(r)), axis=1)
        except Exception:
            pass

        # Derive a display market label for consistent "Asset - Market" rendering.
        if not df_sweeps.empty and "market" not in df_sweeps.columns:
            def _infer_market_from_sweep_row(row: dict) -> str:
                try:
                    if not isinstance(row, dict):
                        return ""
                    for k in ("base_config_json", "sweep_definition_json", "range_params_json"):
                        raw = row.get(k)
                        try:
                            obj = json.loads(raw) if isinstance(raw, str) and raw.strip() else (raw if isinstance(raw, dict) else None)
                        except Exception:
                            obj = None
                        if not isinstance(obj, dict):
                            continue
                        ds = obj.get("data_settings") if isinstance(obj.get("data_settings"), dict) else None
                        mkt = None
                        if isinstance(ds, dict):
                            mkt = ds.get("market_type") or ds.get("market")
                        if not mkt:
                            mkt = obj.get("market_type") or obj.get("market") or obj.get("market_type_hint")
                        if mkt:
                            m = str(mkt).strip().lower()
                            if m in {"perp", "perps", "futures"}:
                                return "PERPS"
                            if m in {"spot"}:
                                return "SPOT"
                            return str(mkt).strip().upper()
                except Exception:
                    return ""
                return ""

            try:
                df_sweeps = df_sweeps.copy()
            except Exception:
                pass
            try:
                df_sweeps["market"] = df_sweeps.apply(lambda r: _infer_market_from_sweep_row(dict(r)), axis=1)
            except Exception:
                df_sweeps["market"] = ""

        # Best-effort attach icon_uri for the sweeps grid (matches Results grids behavior).
        if not df_sweeps.empty and "icon_uri" not in df_sweeps.columns and "symbol" in df_sweeps.columns:
            try:
                symbols = (
                    df_sweeps["symbol"].fillna("").astype(str).map(lambda s: s.strip()).tolist()
                    if "symbol" in df_sweeps.columns
                    else []
                )
                symbols = [s for s in symbols if s]
                icon_map: dict[str, str] = {}
                if symbols:
                    with open_db_connection() as conn:
                        placeholders = ",".join(["?"] * len(symbols))
                        rows_icons = conn.execute(
                            f"SELECT exchange_symbol, icon_uri FROM symbols WHERE exchange_symbol IN ({placeholders})",
                            tuple(symbols),
                        ).fetchall()
                    for r in rows_icons or []:
                        try:
                            icon_map[str(r[0])] = str(r[1] or "")
                        except Exception:
                            pass
                df_sweeps = df_sweeps.copy()
                df_sweeps["icon_uri"] = df_sweeps["symbol"].astype(str).map(lambda s: icon_map.get(str(s).strip(), "") or None)
            except Exception:
                try:
                    df_sweeps["icon_uri"] = None
                except Exception:
                    pass

        # Best-effort attach sweep category name (for category-based multi-asset sweeps).
        if not df_sweeps.empty and "sweep_category" not in df_sweeps.columns:
            try:
                df_sweeps = df_sweeps.copy()
            except Exception:
                pass
            try:
                df_sweeps["sweep_category"] = ""
            except Exception:
                pass

        # Derived Asset column:
        # - single_asset => base asset from symbol
        # - multi_asset_category => category name
        # - multi_asset_manual => first 3 assets from snapshot
        if not df_sweeps.empty and "asset" not in df_sweeps.columns:
            try:
                df_sweeps = df_sweeps.copy()
            except Exception:
                pass
            df_sweeps["asset"] = ""

            # Ensure sweep_category exists before it is referenced below.
            if "sweep_category" not in df_sweeps.columns:
                df_sweeps["sweep_category"] = ""

            # Best-effort populate sweep_category from (exchange_id, sweep_category_id).
            try:
                if "sweep_category_id" in df_sweeps.columns and "exchange_id" in df_sweeps.columns:
                    with open_db_connection() as _cat_conn:
                        for ex_id, idxs in df_sweeps.groupby("exchange_id").groups.items():
                            ex_norm = str(ex_id or "").strip()
                            if not ex_norm:
                                continue
                            raw_ids = df_sweeps.loc[list(idxs), "sweep_category_id"].tolist()
                            ids: list[int] = []
                            for v in raw_ids or []:
                                try:
                                    if v is None:
                                        continue
                                    ids.append(int(v))
                                except Exception:
                                    continue
                            ids = sorted(set([i for i in ids if i > 0]))
                            if not ids:
                                continue
                            placeholders = ",".join(["?"] * len(ids))
                            rows_cat = _cat_conn.execute(
                                f"SELECT id, name FROM asset_categories WHERE user_id = ? AND exchange_id = ? AND id IN ({placeholders})",
                                tuple([str(user_id or "").strip() or "admin@local", ex_norm] + ids),
                            ).fetchall()
                            id_to_name: dict[int, str] = {}
                            for r in rows_cat or []:
                                try:
                                    id_to_name[int(r[0])] = str(r[1] or "").strip()
                                except Exception:
                                    continue
                            if not id_to_name:
                                continue
                            df_sweeps.loc[list(idxs), "sweep_category"] = df_sweeps.loc[list(idxs), "sweep_category_id"].map(
                                lambda x: id_to_name.get(int(x), "") if x is not None else ""
                            )
            except Exception:
                pass

            def _base_from_symbol(sym: str) -> str:
                s = str(sym or "").strip()
                if not s:
                    return ""
                if "/" in s:
                    return (s.split("/", 1)[0] or "").strip().upper()
                up = s.strip().upper()
                if up.startswith("PERP_") or up.startswith("SPOT_"):
                    parts = [p for p in up.split("_") if p]
                    return (parts[1] if len(parts) >= 2 else up).strip().upper()
                if "-" in s:
                    return (s.split("-", 1)[0] or "").strip().upper()
                return up

            def _preview_assets(raw: Any) -> str:
                try:
                    arr = json.loads(raw) if isinstance(raw, str) and raw.strip() else []
                except Exception:
                    arr = []
                if not isinstance(arr, list):
                    return ""
                bases = [_base_from_symbol(x) for x in arr]
                bases = [b for b in bases if b]
                # preserve order, unique
                seen = set()
                uniq: list[str] = []
                for b in bases:
                    if b in seen:
                        continue
                    seen.add(b)
                    uniq.append(b)
                if not uniq:
                    return ""
                head = uniq[:3]
                rest = max(0, len(uniq) - len(head))
                return ", ".join(head) + (f" +{rest}" if rest else "")

            scope = df_sweeps.get("sweep_scope")
            if scope is None:
                df_sweeps["asset"] = df_sweeps.get("symbol", "").apply(_base_from_symbol)
            else:
                scope_s = scope.fillna("").astype(str).str.strip()
                # category sweeps
                mask_cat = scope_s.eq("multi_asset_category")
                if mask_cat.any():
                    df_sweeps.loc[mask_cat, "asset"] = df_sweeps.loc[mask_cat, "sweep_category"].fillna("").astype(str)
                # manual sweeps
                mask_man = scope_s.eq("multi_asset_manual")
                if mask_man.any():
                    df_sweeps.loc[mask_man, "asset"] = df_sweeps.loc[mask_man, "sweep_assets_json"].apply(_preview_assets)
                # single asset
                mask_single = ~mask_cat & ~mask_man
                if mask_single.any():
                    df_sweeps.loc[mask_single, "asset"] = df_sweeps.loc[mask_single, "symbol"].apply(_base_from_symbol)

        # Normalize Asset display to match other grids (e.g., "ETH/USDT:USDT - PERPS").
        if not df_sweeps.empty:
            try:
                df_sweeps = df_sweeps.copy()
            except Exception:
                pass

            try:
                from project_dragon.data_online import woox_symbol_to_ccxt_symbol
            except Exception:
                woox_symbol_to_ccxt_symbol = None

            def _display_symbol_for_row(row: dict) -> str:
                sym = str(row.get("symbol") or "").strip()
                if not sym:
                    return ""
                ex_id = str(row.get("exchange_id") or "").strip()
                ex_norm = normalize_exchange_id(ex_id) if ex_id else ""
                if (ex_norm == "woox" or sym.upper().startswith(("PERP_", "SPOT_"))) and woox_symbol_to_ccxt_symbol:
                    try:
                        display, _ = woox_symbol_to_ccxt_symbol(sym)
                        return str(display or sym).strip()
                    except Exception:
                        return sym
                return sym

            try:
                df_sweeps["asset_display"] = df_sweeps.apply(lambda r: _display_symbol_for_row(dict(r)), axis=1)
            except Exception:
                df_sweeps["asset_display"] = df_sweeps.get("symbol", "")

            def _asset_label_for_row(row: dict) -> str:
                scope_val = str(row.get("sweep_scope") or "").strip().lower()
                if scope_val == "multi_asset_category":
                    return str(row.get("sweep_category") or "").strip()
                if scope_val == "multi_asset_manual":
                    return str(row.get("asset") or "").strip()
                # single asset: base asset derived from symbol
                display = str(row.get("asset_display") or "").strip()
                if display:
                    return display
                sym = str(row.get("symbol") or "").strip()
                return _base_from_symbol(sym)

            try:
                df_sweeps["asset_label"] = df_sweeps.apply(lambda r: _asset_label_for_row(dict(r)), axis=1)
            except Exception:
                df_sweeps["asset_label"] = df_sweeps.get("asset", "")

            try:
                base_label = df_sweeps.get("asset_label")
                if base_label is not None:
                    df_sweeps["asset_display"] = base_label.where(base_label.astype(str).str.strip() != "", df_sweeps.get("asset_display", ""))
                if "asset_display" in df_sweeps.columns:
                    df_sweeps["asset_display"] = df_sweeps["asset_display"].fillna("").astype(str)
            except Exception:
                df_sweeps["asset_display"] = df_sweeps.get("asset_display", df_sweeps.get("symbol", ""))

        # Optional: compute per-sweep best metrics (from all runs; filter applies only to run grid)
        # Do this via SQL aggregation to keep the Sweeps page fast even with many runs.
        try:
            _list_best = globals().get("list_sweep_best_metrics")
            if _list_best is None:
                from project_dragon.storage import list_sweep_best_metrics as _list_best  # type: ignore

            best_rows = _list_best(user_id=user_id)
            df_best = pd.DataFrame(best_rows) if best_rows else pd.DataFrame()
            if not df_best.empty and "sweep_id" in df_best.columns and "id" in df_sweeps.columns:
                df_sweeps = df_sweeps.merge(df_best, left_on="id", right_on="sweep_id", how="left")
        except Exception:
            pass

        # Derive sweep Start/End/Duration (best-effort) from JSON blobs.
        try:
            if not df_sweeps.empty:
                df_sweeps = df_sweeps.copy()

                def _extract_sweep_range(row: dict) -> tuple[Optional[datetime], Optional[datetime]]:
                    try:
                        for k in ("range_params_json", "base_config_json", "sweep_definition_json"):
                            raw = row.get(k)
                            try:
                                obj = (
                                    json.loads(raw)
                                    if isinstance(raw, str) and raw.strip()
                                    else (raw if isinstance(raw, dict) else None)
                                )
                            except Exception:
                                obj = None
                            if not isinstance(obj, dict):
                                continue
                            for sub in (
                                obj,
                                obj.get("range_params") if isinstance(obj.get("range_params"), dict) else None,
                                obj.get("data_settings") if isinstance(obj.get("data_settings"), dict) else None,
                                obj.get("metadata") if isinstance(obj.get("metadata"), dict) else None,
                            ):
                                if not isinstance(sub, dict):
                                    continue
                                s = sub.get("start_ts") or sub.get("start_time") or sub.get("since")
                                e = sub.get("end_ts") or sub.get("end_time") or sub.get("until")

                                def _cd(v: Any) -> Optional[datetime]:
                                    if v is None:
                                        return None
                                    if isinstance(v, (int, float)):
                                        x = float(v)
                                        if x > 10_000_000_000:
                                            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
                                        return datetime.fromtimestamp(x, tz=timezone.utc)
                                    s0 = str(v).strip()
                                    if not s0:
                                        return None
                                    try:
                                        dt = datetime.fromisoformat(s0.replace("Z", "+00:00"))
                                        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                                    except Exception:
                                        return None

                                sd = _cd(s)
                                ed = _cd(e)
                                if sd or ed:
                                    return (sd, ed)
                    except Exception:
                        return (None, None)
                    return (None, None)

                def _fmt_td(sd: Optional[datetime], ed: Optional[datetime]) -> str:
                    if not sd or not ed:
                        return ""
                    try:
                        delta = ed - sd
                        secs = delta.total_seconds()
                        if secs <= 0:
                            return ""
                        days = int(secs // 86400)
                        hours = int((secs % 86400) // 3600)
                        if days > 0:
                            return f"{days}d {hours}h" if hours else f"{days}d"
                        mins = int((secs % 3600) // 60)
                        return f"{hours}h {mins}m" if hours else f"{mins}m"
                    except Exception:
                        return ""

                tz = _ui_display_tz()
                rng = df_sweeps.apply(lambda r: _extract_sweep_range(dict(r)), axis=1)
                df_sweeps["start_date"] = [
                    (_ui_local_date_dmy(sd, tz) if sd else None) for sd, _ in rng
                ]
                df_sweeps["end_date"] = [
                    (_ui_local_date_dmy(ed, tz) if ed else None) for _, ed in rng
                ]
                df_sweeps["duration"] = [_fmt_td(sd, ed) for sd, ed in rng]
        except Exception:
            pass

        # Display columns
        hidden_sweep_cols = {
            "sweep_definition_json",
            "metadata_json",
            "base_config_json",
            "range_params_json",
            "sweep_id",
            "sweep_assets_json",
            "sweep_category_id",
            # Remove from Sweeps overview grid
            "asset",
            "asset_display",
            "strategy_version",
            "data_source",
            "exchange_id",
            "range_mode",
            "user_id",

            # Parent/job control-plane columns (used only for derived status).
            "parent_job_id",
            "parent_job_status",
            "parent_pause_requested",
            "parent_cancel_requested",
        }
        df_sweeps_display = df_sweeps.drop(columns=[c for c in hidden_sweep_cols if c in df_sweeps.columns], errors="ignore")
        # Default ordering for the Sweeps overview grid.
        # ("asset" is represented by the `symbol` column rendered as "Asset".)
        sweeps_order = [
            "id",
            "name",
            "symbol",
            "sweep_category",
            "timeframe",
            "direction",
            "run_count",
            "jobs_progress_pct",
            "status",
            "jobs_total",
            "jobs_done",
            "jobs_failed",
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "best_net_return_pct",
            "best_sharpe",
            "best_roi_pct_on_margin",
            "best_cpc_index",
        ]
        existing_sweeps = [c for c in sweeps_order if c in df_sweeps_display.columns]
        rest_sweeps = [c for c in df_sweeps_display.columns if c not in existing_sweeps]
        df_sweeps_display = df_sweeps_display[existing_sweeps + rest_sweeps]

        # Keep job progress columns present even if upstream data is missing.
        for _col in ("jobs_progress_pct", "jobs_total", "jobs_done", "jobs_failed"):
            if _col not in df_sweeps_display.columns:
                df_sweeps_display[_col] = None

        # --- In-memory filters (no AG Grid Enterprise required) --------
        total_sweeps = int(len(df_sweeps_display))
        sweeps_filter_spec = {
            "search": {
                "type": "text",
                "label": "Search",
                "columns": ["name", "strategy_name", "symbol", "id"],
            },
            "status": "multiselect",
            "strategy_name": "multiselect",
            "symbol": "multiselect",
            "timeframe": "multiselect",
            "direction": "multiselect",
            "run_count": "range",
            "created_at": {"type": "date_range", "label": "Created"},
        }
        # Optional metrics (only show if columns exist)
        if "best_net_return_pct" in df_sweeps_display.columns:
            sweeps_filter_spec["best_net_return_pct"] = {"type": "range", "label": "Best net return (fraction)"}
        if "best_sharpe" in df_sweeps_display.columns:
            sweeps_filter_spec["best_sharpe"] = "range"
        if "best_roi_pct_on_margin" in df_sweeps_display.columns:
            sweeps_filter_spec["best_roi_pct_on_margin"] = {"type": "range", "label": "Best ROI % (margin)"}

        # Use a short-lived, dedicated connection for filter preset reads/writes.
        # This avoids coupling to any other long-lived connection that may be busy.
        with open_db_connection() as _filters_conn:
            fr = render_table_filters(
                df_sweeps_display,
                sweeps_filter_spec,
                scope_key="sweeps:overview",
                dataset_cache_key=f"sweeps:{total_sweeps}",
                presets_conn=_filters_conn,
                presets_user_id=user_id,
                presets_scope_key="shared:results_sweeps",
            )
        df_sweeps_display = fr.df
        st.caption(f"{int(len(df_sweeps_display))} rows shown / {total_sweeps} total")

        # Ensure timestamp columns are presentation-safe for AGGrid (strings, not datetimes).
        # Use ISO local dates so lexical sort works and the browser can't reinterpret timezones.
        try:
            tz = _ui_display_tz()
            if "created_at" in df_sweeps_display.columns:
                df_sweeps_display["created_at"] = [
                    _ui_local_dt_ampm(v, tz) for v in df_sweeps_display["created_at"].tolist()
                ]
        except Exception:
            pass

        # IMPORTANT: streamlit-aggrid `pre_selected_rows` expects 0-based positional indices.
        # Ensure a clean RangeIndex so we can reliably preselect by row number.
        df_sweeps_display = df_sweeps_display.reset_index(drop=True)

        # Preselect the sweep row if we have one.
        pre_selected_sweep_rows: List[int] = []
        try:
            sid0 = st.session_state.get("sweeps_selected_sweep_id")
            if sid0 is not None and "id" in df_sweeps_display.columns:
                pre_selected_sweep_rows = [
                    int(i)
                    for i in df_sweeps_display.index[df_sweeps_display["id"].astype(str) == str(sid0)].tolist()
                ]
        except Exception:
            pre_selected_sweep_rows = []

        # --- Sweeps grid (top) -------------------------------------------
        st.markdown("### Sweeps")

        # Sweeps grid controls (Results-style): keep these directly above the sweeps grid
        # so it's unambiguous that they apply to the Sweeps table (not the runs table below).
        sweeps_controls = st.columns([1, 2, 6], gap="small", vertical_alignment="bottom")
        with sweeps_controls[0]:
            picker_cols = [c for c in df_sweeps_display.columns.tolist() if c not in {"sweep_action_click", "icon_uri", "market"}]

            persisted = st.session_state.get("sweeps_visible_cols")
            if isinstance(persisted, list) and persisted:
                persisted_base = [c for c in persisted if c in picker_cols]
            else:
                # Mirror defaults used for rendering the grid.
                persisted_base = [
                    c
                    for c in (
                        "id",
                        "name",
                        "status",
                        "symbol",
                        "sweep_category",
                        "timeframe",
                        "direction",
                        "run_count",
                        "best_roi_pct_on_margin",
                        "best_cpc_index",
                        "best_net_return_pct",
                        "best_sharpe",
                        "created_at",
                        "start_date",
                        "end_date",
                        "duration",
                    )
                    if c in picker_cols
                ]

            chosen: list[str] = []
            with st.popover("Cols", help="Choose visible Sweeps columns"):
                for c in picker_cols:
                    label = str(c).replace("_", " ").title()
                    if c == "sweep_category":
                        label = "Category"
                    elif c == "best_roi_pct_on_margin":
                        label = "Best ROI"
                    elif c == "best_cpc_index":
                        label = "Best CPC"
                    elif c == "best_net_return_pct":
                        label = "Best Net Return"
                    elif c == "best_sharpe":
                        label = "Best Sharpe"
                    checked = st.checkbox(label, value=(c in persisted_base), key=f"sweeps_overview_col_{c}")
                    if checked:
                        chosen.append(c)

            last_persisted = st.session_state.get("sweeps_visible_cols_last_persisted")
            if isinstance(chosen, list) and chosen and chosen != last_persisted:
                try:
                    with open_db_connection() as _prefs_conn:
                        set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols", list(chosen))
                        # Mark schema/version for visible columns so we can migrate old saved lists.
                        set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols_version", 2)
                    st.session_state["sweeps_visible_cols"] = list(chosen)
                    st.session_state["sweeps_visible_cols_last_persisted"] = list(chosen)
                    st.rerun()
                except Exception:
                    pass

        with sweeps_controls[1]:
            btns = st.columns([1, 1], gap="small")

            def _request_save_layout_sweeps() -> None:
                st.session_state["sweeps_save_layout_requested"] = True

            with btns[0]:
                st.button(
                    "Save",
                    key="sweeps_save_layout",
                    on_click=_request_save_layout_sweeps,
                    help="Save Sweeps column layout",
                    use_container_width=True,
                )

            with btns[1]:
                if st.button(
                    "Reset",
                    key="sweeps_reset_layout",
                    help="Reset Sweeps columns + layout",
                    use_container_width=True,
                ):
                    try:
                        with open_db_connection() as _prefs_conn:
                            set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols", None)
                            set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols_version", None)
                            set_user_setting(_prefs_conn, user_id, "ui.sweeps.columns_state", None)
                    except Exception:
                        pass
                    for k in (
                        "sweeps_visible_cols",
                        "sweeps_visible_cols_last_persisted",
                        "sweeps_visible_cols_loaded",
                        "sweeps_visible_cols_version",
                        "sweeps_visible_cols_version_loaded",
                        "sweeps_columns_state",
                        "sweeps_columns_state_runtime",
                        "sweeps_columns_state_last_persisted",
                        "sweeps_columns_state_loaded",
                    ):
                        st.session_state.pop(k, None)
                    st.rerun()

        with sweeps_controls[2]:
            st.caption("Sweeps grid settings")

        custom_css = _aggrid_dark_custom_css()

        # IMPORTANT: load persisted column state BEFORE we compute per-column widths.
        # Otherwise a browser refresh would render defaults on the first pass.
        if not st.session_state.get("sweeps_columns_state_loaded", False):
            try:
                with open_db_connection() as _prefs_conn:
                    saved = get_user_setting(_prefs_conn, user_id, "ui.sweeps.columns_state", default=None)
                if isinstance(saved, list) and saved:
                    st.session_state["sweeps_columns_state"] = saved
                    st.session_state["sweeps_columns_state_last_persisted"] = saved
            except Exception:
                pass
            st.session_state["sweeps_columns_state_loaded"] = True

        # NOTE: Use section-local variable names. The Sweeps page also defines
        # `timeframe_style` later for the Runs grid; reusing the same name here
        # causes an UnboundLocalError (Python treats it as a local assigned later).
        status_style_sweeps_overview = aggrid_pill_style("status")

        sweeps_header_overrides = {
            "id": "ID",
            "name": "Name",
            "symbol": "Asset",
            "sweep_category": "Category",
            "timeframe": "Timeframe",
            "direction": "Direction",
            "status": "Status",
            "run_count": "Runs",
            "jobs_progress_pct": "Jobs %",
            "jobs_total": "Jobs",
            "jobs_done": "Done",
            "jobs_failed": "Failed",
            "jobs_cancelled": "Cancelled",
            "jobs_running": "Running",
            "jobs_queued": "Queued",
            "created_at": "Run Date",
            "start_date": "Start",
            "end_date": "End",
            "duration": "Duration",
            "best_net_return_pct": "Best Net Return",
            "best_roi_pct_on_margin": "Best ROI",
            "best_sharpe": "Best Sharpe",
            "best_cpc_index": "Best CPC",
        }
        sweeps_acronyms = {"id", "roi", "cpc", "api", "url", "pnl", "atr", "ma", "rsi", "macd", "bb"}

        def _sweeps_header_label(col: str) -> str:
            if not col:
                return ""
            if col in sweeps_header_overrides:
                return sweeps_header_overrides[col]
            parts = [p for p in str(col).replace("_", " ").split() if p]
            out: list[str] = []
            for p in parts:
                lo = p.lower()
                out.append(p.upper() if lo in sweeps_acronyms else p.capitalize())
            return " ".join(out)

        _sweeps_state = st.session_state.get("sweeps_columns_state")
        _sweeps_widths: dict[str, int] = {}
        if isinstance(_sweeps_state, list):
            for item in _sweeps_state:
                if not isinstance(item, dict):
                    continue
                col_id = item.get("colId") or item.get("col_id") or item.get("field")
                if not col_id:
                    continue
                width_val = item.get("width") or item.get("actualWidth")
                try:
                    width_int = int(width_val)
                except Exception:
                    continue
                if width_int > 0:
                    _sweeps_widths[str(col_id)] = width_int

        def _sweeps_w(col: str, default: int) -> int:
            try:
                return int(_sweeps_widths.get(str(col), default))
            except Exception:
                return default
        roi_pct_formatter = JsCode(
            """
            function(params) {
                if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                const v = Number(params.value);
                if (!isFinite(v)) { return ''; }
                return Math.round(v).toString() + '%';
            }
            """
        )

        jobs_progress_renderer_sweeps = JsCode(
            """
            class JobsProgressRenderer {
                init(params) {
                    const v = Number(params.value);
                    const total = (params && params.data && params.data.jobs_total !== undefined && params.data.jobs_total !== null)
                        ? Number(params.data.jobs_total)
                        : NaN;

                    // Conditional: only render when it is meaningful.
                    if (!isFinite(v) || !isFinite(total) || total <= 0) {
                        const empty = document.createElement('div');
                        empty.style.width = '100%';
                        empty.style.textAlign = 'center';
                        empty.style.color = '#64748b';
                        empty.innerText = '';
                        this.eGui = empty;
                        return;
                    }

                    const pct = Math.max(0, Math.min(100, v));

                    let barRgb = '56,139,253';
                    let textColor = '#b6d4fe';
                    if (pct >= 95) {
                        barRgb = '46,160,67';
                        textColor = '#7ee787';
                    } else if (pct >= 50) {
                        barRgb = '250,204,21';
                        textColor = '#facc15';
                    }

                    const wrap = document.createElement('div');
                    wrap.style.display = 'flex';
                    wrap.style.alignItems = 'center';
                    wrap.style.justifyContent = 'center';
                    wrap.style.width = '100%';
                    wrap.style.height = '100%';

                    const outer = document.createElement('div');
                    outer.style.width = '100%';
                    outer.style.maxWidth = '110px';
                    outer.style.height = '14px';
                    outer.style.borderRadius = '999px';
                    outer.style.backgroundColor = 'rgba(148,163,184,0.14)';
                    outer.style.position = 'relative';
                    outer.style.overflow = 'hidden';

                    const inner = document.createElement('div');
                    inner.style.height = '100%';
                    inner.style.width = pct.toFixed(0) + '%';
                    inner.style.backgroundColor = `rgba(${barRgb},0.35)`;

                    const label = document.createElement('div');
                    label.style.position = 'absolute';
                    label.style.top = '0';
                    label.style.left = '0';
                    label.style.right = '0';
                    label.style.bottom = '0';
                    label.style.display = 'flex';
                    label.style.alignItems = 'center';
                    label.style.justifyContent = 'center';
                    label.style.fontSize = '11px';
                    label.style.fontWeight = '500';
                    label.style.color = textColor;
                    label.innerText = pct.toFixed(0) + '%';

                    outer.appendChild(inner);
                    outer.appendChild(label);
                    wrap.appendChild(outer);
                    this.eGui = wrap;
                }
                getGui() { return this.eGui; }
                refresh(params) { return false; }
            }
            """
        )

        pct_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
                            const shown = (Math.abs(v) <= 1.0) ? (v * 100.0) : v;
                                                        return shown.toFixed(0) + '%';
            }
            """
        )
        num2_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
              return v.toFixed(2);
            }
            """
        )

        metrics_gradient_style = JsCode(
            """
            function(params) {
                const col = (params && params.colDef && params.colDef.field) ? params.colDef.field.toString() : '';
                const v = Number(params.value);
                if (!isFinite(v)) { return {}; }

                const greenText = '#7ee787';
                const redText = '#ff7b72';
                const neutralText = '#cbd5e1';
                const greenRgb = '46,160,67';
                const redRgb = '248,81,73';

                function clamp01(x) { return Math.max(0.0, Math.min(1.0, x)); }
                function alphaFromT(t) { const tt = clamp01(t); return 0.10 + (0.22 * tt); }
                function styleGoodBad(isGood, t) {
                    const a = alphaFromT(t);
                    if (isGood === true) return { color: greenText, backgroundColor: `rgba(${greenRgb},${a})`, textAlign: 'center' };
                    if (isGood === false) return { color: redText, backgroundColor: `rgba(${redRgb},${a})`, textAlign: 'center' };
                    return { color: neutralText, textAlign: 'center' };
                }

                function asRatio(x) {
                    if (!isFinite(x)) { return x; }
                    return (Math.abs(x) <= 1.0) ? x : (x / 100.0);
                }

                if (col === 'best_net_return_pct') {
                    const r = asRatio(v);
                    const t = clamp01(Math.abs(r) / 0.50);
                    if (r > 0) return styleGoodBad(true, t);
                    if (r < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_roi_pct_on_margin') {
                    // v is already a percent
                    const t = clamp01(Math.abs(v) / 200.0);
                    if (v > 0) return styleGoodBad(true, t);
                    if (v < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_sharpe') {
                    if (v >= 1.0) {
                        const t = clamp01((v - 1.0) / 2.0);
                        return styleGoodBad(true, t);
                    }
                    if (v < 0.0) {
                        const t = clamp01(Math.abs(v) / 1.5);
                        return styleGoodBad(false, t);
                    }
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_cpc_index') {
                    // CPC Index is unitless; treat >1 as stronger.
                    if (v >= 1.0) {
                        const t = clamp01((v - 1.0) / 2.0);
                        return styleGoodBad(true, t);
                    }
                    if (v < 0.0) {
                        const t = clamp01(Math.abs(v) / 1.5);
                        return styleGoodBad(false, t);
                    }
                    return { color: neutralText, textAlign: 'center' };
                }
                return {};
            }
            """
        )

        gb_s = GridOptionsBuilder.from_dataframe(df_sweeps_display)
        gb_s.configure_default_column(filter=True, sortable=True, resizable=True)
        gb_s.configure_selection(
            "single",
            # IMPORTANT: `use_checkbox=True` creates a separate selection column.
            # We want the selection checkbox to live only inside the Asset column.
            use_checkbox=False,
            pre_selected_rows=pre_selected_sweep_rows,
        )
        # Prefer checkbox-driven selection for consistency with Results.
        gb_s.configure_grid_options(headerHeight=52, rowHeight=38, animateRows=False, suppressRowClickSelection=False)
        sym_filter = "agSetColumnFilter" if use_enterprise else True

        # Visible columns (Results-style): persisted separately from column sizing/order.
        if not st.session_state.get("sweeps_visible_cols_loaded", False):
            try:
                with open_db_connection() as _prefs_conn:
                    saved_cols = get_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols", default=None)
                    saved_ver = get_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols_version", default=None)
                if isinstance(saved_ver, (int, float)):
                    st.session_state["sweeps_visible_cols_version"] = int(saved_ver)
                else:
                    st.session_state["sweeps_visible_cols_version"] = None
                st.session_state["sweeps_visible_cols_version_loaded"] = True

                if isinstance(saved_cols, list) and saved_cols:
                    # One-time migration: older saved visible column lists (pre Best ROI/CPC)
                    # should include these new default columns.
                    try:
                        ver = st.session_state.get("sweeps_visible_cols_version")
                    except Exception:
                        ver = None
                    # v2: add Best ROI/CPC.
                    if not isinstance(ver, int) or ver < 2:
                        required_v2 = [
                            c
                            for c in ("best_roi_pct_on_margin", "best_cpc_index")
                            if c in df_sweeps_display.columns and c not in saved_cols
                        ]
                        if required_v2:
                            saved_cols = list(saved_cols) + required_v2
                            try:
                                with open_db_connection() as _prefs_conn:
                                    set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols", list(saved_cols))
                                    set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols_version", 2)
                                st.session_state["sweeps_visible_cols_version"] = 2
                                ver = 2
                            except Exception:
                                pass

                    # v3: ensure Jobs % is visible by default.
                    if not isinstance(ver, int) or ver < 3:
                        required_v3 = [
                            c
                            for c in ("jobs_progress_pct",)
                            if c in df_sweeps_display.columns and c not in saved_cols
                        ]
                        if required_v3:
                            # Insert Jobs % just before Status if present; otherwise append.
                            out_cols = list(saved_cols)
                            try:
                                idx = out_cols.index("status")
                                for c in required_v3:
                                    out_cols.insert(idx, c)
                                    idx += 1
                            except ValueError:
                                out_cols.extend(required_v3)
                            saved_cols = out_cols
                            try:
                                with open_db_connection() as _prefs_conn:
                                    set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols", list(saved_cols))
                                    set_user_setting(_prefs_conn, user_id, "ui.sweeps.visible_cols_version", 3)
                                st.session_state["sweeps_visible_cols_version"] = 3
                            except Exception:
                                pass

                    st.session_state["sweeps_visible_cols"] = saved_cols
                    st.session_state["sweeps_visible_cols_last_persisted"] = saved_cols
            except Exception:
                pass
            st.session_state["sweeps_visible_cols_loaded"] = True

        default_visible_cols = [
            "id",
            "name",
            "symbol",
            "sweep_category",
            "timeframe",
            "direction",
            "run_count",
            "jobs_progress_pct",
            "status",
            "jobs_total",
            "jobs_failed",
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "best_net_return_pct",
            "best_sharpe",
            "best_roi_pct_on_margin",
            "best_cpc_index",
            "sweep_actions",
        ]
        _persisted_visible = st.session_state.get("sweeps_visible_cols")
        if isinstance(_persisted_visible, list) and _persisted_visible:
            visible_cols_sweeps = [c for c in _persisted_visible if c in df_sweeps_display.columns]
        else:
            visible_cols_sweeps = [c for c in default_visible_cols if c in df_sweeps_display.columns]
        # Always keep Jobs % and Actions visible when present.
        if "jobs_progress_pct" in df_sweeps_display.columns and "jobs_progress_pct" not in visible_cols_sweeps:
            try:
                idx = visible_cols_sweeps.index("status")
                visible_cols_sweeps.insert(idx, "jobs_progress_pct")
            except Exception:
                visible_cols_sweeps.append("jobs_progress_pct")
        if "sweep_actions" in df_sweeps_display.columns and "sweep_actions" not in visible_cols_sweeps:
            visible_cols_sweeps.append("sweep_actions")

        # Standardize Asset column sorting/filtering: use "symbol - market" as the value.
        asset_market_getter = asset_market_getter_js(
            symbol_field="symbol",
            market_field="market",
            base_asset_field="asset_display",
            asset_field="asset_display",
        )

        # Date formatting (UTC) for sweeps overview.
        ddmm_hhmm_utc_formatter = JsCode(
            """
            function(params) {
                try {
                    const v = params && params.value !== undefined && params.value !== null ? params.value : '';
                    const s = v.toString().trim();
                    if (!s) return '';

                    let d;
                    if (/^[0-9]+$/.test(s)) {
                        const n = parseInt(s, 10);
                        d = new Date(n > 1000000000000 ? n : (n * 1000));
                    } else {
                        d = new Date(s);
                    }
                    if (isNaN(d.getTime())) return s;

                    const dd = String(d.getUTCDate()).padStart(2, '0');
                    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
                    const hh = String(d.getUTCHours()).padStart(2, '0');
                    const mi = String(d.getUTCMinutes()).padStart(2, '0');
                    return `${dd}/${mm} ${hh}:${mi}`;
                } catch (e) {
                    const v = params && params.value !== undefined && params.value !== null ? params.value : '';
                    return v;
                }
            }
            """
        )

        asset_renderer_sweeps = asset_renderer_js(icon_field="icon_uri", size_px=18, text_color="#FFFFFF")

        metrics_gradient_style = JsCode(
            """
            function(params) {
                const col = (params && params.colDef && params.colDef.field) ? params.colDef.field.toString() : '';
                const v = Number(params.value);
                if (!isFinite(v)) { return {}; }

                const greenText = '#7ee787';
                const redText = '#ff7b72';
                const neutralText = '#cbd5e1';
                const greenRgb = '46,160,67';
                const redRgb = '248,81,73';

                function clamp01(x) { return Math.max(0.0, Math.min(1.0, x)); }
                function alphaFromT(t) { const tt = clamp01(t); return 0.10 + (0.22 * tt); }
                function styleGoodBad(isGood, t) {
                    const a = alphaFromT(t);
                    if (isGood === true) return { color: greenText, backgroundColor: `rgba(${greenRgb},${a})`, textAlign: 'center' };
                    if (isGood === false) return { color: redText, backgroundColor: `rgba(${redRgb},${a})`, textAlign: 'center' };
                    return { color: neutralText, textAlign: 'center' };
                }

                function asRatio(x) {
                    if (!isFinite(x)) { return x; }
                    return (Math.abs(x) <= 1.0) ? x : (x / 100.0);
                }

                if (col === 'best_net_return_pct') {
                    const r = asRatio(v);
                    const t = clamp01(Math.abs(r) / 0.50);
                    if (r > 0) return styleGoodBad(true, t);
                    if (r < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_roi_pct_on_margin') {
                    // v is already a percent
                    const t = clamp01(Math.abs(v) / 200.0);
                    if (v > 0) return styleGoodBad(true, t);
                    if (v < 0) return styleGoodBad(false, t);
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_sharpe') {
                    if (v >= 1.0) {
                        const t = clamp01((v - 1.0) / 2.0);
                        return styleGoodBad(true, t);
                    }
                    if (v < 0.0) {
                        const t = clamp01(Math.abs(v) / 1.5);
                        return styleGoodBad(false, t);
                    }
                    return { color: neutralText, textAlign: 'center' };
                }
                if (col === 'best_cpc_index') {
                    // CPC Index is unitless; treat >1 as stronger.
                    if (v >= 1.0) {
                        const t = clamp01((v - 1.0) / 2.0);
                        return styleGoodBad(true, t);
                    }
                    if (v < 0.0) {
                        const t = clamp01(Math.abs(v) / 1.5);
                        return styleGoodBad(false, t);
                    }
                    return { color: neutralText, textAlign: 'center' };
                }
                return {};
            }
            """
        )

        if "run_count" in df_sweeps_display.columns:
            gb_s.configure_column(
                "run_count",
                headerName="Runs",
                width=_sweeps_w("run_count", 90),
                filter=False,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("run_count" not in visible_cols_sweeps),
            )

        if "jobs_progress_pct" in df_sweeps_display.columns:
            gb_s.configure_column(
                "jobs_progress_pct",
                headerName="Jobs %",
                width=_sweeps_w("jobs_progress_pct", 140),
                filter=False,
                type=["numericColumn"],
                cellRenderer=jobs_progress_renderer_sweeps,
                cellClass="ag-center-aligned-cell",
                hide=("jobs_progress_pct" not in visible_cols_sweeps),
            )
        if "jobs_total" in df_sweeps_display.columns:
            gb_s.configure_column(
                "jobs_total",
                headerName="Jobs",
                width=_sweeps_w("jobs_total", 90),
                filter=False,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("jobs_total" not in visible_cols_sweeps),
            )
        if "jobs_done" in df_sweeps_display.columns:
            gb_s.configure_column(
                "jobs_done",
                headerName="Done",
                width=_sweeps_w("jobs_done", 90),
                filter=False,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("jobs_done" not in visible_cols_sweeps),
            )
        if "jobs_failed" in df_sweeps_display.columns:
            gb_s.configure_column(
                "jobs_failed",
                headerName="Failed",
                width=_sweeps_w("jobs_failed", 95),
                filter=False,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("jobs_failed" not in visible_cols_sweeps),
            )

        shared_col_defs: list[dict[str, Any]] = []
        if "timeframe" in df_sweeps_display.columns:
            shared_col_defs.append(
                col_timeframe_pill(
                    field="timeframe",
                    header=_sweeps_header_label("timeframe"),
                    width=_sweeps_w("timeframe", 95),
                    filter=sym_filter,
                    hide=("timeframe" not in visible_cols_sweeps),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "direction" in df_sweeps_display.columns:
            shared_col_defs.append(
                col_direction_pill(
                    field="direction",
                    header=_sweeps_header_label("direction"),
                    width=_sweeps_w("direction", 110),
                    filter=sym_filter,
                    hide=("direction" not in visible_cols_sweeps),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "start_date" in df_sweeps_display.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_date",
                    _sweeps_header_label("start_date"),
                    width=_sweeps_w("start_date", 110),
                    filter=True,
                    hide=("start_date" not in visible_cols_sweeps),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_date" in df_sweeps_display.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_date",
                    _sweeps_header_label("end_date"),
                    width=_sweeps_w("end_date", 110),
                    filter=True,
                    hide=("end_date" not in visible_cols_sweeps),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        apply_columns(gb_s, shared_col_defs, saved_widths=_sweeps_widths)

        if "id" in df_sweeps_display.columns:
            gb_s.configure_column(
                "id",
                headerName=_sweeps_header_label("id"),
                width=_sweeps_w("id", 130),
                cellClass="ag-center-aligned-cell",
                hide=("id" not in visible_cols_sweeps),
            )
        if "name" in df_sweeps_display.columns:
            gb_s.configure_column(
                "name",
                headerName=_sweeps_header_label("name"),
                width=_sweeps_w("name", 220),
                hide=("name" not in visible_cols_sweeps),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        if "symbol" in df_sweeps_display.columns:
            gb_s.configure_column(
                "symbol",
                headerName=_sweeps_header_label("symbol"),
                width=_sweeps_w("symbol", 200),
                filter=sym_filter,
                hide=("symbol" not in visible_cols_sweeps),
                wrapHeaderText=True,
                autoHeaderHeight=True,
                cellRenderer=asset_renderer_sweeps,
                valueGetter=asset_market_getter,
            )
        if "sweep_category" in df_sweeps_display.columns:
            gb_s.configure_column(
                "sweep_category",
                headerName=_sweeps_header_label("sweep_category"),
                width=_sweeps_w("sweep_category", 170),
                filter=True,
                hide=("sweep_category" not in visible_cols_sweeps),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
        if "status" in df_sweeps_display.columns:
            gb_s.configure_column(
                "status",
                headerName=_sweeps_header_label("status"),
                width=_sweeps_w("status", 120),
                filter=True,
                cellStyle=status_style_sweeps_overview,
                cellClass="ag-center-aligned-cell",
                hide=("status" not in visible_cols_sweeps),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )

        if "created_at" in df_sweeps_display.columns:
            gb_s.configure_column(
                "created_at",
                headerName=_sweeps_header_label("created_at"),
                width=_sweeps_w("created_at", 160),
                filter=True,
                hide=("created_at" not in visible_cols_sweeps),
            )

        if "best_net_return_pct" in df_sweeps_display.columns:
            gb_s.configure_column(
                "best_net_return_pct",
                headerName=_sweeps_header_label("best_net_return_pct"),
                valueFormatter=pct_formatter,
                width=_sweeps_w("best_net_return_pct", 150),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("best_net_return_pct" not in visible_cols_sweeps),
            )
        if "best_roi_pct_on_margin" in df_sweeps_display.columns:
            gb_s.configure_column(
                "best_roi_pct_on_margin",
                headerName=_sweeps_header_label("best_roi_pct_on_margin"),
                valueFormatter=roi_pct_formatter,
                width=_sweeps_w("best_roi_pct_on_margin", 150),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("best_roi_pct_on_margin" not in visible_cols_sweeps),
            )
        if "best_sharpe" in df_sweeps_display.columns:
            gb_s.configure_column(
                "best_sharpe",
                headerName=_sweeps_header_label("best_sharpe"),
                valueFormatter=num2_formatter,
                width=_sweeps_w("best_sharpe", 120),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("best_sharpe" not in visible_cols_sweeps),
            )

        if "best_cpc_index" in df_sweeps_display.columns:
            gb_s.configure_column(
                "best_cpc_index",
                headerName=_sweeps_header_label("best_cpc_index"),
                valueFormatter=num2_formatter,
                width=_sweeps_w("best_cpc_index", 120),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("best_cpc_index" not in visible_cols_sweeps),
            )
        if "duration" in df_sweeps_display.columns:
            gb_s.configure_column(
                "duration",
                headerName=_sweeps_header_label("duration"),
                width=_sweeps_w("duration", 105),
                cellClass="ag-center-aligned-cell",
                hide=("duration" not in visible_cols_sweeps),
            )

        if "icon_uri" in df_sweeps_display.columns:
            gb_s.configure_column("icon_uri", hide=True)
        if "market" in df_sweeps_display.columns:
            gb_s.configure_column("market", hide=True)

        # Hide any other columns not selected (and apply title-case headers).
        try:
            always_hidden = {"icon_uri", "market"}
            configured_cols = {
                "id",
                "name",
                "symbol",
                "sweep_category",
                "timeframe",
                "direction",
                "status",
                "run_count",
                "jobs_progress_pct",
                "jobs_total",
                "jobs_done",
                "jobs_failed",
                "created_at",
                "start_date",
                "end_date",
                "duration",
                "best_net_return_pct",
                "best_roi_pct_on_margin",
                "best_sharpe",
                "best_cpc_index",
            }
            for c in df_sweeps_display.columns.tolist():
                if c in always_hidden or c == "sweep_actions":
                    continue
                if c in configured_cols:
                    if c not in visible_cols_sweeps:
                        gb_s.configure_column(c, hide=True)
                    continue
                gb_s.configure_column(
                    c,
                    headerName=_sweeps_header_label(str(c)),
                    hide=(c not in visible_cols_sweeps),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
        except Exception:
            pass

        grid_options_sweeps = gb_s.build()
        try:
            if pre_selected_sweep_rows:
                grid_options_sweeps["preSelectedRows"] = list(pre_selected_sweep_rows)
        except Exception:
            pass

        # Force visual selection (checkbox + highlight) on first render.
        # Some ag-grid/streamlit-aggrid versions ignore initialState rowSelection.
        try:
            target_sweep_id = str(st.session_state.get("sweeps_selected_sweep_id") or "").strip()
        except Exception:
            target_sweep_id = ""
        if target_sweep_id:
            try:
                grid_options_sweeps["getRowId"] = JsCode(
                    """
                    function(params) {
                        try {
                            return (params && params.data && params.data.id !== undefined && params.data.id !== null)
                                ? params.data.id.toString()
                                : undefined;
                        } catch (e) { return undefined; }
                    }
                    """
                )
                grid_options_sweeps["onFirstDataRendered"] = JsCode(
                    f"""
                    function(params) {{
                        const target = {json.dumps(target_sweep_id)};
                        if (!target) return;
                        try {{
                            params.api.forEachNode(function(node) {{
                                try {{
                                    const id = (node && node.data && node.data.id !== undefined && node.data.id !== null)
                                        ? node.data.id.toString()
                                        : '';
                                    if (id === target) {{
                                        node.setSelected(true, true);
                                        if (node.rowIndex !== undefined && node.rowIndex !== null) {{
                                            params.api.ensureIndexVisible(node.rowIndex, 'middle');
                                        }}
                                    }}
                                }} catch (e) {{}}
                            }});
                        }} catch (e) {{}}
                    }}
                    """
                )
            except Exception:
                pass

        runtime_state = st.session_state.get("sweeps_columns_state_runtime")
        saved_state = st.session_state.get("sweeps_columns_state")
        columns_state = None
        state_source = runtime_state if isinstance(runtime_state, list) and runtime_state else saved_state
        if isinstance(state_source, list) and state_source:
            try:
                sanitized: list[dict] = []
                valid_ids = {str(c) for c in df_sweeps_display.columns.tolist()}
                for item in state_source:
                    if not isinstance(item, dict):
                        continue
                    d = dict(item)
                    # Visible columns are controlled separately (ui.sweeps.visible_cols).
                    d.pop("hide", None)
                    d.pop("headerName", None)
                    col_id = d.get("colId") or d.get("col_id") or d.get("field")
                    if col_id is None or str(col_id) not in valid_ids:
                        continue
                    sanitized.append(d)
                columns_state = sanitized or None
            except Exception:
                columns_state = None

        grid_sweeps = AgGrid(
            df_sweeps_display,
            gridOptions=grid_options_sweeps,
            theme="dark",
            custom_css=custom_css,
            # Selection changes must trigger a rerun so the runs grid appears.
            update_mode=(
                GridUpdateMode.SELECTION_CHANGED
                | GridUpdateMode.VALUE_CHANGED
                | getattr(GridUpdateMode, "COLUMN_RESIZED", 0)
            ),
            data_return_mode=DataReturnMode.AS_INPUT,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            height=320,
            key="sweeps_grid_v2",
            enable_enterprise_modules=use_enterprise,
            columns_state=columns_state,
        )

        # Persist sweeps layout only on explicit request (Results-style).
        if st.session_state.get("sweeps_save_layout_requested"):
            try:
                new_state = _extract_aggrid_columns_state(grid_sweeps)
                if isinstance(new_state, list) and new_state:
                    sanitized_state: list[dict] = []
                    for item in new_state:
                        if not isinstance(item, dict):
                            continue
                        col_id = item.get("colId") or item.get("col_id") or item.get("field")
                        d = dict(item)
                        # Hide is controlled by ui.sweeps.visible_cols.
                        d.pop("hide", None)
                        d.pop("headerName", None)
                        if "width" not in d:
                            try:
                                d["width"] = int(d.get("actualWidth") or 0) or d.get("width")
                            except Exception:
                                pass
                        sanitized_state.append(d)
                    with open_db_connection() as _prefs_conn:
                        set_user_setting(_prefs_conn, user_id, "ui.sweeps.columns_state", sanitized_state)
                    st.session_state["sweeps_columns_state"] = sanitized_state
                    st.session_state["sweeps_columns_state_last_persisted"] = sanitized_state
                    st.success("Layout saved.")
                else:
                    st.warning("No layout changes detected to save.")
            except Exception as exc:
                st.error(f"Failed to save layout: {exc}")
            finally:
                st.session_state.pop("sweeps_save_layout_requested", None)

        selected_sweep_id: Optional[str] = None
        selected_sweep_rows_raw = grid_sweeps.get("selected_rows")
        selected_sweep_rows: list[dict[str, Any]] = []
        if isinstance(selected_sweep_rows_raw, list):
            selected_sweep_rows = selected_sweep_rows_raw
        elif isinstance(selected_sweep_rows_raw, pd.DataFrame):
            try:
                selected_sweep_rows = selected_sweep_rows_raw.to_dict("records")
            except Exception:
                selected_sweep_rows = []

        if selected_sweep_rows:
            try:
                selected_sweep_id = str(selected_sweep_rows[0].get("id") or "").strip() or None
            except Exception:
                selected_sweep_id = None
        if selected_sweep_id is None:
            try:
                sid0 = st.session_state.get("sweeps_selected_sweep_id")
                selected_sweep_id = str(sid0).strip() if sid0 is not None else None
                if selected_sweep_id == "":
                    selected_sweep_id = None
            except Exception:
                selected_sweep_id = None
        if selected_sweep_id is None and deep_link_sweep_id is not None:
            selected_sweep_id = str(deep_link_sweep_id).strip() or None

        def _render_sweeps_overview_chart(df_in: pd.DataFrame, highlight_id: Optional[str]) -> None:
            if df_in is None or df_in.empty:
                return
            df_chart = df_in.copy()
            if "created_at" not in df_chart.columns:
                return
            try:
                df_chart["created_at_ts"] = pd.to_datetime(df_chart["created_at"], errors="coerce", utc=True)
            except Exception:
                return
            df_chart = df_chart.dropna(subset=["created_at_ts"])
            if df_chart.empty:
                return

            metric_candidates = [
                "best_roi_pct_on_margin",
                "best_net_return_pct",
                "best_sharpe",
                "run_count",
            ]
            y_col = None
            for c in metric_candidates:
                if c in df_chart.columns and df_chart[c].notna().any():
                    y_col = c
                    break
            if y_col is None:
                return

            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df_chart["created_at_ts"],
                    y=df_chart[y_col],
                    mode="markers",
                    name="Sweeps",
                    text=df_chart.get("name", ""),
                    customdata=df_chart.get("id", ""),
                    hovertemplate="%{text}<br>%{y}<extra></extra>",
                )
            )

            if highlight_id:
                try:
                    mask = df_chart.get("id").astype(str) == str(highlight_id)
                except Exception:
                    mask = None
                if mask is not None and mask.any():
                    sel = df_chart.loc[mask]
                    fig.add_trace(
                        go.Scatter(
                            x=sel["created_at_ts"],
                            y=sel[y_col],
                            mode="markers",
                            name="Selected",
                            marker={"size": 14, "symbol": "diamond-open"},
                            showlegend=False,
                            text=sel.get("name", ""),
                            hovertemplate="%{text}<br>%{y}<extra></extra>",
                        )
                    )

            fig.update_layout(
                template="plotly_dark",
                height=260,
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="Created",
                yaxis_title=_sweeps_header_label(y_col) if "_sweeps_header_label" in locals() else y_col,
                showlegend=False,
            )
            st.plotly_chart(fig, width="stretch", key="sweeps_overview_chart")

        _render_sweeps_overview_chart(df_sweeps_display, selected_sweep_id)

        if selected_sweep_id is None:
            st.info("Select a sweep in the grid to load its runs below.")
            return

        st.session_state["deep_link_sweep_id"] = str(selected_sweep_id)
        st.session_state["sweeps_selected_sweep_id"] = str(selected_sweep_id)
        _update_query_params(sweep_id=str(selected_sweep_id))

        # Lookup full sweep row for the rerun button/definition expander.
        sweep_row = next((r for r in sweeps if isinstance(r, dict) and str(r.get("id")) == str(selected_sweep_id)), None)
        if sweep_row is not None:
            st.markdown("---")
            st.markdown(f"### Selected sweep: {sweep_row.get('name')} (#{selected_sweep_id})")

            derived_status_label: Optional[str] = None
            try:
                if not df_sweeps.empty and "id" in df_sweeps.columns and "status" in df_sweeps.columns:
                    m = df_sweeps[df_sweeps["id"].astype(str) == str(selected_sweep_id)]
                    if not m.empty:
                        derived_status_label = str(m.iloc[0].get("status") or "").strip() or None
            except Exception:
                derived_status_label = None

            # --- Sweep control plane (pause/resume/cancel) ----------------
            try:
                with open_db_connection() as _ctrl_conn:
                    parent_info = get_sweep_parent_jobs_by_sweep(_ctrl_conn, sweep_ids=[str(selected_sweep_id)])
                p = (parent_info or {}).get(str(selected_sweep_id)) if isinstance(parent_info, dict) else None
            except Exception:
                p = None

            if not isinstance(p, dict) or p.get("parent_job_id") is None:
                st.warning("This sweep has no parent planner job yet (sweep_parent).")
            else:
                parent_job_id = int(p.get("parent_job_id"))
                pause_req = int(p.get("pause_requested") or 0)
                cancel_req = int(p.get("cancel_requested") or 0)
                parent_status = str(p.get("status") or "")

                state_label = "Running"
                if cancel_req:
                    # Prefer showing Cancelled when the sweep's children are all terminal (and at least one was cancelled)
                    # or when the parent job is already cancelled.
                    counts_row: Optional[dict] = None
                    try:
                        if "jobs_total" in df_sweeps.columns and "id" in df_sweeps.columns:
                            m = df_sweeps[df_sweeps["id"].astype(str) == str(selected_sweep_id)]
                            if not m.empty:
                                counts_row = dict(m.iloc[0].to_dict())
                    except Exception:
                        counts_row = None

                    try:
                        jobs_total = int((counts_row or sweep_row).get("jobs_total") or 0)
                    except Exception:
                        jobs_total = 0
                    try:
                        jobs_done = int((counts_row or sweep_row).get("jobs_done") or 0)
                    except Exception:
                        jobs_done = 0
                    try:
                        jobs_failed = int((counts_row or sweep_row).get("jobs_failed") or 0)
                    except Exception:
                        jobs_failed = 0
                    try:
                        jobs_cancelled = int((counts_row or sweep_row).get("jobs_cancelled") or 0)
                    except Exception:
                        jobs_cancelled = 0
                    terminal_children = jobs_done + jobs_failed + jobs_cancelled

                    if parent_status in {"cancelled", "canceled"}:
                        state_label = "Cancelled"
                    elif jobs_total > 0 and terminal_children >= jobs_total and jobs_cancelled > 0:
                        state_label = "Cancelled"
                    elif parent_status in {"done", "failed", "error"} and jobs_cancelled > 0:
                        state_label = "Cancelled"
                    else:
                        state_label = "Cancelling"
                elif pause_req:
                    state_label = "Paused"
                elif parent_status in {"done"}:
                    state_label = "Planned"
                elif parent_status in {"failed", "error"}:
                    state_label = "Error"

                if derived_status_label:
                    state_label = derived_status_label

                is_terminal = str(state_label or "").strip().lower() in {"done", "failed", "cancelled"}

                st.caption(f"Sweep control: state={state_label} (parent_job_id={parent_job_id}, status={parent_status})")
                if not is_terminal:
                    c1, c2, c3, c4 = st.columns([1, 1, 1, 3], gap="small")
                    with c1:
                        pause_btn = st.button("Pause", key=f"sweep_pause_{selected_sweep_id}", disabled=bool(cancel_req) or bool(pause_req))
                    with c2:
                        resume_btn = st.button("Resume", key=f"sweep_resume_{selected_sweep_id}", disabled=bool(cancel_req) or (not bool(pause_req)))
                    with c3:
                        cancel_btn = st.button(
                            "Cancel",
                            key=f"sweep_cancel_{selected_sweep_id}",
                            disabled=bool(cancel_req),
                            help="Stops new child job claims; running children may finish (v1).",
                        )

                    if pause_btn:
                        with open_db_connection() as _ctrl_conn:
                            set_job_pause_requested(_ctrl_conn, parent_job_id, True)
                        st.success("Pause requested.")
                        st.rerun()

                    if resume_btn:
                        with open_db_connection() as _ctrl_conn:
                            set_job_pause_requested(_ctrl_conn, parent_job_id, False)
                        st.success("Resumed.")
                        st.rerun()

                    if cancel_btn:
                        with open_db_connection() as _ctrl_conn:
                            set_job_cancel_requested(_ctrl_conn, parent_job_id, True)
                        st.success("Cancel requested.")
                        st.rerun()
                else:
                    st.caption("Sweep completed. Controls are hidden for terminal sweeps.")

            try:
                definition = json.loads(sweep_row.get("sweep_definition_json", "{}"))
            except (TypeError, json.JSONDecodeError):
                definition = None
            with st.expander("Sweep definition", expanded=False):
                if definition is not None:
                    st.json(definition)
                else:
                    st.info("Unable to parse sweep definition.")


        # --- Runs grid (bottom, Results-style) ---------------------------
        st.markdown("---")
        selected_sweep_run_count = 0
        try:
            if isinstance(sweep_row, dict) and sweep_row.get("run_count") is not None:
                selected_sweep_run_count = int(sweep_row.get("run_count") or 0)
            else:
                selected_sweep_run_count = int(get_sweep_run_count(user_id=user_id, sweep_id=str(selected_sweep_id)))
        except Exception:
            selected_sweep_run_count = 0

        st.markdown(f"### Runs in selected sweep: {int(selected_sweep_run_count)}")

        # Shared filters (Results/Runs Explorer/Sweeps runs) for consistency.
        with open_db_connection() as _filter_conn:
            options_extra = _runs_global_filters_to_extra_where(get_global_filters())
            options_extra["sweep_id"] = str(selected_sweep_id)
            symbol_opts = list_backtest_run_symbols(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            tf_opts = list_backtest_run_timeframes(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            strat_opts = list_backtest_run_strategies_filtered(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
            bounds = get_runs_numeric_bounds(
                _filter_conn,
                extra_where=options_extra if options_extra else None,
                user_id=user_id,
            )
        # Default Run type to Sweep for this grid.
        if f"sweeps_runs_run_type" not in st.session_state:
            st.session_state["sweeps_runs_run_type"] = "Sweep"
        run_filters = _runs_server_filter_controls(
            "sweeps_runs",
            include_run_type=True,
            on_change=None,
            symbol_options=symbol_opts,
            strategy_options=strat_opts,
            timeframe_options=tf_opts,
            bounds=bounds,
        )

        # Load per-user persisted grid preferences for the Sweeps > Runs grid once per session.
        if not st.session_state.get("sweeps_runs_prefs_loaded", False):
            try:
                with open_db_connection() as _prefs_conn:
                    saved_visible = get_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.visible_cols", default=None)
                    saved_col_state = get_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.columns_state", default=None)
                if isinstance(saved_visible, list) and saved_visible:
                    st.session_state["sweeps_runs_visible_cols"] = saved_visible
                    st.session_state["sweeps_runs_visible_cols_last_persisted"] = list(saved_visible)
                if isinstance(saved_col_state, list) and saved_col_state:
                    st.session_state["sweeps_runs_columns_state"] = saved_col_state
                    st.session_state["sweeps_runs_columns_state_last_persisted"] = saved_col_state
            except Exception:
                pass
            st.session_state["sweeps_runs_prefs_loaded"] = True

        filters_for_runs = dict(run_filters or {})
        gf_for_runs = get_global_filters()
        if bool(gf_for_runs.get("enabled", True)) and bool(gf_for_runs.get("profitable_only", False)):
            filters_for_runs["net_return_pct_gt"] = 0.0
        filters_for_runs["sweep_id"] = str(selected_sweep_id)

        rows = load_backtest_runs_explorer_rows(
            user_id=user_id,
            limit=max(1, int(selected_sweep_run_count or 1)),
            filters=filters_for_runs,
        )

        if not rows:
            st.info("No runs recorded for this sweep yet.")
            return

        df = pd.DataFrame(rows)

        # Derive Start/End/Duration columns from stored run fields if present.
        try:
            if {"start_time", "end_time"}.issubset(set(df.columns)):
                sdt = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
                edt = pd.to_datetime(df["end_time"], errors="coerce", utc=True)
                df["start_date"] = sdt.dt.date.astype(str).where(sdt.notna(), None)
                df["end_date"] = edt.dt.date.astype(str).where(edt.notna(), None)
                dur = (edt - sdt)

                def _fmt_td(x: Any) -> str:
                    try:
                        if x is None or pd.isna(x):
                            return ""
                        secs = float(getattr(x, "total_seconds", lambda: 0.0)())
                        if secs <= 0:
                            return ""
                        days = int(secs // 86400)
                        hours = int((secs % 86400) // 3600)
                        if days > 0:
                            return f"{days}d {hours}h" if hours else f"{days}d"
                        mins = int((secs % 3600) // 60)
                        return f"{hours}h {mins}m" if hours else f"{mins}m"
                    except Exception:
                        return ""

                df["duration"] = dur.map(_fmt_td)
        except Exception:
            pass
        if "direction" not in df.columns:
            try:
                if "config_json" in df.columns:
                    df["direction"] = df["config_json"].apply(infer_trade_direction_label)
                else:
                    df["direction"] = ""
            except Exception:
                df["direction"] = ""

        if "shortlist" in df.columns:
            df["shortlist"] = df["shortlist"].astype(bool)
        if "icon_uri" not in df.columns:
            df["icon_uri"] = None

        if "asset" not in df.columns:
            base = df.get("base_asset") if "base_asset" in df.columns else None
            if base is not None:
                df["asset"] = base.fillna("").astype(str).str.upper().replace({"NAN": ""})
            else:
                df["asset"] = ""

        if "symbol" in df.columns:
            try:
                df["symbol_full"] = df["symbol"].fillna("").astype(str)
            except Exception:
                df["symbol_full"] = df["symbol"].astype(str)
        else:
            df["symbol_full"] = ""

        if "symbol" not in df.columns:
            df["symbol"] = ""

        if "market_type" in df.columns and "market" not in df.columns:
            df["market"] = df["market_type"]
        if "strategy_name" in df.columns and "strategy" not in df.columns:
            df["strategy"] = df["strategy_name"]

        if "market" in df.columns:
            try:
                m = df["market"].fillna("").astype(str).str.strip().str.lower()
                m = m.replace({"perp": "perps", "futures": "perps", "future": "perps"})
                df["market"] = m.map({"perps": "PERPS", "spot": "SPOT"}).fillna(m.str.upper())
            except Exception:
                pass

        if "sweep_name" in df.columns:
            try:
                df["sweep_name"] = df["sweep_name"].fillna("").astype(str)
            except Exception:
                pass

        # Remove sweep-internal columns from the Sweeps > Runs grid.
        # This prevents them from appearing as default columns in AG Grid.
        df = df.drop(
            columns=["sweep_exchange_id", "sweep_scope", "sweep_category_id", "sweep_assets_json"],
            errors="ignore",
        )

        preferred = [
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "avg_position_time",
            "symbol",
            "timeframe",
            "direction",
            "strategy",
            "sweep_name",
        ]
        ordered_cols: List[str] = []
        for c in preferred:
            if c in df.columns and c not in ordered_cols:
                ordered_cols.append(c)
        for c in df.columns.tolist():
            if c in ordered_cols:
                continue
            ordered_cols.append(c)
        df = df[ordered_cols]

        # Default ordering: CPC Index desc (before computing pre-selected row indices).
        try:
            if "cpc_index" in df.columns:
                df = df.sort_values(by="cpc_index", ascending=False, na_position="last")
        except Exception:
            pass

        # Ensure positional indices for preselection.
        df = df.reset_index(drop=True)

        # Preselect active run (from deep link or last chosen) in the runs grid.
        wanted_run_id = str(st.session_state.get("sweeps_active_run_id") or "").strip()
        if not wanted_run_id:
            wanted_run_id = str(deep_link_run_val or "").strip()
        pre_selected_run_rows: List[int] = []
        try:
            if wanted_run_id and "run_id" in df.columns:
                pre_selected_run_rows = [
                    int(i)
                    for i in df.index[df["run_id"].astype(str) == wanted_run_id].tolist()
                ]
        except Exception:
            pre_selected_run_rows = []

        # No Actions column in Sweeps > Runs: buttons live on the run detail page.

        always_visible_cols = [c for c in ("symbol",) if c in df.columns]
        candidate_cols = [
            c
            for c in df.columns.tolist()
            if c
            not in {
                "icon_uri",
                "symbol_full",
                "metadata_json",
                "sweep_assets_json",
                "sweep_exchange_id",
                "sweep_scope",
                "sweep_category_id",
                "_exchange_id_for_cat",
                "config_json",
                "market_type",
                "strategy_name",
                "strategy_version",
                "sweep_id",
                "base_asset",
                "quote_asset",
                "asset",
                "asset_display",
                "market",
                "avg_position_time_seconds",
            }
        ]
        default_visible = st.session_state.get("sweeps_runs_visible_cols")
        if not isinstance(default_visible, list) or not default_visible:
            default_visible = candidate_cols
        try:
            default_visible = [c for c in default_visible if c in candidate_cols and c != "market"]
        except Exception:
            default_visible = candidate_cols
        for c in always_visible_cols:
            if c not in default_visible:
                default_visible.append(c)

        # Determine visible columns from session state; UI control is rendered below.
        raw_visible = st.session_state.get("sweeps_runs_visible_cols")
        if isinstance(raw_visible, list) and ("avg_position_time_s" in raw_visible or "avg_position_time_seconds" in raw_visible):
            raw_visible = [
                ("avg_position_time" if c in {"avg_position_time_s", "avg_position_time_seconds"} else c)
                for c in raw_visible
            ]
            st.session_state["sweeps_runs_visible_cols"] = list(raw_visible)
        if isinstance(raw_visible, list) and raw_visible:
            sanitized_visible = [c for c in raw_visible if c in candidate_cols and c != "market"]
            if sanitized_visible != raw_visible:
                st.session_state["sweeps_runs_visible_cols"] = sanitized_visible
            chosen_visible = sanitized_visible
        else:
            chosen_visible = [c for c in default_visible if c in candidate_cols and c != "market"]
        for c in always_visible_cols:
            if c not in chosen_visible:
                chosen_visible.append(c)

        # Ensure key metric columns show up even with older persisted prefs.
        if "roi_pct_on_margin" in candidate_cols and "roi_pct_on_margin" not in chosen_visible:
            chosen_visible.append("roi_pct_on_margin")
        if "max_drawdown_pct" in candidate_cols and "max_drawdown_pct" not in chosen_visible:
            chosen_visible.append("max_drawdown_pct")
        visible_cols = chosen_visible

        asset_market_getter = asset_market_getter_js(
            symbol_field="symbol",
            market_field="market",
            base_asset_field="asset_display",
            asset_field="asset_display",
        )

        roi_pct_formatter = JsCode(
            """
            function(params) {
                if (params.value === null || params.value === undefined || params.value === '') { return ''; }
                const v = Number(params.value);
                if (!isFinite(v)) { return ''; }
                return Math.round(v).toString() + '%';
            }
            """
        )

        asset_renderer = asset_renderer_js(icon_field="icon_uri", size_px=18, text_color="#FFFFFF")

        pct_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
                            const shown = (Math.abs(v) <= 1.0) ? (v * 100.0) : v;
                                                        return shown.toFixed(0) + '%';
            }
            """
        )
        num2_formatter = JsCode(
            """
            function(params) {
              if (params.value === null || params.value === undefined || params.value === '') { return ''; }
              const v = Number(params.value);
              if (!isFinite(v)) { return ''; }
              return v.toFixed(2);
            }
            """
        )
        metrics_gradient_style = JsCode(_aggrid_metrics_gradient_style_js())
        dd_style = JsCode(
            """
            function(params) {
                const v = Number(params.value);
                if (!isFinite(v)) { return {}; }
                const r = (Math.abs(v) <= 1.0) ? v : (v / 100.0);
                const t = Math.max(0.0, Math.min(1.0, r / 0.30));
                const alpha = 0.10 + (0.25 * t);
                return { color: '#ff7b72', backgroundColor: `rgba(248,81,73,${alpha})`, textAlign: 'center' };
            }
            """
        )
        # Presentation-only: convert date columns to local strings before AGGrid.
        # For the Sweeps > Runs grid, show Run Date as DD/MM hh:mm AM/PM.
        try:
            tz = _ui_display_tz()
            if "created_at" in df.columns:
                df["created_at"] = [_ui_local_dt_ampm(v, tz) for v in df["created_at"].tolist()]
            for _c in ("start_date", "end_date"):
                if _c in df.columns:
                    df[_c] = [_ui_local_date_iso(v, tz) for v in df[_c].tolist()]
        except Exception:
            pass

        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(filter=True, sortable=True, resizable=True)
        # IMPORTANT: `use_checkbox=True` creates a separate selection column.
        # We want the selection checkbox to live only inside the Asset column.
        gb.configure_selection("single", use_checkbox=False)
        gb.configure_grid_options(headerHeight=58, rowHeight=38, suppressRowHoverHighlight=False, animateRows=False)

        # Prefer set filters when enterprise modules are enabled
        sym_filter = "agSetColumnFilter" if use_enterprise else True

        # Column width helper for the Sweeps > Runs grid.
        # NOTE: This must be defined in this code path; a same-named helper exists in other pages
        # but is conditionally defined, which can otherwise trigger UnboundLocalError here.
        _sweeps_runs_widths: dict[str, int] = {}
        try:
            _sweeps_runs_saved = st.session_state.get("sweeps_runs_columns_state")
            if isinstance(_sweeps_runs_saved, list) and _sweeps_runs_saved:
                for item in _sweeps_runs_saved:
                    if not isinstance(item, dict):
                        continue
                    col_id = item.get("colId") or item.get("col_id") or item.get("field")
                    if not col_id:
                        continue
                    width_val = item.get("width") or item.get("actualWidth")
                    if width_val is None:
                        continue
                    try:
                        _sweeps_runs_widths[str(col_id)] = int(width_val)
                    except Exception:
                        continue
        except Exception:
            pass

        def _sweeps_runs_w(col: str, default: int) -> int:
            try:
                return max(60, int(_sweeps_runs_widths.get(str(col), default)))
            except Exception:
                return default

        debug_grid = str(os.environ.get("DRAGON_DEBUG_GRID") or os.environ.get("DRAGON_DEBUG") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        debug_logger = logging.getLogger(__name__)
        configured_cols: set[str] = set()
        configured_headers: dict[str, bool] = {}
        timeframe_pill_applied = False
        direction_pill_applied = False

        def _track_col(col: str, *, has_header: bool) -> None:
            configured_cols.add(col)
            if has_header:
                configured_headers[col] = True
            else:
                configured_headers.setdefault(col, False)

        shared_col_defs: list[dict[str, Any]] = []
        if "start_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "start_date",
                    col_label("start_date"),
                    width=_sweeps_runs_w("start_date", 110),
                    hide=("start_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "end_date" in df.columns:
            shared_col_defs.append(
                col_date_ddmmyy(
                    "end_date",
                    col_label("end_date"),
                    width=_sweeps_runs_w("end_date", 110),
                    hide=("end_date" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
        if "timeframe" in df.columns:
            shared_col_defs.append(
                col_timeframe_pill(
                    field="timeframe",
                    header=col_label("timeframe"),
                    width=_sweeps_runs_w("timeframe", 90),
                    filter=sym_filter,
                    hide=("timeframe" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
            timeframe_pill_applied = True
        if "direction" in df.columns:
            shared_col_defs.append(
                col_direction_pill(
                    field="direction",
                    header=col_label("direction"),
                    width=_sweeps_runs_w("direction", 100),
                    filter=sym_filter,
                    hide=("direction" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )
            direction_pill_applied = True
        if "avg_position_time" in df.columns:
            shared_col_defs.append(
                col_avg_position_time(
                    field="avg_position_time",
                    header=col_label("avg_position_time"),
                    width=_sweeps_runs_w("avg_position_time", 150),
                    hide=("avg_position_time" not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
            )

        apply_columns(gb, shared_col_defs, saved_widths=_sweeps_runs_widths)
        for col_def in shared_col_defs:
            field = col_def.get("field") if isinstance(col_def, dict) else None
            if field:
                _track_col(str(field), has_header=True)

        # Show Run Date first (requested), then Asset.
        if "created_at" in df.columns:
            gb.configure_column(
                "created_at",
                headerName=col_label("created_at"),
                width=_sweeps_runs_w("created_at", 160),
                pinned="left",
                hide=("created_at" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("created_at", has_header=True)
        gb.configure_column(
            "symbol",
            headerName=col_label("symbol"),
            width=_sweeps_runs_w("symbol", 220),
            filter=sym_filter,
            hide=("symbol" not in visible_cols),
            wrapHeaderText=True,
            autoHeaderHeight=True,
            cellRenderer=asset_renderer,
            valueGetter=asset_market_getter,
            checkboxSelection=True,
            headerCheckboxSelection=False,
        )
        _track_col("symbol", has_header=True)
        if "duration" in df.columns:
            gb.configure_column(
                "duration",
                headerName=col_label("duration"),
                width=_sweeps_runs_w("duration", 110),
                hide=("duration" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
                cellClass="ag-center-aligned-cell",
            )
            _track_col("duration", has_header=True)
        if "strategy" in df.columns:
            gb.configure_column(
                "strategy",
                headerName=col_label("strategy"),
                width=_sweeps_runs_w("strategy", 160),
                filter=sym_filter,
                hide=("strategy" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("strategy", has_header=True)
        if "sweep_name" in df.columns:
            gb.configure_column(
                "sweep_name",
                headerName=col_label("sweep_name"),
                width=_sweeps_runs_w("sweep_name", 180),
                filter=True,
                hide=("sweep_name" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("sweep_name", has_header=True)

        for col in ("net_return_pct", "max_drawdown_pct", "win_rate"):
            if col in df.columns:
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    valueFormatter=pct_formatter,
                    width=_sweeps_runs_w(col, 130 if col == "max_drawdown_pct" else 120),
                    cellStyle=dd_style if col == "max_drawdown_pct" else metrics_gradient_style,
                    type=["numericColumn"],
                    cellClass="ag-center-aligned-cell",
                    hide=(col not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
                _track_col(col, has_header=True)

        if "roi_pct_on_margin" in df.columns:
            gb.configure_column(
                "roi_pct_on_margin",
                headerName=col_label("roi_pct_on_margin"),
                valueFormatter=roi_pct_formatter,
                width=_sweeps_runs_w("roi_pct_on_margin", 140),
                cellStyle=metrics_gradient_style,
                type=["numericColumn"],
                cellClass="ag-center-aligned-cell",
                hide=("roi_pct_on_margin" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("roi_pct_on_margin", has_header=True)
        for col in ("net_profit", "sharpe", "sortino", "profit_factor", "cpc_index", "common_sense_ratio"):
            if col in df.columns:
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    valueFormatter=num2_formatter,
                    width=_sweeps_runs_w(col, 120),
                    cellStyle=metrics_gradient_style,
                    type=["numericColumn"],
                    cellClass="ag-center-aligned-cell",
                    hide=(col not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
                _track_col(col, has_header=True)

        # Shortlist editable columns
        if "shortlist" in df.columns:
            gb.configure_column(
                "shortlist",
                headerName=col_label("shortlist"),
                editable=True,
                cellRenderer="agCheckboxCellRenderer",
                width=_sweeps_runs_w("shortlist", 105),
                filter=False,
                hide=("shortlist" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("shortlist", has_header=True)
        if "shortlist_note" in df.columns:
            gb.configure_column(
                "shortlist_note",
                headerName=col_label("shortlist_note"),
                editable=True,
                width=_sweeps_runs_w("shortlist_note", 200),
                hide=("shortlist_note" not in visible_cols),
                wrapHeaderText=True,
                autoHeaderHeight=True,
            )
            _track_col("shortlist_note", has_header=True)

        # Ensure the Cols picker controls visibility for all candidate columns.
        # GridOptionsBuilder.from_dataframe() creates default column defs for every df column;
        # without explicitly setting `hide`, extra columns would remain visible regardless of picker state.
        _custom_configured_cols = {
            "symbol",
            "created_at",
            "start_date",
            "end_date",
            "duration",
            "timeframe",
            "direction",
            "strategy",
            "sweep_name",
            "net_return_pct",
            "roi_pct_on_margin",
            "max_drawdown_pct",
            "win_rate",
            "net_profit",
            "sharpe",
            "sortino",
            "profit_factor",
            "cpc_index",
            "common_sense_ratio",
            "shortlist",
            "shortlist_note",
        }
        try:
            for col in candidate_cols:
                if col in _custom_configured_cols:
                    continue
                if col not in df.columns:
                    continue
                gb.configure_column(
                    col,
                    headerName=col_label(col),
                    hide=(col not in visible_cols),
                    wrapHeaderText=True,
                    autoHeaderHeight=True,
                )
                _track_col(col, has_header=True)
        except Exception:
            pass

        for hidden in (
            "run_id",
            "icon_uri",
            "symbol_full",
            "config_json",
            "market_type",
            "strategy_name",
            "strategy_version",
            "start_time",
            "end_time",
            "sweep_id",
            "sweep_exchange_id",
            "sweep_scope",
            "sweep_category_id",
            "base_asset",
            "quote_asset",
            "asset",
            "market",
        ):
            if hidden in df.columns:
                gb.configure_column(hidden, hide=True)
                _track_col(hidden, has_header=False)

        if debug_grid:
            debug_logger.info("sweeps_runs.grid_columns configured=%s", sorted(configured_cols))
            debug_logger.info(
                "sweeps_runs.grid_headers %s",
                {c: configured_headers.get(c, False) for c in sorted(configured_cols)},
            )
            debug_logger.info(
                "sweeps_runs.grid_pills timeframe=%s direction=%s",
                timeframe_pill_applied,
                direction_pill_applied,
            )

        grid_options_runs = gb.build()
        try:
            if pre_selected_run_rows:
                grid_options_runs["preSelectedRows"] = list(pre_selected_run_rows[:1])
        except Exception:
            pass

        # Force visual selection (checkbox + highlight) on first render for the runs grid.
        if wanted_run_id:
            try:
                grid_options_runs["getRowId"] = JsCode(
                    """
                    function(params) {
                        try {
                            return (params && params.data && params.data.run_id !== undefined && params.data.run_id !== null)
                                ? params.data.run_id.toString()
                                : undefined;
                        } catch (e) { return undefined; }
                    }
                    """
                )
                grid_options_runs["onFirstDataRendered"] = JsCode(
                    f"""
                    function(params) {{
                        const target = {json.dumps(wanted_run_id)};
                        if (!target) return;
                        try {{
                            params.api.forEachNode(function(node) {{
                                try {{
                                    const rid = (node && node.data && node.data.run_id !== undefined && node.data.run_id !== null)
                                        ? node.data.run_id.toString()
                                        : '';
                                    if (rid === target) {{
                                        node.setSelected(true, true);
                                        if (node.rowIndex !== undefined && node.rowIndex !== null) {{
                                            params.api.ensureIndexVisible(node.rowIndex, 'middle');
                                        }}
                                    }}
                                }} catch (e) {{}}
                            }});
                        }} catch (e) {{}}
                    }}
                    """
                )
            except Exception:
                pass

        # Restore persisted column sizing/order (sanitized) for the sweeps runs grid.
        saved_state = st.session_state.get("sweeps_runs_columns_state")
        columns_state = None
        if isinstance(saved_state, list) and saved_state:
            try:
                sanitized: list[dict] = []
                valid_ids = {str(c) for c in df.columns.tolist()}
                for item in saved_state:
                    if not isinstance(item, dict):
                        continue
                    d = dict(item)
                    d.pop("hide", None)
                    d.pop("headerName", None)
                    col_id = d.get("colId") or d.get("col_id") or d.get("field")
                    # If a column is no longer in the dataframe, drop it from the restored state.
                    if str(col_id) not in valid_ids:
                        continue
                    sanitized.append(d)
                columns_state = sanitized or None
            except Exception:
                columns_state = None

        # Runs grid controls (compact one-line bar) – directly above the runs grid.
        runs_controls = st.columns([1, 2, 6], gap="small", vertical_alignment="bottom")
        with runs_controls[0]:
            picker_cols = list(candidate_cols)

            persisted = st.session_state.get("sweeps_runs_visible_cols")
            if isinstance(persisted, list) and persisted:
                persisted_base = [c for c in persisted if c in picker_cols and c != "market"]
            else:
                persisted_base = [c for c in visible_cols if c in picker_cols and c != "market"]

            chosen_base: list[str] = []
            with st.popover("Cols", help="Choose visible Runs columns"):
                for c in picker_cols:
                    label = col_label(c)
                    checked = st.checkbox(
                        label,
                        value=(c in persisted_base),
                        key=f"sweeps_runs_col_{selected_sweep_id}_{c}",
                    )
                    if checked:
                        chosen_base.append(c)

            chosen_visible_ui = [c for c in picker_cols if c in chosen_base]
            for c in always_visible_cols:
                if c not in chosen_visible_ui:
                    chosen_visible_ui.append(c)

            last_persisted = st.session_state.get("sweeps_runs_visible_cols_last_persisted")
            if isinstance(chosen_visible_ui, list) and chosen_visible_ui and chosen_visible_ui != last_persisted:
                try:
                    with open_db_connection() as _prefs_conn:
                        set_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.visible_cols", list(chosen_visible_ui))
                    st.session_state["sweeps_runs_visible_cols"] = list(chosen_visible_ui)
                    st.session_state["sweeps_runs_visible_cols_last_persisted"] = list(chosen_visible_ui)
                    st.rerun()
                except Exception:
                    pass

        with runs_controls[1]:
            btns = st.columns([1, 1], gap="small")

            def _request_save_layout_runs() -> None:
                st.session_state["sweeps_runs_save_layout_requested"] = True

            with btns[0]:
                st.button(
                    "Save",
                    key=f"sweeps_runs_save_layout_{selected_sweep_id}",
                    on_click=_request_save_layout_runs,
                    help="Save Runs column layout",
                    use_container_width=True,
                )

            with btns[1]:
                if st.button(
                    "Reset",
                    key=f"sweeps_runs_reset_layout_{selected_sweep_id}",
                    help="Reset Runs columns + layout",
                    use_container_width=True,
                ):
                    try:
                        with open_db_connection() as _prefs_conn:
                            set_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.visible_cols", None)
                            set_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.columns_state", None)
                    except Exception:
                        pass
                    for k in (
                        "sweeps_runs_visible_cols",
                        "sweeps_runs_visible_cols_last_persisted",
                        "sweeps_runs_columns_state",
                        "sweeps_runs_columns_state_last_persisted",
                        "sweeps_runs_prefs_loaded",
                    ):
                        st.session_state.pop(k, None)
                    st.rerun()

        with runs_controls[2]:
            st.caption("Runs grid settings")

        grid_runs = AgGrid(
            df,
            gridOptions=grid_options_runs,
            theme="dark",
            custom_css=custom_css,
            update_mode=(
                GridUpdateMode.SELECTION_CHANGED
                | GridUpdateMode.VALUE_CHANGED
                | GridUpdateMode.MODEL_CHANGED
                | getattr(GridUpdateMode, "COLUMN_RESIZED", 0)
            ),
            data_return_mode=DataReturnMode.AS_INPUT,
            fit_columns_on_grid_load=False,
            allow_unsafe_jscode=True,
            height=520,
            # Bump key version to ensure schema changes (e.g., dropping sweep_* columns)
            # fully remount the component and don't get "stuck" on a previous column set.
            key="sweep_runs_grid_v2",
            enable_enterprise_modules=use_enterprise,
            columns_state=columns_state,
        )

        # Persist sweeps-runs layout only on explicit request (Results-style).
        if st.session_state.get("sweeps_runs_save_layout_requested"):
            try:
                new_state = _extract_aggrid_columns_state(grid_runs)
                if isinstance(new_state, list) and new_state:
                    sanitized_state: list[dict] = []
                    for item in new_state:
                        if not isinstance(item, dict):
                            continue
                        col_id = item.get("colId") or item.get("col_id") or item.get("field")
                        d = dict(item)
                        d.pop("hide", None)
                        d.pop("headerName", None)
                        if "width" not in d:
                            try:
                                d["width"] = int(d.get("actualWidth") or 0) or d.get("width")
                            except Exception:
                                pass
                        sanitized_state.append(d)
                    with open_db_connection() as _prefs_conn:
                        set_user_setting(_prefs_conn, user_id, "ui.sweeps_runs.columns_state", sanitized_state)
                    st.session_state["sweeps_runs_columns_state"] = sanitized_state
                    st.session_state["sweeps_runs_columns_state_last_persisted"] = sanitized_state
                    st.success("Layout saved.")
            except Exception as exc:
                st.error(f"Failed to save layout: {exc}")
            finally:
                st.session_state.pop("sweeps_runs_save_layout_requested", None)

        grid_data_raw = grid_runs.get("data")
        if isinstance(grid_data_raw, pd.DataFrame):
            try:
                grid_data = grid_data_raw.to_dict("records")
            except Exception:
                grid_data = []
        elif isinstance(grid_data_raw, list):
            grid_data = grid_data_raw
        else:
            try:
                grid_data = list(grid_data_raw) if grid_data_raw is not None else []
            except Exception:
                grid_data = []

        # (Actions removed) Keep selection navigation simple.

        baseline_key = f"sweeps_runs_shortlist_baseline_{selected_sweep_id}"
        baseline = st.session_state.get(baseline_key)
        if not isinstance(baseline, dict):
            baseline = {}
            if not df.empty and "run_id" in df.columns and "shortlist" in df.columns:
                for _, r in df.iterrows():
                    rid = str(r.get("run_id") or r.get("id") or r.get("runId") or "").strip()
                    if not rid:
                        continue
                    baseline[rid] = {
                        "shortlist": bool(r.get("shortlist")),
                        "note": str(r.get("shortlist_note") or "").strip(),
                    }
            st.session_state[baseline_key] = baseline

        # Persist shortlist edits (avoid pandas conversion on every rerun).
        did_shortlist_change = False
        changed_flags: Dict[str, bool] = {}
        changed_notes: Dict[str, str] = {}
        if isinstance(grid_data, list) and baseline:
            for row in grid_data:
                if not isinstance(row, dict):
                    continue
                rid = str(row.get("run_id") or row.get("id") or row.get("runId") or "").strip()
                if not rid:
                    continue
                old = baseline.get(rid)
                if old is None:
                    continue
                cur_shortlisted = bool(row.get("shortlist") or False)
                cur_note = str(row.get("shortlist_note") or "").strip()
                old_shortlisted = bool(old.get("shortlist") or False)
                old_note = str(old.get("note") or "").strip()
                if old_shortlisted != cur_shortlisted or old_note != cur_note:
                    changed_flags[rid] = cur_shortlisted
                    changed_notes[rid] = cur_note

        if changed_flags:
            try:
                for rid, flag in changed_flags.items():
                    update_run_shortlist_fields(
                        rid,
                        shortlist=bool(flag),
                        shortlist_note=str(changed_notes.get(rid) or "").strip(),
                        user_id=user_id,
                    )
                    baseline[rid] = {
                        "shortlist": bool(flag),
                        "note": str(changed_notes.get(rid) or "").strip(),
                    }
                st.session_state[baseline_key] = baseline
                did_shortlist_change = True
            except Exception as exc:
                st.error(f"Failed to persist shortlist changes: {exc}")

        selected_rows_raw = grid_runs.get("selected_rows")
        selected_rows: list[dict[str, Any]] = []
        if isinstance(selected_rows_raw, list):
            selected_rows = selected_rows_raw
        elif isinstance(selected_rows_raw, pd.DataFrame):
            try:
                selected_rows = selected_rows_raw.to_dict("records")
            except Exception:
                selected_rows = []

        active_run_id = str(st.session_state.get("sweeps_active_run_id") or "").strip()
        if deep_link_run_int is not None and not active_run_id:
            active_run_id = str(deep_link_run_int)
        if selected_rows:
            try:
                new_active = str(
                    selected_rows[0].get("run_id")
                    or selected_rows[0].get("id")
                    or selected_rows[0].get("runId")
                    or ""
                ).strip()
            except Exception:
                new_active = ""
            edited_ids = set(changed_flags.keys()) if isinstance(changed_flags, dict) else set()
            # Navigate on row selection changes. Only block if the selected row was edited
            # (shortlist value change) in the same rerun, or if an action click rerun is in-flight.
            if new_active and new_active != active_run_id and (new_active not in edited_ids):
                st.session_state["sweeps_active_run_id"] = new_active
                st.session_state["deep_link_run_id"] = new_active
                st.session_state["current_page"] = "Run"
                _update_query_params(page="Run", run_id=new_active)
                st.rerun()

        # --- Sweep actions (bottom of page) ------------------------------
        st.markdown("---")
        with st.container(border=True):
            st.markdown("#### Sweep actions")

            a = st.columns([1, 1, 3])

            # Re-run / edit sweep: open Backtest page prefilled from this sweep definition.
            if a[0].button("Re-run sweep", key=f"rerun_sweep_btn_{selected_sweep_id}"):
                try:
                    _apply_sweep_definition_to_backtest_ui(sweep_row or {}, settings=settings)
                    st.session_state["current_page"] = "Backtest"
                    st.session_state["deep_link_sweep_id"] = str(selected_sweep_id)
                    _update_query_params(page="Backtest", sweep_id=str(selected_sweep_id), run_id=None)
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to open Backtest from sweep: {exc}")

            # Delete sweep (and all runs) with checkbox confirmation.
            confirm_delete = st.checkbox(
                "I understand this will delete the sweep and all its runs",
                value=False,
                key=f"confirm_delete_sweep_{selected_sweep_id}",
            )
            if a[1].button(
                "Delete sweep + runs",
                key=f"delete_sweep_btn_{selected_sweep_id}",
                type="primary",
                disabled=not bool(confirm_delete),
            ):
                try:
                    from project_dragon.storage import delete_sweep_and_runs  # local import

                    res = delete_sweep_and_runs(user_id=user_id, sweep_id=str(selected_sweep_id))
                    st.success(
                        f"Deleted sweep. runs={int(res.get('runs_deleted') or 0)}, details={int(res.get('details_deleted') or 0)}"
                    )
                except Exception as exc:
                    st.error(f"Delete failed: {exc}")
                    return

                # Clear selection + deep links so the page doesn't try to render deleted content.
                for k in (
                    "deep_link_sweep_id",
                    "sweeps_selected_sweep_id",
                    "sweeps_list_selected_ids",
                    "sweeps_active_run_id",
                    "deep_link_run_id",
                ):
                    st.session_state.pop(k, None)
                _update_query_params(page="Sweeps", sweep_id=None, run_id=None)
                st.rerun()

            a[2].caption("Re-run opens Backtest prefilled; delete is irreversible.")

        # (Removed) Selected run section: selection already navigates to Run detail.

    if current_page == "Run":
        qp_run_id = _extract_query_param(st.query_params, "run_id")
        run_id = str(qp_run_id or st.session_state.get("deep_link_run_id") or "").strip()
        if not run_id:
            st.info("No run selected. Open a run from Results or Sweeps.")
            return

        # Charts load immediately on the Run detail page.
        show_charts = True

        with _perf("run.get_run"):
            run = get_backtest_run(run_id)
        if not run:
            st.warning(f"Run not found: {run_id}")
            return

        with _perf("run.render_detail"):
            with open_db_connection() as conn:
                render_run_detail_from_db(
                    conn,
                    run,
                    show_charts=show_charts,
                    chart_downsample_keep_frac=0.75,
                )

    if current_page == "Candle Cache":
        st.subheader("Candle cache")
        tz = _ui_display_tz()
        exchange_options = ["woox", "binance", "bybit", "okx", "kraken", "Custom…"]
        exchange_choice = st.selectbox("Exchange", exchange_options, index=0, key="cache_exchange_choice")
        if exchange_choice == "Custom…":
            exchange_id = st.text_input("Custom exchange id", value="woox", key="cache_exchange_custom")
        else:
            exchange_id = exchange_choice
        exchange_id = normalize_exchange_id(exchange_id) or (str(exchange_id or "").strip().lower())

        markets: dict = {}
        exchange_timeframes: list[str] = []
        try:
            with _perf("candle_cache.metadata"):
                markets, exchange_timeframes = fetch_ccxt_metadata(exchange_id)
        except Exception as exc:
            st.error(f"Failed to load markets for '{exchange_id}': {exc}")
            markets = {}
            exchange_timeframes = []

        market_type_choice = st.selectbox("Market type", ["Perps", "Spot", "All"], index=0, key="cache_market_type")

        symbol_map: dict[str, str] = {}
        for market in markets.values():
            symbol_val = market.get("symbol")
            if not symbol_val:
                continue
            is_perp = bool(market.get("swap") or market.get("future"))
            if market_type_choice == "Perps" and not is_perp:
                continue
            if market_type_choice == "Spot" and is_perp:
                continue
            label_suffix = "PERP" if is_perp else "SPOT"
            symbol_map[f"{symbol_val} [{label_suffix}]"] = symbol_val
        symbol_options = sorted(symbol_map.keys())

        preferred_label = "ETH/USDT:USDT [PERP]"
        if symbol_options:
            options_with_custom = symbol_options + ["Custom…"]
            default_symbol_index = options_with_custom.index(preferred_label) if preferred_label in options_with_custom else 0
            symbol_choice = st.selectbox("Symbol", options_with_custom, index=default_symbol_index, key="cache_symbol_choice")
        else:
            symbol_choice = st.selectbox("Symbol", ["Custom…"], index=0, key="cache_symbol_choice")

        if symbol_choice == "Custom…":
            symbol_value = st.text_input("Custom symbol", value="BTC/USDT", key="cache_symbol_custom")
        else:
            symbol_value = symbol_map[symbol_choice]

        timeframe_input = st.text_input("Timeframe", value="1h")
        default_end = datetime.now(timezone.utc)
        default_start = default_end - timedelta(days=7)
        start_dt = st.datetime_input("Start (UTC)", value=default_start)
        end_dt = st.datetime_input("End (UTC)", value=default_end)
        keep_months = st.number_input("Keep months (for purge)", value=6, min_value=1, step=1)

        def _dt_to_ms(dt_val: Optional[datetime]) -> Optional[int]:
            if not dt_val:
                return None
            return int(dt_val.timestamp() * 1000)

        if st.button("Download/refresh", type="primary"):
            if start_dt and end_dt and start_dt >= end_dt:
                st.error("End must be after start.")
            else:
                try:
                    candles_cached = get_candles_with_cache(
                        exchange_id=exchange_id,
                        symbol=symbol_value,
                        timeframe=timeframe_input,
                        since=_dt_to_ms(start_dt),
                        until=_dt_to_ms(end_dt),
                        range_mode="date range",
                        market_type=market_type_choice.lower(),
                    )
                    st.success(f"Fetched {len(candles_cached)} candles and cached them.")
                except Exception as exc:
                    st.error(f"Download failed: {exc}")

        cols_cache = st.columns(2)
        if cols_cache[0].button("Show coverage"):
            with open_db_connection() as conn:
                min_ts, max_ts = get_cached_coverage(
                    conn,
                    exchange_id,
                    market_type_choice.lower(),
                    symbol_value,
                    timeframe_input,
                )
            if min_ts is None or max_ts is None:
                st.info("No cache coverage for this selection yet.")
            else:
                st.success(f"Coverage: {fmt_dt(min_ts, tz)} → {fmt_dt(max_ts, tz)}")

        if cols_cache[1].button("Purge old data"):
            with open_db_connection() as conn:
                deleted = purge_old_candles(conn, int(keep_months))
            st.warning(f"Deleted {deleted} cached candles older than {keep_months} months.")

        with open_db_connection() as conn:
            cache_rows = list_cached_coverages(conn)
        if cache_rows:
            for row in cache_rows:
                for key in ("min_ts", "max_ts"):
                    if row.get(key) is not None:
                        row[key] = fmt_dt(row[key], tz)
            st.dataframe(pd.DataFrame(cache_rows), hide_index=True, width="stretch")
        else:
            st.info("No cached candles yet.")

    if current_page == "Jobs":
        st.subheader("Jobs")
        auto = st.toggle("Auto refresh", value=True)

        st.caption("Backtest jobs run in `project_dragon.backtest_worker`; live bots run in `project_dragon.live_worker`.")

        st.caption("Columns: id · type · status · interactive · priority · symbol · timeframe · sweep · progress")

        def _render_job_row(job: dict, sweeps: dict, allow_cancel: bool = True) -> None:
            job_id = job.get("id")
            job_type = job.get("job_type", "?")
            status = job.get("status", "?")
            payload_raw = job.get("payload_json") or job.get("payload")
            payload: dict = {}
            if isinstance(payload_raw, str):
                try:
                    payload = json.loads(payload_raw)
                except json.JSONDecodeError:
                    payload = {}
            data_settings = payload.get("data_settings", {}) if isinstance(payload, dict) else {}
            metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
            symbol = data_settings.get("symbol") or metadata.get("symbol") or ""
            timeframe = data_settings.get("timeframe") or metadata.get("timeframe") or ""
            sweep_id = payload.get("sweep_id") if isinstance(payload, dict) else None
            sweep_name = None
            if sweep_id is not None:
                sweep_row = sweeps.get(str(sweep_id))
                if sweep_row:
                    sweep_name = sweep_row.get("name") or f"Sweep {sweep_id}"
            run_id = job.get("run_id")
            bot_id = job.get("bot_id")
            priority = job.get("priority")
            is_interactive = job.get("is_interactive")
            raw_prog = job.get("progress", 0.0)
            try:
                prog = float(raw_prog or 0.0)
            except (TypeError, ValueError):
                prog = 0.0
            prog = max(0.0, min(1.0, prog))
            msg = (job.get("message") or "").strip()

            cols = st.columns([1, 2, 2, 1, 1, 2, 2, 2, 3, 1, 1, 1, 1])
            cols[0].write(f"#{job_id}")
            cols[1].write(job_type)
            cols[2].write(status)
            cols[3].write("YES" if int(is_interactive or 0) == 1 else "")
            cols[4].write(str(priority) if priority is not None else "")
            cols[5].write(symbol or "–")
            cols[6].write(timeframe or "–")
            cols[7].write(sweep_name or "–")
            cols[8].progress(prog, text=msg or None)

            if sweep_id is not None:
                if cols[9].button("Open sweep", key=f"open_sweep_{job_id}"):
                    # Persist deep link + selection so Sweeps tab focuses the target row even if a prior selection exists
                    sweep_id_str = str(sweep_id).strip()
                    st.session_state["deep_link_sweep_id"] = sweep_id_str
                    st.session_state["sweeps_selected_sweep_id"] = sweep_id_str
                    st.session_state["sweeps_list_selected_ids"] = [sweep_id_str]
                    st.session_state.pop(f"sweep_runs_{sweep_id}_selected_ids", None)
                    st.session_state.pop("results_runs_selected_ids", None)
                    st.session_state["current_page"] = "Sweeps"
                    _update_query_params(page="Sweeps", sweep_id=sweep_id_str, run_id=None, view=None, bot_id=None)
                    st.rerun()
            else:
                cols[9].write("")
            if run_id is not None:
                if cols[10].button("Open run", key=f"open_run_{job_id}"):
                    # Persist deep link + selection so Results tab focuses the target row even if a prior selection exists
                    try:
                        run_id_int = int(run_id)
                    except (TypeError, ValueError):
                        run_id_int = None
                    st.session_state["deep_link_run_id"] = run_id
                    st.session_state["results_runs_selected_ids"] = [
                        run_id_int if run_id_int is not None else run_id
                    ]
                    st.session_state.pop("sweeps_list_selected_ids", None)
                    st.session_state["current_page"] = "Results"
                    _update_query_params(page="Results", run_id=run_id, sweep_id=None, view=None, bot_id=None)
                    st.rerun()
            else:
                cols[10].write("")

            if bot_id is not None:
                if cols[11].button("Open bot", key=f"open_bot_{job_id}"):
                    try:
                        bot_id_int = int(bot_id)
                    except (TypeError, ValueError):
                        bot_id_int = bot_id
                    st.session_state["current_page"] = "Live Bots"
                    _set_selected_bot(bot_id_int)
                    st.rerun()
            else:
                cols[11].write("")

            err_text = str(job.get("error_text") or "").strip()
            if err_text:
                with st.expander(f"Job #{job_id} error", expanded=False):
                    st.code(err_text)

            if allow_cancel and status in {"queued", "running", "cancel_requested"}:
                if cols[12].button("Cancel", key=f"cancel_job_{job_id}"):
                    with _job_conn() as conn:
                        request_cancel_job(conn, int(job_id))
                    st.info(f"Cancel requested for job #{job_id}")
                    st.rerun()
            else:
                cols[12].write("")

        def _render_jobs_once() -> None:
            with _job_conn() as conn:
                # NOTE: `list_jobs()` is limited; for correctness (and to avoid showing
                # 0 active when active jobs exist beyond the limit), query active + history separately.
                try:
                    total_jobs = int(conn.execute("SELECT COUNT(*) AS n FROM jobs").fetchone()[0])
                except Exception:
                    total_jobs = 0

                def _rows_to_jobs(rows: list[Any]) -> list[dict[str, Any]]:
                    out: list[dict[str, Any]] = []
                    for r in rows or []:
                        try:
                            d = dict(r)
                        except Exception:
                            continue
                        if d.get("run_id") is not None:
                            d["run_id"] = str(d["run_id"])
                        out.append(d)
                    return out

                active_statuses = ("queued", "running", "cancel_requested")
                active_rows = execute_fetchall(
                    conn,
                    """
                    SELECT * FROM jobs
                    WHERE status IN ('queued','running','cancel_requested')
                    ORDER BY COALESCE(updated_at, created_at) DESC
                    """,
                )
                history_rows = execute_fetchall(
                    conn,
                    """
                    SELECT * FROM jobs
                    WHERE status NOT IN ('queued','running','cancel_requested')
                    ORDER BY COALESCE(NULLIF(created_at, ''), NULLIF(started_at, ''), NULLIF(finished_at, '')) DESC NULLS LAST, id DESC
                    LIMIT 20
                    """,
                )

                jobs_running = _rows_to_jobs(active_rows)
                jobs_history = _rows_to_jobs(history_rows)
                sweeps = {str(row.get("id")): row for row in list_sweeps()} if "list_sweeps" in globals() else {}

            st.caption(f"Total jobs in DB: {total_jobs}")

            backtest_types = {"backtest_run", "sweep_parent"}
            bot_types = {"live_bot"}
            active_backtests = [j for j in jobs_running if str(j.get("job_type") or "") in backtest_types]
            active_bots = [j for j in jobs_running if str(j.get("job_type") or "") in bot_types]
            active_other = [
                j
                for j in jobs_running
                if j not in active_backtests and j not in active_bots
            ]

            st.markdown(f"### Active backtests ({len(active_backtests)})")
            if not active_backtests:
                st.info("No active backtests.")
            else:
                limit_active_backtests = 20
                if len(active_backtests) > limit_active_backtests:
                    st.caption(f"Showing {limit_active_backtests} of {len(active_backtests)} active backtests")
                for job in active_backtests[:limit_active_backtests]:
                    _render_job_row(job, sweeps, allow_cancel=True)

            st.divider()
            st.markdown(f"### Active bots ({len(active_bots)})")
            if not active_bots:
                st.info("No active bots.")
            else:
                for job in active_bots:
                    _render_job_row(job, sweeps, allow_cancel=True)

            if active_other:
                st.divider()
                st.markdown(f"### Active other ({len(active_other)})")
                for job in active_other:
                    _render_job_row(job, sweeps, allow_cancel=True)

            st.divider()
            st.markdown("### Recent jobs (history)")
            if not jobs_history:
                st.info("No past jobs yet.")
            else:
                for job in jobs_history:
                    _render_job_row(job, sweeps, allow_cancel=False)

        def jobs_panel() -> None:
            refresh_seconds = max(0.5, float(st.session_state.get("jobs_refresh_seconds", jobs_refresh_seconds)))
            if auto:
                # Auto mode: schedule periodic reruns via fragment; key isolates the auto refresher
                @st.fragment(run_every=timedelta(seconds=refresh_seconds))
                def _render_auto() -> None:
                    _render_jobs_once()

                _render_auto()
            else:
                # Manual mode: render once without scheduling reruns
                _render_jobs_once()

        jobs_panel()

    if current_page == "Settings":
        st.subheader("Settings")
        with open_db_connection() as conn:
            current_settings = get_app_settings(conn)
            init_bal_val = float(current_settings.get("initial_balance_default", 1000.0))
            fee_spot_val = float(current_settings.get("fee_spot_pct", 0.10))
            fee_perps_val = float(current_settings.get("fee_perps_pct", 0.06))
            fee_stocks_val = float(current_settings.get("fee_stocks_pct", 0.10))
            jobs_refresh_val = float(current_settings.get("jobs_refresh_seconds", jobs_refresh_seconds))
            prefer_bbo_val = bool(current_settings.get("prefer_bbo_maker", True))
            bbo_level_val = int(current_settings.get("bbo_queue_level", 1))
            live_kill_val = bool(current_settings.get("live_kill_switch_enabled", False))
            live_trading_enabled_val = bool(current_settings.get("live_trading_enabled", False))

            display_tz_name_val = get_user_setting(conn, user_id, "display_timezone", default=DEFAULT_DISPLAY_TZ_NAME)

            left_col, right_col = st.columns(2)

            with left_col:
                with st.container(border=True):
                    st.markdown("### Live trading")
                    st.warning(
                        "LIVE TRADING ENABLED (Worker Processing): When OFF, the live worker will not process bars or place/cancel orders.\n\n"
                        "This is separate from the kill switch."
                    )
                    live_trading_enabled_input = st.toggle(
                        "Enable Live Trading (Worker Processing)",
                        value=live_trading_enabled_val,
                        key="settings_live_trading_enabled",
                    )
                    st.caption(f"Current: {'ON' if live_trading_enabled_input else 'OFF'}")

                    st.error(
                        "LIVE KILL SWITCH: When enabled, the live worker will block ALL order placement/cancels (including flatten).\n\n"
                        "Use this if you suspect something is wrong and want to freeze trading immediately."
                    )
                    live_kill_input = st.toggle(
                        "Enable live kill switch",
                        value=live_kill_val,
                        key="settings_live_kill_switch",
                    )

                with st.container(border=True):
                    st.markdown("### Display")

                    tz_common = [
                        "Australia/Brisbane",
                        "UTC",
                        "Australia/Sydney",
                        "Australia/Melbourne",
                        "Asia/Singapore",
                        "Asia/Hong_Kong",
                        "Asia/Tokyo",
                        "Europe/London",
                        "Europe/Berlin",
                        "America/New_York",
                        "America/Chicago",
                        "America/Los_Angeles",
                    ]
                    tz_current = str(display_tz_name_val or "").strip() or DEFAULT_DISPLAY_TZ_NAME
                    if tz_current not in tz_common:
                        tz_options = [tz_current] + tz_common
                    else:
                        tz_options = tz_common
                    display_tz_input = st.selectbox(
                        "Display timezone",
                        options=tz_options,
                        index=max(0, tz_options.index(tz_current)) if tz_current in tz_options else 0,
                        help="Applies to all timestamps in the UI. Stored per user. DB remains UTC.",
                        key="settings_display_timezone",
                    )

                with st.container(border=True):
                    st.markdown("### Periods")
                    periods = list_saved_periods(conn, user_id)
                    if periods:
                        df_periods = pd.DataFrame(periods)
                        show_cols = [c for c in ["name", "start_ts", "end_ts", "updated_at"] if c in df_periods.columns]
                        st.dataframe(df_periods[show_cols], hide_index=True, width="stretch")
                    else:
                        st.info("No saved periods yet.")

                    period_ids: list[int] = []
                    period_rows_by_id: Dict[int, dict] = {}
                    for p in periods or []:
                        try:
                            pid = int(p.get("id"))
                        except Exception:
                            continue
                        name = str(p.get("name") or "").strip()
                        if not name:
                            continue
                        if pid not in period_rows_by_id:
                            period_rows_by_id[pid] = p
                            period_ids.append(pid)

                    selected_id = st.selectbox(
                        "Saved periods",
                        options=[None] + period_ids,
                        index=0,
                        key="settings_period_selected",
                        format_func=lambda pid: "(new)" if pid is None else str((period_rows_by_id.get(int(pid)) or {}).get("name") or "").strip(),
                    )
                    selected_row = None
                    if selected_id is not None:
                        selected_row = period_rows_by_id.get(int(selected_id))

                    name_default = str((selected_row or {}).get("name") or "") if selected_row else ""
                    start_default = _normalize_timestamp((selected_row or {}).get("start_ts")) or datetime.now(timezone.utc) - timedelta(days=7)
                    end_default = _normalize_timestamp((selected_row or {}).get("end_ts")) or datetime.now(timezone.utc)

                    period_name = st.text_input("Name", value=name_default, key="settings_period_name")
                    start_dt = st.datetime_input("Start (UTC)", value=start_default, key="settings_period_start")
                    end_dt = st.datetime_input("End (UTC)", value=end_default, key="settings_period_end")
                    if start_dt and end_dt and start_dt >= end_dt:
                        st.warning("End time must be after start time.")

                    btns = st.columns(3)
                    with btns[0]:
                        if st.button("Save period", key="settings_period_save"):
                            if not str(period_name or "").strip():
                                st.error("Name is required.")
                            elif start_dt >= end_dt:
                                st.error("End time must be after start time.")
                            else:
                                create_saved_period(
                                    conn,
                                    user_id=user_id,
                                    name=str(period_name).strip(),
                                    start_ts=start_dt.astimezone(timezone.utc).isoformat(),
                                    end_ts=end_dt.astimezone(timezone.utc).isoformat(),
                                )
                                st.success("Saved period.")
                                st.rerun()
                    with btns[1]:
                        if st.button("Update", key="settings_period_update", disabled=selected_id is None):
                            if selected_id is None:
                                st.error("Select a period to update.")
                            elif not str(period_name or "").strip():
                                st.error("Name is required.")
                            elif start_dt >= end_dt:
                                st.error("End time must be after start time.")
                            else:
                                update_saved_period(
                                    conn,
                                    user_id=user_id,
                                    period_id=int(selected_id),
                                    name=str(period_name).strip(),
                                    start_ts=start_dt.astimezone(timezone.utc).isoformat(),
                                    end_ts=end_dt.astimezone(timezone.utc).isoformat(),
                                )
                                st.success("Updated period.")
                                st.rerun()
                    with btns[2]:
                        if st.button("Delete", key="settings_period_delete", disabled=selected_id is None):
                            if selected_id is None:
                                st.error("Select a period to delete.")
                            else:
                                delete_saved_period(conn, user_id=user_id, period_id=int(selected_id))
                                st.success("Deleted period.")
                                st.rerun()

            with right_col:
                with st.container(border=True):
                    st.markdown("### Defaults & fees")
                    init_bal_input = st.number_input(
                        "Default initial balance",
                        value=float(st.session_state.get("settings_initial_balance_default", init_bal_val)),
                        min_value=0.0,
                        step=100.0,
                        key="settings_initial_balance_default",
                    )
                    fee_spot_input = st.number_input(
                        "Spot fee (%)",
                        value=float(st.session_state.get("settings_fee_spot_pct", fee_spot_val)),
                        min_value=0.0,
                        step=0.01,
                        format="%.4f",
                        key="settings_fee_spot_pct",
                    )
                    fee_perps_input = st.number_input(
                        "Perps fee (%)",
                        value=float(st.session_state.get("settings_fee_perps_pct", fee_perps_val)),
                        min_value=0.0,
                        step=0.01,
                        format="%.4f",
                        key="settings_fee_perps_pct",
                    )
                    fee_stocks_input = st.number_input(
                        "Stocks fee (%)",
                        value=float(st.session_state.get("settings_fee_stocks_pct", fee_stocks_val)),
                        min_value=0.0,
                        step=0.01,
                        format="%.4f",
                        key="settings_fee_stocks_pct",
                    )

                with st.container(border=True):
                    st.markdown("### Jobs")
                    jobs_refresh_input = st.number_input(
                        "Jobs auto-refresh interval (seconds)",
                        value=float(st.session_state.get("settings_jobs_refresh_seconds", jobs_refresh_val)),
                        min_value=0.5,
                        step=0.5,
                        format="%.1f",
                        help="Polling interval for live jobs; set higher to reduce DB/API load.",
                        key="settings_jobs_refresh_seconds",
                    )

                with st.container(border=True):
                    st.markdown("### Execution")
                    prefer_bbo_input = st.toggle(
                        "Prefer maker via BBO (Queue)",
                        value=prefer_bbo_val,
                        key="settings_prefer_bbo_maker",
                    )
                    bbo_level_input = st.selectbox(
                        "BBO queue level",
                        options=[1, 2, 3, 4, 5],
                        index=max(0, min(4, bbo_level_val - 1)),
                        key="settings_bbo_queue_level",
                    )

            st.markdown("---")
            if st.button("Save settings", type="primary"):
                set_setting(conn, "live_trading_enabled", bool(live_trading_enabled_input))
                set_setting(conn, "live_kill_switch_enabled", bool(live_kill_input))
                set_setting(conn, "initial_balance_default", float(init_bal_input))
                set_setting(conn, "fee_spot_pct", float(fee_spot_input))
                set_setting(conn, "fee_perps_pct", float(fee_perps_input))
                set_setting(conn, "fee_stocks_pct", float(fee_stocks_input))
                set_setting(conn, "jobs_refresh_seconds", float(jobs_refresh_input))
                set_setting(conn, "prefer_bbo_maker", bool(prefer_bbo_input))
                set_setting(conn, "bbo_queue_level", int(bbo_level_input))
                set_user_setting(conn, user_id, "display_timezone", str(display_tz_input))
                st.session_state["display_timezone_name"] = str(display_tz_input)
                st.session_state["jobs_refresh_seconds"] = float(jobs_refresh_input)
                st.success("Settings saved.")
                st.rerun()

            st.markdown("---")
            with st.expander("Data Retention (Advanced)", expanded=False):
                st.caption("Safely reclaim DB space by clearing large artifacts or deleting low-performing runs.")

                # Defer sweep list load to keep Settings fast.
                enable_sweep_filtering = st.checkbox(
                    "Enable sweep filtering (loads up to 200 sweeps)",
                    help=(
                        "Optional: loads a sweep list so the Data Retention tools can be limited to specific sweeps. "
                        "This is disabled by default to keep the Settings page fast (it avoids querying the sweeps table)."
                    ),
                    value=bool(st.session_state.get("retention_enable_sweep_filtering", False)),
                    key="retention_enable_sweep_filtering",
                )

                sweep_options: list[str] = []
                sweep_id_by_label: Dict[str, str] = {}
                if enable_sweep_filtering:
                    sweep_rows = []
                    try:
                        with open_db_connection(read_only=True) as sweeps_conn:
                            sweep_rows = sweeps_conn.execute(
                                "SELECT id, name FROM sweeps ORDER BY created_at DESC LIMIT 200"
                            ).fetchall()
                        sweep_rows = [dict(r) for r in sweep_rows or []]
                    except Exception:
                        sweep_rows = []
                    for s in sweep_rows or []:
                        sid = str(s.get("id") or "").strip()
                        name = str(s.get("name") or "").strip()
                        if not sid:
                            continue
                        label = f"{sid} · {name}" if name else sid
                        sweep_options.append(label)
                        sweep_id_by_label[label] = sid

                with st.expander("Cleanup: Delete detailed backtest data (keep basics)", expanded=False):
                    col_a, col_b = st.columns([1, 2])
                    with col_a:
                        days_details = st.number_input(
                            "Older than (days)",
                            min_value=0,
                            step=1,
                            value=int(st.session_state.get("retention_details_days", 7)),
                            key="retention_details_days",
                        )
                    with col_b:
                        selected_sweeps = st.multiselect(
                            "Restrict to sweeps (optional)",
                            options=sweep_options,
                            disabled=(not enable_sweep_filtering),
                            key="retention_details_sweeps",
                        )
                    selected_sweep_ids = [
                        sweep_id_by_label.get(x) for x in selected_sweeps if sweep_id_by_label.get(x)
                    ]

                    preview_key = "retention_details_preview_count"
                    refresh_preview = st.button(
                        "Refresh preview",
                        key="retention_details_preview_btn",
                    )
                    if refresh_preview:
                        try:
                            st.session_state[preview_key] = count_runs_matching_filters(
                                older_than_days=int(days_details),
                                sweep_ids=selected_sweep_ids,
                                conn=conn,
                            )
                        except Exception:
                            st.session_state[preview_key] = 0
                    preview_count = st.session_state.get(preview_key)
                    if preview_count is None:
                        st.caption("Runs affected: — (click Refresh preview)")
                    else:
                        st.caption(f"Runs affected: {int(preview_count)}")

                    confirm_details = st.text_input(
                        "Type DELETE DETAILS to confirm",
                        value="",
                        key="retention_confirm_details",
                    )
                    if st.button(
                        "Delete detailed data",
                        type="primary",
                        disabled=(confirm_details.strip() != "DELETE DETAILS"),
                        key="retention_delete_details_btn",
                    ):
                        try:
                            res = clear_run_details_payloads(
                                older_than_days=int(days_details),
                                sweep_ids=selected_sweep_ids,
                            )
                            st.success(f"Cleared details for {int(res.get('details_updated') or 0)} run(s).")
                        except Exception as exc:
                            st.error(f"Retention failed: {exc}")

                with st.expander("Cleanup: Delete low-profit runs", expanded=False):
                    r1, r2, r3 = st.columns([1, 1, 1])
                    with r1:
                        profit_threshold = st.number_input(
                            "Profit threshold (USD)",
                            value=float(st.session_state.get("retention_profit_threshold", 0.0)),
                            step=1.0,
                            key="retention_profit_threshold",
                        )
                    with r2:
                        days_low_profit = st.number_input(
                            "Older than (days)",
                            min_value=0,
                            step=1,
                            value=int(st.session_state.get("retention_lowprofit_days", 14)),
                            key="retention_lowprofit_days",
                        )
                    with r3:
                        only_completed = st.toggle(
                            "Only completed runs",
                            value=bool(st.session_state.get("retention_only_completed", True)),
                            key="retention_only_completed",
                        )

                    roi_enabled = st.toggle(
                        "Apply ROI threshold",
                        value=bool(st.session_state.get("retention_roi_enabled", False)),
                        key="retention_roi_enabled",
                    )
                    roi_threshold = None
                    if roi_enabled:
                        roi_threshold = st.number_input(
                            "ROI threshold (%)",
                            value=float(st.session_state.get("retention_roi_threshold", 0.0)),
                            step=0.5,
                            key="retention_roi_threshold",
                        )

                    selected_sweeps_low = st.multiselect(
                        "Restrict to sweeps (optional)",
                        options=sweep_options,
                        disabled=(not enable_sweep_filtering),
                        key="retention_lowprofit_sweeps",
                    )
                    selected_sweep_ids_low = [
                        sweep_id_by_label.get(x) for x in selected_sweeps_low if sweep_id_by_label.get(x)
                    ]

                    preview_key_low = "retention_lowprofit_preview_count"
                    refresh_preview_low = st.button(
                        "Refresh preview",
                        key="retention_lowprofit_preview_btn",
                    )
                    if refresh_preview_low:
                        try:
                            st.session_state[preview_key_low] = count_runs_matching_filters(
                                older_than_days=int(days_low_profit),
                                sweep_ids=selected_sweep_ids_low,
                                max_profit=float(profit_threshold),
                                roi_threshold_pct=(float(roi_threshold) if roi_threshold is not None else None),
                                only_completed=bool(only_completed),
                                conn=conn,
                            )
                        except Exception:
                            st.session_state[preview_key_low] = 0
                    preview_low = st.session_state.get(preview_key_low)
                    if preview_low is None:
                        st.caption("Runs to delete: — (click Refresh preview)")
                    else:
                        st.caption(f"Runs to delete: {int(preview_low)}")

                    confirm_runs = st.text_input(
                        "Type DELETE RUNS to confirm",
                        value="",
                        key="retention_confirm_runs",
                    )
                    if st.button(
                        "Delete low-profit runs",
                        type="primary",
                        disabled=(confirm_runs.strip() != "DELETE RUNS"),
                        key="retention_delete_runs_btn",
                    ):
                        try:
                            res = delete_runs_and_children(
                                older_than_days=int(days_low_profit),
                                sweep_ids=selected_sweep_ids_low,
                                max_profit=float(profit_threshold),
                                roi_threshold_pct=(float(roi_threshold) if roi_threshold is not None else None),
                                only_completed=bool(only_completed),
                                batch_size=500,
                            )
                            st.success(
                                "Deleted runs. "
                                f"runs={int(res.get('runs_deleted') or 0)}, "
                                f"details={int(res.get('details_deleted') or 0)}, "
                                f"shortlists={int(res.get('shortlists_deleted') or 0)}, "
                                f"bot_map={int(res.get('bot_run_map_deleted') or 0)}"
                            )
                        except Exception as exc:
                            st.error(f"Delete failed: {exc}")

            st.markdown("---")
            st.markdown("## Assets")
            st.caption("Manage symbols and user-defined categories for multi-asset sweeps.")

            with st.container(border=True):
                st.markdown("### Crypto icons")
                st.caption("Uses the local spothq cryptocurrency icon pack. Icons are cached in the DB for fast rendering.")

                icons_force_refresh = st.checkbox(
                    "Force refresh cached icons",
                    value=False,
                    key="assets_icons_force_refresh",
                )
                if st.button("Sync missing crypto icons", key="assets_sync_crypto_icons"):
                    repo_root = Path(__file__).resolve().parents[2]
                    spothq_root = repo_root / "app" / "assets" / "crypto_icons" / "spothq" / "cryptocurrency-icons"
                    try:
                        updated = sync_symbol_icons_from_spothq_pack(
                            conn,
                            icons_root=str(spothq_root),
                            force=bool(icons_force_refresh),
                            max_updates=2000,
                        )
                        conn.commit()
                        st.success(f"Synced icons for {updated} asset(s).")
                        st.rerun()
                    except FileNotFoundError:
                        st.warning(
                            "spothq icon pack not found. Ensure it exists at app/assets/crypto_icons/spothq/cryptocurrency-icons."
                        )
                    except Exception as exc:
                        st.error(f"Icon sync failed: {exc}")

            # Exchange selector (canonical): prefer 'woox' and dedupe aliases.
            try:
                with open_db_connection(read_only=True) as _conn:
                    ex_rows = _conn.execute("SELECT DISTINCT exchange_id FROM assets ORDER BY exchange_id").fetchall()
                exchange_seen = [str(r[0]) for r in ex_rows or [] if r and str(r[0] or "").strip()]
            except Exception:
                exchange_seen = []
            exchange_defaults = ["woox"]
            exchange_options: list[str] = []
            for x in exchange_defaults + exchange_seen:
                nx = normalize_exchange_id(x) or str(x or "").strip().lower()
                if not nx:
                    continue
                if nx not in exchange_options:
                    exchange_options.append(nx)

            selected_exchange = st.selectbox(
                "Exchange ID",
                options=exchange_options if exchange_options else ["woox"],
                index=0,
                key="assets_exchange_id",
            )
            exchange_id_assets = normalize_exchange_id(selected_exchange) or (str(selected_exchange or "").strip().lower() or "woox")

            # --- Assets table + status controls ------------------------
            if "assets_page" not in st.session_state:
                st.session_state["assets_page"] = 1
            if "assets_page_size" not in st.session_state:
                st.session_state["assets_page_size"] = 200
            if "assets_filter_model" not in st.session_state:
                st.session_state["assets_filter_model"] = None
            if "assets_sort_model" not in st.session_state:
                st.session_state["assets_sort_model"] = None

            def _reset_assets_page() -> None:
                st.session_state["assets_page"] = 1

            # --- Categories + membership manager (grouped) -------------
            cats = []
            try:
                cats = list_categories(str(user_id or "").strip() or "admin@local", exchange_id_assets)
            except Exception:
                cats = []

            category_labels = [str(c.get("name") or "").strip() for c in cats if str(c.get("name") or "").strip()]
            category_filter_opts = ["All"] + category_labels
            chosen_category = st.selectbox(
                "Category filter",
                options=category_filter_opts,
                index=0,
                key="assets_category_filter",
                on_change=_reset_assets_page,
            )
            category_filter = None if chosen_category == "All" else chosen_category

            search_text = st.text_input(
                "Search",
                value=str(st.session_state.get("assets_search_text") or ""),
                key="assets_search_text",
                on_change=_reset_assets_page,
            )

            page_size_options = [50, 100, 200, 500]
            page_size = int(st.session_state.get("assets_page_size") or 200)
            if page_size not in page_size_options:
                page_size_options.append(page_size)
                page_size_options = sorted(set(page_size_options))
            page = int(st.session_state.get("assets_page") or 1)

            assets_rows = []
            total_assets = 0
            try:
                with open_db_connection(read_only=True) as _assets_conn:
                    assets_rows, total_assets = list_assets_server_side(
                        conn=_assets_conn,
                        user_id=str(user_id or "").strip() or "admin@local",
                        exchange_id=exchange_id_assets,
                        page=page,
                        page_size=page_size,
                        filter_model=st.session_state.get("assets_filter_model"),
                        sort_model=st.session_state.get("assets_sort_model"),
                        category_name=category_filter,
                        search_text=search_text,
                        status="all",
                    )
            except Exception:
                assets_rows = []
                total_assets = 0

            left_a, right_a = st.columns([2, 1])
            sym_rows: list[dict[str, Any]] = []
            with left_a:
                with st.container(border=True):
                    st.markdown("### Assets")
                    if assets_rows:
                        df_assets = pd.DataFrame(assets_rows)
                        if "categories" in df_assets.columns:
                            df_assets["categories"] = df_assets["categories"].apply(
                                lambda v: ", ".join(v) if isinstance(v, list) else str(v or "")
                            )
                        show_cols = [c for c in ["symbol", "base_asset", "quote_asset", "status", "categories"] if c in df_assets.columns]

                        use_enterprise = bool(os.environ.get("AGGRID_ENTERPRISE"))
                        df_assets_view = df_assets[show_cols].copy()
                        gb_assets = GridOptionsBuilder.from_dataframe(df_assets_view)
                        gb_assets.configure_default_column(filter=True, sortable=True, resizable=True)
                        gb_assets.configure_grid_options(
                            headerHeight=44,
                            rowHeight=34,
                            suppressRowHoverHighlight=False,
                            animateRows=False,
                        )
                        if "symbol" in df_assets.columns:
                            gb_assets.configure_column("symbol", headerName="Symbol", width=160)
                        if "base_asset" in df_assets.columns:
                            gb_assets.configure_column("base_asset", headerName="Base", width=120)
                        if "quote_asset" in df_assets.columns:
                            gb_assets.configure_column("quote_asset", headerName="Quote", width=120)
                        if "status" in df_assets.columns:
                            gb_assets.configure_column("status", headerName="Status", width=110)
                        if "categories" in df_assets.columns:
                            gb_assets.configure_column("categories", headerName="Categories", width=200)

                        grid_assets = AgGrid(
                            df_assets_view,
                            gridOptions=gb_assets.build(),
                            update_mode=GridUpdateMode.MODEL_CHANGED,
                            data_return_mode=DataReturnMode.AS_INPUT,
                            fit_columns_on_grid_load=False,
                            allow_unsafe_jscode=True,
                            enable_enterprise_modules=use_enterprise,
                            theme="dark",
                            custom_css=_aggrid_dark_custom_css(),
                            height=480,
                            key="assets_grid_v1",
                        )

                        grid_filter_model, grid_sort_model = _extract_aggrid_models(grid_assets)
                        if grid_filter_model is not None or grid_sort_model is not None:
                            if grid_filter_model != st.session_state.get("assets_filter_model") or grid_sort_model != st.session_state.get("assets_sort_model"):
                                st.session_state["assets_filter_model"] = grid_filter_model
                                st.session_state["assets_sort_model"] = grid_sort_model
                                st.session_state["assets_page"] = 1
                                st.rerun()

                        max_page = max(1, math.ceil(int(total_assets or 0) / float(page_size))) if total_assets else 1
                        if page > max_page:
                            st.session_state["assets_page"] = max_page
                            st.rerun()

                        pager = st.columns([1, 1, 2, 2, 2])
                        with pager[0]:
                            if st.button("← Prev", key="assets_prev", disabled=page <= 1):
                                st.session_state["assets_page"] = max(1, page - 1)
                                st.rerun()
                        with pager[1]:
                            if st.button("Next →", key="assets_next", disabled=page >= max_page):
                                st.session_state["assets_page"] = min(max_page, page + 1)
                                st.rerun()
                        with pager[2]:
                            st.caption(f"Page {page} / {max_page}")
                        with pager[3]:
                            st.caption(f"Total rows: {int(total_assets)}")
                        with pager[4]:
                            st.selectbox(
                                "Page size",
                                options=page_size_options,
                                index=page_size_options.index(int(page_size)),
                                key="assets_page_size",
                                on_change=_reset_assets_page,
                                label_visibility="collapsed",
                            )

                        if st.checkbox("Show assets grid debug", value=False, key="assets_debug_grid"):
                            debug_info = {}
                            try:
                                debug_info = debug_assets_server_side_query(
                                    user_id=str(user_id or "").strip() or "admin@local",
                                    exchange_id=exchange_id_assets,
                                    page=page,
                                    page_size=page_size,
                                    filter_model=st.session_state.get("assets_filter_model"),
                                    sort_model=st.session_state.get("assets_sort_model"),
                                    category_name=category_filter,
                                    search_text=search_text,
                                    status="all",
                                )
                            except Exception:
                                debug_info = {}
                            st.json(
                                {
                                    "page": page,
                                    "page_size": page_size,
                                    "total": int(total_assets),
                                    "category": category_filter,
                                    "search_text": search_text,
                                    "filter_model": st.session_state.get("assets_filter_model"),
                                    "sort_model": st.session_state.get("assets_sort_model"),
                                    "translated": debug_info,
                                }
                            )
                    else:
                        st.info("No assets stored yet. Add a symbol below or run a backtest to seed assets.")

            with right_a:
                with st.container(border=True):
                    st.markdown("### Add / update")
                    sym_in = st.text_input("Symbol", value="", key="assets_add_symbol")
                    if st.button("Upsert asset", key="assets_upsert"):
                        try:
                            upsert_asset(exchange_id_assets, str(sym_in or "").strip(), None, None)
                            st.success("Saved.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed: {exc}")

                with st.container(border=True):
                    st.markdown("### Enable / disable")
                    try:
                        sym_rows = list_asset_symbols(exchange_id_assets, status="all")
                    except Exception:
                        sym_rows = []
                    sym_opts = [str(r.get("symbol") or "") for r in (sym_rows or []) if str(r.get("symbol") or "").strip()]
                    chosen = st.multiselect("Symbols", options=sorted(set(sym_opts)), key="assets_status_symbols")
                    c1, c2 = st.columns(2)
                    if c1.button("Enable", key="assets_enable"):
                        try:
                            n = set_assets_status(exchange_id_assets, chosen, "active")
                            st.success(f"Enabled {n} asset(s).")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed: {exc}")
                    if c2.button("Disable", key="assets_disable"):
                        try:
                            n = set_assets_status(exchange_id_assets, chosen, "disabled")
                            st.success(f"Disabled {n} asset(s).")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed: {exc}")

            cat_left, cat_right = st.columns([2, 1])
            with cat_left:
                with st.container(border=True):
                    st.markdown("### Categories")
                    if cats:
                        df_c = pd.DataFrame(cats)
                        if "updated_at" in df_c.columns:
                            try:
                                _tz = _ui_display_tz()
                                df_c["updated_at"] = df_c["updated_at"].apply(lambda v: fmt_dt_short(v, _tz))
                            except Exception:
                                pass
                        show_cols = [c for c in ["name", "source", "member_count", "updated_at"] if c in df_c.columns]
                        st.dataframe(df_c[show_cols], hide_index=True, width="stretch")
                    else:
                        st.info("No categories yet.")

                    st.markdown("#### Edit membership")
                    if not cats:
                        st.info("Create a category first.")
                    else:
                        cat_labels = [f"{c.get('name')}" for c in cats]
                        picked = st.selectbox("Category", options=cat_labels, index=0, key="assets_pick_category")
                        cat_row = next((c for c in cats if str(c.get("name") or "") == str(picked or "")), None)
                        cat_id = int(cat_row.get("id")) if isinstance(cat_row, dict) and cat_row.get("id") is not None else None
                        existing_members: list[str] = []
                        try:
                            if cat_id is not None:
                                existing_members = get_category_symbols(
                                    str(user_id or "").strip() or "admin@local",
                                    exchange_id_assets,
                                    int(cat_id),
                                )
                        except Exception:
                            existing_members = []

                        active_assets = [
                            str(r.get("symbol") or "")
                            for r in (sym_rows or [])
                            if str(r.get("status") or "").strip().lower() == "active"
                        ]
                        active_assets = sorted(set([s for s in active_assets if s.strip()]))
                        chosen_members = st.multiselect(
                            "Members",
                            options=active_assets,
                            default=[s for s in existing_members if s in set(active_assets)],
                            key="assets_members_multiselect",
                        )

                        c1, c2 = st.columns([1, 1])
                        if c1.button("Save membership", key="assets_save_membership"):
                            if cat_id is None:
                                st.error("Invalid category.")
                            else:
                                new_set = set([str(s).strip() for s in chosen_members if str(s).strip()])
                                old_set = set([str(s).strip() for s in existing_members if str(s).strip()])
                                to_add = sorted(list(new_set - old_set))
                                to_remove = sorted(list(old_set - new_set))
                                try:
                                    set_category_membership(
                                        str(user_id or "").strip() or "admin@local",
                                        exchange_id_assets,
                                        int(cat_id),
                                        to_add,
                                        to_remove,
                                    )
                                    st.success("Updated.")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Failed: {exc}")

                        if c2.button("Delete category", key="assets_delete_category"):
                            if cat_id is None:
                                st.error("Invalid category.")
                            else:
                                st.session_state["assets_delete_category_id"] = int(cat_id)

                        pending_delete = st.session_state.get("assets_delete_category_id")
                        if pending_delete is not None:
                            st.warning("Delete removes the category and all its memberships.")
                            confirm_del = st.checkbox("Confirm delete", value=False, key="assets_delete_confirm")
                            if st.button("Confirm delete now", key="assets_delete_confirm_btn", disabled=not confirm_del):
                                try:
                                    delete_category(
                                        str(user_id or "").strip() or "admin@local",
                                        exchange_id_assets,
                                        int(pending_delete),
                                    )
                                    st.session_state.pop("assets_delete_category_id", None)
                                    st.session_state.pop("assets_delete_confirm", None)
                                    st.success("Deleted.")
                                    st.rerun()
                                except Exception as exc:
                                    st.error(f"Delete failed: {exc}")

            with cat_right:
                with st.container(border=True):
                    st.markdown("### Category tools")

                    st.markdown("#### Sync assets from exchange")
                    st.caption("Loads all CCXT markets and upserts into the local assets table.")
                    if st.button("Sync assets from CCXT", key="assets_sync_ccxt_btn"):
                        try:
                            markets, _tfs = fetch_ccxt_metadata(exchange_id_assets)
                            rows: list[tuple[str, Optional[str], Optional[str]]] = []
                            if isinstance(markets, dict):
                                for _k, m in markets.items():
                                    if not isinstance(m, dict):
                                        continue
                                    sym = str(m.get("symbol") or "").strip()
                                    if not sym:
                                        continue
                                    rows.append((sym, m.get("base"), m.get("quote")))
                            n = upsert_assets_bulk(exchange_id_assets, rows)
                            st.success(f"Synced {n} assets.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Sync failed: {exc}")

                    st.markdown("#### Create category")
                    new_name = st.text_input("Name", value="", key="assets_new_category")
                    if st.button("Create", key="assets_create_category"):
                        try:
                            create_category(
                                str(user_id or "").strip() or "admin@local",
                                exchange_id_assets,
                                new_name,
                                source="user",
                            )
                            st.success("Created.")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Failed: {exc}")

                    st.markdown("#### Auto-seed categories")
                    confirm = st.checkbox(
                        "I understand this adds auto categories (merge)",
                        value=False,
                        key="assets_seed_confirm",
                    )
                    if st.button("Auto-seed (merge)", key="assets_seed_btn", disabled=not confirm):
                        try:
                            res = seed_asset_categories_if_empty(
                                str(user_id or "").strip() or "admin@local",
                                exchange_id_assets,
                                force=True,
                            )
                            st.success(f"Seed result: {res}")
                            st.rerun()
                        except Exception as exc:
                            st.error(f"Seed failed: {exc}")

    _render_perf_panel()
    st.caption(f"Storage: {db_path}")


if __name__ == "__main__":
    main()