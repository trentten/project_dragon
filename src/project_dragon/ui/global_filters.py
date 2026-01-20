from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import streamlit as st


@dataclass(frozen=True)
class GlobalFilters:
    enabled: bool = True
    # Global created-at filter (run creation dates), expressed as a preset label.
    created_at_preset: Optional[str] = None
    # Backwards compat: older sessions stored an explicit date range tuple.
    date_range: Optional[Tuple[date, date]] = None
    account_ids: List[int] = None  # type: ignore[assignment]
    exchanges: List[str] = None  # type: ignore[assignment]
    market_types: List[str] = None  # type: ignore[assignment]
    symbols: List[str] = None  # type: ignore[assignment]
    strategies: List[str] = None  # type: ignore[assignment]
    timeframes: List[str] = None  # type: ignore[assignment]
    profitable_only: bool = False


def _as_list_str(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        out: List[str] = []
        for x in v:
            s = str(x or "").strip()
            if s:
                out.append(s)
        return out
    s = str(v or "").strip()
    return [s] if s else []


def _as_list_int(v: Any) -> List[int]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        out: List[int] = []
        for x in v:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out
    try:
        return [int(v)]
    except Exception:
        return []


def ensure_global_filters_initialized() -> None:
    if "global_filters" not in st.session_state or not isinstance(st.session_state.get("global_filters"), dict):
        st.session_state["global_filters"] = {
            "enabled": True,
            "created_at_preset": "All Time",
            "date_range": None,
            "account_ids": [],
            "exchanges": [],
            "market_types": [],
            "symbols": [],
            "strategies": [],
            "timeframes": [],
            "profitable_only": False,
        }
    else:
        # Backfill new key into existing sessions.
        gf = st.session_state.get("global_filters")
        if isinstance(gf, dict) and "created_at_preset" not in gf:
            gf = dict(gf)
            gf["created_at_preset"] = "All Time"
            st.session_state["global_filters"] = gf
        if isinstance(gf, dict) and "enabled" not in gf:
            gf = dict(gf)
            gf["enabled"] = True
            st.session_state["global_filters"] = gf
        if isinstance(gf, dict) and "profitable_only" not in gf:
            gf = dict(gf)
            gf["profitable_only"] = False
            st.session_state["global_filters"] = gf


def get_global_filters() -> Dict[str, Any]:
    ensure_global_filters_initialized()
    gf = st.session_state.get("global_filters")
    return gf if isinstance(gf, dict) else {}


def set_global_filters(new_filters: Dict[str, Any]) -> None:
    ensure_global_filters_initialized()
    st.session_state["global_filters"] = dict(new_filters or {})


def apply_global_filters(df: pd.DataFrame, global_filters: Dict[str, Any]) -> pd.DataFrame:
    """Apply global filters to a dataframe.

    This is intentionally best-effort and column-flexible so it can be reused across
    Dashboard, Live Bots, and Results without deep refactors.

        Supported filters (when matching columns exist):
        - created_at_preset: filters on created_at only (run creation dates)
            (Backwards compat: date_range tuple still supported.)
    - account_ids: filters on [account_id, Account ID]
    - exchanges: filters on [exchange_id, exchange, Exchange]
    - market_types: filters on [market_type, market, Market]
    - symbols: filters on [symbol, Symbol]
    - strategies: filters on [strategy, strategy_name, Strategy]
    - timeframes: filters on [timeframe, TF, Timeframe]
    """

    if df is None or df.empty:
        return df

    gf = global_filters or {}
    if not bool(gf.get("enabled", True)):
        return df

    # --- Created-at date range ----------------------------------------
    # Goal: consistent semantics across the app: filter by run creation dates.
    preset = str(gf.get("created_at_preset") or "").strip()

    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None

    if preset and preset != "All Time":
        # These are approximate (month=30 days). Good enough for UX.
        now = datetime.now(timezone.utc)
        today = now.date()
        if preset == "Last Week":
            start_d = today - timedelta(days=7)
            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)
        elif preset == "Last Month":
            start_d = today - timedelta(days=30)
            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)
        elif preset == "Last 3 Months":
            start_d = today - timedelta(days=90)
            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)
        elif preset == "Last 6 Months":
            start_d = today - timedelta(days=180)
            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
            end_dt = datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc)

    # Backwards compat: old sessions stored explicit tuples.
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

    # Prefer filtering by created_at when present (run semantics).
    # Otherwise, fall back to filtering by the most likely event timestamp column.
    if start_dt is not None and end_dt is not None:
        ts_col: Optional[str] = None
        if "created_at" in df.columns:
            ts_col = "created_at"
        else:
            for cand in (
                "event_ts",
                "ts",
                "closed_at",
                "opened_at",
                "snapshot_updated_at",
                "heartbeat_at",
                "updated_at",
            ):
                if cand in df.columns:
                    ts_col = cand
                    break
        if ts_col:
            ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
            mask = ts.notna() & (ts >= start_dt) & (ts <= end_dt)
            df = df.loc[mask].copy()

    # --- Account -------------------------------------------------------
    account_ids = _as_list_int(gf.get("account_ids"))
    if account_ids:
        for col in ("account_id", "Account ID"):
            if col in df.columns:
                try:
                    df = df.loc[pd.to_numeric(df[col], errors="coerce").isin(account_ids)].copy()
                except Exception:
                    pass
                break

    # --- Exchange ------------------------------------------------------
    exchanges = [s.lower() for s in _as_list_str(gf.get("exchanges"))]
    if exchanges:
        for col in ("exchange_id", "exchange", "Exchange"):
            if col in df.columns:
                s = df[col].fillna("").astype(str).str.strip().str.lower()
                df = df.loc[s.isin(exchanges)].copy()
                break

    # --- Market type ---------------------------------------------------
    market_types = [s.upper() for s in _as_list_str(gf.get("market_types"))]
    if market_types:
        for col in ("market_type", "market", "Market"):
            if col in df.columns:
                s = df[col].fillna("").astype(str).str.strip().str.upper()
                df = df.loc[s.isin(market_types)].copy()
                break

    # --- Symbols -------------------------------------------------------
    symbols = [s.upper() for s in _as_list_str(gf.get("symbols"))]
    if symbols:
        for col in ("symbol", "Symbol"):
            if col in df.columns:
                s = df[col].fillna("").astype(str).str.strip().str.upper()
                df = df.loc[s.isin(symbols)].copy()
                break

    # --- Strategy ------------------------------------------------------
    strategies = _as_list_str(gf.get("strategies"))
    if strategies:
        strategies_l = [s.lower() for s in strategies]
        for col in ("strategy", "strategy_name", "Strategy"):
            if col in df.columns:
                s = df[col].fillna("").astype(str).str.strip().str.lower()
                df = df.loc[s.isin(strategies_l)].copy()
                break

    # --- Profitable-only ------------------------------------------------
    if bool(gf.get("profitable_only", False)):
        try:
            if "net_return_pct" in df.columns:
                df = df.loc[pd.to_numeric(df["net_return_pct"], errors="coerce") > 0.0].copy()
        except Exception:
            pass

    # --- Timeframe -----------------------------------------------------
    timeframes = [s.lower() for s in _as_list_str(gf.get("timeframes"))]
    if timeframes:
        for col in ("timeframe", "TF", "Timeframe"):
            if col in df.columns:
                s = df[col].fillna("").astype(str).str.strip().str.lower()
                df = df.loc[s.isin(timeframes)].copy()
                break

    return df
