from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

import pandas as pd
import streamlit as st


FilterType = str


@dataclass(frozen=True)
class FilterDef:
    """A small, UI-facing filter definition.

    This is intentionally minimal so we can later map the returned `active_filters`
    cleanly to SQL WHERE clauses (server-side filtering), without changing the UI.
    """

    type: FilterType
    column: Optional[str] = None
    label: Optional[str] = None
    columns: Optional[Sequence[str]] = None  # for text search
    scale: float = 1.0  # display scale for range sliders (e.g., pct columns stored as 0..1)
    options: Optional[Sequence[Any]] = None  # for radio/select
    horizontal: bool = False  # for radio


FilterSpec = Mapping[str, Union[FilterType, Mapping[str, Any], FilterDef]]


@dataclass(frozen=True)
class TableFilterResult:
    df: pd.DataFrame
    active_filters: Dict[str, Any]


def _load_saved_filter_presets(*, conn: Any, user_id: str, scope_key: str) -> Dict[str, Dict[str, Any]]:
    try:
        from project_dragon.storage import get_user_setting

        raw = get_user_setting(conn, user_id, f"ui.filter_presets.{scope_key}", default=None)
        if not isinstance(raw, dict):
            return {}
        presets = raw.get("presets")
        if not isinstance(presets, dict):
            return {}
        # Store shape: {name: {"filters": {...}, "saved_at": "..."}}
        out: Dict[str, Dict[str, Any]] = {}
        for name, payload in presets.items():
            if not isinstance(name, str) or not name.strip():
                continue
            if isinstance(payload, dict) and isinstance(payload.get("filters"), dict):
                out[name] = payload
        return out
    except Exception:
        return {}


def _write_saved_filter_presets(*, conn: Any, user_id: str, scope_key: str, presets: Dict[str, Dict[str, Any]]) -> None:
    try:
        from project_dragon.storage import set_user_setting

        set_user_setting(conn, user_id, f"ui.filter_presets.{scope_key}", {"version": 1, "presets": presets})
    except Exception:
        return


@dataclass(frozen=True)
class TableFilterResult:
    df: pd.DataFrame
    active_filters: Dict[str, Any]


def _as_filter_def(name: str, spec: Union[FilterType, Mapping[str, Any], FilterDef]) -> FilterDef:
    if isinstance(spec, FilterDef):
        return spec
    if isinstance(spec, str):
        return FilterDef(type=spec, column=None, label=None)
    if isinstance(spec, Mapping):
        return FilterDef(
            type=str(spec.get("type") or "multiselect"),
            column=spec.get("column"),
            label=spec.get("label"),
            columns=spec.get("columns"),
            scale=float(spec.get("scale") or 1.0),
            options=spec.get("options") or spec.get("choices"),
            horizontal=bool(spec.get("horizontal") or False),
        )
    return FilterDef(type="multiselect")


def _dataset_signature(df: pd.DataFrame) -> str:
    try:
        cols = ",".join([str(c) for c in df.columns.tolist()])
    except Exception:
        cols = ""
    payload = f"n={len(df)}|cols={cols}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:12]


def _compute_unique_options(df: pd.DataFrame, column: str) -> List[Any]:
    if column not in df.columns:
        return []
    try:
        ser = df[column]
    except Exception:
        return []

    # Keep original types when possible, but ensure we can sort.
    try:
        vals = ser.dropna().unique().tolist()
    except Exception:
        try:
            vals = pd.Series(ser).dropna().unique().tolist()
        except Exception:
            vals = []

    # Common footgun: string columns can carry hidden whitespace / casing differences
    # (e.g., '4h ' vs '4h', 'short' vs 'SHORT'). Normalize display options by trimming.
    try:
        cleaned: list[Any] = []
        seen: set[Any] = set()
        for v in vals:
            vv = v.strip() if isinstance(v, str) else v
            # Deduplicate while preserving order.
            if vv in seen:
                continue
            seen.add(vv)
            cleaned.append(vv)
        vals = cleaned
    except Exception:
        pass

    def _sort_key(v: Any) -> str:
        try:
            return str(v)
        except Exception:
            return ""

    try:
        return sorted(vals, key=_sort_key)
    except Exception:
        return vals


def _is_active_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, tuple, set, dict)):
        return bool(v)
    return True


def clear_filter_state(*, key_prefix: str, filter_names: Iterable[str]) -> None:
    """Remove Streamlit widget keys for a given filter bar."""

    for name in filter_names:
        st.session_state.pop(f"{key_prefix}__{name}", None)


def render_filter_bar(
    df: pd.DataFrame,
    spec: FilterSpec,
    *,
    key_prefix: str,
    dataset_key: Optional[str] = None,
    clear_label: str = "Clear filters",
    # Optional per-user preset persistence.
    presets_conn: Any = None,
    presets_user_id: Optional[str] = None,
    presets_scope_key: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Render a reusable filter bar and return (filtered_df, active_filters).

    - Does in-memory filtering (no per-row DB calls).
    - Persists widget values via `st.session_state` using `key_prefix`.
    - Returns `active_filters` (only active/non-empty values) suitable for later SQL mapping.
    """

    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df, {}

    spec_norm: Dict[str, FilterDef] = {k: _as_filter_def(k, v) for k, v in (spec or {}).items()}
    filter_names = list(spec_norm.keys())

    sig = dataset_key or _dataset_signature(df)
    options_cache_key = f"{key_prefix}__options__{sig}"
    options_cache: Dict[str, List[Any]] = st.session_state.get(options_cache_key, {})
    if not isinstance(options_cache, dict):
        options_cache = {}

    # Presets (optional) rendered in their own box.
    presets_enabled = presets_conn is not None and bool((presets_user_id or "").strip())
    preset_store_key = str(presets_scope_key or key_prefix)
    save_requested = False
    saved_presets: Dict[str, Dict[str, Any]] = {}
    selected_preset_name: str = ""

    if presets_enabled:
        try:
            preset_box = st.container(border=True)
        except TypeError:
            preset_box = st.container()
        with preset_box:
            st.markdown("**Saved filters**")
            # Two-row layout: labels row + widgets row.
            # Collapsing widget labels is the most reliable way to make buttons align
            # visually inline with the input boxes.
            label_cols = st.columns([5, 1, 4, 1, 1, 1])
            with label_cols[0]:
                st.caption("Name")
            with label_cols[2]:
                st.caption("Preset")

            widget_cols = st.columns([5, 1, 4, 1, 1, 1])
            with widget_cols[0]:
                st.text_input(
                    "Name",
                    value=st.session_state.get(f"{key_prefix}__preset_name", ""),
                    key=f"{key_prefix}__preset_name",
                    placeholder="e.g., short_4h",
                    label_visibility="collapsed",
                )
            with widget_cols[1]:
                save_requested = st.button("Save", key=f"{key_prefix}__preset_save", use_container_width=True)
            with widget_cols[2]:
                saved_presets = _load_saved_filter_presets(
                    conn=presets_conn, user_id=str(presets_user_id).strip(), scope_key=preset_store_key
                )
                preset_names = sorted(saved_presets.keys(), key=lambda s: s.casefold())
                selected_preset_name = st.selectbox(
                    "Preset",
                    options=[""] + preset_names,
                    index=0,
                    key=f"{key_prefix}__preset_selected",
                    label_visibility="collapsed",
                )
            with widget_cols[3]:
                load_clicked = st.button(
                    "Load",
                    key=f"{key_prefix}__preset_load",
                    disabled=not bool(selected_preset_name),
                    use_container_width=True,
                )
            with widget_cols[4]:
                delete_clicked = st.button(
                    "Delete",
                    key=f"{key_prefix}__preset_delete",
                    disabled=not bool(selected_preset_name),
                    use_container_width=True,
                )
            with widget_cols[5]:
                clear_clicked = st.button(clear_label, key=f"{key_prefix}__clear", use_container_width=True)

            if clear_clicked:
                # IMPORTANT: popping keys is not sufficient for Streamlit widgets.
                # The browser can rehydrate the previous value on rerun.
                # Set widget keys back to their default values instead.
                try:
                    # Preset UI
                    st.session_state[f"{key_prefix}__preset_name"] = ""
                    st.session_state[f"{key_prefix}__preset_selected"] = ""
                except Exception:
                    pass

                for name, fd in spec_norm.items():
                    ftype = (fd.type or "multiselect").strip().lower()
                    widget_key = f"{key_prefix}__{name}"

                    if ftype == "multiselect":
                        st.session_state[widget_key] = []

                    elif ftype in {"text", "search"}:
                        st.session_state[widget_key] = ""

                    elif ftype in {"radio", "select", "choice"}:
                        options = list(fd.options or [])
                        if not options:
                            col = fd.column or name
                            options = _compute_unique_options(df, col)
                        st.session_state[widget_key] = options[0] if options else None

                    elif ftype == "date_range":
                        col = fd.column or name
                        dt = (
                            pd.to_datetime(df[col], errors="coerce", utc=True)
                            if col in df.columns
                            else pd.Series([], dtype="datetime64[ns, UTC]")
                        )
                        dt = dt.dropna()
                        if dt.empty:
                            st.session_state.pop(widget_key, None)
                        else:
                            start = dt.min().date()
                            end = dt.max().date()
                            st.session_state[widget_key] = (start, end)

                    elif ftype == "range":
                        col = fd.column or name
                        try:
                            ser = pd.to_numeric(df[col], errors="coerce").dropna() if col in df.columns else None
                        except Exception:
                            ser = None
                        if ser is None or getattr(ser, "empty", True):
                            st.session_state.pop(widget_key, None)
                        else:
                            lo = float(ser.min())
                            hi = float(ser.max())
                            if lo == hi:
                                st.session_state.pop(widget_key, None)
                            else:
                                scale = float(fd.scale or 1.0)
                                st.session_state[widget_key] = (float(lo * scale), float(hi * scale))

                    else:
                        st.session_state.pop(widget_key, None)

                # Reset cached options (safe; they will be recomputed lazily).
                st.session_state.pop(options_cache_key, None)
                st.rerun()

            if load_clicked and selected_preset_name:
                payload = saved_presets.get(selected_preset_name) or {}
                filt = payload.get("filters") if isinstance(payload, dict) else None
                if isinstance(filt, dict):
                    for n in filter_names:
                        st.session_state[f"{key_prefix}__{n}"] = filt.get(n)
                    st.rerun()

            if delete_clicked and selected_preset_name:
                if selected_preset_name in saved_presets:
                    saved_presets.pop(selected_preset_name, None)
                    _write_saved_filter_presets(
                        conn=presets_conn,
                        user_id=str(presets_user_id).strip(),
                        scope_key=preset_store_key,
                        presets=saved_presets,
                    )
                    st.session_state[f"{key_prefix}__preset_selected"] = ""
                    st.rerun()
    else:
        # No preset storage available (e.g. missing DB connection). Still offer Clear.
        if st.button(clear_label, key=f"{key_prefix}__clear"):
            for name, fd in spec_norm.items():
                ftype = (fd.type or "multiselect").strip().lower()
                widget_key = f"{key_prefix}__{name}"

                if ftype == "multiselect":
                    st.session_state[widget_key] = []

                elif ftype in {"text", "search"}:
                    st.session_state[widget_key] = ""

                elif ftype in {"radio", "select", "choice"}:
                    options = list(fd.options or [])
                    if not options:
                        col = fd.column or name
                        options = _compute_unique_options(df, col)
                    st.session_state[widget_key] = options[0] if options else None

                elif ftype == "date_range":
                    col = fd.column or name
                    dt = (
                        pd.to_datetime(df[col], errors="coerce", utc=True)
                        if col in df.columns
                        else pd.Series([], dtype="datetime64[ns, UTC]")
                    )
                    dt = dt.dropna()
                    if dt.empty:
                        st.session_state.pop(widget_key, None)
                    else:
                        start = dt.min().date()
                        end = dt.max().date()
                        st.session_state[widget_key] = (start, end)

                elif ftype == "range":
                    col = fd.column or name
                    try:
                        ser = pd.to_numeric(df[col], errors="coerce").dropna() if col in df.columns else None
                    except Exception:
                        ser = None
                    if ser is None or getattr(ser, "empty", True):
                        st.session_state.pop(widget_key, None)
                    else:
                        lo = float(ser.min())
                        hi = float(ser.max())
                        if lo == hi:
                            st.session_state.pop(widget_key, None)
                        else:
                            scale = float(fd.scale or 1.0)
                            st.session_state[widget_key] = (float(lo * scale), float(hi * scale))

                else:
                    st.session_state.pop(widget_key, None)

            st.session_state.pop(options_cache_key, None)
            st.rerun()

    # Render controls in a compact grid.
    # - Non-range filters: 5 columns across.
    # - Range sliders: each on its own line (full width).
    primary_items: list[tuple[str, FilterDef]] = []
    range_items: list[tuple[str, FilterDef]] = []
    for name, fd in spec_norm.items():
        ftype = (fd.type or "multiselect").strip().lower()
        if ftype == "range":
            range_items.append((name, fd))
        else:
            primary_items.append((name, fd))

    # Uniform widths: 5 equal columns across.
    control_cols = st.columns(5)
    col_i = 0

    raw_values: Dict[str, Any] = {}

    for name, fd in primary_items:
        ftype = (fd.type or "multiselect").strip().lower()
        label = fd.label or str(name).replace("_", " ").title()
        widget_key = f"{key_prefix}__{name}"

        with control_cols[col_i % 5]:
            if ftype == "multiselect":
                col = fd.column or name
                if name not in options_cache:
                    options_cache[name] = _compute_unique_options(df, col)
                options = options_cache.get(name, [])
                val = st.multiselect(label, options=options, default=None, key=widget_key)
                raw_values[name] = val

            elif ftype == "range":
                # Range sliders are rendered below in their own section.
                raw_values[name] = None

            elif ftype in {"text", "search"}:
                val = st.text_input(label, value="", key=widget_key)
                raw_values[name] = val

            elif ftype in {"radio", "select", "choice"}:
                # Use explicit options if provided; otherwise, fall back to unique values in the column.
                options = list(fd.options or [])
                if not options:
                    col = fd.column or name
                    options = _compute_unique_options(df, col)
                # Defensive: ensure we always pass a sequence.
                if not isinstance(options, list):
                    try:
                        options = list(options)
                    except Exception:
                        options = []
                if not options:
                    raw_values[name] = None
                else:
                    if ftype == "select":
                        val = st.selectbox(label, options=options, index=0, key=widget_key)
                    else:
                        val = st.radio(label, options=options, index=0, key=widget_key, horizontal=bool(fd.horizontal))
                    raw_values[name] = val

            elif ftype == "date_range":
                col = fd.column or name
                # Default to full available range.
                dt = pd.to_datetime(df[col], errors="coerce", utc=True) if col in df.columns else pd.Series([], dtype="datetime64[ns, UTC]")
                dt = dt.dropna()
                if dt.empty:
                    raw_values[name] = None
                else:
                    start = dt.min().date()
                    end = dt.max().date()
                    val = st.date_input(label, value=(start, end), key=widget_key)
                    if isinstance(val, tuple) and len(val) == 2 and isinstance(val[0], date) and isinstance(val[1], date):
                        # Keep date filters inactive when left at the full span.
                        if val[0] == start and val[1] == end:
                            raw_values[name] = None
                        else:
                            raw_values[name] = [val[0].isoformat(), val[1].isoformat()]
                    else:
                        raw_values[name] = None
            else:
                # Unknown filter type: ignore
                raw_values[name] = None

        col_i += 1

    # Render range sliders in a 5-column grid as well.
    if range_items:
        st.markdown("<div style='height: 0.35rem'></div>", unsafe_allow_html=True)
        slider_cols = st.columns(5)
        for i, (name, fd) in enumerate(range_items):
            label = fd.label or str(name).replace("_", " ").title()
            widget_key = f"{key_prefix}__{name}"
            col = fd.column or name
            with slider_cols[i % 5]:
                if col not in df.columns:
                    raw_values[name] = None
                    continue
                try:
                    ser = pd.to_numeric(df[col], errors="coerce").dropna()
                except Exception:
                    raw_values[name] = None
                    continue
                if ser.empty:
                    raw_values[name] = None
                    continue
                lo = float(ser.min())
                hi = float(ser.max())
                if lo == hi:
                    raw_values[name] = None
                    st.caption(f"{label}: {lo:.4g}")
                    continue
                scale = float(fd.scale or 1.0)
                lo_disp = lo * scale
                hi_disp = hi * scale
                val = st.slider(
                    label,
                    min_value=float(lo_disp),
                    max_value=float(hi_disp),
                    value=(float(lo_disp), float(hi_disp)),
                    key=widget_key,
                )
                # Keep range filters inactive when left at the full span.
                try:
                    a0, a1 = float(val[0]), float(val[1])
                    eps = 1e-12
                    if abs(a0 - float(lo_disp)) <= eps and abs(a1 - float(hi_disp)) <= eps:
                        raw_values[name] = None
                    else:
                        raw_values[name] = [a0 / scale, a1 / scale]
                except Exception:
                    raw_values[name] = None

    # Persist options cache for this dataset load.
    st.session_state[options_cache_key] = options_cache

    # Apply filters in-memory.
    filtered = df
    for name, fd in spec_norm.items():
        ftype = (fd.type or "multiselect").strip().lower()
        val = raw_values.get(name)
        if not _is_active_value(val):
            continue

        if ftype == "multiselect":
            col = fd.column or name
            if col in filtered.columns and isinstance(val, list) and val:
                ser = filtered[col]

                # Robust matching for string-like values: trim + casefold
                # so UI selections match even if the dataset has inconsistent casing/whitespace.
                try:
                    if ser.dtype == object or pd.api.types.is_string_dtype(ser):
                        ser_norm = ser.astype(str).str.strip().str.casefold()
                        val_norm = [str(v).strip().casefold() for v in val]
                        filtered = filtered[ser_norm.isin(val_norm)]
                    else:
                        filtered = filtered[ser.isin(val)]
                except Exception:
                    filtered = filtered[filtered[col].isin(val)]

        elif ftype == "range":
            col = fd.column or name
            if col in filtered.columns and isinstance(val, list) and len(val) == 2:
                lo, hi = val
                ser = pd.to_numeric(filtered[col], errors="coerce")
                mask = ser.notna() & (ser >= float(lo)) & (ser <= float(hi))
                filtered = filtered[mask]

        elif ftype in {"radio", "select", "choice"}:
            col = fd.column or name
            if col in filtered.columns:
                try:
                    if pd.api.types.is_string_dtype(filtered[col]):
                        want = str(val).strip().casefold()
                        got = filtered[col].astype(str).str.strip().str.casefold()
                        filtered = filtered[got.eq(want)]
                    else:
                        filtered = filtered[filtered[col].eq(val)]
                except Exception:
                    filtered = filtered[filtered[col].eq(val)]

        elif ftype in {"text", "search"}:
            q = str(val or "").strip().lower()
            if not q:
                continue
            cols = list(fd.columns or [])
            if not cols:
                # Safe default: search across object columns only.
                try:
                    cols = [c for c in filtered.columns if str(filtered[c].dtype) == "object"]
                except Exception:
                    cols = []
            cols = [c for c in cols if c in filtered.columns]
            if not cols:
                continue

            hay = filtered[cols].astype(str).fillna("").agg(" ".join, axis=1).str.lower()
            tokens = [t for t in q.split() if t]
            if not tokens:
                continue
            mask = None
            for t in tokens:
                m = hay.str.contains(t, regex=False)
                mask = m if mask is None else (mask & m)
            if mask is not None:
                filtered = filtered[mask]

        elif ftype == "date_range":
            col = fd.column or name
            if col in filtered.columns and isinstance(val, list) and len(val) == 2:
                start_s, end_s = str(val[0]), str(val[1])
                dt = pd.to_datetime(filtered[col], errors="coerce", utc=True)
                mask = dt.notna()
                try:
                    start_dt = pd.to_datetime(start_s, utc=True)
                    end_dt = pd.to_datetime(end_s, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(milliseconds=1)
                    mask = mask & (dt >= start_dt) & (dt <= end_dt)
                except Exception:
                    pass
                filtered = filtered[mask]

    active_filters = {k: v for k, v in raw_values.items() if _is_active_value(v)}

    # Save preset (must happen after widget values are known).
    if presets_enabled and save_requested:
        name = str(st.session_state.get(f"{key_prefix}__preset_name", "")).strip()
        if name:
            presets_now = _load_saved_filter_presets(
                conn=presets_conn, user_id=str(presets_user_id).strip(), scope_key=preset_store_key
            )
            presets_now[name] = {
                "filters": raw_values,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_saved_filter_presets(
                conn=presets_conn,
                user_id=str(presets_user_id).strip(),
                scope_key=preset_store_key,
                presets=presets_now,
            )
            st.session_state[f"{key_prefix}__preset_selected"] = name
            st.rerun()
        else:
            st.warning("Enter a name to save this filter.")

    return filtered, active_filters


def render_table_filters(
    df: pd.DataFrame,
    filter_spec: FilterSpec,
    *,
    # Old API (used across app)
    scope_key: Optional[str] = None,
    dataset_cache_key: Optional[str] = None,
    # Newer/internal naming
    key_prefix: Optional[str] = None,
    dataset_key: Optional[str] = None,
    clear_label: str = "Clear filters",
    # Optional saved-filter persistence.
    presets_conn: Any = None,
    presets_user_id: Optional[str] = None,
    presets_scope_key: Optional[str] = None,
) -> TableFilterResult:
    """Compatibility wrapper around `render_filter_bar`.

    Returns an object with `.df` and `.active_filters` as expected by the Streamlit app.
    """

    prefix = scope_key or key_prefix
    if not prefix:
        raise TypeError("render_table_filters() missing required keyword argument: 'scope_key'")

    filtered_df, active = render_filter_bar(
        df,
        filter_spec,
        key_prefix=str(prefix),
        dataset_key=dataset_cache_key or dataset_key,
        clear_label=clear_label,
        presets_conn=presets_conn,
        presets_user_id=presets_user_id,
        presets_scope_key=presets_scope_key,
    )
    return TableFilterResult(df=filtered_df, active_filters=active)
