from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Optional
import re

import streamlit as st

try:
    from st_aggrid import JsCode  # type: ignore
except Exception:  # pragma: no cover
    JsCode = None  # type: ignore


def inject_trade_stream_css(max_width_px: int = 1920) -> None:
    """Global CSS for a tighter, wider layout + Dragon dark theme.

    Important: do NOT override Streamlit focus rings globally.
    """

    # Dragon palette (presentation-only)
    app_bg = "#1c1c1d"
    sidebar_bg = "#111112"
    card_bg = "#262627"
    border = "#323233"
    input_bg = "#333334"
    text = "rgba(242, 242, 242, 0.96)"
    text_muted = "rgba(242, 242, 242, 0.70)"
    accent = "#F28C28"
    selected_bg = "#3d3d3e"
    accent_overlay = "rgba(242, 140, 40, 0.10)"

    # Tabler icon data-URIs for CSS pseudo-elements.
    # Use as_img=True and extract the <img src="..."> data uri.
    try:
        from project_dragon.ui.icons import render_tabler_icon  # local import to avoid heavy deps at module import time

        _src_re = re.compile(r'src="([^"]+)"', flags=re.IGNORECASE)

        def _tabler_data_uri(name: str, *, size_px: int = 18, color: str = accent) -> str:
            html = render_tabler_icon(name, size_px=size_px, color=color, variant="filled", as_img=True)
            m = _src_re.search(html)
            return m.group(1) if m else ""

        nav_dashboard = _tabler_data_uri("layout-dashboard")
        nav_backtest = _tabler_data_uri("flask")
        nav_results = _tabler_data_uri("presentation-analytics")
        nav_runs = _tabler_data_uri("binoculars")
        nav_sweeps = _tabler_data_uri("sort-descending-2")
        nav_bots = _tabler_data_uri("atom-2")
        nav_accounts = _tabler_data_uri("user")
        nav_jobs = _tabler_data_uri("stack-2")
        nav_cache = _tabler_data_uri("chart-candle")
        nav_settings = _tabler_data_uri("settings")

        act_rerun = _tabler_data_uri("replace")
        act_create_bot = _tabler_data_uri("copy-plus")
        act_open_sweep = _tabler_data_uri("versions")
        act_play = _tabler_data_uri("player-play")
        act_pause = _tabler_data_uri("player-pause")
        act_cancel = _tabler_data_uri("circle-x")
        act_delete = _tabler_data_uri("trash")
    except Exception:  # pragma: no cover
        nav_dashboard = nav_backtest = nav_results = nav_runs = nav_sweeps = ""
        nav_bots = nav_accounts = nav_jobs = nav_cache = nav_settings = ""
        act_rerun = act_create_bot = act_open_sweep = ""
        act_play = act_pause = act_cancel = act_delete = ""

    max_width_px = int(max(1200, min(2400, max_width_px)))
    st.markdown(
        f"""
        <style>
                    /* ---- Dragon theme palette (do not touch focus rings) ---- */
                    div[data-testid="stAppViewContainer"] {{
                        background-color: {app_bg};
                        color: {text};
                    }}

                    header[data-testid="stHeader"] {{
                        background-color: {app_bg};
                    }}

                    section[data-testid="stSidebar"] {{
                        background-color: {sidebar_bg};
                        border-right: 1px solid {border};
                    }}

                    /* Bordered containers (cards) */
                    div[data-testid="stVerticalBlockBorderWrapper"] > div {{
                        background-color: {card_bg};
                        border: 1px solid {border};
                        border-radius: 0.75rem;
                    }}

                    /* Muted text */
                    .dragon-muted {{
                        color: {text_muted};
                    }}

                    /* Inputs: match palette (no focus overrides) */
                    div[data-testid="stTextInput"] input,
                    div[data-testid="stNumberInput"] input,
                    div[data-testid="stTextArea"] textarea {{
                        background-color: {input_bg};
                        border-color: {border};
                        color: {text};
                    }}

                    /* Selectbox container */
                    div[data-testid="stSelectbox"] > div > div {{
                        background-color: {input_bg};
                        border-color: {border};
                        color: {text};
                    }}

                    /* Buttons: neutral outline; no orange outlines */
                    div[data-testid="stButton"] > button,
                    div[data-testid="baseButton-secondary"] > button,
                    div[data-testid="baseButton-primary"] > button {{
                        border-color: {border};
                    }}

                    /* --- Icon button helpers (CSS pseudo-elements) --- */
                    div[class*="st-key-nav_btn_"] button,
                    div[class*="st-key-run_actions_"] button,
                    div[class*="st-key-rerun_sweep_btn_"] button,
                    div[class*="st-key-delete_sweep_btn_"] button,
                    div[class*="st-key-sweep_pause_"] button,
                    div[class*="st-key-sweep_resume_"] button,
                    div[class*="st-key-sweep_cancel_"] button,
                    div[class*="st-key-detail_resume"] button,
                    div[class*="st-key-detail_pause"] button,
                    div[class*="st-key-detail_stop"] button,
                    div[class*="st-key-cancel_job_"] button,
                    div[class*="st-key-acct_delete_"] button {{
                        display: inline-flex;
                        align-items: center;
                        gap: 0.45rem;
                    }}
                    div[class*="st-key-nav_btn_"] button::before,
                    div[class*="st-key-run_actions_"] button::before,
                    div[class*="st-key-rerun_sweep_btn_"] button::before,
                    div[class*="st-key-delete_sweep_btn_"] button::before,
                    div[class*="st-key-sweep_pause_"] button::before,
                    div[class*="st-key-sweep_resume_"] button::before,
                    div[class*="st-key-sweep_cancel_"] button::before,
                    div[class*="st-key-detail_resume"] button::before,
                    div[class*="st-key-detail_pause"] button::before,
                    div[class*="st-key-detail_stop"] button::before,
                    div[class*="st-key-cancel_job_"] button::before,
                    div[class*="st-key-acct_delete_"] button::before {{
                        content: "";
                        width: 18px;
                        height: 18px;
                        background-repeat: no-repeat;
                        background-position: center;
                        background-size: 18px 18px;
                        flex: 0 0 18px;
                    }}

                    /* Sidebar nav selected styling (background + subtle orange tint + optional left bar)
                         Keep it scoped to nav buttons only. */
                    section[data-testid="stSidebar"] div[class*="st-key-nav_btn_"] button {{
                        background-color: {sidebar_bg};
                        border: 1px solid {border};
                        color: {text};
                        justify-content: flex-start;
                        width: 100%;
                        position: relative;
                    }}
                    section[data-testid="stSidebar"] div[class*="st-key-nav_btn_"] button[kind="primary"] {{
                        background-color: {selected_bg};
                        background-image: linear-gradient(0deg, {accent_overlay}, {accent_overlay});
                    }}
                    section[data-testid="stSidebar"] div[class*="st-key-nav_btn_"] button[kind="primary"]::after {{
                        content: "";
                        position: absolute;
                        left: 0;
                        top: 4px;
                        bottom: 4px;
                        width: 3px;
                        background: {accent};
                        border-radius: 2px;
                    }}

                    /* ---- Sidebar nav icons (Tabler filled) ---- */
                      div[class*="st-key-nav_btn_dashboard"] button::before {{ background-image: url("{nav_dashboard}"); }}
                      div[class*="st-key-nav_btn_backtest"] button::before {{ background-image: url("{nav_backtest}"); }}
                      div[class*="st-key-nav_btn_results"] button::before {{ background-image: url("{nav_results}"); }}
                      div[class*="st-key-nav_btn_runs_explorer"] button::before {{ background-image: url("{nav_runs}"); }}
                      div[class*="st-key-nav_btn_sweeps"] button::before {{ background-image: url("{nav_sweeps}"); }}
                      div[class*="st-key-nav_btn_live"] button::before {{ background-image: url("{nav_bots}"); }}
                      div[class*="st-key-nav_btn_accounts"] button::before {{ background-image: url("{nav_accounts}"); }}
                      div[class*="st-key-nav_btn_jobs"] button::before {{ background-image: url("{nav_jobs}"); }}
                      div[class*="st-key-nav_btn_candle_cache"] button::before {{ background-image: url("{nav_cache}"); }}
                      div[class*="st-key-nav_btn_settings"] button::before {{ background-image: url("{nav_settings}"); }}

                    /* ---- Action button icons (Tabler filled) ---- */
                      div[class*="st-key-run_actions_rerun_"] button::before {{ background-image: url("{act_rerun}"); }}
                      div[class*="st-key-run_actions_create_bot_"] button::before {{ background-image: url("{act_create_bot}"); }}
                      div[class*="st-key-run_actions_open_sweep_"] button::before {{ background-image: url("{act_open_sweep}"); }}
                      div[class*="st-key-rerun_sweep_btn_"] button::before {{ background-image: url("{act_rerun}"); }}
                      div[class*="st-key-delete_sweep_btn_"] button::before {{ background-image: url("{act_delete}"); }}
                      div[class*="st-key-sweep_pause_"] button::before {{ background-image: url("{act_pause}"); }}
                      div[class*="st-key-sweep_resume_"] button::before {{ background-image: url("{act_play}"); }}
                      div[class*="st-key-sweep_cancel_"] button::before {{ background-image: url("{act_cancel}"); }}
                      div[class*="st-key-detail_resume"] button::before {{ background-image: url("{act_play}"); }}
                      div[class*="st-key-detail_pause"] button::before {{ background-image: url("{act_pause}"); }}
                      div[class*="st-key-detail_stop"] button::before {{ background-image: url("{act_cancel}"); }}
                      div[class*="st-key-cancel_job_"] button::before {{ background-image: url("{act_cancel}"); }}
                      div[class*="st-key-acct_delete_"] button::before {{ background-image: url("{act_delete}"); }}

          /* Wider content area (Streamlit 'wide' still has a max-width + padding) */
          div[data-testid="stAppViewContainer"] .block-container {{
            max-width: {max_width_px}px;
                        /* If this is too small, page headings get clipped under the top chrome */
                                                padding-top: 3.25rem;
                        padding-left: 1.0rem;
                        padding-right: 1.0rem;
                                                padding-bottom: 1.0rem;
          }}

          @media (min-width: 1400px) {{
            div[data-testid="stAppViewContainer"] .block-container {{
                            padding-left: 1.25rem;
                            padding-right: 1.25rem;
            }}
          }}

          /* Tighten bordered containers (cards) */
                    div[data-testid="stVerticalBlockBorderWrapper"] {{
                        padding: 0 !important;
                    }}
                    div[data-testid="stVerticalBlockBorderWrapper"] > div {{
                        padding-top: 0.25rem !important;
                        padding-bottom: 0.25rem !important;
                        padding-left: 0.55rem !important;
                        padding-right: 0.55rem !important;
                    }}

                    /* Streamlit wraps st.markdown content; remove default paragraph/div margins inside bordered boxes */
                    div[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown,
                    div[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown > div,
                    div[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown p {{
                        margin-top: 0 !important;
                        margin-bottom: 0 !important;
                    }}
                    div[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown > div {{
                        padding-top: 0 !important;
                        padding-bottom: 0 !important;
                    }}

          /* Reduce vertical whitespace between blocks */
          div[data-testid="stVerticalBlock"] {{
                        gap: 0.6rem;
          }}

          /* Simple chips row */
          .dragon-chip {{
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.15rem 0.55rem;
            margin-right: 0.35rem;
            margin-bottom: 0.25rem;
            font-size: 0.8rem;
            font-weight: 600;
                        border: 1px solid rgba(255,255,255,0.12);
                        background: rgba(242,242,242,0.06);
                        color: {text};
          }}

          /* KPI tile */
                    .dragon-kpi-card {{
                        border: 1px solid {border};
                        border-radius: 0.75rem;
                        padding: 0.45rem 0.60rem;
                        margin-bottom: 24px;
                        background: rgba(242,242,242,0.04);
                        display: block;
                        width: 100%;
                        box-sizing: border-box;
                    }}
                    .dragon-kpi-wrap {{
                        display: flex;
                        flex-direction: column;
                        justify-content: center;
                        min-height: 72px;
                    }}
          .dragon-kpi-label {{
            font-size: 0.78rem;
                        color: {text_muted};
            margin-bottom: 0.15rem;
          }}
          .dragon-kpi-value {{
            font-size: 1.35rem;
            font-weight: 800;
            line-height: 1.1;
          }}
          .dragon-kpi-subtext {{
            font-size: 0.78rem;
                        color: {text_muted};
            margin-top: 0.15rem;
          }}

                    /* Card title (avoid default markdown <p> margins inside bordered containers) */
                    .dragon-card-title {{
                        font-size: 0.95rem;
                        font-weight: 800;
                        margin: 0;
                        padding: 0;
                        margin-bottom: 0.75rem;
                        line-height: 1.2;
                    }}

                    /* Page title (align with header actions) */
                    .dragon-page-title {{
                        font-size: 1.55rem;
                        font-weight: 900;
                        margin: 0;
                        padding: 0;
                        line-height: 1.15;
                    }}

                                /* Compact two-column metric list (TradeStream-style) */
                                .dragon-metrics-grid {{
                                    display: grid;
                                    grid-template-columns: 1fr 1fr;
                                    gap: 0.75rem;
                                }}
                                .dragon-metrics-table {{
                                    width: 100%;
                                    border-collapse: collapse;
                                    border-spacing: 0;
                                }}
                                .dragon-metrics-table tr {{
                                    border-bottom: 1px solid rgba(255,255,255,0.08);
                                }}
                                .dragon-metrics-table tr:last-child {{
                                    border-bottom: none;
                                }}
                                .dragon-metrics-table td {{
                                    padding: 0.30rem 0.25rem;
                                    font-size: 0.86rem;
                                    vertical-align: middle;
                                }}
                                .dragon-metrics-table td.label {{
                                    color: {text_muted};
                                    font-weight: 650;
                                    white-space: nowrap;
                                }}
                                .dragon-metrics-table td.value {{
                                    text-align: right;
                                    font-weight: 800;
                                    white-space: nowrap;
                                }}
                                .dragon-row-bg {{
                                    background: rgba(242,242,242,0.04);
                                }}
                                .dragon-muted {{
                                    color: {text_muted};
                                    font-size: 0.86rem;
                                }}

                                /* Compact key/value table (2 columns) */
                                .dragon-kv-table {{
                                    width: 100%;
                                    border-collapse: collapse;
                                }}
                                .dragon-kv-table td {{
                                    padding: 0.26rem 0.25rem;
                                    font-size: 0.86rem;
                                }}
                                .dragon-kv-table td.label {{
                                    color: {text_muted};
                                    font-weight: 700;
                                }}
                                .dragon-kv-table td.value {{
                                    text-align: right;
                                    font-weight: 850;
                                }}

                                /* Long/Short ratio bar */
                                .dragon-ratio-wrap {{
                                    width: 100%;
                                }}
                                .dragon-ratio-header {{
                                    display: flex;
                                    justify-content: space-between;
                                    align-items: baseline;
                                    gap: 0.75rem;
                                    margin-bottom: 0.35rem;
                                }}
                                .dragon-ratio-header .label {{
                                    font-size: 0.86rem;
                                    color: {text_muted};
                                    font-weight: 700;
                                }}
                                .dragon-ratio-header .value {{
                                    font-size: 0.95rem;
                                    font-weight: 900;
                                }}
                                .dragon-splitbar {{
                                    width: 100%;
                                    height: 12px;
                                    border-radius: 999px;
                                    overflow: hidden;
                                    background: rgba(242,242,242,0.10);
                                    border: 1px solid rgba(255,255,255,0.12);
                                }}
                                .dragon-splitbar .long {{
                                    height: 100%;
                                    background: rgba(46,160,67,0.55);
                                }}
                                .dragon-splitbar .short {{
                                    height: 100%;
                                    background: rgba(248,81,73,0.55);
                                }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_active_filter_chips(global_filters: Dict[str, Any]) -> None:
    chips: List[str] = []
    gf = global_filters or {}

    preset = str(gf.get("created_at_preset") or "").strip()
    if preset:
        chips.append(f"Created: {preset}")

    def _add_multi(label: str, key: str, upper: bool = False) -> None:
        vals = gf.get(key)
        if not vals:
            return
        if isinstance(vals, (list, tuple, set)):
            items = [str(x or "").strip() for x in vals if str(x or "").strip()]
        else:
            items = [str(vals).strip()] if str(vals).strip() else []
        if not items:
            return
        if upper:
            items = [s.upper() for s in items]
        if len(items) > 3:
            chips.append(f"{label}: {items[0]}, {items[1]}, +{len(items)-2}")
        else:
            chips.append(f"{label}: {', '.join(items)}")

    _add_multi("Accounts", "account_ids")
    _add_multi("Exchange", "exchanges", upper=True)
    _add_multi("Market", "market_types", upper=True)
    _add_multi("Symbols", "symbols", upper=True)
    _add_multi("Strategy", "strategies")
    _add_multi("TF", "timeframes")

    if not chips:
        return

    st.markdown("".join([f"<span class='dragon-chip'>{c}</span>" for c in chips]), unsafe_allow_html=True)


def render_kpi_tile(label: str, value: str, subtext: Optional[str] = None) -> None:
    # IMPORTANT: keep HTML unindented; Markdown treats 4+ leading spaces as a code block.
    sub_html = f'<div class="dragon-kpi-subtext">{subtext}</div>' if subtext else ""
    st.markdown(
        f"<div class=\"dragon-kpi-card\">"
        f"<div class=\"dragon-kpi-wrap\">"
        f"<div class=\"dragon-kpi-label\">{label}</div>"
        f"<div class=\"dragon-kpi-value\">{value}</div>"
        f"{sub_html}"
        f"</div>"
        f"</div>",
        unsafe_allow_html=True,
    )


def render_card(title: str, render_fn: Callable[[], None]) -> None:
    with st.container(border=True):
        st.markdown(f"<div class='dragon-card-title'>{title}</div>", unsafe_allow_html=True)
        render_fn()


def aggrid_pill_style(kind: str) -> Any:
    """Shared pill/badge cellStyle for AgGrid columns.

    kind: 'timeframe' | 'status' | 'risk'
    """

    if JsCode is None:
        return None

    kind_n = (kind or "").strip().lower()

    if kind_n == "timeframe":
        return JsCode(
            r"""
            function(params) {
                const raw = (params.value || '').toString().trim().toLowerCase();
                if (!raw) { return {}; }
                const m = raw.match(/^(\d+)([mhdw])$/);
                if (!m) {
                    return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.10)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                const n = Number(m[1]);
                const u = m[2];
                if (!isFinite(n) || n <= 0) { return {}; }
                let minutes = n;
                if (u === 'h') minutes = n * 60;
                else if (u === 'd') minutes = n * 1440;
                else if (u === 'w') minutes = n * 10080;

                // Semantics: larger timeframes look "healthier" (green).
                if (minutes >= 240) {
                    return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (minutes >= 15) {
                    return { color: '#facc15', backgroundColor: 'rgba(250,204,21,0.16)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                return { color: '#b6d4fe', backgroundColor: 'rgba(56,139,253,0.14)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
            }
            """
        )

    if kind_n == "status":
        return JsCode(
            r"""
            function(params) {
                const s = (params.value || '').toString().trim().toLowerCase();
                if (!s) { return {}; }
                const parts = s.split('→').map(x => x.trim());
                const eff = (parts.length >= 2 && parts[1]) ? parts[1] : parts[0];

                // More specific terms first.
                if (eff.includes('cancelling') || eff.includes('canceling')) {
                    return { color: '#b6d4fe', backgroundColor: 'rgba(56,139,253,0.14)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('error')) {
                    return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('fail')) {
                    return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('run')) {
                    return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('done') || eff.includes('complete') || eff.includes('success')) {
                    // Differentiate "Done" from "Running" (purple vs green).
                    return { color: '#a371f7', backgroundColor: 'rgba(163,113,247,0.18)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('pause')) {
                    return { color: '#facc15', backgroundColor: 'rgba(250,204,21,0.16)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('queue') || eff.includes('starting') || eff.includes('stopping')) {
                    return { color: '#b6d4fe', backgroundColor: 'rgba(56,139,253,0.14)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('cancel')) {
                    return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.10)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
                }
                return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.12)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
            }
            """
        )

    # risk / degraded
    return JsCode(
        r"""
        function(params) {
            const s = (params.value || '').toString().trim().toUpperCase();
            if (!s) { return { color: '#94a3b8', textAlign: 'center' }; }
            if (s.includes('RISK') || s.includes('OPEN')) {
                return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.14)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
            }
            if (s.includes('HALF_OPEN') || s.includes('DEG') || s.includes('STALE') || s.includes('HEDGE') || s.includes('KILL') || s.includes('WARN')) {
                return { color: '#facc15', backgroundColor: 'rgba(250,204,21,0.14)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
            }
            return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.10)', fontWeight: '500', textAlign: 'center', borderRadius: '999px' };
        }
        """
    )
