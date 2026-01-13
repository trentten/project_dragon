from __future__ import annotations

from datetime import date
from typing import Any, Callable, Dict, List, Optional

import streamlit as st

try:
    from st_aggrid import JsCode  # type: ignore
except Exception:  # pragma: no cover
    JsCode = None  # type: ignore


def inject_trade_stream_css(max_width_px: int = 1920) -> None:
    """Global CSS tweaks for a tighter, wider layout (Phase 1)."""

    max_width_px = int(max(1200, min(2400, max_width_px)))
    st.markdown(
        f"""
        <style>
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
            border: 1px solid rgba(148,163,184,0.30);
            background: rgba(148,163,184,0.10);
            color: rgba(226,232,240,0.95);
          }}

          /* KPI tile */
                    .dragon-kpi-card {{
                        border: 1px solid rgba(148,163,184,0.22);
                        border-radius: 0.75rem;
                        padding: 0.45rem 0.60rem;
                        margin-bottom: 24px;
                        background: rgba(148,163,184,0.06);
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
            color: rgba(148,163,184,0.95);
            margin-bottom: 0.15rem;
          }}
          .dragon-kpi-value {{
            font-size: 1.35rem;
            font-weight: 800;
            line-height: 1.1;
          }}
          .dragon-kpi-subtext {{
            font-size: 0.78rem;
            color: rgba(148,163,184,0.95);
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
                                    border-bottom: 1px solid rgba(148,163,184,0.16);
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
                                    color: rgba(148,163,184,0.95);
                                    font-weight: 650;
                                    white-space: nowrap;
                                }}
                                .dragon-metrics-table td.value {{
                                    text-align: right;
                                    font-weight: 800;
                                    white-space: nowrap;
                                }}
                                .dragon-row-bg {{
                                    background: rgba(148,163,184,0.06);
                                }}
                                .dragon-muted {{
                                    color: rgba(148,163,184,0.85);
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
                                    color: rgba(148,163,184,0.95);
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
                                    color: rgba(148,163,184,0.95);
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
                                    background: rgba(148,163,184,0.14);
                                    border: 1px solid rgba(148,163,184,0.18);
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
                if (eff.includes('error')) {
                    return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.18)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('run')) {
                    return { color: '#7ee787', backgroundColor: 'rgba(46,160,67,0.18)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('pause')) {
                    return { color: '#facc15', backgroundColor: 'rgba(250,204,21,0.16)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
                }
                if (eff.includes('queue') || eff.includes('starting') || eff.includes('stopping')) {
                    return { color: '#b6d4fe', backgroundColor: 'rgba(56,139,253,0.14)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
                }
                return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.12)', fontWeight: '700', textAlign: 'center', borderRadius: '999px' };
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
                return { color: '#ff7b72', backgroundColor: 'rgba(248,81,73,0.14)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
            }
            if (s.includes('HALF_OPEN') || s.includes('DEG') || s.includes('STALE') || s.includes('HEDGE') || s.includes('KILL') || s.includes('WARN')) {
                return { color: '#facc15', backgroundColor: 'rgba(250,204,21,0.14)', fontWeight: '800', textAlign: 'center', borderRadius: '999px' };
            }
            return { color: '#cbd5e1', backgroundColor: 'rgba(148,163,184,0.10)', fontWeight: '700', textAlign: 'center', borderRadius: '999px' };
        }
        """
    )
