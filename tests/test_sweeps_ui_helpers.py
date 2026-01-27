from __future__ import annotations

from project_dragon.streamlit_app import _aggrid_metrics_gradient_style_js, _sanitize_widget_choice


def test_sanitize_widget_choice_maps_case() -> None:
    state = {"sweep_kind": "range"}
    out = _sanitize_widget_choice(state, "sweep_kind", ["Range", "List"], default="Range")
    assert out == "Range"
    assert state["sweep_kind"] == "Range"


def test_sanitize_widget_choice_invalid_defaults() -> None:
    state = {"sweep_kind": "bad"}
    out = _sanitize_widget_choice(state, "sweep_kind", ["Range", "List"], default="Range")
    assert out == "Range"
    assert state["sweep_kind"] == "Range"


def test_metrics_gradient_style_includes_roi() -> None:
    js = _aggrid_metrics_gradient_style_js()
    assert "roi_pct_on_margin" in js
