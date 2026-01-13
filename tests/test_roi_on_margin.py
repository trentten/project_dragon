import pytest

from project_dragon.metrics import compute_roi_pct_on_margin, get_effective_leverage


def test_compute_roi_pct_on_margin_basic() -> None:
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=10.0, leverage=10.0) == pytest.approx(100.0)
    assert compute_roi_pct_on_margin(net_pnl=-1.0, notional=10.0, leverage=10.0) == pytest.approx(-100.0)
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=10.0, leverage=1.0) == pytest.approx(10.0)


def test_compute_roi_pct_on_margin_missing_or_invalid() -> None:
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=None, leverage=10.0) is None
    assert compute_roi_pct_on_margin(net_pnl=None, notional=10.0, leverage=10.0) is None
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=10.0, leverage=None) is None
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=10.0, leverage=0.0) is None
    assert compute_roi_pct_on_margin(net_pnl=1.0, notional=10.0, leverage=-5.0) is None


def test_get_effective_leverage_from_config_dict() -> None:
    assert get_effective_leverage({"_futures": {"leverage": 10}}) == pytest.approx(10.0)
    assert get_effective_leverage({"_runtime": {"applied_leverage": 5}}) == pytest.approx(5.0)


def test_get_effective_leverage_from_json_string() -> None:
    assert get_effective_leverage('{"_futures": {"leverage": 12}}') == pytest.approx(12.0)


def test_get_effective_leverage_from_snapshot_shapes() -> None:
    assert get_effective_leverage({"data": [{"leverage": "20"}]}) == pytest.approx(20.0)
    assert get_effective_leverage([{"foo": 1}, {"leverage": 7}]) == pytest.approx(7.0)


def test_get_effective_leverage_rejects_non_positive() -> None:
    assert get_effective_leverage({"_futures": {"leverage": 0}}) is None
    assert get_effective_leverage({"_futures": {"leverage": -1}}) is None
