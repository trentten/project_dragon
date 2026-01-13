from __future__ import annotations

from project_dragon.runs_explorer import compute_config_diff_flags, flatten_config


def test_flatten_config_nested_and_list_indices() -> None:
    cfg = {
        "tp": {"mode": "atr", "pct": 1.5},
        "dca": {"levels": [{"mult": 1.2}, {"mult": 1.5}]},
    }
    flat = flatten_config(cfg, strategy="dragon_dca_atr")
    assert flat.get("tp.mode") == "atr"
    assert flat.get("tp.pct") == 1.5
    assert flat.get("dca.levels.0.mult") == 1.2
    assert flat.get("dca.levels.1.mult") == 1.5


def test_flatten_config_skips_runtime_and_secrets() -> None:
    cfg = {
        "_runtime": {"last_ts": 123},
        "api_key": "SECRET",
        "tp": {"mode": "fixed"},
    }
    flat = flatten_config(cfg, strategy="x")
    assert "_runtime.last_ts" not in flat
    assert "api_key" not in flat
    assert flat.get("tp.mode") == "fixed"


def test_compute_config_diff_flags_identical_vs_differing() -> None:
    cfgs = [
        {"a": 1, "b": "x"},
        {"a": 1, "b": "y"},
        {"a": 1, "b": "x"},
    ]
    modes, flags = compute_config_diff_flags(cfgs, keys=["a", "b"])
    assert modes["a"] == 1
    assert modes["b"] == "x"

    # a identical across all => no diffs
    assert all(not f["a"] for f in flags)

    # b differs only for the middle row
    assert flags[0]["b"] is False
    assert flags[1]["b"] is True
    assert flags[2]["b"] is False
