from __future__ import annotations

from project_dragon.streamlit_app import DURATION_CHOICES, SWEEPABLE_FIELDS, _SWEEP_FIELD_KEY_TO_BASE_KEY


def test_duration_choices_include_longer_ranges() -> None:
    assert "3 months" in DURATION_CHOICES
    assert "6 months" in DURATION_CHOICES
    assert "12 months" in DURATION_CHOICES


def test_entry_fields_removed_from_sweeps() -> None:
    assert "general.max_entries" not in SWEEPABLE_FIELDS
    assert "general.entry_timeout_min" not in SWEEPABLE_FIELDS
    assert "general.entry_cooldown_min" not in SWEEPABLE_FIELDS

    assert "general.max_entries" not in _SWEEP_FIELD_KEY_TO_BASE_KEY
    assert "general.entry_timeout_min" not in _SWEEP_FIELD_KEY_TO_BASE_KEY
    assert "general.entry_cooldown_min" not in _SWEEP_FIELD_KEY_TO_BASE_KEY
