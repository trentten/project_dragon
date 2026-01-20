from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from project_dragon.indicators import htf_ma_mapping, moving_average


def test_htf_ma_mapping_matches_reference() -> None:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    timestamps = [start + timedelta(hours=i) for i in range(24)]
    closes = [float(i + 1) for i in range(24)]

    period = 3
    interval_min = 360  # 6H

    # Reference implementation: resample last close, compute SMA on HTF, then ffill to base.
    base_series = pd.Series(closes, index=pd.to_datetime(timestamps, utc=True))
    htf_closes = base_series.resample("360min", label="right", closed="right").last().dropna()
    htf_values = [float(v) for v in htf_closes.values]
    htf_ma_values = []
    for i in range(len(htf_values)):
        ma_val = moving_average(htf_values[: i + 1], period, "sma")
        htf_ma_values.append(float("nan") if ma_val is None else float(ma_val))
    htf_ma_ref = pd.Series(htf_ma_values, index=htf_closes.index)
    mapped_ref = htf_ma_ref.reindex(base_series.index, method="ffill")

    _, _, _, mapped_out = htf_ma_mapping(timestamps, closes, interval_min, period, "sma")

    pd.testing.assert_series_equal(mapped_out, mapped_ref)
