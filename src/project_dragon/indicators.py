from __future__ import annotations

from datetime import datetime
import math
from typing import Optional, Sequence, Tuple

import pandas as pd


def moving_average(values: Sequence[float], period: int, ma_type: str = "sma") -> Optional[float]:
    if period <= 0 or len(values) < period:
        return None
    window = values[-period:]
    if ma_type and ma_type.lower().startswith("ema"):
        alpha = 2.0 / (period + 1.0)
        ema_val = window[0]
        for price in window[1:]:
            ema_val = alpha * price + (1 - alpha) * ema_val
        return ema_val
    return sum(window) / period


def _to_utc_index(timestamps: Sequence[datetime]) -> pd.DatetimeIndex:
    if timestamps is None:
        return pd.DatetimeIndex([])
    try:
        if len(timestamps) == 0:
            return pd.DatetimeIndex([])
    except Exception:
        return pd.DatetimeIndex([])
    return pd.to_datetime(list(timestamps), utc=True)


def _infer_base_interval_min(index: pd.DatetimeIndex) -> Optional[float]:
    if index is None or len(index) < 2:
        return None
    try:
        diffs = index.to_series().diff().dt.total_seconds() / 60.0
        diffs = diffs[diffs > 0]
        if diffs.empty:
            return None
        return float(diffs.iloc[0])
    except Exception:
        return None


def resample_last_close_series(
    timestamps: Sequence[datetime],
    closes: Sequence[float],
    interval_min: int,
) -> pd.Series:
    if timestamps is None or closes is None:
        return pd.Series(dtype=float)
    try:
        if len(timestamps) == 0 or len(closes) == 0:
            return pd.Series(dtype=float)
    except Exception:
        return pd.Series(dtype=float)
    base_index = _to_utc_index(timestamps)
    base_series = pd.Series(list(closes), index=base_index).sort_index()
    try:
        interval_val = int(interval_min or 0)
    except (TypeError, ValueError):
        interval_val = 0
    if interval_val <= 0:
        return base_series

    base_min = _infer_base_interval_min(base_series.index)
    if base_min is not None and interval_val <= base_min + 1e-9:
        return base_series

    rule = f"{interval_val}min"
    return base_series.resample(rule, label="right", closed="right").last().dropna()


def htf_ma_mapping(
    timestamps: Sequence[datetime],
    closes: Sequence[float],
    interval_min: int,
    period: int,
    ma_type: str = "sma",
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    base_series = pd.Series(list(closes), index=_to_utc_index(timestamps)).sort_index()
    htf_closes = resample_last_close_series(base_series.index, base_series.values, interval_min)

    if htf_closes.empty:
        empty = pd.Series(dtype=float)
        mapped_empty = pd.Series(index=base_series.index, dtype=float)
        return base_series, htf_closes, empty, mapped_empty

    values = [float(v) for v in htf_closes.values]
    ma_values: list[float] = []
    for i in range(len(values)):
        ma_val = moving_average(values[: i + 1], period, ma_type)
        ma_values.append(float(ma_val) if ma_val is not None else math.nan)

    htf_ma = pd.Series(ma_values, index=htf_closes.index)
    mapped_ma = htf_ma.reindex(base_series.index, method="ffill")
    return base_series, htf_closes, htf_ma, mapped_ma


def bollinger_bands(
    values: Sequence[float],
    period: int,
    dev_up: float = 2.0,
    dev_down: float = 2.0,
    ma_type: str = "sma",
) -> Optional[Tuple[float, float, float]]:
    if period <= 0 or len(values) < period:
        return None
    mid = moving_average(values, period, ma_type)
    if mid is None:
        return None
    window = values[-period:]
    variance = sum((x - mid) ** 2 for x in window) / len(window)
    std = math.sqrt(variance)
    upper = mid + std * dev_up
    lower = mid - std * dev_down
    return mid, upper, lower


def macd(values: Sequence[float], fast: int, slow: int, signal: int) -> Optional[Tuple[float, float, float]]:
    if fast <= 0 or slow <= 0 or signal <= 0 or len(values) < slow + signal:
        return None
    alpha_fast = 2.0 / (fast + 1.0)
    alpha_slow = 2.0 / (slow + 1.0)
    fast_ema = slow_ema = None
    macd_line_series: list[float] = []
    for price in values:
        fast_ema = price if fast_ema is None else alpha_fast * price + (1 - alpha_fast) * fast_ema
        slow_ema = price if slow_ema is None else alpha_slow * price + (1 - alpha_slow) * slow_ema
        macd_line_series.append(fast_ema - slow_ema)
    signal_alpha = 2.0 / (signal + 1.0)
    signal_val = None
    for value in macd_line_series:
        signal_val = value if signal_val is None else signal_alpha * value + (1 - signal_alpha) * signal_val
    if signal_val is None:
        return None
    macd_value = macd_line_series[-1]
    hist = macd_value - signal_val
    return macd_value, signal_val, hist


def rsi(values: Sequence[float], period: int) -> Optional[float]:
    if period <= 0 or len(values) < period + 1:
        return None
    window = values[-(period + 1):]
    gains = losses = 0.0
    for prev, curr in zip(window[:-1], window[1:]):
        delta = curr - prev
        if delta >= 0:
            gains += delta
        else:
            losses += -delta
    avg_gain = gains / period
    avg_loss = losses / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))
