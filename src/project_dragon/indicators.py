from __future__ import annotations

import math
from typing import Optional, Sequence, Tuple


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
