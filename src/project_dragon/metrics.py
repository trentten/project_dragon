from __future__ import annotations

import math
from statistics import mean
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .domain import Trade


def compute_metrics(equity_curve: Iterable[float], trades: List[Trade], initial_equity: float) -> Dict[str, float]:
    equity = list(equity_curve)
    metrics: Dict[str, float] = {}

    if not equity or initial_equity <= 0:
        return metrics

    final_equity = equity[-1]
    net_profit = final_equity - initial_equity
    return_pct = net_profit / initial_equity if initial_equity else 0.0

    max_dd_abs, max_dd_pct = _max_drawdown(equity)
    win_rate, profit_factor_trades, gross_profit, gross_loss = _trade_quality(trades)
    sharpe = _sharpe_ratio(equity)
    sortino = _sortino_ratio(equity)

    returns = _periodic_returns(equity)
    gain_to_pain = _gain_to_pain(returns)
    tail_ratio = _tail_ratio(returns)
    common_sense_ratio = _common_sense_ratio(tail_ratio, gain_to_pain)

    reward_risk = _reward_risk_ratio(trades)
    base_cpc = net_profit / max_dd_abs if max_dd_abs > 0 else None
    cpc_index = _cpc_index(win_rate, profit_factor_trades, reward_risk, base_cpc)

    avg_pos_time_s = _avg_position_time_seconds(trades)

    metrics.update(
        {
            "final_equity": final_equity,
            "net_profit": net_profit,
            "net_return_pct": return_pct,
            "return_pct": return_pct,
            "max_drawdown_abs": max_dd_abs,
            "max_drawdown_pct": max_dd_pct,
            "max_drawdown": max_dd_pct,
            "win_rate": win_rate,
            "profit_factor": profit_factor_trades,
            "sharpe": sharpe,
            "sharpe_ratio": sharpe,
            "sortino": sortino,
            "sortino_ratio": sortino,
            "gain_to_pain": gain_to_pain,
            "tail_ratio": tail_ratio,
            "cpc_index": cpc_index,
            "common_sense_ratio": common_sense_ratio,
            "total_trades": len(trades),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "reward_risk": reward_risk,
            "avg_position_time_s": avg_pos_time_s,
        }
    )
    return metrics


def _avg_position_time_seconds(trades: List[Trade]) -> Optional[float]:
    """Average time-in-position for *closed* positions.

    We infer position open/close from the trade stream by tracking net position size:
    - BUY (Side.LONG) increases net position
    - SELL (Side.SHORT) decreases net position

    A position is considered "open" when net size transitions 0 -> non-zero, and
    "closed" when it transitions non-zero -> 0.

    Returns seconds (float) or None when not computable.
    """

    if not trades:
        return None

    net_qty = 0.0
    open_ts: Optional[datetime] = None
    durations: List[float] = []

    for t in trades:
        ts = getattr(t, "timestamp", None)
        if ts is None:
            continue
        try:
            qty = float(getattr(t, "size", 0.0) or 0.0)
        except Exception:
            qty = 0.0
        if qty <= 0:
            continue

        prev = net_qty
        try:
            side = getattr(t, "side", None)
            if getattr(side, "name", None) == "LONG" or str(side) == "Side.LONG":
                net_qty += qty
            else:
                net_qty -= qty
        except Exception:
            continue

        # Open: 0 -> non-zero
        if prev == 0.0 and net_qty != 0.0 and open_ts is None:
            open_ts = ts

        # Close: non-zero -> 0
        if prev != 0.0 and net_qty == 0.0 and open_ts is not None:
            try:
                dt = (ts - open_ts).total_seconds()
            except Exception:
                dt = None
            if dt is not None and dt >= 0:
                durations.append(float(dt))
            open_ts = None

    if not durations:
        return None
    try:
        return float(mean(durations))
    except Exception:
        return None


def compute_max_drawdown(equity_curve: Iterable[float]) -> tuple[float, float]:
    """Compute max drawdown (absolute, percent) for an equity-like series.

    Percent drawdown is returned as a ratio (e.g., 0.25 for -25%).
    """

    equity = list(equity_curve)
    if not equity:
        return 0.0, 0.0
    return _max_drawdown(equity)


def _max_drawdown(equity: List[float]) -> tuple[float, float]:
    peak = equity[0]
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    for value in equity:
        if value > peak:
            peak = value
        drawdown = peak - value
        if drawdown > max_dd_abs:
            max_dd_abs = drawdown
            max_dd_pct = (drawdown / peak) if peak > 0 else 0.0
    return max_dd_abs, max_dd_pct


def _trade_quality(trades: List[Trade]) -> tuple[float, Optional[float], float, float]:
    wins = losses = 0
    gross_profit = gross_loss = 0.0
    for trade in trades:
        pnl = getattr(trade, "pnl", None)
        if pnl is None:
            pnl = getattr(trade, "realized_pnl", 0.0)
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        elif pnl < 0:
            losses += 1
            gross_loss += pnl

    total_closed = wins + losses
    win_rate = (wins / total_closed) if total_closed > 0 else float("nan")
    if gross_loss < 0:
        profit_factor = gross_profit / abs(gross_loss)
    elif gross_profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = None
    return win_rate, profit_factor, gross_profit, gross_loss


def _periodic_returns(equity: List[float]) -> List[float]:
    returns: List[float] = []
    for prev, curr in zip(equity[:-1], equity[1:]):
        if prev > 0:
            returns.append((curr - prev) / prev)
    return returns


def _sharpe_ratio(equity: List[float]) -> float:
    returns = _periodic_returns(equity)
    if not returns:
        return float("nan")
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / len(returns)
    std = math.sqrt(variance)
    if std == 0:
        return float("nan")
    return mean / std * math.sqrt(len(returns))


def _sortino_ratio(equity: List[float]) -> float:
    returns = _periodic_returns(equity)
    if not returns:
        return float("nan")
    mean = sum(returns) / len(returns)
    downside = [min(0.0, r) for r in returns]
    downside_variance = sum(d ** 2 for d in downside) / len(returns)
    downside_std = math.sqrt(downside_variance)
    if downside_std == 0:
        return float("nan")
    return mean / downside_std * math.sqrt(len(returns))


def _gain_to_pain(returns: List[float]) -> Optional[float]:
    if not returns:
        return None
    pos = sum(r for r in returns if r > 0)
    neg = sum(r for r in returns if r < 0)
    if neg >= 0:
        return None
    return pos / abs(neg)


def _tail_ratio(returns: List[float]) -> Optional[float]:
    if not returns or len(returns) < 2:
        return None
    ordered = sorted(returns)
    hi = _percentile(ordered, 95)
    lo = _percentile(ordered, 5)
    if lo is None or hi is None or lo >= 0:
        return None
    return hi / abs(lo)


def _common_sense_ratio(tail_ratio: Optional[float], gain_to_pain: Optional[float]) -> Optional[float]:
    if tail_ratio is None or gain_to_pain is None:
        return None
    return tail_ratio * gain_to_pain


def _reward_risk_ratio(trades: List[Trade]) -> Optional[float]:
    if not trades:
        return None
    profits = [getattr(t, "pnl", getattr(t, "realized_pnl", 0.0)) for t in trades if getattr(t, "pnl", getattr(t, "realized_pnl", 0.0)) > 0]
    losses = [getattr(t, "pnl", getattr(t, "realized_pnl", 0.0)) for t in trades if getattr(t, "pnl", getattr(t, "realized_pnl", 0.0)) < 0]
    if not profits or not losses:
        return None
    avg_win = mean(profits)
    avg_loss = mean(losses)
    if avg_loss >= 0:
        return None
    return avg_win / abs(avg_loss)


def _cpc_index(
    win_rate: float,
    profit_factor: Optional[float],
    reward_risk: Optional[float],
    fallback: Optional[float] = None,
) -> Optional[float]:
    if profit_factor is not None and reward_risk is not None and math.isfinite(win_rate):
        return profit_factor * win_rate * reward_risk
    return fallback


def _percentile(sorted_values: List[float], percentile: float) -> Optional[float]:
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * (percentile / 100)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    d0 = sorted_values[int(f)] * (c - k)
    d1 = sorted_values[int(c)] * (k - f)
    return d0 + d1


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        f = float(value)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def get_effective_leverage(obj: Any) -> Optional[float]:
    """Best-effort leverage extraction.

    Supports dict-like configs/snapshots and JSON strings.
    Returns None when missing/invalid/non-positive.
    """

    import json

    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return None
        try:
            obj = json.loads(s)
        except Exception:
            return None

    if isinstance(obj, list):
        for item in obj:
            if not isinstance(item, dict):
                continue
            lev = _safe_float(item.get("leverage"))
            if lev is not None and lev > 0:
                return lev
            inner_pos = item.get("position") if isinstance(item.get("position"), dict) else None
            if isinstance(inner_pos, dict):
                lev = _safe_float(inner_pos.get("leverage"))
                if lev is not None and lev > 0:
                    return lev
        return None

    if not isinstance(obj, dict):
        return None

    # Config shape: {_futures: {leverage}}, runtime: {_runtime: {applied_leverage}}
    fut = obj.get("_futures") if isinstance(obj.get("_futures"), dict) else None
    if isinstance(fut, dict):
        lev = _safe_float(fut.get("leverage"))
        if lev is not None and lev > 0:
            return lev

    runtime = obj.get("_runtime") if isinstance(obj.get("_runtime"), dict) else None
    if isinstance(runtime, dict):
        lev = _safe_float(runtime.get("applied_leverage"))
        if lev is not None and lev > 0:
            return lev

    # Snapshot shapes: sometimes leverage is top-level or nested.
    lev = _safe_float(obj.get("leverage"))
    if lev is not None and lev > 0:
        return lev

    pos = obj.get("position") if isinstance(obj.get("position"), dict) else None
    if isinstance(pos, dict):
        lev = _safe_float(pos.get("leverage"))
        if lev is not None and lev > 0:
            return lev

    # Common API wrappers: {data: [{...}]}
    for wrapper_key in ("data", "result", "rows"):
        wrapped = obj.get(wrapper_key)
        if isinstance(wrapped, list):
            for item in wrapped:
                if not isinstance(item, dict):
                    continue
                lev = _safe_float(item.get("leverage"))
                if lev is not None and lev > 0:
                    return lev
                inner_pos = item.get("position") if isinstance(item.get("position"), dict) else None
                if isinstance(inner_pos, dict):
                    lev = _safe_float(inner_pos.get("leverage"))
                    if lev is not None and lev > 0:
                        return lev

    totals = obj.get("totals") if isinstance(obj.get("totals"), dict) else None
    if isinstance(totals, dict):
        lev = _safe_float(totals.get("leverage"))
        if lev is not None and lev > 0:
            return lev

    return None


def compute_roi_pct_on_margin(
    *,
    net_pnl: Any,
    notional: Any,
    leverage: Any,
) -> Optional[float]:
    """Compute ROI% on initial margin.

    margin = abs(notional) / leverage
    roi_pct = (net_pnl / margin) * 100
    """

    pnl_f = _safe_float(net_pnl)
    notional_f = _safe_float(notional)
    lev_f = _safe_float(leverage)
    if pnl_f is None or notional_f is None or lev_f is None:
        return None
    if lev_f <= 0:
        return None

    margin = abs(notional_f) / lev_f
    if margin <= 0:
        return None
    return (pnl_f / margin) * 100.0
