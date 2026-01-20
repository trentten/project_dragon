from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from project_dragon.config_dragon import (
    BBandsConfig,
    DcaSettings,
    DragonAlgoConfig,
    DynamicActivationConfig,
    ExitSettings,
    GeneralSettings,
    InitialEntrySizingMode,
    MACDConfig,
    OrderStyle,
    RSIConfig,
    StopLossMode,
    TakeProfitMode,
    TrendFilterConfig,
)
from project_dragon.candle_cache import get_candles_analysis_window
from project_dragon.data_online import get_candles_with_cache, load_ccxt_candles
from project_dragon.domain import Candle
from project_dragon.engine import BacktestConfig, BacktestEngine
from project_dragon.strategy_dragon import DragonDcaAtrStrategy


@dataclass
class DataSettings:
    data_source: str
    exchange_id: Optional[str] = None
    market_type: Optional[str] = None
    symbol: Optional[str] = None
    timeframe: Optional[str] = None
    range_mode: str = "bars"
    range_params: Dict[str, Any] = None
    initial_balance: float = 0.0
    fee_rate: float = 0.0

    def __post_init__(self) -> None:
        if self.range_params is None:
            self.range_params = {}


def timeframe_to_minutes(tf: str) -> int:
    s = str(tf or "").strip().lower()
    if not s:
        return 0
    mult = 1
    if s.endswith("m"):
        mult = 1
        num = s[:-1]
    elif s.endswith("h"):
        mult = 60
        num = s[:-1]
    elif s.endswith("d"):
        mult = 60 * 24
        num = s[:-1]
    elif s.endswith("w"):
        mult = 60 * 24 * 7
        num = s[:-1]
    else:
        # Try raw minutes.
        num = s
        mult = 1
    try:
        return int(float(num) * mult)
    except Exception:
        return 0


def timeframe_to_seconds(tf: str) -> int:
    return int(max(1, timeframe_to_minutes(tf)) * 60)


def estimate_indicator_warmup_seconds(cfg: DragonAlgoConfig, base_timeframe: str) -> int:
    """Estimate warmup seconds required for multi-timeframe indicators.

    This uses the *indicator* timeframe for warmup, not the base timeframe.
    """

    base_sec = timeframe_to_seconds(base_timeframe)
    if base_sec <= 0:
        base_sec = 60

    def _sec_for(interval_min: int, length: int, *, mult: float = 1.0) -> int:
        try:
            interval_s = int(max(1, int(interval_min))) * 60
        except Exception:
            interval_s = 60
        try:
            ln = int(max(1, int(length)))
        except Exception:
            ln = 1
        seconds = int(math.ceil(float(ln) * float(interval_s) * float(mult)))
        # Always ensure at least one base bar extra for stability.
        return max(int(base_sec), int(seconds) + int(base_sec))

    warmup_s: list[int] = []

    # Trend MA (multi-timeframe)
    try:
        ma_type = str(cfg.trend.ma_type or "sma").lower()
        ma_mult = 2.0 if ma_type.startswith("ema") else 1.0
        warmup_s.append(_sec_for(cfg.trend.ma_interval_min, cfg.trend.ma_len, mult=ma_mult))
    except Exception:
        pass

    # Bollinger Bands
    try:
        bb_mult = 2.0 if str(cfg.bbands.ma_type or "sma").lower().startswith("ema") else 1.0
        warmup_s.append(_sec_for(cfg.bbands.interval_min, cfg.bbands.length, mult=bb_mult))
    except Exception:
        pass

    # MACD (EMA-based): use slow + signal
    try:
        macd_len = int(getattr(cfg.macd, "slow", 26) or 26) + int(getattr(cfg.macd, "signal", 9) or 9)
        warmup_s.append(_sec_for(cfg.macd.interval_min, macd_len, mult=2.0))
    except Exception:
        pass

    # RSI
    try:
        warmup_s.append(_sec_for(cfg.rsi.interval_min, int(getattr(cfg.rsi, "length", 14) or 14) + 1, mult=1.0))
    except Exception:
        pass

    # ATR (base timeframe)
    try:
        atr_len = int(getattr(cfg.exits, "atr_period", 0) or 0)
    except Exception:
        atr_len = 0
    if atr_len > 0:
        warmup_s.append(_sec_for(timeframe_to_minutes(base_timeframe), atr_len + 1, mult=1.0))

    # Entry MA gate (base timeframe)
    try:
        g = cfg.general
        g_len = int(getattr(g, "ma_length", 0) or 0)
        if g_len > 0:
            g_mult = 2.0 if str(getattr(g, "ma_type", "sma") or "sma").lower().startswith("ema") else 1.0
            warmup_s.append(_sec_for(timeframe_to_minutes(base_timeframe), g_len, mult=g_mult))
    except Exception:
        pass

    return int(max(warmup_s) if warmup_s else 0)


def compute_analysis_window(cfg: DragonAlgoConfig, data_settings: DataSettings) -> Dict[str, Any]:
    """Derive analysis vs display windows for multi-timeframe indicators."""

    params = data_settings.range_params or {}
    range_mode = str(data_settings.range_mode or "bars").lower()
    base_tf = str(data_settings.timeframe or "1h")
    base_sec = timeframe_to_seconds(base_tf)
    warmup_seconds = estimate_indicator_warmup_seconds(cfg, base_tf)
    warmup_bars = int(math.ceil(float(warmup_seconds) / float(base_sec))) if base_sec > 0 else 0

    def _to_ms(v: Any) -> Optional[int]:
        dt = _normalize_timestamp(v)
        if dt is None:
            return None
        return int(dt.timestamp() * 1000)

    display_start_ms = _to_ms(params.get("since"))
    display_end_ms = _to_ms(params.get("until"))
    display_limit = None
    analysis_limit = None

    if range_mode == "bars" and display_start_ms is None and display_end_ms is None:
        try:
            display_limit = int(params.get("limit")) if params.get("limit") is not None else None
        except Exception:
            display_limit = None
        if display_limit is not None and warmup_bars > 0:
            analysis_limit = int(display_limit) + int(warmup_bars)
    else:
        # Explicit range: compute analysis_start from display_start.
        if display_start_ms is not None and warmup_seconds > 0:
            analysis_limit = None

    analysis_start_ms = None
    if display_start_ms is not None and warmup_seconds > 0:
        analysis_start_ms = int(display_start_ms) - int(warmup_seconds * 1000)
        if analysis_start_ms < 0:
            analysis_start_ms = 0
    analysis_end_ms = display_end_ms

    effective_range_mode = "range" if (display_start_ms is not None or display_end_ms is not None) else range_mode

    return {
        "range_mode": range_mode,
        "effective_range_mode": effective_range_mode,
        "display_start_ms": display_start_ms,
        "display_end_ms": display_end_ms,
        "display_limit": display_limit,
        "analysis_start_ms": analysis_start_ms,
        "analysis_end_ms": analysis_end_ms,
        "analysis_limit": analysis_limit,
        "warmup_seconds": int(warmup_seconds),
        "warmup_bars": int(warmup_bars),
        "base_timeframe_sec": int(base_sec),
    }


def compute_analysis_window_from_snapshot(
    config_snapshot: Dict[str, Any],
    data_settings: DataSettings,
) -> Dict[str, Any]:
    cfg = dragon_config_from_snapshot(config_snapshot)
    return compute_analysis_window(cfg, data_settings)


def _normalize_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        seconds = value / 1000 if value > 1_000_000_000_000 else value
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def generate_dummy_candles(n: int = 300, *, start_price: float = 100.0) -> List[Candle]:
    candles: List[Candle] = []
    price = float(start_price)
    t = datetime.now(timezone.utc) - timedelta(minutes=max(int(n), 1))
    for i in range(int(n)):
        t = t + timedelta(minutes=1)
        # Deterministic wobble (no randomness so tests are stable)
        delta = ((i % 10) - 5) * 0.01
        open_ = price
        close = max(0.1, price * (1 + delta / 100.0))
        high = max(open_, close) * 1.002
        low = min(open_, close) * 0.998
        volume = 1.0
        candles.append(Candle(timestamp=t, open=open_, high=high, low=low, close=close, volume=volume))
        price = close
    return candles


def _safe_order_style(value: Any, default: OrderStyle = OrderStyle.MARKET) -> OrderStyle:
    if isinstance(value, OrderStyle):
        return value
    if isinstance(value, str):
        try:
            return OrderStyle(value)
        except ValueError:
            try:
                return OrderStyle(value.lower())
            except ValueError:
                return default
    return default


def _safe_initial_entry_sizing_mode(value: Any) -> InitialEntrySizingMode:
    if isinstance(value, InitialEntrySizingMode):
        return value
    raw = str(value or "").strip().lower()
    if raw in {"fixed_usd", "fixed", "usd", "$"} or raw.endswith("fixed_usd"):
        return InitialEntrySizingMode.FIXED_USD
    return InitialEntrySizingMode.PCT_BALANCE


def dragon_config_from_snapshot(config_dict: Dict[str, Any]) -> DragonAlgoConfig:
    if not config_dict:
        raise ValueError("Missing config data")

    general_dict = config_dict.get("general", {}) if isinstance(config_dict.get("general"), dict) else {}
    dynamic_dict = general_dict.get("dynamic_activation", {}) if isinstance(general_dict.get("dynamic_activation"), dict) else {}

    general = GeneralSettings(
        max_entries=int(general_dict.get("max_entries", 1) or 1),
        initial_entry_balance_pct=float(general_dict.get("initial_entry_balance_pct") or 10.0),
        initial_entry_sizing_mode=_safe_initial_entry_sizing_mode(general_dict.get("initial_entry_sizing_mode")),
        initial_entry_fixed_usd=float(general_dict.get("initial_entry_fixed_usd") or 0.0),
        use_indicator_consensus=bool(general_dict.get("use_indicator_consensus", True)),
        entry_order_style=_safe_order_style(general_dict.get("entry_order_style", OrderStyle.MARKET.value)),
        entry_timeout_min=int(general_dict.get("entry_timeout_min", 0) or 0),
        entry_cooldown_min=int(general_dict.get("entry_cooldown_min", 0) or 0),
        exit_order_style=_safe_order_style(general_dict.get("exit_order_style", OrderStyle.MARKET.value)),
        exit_timeout_min=int(general_dict.get("exit_timeout_min", 0) or 0),
        allow_long=bool(general_dict.get("allow_long", True)),
        allow_short=bool(general_dict.get("allow_short", False)),
        lock_atr_on_entry_for_dca=bool(general_dict.get("lock_atr_on_entry_for_dca", False)),
        use_avg_entry_for_dca_base=bool(general_dict.get("use_avg_entry_for_dca_base", True)),
        plot_dca_levels=bool(general_dict.get("plot_dca_levels", False)),
        dynamic_activation=DynamicActivationConfig(
            entry_pct=float(dynamic_dict.get("entry_pct") or 0.0),
            dca_pct=float(dynamic_dict.get("dca_pct") or 0.0),
            tp_pct=float(dynamic_dict.get("tp_pct") or 0.0),
        ),
        prefer_bbo_maker=bool(general_dict.get("prefer_bbo_maker", True)),
        bbo_queue_level=int(general_dict.get("bbo_queue_level", 1) or 1),
        use_ma_direction=bool(general_dict.get("use_ma_direction", False)),
        ma_type=str(general_dict.get("ma_type") or "Ema"),
        ma_length=int(general_dict.get("ma_length") or 200),
        ma_source=str(general_dict.get("ma_source") or "close"),
    )

    exits_dict = config_dict.get("exits", {}) if isinstance(config_dict.get("exits"), dict) else {}

    sl_mode_raw = str(exits_dict.get("sl_mode") or "").strip().upper()
    try:
        sl_mode = StopLossMode(sl_mode_raw) if sl_mode_raw else StopLossMode.PCT
    except Exception:
        sl_mode = StopLossMode.PCT

    tp_mode_raw = str(exits_dict.get("tp_mode") or "").strip().upper()
    if not tp_mode_raw:
        tp_mode_raw = TakeProfitMode.ATR.value if bool(exits_dict.get("use_atr_tp", False)) else TakeProfitMode.PCT.value
    try:
        tp_mode = TakeProfitMode(tp_mode_raw)
    except Exception:
        tp_mode = TakeProfitMode.ATR

    atr_period = int(exits_dict.get("atr_period") or exits_dict.get("tp_atr_period") or 14)

    exits = ExitSettings(
        sl_mode=sl_mode,
        sl_pct=float(exits_dict.get("sl_pct") or exits_dict.get("fixed_sl_pct") or 0.0),
        trail_activation_pct=float(exits_dict.get("trail_activation_pct") or 0.0),
        trail_distance_pct=float(exits_dict.get("trail_distance_pct") or exits_dict.get("trail_stop_pct") or 0.0),
        atr_period=atr_period,
        sl_atr_mult=float(exits_dict.get("sl_atr_mult") or 0.0),
        trail_activation_atr_mult=float(exits_dict.get("trail_activation_atr_mult") or 0.0),
        trail_distance_atr_mult=float(exits_dict.get("trail_distance_atr_mult") or 0.0),
        tp_mode=tp_mode,
        tp_pct=float(exits_dict.get("tp_pct") or exits_dict.get("fixed_tp_pct") or 0.0),
        tp_atr_mult=float(exits_dict.get("tp_atr_mult") or exits_dict.get("tp_atr_multiple") or 0.0),
        tp_replace_threshold_pct=float(exits_dict.get("tp_replace_threshold_pct") or 0.05),
    )

    dca_dict = config_dict.get("dca", {}) if isinstance(config_dict.get("dca"), dict) else {}
    dca = DcaSettings(
        base_deviation_pct=float(dca_dict.get("base_deviation_pct") or 0.9),
        deviation_multiplier=float(dca_dict.get("deviation_multiplier") or 2.0),
        volume_multiplier=float(dca_dict.get("volume_multiplier") or 2.2),
    )

    trend_dict = config_dict.get("trend", {}) if isinstance(config_dict.get("trend"), dict) else {}
    trend = TrendFilterConfig(
        ma_interval_min=int(trend_dict.get("ma_interval_min") or 120),
        ma_len=int(trend_dict.get("ma_len") or trend_dict.get("ma_length") or 250),
        ma_type=str(trend_dict.get("ma_type") or "Sma"),
    )

    bb_dict = config_dict.get("bbands", {}) if isinstance(config_dict.get("bbands"), dict) else {}
    bbands = BBandsConfig(
        interval_min=int(bb_dict.get("interval_min") or 60),
        length=int(bb_dict.get("length") or 12),
        dev_up=float(bb_dict.get("dev_up") or bb_dict.get("bb_dev_up") or 2.0),
        dev_down=float(bb_dict.get("dev_down") or bb_dict.get("bb_dev_down") or 2.0),
        ma_type=str(bb_dict.get("ma_type") or bb_dict.get("bb_ma_type") or "Sma"),
        deviation=float(bb_dict.get("deviation") or bb_dict.get("bbands_deviation") or 0.2),
        require_fcc=bool(bb_dict.get("require_fcc") or bb_dict.get("bb_require_fcc") or False),
        reset_middle=bool(bb_dict.get("reset_middle") or bb_dict.get("bb_reset_middle") or False),
        allow_mid_sells=bool(bb_dict.get("allow_mid_sells") or bb_dict.get("bb_allow_mid_sells") or False),
    )

    macd_dict = config_dict.get("macd", {}) if isinstance(config_dict.get("macd"), dict) else {}
    macd = MACDConfig(
        interval_min=int(macd_dict.get("interval_min") or 360),
        fast=int(macd_dict.get("fast") or macd_dict.get("macd_fast") or 12),
        slow=int(macd_dict.get("slow") or macd_dict.get("macd_slow") or 26),
        signal=int(macd_dict.get("signal") or macd_dict.get("macd_signal") or 9),
    )

    rsi_dict = config_dict.get("rsi", {}) if isinstance(config_dict.get("rsi"), dict) else {}
    rsi = RSIConfig(
        interval_min=int(rsi_dict.get("interval_min") or 1440),
        length=int(rsi_dict.get("length") or 12),
        buy_level=float(rsi_dict.get("buy_level") or rsi_dict.get("rsi_buy_level") or 30.0),
        sell_level=float(rsi_dict.get("sell_level") or rsi_dict.get("rsi_sell_level") or 70.0),
    )

    return DragonAlgoConfig(
        general=general,
        exits=exits,
        dca=dca,
        trend=trend,
        bbands=bbands,
        macd=macd,
        rsi=rsi,
    )


def load_candles_for_settings(data_settings: DataSettings, *, config_snapshot: Optional[Dict[str, Any]] = None) -> Tuple[List[Candle], str, str, str]:
    ds = data_settings
    if str(ds.data_source).lower() == "synthetic":
        count = int((ds.range_params or {}).get("count", 300))
        candles = generate_dummy_candles(n=count)
        return candles, f"Synthetic – {len(candles)} candles", "SYNTH", "synthetic"

    params = ds.range_params or {}
    exchange_id = ds.exchange_id or "binance"
    symbol = ds.symbol or "BTC/USDT"
    timeframe = ds.timeframe or "1h"
    range_mode = (ds.range_mode or "bars").lower()
    market_type = (ds.market_type or "unknown")

    analysis_info: Optional[Dict[str, Any]] = None
    if isinstance(config_snapshot, dict) and config_snapshot:
        try:
            analysis_info = compute_analysis_window_from_snapshot(config_snapshot, ds)
        except Exception:
            analysis_info = None

    try:
        _analysis_start, _display_start, _display_end, _candles_analysis, candles_display = get_candles_analysis_window(
            data_settings=ds,
            analysis_info=analysis_info,
            fetch_fn=get_candles_with_cache,
        )
        candles = list(candles_display)
    except Exception as exc:
        # Best-effort fallback: skip caching when DB is locked.
        # This covers both read/write contention and environments without a writable cache.
        msg = str(exc).lower()
        if "database is locked" in msg or "locked" in msg:
            _analysis_start, _display_start, _display_end, _candles_analysis, candles_display = get_candles_analysis_window(
                data_settings=ds,
                analysis_info=analysis_info,
                fetch_fn=load_ccxt_candles,
            )
            candles = list(candles_display)
        else:
            raise

    label = f"Crypto (CCXT) – {exchange_id} {symbol} {timeframe}, {len(candles)} candles"
    return candles, label, symbol, timeframe


def run_single_backtest(
    config_snapshot: Dict[str, Any],
    data_settings: DataSettings,
    *,
    candles_override: Optional[List[Candle]] = None,
    label_override: Optional[str] = None,
    storage_symbol_override: Optional[str] = None,
    storage_timeframe_override: Optional[str] = None,
) -> Tuple[Dict[str, Any], Any, str, str, str]:
    """Run a single backtest and return (config_for_storage, result, label, storage_symbol, storage_timeframe)."""

    cfg = dragon_config_from_snapshot(config_snapshot)

    analysis_info = compute_analysis_window(cfg, data_settings)

    if candles_override is not None:
        candles = list(candles_override)
        label = str(label_override or "")
        if not label:
            try:
                exchange_id = data_settings.exchange_id or "binance"
                symbol = data_settings.symbol or "BTC/USDT"
                timeframe = data_settings.timeframe or "1h"
                label = f"Crypto (CCXT) – {exchange_id} {symbol} {timeframe}, {len(candles)} candles"
            except Exception:
                label = f"Crypto – {len(candles)} candles"

        storage_symbol = str(storage_symbol_override or (data_settings.symbol or ""))
        storage_timeframe = str(storage_timeframe_override or (data_settings.timeframe or ""))
        # Maintain legacy behavior (never empty) for storage symbol/timeframe.
        storage_symbol = storage_symbol or (data_settings.symbol or "BTC/USDT")
        storage_timeframe = storage_timeframe or (data_settings.timeframe or "1h")
    else:
        # Fetch analysis window (display + warmup) so multi-timeframe indicators are ready from the first displayed bar.
        if str(data_settings.data_source).lower() == "synthetic":
            count = int((data_settings.range_params or {}).get("count", 300))
            candles = generate_dummy_candles(n=count)
            label = f"Synthetic – {len(candles)} candles"
            storage_symbol = "SYNTH"
            storage_timeframe = "synthetic"
        else:
            params = data_settings.range_params or {}
            exchange_id = data_settings.exchange_id or "binance"
            symbol = data_settings.symbol or "BTC/USDT"
            timeframe = data_settings.timeframe or "1h"
            market_type = (data_settings.market_type or "unknown")

            try:
                _analysis_start, _display_start, _display_end, candles, _candles_display = get_candles_analysis_window(
                    data_settings=data_settings,
                    analysis_info=analysis_info,
                    fetch_fn=get_candles_with_cache,
                )
            except Exception as exc:
                msg = str(exc).lower()
                if "database is locked" in msg or "locked" in msg:
                    _analysis_start, _display_start, _display_end, candles, _candles_display = get_candles_analysis_window(
                        data_settings=data_settings,
                        analysis_info=analysis_info,
                        fetch_fn=load_ccxt_candles,
                    )
                else:
                    raise

            label = f"Crypto (CCXT) – {exchange_id} {symbol} {timeframe}, {len(candles)} candles"
            storage_symbol = symbol
            storage_timeframe = timeframe

    warmup_candles: List[Candle] = []
    candles_main: List[Candle] = []
    try:
        display_start_ms = analysis_info.get("display_start_ms")
        display_end_ms = analysis_info.get("display_end_ms")
        display_limit = analysis_info.get("display_limit")
        if display_start_ms is not None:
            for c in candles:
                ts = _normalize_timestamp(getattr(c, "timestamp", None))
                if ts is None:
                    continue
                ts_ms = int(ts.timestamp() * 1000)
                if ts_ms < int(display_start_ms):
                    warmup_candles.append(c)
                else:
                    if display_end_ms is None or ts_ms <= int(display_end_ms):
                        candles_main.append(c)
        elif display_limit is not None and display_limit > 0:
            candles_main = list(candles)[-int(display_limit):]
            warmup_candles = list(candles)[: max(0, len(candles) - len(candles_main))]
        if not candles_main:
            candles_main = list(candles)
    except Exception:
        warmup_candles = []
        candles_main = list(candles)

    engine = BacktestEngine(BacktestConfig(initial_balance=data_settings.initial_balance, fee_rate=data_settings.fee_rate))
    strategy = DragonDcaAtrStrategy(config=cfg)
    if warmup_candles:
        try:
            strategy.prime_history(warmup_candles)
        except Exception:
            pass

    result = engine.run(candles_main, strategy)
    if not getattr(result, "candles", None):
        result.candles = candles_main

    # Persist analysis vs display window info for chart overlays.
    try:
        result.run_context = {
            **(result.run_context or {}),
            "display_start_ms": analysis_info.get("display_start_ms"),
            "display_end_ms": analysis_info.get("display_end_ms"),
            "display_limit": analysis_info.get("display_limit"),
            "analysis_start_ms": analysis_info.get("analysis_start_ms"),
            "analysis_end_ms": analysis_info.get("analysis_end_ms"),
            "analysis_limit": analysis_info.get("analysis_limit"),
            "warmup_seconds": analysis_info.get("warmup_seconds"),
            "warmup_bars": analysis_info.get("warmup_bars"),
            "base_timeframe_sec": analysis_info.get("base_timeframe_sec"),
            "analysis_range_mode": analysis_info.get("effective_range_mode"),
            "data_settings": {
                "data_source": getattr(data_settings, "data_source", None),
                "exchange_id": getattr(data_settings, "exchange_id", None),
                "market_type": getattr(data_settings, "market_type", None),
                "symbol": getattr(data_settings, "symbol", None),
                "timeframe": getattr(data_settings, "timeframe", None),
                "range_mode": getattr(data_settings, "range_mode", None),
                "range_params": dict(getattr(data_settings, "range_params", None) or {}),
            },
        }
    except Exception:
        pass


    # Store config snapshot; preserve additional keys (e.g. _futures) if present.
    cfg_for_storage: Any = cfg
    try:
        cfg_for_storage = asdict(cfg)
    except Exception:
        cfg_for_storage = cfg
    if isinstance(cfg_for_storage, dict) and isinstance(config_snapshot, dict):
        # Merge in opaque extras like _futures.
        for k, v in config_snapshot.items():
            if k.startswith("_") and k not in cfg_for_storage:
                cfg_for_storage[k] = v

    return cfg_for_storage, result, label, storage_symbol, storage_timeframe
