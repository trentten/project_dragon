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


def load_candles_for_settings(data_settings: DataSettings) -> Tuple[List[Candle], str, str, str]:
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

    try:
        candles = get_candles_with_cache(
            exchange_id=exchange_id,
            symbol=symbol,
            timeframe=timeframe,
            limit=params.get("limit"),
            since=params.get("since"),
            until=params.get("until"),
            range_mode=range_mode,
            market_type=market_type,
        )
    except Exception as exc:
        # Best-effort fallback: skip caching when DB is locked.
        # This covers both read/write contention and environments without a writable cache.
        msg = str(exc).lower()
        if "database is locked" in msg or "locked" in msg:
            candles = load_ccxt_candles(
                exchange_id,
                symbol,
                timeframe,
                limit=params.get("limit"),
                since=params.get("since"),
                until=params.get("until"),
                range_mode=range_mode,
                market_type=market_type,
            )
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
        candles, label, storage_symbol, storage_timeframe = load_candles_for_settings(data_settings)

    warmup_candles: List[Candle] = []
    try:
        if str(data_settings.data_source).lower() != "synthetic" and candles:
            tf = str(data_settings.timeframe or "")
            base_min = timeframe_to_minutes(tf) or 1

            def _bars_needed(interval_min: int, length: int) -> int:
                step = max(1, int(round(max(int(interval_min or 1), 1) / float(base_min))))
                return step * max(int(length or 1), 1)

            max_req = max(
                _bars_needed(cfg.trend.ma_interval_min, int(getattr(cfg.trend, "ma_len", 1) or 1)),
                _bars_needed(cfg.bbands.interval_min, int(getattr(cfg.bbands, "length", 1) or 1)),
                _bars_needed(
                    cfg.macd.interval_min,
                    int(getattr(cfg.macd, "slow", 26) or 26) + int(getattr(cfg.macd, "signal", 9) or 9),
                ),
                _bars_needed(cfg.rsi.interval_min, int(getattr(cfg.rsi, "length", 14) or 14) + 1),
                _bars_needed(cfg.trend.ma_interval_min, int(getattr(cfg.exits, "atr_period", 14) or 14) + 1),
            )
            warmup_bars = max(int(max_req), 0)

            if warmup_bars > 0:
                first_ts = getattr(candles[0], "timestamp", None)
                first_dt = _normalize_timestamp(first_ts)
                if first_dt is not None:
                    first_ms = int(first_dt.timestamp() * 1000)
                    ms_per_bar = max(int(base_min) * 60_000, 60_000)
                    slack_bars = max(10, int(round(float(warmup_bars) * 0.10)))
                    since_ms = max(0, int(first_ms) - int((warmup_bars + slack_bars) * ms_per_bar))
                    until_ms = max(0, int(first_ms) - 1)

                    # Direct fetch (no cache) to avoid compounding cache contention.
                    exchange_id = data_settings.exchange_id or "binance"
                    symbol = data_settings.symbol or "BTC/USDT"
                    market_type = (data_settings.market_type or "unknown")
                    warm_limit = min(max(int(warmup_bars) + int(slack_bars) + 10, 0), 250_000)
                    warmup_candles = load_ccxt_candles(
                        exchange_id,
                        symbol,
                        tf,
                        limit=warm_limit,
                        since=int(since_ms),
                        until=int(until_ms),
                        range_mode="range",
                        market_type=market_type,
                    )
                    if warmup_candles:
                        trimmed = []
                        for c in warmup_candles:
                            try:
                                ts_ms = int(c.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)
                            except Exception:
                                continue
                            if ts_ms < first_ms:
                                trimmed.append(c)
                        warmup_candles = trimmed[-warmup_bars:] if len(trimmed) > warmup_bars else trimmed
    except Exception:
        warmup_candles = []

    engine = BacktestEngine(BacktestConfig(initial_balance=data_settings.initial_balance, fee_rate=data_settings.fee_rate))
    strategy = DragonDcaAtrStrategy(config=cfg)
    if warmup_candles:
        try:
            strategy.prime_history(warmup_candles)
        except Exception:
            pass

    result = engine.run(candles, strategy)
    if not getattr(result, "candles", None):
        result.candles = candles

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
