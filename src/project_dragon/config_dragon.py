from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class OrderStyle(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    MAKER_OR_CANCEL = "maker_or_cancel"


@dataclass
class DynamicActivationConfig:
    # percentage distance from target at which orders become active
    entry_pct: float = 0.0   # e.g. 0.2 => 0.2% from target
    dca_pct: float = 0.0
    tp_pct: float = 0.0


@dataclass
class GeneralSettings:
    max_entries: int
    use_indicator_consensus: bool
    entry_order_style: OrderStyle
    entry_timeout_min: int
    entry_cooldown_min: int
    exit_order_style: OrderStyle
    exit_timeout_min: int
    allow_long: bool
    allow_short: bool
    lock_atr_on_entry_for_dca: bool
    use_avg_entry_for_dca_base: bool
    plot_dca_levels: bool
    dynamic_activation: DynamicActivationConfig


@dataclass
class ExitSettings:
    fixed_sl_pct: float
    fixed_tp_pct: float
    trail_activation_pct: float
    trail_stop_pct: float
    use_atr_tp: bool
    tp_atr_multiple: float
    tp_atr_period: int
    tp_replace_threshold_pct: float


@dataclass
class DcaSettings:
    base_deviation_pct: float
    deviation_multiplier: float
    volume_multiplier: float


@dataclass
class TrendFilterConfig:
    ma_interval_min: int
    ma_len: int
    ma_type: str  # later: Enum("SMA", "EMA", ...)


@dataclass
class BBandsConfig:
    interval_min: int
    length: int
    dev_up: float
    dev_down: float
    ma_type: str
    deviation: float
    require_fcc: bool
    reset_middle: bool
    allow_mid_sells: bool


@dataclass
class MACDConfig:
    interval_min: int
    fast: int
    slow: int
    signal: int


@dataclass
class RSIConfig:
    interval_min: int
    length: int
    buy_level: float
    sell_level: float


@dataclass
class DragonAlgoConfig:
    general: GeneralSettings
    exits: ExitSettings
    dca: DcaSettings
    trend: TrendFilterConfig
    bbands: BBandsConfig
    macd: MACDConfig
    rsi: RSIConfig


# --- Example config using your Haas JSON block ---------------------------

dragon_config_example = DragonAlgoConfig(
    general=GeneralSettings(
        max_entries=10,
        use_indicator_consensus=True,
        entry_order_style=OrderStyle.MAKER_OR_CANCEL,
        entry_timeout_min=10,
        entry_cooldown_min=10,
        exit_order_style=OrderStyle.MAKER_OR_CANCEL,
        exit_timeout_min=10,
        allow_long=True,
        allow_short=False,
        lock_atr_on_entry_for_dca=False,
        use_avg_entry_for_dca_base=True,
        plot_dca_levels=False,
        dynamic_activation=DynamicActivationConfig(
            entry_pct=0.0,  # adjust later
            dca_pct=0.0,
            tp_pct=0.0,
        ),
    ),
    exits=ExitSettings(
        fixed_sl_pct=9.0,
        fixed_tp_pct=1.0,
        trail_activation_pct=5.0,
        trail_stop_pct=7.0,
        use_atr_tp=True,
        tp_atr_multiple=20.0,
        tp_atr_period=14,
        tp_replace_threshold_pct=0.05,
    ),
    dca=DcaSettings(
        base_deviation_pct=0.9,
        deviation_multiplier=2.0,
        volume_multiplier=2.2,
    ),
    trend=TrendFilterConfig(
        ma_interval_min=120,
        ma_len=250,
        ma_type="Sma",
    ),
    bbands=BBandsConfig(
        interval_min=60,
        length=12,
        dev_up=2.0,
        dev_down=2.0,
        ma_type="Sma",
        deviation=0.2,
        require_fcc=False,
        reset_middle=False,
        allow_mid_sells=False,
    ),
    macd=MACDConfig(
        interval_min=360,
        fast=12,
        slow=26,
        signal=7,
    ),
    rsi=RSIConfig(
        interval_min=1440,
        length=12,
        buy_level=30,
        sell_level=70,
    ),
)
