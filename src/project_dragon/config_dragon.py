from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class OrderStyle(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    MAKER_OR_CANCEL = "maker_or_cancel"


class InitialEntrySizingMode(str, Enum):
    PCT_BALANCE = "pct_balance"
    FIXED_USD = "fixed_usd"


class StopLossMode(str, Enum):
    PCT = "PCT"
    ATR = "ATR"


class TakeProfitMode(str, Enum):
    PCT = "PCT"
    ATR = "ATR"


@dataclass
class DynamicActivationConfig:
    # percentage distance from target at which orders become active
    entry_pct: float = 0.0   # e.g. 0.2 => 0.2% from target
    dca_pct: float = 0.0
    tp_pct: float = 0.0


@dataclass
class GeneralSettings:
    max_entries: int = 10
    # Initial entry sizing as % of broker balance (cash/equity proxy).
    # Backtests: balance starts at backtest initial_balance.
    # Live: balance must be provided via config _risk.balance_override or env DRAGON_LIVE_BALANCE.
    initial_entry_balance_pct: float = 10.0
    # Alternative sizing mode for the initial entry.
    # - pct_balance: use initial_entry_balance_pct
    # - fixed_usd: use initial_entry_fixed_usd
    initial_entry_sizing_mode: InitialEntrySizingMode = InitialEntrySizingMode.PCT_BALANCE
    initial_entry_fixed_usd: float = 0.0
    use_indicator_consensus: bool = True
    entry_order_style: OrderStyle = OrderStyle.MAKER_OR_CANCEL
    entry_timeout_min: int = 10
    entry_cooldown_min: int = 10
    exit_order_style: OrderStyle = OrderStyle.MAKER_OR_CANCEL
    exit_timeout_min: int = 10
    allow_long: bool = True
    allow_short: bool = False
    lock_atr_on_entry_for_dca: bool = False
    use_avg_entry_for_dca_base: bool = True
    plot_dca_levels: bool = False
    dynamic_activation: DynamicActivationConfig = field(default_factory=DynamicActivationConfig)
    prefer_bbo_maker: bool = True
    bbo_queue_level: int = 1

    # Optional entry gate: require price to be on the correct side of an MA.
    # This gate applies only to NEW entries (not to position/order management).
    use_ma_direction: bool = False
    ma_type: str = "Ema"
    ma_length: int = 200
    ma_source: str = "close"


@dataclass
class ExitSettings:
    # Stop Loss mode controls ALL SL-related settings:
    # - initial stop
    # - trailing activation
    # - trailing distance
    sl_mode: StopLossMode = StopLossMode.PCT
    # Percent SL
    sl_pct: float = 9.0
    trail_activation_pct: float = 5.0
    trail_distance_pct: float = 7.0
    # ATR SL
    atr_period: int = 14
    sl_atr_mult: float = 3.0
    trail_activation_atr_mult: float = 1.0
    trail_distance_atr_mult: float = 2.0

    # Take Profit mode (already supported; now explicit)
    tp_mode: TakeProfitMode = TakeProfitMode.ATR
    tp_pct: float = 1.0
    tp_atr_mult: float = 20.0

    tp_replace_threshold_pct: float = 0.05


@dataclass
class DcaSettings:
    base_deviation_pct: float = 0.9
    deviation_multiplier: float = 2.0
    volume_multiplier: float = 2.2


@dataclass
class TrendFilterConfig:
    ma_interval_min: int = 120
    ma_len: int = 250
    ma_type: str = "Sma"  # later: Enum("SMA", "EMA", ...)


@dataclass
class BBandsConfig:
    interval_min: int = 60
    length: int = 12
    dev_up: float = 2.0
    dev_down: float = 2.0
    ma_type: str = "Sma"
    deviation: float = 0.2
    require_fcc: bool = False
    reset_middle: bool = False
    allow_mid_sells: bool = False


@dataclass
class MACDConfig:
    interval_min: int = 360
    fast: int = 12
    slow: int = 26
    signal: int = 7


@dataclass
class RSIConfig:
    interval_min: int = 1440
    length: int = 12
    buy_level: float = 30
    sell_level: float = 70


@dataclass
class DragonAlgoConfig:
    general: GeneralSettings = field(default_factory=GeneralSettings)
    exits: ExitSettings = field(default_factory=ExitSettings)
    dca: DcaSettings = field(default_factory=DcaSettings)
    trend: TrendFilterConfig = field(default_factory=TrendFilterConfig)
    bbands: BBandsConfig = field(default_factory=BBandsConfig)
    macd: MACDConfig = field(default_factory=MACDConfig)
    rsi: RSIConfig = field(default_factory=RSIConfig)


# --- Example config using your Haas JSON block ---------------------------

dragon_config_example = DragonAlgoConfig(
    general=GeneralSettings(
        max_entries=10,
        initial_entry_balance_pct=10.0,
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
        prefer_bbo_maker=True,
        bbo_queue_level=1,
        use_ma_direction=False,
        ma_type="Ema",
        ma_length=200,
        ma_source="close",
    ),
    exits=ExitSettings(
        sl_mode=StopLossMode.PCT,
        sl_pct=9.0,
        trail_activation_pct=5.0,
        trail_distance_pct=7.0,
        atr_period=14,
        sl_atr_mult=3.0,
        trail_activation_atr_mult=1.0,
        trail_distance_atr_mult=2.0,
        tp_mode=TakeProfitMode.ATR,
        tp_pct=1.0,
        tp_atr_mult=20.0,
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
