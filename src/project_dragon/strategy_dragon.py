from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .broker_sim import BrokerSim
from .config_dragon import DragonAlgoConfig, StopLossMode, TakeProfitMode
from .domain import Candle, OrderStatus, PositionSide, Side
from .indicators import bollinger_bands, macd, moving_average, rsi


@dataclass
class IndicatorContext:
    trend_ma: float
    trend_long: bool
    trend_short: bool
    bb_mid: float
    bb_upper: float
    bb_lower: float
    bb_long: bool
    bb_short: bool
    macd_line: float
    macd_signal: float
    macd_hist: float
    macd_long: bool
    macd_short: bool
    rsi: float
    rsi_long: bool
    rsi_short: bool
    consensus_long: bool
    consensus_short: bool


@dataclass
class DragonDcaAtrState:
    # Mirrors Haas persistent state (we'll add more fields as we port)
    dca_level: int = 0
    entry_count: int = 0
    base_price: float = 0.0
    base_qty: float = 0.0

    pending_entry_order_id: Optional[int] = None
    pending_dca_order_id: Optional[int] = None
    tp_order_id: Optional[int] = None

    tp_init_dist: float = 0.0
    dca_recalc_tp: bool = False
    saved_dev_base: float = 0.0

    active_tp_price: Optional[float] = None
    active_sl_price: Optional[float] = None
    current_tp_price: Optional[float] = None
    current_sl_price: Optional[float] = None

    trailing_active: bool = False
    trailing_peak: float = 0.0
    trailing_trough: float = 0.0

    last_entry_index: Optional[int] = None  # for cooldown in bars

    price_history: List[Candle] = field(default_factory=list)
    base_interval_min: Optional[float] = None
    last_indicator_ctx: Optional[IndicatorContext] = None

    next_dca_price: Optional[float] = None

    atr_last_index: Optional[int] = None
    atr_last_value: Optional[float] = None


@dataclass
class DragonDcaAtrStrategy:
    config: DragonAlgoConfig
    state: DragonDcaAtrState = field(default_factory=DragonDcaAtrState)

    debug_avg_entry_prices: List[Optional[float]] = field(default_factory=list)
    debug_next_dca_prices: List[Optional[float]] = field(default_factory=list)
    debug_tp_levels: List[Optional[float]] = field(default_factory=list)
    debug_sl_levels: List[Optional[float]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.debug_tp_levels = []
        self.debug_sl_levels = []

    def prime_history(self, candles: List[Candle]) -> None:
        """Preload candle history so indicators are stable from the first traded/visible bar.

        This intentionally only updates internal price history/base interval tracking.
        It must not place orders or mutate position/trade-related state.
        """

        if not candles:
            return
        for c in candles:
            self._update_price_history(c)

    def on_bar(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        Main per-bar logic (v0):
          - if flat: simple initial long entry
          - if in position: simple DCA based on base deviation & multiplier
        """
        pos_side = broker.position.side

        self._update_price_history(candle)
        indicator_ctx = self._compute_indicator_context(candle)
        if indicator_ctx:
            self.state.last_indicator_ctx = indicator_ctx

        self._refresh_pending_orders(i, broker)

        if pos_side == PositionSide.FLAT:
            self._handle_flat_state(i, candle, broker, indicator_ctx)
        else:
            self._handle_in_position(i, candle, broker, indicator_ctx)

        in_pos = broker.position.side in (PositionSide.LONG, PositionSide.SHORT) and broker.position.size > 0
        if in_pos:
            avg_entry = broker.position.avg_price
            next_dca = self.state.next_dca_price or self._infer_next_dca_price(broker)
            self.state.next_dca_price = next_dca
        else:
            avg_entry = None
            next_dca = None
            self.state.next_dca_price = None
            self.state.current_tp_price = None
            self.state.current_sl_price = None

        self.debug_avg_entry_prices.append(avg_entry)
        self.debug_next_dca_prices.append(next_dca)
        self.debug_tp_levels.append(self.state.current_tp_price if in_pos else None)
        self.debug_sl_levels.append(self.state.current_sl_price if in_pos else None)

    def on_finalize(self, broker: BrokerSim) -> None:
        # For now, nothing special here.
        pass

    # --- Internal helpers ------------------------------------------------

    def _cooldown_passed(self, i: int) -> bool:
        """
        Simple bar-based cooldown: assumes 1 bar ≈ 1 minute in the example.
        For real data we may align this with actual timestamps.
        """
        cd_min = self.config.general.entry_cooldown_min
        if cd_min <= 0:
            return True
        if self.state.last_entry_index is None:
            return True
        # In the dummy example, 1 bar = 1 minute.
        return (i - self.state.last_entry_index) >= cd_min

    def _update_price_history(self, candle: Candle) -> None:
        history = self.state.price_history
        history.append(candle)
        if len(history) >= 2 and self.state.base_interval_min is None:
            delta_min = (history[-1].timestamp - history[-2].timestamp).total_seconds() / 60.0
            if delta_min > 0:
                self.state.base_interval_min = delta_min
        limit = self._history_limit()
        if len(history) > limit:
            del history[: len(history) - limit]

    def _history_limit(self) -> int:
        max_req = max(
            self._bars_needed(self.config.trend.ma_interval_min, self.config.trend.ma_len),
            self._bars_needed(self.config.bbands.interval_min, self.config.bbands.length),
            self._bars_needed(self.config.macd.interval_min, self.config.macd.slow + self.config.macd.signal),
            self._bars_needed(self.config.rsi.interval_min, self.config.rsi.length + 1),
        )
        return max(int(max_req * 2), 500)

    def _bars_needed(self, interval_min: int, length: int) -> int:
        base = self.state.base_interval_min or 1.0
        step = max(1, int(round(max(interval_min, 1) / base)))
        return step * max(length, 1)

    def _resample_closes(self, interval_min: int) -> List[float]:
        if not self.state.price_history:
            return []
        closes = [c.close for c in self.state.price_history]
        base = self.state.base_interval_min or 1.0
        step = max(1, int(round(max(interval_min, 1) / base)))
        if step <= 1:
            return closes
        resampled: List[float] = []
        for idx in range(step - 1, len(closes), step):
            resampled.append(closes[idx])
        last_close = closes[-1]
        if not resampled or resampled[-1] != last_close:
            resampled.append(last_close)
        return resampled

    def _resample_candles(self, interval_min: int) -> List[Candle]:
        history = self.state.price_history
        if not history:
            return []
        base = self.state.base_interval_min or 1.0
        step = max(1, int(round(max(interval_min, 1) / base)))
        if step <= 1:
            return history[:]
        resampled: List[Candle] = []
        for start in range(0, len(history), step):
            chunk = history[start : start + step]
            if not chunk:
                continue
            resampled.append(
                Candle(
                    timestamp=chunk[-1].timestamp,
                    open=chunk[0].open,
                    high=max(c.high for c in chunk),
                    low=min(c.low for c in chunk),
                    close=chunk[-1].close,
                    volume=sum(c.volume for c in chunk),
                )
            )
        if resampled and resampled[-1].timestamp != history[-1].timestamp:
            resampled.append(history[-1])
        return resampled

    def _compute_indicator_context(self, candle: Candle) -> Optional[IndicatorContext]:
        trend_series = self._resample_closes(self.config.trend.ma_interval_min)
        if len(trend_series) < self.config.trend.ma_len:
            return None
        trend_ma = moving_average(trend_series, self.config.trend.ma_len, self.config.trend.ma_type)
        if trend_ma is None:
            return None
        trend_long = candle.close > trend_ma
        trend_short = candle.close < trend_ma

        bb_series = self._resample_closes(self.config.bbands.interval_min)
        bb_result = bollinger_bands(
            bb_series,
            self.config.bbands.length,
            self.config.bbands.dev_up,
            self.config.bbands.dev_down,
            self.config.bbands.ma_type,
        )
        if bb_result is None:
            return None
        bb_mid, bb_upper, bb_lower = bb_result
        bb_long = candle.close <= bb_lower
        bb_short = candle.close >= bb_upper

        macd_series = self._resample_closes(self.config.macd.interval_min)
        macd_result = macd(macd_series, self.config.macd.fast, self.config.macd.slow, self.config.macd.signal)
        if macd_result is None:
            return None
        macd_line, macd_signal_line, macd_hist = macd_result
        macd_long = macd_hist > 0
        macd_short = macd_hist < 0

        rsi_series = self._resample_closes(self.config.rsi.interval_min)
        rsi_value = rsi(rsi_series, self.config.rsi.length)
        if rsi_value is None:
            return None
        rsi_long = rsi_value <= self.config.rsi.buy_level
        rsi_short = rsi_value >= self.config.rsi.sell_level

        bullish_votes = sum(int(flag) for flag in (bb_long, macd_long, rsi_long))
        bearish_votes = sum(int(flag) for flag in (bb_short, macd_short, rsi_short))

        return IndicatorContext(
            trend_ma=trend_ma,
            trend_long=trend_long,
            trend_short=trend_short,
            bb_mid=bb_mid,
            bb_upper=bb_upper,
            bb_lower=bb_lower,
            bb_long=bb_long,
            bb_short=bb_short,
            macd_line=macd_line,
            macd_signal=macd_signal_line,
            macd_hist=macd_hist,
            macd_long=macd_long,
            macd_short=macd_short,
            rsi=rsi_value,
            rsi_long=rsi_long,
            rsi_short=rsi_short,
            consensus_long=bullish_votes >= 2,
            consensus_short=bearish_votes >= 2,
        )

    def _handle_flat_state(
        self,
        i: int,
        candle: Candle,
        broker: BrokerSim,
        indicator_ctx: Optional[IndicatorContext] = None,
    ) -> None:
        g = self.config.general

        if not g.allow_long and not g.allow_short:
            return

        if not self._cooldown_passed(i):
            return

        if broker.balance <= 0:
            return

        indicator_ctx = indicator_ctx or self._compute_indicator_context(candle)
        if indicator_ctx is None:
            return
        self.state.last_indicator_ctx = indicator_ctx

        wants_long = bool(g.allow_long) and bool(indicator_ctx.trend_long)
        wants_short = bool(g.allow_short) and bool(indicator_ctx.trend_short)

        if self.config.general.use_indicator_consensus:
            long_signal = bool(indicator_ctx.consensus_long)
            short_signal = bool(indicator_ctx.consensus_short)
        else:
            long_signal = bool(indicator_ctx.bb_long)
            short_signal = bool(indicator_ctx.bb_short)

        if wants_long and long_signal:
            if self._passes_ma_direction_gate(side=Side.LONG, candle=candle):
                self._update_initial_entry(i, candle, broker, side=Side.LONG)
            return
        if wants_short and short_signal:
            if self._passes_ma_direction_gate(side=Side.SHORT, candle=candle):
                self._update_initial_entry(i, candle, broker, side=Side.SHORT)
            return

    def _passes_ma_direction_gate(self, *, side: Side, candle: Candle) -> bool:
        """Optional entry-only MA gate.

        When enabled, entries are allowed only if price is on the correct
        side of a configured MA. This gate applies ONLY to new entries.
        """

        g = self.config.general
        if not bool(getattr(g, "use_ma_direction", False)):
            return True

        ma_val = self._compute_entry_gate_ma()
        if ma_val is None:
            return False

        if side == Side.LONG:
            return candle.close > ma_val
        return candle.close < ma_val

    def _compute_entry_gate_ma(self) -> Optional[float]:
        g = self.config.general
        try:
            length = int(getattr(g, "ma_length", 0) or 0)
        except (TypeError, ValueError):
            length = 0
        if length <= 0:
            return None

        src = str(getattr(g, "ma_source", "close") or "close").strip().lower()
        series: List[float] = []
        for c in self.state.price_history:
            if src == "open":
                series.append(float(c.open))
            elif src == "high":
                series.append(float(c.high))
            elif src == "low":
                series.append(float(c.low))
            elif src == "hl2":
                series.append(float((c.high + c.low) / 2.0))
            elif src == "hlc3":
                series.append(float((c.high + c.low + c.close) / 3.0))
            elif src == "ohlc4":
                series.append(float((c.open + c.high + c.low + c.close) / 4.0))
            else:
                series.append(float(c.close))

        if len(series) < length:
            return None

        ma_type = str(getattr(g, "ma_type", "Ema") or "Ema")
        return moving_average(series, length, ma_type)

    def _handle_in_position(
        self,
        i: int,
        candle: Candle,
        broker: BrokerSim,
        indicator_ctx: Optional[IndicatorContext] = None,
    ) -> None:
        if broker.position.side not in (PositionSide.LONG, PositionSide.SHORT) or broker.position.size <= 0:
            return

        self._manage_dca(i, candle, broker)
        if broker.position.side in (PositionSide.LONG, PositionSide.SHORT) and broker.position.size > 0:
            self._update_exits(i, candle, broker)
        if broker.position.side in (PositionSide.LONG, PositionSide.SHORT) and broker.position.size > 0:
            self._update_trailing_exit(i, candle, broker)

    def _refresh_pending_orders(self, i: int, broker: BrokerSim) -> None:
        if self.state.pending_entry_order_id is not None:
            order = broker.get_order(self.state.pending_entry_order_id)
            if not order or order.status == OrderStatus.CANCELLED:
                self.state.pending_entry_order_id = None
            elif order.status == OrderStatus.FILLED:
                self.state.pending_entry_order_id = None
                self._finalize_initial_entry(i, broker)

        if self.state.pending_dca_order_id is not None:
            order = broker.get_order(self.state.pending_dca_order_id)
            if not order or order.status == OrderStatus.CANCELLED:
                self.state.pending_dca_order_id = None
            elif order.status == OrderStatus.FILLED:
                self.state.pending_dca_order_id = None
                self._finalize_dca_fill(i, broker)

        if self.state.tp_order_id is not None:
            order = broker.get_order(self.state.tp_order_id)
            if not order or order.status == OrderStatus.CANCELLED:
                self.state.tp_order_id = None
            elif order.status == OrderStatus.FILLED:
                self.state.tp_order_id = None
                self._reset_state_to_flat(broker)

    def _finalize_initial_entry(self, i: int, broker: BrokerSim) -> None:
        self.state.entry_count = 1
        self.state.dca_level = 0
        self.state.base_price = broker.position.avg_price
        self.state.base_qty = broker.position.size
        self.state.last_entry_index = i
        self.state.dca_recalc_tp = True
        self.state.tp_init_dist = 0.0
        self.state.trailing_active = False
        self.state.trailing_peak = 0.0
        self.state.trailing_trough = 0.0

    def _finalize_dca_fill(self, i: int, broker: BrokerSim) -> None:
        self.state.dca_level += 1
        self.state.entry_count += 1
        self.state.base_price = broker.position.avg_price
        # IMPORTANT: keep `base_qty` as the *initial entry order qty*.
        # DCA sizing is derived from this base, not from total position size.
        self.state.last_entry_index = i
        self.state.dca_recalc_tp = True
        self.state.active_tp_price = None
        self.state.trailing_active = False
        self.state.trailing_peak = 0.0
        self.state.trailing_trough = 0.0

    def _update_initial_entry(self, i: int, candle: Candle, broker: BrokerSim, *, side: Side) -> None:
        if self.state.entry_count > 0:
            return

        mode_raw = getattr(self.config.general, "initial_entry_sizing_mode", "pct_balance")
        mode_val = getattr(mode_raw, "value", mode_raw)
        mode = str(mode_val or "pct_balance").strip().lower()

        notional = 0.0
        if mode in {"fixed_usd", "fixed", "usd", "$"} or mode.endswith("fixed_usd"):
            try:
                notional = float(getattr(self.config.general, "initial_entry_fixed_usd", 0.0) or 0.0)
            except (TypeError, ValueError):
                notional = 0.0

            # Guard against nonsensical inputs.
            if notional < 0:
                notional = 0.0

            # If the broker provides a balance and it's positive (backtest, some live setups),
            # cap fixed sizing to available balance to avoid accidental over-sizing.
            try:
                bal = float(getattr(broker, "balance", 0.0) or 0.0)
            except (TypeError, ValueError):
                bal = 0.0
            if bal > 0:
                notional = min(notional, bal)
        else:
            pct = 10.0
            try:
                pct = float(getattr(self.config.general, "initial_entry_balance_pct", 10.0) or 0.0)
            except (TypeError, ValueError):
                pct = 10.0
            # Guard against nonsensical inputs.
            if pct < 0:
                pct = 0.0
            if pct > 100.0:
                pct = 100.0

            notional = float(getattr(broker, "balance", 0.0) or 0.0) * (pct / 100.0)
        if candle.close <= 0 or notional <= 0:
            return

        size = notional / candle.close
        if size <= 0:
            return

        entry_note = "Initial-Long" if side == Side.LONG else "Initial-Short"

        activation_pct = self.config.general.dynamic_activation.entry_pct
        if activation_pct > 0:
            if self.state.pending_entry_order_id is None:
                oid = broker.place_dynamic_limit(
                    side=side,
                    price=candle.close,
                    size=size,
                    activation_pct=activation_pct,
                    note=entry_note,
                )
                self.state.pending_entry_order_id = oid
            return

        trade = broker.place_market(
            side=side,
            price=candle.close,
            size=size,
            note=entry_note,
        )

        self.state.entry_count = 1
        self.state.dca_level = 0
        self.state.base_price = broker.position.avg_price or trade.price
        self.state.base_qty = broker.position.size or trade.size
        self.state.last_entry_index = i
        self.state.pending_entry_order_id = None
        self.state.pending_dca_order_id = None
        self.state.dca_recalc_tp = True
        self.state.tp_init_dist = 0.0
        self.state.active_tp_price = None
        self.state.active_sl_price = None
        self.state.trailing_active = False
        self.state.trailing_peak = 0.0
        self.state.trailing_trough = 0.0

    def _manage_dca(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        g = self.config.general
        dca_cfg = self.config.dca

        if self.state.entry_count == 0:
            self.state.next_dca_price = None
            return

        if self.state.entry_count >= g.max_entries:
            self.state.next_dca_price = None
            return

        base_price = self._determine_dca_base_price(broker)
        if base_price <= 0:
            self.state.next_dca_price = None
            return

        level = self.state.dca_level
        dev_pct = dca_cfg.base_deviation_pct * (dca_cfg.deviation_multiplier ** level)
        dev_frac = dev_pct / 100.0
        if broker.position.side == PositionSide.LONG:
            target_price = base_price * (1.0 - dev_frac)
        else:
            target_price = base_price * (1.0 + dev_frac)
        self.state.next_dca_price = target_price

        if broker.position.side == PositionSide.LONG:
            if candle.low > target_price:
                return
        else:
            if candle.high < target_price:
                return

        dca_activation = g.dynamic_activation.dca_pct
        if dca_activation > 0:
            if self.state.pending_dca_order_id is not None:
                return
            if self.state.base_qty <= 0:
                # Should be set on initial entry finalize, but guard anyway.
                self.state.base_qty = broker.position.size
            # DCA-L1 matches initial entry size; subsequent rungs multiply by volume_multiplier.
            dca_size = self.state.base_qty * (dca_cfg.volume_multiplier ** level)
            if dca_size <= 0:
                return
            oid = broker.place_dynamic_limit(
                side=Side.LONG if broker.position.side == PositionSide.LONG else Side.SHORT,
                price=target_price,
                size=dca_size,
                activation_pct=dca_activation,
                note=(f"DCA-L{level + 1}" if broker.position.side == PositionSide.LONG else f"DCA-S{level + 1}"),
            )
            self.state.pending_dca_order_id = oid
            return

        dca_size = self._calculate_dca_size(broker, level)
        if dca_size <= 0:
            return

        # Conservative: if the candle traded through the target, assume a fill at the target.
        fill_price = target_price
        broker.place_market(
            side=Side.LONG if broker.position.side == PositionSide.LONG else Side.SHORT,
            price=fill_price,
            size=dca_size,
            note=(f"DCA-L{level + 1}" if broker.position.side == PositionSide.LONG else f"DCA-S{level + 1}"),
        )

        self.state.dca_level += 1
        self.state.entry_count += 1
        self.state.last_entry_index = i
        self.state.pending_dca_order_id = None
        self.state.dca_recalc_tp = True
        self.state.active_tp_price = None
        self.state.trailing_active = False
        self.state.trailing_peak = 0.0
        self.state.trailing_trough = 0.0

        if g.use_avg_entry_for_dca_base:
            self.state.base_price = broker.position.avg_price
        self.state.active_tp_price = None
        self.state.next_dca_price = None

    def _determine_dca_base_price(self, broker: BrokerSim) -> float:
        if self.config.general.use_avg_entry_for_dca_base:
            return broker.position.avg_price

        if self.state.base_price <= 0:
            self.state.base_price = broker.position.avg_price
        return self.state.base_price or 0.0

    def _calculate_dca_size(self, broker: BrokerSim, level_index: int) -> float:
        if self.state.base_qty <= 0:
            self.state.base_qty = broker.position.size
        base_qty = self.state.base_qty or 0.0
        # DCA-L1 matches initial entry size; subsequent rungs multiply by volume_multiplier.
        return base_qty * (self.config.dca.volume_multiplier ** level_index)

    def _update_exits(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        if broker.position.side not in (PositionSide.LONG, PositionSide.SHORT) or broker.position.size <= 0:
            return

        exits = self.config.exits
        aep = broker.position.avg_price or 0.0
        if aep <= 0:
            return

        atr_val: Optional[float] = None
        if self.config.exits.tp_mode == TakeProfitMode.ATR or self.config.exits.sl_mode == StopLossMode.ATR:
            atr_val = self._atr_value(i)

        tp_target: Optional[float] = None
        tp_note = "TP_PCT"
        tp_pct = exits.tp_pct / 100.0 if exits.tp_pct > 0 else 0.0

        if exits.tp_mode == TakeProfitMode.ATR and exits.tp_atr_mult > 0:
            if self.state.tp_init_dist <= 0:
                if atr_val is not None and atr_val > 0:
                    self.state.tp_init_dist = atr_val * exits.tp_atr_mult
            if self.state.tp_init_dist > 0:
                if broker.position.side == PositionSide.LONG:
                    tp_target = aep + self.state.tp_init_dist
                else:
                    tp_target = aep - self.state.tp_init_dist
                tp_note = "TP_ATR"
        elif tp_pct > 0:
            if broker.position.side == PositionSide.LONG:
                tp_target = aep * (1 + tp_pct)
            else:
                tp_target = aep * (1 - tp_pct)
            self.state.tp_init_dist = 0.0
        else:
            self.state.tp_init_dist = 0.0

        sl_target: Optional[float] = None
        sl_note = "SL_PCT"
        if exits.sl_mode == StopLossMode.ATR:
            if atr_val is not None and atr_val > 0 and exits.sl_atr_mult > 0:
                dist = atr_val * float(exits.sl_atr_mult)
                if broker.position.side == PositionSide.LONG:
                    sl_target = aep - dist
                else:
                    sl_target = aep + dist
                sl_note = "SL_ATR"
        else:
            sl_pct = exits.sl_pct / 100.0 if exits.sl_pct > 0 else 0.0
            if sl_pct > 0:
                if broker.position.side == PositionSide.LONG:
                    sl_target = aep * (1 - sl_pct)
                else:
                    sl_target = aep * (1 + sl_pct)

        if tp_target is not None and self.state.dca_recalc_tp:
            tp_target = max(tp_target, 0.0)  # re-anchor on new AEP
            self.state.dca_recalc_tp = False

        self.state.active_tp_price = tp_target
        self.state.active_sl_price = sl_target
        self.state.current_tp_price = tp_target
        self.state.current_sl_price = sl_target

        exit_price: Optional[float] = None
        exit_note: Optional[str] = None

        if broker.position.side == PositionSide.LONG:
            if sl_target is not None and candle.low <= sl_target:
                exit_price = sl_target
                exit_note = sl_note
            elif tp_target is not None and candle.high >= tp_target:
                exit_price = tp_target
                exit_note = tp_note
        else:
            if sl_target is not None and candle.high >= sl_target:
                exit_price = sl_target
                exit_note = sl_note
            elif tp_target is not None and candle.low <= tp_target:
                exit_price = tp_target
                exit_note = tp_note

        if exit_price is not None and exit_note is not None:
            self._close_position(broker, exit_price, exit_note)
            self._reset_state_to_flat(broker)

        tp_activation = self.config.general.dynamic_activation.tp_pct
        if tp_activation > 0 and tp_target is not None and broker.position.size > 0:
            self._ensure_dynamic_tp(broker, tp_target, tp_activation)
            return
        if broker.position.side not in (PositionSide.LONG, PositionSide.SHORT) or broker.position.size <= 0:
            self.state.current_tp_price = None
            self.state.current_sl_price = None

    def _atr_value(self, i: int) -> Optional[float]:
        if self.state.atr_last_index == i:
            return self.state.atr_last_value

        exits = self.config.exits
        try:
            period = int(getattr(exits, "atr_period", 0) or 0)
        except (TypeError, ValueError):
            period = 0
        if period <= 0:
            self.state.atr_last_index = i
            self.state.atr_last_value = None
            return None

        atr_val = self._compute_atr(interval_min=1, period=period)
        self.state.atr_last_index = i
        self.state.atr_last_value = atr_val
        return atr_val

    def _compute_atr(self, interval_min: int, period: int) -> Optional[float]:
        if period <= 0:
            return None
        candles = self._resample_candles(interval_min)
        if len(candles) < period + 1:
            return None
        relevant = candles[-(period + 1) :]
        prev_close = relevant[0].close
        trs: List[float] = []
        for candle in relevant[1:]:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - prev_close),
                abs(candle.low - prev_close),
            )
            trs.append(tr)
            prev_close = candle.close
        if not trs:
            return None
        return sum(trs) / len(trs)

    def _close_position(self, broker: BrokerSim, price: float, note: str) -> None:
        self._cancel_pending_orders(broker)
        size = broker.position.size
        if size <= 0:
            return
        if broker.position.side == PositionSide.LONG:
            broker.place_market(side=Side.SHORT, price=price, size=size, note=note)
        elif broker.position.side == PositionSide.SHORT:
            broker.place_market(side=Side.LONG, price=price, size=size, note=note)

    def _reset_state_to_flat(self, broker: BrokerSim) -> None:
        self._cancel_pending_orders(broker)
        self.state.entry_count = 0
        self.state.dca_level = 0
        self.state.base_price = 0.0
        self.state.base_qty = 0.0
        self.state.pending_entry_order_id = None
        self.state.pending_dca_order_id = None
        self.state.tp_order_id = None
        self.state.tp_init_dist = 0.0
        self.state.dca_recalc_tp = False
        self.state.saved_dev_base = 0.0
        self.state.active_tp_price = None
        self.state.active_sl_price = None
        self.state.current_tp_price = None
        self.state.current_sl_price = None
        self.state.trailing_active = False
        self.state.trailing_peak = 0.0
        self.state.trailing_trough = 0.0
        self.state.last_entry_index = None
        self.state.next_dca_price = None

    def _cancel_pending_orders(self, broker: BrokerSim) -> None:
        for attr in ("pending_entry_order_id", "pending_dca_order_id", "tp_order_id"):
            oid = getattr(self.state, attr)
            if oid:
                broker.cancel_order(oid)
                setattr(self.state, attr, None)

    def _ensure_dynamic_tp(self, broker: BrokerSim, price: float, activation_pct: float) -> None:
        existing_id = self.state.tp_order_id
        replace = False
        if existing_id is not None:
            order = broker.get_order(existing_id)
            if not order or order.status != OrderStatus.OPEN:
                self.state.tp_order_id = None
            else:
                current_target = order.target_price or order.price or price
                rel_delta = abs(current_target - price) / price if price > 0 else 0.0
                if self.state.dca_recalc_tp or rel_delta >= (self.config.exits.tp_replace_threshold_pct / 100.0):
                    broker.cancel_order(existing_id)
                    self.state.tp_order_id = None
                else:
                    return
        size = broker.position.size
        if size <= 0:
            return
        if broker.position.side == PositionSide.LONG:
            exit_side = Side.SHORT
        elif broker.position.side == PositionSide.SHORT:
            exit_side = Side.LONG
        else:
            return
        oid = broker.place_dynamic_limit(
            side=exit_side,
            price=price,
            size=size,
            activation_pct=activation_pct,
            note="TP_DYNAMIC",
        )
        self.state.tp_order_id = oid
        self.state.active_tp_price = price
        self.state.dca_recalc_tp = False

    def _update_trailing_exit(self, index: int, candle: Candle, broker: BrokerSim) -> None:
        exits = self.config.exits
        sl_mode = exits.sl_mode

        position = broker.position
        if position.side not in (PositionSide.LONG, PositionSide.SHORT) or position.size <= 0:
            return

        avg_entry = position.avg_price or 0.0
        if avg_entry <= 0:
            return

        current_close = candle.close
        current_high = candle.high
        current_low = candle.low

        if sl_mode == StopLossMode.ATR:
            atr_val = self._atr_value(index)
            if atr_val is None or atr_val <= 0:
                return
            if exits.trail_distance_atr_mult <= 0:
                return

            activation_dist = atr_val * float(exits.trail_activation_atr_mult or 0.0)
            stop_dist = atr_val * float(exits.trail_distance_atr_mult)

            if position.side == PositionSide.LONG:
                if not self.state.trailing_active:
                    if activation_dist <= 0 or (current_close - avg_entry) >= activation_dist:
                        self.state.trailing_active = True
                        self.state.trailing_peak = max(current_high, current_close)
                    return
                self.state.trailing_peak = max(self.state.trailing_peak, current_high)
                if self.state.trailing_peak <= 0:
                    return
                stop_price = self.state.trailing_peak - stop_dist
                self.state.current_sl_price = stop_price
                if current_low <= stop_price:
                    exit_price = max(stop_price, current_close)
                    self._close_position(broker, exit_price, note="TRAILING_EXIT")
                    self._reset_state_to_flat(broker)
                return

            # SHORT trailing (ATR)
            if not self.state.trailing_active:
                if activation_dist <= 0 or (avg_entry - current_close) >= activation_dist:
                    self.state.trailing_active = True
                    self.state.trailing_trough = min(current_low, current_close)
                return
            if self.state.trailing_trough <= 0:
                self.state.trailing_trough = min(current_low, current_close)
            self.state.trailing_trough = min(self.state.trailing_trough, current_low)
            stop_price = self.state.trailing_trough + stop_dist
            self.state.current_sl_price = stop_price
            if current_high >= stop_price:
                exit_price = min(stop_price, current_close)
                self._close_position(broker, exit_price, note="TRAILING_EXIT")
                self._reset_state_to_flat(broker)
            return

        # Percent trailing (legacy behavior)
        activation_pct = exits.trail_activation_pct / 100.0
        stop_pct = exits.trail_distance_pct / 100.0

        if stop_pct <= 0:
            return

        if position.side == PositionSide.LONG:
            unrealized_return = (current_close - avg_entry) / avg_entry if avg_entry > 0 else 0.0
            if not self.state.trailing_active:
                if activation_pct <= 0 or unrealized_return >= activation_pct:
                    self.state.trailing_active = True
                    self.state.trailing_peak = max(current_high, current_close)
                return
            self.state.trailing_peak = max(self.state.trailing_peak, current_high)
            if self.state.trailing_peak <= 0:
                return
            stop_price = self.state.trailing_peak * (1 - stop_pct)
            self.state.current_sl_price = stop_price
            if current_low <= stop_price:
                exit_price = max(stop_price, current_close)
                self._close_position(broker, exit_price, note="TRAILING_EXIT")
                self._reset_state_to_flat(broker)
        else:
            # SHORT trailing: track trough, stop above by stop_pct
            unrealized_return = (avg_entry - current_close) / avg_entry if avg_entry > 0 else 0.0
            if not self.state.trailing_active:
                if activation_pct <= 0 or unrealized_return >= activation_pct:
                    self.state.trailing_active = True
                    self.state.trailing_trough = min(current_low, current_close)
                return
            if self.state.trailing_trough <= 0:
                self.state.trailing_trough = min(current_low, current_close)
            self.state.trailing_trough = min(self.state.trailing_trough, current_low)
            stop_price = self.state.trailing_trough * (1 + stop_pct)
            self.state.current_sl_price = stop_price
            if current_high >= stop_price:
                exit_price = min(stop_price, current_close)
                self._close_position(broker, exit_price, note="TRAILING_EXIT")
                self._reset_state_to_flat(broker)

    def _schedule_next_dca(self, price: float) -> None:
        level = self.state.dca_level
        dca_cfg = self.config.dca

        dev_pct = dca_cfg.base_deviation_pct * (dca_cfg.deviation_multiplier ** level)
        dev_frac = dev_pct / 100.0
        target_price = price * (1.0 - dev_frac)

        self.state.next_dca_price = price

        dca_activation = self.config.general.dynamic_activation.dca_pct
        if dca_activation > 0:
            if self.state.pending_dca_order_id is not None:
                return
            if self.state.base_qty <= 0:
                self.state.base_qty = broker.position.size
            dca_size = self.state.base_qty * (dca_cfg.volume_multiplier ** level)
            if dca_size <= 0:
                return
            oid = broker.place_dynamic_limit(
                side=Side.LONG,
                price=target_price,
                size=dca_size,
                activation_pct=dca_activation,
                note=f"DCA-L{level + 1}",
            )
            self.state.pending_dca_order_id = oid
            return

        broker.place_market(
            side=Side.LONG,
            price=target_price,
            size=dca_size,
            note=f"DCA-L{level + 1}",
        )

    def _clear_dca_state(self) -> None:
        self.state.next_dca_price = None

    def _infer_next_dca_price(self, broker: BrokerSim) -> Optional[float]:
        pos_side = broker.position.side
        want_order_side = Side.LONG if pos_side == PositionSide.LONG else Side.SHORT
        for order in broker.orders.values():
            if (
                order.status == OrderStatus.OPEN
                and order.side == want_order_side
                and (order.note or "").lower().startswith("dca")
            ):
                return order.price or order.target_price
        return None
