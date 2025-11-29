from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .broker_sim import BrokerSim
from .config_dragon import DragonAlgoConfig
from .domain import Candle, PositionSide, Side


@dataclass
class DragonDcaAtrState:
    # Mirrors Haas persistent state (we'll add more fields as we port)
    dca_level: int = 0
    entry_count: int = 0
    base_price: float = 0.0
    base_qty: float = 0.0

    pending_dca_order_id: Optional[int] = None
    tp_order_id: Optional[int] = None

    tp_init_dist: float = 0.0
    dca_recalc_tp: bool = False

    last_entry_index: Optional[int] = None  # for cooldown in bars


@dataclass
class DragonDcaAtrStrategy:
    config: DragonAlgoConfig
    state: DragonDcaAtrState = field(default_factory=DragonDcaAtrState)

    def on_bar(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        Main per-bar logic (v0):
          - if flat: simple initial long entry
          - if in position: simple DCA based on base deviation & multiplier
        """
        pos_side = broker.position.side

        if pos_side == PositionSide.FLAT:
            self._handle_flat_state(i, candle, broker)
        else:
            self._handle_in_position(i, candle, broker)

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

    def _handle_flat_state(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        v0 behaviour:
          - If long trades allowed and cooldown passed -> open an initial long.
          - Position size: use ~10% of current balance as notional.
        """
        g = self.config.general

        if not g.allow_long:
            return

        if not self._cooldown_passed(i):
            return

        if broker.balance <= 0:
            return

        # Use 10% of balance as notional for this example
        notional = broker.balance * 0.10
        if candle.close <= 0:
            return

        size = notional / candle.close
        if size <= 0:
            return

        trade = broker.place_market(
            side=Side.LONG,
            price=candle.close,
            size=size,
            note="Initial-Long",
        )

        # Update state after fill
        self.state.entry_count = 1
        self.state.dca_level = 0
        self.state.base_price = broker.position.avg_price
        self.state.base_qty = broker.position.size
        self.state.last_entry_index = i

    def _handle_in_position(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        v0 behaviour:
          - Long-only DCA ladder based on:
              base deviation %, deviation multiplier, volume multiplier
          - Uses the avg entry price as the base (as per config)
          - No exits yet – we're just testing entries and DCA.
        """
        if broker.position.side != PositionSide.LONG:
            # For now, we only implement long side.
            return

        g = self.config.general
        dca_cfg = self.config.dca

        # Respect max entries
        if self.state.entry_count >= g.max_entries:
            return

        # Determine base price for DCA calculation
        base_price = (
            broker.position.avg_price if g.use_avg_entry_for_dca_base else self.state.base_price
        )
        if base_price <= 0:
            return

        # Calculate deviation for the *next* DCA level
        # base_deviation_pct is in percent, e.g. 0.9 means 0.9%
        level = self.state.dca_level
        dev_pct = dca_cfg.base_deviation_pct * (dca_cfg.deviation_multiplier ** level)
        dev_frac = dev_pct / 100.0

        # For long: we want price to drop below base by dev_pct
        target_price = base_price * (1.0 - dev_frac)

        # If current close hasn't reached the target yet, do nothing
        if candle.close > target_price:
            return

        # Otherwise, place a DCA buy (increase size by volume multiplier each rung)
        # Simple approach: size = initial_size * (volume_multiplier ** level)
        # We use base_qty as the "initial" size.
        if self.state.base_qty <= 0:
            self.state.base_qty = broker.position.size

        dca_size = self.state.base_qty * (dca_cfg.volume_multiplier ** level)

        if dca_size <= 0:
            return

        trade = broker.place_market(
            side=Side.LONG,
            price=candle.close,
            size=dca_size,
            note=f"DCA-L{level + 1}",
        )

        # Update state after DCA fill
        self.state.dca_level += 1
        self.state.entry_count += 1
        self.state.base_price = broker.position.avg_price
        self.state.base_qty = broker.position.size
        self.state.last_entry_index = i
