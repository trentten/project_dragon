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


@dataclass
class DragonDcaAtrStrategy:
    config: DragonAlgoConfig
    state: DragonDcaAtrState = field(default_factory=DragonDcaAtrState)

    def on_bar(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        Main per-bar logic – this will eventually mirror your Haas script:
          - compute signals (trend + consensus)
          - if flat: maybe open initial entry
          - if in position: manage DCA, TP, trailing exit
        """
        pos_side = broker.position.side

        if pos_side == PositionSide.FLAT:
            self._handle_flat_state(i, candle, broker)
        else:
            self._handle_in_position(i, candle, broker)

    def on_finalize(self, broker: BrokerSim) -> None:
        """
        End-of-backtest hook. Later we can compute strategy-specific
        diagnostics or custom reports.
        """
        # For now, nothing special here.
        pass

    # --- Internal helpers ------------------------------------------------

    def _handle_flat_state(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        Mirrors the 'if botPos == NoPosition' block in Haas:
          - evaluate indicator consensus + trend filter
          - potentially place initial entry order
        """
        # TODO:
        # - compute indicator signals over historical candles
        # - apply MA filter (c > MA for longs)
        # - entry cooldown logic
        #
        # For now this is a stub so the engine compiles & runs.
        pass

    def _handle_in_position(self, i: int, candle: Candle, broker: BrokerSim) -> None:
        """
        Mirrors the in-position logic in your Haas script:
          - manage DCA
          - manage TP (ATR or fixed)
          - manage trailing exit / stop-loss
        """
        # TODO:
        # - Port DCA ladder: base dev, multipliers, average entry base
        # - Port ATR-based TP with re-anchoring after DCA
        # - Port trailing stop logic
        pass

    # As we port from Haas, we'll add:
    #   - _update_initial_entry(...)
    #   - _process_initial_entry_fill(...)
    #   - _manage_dca(...)
    #   - _update_tp(...)
    #   - _update_trailing_exit(...)
