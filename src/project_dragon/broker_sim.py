from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from .domain import (
    Candle,
    Order,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
    Side,
    Trade,
    OrderActivationState,
)


@dataclass
class BrokerSim:
    balance: float = 1_000.0
    fee_rate: float = 0.0004  # 0.04% per side, tweak as needed
    fees_total: float = 0.0
    position: Position = field(default_factory=Position)
    orders: Dict[int, Order] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)

    current_index: int = -1
    current_timestamp: Optional[datetime] = None
    _last_price: float = 0.0

    _next_order_id: int = 1

    def _new_order_id(self) -> int:
        oid = self._next_order_id
        self._next_order_id += 1
        return oid

    # --- Order placement -------------------------------------------------

    def place_limit(self, side: Side, price: float, size: float, note: str = "") -> int:
        oid = self._new_order_id()
        self.orders[oid] = Order(
            id=oid,
            side=side,
            type=OrderType.LIMIT,
            price=price,
            size=size,
            note=note,
            target_price=price,
            activation_state=OrderActivationState.ACTIVE,
        )
        return oid

    def place_dynamic_limit(
        self,
        side: Side,
        price: float,
        size: float,
        activation_pct: float,
        note: str = "",
    ) -> int:
        if activation_pct <= 0:
            return self.place_limit(side=side, price=price, size=size, note=note)
        oid = self._new_order_id()
        self.orders[oid] = Order(
            id=oid,
            side=side,
            type=OrderType.LIMIT,
            price=price,
            size=size,
            note=note,
            target_price=price,
            activation_band_pct=activation_pct,
            activation_state=OrderActivationState.PARKED,
        )
        return oid

    def place_market(self, side: Side, size: float, note: str = "", price: float | None = None) -> Trade:
        """
        Immediate-or-cancel style helper used by strategies.
        `price` is optional; if omitted the trade uses the broker's notion of last/target price,
        so call sites should pass the candle close when available.
        """
        if price is None:
            price = self._last_price or 0.0  # `_last_price` can be maintained elsewhere; default to 0
        trade = self._execute_trade(side=side, price=price, size=size, note=note)
        return trade

    # --- Order cancellation ----------------------------------------------

    def cancel_order(self, order_id: int) -> None:
        order = self.orders.get(order_id)
        if order and order.status == OrderStatus.OPEN:
            order.status = OrderStatus.CANCELLED

    def get_order(self, order_id: int) -> Order | None:
        return self.orders.get(order_id)

    # --- Fill simulation for LIMIT orders --------------------------------

    def process_candle(self, candle: Candle) -> None:
        """
        Simple fill model:
        - For BUY limit (Side.LONG): fill if candle.low <= price.
        - For SELL limit (Side.SHORT): fill if candle.high >= price.
        Full fill only for now; later we can add partials.
        """
        for order in list(self.orders.values()):
            if order.status != OrderStatus.OPEN or order.type != OrderType.LIMIT:
                continue
            self._update_dynamic_state(order, candle)
            if order.activation_state != OrderActivationState.ACTIVE:
                continue

            if order.side == Side.LONG:
                if candle.low <= (order.price or 0):
                    self._fill_order(order, candle)
            else:
                if candle.high >= (order.price or 0):
                    self._fill_order(order, candle)

    def _fill_order(self, order: Order, candle: Candle) -> None:
        fill_price = order.price or candle.close
        size = order.size

        trade = self._execute_trade(
            side=order.side,
            price=fill_price,
            size=size,
            note=order.note,
        )
        trade.timestamp = candle.timestamp

        order.status = OrderStatus.FILLED
        order.filled_size = size

    def _update_dynamic_state(self, order: Order, candle: Candle) -> None:
        if order.activation_band_pct is None or order.activation_band_pct <= 0:
            order.activation_state = OrderActivationState.ACTIVE
            return
        ref_price = candle.close
        target = order.target_price or order.price or ref_price
        if ref_price <= 0 or target <= 0:
            order.activation_state = OrderActivationState.PARKED
            return
        dist = abs(ref_price - target) / target
        threshold = (order.activation_band_pct or 0) / 100.0
        if dist <= threshold:
            order.activation_state = OrderActivationState.ACTIVE
        else:
            order.activation_state = OrderActivationState.PARKED
        order.last_activation_price = ref_price

    # --- Position & PnL logic --------------------------------------------

    def _execute_trade(self, side: Side, price: float, size: float, note: str = "") -> Trade:
        """
        Maintains a single net position (LONG, SHORT, or FLAT).

        Semantics:
        - Side.LONG  => BUY (opens/increases long, closes/reduces short)
        - Side.SHORT => SELL (opens/increases short, closes/reduces long)
        """
        pnl = 0.0
        executed_size = float(size)

        if side == Side.LONG:
            # BUY: open/increase long, or close/reduce short
            if self.position.side in (PositionSide.FLAT, PositionSide.LONG):
                # Open/increase long
                executed_size = float(size)
                cost = price * executed_size
                fee = cost * self.fee_rate
                new_size = self.position.size + size
                new_avg = (
                    (self.position.avg_price * self.position.size) + cost
                ) / new_size if new_size > 0 else 0.0
                self.position.side = PositionSide.LONG
                self.position.size = new_size
                self.position.avg_price = new_avg
                self.balance -= cost
                self.balance -= fee
                self.fees_total += float(fee or 0.0)
            else:
                # Close/reduce short
                portion = min(float(size), float(self.position.size))
                executed_size = float(portion)
                cost = price * executed_size
                fee = cost * self.fee_rate
                pnl = (self.position.avg_price - price) * executed_size
                self.balance -= cost
                self.balance -= fee
                self.fees_total += float(fee or 0.0)
                self.position.size -= executed_size
                if self.position.size <= 0:
                    self.position.reset()

        else:
            # SELL: open/increase short, or close/reduce long
            if self.position.side == PositionSide.LONG:
                # Close/reduce long
                portion = min(float(size), float(self.position.size))
                executed_size = float(portion)
                cost = price * executed_size
                fee = cost * self.fee_rate
                pnl = (price - self.position.avg_price) * executed_size
                self.balance += cost
                self.balance -= fee
                self.fees_total += float(fee or 0.0)
                self.position.size -= executed_size
                if self.position.size <= 0:
                    self.position.reset()
            else:
                # Open/increase short
                executed_size = float(size)
                cost = price * executed_size
                fee = cost * self.fee_rate
                new_size = self.position.size + size
                new_avg = (
                    (self.position.avg_price * self.position.size) + cost
                ) / new_size if new_size > 0 else 0.0
                self.position.side = PositionSide.SHORT
                self.position.size = new_size
                self.position.avg_price = new_avg
                # "Short sale" cashflow model: receive proceeds now.
                self.balance += cost
                self.balance -= fee
                self.fees_total += float(fee or 0.0)

        trade = Trade(
            index=self.current_index,
            timestamp=self.current_timestamp,
            side=side,
            price=price,
            size=executed_size,
            pnl=pnl,
            note=note,
        )
        self.trades.append(trade)
        return trade

    def update_bar_context(self, index: int, candle: Candle) -> None:
        self.current_index = index
        self.current_timestamp = candle.timestamp
        self._last_price = candle.close
        self._current_candle = candle
