from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from .domain import (
    Candle,
    Order,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
    Side,
    Trade,
)


@dataclass
class BrokerSim:
    balance: float = 1_000.0
    fee_rate: float = 0.0004  # 0.04% per side, tweak as needed
    position: Position = field(default_factory=Position)
    orders: Dict[int, Order] = field(default_factory=dict)
    trades: List[Trade] = field(default_factory=list)

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
        )
        return oid

    def place_market(self, side: Side, price: float, size: float, note: str = "") -> Trade:
        """
        Immediate fill at provided price.
        Engine or strategy will pass last/close price in backtest.
        """
        trade = self._execute_trade(side=side, price=price, size=size, note=note)
        return trade

    def cancel_order(self, order_id: int) -> None:
        order = self.orders.get(order_id)
        if order and order.status == OrderStatus.OPEN:
            order.status = OrderStatus.CANCELLED

    # --- Fill simulation for LIMIT orders --------------------------------

    def process_candle(self, candle: Candle) -> None:
        """
        Simple fill model:
        - For BUY limit: fill if candle.low <= price.
        - For SELL limit: fill if candle.high >= price.
        Full fill only for now; later we can add partials.
        """
        for order in list(self.orders.values()):
            if order.status != OrderStatus.OPEN or order.type != OrderType.LIMIT:
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

    # --- Position & PnL logic --------------------------------------------

    def _execute_trade(self, side: Side, price: float, size: float, note: str = "") -> Trade:
        """
        Maintains a single net position (long or flat for now).
        Extension to true long/short netting is possible later.
        """
        fee = price * size * self.fee_rate
        cost = price * size

        if side == Side.LONG:
            # Open/increase long position
            if self.position.side in (PositionSide.FLAT, PositionSide.LONG):
                new_size = self.position.size + size
                if new_size > 0:
                    new_avg = (
                        self.position.avg_price * self.position.size + cost
                    ) / new_size
                else:
                    new_avg = 0.0
                self.position.side = PositionSide.LONG
                self.position.size = new_size
                self.position.avg_price = new_avg

                self.balance -= cost
                self.balance -= fee

                pnl = 0.0  # no realized pnl on opening/add
            else:
                # TODO: handle closing shorts later
                pnl = 0.0
        else:
            # Selling: reduce or close long
            if self.position.side == PositionSide.LONG:
                portion = min(size, self.position.size)
                pnl = (price - self.position.avg_price) * portion
                self.balance += price * portion
                self.balance -= fee
                self.position.size -= portion
                if self.position.size <= 0:
                    self.position.reset()
            else:
                # TODO: opening/closing shorts later
                pnl = 0.0

        trade = Trade(
            timestamp=None,
            side=side,
            price=price,
            size=size,
            pnl=pnl,
            note=note,
        )
        self.trades.append(trade)
        return trade
