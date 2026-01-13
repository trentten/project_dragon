from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from project_dragon.brokers.woox_broker import WooXBroker, DynamicOrder
from project_dragon.data_online import load_ccxt_candles
from project_dragon.domain import Candle


def _timeframe_to_ms(tf: str) -> int:
    if not tf:
        return 60_000
    unit = tf[-1]
    try:
        val = int(tf[:-1])
    except ValueError:
        return 60_000
    mult = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}.get(unit, 60_000)
    return val * mult


@dataclass
class BarDrivenRunner:
    broker: WooXBroker
    strategy: any
    symbol: str
    timeframe: str = "1m"
    poll_interval_seconds: float = 2.0
    exchange_id: str = "woo"

    def __post_init__(self) -> None:
        self._stop_event = threading.Event()
        self._last_bar_ts: Optional[int] = None
        self._bar_index = 0

    def stop(self) -> None:
        self._stop_event.set()

    def run(self, max_loops: Optional[int] = None) -> None:
        loops = 0
        while not self._stop_event.is_set():
            loops += 1
            if max_loops is not None and loops > max_loops:
                break
            candles = load_ccxt_candles(self.exchange_id, self.symbol, self.timeframe, limit=2)
            if not candles:
                time.sleep(self.poll_interval_seconds)
                continue
            latest = candles[-1]
            if latest.timestamp.tzinfo is None:
                latest_ts_ms = int(latest.timestamp.timestamp() * 1000)
            else:
                latest_ts_ms = int(latest.timestamp.replace(tzinfo=timezone.utc).timestamp() * 1000)

            ms_per_bar = _timeframe_to_ms(self.timeframe)
            bar_closed = self._last_bar_ts is None or latest_ts_ms > self._last_bar_ts
            if bar_closed:
                self._process_bar(latest)
                self._last_bar_ts = latest_ts_ms
                self._bar_index += 1

            time.sleep(self.poll_interval_seconds)

    def _process_bar(self, candle: Candle) -> None:
        self.broker.sync()
        self._handle_dynamic_orders(candle.close)
        if hasattr(self.broker, "update_bar_context"):
            # for compatibility; WooXBroker lacks update_bar_context but strategy may not require it
            try:
                self.broker.update_bar_context(self._bar_index, candle)  # type: ignore[attr-defined]
            except Exception:
                pass
        self.strategy.on_bar(self._bar_index, candle, self.broker)
        # After strategy actions, re-evaluate dynamic orders with latest price
        self._handle_dynamic_orders(candle.close)

    def _handle_dynamic_orders(self, ref_price: float) -> None:
        if ref_price <= 0:
            return
        for dyn in list(self.broker.dynamic_orders.values()):
            if dyn.activation_pct <= 0:
                if not dyn.active:
                    self.broker.activate_dynamic(dyn)
                continue
            dist = abs(ref_price - dyn.price) / dyn.price if dyn.price else 1.0
            threshold = (dyn.activation_pct or 0) / 100.0
            if dist <= threshold:
                if not dyn.active:
                    self.broker.activate_dynamic(dyn)
            else:
                if dyn.active:
                    self.broker.cancel_dynamic(dyn)