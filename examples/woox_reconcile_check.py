from __future__ import annotations

import pprint
from typing import Any, Dict, List

from project_dragon.brokers.woox_broker import WooXBroker
from project_dragon.domain import Order, OrderActivationState, OrderStatus, OrderType, Side
from project_dragon.live.trade_config import derive_primary_position_side_from_allow_flags
from project_dragon.live_worker import _emit_order_diffs, _order_state_snapshot


class DummyClient:
    """Offline stub that replays predefined order snapshots."""

    def __init__(self, snapshots: List[List[Dict[str, Any]]]):
        self.snapshots = snapshots
        self.call_idx = 0
        self.get_order_calls: List[Dict[str, Any]] = []

    def set_position_mode(self, mode: str):
        return {}

    def get_positions(self, symbol: str):
        return {"rows": []}

    def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
        idx = min(self.call_idx, len(self.snapshots) - 1)
        self.call_idx += 1
        return {"rows": self.snapshots[idx]}

    def get_order(self, order_id: Any, symbol: str, client_order_id: Any = None):
        # Return mismatching identifiers to force verification failure.
        self.get_order_calls.append({"order_id": order_id, "client_order_id": client_order_id})
        return {
            "data": {
                "orderId": "ex-wrong",
                "clientOrderId": None,
                "status": "INCOMPLETE",
            }
        }

    def place_order(self, body: Dict[str, Any]):
        return {
            "orderId": f"ex-{self.call_idx+1}",
            "clientOrderId": body.get("clientOrderId"),
        }

    def cancel_order(self, order_id, symbol: str, client_order_id=None):
        return {}

    def get_orderbook(self, symbol: str, depth: int = 1):
        return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}


def main() -> None:
    # --- Allow flags -> primary leg mapping -------------------------------
    assert derive_primary_position_side_from_allow_flags(allow_long=True, allow_short=False) == "LONG"
    assert derive_primary_position_side_from_allow_flags(allow_long=False, allow_short=True) == "SHORT"

    # --- Positions: hedge-safe parsing + primary leg alias -----------------
    class DummyPositionsClient:
        def __init__(self, payload: Dict[str, Any]):
            self.payload = payload

        def set_position_mode(self, mode: str):
            return {}

        def get_positions(self, symbol: str):
            return self.payload

        def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
            return {"rows": []}

        def get_orderbook(self, symbol: str, depth: int = 1):
            return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}

    short_payload = {
        "positions": [
            {
                "symbol": "PERP_DEMO",
                "positionSide": "BOTH",
                "holding": "-0.5",
                "averageOpenPrice": "200.0",
                "markPrice": "201.0",
                "timestamp": 1766464474939,
                "leverage": "10",
            }
        ]
    }
    short_client = DummyPositionsClient(short_payload)
    short_broker = WooXBroker(short_client, symbol="PERP_DEMO", bot_id="demo-short", prefer_bbo_maker=False, primary_position_side="SHORT")
    assert "SHORT" in short_broker.positions
    assert short_broker.position.side.name == "SHORT"
    assert abs(short_broker.position.size - 0.5) < 1e-9

    long_payload = {
        "positions": [
            {
                "symbol": "PERP_DEMO",
                "positionSide": "BOTH",
                "holding": "0.75",
                "averageOpenPrice": "150.0",
                "markPrice": "151.0",
                "timestamp": 1766464474939,
                "leverage": "5",
            }
        ]
    }
    long_client = DummyPositionsClient(long_payload)
    long_broker = WooXBroker(long_client, symbol="PERP_DEMO", bot_id="demo-long", prefer_bbo_maker=False, primary_position_side="LONG")
    assert "LONG" in long_broker.positions
    assert long_broker.position.side.name == "LONG"
    assert abs(long_broker.position.size - 0.75) < 1e-9

    hedge_payload = {
        "positions": [
            {
                "symbol": "PERP_DEMO",
                "positionSide": "LONG",
                "holding": "1.0",
                "averageOpenPrice": "100.0",
                "markPrice": "101.0",
                "timestamp": 1766464474939,
            },
            {
                "symbol": "PERP_DEMO",
                "positionSide": "SHORT",
                "holding": "-2.0",
                "averageOpenPrice": "110.0",
                "markPrice": "109.0",
                "timestamp": 1766464474939,
            },
        ]
    }
    hedge_client = DummyPositionsClient(hedge_payload)
    hedge_broker_short = WooXBroker(
        hedge_client,
        symbol="PERP_DEMO",
        bot_id="demo-hedge",
        prefer_bbo_maker=False,
        primary_position_side="SHORT",
    )
    assert "LONG" in hedge_broker_short.positions
    assert "SHORT" in hedge_broker_short.positions
    assert hedge_broker_short.position.side.name == "SHORT"
    assert abs(hedge_broker_short.position.size - 2.0) < 1e-9

    # Snapshots: initial open, partial (clientOrderId missing), then empty to force misses.
    snapshots: List[List[Dict[str, Any]]] = [
        [
            {
                "orderId": "ex-1",
                "clientOrderId": "DRAGON-demo-1",
                "status": "INCOMPLETE",
                "side": "BUY",
                "positionSide": "LONG",
                "price": 100.0,
                "quantity": 1.0,
                "type": "LIMIT",
            }
        ],
        [
            {
                "orderId": "ex-1",
                "status": "PARTIALLY_FILLED",
                "side": "BUY",
                "positionSide": "LONG",
                "price": 100.0,
                "quantity": 1.0,
                "filledQty": 0.4,
                "avgPrice": 100.5,
                "type": "LIMIT",
            }
        ],
        [],
        [],
        [],
    ]

    client = DummyClient(snapshots)
    broker = WooXBroker(client, symbol="PERP_DEMO", bot_id="demo", prefer_bbo_maker=False)

    events: List[Dict[str, Any]] = []

    def emit(level: str, event_type: str, message: str, payload: Dict[str, Any]):
        events.append({"level": level, "event": event_type, "msg": message, "payload": payload})

    broker.set_event_logger(emit)

    # Seed local intent; clientOrderId will be DRAGON-demo-1
    local_id = broker.place_limit("BUY", "LONG", price=100.0, size=1.0, note="demo-intent", local_id=1)
    broker._local_order_id = max(broker._local_order_id, local_id + 1)

    # Local-only parked intent should never be auto-cancelled by sync_orders.
    parked_local_id = broker._new_local_order_id()
    parked_order = Order(
        id=parked_local_id,
        side=Side.LONG,
        type=OrderType.LIMIT,
        price=99.0,
        size=1.0,
        status=OrderStatus.OPEN,
        note="parked-intent",
    )
    setattr(parked_order, "activation_state", OrderActivationState.PARKED)
    broker.open_orders[parked_local_id] = parked_order

    prev = _order_state_snapshot(broker)
    for idx in range(len(snapshots)):
        broker.sync_orders()
        curr = _order_state_snapshot(broker)
        _emit_order_diffs(prev, curr, emit)
        order = broker.get_order(local_id)
        print(
            f"sync {idx}: status={order.status.name} filled={getattr(order, 'filled_size', 0.0)} avg={getattr(order, 'avg_fill_price', None)}"
        )
        prev = curr

    parked_after = broker.get_order(parked_local_id)
    print(f"\nParked intent status: {parked_after.status.name}")
    assert parked_after is not None
    assert parked_after.status == OrderStatus.OPEN

    order = broker.get_order(local_id)
    assert order is not None
    # clientOrderId should survive missing field in snapshot
    assert getattr(order, "client_order_id", None) == "DRAGON-demo-1"
    # order should cancel after misses (mock get_order does not verify)
    assert order.status == OrderStatus.CANCELLED

    # Warning cadence: only miss_count 1 and 3
    warning_counts = [e for e in events if e["event"] == "sync_warning" and "miss_count" in e.get("payload", {})]
    print("Warning events:", warning_counts)
    assert len(warning_counts) == 2
    assert {e["payload"].get("miss_count") for e in warning_counts} == {1, 3}

    # Cancellation event emitted
    cancel_events = [e for e in events if e["event"] == "order_missing_cancelled"]
    assert len(cancel_events) == 1
    assert cancel_events[0]["payload"].get("miss_count") == 3

    print("\nCaptured events:")
    pprint.pprint(events)

    print("\nFinal order status:", order.status.name)


if __name__ == "__main__":
    main()
