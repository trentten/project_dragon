"""Offline check for WooX hedge mode enforcement.

Run:
  python -m examples.woox_positions_check

This does not hit the network.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from project_dragon.brokers.woox_broker import WooXBroker, WooXBrokerError


class DummyClient:
    def __init__(self, positions_payload: Dict[str, Any]):
        self._positions_payload = positions_payload

    def set_position_mode(self, mode: str):
        return {}

    def get_positions(self, symbol: str):
        return self._positions_payload

    def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
        return {"rows": []}


def main() -> None:
    events: List[Dict[str, Any]] = []

    def _logger(level: str, event_type: str, message: str, payload: Optional[Dict[str, Any]] = None) -> None:
        events.append(
            {
                "level": level,
                "event_type": event_type,
                "message": message,
                "payload": payload or {},
            }
        )

    payload_one_way = {
        "rows": [
            {
                "symbol": "PERP_DEMO",
                "positionSide": "BOTH",
                "holding": -500,
                "averageOpenPrice": 100.0,
            }
        ]
    }

    # require_hedge_mode=True should fail hard.
    broker_strict = WooXBroker(
        DummyClient(payload_one_way),
        symbol="PERP_DEMO",
        bot_id="demo-strict",
        require_hedge_mode=True,
        auto_sync=False,
    )
    broker_strict.set_event_logger(_logger)

    raised = False
    try:
        broker_strict.sync_positions()
    except WooXBrokerError:
        raised = True

    assert raised, "Expected WooXBrokerError when require_hedge_mode=True and positionSide=BOTH"
    assert any(e.get("event_type") == "hedge_mode_required" for e in events), "Expected hedge_mode_required event"

    # require_hedge_mode=False should normalize BOTH via sign of holding.
    events.clear()
    broker_compat = WooXBroker(
        DummyClient(payload_one_way),
        symbol="PERP_DEMO",
        bot_id="demo-compat",
        require_hedge_mode=False,
        auto_sync=False,
    )
    broker_compat.set_event_logger(_logger)
    broker_compat.sync_positions()

    assert "SHORT" in (broker_compat.positions or {}), "Expected BOTH holding -500 to normalize into SHORT position"
    assert not any(e.get("event_type") == "hedge_mode_required" for e in events), "Did not expect hedge_mode_required event in compat mode"

    print("OK: hedge mode enforcement works as expected")


if __name__ == "__main__":
    main()
