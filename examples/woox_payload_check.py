from __future__ import annotations

from project_dragon.brokers.woox_broker import WooXBroker
from project_dragon.brokers.woox_client import WooXAPIError


class DummyClient:
    def __init__(self) -> None:
        self.last_order = None

    def set_position_mode(self, mode: str):
        return {}

    def get_positions(self, symbol: str):
        return {"rows": []}

    def get_orders(self, symbol: str, status: str = "INCOMPLETE"):
        return {"rows": []}

    def place_order(self, body):
        self.last_order = body
        return {"orderId": "1", "clientOrderId": body.get("clientOrderId")}

    def cancel_order(self, order_id, symbol: str, client_order_id=None):
        return {}

    def get_orderbook(self, symbol: str, depth: int = 1):
        return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}


class FailBBOClient(DummyClient):
    def place_order(self, body):
        order_type = body.get("type")
        if order_type in {"BID", "ASK"}:
            raise WooXAPIError("simulated BBO failure")
        self.last_order = body
        return {"orderId": "fallback", "clientOrderId": body.get("clientOrderId")}


def run_checks() -> None:
    client = DummyClient()
    broker = WooXBroker(client, symbol="TEST_SYMBOL", bot_id="payload-check")

    # clientOrderId stability should be deterministic per local id
    cid_a = broker._client_order_id(42)
    cid_b = broker._client_order_id(42)
    assert cid_a == cid_b and isinstance(cid_a, str) and len(cid_a) > 0

    broker.place_bbo_queue_limit(
        order_side="BUY",
        position_side="LONG",
        size=1.0,
        bid_ask_level=2,
        note="bbo-check",
        reduce_only=False,
    )
    payload_bbo = client.last_order
    assert payload_bbo["type"] == "BID"
    assert "price" not in payload_bbo
    for key in ("bidAskLevel", "quantity", "positionSide", "reduceOnly", "clientOrderId"):
        assert key in payload_bbo
    assert payload_bbo["bidAskLevel"] == 2
    assert payload_bbo.get("orderTag") == "bbo-check"
    assert str(payload_bbo["clientOrderId"]).startswith("DRAGON-")

    # SELL-side BBO should be ASK with no price and carry the note
    broker.place_bbo_queue_limit(
        order_side="SELL",
        position_side="SHORT",
        size=1.0,
        bid_ask_level=3,
        note="bbo-sell",
        reduce_only=True,
    )
    payload_bbo_sell = client.last_order
    assert payload_bbo_sell["type"] == "ASK"
    assert "price" not in payload_bbo_sell
    for key in ("bidAskLevel", "quantity", "positionSide", "reduceOnly", "clientOrderId"):
        assert key in payload_bbo_sell
    assert payload_bbo_sell["bidAskLevel"] == 3
    assert payload_bbo_sell["positionSide"] == "SHORT"
    assert payload_bbo_sell.get("reduceOnly") is True
    assert payload_bbo_sell.get("orderTag") == "bbo-sell"

    # Clamp bidAskLevel low/high
    broker.place_bbo_queue_limit(order_side="BUY", position_side="LONG", size=1.0, bid_ask_level=0)
    assert client.last_order["bidAskLevel"] == 1
    broker.place_bbo_queue_limit(order_side="BUY", position_side="LONG", size=1.0, bid_ask_level=99)
    assert client.last_order["bidAskLevel"] == 5

    # Fallback path: BBO placement fails, should place POST_ONLY at top of book
    fail_client = FailBBOClient()
    fb_broker = WooXBroker(fail_client, symbol="TEST_SYMBOL", bot_id="fallback-check")
    fb_broker.prefer_bbo_maker = True

    fb_broker.place_bbo_queue_limit(
        order_side="BUY",
        position_side="LONG",
        size=1.0,
        bid_ask_level=1,
        note="fallback-buy",
        reduce_only=False,
    )
    fb_payload = fail_client.last_order
    assert fb_payload["type"] == "POST_ONLY"
    assert fb_payload["price"] == 100.0  # bid
    assert fb_payload.get("postOnlyAdjusted") is True
    assert fb_payload.get("orderTag") == "fallback-buy"
    assert fb_payload.get("clientOrderId") and isinstance(fb_payload.get("clientOrderId"), str)
    assert "postOnly" not in fb_payload

    fb_broker.place_bbo_queue_limit(
        order_side="SELL",
        position_side="SHORT",
        size=1.0,
        bid_ask_level=1,
        note="fallback-sell",
        reduce_only=False,
    )
    fb_payload_sell = fail_client.last_order
    assert fb_payload_sell["type"] == "POST_ONLY"
    assert fb_payload_sell["price"] == 101.0  # ask
    assert fb_payload_sell.get("postOnlyAdjusted") is True
    assert fb_payload_sell.get("orderTag") == "fallback-sell"
    assert fb_payload_sell.get("clientOrderId") and isinstance(fb_payload_sell.get("clientOrderId"), str)
    assert "postOnly" not in fb_payload_sell

    print("WooX payload checks passed.")


if __name__ == "__main__":
    run_checks()
