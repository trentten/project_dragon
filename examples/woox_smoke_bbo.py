from __future__ import annotations

import argparse
import os
import sys
import time

from project_dragon.brokers.woox_broker import WooXBroker
from project_dragon.brokers.woox_client import WooXAPIError, WooXClient


class PreviewClient:
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
        return {"orderId": "demo", "clientOrderId": body.get("clientOrderId")}

    def cancel_order(self, order_id, symbol: str, client_order_id=None):
        return {}

    def get_orderbook(self, symbol: str, depth: int = 1):
        return {"bids": [[100.0, 1]], "asks": [[101.0, 1]]}


def main() -> None:
    parser = argparse.ArgumentParser(description="WooX BBO maker smoke test (safe by default)")
    parser.add_argument("--symbol", default="PERP_BTC_USDT", help="Symbol to test")
    parser.add_argument("--qty", type=float, default=0.001, help="Order size")
    parser.add_argument("--bbo-level", type=int, default=1, help="BBO queue level (1-5)")
    parser.add_argument("--prefer-bbo", action="store_true", help="Prefer BBO maker placement")
    parser.add_argument("--bot-id", default="smoke", help="Bot id for clientOrderId prefix")
    parser.add_argument("--really-trade", action="store_true", help="Actually place/cancel orders")
    args = parser.parse_args()

    api_key = os.environ.get("WOOX_API_KEY")
    api_secret = os.environ.get("WOOX_API_SECRET")
    base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")

    if not api_key or not api_secret:
        print("Set WOOX_API_KEY and WOOX_API_SECRET in env before running.")
        sys.exit(1)

    if not args.really_trade:
        client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
        try:
            client.get_positions(args.symbol)
            print("Auth check OK (get_positions)")
        except WooXAPIError as exc:
            print(f"Auth check failed: {exc}")
            sys.exit(1)

        preview_client = PreviewClient()
        broker = WooXBroker(preview_client, symbol=args.symbol, bot_id=args.bot_id)
        broker.prefer_bbo_maker = bool(args.prefer_bbo)
        broker.bbo_level = max(1, min(5, int(args.bbo_level)))
        broker.place_bbo_queue_limit(
            order_side="BUY",
            position_side="LONG",
            size=args.qty,
            bid_ask_level=broker.bbo_level,
            note="preview",
            reduce_only=False,
        )
        print("Would place BBO order with payload:")
        print(preview_client.last_order)
        sys.exit(0)

    client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
    broker = WooXBroker(client, symbol=args.symbol, bot_id=args.bot_id)
    broker.prefer_bbo_maker = bool(args.prefer_bbo)
    broker.bbo_level = max(1, min(5, int(args.bbo_level)))

    try:
        oid = broker.place_bbo_queue_limit(
            order_side="BUY",
            position_side="LONG",
            size=args.qty,
            bid_ask_level=broker.bbo_level,
            note="smoke-bbo",
            reduce_only=False,
        )
        print(f"Placed BBO order local_id={oid}")
        time.sleep(2)
        order = broker.get_order(oid)
        ext_id = getattr(order, "external_id", None) if order else None
        client_oid = getattr(order, "client_order_id", None) if order else None
        if ext_id or client_oid:
            broker.client.cancel_order(ext_id, args.symbol, client_order_id=client_oid)
            print(f"Cancelled order ext={ext_id} client_oid={client_oid}")
        else:
            print("No external id found to cancel (was it rejected?)")
    except WooXAPIError as exc:
        print(f"Live BBO smoke failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
