from __future__ import annotations

import os
import time
import argparse

from project_dragon.brokers.woox_client import WooXClient, WooXAPIError
from project_dragon.brokers.woox_broker import WooXBroker
from project_dragon.domain import Side


def main() -> None:
    parser = argparse.ArgumentParser(description="WooX BBO queue smoke test")
    parser.add_argument("--symbol", default="PERP_BTC_USDT", help="Symbol to test")
    parser.add_argument("--qty", type=float, default=0.001, help="Order size")
    parser.add_argument("--really-trade", action="store_true", help="Actually place/cancel orders")
    args = parser.parse_args()

    api_key = os.environ.get("WOOX_API_KEY")
    api_secret = os.environ.get("WOOX_API_SECRET")
    base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")

    if not api_key or not api_secret:
        print("Set WOOX_API_KEY and WOOX_API_SECRET in env before running.")
        return

    if not args.really_trade:
        print("Dry run only (use --really-trade to send live orders)")
        return

    client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
    broker = WooXBroker(client, symbol=args.symbol)

    try:
        broker.sync_positions()
    except WooXAPIError as exc:
        print(f"Failed to sync positions: {exc}")
        return

    for level in (1, 3):
        try:
            oid = broker.place_bbo_queue_limit(
                order_side="BUY",
                position_side="LONG",
                size=args.qty,
                bid_ask_level=level,
                note=f"bbo-smoke-L{level}",
                reduce_only=False,
            )
            print(f"Placed BBO level {level} local_id={oid}")
            time.sleep(2)
            order = broker.open_orders.get(oid)
            ext_id = getattr(order, "external_id", None) if order else None
            if ext_id:
                broker.client.cancel_order(ext_id, args.symbol)
                print(f"Cancelled order external_id={ext_id}")
            else:
                broker.cancel_dynamic(broker.dynamic_orders.get(oid)) if oid in broker.dynamic_orders else broker.cancel_order(oid)
                print("Cancelled via local id")
        except WooXAPIError as exc:
            print(f"Level {level} failed: {exc}")
        time.sleep(1)


if __name__ == "__main__":
    main()
