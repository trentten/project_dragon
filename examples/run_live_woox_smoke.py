from __future__ import annotations

import os
from dataclasses import dataclass

from project_dragon.brokers.woox_client import WooXClient, WooXAPIError
from project_dragon.brokers.woox_broker import WooXBroker
from project_dragon.domain import Side


@dataclass
class Args:
    symbol: str = "PERP_BTC_USDT"
    qty: float = 0.001
    really_trade: bool = False


def main() -> None:
    args = Args()
    api_key = os.environ.get("WOOX_API_KEY")
    api_secret = os.environ.get("WOOX_API_SECRET")
    base_url = os.environ.get("WOOX_BASE_URL", "https://api.woox.io")

    if not api_key or not api_secret:
        print("Set WOOX_API_KEY and WOOX_API_SECRET in env before running.")
        return

    client = WooXClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
    broker = WooXBroker(client, symbol=args.symbol)

    try:
        broker.sync_positions()
        long_pos = broker.get_position(Side.LONG)
        short_pos = broker.get_position(Side.SHORT)
        print("LONG position", long_pos)
        print("SHORT position", short_pos)
    except WooXAPIError as exc:
        print(f"Failed to sync positions: {exc}")
        return

    if not args.really_trade:
        print("Skipping live order because really_trade is False; set Args.really_trade=True to send.")
        return

    try:
        oid = broker.place_limit(Side.LONG, price=1.0, size=args.qty, note="smoke-test", post_only=True)
        print(f"Placed test limit order {oid}; cancelling...")
        broker.cancel_order(oid)
        print("Cancelled test order")
    except WooXAPIError as exc:
        print(f"Order failed: {exc}")


if __name__ == "__main__":
    main()
