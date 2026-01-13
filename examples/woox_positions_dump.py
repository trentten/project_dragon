import json
import os
from project_dragon.brokers.woox_client import WooXClient

def main():
    api_key = os.environ["WOOX_API_KEY"]
    api_secret = os.environ["WOOX_API_SECRET"]
    symbol = os.environ.get("WOOX_SYMBOL", "PERP_BTC_USDT")  # change if needed

    client = WooXClient(api_key=api_key, api_secret=api_secret)
    data = client.get_positions(symbol)

    print(json.dumps(data, indent=2, sort_keys=True))

if __name__ == "__main__":
    main()
