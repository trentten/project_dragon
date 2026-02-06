from project_dragon.exchange_normalization import (
    canonical_exchange_id,
    canonical_symbol,
    canonical_woox_symbol,
    exchange_id_for_ccxt,
    exchange_ids_for_query,
)


def test_canonical_exchange_id_woox_aliases() -> None:
    assert canonical_exchange_id("woox") == "woox"
    assert canonical_exchange_id("woo") == "woox"
    assert canonical_exchange_id("woo-x") == "woox"
    assert canonical_exchange_id("woo_x") == "woox"


def test_exchange_id_for_ccxt_maps_woox_to_woo() -> None:
    assert exchange_id_for_ccxt("woox") == "woo"
    assert exchange_id_for_ccxt("woo") == "woo"


def test_exchange_ids_for_query_contains_canonical_first() -> None:
    ids = exchange_ids_for_query("woo")
    assert ids[0] == "woox"
    assert "woo" in ids


def test_canonical_woox_symbol_formats() -> None:
    assert canonical_woox_symbol("PERP_ETH_USDT") == "PERP_ETH_USDT"
    assert canonical_woox_symbol("SPOT_BTC_USDT") == "SPOT_BTC_USDT"
    assert canonical_woox_symbol("BTC/USDT") == "SPOT_BTC_USDT"
    assert canonical_woox_symbol("BTC/USDT:USDT") == "PERP_BTC_USDT"
    assert canonical_woox_symbol("ETH-PERP") == "PERP_ETH_USDT"
    assert canonical_woox_symbol("BTC-USDT") == "SPOT_BTC_USDT"


def test_canonical_symbol_uses_exchange_rules() -> None:
    assert canonical_symbol("woo", "BTC/USDT") == "SPOT_BTC_USDT"
    assert canonical_symbol("woox", "ETH-PERP") == "PERP_ETH_USDT"
