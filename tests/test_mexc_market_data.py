"""Decimal-safe order-book snapshot parsing and symbol price filters.

Docs checked 2026-10-23:
- https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/order-book
- https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/exchange-information
  (PERCENT_PRICE_BY_SIDE filter semantics)
No network: every response is the documented JSON sample.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.mexc import DepthValidationError, parse_depth
from tg_signal_ingest.mexc.symbols import SymbolRegistry, SymbolValidationError

DOC_DEPTH = {
    "lastUpdateId": 1112416,
    "bids": [["15.00000", "49999.00000"], ["14.999", "1"]],
    "asks": [["15.001", "2.5"], ["15.02", "10"]],
}


def test_parse_documented_depth_sample_produces_decimals():
    snap = parse_depth(DOC_DEPTH, symbol="mxusdt")
    assert snap.symbol == "MXUSDT"
    assert snap.last_update_id == 1112416
    assert snap.best_bid == Decimal("15.00000")
    assert snap.best_ask == Decimal("15.001")
    assert snap.spread == Decimal("0.001")
    assert not snap.crossed()


def test_depth_validation_rejects_documented_shapes_violations():
    with pytest.raises(DepthValidationError):
        parse_depth({"bids": [], "asks": []})  # no lastUpdateId
    with pytest.raises(DepthValidationError):
        parse_depth({"lastUpdateId": 1, "bids": None, "asks": []})
    with pytest.raises(DepthValidationError):
        parse_depth({"lastUpdateId": 1, "bids": [["1", "2"]], "asks": [["x", "y"]]})
    with pytest.raises(DepthValidationError):
        parse_depth({"lastUpdateId": 1, "bids": [["1", "2"]], "asks": [["0.5", "2"]]})


def test_depth_never_touches_binary_floats():
    snap = parse_depth(DOC_DEPTH)
    for level in (*snap.bids, *snap.asks):
        for value in level:
            assert isinstance(value, Decimal)


# ------------------------------------------------- PERCENT_PRICE_BY_SIDE
SAMPLE_WITH_FILTER = {
    "symbol": "BTCUSDT",
    "status": "1",
    "baseAsset": "BTC",
    "baseAssetPrecision": 6,
    "quoteAsset": "USDT",
    "quoteAssetPrecision": 2,
    "baseSizePrecision": "0.000001",
    "quoteAmountPrecision": "1",
    "quoteAmountPrecisionMarket": "5",
    "maxQuoteAmount": "2000000",
    "maxQuoteAmountMarket": "100000",
    "orderTypes": ["LIMIT", "MARKET"],
    "isSpotTradingAllowed": True,
    "permissions": ["SPOT"],
    "tradeSideType": 1,
    "filters": [
        {"filterType": "PERCENT_PRICE_BY_SIDE", "bidMultiplierUp": "0.2", "askMultiplierDown": "0.2"}
    ],
}


def _meta():
    return SymbolRegistry.parse_one(SAMPLE_WITH_FILTER)


def test_percent_price_filters_are_parsed_from_exchange_info():
    meta = _meta()
    assert meta.bid_multiplier_up == Decimal("0.2")
    assert meta.ask_multiplier_down == Decimal("0.2")


def test_buy_price_above_documented_ceiling_fails_closed():
    meta = _meta()
    last = Decimal("100")
    with pytest.raises(SymbolValidationError):
        meta.validate_price_guards(side="BUY", price=Decimal("125"), reference_price=last)
    ok_price = (last * (Decimal(1) + Decimal("0.2"))).quantize(Decimal("0.01"))
    meta.validate_price_guards(side="BUY", price=Decimal("120"), reference_price=last)
    assert ok_price == Decimal("120.00")


def test_sell_price_below_documented_floor_fails_closed():
    meta = _meta()
    with pytest.raises(SymbolValidationError):
        meta.validate_price_guards(
            side="SELL", price=Decimal("79"), reference_price=Decimal("100")
        )
    meta.validate_price_guards(side="SELL", price=Decimal("80.5"), reference_price=Decimal("100"))


def test_unpublished_filters_do_not_reject_stale_references():
    row = {k: v for k, v in SAMPLE_WITH_FILTER.items() if k != "filters"}
    meta = SymbolRegistry.parse_one(row)
    assert meta.bid_multiplier_up is None
    # Without an exchange-published multiplier there is nothing to enforce;
    # silently rejecting on a local guess would fail OPEN to another session.
    meta.validate_price_guards(side="BUY", price=Decimal("9999"), reference_price=Decimal("1"))
    # Malformed known filter is an error, not silently ignored:
    broken = {**SAMPLE_WITH_FILTER, "filters": [{"filterType": "PERCENT_PRICE_BY_SIDE"}]}
    with pytest.raises(SymbolValidationError):
        SymbolRegistry.parse_one(broken)
