"""MEXC symbol metadata parsing, precision and caching (offline)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.mexc.symbols import SymbolRegistry, SymbolValidationError

SAMPLE = {
    "symbol": "BTCUSDT",
    "status": "1",
    "baseAsset": "BTC",
    "baseAssetPrecision": 6,
    "quoteAsset": "USDT",
    "quoteAssetPrecision": 2,
    "quotePrecision": 8,
    "baseSizePrecision": "0.000001",
    "quoteAmountPrecision": "1",
    "quoteAmountPrecisionMarket": "5",
    "maxQuoteAmount": "2000000",
    "maxQuoteAmountMarket": "100000",
    "orderTypes": ["LIMIT", "MARKET", "LIMIT_MAKER"],
    "isSpotTradingAllowed": True,
    "permissions": ["SPOT"],
    "tradeSideType": 1,
}


class FakeExchange:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    def exchange_info(self, symbol=None, symbols=None):
        self.calls += 1
        return {"symbols": self.rows}


def test_symbol_meta_parses_official_fields_and_quantizes_down():
    meta = SymbolRegistry.parse_one(SAMPLE)
    assert meta.tradable
    assert meta.base_asset == "BTC"
    assert meta.quote_asset == "USDT"
    assert meta.min_qty == Decimal("0.000001")
    assert meta.min_quote == Decimal("1")
    assert meta.quantize_quantity(Decimal("0.123456789")) == Decimal("0.123456")
    assert meta.quantize_price(Decimal("100.129")) == Decimal("100.12")


def test_symbol_validation_rejects_minimum_and_policy_violations():
    meta = SymbolRegistry.parse_one(SAMPLE)
    with pytest.raises(SymbolValidationError):
        meta.validate_order(
            side="BUY", order_type="LIMIT", quantity=Decimal("0.0000001"),
            quote_amount=Decimal("1"), price=Decimal("100"),
        )
    with pytest.raises(SymbolValidationError):
        meta.validate_order(
            side="BUY", order_type="LIMIT", quantity=Decimal("0.1"),
            quote_amount=Decimal("0.5"), price=Decimal("100"),
        )
    with pytest.raises(SymbolValidationError):
        meta.validate_order(
            side="BUY", order_type="MARKET", quantity=None,
            quote_amount=Decimal("1"), price=None,
        )


def test_non_spot_or_paused_symbol_is_not_tradable():
    meta = SymbolRegistry.parse_one({**SAMPLE, "permissions": ["FUTURES"]})
    assert not meta.tradable
    paused = SymbolRegistry.parse_one({**SAMPLE, "status": "2"})
    assert not paused.tradable


def test_registry_caches_exchange_info_within_ttl():
    exchange = FakeExchange([SAMPLE])
    now = [0.0]
    registry = SymbolRegistry(exchange, ttl_s=60, clock=lambda: now[0])
    first = registry.get("btcusdt")
    second = registry.get("BTCUSDT")
    assert first is second
    assert exchange.calls == 1
    now[0] = 61
    registry.get("BTCUSDT")
    assert exchange.calls == 2


def test_registry_unknown_symbol_is_rejected():
    registry = SymbolRegistry(FakeExchange([SAMPLE]))
    with pytest.raises(SymbolValidationError):
        registry.get("NOPEUSDT")


def test_missing_minimum_constraints_fail_closed():
    for field in ("baseSizePrecision", "quoteAmountPrecision", "quoteAmountPrecisionMarket"):
        row = {k: v for k, v in SAMPLE.items() if k != field}
        with pytest.raises(SymbolValidationError):
            SymbolRegistry.parse_one(row)
