"""Futures/leveraged signal text is ingested but executes only Spot BUY."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.test_trading_engine import FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.trading.parser import SignalParser


class RecordingMexc(FakeMexc):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.place_kwargs: list[dict] = []

    def place_order(self, **kwargs):
        self.place_kwargs.append(dict(kwargs))
        return super().place_order(**kwargs)


@pytest.mark.parametrize(
    "text",
    [
        "FUTURES LONG BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120",
        "PERPETUAL LONG ETHUSDT.P\nENTRY: 2000\nSL: 1900\nTP1: 2100\nTP2: 2200",
        "BUY BTCUSDT 10x leverage\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120",
    ],
)
def test_valid_futures_like_long_is_parsed_and_executed_as_spot_buy(cfg, repo, text):
    parsed = SignalParser().parse(text)
    assert parsed.kind == "alert"
    assert parsed.action == "buy"
    assert parsed.symbol

    client = RecordingMexc(place_response={"orderId": "ex-futures", "status": "NEW"})
    engine = _engine(cfg, repo, client)
    intent = engine.process_item(_item(100, 501), _store_message(repo, 501, text))

    assert intent is not None
    assert intent["side"] == "buy"
    assert intent["order_type"] == "limit"
    assert len(client.place_kwargs) == 1
    payload = client.place_kwargs[0]
    assert payload["side"] == "BUY"
    assert payload["order_type"] == "LIMIT"
    assert payload["price"] is not None
    assert "quote_order_qty" not in payload


def test_perp_suffix_normalizes_to_spot_symbol():
    parsed = SignalParser().parse("TOOBIT:XYZUSDT.P(15)\nLONG\nENTRY: 1.5\nSL: 1.4\nTP1: 1.6\nTP2: 1.7")
    assert parsed.kind == "alert"
    assert parsed.symbol == "XYZUSDT"
    assert parsed.action == "buy"


def test_futures_short_is_ingested_but_never_executed(cfg, repo):
    client = RecordingMexc()
    engine = _engine(cfg, repo, client)
    text = "FUTURES SHORT BTCUSDT\nENTRY: 100\nSL: 105\nTP1: 90\nTP2: 80"
    parsed = SignalParser().parse(text)
    assert parsed.kind == "alert"
    assert parsed.action == "sell"

    result = engine.process_item(_item(100, 502), _store_message(repo, 502, text))
    assert result is None
    assert client.place_kwargs == []
    signal = engine.store.get_signal("100:502:0")
    assert signal is not None and "SHORT/SELL" in signal["rejection_reason"]


@pytest.mark.parametrize(
    "text",
    [
        "FUTURES LONG BTCUSDT\nSL: 95\nTP1: 110\nTP2: 120",
        "FUTURES LONG BTCUSDT\nENTRY: 100\nTP1: 110\nTP2: 120",
        "FUTURES LONG BTCUSDT\nENTRY: 100\nSL: 95\nTP2: 120",
        "FUTURES LONG BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110",
        "FUTURES LONG BTCUSDT\nENTRY: 100\nSL: 110\nTP1: 110\nTP2: 120",
    ],
)
def test_futures_signal_missing_or_invalid_required_fields_still_rejected(cfg, repo, text):
    client = RecordingMexc()
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 503), _store_message(repo, 503, text))
    assert result is None
    assert client.place_kwargs == []


def test_futures_long_uses_fixed_quote_not_leverage_or_deposit(cfg, repo):
    client = RecordingMexc(place_response={"orderId": "ex-fixed", "status": "NEW"})
    cfg.trading.order_quote_amount = 10.0
    engine = _engine(cfg, repo, client)
    text = "FUTURES LONG BTCUSDT 25x\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120\nDEPOSIT: 50%\nLEVERAGE: 25x"
    intent = engine.process_item(_item(100, 504), _store_message(repo, 504, text))
    assert intent is not None
    assert Decimal(str(intent["quote_amount"])) == Decimal("10.0")
    assert len(client.place_kwargs) == 1
    assert client.place_kwargs[0]["order_type"] == "LIMIT"
