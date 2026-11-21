"""Futures/leveraged signal text is ingested but executes only Spot BUY."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.channel_template import channel_signal
from tests.test_trading_engine import FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.trading.parser import SignalParser


class RecordingMexc(FakeMexc):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.place_kwargs: list[dict] = []

    def place_order(self, **kwargs):
        self.place_kwargs.append(dict(kwargs))
        return super().place_order(**kwargs)


@pytest.mark.parametrize("symbol", ["BTCUSDT", "ETHUSDT", "1INCHUSDT"])
def test_valid_futures_like_long_is_parsed_and_executed_as_spot_buy(cfg, repo, symbol):
    text = channel_signal(symbol=symbol)
    parsed = SignalParser().parse(text)
    assert parsed.kind == "signal"
    assert parsed.action == "buy"
    assert parsed.symbol == symbol

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
    parsed = SignalParser().parse(channel_signal(symbol="XYZUSDT"))
    assert parsed.kind == "signal"
    assert parsed.symbol == "XYZUSDT"
    assert parsed.action == "buy"


def test_futures_short_is_ingested_but_never_executed(cfg, repo):
    client = RecordingMexc()
    engine = _engine(cfg, repo, client)
    text = channel_signal(direction="SHORT", stop="105", tp1="90", tp2="80.0000000000000000000001")
    parsed = SignalParser().parse(text)
    assert parsed.kind == "signal"
    assert parsed.action == "sell"

    result = engine.process_item(_item(100, 502), _store_message(repo, 502, text))
    assert result is None
    assert client.place_kwargs == []
    signal = engine.store.get_signal("100:502:0")
    assert signal is not None and "SHORT/SELL" in signal["rejection_reason"]
    assert signal["signal_id"] == "100:502:0"  # not the channel-printed ID
    assert (signal["parser_version"], signal["signal_kind"], signal["action"]) == (3, "alert", "sell")
    assert signal["target_prices"] == "90,80.0000000000000000000001,130,140"


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
    text = channel_signal() + "\n💰Deposit 50%\n💠MAX LEV Suggested = 25x"
    intent = engine.process_item(_item(100, 504), _store_message(repo, 504, text))
    assert intent is not None
    assert Decimal(str(intent["quote_amount"])) == Decimal("10.0")
    assert len(client.place_kwargs) == 1
    assert client.place_kwargs[0]["order_type"] == "LIMIT"
