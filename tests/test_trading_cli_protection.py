"""CLI-backed live execution: TP/SL payload, uncertainty and suppression.

Everything is offline: the exchange and the CLI are fakes and no subprocess is
started.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem
from tg_signal_ingest.trading import TradingEngine
from tg_signal_ingest.trading.trade_manager import TradeManager

NOW = datetime(2026, 10, 25, 12, 0, tzinfo=UTC)
LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
ALERT = "BUY BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120"


def _item(mid: int) -> QueueItem:
    return QueueItem(
        queue_id=0,
        channel_id=100,
        message_id=mid,
        item_type="message",
        status="processing",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )


def _store_message(repo, mid: int) -> dict:
    class Msg:
        id = mid
        message = ALERT
        date = NOW - timedelta(seconds=10)
        edit_date = None
        media = None

        def to_dict(self):
            return {}

    repo.upsert_message(message_to_record(100, Msg(), "live"))
    record = repo.get_message_raw(100, mid)
    assert record is not None
    return record


class FakeMeta:
    base_asset = "BTC"
    quote_asset = "USDT"

    def quantize_quantity(self, quantity):
        return quantity

    def quantize_price(self, price):
        return price

    def validate_order(self, **kwargs):
        return None


class FakeRegistry:
    def get(self, symbol):
        return FakeMeta()


class FakeCliClient:
    supports_attached_protection = True

    def __init__(self):
        self.place_kwargs = None
        self.place_calls = 0
        self.verify_calls = 0
        self.verify_result = {
            "verified": True,
            "take_profit": "110",
            "stop_loss": "95",
        }
        self.place_error = None
        self.symbols = FakeRegistry()

    def ticker_price(self, symbol=None):
        return {"symbol": symbol, "price": "100"}

    def place_order(self, **kwargs):
        self.place_calls += 1
        self.place_kwargs = kwargs
        if self.place_error is not None:
            raise self.place_error
        return {"orderId": "ex-1", "status": "NEW", "executedQty": "0", "cumulativeQuoteQty": "0"}

    def open_orders(self, symbol=None):
        return []

    def account(self):
        return {
            "canTrade": True,
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "BTC", "free": "1000", "locked": "0", "available": "1000"},
                {"asset": "USDT", "free": "1000", "locked": "0", "available": "1000"},
            ],
        }

    def query_order(self, **kwargs):
        return {"orderId": "ex-1", "status": "NEW", "executedQty": "0", "cumulativeQuoteQty": "0"}

    def verify_protection(self, **kwargs):
        self.verify_calls += 1
        return dict(self.verify_result)

    def cancel_order(self, **kwargs):
        return {"status": "CANCELED"}


def _engine(cfg, repo, client):
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.order_quote_amount_source = "test"
    engine = TradingEngine(cfg, repo, client=client, registry=FakeRegistry(), now=lambda: NOW)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    return engine


def test_live_limit_buy_attaches_tp_sl_and_verifies(cfg, repo):
    client = FakeCliClient()
    engine = _engine(cfg, repo, client)
    intent = engine.process_item(_item(1), _store_message(repo, 1))
    assert intent is not None
    assert client.place_calls == 1
    assert client.place_kwargs["take_profit_price"] == Decimal("110")
    assert client.place_kwargs["stop_loss_price"] == Decimal("95")
    assert client.place_kwargs["client_order_id"] == intent["client_order_id"]

    protection = engine.store.get_protection(intent["intent_id"])
    assert protection is not None
    assert protection["status"] == "active"
    assert protection["verified_at"] is not None
    assert protection["take_profit_price"] == "110"
    assert protection["stop_loss_price"] == "95"


def test_verification_does_not_claim_protection_without_echo(cfg, repo):
    client = FakeCliClient()
    client.verify_result = {"verified": False, "take_profit": None, "stop_loss": None}
    engine = _engine(cfg, repo, client)
    intent = engine.process_item(_item(1), _store_message(repo, 1))
    assert intent is not None
    protection = engine.store.get_protection(intent["intent_id"])
    assert protection is not None
    assert protection["status"] == "unverified"
    assert "did not echo" in (protection["last_error"] or "")


def test_duplicate_placement_is_unknown_and_never_retried(cfg, repo):
    from tg_signal_ingest.mexc.errors import MexcDuplicateOrder

    client = FakeCliClient()
    client.place_error = MexcDuplicateOrder("duplicate clientOrderId")
    engine = _engine(cfg, repo, client)
    intent = engine.process_item(_item(1), _store_message(repo, 1))
    assert intent is not None
    assert client.place_calls == 1
    assert intent["state"] == "unknown_requires_reconciliation"
    protection = engine.store.get_protection(intent["intent_id"])
    assert protection is not None
    assert protection["status"] == "unknown_requires_reconciliation"


def test_market_entry_has_no_attached_protection_prices(cfg, repo):
    client = FakeCliClient()
    engine = _engine(cfg, repo, client)
    tp, sl = engine._protection_prices(
        {
            "intent_id": "intent-x",
            "signal_id": "100:1:0",
            "side": "buy",
            "order_type": "market",
            "limit_price": None,
            "client_order_id": "tsi-x",
            "symbol": "BTCUSDT",
        }
    )
    assert tp is None and sl is None


def test_standalone_tp_path_is_disabled():
    """F-01: exits are OCO-only; the old TP path must never submit an order."""

    class Store:
        def __init__(self):
            self.states: list[tuple[str, str]] = []

        def update_trade_state(self, trade_id, state, *, note=""):
            self.states.append((trade_id, state))

    tm = object.__new__(TradeManager)
    tm.store = Store()
    notified: list[str] = []
    tm._notify_trade = lambda trade, key, text, **kw: notified.append(key)
    trade = {"trade_id": "t1", "symbol": "BTCUSDT", "tp1_price": "110", "sl_price": "95"}

    tm._enter_tp1(trade)
    tm._enter_tp2(trade)

    assert tm.store.states == [("t1", "needs_reconciliation"), ("t1", "needs_reconciliation")]
    assert notified == ["LEGACY_EXIT_DISABLED", "LEGACY_EXIT_DISABLED"]


def test_submit_tp_order_refuses_to_submit():
    tm = object.__new__(TradeManager)
    with pytest.raises(Exception, match="OCO"):
        tm._submit_tp_order(
            {"trade_id": "t1", "symbol": "BTCUSDT"},
            "tp1",
            {"trade_order_id": "x", "client_order_id": "c"},
            Decimal("1"),
            Decimal("1"),
        )
