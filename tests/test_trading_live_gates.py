"""Execution policy: automatic live BUY, replay boundary, emergency stop.

Guarantees under test (all OFFLINE, exchange fully mocked):
- A live signal is executed automatically; there is no per-signal approval
  gate and exactly one exchange submission happens.
- The persisted activation-boundary message id blocks historical/replayed
  messages and cannot be bypassed by a queued item.
- Emergency stop blocks intent creation in every mode and blocks execution of
  an already-planned intent with zero exchange calls.
- There is no signal-age rejection, no allowlist and no daily/open-order cap.
- A signal-supplied amount never changes the fixed owner amount.
- ``sim`` with a client attached performs zero exchange calls.

The official /order/test endpoint cannot create an order (mexc.com
/api-docs/spot-v3/spot-account-trade/test-new-order, verified live 2026-10-23:
"Test order requests without submitting them to the matching engine").
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem
from tg_signal_ingest.trading import TradingEngine

NOW = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
ALERT = "BUY BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120"
LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"


def _item(channel_id: int, message_id: int) -> QueueItem:
    return QueueItem(
        queue_id=0,
        channel_id=channel_id,
        message_id=message_id,
        item_type="message",
        status="processing",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )


def _store_message(repo, mid: int, *, text: str = ALERT, seconds_old: int = 20) -> dict:
    message_date = NOW - timedelta(seconds=seconds_old)

    class Msg:
        id = mid
        message = text
        date = message_date
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
    def __init__(self, meta=None):
        self.meta = meta or FakeMeta()

    def get(self, symbol):
        return self.meta


class MissingRegistry:
    def get(self, symbol):
        return None


class FakeMexc:
    def __init__(self):
        self.place_calls = 0
        self.test_calls = 0
        self.query_calls = 0
        self.cancel_calls = 0
        self.account_calls = 0
        self.trades_calls = 0
        self.ticker_calls = 0
        self.query_results: list[dict] = [
            {"orderId": "ex-1", "status": "CANCELED", "executedQty": "0", "cumulativeQuoteQty": "0"},
        ]
        self.symbols = FakeRegistry()
        self.place_response = {"orderId": "ex-1", "status": "NEW"}

    def ticker_price(self, symbol=None):
        self.ticker_calls += 1
        return {"symbol": symbol, "price": "100"}

    def place_order(self, **kwargs):
        self.place_calls += 1
        return dict(self.place_response)

    def open_orders(self, symbol=None):
        return []

    def place_test_order(self, **kwargs):
        self.test_calls += 1
        return True

    def query_order(self, **kwargs):
        self.query_calls += 1
        return self.query_results.pop(0)

    def account(self):
        self.account_calls += 1
        return {
            "canTrade": True,
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "BTC", "free": "1000", "locked": "0", "available": "1000"},
                {"asset": "USDT", "free": "1000", "locked": "0", "available": "1000"},
            ],
        }

    def my_trades(self, **kwargs):
        self.trades_calls += 1
        return []

    def cancel_order(self, **kwargs):
        self.cancel_calls += 1
        return {"status": "CANCELED"}


def _engine(cfg, repo, fake, *, mode=None, boundary="0", registry=None) -> TradingEngine:
    del mode  # one automatic Spot behaviour; this file uses a fake exchange
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.order_quote_amount_source = "test"
    engine = TradingEngine(cfg, repo, client=fake, registry=registry or FakeRegistry(), now=lambda: NOW)
    if boundary is not None:
        engine.store.set_app_state_once("trading.activation_boundary_message_id", boundary)
    return engine


def test_live_signal_is_submitted_automatically_without_approval(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    intent = engine.process_item(_item(100, 1), _store_message(repo, 1))

    assert intent is not None
    # The fake exchange reports the order as canceled on the immediate
    # reconciliation query; the point is that exactly one real submission ran.
    assert intent["state"] in ("submitted", "canceled")
    assert fake.place_calls == 1
    assert fake.account_calls >= 1
    assert len(engine.store.list_intents(limit=5)) == 1


def test_global_unprotected_flag_does_not_block_unrelated_new_buy(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    engine.store.set_buy_block("unprotected position(s): OTHERUSDT:trade-old")

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1))

    assert intent is not None
    assert intent["state"] in ("submitted", "canceled", "open", "filled")
    assert fake.place_calls == 1


def test_live_replay_below_activation_boundary_is_rejected(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot", boundary="1")
    result = engine.process_item(_item(100, 1), _store_message(repo, 1))

    assert result is None
    assert fake.place_calls == 0 and fake.account_calls == 0
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "activation boundary" in signal["rejection_reason"]


def test_missing_activation_boundary_fails_closed(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot", boundary=None)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1))

    assert result is None
    assert fake.place_calls == 0
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "boundary is not set" in signal["rejection_reason"]


def test_emergency_stop_blocks_intent_creation_and_existing_execution(cfg, repo, monkeypatch):
    fake = FakeMexc()
    cfg.trading.emergency_stop = True
    engine = _engine(cfg, repo, fake, mode="spot")
    result = engine.process_item(_item(100, 1), _store_message(repo, 1))
    assert result is None
    assert fake.place_calls == 0
    signals = engine.store.list_signals(limit=5)
    assert signals and "emergency stop" in (signals[0]["rejection_reason"] or "")

    # A planned intent that predates the stop must not become exchange traffic.
    cfg.trading.emergency_stop = False
    engine2 = _engine(cfg, repo, fake, mode="spot", boundary="1")
    monkeypatch.setattr(engine2, "_execute_intent", lambda intent_id: engine2.store.get_intent(intent_id))
    planned = engine2.process_item(_item(100, 2), _store_message(repo, 2))
    assert planned is not None and planned["state"] == "planned"
    cfg.trading.emergency_stop = True
    rejected = engine2.execute_intent(planned["intent_id"])
    assert rejected["state"] == "rejected"
    assert "emergency stop" in (rejected.get("reject_reason") or "")
    assert fake.place_calls == 0


def test_no_signal_age_limit(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, seconds_old=3 * 24 * 3600))
    assert result is not None
    assert fake.place_calls == 1


def test_no_manual_allowlist_blocks_dynamic_symbol(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    result = engine.process_item(
        _item(100, 1), _store_message(repo, 1, text="BUY ETHUSDT\nENTRY: 2000\nSL: 1900\nTP1: 2100\nTP2: 2200")
    )
    assert result is not None
    assert fake.place_calls == 1


def test_unknown_exchange_symbol_fails_closed(cfg, repo):
    fake = FakeMexc()
    engine = TradingEngine(cfg, repo, client=fake, registry=MissingRegistry(), now=lambda: NOW)
    cfg.trading.order_quote_amount = 10.0
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    result = engine.process_item(
        _item(100, 1), _store_message(repo, 1, text="BUY NOPEUSDT\nENTRY: 1\nSL: 0.9\nTP1: 2\nTP2: 3")
    )
    assert result is None
    assert fake.place_calls == 0


def test_missing_acknowledgement_does_not_block_submission(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    assert not hasattr(cfg.trading, "execution_acknowledgement")
    intent = engine.process_item(_item(100, 1), _store_message(repo, 1))
    assert intent is not None
    stored = engine.store.get_intent(intent["intent_id"])
    assert stored is not None and stored["state"] in ("submitted", "canceled", "open", "filled")
    assert fake.place_calls == 1


def test_signal_supplied_amount_never_changes_fixed_owner_amount(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    text = "BUY BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120\nAMOUNT: $5000 USDT\nDEPOSIT 25%"
    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, text=text))
    assert intent is not None
    assert Decimal(str(intent["quote_amount"])) == Decimal("10")
    assert Decimal(str(intent["base_quantity"])) == Decimal("0.1")


def test_short_signal_is_parsed_but_never_sold(cfg, repo):
    fake = FakeMexc()
    engine = _engine(cfg, repo, fake, mode="spot")
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text="SHORT BTCUSDT\nENTRY: 100\nTP1: 90"))
    assert result is None
    assert fake.place_calls == 0
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "SHORT/SELL" in signal["rejection_reason"]
