"""Durable intent lifecycle: idempotency, uncertainty, partial fills, restart."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tg_signal_ingest.mexc.errors import MexcNetworkError, MexcOrderNotFound, MexcRateLimitError
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem
from tg_signal_ingest.trading import SignalParser, TradingEngine, TradingStore

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
MSG_DATE = NOW - timedelta(seconds=20)


class Msg:
    def __init__(self, mid: int, text: str, edit_date=None):
        self.id = mid
        self.message = text
        self.date = MSG_DATE
        self.edit_date = edit_date
        self.media = None

    def to_dict(self):
        return {}


def _item(channel_id: int, message_id: int, item_type: str = "message") -> QueueItem:
    return QueueItem(
        queue_id=0, channel_id=channel_id, message_id=message_id, item_type=item_type,
        status="processing", targets_version=0, attempts=0, available_at="", created_at="",
    )


def _store_message(repo, mid: int, text: str, edit_date=None, source: str = "live") -> dict:
    repo.upsert_message(message_to_record(100, Msg(mid, text, edit_date), source))
    record = repo.get_message_raw(100, mid)
    assert record is not None
    return record


def _engine(cfg, repo, client=None, mode="sim") -> TradingEngine:
    cfg.trading.mode = mode
    cfg.trading.live_enabled_token = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.order_quote_amount_source = "test"
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW)
    if mode in ("testonly", "live"):
        # General lifecycle tests use a zero boundary so every stored message
        # is considered new; dedicated replay tests set a real boundary.
        engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    return engine


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


class FakeMexc:
    def __init__(
        self,
        *,
        price="100",
        place_response=None,
        place_error=None,
        query_results=None,
        query_error=None,
        trades=None,
        trades_error=None,
        account_response=None,
        account_error=None,
    ):
        self.price = price
        self.place_response = place_response or {"orderId": "ex-1", "status": "NEW"}
        self.place_error = place_error
        self.query_results = list(query_results or [])
        self.query_error = query_error
        self.trades = trades or []
        self.trades_error = trades_error
        self.account_response = account_response
        self.account_error = account_error
        self.symbols = FakeRegistry()
        self.place_calls = 0
        self.test_calls = 0
        self.query_calls = 0
        self.cancel_calls = 0
        self.account_calls = 0
        self.trades_calls = 0
        self.open_orders_calls = 0

    def ticker_price(self, symbol=None):
        return {"symbol": symbol, "price": self.price}

    def place_order(self, **kwargs):
        self.place_calls += 1
        if self.place_error:
            raise self.place_error
        return dict(self.place_response)

    def place_test_order(self, **kwargs):
        self.test_calls += 1
        return True

    def query_order(self, **kwargs):
        self.query_calls += 1
        if self.query_error:
            raise self.query_error
        if not self.query_results:
            raise AssertionError("unexpected query_order call")
        return self.query_results.pop(0)

    def account(self):
        self.account_calls += 1
        if self.account_error:
            raise self.account_error
        if self.account_response is not None:
            return dict(self.account_response)
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
        if self.trades_error:
            raise self.trades_error
        return list(self.trades)

    def cancel_order(self, **kwargs):
        self.cancel_calls += 1
        return {"status": "CANCELED"}

    def open_orders(self, symbol=None):
        self.open_orders_calls += 1
        return [{"orderId": "external-1", "symbol": "GOLD(XAUT)USDT", "status": "NEW"}]


ALERT = "BUY BTCUSDT\nENTRY: 100\nTP1: 110\nSL: 95"


def test_sim_alert_creates_one_filled_intent(cfg, repo):
    engine = _engine(cfg, repo)
    record = _store_message(repo, 1, ALERT)

    intent = engine.process_item(_item(100, 1), record)

    assert intent is not None
    assert intent["state"] == "filled"
    assert intent["exchange_status"] == "SIMULATED_FILLED"
    assert intent["base_quantity"] == "0.1"
    assert Decimal(intent["cumulative_quote"]) == Decimal("10")
    assert engine.client is None


def test_edit_reuses_message_identity_and_never_places_second_order(cfg, repo):
    engine = _engine(cfg, repo)
    record = _store_message(repo, 2, ALERT)
    first = engine.process_item(_item(100, 2), record)
    assert first is not None

    edited = _store_message(repo, 2, ALERT + "\nTP2: 120", edit_date=NOW - timedelta(seconds=5))
    assert int(edited["processing_version"]) == 1
    second = engine.process_item(_item(100, 2, "edit"), edited)

    assert second is not None
    assert second["intent_id"] == first["intent_id"]
    assert engine.store.list_intents(limit=10).__len__() == 1
    statuses = {row["signal_id"]: row["status"] for row in engine.store.list_signals(limit=10)}
    assert statuses["100:2:1"] == "superseded"


def test_repeated_identical_content_on_a_new_message_is_a_new_signal(cfg, repo):
    """Message identity - not text content - defines duplicate delivery."""

    engine = _engine(cfg, repo)
    first = engine.process_item(_item(100, 3), _store_message(repo, 3, ALERT))
    second = engine.process_item(_item(100, 4), _store_message(repo, 4, ALERT))

    assert first is not None and second is not None
    assert first["intent_id"] != second["intent_id"]
    assert len(engine.store.list_intents(limit=10)) == 2


def test_alert_without_stop_loss_is_accepted(cfg, repo):
    """A signal is not rejected merely because it omits a stop loss."""

    engine = _engine(cfg, repo)
    record = _store_message(repo, 5, "BUY BTCUSDT\nENTRY 100\nTP1 110")
    intent = engine.process_item(_item(100, 5), record)
    assert intent is not None
    assert intent["state"] == "filled"
    assert Decimal(intent["quote_amount"]) == Decimal("10")


def test_no_arbitrary_daily_or_open_order_caps(cfg, repo):
    engine = _engine(cfg, repo)
    for mid in (6, 7, 8):
        intent = engine.process_item(
            _item(100, mid),
            _store_message(repo, mid, f"BUY BTCUSDT\nENTRY {100 + mid}\nTP1 120"),
        )
        assert intent is not None
    assert len(engine.store.list_intents(limit=10)) == 3


def test_network_uncertainty_never_retries_and_reconciles_to_filled(cfg, repo):
    client = FakeMexc(
        place_error=MexcNetworkError("timeout after send"),
        trades=[{"id": "t-net", "price": "100", "qty": "0.1", "quoteQty": "10"}],
    )
    engine = _engine(cfg, repo, client, mode="live")
    record = _store_message(repo, 8, ALERT)

    intent = engine.process_item(_item(100, 8), record)
    assert intent is not None
    assert intent["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1

    edited = _store_message(repo, 8, ALERT + "\nTP2: 120", edit_date=NOW - timedelta(seconds=5))
    again = engine.process_item(_item(100, 8, "edit"), edited)
    assert again is not None
    assert again["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1  # no blind retry

    client.query_error = MexcOrderNotFound("no order")
    stats = engine.reconcile_pending()
    assert stats.not_found == 1
    still = engine.store.get_intent(intent["intent_id"])
    assert still is not None and still["state"] == "unknown_requires_reconciliation"

    client.query_error = None
    client.query_results.append({
        "orderId": "ex-1", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0.1",
        "cumulativeQuoteQty": "10", "status": "FILLED", "time": 1,
    })
    stats = engine.reconcile_pending()
    assert stats.resolved == 1
    final = engine.store.get_intent(intent["intent_id"])
    assert final is not None
    assert final["state"] == "filled"
    assert final["executed_qty"] == "0.1"


def test_partial_fill_is_preserved_then_canceled(cfg, repo):
    partial = {
        "orderId": "ex-2", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0.04",
        "cumulativeQuoteQty": "4", "status": "PARTIALLY_FILLED", "time": 1,
    }
    canceled = {**partial, "status": "CANCELED"}
    client = FakeMexc(
        place_response={"orderId": "ex-2", "status": "NEW"},
        query_results=[partial],
        trades=[{
            "id": "t1", "price": "100", "qty": "0.04", "quoteQty": "4", "orderId": "ex-2",
            "commission": "0.004", "commissionAsset": "USDT", "time": 123,
            "isBuyer": True, "isMaker": False,
        }],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 9), _store_message(repo, 9, ALERT))

    assert intent is not None
    assert intent["state"] == "open"
    assert intent["executed_qty"] == "0.04"
    assert engine.store.fills_count() == 1

    client.query_results.append(canceled)
    engine.reconcile_pending()
    final = engine.store.get_intent(intent["intent_id"])
    assert final is not None
    assert final["state"] == "canceled"
    assert final["executed_qty"] == "0.04"
    assert final["cumulative_quote"] == "4"
    assert int(final["fill_sync_required"]) == 0

    fills = engine.store.fills_for_order("ex-2")
    assert len(fills) == 1
    assert fills[0]["exchange_trade_id"] == "t1"
    assert fills[0]["fee_asset"] == "USDT"
    assert fills[0]["fee_amount"] == "0.004"
    assert fills[0]["quantity"] == "0.04"
    assert fills[0]["trade_time_ms"] == 123

    # Repeated reconciliation is idempotent and already-complete orders drop
    # out of the pending set.
    again = engine.reconcile_pending()
    assert again.checked == 0
    assert engine.store.fills_count() == 1


def test_restart_reconciles_intent_left_submitting(cfg, repo):
    client = FakeMexc(query_results=[{
        "orderId": "ex-3", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0", "cumulativeQuoteQty": "0",
        "status": "NEW", "time": 1,
    }])
    engine = _engine(cfg, repo, client, mode="live")
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=10, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed, signal_id=signal_id, channel_id=100, message_id=10, edit_version=0,
        idempotency_key=TradingEngine.idempotency_key(100, 10, parsed),
        client_order_id="tsirestart", mode="live", order_type="limit",
        quote_amount=Decimal("10"), base_quantity=Decimal("0.1"), limit_price=Decimal("100"),
    )
    store.transition_intent(intent["intent_id"], "submitting", note="process crashed after submit")

    stats = engine.reconcile_pending()

    assert stats.checked == 1
    resolved = store.get_intent(intent["intent_id"])
    assert resolved is not None and resolved["state"] == "open"


def test_local_filled_is_not_downgraded_by_exchange_open():
    state, outcome, note = TradingEngine._map_exchange_state("filled", "NEW")
    assert state == "filled"
    assert outcome == "discrepancy"
    assert "refusing to un-fill" in note


def test_testonly_uses_official_test_endpoint_not_real_order(cfg, repo):
    class NoRealOrders(FakeMexc):
        def place_order(self, **kwargs):
            raise AssertionError("real place_order must never be called in testonly mode")

    client = NoRealOrders()
    engine = _engine(cfg, repo, client, mode="testonly")
    intent = engine.process_item(_item(100, 12), _store_message(repo, 12, ALERT))

    assert intent is not None
    assert client.test_calls == 1
    assert intent["state"] == "canceled_local"
    assert intent["exchange_status"] == "TEST_ONLY_ACCEPTED"
    assert intent["terminal_at"] is not None


def test_database_failure_after_submit_does_not_resubmit(cfg, repo):
    import sqlite3

    client = FakeMexc(
        place_response={"orderId": "ex-5", "status": "NEW"},
        query_results=[{
            "orderId": "ex-5", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
            "price": "100", "origQty": "0.1", "executedQty": "0", "cumulativeQuoteQty": "0",
            "status": "NEW", "time": 1,
        }],
    )
    engine = _engine(cfg, repo, client, mode="live")
    record = _store_message(repo, 13, ALERT)
    original = engine.store.upsert_exchange_order

    def failing_upsert(**kwargs):
        raise sqlite3.OperationalError("database is locked")

    engine.store.upsert_exchange_order = failing_upsert  # type: ignore[method-assign]
    try:
        engine.process_item(_item(100, 13), record)
    except sqlite3.OperationalError:
        pass
    else:  # pragma: no cover - the failure must surface to the durable queue
        raise AssertionError("expected the database failure to propagate")

    assert client.place_calls == 1
    engine.store.upsert_exchange_order = original  # type: ignore[method-assign]

    # Queue retry sees the existing intent in 'submitting' and must not submit again.
    retried = engine.process_item(_item(100, 13), record)
    assert retried is not None
    assert retried["state"] == "submitting"
    assert client.place_calls == 1

    engine.reconcile_pending()
    resolved = engine.store.get_intent(retried["intent_id"])
    assert resolved is not None and resolved["state"] == "open"


def test_edit_changing_action_cannot_create_second_order(cfg, repo):
    engine = _engine(cfg, repo)
    first = engine.process_item(_item(100, 20), _store_message(repo, 20, ALERT))
    assert first is not None

    edited = _store_message(
        repo, 20, "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105",
        edit_date=NOW - timedelta(seconds=5),
    )
    second = engine.process_item(_item(100, 20, "edit"), edited)

    assert second is not None and second["intent_id"] == first["intent_id"]
    assert len(engine.store.list_intents(limit=10)) == 1
    signal = engine.store.get_signal("100:20:1")
    assert signal is not None and signal["status"] == "superseded"
    assert "different action/symbol" in signal["rejection_reason"]


def test_cancel_after_edit_still_cancels_superseded_signal_intent(cfg, repo):
    open_order = {
        "orderId": "ex-edit", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    canceled = {**open_order, "status": "CANCELED"}
    client = FakeMexc(
        place_response={"orderId": "ex-edit", "status": "NEW"},
        query_results=[open_order, canceled],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 21), _store_message(repo, 21, ALERT))
    assert intent is not None and intent["state"] == "open"

    edited = _store_message(
        repo, 21, ALERT + "\nTP2: 120", edit_date=NOW - timedelta(seconds=5),
    )
    engine.process_item(_item(100, 21, "edit"), edited)
    old_signal = engine.store.get_signal("100:21:0")
    assert old_signal is not None and old_signal["status"] == "superseded"

    engine.process_item(
        _item(100, 22), _store_message(repo, 22, "CANCEL BTCUSDT")
    )
    final = engine.store.get_intent(intent["intent_id"])
    assert client.cancel_calls == 1
    assert final is not None and final["state"] == "canceled"


def test_delete_after_edit_still_cancels_superseded_signal_intent(cfg, repo):
    open_order = {
        "orderId": "ex-del", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    canceled = {**open_order, "status": "CANCELED"}
    client = FakeMexc(
        place_response={"orderId": "ex-del", "status": "NEW"},
        query_results=[open_order, canceled],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 23), _store_message(repo, 23, ALERT))
    assert intent is not None and intent["state"] == "open"

    edited = _store_message(
        repo, 23, ALERT + "\nTP2: 120", edit_date=NOW - timedelta(seconds=5),
    )
    engine.process_item(_item(100, 23, "edit"), edited)
    engine.handle_delete(100, 23)

    final = engine.store.get_intent(intent["intent_id"])
    assert client.cancel_calls == 1
    assert final is not None and final["state"] == "canceled"


def test_rate_limited_submit_is_unknown_and_reconciled_not_resubmitted(cfg, repo):
    filled = {
        "orderId": "ex-rate", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0.1",
        "cumulativeQuoteQty": "10", "status": "FILLED", "time": 1,
    }
    client = FakeMexc(
        place_error=MexcRateLimitError("429 after send", retry_after_s=5),
        query_results=[filled],
        trades=[{"id": "t-rate", "price": "100", "qty": "0.1", "quoteQty": "10"}],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 24), _store_message(repo, 24, ALERT))

    assert intent is not None
    assert intent["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1

    engine.reconcile_pending()
    final = engine.store.get_intent(intent["intent_id"])
    assert final is not None and final["state"] == "filled"
    assert client.place_calls == 1


def test_later_cancel_processed_first_blocks_older_queued_alert(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)

    cancel = engine.process_item(
        _item(100, 31), _store_message(repo, 31, "CANCEL BTCUSDT")
    )
    assert cancel is None

    result = engine.process_item(_item(100, 30), _store_message(repo, 30, ALERT))
    assert result is None
    assert client.place_calls == 0
    assert engine.store.list_intents(limit=10) == []
    signal = engine.store.get_signal("100:30:0")
    assert signal is not None and signal["status"] == "rejected"
    assert "later cancel" in signal["rejection_reason"]


def test_not_found_for_open_intent_never_becomes_terminal_failed(cfg, repo):
    new_order = {
        "orderId": "ex-missing", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-missing", "status": "NEW"},
        query_results=[new_order],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 32), _store_message(repo, 32, ALERT))
    assert intent is not None and intent["state"] == "open"

    client.query_error = MexcOrderNotFound("gone")
    stats = engine.reconcile_pending()
    assert stats.not_found == 1
    final = engine.store.get_intent(intent["intent_id"])
    assert final is not None and final["state"] == "unknown_requires_reconciliation"


def test_old_cancel_processed_late_does_not_cancel_newer_order(cfg, repo):
    open_order = {
        "orderId": "ex-newer", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-newer", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client, mode="live")
    newer = engine.process_item(_item(100, 51), _store_message(repo, 51, ALERT))
    assert newer is not None and newer["state"] == "open"

    engine.process_item(_item(100, 50), _store_message(repo, 50, "CANCEL BTCUSDT"))
    final = engine.store.get_intent(newer["intent_id"])
    assert client.cancel_calls == 0
    assert final is not None and final["state"] == "open"
    newer_signal = engine.store.get_signal("100:51:0")
    assert newer_signal is not None and newer_signal["status"] == "planned"


def test_backfill_sourced_signal_is_not_executed_in_live_mode(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="live")
    record = _store_message(repo, 40, ALERT, source="backfill")
    assert engine.process_item(_item(100, 40), record) is None
    assert client.place_calls == 0
    assert engine.store.list_intents(limit=10) == []
    signal = engine.store.get_signal("100:40:0")
    assert signal is not None and signal["status"] == "rejected"
    assert "backfill" in signal["rejection_reason"]


def test_backfill_sourced_cancel_does_not_touch_live_orders(cfg, repo):
    open_order = {
        "orderId": "ex-live", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-live", "status": "NEW"},
        query_results=[open_order, {**open_order, "orderId": "ex-live-2"}],
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 52), _store_message(repo, 52, ALERT))
    assert intent is not None and intent["state"] == "open"

    cancel_record = _store_message(repo, 51, "CANCEL BTCUSDT", source="backfill")
    engine.process_item(_item(100, 51), cancel_record)
    final = engine.store.get_intent(intent["intent_id"])
    assert client.cancel_calls == 0
    assert final is not None and final["state"] == "open"

    # The ignored historical cancel must not block an older live message either.
    older = engine.process_item(
        _item(100, 49), _store_message(repo, 49, "BUY BTCUSDT ENTRY 100 TP 111 SL 96")
    )
    assert older is not None


def _account(usdt: str = "1000", btc: str = "1000") -> dict:
    return {
        "canTrade": True,
        "permissions": ["SPOT"],
        "balances": [
            {"asset": "BTC", "free": btc, "locked": "0", "available": btc},
            {"asset": "USDT", "free": usdt, "locked": "0", "available": usdt},
        ],
    }


def test_balance_preflight_rejects_insufficient_quote_before_submission(cfg, repo):
    client = FakeMexc(account_response=_account(usdt="5"))
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 70), _store_message(repo, 70, ALERT))

    assert intent is not None and intent["state"] == "rejected"
    assert client.place_calls == 0
    assert client.account_calls == 1
    assert "insufficient" in intent["reject_reason"]


def test_sell_signal_is_refused_before_any_balance_or_exchange_call(cfg, repo):
    client = FakeMexc(account_response=_account(btc="0.01"))
    engine = _engine(cfg, repo, client, mode="live")
    sell = "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105"
    intent = engine.process_item(_item(100, 71), _store_message(repo, 71, sell))

    assert intent is None
    assert client.place_calls == 0 and client.account_calls == 0
    signal = engine.store.get_signal("100:71:0")
    assert signal is not None and "SHORT/SELL" in signal["rejection_reason"]


def test_balance_api_failure_rejects_without_submitting(cfg, repo):
    from tg_signal_ingest.mexc.errors import MexcNetworkError as NetErr

    client = FakeMexc(account_error=NetErr("account endpoint unavailable"))
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 72), _store_message(repo, 72, ALERT))

    assert intent is not None and intent["state"] == "rejected"
    assert client.place_calls == 0
    assert "balance query failed" in intent["reject_reason"]


def test_balance_preflight_counts_competing_local_intents(cfg, repo):
    client = FakeMexc(account_response=_account(usdt="15"))
    engine = _engine(cfg, repo, client, mode="live")
    cfg.trading.remaining_balance_reserve_pct = 0.0

    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=73, edit_version=0)
    store.create_intent(
        signal=parsed, signal_id=signal_id, channel_id=100, message_id=73, edit_version=0,
        idempotency_key=TradingEngine.idempotency_key(100, 73, parsed),
        client_order_id="tsicompeting", mode="live", order_type="limit",
        quote_amount=Decimal("10"), base_quantity=Decimal("0.1"), limit_price=Decimal("100"),
    )

    second = engine.process_item(
        _item(100, 74), _store_message(repo, 74, "BUY BTCUSDT ENTRY 100 TP 111 SL 96")
    )
    assert second is not None and second["state"] == "rejected"
    assert client.place_calls == 0
    assert "outstanding" in second["reject_reason"] or "insufficient" in second["reject_reason"]


def test_balance_preflight_refetches_for_each_live_order(cfg, repo):
    new_a = {
        "orderId": "ex-a", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    new_b = {**new_a, "orderId": "ex-b"}
    client = FakeMexc(query_results=[new_a, new_b])
    engine = _engine(cfg, repo, client, mode="live")
    first = engine.process_item(_item(100, 75), _store_message(repo, 75, ALERT))
    second = engine.process_item(
        _item(100, 76), _store_message(repo, 76, "BUY BTCUSDT ENTRY 100 TP 111 SL 96")
    )
    assert first is not None and first["state"] == "open"
    assert second is not None and second["state"] == "open"
    assert client.account_calls == 2


def test_transient_my_trades_failure_keeps_fill_sync_pending_then_recovers(cfg, repo):
    new_order = {
        "orderId": "ex-fill", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    filled = {**new_order, "executedQty": "0.1", "cumulativeQuoteQty": "10", "status": "FILLED"}
    client = FakeMexc(
        place_response={"orderId": "ex-fill", "status": "NEW"},
        query_results=[new_order, filled],
        trades_error=MexcNetworkError("myTrades temporarily unavailable"),
    )
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 80), _store_message(repo, 80, ALERT))
    assert intent is not None and intent["state"] == "open"
    assert int(intent["fill_sync_required"]) == 0

    stats = engine.reconcile_pending()
    after_failure = engine.store.get_intent(intent["intent_id"])
    assert stats.checked == 1 and stats.ambiguous == 1
    assert after_failure is not None and after_failure["state"] == "filled"
    assert int(after_failure["fill_sync_required"]) == 1
    assert engine.store.fills_count() == 0
    assert client.place_calls == 1  # failure never resubmits

    client.trades_error = None
    client.trades = [{
        "id": "t-fill", "price": "100", "qty": "0.1", "quoteQty": "10",
        "commission": "0.01", "commissionAsset": "USDT", "time": 7,
        "isBuyer": True, "isMaker": False,
    }]
    recovered = engine.reconcile_pending()
    final = engine.store.get_intent(intent["intent_id"])
    assert recovered.checked == 1
    assert final is not None and int(final["fill_sync_required"]) == 0
    assert final["fill_synced_at"] is not None
    assert engine.store.fills_count() == 1
    assert client.place_calls == 1
    assert client.query_calls == 2  # open reconcile + the transition to filled


def test_restart_recovers_terminal_order_with_pending_fills(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=81, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed, signal_id=signal_id, channel_id=100, message_id=81, edit_version=0,
        idempotency_key=TradingEngine.idempotency_key(100, 81, parsed),
        client_order_id="tsirestart-fills", mode="live", order_type="limit",
        quote_amount=Decimal("10"), base_quantity=Decimal("0.1"), limit_price=Decimal("100"),
    )
    # Simulate a crash after the order became terminal but before fills were read.
    store.transition_intent(
        intent["intent_id"], "filled", note="crash boundary",
        exchange_order_id="ex-restart", exchange_status="FILLED",
        executed_qty=Decimal("0.1"), cumulative_quote=Decimal("10"),
        category="reconcile", fill_sync_required=True,
    )

    client = FakeMexc(trades=[{
        "id": "t-restart", "price": "100", "qty": "0.1", "quoteQty": "10",
        "commission": "0.01", "commissionAsset": "USDT", "time": 9,
    }])
    engine = _engine(cfg, repo, client, mode="live")  # new engine == restart
    stats = engine.reconcile_pending()

    recovered = engine.store.get_intent(intent["intent_id"])
    assert stats.checked == 1
    assert recovered is not None and recovered["state"] == "filled"
    assert int(recovered["fill_sync_required"]) == 0
    assert engine.store.fills_count() == 1
    assert client.query_calls == 0  # terminal fill sync never queries/submits


def test_missing_trade_id_is_deduplicated_by_synthetic_identity(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    trade = {"price": "100", "qty": "0.5", "quoteQty": "50", "commission": "0.1", "time": 11}
    assert store.add_fill(exchange_order_id="ex-synth", symbol="BTCUSDT", raw=trade) is True
    assert store.add_fill(exchange_order_id="ex-synth", symbol="BTCUSDT", raw=trade) is False
    assert store.fills_count() == 1


def test_concurrent_duplicate_delivery_creates_exactly_one_intent(cfg, repo):
    from concurrent.futures import ThreadPoolExecutor

    engine = _engine(cfg, repo, mode="sim")
    record = _store_message(repo, 90, ALERT)
    item = _item(100, 90)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = [pool.submit(engine.process_item, item, record) for _ in range(4)]

    intents = engine.store.list_intents(limit=20)
    assert len(intents) == 1
    # A loser of the reservation race returns the existing intent; it never
    # creates a second order and never re-executes it.
    assert sum(1 for r in results if r.result() is not None) >= 1


def test_preexisting_external_orders_are_never_adopted_cancelled_or_counted(cfg, repo):
    own_order = {
        "orderId": "ex-1", "symbol": "BTCUSDT", "side": "BUY",
        "type": "LIMIT", "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(query_results=[own_order])
    engine = _engine(cfg, repo, client, mode="live")
    intent = engine.process_item(_item(100, 91), _store_message(repo, 91, ALERT))

    assert intent is not None
    assert client.place_calls == 1
    assert client.open_orders_calls == 0
    assert client.cancel_calls == 0
    row = repo.db.conn.execute(
        "SELECT COUNT(*) AS n FROM exchange_orders WHERE exchange_order_id = 'external-1'"
    ).fetchone()
    assert int(row["n"]) == 0


def test_planned_intent_resumes_after_crash_without_duplicate_submission(cfg, repo):
    open_order = {
        "orderId": "ex-resume", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-resume", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client, mode="live")
    parsed = engine.parser.parse(ALERT)
    signal_id, _ = engine.store.record_signal(
        parsed, channel_id=100, message_id=1, edit_version=0
    )
    key = engine.idempotency_key(100, 1, parsed)
    intent, inserted = engine.store.create_intent(
        signal=parsed, signal_id=signal_id, channel_id=100, message_id=1,
        edit_version=0, idempotency_key=key, client_order_id="tsi" + key[:28],
        mode="live", order_type="limit", quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"), limit_price=Decimal("100"),
    )
    assert inserted is True and intent["state"] == "planned"

    # Simulate the process dying after planning but before submission: the
    # queue retry redelivers the same Telegram message.
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is not None and result["state"] == "open"
    assert client.place_calls == 1

    # A duplicate delivery after the claim must not place a second order.
    again = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert again is not None and again["state"] == "open"
    assert client.place_calls == 1


def test_claimed_submitting_intent_is_never_retried(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="live")
    parsed = engine.parser.parse(ALERT)
    signal_id, _ = engine.store.record_signal(
        parsed, channel_id=100, message_id=1, edit_version=0
    )
    key = engine.idempotency_key(100, 1, parsed)
    intent, _ = engine.store.create_intent(
        signal=parsed, signal_id=signal_id, channel_id=100, message_id=1,
        edit_version=0, idempotency_key=key, client_order_id="tsi" + key[:28],
        mode="live", order_type="limit", quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"), limit_price=Decimal("100"),
    )
    # Crash after the atomic claim: an exchange call may be in flight.
    engine.store.claim_intent_for_submission(intent["intent_id"], note="test claim")

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is not None and result["state"] == "submitting"
    assert client.place_calls == 0


def test_duplicate_delivery_of_open_intent_does_not_submit_again(cfg, repo):
    open_order = {
        "orderId": "ex-dup", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-dup", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client, mode="live")

    first = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert first is not None and first["state"] == "open"
    second = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert second is not None and second["intent_id"] == first["intent_id"]
    assert client.place_calls == 1
