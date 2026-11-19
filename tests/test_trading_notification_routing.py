"""End-to-end notification policy over the durable audit-event worker.

All exchange/Telegram clients are deterministic fakes; there is no live I/O.
"""

from __future__ import annotations

from tests.test_trading_budget import DEVELOPER, OWNER, FlakyBot, _notifier
from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.mexc.errors import MexcNetworkError, MexcOrderRejected


def _texts(bot: FlakyBot) -> list[str]:
    return [text for _, text in bot.sent]


def _joined(bot: FlakyBot) -> str:
    return "\n".join(_texts(bot))


def _assert_both(bot: FlakyBot) -> None:
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}


def test_genuine_buy_notifies_owner_and_developer_once(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()

    assert result is not None and result["state"] in ("submitted", "open", "filled")
    assert len(bot.sent) >= 2
    _assert_both(bot)
    assert "ورود: 100" in _joined(bot)
    joined = _joined(bot)
    assert "سفارش خرید در صرافی پذیرفته شد" in joined
    assert joined.count("سیگنال خرید معتبر") == 1  # owners only; developer is not an owner
    assert "سیگنال جدید دریافت شد" not in joined


def test_signal_decisions_are_only_local_audit_events(cfg, repo):
    engine = _engine(cfg, repo, FakeMexc())
    engine.process_item(
        _item(100, 1),
        _store_message(repo, 1, "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105"),
    )
    assert not hasattr(engine, "notifier")
    assert not any(row["event_type"] == "signal.received" for row in engine.store.db.conn.execute(
        "SELECT event_type FROM audit_events WHERE event_class='domain'"
    ).fetchall())


def test_sell_signal_is_internal_only_and_never_notified(cfg, repo):
    """Spot SELL/SHORT diagnostics stay local; no Telegram route is allowed."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105"

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    assert result is None
    assert client.place_calls == 0
    assert bot.sent == []
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "Spot BUY only" in signal["rejection_reason"]


def test_locally_rejected_genuine_buy_is_internal_only(cfg, repo):
    """A locally rejected signal is recorded but never exposed in Telegram."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = "BUY BTCUSDT\nTP1: 110"

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    assert result is None
    assert client.place_calls == 0
    assert bot.sent == []
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "no entry price" in signal["rejection_reason"]


def test_rejection_is_recorded_once_and_has_no_legacy_delivery(cfg, repo):
    """The immutable diagnostic event is retained without a Telegram delivery."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = "BUY BTCUSDT\nTP1: 110"  # missing entry -> rejected locally

    engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    assert bot.sent == []
    events = engine.store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE event_class='domain' AND event_type='signal.rejected'"
    ).fetchone()[0]
    assert events == 1
    assert engine.store.get_signal("100:1:0")["status"] == "rejected"


def test_fixed_per_order_amount_is_submitted_without_a_budget_cap(cfg, repo):
    open_order = {
        "orderId": "ex-over-ref",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0",
        "cumulativeQuoteQty": "0",
        "status": "NEW",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-over-ref", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()

    # The fixed per-order amount is submitted; there is no aggregate cap.
    assert result is not None and result["state"] == "open"
    assert client.place_calls == 1
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "aggregate test budget" not in joined
    assert "سفارش خرید در صرافی پذیرفته شد" in joined


def test_exchange_accepted_open_is_not_reported_as_filled(cfg, repo):
    open_order = {
        "orderId": "ex-open",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0",
        "cumulativeQuoteQty": "0",
        "status": "NEW",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-open", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()

    assert result is not None and result["state"] == "open"
    assert client.place_calls == 1
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "سفارش خرید در صرافی پذیرفته شد" in joined
    assert "ex-open" not in joined
    assert "NEW" not in joined
    assert "FILLED" not in joined


def test_exchange_rejection_notifies_both_as_submission_rejected(cfg, repo):
    client = FakeMexc(place_error=MexcOrderRejected("insufficient balance"))
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()

    assert result is not None and result["state"] == "rejected"
    assert client.place_calls == 1
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "سفارش خرید ناموفق بود" in joined
    assert "insufficient balance" in joined
    assert "نتیجهٔ درخواست صرافی" not in joined
    assert "REJECTED_LOCALLY" not in joined


def test_uncertain_submission_and_reconciliation_update_are_truthful(cfg, repo):
    filled = {
        "orderId": "ex-uncertain",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0.1",
        "cumulativeQuoteQty": "10",
        "status": "FILLED",
        "time": 1,
    }
    trade = {
        "id": "t-uncertain",
        "price": "100",
        "qty": "0.1",
        "quoteQty": "10",
        "commission": "0.01",
        "commissionAsset": "USDT",
        "time": 2,
    }
    client = FakeMexc(
        place_error=MexcNetworkError("timeout after send"),
        query_results=[filled],
        trades=[trade],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()
    assert result is not None and result["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "نامشخص" in joined
    assert "MexcNetworkError" not in joined

    intent_id = result["intent_id"]
    sent_before_reconcile = len(bot.sent)
    reconciled = engine.reconcile_intent(intent_id)
    notifier.flush()
    assert reconciled["state"] == "filled"
    assert client.place_calls == 1  # reconciliation never resubmits
    assert len(bot.sent) > sent_before_reconcile
    _assert_both(bot)
    update = "\n".join(_texts(bot)[sent_before_reconcile:])
    assert "سفارش خرید کامل اجرا شد" in update
    assert "مقدار اجراشده: 0.1" in update
    assert "ex-uncertain" not in _joined(bot)

    # Repeating the same reconciliation must not duplicate the notification.
    sent_after_reconcile = len(bot.sent)
    client.query_results.append(dict(filled))
    engine.reconcile_intent(intent_id)
    notifier.flush()
    assert client.place_calls == 1
    assert len(bot.sent) == sent_after_reconcile


def test_partial_fill_then_full_fill_sends_meaningful_updates(cfg, repo):
    partial = {
        "orderId": "ex-part",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0.05",
        "cumulativeQuoteQty": "5",
        "status": "PARTIALLY_FILLED",
        "time": 1,
    }
    filled = {
        **partial,
        "executedQty": "0.1",
        "cumulativeQuoteQty": "10",
        "status": "FILLED",
        "time": 3,
    }
    first_trade = {
        "id": "t-part-1",
        "price": "100",
        "qty": "0.05",
        "quoteQty": "5",
        "commission": "0.005",
        "commissionAsset": "USDT",
        "time": 2,
    }
    second_trade = {
        "id": "t-part-2",
        "price": "100",
        "qty": "0.05",
        "quoteQty": "5",
        "commission": "0.005",
        "commissionAsset": "USDT",
        "time": 3,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-part", "status": "PARTIALLY_FILLED"},
        query_results=[partial],
        trades=[first_trade],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()
    assert result is not None and result["state"] == "open"
    assert len(bot.sent) >= 2
    first = _joined(bot)
    assert "اجرای خرید در صرافی تأیید شد" in first
    assert "مقدار: 0.05 · قیمت: 100" in first
    assert "PARTIALLY_FILLED" not in first
    assert "تاریخچهٔ Fill" not in first

    client.query_results.append(filled)
    client.trades = [first_trade, second_trade]
    reconciled = engine.reconcile_intent(result["intent_id"])
    notifier.flush()
    assert reconciled["state"] == "filled"
    assert len(bot.sent) > 2
    update = _joined(bot)
    assert "اجرای خرید در صرافی تأیید شد" in update
    assert "مقدار: 0.05 · قیمت: 100" in update
    assert client.place_calls == 1


def test_not_a_signal_diagnostics_never_reach_telegram(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    cases = [
        "hello everyone, market is quiet today",
        "TP1 HIT BTCUSDT ✅",
        "BTCUSDT entry 100 tp 110",
    ]

    for message_id, text in enumerate(cases, 1):
        engine.process_item(_item(100, message_id), _store_message(repo, message_id, text))
    notifier.flush()

    assert bot.sent == []
    assert engine.store.list_intents(limit=10) == []
    assert engine.store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE category='parse'"
    ).fetchone()[0] >= len(cases)
    assert client.place_calls == 0


def test_historical_backfill_and_replay_are_silent_and_non_tradeable(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    # Historical genuine signal: no notification, no order.
    assert engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT, source="backfill")) is None
    # Historical non-signal: no developer diagnostic either.
    assert (
        engine.process_item(
            _item(100, 2),
            _store_message(repo, 2, "ordinary historical chatter", source="backfill"),
        )
        is None
    )
    notifier.flush()
    assert bot.sent == []
    assert engine.store.list_intents(limit=10) == []
    assert client.place_calls == 0

    # A live message at/below the activation boundary is replay traffic too.
    engine.store.ensure_activation_boundary(100, minimum=3)
    assert engine.process_item(_item(100, 3), _store_message(repo, 3, ALERT)) is None
    notifier.flush()
    assert bot.sent == []
    assert engine.store.list_intents(limit=10) == []
    assert client.place_calls == 0


def test_retry_and_restart_do_not_duplicate_notifications_or_orders(cfg, repo):
    open_order = {
        "orderId": "ex-retry",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0",
        "cumulativeQuoteQty": "0",
        "status": "NEW",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-retry", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    record = _store_message(repo, 1, ALERT)

    first = engine.process_item(_item(100, 1), record)
    notifier.flush()
    assert first is not None and client.place_calls == 1
    assert len(bot.sent) >= 2
    first_count = len(bot.sent)

    # Queue retry in the same process.
    assert engine.process_item(_item(100, 1), record) is not None
    notifier.flush()
    assert client.place_calls == 1
    assert len(bot.sent) == first_count

    # Restart with a fresh engine and a fresh notifier/bot.
    restarted = _engine(cfg, repo, client)
    bot2 = FlakyBot()
    notifier2 = _notifier(restarted, bot2)
    assert restarted.process_item(_item(100, 1), record) is not None
    notifier2.flush()
    assert client.place_calls == 1
    assert bot2.sent == []
    assert len(restarted.store.list_intents(limit=10)) == 1
