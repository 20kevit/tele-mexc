"""Notification routing policy: only valid LONG/BUY signals produce the
"signal received" receipt; SHORT/SELL and locally rejected genuine signals
still reach owner and developer as explicit rejection decisions, while
ordinary channel messages stay silent (stored and audited only).

All exchange/Telegram clients are deterministic fakes.  No test can reach
MEXC or Telegram, and no test places a real order.
"""

from __future__ import annotations

from tests.channel_template import channel_signal
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
    assert "سفارش ورود در صرافی پذیرفته شد" in _joined(bot)
    assert "سیگنال جدید دریافت شد" in _joined(bot)


def test_genuine_sell_rejects_without_signal_receipt(cfg, repo):
    """Only valid LONG/BUY signals produce the signal-received receipt.

    A SHORT/SELL signal is refused locally and reported as an explicit
    rejection decision to both owners and the programmer — never as a receipt.
    """
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = channel_signal(direction="SHORT", stop="105", tp1="90", tp2="80")

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    assert result is None
    assert client.place_calls == 0
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "Spot BUY only" in joined
    assert "سیگنال رد شد" in joined
    # Owner policy: SHORT/SELL never produces the signal-received receipt.
    assert "سیگنال جدید دریافت شد" not in joined
    assert "ACCEPTED_OPEN" not in joined and "SUBMISSION_REJECTED" not in joined


def test_locally_rejected_genuine_buy_notifies_both_owners_with_reason(cfg, repo):
    """A genuine signal rejected locally is reported to both owners."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = channel_signal(stop="101")

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    assert result is None
    assert client.place_calls == 0
    assert len(bot.sent) >= 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "SL" in joined
    assert "سیگنال رد شد" in joined
    assert "SUBMISSION_REJECTED" not in joined and "REJECTED_BY_EXCHANGE" not in joined


def test_rejection_is_delivered_once_not_duplicated_by_legacy_path(cfg, repo):
    """F2: a local rejection is one domain event, never a legacy/domain pair."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    text = channel_signal(stop="101")  # invalid stop -> rejected locally

    engine.process_item(_item(100, 1), _store_message(repo, 1, text))
    notifier.flush()

    # One canonical rejection rendering per recipient (owner + developer); it
    # must still never be duplicated for the same recipient.
    per_recipient: dict[int, int] = {}
    for chat, text in bot.sent:
        if "سیگنال رد شد" in text:
            per_recipient[chat] = per_recipient.get(chat, 0) + 1
    assert per_recipient == {OWNER: 1, DEVELOPER: 1}
    assert "status: REJECTED_LOCALLY" not in "\n".join(t for _, t in bot.sent)


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
    assert "سفارش ورود در صرافی پذیرفته شد" in joined


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
    assert "سفارش ورود در صرافی پذیرفته شد" in joined
    assert "ex-open" in joined
    assert "وضعیت صرافی: NEW" in joined
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
    assert "سفارش ورود رد شد" in joined
    assert "insufficient balance" in joined
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
    assert "MexcNetworkError" in joined

    intent_id = result["intent_id"]
    sent_before_reconcile = len(bot.sent)
    reconciled = engine.reconcile_intent(intent_id)
    notifier.flush()
    assert reconciled["state"] == "filled"
    assert client.place_calls == 1  # reconciliation never resubmits
    assert len(bot.sent) > sent_before_reconcile
    _assert_both(bot)
    update = "\n".join(_texts(bot)[sent_before_reconcile:])
    assert "سفارش ورود کامل پر شد" in update or "وضعیت سفارش" in update
    assert "ex-uncertain" in _joined(bot)

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
    assert "PARTIALLY_FILLED" in first
    assert "تاریخچهٔ Fill هنوز کامل همگام نشده است" in first
    assert "سفارش ورود کامل پر شد" not in first

    client.query_results.append(filled)
    client.trades = [first_trade, second_trade]
    reconciled = engine.reconcile_intent(result["intent_id"])
    notifier.flush()
    assert reconciled["state"] == "filled"
    assert len(bot.sent) > 2
    update = _joined(bot)
    assert "سفارش ورود کامل پر شد" in update
    assert "مقدار پرشده: 0.1" in update
    assert client.place_calls == 1


def test_ordinary_channel_messages_stay_silent_and_non_tradeable(cfg, repo):
    """Ordinary channel messages are stored and audited but never announced.

    Additional/chatter/result messages must not produce a Telegram
    notification for any recipient and must never create an order.
    """
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
        assert engine.process_item(_item(100, message_id), _store_message(repo, message_id, text)) is None
    notifier.flush()

    assert bot.sent == []
    assert engine.store.list_intents(limit=10) == []
    assert client.place_calls == 0
    # Every ordinary message is still durably recorded and audited.
    statuses = {engine.store.get_signal(f"100:{mid}:0")["status"] for mid in range(1, len(cases) + 1)}
    assert statuses == {"parsed"}


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
