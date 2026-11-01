"""Notification routing policy: owner gets every genuine signal; developer
also gets NOT_A_SIGNAL diagnostics.

All exchange/Telegram clients are deterministic fakes.  No test can reach
MEXC or Telegram, and no test places a real order.
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
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] in ("submitted", "open", "filled")
    assert len(bot.sent) == 2  # one consolidated message per recipient
    _assert_both(bot)
    for text in _texts(bot):
        assert ALERT in text
        assert "exchange submission attempted: yes" in text
        assert "source: channel 100 / message 1" in text
        assert "original message:" in text
    assert "سیگنال جدید دریافت شد" not in _joined(bot)


def test_genuine_sell_notifies_developer_only_and_is_explicitly_ignored(cfg, repo):
    """Owner policy: routine local signal rejections go to the developer only,
    never to the owner; this is explicit role routing, not a severity preference."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)
    text = "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105"

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))

    assert result is None
    assert client.place_calls == 0
    assert len(bot.sent) == 1
    assert {chat for chat, _ in bot.sent} == {DEVELOPER}
    joined = _joined(bot)
    assert "status: IGNORED_SELL_SPOT_BUY_ONLY" in joined
    assert "Spot BUY only" in joined
    assert "exchange submission attempted: no" in joined
    assert text in joined
    assert "ACCEPTED_OPEN" not in joined and "SUBMISSION_REJECTED" not in joined


def test_locally_rejected_genuine_buy_notifies_developer_only_with_reason(cfg, repo):
    """A genuine signal rejected locally is reported to the developer only."""
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)
    text = "BUY BTCUSDT\nTP1: 110"

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, text))

    assert result is None
    assert client.place_calls == 0
    assert len(bot.sent) == 1
    assert {chat for chat, _ in bot.sent} == {DEVELOPER}
    joined = _joined(bot)
    assert "status: REJECTED_LOCALLY" in joined
    assert "no entry price" in joined
    assert "exchange submission attempted: no" in joined
    assert "SUBMISSION_REJECTED" not in joined and "REJECTED_BY_EXCHANGE" not in joined
    assert text in joined


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
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    # The fixed per-order amount is submitted; there is no aggregate cap.
    assert result is not None and result["state"] == "open"
    assert client.place_calls == 1
    assert len(bot.sent) == 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "aggregate test budget" not in joined
    assert "status: ACCEPTED_OPEN" in joined
    assert "exchange submission attempted: yes" in joined


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
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] == "open"
    assert client.place_calls == 1
    assert len(bot.sent) == 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "status: ACCEPTED_OPEN" in joined
    assert "exchange order id: ex-open" in joined
    assert "exchange status: NEW" in joined
    assert "exchange submission attempted: yes" in joined
    assert "FILLED" not in joined


def test_exchange_rejection_notifies_both_as_submission_rejected(cfg, repo):
    client = FakeMexc(place_error=MexcOrderRejected("insufficient balance"))
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] == "rejected"
    assert client.place_calls == 1
    assert len(bot.sent) == 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "status: SUBMISSION_REJECTED" in joined
    assert "insufficient balance" in joined
    assert "exchange submission attempted: yes" in joined
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
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is not None and result["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1
    assert len(bot.sent) == 2
    _assert_both(bot)
    joined = _joined(bot)
    assert "status: UNCERTAIN_AWAITING_RECONCILIATION" in joined
    assert "exchange submission attempted: yes" in joined
    assert "timeout after send" in joined

    intent_id = result["intent_id"]
    reconciled = engine.reconcile_intent(intent_id)
    assert reconciled["state"] == "filled"
    assert client.place_calls == 1  # reconciliation never resubmits
    assert len(bot.sent) == 4
    _assert_both(bot)
    update = "\n".join(_texts(bot)[2:])
    assert "status: FILLED" in update
    assert "exchange order id: ex-uncertain" in update
    assert "exchange submission attempted: yes" in update
    assert "remaining quantity: 0" in update

    # Repeating the same reconciliation must not duplicate the notification.
    client.query_results.append(dict(filled))
    engine.reconcile_intent(intent_id)
    assert client.place_calls == 1
    assert len(bot.sent) == 4


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
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is not None and result["state"] == "open"
    assert len(bot.sent) == 2
    first = _joined(bot)
    assert "status: PARTIALLY_FILLED" in first
    assert "executed qty: 0.05" in first
    assert "status: FILLED" not in first

    client.query_results.append(filled)
    client.trades = [first_trade, second_trade]
    reconciled = engine.reconcile_intent(result["intent_id"])
    assert reconciled["state"] == "filled"
    assert len(bot.sent) == 4
    update = "\n".join(_texts(bot)[2:])
    assert "status: FILLED" in update
    assert "executed qty: 0.1" in update
    assert "remaining quantity: 0" in update
    assert client.place_calls == 1


def test_not_a_signal_goes_to_developer_only_with_exact_text(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)
    cases = [
        "hello everyone, market is quiet today",
        "TP1 HIT BTCUSDT ✅",
        "BTCUSDT entry 100 tp 110",
    ]

    for message_id, text in enumerate(cases, 1):
        engine.process_item(_item(100, message_id), _store_message(repo, message_id, text))

    assert len(bot.sent) == len(cases)
    assert {chat for chat, _ in bot.sent} == {DEVELOPER}
    assert all(chat != OWNER for chat, _ in bot.sent)
    for (_, sent_text), original in zip(bot.sent, cases, strict=True):
        assert original in sent_text
        assert "status: NOT_A_SIGNAL" in sent_text
        assert "original message:" in sent_text
    joined = _joined(bot)
    assert "classification: info" in joined
    assert "classification: result" in joined
    assert "classification: ambiguous" in joined
    assert engine.store.list_intents(limit=10) == []
    assert client.place_calls == 0


def test_historical_backfill_and_replay_are_silent_and_non_tradeable(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)

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
    assert bot.sent == []
    assert engine.store.list_intents(limit=10) == []
    assert client.place_calls == 0

    # A live message at/below the activation boundary is replay traffic too.
    engine.store.ensure_activation_boundary(100, minimum=3)
    assert engine.process_item(_item(100, 3), _store_message(repo, 3, ALERT)) is None
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
    _notifier(engine, bot)
    record = _store_message(repo, 1, ALERT)

    first = engine.process_item(_item(100, 1), record)
    assert first is not None and client.place_calls == 1
    assert len(bot.sent) == 2

    # Queue retry in the same process.
    assert engine.process_item(_item(100, 1), record) is not None
    assert client.place_calls == 1
    assert len(bot.sent) == 2

    # Restart with a fresh engine and a fresh notifier/bot.
    restarted = _engine(cfg, repo, client)
    bot2 = FlakyBot()
    _notifier(restarted, bot2)
    assert restarted.process_item(_item(100, 1), record) is not None
    assert client.place_calls == 1
    assert bot2.sent == []
    assert len(restarted.store.list_intents(limit=10)) == 1
