"""Durable owner/developer notification outbox and independent retry."""

from __future__ import annotations

from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.control.bot_api import BotApiError
from tg_signal_ingest.trading.notify import TradingNotifier

OWNER = 100000001
DEVELOPER = 100000002


class FlakyBot:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []
        self.fail_chats: set[int] = set()
        self.calls = 0

    def send_message(self, chat_id: int, text: str):
        self.calls += 1
        if chat_id in self.fail_chats:
            raise BotApiError("telegram unavailable")
        self.sent.append((chat_id, text))
        return {"message_id": len(self.sent)}


def _notifier(engine, bot):
    n = TradingNotifier(engine.store, bot, owner_id=OWNER, developer_id=DEVELOPER)
    engine.notifier = n
    return n


def test_both_owner_and_developer_receive_trading_notifications(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.place_calls == 1

    recipients = {chat for chat, _ in bot.sent}
    assert recipients == {OWNER, DEVELOPER}
    joined = "\n".join(text for _, text in bot.sent)
    assert "BTCUSDT" in joined
    assert "10" in joined
    assert "secret" not in joined.lower()


def test_failed_notification_retries_without_resubmitting_the_order(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    bot.fail_chats = {DEVELOPER}
    notifier = _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.place_calls == 1
    assert any(chat == OWNER for chat, _ in bot.sent)
    assert notifier.pending_count() > 0

    bot.fail_chats.clear()
    notifier.flush()
    assert notifier.pending_count() == 0
    assert any(chat == DEVELOPER for chat, _ in bot.sent)
    assert client.place_calls == 1  # notification retry never touches the exchange


def test_notification_failure_never_blocks_trading(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    bot.fail_chats = {OWNER, DEVELOPER}
    _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.place_calls == 1


def test_notification_events_are_idempotent(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    # signal_accepted is delivered to both owner and developer; repeating the
    # same event_key must not duplicate the delivery.
    notifier.notify(event_key="fixed-event", text="hello", category="signal_accepted")
    notifier.notify(event_key="fixed-event", text="hello", category="signal_accepted")
    assert len(bot.sent) == 2  # one per role, not four
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}


def test_owner_event_stream_is_never_suppressed_by_minimum_severity(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    # The legacy severity preference must not hide owner-level events.
    notifier.notify(event_key="diag", text="debug detail", category="diagnostic")
    assert len(bot.sent) == 2
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}

    notifier.store.set_notification_min_severity("WARNING")
    notifier.notify(event_key="info", text="routine info", category="order_lifecycle")
    assert len(bot.sent) == 4

    notifier.notify(event_key="warn", text="reconcile warning", category="reconciliation")
    assert len(bot.sent) == 6


def test_critical_threshold_still_receives_every_owner_event(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    notifier.store.set_notification_min_severity("CRITICAL")

    notifier.notify(event_key="err", text="failed", category="order_lifecycle")
    assert len(bot.sent) == 2

    notifier.notify(event_key="crit", text="unprotected", category="critical")
    assert len(bot.sent) == 4


def test_distinct_trades_never_suppress_each_other(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    notifier.notify_trade({"trade_id": "t1", "signal_id": "s1"}, "ENTRY_SUBMITTED", "first")
    notifier.notify_trade({"trade_id": "t2", "signal_id": "s2"}, "ENTRY_SUBMITTED", "second")

    texts = [text for _, text in bot.sent]
    assert len(texts) == 4  # two trades x two roles
    assert texts.count("first") == 2
    assert texts.count("second") == 2


def test_both_owners_and_programmer_receive_every_event_category(cfg, repo):
    from tg_signal_ingest.trading.notify import TradingNotifier

    engine = _engine(cfg, repo)
    bot = FlakyBot()
    owner_b = 100000003
    notifier = TradingNotifier(
        engine.store,
        bot,
        owner_id=OWNER,
        owner_b_id=owner_b,
        developer_id=DEVELOPER,
    )
    engine.notifier = notifier

    required = [
        "signal.received",
        "signal.ignored",
        "signal.rejected",
        "signal.duplicate",
        "signal.historical_rejected",
        "parser.result",
        "validation.result",
        "risk.evaluation",
        "intent.created",
        "entry.submission_started",
        "exchange.acknowledgement",
        "submission.timeout",
        "submission.uncertain",
        "order.accepted",
        "order.rejected",
        "fill.partial",
        "fill.full",
        "fill.synchronized",
        "oco.creation_attempt",
        "oco.acknowledged.tp1",
        "oco.acknowledged.tp2",
        "oco.rejected",
        "oco.partial_protection",
        "oco.full_protection",
        "stop.activation",
        "tp.activation",
        "cancel.request",
        "cancel.confirmation",
        "cancel.failure",
        "reconciliation.started",
        "reconciliation.result",
        "restart.recovery",
        "conditional_entry.state",
        "api.error",
        "telegram.connectivity",
        "mexc.connectivity",
        "rate_limit",
        "database.error",
        "worker.failure",
        "service.startup",
        "service.shutdown",
        "readiness.change",
        "pause.state",
    ]
    for index, event_key in enumerate(required):
        notifier.notify(event_key=f"evt:{index}:{event_key}", text=f"event {event_key}", category="diagnostic")

    recipients = [chat for chat, _ in bot.sent]
    for chat_id in (OWNER, owner_b, DEVELOPER):
        assert recipients.count(chat_id) == len(required)
    for event_key in required:
        assert any(event_key in text for _, text in bot.sent)
    # Independent per-recipient outbox rows, not one broadcast row.
    rows = repo.db.conn.execute("SELECT role, COUNT(*) AS n FROM trading_notifications GROUP BY role").fetchall()
    assert {row["role"] for row in rows} == {"owner", "owner_b", "developer"}
    assert all(int(row["n"]) == len(required) for row in rows)
