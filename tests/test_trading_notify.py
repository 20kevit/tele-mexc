"""Durable owner/developer notification outbox and independent retry."""

from __future__ import annotations

from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.control.bot_api import BotApiError, BotApiNotSentError
from tg_signal_ingest.notification_dispatcher import NotificationDispatcher
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


def test_both_owner_and_developer_receive_trading_notifications(cfg, repo, tmp_path):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    dispatcher = NotificationDispatcher(
        engine.store.db.path,
        tmp_path / "notification-delivery.sqlite3",
        bot,
        {"owner": OWNER, "developer": DEVELOPER},
    )
    try:
        intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
        dispatcher.run_once()
        assert intent is not None
        assert client.place_calls == 1

        recipients = {chat for chat, _ in bot.sent}
        assert recipients == {OWNER, DEVELOPER}
        joined = "\n".join(text for _, text in bot.sent)
        assert "BTCUSDT" in joined
        assert "10" in joined
        assert "secret" not in joined.lower()
    finally:
        dispatcher.close()


def test_failed_notification_retries_without_resubmitting_the_order(cfg, repo, tmp_path):
    class RetryableBot(FlakyBot):
        def send_message(self, chat_id: int, text: str):
            self.calls += 1
            if chat_id == DEVELOPER and chat_id in self.fail_chats:
                raise BotApiNotSentError("telegram connection failed before send")
            self.sent.append((chat_id, text))
            return {"message_id": len(self.sent)}

    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = RetryableBot()
    bot.fail_chats = {DEVELOPER}
    dispatcher = NotificationDispatcher(
        engine.store.db.path,
        tmp_path / "notification-delivery.sqlite3",
        bot,
        {"owner": OWNER, "developer": DEVELOPER},
    )
    try:
        intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
        dispatcher.run_once()
        assert intent is not None
        assert client.place_calls == 1
        assert any(chat == OWNER for chat, _ in bot.sent)
        retry = dispatcher.store.conn.execute(
            "SELECT status FROM deliveries WHERE recipient_role='developer' ORDER BY created_at LIMIT 1"
        ).fetchone()
        assert retry["status"] == "retry"

        bot.fail_chats.clear()
        dispatcher.store.conn.execute(
            "UPDATE deliveries SET next_attempt_at='2000-01-01T00:00:00Z' WHERE recipient_role='developer'"
        )
        dispatcher.run_once()
        assert any(chat == DEVELOPER for chat, _ in bot.sent)
        assert client.place_calls == 1  # notification retry never touches the exchange
    finally:
        dispatcher.close()


def test_notification_failure_never_blocks_trading(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    bot.fail_chats = {OWNER, DEVELOPER}
    notifier = _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    notifier.flush()
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
    notifier.flush()
    assert len(bot.sent) == 2  # one per role, not four
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}


def test_owner_event_stream_is_never_suppressed_by_minimum_severity(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    # The legacy severity preference must not hide owner-level events.
    notifier.notify(event_key="diag", text="debug detail", category="diagnostic")
    notifier.flush()
    assert len(bot.sent) == 2
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}

    notifier.store.set_notification_min_severity("WARNING")
    notifier.notify(event_key="info", text="routine info", category="order_lifecycle")
    notifier.flush()
    assert len(bot.sent) == 4

    notifier.notify(event_key="warn", text="reconcile warning", category="reconciliation")
    notifier.flush()
    assert len(bot.sent) == 6


def test_critical_threshold_still_receives_every_owner_event(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    notifier.store.set_notification_min_severity("CRITICAL")

    notifier.notify(event_key="err", text="failed", category="order_lifecycle")
    notifier.flush()
    assert len(bot.sent) == 2

    notifier.notify(event_key="crit", text="unprotected", category="critical")
    notifier.flush()
    assert len(bot.sent) == 4


def test_two_workers_never_deliver_the_same_outbox_row(cfg, repo):
    import threading
    import time

    engine = _engine(cfg, repo)
    sent: list[tuple[int, str]] = []
    lock = threading.Lock()

    class SlowBot:
        def send_message(self, chat_id: int, text: str):
            time.sleep(0.01)
            with lock:
                sent.append((int(chat_id), str(text)))
            return {"message_id": len(sent)}

    notifier_a = TradingNotifier(engine.store, SlowBot(), owner_id=OWNER, developer_id=DEVELOPER)
    notifier_b = TradingNotifier(engine.store, SlowBot(), owner_id=OWNER, developer_id=DEVELOPER)
    notifier_a.notify(event_key="concurrent", text="one event", category="critical")
    # notify() only persists locally; race two workers over a fresh outbox row.
    notifier_a.store.enqueue_notification(
        event_key="concurrent-2",
        signal_id=None,
        intent_id=None,
        role="owner",
        chat_id=OWNER,
        text="raced",
        category="critical",
        event_type="test.concurrent",
    )
    barrier = threading.Barrier(2)

    def run(notifier):
        barrier.wait()
        notifier.flush()

    threads = [threading.Thread(target=run, args=(n,)) for n in (notifier_a, notifier_b)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert [text for _, text in sent].count("raced") == 1


def test_distinct_trades_never_suppress_each_other(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    notifier.notify_trade({"trade_id": "t1", "signal_id": "s1"}, "ENTRY_SUBMITTED", "first")
    notifier.notify_trade({"trade_id": "t2", "signal_id": "s2"}, "ENTRY_SUBMITTED", "second")
    notifier.flush()

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
    notifier.flush(limit=200)

    recipients = [chat for chat, _ in bot.sent]
    for chat_id in (OWNER, owner_b, DEVELOPER):
        assert recipients.count(chat_id) == len(required)
    for event_key in required:
        assert any(event_key in text for _, text in bot.sent)
    # Independent per-recipient outbox rows, not one broadcast row.
    rows = repo.db.conn.execute("SELECT role, COUNT(*) AS n FROM trading_notifications GROUP BY role").fetchall()
    assert {row["role"] for row in rows} == {"owner", "owner_b", "developer"}
    assert all(int(row["n"]) == len(required) for row in rows)
