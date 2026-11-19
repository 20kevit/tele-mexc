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
            "SELECT status FROM deliveries WHERE recipient_role='developer' "
            "AND status='retry' ORDER BY created_at LIMIT 1"
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


def test_debug_diagnostic_is_filtered_before_legacy_outbox(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = _notifier(engine, bot)

    notifier.notify(event_key="diag", text="debug detail", category="diagnostic")
    assert notifier.pending_count() == 0
    notifier.flush()
    assert bot.sent == []

    # A real operational warning remains eligible for the separate dispatcher.
    notifier.notify(event_key="TRADE_ERROR", text="action needed", category="order_lifecycle")
    assert notifier.pending_count() == 2


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


def test_debug_and_signal_decisions_never_enter_legacy_telegram_outbox(cfg, repo):
    from tg_signal_ingest.trading.notify import TradingNotifier

    engine = _engine(cfg, repo)
    bot = FlakyBot()
    notifier = TradingNotifier(
        engine.store,
        bot,
        owner_id=OWNER,
        owner_b_id=100000003,
        developer_id=DEVELOPER,
    )
    notifier.notify(event_key="debug-event", text="debug detail", category="diagnostic")
    notifier.notify(
        event_key="signal-rejected", text="rejection reason", category="signal_decision",
        event_type="signal.rejected",
    )
    notifier.flush(limit=200)

    assert bot.sent == []
    assert notifier.pending_count() == 0
    assert repo.db.conn.execute("SELECT COUNT(*) FROM trading_notifications").fetchone()[0] == 0
