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
    n = TradingNotifier(
        engine.store, bot, owner_id=OWNER, developer_id=DEVELOPER
    )
    engine.notifier = n
    return n


def test_both_owner_and_developer_receive_trading_notifications(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="testonly")
    bot = FlakyBot()
    _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.test_calls == 1

    recipients = {chat for chat, _ in bot.sent}
    assert recipients == {OWNER, DEVELOPER}
    joined = "\n".join(text for _, text in bot.sent)
    assert "BTCUSDT" in joined
    assert "10" in joined
    assert "secret" not in joined.lower()


def test_failed_notification_retries_without_resubmitting_the_order(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="testonly")
    bot = FlakyBot()
    bot.fail_chats = {DEVELOPER}
    notifier = _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.test_calls == 1
    assert any(chat == OWNER for chat, _ in bot.sent)
    assert notifier.pending_count() > 0

    bot.fail_chats.clear()
    notifier.flush()
    assert notifier.pending_count() == 0
    assert any(chat == DEVELOPER for chat, _ in bot.sent)
    assert client.test_calls == 1  # notification retry never touches the exchange


def test_notification_failure_never_blocks_trading(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="testonly")
    bot = FlakyBot()
    bot.fail_chats = {OWNER, DEVELOPER}
    _notifier(engine, bot)

    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None
    assert client.test_calls == 1


def test_notification_events_are_idempotent(cfg, repo):
    engine = _engine(cfg, repo, mode="testonly")
    bot = FlakyBot()
    notifier = _notifier(engine, bot)
    notifier.notify(event_key="fixed-event", text="hello")
    notifier.notify(event_key="fixed-event", text="hello")
    assert len(bot.sent) == 2  # one per role, not four
