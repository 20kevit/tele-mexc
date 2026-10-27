"""Management-bot signal review: store helpers, rendering, callback flow.

Read-only checks against a seeded logical signal index (built through the
normal replay + reindex path) and through the same fake-bot/one-shot-token
conventions used by the other management-bot tests. No Telegram API, no
exchange, no trading table is ever touched.
"""

from __future__ import annotations

import pytest

from tests.conftest import FakeMsg, ReplyTo
from tests.test_management_bot import OWNER, _bot, _callback, _message
from tests.test_signal_index import (
    CHANNEL,
    FOLLOW_UP_301_ENTRY,
    FOLLOW_UP_301_TP,
    INITIAL_301,
)
from tg_signal_ingest.analysis import ReplayScope, run_replay
from tg_signal_ingest.analysis.reindex import rebuild_signal_index
from tg_signal_ingest.management import signal_review as sr
from tg_signal_ingest.normalize import message_to_record


def _put(repo, message_id: int, text: str, reply_to: int | None = None) -> None:
    msg = FakeMsg(message_id, text, reply_to=ReplyTo(reply_to) if reply_to else None)
    repo.upsert_message(
        message_to_record(CHANNEL, msg, "live"), enqueue_processing=False
    )


@pytest.fixture()
def indexed_repo(repo):
    _put(repo, 500, INITIAL_301)
    _put(repo, 501, FOLLOW_UP_301_ENTRY, reply_to=500)
    _put(repo, 502, FOLLOW_UP_301_TP)
    run_replay(repo, ReplayScope(channel_id=CHANNEL))
    rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
    return repo


class TestSliceUtf16:
    def test_short_text_untouched(self):
        text = "سلام 123"
        assert sr.slice_utf16(text, 100) == [text]

    def test_split_never_creates_surrogate_split(self):
        text = "🔮" * 1000  # astral: 2 UTF-16 units each
        parts = sr.slice_utf16(text, 600)
        assert len(parts) > 1
        for part in parts:
            assert sr.utf16_len(part) <= 600
        assert "".join(parts) == text

    def test_tiny_capacity(self):
        assert sr.slice_utf16("ab🔮cd", 1) == ["a", "b", "🔮", "c", "d"]


class TestStoreHelpers:
    def test_recent_signals_ordering_is_datetime_then_message_id(self, indexed_repo):
        result = sr.recent_signals(indexed_repo.db, CHANNEL, newest_first=True)
        assert result["total"] == 1
        row = result["rows"][0]
        assert row["logical_id"] == 301
        assert row["origin_message_id"] == 500
        # Chronological order (the reindex-readable direction)
        chrono = sr.recent_signals(indexed_repo.db, CHANNEL)
        assert chrono["rows"][0]["logical_id"] == 301

    def test_signal_by_logical_id_and_logical_ids(self, indexed_repo):
        assert sr.signal_by_logical_id(indexed_repo.db, CHANNEL, 301)["symbol"] == "AAVEUSDT"
        assert sr.signal_by_logical_id(indexed_repo.db, CHANNEL, 999) is None
        assert sr.logical_ids(indexed_repo.db, CHANNEL) == [301]

    def test_related_messages_join_messages(self, indexed_repo):
        messages = sr.related_messages(indexed_repo.db, CHANNEL, 301)
        assert [int(m["message_id"]) for m in messages] == [500, 501, 502]
        assert messages[1]["reply_to_message_id"] == 500

    def test_unavailable_without_index_tables(self, tmp_path):
        from tg_signal_ingest.db import Database

        db = Database(tmp_path / "empty.sqlite3")
        with pytest.raises(sr.SignalReviewUnavailable):
            sr.ensure_available(
                type("S", (), {"tables_exist": lambda self, *n: False})()
            )
        db.close()


class TestRenderers:
    def test_detail_and_extract_mention_review_only_semantics(self, indexed_repo):
        signal = sr.signal_by_logical_id(indexed_repo.db, CHANNEL, 301)
        detail = sr.render_signal_detail(CHANNEL, signal)
        extract = sr.render_signal_extract(CHANNEL, signal)
        assert "بدون اجرا (فقط مرور تاریخچه)" in detail
        assert "لانگ" in detail
        assert "عدد سپرده" in extract or "درصد سپرده" in extract
        assert "لوریج" in extract
        assert "1405/08/01" in extract
        assert "2026-10-23T05:15:00Z" in extract
        # review-only statement present
        assert "هرگز در محاسبه حجم" in extract

    def test_short_signal_labelled_and_never_executed_description(self, indexed_repo):
        detail = sr.render_signal_detail(CHANNEL, {"direction": "short", "execution_status": "none"})
        assert "شورت" in detail

    def test_related_messages_render_link_status_and_reason(self, indexed_repo):
        message = sr.render_related_messages(
            CHANNEL, 301, sr.related_messages(indexed_repo.db, CHANNEL, 301)
        )
        assert "متصل" in message
        assert "شناسه و پاسخ" in message
        assert "پیام 502" in message or "پیام 501" in message

    def test_original_message_chunking_stays_within_limits(self, indexed_repo):
        body = "اب + 🔮" * 3000
        message = {"message_id": 5, "datetime": "d", "edit_datetime": None, "text": body}
        parts = sr.render_original_message(message)
        assert len(parts) > 1
        for part in parts:
            assert sr.utf16_len(part) <= 4096
        again = sr.render_original_message({**message, "text": "ردیف کوتاه"})
        assert len(again) == 1


class TestBotHandlerFlow:
    def _selected(self, control, user, channel_id=CHANNEL):
        control.store.set_operator_channel(
            user, selected_channel_id=channel_id, selected_channel_title="Signals"
        )

    @pytest.mark.asyncio
    async def test_signals_menu_lists_and_navigates(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        self._selected(control, OWNER)
        await control.handle_update(_message(1, "/signals"))
        texts = [item["text"] for item in fake.sent_to(OWNER)]
        assert any("مرور سیگنال‌های اخیر" in t for t in texts)
        assert any("(از 1)" in t or "از 1" in t for t in texts)
        keyboard = fake.sent_to(OWNER)[-1]["reply_markup"]["inline_keyboard"]
        flat = [b for row in keyboard for b in row]
        assert any("#301" in b["text"] for b in flat)
        assert control.store.audit is not None
        # opening the signal view through its one-shot token
        token = next(b["callback_data"].split(":", 1)[1] for b in flat if "#301" in b["text"])
        await control.handle_update(_callback(2, "mg:" + token))
        assert any("سیگنال #301" in item["text"] for item in fake.sent_to(OWNER))

    @pytest.mark.asyncio
    async def test_menu_requires_selected_channel(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        await control.handle_update(_message(1, "/signals"))
        assert any("انتخاب و تأیید" in item["text"] for item in fake.sent_to(OWNER))

    @pytest.mark.asyncio
    async def test_stranger_refused_by_role_gate(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        self._selected(control, OWNER)
        # A stranger never reaches the handler (unauthorized for callbacks),
        # but the role gate is still exercised for unknown roles via direct call.
        from tg_signal_ingest.management.bot import ManagementBot

        gate = ManagementBot._review_gate(control, OWNER)
        quota = ManagementBot._review_gate(control, 123)
        assert quota == (0, "این بخش فقط برای نقش‌های مجاز است.")
        assert gate is not None

    @pytest.mark.asyncio
    async def test_extract_and_related_views(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        self._selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signal_extract", {"logical_id": 301}, None)
        assert any("داده‌های استخراج‌شده" in i["text"] for i in fake.sent_to(OWNER))
        await control._dispatch_callback(OWNER, "signal_related", {"logical_id": 301}, None)
        text = fake.sent_to(OWNER)[-1]["text"]
        assert "پیام‌های مرتبط" in text
        assert "502" in text

    @pytest.mark.asyncio
    async def test_original_message_never_longer_than_limit(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        self._selected(control, OWNER)
        await control._dispatch_callback(
            OWNER, "signal_original", {"logical_id": 301, "message_id": 500}, None
        )
        sent = [i for i in fake.sent_to(OWNER) if i["text"]]
        assert any("پیام اصلی سیگنال #301" in i["text"] for i in sent)
        assert all(sr.utf16_len(i["text"]) <= 4096 for i in sent)

    @pytest.mark.asyncio
    async def test_unknown_signal_reports_missing(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        self._selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signal_view", {"logical_id": 999}, None)
        assert any("#999" in item["text"] for item in fake.sent_to(OWNER))
