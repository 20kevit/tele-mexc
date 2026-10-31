"""UI-reliability regression tests for the management bot interface.

These pin the Telegram limits established from the official Bot API contract:

* one text message is at most 4096 UTF-16 code units ("after entities
  parsing"), not 4096 Python characters — astral emoji count as two;
* ``callback_data`` is at most 64 *bytes*, not 64 characters;
* button labels are bounded and never cut mid-code-point;
* callback queries are answered promptly and exactly according to the
  one-shot token contract;
* long and astral-heavy content is chunked, never silently dropped, and the
  raw stored text stays intact.

No Telegram API, no live session and no exchange is ever touched: the fake
bot from ``tests.test_management_bot`` stands in for the transport.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from tests.conftest import FakeMsg, ReplyTo
from tests.test_management_bot import (
    DEVELOPER,
    OWNER,
    STRANGER,
    Msg,
    _bot,
    _callback,
    _channel,
)
from tests.test_signal_index import (
    CHANNEL,
    FOLLOW_UP_301_ENTRY,
    FOLLOW_UP_301_TP,
    INITIAL_301,
)
from tg_signal_ingest.analysis import ReplayScope, run_replay
from tg_signal_ingest.analysis.reindex import rebuild_signal_index
from tg_signal_ingest.management import raw_review as rr
from tg_signal_ingest.management import signal_review as sr
from tg_signal_ingest.management.bot import MAX_MESSAGE_UTF16, _btn
from tg_signal_ingest.normalize import message_to_record


def units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _put(repo, message_id: int, text: str, reply_to: int | None = None) -> None:
    msg = FakeMsg(message_id, text, reply_to=ReplyTo(reply_to) if reply_to else None)
    repo.upsert_message(message_to_record(CHANNEL, msg, "live"), enqueue_processing=False)


def _seed_signals(repo, logical_ids: list[int]) -> None:
    """Insert deterministic rows for pagination tests (no parser involved)."""
    for n, logical_id in enumerate(logical_ids):
        _put(
            repo,
            7000 + n,
            f"🆔 #{logical_id}\n *LONG*🟢\n ⚡️X:BTCUSDT\n Entry: 1",
        )
        repo.db.conn.execute(
            """
            INSERT OR REPLACE INTO channel_signals
                (channel_id, logical_id, direction, exchange, symbol_raw, symbol,
                 quote_asset, timeframe, entry_price, stop_loss, targets_json,
                 full_target, source_date_text, source_time_text,
                 source_timestamp_iso, origin_message_id, parser_version,
                 candidate_id, classification, parse_status, ambiguities_json,
                 status, execution_status, created_at, updated_at)
            VALUES (?, ?, 'long', 'x', 'BTCUSDT.P', 'BTCUSDT', 'USDT', 60,
                    '1.0', '0.9', '[]', NULL, '1405/08/01', '08:45',
                    '2026-10-23T05:15:00Z', ?, 2, NULL,
                    'initial_entry', 'parsed', '[]', 'active', 'none', '', '')
            """,
            (CHANNEL, logical_id, 7000 + n),
        )
    repo.db.conn.commit()


@pytest.fixture()
def indexed_repo(repo):
    _put(repo, 500, INITIAL_301)
    _put(repo, 501, FOLLOW_UP_301_ENTRY, reply_to=500)
    _put(repo, 502, FOLLOW_UP_301_TP)
    run_replay(repo, ReplayScope(channel_id=CHANNEL))
    rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
    return repo


def _selected(control, user, channel_id=CHANNEL, title="Signals"):
    control.store.set_operator_channel(
        user, selected_channel_id=channel_id, selected_channel_title=title
    )


# --------------------------------------------------------------- length safety


class TestShortenUtf16:
    def test_bounded_units_and_marker(self):
        text = "سلام " + "ط" * 2000
        bounded = sr.shorten_utf16(text, 240)
        assert units(bounded) <= 240
        assert bounded.endswith("…")

    def test_astral_characters_never_split(self):
        bounded = sr.shorten_utf16("🔮" * 500, 101)
        assert units(bounded) <= 101
        assert bounded.endswith("…")
        for char in bounded[:-1]:  # every kept code point is whole
            assert ord(char) > 0xFFFF

    def test_fits_untouched(self):
        assert sr.shorten_utf16("تارگت ۱۲۳", 30) == "تارگت ۱۲۳"

    def test_zero_limit(self):
        assert sr.shorten_utf16("abc", 0) == ""


class TestRenderSignalExtractBounded:
    def _signal(self, **overrides):
        base = {
            "logical_id": 301, "direction": "long", "symbol": "AAVEUSDT",
            "symbol_raw": "AAVEUSDT", "exchange": "toobit", "quote_asset": "USDT",
            "timeframe": "15", "entry_price": "270", "stop_loss": "264",
            "targets_json": '[{"index": 1, "price": "275"}]', "full_target": "290",
            "deposit_pct": "1.00", "suggested_leverage": "8",
            "leverage_pct": "-17.39", "classification": "initial_entry",
            "parse_status": "parsed", "ambiguities_json": [],
            "source_date_text": "1405/08/01", "source_time_text": "08:45",
            "source_timestamp_iso": "2026-10-23T05:15:00Z",
            "origin_message_id": 500, "edit_version": 0,
        }
        base.update(overrides)
        return base

    def test_huge_json_fields_stay_deliverable(self):
        huge_targets = "[" + ", ".join(
            f'{{"index": {i}, "price": "7.{i:03d}"}}' for i in range(1, 400)
        ) + "]"
        huge_ambiguities = ["ambiguous_target"] + [f"amb_{i}" for i in range(300)]
        text = sr.render_signal_extract(
            CHANNEL,
            self._signal(
                targets_json=huge_targets,
                ambiguities_json=huge_ambiguities,
                symbol_raw="A" * 5000,  # hostile free-form column
            ),
        )
        assert units(text) <= MAX_MESSAGE_UTF16
        assert "…" in text  # truncation is visible, not silent
        assert "هرگز سفارشی ایجاد نمی‌کنند" in text  # safety statement survives
        assert "کوتاه" in text

    def test_ordinary_signal_fully_readable(self):
        text = sr.render_signal_extract(CHANNEL, self._signal())
        assert "AAVEUSDT" in text
        # the full target list is shown verbatim, nothing shed
        assert '[{"index": 1, "price": "275"}]' in text
        assert "1405/08/01" in text


class TestRenderRelatedMessagesSegments:
    def _rows(self, count: int, body: str) -> list[dict]:
        return [
            {
                "message_id": 100 + i,
                "datetime": "2026-01-01 10:00:00+00:00",
                "edit_datetime": None,
                "text": body,
                "caption": None,
                "status": "linked",
                "method": "reply",
                "classification": "modification",
                "reason": "explicit_id_and_reply_identify_different_signals",
                "edit_captures": 0,
                "reply_to_message_id": None,
            }
            for i in range(count)
        ]

    def test_sixteen_long_messages_now_fit(self):
        rows = self._rows(
            16, "تحلیل تکنیکال و قیمت " * 26  # ~546 UTF-16 units per message
        )
        segments = sr.render_related_messages(1, 301, rows)
        assert len(segments) > 1
        for segment in segments:
            assert units(segment) <= MAX_MESSAGE_UTF16, units(segment)
        joined = "\n".join(segments)
        for i in range(16):
            assert f"پیام {100 + i}" in joined  # every message represented
        assert f"بخش 1 از {len(segments)}" in segments[0]
        assert f"بخش {len(segments)} از {len(segments)}" in segments[-1]

    def test_single_huge_body_bounded_and_marked(self):
        rows = self._rows(1, "ط" * 5000)
        segments = sr.render_related_messages(1, 301, rows)
        assert len(segments) == 1
        assert units(segments[0]) <= MAX_MESSAGE_UTF16
        assert "کوتاه‌شده" in segments[0]

    def test_footer_only_on_last_segment(self):
        rows = self._rows(40, "پیام " * 50)
        segments = sr.render_related_messages(1, 301, rows)
        assert len(segments) > 1
        for segment in segments[:-1]:
            assert "هرگز حذف یا جایگزین نمی‌شود" not in segment
        assert "هرگز حذف یا جایگزین نمی‌شود" in segments[-1]

    def test_empty_list_is_a_single_bounded_screen(self):
        segments = sr.render_related_messages(1, 301, [])
        assert len(segments) == 1
        assert units(segments[0]) <= MAX_MESSAGE_UTF16
        assert "پیام مرتبطی ثبت نشده است" in segments[0]

    def test_order_preserved(self):
        rows = []
        for i in range(5):
            row = self._rows(1, "تحلیل تکنیکال و قیمت پوزیشن")[0]
            rows.append(dict(row, message_id=200 + i))
        segments = sr.render_related_messages(1, 301, rows)
        joined = "\n".join(segments)
        assert (
            joined.index("پیام 200")
            < joined.index("پیام 201")
            < joined.index("پیام 204")
        )


# ---------------------------------------------------------------- bot flows


class TestSignalRelatedDelivery:
    async def test_many_related_messages_all_within_limit(self, cfg, indexed_repo):
        rows = [
            {"message_id": 100 + i, "datetime": "2026-01-01", "text": "ط" * 900,
             "caption": None, "status": "linked", "method": "reply",
             "classification": "modification", "reason": None, "edit_captures": 0}
            for i in range(16)
        ]
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        with patch(
            "tg_signal_ingest.management.bot.related_messages", return_value=rows
        ):
            await control._dispatch_callback(OWNER, "signal_related", {"logical_id": 301}, None)
        sent = [
            item for item in fake.sent_to(OWNER) if "پیام‌های مرتبط" in item["text"]
        ]
        assert len(sent) >= 2
        for item in sent:
            assert units(item["text"]) <= MAX_MESSAGE_UTF16
        assert "بخش 1 از" in sent[0]["text"]
        # navigation attached to first and last, absent in between
        assert sent[0]["reply_markup"] is not None
        assert sent[-1]["reply_markup"] is not None
        for middle in sent[1:-1]:
            assert middle["reply_markup"] is None

    async def test_partial_delivery_is_reported(self, cfg, indexed_repo):
        rows = [
            {"message_id": 100 + i, "datetime": "2026-01-01", "text": "ط" * 300,
             "caption": None, "status": "linked", "method": "reply",
             "classification": "modification", "reason": None, "edit_captures": 0}
            for i in range(3)
        ]
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        calls = {"n": 0}
        inner_send = fake.send_message

        def flaky(chat_id, text, *, reply_markup=None):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated transport failure")
            return inner_send(chat_id, text, reply_markup=reply_markup)

        segments = ["seg1", "seg2", "seg3"]
        with (
            patch(
                "tg_signal_ingest.management.bot.related_messages", return_value=rows
            ),
            patch.object(fake, "send_message", side_effect=flaky),
            patch(
                "tg_signal_ingest.management.bot.render_related_messages",
                return_value=segments,
            ),
        ):
            await control._signal_related(OWNER, 301)
        texts = [item["text"] for item in fake.sent_to(OWNER)]
        assert texts[0] == "seg1"
        assert texts[1] == "seg3"  # failed segment is not duplicated remotely
        assert any(
            "ارسال بخش(های) 2" in t and "3 بخش" in t for t in texts
        ), texts


# ---------------------------------------------------------------- keyboard safety


class TestKeyboardLimits:
    def _flatten(self, markup):
        return [
            button
            for row in (markup or {}).get("inline_keyboard", [])
            for button in row
        ]

    async def test_signal_menu_buttons_within_telegram_limits(self, cfg, indexed_repo):
        _seed_signals(indexed_repo, [3001 + i for i in range(9)])
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signals", {}, None)
        markup = fake.sent_to(OWNER)[-1]["reply_markup"]
        keyboard = markup["inline_keyboard"]
        flat = self._flatten(markup)
        assert len(flat) <= 100  # Telegram's inline keyboard size
        for row in keyboard:
            assert len(row) <= 8
        for button in flat:
            assert units(button["text"]) <= 64
            assert len(button["callback_data"].encode("utf-8")) <= 64

    def test_btn_label_with_astral_characters_bounded(self):
        button = _btn("🟢 " + "🔮" * 70 + " ICPU.P", "mg:" + "t" * 32)
        assert units(button["text"]) <= 64
        assert button["text"].startswith("🟢")
        assert "🔮" in button["text"]  # astral code points survive whole

    def test_btn_rejects_oversized_callback_data(self):
        with pytest.raises(ValueError):
            _btn("ok", "mg:" + "x" * 80)

    async def test_all_emitted_callback_data_within_64_bytes(self, cfg, indexed_repo):
        _seed_signals(indexed_repo, [3001, 3002, 3003])
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signals", {}, None)
        await control._dispatch_callback(OWNER, "signal_view", {"logical_id": 3001}, None)
        await control._dispatch_callback(OWNER, "signal_extract", {"logical_id": 3001}, None)
        assert fake.sent
        for item in fake.sent:
            for row in (item["reply_markup"] or {}).get("inline_keyboard", []):
                for button in row:
                    data = button["callback_data"]
                    assert data.startswith("mg:")
                    assert len(data.encode("utf-8")) <= 64


# ---------------------------------------------------------------- pagination


class TestPaginationBoundaries:
    async def test_three_pages_fwd_back_and_last(self, cfg, indexed_repo):
        _seed_signals(indexed_repo, [3100 + i for i in range(17)])
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signals", {}, None)
        last = fake.sent_to(OWNER)[-1]
        assert "از 18" in last["text"]  # "… (8 از 18)" total is shown
        kb1 = last["reply_markup"]["inline_keyboard"]
        next_btn = next(
            b for row in kb1 for b in row if "بعدی" in b["text"]
        )
        await control.handle_update(_callback(2, next_btn["callback_data"]))
        page2 = fake.sent_to(OWNER)[-1]
        assert "از 18" in page2["text"]
        kb2 = page2["reply_markup"]["inline_keyboard"]
        assert any("قبلی" in b["text"] for row in kb2 for b in row)
        assert any("بعدی" in b["text"] for row in kb2 for b in row)
        prev = next(b for row in kb2 for b in row if "قبلی" in b["text"])
        await control.handle_update(_callback(3, prev["callback_data"]))
        assert "از 18" in fake.sent_to(OWNER)[-1]["text"]
        next2 = next(
            b
            for row in kb2
            for b in row
            if "بعدی" in b["text"]
        )
        await control.handle_update(_callback(4, next2["callback_data"]))
        page3 = fake.sent_to(OWNER)[-1]
        assert "از 18" in page3["text"]
        assert not any(
            "بعدی" in b["text"]
            for row in page3["reply_markup"]["inline_keyboard"]
            for b in row
        )

    async def test_stale_offset_snaps_to_last_page(self, cfg, indexed_repo):
        _seed_signals(indexed_repo, [3200 + i for i in range(9)])
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signals_page", {"offset": 9999}, None)
        text = fake.sent_to(OWNER)[-1]["text"]
        assert "سیگنال ساختاریافته‌ای" not in text  # never rendered as empty
        assert "از 10" in text

    async def test_single_record_and_no_nav(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(OWNER, "signals", {}, None)
        text = fake.sent_to(OWNER)[-1]["text"]
        assert "از 1" in text
        kb = fake.sent_to(OWNER)[-1]["reply_markup"]["inline_keyboard"]
        assert not any(
            "بعدی" in b["text"] or "قبلی" in b["text"] for row in kb for b in row
        )

    async def test_empty_index_shows_empty_state(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER, channel_id=777, title="Empty")
        await control._dispatch_callback(OWNER, "signals", {}, None)
        text = fake.sent_to(OWNER)[-1]["text"]
        assert "سیگنال ساختاریافته" in text  # honest empty state, not blank
        assert fake.sent_to(OWNER)[-1]["reply_markup"] is not None


# ---------------------------------------------------------------- callback lifecycle


class TestCallbackLifecycle:
    async def test_deleted_message_tap_is_stale_not_access_denied(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        token = control.store.issue_callback(
            owner_id=OWNER, chat_id=OWNER, action="signals"
        )
        update = {
            "update_id": 5,
            "callback_query": {
                "id": "cb-5",
                "from": {"id": OWNER},
                # Telegram drops `message` when it is deleted / inaccessible
                "data": "mg:" + token,
            },
        }
        await control.handle_update(update)
        answered = [t for _, t in fake.answered]
        assert answered
        assert not any("دسترسی مجاز نیست" in t for t in answered)
        assert any("دیگر معتبر نیست" in t for t in answered)

    async def test_stranger_without_message_refused(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        update = {
            "update_id": 6,
            "callback_query": {"id": "cb-6", "from": {"id": STRANGER}, "data": "mg:x"},
        }
        await control.handle_update(update)
        assert "دسترسی مجاز نیست" in fake.answered[-1][1]

    async def test_replay_refusal(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        token = control.store.issue_callback(
            owner_id=OWNER, chat_id=OWNER, action="signals"
        )
        await control.handle_update(_callback(1, "mg:" + token))
        after_first = len(fake.sent)
        await control.handle_update(_callback(2, "mg:" + token))
        assert len(fake.sent) == after_first
        assert "منقضی" in fake.answered[-1][1]

    async def test_malformed_payloads_answered_promptly(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        for update_id, data in enumerate(("mg:", "not-mg-format", ""), start=1):
            fake.answered.clear()
            await control.handle_update(_callback(update_id, data))
            assert fake.answered and fake.answered[-1][0] == f"cb-{update_id}"
            assert "دکمه نامعتبر" in fake.answered[-1][1]
        # unknown but well-formed token → expiry wording
        fake.answered.clear()
        await control.handle_update(_callback(9, "mg:" + "z" * 32))
        assert "منقضی" in fake.answered[-1][1]

    async def test_wrong_operator_and_group_taps_denied(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        token = control.store.issue_callback(
            owner_id=OWNER, chat_id=OWNER, action="signals"
        )
        await control.handle_update(_callback(1, "mg:" + token, user=DEVELOPER))
        assert "دسترسی مجاز نیست" in fake.answered[-1][1]
        await control.handle_update(
            _callback(2, "mg:" + token, user=OWNER, chat_type="group")
        )
        assert "دسترسی مجاز نیست" in fake.answered[-1][1]

    async def test_expired_token_rejected(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        token = control.store.issue_callback(
            owner_id=OWNER, chat_id=OWNER, action="signals", ttl_s=-10
        )
        await control.handle_update(_callback(3, "mg:" + token))
        assert "منقضی" in fake.answered[-1][1]


# ---------------------------------------------------------------- raw original integrity


class TestOriginalMessageIntegrity:
    async def test_long_astral_original_is_chunked_within_limits(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        body = "🎯 لانگ ICPUSDT\n" + "کلمه و تارگت و ویرایش 🔮 " * 2000
        _put(indexed_repo, 500, body)  # replace the stored original verbatim
        await control._dispatch_callback(
            OWNER, "signal_original", {"logical_id": 301, "message_id": 500}, None
        )
        parts = [
            i["text"] for i in fake.sent_to(OWNER) if i["text"].startswith("📜")
        ]
        assert len(parts) > 1
        assert all(units(t) <= MAX_MESSAGE_UTF16 for t in parts)
        assert "🎯 لانگ ICPUSDT" in parts[0]
        assert "🔮" in parts[0]  # astral content preserved, not dropped
        assert any("(بخش 2" in t for t in parts)
        # every original character survives the split, byte for byte; the only
        # decoration is the per-part continuity marker and the repeated header
        total = len(parts)
        reconstructed = "".join(
            parts[k - 1].split(f"\n(بخش {k} از {total})\n", 1)[1]
            for k in range(1, total + 1)
        )
        assert reconstructed == body

    async def test_short_original_single_part_with_keyboard(self, cfg, indexed_repo):
        control, fake, _ = _bot(cfg, indexed_repo)
        _selected(control, OWNER)
        await control._dispatch_callback(
            OWNER, "signal_original", {"logical_id": 301, "message_id": 500}, None
        )
        parts = [
            i["text"] for i in fake.sent_to(OWNER) if i["text"].startswith("📜")
        ]
        assert len(parts) == 1
        assert INITIAL_301 in parts[0]
        assert "دست‌نخورده" in fake.sent_to(OWNER)[-1]["text"]
        assert fake.sent_to(OWNER)[-1]["reply_markup"] is not None

    def test_missing_original_reports_absence(self, cfg, indexed_repo):
        assert sr.origin_message_text(indexed_repo.db, CHANNEL, 424242) is None


# ---------------------------------------------------------------- preview screen


class TestPreviewBoundaries:
    async def test_astral_heavy_preview_stays_deliverable(self, cfg, repo):
        emoji_messages = [Msg(10 + i, "🔮" * 280) for i in range(10)]
        dialogs = [_channel(1, "Coins", datetime(2026, 1, 1, tzinfo=UTC))]
        control, fake, _ = _bot(cfg, repo, dialogs=dialogs, messages=emoji_messages)
        _selected(control, OWNER, channel_id=1, title="Coins")
        control.store.update_state(telegram_state="authenticated")
        await control._preview_latest(OWNER)
        previews = [
            i for i in fake.sent_to(OWNER) if i["text"].startswith("👁 پیش‌نمایش")
        ]
        assert len(previews) == 1
        text = previews[0]["text"]
        assert units(text) <= MAX_MESSAGE_UTF16
        assert "🔮" in text  # astral content cannot silently vanish
        assert "محدودیت طول" in text  # honest overflow notice when items are shed


# ---------------------------------------------------------------- raw review boundary


class TestRawReviewBounds:
    def test_batches_never_exceed_limit_with_emoji(self):
        msgs = [
            Msg(i, "🔮" * 3000)
            for i in range(1, 4)
        ]
        batches = rr.build_batches(msgs)
        assert batches
        for batch in batches:
            assert units(batch) <= rr.MAX_BATCH_UTF16

    def test_split_contract_reconstructs_exactly(self):
        text = ("سطر پارسی\n" * 900) + "🔮" * 50
        parts = rr._slice_utf16(text, 300)
        assert "".join(parts) == text
        for part in parts[:-1]:
            assert units(part) <= 300
