"""Complete-archive mission tests: full-history pagination and bot pagination.

Two independent areas, kept apart:

1. Full historical backfill (``run_backfill``) against a deterministic fake
   Telegram pagination client: both boundary crossing with message-ID gaps,
   empty history, interrupted/resumed runs, duplicate delivery, flood-wait
   respect, and raw/reply metadata preservation.

2. Management-bot archive pagination over a 20-signal seeded index built
   through the normal replay + reindex path: first/last page boundaries,
   forward/back page movement, and signal-to-signal prev/next endpoints.

Everything is mock-only: no Telegram API, no exchange access, and the trading
tables must stay untouched throughout.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import pytest
from telethon.errors import FloodWaitError

from tests.conftest import FAKE_MESSAGE_EPOCH, FakeMsg, ReplyTo
from tests.test_management_bot import OWNER, _bot, _callback, _message
from tg_signal_ingest import backfill
from tg_signal_ingest.analysis import ReplayScope, run_replay
from tg_signal_ingest.analysis.reindex import rebuild_signal_index
from tg_signal_ingest.backfill import run_backfill
from tg_signal_ingest.management import signal_review as sr
from tg_signal_ingest.normalize import message_to_record

CHANNEL = 100


# ---------------------------------------------------------------------------
# Full-history pagination
# ---------------------------------------------------------------------------


class FakePageClient:
    """Telegram pagination mock.

    ``get_messages(entity_ref, limit=l, offset_id=o, min_id=m)`` returns the
    ids strictly below ``o`` (all ids when ``o is None``) that are not below
    ``m``, in descending id order — the same window semantics the real client
    exposes for history pagination.
    """

    def __init__(self, by_id: dict[int, FakeMsg]):
        self._by_id = dict(by_id)
        self.calls = 0

    async def get_messages(self, entity_ref, limit, offset_id=None, min_id=0, **kwargs):
        self.calls += 1
        ids = [i for i in self._by_id if i >= min_id]
        if offset_id is not None:
            ids = [i for i in ids if i < offset_id]
        ids.sort(reverse=True)
        return [self._by_id[i] for i in ids[:limit]]


def _seeded(repo):
    """Register the mission channel and mark it selected (run_backfill's
    documented prerequisite), without seeding any message row."""
    repo.ensure_channel(CHANNEL, 123, "sigchan", "Sig Chan")
    repo.select_channel(CHANNEL)
    return repo


class TestFullHistoryPagination:
    def test_pagination_crosses_both_boundaries_even_with_gaps(self, repo, cfg, monkeypatch):
        # ids 1..6 and 20..30: the 7..19 gap must not stop or confuse traversal.
        monkeypatch.setattr(cfg, "backfill_batch_size", 5)
        ids = list(range(1, 7)) + list(range(20, 31))
        client = FakePageClient({i: FakeMsg(i, "m") for i in ids})
        repo = _seeded(repo)

        rep = asyncio.run(run_backfill(client, repo, cfg))

        assert rep.completed is True
        assert rep.failed == 0
        assert rep.discovered == len(ids) == 17
        assert rep.oldest_id_seen == 1  # oldest boundary: lowest available id
        counts = repo.message_counts(CHANNEL)
        assert counts.total == 17
        assert counts.max_id == 30  # newest boundary reached
        state = repo.sync_state(CHANNEL)
        assert state["backfill_status"] == "done"
        assert state["backfill_message_count"] == 17

    def test_empty_history_completes_with_no_rows(self, repo, cfg):
        client = FakePageClient({})
        repo = _seeded(repo)

        rep = asyncio.run(run_backfill(client, repo, cfg))

        assert rep.completed is True
        assert rep.discovered == 0
        assert repo.message_counts(CHANNEL).total == 0
        assert repo.sync_state(CHANNEL)["backfill_status"] == "done"

    def test_single_message_history_completes(self, repo, cfg):
        client = FakePageClient({1: FakeMsg(1, "only")})
        repo = _seeded(repo)

        rep = asyncio.run(run_backfill(client, repo, cfg))

        assert rep.completed is True
        assert rep.inserted == 1
        assert rep.oldest_id_seen == 1
        assert repo.message_counts(CHANNEL).total == 1

    def test_interrupted_run_resumes_and_never_duplicates(self, repo, cfg, monkeypatch):
        monkeypatch.setattr(cfg, "backfill_batch_size", 10)
        ids = list(range(1, 36))
        client = FakePageClient({i: FakeMsg(i, "m") for i in ids})
        repo = _seeded(repo)

        first = asyncio.run(run_backfill(client, repo, cfg, max_pages=1))
        assert first.completed is False  # interrupted; nothing lost
        partial = repo.message_counts(CHANNEL).total
        assert 0 < partial < len(ids)
        state = repo.sync_state(CHANNEL)
        assert state["backfill_status"] == "in_progress"
        assert state["backfill_next_max_id"] is not None

        final = asyncio.run(run_backfill(client, repo, cfg))
        assert final.completed is True
        assert final.failed == 0
        assert repo.message_counts(CHANNEL).total == len(ids)

        # a third pass over the now-done run must be a no-op
        rerun = asyncio.run(run_backfill(client, repo, cfg))
        assert rerun.completed is True
        assert rerun.discovered == 0
        assert repo.message_counts(CHANNEL).total == len(ids)

    def test_duplicate_delivery_repeated_runs_are_harmless(self, repo, cfg):
        ids = list(range(1, 31))
        client = FakePageClient({i: FakeMsg(i, "m") for i in ids})
        repo = _seeded(repo)
        asyncio.run(run_backfill(client, repo, cfg))
        total = repo.message_counts(CHANNEL).total

        # simulate duplicate delivery: force a full re-pass over the same ids
        with repo.db.transaction() as conn:
            conn.execute(
                "UPDATE sync_state SET backfill_status='in_progress', backfill_next_max_id=NULL WHERE channel_id=?",
                (CHANNEL,),
            )
        second = asyncio.run(run_backfill(client, repo, cfg))

        assert second.completed is True
        assert second.inserted == 0
        assert second.updated == 0
        assert second.unchanged == len(ids)
        assert repo.message_counts(CHANNEL).total == total

    def test_flood_wait_is_respected_before_retry(self, repo, cfg, monkeypatch):
        class FloodOnceClient(FakePageClient):
            """Raises Telegram's flood-wait once, on the first paged fetch
            (after the initial newest-message probe), then behaves."""

            def __init__(self, by_id):
                super().__init__(by_id)
                self.do_flood = True

            async def get_messages(self, entity_ref, limit, offset_id=None, min_id=0, **kwargs):
                if self.do_flood and offset_id is not None:
                    self.do_flood = False
                    raise FloodWaitError(None, 4)
                return await super().get_messages(entity_ref, limit=limit, offset_id=offset_id, min_id=min_id, **kwargs)

        client = FloodOnceClient({1: FakeMsg(1, "solo")})
        repo = _seeded(repo)
        sleeps: list[float] = []

        async def recorded_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        monkeypatch.setattr(backfill.asyncio, "sleep", recorded_sleep)
        rep = asyncio.run(run_backfill(client, repo, cfg))

        assert sleeps and sleeps[0] >= 4  # Telegram's requested wait honoured
        assert rep.completed is True
        assert rep.failed == 0
        assert repo.message_counts(CHANNEL).total == 1
        assert client.do_flood is False  # retried after the wait, not aborted

    def test_raw_text_reply_and_raw_json_preserved(self, repo, cfg):
        base = FAKE_MESSAGE_EPOCH + timedelta(seconds=100)
        msg = FakeMsg(
            7,
            "🆔 #7\nسکوت",
            date=base,
            edit_date=base + timedelta(minutes=5),
            reply_to=ReplyTo(3),
        )
        client = FakePageClient({3: FakeMsg(3, "پایه"), 7: msg})
        repo = _seeded(repo)

        rep = asyncio.run(run_backfill(client, repo, cfg))
        assert rep.completed is True

        rows = repo.db.conn.execute(
            "SELECT message_id, text, reply_to_message_id, datetime, edit_datetime, raw_json "
            "FROM messages WHERE channel_id=? AND message_id IN (3, 7) ORDER BY message_id",
            (CHANNEL,),
        ).fetchall()
        base_row, follow = [dict(r) for r in rows]
        assert base_row["text"] == "پایه"
        assert base_row["reply_to_message_id"] is None
        assert follow["text"].startswith("🆔 #7")
        assert follow["reply_to_message_id"] == 3
        assert follow["edit_datetime"] is not None
        raw = json.loads(follow["raw_json"])
        assert raw["id"] == 7
        assert raw["message"].startswith("🆔 #7")


# ---------------------------------------------------------------------------
# Management-bot full-archive pagination
# ---------------------------------------------------------------------------


def _initial(n: int, short: bool = False) -> str:
    entry = 3 + n * 0.01
    sign = -1 if short else 1
    bow = "🔴" if short else "🟢"
    side = "SHORT" if short else "LONG"
    return (
        f"🆔 #{n}\n⏰ 1405/08/01\n TIME =08:{n % 60:02d}\n *{side}*{bow}\n"
        f" ⚡️TOOBIT:SYMXUSDT.P(15)\n\n"
        f" ✅Entry: {entry:.2f}\n 🔰Stop Loss : {entry - sign * 0.05:.2f}\n\n"
        f" 📌TP1 : {entry + sign * 0.02:.2f}\n 📌TP2 : {entry + sign * 0.04:.2f}\n\n"
        f" 🤑FULL Target : {entry + sign * 0.07:.2f}\n"
        f" 💰Deposit 1.00%\n 💠MAX LEV Suggested  =  8 (-17.39%)"
    )


@pytest.fixture()
def archive_repo(repo):
    """20 initial signals (#1..#20 chronological), one SHORT every 5th."""

    for n in range(1, 21):
        msg_id = 500 + n - 1
        msg = FakeMsg(msg_id, _initial(n, short=n % 5 == 0))
        repo.upsert_message(message_to_record(CHANNEL, msg, "live"), enqueue_processing=False)
    run_replay(repo, ReplayScope(channel_id=CHANNEL))
    rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
    return repo


def _flat_kb(reply_markup):
    return [b for row in reply_markup["inline_keyboard"] for b in row]


def _next_button(buttons):
    return next(b for b in buttons if "بعدی" in b["text"])


class TestBotArchivePagination:
    def _selected(self, control):
        control.store.set_operator_channel(OWNER, selected_channel_id=CHANNEL, selected_channel_title="Signals")

    @pytest.mark.asyncio
    async def test_archive_paginates_across_pages_and_boundaries(self, cfg, archive_repo):
        control, fake, _ = _bot(cfg, archive_repo)
        self._selected(control)

        await control.handle_update(_message(1, "/signals"))
        page1 = fake.sent_to(OWNER)[-1]
        assert "(8 از 20)" in page1["text"]
        buttons1 = _flat_kb(page1["reply_markup"])
        assert sum(1 for b in buttons1 if b["text"].startswith(("🟢", "🔴"))) == 8
        assert any("بعدی" in b["text"] for b in buttons1)
        assert not any("قبلی" in b["text"] for b in buttons1)  # first page: no back

        await control.handle_update(_callback(2, _next_button(buttons1)["callback_data"]))
        page2 = fake.sent_to(OWNER)[-1]
        buttons2 = _flat_kb(page2["reply_markup"])
        assert any("قبلی" in b["text"] for b in buttons2)

        await control.handle_update(_callback(3, _next_button(buttons2)["callback_data"]))
        page3 = fake.sent_to(OWNER)[-1]
        assert "(4 از 20)" in page3["text"]  # last partial page
        buttons3 = _flat_kb(page3["reply_markup"])
        assert not any("بعدی" in b["text"] for b in buttons3)  # last page: no forward
        assert any("قبلی" in b["text"] for b in buttons3)

        prev = next(b for b in buttons3 if "قبلی" in b["text"])
        await control.handle_update(_callback(4, prev["callback_data"]))
        page2b = fake.sent_to(OWNER)[-1]
        assert "(8 از 20)" in page2b["text"]
        # same rows as before (one-shot tokens are re-minted per render)
        assert [b["text"] for b in _flat_kb(page2b["reply_markup"])] == [b["text"] for b in buttons2]

        # pagination is read-only: no trading tables were touched
        for table in ("signals", "order_intents", "exchange_orders"):
            assert archive_repo.db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0

    @pytest.mark.asyncio
    async def test_signal_prev_next_moves_by_chronology(self, cfg, archive_repo):
        control, fake, _ = _bot(cfg, archive_repo)
        self._selected(control)

        ids = sr.logical_ids(archive_repo.db, CHANNEL)  # chronological, oldest first
        assert ids == list(range(1, 21))

        # middle signal: both prev and next buttons
        await control._signal_view(OWNER, ids[9])
        detail = fake.sent_to(OWNER)[-1]
        assert f"سیگنال #{ids[9]}" in detail["text"]
        labels = [b["text"] for b in _flat_kb(detail["reply_markup"])]
        assert any("سیگنال قبلی" in label for label in labels)
        assert any("سیگنال بعدی" in label for label in labels)

        # oldest signal: no previous
        await control._signal_view(OWNER, ids[0])
        labels = [b["text"] for b in _flat_kb(fake.sent_to(OWNER)[-1]["reply_markup"])]
        assert not any("سیگنال قبلی" in label for label in labels)
        assert any("سیگنال بعدی" in label for label in labels)

        # newest signal: no next
        await control._signal_view(OWNER, ids[-1])
        labels = [b["text"] for b in _flat_kb(fake.sent_to(OWNER)[-1]["reply_markup"])]
        assert not any("سیگنال بعدی" in label for label in labels)
        assert any("سیگنال قبلی" in label for label in labels)

    @pytest.mark.asyncio
    async def test_unknown_signal_beyond_archive_reports_missing(self, cfg, archive_repo):
        control, fake, _ = _bot(cfg, archive_repo)
        self._selected(control)

        await control._signal_view(OWNER, 999)

        assert any("ثبت نشده است" in item["text"] for item in fake.sent_to(OWNER))
