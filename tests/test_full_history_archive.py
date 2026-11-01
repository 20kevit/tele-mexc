"""Full-history pagination tests for the production backfill path.

``run_backfill`` is exercised against a deterministic fake Telegram pagination
client: boundary crossing with message-ID gaps, empty history,
interrupted/resumed runs, duplicate delivery, flood-wait respect, and
raw/reply metadata preservation.

Everything is mock-only: no Telegram API, no exchange access, and the trading
tables must stay untouched throughout.
"""

from __future__ import annotations

import asyncio
import json
from datetime import timedelta

from telethon.errors import FloodWaitError

from tests.conftest import FAKE_MESSAGE_EPOCH, FakeMsg, ReplyTo
from tg_signal_ingest import backfill
from tg_signal_ingest.backfill import run_backfill

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
