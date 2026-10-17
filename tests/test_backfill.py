"""Backfill pagination, resumability, checkpoint, and since-mode behavior."""

from __future__ import annotations

from datetime import timedelta

import pytest

from tests.conftest import FAKE_MESSAGE_EPOCH, FakeMsg
from tg_signal_ingest.backfill import run_backfill


class FakePageClient:
    """Deterministic Telegram pagination mock.

    ``get_messages(offset_id=o, min_id=m, limit=l)`` returns messages whose ids
    satisfy ``m <= id < o`` (bounded by Telegram behavior approximations), in
    descending id order.
    """

    def __init__(self, messages: list[FakeMsg]):
        self._by_id = {m.id: m for m in messages}
        self.newest = max((m.id for m in messages), default=0)
        self.calls: list[tuple] = []

    async def get_messages(self, ref, limit=..., offset_id=None, min_id=0, offset_date=None, **kw):
        self.calls.append((limit, offset_id, min_id, bool(offset_date)))
        ids = [i for i in self._by_id if i >= min_id]
        if offset_id is not None:
            ids = [i for i in ids if i < offset_id]
        ids.sort(reverse=True)
        selected = ids[:limit] if isinstance(limit, int) else []
        return [self._by_id[i] for i in selected]


@pytest.fixture()
def populated_repo(repo):
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    repo.init_sync_state(100, "full", None)
    return repo


def test_backfill_migrates_full_history(cfg, populated_repo):
    messages = [FakeMsg(i, f"m{i}") for i in range(1, 26)]
    client = FakePageClient(messages)
    run_backfill.__wrapped__ if hasattr(run_backfill, "__wrapped__") else None
    import asyncio

    rep = asyncio.run(run_backfill(client, populated_repo, cfg, channel_bare_id=100))
    assert rep.discovered == 25
    assert rep.inserted == 25
    assert rep.updated == 0
    assert rep.failed == 0
    assert rep.completed is True
    counts = populated_repo.message_counts(100)
    assert counts.total == 25
    assert counts.max_id == 25
    assert counts.min_id == 1
    state = populated_repo.sync_state(100)
    assert state["backfill_status"] == "done"
    assert state["backfill_next_max_id"] is None


def test_backfill_is_deduplicating_on_rerun(cfg, populated_repo):
    messages = [FakeMsg(i, f"m{i}") for i in range(1, 26)]
    client = FakePageClient(messages)
    import asyncio

    asyncio.run(run_backfill(client, populated_repo, cfg, channel_bare_id=100))
    rep = asyncio.run(run_backfill(client, populated_repo, cfg, channel_bare_id=100))
    assert rep.inserted == 0
    assert rep.unchanged == 0
    assert rep.completed is True
    assert populated_repo.message_counts(100).total == 25


def test_backfill_resumes_from_checkpoint(cfg, populated_repo):
    import asyncio

    messages = [FakeMsg(i, f"m{i}") for i in range(1, 101)]
    FakePageClient(messages)
    cfg.backfill_batch_size = 10

    # First run imported 30 then was killed after each page committed msgs.
    # Simulate by running with max pages then reloading state.
    client2 = FakePageClient(messages)
    # Instead of truly killing, run a pass with a small page window: patch the client
    # so page 1 sees the full set and we interrupt by making run stop after N pages.
    rep_partial = asyncio.run(run_backfill(client2, populated_repo, cfg, channel_bare_id=100, max_pages=2))
    assert rep_partial.completed is False
    state = populated_repo.sync_state(100)
    assert state["backfill_status"] == "in_progress"
    assert state["backfill_next_max_id"] == 81  # checkpoint: exclusive top of the next fetch

    # Resume on a fresh client instance (like after a process restart)
    client3 = FakePageClient(messages)
    rep = asyncio.run(run_backfill(client3, populated_repo, cfg, channel_bare_id=100))
    assert rep.completed is True
    counts = populated_repo.message_counts(100)
    assert counts.total == 100
    assert rep.discovered == 80
    assert rep.inserted == 80
    # no duplicates and ids 1..80 present
    assert counts.min_id == 1


def test_backfill_since_filters_older_messages(cfg, populated_repo):
    import asyncio

    # ids 1..20 at epoch+id seconds; since = epoch+10s -> ids >= 11 (broadly)
    messages = [FakeMsg(i, f"m{i}") for i in range(1, 21)]
    client = FakePageClient(messages)
    cfg.backfill_scope = "since"
    cfg.backfill_since = (FAKE_MESSAGE_EPOCH + timedelta(seconds=10)).isoformat()
    rep = asyncio.run(run_backfill(client, populated_repo, cfg, channel_bare_id=100))
    assert rep.completed is True
    rows = populated_repo.list_messages(100, limit=100, newest_first=True)
    ids = sorted(r["message_id"] for r in rows)
    found_ids = set(ids)
    assert found_ids == set(range(10, 21))  # id 10 is posted exactly at 'since'


def test_backfill_failure_counts_reported(cfg, populated_repo):
    import asyncio

    messages = [FakeMsg(i, f"m{i}") for i in range(1, 6)]

    client = FakePageClient(messages)
    rep = asyncio.run(run_backfill(client, populated_repo, cfg, channel_bare_id=100))
    assert rep.completed is True
    assert rep.failed == 0
