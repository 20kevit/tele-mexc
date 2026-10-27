"""Regression tests for defects found in the production-readiness audit."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from tests.conftest import FakeMsg
from tests.test_service import FakeClient
from tg_signal_ingest.backfill import BackfillReport
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import UpsertOutcome
from tg_signal_ingest.service import IngestService


class TestStaleEditGuard:
    """Backfill/before live race: an older history snapshot must never
    overwrite a newer live-applied edit (audit defect 2)."""

    def test_backfill_stale_version_does_not_regress_live_edit(self, repo):
        newer = FakeMsg(300, "edited text", edit_date=datetime(2026, 3, 2, tzinfo=UTC))
        repo.upsert_message(message_to_record(100, newer, "live"))
        v_after_live = repo.get_message_raw(100, 300)["processing_version"]

        stale = FakeMsg(300, "original text")  # no edit_date: what was fetched pre-edit
        result = repo.upsert_message(message_to_record(100, stale, "backfill"))
        assert result.outcome is UpsertOutcome.UNCHANGED
        raw = repo.get_message_raw(100, 300)
        assert raw["text"] == "edited text"
        assert raw["processing_version"] == v_after_live
        # no bogus edit job
        rows = repo.list_queue()
        assert [r["item_type"] for r in rows] == ["message"]

    def test_backfill_awaiting_first_edit_still_updates(self, repo):
        live = FakeMsg(301, "live text")  # no edit_date yet
        repo.upsert_message(message_to_record(100, live, "live"))
        older = FakeMsg(301, "live text", edit_date=datetime(2026, 3, 1, tzinfo=UTC))
        result = repo.upsert_message(message_to_record(100, older, "backfill"))
        assert result.outcome is UpsertOutcome.UPDATED  # existing edit_datetime is None -> allowed


class TestConsumerSurvivability:
    """One unexpected consumer error must not kill the loop nor strand
    claimed items (audit defect 3)."""

    def test_run_survives_claim_failure_and_requeues(self, repo):
        from tg_signal_ingest.normalize import message_to_record

        repo.upsert_message(message_to_record(100, FakeMsg(700, "work"), "live"))
        engine_calls = {"n": 0}
        original_claim = repo.claim_pending

        def flaky_claim(limit=50):
            engine_calls["n"] += 1
            if engine_calls["n"] == 1:
                raise RuntimeError("transient sqlite failure")
            return original_claim(limit=limit)

        repo.claim_pending = flaky_claim  # type: ignore[method-assign]

        from tg_signal_ingest.processing import ProcessingEngine

        engine = ProcessingEngine(repo)
        stop = asyncio.Event()
        loop = asyncio.new_event_loop()
        try:
            task = loop.create_task(engine.run(stop))
            deadline = loop.time() + 5
            while loop.time() < deadline:
                loop.run_until_complete(asyncio.sleep(0.05))
                counts = repo.queue_counts()
                if counts["processed"] == 1:
                    break
            stop.set()
            loop.run_until_complete(task)
        finally:
            loop.close()
        assert repo.queue_counts()["processed"] == 1
        assert engine_calls["n"] >= 2  # the loop survived the failure


class SlowBackfill:
    """Backfill runner that stays alive long enough to detect overlaps."""

    def __init__(self):
        self.active = 0
        self.max_active = 0

    async def __call__(self, client, repo_arg, cfg_arg, channel_bare_id=None, **kw):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.08)
        finally:
            self.active -= 1
        return BackfillReport(completed=True)


class TestTaskReaping:
    """Reconnect cycles must not leak backfill/queue tasks (audit defect 1).

    Without the reap, cycle N+1 starts a backfill worker while cycle N's
    runner is still sleeping -> observed concurrency 2. With the fix, the
    previous task is cancelled before the new one starts -> max concurrency 1.
    """

    async def test_backfill_workers_never_overlap_across_cycles(self, cfg, repo):
        repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
        repo.select_channel(100)
        runner = SlowBackfill()

        class FastDisconnectingClient(FakeClient):
            def run_until_disconnected(self):
                async def _run():
                    await asyncio.sleep(0.01)  # cycle ends almost immediately
                return _run()

        client = FastDisconnectingClient()
        svc = IngestService(cfg, repo, client=client, backfill_runner=runner)
        task = asyncio.ensure_future(svc.run())
        await asyncio.sleep(0.3)
        svc.request_stop()
        await asyncio.wait_for(task, timeout=10)

        assert runner.max_active == 1, (
            f"backfill workers overlapped across connection cycles: {runner.max_active}"
        )
        assert svc._backfill_task is None
        assert svc._queue_task is None
        assert client.disconnect_count >= 1
