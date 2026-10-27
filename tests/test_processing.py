"""Downstream processing queue tests (engine-level, deterministic)."""

from __future__ import annotations

import asyncio

from tg_signal_ingest.processing import InterfaceTestHandler, ProcessingEngine
from tg_signal_ingest.repository import QueueItem


class TestEngine:
    def test_message_processed_exactly_once(self, repo):
        from tests.conftest import FakeMsg
        from tg_signal_ingest.normalize import message_to_record

        repo.upsert_message(message_to_record(100, FakeMsg(1, "a"), "live"))
        tester = InterfaceTestHandler()
        engine = ProcessingEngine(repo)
        engine.register_handler("message", tester.handler())
        stats = asyncio.run(engine.run_once())
        assert stats.processed == 1
        assert len(tester.handled) == 1
        # second pass: nothing to do (already processed)
        stats = asyncio.run(engine.run_once())
        assert stats.processed == 0
        assert len(tester.handled) == 1

    def test_edit_notifies_and_skips_superseded(self, repo):
        from tests.conftest import FakeMsg
        from tg_signal_ingest.normalize import message_to_record

        repo.upsert_message(message_to_record(100, FakeMsg(2, "v1"), "live"))
        repo.upsert_message(message_to_record(100, FakeMsg(2, "v2"), "live"))
        # queue: message(v0), edit(v1)
        seen: list[tuple] = []

        async def f(item, record):
            seen.append((item.item_type, int(record["processing_version"])))

        engine = ProcessingEngine(repo)
        engine.register_handler("message", f)
        engine.register_handler("edit", f)
        stats = asyncio.run(engine.run_once())
        assert stats.processed == 2
        assert ("edit", 1) in seen
        # the message item was queued at v0, but by processing time the row is v1
        assert ("message", 1) in seen

    def test_failure_retries_until_max_attempts_then_failed(self, repo):
        from tests.conftest import FakeMsg
        from tg_signal_ingest.normalize import message_to_record

        repo.processing_max_attempts = 2
        repo.retry_backoff_scale = 0.0
        repo.upsert_message(message_to_record(100, FakeMsg(3, "boom"), "live"))

        async def failing(item, record):
            raise RuntimeError("kaput")

        engine = ProcessingEngine(repo)
        engine.register_handler("message", failing)
        stats = asyncio.run(engine.run_once())  # attempt 1 -> pending
        stats = asyncio.run(engine.run_once())  # attempt 2 -> failed
        assert stats.failed == 1  # final attempt lands in 'failed', no further retry
        counts = repo.queue_counts()
        assert counts["failed"] == 1
        assert counts["pending"] == 0
        assert "kaput" in repo.list_queue("failed")[0]["last_error"]

    def test_edit_superseded_by_newer_version(self, repo):
        from tests.conftest import FakeMsg
        from tg_signal_ingest.normalize import message_to_record

        repo.upsert_message(message_to_record(100, FakeMsg(4, "a"), "live"))
        repo.upsert_message(message_to_record(100, FakeMsg(4, "b"), "live"))
        # stale edit item for v1: force to check via version trick is hard;
        # instead simulate: claim the edit item, change version field target
        rows = repo.list_queue("pending", limit=10)
        edit_row = [r for r in rows if r["item_type"] == "edit"][0]
        stale = QueueItem(
            queue_id=edit_row["queue_id"], channel_id=edit_row["channel_id"],
            message_id=edit_row["message_id"], item_type="edit", status="processing",
            targets_version=99, attempts=0, available_at=edit_row["available_at"],
            created_at=edit_row["created_at"],
        )
        handled: list = []

        async def h(item, record):
            handled.append(record["message_id"])

        engine = ProcessingEngine(repo)
        engine.register_handler("edit", h)
        import asyncio

        stats = asyncio.run(engine._handle_item(stale))  # returns (success, detail)
        success, detail = stats
        assert success is True
        assert detail.startswith("superseded")
        assert handled == []

    def test_delete_item_without_message_row_is_ok(self, repo):
        repo.enqueue(100, 4242, "delete", 0)
        engine = ProcessingEngine(repo)
        stats = asyncio.run(engine.run_once())
        assert stats.processed == 1

    def test_reprocess_enqueues_current_version(self, repo):
        from tests.conftest import FakeMsg
        from tg_signal_ingest.normalize import message_to_record

        repo.upsert_message(message_to_record(100, FakeMsg(5, "one"), "live"))
        repo.upsert_message(message_to_record(100, FakeMsg(5, "two"), "live"))
        repo.claim_pending()  # drain all pending claims
        version = repo.reprocess_message(100, 5)
        assert version == 1
        rows = repo.list_queue(limit=10)
        assert any(r["item_type"] == "edit" and r["targets_version"] == 1 and r["status"] == "pending" for r in rows)
