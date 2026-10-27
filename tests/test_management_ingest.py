"""Data-only, resumable management history import."""

from __future__ import annotations

from types import SimpleNamespace

from tests.conftest import FakeMsg
from tg_signal_ingest.management.ingest import IngestionWorker
from tg_signal_ingest.management.store import ManagementStore


class FakeHistoryClient:
    def __init__(self, messages, *, fail=None):
        self.messages = {m.id: m for m in messages}
        self.fail = fail
        self.get_entity_calls = 0
        self.page_calls: list[tuple[int, int]] = []

    async def get_entity(self, _peer):
        self.get_entity_calls += 1
        return SimpleNamespace(access_hash=999, username="source", title="Source")

    async def get_messages(self, _entity, limit=1, offset_id=0, min_id=0):
        self.page_calls.append((limit, offset_id))
        if self.fail == "always":
            raise OSError("network lost")
        rows = [m for mid, m in sorted(self.messages.items(), reverse=True) if mid < (offset_id or 10**18)]
        if self.fail == "after-first" and len(self.page_calls) > 2:
            raise OSError("network lost")
        return rows[:limit]


async def _run(repo, cfg, client, *, checkpoint=None, cancel_before=False):
    store = ManagementStore(repo.db)
    job = store.create_job(77, "Source")
    if checkpoint is not None:
        store.update_job(job["job_id"], checkpoint_next_max_id=checkpoint, newest_seen_id=10)
    if cancel_before:
        store.request_cancel(job["job_id"])
    worker = IngestionWorker(cfg, repo, store)
    report = await worker.run(client, job["job_id"])
    return store, store.get_job(job["job_id"]), report


async def test_full_history_is_complete_data_only_and_does_not_enqueue(repo, cfg):
    client = FakeHistoryClient([FakeMsg(i, f"m{i}") for i in range(1, 11)])
    store, job, report = await _run(repo, cfg, client)

    assert report.status == "completed" and report.completed
    assert job is not None and job["status"] == "completed"
    assert job["inserted"] == 10 and job["checkpoint_next_max_id"] is None
    assert repo.message_counts(77).total == 10
    queue = repo.db.conn.execute("SELECT COUNT(*) FROM processing_queue").fetchone()[0]
    assert queue == 0
    assert repo.selected_channel() is None
    assert store.latest_job()["status"] == "completed"


async def test_resume_starts_below_saved_checkpoint(repo, cfg):
    client = FakeHistoryClient([FakeMsg(i, f"m{i}") for i in range(1, 11)])
    _store, job, report = await _run(repo, cfg, client, checkpoint=6)

    assert report.status == "completed"
    assert job["inserted"] == 5
    assert repo.message_counts(77).total == 5
    assert client.page_calls[0] == (200, 6)
    assert repo.message_counts(77).min_id == 1


async def test_cancel_before_start_preserves_job(repo, cfg):
    _store, job, report = await _run(repo, cfg, FakeHistoryClient([]), cancel_before=True)
    assert report.status == "cancelled"
    assert job is not None and job["status"] == "cancelled" and job["cancel_requested"] == 1


async def test_network_failure_is_durable_and_retryable(repo, cfg):
    _store, job, report = await _run(repo, cfg, FakeHistoryClient([], fail="always"))
    assert report.status == "failed"
    assert job is not None and job["status"] == "failed"
    assert "network failure" in (job["error"] or "")
    assert job["checkpoint_next_max_id"] is None


async def test_row_failure_marks_partial_but_keeps_progress(repo, cfg, monkeypatch):
    original = repo.upsert_message

    def fail_row(record, **kwargs):
        if record.message_id == 2:
            raise ValueError("corrupt row")
        return original(record, **kwargs)

    monkeypatch.setattr(repo, "upsert_message", fail_row)
    messages = [FakeMsg(1, "one"), FakeMsg(2, "two"), FakeMsg(3, "three")]
    _store, job, report = await _run(repo, cfg, FakeHistoryClient(messages))

    assert report.status == "partial"
    assert job is not None and job["status"] == "partial"
    assert job["failed"] == 1
    assert repo.message_counts(77).total == 2


async def test_cancel_requested_during_run_stops_after_page(repo, cfg):
    store = ManagementStore(repo.db)
    job = store.create_job(77, "Source")

    class CancellingClient(FakeHistoryClient):
        async def get_messages(self, entity, limit=1, offset_id=0, min_id=0):
            store.request_cancel(job["job_id"])
            return await super().get_messages(entity, limit=limit, offset_id=offset_id, min_id=min_id)

    worker = IngestionWorker(cfg, repo, store)
    report = await worker.run(CancellingClient([FakeMsg(i) for i in range(1, 4)]), job["job_id"])
    assert report.status == "cancelled"
    assert store.get_job(job["job_id"])["status"] == "cancelled"
    assert repo.message_counts(77).total == 0


async def test_channel_denial_is_failed_not_completed(repo, cfg):
    class Denied(FakeHistoryClient):
        async def get_entity(self, _peer):
            raise PermissionError("private channel")

    _store, job, report = await _run(repo, cfg, Denied([]))
    assert report.status == "failed"
    assert job["status"] == "failed"
    assert "channel access failed" in (job["error"] or "")


async def test_bounded_validation_import_caps_exactly_and_avoids_sync_state(repo, cfg):
    store = ManagementStore(repo.db)
    job = store.create_job(77, "Source", max_messages=3, request_id="val-sample")
    client = FakeHistoryClient([FakeMsg(i, f"m{i}") for i in range(1, 11)])
    worker = IngestionWorker(cfg, repo, store)

    report = await worker.run(client, job["job_id"])
    stored = store.get_job(job["job_id"])

    assert report.status == "completed"
    assert report.bounded is True
    assert report.discovered == 3 and report.inserted == 3
    assert stored is not None
    assert stored["status"] == "completed"
    assert stored["max_messages"] == 3
    assert stored["request_id"] == "val-sample"
    assert stored["oldest_seen_id"] == 8
    assert stored["newest_seen_id"] == 10
    assert repo.message_counts(77).min_id == 8
    assert repo.message_counts(77).max_id == 10
    sync_rows = repo.db.conn.execute("SELECT COUNT(*) FROM sync_state").fetchone()[0]
    assert sync_rows == 0
    queue = repo.db.conn.execute("SELECT COUNT(*) FROM processing_queue").fetchone()[0]
    assert queue == 0


async def test_bounded_validation_import_completes_when_channel_is_smaller(repo, cfg):
    store = ManagementStore(repo.db)
    job = store.create_job(77, "Source", max_messages=10, request_id="val-small")
    client = FakeHistoryClient([FakeMsg(i, f"m{i}") for i in range(1, 3)])
    worker = IngestionWorker(cfg, repo, store)

    report = await worker.run(client, job["job_id"])

    assert report.status == "completed" and report.bounded is True
    assert repo.message_counts(77).total == 2
    assert store.get_job(job["job_id"])["oldest_seen_id"] == 1
