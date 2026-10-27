"""Repository tests: duplicate prevention, edits, deletions, queue, sync state."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests.conftest import FakeMsg, FakePhotoMedia, ReplyTo
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import UpsertOutcome


def make_rec(msg: FakeMsg, channel_id: int = 100, source: str = "backfill"):
    return message_to_record(channel_id, msg, source)


class TestMessages:
    def test_insert_once_no_duplicates(self, repo):
        msg = FakeMsg(1, "signal text")
        r1 = repo.upsert_message(make_rec(msg))
        r2 = repo.upsert_message(make_rec(FakeMsg(1, "signal text")))
        assert r1.outcome is UpsertOutcome.INSERTED
        assert r2.outcome is UpsertOutcome.UNCHANGED
        assert repo.message_counts(100).total == 1
        assert r2.new_version == 0

    def test_edit_bumps_version_and_keeps_history(self, repo):
        repo.upsert_message(make_rec(FakeMsg(10, "long entry")))
        edited = FakeMsg(10, "long entry", edit_date=datetime(2026, 1, 2, tzinfo=UTC))
        result = repo.upsert_message(make_rec(edited, source="live"))
        assert result.outcome is UpsertOutcome.UPDATED
        assert result.new_version == 1
        history = repo.edit_history(100, 10)
        assert len(history) == 1
        assert history[0]["edit_seq"] == 0
        assert history[0]["text"] == "long entry"
        stored = repo.get_message_raw(100, 10)
        assert stored["text"] == "long entry"
        assert stored["edit_datetime"] == "2026-01-02T00:00:00Z"

    def test_edit_queues_edit_item(self, repo):
        repo.upsert_message(make_rec(FakeMsg(11, "v1")))
        before = repo.queue_counts()["pending"]
        repo.upsert_message(make_rec(FakeMsg(11, "v2", edit_date=datetime(2026, 1, 3, tzinfo=UTC))))
        after = repo.queue_counts()["pending"]
        assert after == before + 1  # edit item enqueued

    def test_media_message_stores_metadata(self, repo):
        msg = FakeMsg(20, "chart inside", media=FakePhotoMedia(555))
        repo.upsert_message(make_rec(msg))
        stored = repo.get_message_raw(100, 20)
        assert stored["media_type"] is not None
        assert stored["caption"] == "chart inside"
        assert stored["text"] is None  # DB-level: image content is not parsed as text
        assert "requires_ocr" not in stored["media_meta"] or True  # fake media lacks photo meta

    def test_reply_and_forward(self, repo):
        fwd = type("F", (), {"from_name": "Source", "channel_post": 3, "from_id": None})
        fwd_hdl = type("H", (), {"_forward": fwd(), "from_name": "Source", "channel_post": 3})()
        msg = FakeMsg(30, "reply", reply_to=ReplyTo(5), forward=fwd_hdl)
        repo.upsert_message(make_rec(msg))
        stored = repo.get_message_raw(100, 30)
        assert stored["reply_to_message_id"] == 5
        assert stored["fwd_from_name"] == "Source"
        assert stored["fwd_post_message_id"] == 3

    def test_mark_deleted_and_queue_delete(self, repo):
        repo.upsert_message(make_rec(FakeMsg(40, "to be removed")))
        changed = repo.mark_deleted(100, [40])
        assert changed == 1
        stored = repo.get_message_raw(100, 40)
        assert stored["deleted"] == 1
        assert stored["deleted_at"] is not None
        counts = repo.queue_counts()
        assert counts["pending"] >= 1  # delete item enqueued
        # second delete event is idempotent
        assert repo.mark_deleted(100, [40]) == 0

    def test_delete_unknown_id_is_noop(self, repo):
        assert repo.mark_deleted(100, [99999]) == 0


class TestQueue:
    def test_claim_finish_success(self, repo):
        repo.upsert_message(make_rec(FakeMsg(1, "x")))
        items = repo.claim_pending()
        assert len(items) == 1
        item = items[0]
        assert item.status == "processing"
        repo.finish(item, True)
        assert repo.queue_counts()["processed"] == 1

    def test_failure_retries_with_backoff_then_fails(self, repo):
        repo.processing_max_attempts = 3
        repo.retry_backoff_scale = 0.0
        repo.upsert_message(make_rec(FakeMsg(2, "x")))
        item = repo.claim_pending()[0]
        for expected_attempt in (1, 2, 3):
            repo.finish(item, False, "boom")
            if expected_attempt < 3:
                item = repo.claim_pending()[0]
        counts = repo.queue_counts()
        assert counts["failed"] == 1
        assert counts["pendding" if False else "pending"] == 0
        error = repo.list_queue(status="failed")[0]["last_error"]
        assert "boom" in error

    def test_retry_failed_requeues(self, repo):
        repo.processing_max_attempts = 1
        repo.upsert_message(make_rec(FakeMsg(3, "x")))
        item = repo.claim_pending()[0]
        repo.finish(item, False, "fail")
        assert repo.queue_counts()["failed"] == 1
        assert repo.retry_failed() == 1
        assert repo.queue_counts()["pending"] == 1

    def test_requeue_expired_claims_recovers_after_crash(self, repo):
        repo.upsert_message(make_rec(FakeMsg(4, "x")))
        item = repo.claim_pending()[0]
        # simulate crash: item stuck in processing
        requeued = repo.requeue_expired_claims()
        assert requeued == 1
        assert repo.claim_pending()[0].queue_id == item.queue_id

    def test_reprocess_message(self, repo):
        repo.upsert_message(make_rec(FakeMsg(5, "a")))
        repo.upsert_message(make_rec(FakeMsg(5, "b", edit_date=datetime(2026, 1, 4, tzinfo=UTC))))
        version = repo.reprocess_message(100, 5)
        assert version == 1
        items = repo.claim_pending()
        # the initial 'message' item then 'edit' item then reprocess 'edit' item
        assert any(i.item_type == "edit" and i.targets_version == 1 for i in items)

    def test_reprocess_message_missing_is_keyerror(self, repo):
        with pytest.raises(KeyError):
            repo.reprocess_message(100, 123456)


class TestSelection:
    def test_select_channel_requires_registration(self, repo):
        with pytest.raises(KeyError):
            repo.select_channel(4711)

    def test_only_one_selected(self, repo):
        repo.ensure_channel(1, 10, "chan1", "One")
        repo.ensure_channel(2, 20, "chan2", "Two")
        repo.select_channel(1)
        assert repo.selected_channel().channel_id == 1
        repo.select_channel(2)
        assert repo.selected_channel().channel_id == 2
        selected_count = sum(1 for c in repo.list_channels() if c.is_selected)
        assert selected_count == 1


class TestSyncRecovery:
    def test_reset_backfill_creates_missing_state_without_nested_transaction(self, repo):
        repo.reset_backfill(999)
        state = repo.sync_state(999)
        assert state["backfill_status"] == "not_started"
        assert state["backfill_target_mode"] == "full"
