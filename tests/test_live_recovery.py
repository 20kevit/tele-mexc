"""Live-message insertion, edits, and gap recovery behavior (deterministic fakes)."""

from __future__ import annotations

from datetime import UTC, datetime

from tests.conftest import FakeMsg
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import UpsertOutcome


class TestLiveInsertion:
    def test_live_message_stored_as_source_live(self, repo):
        repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
        rec = message_to_record(100, FakeMsg(500, "fresh live"), "live")
        result = repo.upsert_message(rec)
        assert result.outcome is UpsertOutcome.INSERTED
        stored = repo.get_message_raw(100, 500)
        assert stored["source"] == "live"
        state = repo.sync_state(100)
        state = repo.sync_state(100)
        repo.record_live_event(100, 500)
        state = repo.sync_state(100)
        assert state["live_last_seen_message_id"] == 500
        assert state["live_last_event_at"] is not None

    def test_live_checkpoint_advances_monotonically(self, repo):
        repo.record_live_event(100, 5)
        repo.record_live_event(100, 10)
        repo.record_live_event(100, 7)  # late/duplicate event must not rewind
        assert repo.sync_state(100)["live_last_seen_message_id"] == 10

    def test_backfill_then_live_merge(self, repo):
        repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
        # history imported id 400 (backfill)
        repo.upsert_message(message_to_record(100, FakeMsg(400, "history"), "backfill"))
        # live event for the same id (duplicate) -> unchanged, source intact
        result = repo.upsert_message(message_to_record(100, FakeMsg(400, "history"), "live"))
        assert result.outcome is UpsertOutcome.UNCHANGED
        # newer live id inserts
        repo.upsert_message(message_to_record(100, FakeMsg(900, "live now"), "live"))
        counts = repo.message_counts(100)
        assert counts.total == 2
        assert counts.max_id == 900


class TestEdits:
    def test_edited_live_message_updates_in_place(self, repo):
        repo.upsert_message(message_to_record(100, FakeMsg(600, "signal"), "live"))
        edited = FakeMsg(600, "signal", edit_date=datetime(2026, 2, 1, tzinfo=UTC))
        result = repo.upsert_message(message_to_record(100, edited, "live"))
        assert result.outcome is UpsertOutcome.UPDATED
        raw = repo.get_message_raw(100, 600)
        assert raw["edit_datetime"] == "2026-02-01T00:00:00Z"
        assert raw["processing_version"] == 1
        history = repo.edit_history(100, 600)
        assert len(history) == 1  # previous version preserved for audit

    def test_signal_edit_is_observable_downstream(self, repo):
        repo.upsert_message(message_to_record(100, FakeMsg(601, "TP: 1.2"), "live"))
        repo.upsert_message(
            message_to_record(
                100, FakeMsg(601, "TP: 1.3", edit_date=datetime(2026, 2, 2, tzinfo=UTC)), "live"
            )
        )
        counts = repo.queue_counts()
        assert counts["pending"] == 2  # 'message' + 'edit'
        rows = repo.list_queue("pending", limit=10)
        edit_item = [r for r in rows if r["item_type"] == "edit"][0]
        assert edit_item["targets_version"] == 1


class TestRecovery:
    def test_recovery_source_marks_restored_messages(self, repo):
        repo.upsert_message(message_to_record(100, FakeMsg(700, "recovered"), "recovery"))
        stored = repo.get_message_raw(100, 700)
        assert stored["source"] == "recovery"
