"""Durable management state: per-operator flows, tokens, snapshots and jobs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tg_signal_ingest.control.channels import ChannelChoice
from tg_signal_ingest.management.store import ManagementStore


def _clock():
    current = [datetime(2026, 1, 1, tzinfo=UTC)]
    return current, lambda: current[0]


def test_callback_tokens_are_single_use_and_fully_bound(repo):
    _now, clock = _clock()
    store = ManagementStore(repo.db, now=clock)
    token = store.issue_callback(owner_id=7, chat_id=7, action="select", payload={"channel_id": 1})

    assert store.consume_callback(token, owner_id=8, chat_id=7)[0] == "wrong_owner"
    assert store.consume_callback(token, owner_id=7, chat_id=8)[0] == "wrong_chat"
    status, row = store.consume_callback(token, owner_id=7, chat_id=7)
    assert status == "ok"
    assert row is not None and row["action"] == "select"
    assert store.consume_callback(token, owner_id=7, chat_id=7)[0] == "replayed"


def test_callback_tokens_expire(repo):
    current, clock = _clock()
    store = ManagementStore(repo.db, now=clock)
    token = store.issue_callback(owner_id=7, chat_id=7, action="status", ttl_s=10)
    current[0] += timedelta(seconds=11)
    assert store.consume_callback(token, owner_id=7, chat_id=7)[0] == "expired"
    assert store.consume_callback("does-not-exist", owner_id=7, chat_id=7)[0] == "missing"


def test_flow_state_is_isolated_per_operator_and_recovered_on_restart(repo):
    store = ManagementStore(repo.db)
    assert store.flow(7)["state"] == "idle"
    store.set_flow(7, "auth_code")
    store.set_flow(8, "auth_phone")
    assert store.flow(7)["state"] == "auth_code"
    assert store.flow(8)["state"] == "auth_phone"

    assert store.recover_flows() == 2
    assert store.flow(7)["state"] == "idle"
    assert store.flow(8)["state"] == "idle"
    assert store.pending_flow_count() == 0


def test_callback_invalidation_is_operator_scoped(repo):
    store = ManagementStore(repo.db)
    mine = store.issue_callback(owner_id=7, chat_id=7, action="status")
    theirs = store.issue_callback(owner_id=8, chat_id=8, action="status")
    assert store.invalidate_callbacks(7) == 1
    assert store.consume_callback(mine, owner_id=7, chat_id=7)[0] == "replayed"
    assert store.consume_callback(theirs, owner_id=8, chat_id=8)[0] == "ok"


def test_snapshots_keep_ten_item_pages_and_discard_old_ones(repo):
    store = ManagementStore(repo.db)
    first = store.create_snapshot(
        [ChannelChoice(i, i * 10, f"channel {i}", None, "کانال") for i in range(1, 13)],
        operator_id=7,
    )
    assert store.current_snapshot(7) == first
    assert store.snapshot_count(first) == 12
    page = store.snapshot_page(first, 10)
    assert [item["channel_id"] for item in page] == [11, 12]
    assert len(store.snapshot_page(first, 0)) == 10

    second = store.create_snapshot(
        [ChannelChoice(20, 200, "next", "next", "کانال")], operator_id=7
    )
    third = store.create_snapshot(
        [ChannelChoice(30, 300, "newest", None, "کانال")], operator_id=7
    )
    assert store.current_snapshot(7) == third
    assert store.snapshot_count(first) == 0
    assert store.snapshot_count(second) == 1
    assert store.snapshot_count(third) == 1


def test_snapshots_are_not_visible_across_operators(repo):
    store = ManagementStore(repo.db)
    owner_snapshot = store.create_snapshot(
        [ChannelChoice(1, 10, "owner", None, "کانال")], operator_id=7
    )
    dev_snapshot = store.create_snapshot(
        [ChannelChoice(2, 20, "dev", None, "کانال")], operator_id=8
    )
    assert store.current_snapshot(7) == owner_snapshot
    assert store.current_snapshot(8) == dev_snapshot
    assert store.snapshot_channel(owner_snapshot, 1, operator_id=7) is not None
    assert store.snapshot_channel(owner_snapshot, 1, operator_id=8) is None
    assert store.snapshot_channel(dev_snapshot, 2, operator_id=7) is None


def test_operator_channel_selection_is_isolated(repo):
    store = ManagementStore(repo.db)
    store.set_operator_channel(
        7,
        selected_channel_id=101,
        selected_channel_title="Owner source",
        selection_snapshot_id="snap",
    )
    assert store.operator_channel(7)["selected_channel_id"] == 101
    assert store.operator_channel(8)["selected_channel_id"] is None
    store.clear_operator_channel(7)
    assert store.operator_channel(7)["selected_channel_id"] is None


def test_jobs_are_resumable_and_old_updates_are_idempotent(repo):
    store = ManagementStore(repo.db)
    assert store.mark_update(100) is True
    assert store.mark_update(100) is False

    job = store.create_job(55, "source")
    assert job["status"] == "pending"
    assert store.active_job()["job_id"] == job["job_id"]
    assert store.claim_job(job["job_id"])["status"] == "running"
    done = store.finish_job(job["job_id"], "interrupted", error="stopped", checkpoint_next_max_id=40)
    assert done["status"] == "interrupted" and done["checkpoint_next_max_id"] == 40
    assert store.active_job()["job_id"] == job["job_id"]  # interrupted is resumable
    resumed = store.resume_job(job["job_id"])
    assert resumed["status"] == "pending" and resumed["checkpoint_next_max_id"] == 40
    assert store.active_job()["job_id"] == job["job_id"]
    assert store.recover_jobs() == 0

    other = store.create_job(56, "other")
    assert store.claim_job(other["job_id"])["status"] == "running"
    assert store.recover_jobs() == 1
    assert store.get_job(other["job_id"])["status"] == "interrupted"


def _select(store: ManagementStore, operator_id: int, channel_id: int = 77) -> None:
    snapshot = store.create_snapshot(
        [ChannelChoice(channel_id, 770, "source", None, "کانال")],
        operator_id=operator_id,
    )
    store.set_operator_channel(
        operator_id,
        selected_channel_id=channel_id,
        selected_channel_title="source",
        selected_channel_username=None,
        selection_snapshot_id=snapshot,
    )


def test_validation_request_requires_owner_decision_and_starts_once(repo):
    store = ManagementStore(repo.db)
    _select(store, 7)
    request = store.create_import_request(
        channel_id=77,
        channel_title="source",
        channel_access_hash=770,
        selection_snapshot_id=store.current_snapshot(7),
        max_messages=10,
        requested_by=7,
    )
    assert request["status"] == "requested"
    assert store.active_import_request()["request_id"] == request["request_id"]

    status, decided = store.decide_import_request(
        request["request_id"], approved=True, decided_by=8
    )
    assert status == "ok"
    assert decided is not None and decided["status"] == "approved"
    assert store.latest_job() is None

    status, started, job = store.start_import_request(request["request_id"])
    assert status == "ok"
    assert started is not None and started["status"] == "started"
    assert job is not None and job["status"] == "pending"
    assert job["max_messages"] == 10
    assert job["request_id"] == request["request_id"]

    status, _again, second = store.start_import_request(request["request_id"])
    assert status == "started"
    assert second is None
    assert store.latest_job()["job_id"] == job["job_id"]


def test_developer_can_self_authorize_only_their_own_bounded_request(repo):
    store = ManagementStore(repo.db)
    _select(store, 7)
    request = store.create_import_request(
        channel_id=77,
        channel_title="source",
        channel_access_hash=770,
        selection_snapshot_id=store.current_snapshot(7),
        max_messages=10,
        requested_by=7,
        self_authorized_by=7,
    )
    assert request["status"] == "approved"
    assert request["decided_by"] == 7
    assert request["decided_at"] is not None
    assert store.latest_job() is None

    status, started, job = store.start_import_request(request["request_id"])
    assert status == "ok"
    assert started is not None and started["status"] == "started"
    assert job is not None and job["max_messages"] == 10

    with pytest.raises(ValueError):
        store.create_import_request(
            channel_id=77,
            channel_title="source",
            channel_access_hash=770,
            selection_snapshot_id=store.current_snapshot(7),
            max_messages=10,
            requested_by=7,
            self_authorized_by=8,
        )


def test_validation_request_expires_and_selection_change_invalidates(repo):
    current, clock = _clock()
    store = ManagementStore(repo.db, now=clock)
    _select(store, 7)
    request = store.create_import_request(
        channel_id=77,
        channel_title="source",
        channel_access_hash=770,
        selection_snapshot_id=store.current_snapshot(7),
        max_messages=10,
        requested_by=7,
    )
    current[0] += timedelta(seconds=1801)
    status, expired = store.decide_import_request(
        request["request_id"], approved=True, decided_by=8
    )
    assert status == "expired"
    assert expired is not None and expired["status"] == "expired"

    second = store.create_import_request(
        channel_id=77,
        channel_title="source",
        channel_access_hash=770,
        selection_snapshot_id=store.current_snapshot(7),
        max_messages=10,
        requested_by=7,
    )
    assert store.decide_import_request(
        second["request_id"], approved=True, decided_by=8
    )[0] == "ok"
    store.set_operator_channel(
        7,
        selected_channel_id=78,
        selected_channel_title="different",
        selection_snapshot_id=store.current_snapshot(7),
    )
    status, cancelled, job = store.start_import_request(second["request_id"])
    assert status == "selection_changed"
    assert cancelled is not None and cancelled["status"] == "cancelled"
    assert job is None
    assert store.latest_job() is None


def test_review_drafts_expire_and_are_operator_scoped(repo):
    current, clock = _clock()
    store = ManagementStore(repo.db, now=clock)
    store.set_review_draft(
        7,
        request_id="val-1",
        candidate_id="cand-1",
        page_index=2,
        fields={"symbol": "BTCUSDT"},
    )
    draft = store.get_review_draft(7)
    assert draft is not None
    assert draft["fields"]["symbol"] == "BTCUSDT"
    assert store.get_review_draft(8) is None
    current[0] += timedelta(seconds=901)
    assert store.get_review_draft(7) is None


def test_restart_expires_or_fails_unfinished_validation_requests(repo):
    store = ManagementStore(repo.db)
    _select(store, 7)
    request = store.create_import_request(
        channel_id=77,
        channel_title="source",
        channel_access_hash=770,
        selection_snapshot_id=store.current_snapshot(7),
        max_messages=10,
        requested_by=7,
    )
    store.decide_import_request(request["request_id"], approved=True, decided_by=8)
    _status, _request, job = store.start_import_request(request["request_id"])
    assert job is not None
    store.claim_job(job["job_id"])

    assert store.recover_jobs() == 1
    recovered = store.recover_import_requests()
    assert recovered["interrupted"] == 1
    after = store.get_import_request(request["request_id"])
    assert after is not None and after["status"] == "failed"
    assert "restart" in str(after["error"])
