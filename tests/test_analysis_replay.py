"""Replay, idempotency, edit/delete handling and failure isolation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from tests.conftest import FakeMsg, ReplyTo
from tg_signal_ingest.analysis import (
    ANALYSIS_PARSER_VERSION,
    AnalysisStore,
    ReplayScope,
    SignalAnalysisParser,
    parse_message,
    run_replay,
)
from tg_signal_ingest.normalize import message_to_record

CHANNEL = 100
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


def _insert(
    repo,
    message_id: int,
    text: str | None,
    *,
    channel_id: int = CHANNEL,
    edit_date: datetime | None = None,
    reply_to: int | None = None,
):
    reply = ReplyTo(reply_to) if reply_to is not None else None
    msg = FakeMsg(
        message_id,
        text,
        date=EPOCH + timedelta(seconds=message_id),
        edit_date=edit_date,
        reply_to=reply,
    )
    return repo.upsert_message(
        message_to_record(channel_id, msg, "live"), enqueue_processing=False
    )


def _candidate(repo, message_id: int, *, channel_id: int = CHANNEL, edit_version: int = 0):
    return repo.db.conn.execute(
        "SELECT * FROM signal_candidates WHERE channel_id = ? AND message_id = ? "
        "AND edit_version = ? AND parser_version = ?",
        (channel_id, message_id, edit_version, ANALYSIS_PARSER_VERSION),
    ).fetchone()


class TestReplayBasics:
    def test_inserts_candidate_for_persisted_message(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")

        stats = run_replay(repo, ReplayScope(channel_id=CHANNEL))

        assert stats.scanned == 1
        assert stats.inserted == 1
        assert stats.duplicates == 0
        assert stats.failures == 0
        assert stats.needs_review == 0
        assert stats.by_classification == {"new_signal": 1}
        assert stats.last_message_id == 1
        assert stats.run_id is not None

        row = _candidate(repo, 1)
        assert row is not None
        assert row["classification"] == "new_signal"
        assert row["symbol"] == "BTCUSDT"
        assert row["side"] == "buy"
        assert row["status"] == "current"
        assert row["review_status"] == "unreviewed"

    def test_replay_is_idempotent(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        _insert(repo, 2, "CANCEL ALL")

        first = run_replay(repo, ReplayScope(channel_id=CHANNEL))
        second = run_replay(repo, ReplayScope(channel_id=CHANNEL))

        assert first.inserted == 2
        assert second.inserted == 0
        assert second.duplicates == 2
        assert second.superseded == 0
        count = repo.db.conn.execute(
            "SELECT COUNT(*) AS c FROM signal_candidates"
        ).fetchone()["c"]
        assert count == 2

    def test_dry_run_writes_nothing(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")

        stats = run_replay(repo, ReplayScope(channel_id=CHANNEL, dry_run=True))

        assert stats.dry_run is True
        assert stats.scanned == 1
        assert stats.inserted == 0
        assert stats.by_classification == {"new_signal": 1}
        store = AnalysisStore(repo.db)
        assert store.summary()["total"] == 0
        assert store.latest_run() is None
        assert store.unresolved_failure_count() == 0

    def test_runs_are_recorded_with_counts(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        stats = run_replay(repo, ReplayScope(channel_id=CHANNEL))

        store = AnalysisStore(repo.db)
        run = store.latest_run()
        assert run is not None
        assert run["run_id"] == stats.run_id
        assert run["parser_version"] == ANALYSIS_PARSER_VERSION
        assert run["status"] == "completed"
        assert run["finished_at"] is not None
        assert run["scope"]["channel_id"] == CHANNEL
        assert run["counts"]["inserted"] == 1
        assert run["counts"]["scanned"] == 1


class TestScope:
    def test_channel_scope(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95", channel_id=100)
        _insert(repo, 2, "BUY ETHUSDT ENTRY 10 TP 12 SL 9", channel_id=101)

        stats = run_replay(repo, ReplayScope(channel_id=101))

        assert stats.scanned == 1
        assert _candidate(repo, 2, channel_id=101) is not None
        assert _candidate(repo, 1, channel_id=100) is None

    def test_limit_and_after_message_id(self, repo):
        for message_id in range(1, 6):
            _insert(repo, message_id, f"BUY BTCUSDT ENTRY {message_id}0 TP 120 SL 5")

        limited = run_replay(repo, ReplayScope(channel_id=CHANNEL, limit=2))
        assert limited.scanned == 2
        assert limited.last_message_id == 2

        resumed = run_replay(
            repo, ReplayScope(channel_id=CHANNEL, after_message_id=2)
        )
        assert resumed.scanned == 3
        assert resumed.last_message_id == 5

    def test_since_and_until(self, repo):
        for message_id in range(1, 6):
            _insert(repo, message_id, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")

        stats = run_replay(
            repo,
            ReplayScope(
                channel_id=CHANNEL,
                since="2026-01-01T00:00:02Z",
                until="2026-01-01T00:00:04Z",
            ),
        )
        assert stats.scanned == 3
        assert stats.last_message_id == 4

    def test_missing_channel_is_an_empty_scope_not_an_error(self, repo):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95", channel_id=100)
        stats = run_replay(repo, ReplayScope(channel_id=999))
        assert stats.scanned == 0
        assert stats.inserted == 0

    def test_message_id_range_selects_exact_bounded_sample(self, repo):
        for message_id in range(1, 11):
            _insert(repo, message_id, f"BUY BTCUSDT ENTRY {message_id}0 TP 120 SL 5")

        stats = run_replay(
            repo,
            ReplayScope(
                channel_id=CHANNEL,
                message_id_min=8,
                message_id_max=10,
            ),
        )

        assert stats.scanned == 3
        assert stats.last_message_id == 10
        assert _candidate(repo, 7) is None
        assert _candidate(repo, 8) is not None
        assert _candidate(repo, 9) is not None
        assert _candidate(repo, 10) is not None


class TestEditsAndDeletes:
    def test_edit_supersedes_previous_candidate(self, repo):
        _insert(repo, 10, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        first = run_replay(repo, ReplayScope(channel_id=CHANNEL))
        assert first.inserted == 1

        _insert(
            repo,
            10,
            "BUY BTCUSDT ENTRY 101 TP 121 SL 96",
            edit_date=EPOCH + timedelta(minutes=5),
        )
        second = run_replay(repo, ReplayScope(channel_id=CHANNEL))

        assert second.inserted == 1
        assert second.superseded == 1
        old = _candidate(repo, 10, edit_version=0)
        new = _candidate(repo, 10, edit_version=1)
        assert old["status"] == "superseded"
        assert new["status"] == "current"
        assert new["entry_price"] == "101"
        assert new["source_text_sha256"] != old["source_text_sha256"]

        again = run_replay(repo, ReplayScope(channel_id=CHANNEL))
        assert again.duplicates == 1
        assert again.inserted == 0

    def test_deleted_source_marks_candidate_not_the_raw_message(self, repo):
        _insert(repo, 11, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        repo.mark_deleted(CHANNEL, [11])
        raw_before = repo.get_message_raw(CHANNEL, 11)
        stats = run_replay(repo, ReplayScope(channel_id=CHANNEL))

        assert stats.deleted_marked == 1
        row = _candidate(repo, 11)
        assert row["status"] == "source_deleted"
        raw_after = repo.get_message_raw(CHANNEL, 11)
        assert raw_after is not None
        assert raw_after["text"] == raw_before["text"]
        assert raw_before["deleted"] == raw_after["deleted"] == 1

    def test_exclude_deleted_skips_marking(self, repo):
        _insert(repo, 12, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        repo.mark_deleted(CHANNEL, [12])

        stats = run_replay(
            repo, ReplayScope(channel_id=CHANNEL, include_deleted=False)
        )
        assert stats.scanned == 0
        assert _candidate(repo, 12)["status"] == "current"


class _FailingParser:
    version = ANALYSIS_PARSER_VERSION

    def __init__(self):
        self._delegate = SignalAnalysisParser()

    def parse(self, text, **kwargs):
        if text and "BOOM" in text:
            raise ValueError("synthetic parser failure")
        return self._delegate.parse(text, **kwargs)


class TestFailureIsolation:
    def test_one_failure_does_not_stop_the_run(self, repo):
        _insert(repo, 20, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        _insert(repo, 21, "BOOM")

        stats = run_replay(
            repo, ReplayScope(channel_id=CHANNEL), parser=_FailingParser()
        )

        assert stats.scanned == 2
        assert stats.failures == 1
        assert stats.inserted == 1
        store = AnalysisStore(repo.db)
        failures = store.list_failures()
        assert len(failures) == 1
        failure = failures[0]
        assert failure["message_id"] == 21
        assert failure["error_type"] == "ValueError"
        assert failure["attempts"] == 1
        assert failure["resolved_at"] is None
        assert _candidate(repo, 21) is None

    def test_retry_increments_attempts_then_resolves(self, repo):
        _insert(repo, 22, "BOOM")

        first = run_replay(
            repo, ReplayScope(channel_id=CHANNEL), parser=_FailingParser()
        )
        second = run_replay(
            repo, ReplayScope(channel_id=CHANNEL), parser=_FailingParser()
        )
        assert first.failures == second.failures == 1
        store = AnalysisStore(repo.db)
        failure = store.list_failures()[0]
        assert failure["attempts"] == 2

        recovered = run_replay(repo, ReplayScope(channel_id=CHANNEL))
        assert recovered.failures == 0
        assert store.unresolved_failure_count() == 0
        assert store.list_failures() == []
        resolved = store.list_failures(unresolved_only=False)[0]
        assert resolved["resolved_at"] is not None
        assert _candidate(repo, 22) is not None

    def test_dry_run_failure_writes_no_failure_row(self, repo):
        _insert(repo, 23, "BOOM")
        stats = run_replay(
            repo,
            ReplayScope(channel_id=CHANNEL, dry_run=True),
            parser=_FailingParser(),
        )
        assert stats.failures == 1
        assert AnalysisStore(repo.db).unresolved_failure_count() == 0


class TestRelationshipsAndPersistence:
    def test_update_resolves_parent_by_unique_symbol(self, repo):
        _insert(repo, 30, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        _insert(repo, 31, "BTCUSDT SL moved to 96")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        parent = _candidate(repo, 30)
        update = _candidate(repo, 31)
        assert update["classification"] == "stop_loss_update"
        assert update["relationship"] == "unique_symbol"
        assert update["parent_candidate_id"] == parent["candidate_id"]

    def test_reply_resolves_parent(self, repo):
        _insert(repo, 32, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        _insert(repo, 33, "UPDATE BTCUSDT TP 115", reply_to=32)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        parent = _candidate(repo, 32)
        update = _candidate(repo, 33)
        assert update["relationship"] == "reply"
        assert update["referenced_message_id"] == 32
        assert update["parent_candidate_id"] == parent["candidate_id"]

    def test_update_without_active_parent_is_flagged(self, repo):
        _insert(repo, 34, "BTCUSDT SL moved to 96")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        update = _candidate(repo, 34)
        assert update["relationship"] == "ambiguous"
        assert update["parse_status"] == "needs_review"
        assert "no_active_signal_for_symbol" in update["ambiguities_json"]

    def test_decimal_values_are_stored_as_canonical_text(self, repo):
        _insert(repo, 35, "BUY BTCUSDT ENTRY 0.100 TP 0.200 SL 0.050")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        row = _candidate(repo, 35)
        assert row["entry_price"] == "0.100"
        assert row["stop_loss"] == "0.050"
        assert '"0.200"' in row["targets_json"]
        for field in ("entry_price", "entry_low_price", "entry_high_price", "stop_loss"):
            value = row[field]
            assert value is None or isinstance(value, str)

    def test_parser_version_is_stamped(self, repo):
        _insert(repo, 36, "CANCEL ALL")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        row = _candidate(repo, 36)
        assert row["parser_version"] == ANALYSIS_PARSER_VERSION
        assert row["idempotency_key"]


def test_parse_and_replay_agree_on_classification(repo):
    text = "BUY BTCUSDT ENTRY 100 TP 120 SL 95"
    _insert(repo, 40, text)
    run_replay(repo, ReplayScope(channel_id=CHANNEL))
    direct = parse_message(text)
    row = _candidate(repo, 40)
    assert row["classification"] == str(direct.classification)
    assert row["source_text_sha256"] == direct.text_sha256
