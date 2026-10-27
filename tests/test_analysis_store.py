"""Candidate store, migration 8, review audit and correction validation."""

from __future__ import annotations

import sqlite3

import pytest

from tests.conftest import FakeMsg
from tg_signal_ingest.analysis import (
    AnalysisError,
    AnalysisStore,
    ReplayScope,
    ReviewStatus,
    run_replay,
    validate_correction,
)
from tg_signal_ingest.analysis.store import candidate_identity
from tg_signal_ingest.db import Database
from tg_signal_ingest.normalize import message_to_record


def _candidate_id(repo, message_id: int) -> str:
    row = repo.db.conn.execute(
        "SELECT candidate_id FROM signal_candidates WHERE message_id = ? ORDER BY edit_version DESC LIMIT 1",
        (message_id,),
    ).fetchone()
    return str(row["candidate_id"])


def _analyse(repo, message_id: int, text: str) -> str:
    repo.upsert_message(
        message_to_record(100, FakeMsg(message_id, text), "live"),
        enqueue_processing=False,
    )
    run_replay(repo, ReplayScope(channel_id=100))
    return _candidate_id(repo, message_id)


class TestMigration:
    def test_migration_8_tables_exist_and_are_idempotent(self, tmp_path):
        db = Database(tmp_path / "db.sqlite3")
        applied = db.migrate()
        assert 8 in applied
        names = {
            row["name"]
            for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {
            "signal_candidates",
            "signal_candidate_reviews",
            "analysis_runs",
            "analysis_failures",
        } <= names
        assert db.migrate() == []
        db.close()

    def test_invalid_rows_are_rejected_by_schema_checks(self, repo):
        with pytest.raises(sqlite3.IntegrityError), repo.db.transaction() as conn:
            conn.execute(
                "INSERT INTO signal_candidates "
                "(candidate_id, channel_id, message_id, edit_version, source_datetime, "
                "source_text_sha256, parser_version, classification, market_type, "
                "targets_json, relationship, parse_status, idempotency_key, created_at, updated_at) "
                "VALUES ('x', 1, 1, 0, 'now', 'h', 1, 'new_signal', 'spot', '[]', "
                "'none', 'parsed', 'k', 'now', 'now')"
            )
            conn.execute(
                "INSERT INTO signal_candidates "
                "(candidate_id, channel_id, message_id, edit_version, source_datetime, "
                "source_text_sha256, parser_version, classification, market_type, "
                "targets_json, relationship, parse_status, idempotency_key, created_at, updated_at) "
                "VALUES ('x', 1, 1, 0, 'now', 'h', 1, 'not_a_class', 'spot', '[]', "
                "'none', 'parsed', 'k2', 'now', 'now')"
            )


class TestCandidateIdentity:
    def test_identity_is_deterministic_and_versioned(self):
        first = candidate_identity(1, 2, 3, 1)
        second = candidate_identity(1, 2, 3, 1)
        assert first == second
        assert first[0].startswith("cand-")
        assert first != candidate_identity(1, 2, 4, 1)
        assert first != candidate_identity(1, 2, 3, 2)


class TestReviews:
    def test_confirm_is_append_only_and_does_not_change_machine_fields(self, repo):
        candidate_id = _analyse(repo, 1, "CANCEL BTCUSDT")
        store = AnalysisStore(repo.db)
        before = store.get_candidate(candidate_id)
        raw_before = repo.get_message_raw(100, 1)

        review = store.add_review(
            candidate_id, reviewer="operator", decision="confirm", note="looks right"
        )
        assert review["decision"] == "confirm"
        assert review["corrected"] is None
        after = store.get_candidate(candidate_id)
        assert after["classification"] == before["classification"]
        assert after["symbol"] == before["symbol"]
        assert after["review_status"] == str(ReviewStatus.CONFIRMED)
        assert after["reviewed_at"] is not None
        assert repo.get_message_raw(100, 1)["text"] == raw_before["text"]

    def test_corrections_are_audit_rows_not_machine_edits(self, repo):
        candidate_id = _analyse(repo, 2, "CANCEL BTCUSDT")
        store = AnalysisStore(repo.db)
        before = store.get_candidate(candidate_id)

        first = store.add_review(
            candidate_id,
            reviewer="operator",
            decision="correct",
            corrected=validate_correction(
                {"symbol": "ETHUSDT", "entry_price": "10.50", "targets": ["12", "13"]}
            ),
            note="wrong pair",
        )
        second = store.add_review(
            candidate_id,
            reviewer="second-reviewer",
            decision="reject",
            note="withdrawn",
        )

        history = store.list_reviews(candidate_id)
        assert [row["review_id"] for row in history] == [
            first["review_id"],
            second["review_id"],
        ]
        assert history[0]["corrected"]["symbol"] == "ETHUSDT"
        assert history[0]["corrected"]["targets"] == [
            {"index": 1, "price": "12"},
            {"index": 2, "price": "13"},
        ]
        assert history[1]["decision"] == "reject"

        after = store.get_candidate(candidate_id)
        assert after["symbol"] == before["symbol"] == "BTCUSDT"
        assert after["entry_price"] == before["entry_price"] is None
        assert after["targets"] == []
        assert after["review_status"] == str(ReviewStatus.REJECTED)

    def test_invalid_reviews_are_rejected(self, repo):
        candidate_id = _analyse(repo, 3, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        store = AnalysisStore(repo.db)

        with pytest.raises(AnalysisError):
            store.add_review(candidate_id, reviewer="x", decision="maybe")
        with pytest.raises(AnalysisError):
            store.add_review(candidate_id, reviewer="x", decision="correct")
        with pytest.raises(KeyError):
            store.add_review("cand-missing", reviewer="x", decision="confirm")

    def test_review_does_not_change_another_candidate(self, repo):
        first_id = _analyse(repo, 4, "CANCEL BTCUSDT")
        second_id = _analyse(repo, 5, "CANCEL ETHUSDT")
        store = AnalysisStore(repo.db)
        store.add_review(first_id, reviewer="x", decision="confirm")

        assert store.get_candidate(first_id)["review_status"] == "confirmed"
        assert store.get_candidate(second_id)["review_status"] == "unreviewed"


class TestCorrectionValidation:
    def test_decimal_fields_are_canonicalized(self):
        result = validate_correction(
            {
                "entry_price": "100.50",
                "entry_low_price": 99,
                "entry_high_price": "101.000",
                "stop_loss": "95.5",
            }
        )
        assert result == {
            "entry_price": "100.50",
            "entry_low_price": "99",
            "entry_high_price": "101.000",
            "stop_loss": "95.5",
        }

    def test_targets_accept_list_and_comma_string(self):
        assert validate_correction({"targets": ["10", "11.5"]})["targets"] == [
            {"index": 1, "price": "10"},
            {"index": 2, "price": "11.5"},
        ]
        assert validate_correction({"targets": "10, 11.5"})["targets"] == [
            {"index": 1, "price": "10"},
            {"index": 2, "price": "11.5"},
        ]

    @pytest.mark.parametrize(
        "values",
        [
            {"unknown": "x"},
            {"classification": "trade_now"},
            {"market_type": "futures"},
            {"side": "hold"},
            {"entry_price": "0"},
            {"entry_price": "-1"},
            {"entry_price": "not-a-number"},
            {"stop_loss": "NaN"},
            {"targets": ["0"]},
            {"targets": 10},
        ],
    )
    def test_invalid_corrections_are_rejected(self, values):
        with pytest.raises(AnalysisError):
            validate_correction(values)

    def test_empty_correction_adds_nothing(self):
        assert validate_correction({}) == {}

    def test_missing_fields_are_not_invented(self):
        result = validate_correction({"symbol": None, "entry_price": None})
        assert result == {}
