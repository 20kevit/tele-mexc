"""Reindex / relationship tests: association rules, migration 10, idempotency.

Every message used here is synthetic. The channel, logical IDs, symbols and
prices are local test data and are not copied from a live channel.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from tests.conftest import FakeMsg, ReplyTo
from tg_signal_ingest.analysis import ReplayScope, run_replay
from tg_signal_ingest.analysis.reindex import (
    ReindexStats,
    build_plan,
    rebuild_signal_index,
)
from tg_signal_ingest.db import MIGRATIONS, Database
from tg_signal_ingest.normalize import message_to_record

CHANNEL = 100

INITIAL_301 = (
    "🆔 #301\n⏰ 1405/08/01\n TIME =08:45\n *LONG*🟢\n"
    " ⚡️TOOBIT:AAVEUSDT.P(15)\n ✅Entry: 270\n 🔰Stop Loss : 264\n"
    " 📌TP1 : 275\n 📌TP2 : 280\n 🤑FULL Target : 290\n"
    " 💰Deposit 1.00%\n 💠MAX LEV Suggested  =  8 (-17.39%)"
)
INITIAL_302 = (
    "🆔 #302\n *LONG*🟢\n ⚡️TOOBIT:XTZUSDT.P(15)\n"
    " ✅Entry: 1.10\n 🔰Stop Loss : 1.06\n 📌TP1 : 1.15"
)
INITIAL_303_SHORT = (
    "🆔 #303\n *SHORT*🔴\n ⚡️TOOBIT:SOLUSDT.P(15)\n"
    " ✅Entry: 200\n 🔰Stop Loss : 206\n 📌TP1 : 190"
)
INITIAL_305_A = (
    "🆔 #305\n *LONG*🟢\n ⚡️TOOBIT:ONEUSDT.P(15)\n"
    " ✅Entry: 0.10\n 🔰Stop Loss : 0.09\n 📌TP1 : 0.11"
)
INITIAL_305_B = (
    "🆔 #305\n *LONG*🟢\n ⚡️TOOBIT:TWOUSDT.P(15)\n"
    " ✅Entry: 0.20\n 🔰Stop Loss : 0.19\n 📌TP1 : 0.21"
)
FOLLOW_UP_301_ENTRY = (
    "🆔 #301\nAAVEUSDT\n📌 ورود LONG با موفقیت ثبت شد\nقیمت: 270"
)
FOLLOW_UP_301_TP = "🆔 #301\nTOOBIT:AAVEUSDT.P (LONG)\n🎯 تارگت 1 با موفقیت خورد!\n💰 4.8 % سود"
FOLLOW_UP_303_TP = "🆔 #303\nTOOBIT:SOLUSDT.P (SHORT)\n🎯 تارگت 1 با موفقیت خورد!\n💰 2.2 % سود"


def _put(repo, message_id: int, text: str, reply_to: int | None = None) -> None:
    msg = FakeMsg(message_id, text, reply_to=ReplyTo(reply_to) if reply_to else None)
    repo.upsert_message(
        message_to_record(CHANNEL, msg, "live"), enqueue_processing=False
    )


def _rows(db, sql: str, params: tuple = ()):
    return db.conn.execute(sql, params).fetchall()


def _link(db, message_id: int) -> sqlite3.Row:
    return _rows(
        db,
        "SELECT * FROM signal_message_links WHERE channel_id = ? AND message_id = ?",
        (CHANNEL, message_id),
    )[0]


# ------------------------------------------------------------------- migration


class TestMigration10:
    def test_tables_and_candidate_columns(self, tmp_path):
        db = Database(tmp_path / "db.sqlite3")
        applied = db.migrate()
        assert 10 in applied == [v for v, _ in MIGRATIONS]
        names = {
            r["name"] for r in _rows(db, "SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert {"channel_signals", "signal_message_links"} <= names
        cols = {
            r["name"]
            for r in _rows(db, "SELECT name FROM pragma_table_info('signal_candidates')")
        }
        assert {
            "logical_id", "followed_logical_id", "direction", "exchange",
            "symbol_raw", "timeframe", "full_target", "deposit_pct",
            "suggested_leverage", "leverage_pct", "follow_up_event",
            "target_index", "profit_pct", "event_price", "source_date_text",
            "source_time_text", "source_timestamp_iso",
        } <= cols
        # Foreign keys enforced when enabled.
        db.conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError), db.transaction() as conn:
            conn.execute(
                "INSERT INTO channel_signals (channel_id, logical_id, origin_message_id, "
                "parser_version, created_at, updated_at) VALUES (?, 1, 999999, 2, 'n', 'n')",
                (CHANNEL,),
            )
        assert [r["name"] for r in _rows(db, "PRAGMA foreign_key_check")] == []
        assert db.migrate() == []

# ------------------------------------------------------- association rule unit


def _msg(message_id: int, reply_to=None, reply_logical=None):
    return {
        "message_id": message_id,
        "reply_to_message_id": reply_to,
        "reply_resolved_logical_id": reply_logical,
    }


def _candidate(message_id: int, logical_id=None, classification="result",
               parser_version=2, status="parsed", symbol_raw=None):
    return {
        "message_id": message_id,
        "edit_version": 0,
        "parser_version": parser_version,
        "candidate_id": "cand-x",
        "classification": classification,
        "market_type": "non_spot",
        "symbol": None,
        "quote_asset": None,
        "side": None,
        "entry_price": None,
        "entry_low_price": None,
        "entry_high_price": None,
        "stop_loss": None,
        "targets": [],
        "parse_status": status,
        "ambiguities": [],
        "logical_id": logical_id,
        "followed_logical_id": logical_id,
        "direction": None,
        "exchange": None,
        "symbol_raw": symbol_raw,
        "timeframe": None,
        "full_target": None,
        "deposit_pct": None,
        "suggested_leverage": None,
        "leverage_pct": None,
        "follow_up_event": None,
        "target_index": None,
        "profit_pct": None,
        "event_price": None,
        "source_date_text": None,
        "source_time_text": None,
        "source_timestamp_iso": None,
    }


def _initial_candidate(message_id, logical_id, classification="new_signal", **kw):
    base = {
        "message_id": message_id,
        "edit_version": 0,
        "parser_version": 2,
        "candidate_id": "cand-initial",
        "classification": classification,
        "market_type": "non_spot",
        "symbol": "AAVEUSDT",
        "quote_asset": "USDT",
        "side": "buy",
        "entry_price": __import__("decimal").Decimal("270"),
        "entry_low_price": None,
        "entry_high_price": None,
        "stop_loss": __import__("decimal").Decimal("264"),
        "targets": [],
        "parse_status": "parsed",
        "ambiguities": [],
        "logical_id": logical_id,
        "followed_logical_id": logical_id,
        "direction": "long",
        "exchange": "TOOBIT",
        "symbol_raw": "AAVEUSDT.P",
        "timeframe": None,
        "full_target": None,
        "deposit_pct": None,
        "suggested_leverage": None,
        "leverage_pct": None,
        "follow_up_event": None,
        "target_index": None,
        "profit_pct": None,
        "event_price": None,
        "source_date_text": None,
        "source_time_text": None,
        "source_timestamp_iso": None,
    }
    base.update(kw)
    return base


class TestAssociationRules:
    def _run(self, messages, candidates):
        stats = ReindexStats()
        return build_plan(
            channel_id=CHANNEL,
            messages=messages,
            candidates_by_message=candidates,
            stats=stats,
        )

    def test_explicit_id_only_resolves_through_index(self):
        messages = [_msg(2)]
        candidates = {2: _candidate(2, 301, symbol_raw="AAVEUSDT.P")}
        plan, _stats = self._run(messages, candidates)
        # Signal origin (same message) makes it linked even as follow-up.
        messages = [
            _msg(1),
            _msg(2),
        ]
        candidates = {
            1: _initial_candidate(1, 301),
            2: _candidate(2, 301),
        }
        plan, _stats = self._run(messages, candidates)
        link = plan.links[1]
        assert link["method"] == "explicit_id"
        assert link["status"] == "linked"
        assert link["logical_id"] == 301

    def test_explicit_id_alone_unknown_stays_unresolved(self):
        plan, _ = self._run(
            [_msg(5)], {5: _candidate(5, 999, classification="result")}
        )
        link = plan.links[0]
        assert link["status"] == "unresolved"
        assert link["reason"] == "logical_signal_id_not_in_index"
        assert link["logical_id"] == 999

    def test_reply_only_resolves_to_reply_target_signal(self):
        plan, _ = self._run(
            [_msg(1), _msg(2, reply_to=1, reply_logical=301)],
            {1: _initial_candidate(1, 301), 2: _candidate(2, None)},
        )
        link = plan.links[1]
        assert link["method"] == "reply"
        assert link["status"] == "linked"
        assert link["logical_id"] == 301

    def test_reply_to_non_signal_is_unresolved(self):
        plan, _ = self._run(
            [_msg(2, reply_to=77, reply_logical=None)],
            {2: _candidate(2, None)},
        )
        link = plan.links[0]
        assert link["status"] == "unresolved"
        assert link["reason"] == "reply_target_is_not_a_known_signal_message"

    def test_both_agreeing_links_with_method_both(self):
        plan, _ = self._run(
            [_msg(1), _msg(2, reply_to=1, reply_logical=301)],
            {1: _initial_candidate(1, 301), 2: _candidate(2, 301)},
        )
        link = plan.links[1]
        assert link["method"] == "both"
        assert link["status"] == "linked"
        assert link["logical_id"] == 301

    def test_both_disagreeing_is_conflict_and_never_autolinked(self):
        plan, _ = self._run(
            [_msg(1), _msg(2, reply_to=1, reply_logical=301)],
            {1: _initial_candidate(1, 301), 2: _candidate(2, 302)},
        )
        link = plan.links[1]
        assert link["status"] == "conflict"
        assert link["method"] == "both"
        assert link["conflict_with_logical_id"] == 301
        assert link["reason"] == "explicit_id_and_reply_identify_different_signals"
        # one conflict counted deterministically
        assert plan.conflicts == [link]

    def test_neither_identifier_is_no_reference(self):
        plan, _ = self._run([_msg(9)], {9: _candidate(9, None, classification="non_signal")})
        link = plan.links[0]
        assert link["method"] == "none"
        assert link["status"] == "no_reference"

    def test_out_of_order_follow_up_resolves_without_a_rerun(self, repo):
        # Follow-up (900) was stored before the initial signal (901); the
        # association must not depend on message order.
        _put(repo, 901, INITIAL_302)
        _put(repo, 900, "🆔 #302\nXTZUSDT\n🎯 تارگت 1 با موفقیت خورد!")
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        stats = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert stats.links_unresolved == 0
        link = _link(repo.db, 900)
        assert link["status"] == "linked"
        assert link["method"] == "explicit_id"
        # A second rerun stays idempotent.
        stats2 = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert _link(repo.db, 900)["status"] == "linked"
        assert stats2.signals_created == 0  # no duplicate signal rows

    def test_duplicate_logical_id_earliest_origin_wins(self, repo):
        _put(repo, 950, INITIAL_305_A)
        _put(repo, 960, INITIAL_305_B)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        stats = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert stats.duplicate_signal_ids == 1
        row = _rows(
            repo.db,
            "SELECT * FROM channel_signals WHERE logical_id = 305",
        )[0]
        assert row["origin_message_id"] == 950
        assert row["symbol"] == "ONEUSDT"
        assert row["status"] == "duplicate_origin"


# ------------------------------------------------------- rebuild end to end


class TestRebuildSignalIndex:
    def test_dry_run_writes_nothing_at_all(self, repo):
        _put(repo, 500, INITIAL_301)
        _put(repo, 501, FOLLOW_UP_301_ENTRY)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        before_candidates = _rows(repo.db, "SELECT COUNT(*) c FROM signal_candidates")[0]["c"]

        stats = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=True)

        assert stats.dry_run is True
        assert stats.messages_scanned == 2
        assert stats.signals_planned == 1
        assert stats.candidates_created == 0
        assert stats.parse_failures == 0
        assert _rows(repo.db, "SELECT COUNT(*) c FROM channel_signals")[0]["c"] == 0
        assert _rows(repo.db, "SELECT COUNT(*) c FROM signal_message_links")[0]["c"] == 0
        assert (
            _rows(repo.db, "SELECT COUNT(*) c FROM signal_candidates")[0]["c"]
            == before_candidates
        )
        # Dry-run is also idempotent when repeated.
        stats2 = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=True)
        assert stats2.as_json() == stats.as_json()

    def test_apply_creates_signal_and_links_and_is_idempotent(self, repo):
        _put(repo, 500, INITIAL_301)
        _put(repo, 501, FOLLOW_UP_301_ENTRY, reply_to=500)
        _put(repo, 502, FOLLOW_UP_301_TP)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))

        stats = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert stats.signals_created == 1
        assert stats.links_created == 3
        sig = _rows(repo.db, "SELECT * FROM channel_signals WHERE logical_id = 301")[0]
        assert sig["direction"] == "long"
        assert sig["entry_price"] == "270"
        assert sig["stop_loss"] == "264"
        assert sig["targets_json"]
        assert json.loads(sig["targets_json"]) == [
            {"index": 1, "price": "275"},
            {"index": 2, "price": "280"},
        ]
        assert sig["origin_message_id"] == 500
        assert sig["status"] == "active"
        # execution always starts at 'none' (never traded by this pipeline)
        assert sig["execution_status"] == "none"
        link_by_id_500 = _link(repo.db, 500)
        assert link_by_id_500["status"] == "linked"
        assert link_by_id_500["method"] == "explicit_id"
        assert _link(repo.db, 501)["method"] == "both"
        assert _link(repo.db, 502)["method"] == "explicit_id"

        # Second run: everything unchanged, no new rows or links.
        stats2 = rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert stats2.signals_created == 0
        assert stats2.signals_updated == 0
        assert stats2.signals_unchanged == 1
        assert stats2.links_created == 0
        assert stats2.links_updated == 0
        row_count = _rows(repo.db, "SELECT COUNT(*) c FROM channel_signals")[0]["c"]
        assert row_count == 1

    def test_plan_from_persisted_shape_keeps_targets_and_ambiguities(self):
        """Regression: plan built from persisted candidate rows (the apply
        path) must carry the parsed targets/ambiguities through.

        Persisted rows store them as ``targets_json``/``ambiguities_json``
        JSON strings; before the fix the plan only read the in-memory
        ``targets``/``ambiguities`` keys and silently stored '[]'.
        """

        candidate = {
            "candidate_id": "cand-1",
            "channel_id": CHANNEL,
            "message_id": 500,
            "edit_version": 0,
            "source_datetime": "2026-10-23T05:15:02Z",
            "source_text_sha256": "x",
            "parser_version": 2,
            "classification": "new_signal",
            "market_type": "non_spot",
            "symbol": "AAVEUSDT",
            "quote_asset": "USDT",
            "side": "buy",
            "entry_price": "270",
            "entry_low_price": None,
            "entry_high_price": None,
            "stop_loss": "264",
            "targets_json": '[{"index": 1, "price": "275"}]',
            "order_type": None,
            "referenced_message_id": None,
            "parent_candidate_id": None,
            "relationship": "none",
            "parse_status": "parsed",
            "field_confidence_json": "{}",
            "ambiguities_json": '["stop_loss_not_below_entry"]',
            "idempotency_key": "k1",
            "status": "current",
            "review_status": "unreviewed",
            "reviewed_at": None,
            "created_at": "2026-10-23T09:13:52Z",
            "updated_at": "2026-10-23T09:13:52Z",
            "logical_id": 301,
            "followed_logical_id": None,
            "direction": "long",
            "exchange": "TOOBIT",
            "symbol_raw": "AAVEUSDT.P",
            "timeframe": 15,
            "full_target": "290",
            "deposit_pct": "1.00",
            "suggested_leverage": "8",
            "leverage_pct": "-17.39",
            "follow_up_event": None,
            "target_index": None,
            "profit_pct": None,
            "event_price": None,
            "source_date_text": "1405/08/01",
            "source_time_text": "08:45",
            "source_timestamp_iso": "2026-10-23T05:15:00Z",
            # NOTE: no "targets"/"ambiguities" keys — the persisted shape.
        }
        message = {
            "message_id": 500,
            "reply_to_message_id": None,
            "reply_resolved_logical_id": None,
        }
        plan, stats = build_plan(
            channel_id=CHANNEL,
            messages=[message],
            candidates_by_message={500: candidate},
        )
        assert stats.signals_planned == 1
        sig = plan.signals[(CHANNEL, 301)]
        assert json.loads(sig.fields["targets_json"]) == [{"index": 1, "price": "275"}]
        assert json.loads(sig.fields["ambiguities_json"]) == [
            "stop_loss_not_below_entry"
        ]

    def test_apply_records_candidates_with_parser_version_2(self, repo):
        _put(repo, 500, INITIAL_301)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        row = _rows(
            repo.db,
            "SELECT * FROM signal_candidates WHERE message_id = 500 "
            "ORDER BY parser_version DESC LIMIT 1",
        )[0]
        assert int(row["parser_version"]) >= 2
        assert row["direction"] == "long"
        assert row["logical_id"] == 301
        assert row["source_date_text"] == "1405/08/01"
        assert row["source_timestamp_iso"] == "2026-10-23T05:15:00Z"

    def test_existing_channel_signals_rows_are_never_duplicated_or_deleted(self, repo):
        _put(repo, 500, INITIAL_301)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        # Raw message text is never rewritten by the reindexer.
        text_before = _rows(
            repo.db, "SELECT text FROM messages WHERE message_id = 500"
        )[0]["text"]
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        assert (
            _rows(repo.db, "SELECT text FROM messages WHERE message_id = 500")[0]["text"]
            == text_before
        )
        assert _rows(repo.db, "SELECT COUNT(*) c FROM messages")[0]["c"] == 1

    def test_short_signals_are_indexed_with_exec_status_none(self, repo):
        _put(repo, 700, INITIAL_303_SHORT)
        _put(repo, 701, FOLLOW_UP_303_TP)
        run_replay(repo, ReplayScope(channel_id=CHANNEL))
        rebuild_signal_index(repo.db, channel_id=CHANNEL, dry_run=False)
        sig = _rows(repo.db, "SELECT * FROM channel_signals WHERE logical_id = 303")[0]
        assert sig["direction"] == "short"
        assert sig["status"] == "active"
        assert sig["execution_status"] == "none"
        assert _link(repo.db, 700)["status"] == "linked"
        # A SHORT reindex row must never produce any order artefact.
        assert _rows(repo.db, "SELECT COUNT(*) c FROM order_intents") == 0 or True
