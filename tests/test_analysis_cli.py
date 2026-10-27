"""CLI surface for offline analysis: replay, summary, list, show, review."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from tests.conftest import FakeMsg
from tg_signal_ingest import main
from tg_signal_ingest.analysis import AnalysisStore, ReplayScope, run_replay
from tg_signal_ingest.normalize import message_to_record

FIXTURE = Path(__file__).parent / "fixtures" / "redacted_signal_corpus.jsonl"


def _use_cfg(cfg, monkeypatch):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)


def _insert(repo, message_id: int, text: str, *, channel_id: int = 100):
    repo.upsert_message(
        message_to_record(channel_id, FakeMsg(message_id, text), "live"),
        enqueue_processing=False,
    )


def _replay_args(**overrides):
    values = {
        "channel_id": 100,
        "since": None,
        "until": None,
        "limit": None,
        "after_message_id": None,
        "exclude_deleted": False,
        "dry_run": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _review_args(candidate_id, **overrides):
    values = {
        "candidate_id": candidate_id,
        "decision": "confirm",
        "reviewer": "tester",
        "note": None,
        "set_classification": None,
        "set_market_type": None,
        "set_symbol": None,
        "set_quote_asset": None,
        "set_side": None,
        "set_entry": None,
        "set_entry_low": None,
        "set_entry_high": None,
        "set_stop": None,
        "set_targets": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _list_args(**overrides):
    values = {
        "classification": None,
        "parse_status": None,
        "review_status": None,
        "include_superseded": False,
        "limit": 50,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _candidate_id(repo, message_id: int = 1) -> str:
    return str(
        repo.db.conn.execute(
            "SELECT candidate_id FROM signal_candidates WHERE message_id = ?", (message_id,)
        ).fetchone()["candidate_id"]
    )


class TestReplayCommand:
    def test_requires_a_channel_when_none_selected(self, cfg, monkeypatch, capsys):
        _use_cfg(cfg, monkeypatch)
        assert main.cmd_analyze_replay(_replay_args(channel_id=None)) == 1
        assert "no channel selected" in capsys.readouterr().err

    def test_uses_selected_channel_by_default(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        repo.ensure_channel(100, 1000, "source", "Source")
        repo.select_channel(100)
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))

        assert main.cmd_analyze_replay(_replay_args(channel_id=None)) == 0
        out = capsys.readouterr().out
        assert "scanned=1 inserted=1" in out
        assert "new_signal=1" in out

    def test_dry_run_reports_but_writes_nothing(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        assert main.cmd_analyze_replay(_replay_args(dry_run=True)) == 0
        assert "dry-run" in capsys.readouterr().out
        assert AnalysisStore(repo.db).summary()["total"] == 0

    def test_invalid_date_is_rejected(self, repo, monkeypatch):
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        try:
            main.cmd_analyze_replay(_replay_args(since="not-a-date"))
        except SystemExit as exc:
            assert "--since" in str(exc)
        else:  # pragma: no cover - defensive
            raise AssertionError("expected SystemExit")


class TestReadCommands:
    def test_db_path_override_targets_a_copy_without_touching_configured_db(
        self, cfg, monkeypatch, tmp_path, capsys
    ):
        target = tmp_path / "analysis-copy.sqlite3"
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)

        assert main.cmd_analyze_summary(SimpleNamespace(db_path=str(target))) == 0
        assert target.is_file()
        assert not cfg.db_path.exists()
        assert "candidates: total=0" in capsys.readouterr().out

    def test_db_path_override_is_exposed_by_argparse(self, tmp_path):
        args = main.build_parser().parse_args(
            ["analyze", "summary", "--db-path", str(tmp_path / "copy.sqlite3")]
        )
        assert args.db_path == str(tmp_path / "copy.sqlite3")

    def test_summary_lists_counts_and_latest_run(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "CANCEL BTCUSDT")
        run_replay(repo, ReplayScope(channel_id=100))
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))

        assert main.cmd_analyze_summary(None) == 0
        out = capsys.readouterr().out
        assert "candidates: total=1" in out
        assert "cancellation" in out
        assert "latest run:" in out
        assert "status=completed" in out

    def test_list_filters_and_omits_message_text(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        run_replay(repo, ReplayScope(channel_id=100))
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))

        assert main.cmd_analyze_list(_list_args(classification="new_signal")) == 0
        out = capsys.readouterr().out
        assert "BTCUSDT" in out
        assert "ENTRY" not in out

        assert main.cmd_analyze_list(_list_args(classification="result")) == 0
        assert capsys.readouterr().out == ""

    def test_show_redacts_channel_and_text_by_default(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95")
        run_replay(repo, ReplayScope(channel_id=100))
        candidate_id = _candidate_id(repo)
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))

        assert main.cmd_analyze_show(SimpleNamespace(candidate_id=candidate_id, show_text=False)) == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["channel_id"] == "<redacted-channel>"
        assert "BUY BTCUSDT" not in out
        assert payload["reviews"] == []

        assert main.cmd_analyze_show(SimpleNamespace(candidate_id=candidate_id, show_text=True)) == 0
        out = capsys.readouterr().out
        assert "source text (explicit --show-text):" in out
        assert "BUY BTCUSDT" in out

    def test_show_missing_candidate(self, repo, monkeypatch, capsys):
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        assert main.cmd_analyze_show(SimpleNamespace(candidate_id="cand-missing", show_text=False)) == 1
        assert "candidate not found" in capsys.readouterr().err


class TestReviewCommand:
    def test_confirm_and_correct_append_history(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "CANCEL BTCUSDT")
        run_replay(repo, ReplayScope(channel_id=100))
        candidate_id = _candidate_id(repo)
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        store = AnalysisStore(repo.db)

        assert main.cmd_analyze_review(_review_args(candidate_id, note="ok")) == 0
        assert "decision=confirm" in capsys.readouterr().out

        args = _review_args(
            candidate_id,
            decision="correct",
            set_symbol="ETHUSDT",
            set_entry="10.5",
            set_targets="12,13",
            note="fix",
        )
        assert main.cmd_analyze_review(args) == 0
        history = store.list_reviews(candidate_id)
        assert len(history) == 2
        assert history[0]["decision"] == "confirm"
        assert history[1]["corrected"] == {
            "symbol": "ETHUSDT",
            "entry_price": "10.5",
            "targets": [
                {"index": 1, "price": "12"},
                {"index": 2, "price": "13"},
            ],
        }
        # The machine row remains untouched.
        candidate = store.get_candidate(candidate_id)
        assert candidate["symbol"] == "BTCUSDT"
        assert candidate["entry_price"] is None
        assert candidate["review_status"] == "corrected"

    def test_invalid_correction_exits_two(self, repo, monkeypatch, capsys):
        _insert(repo, 1, "CANCEL BTCUSDT")
        run_replay(repo, ReplayScope(channel_id=100))
        candidate_id = _candidate_id(repo)
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        args = _review_args(candidate_id, decision="correct", set_entry="-5")
        assert main.cmd_analyze_review(args) == 2
        assert "review rejected" in capsys.readouterr().err

    def test_missing_candidate_exits_one(self, repo, monkeypatch, capsys):
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))
        assert main.cmd_analyze_review(_review_args("cand-missing")) == 1
        assert "candidate not found" in capsys.readouterr().err


class TestFailureAndCorpusCommands:
    def test_failures_command_shows_unresolved_and_all(self, repo, monkeypatch, capsys):
        store = AnalysisStore(repo.db)
        store.record_failure(
            channel_id=100,
            message_id=7,
            edit_version=0,
            parser_version=1,
            error_type="ValueError",
            error_message="synthetic",
        )
        monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg_for(repo))

        assert main.cmd_analyze_failures(SimpleNamespace(all=False, limit=50)) == 0
        out = capsys.readouterr().out
        assert "msg=7" in out
        assert "type=ValueError" in out
        assert "unresolved failures: 1" in out

        store.resolve_failure(channel_id=100, message_id=7, edit_version=0, parser_version=1)
        assert main.cmd_analyze_failures(SimpleNamespace(all=False, limit=50)) == 0
        assert "unresolved failures: 0" in capsys.readouterr().out

        assert main.cmd_analyze_failures(SimpleNamespace(all=True, limit=50)) == 0
        assert "resolved=" in capsys.readouterr().out

    def test_corpus_check_warns_that_evidence_is_synthetic(self, capsys):
        assert main.cmd_analyze_corpus(SimpleNamespace(path=str(FIXTURE))) == 0
        out = capsys.readouterr().out
        assert "matched=26 mismatched=0" in out
        assert "no human-reviewed real examples" in out

    def test_corpus_check_missing_file(self, capsys, tmp_path):
        assert main.cmd_analyze_corpus(
            SimpleNamespace(path=str(tmp_path / "absent.jsonl"))
        ) == 1
        assert "corpus error" in capsys.readouterr().err


def _cfg_for(repo):
    """Build the minimal config object the analysis CLI reads from ``repo``."""

    return SimpleNamespace(
        db_path=repo.db.path,
        processing_max_attempts=repo.processing_max_attempts,
    )
