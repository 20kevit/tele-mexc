"""First-startup readiness report: deterministic, non-trading, fail-closed."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tests.test_trading_engine import ALERT
from tg_signal_ingest.trading import readiness as readiness_module
from tg_signal_ingest.trading.readiness import build_readiness_report
from tg_signal_ingest.trading.store import TradingStore


@pytest.fixture()
def configured(cfg, repo, monkeypatch):
    monkeypatch.setattr(readiness_module, "_run_git", lambda args: {"rev-parse": "abc123"}.get(args[0], ""))
    cfg.mexc.api_key = "test-key"
    cfg.mexc.api_secret = "test-secret"
    cfg.mexc.cli_enabled = True
    cfg.mexc.cli_confirm_noninteractive_writes = True
    cfg.control.bot_token = "123:token"
    cfg.control.owner_user_id = 100000001
    cfg.control.owner_b_user_id = 100000003
    cfg.control.developer_user_id = 100000002
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE control_setup SET telegram_state='authenticated', mexc_state='verified', "
            "selected_channel_id=100, paused=0 WHERE id=1"
        )
    TradingStore(repo.db, cfg.trading).ensure_activation_boundary(100, minimum=50)
    return cfg, repo


def _status(report, name: str) -> str:
    return next(check.status for check in report.checks if check.name == name)


def test_ready_report_is_ready_without_network(configured):
    cfg, repo = configured
    report = build_readiness_report(
        cfg,
        repo,
        network=False,
        cli_checker=lambda: {"ok": True, "detail": "0.0.22"},
    )
    assert report.ready, report.render()
    assert report.verdict() == "READY FOR FIRST CONTROLLED STARTUP"
    assert _status(report, "Owner A") == "PASS"
    assert _status(report, "Owner B") == "PASS"
    assert _status(report, "programmer role") == "PASS"
    assert _status(report, "activation boundary") == "PASS"
    assert _status(report, "Telegram authentication") == "NOT_VERIFIED"
    assert "no exchange writes" in report.render()


def test_missing_second_owner_is_optional(configured):
    cfg, repo = configured
    cfg.control.owner_b_user_id = None
    report = build_readiness_report(cfg, repo, cli_checker=lambda: {"ok": True})
    assert report.ready, report.render()
    assert _status(report, "Owner B") == "PASS"


def test_missing_boundary_is_set_at_startup_not_a_manual_blocker(configured):
    cfg, repo = configured
    repo.db.conn.execute("DELETE FROM app_state WHERE key LIKE 'trading.activation_boundary%'")
    repo.db.conn.commit()
    report = build_readiness_report(cfg, repo, cli_checker=lambda: {"ok": True})
    assert report.ready, report.render()
    assert _status(report, "activation boundary") == "WARN"


def test_unresolved_and_unprotected_state_is_reported_but_not_a_global_blocker(configured):
    cfg, repo = configured
    store = TradingStore(repo.db, cfg.trading)
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="readiness-idem",
        client_order_id="readiness-client",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET state='unknown_requires_reconciliation' WHERE intent_id=?",
            (intent["intent_id"],),
        )
        conn.execute(
            "INSERT INTO trades (trade_id, signal_id, idempotency_key, channel_id, message_id, "
            "symbol, side, entry_mode, requested_quote, entry_price, trade_state, "
            "created_at, updated_at) VALUES ('trade-x', ?, 'k', 100, 1, 'BTCUSDT', 'buy', "
            "'direct', '10', '100', 'unprotected_critical', datetime('now'), datetime('now'))",
            (signal_id,),
        )
    report = build_readiness_report(cfg, repo, cli_checker=lambda: {"ok": True})
    assert report.ready, report.render()
    assert _status(report, "unresolved submissions") == "WARN"
    assert _status(report, "unprotected / uncertain positions") == "WARN"


def test_emergency_stop_and_pause_are_reported(configured):
    cfg, repo = configured
    cfg.trading.emergency_stop = True
    with repo.db.transaction() as conn:
        conn.execute("UPDATE control_setup SET paused=1, pause_reason='owner pause' WHERE id=1")
    report = build_readiness_report(cfg, repo, cli_checker=lambda: {"ok": True})
    assert _status(report, "emergency stop") == "FAIL"
    assert _status(report, "pause state") == "WARN"
    assert not report.ready


def _detail(report, name: str) -> str:
    return next(check.detail for check in report.checks if check.name == name)


def test_worktree_classifier_allows_only_documented_local_artifacts():
    status = "\n".join(
        [
            "?? migrations/0001.sql",
            "?? reports/PHASE5_INDEPENDENT_AUDIT.md",
            "?? tests/test_accounting_backfill.py",
            "?? tests/fixtures/accounting/snapshot.json",
            "?? tg_signal_ingest/accounting/store.py",
            "?? reports/subdir/PHASE5.md",
            "?? reports/notes.txt",
            "?? tests/test_other.py",
            "?? tg_signal_ingest/mexc/unreleased.py",
            "?? notes.txt",
            " M docs/trading.md",
        ]
    )
    unexpected, local_artifacts = readiness_module.classify_worktree_status(status)
    assert local_artifacts == [
        "migrations/0001.sql",
        "reports/PHASE5_INDEPENDENT_AUDIT.md",
        "tests/test_accounting_backfill.py",
        "tests/fixtures/accounting/snapshot.json",
        "tg_signal_ingest/accounting/store.py",
    ]
    assert unexpected == [
        "reports/subdir/PHASE5.md",
        "reports/notes.txt",
        "tests/test_other.py",
        "tg_signal_ingest/mexc/unreleased.py",
        "notes.txt",
        " M docs/trading.md",
    ]


def test_known_local_accounting_artifacts_do_not_block_readiness(configured, monkeypatch):
    cfg, repo = configured
    status = "\n".join(
        [
            "?? migrations/",
            "?? reports/PHASE5_INDEPENDENT_AUDIT.md",
            "?? tests/fixtures/accounting/",
            "?? tests/test_accounting_backfill.py",
            "?? tg_signal_ingest/accounting/",
        ]
    )
    monkeypatch.setattr(
        readiness_module,
        "_run_git",
        lambda args: status if args[0] == "status" else "abc123",
    )
    report = build_readiness_report(cfg, repo, network=False, cli_checker=lambda: {"ok": True, "detail": "0.0.22"})
    assert _status(report, "working tree") == "PASS"
    assert "known local accounting artifact" in _detail(report, "working tree")
    assert report.ready, report.render()


def test_tracked_source_modification_still_blocks_readiness(configured, monkeypatch):
    cfg, repo = configured
    status = "?? tg_signal_ingest/accounting/\n M tg_signal_ingest/trading/readiness.py"
    monkeypatch.setattr(
        readiness_module,
        "_run_git",
        lambda args: status if args[0] == "status" else "abc123",
    )
    report = build_readiness_report(cfg, repo, network=False, cli_checker=lambda: {"ok": True, "detail": "0.0.22"})
    assert _status(report, "working tree") == "FAIL"
    assert "tg_signal_ingest/trading/readiness.py" in _detail(report, "working tree")
    assert not report.ready


def test_unexpected_untracked_file_still_blocks_readiness(configured, monkeypatch):
    cfg, repo = configured
    status = "?? reports/PHASE5_INDEPENDENT_AUDIT.md\n?? tools/unreleased_script.sh"
    monkeypatch.setattr(
        readiness_module,
        "_run_git",
        lambda args: status if args[0] == "status" else "abc123",
    )
    report = build_readiness_report(cfg, repo, network=False, cli_checker=lambda: {"ok": True, "detail": "0.0.22"})
    assert _status(report, "working tree") == "FAIL"
    assert "tools/unreleased_script.sh" in _detail(report, "working tree")
    assert not report.ready
