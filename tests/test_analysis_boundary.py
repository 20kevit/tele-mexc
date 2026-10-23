"""Exchange/trading boundary: analysis is offline, read-only for source data."""

from __future__ import annotations

import ast
import socket
from pathlib import Path

from tests.conftest import FakeMsg
from tg_signal_ingest.analysis import (
    ACTIONABLE_CLASSIFICATIONS,
    ReplayScope,
    SignalClassification,
    parse_message,
    run_replay,
)
from tg_signal_ingest.normalize import message_to_record

ANALYSIS_DIR = Path(__file__).resolve().parents[1] / "tg_signal_ingest" / "analysis"
FORBIDDEN_IMPORT_ROOTS = {
    "ccxt",
    "http",
    "httpx",
    "mexc",
    "requests",
    "socket",
    "ssl",
    "subprocess",
    "telegram",
    "telethon",
    "trading",
    "urllib",
}


def _import_roots(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                continue  # relative imports stay inside the project
            if node.module:
                roots.add(node.module.split(".")[0])
    return roots


def test_analysis_package_has_no_network_exchange_or_trading_imports():
    files = sorted(ANALYSIS_DIR.glob("*.py"))
    assert files
    for path in files:
        roots = _import_roots(path)
        assert not (roots & FORBIDDEN_IMPORT_ROOTS), f"{path.name} imports {roots & FORBIDDEN_IMPORT_ROOTS}"


def test_analysis_package_does_not_reference_exchange_clients():
    forbidden_tokens = ("ccxt", "requests.", "httpx.", "aiohttp", "api_key", "api_secret", "create_order")
    for path in sorted(ANALYSIS_DIR.glob("*.py")):
        source = path.read_text(encoding="utf-8").lower()
        for token in forbidden_tokens:
            assert token not in source, f"{path.name} contains {token!r}"


def test_actionable_classifications_are_only_signal_like():
    assert {
        SignalClassification.NEW_SIGNAL,
        SignalClassification.MODIFICATION,
    } == ACTIONABLE_CLASSIFICATIONS
    for classification in (
        SignalClassification.CANCELLATION,
        SignalClassification.TARGET_UPDATE,
        SignalClassification.STOP_LOSS_UPDATE,
        SignalClassification.RESULT,
        SignalClassification.NON_SIGNAL,
        SignalClassification.AMBIGUOUS,
    ):
        assert classification not in ACTIONABLE_CLASSIFICATIONS


def test_parse_and_replay_make_no_socket_calls(repo, monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("analysis attempted network access")

    monkeypatch.setattr(socket, "socket", fail)
    monkeypatch.setattr(socket, "create_connection", fail)
    monkeypatch.setattr(socket, "socketpair", fail)

    repo.upsert_message(
        message_to_record(100, FakeMsg(1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95"), "live"),
        enqueue_processing=False,
    )
    parsed = parse_message("BUY BTCUSDT ENTRY 100 TP 120 SL 95")
    assert parsed.symbol == "BTCUSDT"
    stats = run_replay(repo, ReplayScope(channel_id=100))
    assert stats.inserted == 1


def test_replay_leaves_trading_and_queue_tables_empty(repo):
    repo.upsert_message(
        message_to_record(100, FakeMsg(1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95"), "live"),
        enqueue_processing=False,
    )
    run_replay(repo, ReplayScope(channel_id=100))

    for table in ("processing_queue", "signals", "order_intents", "exchange_orders"):
        count = repo.db.conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        assert count == 0, f"{table} was modified by offline analysis"


def test_analysis_never_rewrites_source_message(repo):
    repo.upsert_message(
        message_to_record(100, FakeMsg(1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95"), "live"),
        enqueue_processing=False,
    )
    before = dict(repo.get_message_raw(100, 1))
    run_replay(repo, ReplayScope(channel_id=100))
    after = dict(repo.get_message_raw(100, 1))

    for field in ("text", "caption", "raw_json", "processing_version", "deleted", "datetime"):
        assert after[field] == before[field]


def test_no_environment_or_secret_files_are_read_by_parser(monkeypatch):
    # A parser that relied on environment credentials would fail here.
    import os

    for key in ("TELEGRAM_API_HASH", "MEXC_API_KEY", "MEXC_API_SECRET", "CONTROL_BOT_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    parsed = parse_message("BUY BTCUSDT ENTRY 100 TP 120 SL 95")
    assert parsed.parse_status.value == "parsed"
    assert os.environ.get("MEXC_API_KEY") is None


def test_network_imports_are_absent_from_requirement_surface():
    # The analysis modules must not add a runtime dependency on exchange SDKs.
    import tomllib

    project = tomllib.loads(
        (ANALYSIS_DIR.parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]
    declared = list(project.get("dependencies", []))
    for group in project.get("optional-dependencies", {}).values():
        declared.extend(group)
    normalized = " ".join(declared).lower()
    for package in ("ccxt", "binance", "mexc"):
        assert package not in normalized


def test_failure_is_recorded_without_attempting_recovery_over_network(repo, monkeypatch):
    class BrokenParser:
        version = 1

        def parse(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    def fail(*_args, **_kwargs):
        raise AssertionError("network access during failure handling")

    monkeypatch.setattr(socket, "create_connection", fail)
    repo.upsert_message(
        message_to_record(100, FakeMsg(1, "BUY BTCUSDT ENTRY 100 TP 120 SL 95"), "live"),
        enqueue_processing=False,
    )
    stats = run_replay(repo, ReplayScope(channel_id=100), parser=BrokenParser())
    assert stats.failures == 1
    assert repo.db.conn.execute(
        "SELECT COUNT(*) AS c FROM analysis_failures WHERE resolved_at IS NULL"
    ).fetchone()["c"] == 1
