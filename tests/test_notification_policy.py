"""Approved notification severity/routing/delivery policy and failure injection.

Every test uses temp SQLite files and deterministic fake transports.  No test
can reach Telegram or MEXC, and no test places, cancels, or reconciles an order.
"""

from __future__ import annotations

import ast
import json
import sqlite3
import time
from pathlib import Path

import pytest

from tg_signal_ingest.control.bot_api import (
    BotApiClient,
    BotApiError,
    BotApiNotSentError,
    BotApiRateLimitError,
    BotApiTransportError,
)
from tg_signal_ingest.notification_dispatcher import (
    DEFAULT_CATEGORY_PREFERENCES,
    MAX_ATTEMPTS,
    NOTIFICATION_CATEGORY_BY_EVENT,
    NotificationDispatcher,
    delivery_decision,
    event_thread_key,
    notification_category,
    recipient_minimum_severity,
    render_domain_event,
)
from tg_signal_ingest.trading.domain_events import (
    EVENT_REGISTRY,
    DomainEventError,
    build_domain_event,
)

OWNER = 101
OWNER_B = 202
DEVELOPER = 303


def _source(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute(
        """
        CREATE TABLE audit_events (
            audit_id INTEGER PRIMARY KEY,
            event_class TEXT,
            event_id TEXT,
            event_type TEXT,
            schema_version INTEGER,
            occurred_at_utc TEXT,
            aggregate_type TEXT,
            aggregate_id TEXT,
            correlation_id TEXT,
            symbol TEXT,
            severity TEXT,
            payload_json TEXT,
            audience_json TEXT
        )
        """
    )
    conn.execute(
        "CREATE TABLE notification_preferences ("
        "role TEXT, category TEXT, enabled INTEGER, updated_at TEXT, "
        "PRIMARY KEY(role,category))"
    )
    conn.execute("CREATE TABLE control_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT)")
    conn.execute(
        """CREATE TABLE trading_notifications (
            notification_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT, category TEXT, event_type TEXT, role TEXT,
            chat_id INTEGER, text TEXT, status TEXT
        )"""
    )
    conn.commit()
    return conn


def _prefs(conn: sqlite3.Connection, values: dict[tuple[str, str], int]) -> None:
    for (role, category), enabled in values.items():
        conn.execute(
            "INSERT OR REPLACE INTO notification_preferences(role,category,enabled,updated_at) "
            "VALUES(?,?,?,'now')",
            (role, category, enabled),
        )
    conn.commit()


def _setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO control_settings(key,value,updated_at) VALUES(?,?,'now')",
        (key, value),
    )
    conn.commit()


class RecordingBot:
    def __init__(self, *, fail=None):
        self.calls: list[tuple[int, str, int | None]] = []
        self.fail = fail or {}
        self.next_id = 1000

    def send_message(self, chat_id: int, text: str, *, reply_to_message_id: int | None = None):
        self.calls.append((chat_id, text, reply_to_message_id))
        error = self.fail.get(chat_id)
        if error is not None:
            raise error
        self.next_id += 1
        return {"message_id": self.next_id}


def _worker(tmp_path: Path, bot, *, recipients=None, prefs=None, settings=None):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    for (role, category), enabled in (prefs or {}).items():
        source.execute(
            "INSERT OR REPLACE INTO notification_preferences(role,category,enabled,updated_at) "
            "VALUES(?,?,?,'now')",
            (role, category, enabled),
        )
    for key, value in (settings or {}).items():
        source.execute(
            "INSERT OR REPLACE INTO control_settings(key,value,updated_at) VALUES(?,?,'now')",
            (key, value),
        )
    source.commit()
    worker = NotificationDispatcher(
        source_path,
        tmp_path / "delivery.sqlite3",
        bot,
        recipients or {"owner": OWNER, "owner_b": OWNER_B, "developer": DEVELOPER},
    )
    return source, worker


def _insert(
    conn: sqlite3.Connection,
    audit_id: int,
    event_type: str,
    payload: dict,
    *,
    severity: str = "INFO",
    event_id: str | None = None,
    audience: list[str] | None = None,
    aggregate_type: str = "intent",
    aggregate_id: str = "intent-1",
    correlation_id: str = "signal-1",
) -> None:
    conn.execute(
        "INSERT INTO audit_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            audit_id,
            "domain",
            event_id or f"evt-{audit_id}",
            event_type,
            1,
            "2026-11-12T12:00:00Z",
            aggregate_type,
            aggregate_id,
            correlation_id,
            "BTCUSDT",
            severity,
            json.dumps(payload),
            json.dumps(audience or ["owner", "owner_b", "developer"]),
        ),
    )
    conn.commit()


def _statuses(worker: NotificationDispatcher, event_id: str) -> dict[str, str]:
    return {
        str(row["recipient_role"]): str(row["status"])
        for row in worker.store.conn.execute(
            "SELECT recipient_role,status FROM deliveries WHERE event_id=? ORDER BY recipient_role",
            (event_id,),
        ).fetchall()
    }


# ------------------------------------------------------------------ routing


def test_every_registered_event_has_a_safe_category_and_renderer():
    sample = {
        "state": "parsed",
        "signal_kind": "alert",
        "reason": "reason",
        "previous_state": "open",
        "new_state": "closed",
        "exchange_order_id": "1",
        "kind": "tp1",
        "target": "tp1",
        "operation_type": "place_order",
        "outcome": "success",
        "event_key": "key",
        "message": "message",
        "detail": "detail",
        "quantity": "1",
    }
    for event_type, spec in EVENT_REGISTRY.items():
        # Every registered producer event must have an explicit user-facing
        # category; the safe "diagnostic" fallback is only for unknown types.
        assert event_type in NOTIFICATION_CATEGORY_BY_EVENT
        category = notification_category(event_type)
        assert category in DEFAULT_CATEGORY_PREFERENCES["owner"] or category == "diagnostic"
        payload = {key: sample[key] for key in spec.required_payload}
        row = {
            "event_type": event_type,
            "event_id": "evt",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(payload),
        }
        rendered = render_domain_event(row)
        assert rendered.strip()
        assert "{" not in rendered  # never dump a raw JSON payload to a recipient


def test_unknown_event_uses_safe_fallback():
    row = {
        "event_type": "future.event_type",
        "event_id": "evt-unknown",
        "symbol": "BTCUSDT",
        "payload_json": "{not-json",
    }
    rendered = render_domain_event(row)
    assert "future.event_type" in rendered
    assert "evt-unknown" in rendered


def test_all_five_severities_follow_the_policy(tmp_path):
    source, worker = _worker(tmp_path, RecordingBot())
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"}, severity="DEBUG")
        _insert(source, 2, "trade.state_changed", {"previous_state": "a", "new_state": "b"}, severity="INFO")
        _insert(source, 3, "trade.state_changed", {"previous_state": "a", "new_state": "b"}, severity="WARNING")
        _insert(source, 4, "trade.state_changed", {"previous_state": "a", "new_state": "b"}, severity="ERROR")
        _insert(source, 5, "trade.state_changed", {"previous_state": "a", "new_state": "b"}, severity="CRITICAL")
        worker.run_once()
        assert _statuses(worker, "evt-1") == {
            "developer": "sent", "owner": "suppressed", "owner_b": "suppressed"
        }
        for event_id in ("evt-2", "evt-3", "evt-4"):
            assert _statuses(worker, event_id) == {
                "developer": "sent", "owner": "sent", "owner_b": "sent"
            }
        # CRITICAL reaches every configured recipient.
        assert _statuses(worker, "evt-5") == {
            "developer": "sent", "owner": "sent", "owner_b": "sent"
        }
    finally:
        worker.close()
        source.close()


def test_debug_never_reaches_owners_even_when_category_is_enabled(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "signal_accepted"): 1, ("owner_b", "signal_accepted"): 1},
    )
    try:
        _insert(source, 1, "signal.received", {"state": "parsed", "signal_kind": "alert"},
                severity="DEBUG")
        worker.run_once()
        assert _statuses(worker, "evt-1") == {
            "developer": "sent", "owner": "suppressed", "owner_b": "suppressed"
        }
        row = worker.store.conn.execute(
            "SELECT last_error FROM deliveries WHERE recipient_role='owner'"
        ).fetchone()
        assert row["last_error"] == "DEBUG is developer-only"
    finally:
        worker.close()
        source.close()


def test_debug_cannot_bypass_disabled_diagnostic_preference(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("developer", "diagnostic"): 0},
    )
    try:
        _insert(source, 1, "future.event", {}, severity="DEBUG")
        worker.run_once()
        assert _statuses(worker, "evt-1")["developer"] == "suppressed"
    finally:
        worker.close()
        source.close()


def test_critical_bypasses_category_minimum_and_audience(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "critical"): 0, ("owner_b", "critical"): 0, ("developer", "critical"): 0},
        settings={"notification_min_severity": "CRITICAL"},
    )
    try:
        _insert(source, 1, "trade.protection_critical",
                {"event_key": "SL_UNPROTECTED_CRITICAL", "detail": "unprotected"},
                severity="CRITICAL", audience=["owner"])
        worker.run_once()
        assert _statuses(worker, "evt-1") == {
            "developer": "sent", "owner": "sent", "owner_b": "sent"
        }
    finally:
        worker.close()
        source.close()


def test_owner_safety_states_bypass_minimum_and_category(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "order_lifecycle"): 0, ("owner_b", "order_lifecycle"): 0},
        settings={"notification_min_severity": "CRITICAL"},
    )
    try:
        _insert(source, 1, "trade.state_changed",
                {"previous_state": "oco_partial", "new_state": "unprotected_critical"},
                severity="WARNING")
        _insert(source, 2, "exchange.write_outcome",
                {"operation_type": "place_order", "outcome": "uncertain"}, severity="WARNING")
        worker.run_once()
        # Owners see the safety alert; the developer still honours the
        # CRITICAL threshold for routine WARNING events.
        assert _statuses(worker, "evt-1") == {
            "developer": "suppressed", "owner": "sent", "owner_b": "sent"
        }
        assert _statuses(worker, "evt-2") == {
            "developer": "suppressed", "owner": "sent", "owner_b": "sent"
        }
    finally:
        worker.close()
        source.close()


def test_per_recipient_minimum_severity_is_independent(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "order_lifecycle"): 1, ("owner_b", "order_lifecycle"): 1},
        settings={
            "notification_min_severity": "ERROR",
            "notification_min_severity.owner_b": "INFO",
        },
    )
    try:
        _insert(source, 1, "trade.state_changed",
                {"previous_state": "a", "new_state": "b"}, severity="WARNING")
        worker.run_once()
        assert _statuses(worker, "evt-1") == {
            "developer": "suppressed", "owner": "suppressed", "owner_b": "sent"
        }
    finally:
        worker.close()
        source.close()


def test_genuine_rejection_is_warning_and_owner_visible(tmp_path):
    assert build_domain_event(
        "signal.rejected",
        aggregate_id="signal-1",
        payload={"reason": "ambiguous signal", "previous_state": "parsed", "new_state": "rejected"},
    ).severity == "WARNING"

    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "signal_decision"): 0, ("owner_b", "signal_decision"): 0,
               ("developer", "signal_decision"): 0},
        settings={"notification_min_severity": "CRITICAL"},
    )
    try:
        _insert(source, 1, "signal.rejected",
                {"reason": "ambiguous signal", "previous_state": "parsed", "new_state": "rejected"},
                severity="WARNING")
        worker.run_once()
        assert _statuses(worker, "evt-1") == {
            "developer": "suppressed", "owner": "sent", "owner_b": "sent"
        }
        text = worker.store.conn.execute(
            "SELECT message_text FROM deliveries WHERE recipient_role='owner'"
        ).fetchone()["message_text"]
        assert "سیگنال رد شد" in text
        assert "✅" not in text
    finally:
        worker.close()
        source.close()


def test_acceptance_requires_an_exchange_order_id():
    with pytest.raises(DomainEventError):
        build_domain_event("intent.submission_accepted", aggregate_id="intent-1", payload={})
    accepted = build_domain_event(
        "intent.submission_accepted",
        aggregate_id="intent-1",
        payload={"exchange_order_id": "987654", "exchange_status": "NEW"},
    )
    assert accepted.severity == "INFO"


def test_routing_helpers_are_deterministic():
    assert severity_enabled_for_test("WARNING", "INFO") is True
    assert recipient_minimum_severity("owner", {}, "INFO") == "INFO"
    assert recipient_minimum_severity("owner_b", {"notification_min_severity.owner_b": "ERROR"}, "INFO") == "ERROR"
    assert recipient_minimum_severity("developer", {}, "INFO") == "INFO"
    assert recipient_minimum_severity("developer", {"notification_min_severity.developer": "DEBUG"}, "INFO") == "DEBUG"
    status, reason = delivery_decision(
        "owner", "DEBUG", "order_lifecycle", "trade.state_changed", {}, {}, {}, "INFO"
    )
    assert (status, reason) == ("suppressed", "DEBUG is developer-only")
    assert event_thread_key(
        "trade.state_changed", aggregate_type="trade", aggregate_id="t1",
        correlation_id="t1", payload={}, severity="INFO",
    ) == "trade:t1"
    assert event_thread_key(
        "trade.protection_critical", aggregate_type="trade", aggregate_id="t1",
        correlation_id="t1", payload={}, severity="CRITICAL",
    ) is None


def severity_enabled_for_test(severity: str, minimum: str) -> bool:
    from tg_signal_ingest.notification_dispatcher import severity_enabled

    return severity_enabled(severity, minimum)


# ----------------------------------------------------------- dedupe/history


def test_history_is_preserved_when_suppressed(tmp_path):
    source, worker = _worker(
        tmp_path,
        RecordingBot(),
        prefs={("owner", "order_lifecycle"): 0},
    )
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute(
            "SELECT status,last_error,message_text,severity,notification_category "
            "FROM deliveries WHERE recipient_role='owner'"
        ).fetchone()
        assert row["status"] == "suppressed"
        assert row["last_error"] == "category disabled"
        assert row["message_text"]
        assert row["severity"] == "INFO"
        assert row["notification_category"] == "order_lifecycle"
    finally:
        worker.close()
        source.close()


def test_only_identical_event_id_is_deduplicated(tmp_path):
    source, worker = _worker(tmp_path, RecordingBot())
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"},
                event_id="same-event")
        worker.run_once()
        first = worker.store.conn.execute("SELECT COUNT(*) AS n FROM deliveries").fetchone()["n"]
        # A duplicate audit row with the same event_id must not create a second
        # delivery even though its audit_id/text differ.
        _insert(source, 2, "trade.state_changed", {"previous_state": "a", "new_state": "c"},
                event_id="same-event")
        worker.run_once()
        second = worker.store.conn.execute("SELECT COUNT(*) AS n FROM deliveries").fetchone()["n"]
        assert first == second == 3

        # Distinct lifecycle events are never collapsed, including a repeated
        # transition to the same state.
        _insert(source, 3, "trade.state_changed", {"previous_state": "b", "new_state": "b"},
                event_id="second-event")
        _insert(source, 4, "trade.state_changed", {"previous_state": "b", "new_state": "b"},
                event_id="third-event")
        worker.run_once()
        assert worker.store.conn.execute("SELECT COUNT(*) AS n FROM deliveries").fetchone()["n"] == 9
    finally:
        worker.close()
        source.close()


# --------------------------------------------------------------- threading


def test_root_and_replies_are_per_recipient(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot)
    try:
        _insert(source, 1, "intent.created", {"state": "planned"}, aggregate_type="intent",
                aggregate_id="intent-1", correlation_id="signal-1")
        _insert(source, 2, "intent.state_changed",
                {"previous_state": "submitting", "new_state": "submitted",
                 "exchange_order_id": "999"},
                aggregate_type="intent", aggregate_id="intent-1", correlation_id="signal-1")
        _insert(source, 3, "trade.protection_critical",
                {"event_key": "SL_UNPROTECTED_CRITICAL", "detail": "unprotected"},
                severity="CRITICAL", aggregate_type="trade", aggregate_id="trade-1",
                correlation_id="trade-1")
        worker.run_once()

        by_owner = [call for call in bot.calls if call[0] == OWNER]
        assert by_owner[0][2] is None  # root
        owner_root = worker.store.thread_root("owner", "intent:intent-1")
        assert owner_root is not None
        assert by_owner[1][2] == owner_root  # replies to owner's own root
        assert by_owner[2][2] is None  # critical is standalone

        roots = worker.store.conn.execute(
            "SELECT recipient_role,thread_key,root_message_id FROM delivery_threads "
            "ORDER BY recipient_role"
        ).fetchall()
        assert {(row["recipient_role"], row["thread_key"]) for row in roots} == {
            ("developer", "intent:intent-1"),
            ("owner", "intent:intent-1"),
            ("owner_b", "intent:intent-1"),
        }
    finally:
        worker.close()
        source.close()


def test_deleted_root_falls_back_to_standalone_not_dead_letter(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot)
    try:
        _insert(source, 1, "intent.created", {"state": "planned"}, aggregate_type="intent",
                aggregate_id="intent-1")
        worker.run_once()
        root_id = worker.store.thread_root("owner", "intent:intent-1")
        assert root_id is not None

        _insert(source, 2, "intent.state_changed",
                {"previous_state": "submitting", "new_state": "submitted",
                 "exchange_order_id": "999"},
                aggregate_type="intent", aggregate_id="intent-1")
        bot.fail[OWNER] = BotApiError("Bad Request: message to be replied not found")
        # The first owner send (reply) fails; the fallback standalone send must
        # succeed because the fake only fails once per chat in this dict.
        def send(chat_id, text, *, reply_to_message_id=None):
            bot.calls.append((chat_id, text, reply_to_message_id))
            if chat_id == OWNER and reply_to_message_id is not None:
                raise BotApiError("Bad Request: message to be replied not found")
            bot.next_id += 1
            return {"message_id": bot.next_id}

        bot.send_message = send
        worker._supports_reply = True
        worker.run_once()
        status = worker.store.conn.execute(
            "SELECT status FROM deliveries WHERE event_id='evt-2' AND recipient_role='owner'"
        ).fetchone()["status"]
        assert status == "sent"
    finally:
        worker.close()
        source.close()


def test_long_legacy_message_is_chunked_in_order(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot, prefs={("owner", "order_lifecycle"): 1})
    try:
        text = "line-0000\n" + "\n".join(f"line-{i:04d}" for i in range(1, 900))
        assert len(text) > 2 * 3900
        source.execute(
            "INSERT INTO trading_notifications(event_key,category,event_type,role,chat_id,text,status) "
            "VALUES('long','order_lifecycle','trade.state_changed','owner',?,?,'pending')",
            (OWNER, text),
        )
        source.commit()
        worker.run_once()
        calls = [call for call in bot.calls if call[0] == OWNER]
        assert len(calls) == 3
        assert "".join(call[1] for call in calls) == text
        parts = worker.store.conn.execute(
            "SELECT part_no,part_count,status FROM deliveries ORDER BY part_no"
        ).fetchall()
        assert [row["part_no"] for row in parts] == [1, 2, 3]
        assert all(row["part_count"] == 3 and row["status"] == "sent" for row in parts)
    finally:
        worker.close()
        source.close()


# ------------------------------------------------------- delivery failures


def test_pre_send_dns_connect_is_retryable(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiNotSentError("dns failure before send")})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute(
            "SELECT status,attempts,next_attempt_at FROM deliveries"
        ).fetchone()
        assert row["status"] == "retry"
        assert row["attempts"] == 1
        assert row["next_attempt_at"]
        # A scheduled retry is not sent again in the same/immediate cycle.
        worker.run_once()
        assert len(bot.calls) == 1
    finally:
        worker.close()
        source.close()


def test_timeout_after_possible_send_becomes_ambiguous_and_is_never_retried(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiTransportError("timeout after possible send")})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute(
            "SELECT status,attempts,next_attempt_at FROM deliveries"
        ).fetchone()
        assert (row["status"], row["attempts"], row["next_attempt_at"]) == ("ambiguous", 1, None)
        worker.run_once()
        assert len(bot.calls) == 1
    finally:
        worker.close()
        source.close()


def test_http_5xx_response_is_ambiguous_not_retried(tmp_path):
    client = BotApiClient(
        "123:token",
        transport=lambda method, payload: {
            "ok": False, "error_code": 502, "description": "Bad Gateway"
        },
    )
    source, worker = _worker(tmp_path, client, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute("SELECT status FROM deliveries").fetchone()
        assert row["status"] == "ambiguous"
    finally:
        worker.close()
        source.close()


def test_permanent_4xx_is_dead_letter_without_retry(tmp_path):
    client = BotApiClient(
        "123:token",
        transport=lambda method, payload: {
            "ok": False, "error_code": 400, "description": "Bad Request: chat not found"
        },
    )
    source, worker = _worker(tmp_path, client, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute(
            "SELECT status,attempts,next_attempt_at FROM deliveries"
        ).fetchone()
        assert (row["status"], row["attempts"], row["next_attempt_at"]) == ("dead_letter", 1, None)
        worker.run_once()
    finally:
        worker.close()
        source.close()


def test_malformed_telegram_response_is_ambiguous(tmp_path):
    client = BotApiClient(
        "123:token",
        transport=lambda method, payload: (_ for _ in ()).throw(TimeoutError()),
    )
    source, worker = _worker(tmp_path, client, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        assert worker.store.conn.execute(
            "SELECT status FROM deliveries"
        ).fetchone()["status"] == "ambiguous"
    finally:
        worker.close()
        source.close()


def test_rate_limit_uses_structured_retry_after_without_blocking(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiRateLimitError("429", retry_after=17)})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        started = time.monotonic()
        worker.run_once()
        assert time.monotonic() - started < 5  # scheduled, never slept
        row = worker.store.conn.execute(
            "SELECT status,next_attempt_at FROM deliveries"
        ).fetchone()
        assert row["status"] == "retry" and row["next_attempt_at"]
        attempt = worker.store.conn.execute(
            "SELECT retry_after_s FROM delivery_attempts"
        ).fetchone()
        assert attempt["retry_after_s"] == 17
    finally:
        worker.close()
        source.close()


def test_worker_crash_after_telegram_accept_before_db_ack_never_resends(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.relay_once()
        claimed = worker.store.claim_next()
        assert claimed is not None
        # Simulate Telegram accepting the message, then the process dying before
        # the delivery row is acknowledged.
        bot.send_message(OWNER, claimed["message_text"])
        worker.store.conn.execute(
            "UPDATE deliveries SET claimed_at='2000-01-01T00:00:00Z' WHERE delivery_id=?",
            (claimed["delivery_id"],),
        )
        recovered = worker.store.recover_stale_claims(older_than_s=1)
        assert recovered == 1
        worker.run_once()
        assert len(bot.calls) == 1  # never a blind duplicate
        assert worker.store.conn.execute(
            "SELECT status FROM deliveries"
        ).fetchone()["status"] == "ambiguous"
    finally:
        worker.close()
        source.close()


def test_stale_worker_cannot_overwrite_a_newer_result(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.relay_once()
        claimed = worker.store.claim_next()
        assert claimed is not None
        worker.store.conn.execute(
            "UPDATE deliveries SET claimed_at='2000-01-01T00:00:00Z' WHERE delivery_id=?",
            (claimed["delivery_id"],),
        )
        assert worker.store.recover_stale_claims(older_than_s=1) == 1
        # The original worker wakes up and tries to record success.
        worker.store.finish(claimed, "sent", message_id=42)
        row = worker.store.conn.execute(
            "SELECT status,telegram_message_id FROM deliveries"
        ).fetchone()
        assert (row["status"], row["telegram_message_id"]) == ("ambiguous", None)
    finally:
        worker.close()
        source.close()


def test_future_retry_does_not_block_a_ready_delivery(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiNotSentError("down")})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER, "owner_b": OWNER_B})
    try:
        # One owner delivery is scheduled far in the future.
        worker.store.conn.execute(
            "INSERT INTO deliveries (delivery_id,event_id,recipient_role,chat_id,event_type,"
            "notification_category,severity,message_text,status,attempts,next_attempt_at,"
            "created_at,updated_at) VALUES('blocked','e1','owner',?,'x','diagnostic','INFO',"
            "'x','retry',3,'2999-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            (OWNER,),
        )
        worker.store.conn.execute(
            "INSERT INTO deliveries (delivery_id,event_id,recipient_role,chat_id,event_type,"
            "notification_category,severity,message_text,status,attempts,created_at,updated_at) "
            "VALUES('ready','e2','owner_b',?,'x','diagnostic','INFO','x','pending',0,"
            "'2026-01-02T00:00:00Z','2026-01-02T00:00:00Z')",
            (OWNER_B,),
        )
        worker.store.conn.commit()
        worker.run_once(delivery_limit=1)
        assert [call[0] for call in bot.calls] == [OWNER_B]
    finally:
        worker.close()
        source.close()


def test_delivery_db_lock_does_not_touch_the_trading_database(tmp_path):
    bot = RecordingBot()
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.relay_once()
        worker.store.conn.execute("PRAGMA busy_timeout=1")
        blocker = sqlite3.connect(tmp_path / "delivery.sqlite3", isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        try:
            with pytest.raises(sqlite3.OperationalError):
                worker.store.claim_next()
        finally:
            blocker.execute("ROLLBACK")
            blocker.close()
        # Recovery after the lock is released; the trading DB was never written.
        worker.run_once()
        assert worker.store.conn.execute(
            "SELECT status FROM deliveries"
        ).fetchone()["status"] == "sent"
        # The dispatcher's view of the trading database is hard read-only.
        with pytest.raises(sqlite3.OperationalError):
            worker._source.execute("INSERT INTO audit_events(audit_id) VALUES(999)")
    finally:
        worker.close()
        source.close()


def test_operator_requeue_refuses_when_sibling_parts_exhaust_shared_budget(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiNotSentError("down")})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        row = worker.store.conn.execute("SELECT * FROM deliveries").fetchone()
        delivery_id = row["delivery_id"]
        worker.store.conn.execute(
            "UPDATE deliveries SET status='dead_letter',attempts=? WHERE delivery_id=?",
            (MAX_ATTEMPTS, delivery_id),
        )
        worker.store.conn.execute(
            "INSERT INTO deliveries "
            "(delivery_id,event_id,recipient_role,chat_id,event_type,notification_category,severity,"
            "message_text,part_no,part_count,status,attempts,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,'sent',?,?,?)",
            (
                "sibling-part", row["event_id"], row["recipient_role"], row["chat_id"],
                row["event_type"], row["notification_category"], row["severity"],
                "second message part", 2, 2, MAX_ATTEMPTS, row["created_at"], row["updated_at"],
            ),
        )
        assert worker.store.requeue(delivery_id) == "budget_exhausted"
        current = worker.store.conn.execute(
            "SELECT status FROM deliveries WHERE delivery_id=?", (delivery_id,)
        ).fetchone()
        assert current["status"] == "dead_letter"
        assert worker.store.conn.execute(
            "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id=? AND outcome='operator_requeue'",
            (delivery_id,),
        ).fetchone()[0] == 0
    finally:
        worker.close()
        source.close()


def test_operator_requeue_preserves_history_and_grants_one_attempt(tmp_path):
    bot = RecordingBot(fail={OWNER: BotApiNotSentError("down")})
    source, worker = _worker(tmp_path, bot, recipients={"owner": OWNER})
    try:
        _insert(source, 1, "trade.state_changed", {"previous_state": "a", "new_state": "b"})
        worker.run_once()
        delivery_id = worker.store.conn.execute("SELECT delivery_id FROM deliveries").fetchone()[0]
        # Model an actually exhausted delivery with all ten send attempts in
        # immutable history; the operator action and next send must keep sequence.
        worker.store.conn.execute("DELETE FROM delivery_attempts WHERE delivery_id=?", (delivery_id,))
        for attempt_no in range(1, MAX_ATTEMPTS + 1):
            worker.store.conn.execute(
                "INSERT INTO delivery_attempts(delivery_id,attempt_no,attempted_at,outcome,error) "
                "VALUES(?,?,?,'dead_letter','previous attempt')",
                (delivery_id, attempt_no, f"2026-11-13T00:00:{attempt_no:02d}Z"),
            )
        worker.store.conn.execute(
            "UPDATE deliveries SET status='dead_letter',attempts=?,next_attempt_at=NULL WHERE delivery_id=?",
            (MAX_ATTEMPTS, delivery_id),
        )
        worker.store.conn.commit()
        assert worker.store.requeue(delivery_id) == "requeued"
        row = worker.store.conn.execute("SELECT status,attempts FROM deliveries").fetchone()
        assert (row["status"], row["attempts"]) == ("pending", MAX_ATTEMPTS - 1)
        claimed = worker.store.claim_next()
        assert claimed is not None
        worker.store.finish(claimed, "sent", message_id=987654)
        history = worker.store.conn.execute(
            "SELECT attempt_no,outcome FROM delivery_attempts WHERE delivery_id=? ORDER BY attempt_no",
            (delivery_id,),
        ).fetchall()
        assert [r["attempt_no"] for r in history] == list(range(1, MAX_ATTEMPTS + 3))
        assert history[-2]["outcome"] == "operator_requeue"
        assert history[-1]["outcome"] == "sent"
        final = worker.store.conn.execute("SELECT status,attempts FROM deliveries").fetchone()
        assert (final["status"], final["attempts"]) == ("sent", MAX_ATTEMPTS)
        assert worker.store.requeue("missing") == "not_found"
    finally:
        worker.close()
        source.close()


def test_notification_callbacks_are_single_use_role_bound_and_expiring(cfg, repo):
    from tg_signal_ingest.trading.store import TradingStore

    store = TradingStore(repo.db, cfg.trading)
    token = store.create_notification_callback(
        notification_id=1, role="owner", action="ack", ttl_s=60
    )
    assert store.consume_notification_callback(token, "developer") is None
    consumed = store.consume_notification_callback(token, "owner")
    assert consumed is not None and consumed["role"] == "owner"
    assert store.consume_notification_callback(token, "owner") is None

    expired = store.create_notification_callback(
        notification_id=2, role="owner", action="expand", ttl_s=-1
    )
    assert store.consume_notification_callback(expired, "owner") is None


# ------------------------------------------------ trading-path independence


def _trading_modules():
    root = Path(__file__).resolve().parents[1] / "tg_signal_ingest"
    paths = sorted((root / "trading").glob("*.py"))
    paths.append(root / "service.py")
    return paths


def _calls_in(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else (
            func.id if isinstance(func, ast.Name) else ""
        )
        if name:
            names.add(name)
    return names


def test_trading_path_has_no_telegram_network_or_flush_calls():
    """Static call-graph guard over the trading critical path.

    ``trading/notify.py`` retains the dormant legacy flush/delivery helpers for
    compatibility, but the producer entry points ``notify``/``enqueue`` must not
    reach them.  Every other trading module (and the serve orchestration) must
    contain no Telegram/network/flush call at all.
    """

    forbidden_calls = {"send_message", "flush", "urlopen", "getresponse", "request"}
    offenders: list[str] = []
    for path in _trading_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        if path.name == "notify.py":
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in {
                    "notify",
                    "enqueue",
                }:
                    reached = _calls_in(node) & forbidden_calls
                    if reached:
                        offenders.append(f"{path.name}:{node.name}:{sorted(reached)}")
            continue
        for name in _calls_in(tree) & forbidden_calls:
            offenders.append(f"{path.name}:{name}")
    assert offenders == []


def test_systemd_worker_dependency_is_one_directional():
    """Static unit check: trading never depends on notification delivery.

    Runtime dependency behavior is separately verified by the isolated systemd
    harness documented in docs/notification-readiness.md.
    """

    deploy = Path(__file__).resolve().parents[1] / "deploy"
    ingest = (deploy / "tg-ingest.service").read_text(encoding="utf-8")
    worker = (deploy / "tg-notification-worker.service").read_text(encoding="utf-8")
    assert "Wants=tg-notification-worker.service" in ingest
    assert "Requires=tg-notification-worker.service" not in ingest
    assert "BindsTo=tg-ingest.service" in worker
    assert "PartOf=tg-ingest.service" in worker
    assert "After=network-online.target tg-ingest.service" in worker
    assert "tg-ingest notifications worker" in worker


def test_notifier_enqueue_never_calls_the_telegram_bot(cfg, repo):
    from tg_signal_ingest.trading.notify import TradingNotifier
    from tg_signal_ingest.trading.store import TradingStore

    class ExplodingBot:
        def send_message(self, *args, **kwargs):
            raise AssertionError("trading path must never call Telegram")

    store = TradingStore(repo.db, cfg.trading)
    notifier = TradingNotifier(store, ExplodingBot(), owner_id=OWNER, developer_id=DEVELOPER)
    # Enqueue is local-only; if this ever called send_message the exploding bot
    # would fail the test.
    notifier.notify(event_key="test:local-only", text="local only", category="order_lifecycle")
    rows = store.db.conn.execute(
        "SELECT COUNT(*) AS n FROM trading_notifications"
    ).fetchone()["n"]
    assert rows == 2
