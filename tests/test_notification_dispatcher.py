"""Failure-oriented tests for the independent notification dispatcher."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from tg_signal_ingest.control.bot_api import (
    BotApiClient,
    BotApiRateLimitError,
    BotApiTransportError,
)
from tg_signal_ingest.main import notification_source_schema_ready
from tg_signal_ingest.notification_dispatcher import (
    NotificationDispatcher,
    heartbeat_status,
    inspect_delivery_db,
    render_domain_event,
)


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


def _event(
    conn: sqlite3.Connection,
    audit_id: int,
    event_id: str = "evt-1",
    *,
    event_type: str = "intent.created",
    severity: str = "INFO",
) -> None:
    conn.execute(
        """
        INSERT INTO audit_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            audit_id,
            "domain",
            event_id,
            event_type,
            1,
            "2026-11-12T12:00:00Z",
            "intent",
            "intent-1",
            "signal-1",
            "BTCUSDT",
            severity,
            json.dumps(
                {
                    "state": "planned",
                    "entry_price": "100",
                    "tp1_price": "110",
                    "tp2_price": "120",
                    "stop_loss_price": "95",
                }
            ),
            '["owner","owner_b","developer"]',
        ),
    )
    conn.commit()


def _domain_event(
    conn: sqlite3.Connection,
    audit_id: int,
    event_type: str,
    payload: dict,
    *,
    severity: str = "INFO",
    event_id: str = "evt-custom",
) -> None:
    conn.execute(
        """
        INSERT INTO audit_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            audit_id,
            "domain",
            event_id,
            event_type,
            1,
            "2026-11-12T12:00:00Z",
            "intent",
            "intent-1",
            "signal-1",
            "BTCUSDT",
            severity,
            json.dumps(payload),
            '["owner","owner_b","developer"]',
        ),
    )
    conn.commit()


class FakeBot:
    def __init__(self, result=None, error=None):
        self.result = result or {"message_id": 7001}
        self.error = error
        self.calls: list[tuple[int, str]] = []

    def send_message(self, chat_id: int, text: str):
        self.calls.append((chat_id, text))
        if self.error:
            raise self.error
        return self.result


def _dispatcher(tmp_path: Path, bot: FakeBot):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    worker = NotificationDispatcher(
        source_path,
        tmp_path / "delivery.sqlite3",
        bot,
        {"owner": 101, "owner_b": 202},
    )
    return source, worker


def test_first_startup_establishes_no_replay_boundary(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    _event(source, 1, "old-event")
    bot = FakeBot()
    worker = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    try:
        # Old events are not replayed when the dispatcher is first installed.
        assert worker.run_once()["relayed"] == 0
        _event(source, 2, "new-event")
        assert worker.run_once()["relayed"] == 1
        assert len(bot.calls) == 1
        assert "#BTCUSDT" in bot.calls[0][1]
        assert "هدف اول (TP1): 110" in bot.calls[0][1]
        assert "هدف دوم (TP2): 120" in bot.calls[0][1]
        assert "حد ضرر: 95" in bot.calls[0][1]
    finally:
        worker.close()
        source.close()


def test_one_event_has_independent_delivery_for_each_recipient(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        _event(source, 1)
        result = worker.run_once()
        assert result == {"relayed": 2, "recovered_ambiguous": 0, "attempted": 2}
        rows = worker.store.conn.execute(
            "SELECT recipient_role,status,telegram_message_id FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert [(r["recipient_role"], r["status"], r["telegram_message_id"]) for r in rows] == [
            ("owner", "sent", 7001),
            ("owner_b", "sent", 7001),
        ]
        assert worker.store.counts() == {"sent": 2}
    finally:
        worker.close()
        source.close()


def test_ambiguous_transport_outcome_is_never_retried(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot(error=BotApiTransportError("network timeout")))
    try:
        _event(source, 1)
        assert worker.run_once()["attempted"] == 2
        assert worker.store.counts() == {"ambiguous": 2}
        assert worker.run_once()["attempted"] == 0
    finally:
        worker.close()
        source.close()


def test_recipient_delivery_states_are_independent(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)

    class PerRecipientBot:
        def send_message(self, chat_id: int, text: str):
            if chat_id == 101:
                raise BotApiTransportError("owner route timeout")
            return {"message_id": 8002}

    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3", PerRecipientBot(),
        {"owner": 101, "owner_b": 202},
    )
    try:
        _event(source, 1)
        assert worker.run_once()["attempted"] == 2
        rows = worker.store.conn.execute(
            "SELECT recipient_role,status,telegram_message_id FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert [(r["recipient_role"], r["status"], r["telegram_message_id"]) for r in rows] == [
            ("owner", "ambiguous", None),
            ("owner_b", "sent", 8002),
        ]
    finally:
        worker.close()
        source.close()


def test_rate_limit_is_scheduled_and_not_retried_in_same_cycle(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot(error=BotApiRateLimitError("429", retry_after=60)))
    try:
        _event(source, 1)
        assert worker.run_once()["attempted"] == 2
        rows = worker.store.conn.execute(
            "SELECT status,next_attempt_at,attempts FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert all(row["status"] == "retry" and row["next_attempt_at"] and row["attempts"] == 1 for row in rows)
        assert worker.run_once()["attempted"] == 0
    finally:
        worker.close()
        source.close()


def test_retry_attempts_stop_at_dead_letter_limit(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3",
        FakeBot(error=BotApiRateLimitError("429", retry_after=1)),
        {"owner": 101},
    )
    try:
        _event(source, 1)
        worker.relay_once()
        for _ in range(10):
            worker.store.conn.execute(
                "UPDATE deliveries SET next_attempt_at='2000-01-01T00:00:00Z' WHERE status='retry'"
            )
            worker.run_once(delivery_limit=1)
        row = worker.store.conn.execute("SELECT status,attempts FROM deliveries").fetchone()
        assert (row["status"], row["attempts"]) == ("dead_letter", 10)
        assert worker.store.conn.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 10
    finally:
        worker.close()
        source.close()


def test_two_worker_claims_cannot_claim_the_same_delivery(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        _event(source, 1)
        worker.relay_once()
        first = worker.store.claim_next()
        second = worker.store.claim_next()
        assert first is not None
        assert second is not None
        assert first["delivery_id"] != second["delivery_id"]
    finally:
        worker.close()
        source.close()




def test_two_dispatcher_instances_cannot_claim_the_same_delivery(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    bot = FakeBot()
    worker_a = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    worker_b = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    try:
        _event(source, 1)
        worker_a.relay_once()
        first = worker_a.store.claim_next()
        second = worker_b.store.claim_next()
        assert first is not None
        assert second is None
    finally:
        worker_a.close()
        worker_b.close()
        source.close()

def test_stale_sending_claim_becomes_ambiguous_not_pending(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        _event(source, 1)
        worker.relay_once()
        claimed = worker.store.claim_next()
        assert claimed is not None
        worker.store.conn.execute(
            "UPDATE deliveries SET claimed_at='2000-01-01T00:00:00Z' WHERE delivery_id=?",
            (claimed["delivery_id"],),
        )
        assert worker.store.recover_stale_claims(older_than_s=1) == 1
        row = worker.store.conn.execute(
            "SELECT status,attempts FROM deliveries WHERE delivery_id=?",
            (claimed["delivery_id"],),
        ).fetchone()
        assert (row["status"], row["attempts"]) == ("ambiguous", 1)
        assert worker.store.conn.execute(
            "SELECT outcome FROM delivery_attempts WHERE delivery_id=?",
            (claimed["delivery_id"],),
        ).fetchone()["outcome"] == "ambiguous"
    finally:
        worker.close()
        source.close()


def test_source_database_is_opened_read_only(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        with pytest.raises(sqlite3.OperationalError):
            worker._source.execute("INSERT INTO audit_events(audit_id) VALUES(99)")
    finally:
        worker.close()
        source.close()


def test_worker_restart_recovers_stale_claim_without_duplicate_send(tmp_path):
    """A crash after claim leaves an ambiguous delivery; a restarted worker
    recovers it without ever re-sending the message blindly."""
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    worker_a = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", FakeBot(), {"owner": 101})
    try:
        _event(source, 1)
        worker_a.relay_once()
        claimed = worker_a.store.claim_next()
        assert claimed is not None
        # Simulate a crash before send completed: the claim is stale.
        worker_a.store.conn.execute(
            "UPDATE deliveries SET claimed_at='2000-01-01T00:00:00Z' WHERE delivery_id=?",
            (claimed["delivery_id"],),
        )
    finally:
        worker_a.close()

    worker_b = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", FakeBot(), {"owner": 101})
    try:
        result = worker_b.run_once()
        assert result["recovered_ambiguous"] == 1
        assert result["attempted"] == 0  # no blind duplicate send
        row = worker_b.store.conn.execute(
            "SELECT status FROM deliveries WHERE delivery_id=?", (claimed["delivery_id"],)
        ).fetchone()
        assert row["status"] == "ambiguous"
    finally:
        worker_b.close()
        source.close()


def test_worker_absence_is_operationally_detectable(tmp_path):
    """A worker that has never run has no delivery ledger; a running worker
    publishes a heartbeat that ``notifications status`` surfaces."""
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    try:
        # No worker has ever initialized the independent ledger.
        assert inspect_delivery_db(tmp_path / "delivery.sqlite3") is None

        worker = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", FakeBot(), {"owner": 101})
        try:
            _event(source, 1)
            worker.run_once()
            result = inspect_delivery_db(tmp_path / "delivery.sqlite3")
            assert result is not None
            assert "last_cycle_at" in result["state"]
        finally:
            worker.close()
    finally:
        source.close()


def test_bot_api_classifies_structured_retry_after_and_server_errors():
    rate_limited = BotApiClient(
        "123:token",
        transport=lambda method, payload: {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests",
            "parameters": {"retry_after": 17},
        },
    )
    with pytest.raises(BotApiRateLimitError) as rate_error:
        rate_limited.send_message(1, "test")
    assert rate_error.value.retry_after == 17

    server_error = BotApiClient(
        "123:token",
        transport=lambda method, payload: {"ok": False, "error_code": 500, "description": "Internal Server Error"},
    )
    # A 5xx response proves the request reached the server but not whether the
    # write was applied; it is ambiguous, never automatically retried.
    with pytest.raises(BotApiTransportError):
        server_error.send_message(1, "test")


def test_bot_api_transport_exception_is_ambiguous():
    client = BotApiClient("123:token", transport=lambda method, payload: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(BotApiTransportError):
        client.send_message(1, "test")



def test_existing_category_preferences_are_applied_per_recipient(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    source.execute(
        "INSERT INTO notification_preferences(role,category,enabled,updated_at) "
        "VALUES('owner','order_lifecycle',0,'now')"
    )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3", bot,
        {"owner": 101, "owner_b": 202, "developer": 303},
    )
    try:
        _event(source, 1, event_type="trade.state_changed", severity="INFO")
        result = worker.run_once()
        assert result["relayed"] == 3
        rows = worker.store.conn.execute(
            "SELECT recipient_role,status,notification_category FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert [(r["recipient_role"], r["status"], r["notification_category"]) for r in rows] == [
            ("developer", "sent", "order_lifecycle"),
            ("owner", "suppressed", "order_lifecycle"),
            ("owner_b", "suppressed", "order_lifecycle"),
        ]
        assert [chat_id for chat_id, _ in bot.calls] == [303]
    finally:
        worker.close()
        source.close()


def test_notification_source_schema_readiness_waits_for_migration_columns(tmp_path):
    import sqlite3

    db_path = tmp_path / "trading.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE audit_events (
            audit_id INTEGER PRIMARY KEY, event_id TEXT, event_type TEXT,
            schema_version INTEGER, occurred_at_utc TEXT, aggregate_id TEXT,
            correlation_id TEXT, symbol TEXT, severity TEXT, payload_json TEXT,
            audience_json TEXT, event_class TEXT
        );
        CREATE TABLE control_settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE notification_preferences (
            role TEXT, category TEXT, enabled INTEGER, updated_at TEXT
        );
        CREATE TABLE trading_notifications (notification_id INTEGER PRIMARY KEY, recipient_role TEXT);
        """
    )
    conn.commit()
    assert not notification_source_schema_ready(db_path)
    conn.execute("ALTER TABLE audit_events ADD COLUMN aggregate_type TEXT")
    conn.commit()
    conn.close()
    assert notification_source_schema_ready(db_path)


def test_owner_b_category_override_takes_precedence_over_owner_fallback(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    source.execute(
        "INSERT INTO notification_preferences "
        "(role,category,enabled,updated_at) VALUES('owner','order_lifecycle',0,'now')"
    )
    source.execute(
        "INSERT INTO notification_preferences "
        "(role,category,enabled,updated_at) VALUES('owner_b','order_lifecycle',1,'now')"
    )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3", bot,
        {"owner": 101, "owner_b": 202, "developer": 303},
    )
    try:
        _event(source, 1, event_type="trade.state_changed", severity="INFO")
        worker.run_once()
        rows = worker.store.conn.execute(
            "SELECT recipient_role,status FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert [(r["recipient_role"], r["status"]) for r in rows] == [
            ("developer", "sent"), ("owner", "suppressed"), ("owner_b", "sent")
        ]
        assert [chat_id for chat_id, _ in bot.calls] == [202, 303]
    finally:
        worker.close()
        source.close()


def test_genuine_rejection_is_owner_visible_despite_category_preference(tmp_path):
    """A genuine rejected/ambiguous signal is WARNING and owner-visible.

    Routine category preferences may silence order chatter, but they must never
    hide an owner-actionable rejection.
    """

    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    for role in ("owner", "owner_b", "developer"):
        source.execute(
            "INSERT INTO notification_preferences(role,category,enabled,updated_at) "
            "VALUES(?, 'signal_decision', 0, 'now')",
            (role,),
        )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3", bot,
        {"owner": 101, "owner_b": 202, "developer": 303},
    )
    try:
        _event(source, 1, event_type="signal.rejected", severity="WARNING")
        worker.run_once()
        rows = worker.store.conn.execute(
            "SELECT recipient_role,status FROM deliveries ORDER BY recipient_role"
        ).fetchall()
        assert [(r["recipient_role"], r["status"]) for r in rows] == [
            ("developer", "suppressed"),
            ("owner", "sent"),
            ("owner_b", "sent"),
        ]
        assert {chat for chat, _ in bot.calls} == {101, 202}
    finally:
        worker.close()
        source.close()


def test_global_minimum_severity_suppresses_below_threshold(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    source.execute(
        "INSERT INTO control_settings(key,value,updated_at) VALUES('notification_min_severity','ERROR','now')"
    )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    try:
        _event(source, 1, severity="WARNING")
        result = worker.run_once()
        assert result["relayed"] == 1 and result["attempted"] == 0
        row = worker.store.conn.execute("SELECT status,last_error FROM deliveries").fetchone()
        assert (row["status"], row["last_error"]) == ("suppressed", "below minimum severity ERROR")
        assert bot.calls == []
    finally:
        worker.close()
        source.close()


def test_owner_unknown_state_alert_bypasses_minimum_severity(tmp_path):
    """F3: owner critical/protection/unknown-state alerts survive a CRITICAL
    minimum-severity preference; routine alerts are still filtered."""
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    source.execute(
        "INSERT INTO control_settings(key,value,updated_at) VALUES('notification_min_severity','CRITICAL','now')"
    )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(
        source_path, tmp_path / "delivery.sqlite3", bot,
        {"owner": 101, "owner_b": 202, "developer": 303},
    )
    try:
        # A trade entering needs_reconciliation (WARNING) is an unknown-state
        # safety alert: every recipient, including both owners, must receive it.
        _domain_event(
            source, 1, "trade.state_changed",
            {"previous_state": "oco_partial", "new_state": "needs_reconciliation"},
            severity="WARNING", event_id="evt-safety",
        )
        result = worker.run_once()
        assert result["relayed"] == 3 and result["attempted"] == 2
        statuses = [
            (row["recipient_role"], row["status"])
            for row in worker.store.conn.execute(
                "SELECT recipient_role,status FROM deliveries WHERE event_id='evt-safety' ORDER BY recipient_role"
            ).fetchall()
        ]
        # Both owners bypass the threshold; the developer still honours it.
        assert statuses == [("developer", "suppressed"), ("owner", "sent"), ("owner_b", "sent")]

        # A routine INFO signal event is still suppressed at CRITICAL threshold.
        _domain_event(
            source, 2, "signal.received", {"state": "parsed", "signal_kind": "alert"},
            event_id="evt-routine",
        )
        worker.run_once()
        routine = worker.store.conn.execute(
            "SELECT recipient_role,status FROM deliveries "
            "WHERE event_id='evt-routine' ORDER BY recipient_role"
        ).fetchall()
        assert [row["status"] for row in routine] == ["suppressed", "suppressed", "suppressed"]
    finally:
        worker.close()
        source.close()


def _legacy_notification(conn: sqlite3.Connection, event_key: str = "TRADE_ERROR") -> None:
    conn.execute(
        "INSERT INTO trading_notifications(event_key,category,event_type,role,chat_id,text,status) "
        "VALUES(?,?,?,?,?,?, 'pending')",
        (event_key, "order_lifecycle", "trade.error", "owner", 101, "legacy formatted message"),
    )
    conn.commit()


def test_legacy_severity_keeps_local_rejection_warning_and_buy_block_critical():
    classify = NotificationDispatcher._legacy_severity
    assert classify("signal_decision", "intent:REJECTED_LOCALLY", "signal.rejected_locally") == "WARNING"
    assert classify("order_lifecycle", "BUY_BLOCK_ACTIVE_deadbeef", "trade.alert") == "CRITICAL"
    assert classify("order_lifecycle", "BUY_BLOCK_CLEARED", "trade.alert") == "INFO"
    assert classify("order_lifecycle", "exchange rejected", "order.rejected") == "ERROR"
    assert classify("order_lifecycle", "exchange rejected", "signal.rejected_by_exchange") == "ERROR"


def test_legacy_outbox_bridge_skips_preexisting_rows_and_delivers_new_rows(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    _legacy_notification(source, "old-before-install")
    bot = FakeBot()
    worker = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    try:
        assert worker.run_once()["attempted"] == 0
        assert bot.calls == []
        _legacy_notification(source, "new-after-install")
        result = worker.run_once()
        assert result["attempted"] == 1
        assert bot.calls == [(101, "legacy formatted message")]
        row = worker.store.conn.execute(
            "SELECT event_id,status,notification_category FROM deliveries"
        ).fetchone()
        assert (row["event_id"], row["status"], row["notification_category"]) == (
            "legacy-notification-2", "sent", "order_lifecycle"
        )
    finally:
        worker.close()
        source.close()


def test_legacy_outbox_bridge_respects_recipient_category_preferences(tmp_path):
    source_path = tmp_path / "trading.sqlite3"
    source = _source(source_path)
    source.execute(
        "INSERT INTO notification_preferences(role,category,enabled,updated_at) "
        "VALUES('owner','order_lifecycle',0,'now')"
    )
    source.commit()
    bot = FakeBot()
    worker = NotificationDispatcher(source_path, tmp_path / "delivery.sqlite3", bot, {"owner": 101})
    try:
        _legacy_notification(source)
        result = worker.run_once()
        assert result["relayed"] == 1 and result["attempted"] == 0
        row = worker.store.conn.execute("SELECT status,last_error FROM deliveries").fetchone()
        assert (row["status"], row["last_error"]) == ("suppressed", "category disabled")
        assert bot.calls == []
    finally:
        worker.close()
        source.close()



def test_status_inspection_is_read_only_and_exposes_heartbeat(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        _event(source, 1)
        worker.run_once()
        worker.store.set_state("worker_started_at", "2026-11-12T12:00:00Z")
        before = worker.store.conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        result = inspect_delivery_db(tmp_path / "delivery.sqlite3", limit=5)
        assert result is not None
        assert result["counts"] == {"sent": 2}
        assert result["state"]["worker_started_at"] == "2026-11-12T12:00:00Z"
        assert len(result["deliveries"]) == 2
        assert worker.store.conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0] == before
        filtered = inspect_delivery_db(tmp_path / "delivery.sqlite3", status="ambiguous")
        assert filtered is not None and filtered["deliveries"] == []
    finally:
        worker.close()
        source.close()


def test_heartbeat_status_detects_missing_fresh_and_stale():
    from datetime import UTC, datetime

    now = datetime(2026, 11, 12, 12, 0, 0, tzinfo=UTC)
    missing = heartbeat_status({}, now=now)
    assert missing["heartbeat_stale"] is None and missing["heartbeat_age_s"] is None

    fresh = heartbeat_status({"last_cycle_at": "2026-11-12T11:59:50Z"}, now=now, stale_after_s=60)
    assert fresh["heartbeat_age_s"] == 10.0
    assert fresh["heartbeat_stale"] is False

    stale = heartbeat_status({"last_cycle_at": "2026-11-12T11:00:00Z"}, now=now, stale_after_s=60)
    assert stale["heartbeat_age_s"] == 3600.0
    assert stale["heartbeat_stale"] is True

    # A malformed heartbeat is reported but never crashes the operator view.
    malformed = heartbeat_status({"last_cycle_at": "not-a-timestamp"}, now=now)
    assert malformed["heartbeat_age_s"] is None and malformed["heartbeat_stale"] is None


def test_inspect_delivery_db_reports_stale_heartbeat(tmp_path):
    source, worker = _dispatcher(tmp_path, FakeBot())
    try:
        _event(source, 1)
        worker.run_once()
        result = inspect_delivery_db(tmp_path / "delivery.sqlite3")
        assert result is not None
        assert result["heartbeat"]["heartbeat_stale"] is False

        worker.store.set_state("last_cycle_at", "2000-01-01T00:00:00Z")
        stale = inspect_delivery_db(tmp_path / "delivery.sqlite3")
        assert stale is not None
        assert stale["heartbeat"]["heartbeat_stale"] is True
    finally:
        worker.close()
        source.close()


def test_notification_status_and_list_commands_are_available():
    from tg_signal_ingest.main import build_parser

    status = build_parser().parse_args(["notifications", "status", "--json"])
    listing = build_parser().parse_args(["notifications", "list", "--status", "ambiguous"])
    assert status.notifications_cmd == "status" and status.json is True
    assert listing.notifications_cmd == "list" and listing.status == "ambiguous"



def test_retry_scheduled_domain_event_has_explicit_persian_message():
    message = render_domain_event(
        {
            "event_type": "trade_order.state_changed",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "kind": "tp1",
                    "previous_state": "submitting",
                    "new_state": "rejected",
                    "attempt": 2,
                    "retryable": True,
                    "next_attempt_at": "2026-11-12T12:00:05+00:00",
                    "reason": "temporary exchange rejection",
                    "price": "110",
                    "quantity": "0.05",
                    "stop_trigger_price": "98",
                    "client_order_id": "client-tp1-abc",
                    "exchange_order_id": "98765",
                }
            ),
        }
    )
    assert "تلاش سفارش حفاظتی رد شد" in message
    assert "تلاش: 3" in message
    assert "تلاش بعدی: 4" in message
    assert "دلیل: temporary exchange rejection" in message
    assert "قیمت: 110" in message
    assert "مقدار: 0.05" in message
    assert "قیمت فعال‌سازی SL: 98" in message
    assert "شناسه سفارش صرافی: 98765" in message
    assert "2026-11-12T12:00:05+00:00" in message


def test_trade_state_renderer_uses_human_readable_persian_state_label():
    message = render_domain_event(
        {
            "event_type": "trade.state_changed",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "previous_state": "oco_partial",
                    "new_state": "closed",
                    "reason": "entry intent rejected with no fills; no position opened",
                }
            ),
        }
    )
    assert "ورود پر نشد؛ معامله بدون ایجاد موقعیت بسته شد" in message
    assert "توضیح: entry intent rejected with no fills" in message
    assert "از: oco_partial" in message
    assert "به: closed" in message



def test_trade_order_event_renders_exchange_order_details_and_rejection_reason():
    message = render_domain_event(
        {
            "event_type": "trade_order.state_changed",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "kind": "tp2",
                    "previous_state": "submitting",
                    "new_state": "open",
                    "price": "120",
                    "quantity": "0.04",
                    "stop_trigger_price": "97",
                    "exchange_order_id": "123456",
                    "exchange_status": "NEW",
                    "client_order_id": "client-1",
                    "reason": "OCO submitted",
                }
            ),
        }
    )
    assert "سفارش حفاظتی در صرافی پذیرفته شد" in message
    assert "نوع سفارش: tp2" in message
    assert "قیمت: 120" in message
    assert "مقدار: 0.04" in message
    assert "قیمت فعال‌سازی SL: 97" in message
    assert "شناسه سفارش صرافی: 123456" in message



def test_intent_state_event_renders_exchange_acknowledgement_details():
    message = render_domain_event(
        {
            "event_type": "intent.state_changed",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "previous_state": "submitting",
                    "new_state": "submitted",
                    "exchange_order_id": "987654",
                    "exchange_status": "NEW",
                    "executed_qty": "0.025",
                    "cumulative_quote": "2.5",
                    "reason": "exchange accepted order 987654",
                }
            ),
        }
    )
    assert "سفارش ورود در صرافی پذیرفته شد" in message
    assert "شناسه سفارش صرافی: 987654" in message
    assert "وضعیت صرافی: NEW" in message
    assert "مقدار پرشده: 0.025" in message
    assert "ارزش تجمعی اجرا: 2.5" in message
    assert "توضیح: exchange accepted order 987654" in message


def test_submission_accepted_renders_acceptance_only_without_fill_amounts():
    message = render_domain_event(
        {
            "event_type": "intent.submission_accepted",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "exchange_order_id": "987654",
                    "client_order_id": "client-accepted",
                    "exchange_status": "NEW",
                }
            ),
        }
    )
    assert "پذیرش ارسال سفارش ورود در صرافی تأیید شد" in message
    assert "شناسه سفارش صرافی: 987654" in message
    assert "شناسه سفارش مشتری: client-accepted" in message
    assert "وضعیت صرافی: NEW" in message
    # Acceptance carries no fill amounts; fills are separate later events.
    assert "مقدار پرشده" not in message
    assert "ارزش تجمعی" not in message


def test_critical_protection_event_renders_as_urgent_persian_message():
    message = render_domain_event(
        {
            "event_type": "trade.protection_critical",
            "symbol": "BTCUSDT",
            "payload_json": json.dumps(
                {
                    "event_key": "SL_UNPROTECTED_CRITICAL",
                    "detail": "قیمت از حد ضرر عبور کرد؛ هیچ OCO فعالی تأیید نشده است",
                }
            ),
        }
    )
    assert message.startswith("🚨 هشدار بحرانی حفاظت معامله")
    assert "#BTCUSDT" in message
    assert "هیچ OCO فعالی تأیید نشده است" in message
