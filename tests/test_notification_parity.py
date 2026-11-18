"""Tests for the read-only notification parity report (F-08 Phase 1).

Fixtures reuse the dispatcher tests' source-DB helpers and drive the REAL
``DeliveryStore.relay_batch`` so the delivery ledger is byte-for-byte what
the worker would persist; the parity report is then required to reproduce
the same expectation logic with SELECT-only access to both databases.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from types import SimpleNamespace

import tg_signal_ingest.main as main_module
from tests.test_notification_dispatcher import _event, _source
from tg_signal_ingest.main import build_parser, cmd_notifications_parity
from tg_signal_ingest.notification_dispatcher import DeliveryStore
from tg_signal_ingest.notification_parity import build_parity_report

RECIPIENTS = {"owner": 111, "owner_b": 222, "developer": 333}


def _relayed_source(path) -> sqlite3.Connection:
    conn = _source(path)
    _event(conn, 1, "evt-1", event_type="intent.created", severity="INFO")
    _event(conn, 2, "evt-2", event_type="trade.state_changed", severity="CRITICAL")
    _event(conn, 3, "evt-3", event_type="signal.rejected", severity="INFO")
    conn.commit()
    return conn


def _events_as_dicts(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(row)
        for row in conn.execute(
            "SELECT audit_id,event_id,event_type,schema_version,occurred_at_utc,aggregate_type,"
            "aggregate_id,correlation_id,symbol,severity,payload_json,audience_json FROM audit_events"
        ).fetchall()
    ]


def _relay(source_path, delivery_path) -> None:
    conn = sqlite3.connect(source_path)
    conn.row_factory = sqlite3.Row
    events = _events_as_dicts(conn)
    conn.close()
    store = DeliveryStore(delivery_path)
    try:
        store.relay_batch(events, RECIPIENTS)
    finally:
        store.close()


def _report(tmp_path, **kwargs) -> dict:
    return build_parity_report(
        tmp_path / "app.db",
        tmp_path / "notification-delivery.sqlite3",
        RECIPIENTS,
        **kwargs,
    )


def test_parity_ok_after_real_relay(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")

    report = _report(tmp_path)

    assert report["parity_ok"] is True
    assert report["gaps_total"] == 0 and report["gaps"] == []
    assert report["orphans_total"] == 0 and report["orphans"] == []
    assert report["unrelayed_within_cursor"] == 0
    # 3 events x 3 recipient roles (audience covers all three; CRITICAL
    # reaches every configured recipient) — suppression also persists rows.
    assert report["expected_deliveries"] == 9
    assert report["delivery_rows"] == 9
    assert report["relay_caught_up"] is True and report["cursor_lag"] == 0
    assert set(report["status_counts_by_role"]) == {"owner", "owner_b", "developer"}


def test_missing_delivery_row_is_reported_as_gap(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    conn = sqlite3.connect(tmp_path / "notification-delivery.sqlite3")
    conn.execute("DELETE FROM deliveries WHERE event_id='evt-2' AND recipient_role='owner_b'")
    conn.commit()
    conn.close()

    report = _report(tmp_path)

    assert report["parity_ok"] is False
    assert report["gaps_total"] == 1
    assert report["delivery_rows"] == 8
    gap = report["gaps"][0]
    assert (gap["event_id"], gap["recipient_role"]) == ("evt-2", "owner_b")
    assert gap["severity"] == "CRITICAL"


def test_cursor_lag_is_inflight_not_a_gap(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    conn = sqlite3.connect(tmp_path / "app.db")
    _event(conn, 4, "evt-4", event_type="intent.created", severity="INFO")
    conn.commit()
    conn.close()

    report = _report(tmp_path)

    assert report["cursor_lag"] == 1 and report["relay_caught_up"] is False
    assert report["domain_events_considered"] == 3  # only events up to the cursor
    assert report["parity_ok"] is True  # in-flight relay lag is not a parity gap


def test_unrelayed_event_within_cursor_is_an_anomaly(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    conn = sqlite3.connect(tmp_path / "app.db")
    _event(conn, 4, "evt-4", event_type="intent.created", severity="INFO")
    conn.commit()
    conn.close()
    # Simulate a relay that advanced its cursor without recording the event.
    store = sqlite3.connect(tmp_path / "notification-delivery.sqlite3")
    store.execute("UPDATE dispatcher_state SET value='4' WHERE key='source_cursor'")
    store.commit()
    store.close()

    report = _report(tmp_path)

    assert report["unrelayed_within_cursor"] == 1
    assert report["gaps_total"] == 0
    assert report["parity_ok"] is False


def test_orphan_delivery_row_is_reported(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    conn = sqlite3.connect(tmp_path / "notification-delivery.sqlite3")
    conn.execute(
        "INSERT INTO deliveries(delivery_id,event_id,recipient_role,chat_id,event_type,"
        "notification_category,severity,message_text,status,created_at,updated_at) "
        "VALUES('d-orphan','evt-orphan','owner',111,'x','diagnostic','INFO','t','pending','2026','2026')"
    )
    conn.commit()
    conn.close()

    report = _report(tmp_path)

    assert report["orphans_total"] == 1
    assert report["orphans"][0]["event_id"] == "evt-orphan"
    assert report["parity_ok"] is False


def test_report_never_mutates_either_database(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    source_path = tmp_path / "app.db"
    delivery_path = tmp_path / "notification-delivery.sqlite3"
    before = {
        "source": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "delivery": hashlib.sha256(delivery_path.read_bytes()).hexdigest(),
    }

    _report(tmp_path)

    after = {
        "source": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "delivery": hashlib.sha256(delivery_path.read_bytes()).hexdigest(),
    }
    assert before == after


def test_missing_delivery_db_is_reported_cleanly(tmp_path):
    _relayed_source(tmp_path / "app.db")

    report = build_parity_report(
        tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3", RECIPIENTS
    )

    assert report["delivery_db_present"] is False
    assert report["parity_ok"] is True
    assert report["delivery_rows"] == 0 and report["expected_deliveries"] == 0


def test_parser_wiring_and_cli_json(tmp_path, capsys, monkeypatch):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")
    cfg = SimpleNamespace(
        db_path=tmp_path / "app.db",
        data_dir=tmp_path,
        control=SimpleNamespace(owner_user_id=111, owner_b_user_id=222, developer_user_id=333),
    )
    monkeypatch.setattr(main_module, "_load_or_exit", lambda **kwargs: cfg)

    argv = build_parser().parse_args(["notifications", "parity", "--json", "--limit", "5"])
    assert argv.func is cmd_notifications_parity and argv.since_audit_id == 0 and argv.limit == 5

    rc = cmd_notifications_parity(argv)
    out = capsys.readouterr().out
    report = json.loads(out)
    assert rc == 0
    assert report["parity_ok"] is True and report["expected_deliveries"] == 9

    # A parity violation must be visible in both text mode and the exit code.
    conn = sqlite3.connect(tmp_path / "notification-delivery.sqlite3")
    conn.execute("DELETE FROM deliveries WHERE event_id='evt-1' AND recipient_role='developer'")
    conn.commit()
    conn.close()
    argv_text = build_parser().parse_args(["notifications", "parity"])
    rc_text = cmd_notifications_parity(argv_text)
    text = capsys.readouterr().out
    assert rc_text == 1
    assert "notification parity report" in text
    assert "gaps_total=1" in text


def test_since_audit_id_limits_window(tmp_path):
    _relayed_source(tmp_path / "app.db")
    _relay(tmp_path / "app.db", tmp_path / "notification-delivery.sqlite3")

    report = _report(tmp_path, since_audit_id=2)

    # Only evt-3 lies in (2, cursor]; expectations shrink accordingly.
    assert report["domain_events_considered"] == 1
    assert report["expected_deliveries"] == 3
    assert report["parity_ok"] is True
