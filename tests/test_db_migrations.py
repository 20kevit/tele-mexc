"""Database migrations and schema basics."""

from __future__ import annotations

import sqlite3

import pytest

import tg_signal_ingest.db as db_module
from tg_signal_ingest.db import MIGRATIONS, Database


def test_migrations_apply_and_are_idempotent(tmp_path):
    path = tmp_path / "db.sqlite3"
    db = Database(path)
    applied = db.migrate()
    assert applied == [v for v, _ in MIGRATIONS]
    assert db.migrate() == []  # rerun is a no-op
    assert db.schema_version() >= 1
    db.close()


def test_core_tables_exist(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.migrate()
    names = {
        r["name"]
        for r in db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    expected = {"channels", "messages", "message_edits", "processing_queue", "sync_state", "app_state"}
    assert expected <= names
    db.close()


def test_wal_mode_active(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.migrate()
    mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal"
    db.close()


def test_transations_rollback_on_error(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.migrate()
    with pytest.raises(sqlite3.IntegrityError), db.transaction() as conn:
        conn.execute(
            "INSERT INTO channels (channel_id, access_hash, title, created_at, updated_at) "
            "VALUES (1, 2, 't', 'now', 'now')"
        )
        conn.execute(
            "INSERT INTO channels (channel_id, access_hash, title, created_at, updated_at) "
            "VALUES (1, 2, 't2', 'now', 'now')"  # PK violation -> rollback
        )
    count = db.conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    assert count == 0
    db.close()


def test_v5_control_tables_and_setup_singleton(tmp_path):
    db = Database(tmp_path / "db.sqlite3")
    db.migrate()
    names = {
        r["name"]
        for r in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    expected = {
        "control_setup", "control_settings", "control_flow", "control_learning_samples",
        "control_approvals", "control_updates", "control_secrets", "control_audit",
        "control_notices",
    }
    assert expected <= names
    rows = db.conn.execute("SELECT id, execution_mode, paused FROM control_setup").fetchall()
    assert [tuple(row) for row in rows] == [(1, "approval", 0)]
    db.close()


def test_v16_database_upgrades_to_v20_and_preserves_rows(tmp_path, monkeypatch):
    """Upgrade a populated v16 schema to v20 without losing legacy evidence."""

    monkeypatch.setattr(db_module, "MIGRATIONS", MIGRATIONS[:16])
    db = Database(tmp_path / "legacy.sqlite3")
    assert db.migrate()[-1] == 16
    assert db.schema_version() == 16

    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO audit_events (correlation, category, event, detail, created_at) "
            "VALUES ('c1', 'lifecycle', 'legacy_event', 'legacy detail', '2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO trades (trade_id, signal_id, idempotency_key, channel_id, message_id, "
            "symbol, side, entry_mode, requested_quote, entry_price, trade_state, created_at, updated_at) "
            "VALUES ('t-1', 's-1', 'idem-1', 1, 1, 'BTCUSDT', 'buy', 'direct', "
            "'10', '100', 'position_open', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO trade_orders (trade_order_id, trade_id, kind, client_order_id, "
            "symbol, side, order_type, price, quantity, allocated_qty, sold_qty, state, "
            "created_at, updated_at) VALUES "
            "('to-1', 't-1', 'take_profit', 'co-1', 'BTCUSDT', 'SELL', 'LIMIT', "
            "'100', '1', '1', '0', 'open', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO trading_notifications (event_key, role, chat_id, text, status, "
            "created_at, updated_at) VALUES "
            "('legacy:1', 'owner', 1, 'legacy text', 'pending', "
            "'2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')"
        )

    monkeypatch.setattr(db_module, "MIGRATIONS", MIGRATIONS)
    applied = db.migrate()
    assert applied == [17, 18, 19, 20]
    assert db.schema_version() == 20
    assert db.migrate() == []  # idempotent

    audit = db.conn.execute("SELECT event_type, source_ref FROM audit_events").fetchone()
    assert audit["event_type"] == "legacy_event"
    assert audit["source_ref"].startswith("legacy:audit:")

    order = db.conn.execute(
        "SELECT stop_trigger_price, leg_role FROM trade_orders WHERE trade_order_id = 'to-1'"
    ).fetchone()
    assert order["stop_trigger_price"] is None
    assert order["leg_role"] == "take_profit"

    notification = db.conn.execute(
        "SELECT category, event_type, text FROM trading_notifications WHERE event_key = 'legacy:1'"
    ).fetchone()
    assert notification["category"] == "legacy"
    assert notification["event_type"] == "legacy"
    assert notification["text"] == "legacy text"

    tables = {
        row["name"]
        for row in db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert {"exchange_operations", "oco_groups", "oco_legs", "notification_preferences"} <= tables
    db.close()
