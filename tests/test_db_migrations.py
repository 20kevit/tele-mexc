"""Database migrations and schema basics."""

from __future__ import annotations

import sqlite3

import pytest

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
