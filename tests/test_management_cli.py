"""Management CLI is read-only for status and refuses unsafe startup."""

from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace

from tg_signal_ingest import main
from tg_signal_ingest.db import Database
from tg_signal_ingest.management.store import ManagementStore


class FakeChannelClient:
    def __init__(self, entity, *, account_id: int = 222):
        self.entity = entity
        self.account_id = account_id
        self.disconnect_calls = 0

    async def get_me(self):
        return SimpleNamespace(
            id=self.account_id,
            first_name="Personal",
            last_name="Operator",
            username="personal",
        )

    async def get_entity(self, _peer):
        return self.entity

    async def disconnect(self):
        self.disconnect_calls += 1


def _personal_selection(cfg):
    cfg.ensure_dirs()
    db = Database(cfg.db_path)
    db.migrate()
    store = ManagementStore(db)
    store.set_operator_channel(
        222,
        selected_channel_id=101,
        selected_channel_title="Source",
        selected_channel_username=None,
        selected_channel_access_hash=1010,
        selection_snapshot_id=None,
    )
    db.close()


def test_auth_check_channel_verifies_access_without_printing_ids(cfg, monkeypatch, capsys):
    cfg.control.owner_user_id = 111
    cfg.control.developer_user_id = 222
    _personal_selection(cfg)
    fake = FakeChannelClient(
        SimpleNamespace(id=101, title="Source", username=None, broadcast=True, megagroup=False)
    )

    async def fake_connect(_cfg):
        return fake

    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)
    monkeypatch.setattr(main, "connect_and_verify", fake_connect)
    rc = asyncio.run(main.cmd_auth_check_channel(None))
    out = capsys.readouterr().out
    assert rc == 0
    assert "channel access: OK" in out
    assert "broadcast" in out
    assert "101" not in out
    assert "Source" not in out
    assert fake.disconnect_calls == 1


def test_auth_check_channel_refuses_mismatched_selections(cfg, monkeypatch, capsys):
    cfg.control.owner_user_id = 111
    cfg.control.developer_user_id = 222
    _personal_selection(cfg)
    db = Database(cfg.db_path)
    store = ManagementStore(db)
    store.set_operator_channel(
        111,
        selected_channel_id=202,
        selected_channel_title="Other",
        selected_channel_username=None,
        selected_channel_access_hash=2020,
        selection_snapshot_id=None,
    )
    db.close()

    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)
    rc = asyncio.run(main.cmd_auth_check_channel(None))
    assert rc == 2
    assert "differ" in capsys.readouterr().err


def _cfg(cfg, monkeypatch):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)


def test_manage_status_does_not_create_or_migrate_missing_database(cfg, monkeypatch, capsys):
    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_status(None) == 1
    assert not cfg.db_path.exists()
    assert "not initialized" in capsys.readouterr().out


def test_manage_status_does_not_migrate_legacy_or_empty_database(cfg, monkeypatch, capsys):
    cfg.ensure_dirs()
    sqlite3.connect(cfg.db_path).close()
    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_status(None) == 1
    assert "schema not initialized" in capsys.readouterr().out
    db = sqlite3.connect(cfg.db_path)
    assert db.execute("SELECT name FROM sqlite_master WHERE name='schema_migrations'").fetchone() is None
    db.close()


def test_manage_status_reads_existing_state_without_writing(cfg, monkeypatch, capsys):
    cfg.ensure_dirs()
    db = Database(cfg.db_path)
    db.migrate()
    repo = __import__("tg_signal_ingest.repository", fromlist=["Repository"]).Repository(db)
    repo.ensure_channel(101, 1010, "source", "Source")
    repo.select_channel(101)
    from tg_signal_ingest.management.store import ManagementStore

    store = ManagementStore(db)
    store.update_state(selected_channel_id=101, selected_channel_title="Source")
    job = store.create_job(101, "Source")
    store.update_job(job["job_id"], status="interrupted", inserted=3, total_stored=3)
    db.close()
    before = cfg.db_path.read_bytes()

    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_status(None) == 0
    out = capsys.readouterr().out
    assert "selected channel: Source" in out
    assert "ingestion: interrupted" in out
    assert "trading/MEXC: deliberately disabled" in out
    assert cfg.db_path.read_bytes() == before


def test_manage_run_requires_bot_credentials(cfg, monkeypatch, capsys):
    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_run(None) == 2
    assert "CONTROL_BOT_TOKEN" in capsys.readouterr().err
    assert not cfg.db_path.exists()


def test_manage_run_requires_both_operator_ids(cfg, monkeypatch, capsys):
    cfg.control.bot_token = "123:abc"
    cfg.control.owner_user_id = 111
    cfg.control.developer_user_id = None
    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_run(None) == 2
    err = capsys.readouterr().err
    assert "CONTROL_DEVELOPER_USER_ID" in err
    assert not cfg.db_path.exists()


def test_manage_status_does_not_print_numeric_operator_ids(cfg, monkeypatch, capsys):
    cfg.control.bot_token = "123:abc"
    cfg.control.owner_user_id = 111111111
    cfg.control.developer_user_id = 222222222
    cfg.ensure_dirs()
    db = Database(cfg.db_path)
    db.migrate()
    db.close()
    _cfg(cfg, monkeypatch)
    assert main.cmd_manage_status(None) == 0
    out = capsys.readouterr().out
    assert "111111111" not in out
    assert "222222222" not in out
    assert "owner=set developer=set" in out
