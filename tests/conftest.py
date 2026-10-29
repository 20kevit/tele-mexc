"""Shared fixtures and fakes for the deterministic (no-network) test suite."""

from __future__ import annotations

import socket
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from tg_signal_ingest.config import AppConfig  # noqa: E402
from tg_signal_ingest.db import Database  # noqa: E402
from tg_signal_ingest.repository import Repository  # noqa: E402

FAKE_MESSAGE_EPOCH = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def block_internet_sockets(monkeypatch):
    """Prevent accidental IPv4/IPv6 connections in the pytest process.

    Unix socket pairs still work for asyncio. Subprocesses need their own
    isolation; this is a guardrail, not an OS-level network sandbox.
    """
    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex
    original_sendto = socket.socket.sendto

    def check(sock):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise AssertionError("Internet sockets are disabled in the test suite")

    def connect(sock, *args, **kwargs):
        check(sock)
        return original_connect(sock, *args, **kwargs)

    def connect_ex(sock, *args, **kwargs):
        check(sock)
        return original_connect_ex(sock, *args, **kwargs)

    def sendto(sock, *args, **kwargs):
        check(sock)
        return original_sendto(sock, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    monkeypatch.setattr(socket.socket, "sendto", sendto)


class FakeMsg:
    """Duck-typed Telegram message compatible with normalize.message_to_record."""

    def __init__(
        self,
        id: int,
        text: str | None = "hello",
        date: datetime | None = None,
        edit_date: datetime | None = None,
        media=None,
        grouped_id: int | None = None,
        reply_to=None,
        forward=None,
    ):
        self.id = id
        self.message = text
        self.date = date if date is not None else FAKE_MESSAGE_EPOCH + timedelta(seconds=id)
        self.edit_date = edit_date
        self.media = media
        self.grouped_id = grouped_id
        self.reply_to = reply_to
        self.forward = forward

    def to_dict(self):
        return {
            "id": self.id,
            "message": self.message,
            "date": self.date.isoformat() if self.date else None,
            "edit_date": self.edit_date.isoformat() if self.edit_date else None,
        }


class FakePhotoMedia:
    def __init__(self, pid: int = 77):
        self.photo = type("P", (), {"id": pid})()
        self.sizes = []


class ReplyTo:
    def __init__(self, reply_to_msg_id: int):
        self.reply_to_msg_id = reply_to_msg_id


@pytest.fixture()
def cfg(tmp_path: Path) -> AppConfig:
    c = AppConfig(
        api_id=12345,
        api_hash="a" * 32,
        data_dir=tmp_path / "data",
        db_path=tmp_path / "data" / "test.sqlite3",
        session_path=tmp_path / "config" / "session",
    )
    c.ensure_dirs()
    return c


@pytest.fixture()
def repo(cfg: AppConfig) -> Repository:
    db = Database(cfg.db_path)
    repo = Repository(db)
    repo.db.migrate()
    yield repo
    db.close()
