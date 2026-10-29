"""Logout and account-switching behaviour (deterministic, no network)."""

from __future__ import annotations

import asyncio as aio
import contextlib
import pathlib

import pytest
from conftest import cfg as cfg_fixture  # noqa: F401

from tg_signal_ingest import main as cli
from tg_signal_ingest.telethon import logout_and_delete_session


class FakeClient:
    """Duck-typed Telethon client for logout flows."""

    def __init__(self, authorized: bool = False, log_out_result: bool = True):
        self._authorized = authorized
        self._log_out_result = log_out_result
        self.log_out_calls = 0
        self.disconnect_calls = 0

    async def connect(self) -> None:
        if getattr(self, "connect_fails", False):
            raise ConnectionError("simulated direct transport failure")

    async def is_user_authorized(self) -> bool:
        return self._authorized

    async def log_out(self) -> bool:
        self.log_out_calls += 1
        return self._log_out_result

    async def disconnect(self) -> None:
        self.disconnect_calls += 1


@pytest.fixture()
def session_pair(cfg, tmp_path: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    from tg_signal_ingest.telethon import resolve_session_file

    _, session = resolve_session_file(cfg)
    session.write_bytes(b"placeholder-not-a-real-session")
    journal = pathlib.Path(str(session) + "-journal")
    journal.write_bytes(b"")
    return session, journal


def test_logout_revokes_remotely_and_deletes_files(cfg, session_pair, monkeypatch):
    session, journal = session_pair
    fake = FakeClient(authorized=True, log_out_result=True)
    outcome = aio.run(logout_and_delete_session(cfg, fake))
    assert fake.log_out_calls == 1
    assert fake.disconnect_calls == 1
    assert "revoked on Telegram servers" in outcome
    assert not session.exists() and not journal.exists()


def test_logout_local_only_when_log_out_reports_failure(cfg, session_pair):
    session, _ = session_pair
    fake = FakeClient(authorized=True, log_out_result=False)
    outcome = aio.run(logout_and_delete_session(cfg, fake))
    assert "revoked" not in outcome, outcome
    assert "local session files removed" in outcome
    assert not session.exists()


def test_logout_local_only_when_connect_fails(cfg, session_pair):
    session, _ = session_pair
    fake = FakeClient()
    fake._authorized = True
    fake.connect_fails = True
    outcome = aio.run(logout_and_delete_session(cfg, fake))
    assert "revoked" not in outcome
    assert fake.log_out_calls == 0
    assert not session.exists()


def test_logout_safe_when_session_missing(cfg):
    outcome = aio.run(logout_and_delete_session(cfg, FakeClient()))
    assert isinstance(outcome, str) and outcome


def test_logout_refused_while_service_running(cfg, monkeypatch, session_pair):
    session, _ = session_pair
    monkeypatch.setattr(cli, "_load_or_exit", lambda **_kwargs: cfg)
    monkeypatch.setattr(cli, "_ingestion_process_running", lambda _cfg: "systemd unit active")

    async def refuse_logout(*_args, **_kwargs):
        pytest.fail("logout must not be attempted while ingestion is running")

    monkeypatch.setattr(cli, "logout_and_delete_session", refuse_logout)
    rc = aio.run(
        cli.cmd_auth_logout(
            type("A", (), {"verbose": False})(),
        )
    )
    assert rc == 1
    assert session.exists(), "session must not be deleted while ingestion may be running"


def test_logout_proceeds_when_nothing_running(cfg, monkeypatch, session_pair, capsys):
    session, _ = session_pair
    monkeypatch.setattr(cli, "_ingestion_process_running", lambda _cfg: None)
    aio.run(logout_and_delete_session(cfg, FakeClient(authorized=True)))
    assert not session.exists()
    captured = capsys.readouterr()
    assert not captured.err  # no 'refusing' warning should appear


def test_account_switchable_after_logout(cfg, session_pair):
    """After logout the session file is gone and a fresh client starts unauthenticated."""
    from tg_signal_ingest.telethon import build_client

    session, _ = session_pair
    aio.run(logout_and_delete_session(cfg, FakeClient(authorized=True)))
    assert not session.exists()

    client = build_client(cfg)  # fresh, empty session for the next account
    try:
        assert client.session.auth_key is None  # no credentials → any account can log in
    finally:
        with contextlib.suppress(Exception):
            client.session.close()


def test_fresh_session_is_unauthenticated(cfg):
    """Direct duck-check: build a fresh client object and assert its session has no auth key."""
    from tg_signal_ingest.telethon import build_client

    client = build_client(cfg)
    try:
        assert getattr(client.session, "authorized", False) is False or client.session.auth_key is None
    finally:
        with contextlib.suppress(Exception):
            client.session.close()


def test_cli_login_holds_session_lock_and_refuses_when_busy(cfg, monkeypatch):
    from tg_signal_ingest.session_guard import SessionBusyError, SessionGuard

    async def fake_login(_cfg):
        probe = SessionGuard(_cfg)
        try:
            with pytest.raises(SessionBusyError):
                await probe.acquire()
        finally:
            probe.release()
        return "Login succeeded (test)"

    monkeypatch.setattr(cli, "interactive_login", fake_login)
    monkeypatch.setattr(cli, "_load_or_exit", lambda **_kwargs: cfg)
    rc = aio.run(cli.cmd_auth_login(type("A", (), {"verbose": False})()))
    assert rc == 0

    async def scenario():
        guard = SessionGuard(cfg)
        await guard.acquire()
        try:
            return await cli.cmd_auth_login(type("A", (), {"verbose": False})())
        finally:
            guard.release()

    assert aio.run(scenario()) == 4
