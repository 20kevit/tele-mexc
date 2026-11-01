"""Durable control-bot state: setup, secrets, dedupe."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tg_signal_ingest.control.secretbox import SecretBox, SecretBoxError
from tg_signal_ingest.control.store import ControlStore

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


class Clock:
    def __init__(self, now: datetime = NOW):
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def _store(repo) -> tuple[ControlStore, Clock]:
    clock = Clock()
    return ControlStore(repo.db, now=clock), clock


def test_setup_state_machine_requires_every_step(repo):
    store, _ = _store(repo)
    assert store.setup()["effective_state"] == "not_configured"

    store.update_setup(telegram_state="pending")
    assert store.effective_state() == "telegram_auth_pending"
    store.update_setup(telegram_state="authenticated")
    assert store.effective_state() == "telegram_authenticated"

    store.update_setup(mexc_state="pending")
    assert store.effective_state() == "mexc_config_pending"
    store.update_setup(mexc_state="verified")
    assert store.effective_state() == "channel_selection_pending"

    store.update_setup(selected_channel_id=-100123, selected_channel_title="Signals")
    assert store.effective_state() == "automatic"

    store.update_setup(paused=1, pause_reason="owner pause")
    assert store.effective_state() == "paused"
    store.update_setup(paused=0)
    assert store.effective_state() == "automatic"


def test_update_dedupe_and_flow_ttl_never_store_sensitive_payload(repo):
    store, clock = _store(repo)
    assert store.mark_update(7) is True
    assert store.mark_update(7) is False

    store.set_flow("mexc_key", {"hint": "x", "phone": "+98912", "code": "12345"})
    flow = store.flow()
    assert flow["state"] == "mexc_key"
    assert flow["payload"] == {"hint": "x"}

    clock.now += timedelta(seconds=901)
    assert store.flow()["expired"] is True
    store.clear_flow()
    assert store.flow()["state"] == "idle"


def test_secretbox_roundtrip_and_tamper_detection(repo):
    store, _ = _store(repo)
    box = SecretBox("correct horse battery staple")
    assert box.available is True
    store.put_secret("mexc_api_secret", "super-secret-value", box)
    assert store.get_secret("mexc_api_secret", box) == "super-secret-value"

    row = repo.db.conn.execute("SELECT * FROM control_secrets WHERE name = 'mexc_api_secret'").fetchone()
    assert "super-secret-value" not in str(dict(row))
    repo.db.conn.execute(
        "UPDATE control_secrets SET ciphertext = ? WHERE name = 'mexc_api_secret'",
        ("00" + str(row["ciphertext"])[2:],),
    )
    repo.db.conn.commit()
    with pytest.raises(SecretBoxError):
        store.get_secret("mexc_api_secret", box)


def test_missing_secret_key_fails_closed(repo):
    store, _ = _store(repo)
    with pytest.raises(SecretBoxError, match="CONTROL_SECRET_KEY"):
        store.put_secret("mexc_api_key", "value", SecretBox(None))
