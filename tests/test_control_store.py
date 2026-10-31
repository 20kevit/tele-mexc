"""Durable control-bot state: setup, learning, approvals, secrets, dedupe."""

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


def _sample(store: ControlStore, message_id: int, *, actionable: bool = True):
    return store.add_learning_sample(
        channel_id=100,
        message_id=message_id,
        edit_version=0,
        message_text=f"BUY BTCUSDT ENTRY 100 TP 110 SL 95 #{message_id}",
        extracted={"symbol": "BTCUSDT"},
        parser_kind="signal",
        actionable=actionable,
    )


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
    assert store.effective_state() == "channel_selected"

    store.update_setup(learning_state="in_progress")
    assert store.effective_state() == "learning_in_progress"

    store.update_setup(learning_state="completed")
    assert store.effective_state() == "automatic"

    store.update_setup(paused=1, pause_reason="owner pause")
    assert store.effective_state() == "paused"
    store.update_setup(paused=0)
    assert store.effective_state() == "automatic"


def test_learning_requires_five_consecutive_actionable_approvals(repo):
    store, _ = _store(repo)
    samples = [_sample(store, i)[0] for i in range(1, 6)]

    for sample in samples[:4]:
        store.decide_learning_sample(sample["sample_id"], True)
    assert store.setup()["learning_streak"] == 4
    assert store.setup()["learning_state"] == "in_progress"

    store.decide_learning_sample(samples[4]["sample_id"], True)
    assert store.setup()["learning_streak"] == 5
    assert store.setup()["learning_state"] == "completed"

    rejected = _sample(store, 6)[0]
    store.decide_learning_sample(rejected["sample_id"], False)
    assert store.setup()["learning_streak"] == 0
    assert store.setup()["learning_state"] == "in_progress"
    assert store.setup()["learning_completed_at"] is None


def test_invalid_or_duplicate_learning_examples_cannot_inflate_progress(repo):
    store, _ = _store(repo)
    invalid = _sample(store, 1, actionable=False)[0]
    with pytest.raises(ValueError, match="cannot be approved"):
        store.decide_learning_sample(invalid["sample_id"], True)

    store.decide_learning_sample(invalid["sample_id"], False)
    assert store.setup()["learning_streak"] == 0

    sample, created = _sample(store, 2)
    assert created is True
    duplicate, created_again = _sample(store, 2)
    assert created_again is False
    assert duplicate["sample_id"] == sample["sample_id"]
    with pytest.raises(ValueError, match="already decided"):
        store.decide_learning_sample(sample["sample_id"], True)
        store.decide_learning_sample(sample["sample_id"], True)


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

    row = repo.db.conn.execute(
        "SELECT * FROM control_secrets WHERE name = 'mexc_api_secret'"
    ).fetchone()
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
