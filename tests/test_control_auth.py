"""Control-bot Telegram login: in-memory secrets, safe failures, no logout."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon.errors import (
    PhoneCodeExpiredError,
    PhoneCodeHashEmptyError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)

from tg_signal_ingest.control import auth as auth_module
from tg_signal_ingest.control.auth import (
    AuthChallengeStale,
    AuthFlowError,
    AuthFlowExpired,
    TelegramLoginManager,
)


class FakeClient:
    def __init__(self, *, authorized=False, me=None):
        self.authorized = authorized
        self.me = me or SimpleNamespace(first_name="Owner", last_name=None, username="owner")
        self.connected = False
        self.disconnects = 0
        self.code_requests: list[str] = []
        self.sign_ins: list[dict] = []
        self.sign_in_error = None
        self.code_hash = "hash-1"

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnects += 1
        self.connected = False

    async def is_user_authorized(self):
        return self.authorized

    async def send_code_request(self, phone):
        self.code_requests.append(phone)
        sent_type = type("SentCodeTypeApp", (), {})()
        return SimpleNamespace(phone_code_hash=self.code_hash, type=sent_type)

    async def get_me(self):
        return self.me

    async def sign_in(self, **kwargs):
        self.sign_ins.append(kwargs)
        if self.sign_in_error is not None:
            raise self.sign_in_error
        if "password" not in kwargs:
            raise SessionPasswordNeededError(None)
        self.authorized = True


def _manager(cfg, client, *, clock=None):
    return TelegramLoginManager(cfg, client_factory=lambda _cfg: client, clock=clock)


async def test_probe_distinguishes_missing_unreachable_and_valid(cfg):
    missing = FakeClient(authorized=False)
    assert await _manager(cfg, missing).probe() == ("missing", None)
    assert missing.disconnects == 1

    valid = FakeClient(authorized=True)
    status, label = await _manager(cfg, valid).probe()
    assert status == "valid"
    assert label == "Owner (@owner)"

    class Broken(FakeClient):
        async def connect(self):
            raise OSError("network down")

    assert await _manager(cfg, Broken()).probe() == ("unreachable", None)


async def test_login_flow_keeps_secrets_in_memory_and_never_logs_out(cfg, monkeypatch):
    client = FakeClient()
    manager = _manager(cfg, client)
    secure_calls: list[int] = []
    monkeypatch.setattr(auth_module, "secure_session_files", lambda _cfg: secure_calls.append(1))

    with pytest.raises(AuthFlowError, match="کد کشور"):
        await manager.start("000")
    assert client.code_requests == []

    message = await manager.start("+15550101337")
    assert "کد ورود" in message
    assert client.code_requests == ["+15550101337"]
    assert manager.active is True

    message = await manager.submit_code("11111")
    assert "دو مرحله" in message
    assert client.sign_ins[-1]["code"] == "11111"
    assert manager.active is True

    label = await manager.submit_password("two-step-password")
    assert label == "Owner (@owner)"
    assert client.sign_ins[-1]["password"] == "two-step-password"
    assert client.authorized is True
    assert secure_calls == [1]
    assert manager.active is False
    assert client.disconnects >= 1


async def test_invalid_code_is_reported_without_echoing_the_code(cfg):
    client = FakeClient()
    client.sign_in_error = PhoneCodeInvalidError(None)
    manager = _manager(cfg, client)
    await manager.start("+15550101337")

    with pytest.raises(AuthFlowError) as excinfo:
        await manager.submit_code("987654")
    assert "نادرست" in str(excinfo.value)
    assert "987654" not in str(excinfo.value)
    assert manager.active is True


async def test_expired_code_returns_a_recoverable_expired_flow(cfg):
    client = FakeClient()
    client.sign_in_error = PhoneCodeExpiredError(None)
    manager = _manager(cfg, client)
    await manager.start("+15550101337")

    with pytest.raises(AuthFlowExpired) as excinfo:
        await manager.submit_code("987654")
    rendered = str(excinfo.value)
    assert "PHONE_CODE_EXPIRED" in rendered
    assert "چت" in rendered
    assert "987654" not in rendered
    assert manager.active is False
    assert client.disconnects >= 1


async def test_login_flow_expires_and_resets(cfg):
    client = FakeClient()
    now = [100.0]
    manager = _manager(cfg, client, clock=lambda: now[0])
    await manager.start("+15550101337")
    now[0] += 601
    with pytest.raises(AuthFlowExpired):
        await manager.submit_code("12345")
    assert manager.active is False
    assert client.disconnects >= 1


async def test_existing_valid_session_refuses_to_start_another_login(cfg):
    client = FakeClient(authorized=True)
    manager = _manager(cfg, client)
    with pytest.raises(AuthFlowError, match="از قبل معتبر"):
        await manager.start("+15550101337")
    assert client.code_requests == []


async def test_code_hash_is_recorded_used_exactly_and_not_reused_after_expiry(cfg):
    client = FakeClient()
    manager = _manager(cfg, client)
    await manager.start("+15550101337")
    assert manager.delivery_type == "SentCodeTypeApp"
    assert manager._flow is not None
    assert manager._flow.phone_code_hash == "hash-1"

    client.sign_in_error = PhoneCodeInvalidError(None)
    with pytest.raises(AuthFlowError, match="PHONE_CODE_INVALID"):
        await manager.submit_code("11111")
    assert client.sign_ins[-1]["phone_code_hash"] == "hash-1"

    client.sign_in_error = PhoneCodeExpiredError(None)
    with pytest.raises(AuthFlowExpired):
        await manager.submit_code("22222")
    with pytest.raises(AuthFlowError, match="فعال نیست"):
        await manager.submit_code("33333")
    assert len(client.sign_ins) == 2

    # A new login receives a new hash and never reuses the stale one.
    client.sign_in_error = None
    client.code_hash = "hash-2"
    await manager.start("+15550101337")
    assert manager._flow is not None
    assert manager._flow.phone_code_hash == "hash-2"
    await manager.submit_code("44444")
    assert client.sign_ins[-1]["phone_code_hash"] == "hash-2"


async def test_missing_phone_code_hash_aborts_without_submission(cfg):
    client = FakeClient()
    client.code_hash = None
    manager = _manager(cfg, client)
    with pytest.raises(AuthFlowError, match="phone_code_hash"):
        await manager.start("+15550101337")
    assert client.sign_ins == []
    assert manager.active is False
    assert client.disconnects >= 1


async def test_duplicate_code_submissions_are_serialized(cfg):
    import asyncio

    client = FakeClient()
    manager = _manager(cfg, client)
    await manager.start("+15550101337")
    results = await asyncio.gather(
        manager.submit_code("11111"),
        manager.submit_code("11111"),
        return_exceptions=True,
    )
    assert len(client.sign_ins) == 1
    assert sum(isinstance(item, AuthFlowError) for item in results) == 1


async def test_stale_hash_maps_to_its_own_category(cfg):
    client = FakeClient()
    client.sign_in_error = PhoneCodeHashEmptyError(None)
    manager = _manager(cfg, client)
    await manager.start("+15550101337")
    with pytest.raises(AuthChallengeStale) as excinfo:
        await manager.submit_code("11111")
    assert excinfo.value.category == "PHONE_CODE_HASH_EMPTY"
    assert "PHONE_CODE_HASH_EMPTY" in str(excinfo.value)
    assert manager.active is False
