"""Terminal-only Telegram login: explicit hash, no chat codes, accurate errors."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon.errors import PhoneCodeExpiredError, PhoneCodeInvalidError

from tg_signal_ingest import telethon as telethon_module
from tg_signal_ingest.telethon import AuthRequired, interactive_login


class FakeLoginClient:
    def __init__(self, *, sign_in_error=None):
        self.authorized = False
        self.sign_in_error = sign_in_error
        self.connected = False
        self.disconnects = 0
        self.code_requests: list[str] = []
        self.sign_ins: list[dict] = []
        self.code_hash = "hash-1"
        self.me = SimpleNamespace(first_name="Owner", last_name=None, username="owner")

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

    async def sign_in(self, **kwargs):
        self.sign_ins.append(kwargs)
        if self.sign_in_error is not None:
            raise self.sign_in_error
        self.authorized = True

    async def get_me(self):
        return self.me


async def test_login_uses_explicit_hash_and_hidden_code_input(cfg, monkeypatch, capsys):
    client = FakeLoginClient()
    monkeypatch.setattr(telethon_module, "secure_session_files", lambda _cfg: None)
    monkeypatch.setattr("builtins.input", lambda _prompt: "+15550101337")
    monkeypatch.setattr(telethon_module, "getpass", lambda _prompt: "12345")

    result = await interactive_login(cfg, client=client)

    assert "Login succeeded" in result
    assert client.code_requests == ["+15550101337"]
    assert client.sign_ins == [
        {"phone": "+15550101337", "code": "12345", "phone_code_hash": "hash-1"}
    ]
    rendered = capsys.readouterr().out
    assert "SentCodeTypeApp" in rendered or "Telegram app" in rendered
    assert "12345" not in rendered
    assert "+15550101337" not in rendered
    assert client.disconnects == 1


async def test_shared_code_expiry_keeps_category_and_never_requests_again(
    cfg, monkeypatch, capsys
):
    client = FakeLoginClient(sign_in_error=PhoneCodeExpiredError(None))
    monkeypatch.setattr(telethon_module, "secure_session_files", lambda _cfg: None)
    monkeypatch.setattr("builtins.input", lambda _prompt: "+15550101337")
    monkeypatch.setattr(telethon_module, "getpass", lambda _prompt: "98765")

    with pytest.raises(AuthRequired) as excinfo:
        await interactive_login(cfg, client=client)

    rendered = str(excinfo.value)
    assert "PHONE_CODE_EXPIRED" in rendered
    assert "shared" in rendered
    assert "98765" not in rendered
    assert len(client.code_requests) == 1
    assert len(client.sign_ins) == 1
    assert client.sign_ins[0]["phone_code_hash"] == "hash-1"


async def test_invalid_code_retries_same_hash_then_reports_category(
    cfg, monkeypatch, capsys
):
    client = FakeLoginClient(sign_in_error=PhoneCodeInvalidError(None))
    monkeypatch.setattr(telethon_module, "secure_session_files", lambda _cfg: None)
    monkeypatch.setattr("builtins.input", lambda _prompt: "+15550101337")
    monkeypatch.setattr(telethon_module, "getpass", lambda _prompt: "11111")
    monkeypatch.setattr(telethon_module.time, "sleep", lambda _seconds: None)

    with pytest.raises(AuthRequired) as excinfo:
        await interactive_login(cfg, client=client)

    assert "allowed code attempts" in str(excinfo.value)
    assert len(client.code_requests) == 1
    assert len(client.sign_ins) == telethon_module.MAX_CODE_ATTEMPTS
    assert {item["phone_code_hash"] for item in client.sign_ins} == {"hash-1"}
    assert "PHONE_CODE_INVALID" in capsys.readouterr().out


async def test_missing_hash_aborts_before_submitting(cfg, monkeypatch):
    client = FakeLoginClient()
    client.code_hash = None
    monkeypatch.setattr(telethon_module, "secure_session_files", lambda _cfg: None)
    monkeypatch.setattr("builtins.input", lambda _prompt: "+15550101337")

    with pytest.raises(AuthRequired, match="phone_code_hash"):
        await interactive_login(cfg, client=client)

    assert client.code_requests == ["+15550101337"]
    assert client.sign_ins == []
    assert client.disconnects == 1
