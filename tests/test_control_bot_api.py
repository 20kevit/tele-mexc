"""Telegram Bot API transport tests (direct networking)."""

from __future__ import annotations

import json

import pytest

from tg_signal_ingest.control import bot_api
from tg_signal_ingest.control.bot_api import BotApiClient, BotApiError, _DirectHTTPSConnection


def test_bot_api_requires_a_well_formed_token():
    with pytest.raises(BotApiError, match="token"):
        BotApiClient("")
    with pytest.raises(BotApiError, match="token"):
        BotApiClient("not-a-token")


def test_bot_api_rejects_non_https_base():
    with pytest.raises(BotApiError, match="https"):
        BotApiClient("123456:secret-token", api_base="http://api.telegram.org")


def test_bot_api_injected_transport_is_test_only_and_redacts_token():
    client = BotApiClient(
        "123456:secret-token",
        transport=lambda method, payload: ({"ok": True, "result": {"method": method, "payload": payload}}),
    )
    assert client.call("getMe", password="x") == {"method": "getMe", "payload": {"password": "x"}}
    assert "secret-token" not in repr(client)


def test_direct_connection_has_verified_tls_context():
    connection = _DirectHTTPSConnection("api.telegram.org", 443, timeout=1)
    assert connection.host == "api.telegram.org"
    assert connection.port == 443
    assert connection._context is not None
    assert connection._context.verify_mode.name == "CERT_REQUIRED"


def test_http_transport_is_direct_and_ignores_ambient_proxy_env(monkeypatch):
    seen: dict[str, object] = {}

    class FakeResponse:
        def read(self) -> bytes:
            return json.dumps({"ok": True, "result": {"id": 1, "username": "test_bot"}}).encode()

    class FakeConnection:
        def __init__(self, host: str, port: int, timeout: float):
            seen.update(host=host, port=port, timeout=timeout)

        def request(self, method, path, body=None, headers=None):
            seen.update(method=method, path=path, body=body, headers=headers)

        def getresponse(self):
            return FakeResponse()

        def close(self):
            seen["closed"] = True

    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1081")
    monkeypatch.setattr(bot_api, "_DirectHTTPSConnection", FakeConnection)
    client = BotApiClient("123456:secret-token")
    assert client.call("getMe") == {"id": 1, "username": "test_bot"}
    assert seen["host"] == "api.telegram.org"
    assert seen["port"] == 443
    assert seen["method"] == "POST"
    assert str(seen["path"]).endswith("/getMe")
    assert seen["closed"] is True


def test_network_error_never_reveals_token(monkeypatch):
    def broken_connection(host: str, port: int, timeout: float):
        raise OSError("direct connect failed")

    monkeypatch.setattr(bot_api, "_DirectHTTPSConnection", broken_connection)
    client = BotApiClient("123456:secret-token")
    with pytest.raises(BotApiError) as excinfo:
        client.call("getMe")
    assert "secret-token" not in str(excinfo.value)
    assert "direct network failure" in str(excinfo.value)
