"""Credential-validation tests against the documented apiKeyInfo response.

Docs checked 2026-10-23:
https://www.mexc.com/api-docs/spot-v3/spot-account-drive/query-api-key-info
is intentionally NOT fetched here; the fixture is the JSON sample published
in the official "Query APIKey Info" page (GET /api/v3/apiKeyInfo,
SPOT_ACCOUNT_READ, weight 1).
"""

from __future__ import annotations

import json
import urllib.parse

import pytest

from tg_signal_ingest.mexc import (
    MexcConfigError,
    MexcPermissionError,
    MexcSpotClient,
    WeightedRateGate,
    parse_api_key_info,
    validate_credentials,
)

DOC_SAMPLE = {
    "note": "P2P",
    "accessKey": "mx0FAKEKEY0000000000",
    "createTime": "2026-08-31T02:22:18.000+00:00",
    "permissions": "CONTRACT_ACCOUNT_READ,SPOT_ACCOUNT_READ,SPOT_ACCOUNT_WRITE,"
    "SPOT_DEAL_READ,SPOT_DEAL_WRITE,SPOT_TRANSFER_READ",
    "remainingValidity": 66,
    "ipWhiteList": "162.133.62.1",
    "status": "VALID",
}


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Hard guard: no test here may open a socket (offline-only suite)."""

    import socket

    def _refuse(*args, **kwargs):
        raise AssertionError("blocked network access in an offline test")

    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket.socket, "connect", _refuse)


class FakeClient:
    def __init__(self, raw):
        self.raw = raw
        self.calls = 0

    def api_key_info(self):
        self.calls += 1
        return self.raw


def test_parse_documented_sample():
    info = parse_api_key_info(DOC_SAMPLE)
    assert info.valid
    assert not info.permanent
    assert info.permissions == (
        "CONTRACT_ACCOUNT_READ",
        "SPOT_ACCOUNT_READ",
        "SPOT_ACCOUNT_WRITE",
        "SPOT_DEAL_READ",
        "SPOT_DEAL_WRITE",
        "SPOT_TRANSFER_READ",
    )
    assert info.ip_whitelist == ("162.133.62.1",)
    assert info.has_write()


def test_validate_credentials_accepts_documented_sample_read_only():
    client = FakeClient(DOC_SAMPLE)
    info = validate_credentials(client)
    assert client.calls == 1  # exactly one read-only GET; no trade call
    assert info.valid


def test_permmanent_key_is_valid():
    info = parse_api_key_info({**DOC_SAMPLE, "remainingValidity": -999})
    assert info.valid and info.permanent


@pytest.mark.parametrize(
    "mutation,expected_exc",
    [
        ({"status": "DELETE"}, MexcConfigError),
        ({"status": "FROZEN"}, MexcConfigError),
        ({"remainingValidity": 0}, MexcConfigError),
        ({"permissions": "SPOT_ACCOUNT_READ,SPOT_DEAL_READ"}, MexcPermissionError),
    ],
)
def test_validate_credentials_fails_closed(mutation, expected_exc):
    with pytest.raises(expected_exc):
        validate_credentials(FakeClient({**DOC_SAMPLE, **mutation}))


def test_missing_read_permission_is_rejected_even_for_valid_key():
    raw = {**DOC_SAMPLE, "permissions": "SPOT_DEAL_WRITE"}
    with pytest.raises(MexcPermissionError):
        validate_credentials(FakeClient(raw))


def test_api_key_info_request_matches_documented_parameters():
    """GET /api/v3/apiKeyInfo requires timestamp + accessKey + signature."""

    class Transport:
        def __init__(self):
            self.calls = []

        def __call__(self, url, method, headers, body, timeout):
            self.calls.append((url, method, headers, body))
            return 200, {"Content-Type": "application/json"}, json.dumps({"status": "VALID"}).encode()

    transport = Transport()
    client = MexcSpotClient(
        "key",
        "secret",
        transport=transport,
        clock=lambda: 1700000000000,
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
    )
    client.api_key_info()
    url, method, headers, body = transport.calls[0]
    assert method == "GET"
    assert url.startswith("https://api.mexc.com/api/v3/apiKeyInfo?")
    query = urllib.parse.parse_qs(url.split("?")[1])
    assert query["accessKey"] == ["key"]
    assert query["timestamp"] == ["1700000000000"]
    assert "signature" in query
    assert headers["X-MEXC-APIKEY"] == "key"
    assert body is None
    assert "MEXC_API_SECRET".lower() not in url.lower()
