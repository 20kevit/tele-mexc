"""Offline tests for the read-only ``tg-ingest trading verify`` command.

The command must classify credential problems without printing the key or the
secret and without any network order attempt; here the exchange client is
replaced and no socket connection is permitted.
"""

from __future__ import annotations

import pytest

from tests.test_mexc_credentials import DOC_SAMPLE
from tg_signal_ingest.config import AppConfig, MexcConfig
from tg_signal_ingest.main import _report_mexc_verification, build_parser


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Hard guard: no test here may open a socket (offline-only suite)."""

    import socket

    def _refuse(*args, **kwargs):
        raise AssertionError("blocked network access in an offline test")

    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket.socket, "connect", _refuse)


@pytest.fixture()
def _credentials_module():
    """The submodule is addressed via sys.modules: the name
    ``tg_signal_ingest.mexc.credentials`` resolves to the classifier function
    exported by ``client.py`` in package attribute space, not to the module."""

    import importlib

    return importlib.import_module("tg_signal_ingest.mexc.credentials")


def _cfg() -> AppConfig:
    cfg = AppConfig()
    cfg.mexc = MexcConfig(api_key="test-key", api_secret="test-secret")
    return cfg


def test_parser_advertises_verify_as_read_only():
    args = build_parser().parse_args(["trading", "verify"])
    assert args.func.__name__ == "cmd_trading_verify"


def test_missing_credentials_fail_closed_without_hint_leaking_values():
    cfg = AppConfig()
    cfg.mexc = MexcConfig()
    code, lines = _report_mexc_verification(cfg)
    output = "\n".join(lines)
    assert code == 2
    assert "missing" in output.lower()
    assert "example" not in output.lower()
    assert "confirms" not in output
    assert output.count("MEXC_API_KEY") == 1  # names only, never values
    assert "test-secret" not in output


def test_missing_credentials_do_not_build_a_client(monkeypatch):
    calls: list[object] = []
    monkeypatch.setattr(
        "tg_signal_ingest.trading.runtime.build_mexc_client",
        lambda cfg: calls.append(cfg) or None,
    )
    cfg = AppConfig()
    cfg.mexc = MexcConfig()
    code, _lines = _report_mexc_verification(cfg)
    assert code == 2
    assert calls == []  # no client construction without credentials


def test_missing_permission_is_reported_without_secrets(monkeypatch, _credentials_module):
    from tg_signal_ingest.mexc.errors import MexcPermissionError

    def _raise(_client):
        raise MexcPermissionError("MEXC API key lacks required permissions: SPOT_DEAL_WRITE")

    monkeypatch.setattr(_credentials_module, "validate_credentials", _raise)
    code, lines = _report_mexc_verification(_cfg())
    output = "\n".join(lines)
    assert code == 1
    assert "MISSING PERMISSION" in output
    assert "test-key" not in output and "test-secret" not in output


def test_auth_failure_is_classified_and_secret_free(monkeypatch, _credentials_module):
    from tg_signal_ingest.mexc.errors import MexcAuthError

    def _raise(_client):
        raise MexcAuthError("400, code 10072", http_status=400, code=10072)

    monkeypatch.setattr(_credentials_module, "validate_credentials", _raise)
    code, lines = _report_mexc_verification(_cfg())
    output = "\n".join(lines)
    assert code == 1
    assert "REJECTED BY MEXC" in output
    assert "no order was attempted" in output
    assert "test-key" not in output and "test-secret" not in output


def test_network_failure_is_classified_and_secret_free(monkeypatch, _credentials_module):
    from tg_signal_ingest.mexc.errors import MexcNetworkError

    def _raise(_client):
        raise MexcNetworkError("timeout", http_status=None, code=None)

    monkeypatch.setattr(_credentials_module, "validate_credentials", _raise)
    code, lines = _report_mexc_verification(_cfg())
    output = "\n".join(lines)
    assert code == 3
    assert "no order was attempted" in output
    assert "test-key" not in output and "test-secret" not in output


def test_successful_verification_reports_only_public_fields(monkeypatch, _credentials_module):
    from tg_signal_ingest.mexc.credentials import parse_api_key_info

    seen: list[object] = []

    def _validate(client):
        seen.append(client)
        return parse_api_key_info(DOC_SAMPLE)

    monkeypatch.setattr(_credentials_module, "validate_credentials", _validate)
    code, lines = _report_mexc_verification(_cfg())
    output = "\n".join(lines)
    assert code == 0
    assert len(seen) == 1  # exactly one client under verification
    assert "read-only GET /api/v3/apiKeyInfo" in output
    assert "no order placed" in output
    assert "VALID" in output
    assert "SPOT_ACCOUNT_READ" in output
    assert "66 day(s) remaining" in output  # non-permanent key: expiry surfaced
    assert "CONTRACT_ACCOUNT_READ" in output  # full permission list, public data
    assert "test-key" not in output and "test-secret" not in output


def test_success_report_contains_no_secret_like_values(monkeypatch, _credentials_module):
    from tg_signal_ingest.mexc.credentials import parse_api_key_info

    def _validate(_client):
        return parse_api_key_info(DOC_SAMPLE)

    monkeypatch.setattr(_credentials_module, "validate_credentials", _validate)
    _code, lines = _report_mexc_verification(
        AppConfig(),  # mode/sim config, credentials already validated by the patched call
        client=object(),
    )
    output = "\n".join(lines)
    # raw documented sample values must never appear in the report
    assert "mx0vgxxxxxxxvwe9Pq" not in output
    assert "162.133.62.1" in output  # whitelist IPs are public account metadata
    assert "P2P" in output  # key note/label helps the owner confirm the right key
