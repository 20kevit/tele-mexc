"""Direct Telegram connectivity diagnostics and Telethon transport tests."""

from __future__ import annotations

import pytest

from tg_signal_ingest import connectivity
from tg_signal_ingest.connectivity import (
    ConnectivityError,
    EndpointCheck,
    describe_direct_transport,
    run_direct_diagnostics,
)


def check(name: str, host: str, *, tcp: bool = True, tls: bool | None = None) -> EndpointCheck:
    return EndpointCheck(name, host, 443, ("203.0.113.10",), tcp, tls, "test result")


def test_diagnostics_report_direct_endpoints_and_ignored_ambient_proxy(monkeypatch):
    def fake_check(name, host, port, *, tls, timeout):
        return check(name, host, tls=True if tls else None)

    monkeypatch.setattr(connectivity, "_check_endpoint", fake_check)
    diag = run_direct_diagnostics(
        environ={"HTTPS_PROXY": "http://127.0.0.1:1", "TELEGRAM_PROXY_HOST": "127.0.0.1"},
        timeout=0.1,
    )
    assert diag.ok
    assert diag.bot_api.name == "Telegram Bot API"
    assert len(diag.telegram_dcs) == len(connectivity.DEFAULT_DC_IPS)
    assert diag.ambient_proxy_vars == ("HTTPS_PROXY",)
    assert diag.legacy_proxy_vars == ("TELEGRAM_PROXY_HOST",)
    assert "direct" in describe_direct_transport().lower()


def test_diagnostics_fail_when_bot_api_tls_fails(monkeypatch):
    def fake_check(name, host, port, *, tls, timeout):
        if tls:
            return EndpointCheck(name, host, port, ("203.0.113.10",), True, False, "TLS failed")
        return check(name, host)

    monkeypatch.setattr(connectivity, "_check_endpoint", fake_check)
    with pytest.raises(ConnectivityError, match="Bot API"):
        run_direct_diagnostics(environ={}, timeout=0.1)


def test_diagnostics_fail_when_all_mtproto_dcs_are_unreachable(monkeypatch):
    def fake_check(name, host, port, *, tls, timeout):
        if tls:
            return check(name, host, tls=True)
        return EndpointCheck(name, host, port, ("203.0.113.10",), False, None, "TCP failed")

    monkeypatch.setattr(connectivity, "_check_endpoint", fake_check)
    with pytest.raises(ConnectivityError, match="data centre"):
        run_direct_diagnostics(environ={}, timeout=0.1)


def test_telethon_client_has_no_proxy_even_with_ambient_proxy_env(cfg, monkeypatch):
    from tg_signal_ingest.telethon import build_client

    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1081")
    monkeypatch.setenv("ALL_PROXY", "socks5://127.0.0.1:1081")
    client = build_client(cfg)
    assert client._connection is not None
    assert client._proxy is None
