"""Failure behavior of the proxy layer: fail clearly, never fall back."""

from __future__ import annotations

import socket
import threading

import pytest

from tg_signal_ingest.config import AppConfig, ProxyConfig
from tg_signal_ingest.proxy import ProxyError, run_proxy_diagnostics


def make_cfg(host="127.0.0.1", port=1, ptype="http") -> AppConfig:
    cfg = AppConfig(api_id=1, api_hash="x")
    cfg.proxy = ProxyConfig(type=ptype, host=host, port=port)
    return cfg


def test_proxy_unreachable_is_proxy_error_not_silent_fallback():
    with pytest.raises(ProxyError) as err:
        run_proxy_diagnostics(make_cfg(port=1))  # port 1: nobody listens
    msg = str(err.value)
    assert "proxy" in msg.lower()
    assert "direct" not in msg.lower() or "blocked" not in msg.lower()


def test_wrong_protocol_is_detected() -> None:
    # Start a plain TCP listener that only accepts connections and closes them
    # immediately after the SOCKS5 greeting: real SOCKS should fail cleanly.
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(4)
    port = server.getsockname()[1]

    stop = threading.Event()

    def serve():
        server.settimeout(0.5)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
            except (TimeoutError, OSError):
                continue
            try:
                conn.recv(16)  # read the greeting, then ignore it
            finally:
                conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    try:
        with pytest.raises(ProxyError):
            run_proxy_diagnostics(make_cfg(port=port, ptype="socks5"), timeout=2.0)
    finally:
        stop.set()
        server.close()
        t.join(timeout=2)


def test_telethon_transport_never_direct(tmp_path):

    from tg_signal_ingest.telethon import build_client

    cfg = make_cfg()
    client = build_client(cfg)
    assert client._connection is not None
    assert client._proxy is not None  # Telethon always gets the explicit proxy
    _proxy_type, host, port = client._proxy[0], client._proxy[1], client._proxy[2]
    assert host == "127.0.0.1"
    assert port == 1  # config value, not a direct-connection default
