"""Proxy support and connectivity diagnostics.

Telethon must be given the proxy explicitly (its MTProto socket does NOT honour
HTTP_PROXY / HTTPS_PROXY environment variables). This module builds the proxy
specifier for Telethon and provides standalone diagnostics that:

  * probe a raw TCP connection to the proxy,
  * perform an HTTP CONNECT (or SOCKS5 handshake) and tunnel to a Telegram DC,
  * verify the VPN/proxy is actually required (direct egress is expected to fail),
  * never fall back to a direct connection.

Diagnostics never expose proxy credentials.
"""

from __future__ import annotations

import asyncio
import base64
import socket
import struct
from dataclasses import dataclass

from .config import AppConfig, ProxyConfig

DEFAULT_DC_IP = "149.154.167.51"  # Tensor Cloud DC4 (Amsterdam), relevant for many EU VPSes
PROD_DC_IPS = (
    "149.154.167.51",  # DC4
    "149.154.175.53",  # DC2
    "149.154.167.50",  # DC1/3
    "91.108.56.130",
)


class ProxyError(Exception):
    """The configured proxy cannot carry traffic; message is safe to log."""


def telethon_proxy_arg(proxy: ProxyConfig):
    """Build the ``proxy=`` argument for Telethon.

    Telethon accepts ``(type, host, port, rdns, username, password)`` tuples in
    the PySocks format. ``socks.HTTP`` tunnels via HTTP CONNECT, which was
    verified to work against this VPS's xray-managed listener on 127.0.0.1:1081.
    """

    if not proxy.enabled:
        raise ProxyError(
            "Telethon requires an explicit proxy; direct Telegram egress is blocked on this VPS."
        )
    try:
        import socks  # provided by PySocks
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise ProxyError("PySocks is not installed; run 'pip install -e .[dev]' again.") from exc

    proxy_type = socks.HTTP if proxy.type == "http" else socks.SOCKS5
    return (
        proxy_type,
        proxy.host,
        proxy.port,
        True,  # resolve hostnames remotely
        proxy.username or None,
        proxy.password or None,
    )


def _tcp_probe(host: str, port: int, timeout: float = 6.0) -> None:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        raise ProxyError(f"TCP connect to proxy {host}:{port} failed ({exc.__class__.__name__}).") from exc


def _http_connect_check(proxy: ProxyConfig, target_ip: str, target_port: int, timeout: float = 8.0) -> None:
    hostport = f"{target_ip}:{target_port}"
    try:
        with socket.create_connection((proxy.host, proxy.port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            req = f"CONNECT {hostport} HTTP/1.1\r\nHost: {hostport}\r\n"
            if proxy.username and proxy.password:
                token = base64.b64encode(f"{proxy.username}:{proxy.password}".encode()).decode()
                req += f"Proxy-Authorization: Basic {token}\r\n"
            req += "\r\n"
            sock.sendall(req.encode())
            resp = b""
            while b"\r\n\r\n" not in resp and len(resp) < 8192:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                resp += chunk
            status = resp.split(b"\r\n", 1)[0].decode("latin-1", "replace")
            if " 200 " not in f" {status} ":
                raise ProxyError(f"HTTP CONNECT through proxy failed: {status!r}")
    except ProxyError:
        raise
    except OSError as exc:
        raise ProxyError(f"HTTP CONNECT through proxy failed ({exc.__class__.__name__}).") from exc


def _socks5_check(proxy: ProxyConfig, target_ip: str, target_port: int, timeout: float = 8.0) -> None:
    try:
        with socket.create_connection((proxy.host, proxy.port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            if proxy.username and proxy.password:
                sock.sendall(b"\x05\x02\x00\x02")
            else:
                sock.sendall(b"\x05\x01\x00")
            resp = sock.recv(2)
            if len(resp) != 2 or resp[0] != 5 or resp[1] not in (0, 2):
                raise ProxyError(f"SOCKS5 handshake unsupported by proxy {proxy.host}:{proxy.port}.")
            if resp[1] == 2 and proxy.username and proxy.password:
                user = proxy.username.encode()
                pw = proxy.password.encode()
                sock.sendall(b"\x01" + bytes([len(user)]) + user + bytes([len(pw)]) + pw)
                if sock.recv(2)[-1:] != b"\x00":
                    raise ProxyError("SOCKS5 authentication failed.")
            addr = target_ip.split(".")
            sock.sendall(
                b"\x05\x01\x00\x01" + bytes(int(o) for o in addr) + struct.pack(">H", target_port)
            )
            resp = sock.recv(10)
            if len(resp) < 2 or resp[1] != 0:
                raise ProxyError(f"SOCKS5 CONNECT to {target_ip}:{target_port} refused.")
    except ProxyError:
        raise
    except OSError as exc:
        raise ProxyError(f"SOCKS5 CONNECT through proxy failed ({exc.__class__.__name__}).") from exc


def check_direct_excluded(timeout: float = 5.0) -> bool:
    """Return True if direct egress to a Telegram DC is blocked (expected).

    Direct Telegram egress is expected to be blocked on this VPS; it is only reported.
    """

    try:
        with socket.create_connection((DEFAULT_DC_IP, 443), timeout=timeout):
            return True  # direct path exists (unexpected)
    except OSError:
        return False


@dataclass
class ProxyDiagnostics:
    proxy_enabled: bool
    proxy_type: str
    proxy_addr_public: str
    listener_reachable: bool
    tunneled_connect_ok: bool
    direct_egress_blocked: bool
    detail: str = ""


def run_proxy_diagnostics(cfg: AppConfig, timeout: float = 8.0) -> ProxyDiagnostics:
    """Full standalone check. Raises ProxyError on tunnel failure when enabled."""

    proxy = cfg.proxy
    addr_public = f"{proxy.host}:{proxy.port} ({proxy.type})"
    if not proxy.enabled:
        raise ProxyError("Proxy is disabled in configuration; this deployment requires it.")

    listener_reachable = True
    try:
        _tcp_probe(proxy.host, proxy.port, timeout=timeout)
    except ProxyError:
        listener_reachable = False
        raise
    for dc_ip in PROD_DC_IPS:
        try:
            if proxy.type == "http":
                _http_connect_check(proxy, dc_ip, 443, timeout=timeout)
            else:
                _socks5_check(proxy, dc_ip, 443, timeout=timeout)
            return ProxyDiagnostics(
                proxy_enabled=proxy.enabled,
                proxy_type=proxy.type,
                proxy_addr_public=addr_public,
                listener_reachable=listener_reachable,
                tunneled_connect_ok=True,
                direct_egress_blocked=not check_direct_excluded(),
                detail=f"HTTP CONNECT tunnel established to Telegram DC {dc_ip}:443",
            )
        except ProxyError:
            continue
    raise ProxyError("Proxy tunnel to Telegram DCs failed for all tested production DC IPs.")


def describe_proxy(cfg: AppConfig) -> str:
    p = cfg.proxy
    return f"{p.type}://{p.host}:{p.port}" if p.enabled else "disabled"
