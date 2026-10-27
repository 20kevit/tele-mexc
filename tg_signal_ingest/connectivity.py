"""Direct Telegram connectivity checks.

Both Telegram transports used by this project are direct:

  * the Bot API client opens an HTTPS connection to ``api.telegram.org``;
  * Telethon opens its own MTProto TCP connection to Telegram data centres.

Neither transport reads ``HTTP_PROXY``/``HTTPS_PROXY`` or any project-specific
proxy setting. The checks here validate DNS, TCP and (for the Bot API) TLS
against Telegram endpoints. They never print credentials and never send
messages.
"""

from __future__ import annotations

import os
import socket
import ssl
from dataclasses import dataclass

from .config import LEGACY_PROXY_ENV

BOT_API_HOST = "api.telegram.org"
BOT_API_PORT = 443
DEFAULT_DC_IPS = (
    "149.154.167.51",  # DC4
    "149.154.175.53",  # DC2
    "149.154.167.50",  # DC1/3
    "91.108.56.130",
)

AMBIENT_PROXY_ENV = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)
class ConnectivityError(Exception):
    """Direct network connectivity failed; the message is safe to display."""


@dataclass(frozen=True)
class EndpointCheck:
    name: str
    host: str
    port: int
    addresses: tuple[str, ...]
    tcp_ok: bool
    tls_ok: bool | None
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.tcp_ok and self.tls_ok is not False


@dataclass(frozen=True)
class DirectDiagnostics:
    bot_api: EndpointCheck
    telegram_dcs: tuple[EndpointCheck, ...]
    ambient_proxy_vars: tuple[str, ...]
    legacy_proxy_vars: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return self.bot_api.ok and any(check.tcp_ok for check in self.telegram_dcs)


def _resolve(host: str, port: int) -> tuple[str, ...]:
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ConnectivityError(
            f"DNS resolution for {host} failed ({exc.__class__.__name__})"
        ) from exc
    addresses = tuple(dict.fromkeys(str(info[4][0]) for info in infos))
    if not addresses:
        raise ConnectivityError(f"DNS resolution for {host} returned no addresses")
    return addresses


def _tcp_connect(host: str, port: int, timeout: float) -> None:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        raise ConnectivityError(
            f"TCP connect to {host}:{port} failed ({exc.__class__.__name__})"
        ) from exc


def _tls_connect(host: str, port: int, timeout: float) -> str:
    context = ssl.create_default_context()
    try:
        with (
            socket.create_connection((host, port), timeout=timeout) as raw,
            context.wrap_socket(raw, server_hostname=host) as tls,
        ):
            version = tls.version() or "TLS"
            return f"TLS handshake OK: {version}"
    except (OSError, ssl.SSLError) as exc:
        raise ConnectivityError(
            f"TLS connect to {host}:{port} failed ({exc.__class__.__name__})"
        ) from exc


def _check_endpoint(
    name: str,
    host: str,
    port: int,
    *,
    tls: bool,
    timeout: float,
) -> EndpointCheck:
    try:
        addresses = _resolve(host, port)
    except ConnectivityError as exc:
        return EndpointCheck(name, host, port, (), False, False if tls else None, str(exc))
    try:
        _tcp_connect(host, port, timeout)
    except ConnectivityError as exc:
        return EndpointCheck(name, host, port, addresses, False, False if tls else None, str(exc))
    if not tls:
        return EndpointCheck(name, host, port, addresses, True, None, "TCP connect OK")
    try:
        detail = _tls_connect(host, port, timeout)
    except ConnectivityError as exc:
        return EndpointCheck(name, host, port, addresses, True, False, str(exc))
    return EndpointCheck(name, host, port, addresses, True, True, detail)


def _present(environ: dict[str, str], names: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(name for name in names if environ.get(name))


def run_direct_diagnostics(
    environ: dict[str, str] | None = None, timeout: float = 8.0
) -> DirectDiagnostics:
    """Check Bot API and MTProto reachability over the normal direct route."""

    env = dict(os.environ if environ is None else environ)
    bot_api = _check_endpoint("Telegram Bot API", BOT_API_HOST, BOT_API_PORT, tls=True, timeout=timeout)
    dcs = tuple(
        _check_endpoint(f"Telegram DC {index}", ip, 443, tls=False, timeout=timeout)
        for index, ip in enumerate(DEFAULT_DC_IPS, start=1)
    )
    diagnostics = DirectDiagnostics(
        bot_api=bot_api,
        telegram_dcs=dcs,
        ambient_proxy_vars=_present(env, AMBIENT_PROXY_ENV),
        legacy_proxy_vars=_present(env, LEGACY_PROXY_ENV),
    )
    if not bot_api.ok:
        raise ConnectivityError(
            f"Direct Telegram Bot API check failed: {bot_api.detail}. "
            "Verify outbound TCP 443 and DNS, then retry."
        )
    if not any(check.tcp_ok for check in dcs):
        raise ConnectivityError(
            "Direct TCP connectivity to every tested Telegram data centre failed. "
            "Verify outbound TCP 443, then retry."
        )
    return diagnostics


def describe_direct_transport() -> str:
    return "direct TCP/TLS (ambient proxy variables are not used)"
