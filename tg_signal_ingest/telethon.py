"""Telethon client construction, proxy wiring, and authentication helpers.

The proxy is passed explicitly to Telethon (its dedicated MTProto socket does
not honour HTTP_PROXY/HTTPS_PROXY). There is no silent direct fallback: if the
proxy is misconfigured the client fails clearly.
"""

from __future__ import annotations

from getpass import getpass
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    PhoneCodeEmptyError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.errors.rpcerrorlist import UserDeactivatedError  # noqa: F401 (listed for docs)

from .config import AppConfig, ConfigError, PROJECT_ROOT
from .proxy import ProxyError, telethon_proxy_arg

_AUTH_CODE_ERROR_TYPES = (PhoneCodeInvalidError, PhoneCodeExpiredError, PhoneCodeEmptyError)
MAX_CODE_ATTEMPTS = 5


def resolve_session_file(cfg: AppConfig) -> tuple[str, Path]:
    """Return (telethon session name, concrete .session path), 0700 on parent."""

    base = Path(cfg.session_path)
    session = base if base.is_absolute() else (PROJECT_ROOT / base)
    if session.suffix != ".session":
        session = session.with_suffix(".session")
    parent = session.parent
    parent.mkdir(parents=True, exist_ok=True)
    try:
        parent.chmod(0o700)
    except OSError:  # pragma: no cover
        pass
    return str(session.with_suffix("")), session


def build_client(cfg: AppConfig) -> TelegramClient:
    if cfg.api_id is None or cfg.api_hash is None:
        raise ConfigError("Telegram API credentials are missing (see docs/OPERATIONS.md).")
    session_name, _session_file = resolve_session_file(cfg)
    proxy = telethon_proxy_arg(cfg.proxy)
    return TelegramClient(
        session_name,
        cfg.api_id,
        cfg.api_hash,
        proxy=proxy,
        device_model="tg-signal-ingest",
        system_version="linux",
        app_version="0.1.0",
        use_ipv6=False,
    )


def secure_session_files(cfg: AppConfig) -> None:
    """Tighten permissions on the session file so it cannot be world-readable."""

    _, session_file = resolve_session_file(cfg)
    try:
        if session_file.is_file():
            session_file.chmod(0o600)
    except OSError:  # pragma: no cover
        pass


class AuthRequired(Exception):
    """Stored session missing/revoked; interactive login by the owner is needed."""


async def connect_and_verify(cfg: AppConfig, client: TelegramClient | None = None) -> TelegramClient:
    """Connect through the proxy and verify the stored session is valid."""

    client = client or build_client(cfg)
    try:
        await client.connect()
    except ConnectionError as exc:
        raise ProxyError(
            f"Telegram MTProto connection through proxy failed ({exc.__class__.__name__})."
        ) from exc
    if not await client.is_user_authorized():
        await client.disconnect()
        raise AuthRequired(
            "No valid Telegram session. The account owner must run 'tg-ingest auth login' "
            "interactively (run it again if the session was revoked)."
        )
    secure_session_files(cfg)
    return client


def _print_account_label(me: object) -> str:
    first = getattr(me, "first_name", None) or ""
    last = getattr(me, "last_name", None) or ""
    username = getattr(me, "username", None)
    label = (first + " " + last).strip() or "unknown"
    return f"{label}" + (f" (@{username})" if username else "")


async def interactive_login(cfg: AppConfig, client: TelegramClient | None = None) -> str:
    """Fully interactive first-time login driven by the account owner.

    Never logs or stores the phone number, code, or 2FA password.
    """

    client = client or build_client(cfg)
    try:
        await client.connect()
    except ConnectionError as exc:
        raise ProxyError(
            "Could not reach Telegram through the configured proxy for login "
            f"({exc.__class__.__name__}). Run 'tg-ingest diagnostics proxy' first."
        ) from exc

    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            secure_session_files(cfg)
            return (
                f"Session already valid for {_print_account_label(me)}; nothing to do. "
                f"Session file: {cfg.session_path} .session"
            )

        phone = input("Phone number with country code (e.g. +15550101337): ").strip()
        if not phone.startswith("+"):
            raise AuthRequired("Phone number must start with your country code, e.g. +15550101337.")

        try:
            await client.send_code_request(phone)
        except UserDeactivatedError as exc:
            raise AuthRequired("Telegram reported this account as deactivated.") from exc
        print("A login code was sent to the account owner via Telegram.")

        for _attempt in range(MAX_CODE_ATTEMPTS):
            code = input("Login code: ").strip()
            try:
                await client.sign_in(phone=phone, code=code)
                break
            except SessionPasswordNeededError:
                password = getpass("Two-step verification password: ")
                try:
                    await client.sign_in(password=password)
                except Exception:
                    await client.disconnect()
                    raise AuthRequired(
                        "Two-step verification password rejected; login aborted. "
                        "Run 'tg-ingest auth login' again."
                    ) from None
                break
            except _AUTH_CODE_ERROR_TYPES:
                print("Code rejected (invalid or expired). Try again.")

        secure_session_files(cfg)
        if not await client.is_user_authorized():
            await client.disconnect()
            raise AuthRequired("Login not completed (codes rejected too many times or aborted).")

        me = await client.get_me()
        secure_session_files(cfg)
        return f"Login succeeded for {_print_account_label(me)}. Session persisted at {cfg.session_path} .session"
    finally:
        secure_session_files(cfg)


async def logout_and_delete_session(cfg: AppConfig, client: TelegramClient | None = None) -> str:
    """Revoke the server-side session and delete the local session file."""

    client = client or build_client(cfg)
    outcome = "local session files removed (server revocation not confirmed)"
    try:
        await client.connect()
        if client.is_user_authorized():
            if await client.log_out():
                outcome = "session revoked on Telegram servers (log_out) and local files removed"
    except (ProxyError, ConnectionError):
        pass  # still delete local files below
    finally:
        try:
            await client.disconnect()
        except Exception:  # noqa: BLE001
            pass
    _, session_file = resolve_session_file(cfg)
    for suffix in ("", "-journal"):
        candidate = Path(str(session_file) + suffix)
        if candidate.exists():
            candidate.unlink()
    return outcome
