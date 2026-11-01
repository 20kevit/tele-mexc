"""Telethon client construction and authentication helpers.

Telethon opens its own MTProto socket directly to Telegram. It does not use
HTTP(S) proxy environment variables, and this project passes no proxy
configuration to it.
"""

from __future__ import annotations

import contextlib
import time
from getpass import getpass
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ApiIdInvalidError,
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeEmptyError,
    PhoneCodeExpiredError,
    PhoneCodeHashEmptyError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.errors.rpcerrorlist import UserDeactivatedError  # noqa: F401 (listed for docs)

from .config import PROJECT_ROOT, AppConfig, ConfigError
from .connectivity import ConnectivityError

MAX_CODE_ATTEMPTS = 5
CODE_RETRY_DELAY_S = 1.0


def _delivery_label(sent: object) -> str:
    """Return a safe delivery-type label; never include the code itself."""

    name = type(getattr(sent, "type", None)).__name__
    labels = {
        "SentCodeTypeApp": "Telegram app (official service chat)",
        "SentCodeTypeSms": "SMS",
        "SentCodeTypeCall": "phone call",
        "SentCodeTypeFlashCall": "flash call",
        "SentCodeTypeMissedCall": "missed call",
        "SentCodeTypeEmailCode": "email",
        "SentCodeTypeFragmentSms": "Fragment SMS",
        "SentCodeTypeFirebaseSms": "Firebase SMS (official mobile apps only)",
    }
    return labels.get(name, name or "unknown")


def resolve_session_file(cfg: AppConfig) -> tuple[str, Path]:
    """Return (telethon session name, concrete .session path), 0700 on parent."""

    base = Path(cfg.session_path)
    session = base if base.is_absolute() else (PROJECT_ROOT / base)
    if session.suffix != ".session":
        session = session.with_suffix(".session")
    parent = session.parent
    parent.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):  # pragma: no cover
        parent.chmod(0o700)
    return str(session.with_suffix("")), session


def build_client(cfg: AppConfig) -> TelegramClient:
    if cfg.api_id is None or cfg.api_hash is None:
        raise ConfigError("Telegram API credentials are missing (see docs/OPERATIONS.md).")
    session_name, _session_file = resolve_session_file(cfg)
    return TelegramClient(
        session_name,
        cfg.api_id,
        cfg.api_hash,
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
    """Connect directly to Telegram and verify the stored session is valid."""

    client = client or build_client(cfg)
    try:
        await client.connect()
    except (ConnectionError, OSError) as exc:
        raise ConnectivityError(
            f"Direct Telegram MTProto connection failed ({exc.__class__.__name__}). "
            "Run 'tg-ingest diagnostics network' and verify outbound TCP 443."
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

    The phone number, code, and 2FA password are read from the operator's
    terminal and never logged or persisted. Telegram invalidates login codes
    that are sent into a Telegram chat, so this flow deliberately never accepts
    a code through the control bot or any other Telegram conversation.
    """

    client = client or build_client(cfg)
    try:
        await client.connect()
    except (ConnectionError, OSError) as exc:
        raise ConnectivityError(
            "Could not reach Telegram directly for login "
            f"({exc.__class__.__name__}). Run 'tg-ingest diagnostics network' first."
        ) from exc

    try:
        if await client.is_user_authorized():
            me = await client.get_me()
            secure_session_files(cfg)
            return (
                f"Session already valid for {_print_account_label(me)}; nothing to do. "
                f"Session file: {cfg.session_path} .session"
            )

        print(
            "SECURITY: enter the phone number, code, and 2FA password only in this "
            "terminal. Never send a login code to Telegram, a bot, email, or another "
            "chat. Telegram invalidates codes shared in a Telegram chat."
        )
        try:
            phone = input("Phone number with country code (e.g. +15550101337; not logged): ").strip()
        except (EOFError, KeyboardInterrupt):
            raise AuthRequired("Login cancelled before a code was requested; no session was changed.") from None
        if not phone.startswith("+"):
            raise AuthRequired("Phone number must start with your country code, e.g. +15550101337.")

        try:
            sent = await client.send_code_request(phone)
        except UserDeactivatedError as exc:
            raise AuthRequired("Telegram reported this account as deactivated.") from exc
        except ApiIdInvalidError as exc:
            raise AuthRequired(
                "Telegram rejected the API credentials (api_id/api_hash pair invalid).\n"
                "  1. Compare with https://my.telegram.org -> API development tools:\n"
                "     run 'tg-ingest config check' and match api_id and api_hash first/last characters.\n"
                "  2. Copy the values again (no quotes, no spaces) into .env, then retry.\n"
                "  3. Apps created minutes ago may need a few minutes to propagate - retry later.\n"
                "  4. If it persists, recreate the app and copy the new pair."
            ) from exc
        except FloodWaitError as exc:
            raise AuthRequired(
                f"Telegram rate-limited login requests; wait {exc.seconds} seconds and try again."
            ) from exc

        code_hash = getattr(sent, "phone_code_hash", None)
        if not code_hash:
            raise AuthRequired(
                "Telegram did not return a phone_code_hash (PHONE_CODE_HASH_EMPTY); "
                "login aborted without a second request."
            )
        print(f"A login code was requested. Delivery type: {_delivery_label(sent)}.")
        print(
            "Only a code delivered by Telegram/SMS/call for this request is valid. "
            "Do not forward or retype it into any Telegram chat."
        )

        for attempt in range(MAX_CODE_ATTEMPTS):
            try:
                code = getpass("Login code (input hidden): ").strip()
            except (EOFError, KeyboardInterrupt):
                raise AuthRequired(
                    "Login cancelled; no code was submitted and the existing session is unchanged."
                ) from None
            if not code:
                raise AuthRequired("Empty code; login cancelled without submitting anything.")
            try:
                # Always use the hash returned by this exact send_code_request.
                # Never retry with a stale hash from an older challenge.
                await client.sign_in(phone=phone, code=code, phone_code_hash=code_hash)
                break
            except SessionPasswordNeededError:
                try:
                    password = getpass("Two-step verification password (input hidden): ")
                except (EOFError, KeyboardInterrupt):
                    raise AuthRequired("Login cancelled at the 2FA step.") from None
                if not password:
                    raise AuthRequired("Empty 2FA password; login cancelled.") from None
                try:
                    await client.sign_in(password=password)
                except PasswordHashInvalidError:
                    raise AuthRequired(
                        "Telegram rejected the 2FA password (PASSWORD_HASH_INVALID); login aborted. No code was stored."
                    ) from None
                except FloodWaitError as exc:
                    raise AuthRequired(f"Telegram rate-limited 2FA attempts; wait {exc.seconds} seconds.") from None
                break
            except PhoneCodeExpiredError:
                # Telegram also uses this category when it invalidates a code
                # because it was shared in a Telegram chat. Do not request a
                # fresh code automatically or retry the same challenge.
                raise AuthRequired(
                    "Telegram rejected the code category PHONE_CODE_EXPIRED. This can mean "
                    "the code expired, was already used, or was invalidated because it was "
                    "shared in a Telegram chat. Do not send codes to chats. Wait before "
                    "starting a new login; no second code was requested."
                ) from None
            except PhoneCodeHashEmptyError:
                raise AuthRequired(
                    "The login challenge is stale (PHONE_CODE_HASH_EMPTY). Start a new "
                    "login only when you are ready; no second code was requested."
                ) from None
            except PhoneCodeInvalidError:
                print(
                    "Telegram rejected the code category PHONE_CODE_INVALID. "
                    "Retry in this terminal if the code is still current."
                )
                if attempt + 1 < MAX_CODE_ATTEMPTS:
                    time.sleep(CODE_RETRY_DELAY_S)
                continue
            except PhoneCodeEmptyError:
                print("Telegram rejected an empty code (PHONE_CODE_EMPTY); retry.")
                if attempt + 1 < MAX_CODE_ATTEMPTS:
                    time.sleep(CODE_RETRY_DELAY_S)
                continue
            except PhoneNumberInvalidError:
                raise AuthRequired(
                    "Telegram rejected the phone number (PHONE_NUMBER_INVALID); no code was stored."
                ) from None
            except FloodWaitError as exc:
                raise AuthRequired(f"Telegram rate-limited code attempts; wait {exc.seconds} seconds.") from None
        else:
            raise AuthRequired("Login not completed after the allowed code attempts; no new code was requested.")

        secure_session_files(cfg)
        if not await client.is_user_authorized():
            raise AuthRequired("Login not completed; no new code was requested.")

        me = await client.get_me()
        secure_session_files(cfg)
        return f"Login succeeded for {_print_account_label(me)}. Session persisted at {cfg.session_path} .session"
    finally:
        with contextlib.suppress(Exception):
            await client.disconnect()
        secure_session_files(cfg)


async def logout_and_delete_session(cfg: AppConfig, client: TelegramClient | None = None) -> str:
    """Revoke the server-side session and delete the local session file."""

    client = client or build_client(cfg)
    outcome = "local session files removed (server revocation not confirmed)"
    try:
        await client.connect()
        if await client.is_user_authorized() and await client.log_out():
            outcome = "session revoked on Telegram servers (log_out) and local files removed"
    except (ConnectivityError, ConnectionError, OSError):
        pass  # still delete local files below
    finally:
        with contextlib.suppress(Exception):
            await client.disconnect()
    _, session_file = resolve_session_file(cfg)
    for suffix in ("", "-journal"):
        candidate = Path(str(session_file) + suffix)
        if candidate.exists():
            candidate.unlink()
    return outcome
