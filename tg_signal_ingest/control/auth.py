"""Personal Telegram account login driven from the control bot.

Phone numbers, login codes, and two-step passwords exist only in memory for the
duration of the flow. They are never persisted, logged, or included in
exception text. The existing session is never logged out or replaced without
an explicit, separately confirmed owner action.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeEmptyError,
    PhoneCodeExpiredError,
    PhoneCodeHashEmptyError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
    UserDeactivatedError,
)

from ..config import AppConfig
from ..telethon import build_client, resolve_session_file, secure_session_files

PHONE_RE = r"^\+[1-9]\d{7,14}$"
MAX_CODE_ATTEMPTS = 5


class AuthFlowError(Exception):
    """Safe-to-display authentication failure."""


class AuthFlowExpired(AuthFlowError):
    """Telegram returned PHONE_CODE_EXPIRED.

    Telegram uses this category for a genuinely expired code, an already-used
    code, and a code it invalidated because it was shared into a Telegram chat.
    Keep the category visible instead of claiming a precise cause.
    """

    category = "PHONE_CODE_EXPIRED"


class AuthChallengeStale(AuthFlowError):
    """The phone_code_hash challenge cannot be used anymore."""

    category = "PHONE_CODE_HASH_EMPTY"


@dataclass
class LoginFlow:
    phone: str
    phone_code_hash: str | None
    stage: str  # code | password
    started_at: float
    attempts: int = 0
    delivery_type: str = "unknown"


@dataclass(frozen=True)
class LogoutResult:
    """Accurate outcome; never claim server revocation that did not happen."""

    server_revoked: bool
    local_removed: bool
    detail: str


class TelegramLoginManager:
    def __init__(
        self,
        cfg: AppConfig,
        *,
        client_factory: Any | None = None,
        ttl_s: int = 600,
        clock: Any | None = None,
        on_close: Callable[[], Awaitable[None] | None] | None = None,
    ):
        self.cfg = cfg
        self._client_factory = client_factory or build_client
        self._ttl_s = ttl_s
        self._clock = clock or time.monotonic
        self._client: Any | None = None
        self._flow: LoginFlow | None = None
        self._on_close = on_close
        # Serialize submissions so a duplicated update cannot race the same
        # phone_code_hash into Telegram twice.
        self._submit_lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._flow is not None and not self.expired

    @property
    def stage(self) -> str:
        return self._flow.stage if self._flow is not None else "idle"

    @property
    def started(self) -> bool:
        """True while an in-memory flow or login client exists."""

        return self._flow is not None or self._client is not None

    @property
    def delivery_type(self) -> str:
        return self._flow.delivery_type if self._flow is not None else "unknown"

    @property
    def expired(self) -> bool:
        return bool(self._flow and self._clock() - self._flow.started_at > self._ttl_s)

    async def _ensure_active(self) -> None:
        if self._flow is not None and self.expired:
            await self.reset()
            raise AuthFlowExpired("مهلت ورود به پایان رسید؛ لطفاً دوباره شروع کنید.")

    async def reset(self) -> None:
        self._flow = None
        client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):
                await client.disconnect()
        if self._on_close is not None:
            result = self._on_close()
            if inspect.isawaitable(result):
                await result

    async def probe(self) -> tuple[str, str | None]:
        """Return (status, safe account label) without modifying the session.

        status is ``valid``, ``missing`` or ``unreachable``. A network failure
        is never reported as a missing session.
        """

        client = None
        try:
            client = self._client_factory(self.cfg)
            await client.connect()
        except Exception:  # noqa: BLE001 - network details are not shown to chat
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            return "unreachable", None
        try:
            if not await client.is_user_authorized():
                return "missing", None
            me = await client.get_me()
            return "valid", _account_label(me)
        except Exception:  # noqa: BLE001
            return "unreachable", None
        finally:
            with contextlib.suppress(Exception):
                await client.disconnect()

    async def start(self, phone: str) -> str:
        phone = normalize_phone(phone)
        if not _valid_phone(phone):
            raise AuthFlowError("شماره تلفن باید با کد کشور و علامت + باشد، مثلاً +98912...")
        if self._flow is not None:
            raise AuthFlowError("یک فرآیند ورود در حال اجراست؛ ابتدا آن را لغو کنید.")
        client = self._client_factory(self.cfg)
        try:
            await client.connect()
            if await client.is_user_authorized():
                await client.disconnect()
                raise AuthFlowError("نشست تلگرام از قبل معتبر است؛ نیازی به ورود دوباره نیست.")
            sent = await client.send_code_request(phone)
        except FloodWaitError as exc:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise AuthFlowError(
                f"تلگرام درخواست ورود را محدود کرده است؛ لطفاً دقیقاً {exc.seconds} ثانیه صبر کنید."
            ) from None
        except UserDeactivatedError as exc:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise AuthFlowError("حساب تلگرام غیرفعال است.") from exc
        except AuthFlowError:
            raise
        except Exception as exc:  # noqa: BLE001
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise AuthFlowError(
                f"ارسال کد ورود ناموفق بود ({exc.__class__.__name__}). اتصال مستقیم به تلگرام را بررسی کنید."
            ) from None
        code_hash = getattr(sent, "phone_code_hash", None)
        if not code_hash:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise AuthFlowError(
                "تلگرام phone_code_hash برنگرداند؛ ورود متوقف شد. لطفاً دوباره تلاش کنید."
            )
        delivery = getattr(getattr(sent, "type", None), "__class__", type(None)).__name__
        self._client = client
        self._flow = LoginFlow(
            phone=phone,
            phone_code_hash=code_hash,
            stage="code",
            started_at=self._clock(),
            delivery_type=delivery,
        )
        return (
            "کد ورود به حساب تلگرام شما ارسال شد. آن را فقط در رابط امن خودتان "
            "وارد کنید؛ هرگز در چت تلگرام نفرستید."
        )

    async def submit_code(self, code: str) -> str:
        async with self._submit_lock:
            return await self._submit_code_locked(code)

    async def _submit_code_locked(self, code: str) -> str:
        await self._ensure_active()
        flow = self._flow
        if flow is None or flow.stage != "code" or self._client is None:
            raise AuthFlowError("فرآیند ورود فعال نیست؛ از /start شروع کنید.")
        code = code.strip()
        if not code:
            raise AuthFlowError("کد ورود خالی است.")
        flow.attempts += 1
        if flow.attempts > MAX_CODE_ATTEMPTS:
            await self.reset()
            raise AuthFlowError("تعداد تلاش‌ها بیش از حد مجاز شد؛ فرآیند ورود لغو شد.")
        try:
            await self._client.sign_in(
                phone=flow.phone, code=code, phone_code_hash=flow.phone_code_hash
            )
        except SessionPasswordNeededError:
            flow.stage = "password"
            return "این حساب رمز تأیید دو مرحله‌ای دارد. لطفاً رمز را بفرستید."
        except PhoneCodeExpiredError:
            await self.reset()
            raise AuthFlowExpired(
                "تلگرام کد را رد کرد (PHONE_CODE_EXPIRED). این دسته می‌تواند "
                "کد منقضی، کد قبلاً استفاده‌شده، یا کدی باشد که به دلیل ارسال "
                "در چت تلگرام باطل شده است. کد را در چت نفرستید."
            ) from None
        except PhoneCodeHashEmptyError:
            await self.reset()
            raise AuthChallengeStale(
                "چالش کد ورود دیگر معتبر نیست (PHONE_CODE_HASH_EMPTY)؛ "
                "برای دریافت چالش تازه، ورود را از ترمینال دوباره آغاز کنید."
            ) from None
        except PhoneCodeInvalidError:
            raise AuthFlowError(
                "تلگرام کد را رد کرد (PHONE_CODE_INVALID)؛ کد نادرست است."
            ) from None
        except PhoneCodeEmptyError:
            raise AuthFlowError(
                "تلگرام کد خالی را رد کرد (PHONE_CODE_EMPTY)."
            ) from None
        except PhoneNumberInvalidError:
            raise AuthFlowError(
                "تلگرام شماره را رد کرد (PHONE_NUMBER_INVALID)؛ ورود متوقف شد."
            ) from None
        except FloodWaitError as exc:
            raise AuthFlowError(
                f"تلگرام محدودیت اعمال کرده است؛ دقیقاً {exc.seconds} ثانیه صبر کنید."
            ) from None
        except Exception as exc:  # noqa: BLE001 - never leak code or transport details
            if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
                raise AuthFlowError(
                    "ارتباط مستقیم با تلگرام برقرار نشد؛ شبکه خروجی را بررسی کنید."
                ) from None
            raise AuthFlowError(
                f"ورود کامل نشد ({exc.__class__.__name__}); دوباره تلاش کنید."
            ) from None
        return await self._finish()

    async def submit_password(self, password: str) -> str:
        async with self._submit_lock:
            return await self._submit_password_locked(password)

    async def _submit_password_locked(self, password: str) -> str:
        await self._ensure_active()
        flow = self._flow
        if flow is None or flow.stage != "password" or self._client is None:
            raise AuthFlowError("فرآیند ورود فعال نیست؛ از /start شروع کنید.")
        if not password:
            raise AuthFlowError("رمز تأیید دو مرحله‌ای خالی است.")
        try:
            await self._client.sign_in(password=password)
        except FloodWaitError as exc:
            raise AuthFlowError(
                f"تلگرام محدودیت اعمال کرده است؛ دقیقاً {exc.seconds} ثانیه صبر کنید."
            ) from None
        except PasswordHashInvalidError:
            flow.attempts += 1
            if flow.attempts > MAX_CODE_ATTEMPTS:
                await self.reset()
                raise AuthFlowError("رمز تأیید دو مرحله‌ای رد شد؛ فرآیند ورود لغو شد.") from None
            raise AuthFlowError("رمز تأیید دو مرحله‌ای نادرست است؛ دوباره تلاش کنید.") from None
        except Exception as exc:  # noqa: BLE001 - never include the password in the error
            if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
                raise AuthFlowError(
                    "ارتباط مستقیم با تلگرام برقرار نشد؛ دوباره تلاش کنید."
                ) from None
            raise AuthFlowError(
                f"تأیید دو مرحله‌ای کامل نشد ({exc.__class__.__name__}); دوباره تلاش کنید."
            ) from None
        return await self._finish()

    async def _finish(self) -> str:
        assert self._client is not None
        if not await self._client.is_user_authorized():
            raise AuthFlowError("ورود کامل نشد؛ لطفاً دوباره تلاش کنید.")
        me = await self._client.get_me()
        label = _account_label(me)
        secure_session_files(self.cfg)
        await self.reset()
        return label

    async def logout(self) -> LogoutResult:
        """Attempt a real server-side Telegram logout.

        Local session files are removed only when Telegram confirms the logout,
        or when the stored session is already unauthenticated. A network or
        server failure leaves the session untouched and reports failure, so the
        operator can retry rather than silently losing state.
        """

        client = None
        was_authorized = False
        server_revoked = False
        try:
            client = self._client_factory(self.cfg)
            await client.connect()
            was_authorized = bool(await client.is_user_authorized())
            if not was_authorized:
                return _remove_local_session(
                    self.cfg,
                    LogoutResult(
                        server_revoked=False,
                        local_removed=True,
                        detail="نشست محلی از قبل معتبر نبود؛ فایل‌های نشست پاک شدند.",
                    ),
                )
            server_revoked = bool(await client.log_out())
        except Exception as exc:  # noqa: BLE001 - no transport details or secrets
            return LogoutResult(
                server_revoked=False,
                local_removed=False,
                detail=(
                    "خروج از سرور تلگرام انجام نشد "
                    f"({exc.__class__.__name__}); نشست محلی حذف نشد و دست‌نخورده است."
                ),
            )
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
        if not server_revoked:
            return LogoutResult(
                server_revoked=False,
                local_removed=False,
                detail=(
                    "تلگرام تأیید نکرد که نشست باطل شده است؛ فایل محلی حذف نشد. "
                    "دوباره تلاش کنید."
                ),
            )
        return _remove_local_session(
            self.cfg,
            LogoutResult(
                server_revoked=True,
                local_removed=True,
                detail="نشست در سرور تلگرام باطل و فایل‌های نشست محلی حذف شدند.",
            ),
        )


def _remove_local_session(cfg: AppConfig, result: LogoutResult) -> LogoutResult:
    _, session_file = resolve_session_file(cfg)
    removed_any = False
    for suffix in ("", "-journal", "-wal", "-shm"):
        candidate = Path(str(session_file) + suffix)
        try:
            if candidate.exists():
                candidate.unlink()
                removed_any = True
        except OSError:
            return LogoutResult(
                server_revoked=result.server_revoked,
                local_removed=False,
                detail=(
                    "وضعیت سرور به‌روزرسانی شد، اما حذف کامل فایل نشست محلی ممکن نشد؛ "
                    "دسترسی فایل را بررسی کنید."
                ),
            )
    return LogoutResult(
        server_revoked=result.server_revoked,
        local_removed=removed_any or result.local_removed,
        detail=result.detail,
    )


def normalize_phone(phone: str) -> str:
    """Normalize common manual formats without guessing a country code."""

    raw = (phone or "").strip()
    raw = re.sub(r"[\s\-()./]", "", raw)
    if raw.startswith("00"):
        raw = "+" + raw[2:]
    if raw and not raw.startswith("+"):
        raw = "+" + raw
    return raw


def _valid_phone(phone: str) -> bool:
    return bool(re.fullmatch(PHONE_RE, phone))


def _account_label(me: Any) -> str:
    first = getattr(me, "first_name", None) or ""
    last = getattr(me, "last_name", None) or ""
    username = getattr(me, "username", None)
    label = (first + " " + last).strip() or "حساب تلگرام"
    return f"{label} (@{username})" if username else label
