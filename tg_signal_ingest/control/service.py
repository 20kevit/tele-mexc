"""Single-owner Persian control bot for the ingestion/trading process.

The bot manages authentication, channel setup, sizing, notifications,
and administrative pause/resume. Trading decisions remain in the existing
intent, risk, balance-preflight, execution, and reconciliation pipeline.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from datetime import UTC, datetime
from typing import Any

from ..config import AppConfig
from ..mexc import MexcSpotClient, SharedWeightedRateGate, SymbolRegistry
from ..repository import Repository
from ..trading.gate import GateTimeout, SubmissionGate
from ..version import format_version_text, get_version_info
from .auth import AuthFlowError, TelegramLoginManager
from .bot_api import BotApiClient, BotApiError
from .channels import ChannelChoice, fetch_channels, find_channel
from .mexc_setup import verify_mexc_client
from .secretbox import SecretBox, SecretBoxError
from .store import ControlStore

log = logging.getLogger("ingest.control")


def _kb(rows: list[list[dict[str, str]]]) -> dict[str, Any]:
    return {"inline_keyboard": rows}


def _btn(text: str, data: str) -> dict[str, str]:
    return {"text": text, "callback_data": data}


def _from_id(payload: dict[str, Any]) -> int | None:
    sender = payload.get("from") or {}
    value = sender.get("id")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _age_s(value: str | None, now: datetime) -> float | None:
    parsed = _parse_ts(value)
    return (now - parsed).total_seconds() if parsed else None


def _fmt(value: Any) -> str:
    return "—" if value in (None, "") else str(value)


class ControlBot:
    def __init__(
        self,
        cfg: AppConfig,
        repo: Repository,
        bot: BotApiClient | Any,
        *,
        store: ControlStore | None = None,
        telethon_factory: Any | None = None,
        mexc_client_factory: Any | None = None,
        probe_override: Any | None = None,
    ):
        self.cfg = cfg
        self.repo = repo
        self.bot = bot
        self.store = store or ControlStore(repo.db)
        self.auth = TelegramLoginManager(cfg, client_factory=telethon_factory)
        self.box = SecretBox(cfg.control.secret_key)
        self._telethon_factory = telethon_factory
        self._mexc_client_factory = mexc_client_factory
        self._probe_override = probe_override
        self._engine: Any | None = None
        self._pending_mexc_key: str | None = None
        self._pending_mexc_secret: str | None = None
        self._pending_mexc_started = 0.0
        self._last_tick = 0.0
        self._load_stored_mexc()

    def _load_stored_mexc(self) -> None:
        """Restore encrypted MEXC credentials after a restart.

        Environment credentials still take precedence; the encrypted store is
        the fallback so the owner does not have to re-enter secrets each run.
        """

        if not self.box.available or (self.cfg.mexc.api_key and self.cfg.mexc.api_secret):
            return
        try:
            key = self.store.get_secret("mexc_api_key", self.box)
            secret = self.store.get_secret("mexc_api_secret", self.box)
        except SecretBoxError:
            log.warning("stored MEXC credentials could not be decrypted; owner must re-enter them")
            return
        if key and secret:
            self.cfg.mexc.api_key = key
            self.cfg.mexc.api_secret = secret
            self.store.audit("mexc_credentials_loaded", "decrypted from the local control store")

    # ------------------------------------------------------------- wiring

    def attach_engine(self, engine: Any) -> None:
        self._engine = engine

    # -------------------------------------------------------- bot transport

    async def _send(self, chat_id: int, text: str, kb: dict[str, Any] | None = None) -> dict[str, Any]:
        return await asyncio.to_thread(self.bot.send_message, chat_id, text, reply_markup=kb)

    async def _edit(self, chat_id: int, message_id: int, text: str, kb: dict[str, Any] | None = None) -> None:
        with contextlib.suppress(BotApiError, Exception):
            await asyncio.to_thread(self.bot.edit_message_text, chat_id, message_id, text, reply_markup=kb)

    async def _answer(self, callback_id: str, text: str | None = None) -> None:
        with contextlib.suppress(Exception):
            await asyncio.to_thread(self.bot.answer_callback_query, callback_id, text)

    async def _delete_sensitive(self, chat_id: int, message_id: int) -> None:
        delay = self.store.get_int_setting("sensitive_cleanup_s", 0)
        if delay:
            await asyncio.sleep(min(delay, 300))
        with contextlib.suppress(Exception):
            await asyncio.to_thread(self.bot.delete_message, chat_id, message_id)

    # ------------------------------------------------------------ updates

    async def handle_update(self, update: dict[str, Any]) -> None:
        update_id = update.get("update_id")
        if update_id is not None and not self.store.mark_update(int(update_id)):
            return
        if isinstance(update.get("callback_query"), dict):
            await self._handle_callback(update["callback_query"])
        elif isinstance(update.get("message"), dict):
            await self._handle_message(update["message"])

    async def _handle_message(self, message: dict[str, Any]) -> None:
        user_id = _from_id(message)
        chat_id = int((message.get("chat") or {}).get("id") or 0)
        if not self._authorized(user_id):
            if chat_id:
                await self._send(chat_id, "⛔ دسترسی مجاز نیست.")
            return
        if not chat_id:
            return
        text = (message.get("text") or "").strip()
        message_id = int(message.get("message_id") or 0)
        if not text:
            await self._send(chat_id, "فقط پیام متنی پشتیبانی می‌شود. /help")
            return

        # Login from chat is not supported. Discard any challenge persisted by
        # an older deployment before processing the next message.
        flow = self.store.flow()
        if flow["state"] in ("auth_phone", "auth_code", "auth_password"):
            self.store.clear_flow()
            await self.auth.reset()
            self.store.update_setup(telegram_state="unauthenticated")
            self.store.refresh_setup_state()
            self.store.audit(
                "chat_login_rejected",
                f"role=owner stale {flow['state']} input refused; terminal login required",
            )
            if not text.startswith("/"):
                await self._delete_sensitive(chat_id, message_id)
                await self._send(
                    chat_id,
                    "ورود از چت غیرفعال است. شماره، کد یا رمز را در چت نفرستید؛ "
                    "از ترمینال امن SSH استفاده کنید. /start",
                )
                return

        if text.startswith("/cancel"):
            flow = self.store.flow()
            await self.auth.reset()
            self._pending_mexc_key = None
            self._pending_mexc_secret = None
            self.store.clear_flow()
            if flow["state"] in ("auth_phone", "auth_code", "auth_password"):
                self.store.update_setup(telegram_state="unauthenticated")
            elif flow["state"] in ("mexc_key", "mexc_secret"):
                self.store.update_setup(mexc_state="not_configured")
            self.store.refresh_setup_state()
            await self._send(chat_id, "عملیات جاری لغو شد. تنظیمات و پیشرفت قبلی حفظ شده است.")
            return
        if text.startswith("/start"):
            await self._show_start(chat_id)
            return
        if text.startswith("/help"):
            await self._show_help(chat_id)
            return
        if text.startswith("/status") or text.startswith("/menu"):
            await self._show_start(chat_id)
            return
        if text.startswith("/pause"):
            await self._pause(chat_id)
            return
        if text.startswith("/resume"):
            await self._resume(chat_id)
            return
        if text.startswith("/version"):
            await self._show_version(chat_id)
            return

        flow = self.store.flow()
        if flow["expired"]:
            state = flow["state"]
            self.store.clear_flow()
            await self.auth.reset()
            self._pending_mexc_key = None
            self._pending_mexc_secret = None
            if state in ("auth_phone", "auth_code", "auth_password"):
                self.store.update_setup(telegram_state="unauthenticated")
            elif state in ("mexc_key", "mexc_secret"):
                self.store.update_setup(mexc_state="not_configured")
            self.store.refresh_setup_state()
            await self._send(chat_id, "مهلت این مرحله به پایان رسید. لطفاً از /start ادامه دهید.")
            return
        state = flow["state"]
        if state == "mexc_key":
            await self._input_mexc_key(chat_id, message_id, text)
        elif state == "mexc_secret":
            await self._input_mexc_secret(chat_id, message_id, text)
        elif state == "channel_search":
            await self._input_channel_search(chat_id, text)
        elif state == "buy_amount":
            await self._input_buy_amount(chat_id, text)
        else:
            await self._send(chat_id, "از منوی زیر انتخاب کنید یا /start را بزنید.", self._menu_kb())

    def _authorized(self, user_id: int | None) -> bool:
        """Return True for the configured owner or the configured developer.

        Both operators have owner-equivalent access to every control-bot command
        and administrative action (start/status, pause/resume, version, MEXC
        setup, channel selection, sizing, notifications, disconnect, adopt).
        Usernames are never used for authorization. The owner
        account and its permissions are preserved; the developer is added as an
        additional explicitly configured numeric Telegram user id.
        """
        if user_id is None:
            return False
        owner = self.cfg.control.owner_user_id
        developer = self.cfg.control.developer_user_id
        if user_id == owner:
            return True
        return developer is not None and user_id == developer

    async def _handle_callback(self, callback: dict[str, Any]) -> None:
        callback_id = str(callback.get("id") or "")
        user_id = _from_id(callback)
        message = callback.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or user_id or 0)
        if not self._authorized(user_id):
            await self._answer(callback_id, "دسترسی مجاز نیست")
            return
        data = str(callback.get("data") or "")
        await self._answer(callback_id)
        try:
            await self._route_callback(chat_id, data)
        except Exception as exc:  # noqa: BLE001 - callbacks must never crash polling
            log.warning("control callback failed (%s)", exc.__class__.__name__)
            await self._send(chat_id, "انجام این عملیات ممکن نشد. جزئیات حساس در لاگ ثبت نمی‌شود.")

    async def _route_callback(self, chat_id: int, data: str) -> None:
        parts = data.split(":")
        head = parts[0] if parts else ""
        if head == "m":
            await self._menu_action(chat_id, parts[1] if len(parts) > 1 else "")
        elif head == "ch":
            await self._channel_action(chat_id, parts)
        elif head == "au":
            await self._auth_action(chat_id, parts)
        elif head == "mx":
            await self._mexc_action(chat_id, parts)
        elif head == "n":
            await self._notification_action(chat_id, parts)
        else:
            await self._send(chat_id, "دکمه نامعتبر است. /start")

    # --------------------------------------------------------------- menus

    def _menu_kb(self) -> dict[str, Any]:
        s = self.store.setup()
        state = s["effective_state"]
        rows: list[list[dict[str, str]]] = []
        if state in ("not_configured", "telegram_auth_pending"):
            rows.append([_btn("🔐 راهنمای ورود امن (ترمینال)", "m:auth")])
        else:
            if s["mexc_state"] != "verified":
                rows.append([_btn("🔑 تنظیم و تأیید MEXC", "m:mexc")])
            if not s.get("selected_channel_id"):
                rows.append([_btn("📡 انتخاب کانال سیگنال", "m:channel")])
            else:
                rows.append([_btn("📡 کانال / تغییر کانال", "m:channel")])
            rows.append([_btn("📏 اندازه سفارش", "m:sizing")])
            rows.append([_btn("🔔 تنظیمات اعلان‌ها", "n:settings")])
            if int(s.get("paused") or 0):
                rows.append([_btn("▶️ ادامه اجرا", "m:resume")])
            else:
                rows.append([_btn("⏸ توقف اجرای سفارش", "m:pause")])
        rows.append([_btn("📊 وضعیت", "m:status"), _btn("❓ راهنما", "m:help")])
        return _kb(rows)

    async def _show_start(self, chat_id: int) -> None:
        s = self.store.setup()
        status, label = await self._probe_session()
        discrepancy = self._discrepancy(s, status)
        text = self._status_text(s, status, label, discrepancy)
        await self._send(chat_id, text + "\n\n" + self._resume_text(s, status, discrepancy), self._menu_kb())

    async def _probe_session(self) -> tuple[str, str | None]:
        if self._probe_override is not None:
            result = self._probe_override()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        return await self.auth.probe()

    @staticmethod
    def _discrepancy(setup: dict[str, Any], status: str) -> str | None:
        auth_state = setup.get("telegram_state")
        if status == "valid" and auth_state != "authenticated":
            return "session_valid_db_unauthenticated"
        if status == "missing" and auth_state == "authenticated":
            return "db_authenticated_session_missing"
        return None

    def _status_text(
        self,
        s: dict[str, Any],
        probe_status: str,
        label: str | None,
        discrepancy: str | None,
    ) -> str:
        channel = s.get("selected_channel_title") or "انتخاب نشده"
        auth_word = (
            "متصل" if probe_status == "valid" else ("نامشخص" if probe_status == "unreachable" else "نیاز به ورود")
        )
        lines = [
            "📊 وضعیت سیستم",
            f"• تلگرام: {auth_word}" + (f" — {label}" if label else ""),
            f"• MEXC: {'تأییدشده' if s['mexc_state'] == 'verified' else 'تنظیم نشده'}",
            f"• کانال: {channel}",
            f"• وضعیت اجرا: {'متوقف' if int(s.get('paused') or 0) else 'فعال'}",
        ]
        if discrepancy == "session_valid_db_unauthenticated":
            lines += [
                "",
                "⚠️ ناسازگاری: نشست تلگرام معتبر است اما در وضعیت ذخیره‌شده ثبت نشده.",
                "هیچ تغییری خودکار اعمال نشد. برای پذیرش این نشست موجود، دکمه «پذیرش نشست» را بزنید.",
            ]
        elif discrepancy == "db_authenticated_session_missing":
            lines += [
                "",
                "⚠️ ناسازگاری: وضعیت ذخیره‌شده می‌گوید تلگرام متصل است، اما نشست معتبر پیدا نشد.",
                "نشست فعلی حذف یا جایگزین نمی‌شود. برای ترمیم، دکمه «ترمیم ورود» را بزنید.",
            ]
        return "\n".join(lines)

    def _resume_text(self, s: dict[str, Any], status: str, discrepancy: str | None) -> str:
        if discrepancy == "session_valid_db_unauthenticated":
            return "برای ادامه، نشست موجود را به‌صورت صریح بپذیرید."
        if discrepancy == "db_authenticated_session_missing":
            return "برای ادامه، ورود تلگرام را ترمیم کنید."
        state = s["effective_state"]
        if state == "not_configured":
            return "برای شروع، ورود حساب تلگرام را از ترمینال امن SSH انجام دهید (.venv/bin/tg-ingest auth login)."
        if state == "telegram_auth_pending":
            return "ورود نیمه‌کاره ذخیره شده است؛ ورود از چت غیرفعال است. از ترمینال امن SSH استفاده کنید."
        if state == "telegram_authenticated":
            return "تلگرام متصل است. اکنون MEXC را تنظیم و تأیید کنید."
        if state == "mexc_config_pending":
            return "تنظیم MEXC نیمه‌کاره است؛ آن را ادامه دهید."
        if state == "channel_selection_pending":
            return "کلید MEXC تأیید شد. کانال سیگنال را انتخاب کنید."
        if state == "channel_selected":
            return "کانال انتخاب شد؛ پردازش خودکار سیگنال‌های معتبر از این پیام‌های جدید فعال است."
        if state == "ready":
            return "همه‌چیز آماده است؛ سیگنال‌های معتبر به‌صورت خودکار پردازش می‌شوند."
        if state == "automatic":
            return "اجرا آماده است. برای توقف اجرای سفارش‌های جدید از /pause استفاده کنید."
        if state == "paused":
            return "اجرای سفارش متوقف است. تنظیمات، کانال، یادگیری و تاریخچه حفظ شده‌اند."
        return "از منوی زیر ادامه دهید."

    async def _show_help(self, chat_id: int) -> None:
        text = (
            "❓ راهنما\n"
            "• /start: نمایش وضعیت و ادامه از همان مرحله (هیچ‌چیز بازنشانی نمی‌شود).\n"
            "• /status: خلاصه وضعیت.\n"
            "• /pause: توقف فوری اجرای سفارش‌های جدید.\n"
            "• /resume: ادامه اجرا.\n"
            "• /cancel: لغو فقط همان مرحله ورود یا ورودی جاری.\n\n"
            "امنیت: ورود تلگرام فقط از ترمینال امن SSH انجام می‌شود؛ شماره، کد ورود "
            "و رمز دو مرحله‌ای را در هیچ چت تلگرامی نفرستید. کلید و رمز MEXC نیز "
            "نباید در چت یا لاگ نوشته شوند."
        )
        await self._send(chat_id, text, _kb([[_btn("بازگشت", "m:status")]]))

    async def _menu_action(self, chat_id: int, action: str) -> None:
        if action == "status":
            await self._show_start(chat_id)
        elif action == "help":
            await self._show_help(chat_id)
        elif action == "auth":
            await self._show_auth(chat_id)
        elif action == "adopt":
            await self._adopt_session(chat_id)
        elif action == "mexc":
            await self._show_mexc(chat_id)
        elif action == "channel":
            await self._show_channels(chat_id, 0)
        elif action == "buy":
            self.store.set_flow("buy_amount")
            await self._send(
                chat_id,
                "مبلغ ثابت خرید را به دلار بفرستید (مثلاً 10):",
                _kb([[_btn("لغو", "m:status")]]),
            )
        elif action == "sizing":
            await self._show_sizing(chat_id)
        elif action == "pause":
            await self._pause(chat_id)
        elif action == "resume":
            await self._resume(chat_id)
        else:
            await self._send(chat_id, "گزینه نامعتبر است.", self._menu_kb())

    # -------------------------------------------------------------- auth

    async def _show_auth(self, chat_id: int) -> None:
        status, label = await self._probe_session()
        s = self.store.setup()
        if status == "valid":
            discrepancy = self._discrepancy(s, status)
            if discrepancy == "session_valid_db_unauthenticated":
                await self._send(
                    chat_id,
                    "نشست تلگرام معتبر است اما در وضعیت ذخیره‌شده ثبت نشده. "
                    "برای پذیرش صریح این نشست (بدون ورود دوباره) دکمه زیر را بزنید.",
                    _kb([[_btn("پذیرش نشست موجود", "m:adopt")], [_btn("بازگشت", "m:status")]]),
                )
                return
            await self._send(
                chat_id,
                f"✅ نشست تلگرام معتبر است{f' — {label}' if label else ''}. "
                "ورود دوباره لازم نیست و هیچ‌چیز بازنشانی نمی‌شود.",
                _kb(
                    [
                        [_btn("قطع اتصال حساب تلگرام", "au:disconnect")],
                        [_btn("بازگشت", "m:status")],
                    ]
                ),
            )
            return
        if status == "unreachable":
            await self._send(
                chat_id,
                "ارتباط با تلگرام برقرار نشد. این وضعیت «نشست نامعتبر» تلقی نمی‌شود؛ "
                "اتصال شبکه مستقیم را بررسی کنید. نشست فعلی دست‌نخورده باقی می‌ماند.",
                _kb([[_btn("تلاش دوباره", "m:auth")], [_btn("بازگشت", "m:status")]]),
            )
            return
        text = (
            "🔐 ورود امن از ترمینال SSH\n"
            "ورود از چت تلگرام غیرفعال است: تلگرام کدهایی را که در چت تلگرام "
            "فرستاده شوند باطل می‌کند.\n\n"
            "روی همان سرور، در یک ترمینال امن SSH اجرا کنید:\n"
            "cd /opt/projects/akha1\n"
            ".venv/bin/tg-ingest auth login\n\n"
            "شماره، کد و رمز فقط در همان ترمینال وارد می‌شوند. کد را برای هیچ‌کس "
            "و در هیچ چتی نفرستید. پس از پایان، «بررسی دوباره نشست» را بزنید."
        )
        await self._send(
            chat_id,
            text,
            _kb([[_btn("🔄 بررسی دوباره نشست", "m:auth")], [_btn("بازگشت", "m:status")]]),
        )

    async def _adopt_session(self, chat_id: int) -> None:
        status, label = await self._probe_session()
        if status != "valid":
            await self._send(chat_id, "نشست معتبر نیست؛ پذیرش انجام نشد.")
            return
        self.store.update_setup(
            telegram_state="authenticated",
            telegram_authorized_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            telegram_account_label=label,
        )
        self.store.audit("telegram_session_adopted", "owner explicitly adopted an existing session")
        self.store.refresh_setup_state()
        await self._show_start(chat_id)

    async def _auth_action(self, chat_id: int, parts: list[str]) -> None:
        action = parts[1] if len(parts) > 1 else ""
        if action == "repair":
            await self._show_auth(chat_id)
        elif action == "disconnect":
            await self._confirm_disconnect(chat_id)
        elif action == "disconnect2":
            await self._disconnect_telegram(chat_id)
        else:
            await self._show_auth(chat_id)

    async def _confirm_disconnect(self, chat_id: int) -> None:
        text = (
            "⚠️ قطع اتصال حساب تلگرام\n"
            "این عمل نشست را در تلگرام باطل و فایل نشست محلی را حذف می‌کند. "
            "کانال، تنظیمات MEXC، نمونه‌های یادگیری و تاریخچه سفارش‌ها حفظ می‌شوند. "
            "پس از قطع اتصال، اجرای سیگنال‌ها متوقف می‌شود.\n\n"
            "برای ادامه دکمه تأیید را بزنید. این عمل از /start هرگز اجرا نمی‌شود."
        )
        await self._send(
            chat_id,
            text,
            _kb([[_btn("تأیید قطع اتصال", "au:disconnect2")], [_btn("انصراف", "m:status")]]),
        )

    async def _disconnect_telegram(self, chat_id: int) -> None:
        from ..telethon import logout_and_delete_session

        try:
            await logout_and_delete_session(self.cfg)
        except Exception as exc:  # noqa: BLE001 - never leak session contents
            await self._send(
                chat_id,
                f"قطع اتصال با خطا مواجه شد ({exc.__class__.__name__}). هیچ تغییر دیگری اعمال نشد.",
            )
            return
        self.store.update_setup(telegram_state="unauthenticated", telegram_account_label=None)
        self.store.refresh_setup_state()
        self.store.audit("telegram_disconnected", "owner explicitly confirmed session revocation")
        await self._send(
            chat_id,
            "حساب تلگرام قطع شد. برای ورود دوباره از ترمینال امن SSH استفاده کنید: .venv/bin/tg-ingest auth login",
        )

    # -------------------------------------------------------------- MEXC

    async def _show_mexc(self, chat_id: int) -> None:
        s = self.store.setup()
        has_cfg = bool(self.cfg.mexc.api_key and self.cfg.mexc.api_secret)
        text = (
            "🔑 تنظیم MEXC\n"
            f"وضعیت: {'تأییدشده' if s['mexc_state'] == 'verified' else 'تأیید نشده'}\n"
            f"برچسب: {s.get('mexc_label') or '—'}\n\n"
            "کلید باید فقط دو مجوز داشته باشد: خواندن موجودی و معامله اسپات. "
            "مجوز برداشت باید غیرفعال باشد. کلید با مجوز برداشت پذیرفته نمی‌شود.\n"
            "کلید و رمز هرگز دوباره نمایش داده نمی‌شوند و در لاگ نوشته نمی‌شوند."
        )
        rows: list[list[dict[str, str]]] = []
        if has_cfg:
            rows.append([_btn("استفاده از کلیدهای موجود در تنظیمات", "mx:existing")])
        if self.box.available:
            rows.append([_btn("افزودن / جایگزینی کلید", "mx:replace")])
        rows.append([_btn("بازگشت", "m:status")])
        if not self.box.available:
            text += (
                "\n\n⚠️ برای ذخیره امن کلید جدید، متغیر CONTROL_SECRET_KEY باید توسط "
                "مدیر سیستم تنظیم شود. بدون آن فقط کلیدهای موجود قابل بررسی هستند."
            )
        await self._send(chat_id, text, _kb(rows))

    async def _mexc_action(self, chat_id: int, parts: list[str]) -> None:
        action = parts[1] if len(parts) > 1 else ""
        if action == "existing":
            await self._verify_existing_mexc(chat_id)
        elif action == "replace":
            await self._begin_mexc_replace(chat_id)
        else:
            await self._show_mexc(chat_id)

    async def _verify_existing_mexc(self, chat_id: int) -> None:
        if not (self.cfg.mexc.api_key and self.cfg.mexc.api_secret):
            await self._send(chat_id, "کلید MEXC در تنظیمات موجود نیست.")
            return
        self.store.update_setup(mexc_state="pending")
        self.store.refresh_setup_state()
        client = self._make_mexc_client(self.cfg.mexc.api_key, self.cfg.mexc.api_secret)
        await self._finish_mexc_verification(chat_id, client, store_new=False)

    def _make_mexc_client(self, key: str, secret: str) -> Any:
        if self._mexc_client_factory is not None:
            return self._mexc_client_factory(key, secret)
        state_dir = self.cfg.data_dir
        state_dir.mkdir(parents=True, exist_ok=True)
        return MexcSpotClient(
            key,
            secret,
            base_url=self.cfg.mexc.base_url,
            recv_window_ms=self.cfg.mexc.recv_window_ms,
            connect_timeout_s=self.cfg.mexc.connect_timeout_s,
            request_timeout_s=self.cfg.mexc.request_timeout_s,
            general_gate=SharedWeightedRateGate(
                max(1, self.cfg.mexc.max_ip_weight_per_10s),
                10.0,
                state_path=state_dir / "mexc-rate-limit-ip.json",
            ),
            uid_gate=SharedWeightedRateGate(
                max(1, self.cfg.mexc.max_uid_weight_per_10s),
                10.0,
                state_path=state_dir / "mexc-rate-limit-uid.json",
            ),
        )

    async def _finish_mexc_verification(self, chat_id: int, client: Any, *, store_new: bool) -> None:
        await self._send(chat_id, "در حال بررسی کلید با MEXC…")
        check = await asyncio.to_thread(verify_mexc_client, client)
        if not check.ok:
            self._pending_mexc_key = None
            self._pending_mexc_secret = None
            self.store.clear_flow()
            self.store.update_setup(mexc_state="not_configured")
            self.store.refresh_setup_state()
            await self._send(
                chat_id,
                f"❌ تأیید MEXC ناموفق بود ({check.category}).\n{check.detail}\n"
                "هیچ کلیدی ذخیره نشد و معامله فعال نمی‌شود.",
                _kb([[_btn("تلاش دوباره", "m:mexc")], [_btn("بازگشت", "m:status")]]),
            )
            return
        if store_new:
            assert self._pending_mexc_key is not None
            assert self._pending_mexc_secret is not None
            self.store.put_secret("mexc_api_key", self._pending_mexc_key, self.box)
            self.store.put_secret("mexc_api_secret", self._pending_mexc_secret, self.box)
        self.cfg.mexc.api_key = self._pending_mexc_key or self.cfg.mexc.api_key
        self.cfg.mexc.api_secret = getattr(self, "_pending_mexc_secret", None) or self.cfg.mexc.api_secret
        if self._engine is not None:
            self._engine.client = client
            self._engine.registry = SymbolRegistry(client)
        self.store.update_setup(
            mexc_state="verified",
            mexc_label=check.detail,
            mexc_verified_at=datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
        self.store.clear_flow()
        self._pending_mexc_key = None
        self._pending_mexc_secret = None
        self.store.audit("mexc_verified", check.detail)
        self.store.refresh_setup_state()
        await self._send(chat_id, f"✅ MEXC تأیید شد ({check.detail}).")
        await self._show_start(chat_id)

    async def _begin_mexc_replace(self, chat_id: int) -> None:
        if not self.box.available:
            await self._send(
                chat_id,
                "ذخیره امن کلید ممکن نیست: CONTROL_SECRET_KEY تنظیم نشده است. کلیدها را در حافظه ذخیره نمی‌کنیم.",
            )
            return
        self._pending_mexc_key = None
        self._pending_mexc_secret = None
        self.store.update_setup(mexc_state="pending")
        self.store.refresh_setup_state()
        self.store.set_flow("mexc_key")
        await self._send(
            chat_id,
            "API Key جدید را بفرستید (فقط خواندن + معامله اسپات، بدون برداشت).\n"
            "این پیام تلاش می‌شود حذف شود؛ حذف تضمینی نیست.",
            _kb([[_btn("لغو", "m:status")]]),
        )

    async def _input_mexc_key(self, chat_id: int, message_id: int, text: str) -> None:
        key = text.strip()
        await self._delete_sensitive(chat_id, message_id)
        if len(key) < 8 or any(ch.isspace() for ch in key):
            await self._send(chat_id, "کلید نامعتبر است. دوباره بفرستید یا /cancel.")
            return
        self._pending_mexc_key = key
        self._pending_mexc_started = time.monotonic()
        self.store.set_flow("mexc_secret")
        await self._send(
            chat_id,
            "API Secret را بفرستید. این پیام نیز تلاش می‌شود حذف شود؛ حذف تضمینی نیست.",
            _kb([[_btn("لغو", "m:status")]]),
        )

    async def _input_mexc_secret(self, chat_id: int, message_id: int, text: str) -> None:
        secret = text.strip()
        await self._delete_sensitive(chat_id, message_id)
        if self._pending_mexc_key is None or time.monotonic() - self._pending_mexc_started > 900:
            self._pending_mexc_key = None
            self._pending_mexc_secret = None
            self.store.clear_flow()
            await self._send(chat_id, "مهلت ورود کلید به پایان رسید؛ دوباره شروع کنید.")
            return
        if len(secret) < 8 or any(ch.isspace() for ch in secret):
            await self._send(chat_id, "Secret نامعتبر است. دوباره بفرستید یا /cancel.")
            return
        self._pending_mexc_secret = secret
        client = self._make_mexc_client(self._pending_mexc_key, secret)
        await self._finish_mexc_verification(chat_id, client, store_new=True)

    # ----------------------------------------------------------- channels

    async def _show_channels(self, chat_id: int, offset: int, query: str | None = None) -> None:
        try:
            client = await self._open_telegram()
        except AuthFlowError as exc:
            await self._send(chat_id, f"⚠️ {exc}")
            return
        try:
            choices, more = await fetch_channels(
                client,
                offset=offset,
                limit=self.store.get_int_setting("channel_page_size", 10),
                query=query,
            )
        finally:
            with contextlib.suppress(Exception):
                await client.disconnect()
        if not choices:
            await self._send(
                chat_id,
                "کانال یا گروه واجد شرایطی پیدا نشد. ابتدا در تلگرام عضو کانال سیگنال شوید. "
                "توجه: این مرحله backfill یا معامله را شروع نمی‌کند.",
                _kb([[_btn("به‌روزرسانی", f"ch:r:{offset}")], [_btn("جستجو", "ch:s")], [_btn("بازگشت", "m:status")]]),
            )
            return
        rows = [[_btn(f"{c.kind}: {c.title}"[:60], f"ch:p:{c.channel_id}:{offset}")] for c in choices]
        nav: list[dict[str, str]] = []
        if offset > 0:
            nav.append(_btn("قبلی", f"ch:g:{max(0, offset - 10)}"))
        if more:
            nav.append(_btn("بعدی", f"ch:g:{offset + 10}"))
        if nav:
            rows.append(nav)
        rows.append([_btn("🔄 به‌روزرسانی", f"ch:r:{offset}"), _btn("🔎 جستجو", "ch:s")])
        rows.append([_btn("بازگشت", "m:status")])
        selected = self.store.setup().get("selected_channel_title")
        prefix = f"کانال فعلی: {selected}\n\n" if selected else ""
        await self._send(
            chat_id,
            prefix + "یک کانال/گروه را انتخاب کنید (حداکثر ۱۰ مورد). "
            "انتخاب کانال به‌تنهایی backfill یا معامله را فعال نمی‌کند.",
            _kb(rows),
        )

    async def _open_telegram(self) -> Any:
        factory = self._telethon_factory
        if factory is None:
            from ..telethon import build_client

            factory = build_client
        client = factory(self.cfg)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise AuthFlowError("نشست تلگرام معتبر نیست؛ ابتدا ورود را کامل کنید.")
            return client
        except Exception:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise

    async def _channel_action(self, chat_id: int, parts: list[str]) -> None:
        action = parts[1] if len(parts) > 1 else ""
        if action == "p" and len(parts) >= 4:
            await self._pick_channel(chat_id, int(parts[2]), int(parts[3]))
        elif action in ("g", "r") and len(parts) >= 3:
            await self._show_channels(chat_id, int(parts[2]))
        elif action == "s":
            self.store.set_flow("channel_search")
            await self._send(chat_id, "بخشی از نام یا @username کانال را بفرستید:", _kb([[_btn("لغو", "m:status")]]))
        else:
            await self._show_channels(chat_id, 0)

    async def _input_channel_search(self, chat_id: int, text: str) -> None:
        self.store.clear_flow()
        await self._show_channels(chat_id, 0, query=text.strip())

    async def _pick_channel(self, chat_id: int, channel_id: int, offset: int) -> None:
        try:
            client = await self._open_telegram()
        except AuthFlowError as exc:
            await self._send(chat_id, f"⚠️ {exc}")
            return
        try:
            choice: ChannelChoice | None = await find_channel(client, channel_id)
        finally:
            with contextlib.suppress(Exception):
                await client.disconnect()
        if choice is None:
            await self._send(chat_id, "این کانال دیگر در دسترس نیست. فهرست را به‌روزرسانی کنید.")
            await self._show_channels(chat_id, offset)
            return
        self.repo.ensure_channel(choice.channel_id, choice.access_hash, choice.username, choice.title)
        self.repo.select_channel(choice.channel_id)
        self.store.update_setup(
            selected_channel_id=choice.channel_id,
            selected_channel_title=choice.title,
        )
        self.store.refresh_setup_state()
        self.store.audit("channel_selected", f"id={choice.channel_id}")
        await self._send(
            chat_id,
            f"✅ کانال انتخاب شد: {choice.title}\n\n"
            "از این پس پیام‌های جدید از طریق مسیر موجود دریافت می‌شوند. "
            "backfill به‌صورت خودکار شروع نمی‌شود. سیگنال‌های معتبر جدید "
            "به‌صورت خودکار پردازش می‌شوند.",
        )
        await self._show_start(chat_id)

    # ------------------------------------------------------------ sizing

    async def _show_sizing(self, chat_id: int) -> None:
        text = (
            "📏 اندازه سفارش\n"
            f"• خرید: مبلغ ثابت {self.cfg.trading.order_quote_amount:g} دلار "
            "(بودجه اسمی، نه تضمین قیمت نهایی)\n"
            f"• بافر کارمزد: {self.cfg.trading.balance_fee_buffer_pct}٪\n\n"
            "این مبلغ در هر سیگنال Spot BUY استفاده می‌شود. "
            "موجودی فقط برای کفایت همان سفارش بررسی می‌شود و هیچ درصدی از حساب "
            "به‌عنوان ذخیره کنار گذاشته نمی‌شود."
        )
        await self._send(
            chat_id,
            text,
            _kb([[_btn("تغییر مبلغ خرید (دلار)", "m:buy")], [_btn("بازگشت", "m:status")]]),
        )

    async def _input_buy_amount(self, chat_id: int, text: str) -> None:
        try:
            value = float(text.strip().replace(",", ""))
        except ValueError:
            await self._send(chat_id, "عدد معتبر بفرستید (مثلاً 10).")
            return
        if not math.isfinite(value) or value <= 0:
            await self._send(chat_id, "مبلغ باید عددی متناهی و بزرگ‌تر از صفر باشد.")
            return
        if self.cfg.trading.order_quote_amount_source == "env":
            await self._send(
                chat_id,
                "مبلغ خرید با متغیر محیطی TRADING_ORDER_QUOTE_AMOUNT قفل شده است؛ "
                "برای تغییر از منو، آن متغیر را حذف کنید.",
            )
            return
        self.store.set_setting("order_quote_amount", f"{value:g}")
        self.cfg.trading.order_quote_amount = value
        self.cfg.trading.order_quote_amount_source = "database (Telegram menu)"
        self.cfg.trading.amount_configured = True
        self.store.clear_flow()
        self.store.audit("sizing_changed", f"order_quote_amount={value:g}")
        await self._send(chat_id, "مبلغ خرید ذخیره شد و در اجرای بعدی استفاده می‌شود.")
        await self._show_sizing(chat_id)

    # -------------------------------------------------- notification settings

    async def _show_notification_settings(self, chat_id: int) -> None:
        """One minimum severity threshold; recipient routing stays separate."""

        current = self.store.notification_min_severity()
        rows = [
            [_btn(f"{'✅ ' if current == level else ''}{level}", f"n:set:{level}")]
            for level in self.store.NOTIFICATION_LEVELS
        ]
        rows.append([_btn("بازگشت", "m:status")])
        await self._send(
            chat_id,
            "🔔 حداقل سطح اعلان\n"
            f"سطح فعلی: {current}\n"
            "همه اعلان‌های این سطح و بالاتر ارسال می‌شوند. "
            "مسیر گیرنده (مالک/توسعه‌دهنده) جدا از سطح اهمیت است.",
            _kb(rows),
        )

    async def _notification_action(self, chat_id: int, parts: list[str]) -> None:
        head = parts[1] if len(parts) > 1 else ""
        if head == "settings":
            await self._show_notification_settings(chat_id)
            return
        if head == "set" and len(parts) >= 3:
            level = parts[2].upper()
            if level not in self.store.NOTIFICATION_LEVELS:
                await self._send(chat_id, "سطح اعلان نامعتبر است.")
                return
            self.store.set_notification_min_severity(level)
            await self._send(
                chat_id,
                f"حداقل سطح اعلان روی {level} تنظیم شد.",
                _kb([[_btn("بازگشت به اعلان‌ها", "n:settings")]]),
            )
            return
        await self._send(chat_id, "عملیات اعلان نامعتبر است.", self._menu_kb())

    async def _show_version(self, chat_id: int) -> None:
        """Read-only runtime version report (owner only, no state changes)."""

        info = get_version_info()
        await self._send(chat_id, format_version_text(info))

    def _submission_gate(self) -> SubmissionGate:
        gate = getattr(self._engine, "submission_gate", None) if self._engine else None
        if gate is not None:
            return gate
        return SubmissionGate(self.repo.db, self.cfg, require_control_setup=True)

    def _pause_atomically(self) -> str | None:
        """Set pause under the exclusive lock; return a failure reason or None."""

        try:
            # Exclusive lock waits for already-claimed submissions, then sets
            # the durable pause state.  The acknowledgement is sent only after
            # this point, so no new exchange submission can start afterwards.
            with self._submission_gate().exclusive(timeout_s=45.0):
                self.store.update_setup(paused=1, pause_reason="owner paused execution")
                self.store.refresh_setup_state()
                self.store.audit("paused", "owner paused new order execution")
        except GateTimeout:
            with contextlib.suppress(Exception):
                self.store.audit(
                    "pause_not_acknowledged",
                    "in-flight submissions did not drain; pause was not acknowledged",
                )
            return "timed out waiting for in-flight submissions to drain"
        return None

    def _resume_atomically(self) -> tuple[str | None, str | None]:
        """Evaluate readiness and unpause under one lock and transaction."""

        gate = self._submission_gate()
        blocker: str | None = None
        try:
            with gate.exclusive(timeout_s=45.0):
                try:
                    with self.repo.db.transaction() as conn:
                        blocker = gate.resume_blocker_in_transaction(conn)
                        if blocker:
                            self.store.audit_in_conn(conn, "resume_refused", blocker)
                        else:
                            self.store.update_setup_in_conn(conn, paused=0, pause_reason=None)
                            self.store.refresh_setup_state_in_conn(conn)
                            self.store.audit_in_conn(
                                conn,
                                "resumed",
                                "owner resumed automatic signal execution",
                            )
                except Exception as exc:  # noqa: BLE001 - transaction rolled back
                    return None, f"resume transaction failed ({exc.__class__.__name__})"
        except GateTimeout:
            return None, "timed out waiting for in-flight submissions to drain"
        return blocker, None

    async def _pause(self, chat_id: int) -> None:
        # Locks and SQLite are blocking; keep the control bot's event loop free.
        failure = await asyncio.to_thread(self._pause_atomically)
        if failure:
            await self._send(
                chat_id,
                "⏸ درخواست توقف ثبت شد اما سفارش در حال ارسال تمام نشد. وضعیت توقف تأیید نشد؛ لطفاً دوباره تلاش کنید.",
            )
            return
        await self._send(chat_id, "⏸ اجرای سفارش‌های جدید متوقف شد.")
        await self._show_start(chat_id)

    async def _resume(self, chat_id: int) -> None:
        blocker, failure = await asyncio.to_thread(self._resume_atomically)

        if blocker:
            await self._send(
                chat_id,
                f"▶️ ادامه اجرا ممکن نیست تا زمانی که شرایط آماده‌سازی کامل شود:\n{blocker}",
            )
            return
        if failure:
            with contextlib.suppress(Exception):
                self.store.audit("resume_failed", failure)
            await self._send(
                chat_id,
                f"▶️ ادامه اجرا انجام نشد و سیستم در حالت توقف باقی ماند. دلیل: {failure}",
            )
            return

        resume = getattr(self._engine, "resume_planned_intents", None)
        if callable(resume):
            with contextlib.suppress(Exception):
                await asyncio.to_thread(resume)
        await self._send(chat_id, "▶️ اجرای خودکار ادامه یافت.")
        await self._show_start(chat_id)

    async def _sync_auto_notices(self) -> None:
        if self._engine is None or self.store.setup().get("effective_state") not in ("ready", "automatic"):
            return
        cutoff = datetime.now(UTC).timestamp() - 6 * 3600
        cutoff_iso = datetime.fromtimestamp(cutoff, UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
        rows = self.repo.db.conn.execute(
            """
            SELECT i.* FROM order_intents i
            LEFT JOIN control_notices n ON n.intent_id = i.intent_id
            WHERE n.intent_id IS NULL AND i.state <> 'planned'
              AND i.planned_at >= ?
            ORDER BY i.updated_at DESC LIMIT 10
            """,
            (cutoff_iso,),
        ).fetchall()
        for raw in rows:
            intent = dict(raw)
            if not self.store.notice_once(intent["intent_id"], "auto"):
                continue
            await self._send(
                int(self.cfg.control.owner_user_id or 0),
                "🤖 اجرای خودکار\n"
                f"نماد: {intent['symbol']} — جهت: {intent['side']} — وضعیت: {intent['state']}\n"
                f"شناسه سفارش: {_fmt(intent.get('exchange_order_id'))}",
            )

    async def tick(self) -> None:
        now = time.monotonic()
        if now - self._last_tick < 3:
            return
        self._last_tick = now
        # Recover deliveries that were in flight when the process ended; they
        # are never retried automatically.  Then flush the trading outbox.
        if self._engine is not None:
            store = self._engine.store
            try:
                recovered = store.recover_ambiguous_sending(older_than_s=120)
                if recovered:
                    log.info("recovered %d ambiguous notifications", recovered)
            except Exception:  # noqa: BLE001
                log.warning("ambiguous-notification recovery failed", exc_info=True)
            notifier = getattr(self._engine, "notifier", None)
            if notifier is not None:
                try:
                    notifier.flush(limit=50)
                except Exception:  # noqa: BLE001 - notifications never block control
                    log.warning("notification flush failed", exc_info=True)
        await self._sync_auto_notices()

    # --------------------------------------------------------------- loop

    async def run(self, stop_event: asyncio.Event) -> None:
        offset: int | None = None
        command_sent = False
        while not stop_event.is_set():
            if not command_sent:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(
                        self.bot.set_my_commands,
                        [
                            {"command": "start", "description": "وضعیت و ادامه"},
                            {"command": "status", "description": "خلاصه وضعیت"},
                            {"command": "pause", "description": "توقف اجرای سفارش"},
                            {"command": "resume", "description": "ادامه اجرا"},
                            {"command": "version", "description": "نسخه اجرا و وضعیت زمان-run"},
                            {"command": "help", "description": "راهنما"},
                        ],
                    )
                command_sent = True
            try:
                updates = await asyncio.to_thread(
                    self.bot.get_updates,
                    offset=offset,
                    timeout=min(10, self.cfg.control.poll_timeout_s),
                )
            except BotApiError as exc:
                log.warning("control bot polling failed: %s", exc)
                await asyncio.sleep(5)
                continue
            except Exception as exc:  # noqa: BLE001
                log.warning("control bot polling error (%s)", exc.__class__.__name__)
                await asyncio.sleep(5)
                continue
            for update in updates:
                update_id = update.get("update_id")
                if update_id is not None:
                    offset = int(update_id) + 1
                try:
                    await self.handle_update(update)
                except Exception as exc:  # noqa: BLE001 - one bad update must not stop the bot
                    log.warning("control update failed (%s)", exc.__class__.__name__)
            try:
                await self.tick()
            except Exception as exc:  # noqa: BLE001
                log.warning("control background tick failed (%s)", exc.__class__.__name__)
