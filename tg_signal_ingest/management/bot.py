"""Two-operator Persian management bot: auth, channels, preview, validation.

The owner and the developer are separate numeric Telegram user IDs. Every
message, callback, login step, pagination action, preview and logout is bound to
the calling operator in a private chat. Usernames/display names are never used
for authorization.

The optional validation workflow is narrowly delegated: the configured
developer can request and self-authorize a tightly bounded historical sample,
then start that one-use job and review its candidates. This delegation applies
only to bounded validation requests; it does not grant the owner role or any
unrelated owner-only control. Legacy requests created before this delegation
remain subject to the owner-only decision path. The offline analysis package is
used after the bounded import; there is no MEXC client, order intent, risk
decision, approval, or trading engine in this module. Selecting a channel or
previewing messages never starts ingestion.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import re
from datetime import UTC, datetime
from typing import Any

from telethon.errors import FloodWaitError, RPCError

from ..analysis import AnalysisError, AnalysisStore, ReplayScope, run_replay, validate_correction
from ..config import AppConfig
from ..control.auth import AuthFlowError, TelegramLoginManager
from ..control.bot_api import BotApiError
from ..control.channels import fetch_channels, find_channel
from ..repository import Repository
from ..session_guard import SessionBusyError, SessionGuard
from ..telethon import build_client
from .ingest import IngestionWorker, IngestReport
from .raw_review import RAW_REVIEW_LIMIT, ChannelUnavailable, NoChannelSelected, run_raw_review
from .signal_review import (
    SignalReviewUnavailable,
    ensure_available,
    logical_ids,
    origin_message_text,
    recent_signals,
    related_messages,
    render_original_message,
    render_related_messages,
    render_signal_detail,
    render_signal_extract,
    shorten_utf16,
    signal_by_logical_id,
    utf16_len,
)
from .store import VALIDATION_LIMITS, ManagementStore, utc_now

log = logging.getLogger("ingest.management.bot")

SENSITIVE_FLOWS = {"auth_phone", "auth_code", "auth_password"}
JOB_RUNNING = {"pending", "running", "interrupted"}
MAX_CHANNELS_SNAPSHOT = 100_000
PREVIEW_LIMIT = 10
# Preview budgets are UTF-16 code units (Telegram's measure), not Python
# characters: emoji/astral content counts twice and would otherwise push a
# whole preview silently over Telegram's 4096-unit hard limit.
PREVIEW_ITEM_UTF16 = 550
PREVIEW_TOTAL_UTF16 = 3800
REVIEW_MESSAGE_UTF16 = 900
REQUEST_ACTIVE_STATUSES = {"requested", "approved", "started", "review"}

ROLE_FA = {"owner": "مالک", "developer": "توسعه‌دهنده"}
REQUEST_STATUS_FA = {
    "requested": "در انتظار تأیید مالک",
    "approved": "مجاز؛ در انتظار شروع توسعه‌دهنده",
    "denied": "ردشده توسط مالک",
    "expired": "منقضی‌شده",
    "started": "در حال اجرا",
    "review": "آماده مرور انسانی",
    "completed": "مرور تکمیل‌شده",
    "failed": "ناموفق",
    "cancelled": "لغوشده",
}
CLASSIFICATION_FA = {
    "new_signal": "سیگنال جدید",
    "modification": "اصلاح سیگنال",
    "cancellation": "لغو سیگنال",
    "target_update": "به‌روزرسانی هدف",
    "stop_loss_update": "به‌روزرسانی حد ضرر",
    "result": "نتیجه",
    "non_signal": "غیرسیگنال",
    "ambiguous": "مبهم",
}
MARKET_FA = {"spot": "اسپات", "non_spot": "غیراسپات", "unknown": "نامشخص"}
SIDE_FA = {"buy": "خرید", "sell": "فروش"}
PARSE_STATUS_FA = {
    "parsed": "تجزیه‌شده",
    "needs_review": "نیازمند مرور",
    "failed": "ناموفق",
}
REVIEW_STATUS_FA = {
    "unreviewed": "بدون تصمیم",
    "confirmed": "تأییدشده",
    "corrected": "اصلاح‌شده",
    "rejected": "ردشده",
}
CORRECTION_FIELDS = {
    "classification",
    "market_type",
    "symbol",
    "quote_asset",
    "side",
    "entry_price",
    "entry_low_price",
    "entry_high_price",
    "stop_loss",
    "targets",
}

# Telegram login codes are normally 5 digits. Detect obvious pasted codes only
# so the bot can delete them and warn; this is not an authentication path.
_CODE_LIKE_RE = re.compile(r"^\s*\d(?:[\s\-–—]*\d){4,7}\s*$")

#: Telegram's per-message hard limit (UTF-16 code units, "after entities
#: parsing"). The bot never attaches entities, but emoji-heavy Persian text
#: can still exceed the limit in Python-character terms, so every budget
#: below is measured with ``utf16_len``.
MAX_MESSAGE_UTF16 = 4096
SAFE_TEXT_UTF16 = 3840


def _looks_like_login_code(text: str) -> bool:
    return bool(_CODE_LIKE_RE.fullmatch(text))


def _btn(text: str, data: str) -> dict[str, str]:
    """Build one inline keyboard button within Telegram's limits.

    Button labels are bounded to 64 units (UTF-16, cutting only on code
    points) and callback payloads to 64 *bytes* — the callback limit is a
    byte limit, not a character limit, and is validated here so a malformed
    page can never silently fail at Telegram.
    """

    label = shorten_utf16(text, 64)
    if label != text:
        label = label.rstrip() or "…"
        label = shorten_utf16(label, 64)
    if len(data.encode("utf-8")) > 64:
        raise ValueError(
            f"callback_data exceeds Telegram's 64-byte limit ({len(data.encode('utf-8'))} bytes)"
        )
    return {"text": label, "callback_data": data}


def _kb(rows: list[list[dict[str, str]]]) -> dict[str, Any]:
    return {"inline_keyboard": rows}


def _from_id(payload: dict[str, Any]) -> int | None:
    value = (payload.get("from") or {}).get("id")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _job_fa(status: str) -> str:
    return {
        "pending": "در انتظار شروع",
        "running": "در حال دریافت",
        "completed": "تکمیل‌شده",
        "partial": "ناقص",
        "failed": "ناموفق",
        "cancelled": "لغوشده",
        "interrupted": "متوقف‌شده (قابل ادامه)",
    }.get(status, status)


def _fmt(value: Any) -> str:
    return "—" if value in (None, "") else str(value)


def _media_label(message: Any) -> str | None:
    """Return a short media marker without downloading or inspecting content."""

    for attr, label in (
        ("photo", "عکس"),
        ("video", "ویدیو"),
        ("video_note", "ویدیوی گرد"),
        ("voice", "پیام صوتی"),
        ("audio", "صدا"),
        ("sticker", "استیکر"),
        ("gif", "گیف"),
        ("document", "فایل"),
        ("poll", "نظرسنجی"),
        ("geo", "موقعیت"),
        ("contact", "مخاطب"),
        ("webpage", "پیش‌نمایش لینک"),
    ):
        if getattr(message, attr, None):
            return label
    if getattr(message, "media", None) is not None:
        return "رسانه"
    return None


def _shorten(text: str, limit: int) -> str:
    """Bound a value by UTF-16 units, cutting only on code points."""

    text = " ".join(str(text).split())
    return shorten_utf16(text, limit)


def format_preview_message(message: Any) -> str:
    """Render one message as inert plain text (no Markdown/HTML execution)."""

    mid = int(getattr(message, "id", 0) or 0)
    date = getattr(message, "date", None)
    if isinstance(date, datetime):
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        stamp = date.astimezone(UTC).strftime("%Y-%m-%d %H:%M")
    else:
        stamp = "—"
    if getattr(message, "deleted", False):
        body = "[پیام حذف‌شده یا در دسترس نیست]"
    else:
        body = str(
            getattr(message, "message", None)
            or getattr(message, "text", None)
            or ""
        ).strip()
        if not body:
            action = getattr(message, "action", None)
            media = _media_label(message)
            if media:
                body = f"[{media}؛ بدون متن]"
            elif action is not None:
                body = "[پیام سرویس]"
            else:
                body = "[بدون متن]"
    return f"#{mid} — {stamp} UTC\n{_shorten(body, PREVIEW_ITEM_UTF16)}"


def _source_text(row: dict[str, Any] | None) -> str:
    if row is None:
        return "[پیام منبع در دسترس نیست]"
    text = str(row.get("text") or row.get("caption") or "").strip()
    if text:
        return _shorten(text, REVIEW_MESSAGE_UTF16)
    if row.get("media_type"):
        return "[پیام رسانه‌ای بدون متن]"
    return "[بدون متن]"


def _entry_text(candidate: dict[str, Any]) -> str:
    low = candidate.get("entry_low_price")
    high = candidate.get("entry_high_price")
    single = candidate.get("entry_price")
    if low is not None and high is not None:
        return f"{low} تا {high}"
    if single is not None:
        return str(single)
    if low is not None:
        return str(low)
    if high is not None:
        return str(high)
    return "—"


def _targets_text(candidate: dict[str, Any]) -> str:
    targets = candidate.get("targets") or []
    if not targets:
        return "—"
    parts = []
    for target in targets:
        if isinstance(target, dict):
            index = target.get("index")
            price = target.get("price")
            parts.append(f"{index}:{price}" if index is not None else str(price))
        else:
            parts.append(str(target))
    return "، ".join(parts)


def _candidate_summary(candidate: dict[str, Any]) -> list[str]:
    lines = [
        f"• دسته: {CLASSIFICATION_FA.get(str(candidate.get('classification')), str(candidate.get('classification')))}",
        f"• بازار: {MARKET_FA.get(str(candidate.get('market_type')), str(candidate.get('market_type')))}",
        f"• نماد: {candidate.get('symbol') or '—'}",
        f"• جهت: {SIDE_FA.get(str(candidate.get('side')), '—')}",
        f"• ورود: {_entry_text(candidate)}",
        f"• حد ضرر: {candidate.get('stop_loss') or '—'}",
        f"• اهداف: {_targets_text(candidate)}",
        f"• وضعیت تجزیه: {PARSE_STATUS_FA.get(str(candidate.get('parse_status')), str(candidate.get('parse_status')))}",
        "• تصمیم انسانی: "
        + REVIEW_STATUS_FA.get(
            str(candidate.get("review_status")), str(candidate.get("review_status"))
        ),
    ]
    ambiguities = candidate.get("ambiguities") or []
    if ambiguities:
        lines.append("• ابهام‌ها: " + "، ".join(str(item) for item in ambiguities))
    return lines


def _review_text(
    request: dict[str, Any],
    message: dict[str, Any] | None,
    candidate: dict[str, Any],
    index: int,
    total: int,
) -> str:
    message_id = int(candidate["message_id"])
    stamp = str((message or {}).get("datetime") or candidate.get("source_datetime") or "—")
    lines = [
        "🧪 مرور نمونه محدود",
        f"کانال: {request['channel_title']}",
        f"نمونه {index + 1} از {total} — پیام #{message_id} — {stamp}",
        "",
        "متن واقعی پیام ذخیره‌شده:",
        _source_text(message),
        "",
        "کاندید ماشینی:",
        *_candidate_summary(candidate),
    ]
    return "\n".join(lines)


def _parse_correction_text(text: str) -> dict[str, Any]:
    """Parse safe ``field=value`` lines without accepting arbitrary keys."""

    out: dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        separator = "=" if "=" in line else (":" if ":" in line else None)
        if separator is None:
            raise ValueError("هر خط باید به شکل field=value باشد")
        key, value = line.split(separator, 1)
        key = key.strip().lower()
        value = value.strip()
        if key not in CORRECTION_FIELDS:
            raise ValueError(f"فیلد ناشناخته: {key}")
        if key == "targets":
            out[key] = [part.strip() for part in re.split(r"[,،]", value) if part.strip()]
        else:
            out[key] = value
    if not out:
        raise ValueError("هیچ فیلدی ارسال نشد")
    return out


def _correction_preview(fields: dict[str, Any]) -> str:
    lines = []
    for key in sorted(fields):
        value = fields[key]
        if key == "targets" and isinstance(value, list):
            value = "، ".join(
                str(item.get("price")) if isinstance(item, dict) else str(item)
                for item in value
            )
        lines.append(f"{key}={value}")
    return "\n".join(lines) if lines else "(خالی)"


class ManagementBot:
    def __init__(
        self,
        cfg: AppConfig,
        repo: Repository,
        bot: Any,
        *,
        store: ManagementStore | None = None,
        telethon_factory: Any | None = None,
        probe_override: Any | None = None,
    ):
        owner = cfg.control.owner_user_id
        developer = cfg.control.developer_user_id
        if owner is None or developer is None:
            raise ValueError(
                "CONTROL_OWNER_USER_ID and CONTROL_DEVELOPER_USER_ID are required"
            )
        if int(owner) == int(developer):
            raise ValueError("owner and developer Telegram user ids must be different")
        self.cfg = cfg
        self.repo = repo
        self.bot = bot
        self.owner_id = int(owner)
        self.developer_id = int(developer)
        self.operators: dict[int, str] = {
            self.owner_id: "owner",
            self.developer_id: "developer",
        }
        self.store = store or ManagementStore(repo.db)
        self._telethon_factory = telethon_factory or build_client
        self._probe_override = probe_override
        self.guard = SessionGuard(cfg)
        self.worker = IngestionWorker(cfg, repo, self.store)
        self._worker_task: asyncio.Task[Any] | None = None
        self._worker_job_id: str | None = None
        self._stop_event: asyncio.Event | None = None

    # ------------------------------------------------------------ lifecycle

    async def startup(self) -> None:
        """Recover safely; never resume a stale in-memory login challenge."""

        abandoned = self.store.recover_flows()
        if abandoned:
            self.store.audit(
                "login_flows_abandoned_on_startup", f"count={abandoned}"
            )
            # A restart cannot resume an in-memory code/2FA challenge. Tell both
            # operators clearly; never touch the stored session or its history.
            await self._notify_all(
                "ℹ️ یک جریان ورود نیمه‌تمام پس از راه‌اندازی مجدد ربات باطل شد "
                "(کد/رمز فقط در حافظه بود). ورود از چت غیرفعال است؛ برای ورود "
                "امن از ترمینال، /start را بزنید. نشست معتبر قبلی دست‌نخورده است."
            )
        state = self.store.state()
        if state.get("telegram_state") == "pending":
            self.store.update_state(telegram_state="unauthenticated")
        interrupted = self.store.recover_jobs()
        if interrupted:
            self.store.audit("ingestion_interrupted_on_startup", f"jobs={interrupted}")
        recovered_requests = {"expired": 0, "interrupted": 0}
        if self.store.tables_exist("mgmt_import_requests", "mgmt_review_drafts"):
            recovered_requests = self.store.recover_import_requests()
            if recovered_requests["expired"]:
                self.store.audit(
                    "validation_requests_expired_on_startup",
                    f"count={recovered_requests['expired']}",
                )
            if recovered_requests["interrupted"]:
                self.store.audit(
                    "validation_import_interrupted_on_startup",
                    f"count={recovered_requests['interrupted']}",
                )
                await self._send(
                    self.developer_id,
                    "⚠️ واردسازی نمونه محدود به‌دلیل راه‌اندازی مجدد متوقف شد. "
                    "پیام‌های ذخیره‌شده حذف نشدند؛ برای ادامه یک درخواست تازه لازم است.",
                )
        log.info(
            "management bot startup complete (abandoned login flows=%s, interrupted jobs=%s, "
            "expired validations=%s, interrupted validations=%s)",
            abandoned,
            interrupted,
            recovered_requests["expired"],
            recovered_requests["interrupted"],
        )

    async def shutdown(self) -> None:
        self.guard.release()
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._worker_task
            if self._worker_job_id:
                job = self.store.get_job(self._worker_job_id)
                if job and job["status"] == "running":
                    self.store.finish_job(
                        self._worker_job_id,
                        "interrupted",
                        error="bot stopped; checkpoint preserved",
                        checkpoint_next_max_id=job.get("checkpoint_next_max_id"),
                    )
        self._worker_task = None
        self._worker_job_id = None

    # ------------------------------------------------------------- transport

    async def _send(
        self, operator_id: int, text: str, *, markup: dict[str, Any] | None = None
    ) -> bool:
        """One bounded outbound text; failures are logged, never fatal.

        ``text`` exceeding Telegram's limit would be rejected by the API and
        silently lost, so the renderer-side budgets above must always be
        respected; this guard records the size to make such mistakes visible
        in the logs without logging any content.
        """

        utf16_size = utf16_len(text)
        if utf16_size > MAX_MESSAGE_UTF16:
            log.error(
                "management send skipped: text too large for Telegram "
                "(utf16=%s limit=%s operator=%s)",
                utf16_size,
                MAX_MESSAGE_UTF16,
                operator_id,
            )
            return False
        try:
            await asyncio.to_thread(
                self.bot.send_message, operator_id, text, reply_markup=markup
            )
            return True
        except Exception as exc:  # noqa: BLE001 - transport failures are logged, never fatal
            log.warning(
                "management send failed (%s) to=%s utf16=%s",
                exc.__class__.__name__,
                operator_id,
                utf16_size,
            )
            return False

    async def _notify_all(
        self, text: str, *, markup: dict[str, Any] | None = None
    ) -> None:
        for operator_id in self.operators:
            await self._send(operator_id, text, markup=markup)

    async def _answer(self, callback_id: str, text: str | None = None) -> None:
        with contextlib.suppress(Exception):
            await asyncio.to_thread(self.bot.answer_callback_query, callback_id, text)

    async def _delete_message(self, operator_id: int, message_id: int) -> None:
        if not message_id:
            return
        with contextlib.suppress(Exception):
            await asyncio.to_thread(
                self.bot.delete_message, operator_id, message_id
            )

    async def _probe_session(self, operator_id: int) -> tuple[str, str | None]:
        if self._probe_override is not None:
            result = self._probe_override()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        async with self.guard.use(operator_id=operator_id):
            return await TelegramLoginManager(
                self.cfg, client_factory=self._telethon_factory
            ).probe()

    # --------------------------------------------------------------- updates

    async def handle_update(self, update: dict[str, Any]) -> None:
        update_id = update.get("update_id")
        if update_id is not None and not self.store.mark_update(int(update_id)):
            return
        if isinstance(update.get("callback_query"), dict):
            await self._handle_callback(update["callback_query"])
            return
        if isinstance(update.get("message"), dict):
            await self._handle_message(update["message"])

    def _operator(self, payload: dict[str, Any]) -> int | None:
        user_id = _from_id(payload)
        chat = payload.get("chat") or (payload.get("message") or {}).get("chat") or {}
        try:
            chat_id = int(chat.get("id") or 0)
        except (TypeError, ValueError):
            return None
        chat_type = str(chat.get("type") or "")
        if (
            user_id in self.operators
            and chat_type == "private"
            and chat_id == user_id
        ):
            return user_id
        return None

    def _role(self, operator_id: int) -> str:
        return self.operators.get(int(operator_id), "unknown")

    async def _handle_message(self, message: dict[str, Any]) -> None:
        operator_id = self._operator(message)
        if operator_id is None:
            # Never disclose account, channel, progress or errors to strangers
            # or groups, and never delete their messages.
            return
        message_id = int(message.get("message_id") or 0)
        flow_state = self.store.flow(operator_id)["state"]
        contact = message.get("contact")
        text = str(message.get("text") or "").strip()
        code_like = _looks_like_login_code(text)
        delete_after = (
            code_like
            or contact is not None
            or (flow_state in SENSITIVE_FLOWS and not text.startswith("/"))
        )
        try:
            # In-chat code/password login is deliberately unavailable. A stale
            # challenge from an older deployment is discarded, never reused.
            if flow_state in SENSITIVE_FLOWS:
                self.store.clear_flow(operator_id)
                self.store.invalidate_callbacks(operator_id)
                self.store.update_state(telegram_state="unauthenticated")
                self.store.audit(
                    "chat_login_rejected",
                    f"role={self._role(operator_id)} stale {flow_state} input refused",
                )
            if contact is not None:
                self.store.audit(
                    "contact_rejected",
                    f"role={self._role(operator_id)} contact sharing disabled",
                )
                await self._send(
                    operator_id,
                    "ورود از طریق چت غیرفعال است. شماره یا مخاطب را در چت نفرستید؛ "
                    "از ترمینال امن SSH استفاده کنید. /start",
                )
                return
            if code_like:
                self.store.audit(
                    "chat_code_rejected",
                    f"role={self._role(operator_id)} code-like message deleted without echo",
                )
                await self._send(
                    operator_id,
                    "⚠️ این پیام شبیه کد ورود بود و حذف شد. کد ورود را هرگز در "
                    "تلگرام یا چت ربات نفرستید؛ تلگرام چنین کدی را باطل می‌کند. "
                    "ورود فقط از ترمینال امن SSH انجام می‌شود. /start",
                )
                return
            if not text:
                await self._send(
                    operator_id,
                    "فقط پیام متنی پشتیبانی می‌شود. ورود از چت غیرفعال است. /help",
                )
                return

            if text.startswith(("/start", "/status", "/menu")):
                await self._show_start(operator_id)
                return
            if text.startswith("/help"):
                await self._show_help(operator_id)
                return
            if text.startswith("/cancel"):
                await self._cancel_input_flow(operator_id)
                return
            if text.startswith("/logout"):
                await self._show_logout_confirm(operator_id)
                return
            if text.startswith("/preview"):
                await self._preview_latest(operator_id)
                return
            if text.startswith("/signals"):
                await self._signal_review_menu(operator_id)
                return
            if text.startswith("/raw"):
                await self._raw_review(operator_id)
                return
            if text.startswith("/pause") or text.startswith("/resume"):
                await self._send(
                    operator_id,
                    "در این نسخه مدیریت، معامله یا پردازش خودکار وجود ندارد؛ "
                    "دریافت تاریخچه نیز از این نسخه آغاز نمی‌شود.",
                )
                return

            if (
                self._role(operator_id) == "developer"
                and self.store.tables_exist("mgmt_review_drafts")
            ):
                draft = self.store.get_review_draft(operator_id)
                if draft is not None:
                    await self._consume_correction_input(operator_id, text)
                    return

            flow = self.store.flow(operator_id)
            if flow["expired"]:
                await self._expire_flow(operator_id, flow["state"])
                return
            if flow["state"] == "channel_search":  # compatibility with older state rows
                self.store.clear_flow(operator_id)
                await self._send(
                    operator_id,
                    "جستجو در این نسخه در دسترس نیست؛ فهرست را دوباره باز کنید.",
                )
                return
            if flow["state"] == "order_amount_input":
                await self._handle_order_amount_input(operator_id, text)
                return
            await self._send(
                operator_id,
                "ورود از چت غیرفعال است. برای ورود امن از ترمینال SSH، /start را بزنید.",
                markup=self._menu(operator_id),
            )
        except Exception as exc:  # noqa: BLE001 - no stack traces or sensitive input in chat
            log.warning("management message handler failed (%s)", exc.__class__.__name__)
            await self._send(
                operator_id,
                "انجام این عملیات ممکن نشد. دوباره تلاش کنید یا /start بزنید.",
            )
        finally:
            if delete_after and message_id:
                # Best-effort deletion; never echo the submitted value.
                await self._delete_message(operator_id, message_id)

    async def _handle_callback(self, callback: dict[str, Any]) -> None:
        callback_id = str(callback.get("id") or "")
        operator_id = self._operator(callback)
        from_id = _from_id(callback)
        # A known operator pressing a button whose containing message is gone
        # (deleted / inaccessible) hits the stale-tap branch, not an
        # authorization failure: Telegram omits ``message`` exactly in that
        # case. Taps from groups or strangers keep a ``message`` and therefore
        # fall through to the plain refusal below.
        if operator_id is None and callback.get("message") is None and from_id in self.operators:
            await self._answer(
                callback_id,
                "این دکمه دیگر معتبر نیست (پیام آن حذف شده یا در دسترس نیست)؛ /start",
            )
            return
        if operator_id is None:
            await self._answer(callback_id, "دسترسی مجاز نیست")
            return
        chat = (callback.get("message") or {}).get("chat") or {}
        try:
            chat_id = int(chat.get("id") or 0)
        except (TypeError, ValueError):
            chat_id = 0
        data = str(callback.get("data") or "")
        token = data[3:] if data.startswith("mg:") else ""
        if not token:
            await self._answer(callback_id, "دکمه نامعتبر است")
            return
        status, row = self.store.consume_callback(
            token, owner_id=operator_id, chat_id=chat_id
        )
        if status in ("wrong_owner", "wrong_chat"):
            await self._answer(callback_id, "دسترسی مجاز نیست")
            return
        if status != "ok" or row is None:
            await self._answer(
                callback_id,
                "این دکمه منقضی، مصرف‌شده یا نامعتبر است؛ /start",
            )
            return
        await self._answer(callback_id)
        try:
            payload = json.loads(row.get("payload_json") or "{}")
            if not isinstance(payload, dict):
                payload = {}
            await self._dispatch_callback(
                operator_id,
                str(row["action"]),
                payload,
                row.get("snapshot_id"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("management callback failed (%s)", exc.__class__.__name__)
            await self._send(operator_id, "انجام این عملیات ممکن نشد. /start")

    async def _dispatch_callback(
        self,
        operator_id: int,
        action: str,
        payload: dict[str, Any],
        snapshot_id: str | None,
    ) -> None:
        if action == "status":
            await self._show_start(operator_id)
        elif action == "help":
            await self._show_help(operator_id)
        elif action == "auth":
            await self._show_auth(operator_id)
        elif action == "refresh":
            await self._refresh_channels(operator_id, 0)
        elif action == "page":
            await self._show_page(
                operator_id, int(payload.get("offset") or 0), snapshot_id
            )
        elif action == "select":
            await self._select_channel(
                operator_id, int(payload.get("channel_id") or 0), snapshot_id
            )
        elif action == "confirm":
            await self._confirm_channel(operator_id, payload, snapshot_id)
        elif action == "preview":
            await self._preview_latest(operator_id)
        elif action == "signals":
            await self._signal_review_menu(operator_id)
        elif action == "signal_view":
            await self._signal_view(
                operator_id, int(payload.get("logical_id") or 0)
            )
        elif action == "signal_extract":
            await self._signal_extract(
                operator_id, int(payload.get("logical_id") or 0)
            )
        elif action == "signal_related":
            await self._signal_related(
                operator_id, int(payload.get("logical_id") or 0)
            )
        elif action == "signal_original":
            await self._signal_original(
                operator_id,
                int(payload.get("logical_id") or 0),
                int(payload.get("message_id") or 0),
            )
        elif action == "signals_page":
            await self._signal_review_menu(
                operator_id, int(payload.get("offset") or 0)
            )
        elif action == "logout":
            await self._show_logout_confirm(operator_id)
        elif action == "logout_confirm":
            await self._perform_logout(operator_id)
        elif action == "cancel_flow":
            await self._cancel_input_flow(operator_id)
        elif action == "order_amount":
            await self._show_order_amount(operator_id)
        elif action == "cancel_job":
            await self._cancel_job(operator_id)
        elif action == "resume_job":
            await self._resume_job()
        elif action == "validation_request":
            await self._show_validation_request(operator_id)
        elif action == "validation_request_confirm":
            await self._create_validation_request(
                operator_id, int(payload.get("max_messages") or 0)
            )
        elif action == "validation_decide":
            await self._decide_validation_request(
                operator_id,
                str(payload.get("request_id") or ""),
                bool(payload.get("approve")),
            )
        elif action == "validation_start":
            await self._start_validation_import(
                operator_id, str(payload.get("request_id") or "")
            )
        elif action == "validation_status":
            await self._show_validation_status(operator_id)
        elif action == "validation_cancel":
            await self._cancel_validation_request(
                operator_id, str(payload.get("request_id") or "")
            )
        elif action == "validation_review":
            await self._show_candidate_page(
                operator_id,
                str(payload.get("request_id") or ""),
                int(payload.get("index") or 0),
            )
        elif action == "review_confirm":
            await self._review_candidate(
                operator_id,
                str(payload.get("request_id") or ""),
                str(payload.get("candidate_id") or ""),
                int(payload.get("index") or 0),
                "confirm",
            )
        elif action == "review_reject":
            await self._review_candidate(
                operator_id,
                str(payload.get("request_id") or ""),
                str(payload.get("candidate_id") or ""),
                int(payload.get("index") or 0),
                "reject",
            )
        elif action == "review_correct":
            await self._begin_correction(
                operator_id,
                str(payload.get("request_id") or ""),
                str(payload.get("candidate_id") or ""),
                int(payload.get("index") or 0),
            )
        elif action == "review_correct_save":
            await self._save_correction(operator_id)
        elif action == "review_correct_cancel":
            self.store.clear_review_draft(operator_id)
            await self._send(operator_id, "اصلاح لغو شد.")
            await self._show_validation_status(operator_id)
        else:
            await self._send(operator_id, "گزینه نامعتبر است. /start")

    # ---------------------------------------------------------------- menus

    def _menu(self, operator_id: int) -> dict[str, Any]:
        state = self.store.state()
        flow = self.store.flow(operator_id)
        rows: list[list[dict[str, str]]] = []
        if flow["state"] in SENSITIVE_FLOWS:
            rows.append(
                [
                    _btn(
                        "✖️ لغو چالش قدیمی",
                        "mg:" + self._token(operator_id, "cancel_flow"),
                    )
                ]
            )
            rows.append(
                [
                    _btn(
                        "🔐 راهنمای ورود امن",
                        "mg:" + self._token(operator_id, "auth"),
                    )
                ]
            )
        elif state.get("telegram_state") != "authenticated":
            rows.append(
                [
                    _btn(
                        "🔐 راهنمای ورود امن (ترمینال)",
                        "mg:" + self._token(operator_id, "auth"),
                    )
                ]
            )
        else:
            rows.append(
                [
                    _btn(
                        "📡 فهرست کانال‌ها",
                        "mg:" + self._token(operator_id, "refresh"),
                    )
                ]
            )
            if self.store.operator_channel(operator_id).get("selected_channel_id"):
                rows.append(
                    [
                        _btn(
                            "👁 پیش‌نمایش ۱۰ پیام آخر",
                            "mg:" + self._token(operator_id, "preview"),
                        )
                    ]
                )
                if self.store.tables_exist("channel_signals", "signal_message_links"):
                    rows.append(
                        [
                            _btn(
                                "🗂 مرور سیگنال‌های اخیر",
                                "mg:" + self._token(operator_id, "signals"),
                            )
                        ]
                    )
            if self.store.validation_available():
                request = self.store.latest_import_request()
                if self._role(operator_id) == "developer":
                    selected = self.store.operator_channel(operator_id).get(
                        "selected_channel_id"
                    )
                    if request is not None and (
                        request["status"] in REQUEST_ACTIVE_STATUSES
                        or request["status"] == "completed"
                    ):
                        rows.append(
                            [
                                _btn(
                                    "🧪 وضعیت اعتبارسنجی",
                                    "mg:" + self._token(operator_id, "validation_status"),
                                )
                            ]
                        )
                    elif selected:
                        rows.append(
                            [
                                _btn(
                                    "🧪 درخواست واردسازی محدود",
                                    "mg:" + self._token(operator_id, "validation_request"),
                                )
                            ]
                        )
                elif self._role(operator_id) == "owner" and request is not None:
                    label = (
                        "🔏 بررسی درخواست واردسازی"
                        if request["status"] == "requested"
                        else "🧪 وضعیت اعتبارسنجی"
                    )
                    rows.append(
                        [
                            _btn(
                                label,
                                "mg:" + self._token(operator_id, "validation_status"),
                            )
                        ]
                    )
        job = self.store.latest_job()
        if job is not None:
            if job["status"] in JOB_RUNNING:
                rows.append(
                    [
                        _btn(
                            "⏹ لغو دریافت",
                            "mg:" + self._token(operator_id, "cancel_job"),
                        )
                    ]
                )
            elif (
                job["status"] in ("cancelled", "failed", "partial", "interrupted")
                and job.get("request_id") is None
            ):
                rows.append(
                    [
                        _btn(
                            "▶️ ادامه دریافت",
                            "mg:" + self._token(operator_id, "resume_job"),
                        )
                    ]
                )
        rows.append(
            [
                _btn("📊 وضعیت", "mg:" + self._token(operator_id, "status")),
                _btn("❓ راهنما", "mg:" + self._token(operator_id, "help")),
            ]
        )
        if state.get("telegram_state") == "authenticated":
            rows.append(
                [
                    _btn(
                        "💰 مبلغ سفارش",
                        "mg:" + self._token(operator_id, "order_amount"),
                    ),
                    _btn(
                        "🚪 خروج از حساب تلگرام",
                        "mg:" + self._token(operator_id, "logout"),
                    ),
                ]
            )
        return _kb(rows)

    def _token(
        self,
        operator_id: int,
        action: str,
        *,
        payload: dict[str, Any] | None = None,
        snapshot_id: str | None = None,
    ) -> str:
        return self.store.issue_callback(
            owner_id=operator_id,
            chat_id=operator_id,
            action=action,
            payload=payload,
            snapshot_id=snapshot_id,
        )

    async def _show_start(self, operator_id: int) -> None:
        flow = self.store.flow(operator_id)
        if flow["state"] in SENSITIVE_FLOWS:
            # Old deployments may have persisted a code/password stage. It can
            # never be resumed; discard it before showing any status.
            await self._expire_flow(operator_id, flow["state"])
            return
        if flow["expired"]:
            await self._expire_flow(operator_id, flow["state"])
            return
        status: str
        label: str | None = None
        try:
            status, label = await self._probe_session(operator_id)
        except SessionBusyError:
            status, label = "session-busy", None
        state = self.store.state()
        if status == "valid":
            if (
                state.get("telegram_state") != "authenticated"
                or state.get("telegram_label") != label
            ):
                self.store.update_state(
                    telegram_state="authenticated",
                    telegram_label=label,
                    telegram_authorized_at=utc_now(),
                )
                self.store.audit(
                    "telegram_session_verified",
                    "existing session verified; no logout/replace",
                )
                state = self.store.state()
        elif status == "missing" and state.get("telegram_state") == "authenticated":
            self.store.update_state(telegram_state="unauthenticated")
            self.store.audit(
                "telegram_session_missing", "stored session is no longer authorized"
            )
            state = self.store.state()
        text = self._status_text(operator_id, status, label, state)
        await self._send(operator_id, text, markup=self._menu(operator_id))

    def _status_text(
        self,
        operator_id: int,
        probe_status: str,
        label: str | None,
        state: dict[str, Any],
    ) -> str:
        auth = {
            "valid": "متصل و تأییدشده",
            "missing": "نیاز به ورود",
            "unreachable": "ارتباط برقرار نشد (نشست دست‌نخورده)",
            "session-busy": "نشست تلگرام برای عملیات دیگر مشغول است",
        }.get(probe_status, probe_status)
        lines = [
            "🤖 مدیریت کانال تلگرام",
            f"• نقش شما: {ROLE_FA.get(self._role(operator_id), 'نامشخص')}",
            f"• حساب تلگرام: {auth}" + (f" — {label}" if label else ""),
        ]
        mine = self.store.operator_channel(operator_id)
        if mine.get("selected_channel_title"):
            lines.append(
                f"• کانال انتخابی شما: {mine['selected_channel_title']} "
                f"(id={mine.get('selected_channel_id')})"
            )
        else:
            lines.append("• کانال انتخابی شما: هنوز انتخاب نشده")
        global_channel = state.get("selected_channel_title")
        if global_channel:
            lines.append(f"• کانال ثبت‌شده پروژه: {global_channel}")
        job = self.store.latest_job()
        if job is None:
            lines.append("• دریافت تاریخچه: هنوز شروع نشده")
        else:
            lines.append(f"• دریافت تاریخچه: {_job_fa(str(job['status']))}")
            if job.get("error"):
                lines.append(f"  آخرین وضعیت: {str(job['error'])[:180]}")
        if self.store.validation_available():
            request = self.store.latest_import_request()
            if request is not None:
                lines.append(
                    "• اعتبارسنجی محدود: "
                    + REQUEST_STATUS_FA.get(
                        str(request["status"]), str(request["status"])
                    )
                    + f" — مرور {int(request['reviewed_count'] or 0)}"
                    f"/{int(request['candidate_count'] or 0)}"
                )
                if request.get("error") and request["status"] in (
                    "failed",
                    "cancelled",
                    "denied",
                    "expired",
                ):
                    lines.append(f"  آخرین وضعیت: {str(request['error'])[:180]}")
        # Order amount display (default and persisted).
        order_amount = self.cfg.trading.order_quote_amount
        source = getattr(self.cfg.trading, "order_quote_amount_source", "default")
        lines.append(
            f"• مبلغ سفارش پیش‌فرض: {order_amount:g} USDT در هر خرید "
            f"(منبع: {source})"
        )

        if probe_status == "valid":
            lines.append(
                "\nپیش‌نمایش پیام‌ها فقط خواندنی است؛ رسانه دانلود و محتوا پردازش نمی‌شود."
            )
        elif probe_status == "missing":
            lines.append(
                "\nبرای ادامه، ورود را از ترمینال امن SSH انجام دهید؛ "
                "کد ورود را در چت نفرستید."
            )
        return "\n".join(lines)

    async def _show_order_amount(self, operator_id: int) -> None:
        """Show current order amount and request a new value."""
        current = self.cfg.trading.order_quote_amount
        source = getattr(self.cfg.trading, "order_quote_amount_source", "default")
        self.store.set_flow(
            operator_id,
            state="order_amount_input",
        )
        text = (
            "💰 مقدار سفارش فعلی:\n"
            f"• مبلغ فعلی: {current:g} USDT (منبع: {source})\n\n"
            "مبلغ جدید را به‌صفته یک عدد مثبت وارد کنید (مثلاً 7.5). "
            "برای لغو، /start بزنید."
        )
        await self._send(operator_id, text)

    async def _handle_order_amount_input(self, operator_id: int, text: str) -> None:
        """Validate and persist a new order quote amount from user input."""
        try:
            value = float(text.strip().replace(",", ""))
        except ValueError:
            self.store.clear_flow(operator_id)
            await self._show_start(operator_id)
            await self._send(
                operator_id,
                "عدد معتبر نیست. لطفاً یک عدد مثبت وارد کنید.",
            )
            return
        if not math.isfinite(value) or value <= 0:
            self.store.clear_flow(operator_id)
            await self._show_start(operator_id)
            await self._send(
                operator_id,
                "مبلغ باید بزرگتر از صفر باشد.",
            )
            return
        self.store.set_setting("order_quote_amount", str(value))
        self.store.clear_flow(operator_id)
        self.cfg.trading.order_quote_amount = value
        self.cfg.trading.order_quote_amount_source = "database (Telegram menu)"
        self.store.audit("sizing_changed", f"order_quote_amount={value:g}")
        await self._show_start(operator_id)
        await self._send(
            operator_id,
            f"مبلغ سفارش تنظیم شد: {value:g} USDT",
        )

    async def _show_help(self, operator_id: int) -> None:
        text = (
            "❓ راهنمای مدیریت کانال\n"
            "• /start یا /status: نمایش وضعیت و منو؛ هیچ نشست، کانال یا پیشرفتی بازنشانی نمی‌شود.\n"
            "• /help: همین راهنما.\n"
            "• ورود امن: فقط از ترمینال SSH با دستور "
            "«.venv/bin/tg-ingest auth login». شماره، کد و رمز را در چت نفرستید.\n"
            "• تلگرام کدهای ارسال‌شده در چت تلگرام را باطل می‌کند؛ به همین دلیل "
            "ورود از چت ربات غیرفعال است.\n"
            "• سرویس مدیریت می‌تواند هنگام ورود ترمینال روشن بماند؛ قفل نشست "
            "مانع استفاده هم‌زمان دو فرایند می‌شود.\n"
            "• کانال‌ها در صفحه‌های ۱۰تایی نمایش داده می‌شوند. انتخاب کانال فقط "
            "برای بررسی است و دریافت تاریخچه را آغاز نمی‌کند.\n"
            "• «پیش‌نمایش ۱۰ پیام آخر» فقط خواندنی است: رسانه دانلود نمی‌شود، "
            "لینک دنبال نمی‌شود و محتوا پردازش یا در صف قرار نمی‌گیرد.\n"
            "• /raw فقط برای توسعه‌دهنده: مرور خام ۱۰۰ پیام آخر کانال انتخابی به‌ترتیب "
            "زمانی (قدیمی‌ترین اول) در چت خصوصی؛ بدون پردازش، طبقه‌بندی یا خلاصه‌سازی، "
            "بدون دانلود رسانه و بدون ذخیره متن در پایگاه‌داده یا لاگ.\n"
            "• اعتبارسنجی محدود فقط توسط توسعه‌دهنده درخواست، خودمجاز، شروع و "
            "مرور می‌شود؛ مالک فقط اطلاع می‌گیرد. این مجوز محدود به همان نمونه "
            "است و هیچ کنترل مالکانه دیگری را باز نمی‌کند. درخواست‌های قدیمی "
            "همچنان مسیر تصمیم مالک را دارند.\n"
            "• در اصلاح کاندید، هر خط را به شکل field=value بفرستید؛ مثلاً "
            "symbol=ETHUSDT یا targets=110,120. اصلاح‌ها فقط به‌صورت رکورد "
            "حسابرسی append-only ذخیره می‌شوند و پیام تلگرام را تغییر نمی‌دهند.\n"
            "• خروج از حساب تلگرام با تأیید صریح انجام می‌شود؛ ابتدا تلاش برای "
            "خروج سمت سرور تلگرام و سپس پاک‌سازی نشست محلی در صورت موفقیت.\n"
            "• پیام‌های حساس تلاش می‌شوند حذف شوند؛ حذف در اعلان‌ها، اسکرین‌شات، "
            "دستگاه‌های دیگر یا نسخه‌های پشتیبان تضمینی نیست.\n\n"
            "این نسخه فقط داده ذخیره و نمونه محدود را آفلاین تحلیل می‌کند: هیچ "
            "پردازش سیگنال معاملاتی، معامله یا سفارشی فعال نیست."
        )
        await self._send(
            operator_id,
            text,
            markup=_kb(
                [[_btn("بازگشت", "mg:" + self._token(operator_id, "status"))]]
            ),
        )

    # ----------------------------------------------------------------- auth

    async def _show_auth(self, operator_id: int) -> None:
        """Explain the supported out-of-band login; never request a code in chat."""

        state = self.store.state()
        try:
            status, label = await self._probe_session(operator_id)
        except SessionBusyError:
            status, label = "session-busy", None
        except AuthFlowError as exc:
            status, label = "unreachable", str(exc)
        if status == "valid":
            if (
                state.get("telegram_state") != "authenticated"
                or state.get("telegram_label") != label
            ):
                self.store.update_state(
                    telegram_state="authenticated",
                    telegram_label=label,
                    telegram_authorized_at=utc_now(),
                )
            await self._send(
                operator_id,
                f"✅ نشست تلگرام معتبر است{f' — {label}' if label else ''}. ورود دوباره لازم نیست.",
                markup=self._menu(operator_id),
            )
            return
        await self._send(
            operator_id,
            "🔐 ورود امن از ترمینال SSH\n"
            "ورود از چت تلگرام غیرفعال است: تلگرام کدهایی را که در چت تلگرام "
            "فرستاده شوند باطل می‌کند و ممکن است ورود را به دلایل امنیتی مسدود کند.\n\n"
            "روی همان سروری که ربات را اجرا می‌کند، در یک ترمینال امن SSH اجرا کنید:\n"
            "cd /opt/projects/akha1\n"
            ".venv/bin/tg-ingest auth login\n\n"
            "شماره، کد و در صورت نیاز رمز دو مرحله‌ای فقط در همان ترمینال وارد "
            "می‌شوند و در چت یا لاگ ذخیره نمی‌شوند. کد را برای هیچ‌کس نفرستید.\n"
            "پس از پایان، دکمه «بررسی دوباره نشست» را بزنید. سرویس مدیریت می‌تواند "
            "در تمام مدت روشن بماند؛ قفل نشست از تداخل جلوگیری می‌کند.",
            markup=_kb(
                [
                    [
                        _btn(
                            "🔄 بررسی دوباره نشست",
                            "mg:" + self._token(operator_id, "status"),
                        )
                    ],
                    [
                        _btn(
                            "🏠 منو",
                            "mg:" + self._token(operator_id, "status"),
                        )
                    ],
                ]
            ),
        )

    async def _expire_flow(self, operator_id: int, state: str) -> None:
        self.store.clear_flow(operator_id)
        self.store.invalidate_callbacks(operator_id)
        if state in SENSITIVE_FLOWS:
            self.store.update_state(telegram_state="unauthenticated")
        await self._send(
            operator_id,
            "مهلت این مرحله به پایان رسید و ورودی قبلی دیگر معتبر نیست. "
            "ورود از چت غیرفعال است؛ از ترمینال SSH استفاده کنید. /start",
        )

    async def _cancel_input_flow(self, operator_id: int) -> None:
        flow = self.store.flow(operator_id)
        was_sensitive = flow["state"] in SENSITIVE_FLOWS
        self.store.clear_flow(operator_id)
        if self.store.tables_exist("mgmt_review_drafts"):
            self.store.clear_review_draft(operator_id)
        self.store.invalidate_callbacks(operator_id)
        if was_sensitive:
            self.store.update_state(telegram_state="unauthenticated")
            self.store.audit("chat_login_rejected", f"role={self._role(operator_id)} cancelled stale flow")
        await self._send(
            operator_id,
            "مرحله ورود جاری لغو شد. نشست معتبر و پیشرفت قبلی حفظ شد. "
            "ورود از چت غیرفعال است؛ از ترمینال SSH استفاده کنید.",
        )

    # -------------------------------------------------------------- channels

    async def _open_client(self) -> Any:
        client = self._telethon_factory(self.cfg)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise AuthFlowError("نشست تلگرام معتبر نیست؛ ابتدا ورود را کامل کنید.")
            return client
        except Exception:
            with contextlib.suppress(Exception):
                await client.disconnect()
            raise

    async def _refresh_channels(self, operator_id: int, offset: int) -> None:
        try:
            async with self.guard.use(operator_id=operator_id):
                client = await self._open_client()
                try:
                    channels, _more = await fetch_channels(
                        client, offset=0, limit=MAX_CHANNELS_SNAPSHOT
                    )
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
        except SessionBusyError:
            await self._send(
                operator_id,
                "نشست تلگرام هم‌اکنون مشغول است؛ کمی بعد دوباره تلاش کنید.",
            )
            return
        except AuthFlowError as exc:
            await self._send(
                operator_id,
                f"⚠️ {exc}",
                markup=_kb(
                    [[_btn("ورود", "mg:" + self._token(operator_id, "auth"))]]
                ),
            )
            return
        except FloodWaitError as exc:
            await self._send(
                operator_id,
                f"تلگرام موقتاً محدودیت گذاشته است؛ {int(exc.seconds)} ثانیه صبر کنید.",
            )
            return
        except (RPCError, ConnectionError, OSError) as exc:
            await self._send(
                operator_id,
                f"دریافت فهرست کانال‌ها ناموفق بود ({exc.__class__.__name__}).",
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._send(
                operator_id,
                f"⚠️ اتصال به تلگرام برقرار نشد ({exc.__class__.__name__}).",
            )
            return

        if not channels:
            self.store.create_snapshot([], operator_id=operator_id)
            await self._send(
                operator_id,
                "کانال یا سوپرگروه واجد شرایطی پیدا نشد. ابتدا در تلگرام عضو کانال شوید. "
                "این مرحله هیچ تاریخی دریافت نمی‌کند.",
                markup=_kb(
                    [[_btn("🔄 به‌روزرسانی", "mg:" + self._token(operator_id, "refresh"))]]
                ),
            )
            return
        snapshot_id = self.store.create_snapshot(channels, operator_id=operator_id)
        self.store.audit(
            "channel_snapshot",
            f"role={self._role(operator_id)} count={len(channels)}",
        )
        await self._show_page(operator_id, offset, snapshot_id)

    async def _show_page(
        self, operator_id: int, offset: int, snapshot_id: str | None
    ) -> None:
        current = self.store.current_snapshot(operator_id)
        if snapshot_id is None or current != snapshot_id:
            await self._send(
                operator_id,
                "این فهرست قدیمی شده است؛ برای دیدن فهرست تازه دکمه به‌روزرسانی را بزنید.",
                markup=_kb(
                    [[_btn("🔄 به‌روزرسانی", "mg:" + self._token(operator_id, "refresh"))]]
                ),
            )
            return
        total = self.store.snapshot_count(snapshot_id)
        if total == 0:
            await self._send(
                operator_id,
                "فهرست خالی است؛ برای گرفتن فهرست جدید دوباره تلاش کنید.",
                markup=_kb(
                    [[_btn("🔄 به‌روزرسانی", "mg:" + self._token(operator_id, "refresh"))]]
                ),
            )
            return
        offset = max(0, min(int(offset), max(0, total - 1)))
        page = self.store.snapshot_page(snapshot_id, offset, 10)
        rows: list[list[dict[str, str]]] = []
        for item in page:
            label = f"{item['kind']}: {item['title']} (id={item['channel_id']})"
            rows.append(
                [
                    _btn(
                        label,
                        "mg:"
                        + self._token(
                            operator_id,
                            "select",
                            payload={"channel_id": int(item["channel_id"])},
                            snapshot_id=snapshot_id,
                        ),
                    )
                ]
            )
        nav: list[dict[str, str]] = []
        if offset > 0:
            nav.append(
                _btn(
                    "قبلی",
                    "mg:"
                    + self._token(
                        operator_id,
                        "page",
                        payload={"offset": max(0, offset - 10)},
                        snapshot_id=snapshot_id,
                    ),
                )
            )
        if offset + len(page) < total:
            nav.append(
                _btn(
                    "۱۰ کانال بعدی",
                    "mg:"
                    + self._token(
                        operator_id,
                        "page",
                        payload={"offset": offset + 10},
                        snapshot_id=snapshot_id,
                    ),
                )
            )
        if nav:
            rows.append(nav)
        rows.append(
            [_btn("🔄 به‌روزرسانی", "mg:" + self._token(operator_id, "refresh"))]
        )
        rows.append(
            [_btn("✖️ انصراف", "mg:" + self._token(operator_id, "status"))]
        )
        mine = self.store.operator_channel(operator_id)
        selected = mine.get("selected_channel_title")
        prefix = f"کانال فعلی شما: {selected}\n\n" if selected else ""
        await self._send(
            operator_id,
            prefix
            + f"یک کانال را انتخاب کنید — صفحه {offset // 10 + 1} از {max(1, (total + 9) // 10)} "
            f"(حداکثر ۱۰ دکمه). انتخاب هنوز به معنی شروع دریافت نیست.",
            markup=_kb(rows),
        )

    async def _select_channel(
        self, operator_id: int, channel_id: int, snapshot_id: str | None
    ) -> None:
        if snapshot_id is None or snapshot_id != self.store.current_snapshot(
            operator_id
        ):
            await self._send(
                operator_id, "این انتخاب قدیمی شده است؛ فهرست را به‌روزرسانی کنید."
            )
            return
        item = self.store.snapshot_channel(
            snapshot_id, channel_id, operator_id=operator_id
        )
        if item is None:
            await self._send(
                operator_id,
                "این کانال در فهرست فعلی نیست؛ فهرست را به‌روزرسانی کنید.",
            )
            return
        # Revalidate against the live dialog list; a cached button is not proof
        # that the channel is still accessible.
        try:
            async with self.guard.use(operator_id=operator_id):
                client = await self._open_client()
                try:
                    choice = await find_channel(client, channel_id)
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
        except SessionBusyError:
            await self._send(
                operator_id, "نشست تلگرام مشغول است؛ کمی بعد دوباره تلاش کنید."
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._send(
                operator_id, f"بررسی کانال ممکن نشد ({exc.__class__.__name__})."
            )
            return
        if choice is None:
            await self._send(
                operator_id,
                "این کانال دیگر در دسترس نیست؛ فهرست را به‌روزرسانی کنید.",
            )
            return
        token = self._token(
            operator_id,
            "confirm",
            payload={
                "channel_id": choice.channel_id,
                "access_hash": choice.access_hash,
                "title": choice.title,
                "username": choice.username,
                "kind": choice.kind,
            },
            snapshot_id=snapshot_id,
        )
        username = f"@{choice.username}" if choice.username else "بدون username"
        await self._send(
            operator_id,
            "🔎 تأیید منبع بررسی\n"
            f"نوع: {choice.kind}\n"
            f"عنوان: {choice.title}\n"
            f"شناسه ثابت: {choice.channel_id}\n"
            f"username: {username}\n\n"
            "فقط اگر همین هویت درست است تأیید کنید. تأیید فقط کانال را برای "
            "پیش‌نمایش انتخاب می‌کند؛ هیچ تاریخی دریافت یا در صف پردازش قرار نمی‌گیرد.",
            markup=_kb(
                [
                    [_btn("✅ تأیید انتخاب", "mg:" + token)],
                    [_btn("انصراف", "mg:" + self._token(operator_id, "refresh"))],
                ]
            ),
        )

    async def _confirm_channel(
        self, operator_id: int, payload: dict[str, Any], snapshot_id: str | None
    ) -> None:
        if snapshot_id is None or snapshot_id != self.store.current_snapshot(
            operator_id
        ):
            await self._send(
                operator_id,
                "این تأیید قدیمی شده است؛ کانال را دوباره انتخاب کنید.",
            )
            return
        channel_id = int(payload.get("channel_id") or 0)
        if channel_id == 0:
            await self._send(operator_id, "درخواست نامعتبر است.")
            return
        item = self.store.snapshot_channel(
            snapshot_id, channel_id, operator_id=operator_id
        )
        if item is None:
            await self._send(
                operator_id,
                "این کانال در فهرست فعلی نیست؛ دوباره انتخاب کنید.",
            )
            return
        try:
            async with self.guard.use(operator_id=operator_id):
                client = await self._open_client()
                try:
                    choice = await find_channel(client, channel_id)
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
        except SessionBusyError:
            await self._send(
                operator_id, "نشست تلگرام مشغول است؛ کمی بعد دوباره تلاش کنید."
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._send(
                operator_id, f"تأیید کانال ممکن نشد ({exc.__class__.__name__})."
            )
            return
        if choice is None:
            await self._send(
                operator_id,
                "کانال دیگر در دسترس نیست؛ فهرست را به‌روزرسانی کنید.",
            )
            return

        # This phase is deliberately read-only: do not change the project's
        # ingestion channel, create an ingestion job, or enqueue anything.
        self.store.set_operator_channel(
            operator_id,
            selected_channel_id=choice.channel_id,
            selected_channel_title=choice.title,
            selected_channel_username=choice.username,
            selected_channel_access_hash=choice.access_hash,
            selection_snapshot_id=snapshot_id,
        )
        self.store.audit(
            "channel_selected_for_preview",
            f"role={self._role(operator_id)}",
        )
        await self._send(
            operator_id,
            f"✅ کانال انتخاب شد: {choice.title} (id={choice.channel_id})\n"
            "این انتخاب فقط برای پیش‌نمایش خواندنی است. هیچ دریافت تاریخچه، "
            "صف پردازش، تحلیل سیگنال یا معامله‌ای آغاز نشد.",
            markup=_kb(
                [
                    [
                        _btn(
                            "👁 پیش‌نمایش ۱۰ پیام آخر",
                            "mg:" + self._token(operator_id, "preview"),
                        )
                    ],
                    [_btn("📡 فهرست کانال‌ها", "mg:" + self._token(operator_id, "refresh"))],
                    [_btn("بازگشت", "mg:" + self._token(operator_id, "status"))],
                ]
            ),
        )

    async def _preview_latest(self, operator_id: int) -> None:
        mine = self.store.operator_channel(operator_id)
        channel_id = int(mine.get("selected_channel_id") or 0)
        if channel_id == 0:
            await self._send(
                operator_id,
                "ابتدا یک کانال را از فهرست انتخاب و تأیید کنید.",
                markup=_kb(
                    [[_btn("📡 فهرست کانال‌ها", "mg:" + self._token(operator_id, "refresh"))]]
                ),
            )
            return
        if self.store.state().get("telegram_state") != "authenticated":
            await self._send(
                operator_id,
                "برای پیش‌نمایش، ابتدا ورود حساب تلگرام را کامل کنید.",
                markup=_kb(
                    [[_btn("🔐 ورود", "mg:" + self._token(operator_id, "auth"))]]
                ),
            )
            return
        title = mine.get("selected_channel_title") or str(channel_id)
        try:
            async with self.guard.use(operator_id=operator_id):
                client = await self._open_client()
                try:
                    choice = await find_channel(client, channel_id)
                    if choice is None:
                        await self._send(
                            operator_id,
                            "این کانال دیگر در دسترس نیست؛ فهرست را به‌روزرسانی کنید.",
                        )
                        return
                    try:
                        entity = await client.get_entity(channel_id)
                    except Exception:  # noqa: BLE001 - fall back to the peer id
                        entity = channel_id
                    messages = await client.get_messages(
                        entity, limit=PREVIEW_LIMIT
                    )
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
        except SessionBusyError:
            await self._send(
                operator_id,
                "نشست تلگرام هم‌اکنون مشغول است؛ کمی بعد دوباره تلاش کنید.",
            )
            return
        except AuthFlowError as exc:
            await self._send(operator_id, f"⚠️ {exc}")
            return
        except FloodWaitError as exc:
            await self._send(
                operator_id,
                f"تلگرام موقتاً محدودیت گذاشته است؛ {int(exc.seconds)} ثانیه صبر کنید.",
            )
            return
        except (RPCError, ConnectionError, OSError) as exc:
            await self._send(
                operator_id,
                f"دریافت پیام‌ها ناموفق بود ({exc.__class__.__name__}).",
            )
            return
        except Exception as exc:  # noqa: BLE001
            await self._send(
                operator_id,
                f"پیش‌نمایش ممکن نشد ({exc.__class__.__name__}).",
            )
            return

        items = list(messages or [])[:PREVIEW_LIMIT]
        if not items:
            body = f"کانال: {title} (id={channel_id})\nهیچ پیام قابل نمایشی وجود ندارد."
        else:
            header = (
                f"👁 پیش‌نمایش {len(items)} پیام آخر\n"
                f"کانال: {title} (id={channel_id})\n"
                "فقط خواندنی؛ رسانه دانلود نشد و محتوا پردازش نشد.\n"
            )
            lines = [format_preview_message(item) for item in items]
            body = header
            for line in lines:
                candidate = "\n\n" + line
                if (
                    utf16_len(body) + utf16_len(candidate)
                    > min(PREVIEW_TOTAL_UTF16, MAX_MESSAGE_UTF16 - 100)
                ):
                    body += "\n\n… ادامه پیام‌ها به دلیل محدودیت طول نمایش داده نشد."
                    break
                body += candidate
        await self._send(
            operator_id,
            body,
            markup=_kb(
                [
                    [
                        _btn(
                            "🔄 به‌روزرسانی پیش‌نمایش",
                            "mg:" + self._token(operator_id, "preview"),
                        )
                    ],
                    [_btn("📡 فهرست کانال‌ها", "mg:" + self._token(operator_id, "refresh"))],
                    [_btn("بازگشت", "mg:" + self._token(operator_id, "status"))],
                ]
            ),
        )
        self.store.audit(
            "latest_messages_previewed",
            f"role={self._role(operator_id)} channel={channel_id} count={len(items)}",
        )

    # ----------------------------------------------------- signal review

    def _review_channel_id(self, operator_id: int) -> int:
        return int(
            self.store.operator_channel(operator_id).get("selected_channel_id") or 0
        )

    def _review_gate(self, operator_id: int) -> tuple[int, str | None]:
        """Common read-only gate: recognised role, selected channel, index
        tables present. Returns (channel_id, error_message); channel_id is
        0 whenever the error message is set."""

        if self._role(operator_id) not in ("owner", "developer"):
            return 0, "این بخش فقط برای نقش‌های مجاز است."
        channel_id = self._review_channel_id(operator_id)
        if channel_id == 0:
            return 0, (
                "ابتدا یک کانال را از فهرست انتخاب و تأیید کنید."
            )
        try:
            ensure_available(self.store)
        except SignalReviewUnavailable:
            return 0, (
                "فهرست سیگنال‌های ساختاریافته هنوز ساخته نشده است؛ ابتدا "
                "اجرای «analyze reindex» لازم است. هیچ داده‌ای تغییر نکرده."
            )
        return channel_id, None

    async def _signal_review_menu(self, operator_id: int, offset: int = 0) -> None:
        channel_id, error = self._review_gate(operator_id)
        if error:
            await self._send(operator_id, error)
            return
        page_size = 8
        page = recent_signals(
            self.repo.db,
            channel_id,
            limit=page_size,
            offset=max(offset, 0),
            newest_first=True,
        )
        rows = page["rows"]
        total = page["total"]
        if total and offset >= total:
            # A stale or out-of-range offset (deleted signals, an old message
            # re-opened later) must never present the channel as empty: snap
            # back to the last page that still has rows.
            last_page_offset = ((max(total, 1) - 1) // page_size) * page_size
            page = recent_signals(
                self.repo.db,
                channel_id,
                limit=page_size,
                offset=last_page_offset,
                newest_first=True,
            )
            rows, offset = page["rows"], last_page_offset
        if not rows:
            await self._send(
                operator_id,
                "برای این کانال هنوز سیگنال ساختاریافته‌ای در فهرست ثبت نشده است.",
                markup=_kb(
                    [
                        [
                            _btn(
                                "📊 وضعیت",
                                "mg:" + self._token(operator_id, "status"),
                            )
                        ]
                    ]
                ),
            )
            return
        body = (
            f"🗂 مرور سیگنال‌های اخیر ({len(rows)} از {total})\n"
            f"کانال: {self.store.operator_channel(operator_id).get('selected_channel_title') or channel_id}"
            f" (id={channel_id})\n"
            "فقط خواندنی: داده‌های استخراج‌شده از پیام‌های آرشیوشده؛ هیچ "
            "سیگنالی سفارش نمی‌سازد و سیگنال‌های شورت هرگز اجرا نمی‌شوند."
        )
        keyboard: list[list[dict[str, str]]] = []
        for signal in rows:
            direction_mark = {"long": "🟢", "short": "🔴"}.get(
                str(signal.get("direction")), "⚪️"
            )
            label = (
                f"{direction_mark} #{_fmt(signal.get('logical_id'))} "
                f"· {signal.get('symbol') or signal.get('symbol_raw') or '؟'}"
            )
            keyboard.append(
                [
                    _btn(
                        label,
                        "mg:"
                        + self._token(
                            operator_id,
                            "signal_view",
                            payload={"logical_id": int(signal["logical_id"])},
                        ),
                    )
                ]
            )
        nav: list[dict[str, str]] = []
        if offset > 0:
            nav.append(
                _btn(
                    "◀️ قبلی",
                    "mg:"
                    + self._token(
                        operator_id, "signals_page",
                        payload={"offset": max(offset - page_size, 0)},
                    ),
                )
            )
        nav.append(
            _btn(
                "🔄 به‌روزرسانی",
                "mg:"
                + self._token(
                    operator_id, "signals_page", payload={"offset": max(offset, 0)}
                ),
            )
        )
        if offset + page_size < total:
            nav.append(
                _btn(
                    "▶️ بعدی",
                    "mg:"
                    + self._token(
                        operator_id, "signals_page", payload={"offset": offset + page_size}
                    ),
                )
            )
        keyboard.append(nav)
        keyboard.append([_btn("📊 وضعیت", "mg:" + self._token(operator_id, "status"))])
        await self._send(operator_id, body, markup=_kb(keyboard))
        self.store.audit(
            "signals_reviewed",
            f"role={self._role(operator_id)} channel={channel_id} shown={len(rows)} total={total}",
        )

    def _signal_nav_keyboard(
        self,
        operator_id: int,
        channel_id: int,
        logical_id: int,
    ) -> dict[str, Any]:
        """Prev/next navigation over the channel's chronological signal list."""

        ids = logical_ids(self.repo.db, channel_id)
        try:
            position = ids.index(int(logical_id))
        except ValueError:
            position = None
        nav_row: list[dict[str, str]] = []
        if position is not None and position > 0:
            nav_row.append(
                _btn(
                    "◀️ سیگنال قبلی",
                    "mg:"
                    + self._token(
                        operator_id, "signal_view", payload={"logical_id": ids[position - 1]}
                    ),
                )
            )
        if position is not None and position < len(ids) - 1:
            nav_row.append(
                _btn(
                    "▶️ سیگنال بعدی",
                    "mg:"
                    + self._token(
                        operator_id, "signal_view", payload={"logical_id": ids[position + 1]}
                    ),
                )
            )
        rows = [nav_row] if nav_row else []
        rows.append(
            [
                _btn(
                    "🗂 فهرست سیگنال‌ها",
                    "mg:" + self._token(operator_id, "signals"),
                ),
                _btn("📊 وضعیت", "mg:" + self._token(operator_id, "status")),
            ]
        )
        return _kb(rows)

    async def _signal_view(self, operator_id: int, logical_id: int) -> None:
        channel_id, error = self._review_gate(operator_id)
        if error:
            await self._send(operator_id, error)
            return
        signal = signal_by_logical_id(self.repo.db, channel_id, logical_id)
        if signal is None:
            await self._send(
                operator_id,
                f"سیگنالی با شناسه #{logical_id} در کانال انتخابی ثبت نشده است.",
            )
            return
        logical = int(signal["logical_id"])
        await self._send(
            operator_id,
            render_signal_detail(channel_id, signal),
            markup=_kb(
                [
                    [
                        _btn(
                            "🔎 داده‌های استخراج‌شده",
                            "mg:"
                            + self._token(
                                operator_id, "signal_extract", payload={"logical_id": logical}
                            ),
                        ),
                        _btn(
                            "💌 پیام‌های مرتبط",
                            "mg:"
                            + self._token(
                                operator_id, "signal_related", payload={"logical_id": logical}
                            ),
                        ),
                    ],
                    [
                        _btn(
                            "📜 پیام اصلی",
                            "mg:"
                            + self._token(
                                operator_id,
                                "signal_original",
                                payload={
                                    "logical_id": logical,
                                    "message_id": int(signal["origin_message_id"]),
                                },
                            ),
                        )
                    ],
                ]
                + self._signal_nav_keyboard(operator_id, channel_id, logical)["inline_keyboard"]
            ),
        )
        self.store.audit(
            "signal_reviewed",
            f"role={self._role(operator_id)} channel={channel_id} logical_id={logical}",
        )

    async def _signal_extract(self, operator_id: int, logical_id: int) -> None:
        channel_id, error = self._review_gate(operator_id)
        if error:
            await self._send(operator_id, error)
            return
        signal = signal_by_logical_id(self.repo.db, channel_id, logical_id)
        if signal is None:
            await self._send(
                operator_id,
                f"سیگنالی با شناسه #{logical_id} در کانال انتخابی ثبت نشده است.",
            )
            return
        await self._send(
            operator_id,
            render_signal_extract(channel_id, signal),
            markup=self._signal_nav_keyboard(operator_id, channel_id, int(logical_id)),
        )

    async def _signal_related(self, operator_id: int, logical_id: int) -> None:
        channel_id, error = self._review_gate(operator_id)
        if error:
            await self._send(operator_id, error)
            return
        messages = related_messages(self.repo.db, channel_id, logical_id)
        if not messages:
            await self._send(
                operator_id,
                f"برای سیگنال #{logical_id} هیچ پیام مرتبطی ثبت نشده است.",
                markup=self._signal_nav_keyboard(operator_id, channel_id, logical_id),
            )
            return
        # Rendering already groups messages into Telegram-safe segments;
        # navigation stays attached to the first and last delivery.
        segments = render_related_messages(channel_id, logical_id, messages)
        keyboard = self._signal_nav_keyboard(operator_id, channel_id, int(logical_id))
        failures: list[int] = []
        for index, segment in enumerate(segments):
            markup = keyboard if index in (0, len(segments) - 1) else None
            sent = await self._send(operator_id, segment, markup=markup)
            if not sent:
                failures.append(index + 1)
        if failures:
            listed = "، ".join(str(n) for n in failures[:10])
            more = "، …" if len(failures) > 10 else ""
            await self._send(
                operator_id,
                f"⚠️ ارسال بخش(های) {listed}{more} از پیام‌های مرتبط ناموفق بود؛ "
                f"مجموعاً {len(segments)} بخش تولید شده بود. دوباره همان دکمه را بزنید.",
            )

    async def _signal_original(self, operator_id: int, logical_id: int, message_id: int) -> None:
        channel_id, error = self._review_gate(operator_id)
        if error:
            await self._send(operator_id, error)
            return
        message = origin_message_text(self.repo.db, channel_id, message_id)
        if message is None:
            await self._send(
                operator_id,
                "پیام اصلی در آرشیو یافت نشد (پیام ممکن است هرگز ذخیره نشده باشد).",
                markup=self._signal_nav_keyboard(operator_id, channel_id, logical_id),
            )
            return
        segments = render_original_message(message, label=f"پیام اصلی سیگنال #{logical_id}")
        keyboard = self._signal_nav_keyboard(operator_id, channel_id, logical_id)
        failures: list[int] = []
        for index, segment in enumerate(segments):
            sent = await self._send(operator_id, segment)
            if not sent:
                failures.append(index + 1)
        if failures:
            listed = "، ".join(str(n) for n in failures[:10])
            more = "، …" if len(failures) > 10 else ""
            await self._send(
                operator_id,
                f"⚠️ ارسال بخش(های) {listed}{more} از پیام اصلی ناموفق بود؛ "
                f"مجموعاً {len(segments)} بخش تولید شده بود. دوباره همان دکمه را بزنید.",
            )
        await self._send(
            operator_id,
            "متن خام دست‌نخورده است؛ هرگز حذف یا بازنویسی نمی‌شود.",
            markup=keyboard,
        )

    async def _raw_review(self, operator_id: int) -> None:
        """Developer-only raw review of the 100 most recent selected-channel messages.

        Read-only by construction: messages are rendered verbatim (order,
        timestamp, edit/forward/reply/media metadata) and delivered in
        Telegram-safe, rate-limited batches to this operator's private chat.
        Nothing is parsed, classified, summarized, filtered or downloaded; no
        database row, audit entry, ingestion job or trading call is produced.
        """

        if self._role(operator_id) != "developer":
            await self._send(operator_id, "این مرحله فقط برای نقش توسعه‌دهنده است.")
            return
        if self.store.state().get("telegram_state") != "authenticated":
            await self._send(
                operator_id,
                "برای مرور خام، ابتدا ورود حساب تلگرام را کامل کنید.",
                markup=_kb([[_btn("🔐 ورود", "mg:" + self._token(operator_id, "auth"))]]),
            )
            return

        async def _send_raw(text: str) -> Any:
            return await asyncio.to_thread(self.bot.send_message, operator_id, text)

        try:
            outcome = await run_raw_review(
                store=self.store,
                guard=self.guard,
                open_client=self._open_client,
                send=_send_raw,
                operator_id=operator_id,
                limit=RAW_REVIEW_LIMIT,
            )
        except NoChannelSelected:
            await self._send(
                operator_id,
                "ابتدا یک کانال را از فهرست انتخاب و تأیید کنید.",
                markup=_kb([[_btn("📡 فهرست کانال‌ها", "mg:" + self._token(operator_id, "refresh"))]]),
            )
            return
        except SessionBusyError:
            await self._send(
                operator_id,
                "نشست تلگرام هم‌اکنون مشغول است؛ کمی بعد دوباره تلاش کنید.",
            )
            return
        except AuthFlowError as exc:
            await self._send(operator_id, f"⚠️ {exc}")
            return
        except ChannelUnavailable:
            await self._send(
                operator_id,
                "این کانال دیگر در دسترس نیست؛ فهرست را به‌روزرسانی کنید.",
            )
            return
        except FloodWaitError as exc:
            await self._send(
                operator_id,
                f"تلگرام موقتاً محدودیت گذاشته است؛ {int(exc.seconds)} ثانیه صبر کنید.",
            )
            return
        except (RPCError, ConnectionError, OSError) as exc:
            await self._send(
                operator_id,
                f"دریافت پیام‌ها ناموفق بود ({exc.__class__.__name__}).",
            )
            return
        except Exception as exc:  # noqa: BLE001 - no stack traces or content in chat
            log.warning("raw review failed (%s)", exc.__class__.__name__)
            await self._send(operator_id, f"مرور خام ممکن نشد ({exc.__class__.__name__}).")
            return
        if outcome.batch_count and not outcome.batches_sent:
            await self._send(
                operator_id,
                "ارسال دسته‌های مرور خام ناموفق بود؛ کمی بعد دوباره تلاش کنید.",
            )

    # --------------------------------------------------------------- logout

    async def _show_logout_confirm(self, operator_id: int) -> None:
        if self.store.state().get("telegram_state") != "authenticated":
            await self._send(
                operator_id,
                "حساب تلگرام در حال حاضر وارد نشده است؛ عملیات خروج لازم نیست.",
            )
            return
        await self._send(
            operator_id,
            "🚪 تأیید خروج از حساب تلگرام\n"
            "این نشست بین هر دو اپراتور مشترک است. با تأیید:\n"
            "• ابتدا تلاش می‌شود نشست در سرور تلگرام واقعاً باطل شود (logOut).\n"
            "• فقط در صورت تأیید تلگرام، فایل نشست محلی حذف می‌شود.\n"
            "• اگر تلگرام تأیید نکند یا شبکه قطع باشد، نشست دست‌نخورده می‌ماند و خطا گزارش می‌شود.\n"
            "• تاریخچه کانال، تنظیمات، توکن ربات و پایگاه‌داده حذف نمی‌شوند.\n"
            "• پس از خروج، ربات مدیریت همچنان فعال است و اپراتور بعدی می‌تواند وارد شود.\n\n"
            "آیا خروج را تأیید می‌کنید؟",
            markup=_kb(
                [
                    [
                        _btn(
                            "✅ تأیید خروج",
                            "mg:" + self._token(operator_id, "logout_confirm"),
                        )
                    ],
                    [_btn("انصراف", "mg:" + self._token(operator_id, "status"))],
                ]
            ),
        )

    async def _perform_logout(self, operator_id: int) -> None:
        try:
            async with self.guard.use(operator_id=operator_id):
                manager = TelegramLoginManager(
                    self.cfg, client_factory=self._telethon_factory
                )
                result = await manager.logout()
        except SessionBusyError:
            await self._send(
                operator_id,
                "نشست تلگرام هم‌اکنون مشغول است؛ خروج انجام نشد.",
            )
            return
        if not (result.server_revoked or result.local_removed):
            await self._send(
                operator_id,
                f"⚠️ خروج انجام نشد. {result.detail}",
            )
            self.store.audit(
                "telegram_logout_failed",
                f"role={self._role(operator_id)} server_revoked=0",
            )
            return
        self.store.clear_all_flows()
        self.store.invalidate_callbacks(None)
        self.store.update_state(
            telegram_state="unauthenticated",
            telegram_label=None,
            telegram_authorized_at=None,
        )
        self.store.audit(
            "telegram_logout_completed",
            f"role={self._role(operator_id)} server_revoked={int(result.server_revoked)}",
        )
        await self._send(
            operator_id,
            f"✅ خروج انجام شد. {result.detail}\n"
            "نشست حساب تلگرام دیگر معتبر نیست. ربات مدیریت فعال است و اپراتور بعدی "
            "می‌تواند ورود خود را آغاز کند.",
        )
        other = next((op for op in self.operators if op != operator_id), None)
        if other is not None:
            await self._send(
                other,
                "ℹ️ نشست حساب تلگرام توسط اپراتور مجاز دیگر خارج شد و دیگر معتبر نیست. "
                "ربات مدیریت فعال است؛ برای ادامه می‌توانید ورود خود را آغاز کنید.",
            )

    # ------------------------------------------------------- validation flow

    async def _show_validation_request(self, operator_id: int) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "این مرحله فقط برای نقش توسعه‌دهنده است.")
            return
        if not self.store.validation_available():
            await self._send(
                operator_id,
                "ساختار اعتبارسنجی در پایگاه‌داده آماده نیست؛ هیچ تغییری انجام نشد. "
                "ابتدا مهاجرت تأییدشده را جداگانه اجرا کنید.",
            )
            return
        active = self.store.active_import_request()
        if active is not None:
            await self._show_validation_status(operator_id)
            return
        details = self.store.selected_channel_details(operator_id)
        if not details.get("selected_channel_id"):
            await self._send(
                operator_id,
                "ابتدا یک کانال را انتخاب و تأیید کنید.",
                markup=_kb(
                    [[_btn("📡 فهرست کانال‌ها", "mg:" + self._token(operator_id, "refresh"))]]
                ),
            )
            return
        title = str(details.get("selected_channel_title") or "کانال انتخاب‌شده")
        rows = [
            [
                _btn(
                    f"{limit} پیام آخر",
                    "mg:"
                    + self._token(
                        operator_id,
                        "validation_request_confirm",
                        payload={"max_messages": limit},
                    ),
                )
            ]
            for limit in VALIDATION_LIMITS
        ]
        rows.append(
            [_btn("بازگشت", "mg:" + self._token(operator_id, "status"))]
        )
        await self._send(
            operator_id,
            "🧪 درخواست واردسازی محدود\n"
            f"کانال: {title}\n"
            "اندازه نمونه را انتخاب کنید. درخواست محدود به‌صورت خودکار توسط "
            "نقش توسعه‌دهنده مجاز می‌شود و مالک فقط مطلع می‌شود؛ هیچ واردسازی "
            "خودکار یا نامحدودی آغاز نمی‌شود.",
            markup=_kb(rows),
        )

    async def _create_validation_request(
        self, operator_id: int, max_messages: int
    ) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "این مرحله فقط برای نقش توسعه‌دهنده است.")
            return
        if not self.store.validation_available():
            await self._send(
                operator_id,
                "ساختار اعتبارسنجی در پایگاه‌داده آماده نیست؛ هیچ تغییری انجام نشد.",
            )
            return
        if max_messages not in VALIDATION_LIMITS:
            await self._send(operator_id, "اندازه نمونه معتبر نیست.")
            return
        details = self.store.selected_channel_details(operator_id)
        channel_id = int(details.get("selected_channel_id") or 0)
        if channel_id == 0:
            await self._send(operator_id, "کانال انتخاب‌شده در دسترس نیست.")
            return
        title = str(details.get("selected_channel_title") or "کانال انتخاب‌شده")
        try:
            request = self.store.create_import_request(
                channel_id=channel_id,
                channel_title=title,
                channel_access_hash=details.get("access_hash"),
                selection_snapshot_id=details.get("selection_snapshot_id"),
                max_messages=max_messages,
                requested_by=operator_id,
                self_authorized_by=operator_id,
            )
        except ValueError:
            await self._send(
                operator_id,
                "یک درخواست اعتبارسنجی فعال وجود دارد؛ ابتدا آن را تعیین تکلیف کنید.",
            )
            return
        self.store.audit(
            "validation_import_requested",
            f"role=developer request={request['request_id']} limit={max_messages}",
        )
        self.store.audit(
            "validation_import_self_authorized",
            f"role=developer request={request['request_id']} "
            f"limit={max_messages} scope=bounded",
        )
        await self._send(
            self.owner_id,
            "ℹ️ توسعه‌دهنده یک درخواست واردسازی محدود را خودمجاز کرد.\n"
            f"کانال: {title}\n"
            f"حداکثر پیام: {max_messages}\n\n"
            "این مجوز فقط برای یک نمونه محدود و یک‌بارمصرف است؛ هیچ اقدام یا "
            "تأییدی از طرف مالک لازم نیست. دریافت هنوز آغاز نشده است.",
        )
        await self._send(
            operator_id,
            "✅ درخواست واردسازی محدود ثبت و خودمجاز شد.\n"
            f"حداکثر پیام: {max_messages}\n\n"
            "برای شروع، دکمه زیر را بزنید. این مجوز یک‌بارمصرف و محدود است.",
            markup=_kb(
                [
                    [
                        _btn(
                            "▶️ شروع واردسازی محدود",
                            "mg:"
                            + self._token(
                                operator_id,
                                "validation_start",
                                payload={"request_id": request["request_id"]},
                            ),
                        )
                    ],
                    [
                        _btn(
                            "وضعیت",
                            "mg:" + self._token(operator_id, "validation_status"),
                        )
                    ],
                ]
            ),
        )

    async def _decide_validation_request(
        self, operator_id: int, request_id: str, approve: bool
    ) -> None:
        # Legacy owner-only path for requests created before developer
        # self-authorization. New bounded requests are approved at creation and
        # never require this callback; unrelated owner-only controls are also
        # untouched.
        if self._role(operator_id) != "owner":
            await self._send(operator_id, "تصمیم درباره این درخواست فقط با مالک است.")
            return
        if not self.store.validation_available():
            await self._send(operator_id, "ساختار اعتبارسنجی آماده نیست.")
            return
        status, request = self.store.decide_import_request(
            request_id, approved=approve, decided_by=operator_id
        )
        if request is None:
            await self._send(operator_id, "درخواست پیدا نشد.")
            return
        if status != "ok":
            await self._send(
                operator_id,
                "این درخواست دیگر در انتظار تصمیم نیست: "
                + REQUEST_STATUS_FA.get(status, status),
            )
            return
        self.store.audit(
            "validation_import_approved" if approve else "validation_import_denied",
            f"role=owner request={request_id} limit={int(request['max_messages'])}",
        )
        if not approve:
            await self._send(
                self.developer_id,
                "❌ مالک درخواست واردسازی محدود را رد کرد.",
            )
            await self._send(operator_id, "درخواست رد شد.")
            return
        await self._send(
            self.developer_id,
            "✅ مالک واردسازی محدود را تأیید کرد. تأیید به‌تنهایی دریافتی را "
            "آغاز نکرد. برای شروع، دکمه زیر را بزنید.",
            markup=_kb(
                [
                    [
                        _btn(
                            "▶️ شروع واردسازی محدود",
                            "mg:"
                            + self._token(
                                self.developer_id,
                                "validation_start",
                                payload={"request_id": request_id},
                            ),
                        )
                    ]
                ]
            ),
        )
        await self._send(
            operator_id,
            "تأیید ثبت شد. دریافت هنوز آغاز نشده است؛ توسعه‌دهنده باید آن را "
            "جداگانه شروع کند.",
        )

    async def _start_validation_import(
        self, operator_id: int, request_id: str
    ) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "شروع واردسازی فقط با توسعه‌دهنده است.")
            return
        if not self.store.validation_available():
            await self._send(
                operator_id,
                "ساختار اعتبارسنجی آماده نیست؛ هیچ دریافتی آغاز نشد.",
            )
            return
        request = self.store.get_import_request(request_id)
        if request is None:
            await self._send(operator_id, "درخواست پیدا نشد.")
            return
        if request["status"] != "approved":
            await self._send(
                operator_id,
                "این درخواست قابل شروع نیست: "
                + REQUEST_STATUS_FA.get(str(request["status"]), str(request["status"])),
            )
            return
        if self._worker_task is not None and not self._worker_task.done():
            await self._send(operator_id, "یک دریافت دیگر در حال اجراست.")
            return
        details = self.store.selected_channel_details(operator_id)
        if (
            not details.get("selected_channel_id")
            or int(details["selected_channel_id"]) != int(request["channel_id"])
        ):
            self.store.finish_import_request(
                request_id,
                "cancelled",
                error="channel selection changed before start",
            )
            self.store.audit(
                "validation_import_cancelled",
                f"role=developer request={request_id} reason=selection_changed",
            )
            await self._send(
                operator_id,
                "کانال انتخاب‌شده از زمان درخواست تغییر کرده است؛ درخواست باطل شد "
                "و هیچ دریافتی آغاز نشد.",
            )
            return
        try:
            self.repo.ensure_channel(
                int(request["channel_id"]),
                int(request.get("channel_access_hash") or 0),
                details.get("selected_channel_username"),
                str(request["channel_title"]),
            )
        except Exception as exc:  # noqa: BLE001
            await self._send(
                operator_id,
                f"ثبت کانال برای واردسازی ممکن نشد ({exc.__class__.__name__}).",
            )
            return
        status, updated_request, job = self.store.start_import_request(request_id)
        if status != "ok" or updated_request is None or job is None:
            messages = {
                "selection_changed": "کانال انتخاب‌شده تغییر کرده است؛ درخواست باطل شد.",
                "active_job": "یک دریافت دیگر فعال است؛ ابتدا آن را تعیین تکلیف کنید.",
                "missing": "درخواست پیدا نشد.",
            }
            await self._send(
                operator_id,
                messages.get(
                    status,
                    "این درخواست دیگر قابل شروع نیست: "
                    + REQUEST_STATUS_FA.get(status, status),
                ),
            )
            return
        self.store.audit(
            "validation_import_started",
            f"role=developer request={request_id} limit={int(updated_request['max_messages'])}",
        )
        await self._send(
            self.owner_id,
            "▶️ توسعه‌دهنده واردسازی محدود مجاز را آغاز کرد. "
            "این مرحله فقط پیام‌های نمونه را ذخیره می‌کند و هیچ معامله یا اتصال "
            "به MEXC انجام نمی‌شود.",
        )
        await self._send(
            operator_id,
            "▶️ واردسازی محدود آغاز شد. پس از پایان، کاندیدهای parser برای "
            "مرور انسانی نمایش داده می‌شوند.",
        )
        await self._start_worker(str(job["job_id"]))

    async def _cancel_validation_request(
        self, operator_id: int, request_id: str
    ) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "لغو درخواست فقط با توسعه‌دهنده است.")
            return
        request = self.store.get_import_request(request_id)
        if request is None:
            await self._send(operator_id, "درخواست پیدا نشد.")
            return
        if request["status"] not in ("requested", "approved"):
            await self._send(
                operator_id,
                "این درخواست در وضعیت قابل لغو نیست: "
                + REQUEST_STATUS_FA.get(str(request["status"]), str(request["status"])),
            )
            return
        self.store.finish_import_request(
            request_id, "cancelled", error="cancelled by developer before start"
        )
        self.store.audit(
            "validation_import_cancelled",
            f"role=developer request={request_id} reason=withdrawn",
        )
        await self._send(operator_id, "درخواست لغو شد؛ هیچ دریافتی آغاز نشد.")
        await self._send(self.owner_id, "ℹ️ توسعه‌دهنده درخواست اعتبارسنجی را لغو کرد.")

    async def _show_validation_status(self, operator_id: int) -> None:
        if not self.store.validation_available():
            await self._send(operator_id, "ساختار اعتبارسنجی آماده نیست.")
            return
        request = self.store.latest_import_request()
        if request is None:
            await self._send(
                operator_id,
                "درخواست اعتبارسنجی وجود ندارد.",
                markup=_kb(
                    [[_btn("بازگشت", "mg:" + self._token(operator_id, "status"))]]
                ),
            )
            return
        status = str(request["status"])
        lines = [
            "🧪 وضعیت اعتبارسنجی محدود",
            f"کانال: {request['channel_title']}",
            f"وضعیت: {REQUEST_STATUS_FA.get(status, status)}",
            f"حداکثر پیام: {int(request['max_messages'])}",
            f"مرور: {int(request['reviewed_count'] or 0)}/{int(request['candidate_count'] or 0)}",
        ]
        if request.get("error"):
            lines.append(f"آخرین وضعیت: {str(request['error'])[:300]}")
        rows: list[list[dict[str, str]]] = []
        role = self._role(operator_id)
        if role == "owner" and status == "requested":
            rows.append(
                [
                    _btn(
                        "✅ تأیید محدود",
                        "mg:"
                        + self._token(
                            operator_id,
                            "validation_decide",
                            payload={
                                "request_id": request["request_id"],
                                "approve": True,
                            },
                        ),
                    ),
                    _btn(
                        "❌ رد درخواست",
                        "mg:"
                        + self._token(
                            operator_id,
                            "validation_decide",
                            payload={
                                "request_id": request["request_id"],
                                "approve": False,
                            },
                        ),
                    ),
                ]
            )
        if role == "developer":
            if status == "approved":
                rows.append(
                    [
                        _btn(
                            "▶️ شروع واردسازی محدود",
                            "mg:"
                            + self._token(
                                operator_id,
                                "validation_start",
                                payload={"request_id": request["request_id"]},
                            ),
                        )
                    ]
                )
                rows.append(
                    [
                        _btn(
                            "لغو درخواست",
                            "mg:"
                            + self._token(
                                operator_id,
                                "validation_cancel",
                                payload={"request_id": request["request_id"]},
                            ),
                        )
                    ]
                )
            elif status == "requested":
                rows.append(
                    [
                        _btn(
                            "لغو درخواست",
                            "mg:"
                            + self._token(
                                operator_id,
                                "validation_cancel",
                                payload={"request_id": request["request_id"]},
                            ),
                        )
                    ]
                )
            elif status in ("review", "completed"):
                rows.append(
                    [
                        _btn(
                            "🔎 مرور کاندیدها",
                            "mg:"
                            + self._token(
                                operator_id,
                                "validation_review",
                                payload={"request_id": request["request_id"], "index": 0},
                            ),
                        )
                    ]
                )
            if status not in REQUEST_ACTIVE_STATUSES:
                rows.append(
                    [
                        _btn(
                            "🧪 درخواست تازه",
                            "mg:"
                            + self._token(operator_id, "validation_request"),
                        )
                    ]
                )
        rows.append([_btn("بازگشت", "mg:" + self._token(operator_id, "status"))])
        await self._send(operator_id, "\n".join(lines), markup=_kb(rows))

    async def _show_candidate_page(
        self, operator_id: int, request_id: str, index: int
    ) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "مرور کاندیدها فقط با توسعه‌دهنده است.")
            return
        if not self.store.validation_available():
            await self._send(operator_id, "ساختار تحلیل آماده نیست.")
            return
        request = self.store.get_import_request(request_id)
        if request is None or request["status"] not in ("review", "completed"):
            await self._send(operator_id, "این درخواست آماده مرور نیست.")
            return
        min_id = request.get("imported_min_id")
        max_id = request.get("imported_max_id")
        if min_id is None or max_id is None:
            await self._send(operator_id, "بازه پیام‌های این نمونه مشخص نیست.")
            return
        analysis = AnalysisStore(self.repo.db)
        candidates = analysis.list_candidates_in_range(
            int(request["channel_id"]), int(min_id), int(max_id)
        )
        if not candidates:
            await self._send(
                operator_id,
                "برای این نمونه کاندیدی ذخیره نشده است.",
                markup=_kb(
                    [[_btn("وضعیت", "mg:" + self._token(operator_id, "validation_status"))]]
                ),
            )
            return
        index = max(0, min(int(index), len(candidates) - 1))
        candidate = candidates[index]
        message = self.repo.get_message_raw(
            int(request["channel_id"]), int(candidate["message_id"])
        )
        text = _review_text(request, message, candidate, index, len(candidates))
        rows: list[list[dict[str, str]]] = [
            [
                _btn(
                    "✅ تأیید",
                    "mg:"
                    + self._token(
                        operator_id,
                        "review_confirm",
                        payload={
                            "request_id": request_id,
                            "candidate_id": candidate["candidate_id"],
                            "index": index,
                        },
                    ),
                ),
                _btn(
                    "✏️ اصلاح",
                    "mg:"
                    + self._token(
                        operator_id,
                        "review_correct",
                        payload={
                            "request_id": request_id,
                            "candidate_id": candidate["candidate_id"],
                            "index": index,
                        },
                    ),
                ),
                _btn(
                    "❌ رد",
                    "mg:"
                    + self._token(
                        operator_id,
                        "review_reject",
                        payload={
                            "request_id": request_id,
                            "candidate_id": candidate["candidate_id"],
                            "index": index,
                        },
                    ),
                ),
            ]
        ]
        nav: list[dict[str, str]] = []
        if index > 0:
            nav.append(
                _btn(
                    "قبلی",
                    "mg:"
                    + self._token(
                        operator_id,
                        "validation_review",
                        payload={"request_id": request_id, "index": index - 1},
                    ),
                )
            )
        if index + 1 < len(candidates):
            nav.append(
                _btn(
                    "بعدی",
                    "mg:"
                    + self._token(
                        operator_id,
                        "validation_review",
                        payload={"request_id": request_id, "index": index + 1},
                    ),
                )
            )
        if nav:
            rows.append(nav)
        rows.append(
            [_btn("وضعیت", "mg:" + self._token(operator_id, "validation_status"))]
        )
        await self._send(operator_id, text, markup=_kb(rows))

    def _candidate_in_request(
        self, request: dict[str, Any], candidate: dict[str, Any]
    ) -> bool:
        min_id = request.get("imported_min_id")
        max_id = request.get("imported_max_id")
        if min_id is None or max_id is None:
            return False
        return (
            int(candidate["channel_id"]) == int(request["channel_id"])
            and int(min_id) <= int(candidate["message_id"]) <= int(max_id)
        )

    async def _apply_review(
        self,
        operator_id: int,
        request_id: str,
        candidate_id: str,
        decision: str,
        *,
        corrected: dict[str, Any] | None = None,
    ) -> tuple[int, int] | None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "ثبت تصمیم فقط با توسعه‌دهنده است.")
            return None
        request = self.store.get_import_request(request_id)
        if request is None or request["status"] not in ("review", "completed"):
            await self._send(operator_id, "این درخواست آماده مرور نیست.")
            return None
        analysis = AnalysisStore(self.repo.db)
        candidate = analysis.get_candidate(candidate_id)
        if candidate is None or not self._candidate_in_request(request, candidate):
            await self._send(operator_id, "کاندید متعلق به این نمونه نیست.")
            return None
        try:
            analysis.add_review(
                candidate_id,
                reviewer="developer",
                decision=decision,
                corrected=corrected,
            )
        except (AnalysisError, KeyError, ValueError) as exc:
            log.warning("candidate review rejected (%s)", exc.__class__.__name__)
            await self._send(
                operator_id,
                "ثبت تصمیم ممکن نشد؛ مقدار اصلاح یا وضعیت کاندید را بررسی کنید.",
            )
            return None
        total = analysis.count_candidates_in_range(
            int(request["channel_id"]),
            int(request["imported_min_id"]),
            int(request["imported_max_id"]),
        )
        reviewed = analysis.count_candidates_in_range(
            int(request["channel_id"]),
            int(request["imported_min_id"]),
            int(request["imported_max_id"]),
            reviewed_only=True,
        )
        self.store.update_import_review_progress(
            request_id, reviewed_count=reviewed, candidate_count=total
        )
        self.store.audit(
            "candidate_review_recorded",
            f"role=developer request={request_id} candidate={candidate_id} decision={decision}",
        )
        if total and reviewed >= total and request["status"] != "completed":
            self.store.finish_import_request(request_id, "completed")
            self.store.audit(
                "validation_review_completed",
                f"role=developer request={request_id} reviewed={reviewed}",
            )
            await self._send(
                self.owner_id,
                "✅ توسعه‌دهنده مرور همه کاندیدهای نمونه محدود را تکمیل کرد. "
                "هیچ معامله یا اتصال به MEXC انجام نشد.",
            )
        return reviewed, total

    async def _review_candidate(
        self,
        operator_id: int,
        request_id: str,
        candidate_id: str,
        index: int,
        decision: str,
    ) -> None:
        result = await self._apply_review(
            operator_id, request_id, candidate_id, decision
        )
        if result is None:
            return
        total = result[1]
        next_index = index + 1 if index + 1 < total else index
        await self._show_candidate_page(operator_id, request_id, next_index)

    async def _begin_correction(
        self, operator_id: int, request_id: str, candidate_id: str, index: int
    ) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "اصلاح کاندید فقط با توسعه‌دهنده است.")
            return
        request = self.store.get_import_request(request_id)
        if request is None or request["status"] not in ("review", "completed"):
            await self._send(operator_id, "این درخواست آماده مرور نیست.")
            return
        candidate = AnalysisStore(self.repo.db).get_candidate(candidate_id)
        if candidate is None or not self._candidate_in_request(request, candidate):
            await self._send(operator_id, "کاندید متعلق به این نمونه نیست.")
            return
        self.store.set_review_draft(
            operator_id,
            request_id=request_id,
            candidate_id=candidate_id,
            page_index=index,
            fields={},
        )
        text = "\n".join(
            [
                "✏️ اصلاح کاندید",
                f"پیام #{int(candidate['message_id'])}",
                "",
                "هر خط را به شکل field=value بفرستید. فیلدهای مجاز:",
                "classification, market_type, symbol, quote_asset, side,",
                "entry_price, entry_low_price, entry_high_price, stop_loss, targets",
                "",
                "نمونه:",
                "symbol=ETHUSDT",
                "side=buy",
                "entry_price=123.45",
                "targets=130,140",
            ]
        )
        await self._send(
            operator_id,
            text,
            markup=_kb(
                [
                    [
                        _btn(
                            "لغو اصلاح",
                            "mg:" + self._token(operator_id, "review_correct_cancel"),
                        )
                    ]
                ]
            ),
        )

    async def _consume_correction_input(self, operator_id: int, text: str) -> None:
        draft = self.store.get_review_draft(operator_id)
        if draft is None:
            return
        try:
            parsed = _parse_correction_text(text)
            merged = {**draft.get("fields", {}), **parsed}
            corrected = validate_correction(merged)
        except ValueError as exc:
            await self._send(operator_id, f"⚠️ {exc}")
            return
        except AnalysisError:
            await self._send(
                operator_id,
                "⚠️ مقدار یکی از فیلدها معتبر نیست؛ مقدار را اصلاح کنید. "
                "هیچ تصمیمی ثبت نشد.",
            )
            return
        if not corrected:
            await self._send(operator_id, "⚠️ هیچ فیلد معتبری ارسال نشد.")
            return
        self.store.set_review_draft(
            operator_id,
            request_id=str(draft["request_id"]),
            candidate_id=str(draft["candidate_id"]),
            page_index=int(draft["page_index"]),
            fields=corrected,
        )
        await self._send(
            operator_id,
            "پیش‌نمایش اصلاح:\n"
            f"{_correction_preview(corrected)}\n\n"
            "در صورت تأیید، این مقادیر به‌صورت یک رکورد حسابرسی append-only "
            "ثبت می‌شوند و فیلدهای ماشینی تغییر نمی‌کنند.",
            markup=_kb(
                [
                    [
                        _btn(
                            "💾 ثبت اصلاح",
                            "mg:" + self._token(operator_id, "review_correct_save"),
                        )
                    ],
                    [
                        _btn(
                            "لغو اصلاح",
                            "mg:" + self._token(operator_id, "review_correct_cancel"),
                        )
                    ],
                ]
            ),
        )

    async def _save_correction(self, operator_id: int) -> None:
        if self._role(operator_id) != "developer":
            await self._send(operator_id, "ثبت اصلاح فقط با توسعه‌دهنده است.")
            return
        draft = self.store.get_review_draft(operator_id)
        if draft is None:
            await self._send(operator_id, "جریان اصلاح منقضی یا لغو شده است.")
            return
        try:
            corrected = validate_correction(draft.get("fields", {}))
        except AnalysisError:
            await self._send(operator_id, "مقادیر اصلاح معتبر نیستند؛ دوباره تلاش کنید.")
            return
        if not corrected:
            await self._send(operator_id, "هیچ فیلد اصلاحی وجود ندارد.")
            return
        request_id = str(draft["request_id"])
        index = int(draft["page_index"])
        result = await self._apply_review(
            operator_id,
            request_id,
            str(draft["candidate_id"]),
            "correct",
            corrected=corrected,
        )
        if result is None:
            return
        self.store.clear_review_draft(operator_id)
        total = result[1]
        next_index = index + 1 if index + 1 < total else index
        await self._show_candidate_page(operator_id, request_id, next_index)

    # -------------------------------------------------------------- ingestion

    async def _start_worker(self, job_id: str) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            return
        if self._worker_task is not None and not self._worker_task.done():
            if self._worker_job_id == job_id:
                return
            await self._notify_all(
                "یک دریافت دیگر در حال اجراست؛ ابتدا آن را لغو کنید."
            )
            return
        self._worker_job_id = job_id
        self._worker_task = asyncio.create_task(self._run_worker(job_id))
        self._worker_task.add_done_callback(self._worker_done)

    def _worker_done(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.error("management worker task failed (%s)", exc.__class__.__name__)

    async def _run_worker(self, job_id: str) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            return
        client = None
        try:
            client = await self._open_client()
            self._stop_event = asyncio.Event()
            report = await self.worker.run(
                client,
                job_id,
                on_progress=self._progress_notice,
                stop_event=self._stop_event,
            )
            current = self.store.get_job(job_id)
            if current is not None and current.get("request_id"):
                await self._finish_validation_import(job_id, report)
            else:
                await self._completion_notice(job_id, report)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            current = self.store.get_job(job_id)
            request_id = current.get("request_id") if current else None
            if current and current["status"] in ("pending", "running"):
                self.store.finish_job(
                    job_id,
                    "interrupted",
                    error=f"worker connection failed ({exc.__class__.__name__})",
                    checkpoint_next_max_id=current.get("checkpoint_next_max_id"),
                )
            if request_id:
                self.store.finish_import_request(
                    str(request_id),
                    "failed",
                    job_id=job_id,
                    error=f"worker failed ({exc.__class__.__name__})",
                )
                self.store.audit(
                    "validation_import_failed",
                    f"role=developer request={request_id} error={exc.__class__.__name__}",
                )
                await self._notify_all(
                    "⚠️ واردسازی محدود متوقف شد. پیام‌های ذخیره‌شده حذف نشدند؛ "
                    "برای ادامه یک درخواست تازه لازم است."
                )
            else:
                await self._notify_all(
                    f"⚠️ دریافت متوقف شد ({exc.__class__.__name__}); checkpoint حفظ شد. "
                    "از دکمه ادامه استفاده کنید."
                )
        finally:
            if client is not None:
                with contextlib.suppress(Exception):
                    await client.disconnect()
            self._worker_task = None
            self._worker_job_id = None
            self._stop_event = None

    async def _progress_notice(self, job: dict[str, Any]) -> None:
        if job.get("request_id"):
            await self._send(
                self.developer_id,
                "⏳ پیشرفت واردسازی محدود\n"
                f"ذخیره‌شده: {int(job.get('total_stored') or 0)} پیام\n"
                f"جدید: {int(job.get('inserted') or 0)} — "
                f"به‌روزرسانی: {int(job.get('updated') or 0)}"
                f" — خطا: {int(job.get('failed') or 0)}",
            )
            return
        await self._notify_all(
            "⏳ پیشرفت دریافت تاریخچه\n"
            f"کانال: {job.get('channel_title')} (id={job.get('channel_id')})\n"
            f"ذخیره‌شده: {int(job.get('total_stored') or 0)} پیام\n"
            f"جدید: {int(job.get('inserted') or 0)} — به‌روزرسانی: {int(job.get('updated') or 0)}"
            f" — خطا: {int(job.get('failed') or 0)}\n"
            f"checkpoint: {_fmt(job.get('checkpoint_next_max_id'))}"
        )

    async def _finish_validation_import(
        self, job_id: str, report: IngestReport
    ) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            return
        request_id = job.get("request_id")
        if not request_id:
            return
        request = self.store.get_import_request(str(request_id))
        if request is None:
            return
        if report.status == "cancelled":
            if request["status"] == "cancelled":
                return
            self.store.finish_import_request(
                str(request_id),
                "cancelled",
                job_id=job_id,
                error=report.error or "cancelled",
            )
            self.store.audit(
                "validation_import_cancelled",
                f"role=developer request={request_id} reason=worker_cancelled",
            )
            await self._notify_all(
                "⏹ واردسازی محدود لغو شد. پیام‌های ذخیره‌شده حذف نشدند و "
                "برای ادامه یک درخواست تازه لازم است."
            )
            return
        min_id = job.get("oldest_seen_id")
        max_id = job.get("newest_seen_id")
        stored = report.inserted + report.updated + report.unchanged
        if report.status not in ("completed", "partial") or min_id is None or max_id is None or stored == 0:
            error = report.error or "bounded import stored no messages"
            self.store.finish_import_request(
                str(request_id), "failed", job_id=job_id, error=error
            )
            self.store.audit(
                "validation_import_failed",
                f"role=developer request={request_id} status={report.status}",
            )
            await self._notify_all(
                "⚠️ واردسازی محدود کامل نشد: "
                + str(error)[:180]
                + "\nپیام‌های ذخیره‌شده حذف نشدند؛ برای ادامه یک درخواست تازه لازم است."
            )
            return
        try:
            stats = run_replay(
                self.repo,
                ReplayScope(
                    channel_id=int(job["channel_id"]),
                    message_id_min=int(min_id),
                    message_id_max=int(max_id),
                ),
            )
        except Exception as exc:  # noqa: BLE001
            error = f"offline analysis failed ({exc.__class__.__name__})"
            self.store.finish_import_request(
                str(request_id), "failed", job_id=job_id, error=error
            )
            self.store.audit(
                "validation_analysis_failed",
                f"role=developer request={request_id} error={exc.__class__.__name__}",
            )
            await self._notify_all(
                "⚠️ پیام‌ها ذخیره شدند اما تحلیل آفلاین کامل نشد. "
                "هیچ معامله‌ای انجام نشد؛ برای تلاش دوباره درخواست تازه لازم است."
            )
            return
        analysis = AnalysisStore(self.repo.db)
        candidate_count = analysis.count_candidates_in_range(
            int(job["channel_id"]), int(min_id), int(max_id)
        )
        if candidate_count == 0:
            self.store.finish_import_request(
                str(request_id),
                "failed",
                job_id=job_id,
                imported_min_id=int(min_id),
                imported_max_id=int(max_id),
                error="offline analysis produced no candidates",
            )
            await self._notify_all(
                "⚠️ پیام‌ها ذخیره شدند اما کاندیدی برای مرور تولید نشد. "
                "هیچ معامله‌ای انجام نشد."
            )
            return
        reviewed = analysis.count_candidates_in_range(
            int(job["channel_id"]),
            int(min_id),
            int(max_id),
            reviewed_only=True,
        )
        self.store.update_import_review_progress(
            str(request_id),
            reviewed_count=reviewed,
            candidate_count=candidate_count,
        )
        self.store.finish_import_request(
            str(request_id),
            "review",
            job_id=job_id,
            imported_min_id=int(min_id),
            imported_max_id=int(max_id),
            candidate_count=candidate_count,
            error=report.error if report.status == "partial" else None,
        )
        self.store.audit(
            "validation_analysis_ready",
            f"role=developer request={request_id} candidates={candidate_count} "
            f"needs_review={stats.needs_review} failures={stats.failures}",
        )
        await self._send(
            self.developer_id,
            "✅ واردسازی محدود و تحلیل آفلاین تمام شد.\n"
            f"کاندیدها: {candidate_count} — نیازمند مرور: {stats.needs_review} — "
            f"خطای تحلیل: {stats.failures}\n"
            "هیچ معامله یا اتصال به MEXC انجام نشد.",
            markup=_kb(
                [
                    [
                        _btn(
                            "🔎 مرور کاندیدها",
                            "mg:"
                            + self._token(
                                self.developer_id,
                                "validation_review",
                                payload={"request_id": request_id, "index": 0},
                            ),
                        )
                    ]
                ]
            ),
        )
        await self._send(
            self.owner_id,
            "✅ واردسازی محدود و تحلیل آفلاین تمام شد. "
            f"تعداد کاندید برای مرور توسعه‌دهنده: {candidate_count}. "
            "هیچ معامله یا اتصال به MEXC انجام نشد.",
        )

    async def _completion_notice(self, job_id: str, report: IngestReport) -> None:
        job = self.store.get_job(job_id)
        if job is None:
            return
        counts = self.repo.message_counts(int(job["channel_id"]))
        if report.status == "completed":
            headline = "✅ دریافت تاریخچه کامل به پایان رسید."
        elif report.status == "partial":
            headline = "⚠️ دریافت ناقص متوقف شد؛ برخی پیام‌ها ذخیره نشدند."
        elif report.status == "cancelled":
            headline = "⏹ دریافت لغو شد؛ پیام‌های ذخیره‌شده و checkpoint حفظ شدند."
        else:
            headline = "⚠️ دریافت کامل نشد؛ checkpoint برای ادامه حفظ شد."
        await self._notify_all(
            headline
            + "\n"
            + f"کانال: {job['channel_title']} (id={job['channel_id']})\n"
            + f"پیام‌های یکتا در دیتابیس: {counts.total}\n"
            + f"وضعیت: {_job_fa(report.status)}\n"
            + f"پیام‌های دارای رسانه (فقط متادیتا): {counts.with_media}\n"
            + (f"خطا: {report.error}\n" if report.error else "")
            + f"زمان: {utc_now()}\n\n"
            + "این مرحله فقط ذخیره‌سازی است؛ پردازش سیگنال، معامله و سفارش فعال نشده است."
        )

    async def _cancel_job(self, operator_id: int | None = None) -> None:
        job = self.store.active_job() or self.store.latest_job()
        if job is None:
            await self._notify_all("دریافت فعالی وجود ندارد.")
            return
        request_id = job.get("request_id")
        self.store.request_cancel(str(job["job_id"]))
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._worker_task
        updated = self.store.get_job(str(job["job_id"]))
        if updated and updated["status"] in ("pending", "running", "interrupted"):
            updated = self.store.finish_job(
                str(job["job_id"]),
                "cancelled",
                error="cancelled by operator; checkpoint preserved",
                checkpoint_next_max_id=updated.get("checkpoint_next_max_id"),
            )
        self._worker_task = None
        self._worker_job_id = None
        if request_id:
            self.store.finish_import_request(
                str(request_id),
                "cancelled",
                job_id=str(job["job_id"]),
                error="cancelled by operator before completion",
            )
            self.store.audit(
                "validation_import_cancelled",
                f"role={self._role(operator_id) if operator_id else 'unknown'} "
                f"request={request_id} reason=operator_cancelled",
            )
        await self._notify_all(
            "⏹ دریافت لغو شد. پیام‌های ذخیره‌شده حذف نشدند و checkpoint برای ادامه حفظ شد."
        )

    async def _resume_job(self) -> None:
        job = self.store.latest_job()
        if job is None:
            await self._notify_all("دریافتی برای ادامه وجود ندارد.")
            return
        if job.get("request_id"):
            await self._notify_all(
                "این دریافت نمونه محدود قابل ادامه نیست؛ برای ادامه یک درخواست "
                "و تأیید تازه لازم است."
            )
            return
        if job["status"] in JOB_RUNNING:
            await self._start_worker(str(job["job_id"]))
            await self._notify_all("دریافت از checkpoint ادامه می‌یابد.")
            return
        if job["status"] not in ("cancelled", "failed", "partial", "interrupted"):
            await self._notify_all(
                "این دریافت قابل ادامه نیست؛ برای دریافت دوباره کانال را تأیید کنید."
            )
            return
        self.store.resume_job(str(job["job_id"]))
        await self._notify_all("▶️ ادامه دریافت از checkpoint آغاز شد.")
        await self._start_worker(str(job["job_id"]))

    # ------------------------------------------------------------------ loop

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.startup()
        offset: int | None = None
        commands_sent = False
        try:
            while not stop_event.is_set():
                if not commands_sent:
                    with contextlib.suppress(Exception):
                        await asyncio.to_thread(
                            self.bot.set_my_commands,
                            [
                                {"command": "start", "description": "وضعیت و منوی مدیریت"},
                                {"command": "status", "description": "خلاصه وضعیت"},
                                {"command": "help", "description": "راهنما"},
                                {"command": "preview", "description": "پیش‌نمایش ۱۰ پیام آخر"},
                                {"command": "raw", "description": "مرور خام ۱۰۰ پیام آخر (توسعه‌دهنده)"},
                                {"command": "logout", "description": "خروج با تأیید از حساب تلگرام"},
                                {"command": "cancel", "description": "لغو مرحله ورود جاری"},
                            ],
                        )
                    commands_sent = True
                try:
                    updates = await asyncio.to_thread(
                        self.bot.get_updates,
                        offset=offset,
                        timeout=min(20, self.cfg.control.poll_timeout_s),
                    )
                except BotApiError as exc:
                    log.warning("management polling failed: %s", exc)
                    await asyncio.sleep(5)
                    continue
                except Exception as exc:  # noqa: BLE001
                    log.warning("management polling error (%s)", exc.__class__.__name__)
                    await asyncio.sleep(5)
                    continue
                for update in updates:
                    update_id = update.get("update_id")
                    if update_id is not None:
                        offset = int(update_id) + 1
                    try:
                        await self.handle_update(update)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("management update failed (%s)", exc.__class__.__name__)
        finally:
            await self.shutdown()
