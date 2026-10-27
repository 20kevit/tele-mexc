"""Developer-only, read-only raw review of the most recent channel messages.

This module backs the ``/raw`` management-bot action and its one-shot runner
(``python -m tg_signal_ingest.management.raw_review``). It reuses the exact
management-selected channel and the existing authenticated employer Telethon
session (guarded by :class:`SessionGuard`), fetches the most recent messages,
renders them verbatim in chronological order (oldest first) and delivers them
to the configured developer's private chat with the management bot in
Telegram-safe, rate-limited batches.

Raw-data inspection only:

* messages are never parsed, classified, summarized, normalized or filtered;
* media is never downloaded and no OCR runs (media type + caption only);
* message content never reaches logs or the database - only the selected
  channel is read, through a read-only connection in the one-shot runner;
* source messages are never edited or deleted;
* no backfill, live monitoring, ingestion, parser or MEXC/trading behavior is
  touched, and no schema migration is ever applied.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from telethon.errors import FloodWaitError, RPCError

from ..config import AppConfig, ConfigError, load_config
from ..control.auth import AuthFlowError
from ..control.bot_api import BotApiClient, BotApiError
from ..control.channels import find_channel
from ..db import Database
from ..session_guard import SessionBusyError, SessionGuard
from ..telethon import build_client
from .store import ManagementStore

log = logging.getLogger("ingest.management.raw_review")

# The review is bounded to the 100 most recent messages and to Telegram-safe
# outgoing batches: the Bot API hard limit is 4096 UTF-16 code units per
# message, and one outgoing message per second keeps well under the send rate.
RAW_REVIEW_LIMIT = 100
MAX_BATCH_UTF16 = 3600
SEND_INTERVAL_S = 1.0
SESSION_BUSY_RETRIES = 5
SESSION_BUSY_RETRY_DELAY_S = 3.0

# Longest possible continuation marker (" (ادامه 999/999)") plus margin; it is
# reserved while slicing oversized messages so every part stays under the cap.
_CONTINUATION_MAX_UTF16 = 24

MEDIA_LABELS: tuple[tuple[str, str], ...] = (
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
)


class RawReviewError(RuntimeError):
    """Base class for raw-review failures that are safe to show in chat."""


class NoChannelSelected(RawReviewError):
    """The operator has no management-selected channel yet."""


class ChannelUnavailable(RawReviewError):
    """The selected channel is no longer reachable through this session."""


# --------------------------------------------------------------------- render


def utf16_len(text: str) -> int:
    """Telegram counts message length in UTF-16 code units, not code points."""

    return len(text.encode("utf-16-le")) // 2


def media_label(message: Any) -> str | None:
    """Return a short media marker without downloading or inspecting content."""

    for attr, label in MEDIA_LABELS:
        if getattr(message, attr, None):
            return label
    if getattr(message, "media", None) is not None:
        return "رسانه"
    return None


def _stamp(value: Any) -> str:
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return moment.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    return "—"


def _forward_label(forward: Any) -> str:
    parts: list[str] = []
    name = getattr(forward, "from_name", None)
    if name:
        parts.append(str(name))
    channel_id = getattr(forward, "channel_id", None)
    if channel_id is not None:
        parts.append(f"channel_id={channel_id}")
    peer = getattr(forward, "from_id", None)
    if peer is not None:
        user_id = (
            getattr(peer, "user_id", None)
            or getattr(peer, "channel_id", None)
            or getattr(peer, "chat_id", None)
        )
        parts.append(f"from_id={user_id}" if user_id is not None else type(peer).__name__)
    date = getattr(forward, "date", None)
    if date is not None:
        parts.append(_stamp(date))
    return ", ".join(parts) if parts else "متادیتای بازنشر در دسترس نیست"


@dataclass(frozen=True)
class RawEntry:
    """One source message plus its display metadata; ``body`` is verbatim."""

    index: int
    total: int
    message_id: int
    stamp: str
    meta: tuple[str, ...]
    body: str

    def head_lines(self) -> list[str]:
        return [self._header(), *self.meta]

    def _header(self) -> str:
        return f"[{self.index}/{self.total}] #{self.message_id} — {self.stamp}"

    def _split_header(self, position: int, count: int) -> str:
        marker = "بخش" if position == 1 else "ادامه"
        return f"{self._header()} ({marker} {position}/{count})"


def message_entry(index: int, total: int, message: Any) -> RawEntry:
    """Build the raw (never shortened, never normalized) entry for one message."""

    message_id = int(getattr(message, "id", 0) or 0)
    stamp = _stamp(getattr(message, "date", None))
    meta: list[str] = []

    edit_date = getattr(message, "edit_date", None)
    if edit_date is not None:
        meta.append(f"ویرایش: {_stamp(edit_date)}")

    forward = getattr(message, "forward", None)
    if forward is not None:
        meta.append(f"بازنشر: {_forward_label(forward)}")

    reply = getattr(message, "reply_to", None)
    reply_id = getattr(reply, "reply_to_msg_id", None)
    if reply_id is None:
        reply_id = getattr(message, "reply_to_msg_id", None)
    if reply_id is not None:
        meta.append(f"در پاسخ به #{int(reply_id)}")

    action = getattr(message, "action", None)
    if action is not None:
        meta.append(f"سرویس: {type(action).__name__}")

    media = media_label(message)
    if media:
        meta.append(f"رسانه: {media} (دانلود نشد)")

    text = getattr(message, "message", None)
    if text is None:
        text = getattr(message, "text", None)
    body = "" if text is None else str(text)
    if not body:
        # Transparent placeholders instead of invented content.
        if getattr(message, "deleted", False):
            body = "[پیام حذف‌شده یا در دسترس نیست]"
        elif media:
            body = "[بدون کپشن]"
        else:
            body = "[بدون متن]"

    return RawEntry(
        index=index,
        total=total,
        message_id=message_id,
        stamp=stamp,
        meta=tuple(meta),
        body=body,
    )


def _slice_utf16(text: str, capacity: int) -> list[str]:
    """Split ``text`` so every part is at most ``capacity`` UTF-16 units.

    A newline boundary is preferred when one exists in the latter half of the
    window, so multi-line messages stay readable. Concatenating the parts
    always reproduces ``text`` exactly.
    """

    if not text:
        return [""]
    if utf16_len(text) <= capacity:
        return [text]

    parts: list[str] = []
    start = 0
    length = len(text)
    while start < length:
        units = 0
        position = start
        last_newline = -1
        while position < length:
            width = 2 if ord(text[position]) > 0xFFFF else 1
            if units + width > capacity:
                break
            units += width
            if text[position] == "\n":
                last_newline = position
            position += 1
        if position == length:
            parts.append(text[start:length])
            break
        if position == start:  # a single wide character larger than capacity
            position += 1
        if last_newline > start + (position - start) // 2:
            parts.append(text[start : last_newline + 1])
            start = last_newline + 1
        else:
            parts.append(text[start:position])
            start = position
    return parts


def render_entry_parts(entry: RawEntry, *, max_utf16: int = MAX_BATCH_UTF16) -> list[str]:
    """Render one entry into one or more parts that each fit ``max_utf16``.

    Oversized messages are split without dropping or altering a single
    character: every part repeats the order/id/timestamp header (plus a
    ``(ادامه k/m)`` marker) so the review stays self-describing.
    """

    head_lines = entry.head_lines()
    head = "\n".join(head_lines)
    if utf16_len(head) + 1 + utf16_len(entry.body) <= max_utf16:
        return ["\n".join([*head_lines, entry.body])]

    continuation = 0
    pieces: list[str] = []
    for _ in range(5):  # converge on the real continuation-marker length
        capacity = max_utf16 - utf16_len(head) - continuation - 1
        if capacity < 1:
            raise ValueError("raw review batch limit is too small for the message header")
        pieces = _slice_utf16(entry.body, capacity)
        next_continuation = (
            utf16_len(f" (ادامه {len(pieces)}/{len(pieces)})") if len(pieces) > 1 else 0
        )
        if next_continuation == continuation:
            break
        continuation = next_continuation

    count = len(pieces)
    parts: list[str] = []
    for position, piece in enumerate(pieces, start=1):
        lines = entry.head_lines()
        if count > 1:
            lines[0] = entry._split_header(position, count)
        parts.append("\n".join([*lines, piece]))
    return parts


def build_batches(
    messages: Sequence[Any], *, max_utf16: int = MAX_BATCH_UTF16
) -> list[str]:
    """Render ``messages`` (oldest first) into Telegram-safe outgoing batches."""

    total = len(messages)
    batches: list[str] = []
    current = ""
    for index, message in enumerate(messages, start=1):
        entry = message_entry(index, total, message)
        for part in render_entry_parts(entry, max_utf16=max_utf16):
            if current:
                candidate = f"{current}\n\n{part}"
                if utf16_len(candidate) <= max_utf16:
                    current = candidate
                    continue
                batches.append(current)
            current = part
    if current:
        batches.append(current)
    return batches


# -------------------------------------------------------------------- delivery


class SendPacer:
    """Rate limit: at most one outgoing message per interval (shared)."""

    def __init__(
        self,
        *,
        interval_s: float | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ):
        self._interval_s = SEND_INTERVAL_S if interval_s is None else interval_s
        self._sleep = sleep if sleep is not None else asyncio.sleep
        self._sends = 0

    async def wait(self) -> None:
        if self._sends:
            await self._sleep(self._interval_s)
        self._sends += 1


@dataclass
class DeliveryReport:
    """Per-send outcome, aligned with the texts that were handed to the sender."""

    results: list[bool] = field(default_factory=list)
    message_ids: list[int | None] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def sent(self) -> int:
        return sum(1 for ok in self.results if ok)

    @property
    def failed(self) -> int:
        return self.total - self.sent


def _message_id(result: Any) -> int | None:
    if isinstance(result, dict):
        try:
            return int(result.get("message_id") or 0) or None
        except (TypeError, ValueError):
            return None
    return None


async def deliver_messages(
    texts: Sequence[str],
    send: Callable[[str], Awaitable[Any]],
    *,
    pacer: SendPacer | None = None,
    interval_s: float | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> DeliveryReport:
    """Send ``texts`` one by one, pacing between sends; never raise on failure.

    Failures are recorded (exception class name only) so a partial delivery is
    reported instead of silently retried or hidden.
    """

    pace = pacer if pacer is not None else SendPacer(interval_s=interval_s, sleep=sleep)
    report = DeliveryReport()
    for text in texts:
        await pace.wait()
        try:
            result = await send(text)
        except Exception as exc:  # noqa: BLE001 - transport failures are reported, never fatal
            report.results.append(False)
            report.message_ids.append(None)
            report.errors.append(type(exc).__name__)
            log.warning("raw review delivery failed (%s)", type(exc).__name__)
            continue
        report.results.append(True)
        report.message_ids.append(_message_id(result))
        report.errors.append("")
    return report


# ----------------------------------------------------------------------- fetch


async def fetch_recent(
    client: Any, channel_id: int, *, limit: int = RAW_REVIEW_LIMIT
) -> list[Any]:
    """Return up to ``limit`` most recent messages, chronological (oldest first).

    Read-only: only ``getMessages`` is used, media is never downloaded and the
    source messages are never modified.
    """

    limit = max(1, min(int(limit), RAW_REVIEW_LIMIT))
    choice = await find_channel(client, channel_id)
    if choice is None:
        raise ChannelUnavailable(f"channel {channel_id} is not available to this session")
    try:
        entity = await client.get_entity(channel_id)
    except Exception:  # noqa: BLE001 - fall back to the peer id, as preview does
        entity = channel_id
    messages = await client.get_messages(entity, limit=limit)
    items = list(messages or [])
    # Telethon returns newest first; the review is chronological, oldest first.
    items.sort(key=lambda item: int(getattr(item, "id", 0) or 0))
    return items[:limit]


def resolve_selected_channel(
    store: ManagementStore, operator_id: int, *, fallback_operator_id: int | None = None
) -> tuple[int, str]:
    """Reuse the exact management-selected channel; never pick another one."""

    for candidate in (operator_id, fallback_operator_id):
        if candidate is None:
            continue
        row = store.operator_channel(int(candidate))
        channel_id = int(row.get("selected_channel_id") or 0)
        if channel_id:
            title = str(row.get("selected_channel_title") or channel_id)
            return channel_id, title
    raise NoChannelSelected("no management-selected channel")


# ---------------------------------------------------------------------- review


@dataclass
class ReviewOutcome:
    channel_id: int
    channel_title: str
    fetched: int
    batch_count: int
    intro: DeliveryReport
    batches: DeliveryReport
    summary: DeliveryReport

    @property
    def batches_sent(self) -> int:
        return self.batches.sent

    @property
    def batches_failed(self) -> int:
        return self.batches.failed

    @property
    def intro_message_id(self) -> int | None:
        return self.intro.message_ids[0] if self.intro.message_ids else None

    @property
    def summary_message_id(self) -> int | None:
        return self.summary.message_ids[0] if self.summary.message_ids else None

    @property
    def batch_message_ids(self) -> list[int]:
        return [mid for mid in self.batches.message_ids if mid is not None]


async def run_raw_review(
    *,
    store: ManagementStore,
    guard: SessionGuard,
    open_client: Callable[[], Awaitable[Any]],
    send: Callable[[str], Awaitable[Any]],
    operator_id: int,
    fallback_operator_id: int | None = None,
    limit: int = RAW_REVIEW_LIMIT,
    max_utf16: int = MAX_BATCH_UTF16,
    pacer: SendPacer | None = None,
    interval_s: float | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> ReviewOutcome:
    """Fetch the latest messages of the selected channel and deliver them raw.

    The shared Telethon session is held only for the read (connect, fetch,
    disconnect); the outgoing batches are sent afterwards without holding it.
    The database is only read (selected channel). No DB write, no audit row,
    no ingestion start and no parser/trading call ever happens here.
    """

    channel_id, title = resolve_selected_channel(
        store, operator_id, fallback_operator_id=fallback_operator_id
    )
    async with guard.use(operator_id=operator_id):
        client = await open_client()
        try:
            messages = await fetch_recent(client, channel_id, limit=limit)
        finally:
            with contextlib.suppress(Exception):
                await client.disconnect()

    batches = build_batches(messages, max_utf16=max_utf16)
    pace = pacer if pacer is not None else SendPacer(interval_s=interval_s, sleep=sleep)

    intro = (
        "🔍 مرور خام (فقط خواندنی)\n"
        f"کانال: {title} (id={channel_id})\n"
        f"دریافت شد: {len(messages)} پیام در {len(batches)} دسته — قدیمی‌ترین اول.\n"
        "بدون پردازش، طبقه‌بندی یا خلاصه‌سازی؛ رسانه دانلود نشد و متن پیام‌ها "
        "دقیقاً همان نسخه اصلی است."
    )
    intro_report = await deliver_messages([intro], send, pacer=pace)
    batch_report = await deliver_messages(batches, send, pacer=pace)

    summary_lines = [
        "✅ پایان مرور خام",
        f"پیام‌های دریافت‌شده: {len(messages)} | دسته‌های ارسال‌شده: "
        f"{batch_report.sent}/{batch_report.total}",
    ]
    if batch_report.failed:
        summary_lines.append(f"⚠️ دسته ناموفق: {batch_report.failed}")
    delivered = [mid for mid in batch_report.message_ids if mid is not None]
    if delivered:
        summary_lines.append("شناسه پیام‌های ربات: " + ", ".join(str(mid) for mid in delivered))
    summary_lines.append("این مرور فقط‌خواندنی بود؛ هیچ متنی در پایگاه‌داده یا لاگ ذخیره نشد.")
    summary_report = await deliver_messages(["\n".join(summary_lines)], send, pacer=pace)

    log.info(
        "raw review delivered (channel=%s fetched=%s batches=%s sent=%s failed=%s)",
        channel_id,
        len(messages),
        batch_report.total,
        batch_report.sent,
        batch_report.failed,
    )
    return ReviewOutcome(
        channel_id=channel_id,
        channel_title=title,
        fetched=len(messages),
        batch_count=batch_report.total,
        intro=intro_report,
        batches=batch_report,
        summary=summary_report,
    )


# ----------------------------------------------------------------------- runner


async def _open_session_client(cfg: AppConfig) -> Any:
    client = build_client(cfg)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise AuthFlowError("نشست تلگرام معتبر نیست؛ ابتدا ورود را کامل کنید.")
        return client
    except Exception:
        with contextlib.suppress(Exception):
            await client.disconnect()
        raise


async def _run_once(
    cfg: AppConfig,
    operator_id: int,
    limit: int,
    *,
    bot: Any | None = None,
    store: ManagementStore | None = None,
    guard: SessionGuard | None = None,
    open_client: Callable[[], Awaitable[Any]] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> int:
    """One-shot, developer-only delivery. Reads the DB read-only; no restart."""

    db: Database | None = None
    if store is None:
        # Explicitly read-only: no migration, no write, no lock contention.
        db = Database(cfg.db_path, read_only=True)
        store = ManagementStore(db)
    try:
        if bot is None:
            bot = BotApiClient(cfg.control.bot_token or "", api_base=cfg.control.api_base)
        if guard is None:
            guard = SessionGuard(cfg)
        if open_client is None:
            open_client = lambda: _open_session_client(cfg)  # noqa: E731 - thin default

        async def _send(text: str) -> Any:
            return await asyncio.to_thread(bot.send_message, int(operator_id), text)

        pace = SendPacer(sleep=sleep)
        attempt = 0
        while True:
            try:
                outcome = await run_raw_review(
                    store=store,
                    guard=guard,
                    open_client=open_client,
                    send=_send,
                    operator_id=int(operator_id),
                    fallback_operator_id=cfg.control.owner_user_id,
                    limit=limit,
                    pacer=pace,
                )
                break
            except SessionBusyError:
                # The running bot (or another operator) is using the shared
                # session right now; wait and retry without disturbing it.
                attempt += 1
                if attempt > SESSION_BUSY_RETRIES:
                    print("telegram session busy; retry later", file=sys.stderr)
                    return 1
                delay = SESSION_BUSY_RETRY_DELAY_S
                if sleep is not None:
                    await sleep(delay)
                else:
                    await asyncio.sleep(delay)
    finally:
        if db is not None:
            db.close()

    print(
        "raw review complete: "
        f"fetched={outcome.fetched} batches={outcome.batch_count} "
        f"delivered={outcome.batches_sent} failed={outcome.batches_failed} "
        f"intro_message_id={outcome.intro_message_id} "
        f"batch_message_ids={outcome.batch_message_ids} "
        f"summary_message_id={outcome.summary_message_id} "
        f"channel_id={outcome.channel_id}"
    )
    return 0 if outcome.batches_failed == 0 else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tg_signal_ingest.management.raw_review",
        description=(
            "Developer-only: fetch the N most recent messages of the "
            "management-selected channel and deliver them verbatim to the "
            "developer's private chat with the management bot (read-only)."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=RAW_REVIEW_LIMIT,
        help=f"how many recent messages to fetch (1-{RAW_REVIEW_LIMIT})",
    )
    args = parser.parse_args(argv)
    limit = max(1, min(int(args.limit), RAW_REVIEW_LIMIT))

    try:
        cfg = load_config()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    if not cfg.control.management_configured or not cfg.control.developer_user_id:
        print(
            "management bot is not configured (CONTROL_BOT_TOKEN / CONTROL_DEVELOPER_USER_ID)",
            file=sys.stderr,
        )
        return 2
    operator_id = int(cfg.control.developer_user_id)

    try:
        return asyncio.run(_run_once(cfg, operator_id, limit))
    except NoChannelSelected:
        print("no management-selected channel; select a channel in the bot first", file=sys.stderr)
        return 2
    except ChannelUnavailable:
        print("selected channel is not available to this session", file=sys.stderr)
        return 1
    except SessionBusyError:
        print("telegram session busy; retry later", file=sys.stderr)
        return 1
    except AuthFlowError as exc:
        print(f"telegram session unavailable: {exc}", file=sys.stderr)
        return 1
    except FloodWaitError as exc:
        print(f"telegram flood wait: {int(exc.seconds)}s", file=sys.stderr)
        return 1
    except (RPCError, ConnectionError, OSError, BotApiError) as exc:
        print(f"raw review failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover - manual developer runner
    raise SystemExit(main())
