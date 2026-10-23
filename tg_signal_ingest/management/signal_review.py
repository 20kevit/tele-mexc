"""Read-only historical signal review for the management bot.

Data source: the logical signal index built by
``tg_signal_ingest.analysis.reindex`` from already-stored messages
(``channel_signals`` + ``signal_message_links``). Everything here is a pure
SELECT against previously ingested data; nothing is parsed again, rewritten,
sent to Telegram/exchanges, or enqueued. SHORT signals are displayed exactly
like LONG ones — they are review data, and this module has no notion of
order sizing, leverage or execution.
"""

from __future__ import annotations

from typing import Any

from ..analysis.reindex import classification_fa

#: Telegram's hard limit is 4096 UTF-16 code units per message; keep a
#: safety margin so keyboards and headers always fit.
MAX_SEGMENT_UTF16 = 3600

ROLE_FA = {"long": "لانگ", "short": "شورت"}

METHOD_FA = {
    "explicit_id": "شناسه صریح سیگنال",
    "reply": "پاسخ به پیام سیگنال",
    "both": "شناسه و پاسخ",
    "none": "بدون مرجع",
}

LINK_STATUS_FA = {
    "linked": "متصل",
    "unresolved": "مرجع حل‌نشده",
    "conflict": "تعارض مراجع",
    "no_reference": "بدون شناسه",
}

EXEC_STATUS_FA = {
    "none": "بدون اجرا (فقط مرور تاریخچه)",
}


def direction_fa(direction: Any) -> str:
    return ROLE_FA.get(str(direction) if direction else "", "نامشخص")


def utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def slice_utf16(text: str, capacity: int) -> list[str]:
    """Split text into segments that each fit ``capacity`` UTF-16 units.

    Segmentation measures real UTF-16 widths (astral characters count as
    two units) so every segment stays inside Telegram's per-message limit.
    """

    if capacity <= 0:
        raise ValueError("capacity must be positive")
    if utf16_len(text) <= capacity:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        budget = capacity
        end = start
        while end < len(text):
            width = 2 if ord(text[end]) > 0xFFFF else 1
            if width > budget:
                break
            budget -= width
            end += 1
        if end == start:  # a single astral character: emit it alone
            end = start + 1
        chunks.append(text[start:end])
        start = end
    return chunks


def _fmt(value: Any) -> str:
    return "—" if value in (None, "") else str(value)


class SignalReviewUnavailable(Exception):
    """Raised when the logical signal index tables do not exist yet."""


def ensure_available(store: Any) -> None:
    if not store.tables_exist("channel_signals", "signal_message_links"):
        raise SignalReviewUnavailable()


def recent_signals(
    db: Any,
    channel_id: int,
    *,
    limit: int = 10,
    offset: int = 0,
    newest_first: bool = False,
) -> dict[str, Any]:
    """Signals of one channel ordered by origin-message datetime.

    The Telegram message ID is the deterministic tie-breaker; the logical
    signal ID is never used for ordering because it is not chronological.
    ``newest_first`` flips the direction for the review menu.
    """

    order = "DESC" if newest_first else "ASC"
    total = db.conn.execute(
        "SELECT COUNT(*) c FROM channel_signals WHERE channel_id = ?",
        (channel_id,),
    ).fetchone()["c"]
    rows = db.conn.execute(
        "SELECT cs.*, m.datetime AS origin_datetime "
        "FROM channel_signals cs JOIN messages m "
        "ON m.channel_id = cs.channel_id AND m.message_id = cs.origin_message_id "
        "WHERE cs.channel_id = ? "
        f"ORDER BY m.datetime {order}, cs.origin_message_id {order} "
        "LIMIT ? OFFSET ?",
        (channel_id, int(limit), max(int(offset), 0)),
    ).fetchall()
    return {"total": total, "rows": [dict(r) for r in rows]}


def signals_peek(
    db: Any, channel_id: int, *, limit: int = 8
) -> list[dict[str, Any]]:
    """Newest signals for the review menu (result order of recent_signals)."""

    return recent_signals(db, channel_id, limit=limit, offset=0)["rows"]


def signal_by_logical_id(
    db: Any, channel_id: int, logical_id: int
) -> dict[str, Any] | None:
    row = db.conn.execute(
        "SELECT cs.*, m.datetime AS origin_datetime "
        "FROM channel_signals cs JOIN messages m "
        "ON m.channel_id = cs.channel_id AND m.message_id = cs.origin_message_id "
        "WHERE cs.channel_id = ? AND cs.logical_id = ?",
        (channel_id, int(logical_id)),
    ).fetchone()
    return dict(row) if row is not None else None


def logical_ids(db: Any, channel_id: int) -> list[int]:
    rows = db.conn.execute(
        "SELECT cs.logical_id, m.datetime FROM channel_signals cs "
        "JOIN messages m ON m.channel_id = cs.channel_id "
        "AND m.message_id = cs.origin_message_id "
        "WHERE cs.channel_id = ? ORDER BY m.datetime ASC, cs.origin_message_id ASC",
        (channel_id,),
    ).fetchall()
    return [int(r["logical_id"]) for r in rows]


def related_messages(db: Any, channel_id: int, logical_id: int) -> list[dict[str, Any]]:
    """Every message linked to the signal (resolved, unresolved, conflict)."""

    rows = db.conn.execute(
        "SELECT l.message_id, l.edit_version, l.method, l.status, l.reason, "
        "l.conflict_with_logical_id, l.classification, m.datetime, m.edit_datetime, "
        "m.text, m.caption, m.reply_to_message_id, "
        "(SELECT COUNT(*) FROM message_edits e WHERE e.channel_id = l.channel_id "
        " AND e.message_id = l.message_id) AS edit_captures "
        "FROM signal_message_links l JOIN messages m "
        "ON m.channel_id = l.channel_id AND m.message_id = l.message_id "
        "WHERE l.channel_id = ? AND l.logical_id = ? "
        "ORDER BY m.datetime ASC, l.message_id ASC",
        (channel_id, int(logical_id)),
    ).fetchall()
    return [dict(r) for r in rows]


def origin_message_text(
    db: Any, channel_id: int, message_id: int
) -> dict[str, Any] | None:
    row = db.conn.execute(
        "SELECT message_id, datetime, edit_datetime, text, caption, "
        "(SELECT COUNT(*) FROM message_edits e WHERE e.channel_id = messages.channel_id "
        " AND e.message_id = messages.message_id) AS edit_captures "
        "FROM messages WHERE channel_id = ? AND message_id = ?",
        (channel_id, int(message_id)),
    ).fetchone()
    return dict(row) if row is not None else None


def _signal_title(channel_id: int, signal: dict[str, Any]) -> str:
    return (
        f"#{_fmt(signal.get('logical_id'))} · "
        f"{_fmt(signal.get('symbol') or signal.get('symbol_raw'))} · "
        f"{direction_fa(signal.get('direction'))}"
    )


def render_signal_line(signal: dict[str, Any]) -> str:
    date = signal.get("source_date_text") or signal.get("origin_datetime") or "—"
    base = signal.get("symbol") or signal.get("symbol_raw") or "—"
    direction = direction_fa(signal.get("direction"))
    time_part = f" · {signal['source_time_text']}" if signal.get("source_time_text") else ""
    status_mark = ""
    if signal.get("status") == "duplicate_origin":
        status_mark = " ⚠️ شناسه تکراری"
    elif signal.get("status") == "needs_review":
        status_mark = " ⚠️ نیازمند مرور"
    return (
        f"#{_fmt(signal.get('logical_id'))} · {base} · {direction}{time_part} · {date}{status_mark}"
    )


def render_signal_detail(channel_id: int, signal: dict[str, Any]) -> str:
    lines = [
        f"🗂 سیگنال #{_fmt(signal.get('logical_id'))} (کانال {channel_id})",
        "",
        f"جهت: {direction_fa(signal.get('direction'))}",
        f"نماد: {_fmt(signal.get('symbol'))}"
        f" (خام: {_fmt(signal.get('symbol_raw'))}, صرافی: {_fmt(signal.get('exchange'))})",
        f"تارگت کامل: {_fmt(signal.get('full_target'))}",
        f"وضعیت ردیف: {_fmt(signal.get('status'))}",
        f"وضعیت اجرا: {EXEC_STATUS_FA.get(str(signal.get('execution_status')), _fmt(signal.get('execution_status')))}",
        f"پیام منبع: {_fmt(signal.get('origin_message_id'))}"
        f" (زمان: {_fmt(signal.get('origin_datetime'))})",
    ]
    if signal.get("status") == "duplicate_origin":
        lines.append(
            "⚠️ برای این شناسه سیگنال چند پیام اولیه وجود دارد؛ قدیمی‌ترین پیام "
            "مبنای این ردیف است."
        )
    return "\n".join(lines)


def render_signal_extract(channel_id: int, signal: dict[str, Any]) -> str:
    targets = _fmt(signal.get("targets_json"))
    lines = [
        f"🔎 داده‌های استخراج‌شده — سیگنال #{_fmt(signal.get('logical_id'))}",
        "",
        f"جهت: {direction_fa(signal.get('direction'))}",
        f"صرافی/نماد: {_fmt(signal.get('exchange'))}:{_fmt(signal.get('symbol_raw'))}"
        f" → {_fmt(signal.get('symbol'))} (کوته: {_fmt(signal.get('quote_asset'))})",
        f"تایم‌فریم: {_fmt(signal.get('timeframe'))}",
        f"ورود: {_fmt(signal.get('entry_price'))}",
        f"حد ضرر: {_fmt(signal.get('stop_loss'))}",
        f"تارگت‌ها: {targets}",
        f"تارگت کامل (FULL Target): {_fmt(signal.get('full_target'))}",
        f"درصد سپرده (فقط اطلاعاتی): {_fmt(signal.get('deposit_pct'))}",
        f"لوریج پیشنهادی (فقط اطلاعاتی): {_fmt(signal.get('suggested_leverage'))}"
        f" ({_fmt(signal.get('leverage_pct'))})",
        f"طبقه‌بندی: {classification_fa(str(signal.get('classification')))}",
        f"وضعیت تجزیه: {_fmt(signal.get('parse_status'))}",
        f"مبهمی‌ها: {_fmt(signal.get('ambiguities_json'))}",
        "",
        f"تاریخ/ساعت منبع: {_fmt(signal.get('source_date_text'))}"
        f" {_fmt(signal.get('source_time_text'))} (تقویم منبع حفظ شده است)",
        f"زمان UTC مشتق‌شده: {_fmt(signal.get('source_timestamp_iso'))}",
        f"پیام منبع: {_fmt(signal.get('origin_message_id'))} · ویرایش: {_fmt(signal.get('edit_version'))}",
        "",
        "این داده‌ها فقط برای مرور تاریخچه‌اند؛ «درصد سپرده» و «لوریج پیشنهادی» "
        "هرگز در محاسبه حجم یا لوریج سفارش به کار نمی‌روند و سیگنال‌های شورت "
        "هرگز سفارشی ایجاد نمی‌کنند.",
    ]
    return "\n".join(lines)


def render_related_messages(
    channel_id: int, logical_id: Any, messages: list[dict[str, Any]]
) -> str:
    lines = [f"💌 پیام‌های مرتبط با سیگنال #{_fmt(logical_id)}", ""]
    if not messages:
        lines.append("پیام مرتبطی ثبت نشده است.")
    for item in messages:
        head = (
            f"• پیام {_fmt(item.get('message_id'))} — "
            f"{_fmt(item.get('datetime'))} — "
            f"{LINK_STATUS_FA.get(str(item.get('status')), _fmt(item.get('status')))}"
            f" ({METHOD_FA.get(str(item.get('method')), _fmt(item.get('method')))})"
        )
        if item.get("edit_captures"):
            head += f" — ویرایش ثبت‌شده ×{item['edit_captures']}"
        lines.append(head)
        lines.append(
            f"  طبقه‌بندی: {classification_fa(str(item.get('classification')))}"
        )
        if item.get("reason"):
            lines.append(f"  دلیل: {_fmt(item.get('reason'))}")
        if item.get("edit_datetime"):
            lines.append(f"  ویرایش‌شده: {_fmt(item.get('edit_datetime'))}")
        body = item.get("text") or item.get("caption") or ""
        if body:
            segment = body if utf16_len(body) <= 600 else (
                slice_utf16(body, 600)[0] + " … (کوتاه‌شده)"
            )
            lines.append(f"  متن: {segment}")
        lines.append("")
    lines.append(
        "پیام‌های با وضعیت «حل‌نشده» مرجعی دارند که پیام اصلی سیگنال آن در "
        "آرشیو ذخیره نشده است؛ متن خام هرگز حذف یا جایگزین نمی‌شود."
    )
    return "\n".join(lines)


def render_original_message(
    message: dict[str, Any], *, label: str = "پیام منبع سیگنال"
) -> list[str]:
    """Original raw message; chunked to stay within Telegram limits."""

    head = [
        f"📜 {label}",
        f"پیام {_fmt(message.get('message_id'))} · {_fmt(message.get('datetime'))}",
    ]
    if message.get("edit_datetime"):
        head.append(f"آخرین ویرایش: {_fmt(message.get('edit_datetime'))}")
    body = message.get("text") or message.get("caption") or "(بدون متن)"
    if message.get("edit_captures"):
        head.append(
            f"این پیام ویرایش‌شده ثبت شده است ({message['edit_captures']} نسخه در message_edits)."
        )
    parts = slice_utf16(body, max(200, MAX_SEGMENT_UTF16 - utf16_len("\n".join(head)) - 40))
    segments: list[str] = []
    total = len(parts)
    for index, segment in enumerate(parts, start=1):
        marker = f"\n(بخش {index} از {total})\n" if total > 1 else "\n"
        segments.append("\n".join(head) + marker + segment)
    return segments
