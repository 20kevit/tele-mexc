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
#: Telegram Bot API hard limit for one text message (UTF-16 code units,
#: "after entities parsing"). This project never attaches entities, but the
#: limit is still measured in UTF-16 units, not Python characters.
MAX_MESSAGE_UTF16 = 4096
#: Safety budget for any single outgoing review segment; keyboards, headers
#: and continuation markers must always fit inside the hard limit.
SAFE_TEXT_UTF16 = 3840

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


#: Marker appended when a value had to be shortened for display. The full
#: value always remains intact in the database; only the presentation is
#: bounded.
SHORTEN_MARK = "…"


def shorten_utf16(text: str, limit: int) -> str:
    """Bound ``text`` to ``limit`` UTF-16 units, cutting only on code points.

    This is presentation-only: it never mutates stored rows. ``cut`` counts
    real UTF-16 widths so astral characters (emoji, rare symbols) can never
    push a message over Telegram's per-message limit, which is measured in
    UTF-16 code units and not Python characters.
    """

    if limit <= 0:
        return ""
    if utf16_len(text) <= limit:
        return text
    out: list[str] = []
    used = 0
    for char in text:
        width = 2 if ord(char) > 0xFFFF else 1
        if used + width > limit - len(SHORTEN_MARK.encode("utf-16-le")) // 2:
            break
        out.append(char)
        used += width
    return "".join(out) + SHORTEN_MARK


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
    """Extracted (parsed) fields; every free-form value is bounded.

    ``targets_json`` and ``ambiguities_json`` are stored exactly as the
    reindex wrote them and are truncated for display only, with an explicit
    marker when that happens. The whole screen always stays inside
    Telegram's single-message limit so it can never be silently dropped.
    """

    targets = shorten_utf16(_fmt(signal.get("targets_json")), 900)
    ambiguities = shorten_utf16(_fmt(signal.get("ambiguities_json")), 500)
    lines = [
        f"🔎 داده‌های استخراج‌شده — سیگنال #{_fmt(signal.get('logical_id'))}",
        "",
        f"جهت: {direction_fa(signal.get('direction'))}",
        f"صرافی/نماد: {shorten_utf16(_fmt(signal.get('exchange')), 100)}"
        f":{shorten_utf16(_fmt(signal.get('symbol_raw')), 100)}"
        f" → {shorten_utf16(_fmt(signal.get('symbol')), 100)}"
        f" (کوته: {shorten_utf16(_fmt(signal.get('quote_asset')), 50)})",
        f"تایم‌فریم: {shorten_utf16(_fmt(signal.get('timeframe')), 50)}",
        f"ورود: {shorten_utf16(_fmt(signal.get('entry_price')), 100)}",
        f"حد ضرر: {shorten_utf16(_fmt(signal.get('stop_loss')), 100)}",
        f"تارگت‌ها: {targets}",
        f"تارگت کامل (FULL Target): {shorten_utf16(_fmt(signal.get('full_target')), 100)}",
        f"درصد سپرده (فقط اطلاعاتی): {shorten_utf16(_fmt(signal.get('deposit_pct')), 50)}",
        f"لوریج پیشنهادی (فقط اطلاعاتی): {shorten_utf16(_fmt(signal.get('suggested_leverage')), 50)}"
        f" ({shorten_utf16(_fmt(signal.get('leverage_pct')), 50)})",
        f"طبقه‌بندی: {shorten_utf16(classification_fa(str(signal.get('classification'))), 60)}",
        f"وضعیت تجزیه: {shorten_utf16(_fmt(signal.get('parse_status')), 50)}",
        f"مبهمی‌ها: {ambiguities}",
        "",
        f"تاریخ/ساعت منبع: {shorten_utf16(_fmt(signal.get('source_date_text')), 50)}"
        f" {shorten_utf16(_fmt(signal.get('source_time_text')), 50)} (تقویم منبع حفظ شده است)",
        f"زمان UTC مشتق‌شده: {shorten_utf16(_fmt(signal.get('source_timestamp_iso')), 50)}",
        f"پیام منبع: {shorten_utf16(_fmt(signal.get('origin_message_id')), 50)}"
        f" · ویرایش: {shorten_utf16(_fmt(signal.get('edit_version')), 50)}",
        "",
        "این داده‌ها فقط برای مرور تاریخچه‌اند؛ «درصد سپرده» و «لوریج پیشنهادی» "
        "هرگز در محاسبه حجم یا لوریج سفارش به کار نمی‌روند و سیگنال‌های شورت "
        "هرگز سفارشی ایجاد نمی‌کنند. مقادیر بسیار طولانی با نشانگر «…» "
        "کوتاه نمایش داده می‌شوند؛ مقدار کامل در پایگاه‌داده دست‌نخورده است.",
    ]
    text = "\n".join(lines)
    # Every field is individually bounded above, so the normal ceiling sits
    # well below the hard limit. If a hostile database still overflows,
    # shed the deficit by shrinking the two JSON digests, then enforce a
    # deterministic per-line bound as a final guard.
    over = utf16_len(text) - SAFE_TEXT_UTF16
    if over > 0:
        floor = 80
        shed = min(over, max(0, utf16_len(targets) - floor))
        if shed:
            targets = shorten_utf16(targets, utf16_len(targets) - shed)
            lines[7] = f"تارگت‌ها: {targets}"
        over = utf16_len("\n".join(lines)) - SAFE_TEXT_UTF16
        if over > 0:
            shed = min(over, max(0, utf16_len(ambiguities) - floor))
            if shed:
                ambiguities = shorten_utf16(ambiguities, utf16_len(ambiguities) - shed)
                lines[13] = f"مبهمی‌ها: {ambiguities}"
        text = "\n".join(lines)
    if utf16_len(text) > SAFE_TEXT_UTF16:
        bounded = [shorten_utf16(line, 180) for line in lines]
        text = "\n".join(bounded)
    return text


def render_related_messages(
    channel_id: int,
    logical_id: Any,
    messages: list[dict[str, Any]],
    *,
    max_utf16: int = 3450,
) -> list[str]:
    """Every message linked to the signal, as bounded Telegram segments.

    Contract: the returned segments are ordered like the input messages,
    each segment fits ``max_utf16`` units so no segment can be dropped by
    Telegram, the first segment carries the intro header and the last one
    the unresolved-status footer, and continuity markers ``(بخش k/m)`` are
    shown whenever more than one segment is needed. Original message text
    is only *shortened for display* with an explicit marker; the stored
    rows are never modified.
    """

    if max_utf16 <= 400:
        raise ValueError("max_utf16 must leave room for headers")

    def item_line(item: dict[str, Any], body_override: str | None = None) -> list[str]:
        head = (
            f"• پیام {_fmt(item.get('message_id'))} — "
            f"{_fmt(item.get('datetime'))} — "
            f"{LINK_STATUS_FA.get(str(item.get('status')), _fmt(item.get('status')))}"
            f" ({METHOD_FA.get(str(item.get('method')), _fmt(item.get('method')))})"
        )
        if item.get("edit_captures"):
            head += f" — ویرایش ثبت‌شده ×{item['edit_captures']}"
        lines: list[str] = [head]
        lines.append(
            f"  طبقه‌بندی: {classification_fa(str(item.get('classification')))}"
        )
        if item.get("reason"):
            lines.append(f"  دلیل: {shorten_utf16(_fmt(item.get('reason')), 200)}")
        if item.get("edit_datetime"):
            lines.append(f"  ویرایش‌شده: {_fmt(item.get('edit_datetime'))}")
        body = item.get("text") or item.get("caption") or ""
        if body:
            shown = body if body_override is None else body_override
            if utf16_len(shown) > 600:
                shown = shorten_utf16(shown, 600) + " … (کوتاه‌شده)"
            lines.append(f"  متن: {shown}")
        lines.append("")
        return lines

    footer = (
        "پیام‌های با وضعیت «حل‌نشده» مرجعی دارند که پیام اصلی سیگنال آن در "
        "آرشیو ذخیره نشده است؛ متن خام هرگز حذف یا جایگزین نمی‌شود."
    )

    # Group item lines into segment bodies within the per-segment budget,
    # leaving room for intro/continuation headers and the footer.
    reserved = 700
    body_cap = max_utf16 - reserved
    bodies: list[list[str]] = []
    current: list[str] = []
    used = 0
    for item in messages:
        block = item_line(item)
        width = utf16_len("\n".join(block))
        if width > body_cap:
            # One huge item still fits alone: bound its displayed body harder.
            overflow = width - body_cap
            body = item.get("text") or item.get("caption") or ""
            if body:
                tighter = max(50, 600 - overflow)
                block = item_line(
                    item,
                    body_override=shorten_utf16(body, tighter) + " … (کوتاه‌شده)",
                )
                width = utf16_len("\n".join(block))
        if current and used + width > body_cap:
            bodies.append(current)
            current = []
            used = 0
        current.extend(block)
        used += width
    if current:
        bodies.append(current)

    intro = f"💌 پیام‌های مرتبط با سیگنال #{_fmt(logical_id)}"
    intro += f" ({len(messages)} پیام)" if messages else ""
    if not messages:
        return [intro + "\n\nپیام مرتبطی ثبت نشده است.\n\n" + footer]
    total = len(bodies)
    segments: list[str] = []
    for index, body in enumerate(bodies, start=1):
        if total == 1:
            head = intro
        else:
            head = f"{intro} — بخش {index} از {total}"
        tail = "\n" + footer if index == total else "\n"
        segments.append(head + "\n\n" + "\n".join(body).rstrip("\n") + tail)
    return segments


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
