"""Independent, durable Telegram notification dispatcher.

The trading database is read-only here.  Relay checkpoints and per-recipient
delivery state live in a separate SQLite database so network I/O never holds
a transaction or lock in the trading process.
"""
from __future__ import annotations

import inspect
import json
import logging
import random
import sqlite3
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .control.bot_api import (
    BotApiError,
    BotApiForbiddenError,
    BotApiNotSentError,
    BotApiRateLimitError,
    BotApiTransportError,
)
from .trading.audit import redact_text

log = logging.getLogger("ingest.notifications.dispatcher")
MAX_ATTEMPTS = 10
BASE_BACKOFF_S = 5
MAX_BACKOFF_S = 300
# The approved policy caps every scheduled interval at five minutes.  A
# Telegram ``retry_after`` longer than this is honoured up to the same ceiling;
# the delivery remains retryable and the next 429 will reschedule it.
MAX_RETRY_AFTER_S = MAX_BACKOFF_S
STALE_CLAIM_S = 120
# A worker that has not refreshed its heartbeat within this window is treated
# as not making progress (dead or hung).
STALE_HEARTBEAT_S = 60
MAX_MESSAGE_CHARS = 3900
SEVERITY_RANK = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
OWNER_ROLES = ("owner", "owner_b")

# Map the domain vocabulary onto existing user-facing preference categories.
NOTIFICATION_CATEGORY_BY_EVENT = {
    "signal.received": "signal_accepted",
    "signal.rejected": "signal_decision",
    "intent.created": "signal_accepted",
    "intent.state_changed": "order_lifecycle",
    "intent.submission_accepted": "order_lifecycle",
    "trade.state_changed": "order_lifecycle",
    "trade_order.state_changed": "order_lifecycle",
    "oco_group.state_changed": "oco_lifecycle",
    "oco_leg.state_changed": "oco_lifecycle",
    "fill.recorded": "order_lifecycle",
    "exchange.write_outcome": "exchange_operation",
    "reconciliation.outcome": "reconciliation",
    "trade.protection_critical": "critical",
    "system.service_event": "critical",
    "system.control_event": "critical",
}

DEFAULT_CATEGORY_PREFERENCES = {
    "owner": {
        "signal_decision": False, "signal_accepted": True,
        "exchange_operation": True, "order_lifecycle": True,
        "oco_lifecycle": True, "take_profit": True, "stop_loss": True,
        "reconciliation": True, "critical": True, "diagnostic": False,
        "notification_delivery": False, "recovery": False,
    },
    "developer": {
        category: True for category in (
            "signal_decision", "signal_accepted", "exchange_operation",
            "order_lifecycle", "oco_lifecycle", "take_profit", "stop_loss",
            "reconciliation", "critical", "diagnostic",
            "notification_delivery", "recovery",
        )
    },
}


def notification_category(event_type: str) -> str:
    return NOTIFICATION_CATEGORY_BY_EVENT.get(event_type, "diagnostic")


def recipient_preference_enabled(
    role: str, category: str, preferences: dict[tuple[str, str], bool]
) -> bool:
    # Honor an explicit Owner B override first; otherwise inherit Owner A's
    # explicit category preference, then Owner A's defaults.
    key = (role, category)
    if key in preferences:
        return preferences[key]
    if role == "owner_b":
        owner_key = ("owner", category)
        if owner_key in preferences:
            return preferences[owner_key]
        role = "owner"
    return DEFAULT_CATEGORY_PREFERENCES.get(role, {}).get(category, False)


def severity_enabled(severity: str, minimum: str) -> bool:
    return SEVERITY_RANK.get(str(severity).upper(), SEVERITY_RANK["INFO"]) >= SEVERITY_RANK.get(
        str(minimum).upper(), SEVERITY_RANK["INFO"]
    )


def recipient_minimum_severity(
    role: str,
    settings: dict[str, str],
    global_minimum: str,
) -> str:
    """Return this recipient's threshold, falling back to the shared default.

    Per-recipient overrides live in ``control_settings`` under
    ``notification_min_severity.<role>`` (for example
    ``notification_min_severity.owner_b``).  They are read-only inputs to the
    dispatcher and never touch the trading transaction path.
    """

    role_value = settings.get(f"notification_min_severity.{role}")
    value = str(role_value or global_minimum).upper()
    return value if value in SEVERITY_RANK else "INFO"


# States/outcomes that represent unresolved exchange-write, protection, or
# reconciliation uncertainty.  Owner alerts in these states are action-needed
# and must never be dropped by routine category/severity preferences.
_OWNER_SAFETY_UNKNOWN_STATES = (
    "unknown_requires_reconciliation",
    "needs_reconciliation",
    "unprotected_critical",
)
_OWNER_SAFETY_UNCERTAIN_OUTCOMES = ("uncertain", "ambiguous", "not_found", "discrepancy")


def owner_safety_alert_bypasses_preferences(
    role: str,
    severity: str,
    category: str,
    event_type: str,
    payload: dict[str, Any] | None = None,
) -> bool:
    """Return True when an owner alert is safety-critical.

    Safety alerts bypass both the routine minimum-severity threshold and the
    category preference.  They are never suppressible by ordinary owner
    tuning; a genuine rejected/ambiguous trading signal is owner-visible by
    policy.  CRITICAL events additionally go to *every configured recipient*.
    """

    if role not in OWNER_ROLES:
        return False
    if str(severity).upper() == "CRITICAL":
        return True
    if event_type in ("signal.rejected", "trade.protection_critical") or event_type.endswith(
        "rejected_locally"
    ):
        return True
    payload = payload or {}
    unknown_state = (
        event_type in ("intent.state_changed", "trade_order.state_changed", "trade.state_changed")
        and payload.get("new_state") in _OWNER_SAFETY_UNKNOWN_STATES
    )
    uncertain_outcome = (
        event_type in ("exchange.write_outcome", "reconciliation.outcome")
        and payload.get("outcome") in _OWNER_SAFETY_UNCERTAIN_OUTCOMES
    )
    unprotected_group = (
        event_type == "oco_group.state_changed"
        and payload.get("protection_status") in ("unprotected", "unprotected_critical", "failed")
    )
    return unknown_state or uncertain_outcome or unprotected_group


# Backwards-compatible name used by earlier callers/tests.
owner_safety_alert_bypasses_min_severity = owner_safety_alert_bypasses_preferences


def delivery_decision(
    role: str,
    severity: str,
    category: str,
    event_type: str,
    payload: dict[str, Any] | None,
    preferences: dict[tuple[str, str], bool],
    settings: dict[str, str],
    global_minimum: str,
) -> tuple[str, str | None]:
    """Decide one recipient's delivery status.

    Returns ``(status, reason)`` where status is ``pending`` or ``suppressed``.
    Suppression is always persisted, so operator history is complete.
    """

    severity_value = str(severity).upper()
    minimum = recipient_minimum_severity(role, settings, global_minimum)

    # DEBUG is developer diagnostics only, even if a legacy category row or a
    # stale preference would otherwise enable it for an owner.
    if severity_value == "DEBUG":
        if role in OWNER_ROLES:
            return "suppressed", "DEBUG is developer-only"
        # DEBUG is the developer diagnostic stream.  It is gated by the
        # developer's category preference but not by the routine owner-noise
        # severity threshold; it can never reach an owner.
        if not recipient_preference_enabled(role, category, preferences):
            return "suppressed", "category disabled"
        return "pending", None

    # CRITICAL always reaches every configured recipient, independent of
    # category enablement and minimum severity.
    if severity_value == "CRITICAL":
        return "pending", None

    if owner_safety_alert_bypasses_preferences(role, severity_value, category, event_type, payload):
        return "pending", None

    if not recipient_preference_enabled(role, category, preferences):
        return "suppressed", "category disabled"
    if not severity_enabled(severity_value, minimum):
        return "suppressed", f"below minimum severity {minimum}"
    return "pending", None


def _is_missing_reply_error(exc: BotApiError) -> bool:
    text = str(exc).lower()
    return "repl" in text and ("not found" in text or "message to be replied" in text)


def _supports_reply_to(bot: Any) -> bool:
    try:
        return "reply_to_message_id" in inspect.signature(bot.send_message).parameters
    except (TypeError, ValueError):
        return False


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Safely parse a domain event payload, never raising on malformed JSON."""
    try:
        value = json.loads(str(event.get("payload_json") or "{}"))
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _safe_text(value: Any, *, limit: int = 500) -> str:
    """Bound, redact, and neutralize untrusted text for Telegram.

    Messages are sent with ``parse_mode=None`` and only control characters are
    removed; Telegram therefore never interprets untrusted signal text as
    markup.  Credentials are redacted with the same patterns used by the audit
    journal.
    """

    text = redact_text(value, limit=limit) or ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = "".join(ch for ch in text if ch in ("\n", "\t") or ord(ch) >= 32)
    return text.strip()


def _now() -> datetime:
    return datetime.now(UTC)


def _stamp(value: datetime | None = None) -> str:
    return (value or _now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _symbol_tag(symbol: Any) -> str:
    raw = str(symbol or "").upper()
    safe = "".join(ch for ch in raw if ch.isalnum() or ch == "_")
    return f"#{safe}" if safe else ""


def heartbeat_status(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
    stale_after_s: int = STALE_HEARTBEAT_S,
) -> dict[str, Any]:
    """Derive worker liveness from the persisted heartbeat.

    A missing heartbeat means the worker has never run (or never refreshed it).
    A heartbeat older than ``stale_after_s`` means the worker is not making
    progress — it is either dead or hung. A heartbeat alone cannot distinguish a
    dead process from a hung-but-alive one; process existence never proves
    health, so operators must not equate ``systemctl is-active`` with delivery.
    """
    last_cycle = state.get("last_cycle_at")
    if not last_cycle:
        return {"last_cycle_at": None, "heartbeat_age_s": None, "heartbeat_stale": None}
    try:
        last_dt = datetime.fromisoformat(str(last_cycle).replace("Z", "+00:00"))
    except ValueError:
        return {"last_cycle_at": str(last_cycle), "heartbeat_age_s": None, "heartbeat_stale": None}
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=UTC)
    reference = now or datetime.now(UTC)
    age = max(0.0, (reference - last_dt).total_seconds())
    return {
        "last_cycle_at": str(last_cycle),
        "heartbeat_age_s": round(age, 1),
        "heartbeat_stale": age > stale_after_s,
    }


def render_domain_event(row: dict[str, Any]) -> str:
    """Render a compact Persian notification from a validated domain event.

    Every string field is treated as untrusted: it is redacted, bounded, and
    stripped of control characters before it reaches the Telegram transport.
    An unknown event type renders a safe generic fallback instead of raising.
    """

    event_type = _safe_text(row.get("event_type") or "event.unknown", limit=120)
    payload = _event_payload(row)
    payload = {
        key: (_safe_text(value, limit=1500) if isinstance(value, str) else value)
        for key, value in payload.items()
    }
    symbol = _symbol_tag(row.get("symbol") or payload.get("symbol"))
    prefix = f"{symbol}\n" if symbol else ""
    if event_type == "signal.received":
        return (
            f"📨 سیگنال جدید دریافت شد\n{prefix}"
            f"جهت: {payload.get('side') or '—'}\n"
            f"ورود: {payload.get('entry_price') or '—'}\n"
            f"هدف اول (TP1): {payload.get('tp1_price') or '—'}\n"
            f"هدف دوم (TP2): {payload.get('tp2_price') or '—'}\n"
            f"حد ضرر: {payload.get('stop_loss_price') or '—'}"
        )
    if event_type in ("system.service_event", "system.control_event"):
        heading = "رویداد سامانه" if event_type == "system.service_event" else "رویداد کنترل ربات"
        return f"ℹ️ {heading}\n{payload.get('message') or payload.get('event_key') or '—'}"
    if event_type == "trade.protection_critical":
        detail = str(payload.get("detail") or "جزئیات در دسترس نیست")
        return f"🚨 هشدار بحرانی حفاظت معامله\n{prefix}{detail}"

    if event_type == "signal.rejected":
        return f"🚫 سیگنال رد شد؛ سفارشی ارسال نشد\n{prefix}دلیل: {payload.get('reason') or '—'}"
    if event_type == "intent.created":
        return (
            f"📥 سیگنال معاملاتی ثبت شد\n{prefix}"
            f"ورود: {payload.get('entry_price') or '—'}\n"
            f"هدف اول (TP1): {payload.get('tp1_price') or '—'}\n"
            f"هدف دوم (TP2): {payload.get('tp2_price') or '—'}\n"
            f"حد ضرر: {payload.get('stop_loss_price') or '—'}\n"
            f"وضعیت سفارش: {payload.get('state') or '—'}"
        )
    if event_type == "intent.submission_accepted":
        details = [f"شناسه سفارش صرافی: {payload.get('exchange_order_id') or '—'}"]
        if payload.get("client_order_id"):
            details.append(f"شناسه سفارش مشتری: {payload['client_order_id']}")
        if payload.get("exchange_status"):
            details.append(f"وضعیت صرافی: {payload['exchange_status']}")
        return f"✅ پذیرش ارسال سفارش ورود در صرافی تأیید شد\n{prefix}" + "\n".join(details)
    if event_type.endswith(".state_changed"):
        previous = payload.get("previous_state") or "—"
        current = payload.get("new_state") or "—"
        labels = {
            "intent.state_changed": "تغییر وضعیت سفارش ورود",
            "trade.state_changed": "تغییر وضعیت معامله",
            "trade_order.state_changed": "تغییر وضعیت سفارش حفاظتی",
            "oco_group.state_changed": "تغییر وضعیت گروه OCO",
            "oco_leg.state_changed": "تغییر وضعیت پایه OCO",
        }
        if event_type == "intent.state_changed":
            state_labels = {
                "submitting": "ارسال سفارش ورود آغاز شد",
                "submitted": "سفارش ورود در صرافی پذیرفته شد",
                "open": "سفارش ورود باز است",
                "partially_filled": "سفارش ورود بخشی پر شده است",
                "filled": "سفارش ورود کامل پر شد",
                "rejected": "سفارش ورود رد شد؛ سفارشی اجرا نشد یا ادامه نیافت",
                "failed": "اجرای سفارش ورود ناموفق بود",
                "unknown_requires_reconciliation": "نتیجهٔ ارسال سفارش ورود نامشخص است؛ تطبیق لازم است",
                "canceled_local": "سفارش ورود محلی لغو شد و به صرافی ارسال نشد",
                "canceled": "سفارش ورود لغو شد",
            }
            details = [f"از: {previous}", f"به: {current}"]
            for key, label in (
                ("exchange_order_id", "شناسه سفارش صرافی"),
                ("exchange_status", "وضعیت صرافی"),
                ("executed_qty", "مقدار پرشده"),
                ("cumulative_quote", "ارزش تجمعی اجرا"),
            ):
                if payload.get(key) is not None:
                    details.append(f"{label}: {payload[key]}")
            if payload.get("fill_sync_required"):
                details.append("تاریخچهٔ Fill هنوز کامل همگام نشده است")
            if payload.get("reason"):
                details.append(f"توضیح: {payload['reason']}")
            icon = "⚠️" if current == "unknown_requires_reconciliation" else (
                "🚫" if current in ("rejected", "failed") else "📥"
            )
            heading = state_labels.get(current, labels.get(event_type, "تغییر وضعیت سفارش ورود"))
            return f"{icon} {heading}\n{prefix}" + "\n".join(details)
        if event_type == "trade_order.state_changed":
            kind = str(payload.get("kind") or "سفارش")
            state = str(current)
            if state == "open":
                heading = "سفارش حفاظتی در صرافی پذیرفته شد"
            elif state == "rejected" and payload.get("retryable") and payload.get("next_attempt_at"):
                heading = "تلاش سفارش حفاظتی رد شد؛ تلاش مجدد زمان‌بندی شد"
            elif state == "rejected":
                heading = "سفارش حفاظتی رد شد"
            elif state == "unknown_requires_reconciliation":
                heading = "نتیجهٔ ارسال سفارش حفاظتی نامشخص است؛ تطبیق لازم است"
            else:
                heading = labels.get(event_type, "تغییر وضعیت سفارش حفاظتی")
            details = [f"نوع سفارش: {kind}"]
            if payload.get("price"):
                details.append(f"قیمت: {payload['price']}")
            if payload.get("quantity"):
                details.append(f"مقدار: {payload['quantity']}")
            if payload.get("stop_trigger_price"):
                details.append(f"قیمت فعال‌سازی SL: {payload['stop_trigger_price']}")
            if payload.get("exchange_order_id"):
                details.append(f"شناسه سفارش صرافی: {payload['exchange_order_id']}")
            if payload.get("exchange_status"):
                details.append(f"وضعیت صرافی: {payload['exchange_status']}")
            if state in ("rejected", "unknown_requires_reconciliation"):
                if payload.get("attempt") is not None:
                    attempt = int(payload["attempt"]) + (1 if state == "rejected" else 0)
                    details.append(f"تلاش: {attempt}")
                if payload.get("reason"):
                    details.append(f"دلیل: {payload['reason']}")
            if state == "rejected" and payload.get("retryable"):
                details.append("قابل تلاش مجدد: بله")
                if payload.get("next_attempt_at"):
                    next_attempt = int(payload.get("attempt") or 0) + 2
                    details.append(f"تلاش بعدی: {next_attempt}")
                    details.append(f"زمان تلاش بعدی (UTC): {payload['next_attempt_at']}")
            icon = "🕒" if state == "rejected" and payload.get("retryable") else "🔄"
            return f"{icon} {heading}\n{prefix}" + "\n".join(details)
        if event_type == "trade.state_changed":
            state_labels = {
                "entry_active": "سفارش ورود فعال شد",
                "position_open": "موقعیت خرید تأیید شد",
                "oco_submitting": "ارسال سفارش‌های حفاظتی OCO آغاز شد",
                "oco_active": "حفاظت OCO فعال شد",
                "oco_partial": "حفاظت OCO ناقص است",
                "unprotected_critical": "موقعیت بدون حفاظت است؛ خریدهای جدید مسدودند",
                "needs_reconciliation": "معامله نیازمند تطبیق و بررسی است",
                "closed": "معامله بسته شد",
                "tp2_completed": "هر دو هدف سود تکمیل شدند",
            }
            heading = state_labels.get(current, labels.get(event_type, "تغییر وضعیت معامله"))
            reason = payload.get("reason")
            if current == "closed" and reason:
                if "no position opened" in str(reason):
                    heading = "ورود پر نشد؛ معامله بدون ایجاد موقعیت بسته شد"
                elif "reconciliation:" in str(reason):
                    heading = "معامله پس از تطبیق قطعی بسته شد"
            details = [f"از: {previous}", f"به: {current}"]
            if reason:
                details.append(f"توضیح: {reason}")
            return f"🔄 {heading}\n{prefix}" + "\n".join(details)
        return f"🔄 {labels.get(event_type, 'تغییر وضعیت')}\n{prefix}از: {previous}\nبه: {current}"
    if event_type == "exchange.write_outcome":
        outcome = str(payload.get("outcome") or "unknown")
        labels = {
            "success": "پاسخ موفق از صرافی دریافت شد",
            "error": "درخواست تغییر‌دهنده با خطا مواجه شد",
            "uncertain": "نتیجهٔ درخواست تغییر‌دهنده نامشخص است؛ ارسال مجدد کور انجام نمی‌شود",
        }
        details = [f"عملیات: {payload.get('operation_type') or '—'}"]
        if payload.get("response_status"):
            details.append(f"وضعیت صرافی: {payload['response_status']}")
        if payload.get("error_class"):
            details.append(f"نوع خطا: {payload['error_class']}")
        if payload.get("exchange_code"):
            details.append(f"کد صرافی: {payload['exchange_code']}")
        heading = "⚠️" if outcome != "success" else "✅"
        return (
            f"{heading} نتیجهٔ درخواست صرافی\n{prefix}"
            f"{labels.get(outcome, outcome)}\n" + "\n".join(details)
        )
    if event_type == "reconciliation.outcome":
        outcome = str(payload.get("outcome") or "unknown")
        labels = {
            "consistent": "وضعیت محلی و صرافی منطبق است",
            "resolved": "وضعیت سفارش با شواهد صرافی تعیین تکلیف شد",
            "discrepancy": "مغایرت بین وضعیت محلی و صرافی شناسایی شد",
            "ambiguous": "نتیجهٔ تطبیق سفارش هنوز نامشخص است",
            "not_found": "سفارش در بررسی صرافی پیدا نشد",
        }
        return (
            f"🔎 نتیجهٔ تطبیق سفارش\n{prefix}{labels.get(outcome, outcome)}\n"
            f"وضعیت محلی: {payload.get('local_state') or '—'}\n"
            f"وضعیت صرافی: {payload.get('exchange_state') or '—'}"
        )
    if event_type == "fill.recorded":
        return (
            f"💹 اجرای معامله در صرافی ثبت شد\n{prefix}"
            f"مقدار: {payload.get('quantity') or '—'}\n"
            f"قیمت: {payload.get('price') or '—'}\n"
            f"مبلغ: {payload.get('quote_amount') or '—'}"
        )
    event_id = _safe_text(row.get("event_id") or "—", limit=80)
    return f"ℹ️ رخداد جدید: {event_type}\n{prefix}شناسه: {event_id}"


def message_chunks(text: str, limit: int = MAX_MESSAGE_CHARS) -> list[str]:
    """Split one rendered event into ordered, bounded Telegram fragments.

    Chunks are never reordered or coalesced with another event.  Each fragment
    is a separate durable delivery so a retry resends only the fragment that
    failed.  Newline boundaries are preferred but the split point is always <=
    ``limit`` and the concatenation preserves the original text.
    """

    if len(text) <= limit:
        return [text] if text else [""]
    chunks: list[str] = []
    offset = 0
    while offset < len(text):
        end = min(offset + limit, len(text))
        if end < len(text):
            newline = text.rfind("\n", offset, end)
            if newline > offset + limit // 2:
                end = newline + 1
        chunks.append(text[offset:end])
        offset = end
    return chunks


def event_thread_key(
    event_type: str,
    *,
    aggregate_type: str | None,
    aggregate_id: str | None,
    correlation_id: str | None,
    payload: dict[str, Any] | None,
    severity: str,
) -> str | None:
    """Return the per-trade/per-intent threading scope for one event.

    The first successfully delivered lifecycle message for a ``(recipient,
    thread_key)`` pair becomes that recipient's root; later lifecycle events
    reply to it.  CRITICAL alerts and signal/system events are standalone.
    """

    if str(severity).upper() == "CRITICAL":
        return None
    if event_type in ("trade.protection_critical",) or event_type.startswith("signal."):
        return None
    if event_type.startswith("system.") or event_type in (
        "exchange.write_outcome",
        "reconciliation.outcome",
    ):
        return None
    payload = payload or {}
    aggregate = str(aggregate_type or "")
    if aggregate == "trade" and aggregate_id:
        return f"trade:{aggregate_id}"
    if aggregate in ("trade_order", "oco_group", "oco_leg") and correlation_id:
        return f"trade:{correlation_id}"
    if aggregate == "intent" and aggregate_id:
        return f"intent:{aggregate_id}"
    if aggregate == "exchange_order":
        intent_id = payload.get("intent_id") or correlation_id
        return f"intent:{intent_id}" if intent_id else None
    return None


class DeliveryStore:
    """SQLite ledger owned exclusively by the notification dispatcher."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dispatcher_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS relayed_events (
                event_id TEXT PRIMARY KEY,
                source_audit_id INTEGER NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                relayed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS relayed_legacy_notifications (
                notification_id INTEGER PRIMARY KEY,
                relayed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                delivery_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                recipient_role TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                notification_category TEXT NOT NULL DEFAULT 'diagnostic',
                severity TEXT NOT NULL,
                message_text TEXT NOT NULL,
                part_no INTEGER NOT NULL DEFAULT 1,
                part_count INTEGER NOT NULL DEFAULT 1,
                thread_key TEXT,
                reply_to_message_id INTEGER,
                status TEXT NOT NULL CHECK (status IN
                    ('pending','sending','retry','sent','ambiguous','suppressed','dead_letter')),
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                claim_token TEXT,
                claimed_at TEXT,
                telegram_message_id INTEGER,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(event_id, recipient_role, part_no)
            );
            CREATE INDEX IF NOT EXISTS idx_deliveries_ready
                ON deliveries(status, next_attempt_at, created_at);
            CREATE TABLE IF NOT EXISTS delivery_attempts (
                delivery_id TEXT NOT NULL REFERENCES deliveries(delivery_id),
                attempt_no INTEGER NOT NULL,
                attempted_at TEXT NOT NULL,
                outcome TEXT NOT NULL,
                error TEXT,
                telegram_message_id INTEGER,
                retry_after_s INTEGER,
                PRIMARY KEY(delivery_id, attempt_no)
            );
            CREATE TABLE IF NOT EXISTS delivery_threads (
                recipient_role TEXT NOT NULL,
                thread_key TEXT NOT NULL,
                root_message_id INTEGER NOT NULL,
                root_delivery_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(recipient_role, thread_key)
            );
            """
        )
        self._upgrade_schema()

    def _upgrade_schema(self) -> None:
        """Add or rebuild columns for ambient pre-release delivery ledgers.

        Delivery state is intentionally disposable/independent from trading.
        The ledger is created by the dispatcher itself, so a pre-release DB may
        carry an older unique key.  Rows are preserved verbatim (part 1 of 1)
        while the table is rebuilt; no trading database is ever touched.
        """

        columns = {str(row["name"]) for row in self.conn.execute("PRAGMA table_info(deliveries)")}
        if "part_no" not in columns:
            if "notification_category" not in columns:
                self.conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN notification_category TEXT NOT NULL DEFAULT 'diagnostic'"
                )
            self.conn.execute("PRAGMA foreign_keys=OFF")
            try:
                self.conn.executescript(
                    """
                    CREATE TABLE deliveries_v2 (
                        delivery_id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL,
                        recipient_role TEXT NOT NULL,
                        chat_id INTEGER NOT NULL,
                        event_type TEXT NOT NULL,
                        notification_category TEXT NOT NULL DEFAULT 'diagnostic',
                        severity TEXT NOT NULL,
                        message_text TEXT NOT NULL,
                        part_no INTEGER NOT NULL DEFAULT 1,
                        part_count INTEGER NOT NULL DEFAULT 1,
                        thread_key TEXT,
                        reply_to_message_id INTEGER,
                        status TEXT NOT NULL CHECK (status IN
                            ('pending','sending','retry','sent','ambiguous','suppressed','dead_letter')),
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at TEXT,
                        claim_token TEXT,
                        claimed_at TEXT,
                        telegram_message_id INTEGER,
                        last_error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(event_id, recipient_role, part_no)
                    );
                    INSERT INTO deliveries_v2 (
                        delivery_id,event_id,recipient_role,chat_id,event_type,
                        notification_category,severity,message_text,part_no,part_count,
                        thread_key,reply_to_message_id,status,attempts,next_attempt_at,
                        claim_token,claimed_at,telegram_message_id,last_error,created_at,updated_at
                    )
                    SELECT delivery_id,event_id,recipient_role,chat_id,event_type,
                        notification_category,severity,message_text,1,1,
                        NULL,NULL,status,attempts,next_attempt_at,
                        claim_token,claimed_at,telegram_message_id,last_error,created_at,updated_at
                    FROM deliveries;
                    DROP TABLE deliveries;
                    ALTER TABLE deliveries_v2 RENAME TO deliveries;
                    CREATE INDEX IF NOT EXISTS idx_deliveries_ready
                        ON deliveries(status, next_attempt_at, created_at);
                    """
                )
            finally:
                self.conn.execute("PRAGMA foreign_keys=ON")
        else:
            if "notification_category" not in columns:
                self.conn.execute(
                    "ALTER TABLE deliveries ADD COLUMN notification_category TEXT NOT NULL DEFAULT 'diagnostic'"
                )
            for name, ddl in (
                ("part_count", "INTEGER NOT NULL DEFAULT 1"),
                ("thread_key", "TEXT"),
                ("reply_to_message_id", "INTEGER"),
            ):
                if name not in columns:
                    self.conn.execute(f"ALTER TABLE deliveries ADD COLUMN {name} {ddl}")

    def transaction(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def close(self) -> None:
        self.conn.close()

    def set_state(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO dispatcher_state(key,value,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            (key, value, _stamp()),
        )

    def cursor(self) -> int:
        row = self.conn.execute("SELECT value FROM dispatcher_state WHERE key='source_cursor'").fetchone()
        return int(row["value"]) if row else 0

    def initialize_cursor(self, source_max_audit_id: int) -> None:
        """Establish a no-replay boundary on first startup only."""
        self.conn.execute(
            "INSERT OR IGNORE INTO dispatcher_state(key,value,updated_at) VALUES('source_cursor',?,?)",
            (str(max(0, int(source_max_audit_id))), _stamp()),
        )

    def legacy_cursor(self) -> int:
        row = self.conn.execute(
            "SELECT value FROM dispatcher_state WHERE key='legacy_notification_cursor'"
        ).fetchone()
        return int(row["value"]) if row else 0

    def initialize_legacy_cursor(self, source_max_notification_id: int) -> None:
        """Establish an independent no-replay boundary for the compatibility outbox."""
        self.conn.execute(
            "INSERT OR IGNORE INTO dispatcher_state(key,value,updated_at) "
            "VALUES('legacy_notification_cursor',?,?)",
            (str(max(0, int(source_max_notification_id))), _stamp()),
        )

    def relay_batch(
        self,
        events: list[dict[str, Any]],
        recipients: dict[str, int],
        preferences: dict[tuple[str, str], bool] | None = None,
        minimum_severity: str = "INFO",
        settings: dict[str, str] | None = None,
    ) -> int:
        if not events:
            return 0
        inserted = 0
        now = _stamp()
        preferences = preferences or {}
        settings = settings or {}
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = self.cursor()
            for event in events:
                audit_id = int(event["audit_id"])
                if audit_id <= cursor:
                    continue
                event_id = str(event["event_id"])
                event_type = str(event["event_type"])
                category = notification_category(event_type)
                severity = str(event.get("severity") or "INFO").upper()
                payload = _event_payload(event)
                try:
                    body = render_domain_event(event)
                except Exception:  # noqa: BLE001 - one malformed event must not stall the relay
                    body = (
                        "ℹ️ رخداد جدید ثبت شد اما متن آن قابل نمایش نیست.\n"
                        f"نوع: {_safe_text(event_type, limit=120)}\n"
                        f"شناسه: {_safe_text(event_id, limit=80)}"
                    )
                self.conn.execute(
                    "INSERT OR IGNORE INTO relayed_events(event_id,source_audit_id,event_type,relayed_at) "
                    "VALUES(?,?,?,?)",
                    (event_id, audit_id, event_type, now),
                )
                try:
                    audience = json.loads(
                        str(event.get("audience_json") or '["owner","owner_b","developer"]')
                    )
                except (TypeError, ValueError):
                    audience = ["owner", "owner_b", "developer"]
                if not isinstance(audience, list):
                    audience = ["owner", "owner_b", "developer"]
                # CRITICAL bypasses audience/category/severity and reaches every
                # configured recipient (the approved severity policy).
                if severity == "CRITICAL":
                    roles = list(recipients)
                else:
                    roles = [str(role) for role in audience]
                chunks = message_chunks(body)
                thread_key = event_thread_key(
                    event_type,
                    aggregate_type=event.get("aggregate_type"),
                    aggregate_id=event.get("aggregate_id"),
                    correlation_id=event.get("correlation_id"),
                    payload=payload,
                    severity=severity,
                )
                for role in roles:
                    chat_id = recipients.get(role)
                    if not chat_id:
                        continue
                    delivery_status, reason = delivery_decision(
                        role,
                        severity,
                        category,
                        event_type,
                        payload,
                        preferences,
                        settings,
                        minimum_severity,
                    )
                    for part_no, chunk in enumerate(chunks, 1):
                        cur = self.conn.execute(
                            """
                            INSERT OR IGNORE INTO deliveries
                            (delivery_id,event_id,recipient_role,chat_id,event_type,notification_category,
                             severity,message_text,part_no,part_count,thread_key,status,last_error,
                             created_at,updated_at)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                uuid.uuid4().hex,
                                event_id,
                                role,
                                int(chat_id),
                                event_type,
                                category,
                                severity,
                                chunk,
                                part_no,
                                len(chunks),
                                thread_key,
                                delivery_status,
                                reason,
                                now,
                                now,
                            ),
                        )
                        inserted += max(0, cur.rowcount)
                cursor = audit_id
            self.conn.execute(
                "INSERT INTO dispatcher_state(key,value,updated_at) VALUES('source_cursor',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (str(cursor), now),
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        return inserted

    def _next_attempt_no(self, delivery_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(attempt_no),0)+1 AS next_no FROM delivery_attempts WHERE delivery_id=?",
            (delivery_id,),
        ).fetchone()
        return int(row["next_no"])

    def recover_stale_claims(self, older_than_s: int = STALE_CLAIM_S) -> int:
        cutoff = _stamp(_now() - timedelta(seconds=max(1, older_than_s)))
        now = _stamp()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            rows = self.conn.execute(
                "SELECT delivery_id,attempts FROM deliveries "
                "WHERE status='sending' AND claimed_at < ?",
                (cutoff,),
            ).fetchall()
            for row in rows:
                attempt = int(row["attempts"]) + 1
                attempt_no = self._next_attempt_no(str(row["delivery_id"]))
                self.conn.execute(
                    "INSERT INTO delivery_attempts "
                    "(delivery_id,attempt_no,attempted_at,outcome,error) VALUES(?,?,?,'ambiguous',?)",
                    (row["delivery_id"], attempt_no, now, "worker stopped during send; outcome unknown"),
                )
                self.conn.execute(
                    "UPDATE deliveries SET status='ambiguous',attempts=?,claim_token=NULL,"
                    "last_error=?,updated_at=? WHERE delivery_id=? AND status='sending'",
                    (attempt, "worker stopped during send; outcome unknown", now, row["delivery_id"]),
                )
            self.conn.execute("COMMIT")
            return len(rows)
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def claim_next(self) -> dict[str, Any] | None:
        now = _stamp()
        token = uuid.uuid4().hex
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM deliveries d WHERE status IN ('pending','retry') "
                "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
                "AND (part_no = 1 OR NOT EXISTS ("
                "  SELECT 1 FROM deliveries earlier WHERE earlier.event_id = d.event_id "
                "  AND earlier.recipient_role = d.recipient_role AND earlier.part_no < d.part_no "
                "  AND earlier.status IN ('pending','sending','retry'))) "
                "ORDER BY created_at,rowid LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                self.conn.execute("COMMIT")
                return None
            sibling_attempts = self.conn.execute(
                "SELECT COALESCE(SUM(attempts),0) AS n FROM deliveries "
                "WHERE event_id=? AND recipient_role=? AND delivery_id<>?",
                (row["event_id"], row["recipient_role"], row["delivery_id"]),
            ).fetchone()["n"]
            if int(row["attempts"]) + int(sibling_attempts) >= MAX_ATTEMPTS:
                self.conn.execute(
                    "UPDATE deliveries SET status='dead_letter',last_error='attempt limit reached',updated_at=? "
                    "WHERE delivery_id=?",
                    (now, row["delivery_id"]),
                )
                self.conn.execute("COMMIT")
                return None
            cur = self.conn.execute(
                "UPDATE deliveries SET status='sending',claim_token=?,claimed_at=?,updated_at=? "
                "WHERE delivery_id=? AND status IN ('pending','retry')",
                (token, now, now, row["delivery_id"]),
            )
            if cur.rowcount != 1:
                self.conn.execute("COMMIT")
                return None
            claimed = dict(row)
            claimed["claim_token"] = token
            self.conn.execute("COMMIT")
            return claimed
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def finish(
        self,
        delivery: dict[str, Any],
        outcome: str,
        *,
        error: str | None = None,
        message_id: int | None = None,
        retry_after_s: int | None = None,
        reply_to_message_id: int | None = None,
        establish_thread: bool = False,
    ) -> None:
        now_dt = _now()
        now = _stamp(now_dt)
        attempt = int(delivery["attempts"]) + 1
        if outcome == "retry" and attempt >= MAX_ATTEMPTS:
            outcome = "dead_letter"
            error = error or "attempt limit reached"
        next_at = None
        if outcome == "retry":
            delay: float
            if retry_after_s:
                delay = max(1, min(int(retry_after_s), MAX_RETRY_AFTER_S))
            else:
                # Full jitter over the exponential ceiling; never a fixed sleep
                # and never longer than the five-minute policy maximum.
                delay = max(
                    1,
                    random.uniform(0, min(MAX_BACKOFF_S, BASE_BACKOFF_S * (2 ** (attempt - 1)))),
                )
            next_at = _stamp(now_dt + timedelta(seconds=delay))
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            cur = self.conn.execute(
                "SELECT status,claim_token FROM deliveries WHERE delivery_id=?",
                (delivery["delivery_id"],),
            ).fetchone()
            if cur is None or cur["status"] != "sending" or cur["claim_token"] != delivery["claim_token"]:
                # Fencing: a stale worker can never overwrite a newer result.
                self.conn.execute("COMMIT")
                return
            if outcome == "retry":
                sibling_attempts = self.conn.execute(
                    "SELECT COALESCE(SUM(attempts),0) AS n FROM deliveries "
                    "WHERE event_id=? AND recipient_role=? AND delivery_id<>?",
                    (delivery["event_id"], delivery["recipient_role"], delivery["delivery_id"]),
                ).fetchone()["n"]
                if attempt + int(sibling_attempts) >= MAX_ATTEMPTS:
                    outcome = "dead_letter"
                    next_at = None
                    error = error or "attempt limit reached"
            attempt_no = self._next_attempt_no(str(delivery["delivery_id"]))
            self.conn.execute(
                "INSERT INTO delivery_attempts "
                "(delivery_id,attempt_no,attempted_at,outcome,error,telegram_message_id,retry_after_s) "
                "VALUES(?,?,?,?,?,?,?)",
                (delivery["delivery_id"], attempt_no, now, outcome, (error or "")[:500], message_id, retry_after_s),
            )
            self.conn.execute(
                "UPDATE deliveries SET status=?,attempts=?,next_attempt_at=?,telegram_message_id=?,"
                "reply_to_message_id=?,claim_token=NULL,claimed_at=NULL,last_error=?,updated_at=? "
                "WHERE delivery_id=?",
                (
                    "sent" if outcome == "sent" else outcome,
                    attempt,
                    next_at,
                    message_id,
                    reply_to_message_id,
                    (error or "")[:500] or None,
                    now,
                    delivery["delivery_id"],
                ),
            )
            thread_key = delivery.get("thread_key")
            if (
                outcome == "sent"
                and establish_thread
                and message_id is not None
                and thread_key
            ):
                self.conn.execute(
                    "INSERT OR IGNORE INTO delivery_threads "
                    "(recipient_role,thread_key,root_message_id,root_delivery_id,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (
                        delivery["recipient_role"],
                        thread_key,
                        int(message_id),
                        delivery["delivery_id"],
                        now,
                        now,
                    ),
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def thread_root(self, recipient_role: str, thread_key: str | None) -> int | None:
        if not thread_key:
            return None
        row = self.conn.execute(
            "SELECT root_message_id FROM delivery_threads WHERE recipient_role=? AND thread_key=?",
            (recipient_role, thread_key),
        ).fetchone()
        return int(row["root_message_id"]) if row is not None else None

    def clear_thread(self, recipient_role: str, thread_key: str | None) -> None:
        if not thread_key:
            return
        self.conn.execute(
            "DELETE FROM delivery_threads WHERE recipient_role=? AND thread_key=?",
            (recipient_role, thread_key),
        )

    def requeue(self, delivery_id: str) -> str:
        """Explicit operator requeue; never automatic.

        Only ambiguous/dead-letter rows can be requeued. The action log uses a
        monotonic sequence independent of the bounded send-attempt budget, so
        an operator retry cannot collide with an earlier attempt number.
        """

        now = _stamp()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT status,attempts,event_id,recipient_role FROM deliveries WHERE delivery_id=?",
                (delivery_id,),
            ).fetchone()
            if row is None:
                self.conn.execute("COMMIT")
                return "not_found"
            status = str(row["status"])
            if status not in ("ambiguous", "dead_letter"):
                self.conn.execute("COMMIT")
                return f"refused_status_{status}"
            # The send budget is shared by all chunks for one event/recipient.
            # Refuse explicitly when sibling parts already consumed it.
            sibling_attempts = self.conn.execute(
                "SELECT COALESCE(SUM(attempts),0) AS n FROM deliveries "
                "WHERE event_id=? AND recipient_role=? AND delivery_id<>?",
                (row["event_id"], row["recipient_role"], delivery_id),
            ).fetchone()["n"]
            if int(sibling_attempts) >= MAX_ATTEMPTS:
                self.conn.execute("COMMIT")
                return "budget_exhausted"
            attempt_no = self._next_attempt_no(delivery_id)
            self.conn.execute(
                "INSERT INTO delivery_attempts "
                "(delivery_id,attempt_no,attempted_at,outcome,error) VALUES(?,?,?, 'operator_requeue',?)",
                (delivery_id, attempt_no, now, "explicit operator requeue"),
            )
            # Preserve the immutable action history and reserve one final send
            # within the shared event/recipient attempt budget.
            self.conn.execute(
                "UPDATE deliveries SET status='pending',attempts=?,next_attempt_at=NULL,"
                "claim_token=NULL,claimed_at=NULL,last_error=NULL,updated_at=? WHERE delivery_id=?",
                (max(0, MAX_ATTEMPTS - 1 - int(sibling_attempts)), now, delivery_id),
            )
            self.conn.execute("COMMIT")
            return "requeued"
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def counts(self) -> dict[str, int]:
        return {
            str(row["status"]): int(row["n"])
            for row in self.conn.execute("SELECT status,COUNT(*) AS n FROM deliveries GROUP BY status")
        }


class NotificationDispatcher:
    """Read-only event relay plus independent per-recipient Telegram delivery."""

    def __init__(
        self,
        source_db_path: str | Path,
        delivery_db_path: str | Path,
        bot: Any,
        recipients: dict[str, int],
        *,
        batch_size: int = 100,
    ):
        self.source_path = Path(source_db_path)
        self.store = DeliveryStore(delivery_db_path)
        self.bot = bot
        self.recipients = {str(k): int(v) for k, v in recipients.items() if v}
        self._supports_reply = _supports_reply_to(bot)
        self.batch_size = max(1, int(batch_size))
        self._source = sqlite3.connect(f"file:{self.source_path}?mode=ro", uri=True, timeout=10)
        self._source.row_factory = sqlite3.Row
        self._source.execute("PRAGMA query_only=ON")
        self._source.execute("PRAGMA busy_timeout=10000")
        max_row = self._source.execute("SELECT COALESCE(MAX(audit_id),0) AS max_id FROM audit_events").fetchone()
        self.store.initialize_cursor(int(max_row["max_id"] or 0))
        legacy_max = self._source.execute(
            "SELECT COALESCE(MAX(notification_id),0) AS max_id FROM trading_notifications"
        ).fetchone()
        self.store.initialize_legacy_cursor(int(legacy_max["max_id"] or 0))

    def close(self) -> None:
        self._source.close()
        self.store.close()

    def _routing_preferences(
        self,
    ) -> tuple[dict[tuple[str, str], bool], str, dict[str, str]]:
        preferences = {
            (str(row["role"]), str(row["category"])): bool(row["enabled"])
            for row in self._source.execute(
                "SELECT role,category,enabled FROM notification_preferences"
            ).fetchall()
        }
        settings = {
            str(row["key"]): str(row["value"])
            for row in self._source.execute("SELECT key,value FROM control_settings").fetchall()
        }
        minimum_severity = str(settings.get("notification_min_severity") or "INFO").upper()
        if minimum_severity not in SEVERITY_RANK:
            minimum_severity = "INFO"
        return preferences, minimum_severity, settings

    def relay_once(self) -> int:
        cursor = self.store.cursor()
        rows = self._source.execute(
            """
            SELECT audit_id,event_id,event_type,schema_version,occurred_at_utc,aggregate_type,
                   aggregate_id,correlation_id,symbol,severity,payload_json,audience_json
            FROM audit_events
            WHERE event_class='domain' AND audit_id > ?
            ORDER BY audit_id LIMIT ?
            """,
            (cursor, self.batch_size),
        ).fetchall()
        preferences, minimum_severity, settings = self._routing_preferences()
        return self.store.relay_batch(
            [dict(row) for row in rows],
            self.recipients,
            preferences,
            minimum_severity,
            settings,
        )

    @staticmethod
    def _legacy_severity(category: str, event_key: str, event_type: str = "") -> str:
        key = f"{category}:{event_key}:{event_type}".upper()
        if any(token in key for token in ("NOT_A_SIGNAL", "NOT A SIGNAL")):
            return "DEBUG"
        if "BUY_BLOCK_ACTIVE" in key:
            return "CRITICAL"
        if "BUY_BLOCK_CLEARED" in key:
            return "INFO"
        # A genuine signal rejected locally is actionable but not an exchange
        # failure. Keep it WARNING; definitive exchange/order rejections remain ERROR.
        if "REJECTED_LOCALLY" in key:
            return "WARNING"
        if "CRITICAL" in key or "EMERGENCY" in key or "UNPROTECTED" in key:
            return "CRITICAL"
        if category == "notification_delivery" or any(
            token in key for token in ("FAILED", "REJECTED", "ERROR", "MISMATCH")
        ):
            return "ERROR"
        if category in ("reconciliation", "recovery") or any(
            token in key for token in ("WARNING", "RECONCIL", "BLOCK", "STALE")
        ):
            return "WARNING"
        if category == "diagnostic":
            return "DEBUG"
        return "INFO"

    def relay_legacy_once(self) -> int:
        """Bridge new rows from the old formatted outbox without replaying history."""
        cursor = self.store.legacy_cursor()
        rows = self._source.execute(
            """
            SELECT notification_id,event_key,category,event_type,role,chat_id,text
            FROM trading_notifications
            WHERE notification_id > ? AND status='pending'
            ORDER BY notification_id LIMIT ?
            """,
            (cursor, self.batch_size),
        ).fetchall()
        if not rows:
            return 0
        preferences, minimum_severity, settings = self._routing_preferences()
        now = _stamp()
        inserted = 0
        self.store.conn.execute("BEGIN IMMEDIATE")
        try:
            current = self.store.legacy_cursor()
            for row in rows:
                notification_id = int(row["notification_id"])
                if notification_id <= current:
                    continue
                event_id = f"legacy-notification-{notification_id}"
                role = str(row["role"])
                category = str(row["category"] or "diagnostic")
                event_type = str(row["event_type"] or "legacy")
                severity = self._legacy_severity(
                    category, str(row["event_key"] or ""), event_type
                )
                status, reason = delivery_decision(
                    role,
                    severity,
                    category,
                    event_type,
                    {},
                    preferences,
                    settings,
                    minimum_severity,
                )
                chunks = message_chunks(_safe_text(row["text"], limit=20000))
                for part_no, chunk in enumerate(chunks, 1):
                    cur = self.store.conn.execute(
                        """
                        INSERT OR IGNORE INTO deliveries
                        (delivery_id,event_id,recipient_role,chat_id,event_type,notification_category,
                         severity,message_text,part_no,part_count,status,last_error,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            uuid.uuid4().hex, event_id, role, int(row["chat_id"]),
                            event_type, category, severity, chunk, part_no, len(chunks),
                            status, reason, now, now,
                        ),
                    )
                    inserted += max(0, cur.rowcount)
                self.store.conn.execute(
                    "INSERT OR IGNORE INTO relayed_legacy_notifications(notification_id,relayed_at) "
                    "VALUES(?,?)",
                    (notification_id, now),
                )
                current = notification_id
            self.store.conn.execute(
                "INSERT INTO dispatcher_state(key,value,updated_at) VALUES('legacy_notification_cursor',?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                (str(current), now),
            )
            self.store.conn.execute("COMMIT")
        except Exception:
            self.store.conn.execute("ROLLBACK")
            raise
        return inserted

    def _send(self, chat_id: int, text: str, reply_to_message_id: int | None) -> dict[str, Any]:
        if reply_to_message_id is not None and self._supports_reply:
            return self.bot.send_message(
                chat_id, text, reply_to_message_id=reply_to_message_id
            ) or {}
        return self.bot.send_message(chat_id, text) or {}

    def deliver_one(self) -> bool:
        delivery = self.store.claim_next()
        if delivery is None:
            return False
        chat_id = int(delivery["chat_id"])
        text = str(delivery["message_text"])
        thread_key = delivery.get("thread_key")
        reply_to = self.store.thread_root(str(delivery["recipient_role"]), thread_key)
        thread_missing = bool(thread_key) and reply_to is None and int(delivery.get("part_no") or 1) == 1
        try:
            result = self._send(chat_id, text, reply_to)
            message_id = int(result["message_id"]) if isinstance(result, dict) and result.get("message_id") else None
            self.store.finish(
                delivery,
                "sent",
                message_id=message_id,
                reply_to_message_id=reply_to,
                establish_thread=thread_missing,
            )
        except BotApiNotSentError as exc:
            # DNS/connect failure: no request byte was written, safe to retry.
            self.store.finish(delivery, "retry", error=str(exc))
        except BotApiRateLimitError as exc:
            self.store.finish(
                delivery, "retry", error="telegram rate limited",
                retry_after_s=exc.retry_after or 30,
            )
        except BotApiForbiddenError as exc:
            self.store.finish(delivery, "suppressed", error=str(exc))
        except BotApiTransportError as exc:
            # The request may have been applied; never retry automatically.
            self.store.finish(delivery, "ambiguous", error=str(exc))
        except BotApiError as exc:
            if reply_to is not None and _is_missing_reply_error(exc):
                # A deleted root must not dead-letter a lifecycle update.  The
                # failed reply was definitively rejected, so one standalone
                # resend is safe; a new root is created for later events.
                self.store.clear_thread(str(delivery["recipient_role"]), thread_key)
                try:
                    result = self._send(chat_id, text, None)
                    message_id = (
                        int(result["message_id"])
                        if isinstance(result, dict) and result.get("message_id")
                        else None
                    )
                    self.store.finish(
                        delivery,
                        "sent",
                        message_id=message_id,
                        establish_thread=thread_missing,
                    )
                except BotApiTransportError as retry_exc:
                    self.store.finish(delivery, "ambiguous", error=str(retry_exc))
                except BotApiError as retry_exc:
                    self.store.finish(delivery, "dead_letter", error=str(retry_exc))
            else:
                self.store.finish(delivery, "dead_letter", error=str(exc))
        except Exception as exc:  # transport boundary: acceptance cannot be excluded
            self.store.finish(delivery, "ambiguous", error=type(exc).__name__)
        return True

    def run_once(self, *, delivery_limit: int = 100) -> dict[str, int]:
        self.store.set_state("last_cycle_at", _stamp())
        relayed = self.relay_once() + self.relay_legacy_once()
        recovered = self.store.recover_stale_claims()
        delivered = 0
        for _ in range(max(1, delivery_limit)):
            if not self.deliver_one():
                break
            delivered += 1
        # Refresh after a potentially long delivery batch so a large backlog
        # does not look like a hung worker.
        self.store.set_state("last_cycle_at", _stamp())
        return {"relayed": relayed, "recovered_ambiguous": recovered, "attempted": delivered}

    def run_forever(self, *, poll_interval_s: float = 1.0) -> None:
        self.store.set_state("worker_started_at", _stamp())
        log.info("notification dispatcher started; source=%s", self.source_path)
        while True:
            try:
                result = self.run_once()
                if any(result.values()):
                    log.info("notification dispatcher cycle: %s", result)
            except Exception:
                log.exception("notification dispatcher cycle failed")
            time.sleep(max(0.2, float(poll_interval_s)))



def requeue_delivery(path: str | Path, delivery_id: str) -> str:
    """Requeue one ambiguous/dead-letter delivery as an explicit operator action."""

    db_path = Path(path)
    if not db_path.is_file():
        return "not_found"
    store = DeliveryStore(db_path)
    try:
        return store.requeue(str(delivery_id))
    finally:
        store.close()


def inspect_delivery_db(
    path: str | Path,
    *,
    limit: int = 20,
    status: str | None = None,
    stale_after_s: int = STALE_HEARTBEAT_S,
) -> dict[str, Any] | None:
    """Read-only summary/history view for the independent delivery ledger."""
    db_path = Path(path)
    if not db_path.is_file():
        return None
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        counts = {
            str(row["status"]): int(row["count"])
            for row in conn.execute("SELECT status,COUNT(*) AS count FROM deliveries GROUP BY status")
        }
        state = {
            str(row["key"]): str(row["value"])
            for row in conn.execute("SELECT key,value FROM dispatcher_state")
        }
        oldest = conn.execute(
            "SELECT MIN(created_at) AS created_at FROM deliveries WHERE status IN ('pending','retry')"
        ).fetchone()
        queue_depth = int(counts.get("pending", 0) + counts.get("retry", 0) + counts.get("sending", 0))
        oldest_pending_at = oldest["created_at"] if oldest else None
        oldest_age_s = None
        if oldest_pending_at:
            try:
                oldest_dt = datetime.fromisoformat(str(oldest_pending_at).replace("Z", "+00:00"))
                if oldest_dt.tzinfo is None:
                    oldest_dt = oldest_dt.replace(tzinfo=UTC)
                oldest_age_s = round(max(0.0, (_now() - oldest_dt).total_seconds()), 1)
            except ValueError:
                oldest_age_s = None
        query = (
            "SELECT delivery_id,event_id,recipient_role,event_type,notification_category,severity,"
            "status,attempts,part_no,part_count,thread_key,reply_to_message_id,telegram_message_id,"
            "last_error,created_at,updated_at FROM deliveries"
        )
        params: list[Any] = []
        if status:
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY created_at DESC,delivery_id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 200)))
        rows = [dict(row) for row in conn.execute(query, params).fetchall()]
        return {
            "counts": counts,
            "queue_depth": queue_depth,
            "dead_letter": int(counts.get("dead_letter", 0)),
            "ambiguous": int(counts.get("ambiguous", 0)),
            "state": state,
            "heartbeat": heartbeat_status(state, stale_after_s=stale_after_s),
            "oldest_pending_at": oldest_pending_at,
            "oldest_pending_age_s": oldest_age_s,
            "deliveries": rows,
        }
    finally:
        conn.close()
