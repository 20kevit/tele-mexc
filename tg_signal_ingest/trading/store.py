"""Transactional persistence for signals, intents, fills and reconciliation.

The trade lifecycle is deliberately database-first: an intent row exists
before any exchange call, and every transition is recorded in
``order_status_history``. This is what makes crash recovery and idempotency
possible.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import socket
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import TradingConfig
from ..db import Database
from .audit import normalize_event_type, normalize_severity, redact_payload, redact_text
from .parser import PARSER_VERSION, ParsedSignal

_APPLICATION_VERSION = "0.1.0"

# Clock health state thresholds (seconds)
CLOCK_SKEW_FRESH_MS: int = 1000    # within 1s of exchange: fresh
CLOCK_SKEW_STALE_MS: int = 2000    # 1-2s drift: degraded
CLOCK_SKEW_UNHEALTHY_MS: int = 5000  # >2s drift: unhealthy (exceeds recvWindow default)


def clock_health_state(skew_ms: int) -> str:
    """Determine clock health state from skew in milliseconds.

    Returns one of: "fresh", "degraded", "unhealthy"
    """
    if skew_ms < CLOCK_SKEW_FRESH_MS:
        return "fresh"
    if skew_ms < CLOCK_SKEW_STALE_MS:
        return "degraded"
    return "unhealthy"


def now_iso_utc() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

ACTIVE_SIGNAL_STATES = ("parsed", "validated", "planned")
OPEN_INTENT_STATES = (
    "planned", "submitting", "submitted", "reconciling", "open", "unknown_requires_reconciliation",
)
RECONCILABLE_INTENT_STATES = tuple(s for s in OPEN_INTENT_STATES if s != "planned")
TERMINAL_INTENT_STATES = ("filled", "canceled", "failed", "rejected", "canceled_local")
PROTECTION_STATES = (
    "not_requested",
    "unverified",
    "active",
    "triggered",
    "completed",
    "rejected",
    "unknown_requires_reconciliation",
)

# Notification routing defaults.  A missing preference row falls back here, so
# the policy is explicit in code as well as in the migration seed.
NOTIFICATION_CATEGORIES = (
    "signal_decision",
    "signal_accepted",
    "exchange_operation",
    "order_lifecycle",
    "oco_lifecycle",
    "take_profit",
    "stop_loss",
    "reconciliation",
    "critical",
    "diagnostic",
    "notification_delivery",
    "recovery",
)

NOTIFICATION_DEFAULTS: dict[str, dict[str, bool]] = {
    "owner": {
        "signal_decision": False,
        "signal_accepted": True,
        "exchange_operation": True,
        "order_lifecycle": True,
        "oco_lifecycle": True,
        "take_profit": True,
        "stop_loss": True,
        "reconciliation": True,
        "critical": True,
        "diagnostic": False,
        "notification_delivery": False,
        "recovery": False,
    },
    "developer": {category: True for category in NOTIFICATION_CATEGORIES},
}


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


class TradingError(Exception):
    """Base class for trading persistence errors (safe to display)."""


class SymbolConflictError(TradingError):
    """Per-symbol concurrency: a conflicting trade already exists for this symbol."""


class RiskLimitExceeded(TradingError):
    """A limit changed between the risk check and the atomic reservation."""


def dec_str(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _row(row: Any) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class TradingStore:
    def __init__(self, db: Database, cfg: TradingConfig, now: Any | None = None):
        self.db = db
        self.cfg = cfg
        self._now = now or (lambda: datetime.now(UTC))

    def _ts(self) -> str:
        return self._now().isoformat(timespec="seconds").replace("+00:00", "Z")

    # ------------------------------------------------------------- audit

    # ── Per-symbol concurrency (§3.2) — available on base store for engine checks
    CONFLICTING_TRADE_STATES: tuple[str, ...] = (
        "pending_entry", "entry_active", "position_open", "oco_submitting",
        "oco_active", "oco_partial", "tp1_active", "tp1_completed",
        "tp2_active", "stop_loss_executing", "exit_pending", "needs_reconciliation",
    )

    def has_active_trade_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        if not symbol:
            return None
        # Consider *every* conflicting trade for the symbol — no LIMIT.
        # Query is pushed to SQLite so an unbounded history cannot hide a
        # conflict behind an arbitrary LIMIT 20 window.
        try:
            placeholders = ", ".join("?" for _ in self.CONFLICTING_TRADE_STATES)
            rows = self.db.conn.execute(
                f"SELECT * FROM trades WHERE symbol = ? AND trade_state IN ({placeholders}) ORDER BY updated_at DESC LIMIT 1",
                (symbol.upper(), *self.CONFLICTING_TRADE_STATES),
            ).fetchall()
        except Exception:
            return None
        return dict(rows[0]) if rows else None

    def audit(
        self,
        correlation: str,
        category: str,
        event: str,
        detail: str | None = None,
        *,
        event_id: str | None = None,
        event_type: str | None = None,
        schema_version: str = "2.0",
        severity: str = "INFO",
        occurred_at_utc: str | None = None,
        recorded_at_utc: str | None = None,
        monotonic_elapsed_ns: int = 0,
        clock_health: str | None = None,
        process_id: int | None = None,
        hostname: str | None = None,
        instance_id: str | None = None,
        application_version: str | None = None,
        component: str | None = None,
        operation: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        telegram_channel_id: int | None = None,
        telegram_message_id: int | None = None,
        message_edit_version: int | None = None,
        signal_id: str | None = None,
        intent_id: str | None = None,
        trade_id: str | None = None,
        order_id: str | None = None,
        exchange_order_id: str | None = None,
        symbol: str | None = None,
        reason_code: str | None = None,
        outcome: str | None = None,
        source_ref: str | None = None,
        evidence_source: str | None = None,
        evidence_limitations: str | None = None,
        payload: dict[str, Any] | None = None,
        actor: str | None = None,
        dedupe_key: str | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> str:
        """Insert one structured audit event and return its ``event_id``.

        The call is safe to embed in an enclosing transaction by passing
        ``conn``.  Text is redacted and bounded; a ``dedupe_key`` makes the
        write idempotent across retries and restarts.  ``event`` remains the
        human-readable name (required by the legacy schema) while
        ``event_type`` is the stable machine key (``category.event``).
        """

        now = self._ts()
        recorded = recorded_at_utc or now
        occurred = occurred_at_utc or recorded
        event_id = event_id or uuid.uuid4().hex
        canonical_type = normalize_event_type(category, event, event_type)
        severity_value = normalize_severity(severity)
        safe_detail = redact_text(detail)
        payload_json = redact_payload(payload) if payload is not None else None
        values = (
            schema_version,
            event_id,
            str(event)[:200],
            canonical_type,
            severity_value,
            occurred,
            recorded,
            int(monotonic_elapsed_ns or 0),
            clock_health,
            process_id if process_id is not None else os.getpid(),
            hostname or socket.gethostname(),
            instance_id,
            application_version or _APPLICATION_VERSION,
            component or category,
            operation or str(event)[:120],
            correlation_id or correlation,
            causation_id,
            telegram_channel_id,
            telegram_message_id,
            message_edit_version,
            signal_id,
            intent_id,
            trade_id,
            order_id,
            exchange_order_id,
            str(symbol).upper() if symbol else None,
            reason_code,
            outcome,
            source_ref,
            evidence_source,
            redact_text(evidence_limitations),
            payload_json,
            actor,
            redact_text(dedupe_key, limit=300),
            safe_detail,
            correlation,
            category,
            now,
        )

        def _insert(target: sqlite3.Connection) -> None:
            verb = "INSERT OR IGNORE" if dedupe_key else "INSERT"
            target.execute(
                f"""{verb} INTO audit_events
                (schema_version, event_id, event, event_type, severity, occurred_at_utc,
                 recorded_at_utc, monotonic_elapsed_ns, clock_health, process_id,
                 hostname, instance_id, application_version, component, operation,
                 correlation_id, causation_id, telegram_channel_id, telegram_message_id,
                 message_edit_version, signal_id, intent_id, trade_id, order_id,
                 exchange_order_id, symbol, reason_code, outcome, source_ref,
                 evidence_source, evidence_limitations, payload_json, actor, dedupe_key,
                 detail, secrets_redacted, correlation, category, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                values,
            )

        if conn is not None:
            _insert(conn)
        else:
            with self.db.transaction() as tx:
                _insert(tx)
        return event_id

    # -------------------------------------------------- exchange evidence

    def begin_exchange_operation(
        self,
        *,
        operation_type: str,
        operation_class: str,
        purpose: str | None = None,
        transport: str | None = None,
        endpoint: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        intent_id: str | None = None,
        trade_id: str | None = None,
        trade_order_id: str | None = None,
        signal_id: str | None = None,
        symbol: str | None = None,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        request_payload: dict[str, Any] | None = None,
        write_attempted: bool = False,
        clock_health: str | None = None,
        evidence_source: str = "local_process",
        evidence_limitations: str | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> str:
        """Persist the intent to call the exchange *before* the call happens.

        A crash between the insert and the response therefore still leaves a
        durable, correlated record that a write may have been attempted.
        """

        if operation_class not in ("read", "write"):
            raise TradingError(f"invalid exchange operation class {operation_class!r}")
        operation_id = uuid.uuid4().hex
        now = self._ts()
        payload_json = redact_payload(request_payload)
        values = (
            operation_id, correlation_id, causation_id, intent_id, trade_id,
            trade_order_id, signal_id, str(symbol).upper() if symbol else None,
            str(operation_type)[:120], operation_class, purpose, transport,
            endpoint, client_order_id, exchange_order_id, payload_json, now,
            1 if write_attempted or operation_class == "write" else 0,
            clock_health, evidence_source, redact_text(evidence_limitations), now, now,
        )

        def _insert(target: sqlite3.Connection) -> None:
            target.execute(
                """INSERT INTO exchange_operations
                (operation_id, correlation_id, causation_id, intent_id, trade_id,
                 trade_order_id, signal_id, symbol, operation_type, operation_class,
                 purpose, transport, endpoint, client_order_id, exchange_order_id,
                 request_payload, request_started_at, write_attempted, clock_health,
                 evidence_source, evidence_limitations, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                values,
            )

        if conn is not None:
            _insert(conn)
        else:
            with self.db.transaction() as tx:
                _insert(tx)
        return operation_id

    def finish_exchange_operation(
        self,
        operation_id: str,
        *,
        success: bool,
        response_payload: dict[str, Any] | None = None,
        response_classification: str | None = None,
        response_status: str | None = None,
        exchange_code: str | None = None,
        http_status: int | None = None,
        exchange_order_id: str | None = None,
        error_class: str | None = None,
        error_message: str | None = None,
        rate_limit_wait_ms: int | None = None,
        uncertainty: str | None = None,
        recovery_attempts: int | None = None,
        observed_at: str | None = None,
        evidence_limitations: str | None = None,
    ) -> None:
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """UPDATE exchange_operations SET
                    request_finished_at = ?, response_received = ?,
                    response_classification = ?, response_status = ?,
                    exchange_code = ?, http_status = ?, response_payload = ?,
                    error_class = ?, error_message = ?, rate_limit_wait_ms = ?,
                    uncertainty = COALESCE(?, uncertainty),
                    recovery_attempts = COALESCE(?, recovery_attempts),
                    observed_at = COALESCE(?, observed_at),
                    evidence_limitations = COALESCE(?, evidence_limitations),
                    updated_at = ?
                WHERE operation_id = ?""",
                (
                    now, 1 if success else 0, response_classification,
                    redact_text(response_status, limit=80), exchange_code, http_status,
                    redact_payload(response_payload), error_class,
                    redact_text(error_message), rate_limit_wait_ms,
                    uncertainty, recovery_attempts, observed_at,
                    redact_text(evidence_limitations), now, operation_id,
                ),
            )

    def mark_operation_uncertain(
        self, operation_id: str, *, reason: str, uncertainty: str = "uncertain"
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE exchange_operations SET uncertainty = ?, error_message = ?, "
                "updated_at = ? WHERE operation_id = ?",
                (uncertainty, redact_text(reason), self._ts(), operation_id),
            )

    def list_exchange_operations(
        self,
        *,
        intent_id: str | None = None,
        trade_id: str | None = None,
        client_order_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if intent_id:
            clauses.append("intent_id = ?")
            params.append(intent_id)
        if trade_id:
            clauses.append("trade_id = ?")
            params.append(trade_id)
        if client_order_id:
            clauses.append("client_order_id = ?")
            params.append(client_order_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.conn.execute(
            f"SELECT * FROM exchange_operations {where} ORDER BY created_at DESC, operation_id DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def exchange_operation_counts(self) -> dict[str, int]:
        return {
            str(row["uncertainty"]): int(row["c"])
            for row in self.db.conn.execute(
                "SELECT uncertainty, COUNT(*) AS c FROM exchange_operations GROUP BY uncertainty"
            )
        }

    # ------------------------------------------------------------ signals

    def record_signal(
        self,
        parsed: ParsedSignal,
        *,
        channel_id: int,
        message_id: int,
        edit_version: int,
        status: str = "parsed",
        rejection_reason: str | None = None,
    ) -> tuple[str, bool]:
        """Insert a parsed signal version; older versions of the same message
        are superseded atomically. Returns (signal_id, inserted)."""

        signal_id = f"{channel_id}:{message_id}:{edit_version}"
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE signals SET status = 'superseded', updated_at = ? "
                "WHERE channel_id = ? AND message_id = ? AND edit_version < ? "
                "AND status IN ('parsed', 'validated', 'planned')",
                (now, channel_id, message_id, edit_version),
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO signals
                    (signal_id, channel_id, message_id, edit_version, parser_version,
                     signal_kind, symbol, action, market_type, entry_price, entry_low_price,
                     entry_high_price, target_prices, stop_loss, order_type, amount_text,
                     fingerprint_sha256, status, rejection_reason, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'spot', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id, channel_id, message_id, edit_version, PARSER_VERSION,
                    parsed.kind, parsed.symbol, parsed.action,
                    dec_str(parsed.entry_price), dec_str(parsed.entry_low_price),
                    dec_str(parsed.entry_high_price),
                    ",".join(format(p, "f") for p in parsed.target_prices) or None,
                    dec_str(parsed.stop_loss), parsed.order_type, parsed.amount_text,
                    parsed.fingerprint_sha256, status, rejection_reason, now, now,
                ),
            )
            inserted = cur.rowcount > 0
        return signal_id, inserted

    def update_signal(self, signal_id: str, status: str, reason: str | None = None) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE signals SET status = ?, rejection_reason = COALESCE(?, rejection_reason), "
                "updated_at = ? WHERE signal_id = ?",
                (status, reason, self._ts(), signal_id),
            )

    def get_signal(self, signal_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute("SELECT * FROM signals WHERE signal_id = ?", (signal_id,)).fetchone()
        )

    def list_signals(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM signals ORDER BY created_at DESC, signal_id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def inconsistent_terminal_signals(self, limit: int = 50) -> list[dict[str, Any]]:
        """Terminal rejected/failed intents whose signal is still active.

        This is a read-only audit.  It exists for rows created before the
        transactional synchronization in :meth:`transition_intent`.
        """

        rows = self.db.conn.execute(
            "SELECT s.signal_id, s.status AS signal_status, s.created_at, "
            "       oi.intent_id, oi.state AS intent_state, oi.reject_reason "
            "FROM order_intents oi JOIN signals s ON s.signal_id = oi.signal_id "
            "WHERE oi.state IN ('rejected', 'failed') "
            "  AND s.status IN ('parsed', 'validated', 'planned') "
            "ORDER BY s.created_at DESC, s.signal_id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
        return [dict(r) for r in rows]

    def repair_inconsistent_terminal_signals(
        self, *, limit: int = 50, apply: bool = False
    ) -> list[dict[str, Any]]:
        """Explicitly reconcile old terminal-intent/signal mismatches.

        Dry-run by default.  Only active signal statuses are changed; newer
        edits (``superseded``), cancels and deletes are never overwritten.  No
        exchange call and no intent transition is made, so this can never
        resubmit an order.
        """

        rows = self.inconsistent_terminal_signals(limit=limit)
        if not apply or not rows:
            return rows
        now = self._ts()
        with self.db.transaction() as conn:
            for row in rows:
                reason = row.get("reject_reason") or (
                    f"terminal intent state is {row['intent_state']}"
                )
                conn.execute(
                    "UPDATE signals SET status = 'rejected', "
                    "rejection_reason = COALESCE(rejection_reason, ?), updated_at = ? "
                    "WHERE signal_id = ? AND status IN ('parsed', 'validated', 'planned')",
                    (str(reason)[:500], now, row["signal_id"]),
                )
                conn.execute(
                    "INSERT INTO audit_events "
                    "(correlation, category, event, detail, created_at) "
                    "VALUES (?, 'lifecycle', 'terminal_signal_repaired', ?, ?)",
                    (
                        str(row["signal_id"])[:200],
                        f"intent={row['intent_id']} state={row['intent_state']}"[:2000],
                        now,
                    ),
                )
        return rows

    def find_duplicate_signal(
        self,
        *,
        fingerprint: str,
        channel_id: int,
        message_id: int,
        window_s: float = 300.0,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        now = now or datetime.now(UTC)
        cutoff = (now - timedelta(seconds=window_s)).isoformat(timespec="seconds").replace("+00:00", "Z")
        return _row(
            self.db.conn.execute(
                """
                SELECT * FROM signals
                WHERE channel_id = ? AND fingerprint_sha256 = ? AND message_id <> ?
                  AND signal_kind IN ('alert', 'update')
                  AND status IN ('parsed', 'validated', 'planned')
                  AND created_at >= ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (channel_id, fingerprint, message_id, cutoff),
            ).fetchone()
        )

    def has_later_cancel(self, channel_id: int, symbol: str | None, message_id: int) -> bool:
        """True if a cancel for this symbol/ALL was posted after ``message_id``.

        Telegram message ids are monotonic per channel. This prevents a queued
        (possibly retried) old alert from creating an order after a cancel that
        was already processed.
        """

        if not symbol:
            return False
        row = self.db.conn.execute(
            "SELECT 1 FROM signals WHERE channel_id = ? AND signal_kind = 'cancel' "
            "AND message_id > ? AND status NOT IN ('rejected', 'superseded', 'deleted') "
            "AND (symbol = ? OR symbol IS NULL) LIMIT 1",
            (channel_id, message_id, symbol.upper()),
        ).fetchone()
        return row is not None

    def active_signals(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if symbol:
            rows = self.db.conn.execute(
                "SELECT * FROM signals WHERE status IN ('validated', 'planned') AND symbol = ? "
                "ORDER BY created_at DESC",
                (symbol.upper(),),
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM signals WHERE status IN ('validated', 'planned') "
                "ORDER BY created_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def signals_for_message(self, channel_id: int, message_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM signals WHERE channel_id = ? AND message_id = ?", (channel_id, message_id)
        ).fetchall()
        return [dict(r) for r in rows]

    def cancel_signals(
        self,
        symbol: str | None,
        reason: str,
        *,
        channel_id: int | None = None,
        before_message_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Mark active signals canceled and return all non-terminal intents.

        Intents are selected independently of signal status: an edit marks the
        previous signal version ``superseded``, but its live order still has to
        be canceled.
        """

        now = self._ts()
        placeholders = ",".join("?" for _ in TERMINAL_INTENT_STATES)
        with self.db.transaction() as conn:
            signal_where = ["status IN ('validated', 'planned')"]
            signal_params: list[Any] = []
            if symbol:
                signal_where.append("symbol = ?")
                signal_params.append(symbol.upper())
            if channel_id is not None:
                signal_where.append("channel_id = ?")
                signal_params.append(channel_id)
            if before_message_id is not None:
                signal_where.append("message_id < ?")
                signal_params.append(before_message_id)
            rows = conn.execute(
                f"SELECT signal_id FROM signals WHERE {' AND '.join(signal_where)}",
                signal_params,
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE signals SET status = 'canceled', rejection_reason = ?, updated_at = ? "
                    "WHERE signal_id = ?",
                    (reason, now, row["signal_id"]),
                )

            intent_where = [f"state NOT IN ({placeholders})"]
            intent_params: list[Any] = list(TERMINAL_INTENT_STATES)
            if symbol:
                intent_where.append("symbol = ?")
                intent_params.append(symbol.upper())
            if channel_id is not None:
                intent_where.append("create_channel_id = ?")
                intent_params.append(channel_id)
            if before_message_id is not None:
                intent_where.append("create_msg_id < ?")
                intent_params.append(before_message_id)
            intents = conn.execute(
                f"SELECT * FROM order_intents WHERE {' AND '.join(intent_where)}",
                intent_params,
            ).fetchall()
        return [dict(row) for row in intents]

    def cancel_signals_for_message(self, channel_id: int, message_id: int, reason: str) -> list[dict[str, Any]]:
        now = self._ts()
        placeholders = ",".join("?" for _ in TERMINAL_INTENT_STATES)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE signals SET status = 'deleted', rejection_reason = ?, updated_at = ? "
                "WHERE channel_id = ? AND message_id = ? "
                "AND status NOT IN ('deleted', 'canceled')",
                (reason, now, channel_id, message_id),
            )
            intents = conn.execute(
                "SELECT * FROM order_intents WHERE create_channel_id = ? AND create_msg_id = ? "
                f"AND state NOT IN ({placeholders})",
                (channel_id, message_id, *TERMINAL_INTENT_STATES),
            ).fetchall()
        return [dict(row) for row in intents]

    # ------------------------------------------------------------ intents

    def daily_totals(self, day_key: str) -> dict[str, Any]:
        row = self.db.conn.execute(
            "SELECT quote_spent, order_count FROM trading_daily_spend WHERE day_key = ?",
            (day_key,),
        ).fetchone()
        return {
            "quote_spent": Decimal(row["quote_spent"]) if row else Decimal(0),
            "order_count": int(row["order_count"]) if row else 0,
        }

    def count_open_intents(self) -> int:
        placeholders = ",".join("?" for _ in OPEN_INTENT_STATES)
        row = self.db.conn.execute(
            f"SELECT COUNT(*) AS c FROM order_intents WHERE state IN ({placeholders})",
            OPEN_INTENT_STATES,
        ).fetchone()
        return int(row["c"])

    def has_active_intent_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        """Return an open ``order_intents`` row for *symbol*, or None.

        Used as a durability-tier fallback for per-symbol concurrency when the
        higher-level TradeManager (``trades`` table) is unavailable — e.g. in
        sim mode.  Any open intent blocks a new BUY for the same symbol
        because the reservation/inventory model cannot yet prove disjointness.
        """
        if not symbol:
            return None
        placeholders = ", ".join("?" for _ in OPEN_INTENT_STATES)
        rows = self.db.conn.execute(
            f"SELECT * FROM order_intents WHERE symbol = ? AND state IN ({placeholders}) ORDER BY created_at DESC LIMIT 1",
            (symbol.upper(), *OPEN_INTENT_STATES),
        ).fetchall()
        return dict(rows[0]) if rows else None

    def count_intents_by_state(self) -> dict[str, int]:
        return {
            str(r["state"]): int(r["c"])
            for r in self.db.conn.execute(
                "SELECT state, COUNT(*) AS c FROM order_intents GROUP BY state"
            )
        }

    # ------------------------------------------------------- budget ledger

    @staticmethod
    def _budget_committed(conn: Any) -> tuple[Decimal, Decimal]:
        """Return (active reservations, settled spend+fees) from the ledger."""

        active = Decimal("0")
        spent = Decimal("0")
        for row in conn.execute(
            "SELECT active, reserved_quote, spent_quote, fee_quote FROM trading_budget_ledger"
        ).fetchall():
            try:
                if int(row["active"]) == 1:
                    active += Decimal(row["reserved_quote"])
                else:
                    spent += Decimal(row["spent_quote"]) + Decimal(row["fee_quote"])
            except (InvalidOperation, TypeError, ValueError):
                # A malformed ledger row must never free budget silently.
                active += Decimal(str(row["reserved_quote"] or "0"))
        return active, spent

    def budget_summary(self) -> dict[str, Decimal]:
        """Cumulative spend tracking (never reset).

        ``limit`` is only the configured *reference* value.  It is never
        enforced: the owner controls the per-order amount with
        ``order_quote_amount`` and this ledger only records what was spent.
        """

        limit = Decimal(str(self.cfg.aggregate_budget_quote))
        active, spent = self._budget_committed(self.db.conn)
        remaining = limit - active - spent
        if remaining < 0:
            remaining = Decimal("0")
        return {
            "limit": limit,
            "reserved": active,
            "spent": spent,
            "remaining": remaining,
        }

    def _reserved_with_fee_buffer(self, quote_amount: Decimal) -> Decimal:
        factor = Decimal(1) + Decimal(str(self.cfg.balance_fee_buffer_pct)) / Decimal(100)
        return quote_amount * factor

    def ensure_budget_reservation(self, intent_id: str) -> None:
        """Create a missing reservation for a planned pre-migration intent."""

        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT 1 FROM trading_budget_ledger WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is not None:
                return
            intent = conn.execute(
                "SELECT quote_amount, mode, state FROM order_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
            if intent is None or intent["mode"] not in ("testonly", "live"):
                return
            if intent["state"] in TERMINAL_INTENT_STATES:
                return
            quote = Decimal(str(intent["quote_amount"] or "0"))
            if quote > 0:
                self._reserve_budget(conn, intent_id, quote, self._ts())

    def _reserve_budget(self, conn: Any, intent_id: str, quote_amount: Decimal, now: str) -> Decimal:
        """Insert the spend-tracking reservation for an intent.

        The reservation is never compared with ``aggregate_budget_quote``.
        That value is a reference/display figure, not an order cap: a per-order
        amount must not be coupled to cumulative spend.  The caller must
        already hold a write transaction so the insert stays atomic.
        """

        reserve = self._reserved_with_fee_buffer(quote_amount)
        conn.execute(
            "INSERT INTO trading_budget_ledger "
            "(intent_id, reserved_quote, spent_quote, fee_quote, active, state, note, "
            "created_at, updated_at) VALUES (?, ?, '0', '0', 1, 'reserved', ?, ?, ?)",
            (intent_id, dec_str(reserve), "spend tracked before submission", now, now),
        )
        return reserve

    def _fees_for_order(self, conn: Any, exchange_order_id: str | None) -> tuple[Decimal, Decimal]:
        """Return (quote spend from fills, quote-valued fees) for an order."""

        if not exchange_order_id:
            return Decimal("0"), Decimal("0")
        rows = conn.execute(
            "SELECT price, quantity, quote_amount, fee_asset, fee_amount "
            "FROM order_fills WHERE exchange_order_id = ?",
            (str(exchange_order_id),),
        ).fetchall()
        quote_total = Decimal("0")
        fee_total = Decimal("0")
        for row in rows:
            try:
                quote = (
                    Decimal(str(row["quote_amount"]))
                    if row["quote_amount"] not in (None, "")
                    else Decimal(str(row["price"])) * Decimal(str(row["quantity"]))
                )
                quote_total += quote
                if row["fee_amount"] not in (None, ""):
                    fee = Decimal(str(row["fee_amount"]))
                    if str(row["fee_asset"] or "").upper() != "USDT":
                        # Conservative quote valuation for a base-asset fee.
                        fee = fee * Decimal(str(row["price"]))
                    fee_total += fee
            except (InvalidOperation, TypeError, ValueError):
                continue
        return quote_total, fee_total

    def _settle_budget(
        self,
        conn: Any,
        *,
        intent_id: str,
        new_state: str,
        cumulative_quote: Decimal | None,
        exchange_order_id: str | None,
        now: str,
    ) -> None:
        """Release or settle the reservation for a state transition."""

        row = conn.execute(
            "SELECT * FROM trading_budget_ledger WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        intent = conn.execute(
            "SELECT symbol, quote_amount FROM order_intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        quote_amount = Decimal(str(intent["quote_amount"])) if intent and intent["quote_amount"] else Decimal("0")
        fills_quote, fees = self._fees_for_order(conn, exchange_order_id)

        if new_state == "unknown_requires_reconciliation":
            if row is None:
                self._reserve_budget(conn, intent_id, quote_amount, now)
                row = conn.execute(
                    "SELECT * FROM trading_budget_ledger WHERE intent_id = ?", (intent_id,)
                ).fetchone()
            conn.execute(
                "UPDATE trading_budget_ledger SET state = 'uncertain', active = 1, note = ?, "
                "updated_at = ? WHERE intent_id = ?",
                ("submission outcome uncertain; reservation frozen", now, intent_id),
            )
            return

        if row is None:
            # Intent created before the ledger migration: account for it now.
            self._reserve_budget(conn, intent_id, quote_amount, now)
            row = conn.execute(
                "SELECT * FROM trading_budget_ledger WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        if row is None:
            return

        if new_state == "filled":
            spent = cumulative_quote if cumulative_quote and cumulative_quote > 0 else fills_quote
            if spent <= 0:
                spent = quote_amount
            if fees <= 0 and spent > 0:
                # Fees are not visible until fills are synced; reserve the
                # configured fee buffer instead of silently freeing it.
                fees = spent * Decimal(str(self.cfg.balance_fee_buffer_pct)) / Decimal(100)
            conn.execute(
                "UPDATE trading_budget_ledger SET reserved_quote = '0', spent_quote = ?, "
                "fee_quote = ?, active = 0, state = 'settled', note = ?, updated_at = ? "
                "WHERE intent_id = ?",
                (dec_str(spent), dec_str(fees), "order filled", now, intent_id),
            )
        elif new_state == "canceled":
            spent = cumulative_quote if cumulative_quote and cumulative_quote > 0 else fills_quote
            conn.execute(
                "UPDATE trading_budget_ledger SET reserved_quote = '0', spent_quote = ?, "
                "fee_quote = ?, active = 0, state = 'settled', note = ?, updated_at = ? "
                "WHERE intent_id = ?",
                (dec_str(spent), dec_str(fees), "order canceled; actual partial spend retained", now, intent_id),
            )
        elif new_state in ("failed", "rejected", "canceled_local"):
            conn.execute(
                "UPDATE trading_budget_ledger SET reserved_quote = '0', spent_quote = '0', "
                "fee_quote = '0', active = 0, state = 'released', note = ?, updated_at = ? "
                "WHERE intent_id = ?",
                (f"released after {new_state}; no charge", now, intent_id),
            )
        else:
            # planned/submitting/submitted/open/... keep the reservation.
            conn.execute(
                "UPDATE trading_budget_ledger SET state = 'reserved', active = 1, updated_at = ? "
                "WHERE intent_id = ?",
                (now, intent_id),
            )

    def create_intent(
        self,
        *,
        signal: ParsedSignal,
        signal_id: str,
        channel_id: int,
        message_id: int,
        edit_version: int,
        idempotency_key: str,
        client_order_id: str,
        mode: str,
        order_type: str,
        quote_amount: Decimal,
        base_quantity: Decimal | None,
        limit_price: Decimal | None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically reserve limits and insert a planned intent.

        Returns (intent, inserted). If the idempotency key already exists the
        existing intent is returned and nothing is reserved or executed.
        """

        now = self._ts()
        day_key = self._now().date().isoformat()
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM order_intents WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                # Retrying the same version must restore the status implied by
                # the durable intent instead of leaving a transient 'validated'
                # (or overwriting a terminal rejection with 'superseded').
                if existing["signal_id"] == signal_id:
                    if existing["state"] in ("rejected", "failed"):
                        conn.execute(
                            "UPDATE signals SET status = 'rejected', updated_at = ? "
                            "WHERE signal_id = ? AND status IN ('parsed', 'validated', 'planned')",
                            (now, signal_id),
                        )
                    elif existing["state"] not in ("canceled", "canceled_local", "filled"):
                        conn.execute(
                            "UPDATE signals SET status = 'planned', updated_at = ? "
                            "WHERE signal_id = ? AND status IN ('parsed', 'validated')",
                            (now, signal_id),
                        )
                return dict(existing), False

            # ── Atomic per-symbol admission (§3.2/§4) ──────────────────
            # This check runs *inside* the same DB transaction that inserts
            # the intent, so two concurrent Engine instances sharing the same
            # SQLite file cannot both pass the check (SQLite's
            # Database.transaction() holds an exclusive lock).  It replaces
            # the previous non-atomic pre-check and does not depend on
            # threading.RLock (which only protects one process).
            if signal.symbol:
                try:
                    placeholders = ", ".join("?" for _ in self.CONFLICTING_TRADE_STATES)
                    conflict = conn.execute(
                        f"SELECT trade_id, trade_state FROM trades WHERE symbol = ? AND trade_state IN ({placeholders}) LIMIT 1",
                        (signal.symbol.upper(), *self.CONFLICTING_TRADE_STATES),
                    ).fetchone()
                    if conflict is not None:
                        raise SymbolConflictError(
                            f"per-symbol concurrency: {signal.symbol.upper()} already has an active trade "
                            f"{conflict['trade_id']} in state {conflict['trade_state']}; "
                            "overlapping BUY not admitted"
                        )
                except SymbolConflictError:
                    raise
                except Exception as exc:
                    # DB error must never silently permit a BUY — fail closed
                    # and surface to Engine for safe rejection + developer alert.
                    raise TradingError(f"per-symbol concurrency check failed: {exc}") from exc

            intent_id = f"intent-{client_order_id}"
            if mode in ("testonly", "live"):
                if quote_amount is None or quote_amount <= 0:
                    raise RiskLimitExceeded("order has no positive quote amount to reserve")
                self._reserve_budget(conn, intent_id, quote_amount, now)

            conn.execute(
                "INSERT OR IGNORE INTO trading_daily_spend (day_key, quote_spent, order_count, updated_at) "
                "VALUES (?, '0', 0, ?)",
                (day_key, now),
            )
            daily = conn.execute(
                "SELECT quote_spent, order_count FROM trading_daily_spend WHERE day_key = ?",
                (day_key,),
            ).fetchone()
            spent = Decimal(daily["quote_spent"])
            count = int(daily["order_count"])

            conn.execute(
                """
                INSERT INTO order_intents
                    (intent_id, signal_id, idempotency_key, client_order_id, symbol, side,
                     order_type, quote_amount, base_quantity, limit_price, mode, state,
                     create_msg_id, create_channel_id, planned_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?, ?, ?)
                """,
                (
                    f"intent-{client_order_id}", signal_id, idempotency_key, client_order_id,
                    signal.symbol, signal.action, order_type, dec_str(quote_amount),
                    dec_str(base_quantity), dec_str(limit_price), mode, message_id, channel_id,
                    now, now,
                ),
            )
            conn.execute(
                "UPDATE trading_daily_spend SET quote_spent = ?, order_count = ?, updated_at = ? "
                "WHERE day_key = ?",
                (dec_str(spent + quote_amount), count + 1, now, day_key),
            )
            conn.execute(
                "UPDATE signals SET status = 'planned', updated_at = ? WHERE signal_id = ?",
                (now, signal_id),
            )
            conn.execute(
                "INSERT INTO order_status_history "
                "(intent_id, previous_state, new_state, note, changed_at) "
                "VALUES (?, NULL, 'planned', ?, ?)",
                (intent_id, f"signal {signal_id} edit={edit_version}", now),
            )
            # Read the row inside the same serialized transaction: the shared
            # sqlite connection must never be read outside the DB lock while
            # another thread owns an open transaction.
            inserted_row = conn.execute(
                "SELECT * FROM order_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            assert inserted_row is not None
        return dict(inserted_row), True

    def get_message(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return the stored source message for notification/reporting use."""

        with self.db.lock:
            row = self.db.conn.execute(
                "SELECT * FROM messages WHERE channel_id = ? AND message_id = ?",
                (channel_id, message_id),
            ).fetchone()
        return _row(row)

    def intent_for_message(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM order_intents WHERE create_channel_id = ? AND create_msg_id = ? "
                "ORDER BY planned_at DESC, intent_id DESC LIMIT 1",
                (channel_id, message_id),
            ).fetchone()
        )

    def get_intent(self, intent_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM order_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        )

    def intent_by_idempotency(self, key: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM order_intents WHERE idempotency_key = ?", (key,)
            ).fetchone()
        )

    def list_intents(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM order_intents ORDER BY planned_at DESC, intent_id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def pending_reconciliation(self, limit: int = 50) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in RECONCILABLE_INTENT_STATES)
        rows = self.db.conn.execute(
            f"SELECT * FROM order_intents WHERE mode = 'live' AND "
            f"(state IN ({placeholders}) OR fill_sync_required = 1) "
            "ORDER BY updated_at LIMIT ?",
            (*RECONCILABLE_INTENT_STATES, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------- order protection

    def upsert_protection(
        self,
        *,
        intent_id: str,
        client_order_id: str,
        symbol: str,
        entry_order_id: str | None = None,
        take_profit_price: Decimal | None = None,
        stop_loss_price: Decimal | None = None,
        protection_mode: str = "attached",
        status: str = "unverified",
        last_error: str | None = None,
    ) -> dict[str, Any]:
        """Persist intended/verified TP-SL state inside one transaction."""

        if protection_mode not in ("attached", "none"):
            raise TradingError(f"invalid protection mode {protection_mode!r}")
        if status not in PROTECTION_STATES:
            raise TradingError(f"invalid protection status {status!r}")
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO order_protection
                    (intent_id, client_order_id, symbol, entry_order_id,
                     take_profit_price, stop_loss_price, protection_mode, status,
                     last_error, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(intent_id) DO UPDATE SET
                    entry_order_id = COALESCE(excluded.entry_order_id, order_protection.entry_order_id),
                    take_profit_price = COALESCE(excluded.take_profit_price, order_protection.take_profit_price),
                    stop_loss_price = COALESCE(excluded.stop_loss_price, order_protection.stop_loss_price),
                    protection_mode = excluded.protection_mode,
                    status = excluded.status,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    intent_id, client_order_id, symbol.upper(), entry_order_id,
                    dec_str(take_profit_price), dec_str(stop_loss_price),
                    protection_mode, status, last_error, now, now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM order_protection WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            assert row is not None
            result = dict(row)
        self.audit(
            intent_id, "lifecycle", f"protection_{status}",
            f"tp={dec_str(take_profit_price)} sl={dec_str(stop_loss_price)}",
        )
        return result

    def get_protection(self, intent_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM order_protection WHERE intent_id = ?", (intent_id,)
            ).fetchone()
        )

    def update_protection(
        self,
        intent_id: str,
        *,
        status: str,
        last_error: str | None = None,
        exchange_tp_order_id: str | None = None,
        exchange_sl_order_id: str | None = None,
    ) -> dict[str, Any] | None:
        if status not in PROTECTION_STATES:
            raise TradingError(f"invalid protection status {status!r}")
        now = self._ts()
        verified = now if status == "active" else None
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE order_protection SET
                    status = ?, last_error = ?,
                    exchange_tp_order_id = COALESCE(?, exchange_tp_order_id),
                    exchange_sl_order_id = COALESCE(?, exchange_sl_order_id),
                    verified_at = COALESCE(?, verified_at),
                    updated_at = ?
                WHERE intent_id = ?
                """,
                (
                    status, last_error, exchange_tp_order_id, exchange_sl_order_id,
                    verified, now, intent_id,
                ),
            )
        self.audit(intent_id, "lifecycle", f"protection_{status}", last_error)
        return self.get_protection(intent_id)

    def pending_protection_verification(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM order_protection WHERE status IN ('unverified', 'unknown_requires_reconciliation') "
            "ORDER BY updated_at LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def outstanding_balance_usage(
        self, symbol: str, *, exclude_intent_id: str | None = None
    ) -> dict[str, Decimal]:
        """Sum live non-terminal intents on ``symbol`` for balance reservation.

        Exchange ``free`` balances already exclude funds locked by open exchange
        orders, so including those open intents here is intentionally
        conservative: the preflight never spends the same local balance twice,
        at the cost of possibly under-trading while orders are open.
        """

        placeholders = ",".join("?" for _ in OPEN_INTENT_STATES)
        sql = (
            f"SELECT side, quote_amount, base_quantity FROM order_intents "
            f"WHERE mode = 'live' AND symbol = ? AND state IN ({placeholders})"
        )
        params: list[Any] = [symbol.upper(), *OPEN_INTENT_STATES]
        if exclude_intent_id is not None:
            sql += " AND intent_id <> ?"
            params.append(exclude_intent_id)
        buy_quote = Decimal("0")
        sell_base = Decimal("0")
        for row in self.db.conn.execute(sql, params).fetchall():
            try:
                if row["side"] == "buy" and row["quote_amount"] is not None:
                    buy_quote += Decimal(row["quote_amount"])
                elif row["side"] == "sell" and row["base_quantity"] is not None:
                    sell_base += Decimal(row["base_quantity"])
            except (InvalidOperation, TypeError, ValueError):
                continue
        return {"quote": buy_quote, "base": sell_base}

    def set_fill_sync_required(self, intent_id: str, required: bool, *, note: str = "") -> None:
        now = self._ts()
        with self.db.transaction() as conn:
            if required:
                conn.execute(
                    "UPDATE order_intents SET fill_sync_required = 1, fill_synced_at = NULL, "
                    "updated_at = ? WHERE intent_id = ?",
                    (now, intent_id),
                )
            else:
                conn.execute(
                    "UPDATE order_intents SET fill_sync_required = 0, fill_synced_at = ?, "
                    "updated_at = ? WHERE intent_id = ?",
                    (now, now, intent_id),
                )
            if note:
                conn.execute(
                    "INSERT INTO audit_events (correlation, category, event, detail, created_at) "
                    "VALUES (?, 'reconcile', ?, ?, ?)",
                    (intent_id, "fills_pending" if required else "fills_synced", note[:2000], now),
                )

    def claim_intent_for_submission(self, intent_id: str, *, note: str) -> bool:
        """Atomically move ``planned`` -> ``submitting``.

        Returns True only for the caller that won the claim.  Exactly one
        delivery (queue retry, duplicate message, conditional trigger) can ever
        submit an entry order for an intent.
        """

        now = self._ts()
        with self.db.transaction() as conn:
            current = conn.execute(
                "SELECT state FROM order_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if current is None:
                raise TradingError(f"intent {intent_id} not found")
            cursor = conn.execute(
                "UPDATE order_intents SET state = 'submitting', updated_at = ? "
                "WHERE intent_id = ? AND state = 'planned'",
                (now, intent_id),
            )
            if cursor.rowcount == 0:
                return False
            conn.execute(
                "INSERT INTO order_status_history "
                "(intent_id, exchange_order_id, previous_state, new_state, note, changed_at) "
                "VALUES (?, NULL, 'planned', 'submitting', ?, ?)",
                (intent_id, note[:1000], now),
            )
            conn.execute(
                "INSERT INTO audit_events (correlation, category, event, detail, created_at) "
                "VALUES (?, 'lifecycle', 'submission_claimed', ?, ?)",
                (intent_id, note[:2000], now),
            )
        return True

    def transition_intent(
        self,
        intent_id: str,
        new_state: str,
        *,
        note: str,
        exchange_order_id: str | None = None,
        exchange_status: str | None = None,
        executed_qty: Decimal | None = None,
        cumulative_quote: Decimal | None = None,
        reject_reason: str | None = None,
        category: str = "lifecycle",
        fill_sync_required: bool | None = None,
    ) -> dict[str, Any]:
        now = self._ts()
        terminal = new_state in TERMINAL_INTENT_STATES
        with self.db.transaction() as conn:
            current = conn.execute(
                "SELECT state, signal_id FROM order_intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if current is None:
                raise TradingError(f"intent {intent_id} not found")
            sets = ["state = ?", "updated_at = ?"]
            params: list[Any] = [new_state, now]
            if exchange_order_id is not None:
                sets.append("exchange_order_id = ?")
                params.append(exchange_order_id)
            if exchange_status is not None:
                sets.append("exchange_status = ?")
                params.append(exchange_status)
            if executed_qty is not None:
                sets.append("executed_qty = ?")
                params.append(dec_str(executed_qty))
            if cumulative_quote is not None:
                sets.append("cumulative_quote = ?")
                params.append(dec_str(cumulative_quote))
            if reject_reason is not None:
                sets.append("reject_reason = ?")
                params.append(reject_reason[:500])
            if new_state == "submitted":
                sets.append("submitted_at = COALESCE(submitted_at, ?)")
                params.append(now)
            if terminal:
                sets.append("terminal_at = COALESCE(terminal_at, ?)")
                params.append(now)
            if fill_sync_required is not None:
                if fill_sync_required:
                    sets.extend(["fill_sync_required = 1", "fill_synced_at = NULL"])
                else:
                    sets.append("fill_sync_required = 0")
                    sets.append("fill_synced_at = ?")
                    params.append(now)
            params.append(intent_id)
            conn.execute(f"UPDATE order_intents SET {', '.join(sets)} WHERE intent_id = ?", params)
            if new_state in ("rejected", "failed"):
                # Terminal local/exchange rejection and its signal status are
                # one invariant.  Never overwrite a newer edit's superseded
                # state, or an explicit cancel/delete.
                conn.execute(
                    "UPDATE signals SET status = 'rejected', "
                    "rejection_reason = COALESCE(rejection_reason, ?), updated_at = ? "
                    "WHERE signal_id = ? AND status IN ('parsed', 'validated', 'planned')",
                    ((reject_reason or note)[:500], now, current["signal_id"]),
                )
            conn.execute(
                "INSERT INTO order_status_history "
                "(intent_id, exchange_order_id, previous_state, new_state, note, changed_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (intent_id, exchange_order_id, current["state"], new_state, note[:1000], now),
            )
            conn.execute(
                "INSERT INTO audit_events (correlation, category, event, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (intent_id, category, new_state, note[:2000], now),
            )
            current_after = conn.execute(
                "SELECT exchange_order_id, cumulative_quote FROM order_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
            eff_order = exchange_order_id or (current_after["exchange_order_id"] if current_after else None)
            eff_cumulative = (
                cumulative_quote
                if cumulative_quote is not None
                else (
                    Decimal(current_after["cumulative_quote"])
                    if current_after and current_after["cumulative_quote"] not in (None, "")
                    else None
                )
            )
            self._settle_budget(
                conn,
                intent_id=intent_id,
                new_state=new_state,
                cumulative_quote=eff_cumulative,
                exchange_order_id=eff_order,
                now=now,
            )
        intent = self.get_intent(intent_id)
        assert intent is not None
        return intent

    def upsert_exchange_order(self, *, intent_id: str, raw: dict[str, Any], discovered_in: str) -> None:
        order_id = raw.get("orderId")
        if order_id is None:
            return
        now = self._ts()
        with self.db.transaction() as conn:
            intent = conn.execute(
                "SELECT symbol, side, order_type FROM order_intents WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
            symbol = raw.get("symbol") or (intent["symbol"] if intent else None)
            side = raw.get("side") or (intent["side"] if intent else None)
            order_type = raw.get("type") or (intent["order_type"] if intent else None)
            conn.execute(
                """
                INSERT INTO exchange_orders
                    (exchange_order_id, client_order_id, intent_id, symbol, side, order_type,
                     price, orig_qty, executed_qty, cumulative_quote, status, time_in_force,
                     exchange_time_ms, discovered_in, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange_order_id) DO UPDATE SET
                    client_order_id = COALESCE(excluded.client_order_id, exchange_orders.client_order_id),
                    intent_id = COALESCE(excluded.intent_id, exchange_orders.intent_id),
                    price = COALESCE(excluded.price, exchange_orders.price),
                    orig_qty = COALESCE(excluded.orig_qty, exchange_orders.orig_qty),
                    executed_qty = COALESCE(excluded.executed_qty, exchange_orders.executed_qty),
                    cumulative_quote = COALESCE(excluded.cumulative_quote, exchange_orders.cumulative_quote),
                    status = excluded.status,
                    time_in_force = COALESCE(excluded.time_in_force, exchange_orders.time_in_force),
                    exchange_time_ms = COALESCE(excluded.exchange_time_ms, exchange_orders.exchange_time_ms),
                    updated_at = excluded.updated_at
                """,
                (
                    str(order_id), raw.get("clientOrderId") or raw.get("origClientOrderId"),
                    intent_id, symbol, side, order_type,
                    raw.get("price"), raw.get("origQty") or raw.get("Qty"),
                    raw.get("executedQty"), raw.get("cumulativeQuoteQty"),
                    _exchange_status(raw.get("status")), raw.get("timeInForce"),
                    raw.get("time"), discovered_in, now, now,
                ),
            )

    def add_fill(
        self,
        *,
        exchange_order_id: str,
        symbol: str,
        raw: dict[str, Any],
        source: str = "rest_query",
    ) -> bool:
        now = self._ts()
        with self.db.transaction() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO order_fills
                    (exchange_order_id, symbol, price, quantity, quote_amount, fee_asset,
                     fee_amount, exchange_trade_id, is_buyer, is_maker, trade_time_ms,
                     source, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    exchange_order_id, symbol, str(raw.get("price")), str(raw.get("qty")),
                    str(raw.get("quoteQty")) if raw.get("quoteQty") is not None else None,
                    raw.get("commissionAsset"),
                    str(raw.get("commission")) if raw.get("commission") is not None else None,
                    _trade_identity(raw, exchange_order_id),
                    1 if raw.get("isBuyer") else 0, 1 if raw.get("isMaker") else 0,
                    raw.get("time"), source, now, now,
                ),
            )
            inserted = cur.rowcount > 0
            if inserted:
                intent = conn.execute(
                    "SELECT intent_id, state, cumulative_quote FROM order_intents "
                    "WHERE exchange_order_id = ? ORDER BY planned_at DESC LIMIT 1",
                    (str(exchange_order_id),),
                ).fetchone()
                if intent is not None:
                    try:
                        cumulative = (
                            Decimal(str(intent["cumulative_quote"]))
                            if intent["cumulative_quote"] not in (None, "")
                            else None
                        )
                    except (InvalidOperation, TypeError, ValueError):
                        cumulative = None
                    self._settle_budget(
                        conn,
                        intent_id=str(intent["intent_id"]),
                        new_state=str(intent["state"]),
                        cumulative_quote=cumulative,
                        exchange_order_id=str(exchange_order_id),
                        now=now,
                    )
            return inserted

    def fill_quantity_total(self, exchange_order_id: str) -> Decimal:
        row = self.db.conn.execute(
            "SELECT quantity FROM order_fills WHERE exchange_order_id = ?",
            (str(exchange_order_id),),
        ).fetchall()
        total = Decimal("0")
        for item in row:
            try:
                total += Decimal(item["quantity"])
            except (InvalidOperation, TypeError, ValueError):
                continue
        return total

    def fills_for_order(self, exchange_order_id: str) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM order_fills WHERE exchange_order_id = ? ORDER BY fill_id",
            (str(exchange_order_id),),
        ).fetchall()
        return [dict(row) for row in rows]

    def record_reconciliation(
        self,
        *,
        intent_id: str | None,
        client_order_id: str | None,
        local_state: str | None,
        exchange_state: str | None,
        outcome: str,
        detail: str,
        emit_audit: bool = True,
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO reconciliation_results "
                "(intent_id, client_order_id, local_state, exchange_state, outcome, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (intent_id, client_order_id, local_state, exchange_state, outcome, detail[:2000], self._ts()),
            )
            if emit_audit and intent_id:
                self.audit(
                    intent_id, "reconcile", f"reconcile_{outcome}",
                    detail[:2000],
                    event_type="reconcile.outcome",
                    trade_id=None,
                    intent_id=intent_id,
                    order_id=client_order_id,
                    outcome=outcome,
                    severity="INFO" if outcome in ("consistent", "filled") else "WARNING",
                    dedupe_key=f"reconcile:{intent_id}:{outcome}:{detail[:40]}",
                    conn=conn,
                )

    def reconciliation_counts(self) -> dict[str, int]:
        return {
            str(r["outcome"]): int(r["c"])
            for r in self.db.conn.execute(
                "SELECT outcome, COUNT(*) AS c FROM reconciliation_results GROUP BY outcome"
            )
        }

    def fills_count(self) -> int:
        return int(self.db.conn.execute("SELECT COUNT(*) FROM order_fills").fetchone()[0])

    # ------------------------------------------------------- trading settings
    # Persisted owner-controlled settings (e.g. order_quote_amount) that can be
    # changed through the Telegram management menu and survive restarts.  They
    # live in the shared ``control_settings`` table so the management bot, the
    # control bot and the trading pipeline all read the same value.
    # Config.toml/env take precedence at load time; DB values override the
    # built-in default when no explicit config source is present.

    def get_setting(self, key: str) -> str | None:
        row = self.db.conn.execute(
            "SELECT value FROM control_settings WHERE key = ?", (key,)
        ).fetchone()
        return str(row["value"]) if row is not None else None

    def set_setting(self, key: str, value: str) -> None:
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO control_settings (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, str(value), now),
            )

    # ------------------------------------------------------- activation state

    def get_app_state(self, key: str) -> str | None:
        row = self.db.conn.execute(
            "SELECT value FROM app_state WHERE key = ?", (key,)
        ).fetchone()
        return str(row["value"]) if row is not None else None

    def set_app_state(self, key: str, value: str) -> None:
        """Upsert one runtime flag; used for the fail-closed BUY block."""

        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO app_state (key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, str(value), now),
            )

    # ------------------------------------------------------- new-BUY safety

    BUY_BLOCK_KEY = "trading.new_buys_blocked"
    BUY_BLOCK_REASON_KEY = "trading.new_buys_block_reason"

    def set_buy_block(self, reason: str | None) -> None:
        """Persist or clear the global new-BUY block.

        The block is intentionally independent of any single trade row: it
        survives restarts and blocks every new BUY while any bot-owned position
        lacks verified exit protection.  Clearing it requires a verified
        protection state (or a fully closed position), not a timeout.
        """

        now = self._ts()
        blocked = "1" if reason else "0"
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO app_state (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (self.BUY_BLOCK_KEY, blocked, now),
            )
            conn.execute(
                "INSERT INTO app_state (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (self.BUY_BLOCK_REASON_KEY, (reason or "")[:500], now),
            )

    def buy_block_reason(self) -> str | None:
        """Return the active block reason, or None when new BUYs are allowed."""

        if self.get_app_state(self.BUY_BLOCK_KEY) != "1":
            return None
        reason = self.get_app_state(self.BUY_BLOCK_REASON_KEY)
        return reason or "an open position lacks verified exit protection"

    def set_app_state_once(self, key: str, value: str) -> str:
        """Set ``key`` only if absent; return the effective persisted value."""

        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO app_state (key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, now),
            )
            row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
        assert row is not None
        return str(row["value"])

    def activation_boundary(self) -> int | None:
        raw = self.get_app_state("trading.activation_boundary_message_id")
        try:
            return int(raw) if raw not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def ensure_activation_boundary(self, channel_id: int, *, minimum: int | None = None) -> int:
        """Persist a safe first-tradeable Telegram message id.

        Only message ids greater than this boundary may create orders.  On the
        first live start the boundary is the newest stored/recovered channel
        message, so a downtime backlog is never replayed as fresh orders.  A
        later startup may move the boundary *forward* (never backwards) to keep
        the same guarantee after a restart.
        """

        existing = self.activation_boundary()
        if existing is not None:
            if minimum is not None and int(minimum) > existing:
                with self.db.transaction() as conn:
                    conn.execute(
                        "UPDATE app_state SET value = ?, updated_at = ? "
                        "WHERE key = 'trading.activation_boundary_message_id'",
                        (str(int(minimum)), self._ts()),
                    )
                return int(minimum)
            return existing
        if minimum is not None:
            # The startup caller captured the newest pre-handler id.  Use that
            # exact snapshot: a message delivered during handler registration
            # is newer and must remain above the boundary.
            boundary = max(0, int(minimum))
        else:
            row = self.db.conn.execute(
                "SELECT COALESCE(MAX(message_id), 0) AS m FROM messages WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            boundary = int(row["m"] or 0)
        effective = self.set_app_state_once(
            "trading.activation_boundary_message_id", str(boundary)
        )
        return int(effective)

    # -------------------------------------------------------- notifications

    def notification_enabled(self, role: str, category: str) -> bool:
        """Return the persisted preference, falling back to the code defaults."""

        row = self.db.conn.execute(
            "SELECT enabled FROM notification_preferences WHERE role = ? AND category = ?",
            (role, category),
        ).fetchone()
        if row is not None:
            return bool(int(row["enabled"]))
        return NOTIFICATION_DEFAULTS.get(role, {}).get(category, False)

    def set_notification_preference(self, role: str, category: str, enabled: bool) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO notification_preferences (role, category, enabled, updated_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(role, category) DO UPDATE SET "
                "enabled = excluded.enabled, updated_at = excluded.updated_at",
                (role, category, 1 if enabled else 0, self._ts()),
            )

    def notification_preferences(self, role: str | None = None) -> list[dict[str, Any]]:
        if role is None:
            rows = self.db.conn.execute(
                "SELECT * FROM notification_preferences ORDER BY role, category"
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM notification_preferences WHERE role = ? ORDER BY category",
                (role,),
            ).fetchall()
        return [dict(row) for row in rows]

    def enqueue_notification(
        self,
        *,
        event_key: str,
        signal_id: str | None,
        intent_id: str | None,
        role: str,
        chat_id: int,
        text: str,
        category: str = "signal_decision",
        event_type: str = "signal.decision",
        compact_text: str | None = None,
        expanded_text: str | None = None,
    ) -> bool:
        now = self._ts()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO trading_notifications "
                "(event_key, signal_id, intent_id, category, event_type, role, chat_id, "
                "text, compact_text, expanded_text, status, attempts, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)",
                (
                    event_key[:300], signal_id, intent_id, category[:80], event_type[:120],
                    role, chat_id, text[:4000], redact_text(compact_text),
                    redact_text(expanded_text), now, now,
                ),
            )
            return cur.rowcount > 0

    def notification(self, notification_id: int) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM trading_notifications WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
        )

    def pending_notifications(self, limit: int = 50) -> list[dict[str, Any]]:
        now = self._ts()
        rows = self.db.conn.execute(
            "SELECT * FROM trading_notifications "
            "WHERE status IN ('pending', 'failed') "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY notification_id LIMIT ?",
            (now, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def claim_notification(self, notification_id: int, token: str) -> bool:
        now = self._ts()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE trading_notifications SET status = 'sending', claim_token = ?, "
                "claimed_at = ?, updated_at = ? "
                "WHERE notification_id = ? AND status IN ('pending', 'failed')",
                (token, now, now, notification_id),
            )
            return cur.rowcount > 0

    def recover_ambiguous_sending(self, *, older_than_s: int = 120) -> int:
        """A crash during delivery leaves an unknowable outcome.

        Such rows are marked ``ambiguous`` and are never resent automatically;
        the owner/developer can inspect and explicitly re-queue them.
        """

        cutoff = (self._now() - timedelta(seconds=max(1, older_than_s))).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE trading_notifications SET status = 'ambiguous', "
                "last_error = COALESCE(last_error, 'process ended during delivery'), "
                "claim_token = NULL, updated_at = ? "
                "WHERE status = 'sending' AND claimed_at IS NOT NULL AND claimed_at < ?",
                (self._ts(), cutoff),
            )
            return cur.rowcount

    def finish_notification(
        self,
        notification_id: int,
        *,
        outcome: str,
        error: str | None = None,
        telegram_message_id: int | None = None,
        retry_after_s: int | None = None,
        attempt_no: int | None = None,
        ambiguous: bool = False,
    ) -> None:
        """Record one delivery attempt and apply its durable outcome."""

        now_dt = self._now()
        now = self._ts()
        next_attempt: str | None = None
        if outcome == "retry" and retry_after_s:
            next_attempt = (
                now_dt + timedelta(seconds=max(1, int(retry_after_s)))
            ).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT attempts, role FROM trading_notifications WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
            if row is None:
                return
            attempt = int(attempt_no or (int(row["attempts"]) + 1))
            conn.execute(
                "INSERT INTO notification_delivery_attempts "
                "(notification_id, attempt_no, attempted_at, outcome, error, "
                "telegram_message_id, retry_after_s, ambiguous) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    notification_id, attempt, now, outcome, redact_text(error),
                    telegram_message_id, retry_after_s, 1 if ambiguous else 0,
                ),
            )
            if outcome == "sent":
                conn.execute(
                    "UPDATE trading_notifications SET status = 'sent', attempts = ?, "
                    "last_error = NULL, sent_at = ?, telegram_message_id = COALESCE(?, telegram_message_id), "
                    "claim_token = NULL, updated_at = ? WHERE notification_id = ?",
                    (attempt, now, telegram_message_id, now, notification_id),
                )
            elif outcome == "retry":
                conn.execute(
                    "UPDATE trading_notifications SET status = 'pending', attempts = ?, "
                    "last_error = ?, next_attempt_at = ?, claim_token = NULL, updated_at = ? "
                    "WHERE notification_id = ?",
                    (attempt, redact_text(error, limit=500), next_attempt, now, notification_id),
                )
            else:
                status = "ambiguous" if ambiguous or outcome == "ambiguous" else "failed"
                conn.execute(
                    "UPDATE trading_notifications SET status = ?, attempts = ?, "
                    "last_error = ?, claim_token = NULL, updated_at = ? WHERE notification_id = ?",
                    (status, attempt, redact_text(error, limit=500), now, notification_id),
                )

    def mark_notification_sent(
        self, notification_id: int, *, telegram_message_id: int | None = None
    ) -> None:
        self.finish_notification(
            notification_id, outcome="sent", telegram_message_id=telegram_message_id
        )

    def mark_notification_failed(self, notification_id: int, error: str) -> None:
        self.finish_notification(notification_id, outcome="failed", error=error)

    def notification_attempts(self, notification_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM notification_delivery_attempts WHERE notification_id = ? "
            "ORDER BY attempt_no",
            (notification_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------- notification callbacks

    def create_notification_callback(
        self,
        *,
        notification_id: int,
        role: str,
        action: str,
        ttl_s: int = 7 * 24 * 3600,
    ) -> str:
        token = secrets.token_urlsafe(18)
        now_dt = self._now()
        expires = (now_dt + timedelta(seconds=ttl_s)).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO notification_callbacks "
                "(token, notification_id, role, action, created_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (token, notification_id, role, action, self._ts(), expires),
            )
        return token

    def notification_callback(self, token: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM notification_callbacks WHERE token = ?", (token,)
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        expires = _parse_iso(data.get("expires_at"))
        if expires is not None and expires < self._now():
            return None
        return data

    def consume_notification_callback(self, token: str, role: str) -> dict[str, Any] | None:
        data = self.notification_callback(token)
        if data is None or data.get("role") != role:
            return None
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE notification_callbacks SET use_count = use_count + 1, used_at = ? "
                "WHERE token = ?",
                (self._ts(), token),
            )
        return data


def _trade_identity(raw: dict[str, Any], exchange_order_id: str) -> str:
    """Stable deduplication key even when the exchange omits the deal id."""

    raw_id = raw.get("id")
    if raw_id is not None and str(raw_id).strip() != "":
        return str(raw_id)
    material = "|".join(
        str(raw.get(key)) for key in ("time", "price", "qty", "quoteQty", "commission", "isBuyer")
    )
    digest = hashlib.sha256(f"{exchange_order_id}|{material}".encode()).hexdigest()
    return f"synth-{digest[:32]}"


def _exchange_status(status: Any) -> str:
    allowed = {"NEW", "FILLED", "PARTIALLY_FILLED", "CANCELED", "PARTIALLY_CANCELED"}
    value = str(status or "UNKNOWN").upper()
    return value if value in allowed else "UNKNOWN"


def is_unique_violation(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.IntegrityError)
