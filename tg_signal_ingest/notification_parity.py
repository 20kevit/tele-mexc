"""Read-only notification parity report (F-08 Phase 1, additive).

Compares the authoritative domain-event ledger (``audit_events`` in the
trading/source database) with the per-recipient delivery ledger managed by
the independent notification dispatcher (``deliveries`` in
``notification-delivery.sqlite3``), **without modifying either database and
without any Telegram activity**.

The expectation logic mirrors the relay exactly: an event that was relayed
(i.e. recorded in ``relayed_events``) must have one delivery row per
expected recipient role — including rows the relay persisted as
``suppressed`` (suppression is always persisted for operator history).
Roles are the event's ``audience_json`` intersected with the configured
recipients, or *every* configured recipient for CRITICAL events; the
per-role pending/suppressed decision is recomputed with the dispatcher's
own :func:`delivery_decision` so the report can never drift from production
routing rules.

This module is a Phase 1 verification aid for the staged dual-ledger
migration (audit F-08).  It opens both databases with SQLite ``mode=ro``
URIs and ``PRAGMA query_only=ON`` — the same technique the dispatcher uses
for its source connection — and performs SELECTs only.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .notification_dispatcher import delivery_decision, notification_category

DEFAULT_AUDIENCE = ("owner", "owner_b", "developer")


def _read_only_conn(path: str | Path) -> sqlite3.Connection:
    """Open ``path`` read-only; raise sqlite3.Error if unusable."""

    conn = sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Mirror NotificationDispatcher._event_payload (never raises)."""

    try:
        value = json.loads(str(event.get("payload_json") or "{}"))
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _expected_audience(event: dict[str, Any]) -> list[str]:
    """Mirror the relay's audience parsing including its fallbacks."""

    try:
        audience = json.loads(str(event.get("audience_json") or "") or '["owner","owner_b","developer"]')
    except (TypeError, ValueError):
        audience = list(DEFAULT_AUDIENCE)
    if not isinstance(audience, list):
        audience = list(DEFAULT_AUDIENCE)
    return [str(role) for role in audience]


def _routing_preferences(source: sqlite3.Connection) -> tuple[dict[tuple[str, str], bool], str, dict[str, str]]:
    """Mirror NotificationDispatcher._routing_preferences (SELECT-only)."""

    preferences = {
        (str(row["role"]), str(row["category"])): bool(row["enabled"])
        for row in source.execute("SELECT role,category,enabled FROM notification_preferences").fetchall()
    }
    settings = {
        str(row["key"]): str(row["value"])
        for row in source.execute("SELECT key,value FROM control_settings").fetchall()
    }
    minimum_severity = str(settings.get("notification_min_severity") or "INFO").upper()
    return preferences, minimum_severity, settings


def build_parity_report(
    source_db_path: str | Path,
    delivery_db_path: str | Path,
    recipients: dict[str, int],
    *,
    since_audit_id: int = 0,
    gap_limit: int = 50,
) -> dict[str, Any]:
    """Build the parity report dict.  Pure read-only; no Telegram activity.

    ``recipients`` maps recipient role to chat id exactly like the worker's
    wiring (``{"owner": ..., "owner_b": ..., "developer": ...}``, zero
    values filtered out by the caller).
    """

    report: dict[str, Any] = {
        "source_db": str(source_db_path),
        "delivery_db": str(delivery_db_path),
        "delivery_db_present": Path(delivery_db_path).exists(),
        "since_audit_id": int(since_audit_id),
    }
    source: sqlite3.Connection | None = None
    delivery: sqlite3.Connection | None = None
    try:
        source = _read_only_conn(source_db_path)
        preferences, minimum_severity, settings = _routing_preferences(source)
        max_row = source.execute("SELECT COALESCE(MAX(audit_id),0) AS max_id FROM audit_events").fetchone()
        source_max = int(max_row["max_id"] or 0)

        if report["delivery_db_present"]:
            delivery = _read_only_conn(delivery_db_path)

        cursor = 0
        relayed: set[str] = set()
        deliveries_by_event: dict[str, dict[str, list[str]]] = {}
        status_counts_by_role: dict[str, dict[str, int]] = {}
        if delivery is not None:
            row = delivery.execute("SELECT value FROM dispatcher_state WHERE key='source_cursor'").fetchone()
            cursor = int(str(row["value"] or "0")) if row is not None else 0
            relayed = {str(r["event_id"]) for r in delivery.execute("SELECT event_id FROM relayed_events")}
            for row in delivery.execute("SELECT event_id, recipient_role, status FROM deliveries").fetchall():
                event_id = str(row["event_id"])
                role = str(row["recipient_role"])
                status = str(row["status"])
                deliveries_by_event.setdefault(event_id, {}).setdefault(role, []).append(status)
                role_counts = status_counts_by_role.setdefault(role, {})
                role_counts[status] = role_counts.get(status, 0) + 1

        events = source.execute(
            """
            SELECT audit_id, event_id, event_type, occurred_at_utc, aggregate_type,
                   aggregate_id, correlation_id, symbol, severity, payload_json, audience_json
            FROM audit_events
            WHERE event_class='domain' AND audit_id > ? AND audit_id <= ?
            ORDER BY audit_id
            """,
            (int(since_audit_id), cursor),
        ).fetchall()

        expected_deliveries = 0
        expected_pending = 0
        expected_suppressed = 0
        delivery_rows = 0
        gaps: list[dict[str, Any]] = []
        gaps_total = 0
        category_counts: dict[str, int] = {}
        unrelayed_within_cursor = 0
        for row in events:
            event = dict(row)
            event_id = str(event["event_id"])
            if event_id not in relayed:
                # Within the relay cursor but absent from the relay ledger:
                # the relay claims progress it never recorded.  Anomaly.
                unrelayed_within_cursor += 1
                continue
            event_type = str(event["event_type"])
            category = notification_category(event_type)
            category_counts[category] = category_counts.get(category, 0) + 1
            severity = str(event.get("severity") or "INFO").upper()
            payload = _event_payload(event)
            if severity == "CRITICAL":
                roles = list(recipients)
            else:
                roles = [role for role in _expected_audience(event) if recipients.get(role)]
            actual = deliveries_by_event.get(event_id, {})
            delivery_rows += sum(len(parts) for parts in actual.values())
            for role in roles:
                expected_deliveries += 1
                decision, _reason = delivery_decision(
                    role, severity, category, event_type, payload, preferences, settings, minimum_severity
                )
                if decision == "pending":
                    expected_pending += 1
                else:
                    expected_suppressed += 1
                if role not in actual:
                    gaps_total += 1
                    if len(gaps) < max(1, int(gap_limit)):
                        gaps.append(
                            {
                                "audit_id": int(event["audit_id"]),
                                "event_id": event_id,
                                "event_type": event_type,
                                "severity": severity,
                                "recipient_role": role,
                                "occurred_at_utc": event.get("occurred_at_utc"),
                            }
                        )

        orphans: list[dict[str, Any]] = []
        orphans_total = 0
        if delivery is not None:
            for row in delivery.execute(
                "SELECT delivery_id, event_id, recipient_role, status FROM deliveries"
            ).fetchall():
                if str(row["event_id"]) not in relayed:
                    orphans_total += 1
                    if len(orphans) < max(1, int(gap_limit)):
                        orphans.append(
                            {
                                "delivery_id": str(row["delivery_id"]),
                                "event_id": str(row["event_id"]),
                                "recipient_role": str(row["recipient_role"]),
                                "status": str(row["status"]),
                            }
                        )

        report.update(
            {
                "source_cursor": cursor,
                "source_max_audit_id": source_max,
                "cursor_lag": max(0, source_max - cursor),
                "relay_caught_up": source_max <= cursor,
                "domain_events_considered": len(events),
                "relayed_events_considered": len(events) - unrelayed_within_cursor,
                "unrelayed_within_cursor": unrelayed_within_cursor,
                "expected_deliveries": expected_deliveries,
                "expected_pending": expected_pending,
                "expected_suppressed": expected_suppressed,
                "delivery_rows": delivery_rows,
                "gaps_total": gaps_total,
                "gaps": gaps,
                "orphans_total": orphans_total,
                "orphans": orphans,
                "status_counts_by_role": {
                    role: dict(sorted(counts.items())) for role, counts in sorted(status_counts_by_role.items())
                },
                "category_counts": dict(sorted(category_counts.items())),
                "parity_ok": gaps_total == 0 and orphans_total == 0 and unrelayed_within_cursor == 0,
            }
        )
        return report
    except sqlite3.Error as exc:
        report["error"] = f"report failed ({exc.__class__.__name__}: {exc})"
        report["parity_ok"] = False
        return report
    finally:
        if delivery is not None:
            delivery.close()
        if source is not None:
            source.close()
