"""Persistence and review audit for offline signal candidates.

Machine candidates are immutable for a given
``(channel, message, edit version, parser version)``. Replaying therefore uses
``INSERT OR IGNORE`` and is idempotent. Human interpretations are append-only
rows in ``signal_candidate_reviews``; they never overwrite the Telegram source
row or the machine-generated candidate fields.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..db import Database
from .model import (
    CandidateStatus,
    MarketType,
    ParsedCandidate,
    ParseStatus,
    Relationship,
    ReviewStatus,
    SignalClassification,
    decimal_text,
)
from .parser import ANALYSIS_PARSER_VERSION

_UPDATE_CLASSIFICATIONS = {
    SignalClassification.MODIFICATION,
    SignalClassification.CANCELLATION,
    SignalClassification.TARGET_UPDATE,
    SignalClassification.STOP_LOSS_UPDATE,
}
_ALL_RE = re.compile(r"(?<![\w])ALL(?![\w])|همه", re.IGNORECASE)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def candidate_identity(
    channel_id: int, message_id: int, edit_version: int, parser_version: int
) -> tuple[str, str]:
    material = f"analysis:{channel_id}:{message_id}:{edit_version}:{parser_version}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"cand-{digest[:20]}", digest


class AnalysisError(Exception):
    """Safe-to-display analysis persistence error."""


class AnalysisStore:
    def __init__(self, db: Database, *, now: Any | None = None):
        self.db = db
        self._now = now or _utc_now

    # --------------------------------------------------------------- runs

    def start_run(self, *, parser_version: int, scope: dict[str, Any]) -> str:
        run_id = f"run-{uuid.uuid4().hex[:12]}"
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO analysis_runs (run_id, parser_version, scope_json, status, counts_json, "
                "started_at) VALUES (?, ?, ?, 'running', '{}', ?)",
                (run_id, parser_version, json.dumps(scope, sort_keys=True, ensure_ascii=False), self._now()),
            )
        return run_id

    def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        counts: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE analysis_runs SET status = ?, counts_json = ?, finished_at = ?, error = ? "
                "WHERE run_id = ?",
                (
                    status,
                    json.dumps(counts or {}, sort_keys=True, ensure_ascii=False),
                    self._now(),
                    (error or "")[:500] or None,
                    run_id,
                ),
            )

    def latest_run(self) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM analysis_runs ORDER BY started_at DESC LIMIT 1"
        ).fetchone()
        return _decode_run(row) if row else None

    # ---------------------------------------------------------- candidates

    def record_candidate(
        self,
        parsed: ParsedCandidate,
        *,
        channel_id: int,
        message_id: int,
        edit_version: int,
        source_datetime: str,
        reply_to_message_id: int | None = None,
        parser_version: int = ANALYSIS_PARSER_VERSION,
    ) -> tuple[str, bool, int, Relationship, str | None]:
        """Insert one machine candidate.

        Returns ``(candidate_id, inserted, superseded_count, relationship,
        parent_candidate_id)``.
        """

        resolved, relationship, parent_id = self.resolve_relationship(
            parsed,
            channel_id=channel_id,
            message_id=message_id,
            reply_to_message_id=reply_to_message_id,
        )
        candidate_id, idempotency_key = candidate_identity(
            channel_id, message_id, edit_version, parser_version
        )
        now = self._now()
        superseded = 0
        with self.db.transaction() as conn:
            # A new edit/parser version makes older current rows historical.
            cur = conn.execute(
                """
                UPDATE signal_candidates SET status = 'superseded', updated_at = ?
                WHERE channel_id = ? AND message_id = ?
                  AND status = 'current'
                  AND NOT (edit_version = ? AND parser_version = ?)
                """,
                (now, channel_id, message_id, edit_version, parser_version),
            )
            superseded = cur.rowcount
            inserted = False
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO signal_candidates
                    (candidate_id, channel_id, message_id, edit_version, source_datetime,
                     source_text_sha256, parser_version, classification, market_type, symbol,
                     quote_asset, side, entry_price, entry_low_price, entry_high_price, stop_loss,
                     targets_json, order_type, referenced_message_id, parent_candidate_id,
                     relationship, parse_status, field_confidence_json, ambiguities_json,
                     idempotency_key, status, review_status, created_at, updated_at,
                     logical_id, followed_logical_id, direction, exchange, symbol_raw,
                     timeframe, full_target, deposit_pct, suggested_leverage, leverage_pct,
                     follow_up_event, target_index, profit_pct, event_price,
                     source_date_text, source_time_text, source_timestamp_iso)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, 'current', 'unreviewed', ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    candidate_id, channel_id, message_id, edit_version, source_datetime,
                    resolved.text_sha256, parser_version, str(resolved.classification),
                    str(resolved.market_type), resolved.symbol, resolved.quote_asset,
                    str(resolved.side) if resolved.side else None,
                    decimal_text(resolved.entry_price), decimal_text(resolved.entry_low_price),
                    decimal_text(resolved.entry_high_price), decimal_text(resolved.stop_loss),
                    json.dumps([t.as_json() for t in resolved.targets], sort_keys=True),
                    resolved.order_type, resolved.referenced_message_id, parent_id,
                    str(relationship), str(resolved.parse_status),
                    json.dumps(resolved.field_confidence, sort_keys=True),
                    json.dumps(list(resolved.ambiguities), ensure_ascii=False),
                    idempotency_key, now, now,
                    resolved.logical_id, resolved.followed_logical_id,
                    str(resolved.direction) if resolved.direction else None,
                    resolved.exchange, resolved.symbol_raw, resolved.timeframe,
                    decimal_text(resolved.full_target), decimal_text(resolved.deposit_pct),
                    decimal_text(resolved.suggested_leverage),
                    decimal_text(resolved.leverage_pct),
                    str(resolved.follow_up_event) if resolved.follow_up_event else None,
                    resolved.target_index, decimal_text(resolved.profit_pct),
                    decimal_text(resolved.event_price),
                    resolved.source_date_text, resolved.source_time_text,
                    resolved.source_timestamp_iso,
                ),
            )
            inserted = cur.rowcount > 0
        return candidate_id, inserted, superseded, relationship, parent_id

    def resolve_relationship(
        self,
        parsed: ParsedCandidate,
        *,
        channel_id: int,
        message_id: int,
        reply_to_message_id: int | None,
    ) -> tuple[ParsedCandidate, Relationship, str | None]:
        ambiguities = list(parsed.ambiguities)
        confidence = dict(parsed.field_confidence)
        referenced = parsed.referenced_message_id
        parent: dict[str, Any] | None = None
        relationship = Relationship.NONE

        if referenced is not None:
            parent = self._current_candidate_for_message(channel_id, referenced)
            if parent is not None:
                relationship = (
                    Relationship.REPLY if reply_to_message_id is not None else Relationship.EXPLICIT_REFERENCE
                )
                confidence["relationship"] = "high"
            elif parsed.classification in _UPDATE_CLASSIFICATIONS:
                relationship = Relationship.AMBIGUOUS
                ambiguities.append("referenced_message_has_no_candidate")
                confidence["relationship"] = "low"
            elif reply_to_message_id is not None:
                relationship = Relationship.REPLY
                confidence["relationship"] = "medium"

        if parsed.classification in _UPDATE_CLASSIFICATIONS and parent is None and referenced is None:
            symbol = parsed.symbol
            if symbol:
                active = self._active_candidates_for_symbol(
                    channel_id, symbol, before_message_id=message_id
                )
                if len(active) == 1:
                    parent = active[0]
                    relationship = Relationship.UNIQUE_SYMBOL
                    confidence["relationship"] = "medium"
                elif len(active) > 1:
                    relationship = Relationship.AMBIGUOUS
                    ambiguities.append("multiple_active_signals_for_symbol")
                    confidence["relationship"] = "low"
                else:
                    relationship = Relationship.AMBIGUOUS
                    ambiguities.append("no_active_signal_for_symbol")
                    confidence["relationship"] = "low"
            elif parsed.classification == SignalClassification.CANCELLATION and _ALL_RE.search(
                parsed.normalized_text
            ):
                relationship = Relationship.ALL_SYMBOLS
                confidence["relationship"] = "high"
            else:
                relationship = Relationship.AMBIGUOUS
                ambiguities.append("unresolved_parent_signal")
                confidence["relationship"] = "low"

        if (
            parent is not None
            and parsed.symbol
            and parent.get("symbol")
            and parsed.symbol != parent["symbol"]
        ):
            ambiguities.append("parent_symbol_mismatch")
            confidence["relationship"] = "low"

        status = ParseStatus.NEEDS_REVIEW if ambiguities else parsed.parse_status
        resolved = replace(
            parsed,
            parse_status=status,
            ambiguities=tuple(dict.fromkeys(ambiguities)),
            field_confidence=confidence,
        )
        return resolved, relationship, parent["candidate_id"] if parent else None

    def _current_candidate_for_message(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM signal_candidates WHERE channel_id = ? AND message_id = ? "
            "AND status = 'current' ORDER BY parser_version DESC, edit_version DESC LIMIT 1",
            (channel_id, message_id),
        ).fetchone()
        return _candidate_row(row) if row else None

    def _active_candidates_for_symbol(
        self, channel_id: int, symbol: str, *, before_message_id: int
    ) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM signal_candidates WHERE channel_id = ? AND symbol = ? "
            "AND status = 'current' AND review_status <> 'rejected' "
            "AND classification IN ('new_signal', 'modification') "
            "AND message_id < ? ORDER BY message_id DESC LIMIT 3",
            (channel_id, symbol.upper(), before_message_id),
        ).fetchall()
        return [_candidate_row(row) for row in rows]

    def mark_source_deleted(self, channel_id: int, message_id: int) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE signal_candidates SET status = 'source_deleted', updated_at = ? "
                "WHERE channel_id = ? AND message_id = ? AND status = 'current'",
                (self._now(), channel_id, message_id),
            )
            return cur.rowcount

    # ------------------------------------------------------------ failures

    def record_failure(
        self,
        *,
        channel_id: int,
        message_id: int,
        edit_version: int,
        parser_version: int,
        error_type: str,
        error_message: str,
    ) -> None:
        now = self._now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO analysis_failures
                    (channel_id, message_id, edit_version, parser_version, error_type,
                     error_message, attempts, first_failed_at, last_failed_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, NULL)
                ON CONFLICT(channel_id, message_id, edit_version, parser_version) DO UPDATE SET
                    attempts = analysis_failures.attempts + 1,
                    error_type = excluded.error_type,
                    error_message = excluded.error_message,
                    last_failed_at = excluded.last_failed_at,
                    resolved_at = NULL
                """,
                (
                    channel_id, message_id, edit_version, parser_version,
                    error_type[:120], error_message[:500], now, now,
                ),
            )

    def resolve_failure(
        self, *, channel_id: int, message_id: int, edit_version: int, parser_version: int
    ) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE analysis_failures SET resolved_at = ? "
                "WHERE channel_id = ? AND message_id = ? AND edit_version = ? AND parser_version = ? "
                "AND resolved_at IS NULL",
                (self._now(), channel_id, message_id, edit_version, parser_version),
            )

    def list_failures(self, *, unresolved_only: bool = True, limit: int = 50) -> list[dict[str, Any]]:
        where = "WHERE resolved_at IS NULL" if unresolved_only else ""
        rows = self.db.conn.execute(
            f"SELECT * FROM analysis_failures {where} ORDER BY last_failed_at DESC, failure_id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def unresolved_failure_count(self) -> int:
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS c FROM analysis_failures WHERE resolved_at IS NULL"
        ).fetchone()
        return int(row["c"])

    # ------------------------------------------------------------- queries

    def get_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM signal_candidates WHERE candidate_id = ?", (candidate_id,)
        ).fetchone()
        return _candidate_row(row) if row else None

    def list_candidates(
        self,
        *,
        classification: str | None = None,
        parse_status: str | None = None,
        review_status: str | None = None,
        include_superseded: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        where = ["1=1"]
        params: list[Any] = []
        if classification:
            where.append("classification = ?")
            params.append(classification)
        if parse_status:
            where.append("parse_status = ?")
            params.append(parse_status)
        if review_status:
            where.append("review_status = ?")
            params.append(review_status)
        if not include_superseded:
            where.append("status = 'current'")
        params.append(limit)
        rows = self.db.conn.execute(
            f"SELECT * FROM signal_candidates WHERE {' AND '.join(where)} "
            "ORDER BY source_datetime DESC, message_id DESC, edit_version DESC LIMIT ?",
            params,
        ).fetchall()
        return [_candidate_row(row) for row in rows]

    def list_candidates_in_range(
        self,
        channel_id: int,
        min_message_id: int,
        max_message_id: int,
        *,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Current candidates for one bounded validation import, oldest first."""

        rows = self.db.conn.execute(
            "SELECT * FROM signal_candidates "
            "WHERE channel_id = ? AND message_id BETWEEN ? AND ? AND status = 'current' "
            "ORDER BY message_id ASC, edit_version DESC, parser_version DESC LIMIT ?",
            (
                int(channel_id),
                int(min_message_id),
                int(max_message_id),
                max(1, int(limit)),
            ),
        ).fetchall()
        return [_candidate_row(row) for row in rows]

    def count_candidates_in_range(
        self,
        channel_id: int,
        min_message_id: int,
        max_message_id: int,
        *,
        reviewed_only: bool = False,
    ) -> int:
        where = "AND review_status <> 'unreviewed'" if reviewed_only else ""
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM signal_candidates "
            "WHERE channel_id = ? AND message_id BETWEEN ? AND ? "
            f"AND status = 'current' {where}",
            (int(channel_id), int(min_message_id), int(max_message_id)),
        ).fetchone()
        return int(row["n"] or 0)

    def summary(self) -> dict[str, Any]:
        def counts(sql: str) -> dict[str, int]:
            return {str(row[0]): int(row[1]) for row in self.db.conn.execute(sql)}

        status = counts("SELECT status, COUNT(*) FROM signal_candidates GROUP BY status")
        classification = counts(
            "SELECT classification, COUNT(*) FROM signal_candidates "
            "WHERE status = 'current' GROUP BY classification"
        )
        parse = counts(
            "SELECT parse_status, COUNT(*) FROM signal_candidates "
            "WHERE status = 'current' GROUP BY parse_status"
        )
        review = counts(
            "SELECT review_status, COUNT(*) FROM signal_candidates "
            "WHERE status = 'current' GROUP BY review_status"
        )
        total = sum(status.values())
        return {
            "total": total,
            "status": status,
            "classification": classification,
            "parse_status": parse,
            "review_status": review,
            "unresolved_failures": self.unresolved_failure_count(),
            "latest_run": self.latest_run(),
        }

    # ------------------------------------------------------------- review

    def add_review(
        self,
        candidate_id: str,
        *,
        reviewer: str,
        decision: str,
        corrected: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        if decision not in ("confirm", "correct", "reject"):
            raise AnalysisError("review decision must be confirm, correct or reject")
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            raise KeyError(f"candidate {candidate_id} not found")
        if decision == "correct" and not corrected:
            raise AnalysisError("a correction must include at least one corrected field")
        reviewed_status = {
            "confirm": ReviewStatus.CONFIRMED,
            "correct": ReviewStatus.CORRECTED,
            "reject": ReviewStatus.REJECTED,
        }[decision]
        now = self._now()
        payload = json.dumps(corrected or {}, sort_keys=True, ensure_ascii=False) if corrected else None
        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT INTO signal_candidate_reviews "
                "(candidate_id, reviewer, decision, corrected_json, note, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (candidate_id, reviewer[:120], decision, payload, (note or "")[:2000] or None, now),
            )
            review_id = cur.lastrowid
            conn.execute(
                "UPDATE signal_candidates SET review_status = ?, reviewed_at = ?, updated_at = ? "
                "WHERE candidate_id = ?",
                (str(reviewed_status), now, now, candidate_id),
            )
        review = self.db.conn.execute(
            "SELECT * FROM signal_candidate_reviews WHERE review_id = ?", (review_id,)
        ).fetchone()
        return _review_row(review) if review else {}

    def list_reviews(self, candidate_id: str) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM signal_candidate_reviews WHERE candidate_id = ? ORDER BY review_id",
            (candidate_id,),
        ).fetchall()
        return [_review_row(row) for row in rows]


def validate_correction(values: dict[str, Any]) -> dict[str, Any]:
    """Validate and canonicalize a human correction without inventing fields."""

    out: dict[str, Any] = {}
    allowed = {
        "classification", "market_type", "symbol", "quote_asset", "side",
        "entry_price", "entry_low_price", "entry_high_price", "stop_loss", "targets",
    }
    unknown = set(values) - allowed
    if unknown:
        raise AnalysisError("unknown correction field(s): " + ", ".join(sorted(unknown)))
    if "classification" in values:
        try:
            out["classification"] = str(SignalClassification(str(values["classification"])))
        except ValueError as exc:
            raise AnalysisError("invalid classification") from exc
    if "market_type" in values:
        try:
            out["market_type"] = str(MarketType(str(values["market_type"])))
        except ValueError as exc:
            raise AnalysisError("market_type must be spot, non_spot or unknown") from exc
    if "side" in values:
        side = str(values["side"]).lower()
        if side not in ("buy", "sell"):
            raise AnalysisError("side must be buy or sell")
        out["side"] = side
    for field in ("entry_price", "entry_low_price", "entry_high_price", "stop_loss"):
        if field not in values or values[field] in (None, ""):
            continue
        try:
            value = Decimal(str(values[field]))
        except (InvalidOperation, ValueError) as exc:
            raise AnalysisError(f"{field} must be a decimal number") from exc
        if not value.is_finite() or value <= 0:
            raise AnalysisError(f"{field} must be a positive finite decimal")
        out[field] = format(value, "f")
    for field in ("symbol", "quote_asset"):
        if field in values and values[field] not in (None, ""):
            out[field] = str(values[field]).upper()
    if "targets" in values:
        raw_targets = values["targets"]
        if isinstance(raw_targets, str):
            raw_targets = [p for p in raw_targets.split(",") if p.strip()]
        if not isinstance(raw_targets, list):
            raise AnalysisError("targets must be a list or comma-separated string")
        clean: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_targets, start=1):
            if isinstance(raw, dict):
                price_raw = raw.get("price")
                target_index = raw.get("index", index)
            else:
                price_raw = raw
                target_index = index
            try:
                price = Decimal(str(price_raw))
            except (InvalidOperation, ValueError) as exc:
                raise AnalysisError("every target must be a decimal number") from exc
            if not price.is_finite() or price <= 0:
                raise AnalysisError("every target must be a positive finite decimal")
            clean.append({"index": target_index, "price": format(price, "f")})
        out["targets"] = clean
    return out


def _candidate_row(row: Any) -> dict[str, Any]:
    data = dict(row)
    for source, target, default in (
        ("targets_json", "targets", []),
        ("field_confidence_json", "field_confidence", {}),
        ("ambiguities_json", "ambiguities", []),
    ):
        raw = data.get(source)
        try:
            data[target] = json.loads(raw) if raw else default
        except (TypeError, ValueError):
            data[target] = default
    return data


def _review_row(row: Any) -> dict[str, Any]:
    data = dict(row)
    raw = data.get("corrected_json")
    try:
        data["corrected"] = json.loads(raw) if raw else None
    except (TypeError, ValueError):
        data["corrected"] = None
    return data


def _decode_run(row: Any) -> dict[str, Any]:
    data = dict(row)
    for source, target in (("scope_json", "scope"), ("counts_json", "counts")):
        raw = data.get(source)
        try:
            data[target] = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            data[target] = {}
    return data


def empty_scope() -> dict[str, Any]:
    return {}


def new_candidate_status() -> str:
    return str(CandidateStatus.CURRENT)
