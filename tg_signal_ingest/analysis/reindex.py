"""Historical signal-index builder (offline, deterministic, idempotent).

Two phases run from already-stored ``messages``:

1. *Candidates phase* — classifies every persisted message with the current
   parser (version 2: structured format + v1 fallback). On apply this uses
   the shared ``AnalysisStore`` machinery, so candidate rows stay immutable
   per (message, edit, parser version) and parse failures are isolated and
   counted; on dry-run classification happens in memory only, with no
   writes at all.

2. *Index phase* — builds the normalized logical-signal index:

   * ``channel_signals`` — one row per (channel, logical signal ID), holding
     the extracted fields of the initial-signal message;
   * ``signal_message_links`` — exactly one association row per message:
     ``linked``, ``unresolved``, ``conflict`` or ``no_reference``.

Association rules (exercised by tests):

* explicit logical ID alone resolves when the message is an initial signal
  of that ID, or the ID already exists in the index;
* a reply to a known signal-origin message alone resolves;
* both present and agreeing -> linked with method ``both``;
* both present and disagreeing -> ``conflict``; never auto-attached;
* otherwise the row records ``unresolved`` (reference not found *yet*) or
  ``no_reference`` (message carries neither identifier).

The rules tolerate out-of-order ingestion: a follow-up may be stored
before (or without) the original signal. The planned index keys are
collected before association, so a stored follow-up resolves in the same
run when the original exists later in the message history; when the
original is genuinely missing, the row stays ``unresolved`` with the
recorded reason (reference not found).

The index never writes to ``messages`` (raw text is never rewritten), never
talks to Telegram or an exchange, and never touches trading tables.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from .parser import SignalAnalysisParser
from .store import AnalysisStore

CLASSIFICATION_FA = {
    "new_signal": "سیگنال جدید",
    "modification": "اصلاح سیگنال",
    "cancellation": "لغو سیگنال",
    "target_update": "به‌روزرسانی هدف",
    "stop_loss_update": "به‌روزرسانی حد ضرر",
    "result": "پیگیری/نتیجه",
    "non_signal": "غیرسیگنال",
    "ambiguous": "مبهم",
}


@dataclass
class ReindexStats:
    """Deterministic counters; also the machine-readable audit summary."""

    messages_scanned: int = 0
    parse_failures: int = 0
    candidates_created: int = 0
    initial_signals: int = 0
    long_signals: int = 0
    short_signals: int = 0
    follow_ups: int = 0
    ambiguous: int = 0
    unrelated: int = 0
    needs_review: int = 0
    signals_planned: int = 0
    duplicate_signal_ids: int = 0
    links_linked: int = 0
    links_unresolved: int = 0
    links_conflict: int = 0
    links_no_reference: int = 0
    signals_created: int = 0
    signals_updated: int = 0
    signals_unchanged: int = 0
    links_created: int = 0
    links_updated: int = 0
    dry_run: bool = True

    def as_json(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class SignalPlan:
    """One planned ``channel_signals`` row."""

    channel_id: int
    logical_id: int
    fields: dict[str, Any]
    origin_message_id: int
    origin_parser_version: int
    duplicate_origin_message_ids: tuple[int, ...] = ()


@dataclass
class IndexPlan:
    signals: dict[tuple[int, int], SignalPlan] = field(default_factory=dict)
    links: list[dict[str, Any]] = field(default_factory=list)

    @property
    def conflicts(self) -> list[dict[str, Any]]:
        return [row for row in self.links if row["status"] == "conflict"]


def _decimal_text(value: Any) -> str | None:
    """Canonical decimal text for a Decimal, or for the string a persisted
    candidate row carries; anything unparseable is returned unchanged."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = Decimal(value)
        except InvalidOperation:
            return value
    return format(value, "f")


def _candidate_row_shaped(
    parsed: Any, *, message_id: int, edit_version: int, parser_version: int
) -> dict[str, Any]:
    """Shape an in-memory parsed candidate exactly like a persisted row."""

    return {
        "message_id": message_id,
        "edit_version": edit_version,
        "parser_version": parser_version,
        "candidate_id": None,
        "classification": str(parsed.classification),
        "market_type": str(parsed.market_type),
        "symbol": parsed.symbol,
        "quote_asset": parsed.quote_asset,
        "side": str(parsed.side) if parsed.side else None,
        "entry_price": parsed.entry_price,
        "entry_low_price": parsed.entry_low_price,
        "entry_high_price": parsed.entry_high_price,
        "stop_loss": parsed.stop_loss,
        "targets": [t.as_json() for t in parsed.targets],
        "parse_status": str(parsed.parse_status),
        "ambiguities": list(parsed.ambiguities),
        "logical_id": parsed.logical_id,
        "followed_logical_id": parsed.followed_logical_id,
        "direction": str(parsed.direction) if parsed.direction else None,
        "exchange": parsed.exchange,
        "symbol_raw": parsed.symbol_raw,
        "timeframe": parsed.timeframe,
        "full_target": parsed.full_target,
        "deposit_pct": parsed.deposit_pct,
        "suggested_leverage": parsed.suggested_leverage,
        "leverage_pct": parsed.leverage_pct,
        "follow_up_event": str(parsed.follow_up_event) if parsed.follow_up_event else None,
        "target_index": parsed.target_index,
        "profit_pct": parsed.profit_pct,
        "event_price": parsed.event_price,
        "source_date_text": parsed.source_date_text,
        "source_time_text": parsed.source_time_text,
        "source_timestamp_iso": parsed.source_timestamp_iso,
    }


def build_plan(
    *,
    channel_id: int,
    messages: list[dict[str, Any]],
    candidates_by_message: dict[int, dict[str, Any] | None],
    stats: ReindexStats | None = None,
) -> tuple[IndexPlan, ReindexStats]:
    """Build the index plan from messages plus their per-message candidate.

    Every message gets exactly one link row; observation counters are
    deterministic (messages ascending by Telegram message ID).
    """

    stats = stats or ReindexStats()
    plan = IndexPlan()

    # Logical signal IDs whose index rows the plan will contain, computed
    # before the iteration so association does not depend on message order
    # (a follow-up may be ingested before its initial signal).
    planned_keys = {
        (channel_id, int(candidate["logical_id"]))
        for candidate in candidates_by_message.values()
        if candidate is not None
        and candidate.get("logical_id") is not None
        and str(candidate["classification"]) == "new_signal"
        and str(candidate["parse_status"]) == "parsed"
    }

    for message in sorted(messages, key=lambda m: int(m["message_id"])):
        stats.messages_scanned += 1
        message_id = int(message["message_id"])
        candidate = candidates_by_message.get(message_id)
        if candidate is None:
            stats.parse_failures += 1
            plan.links.append(
                {
                    "channel_id": channel_id,
                    "message_id": message_id,
                    "edit_version": 0,
                    "candidate_id": None,
                    "logical_id": None,
                    "reply_to_message_id": message.get("reply_to_message_id"),
                    "method": "none",
                    "status": "no_reference",
                    "conflict_with_logical_id": None,
                    "reason": "no_candidate",
                    "classification": None,
                }
            )
            continue

        classification = str(candidate["classification"])
        if classification == "new_signal":
            stats.initial_signals += 1
        elif classification in ("result", "modification", "cancellation",
                                "target_update", "stop_loss_update"):
            stats.follow_ups += 1
        elif classification == "ambiguous":
            stats.ambiguous += 1
        else:
            stats.unrelated += 1
        if str(candidate["parse_status"]) == "needs_review":
            stats.needs_review += 1
        direction = str(candidate.get("direction") or "")
        # long/short counters measure structured messages carrying a
        # direction token, regardless of classification (diagnostic evidence).
        if direction == "long":
            stats.long_signals += 1
        elif direction == "short":
            stats.short_signals += 1

        logical_id = candidate.get("logical_id")
        if (
            classification == "new_signal"
            and logical_id is not None
            and str(candidate["parse_status"]) == "parsed"
        ):
            key = (channel_id, int(logical_id))
            existing = plan.signals.get(key)
            if existing is None:
                plan.signals[key] = SignalPlan(
                    channel_id=channel_id,
                    logical_id=int(logical_id),
                    fields={
                        "direction": candidate.get("direction"),
                        "exchange": candidate.get("exchange"),
                        "symbol_raw": candidate.get("symbol_raw"),
                        "symbol": candidate.get("symbol"),
                        "quote_asset": candidate.get("quote_asset"),
                        "timeframe": candidate.get("timeframe"),
                        "entry_price": candidate.get("entry_price"),
                        "stop_loss": candidate.get("stop_loss"),
                        "targets_json": json.dumps(
                            candidate.get("targets") or [], sort_keys=True
                        ),
                        "full_target": candidate.get("full_target"),
                        "deposit_pct": candidate.get("deposit_pct"),
                        "suggested_leverage": candidate.get("suggested_leverage"),
                        "leverage_pct": candidate.get("leverage_pct"),
                        "source_date_text": candidate.get("source_date_text"),
                        "source_time_text": candidate.get("source_time_text"),
                        "source_timestamp_iso": candidate.get("source_timestamp_iso"),
                        "classification": classification,
                        "parse_status": candidate.get("parse_status"),
                        "ambiguities_json": json.dumps(
                            candidate.get("ambiguities") or [], ensure_ascii=False
                        ),
                    },
                    origin_message_id=message_id,
                    origin_parser_version=int(candidate.get("parser_version") or 0),
                )
            else:
                # Two parsed initial messages claim the same logical signal
                # ID: the earliest message wins deterministically; every
                # later one is recorded (never silently applied).
                stats.duplicate_signal_ids += 1
                if message_id < existing.origin_message_id:
                    existing.duplicate_origin_message_ids = tuple(
                        sorted([existing.origin_message_id, *existing.duplicate_origin_message_ids])
                    )
                    existing.origin_message_id = message_id
                    existing.origin_parser_version = int(
                        candidate.get("parser_version") or 0
                    )
                    existing.fields = {
                        "direction": candidate.get("direction"),
                        "exchange": candidate.get("exchange"),
                        "symbol_raw": candidate.get("symbol_raw"),
                        "symbol": candidate.get("symbol"),
                        "quote_asset": candidate.get("quote_asset"),
                        "timeframe": candidate.get("timeframe"),
                        "entry_price": candidate.get("entry_price"),
                        "stop_loss": candidate.get("stop_loss"),
                        "targets_json": json.dumps(
                            candidate.get("targets") or [], sort_keys=True
                        ),
                        "full_target": candidate.get("full_target"),
                        "deposit_pct": candidate.get("deposit_pct"),
                        "suggested_leverage": candidate.get("suggested_leverage"),
                        "leverage_pct": candidate.get("leverage_pct"),
                        "source_date_text": candidate.get("source_date_text"),
                        "source_time_text": candidate.get("source_time_text"),
                        "source_timestamp_iso": candidate.get("source_timestamp_iso"),
                        "classification": classification,
                        "parse_status": candidate.get("parse_status"),
                        "ambiguities_json": json.dumps(
                            candidate.get("ambiguities") or [], ensure_ascii=False
                        ),
                    }
                else:
                    existing.duplicate_origin_message_ids = tuple(
                        sorted([*existing.duplicate_origin_message_ids, message_id])
                    )

        # ------------------------------------------------- association rules
        reply_to = message.get("reply_to_message_id")
        reply_signal_logical_id = message.get("reply_resolved_logical_id")
        explicit = candidate.get("logical_id")
        resolved_logical: int | None = None
        conflict_with: int | None = None
        reason: str | None = None
        if explicit is not None and reply_signal_logical_id is not None:
            if int(explicit) == int(reply_signal_logical_id):
                method: str | None = "both"
                status = "linked"
                resolved_logical = int(explicit)
            else:
                method = "both"
                status = "conflict"
                conflict_with = int(reply_signal_logical_id)
                reason = "explicit_id_and_reply_identify_different_signals"
        elif explicit is not None:
            conflict_with = None
            method = "explicit_id"
            if (
                classification == "new_signal"
                and str(candidate.get("parse_status")) == "parsed"
            ) or (channel_id, int(explicit)) in planned_keys:
                status = "linked"
                resolved_logical = int(explicit)
                if classification != "new_signal":
                    reason = None
            else:
                status = "unresolved"
                resolved_logical = int(explicit)
                reason = "logical_signal_id_not_in_index"
        elif reply_signal_logical_id is not None and (
            channel_id, int(reply_signal_logical_id)
        ) in planned_keys:
            method = "reply"
            status = "linked"
            resolved_logical = int(reply_signal_logical_id)
        elif reply_to is not None:
            method = "reply"
            status = "unresolved"
            conflict_with = None
            reason = (
                "reply_target_is_not_a_known_signal_message"
                if reply_signal_logical_id is None
                else "logical_signal_id_not_in_index"
            )
        else:
            method = "none"
            status = "no_reference"
            reason = (
                "structured_message_without_reference"
                if candidate.get("symbol_raw") or candidate.get("logical_id")
                else None
            )

        plan.links.append(
            {
                "channel_id": channel_id,
                "message_id": message_id,
                "edit_version": int(candidate.get("edit_version") or 0),
                "candidate_id": candidate.get("candidate_id"),
                "logical_id": resolved_logical,
                "reply_to_message_id": int(reply_to) if reply_to is not None else None,
                "method": method,
                "status": status,
                "conflict_with_logical_id": conflict_with,
                "reason": reason,
                "classification": classification,
            }
        )

    stats.signals_planned = len(plan.signals)
    for link in plan.links:
        status = link["status"]
        if status == "linked":
            stats.links_linked += 1
        elif status == "unresolved":
            stats.links_unresolved += 1
        elif status == "conflict":
            stats.links_conflict += 1
        else:
            stats.links_no_reference += 1
    return plan, stats


def _load_messages(db: Any, channel_id: int) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        "SELECT message_id, datetime, edit_datetime, text, caption, "
        "reply_to_message_id, deleted FROM messages "
        "WHERE channel_id = ? ORDER BY message_id ASC",
        (channel_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _candidates_by_message(db: Any, channel_id: int) -> dict[int, dict[str, Any] | None]:
    """Current candidate per message, preferring the highest parser version."""

    rows = db.conn.execute(
        "SELECT * FROM signal_candidates "
        "WHERE channel_id = ? AND status = 'current' "
        "ORDER BY message_id ASC, parser_version DESC, edit_version DESC",
        (channel_id,),
    ).fetchall()
    mapping: dict[int, dict[str, Any] | None] = {}
    for row in rows:
        data = dict(row)
        msg = int(data["message_id"])
        if msg not in mapping:
            mapping[msg] = data
    return mapping


def _reply_mappings(candidates: dict[int, dict[str, Any] | None]) -> dict[int, int]:
    """Map signal-origin message_id -> logical signal ID."""

    mapping: dict[int, int] = {}
    for candidate in candidates.values():
        if candidate is None:
            continue
        logical = candidate.get("logical_id")
        if logical is not None and str(candidate.get("classification")) == "new_signal":
            mapping.setdefault(int(candidate["message_id"]), int(logical))
    return mapping


def _annotate_reply_resolutions(
    messages: list[dict[str, Any]], mapping: dict[int, int]
) -> None:
    for msg in messages:
        reply_to = msg.get("reply_to_message_id")
        msg["reply_resolved_logical_id"] = (
            mapping.get(int(reply_to)) if reply_to is not None else None
        )


def rebuild_signal_index(
    db: Any,
    *,
    channel_id: int,
    dry_run: bool,
    parser: SignalAnalysisParser | None = None,
    store: AnalysisStore | None = None,
) -> ReindexStats:
    """Rebuild the channel's logical-signal index idempotently.

    ``dry_run=True`` guarantees no writes anywhere. ``dry_run=False`` first
    records parser-v2 candidates for every message lacking one (shared,
    immutable candidate machinery), then applies the plan to
    ``channel_signals`` and ``signal_message_links`` inside one transaction.
    """

    parser = parser or SignalAnalysisParser()
    store = store or AnalysisStore(db)
    dry_run = bool(dry_run)

    messages = _load_messages(db, channel_id)
    # messages_scanned is counted by build_plan; don't pre-set it or the
    # number would be doubled in the reused stats object.
    stats = ReindexStats(dry_run=dry_run)

    if dry_run:
        candidates: dict[int, dict[str, Any] | None] = {}
        for msg in messages:
            parsed = parser.parse(msg.get("text") or msg.get("caption"))
            candidates[int(msg["message_id"])] = _candidate_row_shaped(
                parsed,
                message_id=int(msg["message_id"]),
                edit_version=0,
                parser_version=parser.version,
            )
    else:
        stored = _candidates_by_message(db, channel_id)
        created = 0
        for msg in messages:
            message_id = int(msg["message_id"])
            current = stored.get(message_id)
            if current is None or int(current.get("parser_version") or 0) < parser.version:
                # record_candidate supersedes older parser versions and
                # isolates parse failures (failures table), so one bad
                # message never aborts the run.
                store.record_candidate(
                    parser.parse(msg.get("text") or msg.get("caption")),
                    channel_id=channel_id,
                    message_id=message_id,
                    edit_version=0,
                    source_datetime=str(msg.get("datetime") or ""),
                    reply_to_message_id=msg.get("reply_to_message_id"),
                    parser_version=parser.version,
                )
                created += 1
        # Refresh and use exactly what is persisted now (candidate IDs, and
        # None where a parse/record failure was isolated).
        candidates = _candidates_by_message(db, channel_id)
        stats.candidates_created = created

    mapping = _reply_mappings(candidates)
    _annotate_reply_resolutions(messages, mapping)
    plan, stats = build_plan(
        channel_id=channel_id,
        messages=messages,
        candidates_by_message=candidates,
        stats=stats,
    )
    stats.dry_run = dry_run

    if not dry_run:
        _apply_plan(db, plan, stats)
    return stats


def _signal_state(signal: SignalPlan) -> tuple:
    """Canonical DB-facing tuple of one planned signal row."""

    f = signal.fields
    return (
        f.get("direction"),
        f.get("exchange"),
        f.get("symbol_raw"),
        f.get("symbol"),
        f.get("quote_asset"),
        f.get("timeframe"),
        _decimal_text(f.get("entry_price")),
        _decimal_text(f.get("stop_loss")),
        f.get("targets_json"),
        _decimal_text(f.get("full_target")),
        _decimal_text(f.get("deposit_pct")),
        _decimal_text(f.get("suggested_leverage")),
        _decimal_text(f.get("leverage_pct")),
        f.get("source_date_text"),
        f.get("source_time_text"),
        f.get("source_timestamp_iso"),
        signal.origin_message_id,
        signal.origin_parser_version,
        f.get("classification"),
        f.get("parse_status"),
        f.get("ambiguities_json"),
    )


def _apply_plan(db: Any, plan: IndexPlan, stats: ReindexStats) -> None:
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    created = updated = unchanged = 0
    signal_columns = (
        "channel_id", "logical_id", "direction", "exchange", "symbol_raw",
        "symbol", "quote_asset", "timeframe", "entry_price", "stop_loss",
        "targets_json", "full_target", "deposit_pct", "suggested_leverage",
        "leverage_pct", "source_date_text", "source_time_text",
        "source_timestamp_iso", "origin_message_id", "edit_version",
        "parser_version", "classification", "parse_status", "ambiguities_json",
        "status", "execution_status", "created_at", "updated_at",
    )
    with db.transaction() as conn:
        for key, signal in sorted(plan.signals.items()):
            row = conn.execute(
                "SELECT * FROM channel_signals WHERE channel_id = ? AND logical_id = ?",
                key,
            ).fetchone()
            state = _signal_state(signal)
            if row is None:
                status = (
                    "duplicate_origin" if signal.duplicate_origin_message_ids else "active"
                )
                values = [
                    key[0], key[1],           # channel_id, logical_id
                    *state[0:16],             # direction .. source_timestamp_iso
                    state[16],                # origin_message_id
                    0,                        # edit_version
                    state[17],                # parser_version
                    state[18],                # classification
                    state[19],                # parse_status
                    state[20],                # ambiguities_json
                    status,
                    "none",                   # execution_status: never traded by this
                    now, now,
                ]
                assert len(values) == len(signal_columns)
                conn.execute(
                    f"INSERT INTO channel_signals ({', '.join(signal_columns)}) "
                    f"VALUES ({', '.join('?' for _ in signal_columns)})",
                    values,
                )
                created += 1
            else:
                existing = (
                    row["direction"], row["exchange"], row["symbol_raw"], row["symbol"],
                    row["quote_asset"], row["timeframe"], row["entry_price"],
                    row["stop_loss"], row["targets_json"], row["full_target"],
                    row["deposit_pct"], row["suggested_leverage"], row["leverage_pct"],
                    row["source_date_text"], row["source_time_text"],
                    row["source_timestamp_iso"], int(row["origin_message_id"]),
                    int(row["parser_version"] or 0), row["classification"],
                    row["parse_status"], row["ambiguities_json"] or "[]",
                )
                if existing == state:
                    unchanged += 1
                else:
                    if int(row["origin_message_id"]) == state[16]:
                        status_value = (
                            "duplicate_origin"
                            if signal.duplicate_origin_message_ids
                            else "active"
                        )
                    else:
                        # A different origin already owns this logical signal
                        # ID: the historical row is preserved and the
                        # ambiguity stays visible in its status.
                        status_value = row["status"] or "needs_review"
                    update_sets = [
                        ("direction", state[0]),
                        ("exchange", state[1]),
                        ("symbol_raw", state[2]),
                        ("symbol", state[3]),
                        ("quote_asset", state[4]),
                        ("timeframe", state[5]),
                        ("entry_price", state[6]),
                        ("stop_loss", state[7]),
                        ("targets_json", state[8]),
                        ("full_target", state[9]),
                        ("deposit_pct", state[10]),
                        ("suggested_leverage", state[11]),
                        ("leverage_pct", state[12]),
                        ("source_date_text", state[13]),
                        ("source_time_text", state[14]),
                        ("source_timestamp_iso", state[15]),
                        ("classification", state[18]),
                        ("parse_status", state[19]),
                        ("ambiguities_json", state[20]),
                        ("status", status_value),
                        ("updated_at", now),
                    ]
                    conn.execute(
                        "UPDATE channel_signals SET "
                        + ", ".join(f"{name} = ?" for name, _ in update_sets)
                        + " WHERE channel_id = ? AND logical_id = ?",
                        [*(v for _, v in update_sets), key[0], key[1]],
                    )
                    updated += 1

        for link in plan.links:
            existing = conn.execute(
                "SELECT edit_version, candidate_id, logical_id, reply_to_message_id, "
                "method, status, conflict_with_logical_id, reason, classification "
                "FROM signal_message_links WHERE channel_id = ? AND message_id = ?",
                (link["channel_id"], link["message_id"]),
            ).fetchone()
            desired = (
                int(link.get("edit_version") or 0), link.get("candidate_id"),
                link.get("logical_id"),
                int(link["reply_to_message_id"]) if link.get("reply_to_message_id") is not None else None,
                link.get("method"), link["status"],
                link.get("conflict_with_logical_id"), link.get("reason"),
                link.get("classification"),
            )
            if existing is not None and (
                int(existing["edit_version"] or 0), existing["candidate_id"],
                existing["logical_id"],
                int(existing["reply_to_message_id"])
                if existing["reply_to_message_id"] is not None else None,
                existing["method"], existing["status"],
                existing["conflict_with_logical_id"], existing["reason"],
                existing["classification"],
            ) == desired:
                continue
            if existing is None:
                conn.execute(
                    "INSERT INTO signal_message_links "
                    "(channel_id, message_id, edit_version, candidate_id, logical_id, "
                    " reply_to_message_id, method, status, conflict_with_logical_id, "
                    " reason, classification, linked_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        link["channel_id"], link["message_id"], *desired, now, now,
                    ),
                )
                stats.links_created += 1
            else:
                conn.execute(
                    "UPDATE signal_message_links SET edit_version = ?, candidate_id = ?, "
                    "logical_id = ?, reply_to_message_id = ?, method = ?, status = ?, "
                    "conflict_with_logical_id = ?, reason = ?, classification = ?, "
                    "updated_at = ? "
                    "WHERE channel_id = ? AND message_id = ?",
                    (*desired, now, link["channel_id"], link["message_id"]),
                )
                stats.links_updated += 1
    stats.signals_created = created
    stats.signals_updated = updated
    stats.signals_unchanged = unchanged


def classification_fa(value: str | None) -> str:
    return CLASSIFICATION_FA.get(str(value or ""), str(value or ""))
