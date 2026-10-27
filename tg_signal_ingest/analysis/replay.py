"""Deterministic, restartable offline replay over persisted messages.

The replay consumes only rows already present in ``messages``. It never talks
to Telegram, the exchange, or the network. One malformed row is recorded in
``analysis_failures`` and does not stop the run; a later replay retries it and
resolves the failure row on success.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from ..repository import Repository
from .parser import ANALYSIS_PARSER_VERSION, SignalAnalysisParser
from .store import AnalysisStore

log = logging.getLogger("ingest.analysis.replay")


@dataclass(frozen=True)
class ReplayScope:
    channel_id: int | None = None
    since: str | None = None
    until: str | None = None
    limit: int | None = None
    after_message_id: int | None = None
    message_id_min: int | None = None
    message_id_max: int | None = None
    include_deleted: bool = True
    dry_run: bool = False

    def as_json(self) -> dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "since": self.since,
            "until": self.until,
            "limit": self.limit,
            "after_message_id": self.after_message_id,
            "message_id_min": self.message_id_min,
            "message_id_max": self.message_id_max,
            "include_deleted": self.include_deleted,
            "dry_run": self.dry_run,
        }


@dataclass
class ReplayStats:
    scanned: int = 0
    inserted: int = 0
    duplicates: int = 0
    superseded: int = 0
    needs_review: int = 0
    failures: int = 0
    deleted_marked: int = 0
    by_classification: dict[str, int] = field(default_factory=dict)
    run_id: str | None = None
    last_message_id: int | None = None
    dry_run: bool = False

    def as_json(self) -> dict[str, Any]:
        return {
            "scanned": self.scanned,
            "inserted": self.inserted,
            "duplicates": self.duplicates,
            "superseded": self.superseded,
            "needs_review": self.needs_review,
            "failures": self.failures,
            "deleted_marked": self.deleted_marked,
            "by_classification": dict(sorted(self.by_classification.items())),
            "last_message_id": self.last_message_id,
            "dry_run": self.dry_run,
        }


def _select_messages(repo: Repository, scope: ReplayScope) -> list[Any]:
    where = ["1=1"]
    params: list[Any] = []
    if scope.channel_id is not None:
        where.append("channel_id = ?")
        params.append(scope.channel_id)
    if scope.since:
        where.append("datetime >= ?")
        params.append(scope.since)
    if scope.until:
        where.append("datetime <= ?")
        params.append(scope.until)
    if scope.after_message_id is not None:
        where.append("message_id > ?")
        params.append(scope.after_message_id)
    if scope.message_id_min is not None:
        where.append("message_id >= ?")
        params.append(scope.message_id_min)
    if scope.message_id_max is not None:
        where.append("message_id <= ?")
        params.append(scope.message_id_max)
    if not scope.include_deleted:
        where.append("deleted = 0")
    sql = (
        "SELECT channel_id, message_id, datetime, edit_datetime, text, caption, media_type, "
        "processing_version, deleted, reply_to_message_id, fwd_from_id, fwd_from_name, "
        "fwd_post_message_id, source FROM messages "
        f"WHERE {' AND '.join(where)} ORDER BY datetime ASC, message_id ASC"
    )
    if scope.limit is not None:
        sql += " LIMIT ?"
        params.append(max(0, scope.limit))
    return list(repo.db.conn.execute(sql, params).fetchall())


def run_replay(
    repo: Repository,
    scope: ReplayScope,
    *,
    parser: SignalAnalysisParser | None = None,
    store: AnalysisStore | None = None,
) -> ReplayStats:
    parser = parser or SignalAnalysisParser()
    store = store or AnalysisStore(repo.db)
    stats = ReplayStats(dry_run=scope.dry_run)
    if not scope.dry_run:
        stats.run_id = store.start_run(parser_version=parser.version, scope=scope.as_json())

    try:
        rows = _select_messages(repo, scope)
        for row in rows:
            stats.scanned += 1
            message_id = int(row["message_id"])
            stats.last_message_id = message_id
            channel_id = int(row["channel_id"])
            edit_version = int(row["processing_version"] or 0)

            if int(row["deleted"] or 0):
                if not scope.dry_run:
                    stats.deleted_marked += store.mark_source_deleted(channel_id, message_id)
                continue

            text = row["text"] or row["caption"]
            forwarded = bool(
                row["fwd_from_id"]
                or row["fwd_from_name"]
                or row["fwd_post_message_id"] is not None
            )
            is_edit = bool(row["edit_datetime"]) or edit_version > 0
            media_only = bool(row["media_type"]) and not text
            try:
                parsed = parser.parse(
                    text,
                    is_edit=is_edit,
                    forwarded=forwarded,
                    media_only=media_only,
                    reply_to_message_id=row["reply_to_message_id"],
                )
                resolved, _relationship, _parent = store.resolve_relationship(
                    parsed,
                    channel_id=channel_id,
                    message_id=message_id,
                    reply_to_message_id=row["reply_to_message_id"],
                )
                if not scope.dry_run:
                    _candidate_id, inserted, superseded, _rel, _parent_id = store.record_candidate(
                        resolved,
                        channel_id=channel_id,
                        message_id=message_id,
                        edit_version=edit_version,
                        source_datetime=str(row["datetime"]),
                        reply_to_message_id=row["reply_to_message_id"],
                        parser_version=parser.version,
                    )
                    stats.inserted += int(inserted)
                    stats.duplicates += int(not inserted)
                    stats.superseded += superseded
                    store.resolve_failure(
                        channel_id=channel_id,
                        message_id=message_id,
                        edit_version=edit_version,
                        parser_version=parser.version,
                    )
                if resolved.parse_status.value == "needs_review":
                    stats.needs_review += 1
                key = str(resolved.classification)
                stats.by_classification[key] = stats.by_classification.get(key, 0) + 1
            except Exception as exc:  # noqa: BLE001 - failure isolation is deliberate
                stats.failures += 1
                error_type = exc.__class__.__name__
                if not scope.dry_run:
                    store.record_failure(
                        channel_id=channel_id,
                        message_id=message_id,
                        edit_version=edit_version,
                        parser_version=parser.version,
                        error_type=error_type,
                        error_message=f"{error_type}: processing failed",
                    )
                log.warning(
                    "analysis failed for channel=%s message=%s version=%s type=%s",
                    channel_id, message_id, edit_version, error_type,
                )
        if not scope.dry_run and stats.run_id:
            store.finish_run(stats.run_id, status="completed", counts=stats.as_json())
        return stats
    except Exception as exc:  # noqa: BLE001 - record a fatal run failure, then re-raise
        if not scope.dry_run and stats.run_id:
            store.finish_run(
                stats.run_id,
                status="failed",
                counts=stats.as_json(),
                error=f"{exc.__class__.__name__}: {str(exc)[:300]}",
            )
        raise


def parser_version() -> int:
    return ANALYSIS_PARSER_VERSION
