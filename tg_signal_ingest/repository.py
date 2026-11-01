"""Durable message repository: storage, sync checkpoints, processing queue.

All writes are transactional. The logical message identity is
``(channel_id, message_id)`` (Telegram semantics), not any derived text.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from .db import Database
from .normalize import StoredMessage

log = logging.getLogger("ingest.repository")

UTC_NOW = lambda: datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")  # noqa: E731

MAX_QUEUE_ERROR_LEN = 400


class UpsertOutcome(StrEnum):
    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"
    DELETED_TOMBSTONE = "deleted"


@dataclass
class UpsertResult:
    outcome: UpsertOutcome
    old_version: int
    new_version: int


@dataclass
class QueueItem:
    queue_id: int
    channel_id: int
    message_id: int
    item_type: str
    status: str
    targets_version: int
    attempts: int
    available_at: str
    created_at: str
    last_error: str | None = None


@dataclass
class SyncStatus:
    channel_id: int | None
    channel_title: str | None
    backfill_status: str
    backfill_target_mode: str | None
    backfill_since: str | None
    backfill_next_max_id: int | None
    backfill_oldest_id_seen: int | None
    backfill_message_count: int
    live_last_seen_message_id: int | None
    live_last_event_at: str | None
    last_sync_at: str | None


@dataclass
class MessageCounts:
    total: int
    deleted: int
    with_media: int
    min_id: int | None
    max_id: int | None
    max_datetime: str | None


@dataclass
class ChannelInfo:
    channel_id: int
    access_hash: int
    username: str | None
    title: str
    is_selected: bool
    created_at: str = field(default="")
    updated_at: str = field(default="")


class Repository:
    def __init__(self, db: Database, processing_max_attempts: int = 8):
        self.db = db
        self.processing_max_attempts = processing_max_attempts
        self.retry_backoff_scale = 1.0  # tests set 0 for deterministic retries

    # ----------------------------------------------------------- channels

    def ensure_channel(self, channel_id: int, access_hash: int, username: str | None, title: str) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO channels (channel_id, access_hash, username, title, is_selected, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    access_hash = excluded.access_hash,
                    username = COALESCE(excluded.username, channels.username),
                    title = COALESCE(excluded.title, channels.title),
                    updated_at = excluded.updated_at
                """,
                (channel_id, access_hash, username, title, now, now),
            )

    def selected_channel(self) -> ChannelInfo | None:
        row = self.db.conn.execute(
            "SELECT * FROM channels WHERE is_selected = 1 ORDER BY selected_at DESC LIMIT 1"
        ).fetchone()
        return _channel_from_row(row) if row else None

    def list_channels(self) -> list[ChannelInfo]:
        rows = self.db.conn.execute("SELECT * FROM channels ORDER BY title").fetchall()
        return [_channel_from_row(r) for r in rows]

    def select_channel(self, channel_id: int) -> None:
        with self.db.transaction() as conn:
            existing = conn.execute("SELECT channel_id FROM channels WHERE channel_id = ?", (channel_id,)).fetchone()
            if existing is None:
                raise KeyError(f"channel {channel_id} is not registered; list channels first")
            conn.execute("UPDATE channels SET is_selected = 0, updated_at = ? WHERE is_selected = 1", (UTC_NOW(),))
            conn.execute(
                "UPDATE channels SET is_selected = 1, selected_at = ?, updated_at = ? WHERE channel_id = ?",
                (UTC_NOW(), UTC_NOW(), channel_id),
            )

    # ----------------------------------------------------------- messages

    def upsert_message(self, record: StoredMessage, *, enqueue_processing: bool = True) -> UpsertResult:
        """Insert or update one message.

        ``enqueue_processing`` is disabled by historical backfill: history
        ingestion is data-only and must never activate downstream parsing or
        trading just because a message was stored.
        """

        now = UTC_NOW()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT processing_version, edit_datetime, text, caption, media_type, media_meta, "
                "reply_to_message_id, fwd_from_name, fwd_from_id, fwd_post_message_id, datetime, "
                "sender_id, sender_name, post_author, entities_json "
                "FROM messages WHERE channel_id = ? AND message_id = ?",
                (record.channel_id, record.message_id),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO messages (channel_id, message_id, datetime, edit_datetime, text, caption,
                        media_type, media_meta, grouped_id, reply_to_message_id, fwd_from_name, fwd_from_id,
                        fwd_post_message_id, raw_json, source, processing_version, deleted, deleted_at,
                        first_seen, last_updated, sender_id, sender_name, post_author, entities_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.channel_id,
                        record.message_id,
                        record.datetime,
                        record.edit_datetime,
                        record.text,
                        record.caption,
                        record.media_type,
                        record.media_meta_json,
                        record.grouped_id,
                        record.reply_to_message_id,
                        record.fwd_from_name,
                        record.fwd_from_id,
                        record.fwd_post_message_id,
                        record.raw_json,
                        record.source,
                        now,
                        now,
                        record.sender_id,
                        record.sender_name,
                        record.post_author,
                        record.entities_json,
                    ),
                )
                if enqueue_processing:
                    self._enqueue_locked(conn, record.channel_id, record.message_id, "message", 0)
                return UpsertResult(UpsertOutcome.INSERTED, 0, 0)

            old_version = int(row["processing_version"])
            content_fields = (
                row["text"],
                row["caption"],
                row["media_type"],
                row["media_meta"],
                row["reply_to_message_id"],
                row["fwd_from_name"],
                row["fwd_from_id"],
                row["fwd_post_message_id"],
                row["edit_datetime"],
                row["datetime"],
                row["sender_id"],
                row["sender_name"],
                row["post_author"],
                row["entities_json"],
            )
            new_fields = (
                record.text,
                record.caption,
                record.media_type,
                record.media_meta_json,
                record.reply_to_message_id,
                record.fwd_from_name,
                record.fwd_from_id,
                record.fwd_post_message_id,
                record.edit_datetime,
                record.datetime,
                record.sender_id,
                record.sender_name,
                record.post_author,
                record.entities_json,
            )
            if content_fields == new_fields:
                return UpsertResult(UpsertOutcome.UNCHANGED, old_version, old_version)

            # Staleness guard (race between live stream and history import):
            # never overwrite a row that already reflects an edit with an
            # OLDER snapshot fetched by backfill. A snapshot lacking an edit
            # date is by Telegram semantics pre-edit, i.e. older than any
            # stored row that already carries an edit date.
            existing_edit_dt = row["edit_datetime"]
            incoming_edit_dt = record.edit_datetime
            stale = existing_edit_dt is not None and (
                incoming_edit_dt is None or str(incoming_edit_dt) < str(existing_edit_dt)
            )
            if stale:
                log.debug(
                    "stale history version ignored for %s/%s (fetched %s, stored %s)",
                    record.channel_id,
                    record.message_id,
                    incoming_edit_dt,
                    existing_edit_dt,
                )
                return UpsertResult(UpsertOutcome.UNCHANGED, old_version, old_version)

            new_version = old_version + 1
            conn.execute(
                """
                UPDATE messages SET datetime = ?, edit_datetime = ?, text = ?, caption = ?,
                    media_type = ?, media_meta = ?, grouped_id = ?, reply_to_message_id = ?,
                    fwd_from_name = ?, fwd_from_id = ?, fwd_post_message_id = ?, raw_json = ?,
                    processing_version = ?, last_updated = ?, sender_id = ?, sender_name = ?,
                    post_author = ?, entities_json = ?
                WHERE channel_id = ? AND message_id = ?
                """,
                (
                    record.datetime,
                    record.edit_datetime,
                    record.text,
                    record.caption,
                    record.media_type,
                    record.media_meta_json,
                    record.grouped_id,
                    record.reply_to_message_id,
                    record.fwd_from_name,
                    record.fwd_from_id,
                    record.fwd_post_message_id,
                    record.raw_json,
                    new_version,
                    now,
                    record.sender_id,
                    record.sender_name,
                    record.post_author,
                    record.entities_json,
                    record.channel_id,
                    record.message_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO message_edits (channel_id, message_id, edit_seq, edit_datetime,
                    captured_at, text, caption, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (channel_id, message_id, edit_seq) DO NOTHING
                """,
                (
                    record.channel_id,
                    record.message_id,
                    old_version,
                    row["edit_datetime"],
                    now,
                    row["text"],
                    row["caption"],
                    record.source,
                ),
            )
            if enqueue_processing:
                self._enqueue_locked(conn, record.channel_id, record.message_id, "edit", new_version)
            return UpsertResult(UpsertOutcome.UPDATED, old_version, new_version)

    def mark_deleted(self, channel_id: int, message_ids: Iterable[int]) -> int:
        ids = list(message_ids)
        if not ids:
            return 0
        now = UTC_NOW()
        marked = 0
        with self.db.transaction() as conn:
            for mid in ids:
                res = conn.execute(
                    "UPDATE messages SET deleted = 1, deleted_at = ?, last_updated = ? "
                    "WHERE channel_id = ? AND message_id = ? AND deleted = 0",
                    (now, now, channel_id, mid),
                )
                if res.rowcount:
                    marked += 1
                known = conn.execute(
                    "SELECT 1 FROM messages WHERE channel_id = ? AND message_id = ?", (channel_id, mid)
                ).fetchone()
                if known:
                    self._enqueue_locked(conn, channel_id, mid, "delete", 0)
        return marked

    def get_message_raw(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM messages WHERE channel_id = ? AND message_id = ?", (channel_id, message_id)
        ).fetchone()
        return dict(row) if row else None

    def list_messages(
        self, channel_id: int | None, limit: int = 20, newest_first: bool = True, include_deleted: bool = False
    ) -> list[dict[str, Any]]:
        order = "DESC" if newest_first else "ASC"
        where, params = "1=1", []
        if channel_id is not None:
            where, params = "channel_id = ?", [channel_id]
        base_params = list(params)
        if not include_deleted:
            where += " AND deleted = 0"
        sql = (
            f"SELECT channel_id, message_id, datetime, edit_datetime, substr(text, 1, 120) AS text_preview, "
            f"media_type, processing_version, deleted, source FROM messages WHERE {where} "
            f"ORDER BY datetime {order} LIMIT ?"
        )
        base_params.append(limit)
        rows = self.db.conn.execute(sql, base_params).fetchall()
        return [dict(r) for r in rows]

    def max_message_id(self, channel_id: int) -> int:
        """Newest stored message id for one channel (0 when empty)."""

        row = self.db.conn.execute(
            "SELECT COALESCE(MAX(message_id), 0) AS m FROM messages WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return int(row["m"] or 0)

    def message_counts(self, channel_id: int) -> MessageCounts:
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS total, SUM(deleted) AS deleted, "
            "SUM(CASE WHEN media_type IS NOT NULL THEN 1 ELSE 0 END) AS with_media, "
            "MIN(message_id) AS min_id, MAX(message_id) AS max_id, MAX(datetime) AS max_datetime "
            "FROM messages WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return MessageCounts(
            total=int(row["total"] or 0),
            deleted=int(row["deleted"] or 0),
            with_media=int(row["with_media"] or 0),
            min_id=int(row["min_id"]) if row["min_id"] is not None else None,
            max_id=int(row["max_id"]) if row["max_id"] is not None else None,
            max_datetime=row["max_datetime"],
        )

    def edit_history(self, channel_id: int, message_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM message_edits WHERE channel_id = ? AND message_id = ? ORDER BY edit_seq",
            (channel_id, message_id),
        ).fetchall()
        return [dict(r) for r in rows]

    # ----------------------------------------------------------- sync state

    def sync_state(self, channel_id: int) -> dict[str, Any]:
        row = self.db.conn.execute("SELECT * FROM sync_state WHERE channel_id = ?", (channel_id,)).fetchone()
        return dict(row) if row else {}

    def init_sync_state(self, channel_id: int, mode: str, since: str | None) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (channel_id, backfill_target_mode, backfill_since,
                    backfill_status, updated_at)
                VALUES (?, ?, ?, 'not_started', ?)
                ON CONFLICT (channel_id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (channel_id, mode, since, now),
            )

    def start_backfill(self, channel_id: int, mode: str, since: str | None, start_next_max_id: int | None) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            row = conn.execute("SELECT backfill_status FROM sync_state WHERE channel_id = ?", (channel_id,)).fetchone()
            resume = bool(row) and row["backfill_status"] == "in_progress"
            if resume:  # keep checkpoint
                conn.execute("UPDATE sync_state SET updated_at = ? WHERE channel_id = ?", (now, channel_id))
                return
            conn.execute(
                """
                INSERT INTO sync_state (channel_id, backfill_target_mode, backfill_since, backfill_status,
                    backfill_next_max_id, backfill_started_at, updated_at)
                VALUES (?, ?, ?, 'in_progress', ?, COALESCE(?, ?), ?)
                ON CONFLICT (channel_id) DO UPDATE SET
                    backfill_target_mode = excluded.backfill_target_mode,
                    backfill_since = excluded.backfill_since,
                    backfill_status = 'in_progress',
                    backfill_next_max_id = excluded.backfill_next_max_id,
                    backfill_started_at = COALESCE(sync_state.backfill_started_at, ?),
                    backfill_completed_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (channel_id, mode, since, start_next_max_id, None, now, now, now),
            )

    def backfill_checkpoint(
        self, channel_id: int, next_max_id: int | None, oldest_id_seen: int | None, count_delta: int
    ) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_state SET backfill_next_max_id = ?, updated_at = ?,
                    backfill_oldest_id_seen = COALESCE(
                        CASE WHEN backfill_oldest_id_seen IS NULL THEN ?
                            ELSE MIN(backfill_oldest_id_seen, ?) END, ?),
                    backfill_message_count = backfill_message_count + ?
                WHERE channel_id = ?
                """,
                (next_max_id, now, oldest_id_seen, oldest_id_seen, oldest_id_seen, count_delta, channel_id),
            )

    def complete_backfill(self, channel_id: int, oldest_id_seen: int | None) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                """
                UPDATE sync_state SET backfill_status = 'done',
                    backfill_completed_at = COALESCE(backfill_completed_at, ?),
                    backfill_oldest_id_seen = COALESCE(backfill_oldest_id_seen, ?), updated_at = ?
                WHERE channel_id = ?
                """,
                (now, oldest_id_seen, now, channel_id),
            )

    def reset_backfill(self, channel_id: int) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE sync_state SET backfill_status = 'not_started', backfill_next_max_id = NULL, "
                "backfill_oldest_id_seen = NULL, backfill_completed_at = NULL, updated_at = ? WHERE channel_id = ?",
                (now, channel_id),
            )
            if conn.execute("SELECT changes()").fetchone()[0] == 0:
                conn.execute(
                    """
                    INSERT INTO sync_state (channel_id, backfill_target_mode, backfill_since,
                        backfill_status, updated_at)
                    VALUES (?, 'full', NULL, 'not_started', ?)
                    """,
                    (channel_id, now),
                )

    def record_live_event(self, channel_id: int, message_id: int) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (channel_id, live_last_seen_message_id, live_last_event_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (channel_id) DO UPDATE SET
                    live_last_seen_message_id = CASE WHEN live_last_seen_message_id IS NULL
                        OR excluded.live_last_seen_message_id > live_last_seen_message_id
                        THEN excluded.live_last_seen_message_id ELSE live_last_seen_message_id END,
                    live_last_event_at = excluded.live_last_event_at,
                    updated_at = excluded.updated_at
                """,
                (channel_id, message_id, now, now),
            )

    def record_recovery(self, channel_id: int) -> None:
        now = UTC_NOW()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE sync_state SET live_last_sync_at = ?, updated_at = ? WHERE channel_id = ?",
                (now, now, channel_id),
            )

    def sync_status(self) -> SyncStatus:
        chan = self.selected_channel()
        state = self.sync_state(chan.channel_id) if chan else {}

        def g(key: str, default=None):  # noqa: ANN001
            return state.get(key, default)

        return SyncStatus(
            channel_id=chan.channel_id if chan else None,
            channel_title=chan.title if chan else None,
            backfill_status=str(g("backfill_status", "not_started")),
            backfill_target_mode=g("backfill_target_mode"),
            backfill_since=g("backfill_since"),
            backfill_next_max_id=g("backfill_next_max_id"),
            backfill_oldest_id_seen=g("backfill_oldest_id_seen"),
            backfill_message_count=int(g("backfill_message_count", 0)),
            live_last_seen_message_id=g("live_last_seen_message_id"),
            live_last_event_at=g("live_last_event_at"),
            last_sync_at=g("live_last_sync_at"),
        )

    # ----------------------------------------------------------- queue

    def _enqueue_locked(
        self, conn: Any, channel_id: int, message_id: int, item_type: str, targets_version: int
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO processing_queue
                (channel_id, message_id, item_type, status, targets_version, attempts, available_at, created_at)
            VALUES (?, ?, ?, 'pending', ?, 0, ?, ?)
            """,
            (channel_id, message_id, item_type, targets_version, UTC_NOW(), UTC_NOW()),
        )

    def enqueue(self, channel_id: int, message_id: int, item_type: str, targets_version: int) -> None:
        with self.db.transaction() as conn:
            self._enqueue_locked(conn, channel_id, message_id, item_type, targets_version)

    def claim_pending(self, limit: int = 50) -> list[QueueItem]:
        """Atomically claim nearby-due pending items for processing."""

        now = UTC_NOW()
        items: list[QueueItem] = []
        with self.db.transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM processing_queue WHERE status = 'pending' AND available_at <= ? "
                "ORDER BY created_at LIMIT ?",
                (now, limit),
            ).fetchall()
            claimed_ts = UTC_NOW()
            for row in rows:
                res = conn.execute(
                    "UPDATE processing_queue SET status = 'processing', claimed_at = ? "
                    "WHERE queue_id = ? AND status = 'pending'",
                    (claimed_ts, row["queue_id"]),
                )
                if res.rowcount:
                    items.append(
                        QueueItem(
                            row["queue_id"],
                            row["channel_id"],
                            row["message_id"],
                            row["item_type"],
                            "processing",
                            row["targets_version"],
                            row["attempts"],
                            row["available_at"],
                            row["created_at"],
                        )
                    )
        return items

    def requeue_expired_claims(self) -> int:
        """Recover items claimed by a dead worker; called at service startup
        *before* the queue consumer starts (so nothing legit is re-claimed)."""

        with self.db.transaction() as conn:
            res = conn.execute(
                "UPDATE processing_queue SET status = 'pending', claimed_at = NULL WHERE status = 'processing'"
            )
            return res.rowcount

    def finish(self, item: QueueItem, success: bool, error: str | None = None) -> None:
        now = UTC_NOW()
        attempts = item.attempts + 1
        with self.db.transaction() as conn:
            if success:
                conn.execute(
                    "UPDATE processing_queue SET status = 'processed', processed_at = ?, attempts = ? "
                    "WHERE queue_id = ?",
                    (now, attempts, item.queue_id),
                )
                return
            safe_error = (error or "")[:MAX_QUEUE_ERROR_LEN].replace("\n", " ")
            if attempts >= self.processing_max_attempts:
                conn.execute(
                    "UPDATE processing_queue SET status = 'failed', attempts = ?, last_error = ? WHERE queue_id = ?",
                    (attempts, safe_error, item.queue_id),
                )
            else:
                # bounded exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s
                delay = min(60.0, 2.0 ** (attempts - 1)) * self.retry_backoff_scale
                if delay < 0.001:
                    delay = 0.0
                available_at = (
                    (datetime.now(UTC) + timedelta(seconds=delay)).isoformat(timespec="seconds").replace("+00:00", "Z")
                    if delay
                    else UTC_NOW()
                )
                conn.execute(
                    "UPDATE processing_queue SET status = 'pending', attempts = ?, last_error = ?, "
                    "available_at = ? WHERE queue_id = ?",
                    (attempts, safe_error, available_at, item.queue_id),
                )

    def queue_counts(self) -> dict[str, int]:
        counts = {"pending": 0, "processing": 0, "processed": 0, "failed": 0}
        for row in self.db.conn.execute("SELECT status, COUNT(*) AS c FROM processing_queue GROUP BY status"):
            status = row["status"]
            if status in counts:
                counts[status] = int(row["c"])
        return counts

    def retry_failed(self) -> int:
        with self.db.transaction() as conn:
            res = conn.execute(
                "UPDATE processing_queue SET status = 'pending', attempts = 0, available_at = ?, last_error = NULL "
                "WHERE status = 'failed'",
                (UTC_NOW(),),
            )
            return res.rowcount

    def reprocess_message(self, channel_id: int, message_id: int) -> int:
        """Re-enqueue the current version of a message (e.g. after parser update).

        The version-based UNIQUE constraint prevents duplicate rows, so an
        explicit reprocess resets the existing row for its version instead.
        """

        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT processing_version FROM messages WHERE channel_id = ? AND message_id = ?",
                (channel_id, message_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"message {channel_id}/{message_id} not found")
            version = row["processing_version"]
            item_type = "message" if version == 0 else "edit"
            reset = conn.execute(
                "UPDATE processing_queue SET status = 'pending', attempts = 0, "
                "available_at = ?, processed_at = NULL, claimed_at = NULL, last_error = NULL "
                "WHERE channel_id = ? AND message_id = ? AND item_type = ? AND targets_version = ?",
                (UTC_NOW(), channel_id, message_id, item_type, version),
            )
            if reset.rowcount == 0:
                self._enqueue_locked(conn, channel_id, message_id, item_type, version)
            return version

    def list_queue(self, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        if status:
            rows = self.db.conn.execute(
                "SELECT * FROM processing_queue WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM processing_queue ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


def _channel_from_row(row: Any) -> ChannelInfo:
    return ChannelInfo(
        channel_id=int(row["channel_id"]),
        access_hash=int(row["access_hash"]),
        username=row["username"],
        title=row["title"],
        is_selected=bool(row["is_selected"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
