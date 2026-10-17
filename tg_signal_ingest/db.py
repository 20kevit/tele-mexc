"""SQLite database bootstrap and schema migrations.

Migrations are embedded, idempotent, and applied in order. The connection uses
WAL mode and a busy timeout so concurrent maintenance commands on a busy
database do not produce spurious failures.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id   INTEGER PRIMARY KEY,
            access_hash  INTEGER NOT NULL,
            username     TEXT,
            title        TEXT NOT NULL,
            is_selected  INTEGER NOT NULL DEFAULT 0 CHECK (is_selected IN (0, 1)),
            selected_at  TEXT,
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
            channel_id          INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            datetime            TEXT NOT NULL,
            edit_datetime       TEXT,
            text                TEXT,
            caption             TEXT,
            media_type          TEXT,
            media_meta          TEXT,
            grouped_id          INTEGER,
            reply_to_message_id INTEGER,
            fwd_from_name       TEXT,
            fwd_from_id         INTEGER,
            fwd_post_message_id INTEGER,
            raw_json            TEXT NOT NULL,
            source              TEXT NOT NULL CHECK (source IN ('backfill', 'live', 'recovery')),
            processing_version  INTEGER NOT NULL DEFAULT 0,
            deleted             INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1)),
            deleted_at          TEXT,
            first_seen          TEXT NOT NULL,
            last_updated        TEXT NOT NULL,
            PRIMARY KEY (channel_id, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages (channel_id, datetime);
        CREATE INDEX IF NOT EXISTS idx_messages_version ON messages (channel_id, processing_version);

        CREATE TABLE IF NOT EXISTS message_edits (
            channel_id    INTEGER NOT NULL,
            message_id    INTEGER NOT NULL,
            edit_seq      INTEGER NOT NULL,
            edit_datetime TEXT,
            captured_at   TEXT NOT NULL,
            text          TEXT,
            caption       TEXT,
            source        TEXT,
            PRIMARY KEY (channel_id, message_id, edit_seq)
        );

        CREATE TABLE IF NOT EXISTS processing_queue (
            queue_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id      INTEGER NOT NULL,
            message_id      INTEGER NOT NULL,
            item_type       TEXT NOT NULL CHECK (item_type IN ('message', 'edit', 'delete')),
            status          TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
            targets_version INTEGER NOT NULL,
            attempts        INTEGER NOT NULL DEFAULT 0,
            available_at    TEXT NOT NULL,
            claimed_at      TEXT,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            processed_at    TEXT,
            UNIQUE (channel_id, message_id, item_type, targets_version)
        );
        CREATE INDEX IF NOT EXISTS idx_queue_status ON processing_queue (status, available_at);

        CREATE TABLE IF NOT EXISTS sync_state (
            channel_id                 INTEGER PRIMARY KEY,
            backfill_target_mode       TEXT CHECK (backfill_target_mode IN ('full', 'since') OR backfill_target_mode IS NULL),
            backfill_since             TEXT,
            backfill_status            TEXT NOT NULL DEFAULT 'not_started'
                CHECK (backfill_status IN ('not_started', 'in_progress', 'done')),
            backfill_next_max_id       INTEGER,
            backfill_oldest_id_seen    INTEGER,
            backfill_message_count     INTEGER NOT NULL DEFAULT 0,
            backfill_started_at        TEXT,
            backfill_completed_at      TEXT,
            live_last_seen_message_id  INTEGER,
            live_last_event_at         TEXT,
            live_last_sync_at          TEXT,
            updated_at                 TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_state (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
]


class Database:
    """Thin wrapper over sqlite3 with embedded migrations."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.lock = threading.RLock()
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.isolation_level = None  # autocommit; explicit BEGIN via lock

    def migrate(self) -> list[int]:
        applied: list[int] = []
        with self.lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            done = {r["version"] for r in self.conn.execute("SELECT version FROM schema_migrations")}
            for version, sql in MIGRATIONS:
                if version in done:
                    continue
                with self.transaction():
                    self.conn.executescript(sql)
                    self.conn.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))",
                        (version,),
                    )
                applied.append(version)
        return applied

    def schema_version(self) -> int:
        with self.lock:
            try:
                row = self.conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
            except sqlite3.OperationalError:
                return 0
            return int(row["v"] or 0)

    def transaction(self):
        class _Tx:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                self.conn.execute("BEGIN IMMEDIATE")
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                if exc_type is None:
                    self.conn.execute("COMMIT")
                else:
                    self.conn.execute("ROLLBACK")
                return False

        return _Tx(self.conn)

    def close(self) -> None:
        with self.lock:
            self.conn.close()
