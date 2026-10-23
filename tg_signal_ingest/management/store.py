"""Durable state for the channel-management bot.

There is deliberately no exchange/trading state in this module. Authentication
secrets are kept in memory by ``TelegramLoginManager``; this store persists only
flow *stages*, opaque callback tokens, channel metadata, and ingestion jobs.
"""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import Database

FLOW_TTL_S = 900
CALLBACK_TTL_S = 900
JOB_ACTIVE = ("pending", "running", "interrupted")
JOB_RESUMABLE = ("cancelled", "failed", "partial", "interrupted")
VALIDATION_LIMITS = (10, 25, 50)
VALIDATION_REQUEST_TTL_S = 1800
REVIEW_DRAFT_TTL_S = 900
REQUEST_ACTIVE = ("requested", "approved", "started", "review")

STATE_FIELDS = {
    "telegram_state",
    "telegram_label",
    "telegram_authorized_at",
    "selected_channel_id",
    "selected_channel_title",
    "selected_channel_username",
    "selection_snapshot_id",
}
JOB_FIELDS = {
    "status",
    "checkpoint_next_max_id",
    "oldest_seen_id",
    "newest_seen_id",
    "discovered",
    "inserted",
    "updated",
    "unchanged",
    "failed",
    "total_stored",
    "cancel_requested",
    "error",
    "started_at",
    "completed_at",
    "last_notified_at",
    "max_messages",
    "request_id",
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


class ManagementStore:
    def __init__(self, db: Database, now: Any | None = None):
        self.db = db
        self._now = now or (lambda: datetime.now(UTC))

    def _ts(self) -> str:
        return _iso(self._now())

    # ------------------------------------------------------------------ state

    def state(self) -> dict[str, Any]:
        row = self.db.conn.execute("SELECT * FROM mgmt_state WHERE id = 1").fetchone()
        if row is None:  # pragma: no cover - migration inserts it
            self.db.conn.execute(
                "INSERT OR IGNORE INTO mgmt_state (id, created_at, updated_at) VALUES (1, ?, ?)",
                (self._ts(), self._ts()),
            )
            row = self.db.conn.execute("SELECT * FROM mgmt_state WHERE id = 1").fetchone()
        assert row is not None
        return dict(row)

    def update_state(self, **fields: Any) -> dict[str, Any]:
        unknown = set(fields) - STATE_FIELDS
        if unknown:
            raise ValueError(f"unknown management state fields: {sorted(unknown)}")
        if not fields:
            return self.state()
        assignments = ", ".join(f"{key} = ?" for key in fields)
        values = list(fields.values())
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE mgmt_state SET {assignments}, updated_at = ? WHERE id = 1",
                [*values, self._ts()],
            )
        return self.state()

    # ------------------------------------------------------------------- flow

    def flow(self, operator_id: int) -> dict[str, Any]:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_operator_flows WHERE operator_id = ?", (int(operator_id),)
        ).fetchone()
        if row is None:
            return {"state": "idle", "expired": False, "expires_at": None}
        expires = _parse(row["expires_at"])
        return {
            "state": str(row["state"]),
            "expired": bool(expires and expires <= self._now()),
            "expires_at": row["expires_at"],
        }

    def set_flow(
        self, operator_id: int, state: str, *, ttl_s: int = FLOW_TTL_S
    ) -> None:
        if state not in ("idle", "auth_phone", "auth_code", "auth_password"):
            raise ValueError("invalid management flow state")
        now = self._now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mgmt_operator_flows (operator_id, state, updated_at, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(operator_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at,
                    expires_at = excluded.expires_at
                """,
                (
                    int(operator_id),
                    state,
                    _iso(now),
                    None if state == "idle" else _iso(now + timedelta(seconds=ttl_s)),
                ),
            )

    def clear_flow(self, operator_id: int) -> None:
        self.set_flow(operator_id, "idle")

    def clear_all_flows(self) -> int:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE mgmt_operator_flows SET state = 'idle', updated_at = ?, expires_at = NULL "
                "WHERE state != 'idle'",
                (self._ts(),),
            )
            return cur.rowcount

    def recover_flows(self) -> int:
        """Invalidate abandoned in-memory login stages after a restart.

        Telethon's phone_code_hash and the encrypted login client live only in
        memory. A restart therefore cannot safely resume a code/password stage;
        clearing it is the accurate recovery: the operator starts a fresh flow.
        No session file is touched here.
        """

        return self.clear_all_flows()

    def pending_flow_count(self) -> int:
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM mgmt_operator_flows WHERE state != 'idle'"
        ).fetchone()
        return int(row["n"] or 0)

    # -------------------------------------------------------- operator channels

    def operator_channel(self, operator_id: int) -> dict[str, Any]:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_operator_channels WHERE operator_id = ?",
            (int(operator_id),),
        ).fetchone()
        if row is None:
            return {
                "selected_channel_id": None,
                "selected_channel_title": None,
                "selected_channel_username": None,
                "selected_channel_access_hash": None,
                "selection_snapshot_id": None,
            }
        return dict(row)

    def set_operator_channel(self, operator_id: int, **fields: Any) -> None:
        allowed = {
            "selected_channel_id",
            "selected_channel_title",
            "selected_channel_username",
            "selected_channel_access_hash",
            "selection_snapshot_id",
        }
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"unknown operator channel fields: {sorted(unknown)}")
        current = self.operator_channel(operator_id)
        merged = {**current, **fields}
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mgmt_operator_channels
                    (operator_id, selected_channel_id, selected_channel_title,
                     selected_channel_username, selected_channel_access_hash,
                     selection_snapshot_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(operator_id) DO UPDATE SET
                    selected_channel_id = excluded.selected_channel_id,
                    selected_channel_title = excluded.selected_channel_title,
                    selected_channel_username = excluded.selected_channel_username,
                    selected_channel_access_hash = excluded.selected_channel_access_hash,
                    selection_snapshot_id = excluded.selection_snapshot_id,
                    updated_at = excluded.updated_at
                """,
                (
                    int(operator_id),
                    merged.get("selected_channel_id"),
                    merged.get("selected_channel_title"),
                    merged.get("selected_channel_username"),
                    merged.get("selected_channel_access_hash"),
                    merged.get("selection_snapshot_id"),
                    self._ts(),
                ),
            )

    def clear_operator_channel(self, operator_id: int) -> None:
        self.set_operator_channel(
            operator_id,
            selected_channel_id=None,
            selected_channel_title=None,
            selected_channel_username=None,
            selected_channel_access_hash=None,
            selection_snapshot_id=None,
        )

    def selected_channel_details(self, operator_id: int) -> dict[str, Any]:
        """Return the operator's selected channel plus its snapshot access hash.

        The snapshot join is intentionally scoped to this operator, so one
        operator cannot reuse another operator's selection or stale snapshot.
        """

        row = self.db.conn.execute(
            """
            SELECT o.selected_channel_id, o.selected_channel_title,
                   o.selected_channel_username, o.selection_snapshot_id,
                   COALESCE(o.selected_channel_access_hash, c.access_hash) AS access_hash
            FROM mgmt_operator_channels o
            LEFT JOIN mgmt_snapshots s
                ON s.snapshot_id = o.selection_snapshot_id
               AND s.operator_id = o.operator_id
            LEFT JOIN mgmt_snapshot_channels c
                ON c.snapshot_id = s.snapshot_id
               AND c.channel_id = o.selected_channel_id
            WHERE o.operator_id = ?
            """,
            (int(operator_id),),
        ).fetchone()
        return dict(row) if row else {}

    # --------------------------------------------------- validation requests

    def tables_exist(self, *names: str) -> bool:
        if not names:
            return True
        marks = ",".join("?" for _ in names)
        row = self.db.conn.execute(
            f"SELECT COUNT(*) AS n FROM sqlite_master "
            f"WHERE type = 'table' AND name IN ({marks})",
            tuple(names),
        ).fetchone()
        return int(row["n"] or 0) == len(set(names))

    def validation_available(self) -> bool:
        """The workflow is unavailable until its schema exists.

        This is a read-only preflight: it never creates tables or runs a
        migration. Production can therefore keep running on an older schema
        without the bot silently upgrading it.
        """

        return self.tables_exist(
            "mgmt_import_requests",
            "mgmt_review_drafts",
            "signal_candidates",
            "signal_candidate_reviews",
            "analysis_runs",
        )

    def _expire_requests_conn(self, conn: Any, now: datetime) -> int:
        stamp = _iso(now)
        cur = conn.execute(
            "UPDATE mgmt_import_requests SET status = 'expired', "
            "error = 'authorization expired before use', updated_at = ? "
            "WHERE status IN ('requested', 'approved') AND expires_at <= ?",
            (stamp, stamp),
        )
        return int(cur.rowcount or 0)

    def expire_import_requests(self) -> int:
        with self.db.transaction() as conn:
            return self._expire_requests_conn(conn, self._now())

    def create_import_request(
        self,
        *,
        channel_id: int,
        channel_title: str,
        channel_access_hash: int | None,
        selection_snapshot_id: str | None,
        max_messages: int,
        requested_by: int,
        self_authorized_by: int | None = None,
    ) -> dict[str, Any]:
        """Create one bounded validation request.

        A request is normally ``requested`` and needs the owner. The narrow
        exception is ``self_authorized_by``: the configured developer may
        authorize their own bounded validation request. It is deliberately
        limited to this one request type; it does not grant any owner role or
        any other management/control permission.
        """

        if int(max_messages) not in VALIDATION_LIMITS:
            raise ValueError("max_messages must be one of the configured bounds")
        if self_authorized_by is not None and int(self_authorized_by) != int(
            requested_by
        ):
            raise ValueError("self authorization must come from the requesting operator")
        now = self._now()
        request_id = f"val-{secrets.token_hex(8)}"
        status = "approved" if self_authorized_by is not None else "requested"
        decided_by = int(self_authorized_by) if self_authorized_by is not None else None
        decided_at = _iso(now) if self_authorized_by is not None else None
        with self.db.transaction() as conn:
            self._expire_requests_conn(conn, now)
            marks = ",".join("?" for _ in REQUEST_ACTIVE)
            active = conn.execute(
                f"SELECT request_id FROM mgmt_import_requests "
                f"WHERE status IN ({marks}) LIMIT 1",
                REQUEST_ACTIVE,
            ).fetchone()
            if active is not None:
                raise ValueError("a validation request is already active")
            conn.execute(
                """
                INSERT INTO mgmt_import_requests
                    (request_id, channel_id, channel_title, channel_access_hash,
                     selection_snapshot_id, max_messages, status, requested_by,
                     requested_at, decided_by, decided_at, expires_at,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    int(channel_id),
                    str(channel_title),
                    int(channel_access_hash) if channel_access_hash is not None else None,
                    selection_snapshot_id,
                    int(max_messages),
                    status,
                    int(requested_by),
                    _iso(now),
                    decided_by,
                    decided_at,
                    _iso(now + timedelta(seconds=VALIDATION_REQUEST_TTL_S)),
                    _iso(now),
                    _iso(now),
                ),
            )
        request = self.get_import_request(request_id)
        assert request is not None
        return request

    def get_import_request(self, request_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_import_requests WHERE request_id = ?",
            (str(request_id),),
        ).fetchone()
        return dict(row) if row else None

    def latest_import_request(self) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_import_requests ORDER BY created_at DESC, rowid DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def active_import_request(self) -> dict[str, Any] | None:
        marks = ",".join("?" for _ in REQUEST_ACTIVE)
        row = self.db.conn.execute(
            f"SELECT * FROM mgmt_import_requests WHERE status IN ({marks}) "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1",
            REQUEST_ACTIVE,
        ).fetchone()
        return dict(row) if row else None

    def decide_import_request(
        self, request_id: str, *, approved: bool, decided_by: int
    ) -> tuple[str, dict[str, Any] | None]:
        now = self._now()
        with self.db.transaction() as conn:
            self._expire_requests_conn(conn, now)
            row = conn.execute(
                "SELECT * FROM mgmt_import_requests WHERE request_id = ?",
                (str(request_id),),
            ).fetchone()
            if row is None:
                return "missing", None
            if row["status"] != "requested":
                return str(row["status"]), dict(row)
            status = "approved" if approved else "denied"
            conn.execute(
                "UPDATE mgmt_import_requests SET status = ?, decided_by = ?, "
                "decided_at = ?, error = ?, updated_at = ? "
                "WHERE request_id = ? AND status = 'requested'",
                (
                    status,
                    int(decided_by),
                    _iso(now),
                    None if approved else "denied by owner",
                    _iso(now),
                    str(request_id),
                ),
            )
        return "ok", self.get_import_request(request_id)

    def start_import_request(
        self, request_id: str
    ) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
        """Create exactly one bounded job for an approved request.

        All checks and the job insert happen under one write transaction, so a
        double-click, a changed channel selection, or a concurrent request
        cannot start two imports.
        """

        now = self._now()
        stamp = _iso(now)
        with self.db.transaction() as conn:
            self._expire_requests_conn(conn, now)
            row = conn.execute(
                "SELECT * FROM mgmt_import_requests WHERE request_id = ?",
                (str(request_id),),
            ).fetchone()
            if row is None:
                return "missing", None, None
            if row["status"] != "approved":
                return str(row["status"]), dict(row), None
            selection = conn.execute(
                "SELECT selected_channel_id FROM mgmt_operator_channels "
                "WHERE operator_id = ?",
                (int(row["requested_by"]),),
            ).fetchone()
            if (
                selection is None
                or selection["selected_channel_id"] is None
                or int(selection["selected_channel_id"]) != int(row["channel_id"])
            ):
                conn.execute(
                    "UPDATE mgmt_import_requests SET status = 'cancelled', "
                    "error = 'channel selection changed before start', updated_at = ? "
                    "WHERE request_id = ? AND status = 'approved'",
                    (stamp, str(request_id)),
                )
                return "selection_changed", self.get_import_request(request_id), None
            active = conn.execute(
                "SELECT job_id FROM mgmt_ingestions WHERE status IN ('pending', 'running') "
                "ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if active is not None:
                return "active_job", dict(row), None
            job_id = f"job-{secrets.token_hex(8)}"
            conn.execute(
                """
                INSERT INTO mgmt_ingestions
                    (job_id, channel_id, channel_title, status, max_messages,
                     request_id, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)
                """,
                (
                    job_id,
                    int(row["channel_id"]),
                    str(row["channel_title"]),
                    int(row["max_messages"]),
                    str(request_id),
                    stamp,
                    stamp,
                ),
            )
            conn.execute(
                "UPDATE mgmt_import_requests SET status = 'started', job_id = ?, "
                "updated_at = ? WHERE request_id = ? AND status = 'approved'",
                (job_id, stamp, str(request_id)),
            )
        return "ok", self.get_import_request(request_id), self.get_job(job_id)

    def finish_import_request(
        self,
        request_id: str,
        status: str,
        *,
        job_id: str | None = None,
        imported_min_id: int | None = None,
        imported_max_id: int | None = None,
        candidate_count: int | None = None,
        error: str | None = None,
    ) -> dict[str, Any] | None:
        if status not in ("review", "completed", "failed", "cancelled"):
            raise ValueError("invalid validation request status")
        assignments = ["status = ?", "error = ?", "updated_at = ?"]
        values: list[Any] = [status, (error or "")[:2000] or None, self._ts()]
        for key, value in (
            ("job_id", job_id),
            ("imported_min_id", imported_min_id),
            ("imported_max_id", imported_max_id),
            ("candidate_count", candidate_count),
        ):
            if value is not None:
                assignments.append(f"{key} = ?")
                values.append(int(value) if key != "job_id" else value)
        values.append(str(request_id))
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE mgmt_import_requests SET {', '.join(assignments)} "
                "WHERE request_id = ?",
                values,
            )
        return self.get_import_request(request_id)

    def update_import_review_progress(
        self, request_id: str, *, reviewed_count: int, candidate_count: int
    ) -> dict[str, Any] | None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE mgmt_import_requests SET reviewed_count = ?, "
                "candidate_count = ?, updated_at = ? WHERE request_id = ?",
                (int(reviewed_count), int(candidate_count), self._ts(), str(request_id)),
            )
        return self.get_import_request(request_id)

    def recover_import_requests(self) -> dict[str, int]:
        """Expire stale approvals and fail imports interrupted by a restart.

        A restart cannot safely resume a bounded import because the developer
        must authorize a fresh start. Existing messages are never deleted.
        """

        now = self._now()
        stamp = _iso(now)
        with self.db.transaction() as conn:
            expired = self._expire_requests_conn(conn, now)
            cur = conn.execute(
                "UPDATE mgmt_import_requests SET status = 'failed', "
                "error = 'process restarted; new authorization required', updated_at = ? "
                "WHERE status = 'started' AND (job_id IS NULL OR job_id NOT IN "
                "(SELECT job_id FROM mgmt_ingestions WHERE status = 'running'))",
                (stamp,),
            )
            interrupted = int(cur.rowcount or 0)
        return {"expired": expired, "interrupted": interrupted}

    # ------------------------------------------------------ review drafts

    def set_review_draft(
        self,
        operator_id: int,
        *,
        request_id: str,
        candidate_id: str,
        page_index: int,
        fields: dict[str, Any],
    ) -> None:
        now = self._now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mgmt_review_drafts
                    (operator_id, request_id, candidate_id, page_index, fields_json,
                     created_at, updated_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(operator_id) DO UPDATE SET
                    request_id = excluded.request_id,
                    candidate_id = excluded.candidate_id,
                    page_index = excluded.page_index,
                    fields_json = excluded.fields_json,
                    updated_at = excluded.updated_at,
                    expires_at = excluded.expires_at
                """,
                (
                    int(operator_id),
                    str(request_id),
                    str(candidate_id),
                    int(page_index),
                    json.dumps(fields, sort_keys=True, ensure_ascii=False),
                    _iso(now),
                    _iso(now),
                    _iso(now + timedelta(seconds=REVIEW_DRAFT_TTL_S)),
                ),
            )

    def get_review_draft(self, operator_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_review_drafts WHERE operator_id = ?",
            (int(operator_id),),
        ).fetchone()
        if row is None:
            return None
        expires = _parse(row["expires_at"])
        if expires is None or expires <= self._now():
            self.clear_review_draft(operator_id)
            return None
        data = dict(row)
        try:
            data["fields"] = json.loads(data.get("fields_json") or "{}")
        except (TypeError, ValueError):
            data["fields"] = {}
        return data

    def clear_review_draft(self, operator_id: int) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM mgmt_review_drafts WHERE operator_id = ?",
                (int(operator_id),),
            )

    # -------------------------------------------------------------- idempotency

    def mark_update(self, update_id: int) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO mgmt_updates (update_id, seen_at) VALUES (?, ?)",
                (int(update_id), self._ts()),
            )
            return cur.rowcount > 0

    def audit(self, event: str, detail: str = "") -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO mgmt_audit (event, detail, created_at) VALUES (?, ?, ?)",
                (event[:200], detail[:2000], self._ts()),
            )

    # --------------------------------------------------------------- callbacks

    def issue_callback(
        self,
        *,
        owner_id: int,
        chat_id: int,
        action: str,
        payload: dict[str, Any] | None = None,
        snapshot_id: str | None = None,
        ttl_s: int = CALLBACK_TTL_S,
    ) -> str:
        token = secrets.token_urlsafe(24)
        now = self._now()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mgmt_callbacks
                    (token, owner_id, chat_id, action, payload_json, snapshot_id,
                     expires_at, used_at, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?)
                """,
                (
                    token,
                    int(owner_id),
                    int(chat_id),
                    action[:80],
                    json.dumps(payload or {}, separators=(",", ":"), ensure_ascii=False),
                    snapshot_id,
                    _iso(now + timedelta(seconds=ttl_s)),
                    _iso(now),
                ),
            )
        return token

    def consume_callback(
        self, token: str, *, owner_id: int, chat_id: int
    ) -> tuple[str, dict[str, Any] | None]:
        """Consume exactly once; every binding is checked before any state change."""

        now = self._now()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM mgmt_callbacks WHERE token = ?", (str(token),)
            ).fetchone()
            if row is None:
                return "missing", None
            data = dict(row)
            if int(data["owner_id"]) != int(owner_id):
                return "wrong_owner", None
            if int(data["chat_id"]) != int(chat_id):
                return "wrong_chat", None
            if data["used_at"] is not None:
                return "replayed", data
            expires = _parse(data["expires_at"])
            if expires is None or expires <= now:
                return "expired", data
            cur = conn.execute(
                "UPDATE mgmt_callbacks SET used_at = ? WHERE token = ? AND used_at IS NULL",
                (_iso(now), str(token)),
            )
            if cur.rowcount != 1:  # pragma: no cover - serialized by BEGIN IMMEDIATE
                return "replayed", data
        return "ok", data

    def invalidate_callbacks(self, operator_id: int | None = None) -> int:
        """Burn outstanding buttons so a cancelled/expired flow cannot resume."""

        with self.db.transaction() as conn:
            if operator_id is None:
                cur = conn.execute(
                    "UPDATE mgmt_callbacks SET used_at = ? WHERE used_at IS NULL",
                    (self._ts(),),
                )
            else:
                cur = conn.execute(
                    "UPDATE mgmt_callbacks SET used_at = ? "
                    "WHERE used_at IS NULL AND owner_id = ?",
                    (self._ts(), int(operator_id)),
                )
            return cur.rowcount

    # --------------------------------------------------------------- snapshots

    def create_snapshot(self, channels: list[Any], *, operator_id: int) -> str:
        snapshot_id = secrets.token_urlsafe(12)
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO mgmt_snapshots "
                "(snapshot_id, created_at, channel_count, operator_id) VALUES (?, ?, ?, ?)",
                (snapshot_id, now, len(channels), int(operator_id)),
            )
            for position, channel in enumerate(channels):
                conn.execute(
                    """
                    INSERT INTO mgmt_snapshot_channels
                        (snapshot_id, position, channel_id, access_hash, title,
                         username, kind, last_activity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        position,
                        int(channel.channel_id),
                        int(channel.access_hash),
                        str(channel.title),
                        channel.username,
                        str(channel.kind),
                        getattr(channel, "last_activity", None),
                    ),
                )
            conn.execute(
                """
                INSERT INTO mgmt_operator_channels
                    (operator_id, selection_snapshot_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(operator_id) DO UPDATE SET
                    selection_snapshot_id = excluded.selection_snapshot_id,
                    updated_at = excluded.updated_at
                """,
                (int(operator_id), snapshot_id, now),
            )
            # Keep two snapshots *per operator* so in-flight callbacks can be
            # classified as stale without destroying the other operator's page.
            rows = conn.execute(
                "SELECT snapshot_id FROM mgmt_snapshots WHERE operator_id = ? "
                "ORDER BY created_at DESC, rowid DESC",
                (int(operator_id),),
            ).fetchall()
            for old in rows[2:]:
                conn.execute(
                    "DELETE FROM mgmt_snapshot_channels WHERE snapshot_id = ?",
                    (old["snapshot_id"],),
                )
                conn.execute(
                    "DELETE FROM mgmt_snapshots WHERE snapshot_id = ?", (old["snapshot_id"],)
                )
        return snapshot_id

    def current_snapshot(self, operator_id: int) -> str | None:
        return self.operator_channel(operator_id).get("selection_snapshot_id")

    def snapshot_count(self, snapshot_id: str) -> int:
        row = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM mgmt_snapshot_channels WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        return int(row["n"] or 0)

    def snapshot_page(self, snapshot_id: str, offset: int, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            """
            SELECT * FROM mgmt_snapshot_channels
            WHERE snapshot_id = ? ORDER BY position LIMIT ? OFFSET ?
            """,
            (snapshot_id, max(1, min(int(limit), 10)), max(0, int(offset))),
        ).fetchall()
        return [dict(row) for row in rows]

    def snapshot_channel(
        self, snapshot_id: str, channel_id: int, *, operator_id: int
    ) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            """
            SELECT c.* FROM mgmt_snapshot_channels c
            JOIN mgmt_snapshots s ON s.snapshot_id = c.snapshot_id
            WHERE c.snapshot_id = ? AND c.channel_id = ? AND s.operator_id = ?
            """,
            (snapshot_id, int(channel_id), int(operator_id)),
        ).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------- jobs

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM mgmt_ingestions WHERE job_id = ?", (job_id,)
        ).fetchone()
        return dict(row) if row else None

    def latest_job(self, channel_id: int | None = None) -> dict[str, Any] | None:
        if channel_id is None:
            row = self.db.conn.execute(
                "SELECT * FROM mgmt_ingestions ORDER BY created_at DESC, rowid DESC LIMIT 1"
            ).fetchone()
        else:
            row = self.db.conn.execute(
                "SELECT * FROM mgmt_ingestions WHERE channel_id = ? "
                "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (int(channel_id),),
            ).fetchone()
        return dict(row) if row else None

    def active_job(self, channel_id: int | None = None) -> dict[str, Any] | None:
        marks = ",".join("?" for _ in JOB_ACTIVE)
        params: list[Any] = list(JOB_ACTIVE)
        sql = f"SELECT * FROM mgmt_ingestions WHERE status IN ({marks})"
        if channel_id is not None:
            sql += " AND channel_id = ?"
            params.append(int(channel_id))
        sql += " ORDER BY created_at DESC LIMIT 1"
        row = self.db.conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def create_job(
        self,
        channel_id: int,
        channel_title: str,
        *,
        newest_id: int | None = None,
        max_messages: int | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        existing = self.active_job(channel_id)
        if existing is not None:
            return existing
        job_id = f"job-{secrets.token_hex(8)}"
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO mgmt_ingestions
                    (job_id, channel_id, channel_title, status, checkpoint_next_max_id,
                     newest_seen_id, max_messages, request_id, created_at, updated_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    int(channel_id),
                    str(channel_title),
                    int(newest_id) + 1 if newest_id is not None else None,
                    newest_id,
                    int(max_messages) if max_messages is not None else None,
                    request_id,
                    now,
                    now,
                ),
            )
        result = self.get_job(job_id)
        assert result is not None
        return result

    def update_job(self, job_id: str, **fields: Any) -> dict[str, Any]:
        unknown = set(fields) - JOB_FIELDS
        if unknown:
            raise ValueError(f"unknown ingestion fields: {sorted(unknown)}")
        if not fields:
            job = self.get_job(job_id)
            assert job is not None
            return job
        if "cancel_requested" in fields:
            fields["cancel_requested"] = 1 if fields["cancel_requested"] else 0
        assignments = ", ".join(f"{key} = ?" for key in fields)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE mgmt_ingestions SET {assignments}, updated_at = ? WHERE job_id = ?",
                [*fields.values(), self._ts(), job_id],
            )
        job = self.get_job(job_id)
        assert job is not None
        return job

    def claim_job(self, job_id: str) -> dict[str, Any] | None:
        now = self._ts()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE mgmt_ingestions SET status = 'running', started_at = COALESCE(started_at, ?), "
                "updated_at = ?, error = NULL WHERE job_id = ? AND status IN ('pending', 'interrupted')",
                (now, now, job_id),
            )
            if cur.rowcount != 1:
                return None
        return self.get_job(job_id)

    def recover_jobs(self) -> int:
        """A process restart must never leave a job looking active forever."""

        now = self._ts()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "UPDATE mgmt_ingestions SET status = 'interrupted', "
                "error = 'process stopped during ingestion; checkpoint preserved', updated_at = ? "
                "WHERE status = 'running'",
                (now,),
            )
            return cur.rowcount

    def request_cancel(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job["status"] in ("completed", "cancelled", "failed", "partial"):
            return job
        if job["status"] == "pending":
            return self.update_job(
                job_id,
                status="cancelled",
                cancel_requested=True,
                completed_at=self._ts(),
                error="cancelled by owner before start",
            )
        return self.update_job(job_id, cancel_requested=True)

    def resume_job(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)
        if job["status"] not in JOB_RESUMABLE and job["status"] != "pending":
            raise ValueError("job is not resumable")
        return self.update_job(
            job_id,
            status="pending",
            cancel_requested=False,
            completed_at=None,
            error=None,
        )

    def finish_job(
        self,
        job_id: str,
        status: str,
        *,
        error: str | None = None,
        checkpoint_next_max_id: int | None = None,
    ) -> dict[str, Any]:
        if status not in ("completed", "partial", "failed", "cancelled", "interrupted"):
            raise ValueError("invalid final ingestion status")
        return self.update_job(
            job_id,
            status=status,
            error=(error or "")[:2000] or None,
            checkpoint_next_max_id=checkpoint_next_max_id,
            completed_at=self._ts() if status != "interrupted" else None,
        )
