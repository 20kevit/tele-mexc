"""Durable state for the single-owner control bot.

All state transitions are transactional. No plaintext phone number, login
code, two-step password, bot token, or exchange secret is stored here.
"""

from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import Database
from .secretbox import SecretBox, SecretBoxError

FLOW_TTL_S = 900
SENSITIVE_FLOW_STATES = {
    "auth_phone",
    "auth_code",
    "auth_password",
    "mexc_key",
    "mexc_secret",
}
SENSITIVE_PAYLOAD_KEYS = {
    "phone",
    "phone_number",
    "code",
    "password",
    "two_step_password",
    "api_key",
    "secret",
    "api_secret",
    "phone_code_hash",
}

DEFAULT_SETTINGS: dict[str, str] = {
    "buy_quote_usd": "10",
    "sell_pct_free": "25",
    "learning_target": "5",
    "sensitive_cleanup_s": "0",
    "channel_page_size": "10",
}


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


SETUP_FIELDS = {
    "setup_state",
    "telegram_state",
    "telegram_authorized_at",
    "telegram_account_label",
    "mexc_state",
    "mexc_verified_at",
    "mexc_label",
    "selected_channel_id",
    "selected_channel_title",
    "learning_state",
    "learning_streak",
    "learning_completed_at",
    "paused",
    "pause_reason",
}


class ControlStore:
    def __init__(self, db: Database, now: Any | None = None):
        self.db = db
        self._now = now or (lambda: datetime.now(UTC))

    # ------------------------------------------------------------- helpers

    def _ts(self) -> str:
        return _iso(self._now())

    def _get_setup_row(self) -> dict[str, Any]:
        row = self.db.conn.execute("SELECT * FROM control_setup WHERE id = 1").fetchone()
        if row is None:  # migration always inserts it; defensive for old copies
            self.db.conn.execute("INSERT OR IGNORE INTO control_setup (id, updated_at) VALUES (1, ?)", (self._ts(),))
            row = self.db.conn.execute("SELECT * FROM control_setup WHERE id = 1").fetchone()
        assert row is not None
        return dict(row)

    def setup(self) -> dict[str, Any]:
        data = self._get_setup_row()
        data["effective_state"] = self.effective_state(data)
        return data

    def update_setup(self, **fields: Any) -> dict[str, Any]:
        unknown = set(fields) - SETUP_FIELDS
        if unknown:
            raise ValueError(f"unknown setup fields: {sorted(unknown)}")
        if not fields:
            return self.setup()
        with self.db.transaction() as conn:
            self.update_setup_in_conn(conn, **fields)
        return self.setup()

    def update_setup_in_conn(self, conn: Any, **fields: Any) -> None:
        """Apply setup fields on a caller-owned transaction/connection."""

        unknown = set(fields) - SETUP_FIELDS
        if unknown:
            raise ValueError(f"unknown setup fields: {sorted(unknown)}")
        if not fields:
            return
        sets = []
        params: list[Any] = []
        for key, value in fields.items():
            if key == "paused":
                value = 1 if value else 0
            sets.append(f"{key} = ?")
            params.append(value)
        sets.append("updated_at = ?")
        params.append(self._ts())
        conn.execute(f"UPDATE control_setup SET {', '.join(sets)} WHERE id = 1", params)

    def effective_state(self, setup: dict[str, Any] | None = None) -> str:
        s = setup or self._get_setup_row()
        if int(s.get("paused") or 0):
            return "paused"
        if s.get("telegram_state") == "pending":
            return "telegram_auth_pending"
        if s.get("telegram_state") != "authenticated":
            return "not_configured"
        if s.get("mexc_state") == "pending":
            return "mexc_config_pending"
        if s.get("mexc_state") != "verified":
            return "telegram_authenticated"
        if not s.get("selected_channel_id"):
            return "channel_selection_pending"
        if s.get("learning_state") == "in_progress":
            return "learning_in_progress"
        if s.get("learning_state") == "completed":
            return "automatic"
        return "channel_selected"

    def refresh_setup_state(self) -> str:
        state = self.effective_state()
        self.update_setup(setup_state=state)
        return state

    def refresh_setup_state_in_conn(self, conn: Any) -> str:
        row = conn.execute("SELECT * FROM control_setup WHERE id = 1").fetchone()
        if row is None:
            raise ValueError("control_setup row is missing")
        state = self.effective_state(dict(row))
        self.update_setup_in_conn(conn, setup_state=state)
        return state

    # ------------------------------------------------------------ settings

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        row = self.db.conn.execute("SELECT value FROM control_settings WHERE key = ?", (key,)).fetchone()
        if row is not None:
            return str(row["value"])
        return DEFAULT_SETTINGS.get(key, default)

    def get_int_setting(self, key: str, default: int) -> int:
        try:
            return int(str(self.get_setting(key, str(default))))
        except (TypeError, ValueError):
            return default

    def get_float_setting(self, key: str, default: float) -> float:
        try:
            return float(str(self.get_setting(key, str(default))))
        except (TypeError, ValueError):
            return default

    def set_setting(self, key: str, value: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO control_settings (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, value, self._ts()),
            )

    def settings(self) -> dict[str, Any]:
        return {
            "buy_quote_usd": self.get_float_setting("buy_quote_usd", 10.0),
            "sell_pct_free": self.get_float_setting("sell_pct_free", 25.0),
            "learning_target": max(1, self.get_int_setting("learning_target", 5)),
            "sensitive_cleanup_s": max(0, self.get_int_setting("sensitive_cleanup_s", 0)),
            "channel_page_size": min(10, max(1, self.get_int_setting("channel_page_size", 10))),
        }

    # -------------------------------------------------- notification settings

    def get_notification_setting(self, role: str, category: str) -> bool | None:
        row = self.db.conn.execute(
            "SELECT enabled FROM notification_preferences WHERE role = ? AND category = ?",
            (role, category),
        ).fetchone()
        if row is None:
            return None
        return bool(int(row["enabled"]))

    def set_notification_setting(self, role: str, category: str, enabled: bool) -> None:
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO notification_preferences (role, category, enabled, updated_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(role, category) DO UPDATE SET "
                "enabled = excluded.enabled, updated_at = excluded.updated_at",
                (role, category, 1 if enabled else 0, now),
            )
        self.audit(
            f"notification_{role}_{category}",
            f"{'enabled' if enabled else 'disabled'}: notification {category} for {role}",
        )

    def list_notification_settings(self, role: str | None = None) -> list[dict[str, Any]]:
        if role is None:
            rows = self.db.conn.execute("SELECT * FROM notification_preferences ORDER BY role, category").fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM notification_preferences WHERE role = ? ORDER BY category",
                (role,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ---------------------------------------------------------------- flow

    def set_flow(self, state: str, payload: dict[str, Any] | None = None, ttl_s: int = FLOW_TTL_S) -> None:
        now = self._now()
        clean = {k: v for k, v in (payload or {}).items() if k not in SENSITIVE_PAYLOAD_KEYS}
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE control_flow SET state = ?, payload = ?, updated_at = ?, expires_at = ? WHERE id = 1",
                (
                    state,
                    json.dumps(clean, separators=(",", ":")) if clean else None,
                    _iso(now),
                    _iso(now + timedelta(seconds=ttl_s)),
                ),
            )

    def flow(self) -> dict[str, Any]:
        row = self.db.conn.execute("SELECT * FROM control_flow WHERE id = 1").fetchone()
        if row is None:
            return {"state": "idle", "payload": {}, "expired": False}
        expires = _parse(row["expires_at"])
        expired = bool(expires and expires <= self._now())
        payload: dict[str, Any] = {}
        if row["payload"]:
            try:
                parsed = json.loads(row["payload"])
                payload = parsed if isinstance(parsed, dict) else {}
            except ValueError:
                payload = {}
        return {"state": row["state"], "payload": payload, "expired": expired, "expires_at": row["expires_at"]}

    def clear_flow(self) -> None:
        self.set_flow("idle")

    # ---------------------------------------------------------- idempotency

    def mark_update(self, update_id: int) -> bool:
        """Return True exactly once for each Telegram update id."""

        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO control_updates (update_id, seen_at) VALUES (?, ?)",
                (int(update_id), self._ts()),
            )
            return cur.rowcount > 0

    # --------------------------------------------------------------- audit

    def audit(self, event: str, detail: str = "") -> None:
        with self.db.transaction() as conn:
            self.audit_in_conn(conn, event, detail)

    def audit_in_conn(self, conn: Any, event: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO control_audit (event, detail, created_at) VALUES (?, ?, ?)",
            (event[:200], detail[:2000], self._ts()),
        )

    # ------------------------------------------------------------ learning

    def add_learning_sample(
        self,
        *,
        channel_id: int,
        message_id: int,
        edit_version: int,
        message_text: str,
        extracted: dict[str, Any],
        parser_kind: str,
        actionable: bool,
    ) -> tuple[dict[str, Any], bool]:
        sample_id = f"ls-{secrets.token_hex(8)}"
        now = self._ts()
        with self.db.transaction() as conn:
            seq_row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) + 1 AS n FROM control_learning_samples"
            ).fetchone()
            sequence = int(seq_row["n"])
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO control_learning_samples
                    (sample_id, channel_id, message_id, edit_version, message_text,
                     extracted_json, parser_kind, actionable, decision, sequence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    sample_id,
                    channel_id,
                    message_id,
                    edit_version,
                    message_text,
                    json.dumps(extracted, ensure_ascii=False, separators=(",", ":")),
                    parser_kind,
                    1 if actionable else 0,
                    sequence,
                    now,
                ),
            )
            if cur.rowcount == 0:
                row = conn.execute(
                    "SELECT * FROM control_learning_samples WHERE channel_id = ? AND message_id = ? "
                    "AND edit_version = ?",
                    (channel_id, message_id, edit_version),
                ).fetchone()
                assert row is not None
                return dict(row), False
        row = self.get_learning_sample(sample_id)
        assert row is not None
        return row, True

    def get_learning_sample(self, sample_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM control_learning_samples WHERE sample_id = ?", (sample_id,)
        ).fetchone()
        if row is None:
            return None
        out = dict(row)
        try:
            out["extracted"] = json.loads(out["extracted_json"])
        except ValueError:
            out["extracted"] = {}
        return out

    def pending_learning_sample(self) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT sample_id FROM control_learning_samples WHERE decision = 'pending' ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return self.get_learning_sample(str(row["sample_id"])) if row else None

    def latest_learning_sample(self) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT sample_id FROM control_learning_samples ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return self.get_learning_sample(str(row["sample_id"])) if row else None

    def learning_examples(self, limit: int = 5) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT sample_id FROM control_learning_samples WHERE decision = 'approved' ORDER BY sequence DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [s for r in rows if (s := self.get_learning_sample(str(r["sample_id"]))) is not None]

    def decide_learning_sample(self, sample_id: str, approved: bool, reason: str = "") -> dict[str, Any]:
        now = self._ts()
        decision = "approved" if approved else "rejected"
        with self.db.transaction() as conn:
            row = conn.execute("SELECT * FROM control_learning_samples WHERE sample_id = ?", (sample_id,)).fetchone()
            if row is None:
                raise ValueError("learning sample not found")
            if row["decision"] != "pending":
                raise ValueError("this example was already decided")
            if approved and not int(row["actionable"]):
                raise ValueError("a non-signal or ambiguous extraction cannot be approved")
            conn.execute(
                "UPDATE control_learning_samples SET decision = ?, decision_reason = ?, decided_at = ? "
                "WHERE sample_id = ?",
                (decision, reason[:500], now, sample_id),
            )
        self.recompute_learning_state()
        result = self.get_learning_sample(sample_id)
        assert result is not None
        return result

    def recompute_learning_state(self) -> int:
        """Recompute the consecutive-approval streak from persisted decisions.

        Only actionable valid examples participate. A rejected actionable
        example resets the streak; non-actionable rejections neither count nor
        break it (they were never eligible training data).
        """

        rows = self.db.conn.execute(
            "SELECT actionable, decision FROM control_learning_samples "
            "WHERE decision IN ('approved', 'rejected') ORDER BY sequence"
        ).fetchall()
        streak = 0
        for row in rows:
            if not int(row["actionable"]):
                continue
            if row["decision"] == "approved":
                streak += 1
            else:
                streak = 0
        target = max(1, self.get_int_setting("learning_target", 5))
        setup = self._get_setup_row()
        completed_at = setup.get("learning_completed_at")
        learning_state = setup.get("learning_state") or "not_started"
        if streak >= target:
            learning_state = "completed"
            completed_at = completed_at or self._ts()
        elif learning_state == "completed" and streak < target:
            # A later rejection invalidates completion until the owner restarts.
            learning_state = "in_progress"
            completed_at = None
        elif streak > 0 or rows:
            learning_state = "in_progress"
        self.update_setup(
            learning_streak=streak,
            learning_state=learning_state,
            learning_completed_at=completed_at,
        )
        self.refresh_setup_state()
        return streak

    def reset_learning(self) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM control_learning_samples")
        self.update_setup(
            learning_state="in_progress",
            learning_streak=0,
            learning_completed_at=None,
        )
        self.audit("learning_reset", "owner explicitly restarted the learning phase")
        self.refresh_setup_state()

    # -------------------------------------------------------------- notices

    def notice_once(self, intent_id: str, kind: str) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO control_notices (intent_id, kind, created_at) VALUES (?, ?, ?)",
                (intent_id, kind, self._ts()),
            )
            return cur.rowcount > 0

    # -------------------------------------------------------------- secrets

    def put_secret(self, name: str, plaintext: str, box: SecretBox) -> None:
        if not box.available:
            raise SecretBoxError("CONTROL_SECRET_KEY is required to store exchange credentials")
        enc = box.encrypt(plaintext)
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO control_secrets
                    (name, alg, salt, nonce, ciphertext, mac, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    alg = excluded.alg, salt = excluded.salt, nonce = excluded.nonce,
                    ciphertext = excluded.ciphertext, mac = excluded.mac,
                    updated_at = excluded.updated_at
                """,
                (
                    name,
                    enc["alg"],
                    enc["salt"],
                    enc["nonce"],
                    enc["ciphertext"],
                    enc["mac"],
                    now,
                    now,
                ),
            )

    def get_secret(self, name: str, box: SecretBox) -> str | None:
        row = self.db.conn.execute("SELECT * FROM control_secrets WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        return box.decrypt(dict(row))

    def delete_secret(self, name: str) -> None:
        with self.db.transaction() as conn:
            conn.execute("DELETE FROM control_secrets WHERE name = ?", (name,))
