"""Durable state for the single-owner control bot.

All state transitions are transactional. No plaintext phone number, login
code, two-step password, bot token, or exchange secret is stored here.
"""

from __future__ import annotations

import json
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
        return "automatic"

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
            "sensitive_cleanup_s": max(0, self.get_int_setting("sensitive_cleanup_s", 0)),
            "channel_page_size": min(10, max(1, self.get_int_setting("channel_page_size", 10))),
        }

    # -------------------------------------------------- notification settings

    NOTIFICATION_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

    def notification_min_severity(self) -> str:
        value = (self.get_setting("notification_min_severity", "INFO") or "INFO").upper()
        return value if value in self.NOTIFICATION_LEVELS else "INFO"

    def set_notification_min_severity(self, level: str) -> None:
        value = str(level).upper()
        if value not in self.NOTIFICATION_LEVELS:
            raise ValueError(f"unknown notification level {level!r}")
        self.set_setting("notification_min_severity", value)
        self.audit("notification_level_changed", f"minimum severity={value}")

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
