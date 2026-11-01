"""Authoritative pause and readiness gate for every new exchange order.

The gate is deliberately independent of the UI and of the order state machine:

* ``shared()`` is held for the whole interval between claiming/checking and the
  exchange call.  ``exclusive()`` is used by pause/resume.  It is a kernel
  ``flock``, so a crash releases it and two processes cannot race a stale
  in-memory flag.
* Pause state, setup readiness and emergency stop are read from SQLite inside a
  ``BEGIN IMMEDIATE`` transaction.  For an entry claim the same check runs in
  the same transaction as the ``planned -> submitting`` update, so readiness
  cannot change between validation and claim.
* Any read/parse failure is reported as a blocker.  There is no fallback to
  "assume ready".
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from ..config import AppConfig

# Purposes are intentionally explicit: protective exits must remain possible
# when a *setup* prerequisite (channel/session) has changed, but they still
# stop on pause or emergency stop; there is no acknowledgement gate.
# pipeline.
ENTRY = "entry"
PROTECTION = "protection"
ALL_PURPOSES = (ENTRY, PROTECTION)


class SubmissionBlocked(RuntimeError):
    """A new order must not be sent; ``reason`` is safe for operators."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class GateTimeout(RuntimeError):
    """The pause/resume lock could not be acquired within the deadline."""


class SubmissionGate:
    def __init__(
        self,
        db: Any,
        cfg: AppConfig,
        *,
        runtime: dict[str, Callable[[], bool | None]] | None = None,
        lock_timeout_s: float = 30.0,
    ):
        self.db = db
        self.cfg = cfg
        # Diagnostic callbacks (Telegram connected, channel resolved, boundary
        # armed).  Owner policy is explicit: these never block entries.  They are
        # retained for status/diagnostics only and are not read by ``check``.
        self.runtime = dict(runtime or {})
        self.lock_timeout_s = max(0.1, float(lock_timeout_s))
        self._thread_lock = threading.RLock()
        self._path = self._lock_path(getattr(db, "path", None))

    @staticmethod
    def _lock_path(db_path: Any) -> Path | None:
        if db_path is None:
            return None
        raw = str(db_path)
        if not raw or raw == ":memory:" or raw.startswith("file:"):
            return None
        return Path(raw + ".submission.lock")

    # ------------------------------------------------------------- locking

    @contextlib.contextmanager
    def _flock(self, mode: int, *, timeout_s: float | None = None) -> Iterator[None]:
        timeout = self.lock_timeout_s if timeout_s is None else max(0.1, timeout_s)
        if self._path is None:
            with self._thread_lock:
                yield
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR, 0o600)
        deadline = time.monotonic() + timeout
        try:
            while True:
                try:
                    fcntl.flock(fd, mode | fcntl.LOCK_NB)
                    break
                except (BlockingIOError, OSError) as exc:
                    if time.monotonic() >= deadline:
                        raise GateTimeout("timed out waiting for the order-submission gate") from exc
                    time.sleep(0.02)
            yield
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def shared(self) -> contextlib.AbstractContextManager[None]:
        """Hold while a submission is being validated and sent."""

        return self._flock(fcntl.LOCK_SH)

    def exclusive(self, *, timeout_s: float | None = None) -> contextlib.AbstractContextManager[None]:
        """Block new submissions (pause) or reopen them (resume)."""

        return self._flock(fcntl.LOCK_EX, timeout_s=timeout_s)

    # ----------------------------------------------------------- readiness

    def _buy_block_reason(self, conn: sqlite3.Connection) -> str | None:
        """Return the persisted new-BUY block reason, or None when clear."""

        try:
            row = conn.execute(
                "SELECT value FROM app_state WHERE key = 'trading.new_buys_blocked'"
            ).fetchone()
        except sqlite3.Error:
            return "new-BUY block state is unreadable; failing closed"
        if row is None or str(row[0]) != "1":
            return None
        try:
            reason_row = conn.execute(
                "SELECT value FROM app_state WHERE key = 'trading.new_buys_block_reason'"
            ).fetchone()
        except sqlite3.Error:
            reason_row = None
        reason = str(reason_row[0]) if reason_row is not None and reason_row[0] else ""
        return reason or "an open position lacks verified exit protection"

    @staticmethod
    def _setup_row(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute("SELECT * FROM control_setup WHERE id = 1").fetchone()

    def _pause_reason(self, conn: sqlite3.Connection) -> str | None:
        row = self._setup_row(conn)
        if row is None:
            return "trading pause state is unavailable; failing closed"
        if int(row["paused"] or 0):
            reason = str(row["pause_reason"] or "owner paused execution")
            return f"trading is paused: {reason}"
        return None

    def check(
        self,
        conn: sqlite3.Connection,
        *,
        purpose: str = ENTRY,
        ignore_pause: bool = False,
    ) -> str | None:
        """Return a blocker description, or None when submission is allowed."""

        if purpose not in ALL_PURPOSES:
            return f"unknown submission purpose {purpose!r}"
        if self.cfg.trading.emergency_stop:
            return "emergency stop is active"
        if not ignore_pause:
            pause_reason = self._pause_reason(conn)
            if pause_reason:
                return pause_reason
        if purpose == ENTRY:
            # The persisted BUY block is a real entry gate: it stops every new
            # BUY while an unprotected position exists.  It never touches
            # protection/exit submissions, and it never cancels an order.
            block_reason = self._buy_block_reason(conn)
            if block_reason:
                return f"new BUYs are blocked: {block_reason}"
        # There is intentionally no account-wide/history-wide entry lock here.
        # Historical trade reconciliation state is per-order/per-trade and must
        # never disable unrelated symbols.  New BUY entries are guarded by the
        # exchange-backed same-symbol open-order check in the engine.
        return None

    def precheck(self, conn: sqlite3.Connection, *, purpose: str = ENTRY) -> str | None:
        """Check inside an existing transaction (used by the atomic claim)."""

        return self.check(conn, purpose=purpose)

    def resume_blocker_in_transaction(self, conn: sqlite3.Connection) -> str | None:
        """Evaluate resume readiness on the caller's transaction/connection.

        The caller must already hold the exclusive submission lock and must
        keep using this connection for the unpause update.  Ignoring only the
        paused flag preserves every other entry prerequisite.
        """

        return self.check(conn, purpose=ENTRY, ignore_pause=True)

    @contextlib.contextmanager
    def submission(self, *, purpose: str = ENTRY) -> Iterator[None]:
        """Hold a shared gate and fail closed unless readiness is complete."""

        with self.shared():
            try:
                with self.db.transaction() as conn:
                    reason = self.check(conn, purpose=purpose)
            except Exception as exc:  # noqa: BLE001 - unreadable state fails closed
                raise SubmissionBlocked(f"readiness check failed ({exc.__class__.__name__}); failing closed") from exc
            if reason:
                raise SubmissionBlocked(reason)
            yield

    def resume_blocker(self) -> str | None:
        """Return why resume must be refused, ignoring the paused flag itself."""

        try:
            with self.db.transaction() as conn:
                return self.check(conn, purpose=ENTRY, ignore_pause=True)
        except Exception as exc:  # noqa: BLE001
            return f"readiness check failed ({exc.__class__.__name__}); failing closed"
