"""Single-owner guard for the shared Telethon session file.

The management bot, the ingestion worker, and the CLI all use the same
``tg_session.session`` file. Telethon/SQLite does not safely support concurrent
writers, so this module combines an in-process ``asyncio.Lock`` with a
cross-process ``flock``. A login flow holds the guard for its whole interactive
lifetime; short read-only operations hold it only around connect/read/disconnect.

The lock is advisory and kernel-owned: a crashed process releases it
automatically. It is deliberately a separate file so it does not modify or
delete the session database.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from .config import AppConfig
from .locking import AlreadyRunningError, SingleInstanceLock
from .telethon import resolve_session_file


class SessionBusyError(RuntimeError):
    """Another operator or process is currently using the session."""

    def __init__(self, message: str = "the Telegram session is currently in use"):
        super().__init__(message)


def session_lock_path(cfg: AppConfig) -> Path:
    _, session_file = resolve_session_file(cfg)
    return session_file.with_name(session_file.name + ".lock")


class SessionGuard:
    def __init__(self, cfg: AppConfig):
        self._lock = asyncio.Lock()
        self._lock_path = session_lock_path(cfg)
        self._lease: SingleInstanceLock | None = None
        self._holder: int | None = None

    @property
    def locked(self) -> bool:
        return self._lock.locked()

    @property
    def holder(self) -> int | None:
        return self._holder

    async def acquire(self, *, operator_id: int | None = None) -> None:
        if self._lock.locked():
            raise SessionBusyError()
        await self._lock.acquire()
        lease = SingleInstanceLock(self._lock_path)
        try:
            lease.acquire()
        except AlreadyRunningError:
            self._lock.release()
            raise SessionBusyError(
                "the Telegram session is locked by another process"
            ) from None
        except Exception:
            self._lock.release()
            raise
        self._lease = lease
        self._holder = operator_id

    def release(self, *, operator_id: int | None = None) -> None:
        """Release only if the caller owns the guard (or no owner was recorded)."""

        if self._lease is None:
            return
        if (
            operator_id is not None
            and self._holder is not None
            and int(operator_id) != int(self._holder)
        ):
            return
        lease, self._lease = self._lease, None
        self._holder = None
        lease.release()
        if self._lock.locked():
            self._lock.release()

    @asynccontextmanager
    async def use(self, *, operator_id: int | None = None) -> AsyncIterator[None]:
        await self.acquire(operator_id=operator_id)
        try:
            yield
        finally:
            self.release(operator_id=operator_id)
