"""Cross-process single-instance lock for the production ``serve`` command.

The lock is an advisory ``flock`` held for the whole service lifetime. The
kernel releases it automatically when the process exits, crashes, or is
killed, so a stale lock file never blocks a restart. Owner metadata is written
only for operator diagnostics; process identity is never trusted as the lock
itself.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class AlreadyRunningError(RuntimeError):
    """Another process currently holds the service lock."""

    def __init__(self, lock_path: Path, owner: dict[str, Any] | None):
        self.lock_path = lock_path
        self.owner = owner or {}
        pid = self.owner.get("pid")
        detail = f" (pid {pid})" if pid else ""
        super().__init__(f"another serve instance already holds {lock_path}{detail}")


def _read_owner(path: Path) -> dict[str, Any] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


class SingleInstanceLock:
    """Advisory exclusive lock scoped to one database/service."""

    def __init__(self, lock_path: str | Path):
        self.path = Path(lock_path)
        self._fd: int | None = None

    @classmethod
    def for_database(cls, db_path: str | Path) -> SingleInstanceLock:
        db = Path(db_path)
        return cls(db.with_name(db.name + ".serve.lock"))

    @classmethod
    def for_control(cls, db_path: str | Path) -> SingleInstanceLock:
        """Separate lock so only one process polls the control bot token."""

        db = Path(db_path)
        return cls(db.with_name(db.name + ".control.lock"))

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self) -> SingleInstanceLock:
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            owner = _read_owner(self.path)
            os.close(fd)
            raise AlreadyRunningError(self.path, owner) from exc
        metadata = json.dumps(
            {"pid": os.getpid(), "lock_path": str(self.path)}, separators=(",", ":")
        ).encode("utf-8")
        os.ftruncate(fd, 0)
        os.write(fd, metadata)
        os.fsync(fd)
        self._fd = fd
        return self

    def release(self) -> None:
        import fcntl

        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def __enter__(self) -> SingleInstanceLock:
        return self.acquire()

    def __exit__(self, *_exc: object) -> None:
        self.release()
