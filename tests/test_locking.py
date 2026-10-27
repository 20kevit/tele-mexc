"""Cross-process serve lock: concurrency, stale files and crash recovery."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from tg_signal_ingest import main
from tg_signal_ingest.locking import AlreadyRunningError, SingleInstanceLock

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_acquire_release_and_reacquire(tmp_path):
    lock = SingleInstanceLock(tmp_path / "serve.lock")
    lock.acquire()
    assert lock.held
    assert lock.path.is_file()
    lock.release()
    assert not lock.held
    lock.acquire()
    lock.release()


def test_second_lock_is_refused_with_owner_pid(tmp_path):
    path = tmp_path / "serve.lock"
    first = SingleInstanceLock(path).acquire()
    try:
        with pytest.raises(AlreadyRunningError) as exc:
            SingleInstanceLock(path).acquire()
        assert exc.value.owner.get("pid")
    finally:
        first.release()
    second = SingleInstanceLock(path).acquire()
    second.release()


def test_stale_metadata_without_a_live_lock_is_overwritten(tmp_path):
    path = tmp_path / "serve.lock"
    path.write_text('{"pid": 999999, "lock_path": "stale"}', encoding="utf-8")
    lock = SingleInstanceLock(path).acquire()
    try:
        assert lock.held
    finally:
        lock.release()


def test_abnormal_termination_releases_the_lock(tmp_path):
    path = tmp_path / "serve.lock"
    script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {str(PROJECT_ROOT)!r})
        from tg_signal_ingest.locking import SingleInstanceLock
        lock = SingleInstanceLock({str(path)!r}).acquire()
        print("locked", flush=True)
        time.sleep(30)
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "locked"
        # While the child holds the lock, the parent must be refused.
        with pytest.raises(AlreadyRunningError):
            SingleInstanceLock(path).acquire()
    finally:
        proc.terminate()
        proc.wait(timeout=10)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            lock = SingleInstanceLock(path).acquire()
        except AlreadyRunningError:
            time.sleep(0.05)
            continue
        lock.release()
        break
    else:  # pragma: no cover
        raise AssertionError("lock was not released after the holder was terminated")


def test_serve_command_exits_cleanly_when_lock_is_held(cfg, monkeypatch, capsys):
    lock = SingleInstanceLock.for_database(cfg.db_path)
    lock.acquire()
    calls: list[str] = []

    class FakeService:
        def __init__(self, *args, **kwargs):
            calls.append("constructed")

        async def run(self):
            calls.append("run")
            return 0

    monkeypatch.setattr(main, "_load_or_exit", lambda *a, **k: cfg)
    from tg_signal_ingest import service

    monkeypatch.setattr(service, "IngestService", FakeService)
    try:
        code = asyncio.run(main.cmd_serve(None))
    finally:
        lock.release()

    assert code == 1
    assert calls == []
    assert "already holds" in capsys.readouterr().err
