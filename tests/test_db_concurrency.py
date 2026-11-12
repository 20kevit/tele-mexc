"""Deterministic SQLite isolation tests for the concurrent lifecycle workers.

The trading lifecycle is no longer serialised by one process-wide lock, so
several threads can hold TradeManager locks at once.  These tests pin the
properties the ``Database`` wrapper must keep for every handle type:

* a reader on another thread never observes an uncommitted transaction,
* concurrent writers are serialised and all of their commits survive,
* ``:memory:`` and read-only handles are per-thread too (no shared connection),
* ``close()`` is idempotent, waits for an in-flight writer, and never deadlocks
  against an active reader.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from tg_signal_ingest.db import Database, DatabaseClosedError


def _create_probe(db: Database) -> None:
    with db.transaction() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS probe (id INTEGER PRIMARY KEY, value TEXT)")
        conn.execute("DELETE FROM probe")


def test_file_database_reuses_primary_only_in_creator_thread(tmp_path: Path) -> None:
    db = Database(tmp_path / "connections.sqlite3")
    try:
        primary = db._primary
        assert db.conn is primary

        worker_connections: list[object] = []

        def worker() -> None:
            worker_connections.append(db.conn)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(5)
        assert not thread.is_alive()
        assert len(worker_connections) == 1
        assert worker_connections[0] is not primary
    finally:
        db.close()


def test_reader_on_another_thread_cannot_see_uncommitted_transaction(tmp_path: Path) -> None:
    db = Database(tmp_path / "race.sqlite3")
    try:
        _create_probe(db)
        inserted = threading.Event()
        allow_commit = threading.Event()

        def writer() -> None:
            with db.transaction() as conn:
                conn.execute("INSERT INTO probe (id, value) VALUES (1, 'pending')")
                inserted.set()
                assert allow_commit.wait(5)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            assert inserted.wait(5)
            # A different thread must only ever see committed data.
            during = db.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
        finally:
            allow_commit.set()
            thread.join(5)
        assert not thread.is_alive()
        after = db.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0]
        assert (during, after) == (0, 1)
    finally:
        db.close()


def test_concurrent_writers_are_serialised_and_every_commit_persists(tmp_path: Path) -> None:
    db = Database(tmp_path / "writers.sqlite3")
    try:
        _create_probe(db)
        errors: list[BaseException] = []
        barrier = threading.Barrier(4)

        def worker(worker_id: int) -> None:
            try:
                barrier.wait(5)
                for index in range(10):
                    with db.transaction() as conn:
                        conn.execute(
                            "INSERT INTO probe (value) VALUES (?)", (f"{worker_id}-{index}",)
                        )
            except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(worker_id,)) for worker_id in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        assert errors == []
        assert all(not thread.is_alive() for thread in threads)
        assert db.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0] == 40
    finally:
        db.close()


def test_in_memory_database_uses_thread_local_connections_with_shared_data() -> None:
    """``:memory:`` is private per handle but consistent across its threads."""

    db = Database(":memory:")
    try:
        _create_probe(db)
        with db.transaction() as conn:
            conn.execute("INSERT INTO probe (id, value) VALUES (1, 'committed')")
        seen: list[int] = []
        connection_ids: list[int] = []

        def reader() -> None:
            seen.append(db.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0])
            connection_ids.append(id(db.conn))

        thread = threading.Thread(target=reader)
        thread.start()
        thread.join(5)
        assert not thread.is_alive()
        assert seen == [1]
        assert connection_ids and connection_ids[0] != id(db.conn)
    finally:
        db.close()


def test_in_memory_handles_are_isolated_from_each_other() -> None:
    first = Database(":memory:")
    second = Database(":memory:")
    try:
        _create_probe(first)
        with first.transaction() as conn:
            conn.execute("INSERT INTO probe (id, value) VALUES (1, 'first')")
        rows = second.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='probe'"
        ).fetchall()
        assert rows == []
    finally:
        first.close()
        second.close()


def test_read_only_handle_is_thread_local_and_sees_only_committed_rows(tmp_path: Path) -> None:
    writer = Database(tmp_path / "readonly.sqlite3")
    try:
        _create_probe(writer)
        with writer.transaction() as conn:
            conn.execute("INSERT INTO probe (id, value) VALUES (1, 'committed')")
        reader = Database(tmp_path / "readonly.sqlite3", read_only=True)
        try:
            seen: list[int] = []
            connection_ids: list[int] = []
            errors: list[BaseException] = []

            def read() -> None:
                try:
                    seen.append(reader.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0])
                    connection_ids.append(id(reader.conn))
                except BaseException as exc:  # noqa: BLE001 - surfaced below
                    errors.append(exc)

            threads = [threading.Thread(target=read) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(5)
            assert errors == []
            assert seen == [1, 1, 1]
            assert len(set(connection_ids)) == 3
        finally:
            reader.close()
    finally:
        writer.close()


def test_close_is_idempotent_and_post_close_use_fails_clearly(tmp_path: Path) -> None:
    db = Database(tmp_path / "closed.sqlite3")
    _create_probe(db)
    db.close()
    db.close()  # idempotent
    with pytest.raises(DatabaseClosedError):
        _ = db.conn


def test_close_waits_for_an_in_flight_writer_transaction(tmp_path: Path) -> None:
    db = Database(tmp_path / "closing.sqlite3")
    _create_probe(db)
    entered = threading.Event()
    allow_commit = threading.Event()
    closed = threading.Event()
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            with db.transaction() as conn:
                conn.execute("INSERT INTO probe (id, value) VALUES (1, 'kept')")
                entered.set()
                assert allow_commit.wait(5)
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)

    def closer() -> None:
        try:
            db.close()
            closed.set()
        except BaseException as exc:  # noqa: BLE001 - surfaced below
            errors.append(exc)

    writing = threading.Thread(target=writer)
    writing.start()
    assert entered.wait(5)
    closing = threading.Thread(target=closer)
    closing.start()
    # close() cannot finish while the writer still owns the transaction.
    assert closed.wait(0.2) is False
    allow_commit.set()
    writing.join(5)
    closing.join(5)
    assert errors == []
    assert closed.is_set()

    reopened = Database(tmp_path / "closing.sqlite3")
    try:
        assert reopened.conn.execute("SELECT COUNT(*) FROM probe").fetchone()[0] == 1
    finally:
        reopened.close()


def test_close_with_an_active_reader_does_not_deadlock(tmp_path: Path) -> None:
    db = Database(tmp_path / "active-reader.sqlite3")
    _create_probe(db)
    holding = threading.Event()
    release = threading.Event()
    observed: list[str] = []

    def reader() -> None:
        conn = db.conn
        conn.execute("BEGIN")
        conn.execute("SELECT COUNT(*) FROM probe").fetchone()
        holding.set()
        assert release.wait(5)
        try:
            conn.execute("SELECT COUNT(*) FROM probe").fetchone()
        except sqlite3.Error as exc:
            observed.append(type(exc).__name__)
        else:  # pragma: no cover - the connection was closed under the reader
            observed.append("ok")

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        assert holding.wait(5)
        db.close()  # returns without waiting for the reader
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive()
    assert observed == ["ProgrammingError"]
