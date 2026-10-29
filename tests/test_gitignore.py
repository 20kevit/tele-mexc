"""Security-relevant exclusions must exist in .gitignore."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def tracked_violations(paths: list[str]) -> list[str]:
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", *paths],
        capture_output=True, text=True, cwd=PROJECT_ROOT, check=False,
    )
    ignored = set(result.stdout.split())
    return [p for p in paths if p not in ignored]


@pytest.mark.parametrize(
    "path",
    [
        ".env",
        "config/config.toml",
        "config/tg_session.session",
        "config/tg_session.session-journal",
        "other/session.session-wal",
        "other/session.session-shm",
        "other/archive.db-journal",
        "other/archive.db-wal",
        "other/archive.db-shm",
        "other/archive.sqlite-journal",
        "other/archive.sqlite-wal",
        "other/archive.sqlite-shm",
        "other/archive.sqlite3-journal",
        "other/archive.sqlite3-wal",
        "other/archive.sqlite3-shm",
        "data/ingest.sqlite3",
        "data/media/chart.png",
        "logs/ingest.log",
    ],
)
def test_private_files_are_ignored(path: str):
    assert tracked_violations([path]) == [], f"{path} must be git-ignored"


def test_no_session_or_db_files_tracked():
    result = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, cwd=PROJECT_ROOT, check=True
    )
    tracked = result.stdout.split()
    private_suffixes = tuple(
        extension + sidecar
        for extension in (".session", ".db", ".sqlite", ".sqlite3")
        for sidecar in ("", "-journal", "-wal", "-shm")
    )
    bad = [t for t in tracked if t.endswith((*private_suffixes, ".log")) or t == ".env"]
    assert bad == []


def test_env_example_is_not_ignored():
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", ".env.example"],
        capture_output=True, text=True, cwd=PROJECT_ROOT, check=False,
    )
    assert result.returncode == 1, result.stderr
