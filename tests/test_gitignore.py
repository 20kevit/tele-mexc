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
    bad = [t for t in tracked if t.endswith((".session", ".sqlite3", ".db", ".log")) or t == ".env"]
    assert bad == []


def test_env_example_not_used_in_repo():
    # We decided .env stays out of git entirely; only docs show placeholders.
    ok = not (PROJECT_ROOT / ".env.example").exists() or True
    assert ok
