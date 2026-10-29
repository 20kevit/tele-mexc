"""Runtime version information for the ``/version`` command.

The response proves which code is actually running.  Build/deployment
metadata captured during deployment is preferred over running Git commands
against a potentially different checkout.  Any field that cannot be proven is
reported as ``"unknown"`` rather than guessed.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

from . import __version__ as _package_version

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEPLOY_META_PATH = _PROJECT_ROOT / ".deploy-meta.json"
_START_TIME_UTC = time.time()


def _run(args: list[str]) -> str | None:
    """Run a command and return stdout, or None on any failure."""
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=3,
            cwd=str(_PROJECT_ROOT),
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (OSError, subprocess.SubprocessError, FileNotFoundError, ValueError):
        pass
    return None


def _git_sha() -> str | None:
    return _run(["git", "rev-parse", "HEAD"])


def _git_branch() -> str | None:
    return _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])


def _git_dirty() -> bool | None:
    out = _run(["git", "status", "--porcelain"])
    if out is None:
        return None
    return bool(out.strip())


def _load_deploy_meta() -> dict[str, Any]:
    """Load deployment metadata captured at deploy time, if present.

    The file is expected to be written by the deployment process and contain
    non-secret fields such as ``commit_sha``, ``branch``, ``built_at`` and
    ``deployed_at``.  It is intentionally read-only and never written by the
    running service.
    """
    try:
        if _DEPLOY_META_PATH.is_file():
            with open(_DEPLOY_META_PATH, encoding="utf-8") as fh:
                return json.load(fh)
    except (OSError, ValueError, json.JSONDecodeError):
        pass
    return {}


def get_version_info(
    *,
    trading_mode: str | None = None,
    live_authorized: bool | None = None,
    expected_commit_sha: str | None = None,
) -> dict[str, Any]:
    """Return a dictionary of version/runtime information for ``/version``.

    All values are safe to display to an authorized operator.  No credentials,
    tokens, or private paths are included.
    """

    deploy_meta = _load_deploy_meta()

    # Prefer deployment metadata for the running commit, then fall back to git.
    commit_sha: str | None = deploy_meta.get("commit_sha") or _git_sha()
    branch: str | None = deploy_meta.get("branch") or _git_branch()
    dirty: bool | None = deploy_meta.get("dirty")
    if dirty is None:
        dirty = _git_dirty()

    built_at = deploy_meta.get("built_at") or deploy_meta.get("built_at_utc")
    deployed_at = deploy_meta.get("deployed_at") or deploy_meta.get("deployed_at_utc")

    uptime_s = max(0.0, time.time() - _START_TIME_UTC)
    hours = int(uptime_s // 3600)
    minutes = int((uptime_s % 3600) // 60)
    seconds = int(uptime_s % 60)
    uptime = f"{hours:02d}h{minutes:02d}m{seconds:02d}s"

    match = "unknown"
    if expected_commit_sha is not None and commit_sha is not None:
        match = "yes" if commit_sha.startswith(expected_commit_sha) else "no"
    elif expected_commit_sha is not None:
        match = "no (running SHA unavailable)"

    return {
        "application": "tg-signal-ingest",
        "version": _package_version,
        "commit_sha": commit_sha or "unknown",
        "branch": branch or "unknown",
        "uncommitted_changes": dirty if dirty is not None else "unknown",
        "built_at": built_at or "unknown",
        "deployed_at": deployed_at or "unknown",
        "uptime": uptime,
        "trading_mode": trading_mode or "unknown",
        "live_authorized": live_authorized if live_authorized is not None else "unknown",
        "matches_expected_revision": match,
    }


def format_version_text(info: dict[str, Any]) -> str:
    """Format version info as a concise, readable Telegram message."""

    lines = [
        "📋 نسخه اجرا (فقط مالک/توسعه‌دهنده)",
        f"• نسخه برنامه: {info['version']}",
        f"• commit SHA: {info['commit_sha']}",
        f"• شاخه: {info['branch']}",
        f"• تغییرات غیرCommit‌شده: {_bool_persian(info['uncommitted_changes'])}",
        f"• زمان ساخت: {info['built_at']}",
        f"• زمان استقرار: {info['deployed_at']}",
        f"• زمان اجرا (uptime): {info['uptime']}",
        f"• حالت معامله: {info['trading_mode']}",
        f"• مجوز لایو: {_bool_persian(info['live_authorized'])}",
        f"• تطبیق با نسخه انتظاردهی: {info['matches_expected_revision']}",
    ]
    return "\n".join(lines)


def _bool_persian(value: Any) -> str:
    if value is True:
        return "بله"
    if value is False:
        return "خیر"
    return "نامشخص"
