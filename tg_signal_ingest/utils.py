"""Shared helpers: Telegram marked-id math, logging, tiny CLI tables."""

from __future__ import annotations

import logging
import os
import re
import sys


def marked_channel_id(bare_id: int) -> int:
    """Convert a bare channel id to Telethon's canonical marked id."""

    return int(-1000000000000 - bare_id) if bare_id > 0 else int(bare_id)


def bare_channel_id(marked: int) -> int:
    return -marked - 1000000000000 if marked <= -1000000000000 else abs(marked)


def configure_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(level=level.upper(), format=fmt, stream=sys.stdout)

    # Telethon's own logging is noisy on reconnects; keep warnings only.
    logging.getLogger("telethon").setLevel(logging.WARNING)
    # Secret-safety: the session library never logs secrets, but silence debug.
    logging.getLogger("telethon.session").setLevel(logging.ERROR)


SECRET_ENV_KEYS = re.compile(
    r"api_hash|api_id|password|authorization|cookie|session|token|secret|credential",
    re.IGNORECASE,
)


def redact_env(dict_like: dict[str, str]) -> dict[str, str]:
    return {
        k: ("<redacted>" if SECRET_ENV_KEYS.search(k) else v) for k, v in dict_like.items()
    }


def file_size_human(path: str) -> str:
    try:
        size = os.path.getsize(path)
    except OSError:
        return "?"
    size_f = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if size_f < 1024 or unit == "GB":
            return f"{size_f:.1f} {unit}" if unit != "B" else f"{size_f} B"
        size_f /= 1024
    return f"{size_f} GB"
    return f"{size} GB"
