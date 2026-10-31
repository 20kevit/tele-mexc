"""Shared audit helpers: redaction, normalization, and error classification.

The audit journal is written on the trading critical path, so everything here
is pure, local, and bounded.  Nothing performs network I/O and nothing raises
for malformed input.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

MAX_TEXT = 2000
MAX_PAYLOAD = 4000

# Values that look like credentials.  Replacement happens before anything is
# logged or persisted, so a leaked Bot token can never reach the audit journal.
_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b"), "[REDACTED_BOT_TOKEN]"),
    (re.compile(r"\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE), "[REDACTED_AUTH]"),
    (re.compile(r"\b(?:api[_-]?key|secret|token|password|passphrase|signature)\b"
                r"\s*[=:]\s*['\"]?[^\s,'\"]{4,}", re.IGNORECASE), "[REDACTED_SECRET]"),
    (re.compile(r"\b[A-Za-z0-9+/]{40,}={0,2}\b"), "[REDACTED_BLOB]"),
)

_SENSITIVE_KEYS = frozenset(
    {
        "api_key", "apikey", "secret", "secret_key", "api_secret", "token",
        "bot_token", "password", "passphrase", "signature", "authorization",
        "session", "session_string", "phone", "phone_number", "auth_key",
    }
)

_SEVERITIES = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


def redact_text(value: Any, *, limit: int = MAX_TEXT) -> str | None:
    """Return a bounded, redacted string (or ``None`` for no value)."""

    if value is None:
        return None
    text = str(value)
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    if len(text) > limit:
        text = text[:limit] + "…[truncated]"
    return text


def redact_payload(value: Any) -> str | None:
    """Serialize a payload to bounded JSON with sensitive keys removed."""

    if value is None:
        return None
    cleaned = _redact_value(value)
    import json

    try:
        text = json.dumps(cleaned, ensure_ascii=False, default=str, sort_keys=True)
    except (TypeError, ValueError):
        text = redact_text(cleaned) or ""
    if len(text) > MAX_PAYLOAD:
        text = text[:MAX_PAYLOAD] + "…[truncated]"
    return text


def _redact_value(value: Any, depth: int = 0) -> Any:
    if depth > 6:
        return "[depth-limit]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in list(value.items())[:100]:
            name = str(key)
            if name.strip().lower() in _SENSITIVE_KEYS:
                out[name] = "[REDACTED]"
            else:
                out[name] = _redact_value(item, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        return [_redact_value(item, depth + 1) for item in list(value)[:100]]
    if isinstance(value, str):
        return redact_text(value, limit=MAX_TEXT)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_text(value, limit=MAX_TEXT)


def normalize_severity(value: Any, *, default: str = "INFO") -> str:
    text = str(value or default).strip().upper()
    return text if text in _SEVERITIES else default


def normalize_event_type(category: str, event: str, explicit: str | None = None) -> str:
    """Stable dotted event type: ``category.event`` unless overridden."""

    if explicit:
        return str(explicit).strip()[:120]
    def _slug(value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
        return slug[:60] or "unknown"

    return f"{_slug(category)}.{_slug(event)}"


def correlation_digest(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:32]


def classify_exchange_error(exc: BaseException) -> tuple[str, str, str]:
    """Return ``(response_classification, reason_code, uncertainty)``.

    Classification is based on the exception type name only, so this module
    stays independent from the MEXC client to avoid import cycles.
    """

    name = exc.__class__.__name__
    if name == "MexcRateLimitError":
        return "rate_limited", name, "uncertain"
    uncertain = {
        "MexcNetworkError", "MexcServerError", "MexcUnexpectedResponse",
        "MexcWafError", "MexcTimestampError", "MexcDuplicateOrder",
        "TimeoutError", "URLError", "ConnectionError",
    }
    if name in uncertain:
        return "uncertain", name, "uncertain"
    if name in {"MexcOrderRejected", "MexcOrderNotFound", "MexcInsufficientBalance"}:
        return "rejected", name, "none"
    return "error", name, "none"
