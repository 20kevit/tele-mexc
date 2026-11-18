"""Shared value parsers for the trading layer.

Two deliberately distinct Decimal semantics exist in this codebase:

* :func:`parse_decimal` (Class A) parses whatever was persisted and passes
  ``NaN``/``Infinity`` through unchanged — callers decide what to do with
  non-finite values.
* Finite-checking helpers (Class B, e.g. ``cleanup._decimal``) reject
  non-finite values outright.  These two families must never be merged;
  tests/test_parsing_characterization.py pins the boundary.

``parse_iso_utc`` normalizes persisted ISO-8601 timestamps: a value without
an explicit timezone is assumed UTC.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def parse_decimal(value: Any) -> Decimal | None:
    """Class-A decimal parser: NaN and +/-Infinity pass through unchanged."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def parse_iso_utc(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp; a value without timezone is assumed UTC."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
