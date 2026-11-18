"""Characterization tests for tg_signal_ingest.trading.parsing (F-03).

Pins the observable semantics of the shared Class-A decimal parser and the
ISO-UTC timestamp parser at the moment of consolidation.  The Class-A
NaN/Infinity passthrough (parse_decimal) versus finite-checking rejection
(cleanup._decimal, intentionally untouched) distinction is load-bearing;
the contrast test below freezes that boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from tg_signal_ingest.trading.cleanup import _decimal as finite_decimal
from tg_signal_ingest.trading.parsing import parse_decimal, parse_iso_utc


def test_parse_decimal_none_and_empty():
    assert parse_decimal(None) is None
    assert parse_decimal("") is None


def test_parse_decimal_whitespace():
    # Whitespace-only strings do not parse; padded numerics do (Decimal()
    # strips surrounding whitespace) — both behaviors pinned as-is.
    assert parse_decimal("   ") is None
    assert parse_decimal(" 12.5 ") == Decimal("12.5")


def test_parse_decimal_invalid_inputs():
    assert parse_decimal("abc") is None
    assert parse_decimal("12abc") is None
    assert parse_decimal("1,5") is None
    assert parse_decimal("False") is None


def test_parse_decimal_precision_preserved_exactly():
    value = "0.10000000000000000001"
    parsed = parse_decimal(value)
    assert parsed is not None
    assert str(parsed) == value  # no float round-trip, no exponent rewrite


def test_parse_decimal_exponent_notation_not_normalized():
    parsed = parse_decimal("1E+3")
    assert parsed is not None
    assert parsed == Decimal("1000")
    assert parsed.as_tuple().exponent == 3  # exponent form is kept verbatim


def test_parse_decimal_plain_numbers():
    assert parse_decimal(42) == Decimal("42")
    assert parse_decimal("0.1") == Decimal("0.1")
    assert parse_decimal(Decimal("0.1")) == Decimal("0.1")


def test_parse_decimal_nan_passes_through():
    # Class A: NaN is returned, not rejected and not converted to None.
    for raw in ("NaN", "-nan", float("nan"), Decimal("NaN")):
        parsed = parse_decimal(raw)
        assert parsed is not None, raw
        assert parsed.is_nan(), raw


def test_parse_decimal_infinities_pass_through():
    pos = parse_decimal("+Infinity")
    assert pos is not None and pos.is_infinite() and pos > 0
    neg = parse_decimal("-Infinity")
    assert neg is not None and neg.is_infinite() and neg < 0


def test_class_b_finite_checker_stays_independent():
    # Class B (cleanup._decimal) must reject non-finite values.  If a future
    # refactor merges the two parser families, this test must fail.
    assert finite_decimal("NaN") is None
    assert finite_decimal("+Infinity") is None
    assert finite_decimal("-Infinity") is None
    assert finite_decimal("12.5") == Decimal("12.5")


def test_parse_iso_utc_none_empty_invalid():
    assert parse_iso_utc(None) is None
    assert parse_iso_utc("") is None
    assert parse_iso_utc(" ") is None
    assert parse_iso_utc("not-a-date") is None
    assert parse_iso_utc("2026-13-01T00:00:00Z") is None  # month 13
    assert parse_iso_utc(0) is None


def test_parse_iso_utc_z_suffix_becomes_utc():
    parsed = parse_iso_utc("2026-01-02T03:04:05Z")
    assert parsed == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert parsed is not None and parsed.utcoffset() == timedelta(0)


def test_parse_iso_utc_explicit_offset_preserved_not_converted():
    parsed = parse_iso_utc("2026-01-02T03:04:05+02:00")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=2)
    assert (parsed.year, parsed.month, parsed.day, parsed.hour) == (2026, 1, 2, 3)


def test_parse_iso_utc_naive_assumed_utc():
    parsed = parse_iso_utc("2026-01-02T03:04:05")
    assert parsed == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_parse_iso_utc_date_only_becomes_midnight_utc():
    parsed = parse_iso_utc("2026-01-02")
    assert parsed == datetime(2026, 1, 2, tzinfo=UTC)
