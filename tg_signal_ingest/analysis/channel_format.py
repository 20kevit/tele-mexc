"""Deterministic structured parser for the observed TOOBIT futures-signal format.

The format below was derived from the real channel corpus stored in the local
database and delivered by the bounded historical import (see
``docs/SIGNAL_ANALYSIS.md`` and the fixture
``tests/fixtures/redacted_signal_corpus.jsonl``).

A representative initial signal (stored message, line style preserved):

    🆔 #205
    ⏰ 1405/08/01
     TIME =08:45
     *LONG*🟢
     ⚡️TOOBIT:ICPUSDT.P(15)

     ✅Entry: 2.779
     🔰Stop Loss : 2.719

     📌TP1 : 2.797
     📌TP2 : 2.811
     📌TP3 : 2.824
     📌TP4 : 2.834
      -----------------------------
     🤑FULL Target : 2.869
     -----
     💰Deposit 1.00%
     💠MAX LEV Suggested  =  8 (-17.39%)
     ⭐️ @tradermoein / @chart_Tora

Follow-up messages repeat the same logical signal ID (``🆔 #204``) and describe
entry activation, target hits, closure or reversal, for example:

    🆔 #201
    TOOBIT:MYXUSDT.P (LONG)
    🎯 تارگت 3 با موفقیت خورد!
    💰 12.6 % سود

Guarantees (all covered by tests):

* the original text is never mutated; only a normalized copy is matched;
* all prices are ``Decimal`` and must be finite and strictly positive;
* the Jalali calendar date (``1405/08/01``) is never read as a Gregorian
  date; the original date/time text is preserved verbatim and the derived
  UTC timestamp is stored separately with an explicit, checked conversion
  (channel-local +03:30);
* ``Deposit`` and ``MAX LEV Suggested`` are informational metadata only:
  they are retained but can never determine order sizing or leverage;
* a message must contain the complete initial-signal structure (logical ID,
  direction, exchange:symbol, entry, stop loss and at least one numbered
  target) to be classified as ``new_signal``; anything less is ``ambiguous``
  or is classified by the follow-up rules;
* a follow-up (entry activation, TP hit, closure, reversal, plain updates)
  is classified as ``result`` or the matching update class and is never a
  new initial signal;
* a SHORT signal is stored normally, but ``ParsedCandidate.actionable`` is
  False: it is historical review data and must never trigger an order, a
  sell, or any other trading action downstream.

The parser has no network, exchange, or trading imports.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from .model import (
    QUOTE_ASSETS,
    Direction,
    FollowUpEvent,
    MarketType,
    ParsedCandidate,
    ParseStatus,
    Side,
    SignalClassification,
    TargetPrice,
)

#: Channel-local offset of the observed signal channel (Tehran, +03:30).
CHANNEL_TZ = timezone(timedelta(hours=3, minutes=30), name="+03:30")

#: Jalali year range the explicit calendar conversion applies to; anything in
#: the Gregorian range is parsed directly, which keeps a real Gregorian date
#: like 2026/10/23 readable while a Jalali 1405/08/01 is converted properly.
JALALI_YEAR_RANGE = (1100, 1900)

_DIGIT_TRANSLATION = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")

_LOGICAL_ID_HEADER_RE = re.compile(r"(?:^|\n)\s*(?:🆔\s*)?#\s*(\d{1,9})\b")
_LOGICAL_ID_ANY_RE = re.compile(r"#\s*(\d{1,9})\b")

_DIRECTION_TOKEN_RE = re.compile(r"(?<![A-Za-z])(LONG|SHORT)(?![A-Za-z])")

#: Exchanges whose ``EXCH:SYMBOL`` line style is claimed by this structured
#: parser.  Detection is deliberately strict: without a whitelist the loose
#: ``WORD: VALUE`` pattern would swallow ordinary generic-format messages
#: (e.g. "ENTRY: 100") that belong to the generic heuristic parser.
KNOWN_EXCHANGES = frozenset(
    {
        "TOOBIT",
        "MEXC",
        "BYBIT",
        "BITGET",
        "OKX",
        "BINANCE",
        "BINGX",
        "KUCOIN",
        "GATE",
        "HTX",
    }
)

# "⚡️TOOBIT:ICPUSDT.P(15)" and follow-up variants "TOOBIT:LPTUSDT.P (LONG)".
# The closing parenthesis must not be followed by a `\b` assertion: after a
# `)` both neighbours are non-word characters, so `\b` would fail exactly at
# the end of a line or message ("...(15)") and silently drop the timeframe.
_EXCHANGE_SYMBOL_RE = re.compile(
    r"(?<!\w)([A-Z][A-Z]{2,15})\s*:\s*([A-Z0-9]{2,15})"
    r"(\.P\b)?(?:\s*\((\d{1,4})\))?"
)

_BARE_SYMBOL_LINE_RE = re.compile(
    r"(?:^|\n)\s*([A-Z0-9]{2,10}USDT(?:\.P)?)\s*(?=\n|$)"
)

_ENTRY_LINE_RE = re.compile(r"\bENTRY\b[^:\n]{0,20}[:=]\s*([^\n%:=]{0,40}\d[^\n%:]*)")
_SL_LINE_RE = re.compile(r"\bSTOP\s*LOSS\b[^:\n]{0,20}[:=]\s*([^\n%:=]{0,40}\d[^\n%:]*)")
_TP_LINE_RE = re.compile(r"\bTP\s*([1-9])\s*[:=]\s*([^\n:%=]{0,40}?\d[^\n:%=]*)")
_FULL_TARGET_LINE_RE = re.compile(r"\bFULL\s*TARGET\b\s*[:=]\s*([^\n%:=]{0,40}\d[^\n%:=]*)")
_DEPOSIT_RE = re.compile(r"\bDEPOSIT\b\s*:?\s*([^\n%]{0,40}?)\s*%")
_LEV_RE = re.compile(
    r"\bMAX\s*LEV(?:E[RX]AGE)?\s*SUGGESTED\s*=\s*([^\n(]{0,30}?\d[^\n(]*?)\s*"
    r"(?:\(\s*(-?\d+(?:\.\d+)?)\s*%\s*\))?$",
    re.MULTILINE,
)
_DATE_RE = re.compile(r"([12]\d{3})\s*[/\-.]\s*([01]?\d)\s*[/\-.]\s*([0-3]?\d)")
_TIME_RE = re.compile(r"TIME\s*=\s*(\d{1,2}):(\d{2})(?::(\d{2}))?")
_PRICE_LINE_RE = re.compile(r"(?:\bPRICE\b|\bقیمت\b)\s*[:=]\s*([^\n:%]{0,40}\d)")
_AT_PRICE_RE = re.compile(r"@\s*(\d[\d,]*(?:\.\d+)?)\b(?![\w]*[A-Za-z])")
_PROFIT_RE = re.compile(r"(?:^|(?<=[\s>|:]))(\d+(?:\.\d+)?)\s*%\s*(?:سود|PROFIT)?")
_TP_HIT_FA_RE = re.compile(r"تارگت\s*([1-9])\s*(?:[^.\n]{0,24}?(?:خورد|زد|رسید|لمس|مبارک))")
_TP_HIT_EN_RE = re.compile(r"\bTP\s*([1-9])\s*(?:LONG|SHORT)?\s*(?:HIT|REACHED|✅|✔|☑|OK|DONE)")
_ENTRY_ACTIVATED_FA_RE = re.compile(
    r"ورود(?:\s*پوزیشن)?(?:\s*[A-Z]+)?\s*(?:با\s*موفقیت\s*)?(?:ثبت\s*شد|تریگر\s*شد|فعال\s*شد)"
)
_ENTRY_ACTIVATED_EN_RE = re.compile(r"\bENTRY\b[^:=\n]{0,30}\b(?:ACTIVATED|TRIGGERED|CONFIRMED)\b")
_SL_HIT_RE = re.compile(r"حد\s*ضرر[^\n]{0,24}خورد|\bSTOP\s*LOSS\s*(?:HIT|X)\b|\bSL\s+HIT\b")
_CLOSED_RE = re.compile(r"(?:POS[IE]TION|SIGNAL)\s+CLOSE[SD]\b|پوزیشن\s*بسته\s*شد|سیگنال\s*بسته\s*شد")
_REVERSED_RE = re.compile(r"\bREVERS(?:ED|E)\b|معکوس\s*شد")
_SL_UPDATE_FA_RE = re.compile(r"حد\s*ضرر[^:=%\n]{0,20}(?:به|جدید|تغییر|انتقال|بروز)")
_SL_UPDATE_EN_RE = re.compile(r"\b(?:SL|STOP\s*LOSS)\b[^\n%]{0,30}\b(?:MOVED|UPDATED|NEW)\b")
_TP_UPDATE_FA_RE = re.compile(r"تا?رگت[^:=%\n]{0,20}(?:به|جدید|تغییر|انتقال|بروز)")
_TP_UPDATE_EN_RE = re.compile(r"\b(?:TP|TARGET)\b[^\n%]{0,30}\b(?:MOVED|UPDATED|NEW)\b")


def _normalize_for_matching(raw: str) -> str:
    """Digit/width normalization on a copy; the original is never mutated."""

    text = raw.translate(_DIGIT_TRANSLATION)
    for src, dst in (("\u200c", " "), ("\u00a0", " "), ("\u200e", ""), ("\u200f", "")):
        text = text.replace(src, dst)
    return text


def _decimal(raw: str | None) -> Decimal | None:
    """Finite, strictly-positive price decimal, or ``None``."""

    if raw is None:
        return None
    cleaned = raw.strip().replace(",", "").replace(" ", "")
    if not cleaned or not re.fullmatch(r"\d+(?:\.\d+)?", cleaned):
        return None
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):  # pragma: no cover - fullmatch guards
        return None
    if not value.is_finite() or value <= 0:
        return None
    return value


def _signed_decimal(raw: str | None) -> Decimal | None:
    if raw is None:
        return None
    cleaned = raw.strip().replace(",", "").replace(" ", "")
    if not cleaned or not re.fullmatch(r"[+-]?\d+(?:\.\d+)?", cleaned):
        return None
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError):  # pragma: no cover - fullmatch guards
        return None
    return value if value.is_finite() else None


def _first_number(fragment: str | None) -> Decimal | None:
    if fragment is None:
        return None
    match = re.search(r"-?\d[\d,]*(?:\.\d+)?", fragment)
    if match is None:
        return None
    return _decimal(match.group(0))


@dataclass(frozen=True)
class _Fields:
    payload: dict[str, Any]
    date_text: str | None
    time_text: str | None


def _collect_logical_ids(upper: str) -> list[int]:
    """Logical signal IDs from `🆔 #N` headers (deduplicated, keep order)."""

    ids: list[int] = []
    for match in _LOGICAL_ID_HEADER_RE.finditer(upper):
        value = int(match.group(1))
        if value not in ids:
            ids.append(value)
    return ids


def _extract_direction(upper: str) -> Direction | None:
    tokens: list[str] = []
    for match in _DIRECTION_TOKEN_RE.finditer(upper):
        token = match.group(1)
        if token not in tokens:
            tokens.append(token)
    if len(tokens) == 1:
        return Direction(tokens[0].lower())
    return None  # absent, or both directions present: ambiguous


def _extract_exchange_symbol(upper: str) -> tuple[str | None, str | None, int | None]:
    match = _EXCHANGE_SYMBOL_RE.search(upper)
    if match is None:
        # Follow-ups sometimes carry a bare symbol line (e.g. "ICPUSDT")
        # without the exchange prefix; accept it on a standalone line.
        bare = _BARE_SYMBOL_LINE_RE.search(upper)
        if bare is not None:
            return None, bare.group(1), None
        return None, None, None
    exchange = match.group(1)
    symbol_raw = match.group(2)
    if match.group(3):
        symbol_raw += ".P"
    timeframe = int(match.group(4)) if match.group(4) else None
    return exchange, symbol_raw, timeframe


def _split_symbol(symbol_raw: str) -> tuple[str, str | None]:
    """Split ``ICPUSDT.P`` into base ``ICPUSDT`` and quote ``USDT``."""

    bare = symbol_raw.split(".")[0].upper()
    for quote in sorted(QUOTE_ASSETS, key=len, reverse=True):
        if bare.endswith(quote) and len(bare) > len(quote):
            base = bare[: -len(quote)]
            if base and base.isalnum():
                return bare, quote
    return bare, None


def _extract_fields(upper: str) -> _Fields:
    payload: dict[str, Any] = {}

    entry_match = _ENTRY_LINE_RE.search(upper)
    if entry_match:
        payload["entry"] = _first_number(entry_match.group(1))
    sl_match = _SL_LINE_RE.search(upper)
    if sl_match:
        payload["stop_loss"] = _first_number(sl_match.group(1))
    full_match = _FULL_TARGET_LINE_RE.search(upper)
    if full_match:
        payload["full_target"] = _first_number(full_match.group(1))

    targets: dict[int, Decimal] = {}
    for match in _TP_LINE_RE.finditer(upper):
        index = int(match.group(1))
        if index in targets:
            continue
        value = _first_number(match.group(2))
        if value is not None:
            targets[index] = value
    if targets:
        payload["targets"] = targets

    deposit_match = _DEPOSIT_RE.search(upper)
    if deposit_match:
        payload["deposit_pct"] = _first_number(deposit_match.group(1))

    lev_match = _LEV_RE.search(upper)
    if lev_match:
        payload["suggested_leverage"] = _first_number(lev_match.group(1))
        payload["leverage_pct"] = _signed_decimal(lev_match.group(2))

    # Profit percentage (e.g. "💰 12.6 % سود" or "Profit: 8.5 % @ 1.582").
    # The deposit (1.00%) and suggested-leverage (-17.39%) percentages are
    # masked out beforehand so they can never be mistaken for profit.
    masked = upper
    for span in (deposit_match.span() if deposit_match else None,
                 lev_match.span() if lev_match else None):
        if span:
            masked = masked[: span[0]] + " " * (span[1] - span[0]) + masked[span[1]:]
    profit_match = _PROFIT_RE.search(masked)
    if profit_match:
        payload["profit_pct"] = _first_number(profit_match.group(1))

    price_match = _PRICE_LINE_RE.search(upper)
    if price_match:
        payload["event_price"] = _first_number(price_match.group(1))
    else:
        at_match = _AT_PRICE_RE.search(upper)
        if at_match:
            payload["event_price"] = _decimal(at_match.group(1))

    follow_up_index = _detect_tp_hit(upper)
    if follow_up_index is not None:
        payload["follow_up_index"] = follow_up_index

    date_match = _DATE_RE.search(upper)
    date_text = date_match.group(0) if date_match else None
    time_match = _TIME_RE.search(upper)
    if time_match:
        time_text = ":".join(g for g in time_match.groups() if g is not None)
    else:
        time_text = None
    return _Fields(payload=payload, date_text=date_text, time_text=time_text)


def _detect_tp_hit(upper: str) -> int | None:
    for pattern in (_TP_HIT_FA_RE, _TP_HIT_EN_RE):
        match = pattern.search(upper)
        if match:
            return int(match.group(1))
    return None


def _detect_follow_up_event(upper: str, target_index: int | None) -> FollowUpEvent | None:
    if _TP_HIT_EN_RE.search(upper) or _TP_HIT_FA_RE.search(upper):
        return FollowUpEvent.TP_HIT
    if _SL_HIT_RE.search(upper):
        return FollowUpEvent.STOP_LOSS_HIT
    if _ENTRY_ACTIVATED_FA_RE.search(upper) or _ENTRY_ACTIVATED_EN_RE.search(upper):
        return FollowUpEvent.ENTRY_ACTIVATED
    if _REVERSED_RE.search(upper):
        return FollowUpEvent.REVERSED
    if _CLOSED_RE.search(upper):
        return FollowUpEvent.CLOSED
    if _SL_UPDATE_FA_RE.search(upper) or _SL_UPDATE_EN_RE.search(upper):
        return FollowUpEvent.SL_UPDATE
    if _TP_UPDATE_FA_RE.search(upper) or _TP_UPDATE_EN_RE.search(upper):
        return FollowUpEvent.TP_UPDATE
    return None


def _div(a: int, b: int) -> int:
    """Integer division used by the reference algorithm (non-negative here)."""

    return a // b if (a >= 0) == (b >= 0) else -((-a) // b)


def _mod(a: int, b: int) -> int:
    """Remainder matching :func:`_div` (non-negative for positive `b`)."""

    return a - (_div(a, b) * b)


_JALALI_BREAKS = (
    -61, 9, 38, 199, 426, 686, 756, 818, 1111, 1181, 1210, 1635, 2060, 2097,
    2192, 2262, 2324, 2394, 2456, 3178,
)


def _jal_cal(jy: int) -> tuple[int, int]:
    """Reference Jalali leap/anchor computation; returns ``(march, leap)``.

    ``march`` is the Gregorian day of Farvardin 1 for the Jalali year and
    ``leap`` is the Jalali-style leap indicator (0..4, 4 == leap year).
    """

    break_count = len(_JALALI_BREAKS)
    gy = jy + 621
    leap_j = -14
    jp = _JALALI_BREAKS[0]
    jump = 0
    for index in range(1, break_count):
        boundary = _JALALI_BREAKS[index]
        jump = boundary - jp
        if jy < boundary:
            break
        leap_j += _div(jump, 33) * 8 + _div(_mod(jump, 33), 4)
        jp = boundary
    n = jy - jp
    leap_j += _div(n, 33) * 8 + _div(_mod(n, 33) + 3, 4)
    if _mod(jump, 33) == 4 and jump - n == 4:
        leap_j += 1
    leap_g = _div(gy, 4) - _div((_div(gy, 100) + 1) * 3, 4) - 150
    march = 20 + leap_j - leap_g
    if jump - n < 6:
        n = n - jump + _div(jump + 4, 33) * 33
    leap = _mod(_mod(n + 1, 33) - 1, 4)
    if leap == -1:
        leap = 4
    return march, leap


def _g2d(gy: int, gm: int, gd: int) -> int:
    d = _div(
        (gy + _div(gm - 8, 6) + 100100) * 1461, 4
    ) + _div(153 * _mod(gm + 9, 12) + 2, 5) + gd - 34840408
    d = d - _div(
        _div(gy + _div(gm - 8, 6) + 100100 + _div(8 - gm, 6), 100) * 3, 4
    ) + 752
    return d


def _d2g(jdn: int) -> tuple[int, int, int]:
    j = 4 * jdn + 139361631
    j = j + _div(_div(4 * jdn + 183187720, 146097) * 3, 4) * 4 - 3908
    i = _div(_mod(j, 1461), 4) * 5 + 308
    gd = _div(_mod(i, 153), 5) + 1
    gm = _mod(_div(i, 153), 12) + 1
    gy = _div(j, 1461) - 100100 + _div(8 - gm, 6)
    return gy, gm, gd


def jalali_to_gregorian(jy: int, jm: int, jd: int) -> tuple[int, int, int]:
    """Convert a Jalali calendar date to the Gregorian (y, m, d) triple.

    Faithful, dependency-free port of the reference algorithm used by the
    ``jdatetime``/``jalaali-js`` packages. Example: 1405/08/01 -> 2026/10/23.
    Raises ``ValueError`` for out-of-range inputs.
    """

    if not 1 <= jy <= 3000 or not 1 <= jm <= 12 or not 1 <= jd <= 31:
        raise ValueError(f"unsupported Jalali date {jy}-{jm:02}-{jd:02}")
    # Months 1-6 have 31 days, months 7-11 have 30 and month 12 has 29
    # (30 in a leap year).  The day-of-year offset below is the exact
    # inverse of the month arithmetic in :func:`gregorian_to_jalali`, which
    # keeps the two directions consistent (round-trip safe).
    month_length = (
        31
        if jm <= 6
        else (30 if jm <= 11 else (30 if _jal_cal(jy)[1] == 0 else 29))
    )
    if not 1 <= jd <= month_length:
        raise ValueError(f"invalid Jalali day {jy}-{jm:02}-{jd:02}")
    if jm <= 6:
        day_offset = (jm - 1) * 31
    else:
        day_offset = 186 + (jm - 7) * 30
    jdn = _g2d(*_jal_cal_year_start(jy)) + day_offset + jd - 1
    return _d2g(jdn)


def _jal_cal_year_start(jy: int) -> tuple[int, int, int]:
    """Gregorian date of Farvardin 1 of the given Jalali year."""

    march, _leap = _jal_cal(jy)
    return jy + 621, 3, march


def gregorian_to_jalali(gy: int, gm: int, gd: int) -> tuple[int, int, int]:
    """Inverse of :func:`jalali_to_gregorian`; used for round-trip tests."""

    if not 1 <= gy <= 3400 or not 1 <= gm <= 12 or not 1 <= gd <= 31:
        raise ValueError(f"unsupported Gregorian date {gy}-{gm:02}-{gd:02}")
    jdn = _g2d(gy, gm, gd)
    # Binary-search the Jalali year whose Farvardin 1 JDN is <= jdn.
    low, high = 1, 3000
    while low <= high:
        mid = (low + high) // 2
        start = _g2d(*_jal_cal_year_start(mid))
        if start <= jdn:
            low = mid + 1
        else:
            high = mid - 1
    jy = high
    start = _g2d(*_jal_cal_year_start(jy))
    day_offset = jdn - start  # 0-based day of year
    if day_offset < 186:
        jm = 1 + day_offset // 31
        jd = 1 + day_offset % 31
    else:
        day_offset -= 186
        jm = 7 + day_offset // 30
        jd = 1 + day_offset % 30
    return jy, jm, jd


def source_timestamp_from(date_text: str | None, time_text: str | None) -> str | None:
    """Derive a UTC ISO 8601 timestamp from the observed date/time text.

    The channel uses the Jalali calendar (example: ``1405/08/01``); the
    conversion is explicit and range-checked, never a silent misread of a
    Jalali date as Gregorian (or vice versa). The channel-local wall time is
    converted to UTC with the fixed +03:30 offset.
    """

    if date_text is None:
        return None
    parts = re.findall(r"\d+", date_text)
    if len(parts) != 3:
        return None
    year, month, day = (int(p) for p in parts)
    hour = minute = second = 0
    if time_text is not None:
        tparts = re.findall(r"\d+", time_text)
        if len(tparts) not in (2, 3):
            return None
        hour, minute = int(tparts[0]), int(tparts[1])
        if len(tparts) == 3:
            second = int(tparts[2])
        if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
            return None
    if JALALI_YEAR_RANGE[0] <= year <= JALALI_YEAR_RANGE[1]:
        try:
            gy, gm, gd = jalali_to_gregorian(year, month, day)
        except ValueError:
            return None
        stamp = datetime(gy, gm, gd, hour, minute, second, tzinfo=CHANNEL_TZ)
        return stamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if 1900 < year <= 2200:
        try:
            stamp = datetime(year, month, day, hour, minute, second, tzinfo=CHANNEL_TZ)
        except ValueError:
            return None
        return stamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return None


def parse_structured(
    raw: str | None,
    *,
    reply_to_message_id: int | None = None,  # association handled by the reindexer
) -> ParsedCandidate | None:
    """Parse a message that exhibits the structured channel format.

    Returns ``None`` when none of the characteristic markers are present
    (logical-ID header, or exchange:symbol pair with direction/prices); the
    caller then falls back to the generic heuristic parser.
    """

    if raw is None or raw.strip() == "":
        return None
    text = _normalize_for_matching(raw)
    upper = text.upper()

    ids = _collect_logical_ids(upper)
    exchange, symbol_raw, timeframe = _extract_exchange_symbol(upper)
    has_exchange_pair = (
        exchange is not None
        and symbol_raw is not None
        and exchange in KNOWN_EXCHANGES
        and _split_symbol(symbol_raw)[0]
        and any(
            _split_symbol(symbol_raw)[1] == quote or symbol_raw.endswith(".P")
            for quote in QUOTE_ASSETS
        )
    )
    if not ids and not has_exchange_pair:
        return None

    return _classify_structured(
        text=text,
        upper=upper,
        ids=ids,
        direction=_extract_direction(upper),
        exchange=exchange,
        symbol_raw=symbol_raw,
        timeframe=timeframe,
    )


def _classify_structured(
    *,
    text: str,
    upper: str,
    ids: list[int],
    direction: Direction | None,
    exchange: str | None,
    symbol_raw: str | None,
    timeframe: int | None,
) -> ParsedCandidate:
    ambiguities: list[str] = []
    reasons: list[str] = []
    confidence: dict[str, str] = {}

    fields = _extract_fields(upper)
    payload = fields.payload

    logical_id = ids[0] if len(ids) == 1 else None
    if len(ids) > 1:
        distinct = ",".join(str(i) for i in list(dict.fromkeys(ids))[:4])
        ambiguities.append(f"multiple_logical_ids:{distinct}")

    symbol_base, quote = _split_symbol(symbol_raw) if symbol_raw else (None, None)
    symbol = symbol_base

    entry = payload.get("entry")
    stop_loss = payload.get("stop_loss")
    targets: dict[int, Decimal] = dict(payload.get("targets") or {})
    full_target = payload.get("full_target")
    deposit_pct = payload.get("deposit_pct")
    suggested_leverage = payload.get("suggested_leverage")
    leverage_pct = payload.get("leverage_pct")
    event_price = payload.get("event_price")
    profit_pct = payload.get("profit_pct")
    follow_up_index = payload.get("follow_up_index")

    has_initial_shape = bool(
        logical_id is not None
        and direction is not None
        and exchange is not None
        and symbol is not None
        and entry is not None
        and stop_loss is not None
        and targets
        and "ENTRY" in upper
        and "STOP LOSS" in upper
    )
    tp_index: int | None = None
    follow_up_event: FollowUpEvent | None = None
    if has_initial_shape:
        pass  # classified below as a new signal
    else:
        follow_up_event = _detect_follow_up_event(upper, follow_up_index)

    # A follow-up legitimately omits direction/symbol/prices; only messages
    # that claim the full initial shape (or look like a broken copy of it)
    # must carry complete mandatory fields.
    is_follow_up = follow_up_event is not None and logical_id is not None
    if direction is None and not is_follow_up:
        ambiguities.append("missing_or_conflicting_direction")
    if symbol is None and not is_follow_up:
        ambiguities.append("missing_symbol")
    elif quote is None and not is_follow_up:
        ambiguities.append("unknown_quote_asset")
        quote = None

    side: Side | None = None
    if direction == Direction.LONG:
        side = Side.BUY
    elif direction == Direction.SHORT:
        side = Side.SELL

    if has_initial_shape:
        classification = SignalClassification.NEW_SIGNAL
        reasons.append("complete structured initial signal")
    else:
        tp_index = follow_up_index
        if logical_id is not None and follow_up_event is not None:
            if follow_up_event == FollowUpEvent.TP_UPDATE:
                classification = SignalClassification.TARGET_UPDATE
            elif follow_up_event == FollowUpEvent.SL_UPDATE:
                classification = SignalClassification.STOP_LOSS_UPDATE
            else:
                classification = SignalClassification.RESULT
                reasons.append("structured follow-up report (entry/target/result), not a new signal")
        elif "ENTRY" in upper and "STOP LOSS" in upper:
            # An entry/SL/TP copy that lost a mandatory block is incomplete:
            # never silently accepted as a signal.
            classification = SignalClassification.AMBIGUOUS
            reasons.append("incomplete structured signal shape")
        elif side is not None and symbol is not None:
            classification = SignalClassification.AMBIGUOUS
            reasons.append("signal-like content without a complete structure")
        else:
            classification = SignalClassification.NON_SIGNAL
            reasons.append("no supported trading structure found")

    # Missing mandatory fields only matter for pieces the message claims to
    # carry; follow-up reports legitimately omit them.
    if classification in (
        SignalClassification.NEW_SIGNAL,
        SignalClassification.AMBIGUOUS,
        SignalClassification.TARGET_UPDATE,
        SignalClassification.STOP_LOSS_UPDATE,
    ) and ("ENTRY" in upper or "STOP LOSS" in upper):
        if entry is None and "ENTRY" in upper:
            ambiguities.append("missing_entry_price")
        if stop_loss is None and "STOP LOSS" in upper:
            ambiguities.append("missing_stop_loss")
        if not targets and ("TP" in upper or f"تارگت {1}" in upper):
            ambiguities.append("missing_take_profit")

    if classification == SignalClassification.RESULT and entry is not None:
        # In a follow-up report an "Entry: 2.0867" line is the observed
        # trigger price at the event, never a fresh planning value.
        if event_price is None:
            event_price = entry
        entry = None
        reasons.append("follow-up entry value recorded as event price")

    # ------------------------------------------------- semantic validation
    if has_initial_shape:
        assert side is not None and entry is not None and stop_loss is not None
        if direction == Direction.LONG and stop_loss >= entry:
            ambiguities.append("stop_loss_not_below_entry")
        if direction == Direction.SHORT and stop_loss <= entry:
            ambiguities.append("stop_loss_not_above_entry")
        for index, price in sorted(targets.items()):
            if direction == Direction.LONG and price <= entry:
                ambiguities.append(f"target_below_entry:TP{index}")
            elif direction == Direction.SHORT and price >= entry:
                ambiguities.append(f"target_above_entry:TP{index}")
                break
        if full_target is not None:
            if direction == Direction.LONG and full_target <= entry:
                ambiguities.append("full_target_not_above_entry")
            elif direction == Direction.SHORT and full_target >= entry:
                ambiguities.append("full_target_not_below_entry")

    if has_initial_shape:
        confidence.update(
            {"side": "high", "symbol": "high", "entry": "high",
             "stop_loss": "high", "targets": "high", "market_type": "high"}
        )
    elif classification == SignalClassification.RESULT:
        confidence.update({"side": "high", "symbol": "high", "market_type": "high"})
    else:
        confidence.update({"side": "high" if side else "low", "symbol": "high" if symbol else "low"})
    if symbol is not None and quote is None:
        confidence["symbol"] = "low"
        confidence["market_type"] = "low"
    if suggested_leverage is not None:
        confidence["suggested_leverage"] = "high"
    confidence["classification"] = (
        "low" if classification == SignalClassification.AMBIGUOUS else "high"
    )

    source_timestamp = source_timestamp_from(fields.date_text, fields.time_text)
    reasons.append("structured futures format; Spot review context only")
    # A date that fails its explicit calendar check is never silently
    # dropped; the raw text stays preserved and the gap is flagged.
    if fields.date_text is not None and source_timestamp is None:
        ambiguities.append("source_date_not_converted")

    parse_status = (
        ParseStatus.NEEDS_REVIEW
        if ambiguities or classification == SignalClassification.AMBIGUOUS
        else ParseStatus.PARSED
    )

    return ParsedCandidate(
        classification=classification,
        market_type=MarketType.NON_SPOT,
        symbol=symbol,
        quote_asset=quote,
        side=side,
        entry_price=entry,
        entry_low_price=None,
        entry_high_price=None,
        stop_loss=stop_loss,
        targets=tuple(TargetPrice(index=i, price=p) for i, p in sorted(targets.items())),
        order_type=None,
        # `#N` is a logical signal ID, not a Telegram message reference.
        referenced_message_id=None,
        parse_status=parse_status,
        field_confidence=confidence,
        ambiguities=tuple(dict.fromkeys(ambiguities)),
        reasons=tuple(dict.fromkeys(reasons)),
        normalized_text=" ".join(text.split()).upper(),
        text_sha256="",
        direction=direction,
        logical_id=logical_id,
        followed_logical_id=logical_id,
        exchange=exchange,
        symbol_raw=symbol_raw,
        timeframe=timeframe,
        full_target=full_target,
        deposit_pct=deposit_pct,
        suggested_leverage=suggested_leverage,
        leverage_pct=leverage_pct,
        follow_up_event=follow_up_event,
        target_index=tp_index,
        profit_pct=profit_pct,
        event_price=event_price,
        source_date_text=fields.date_text,
        source_time_text=fields.time_text,
        source_timestamp_iso=source_timestamp,
    )


def follow_up_event_fa(event: str | None) -> str:
    """Persian label used by the review interface."""

    labels: dict[str | None, str] = {
        None: "—",
        "entry_activated": "ورود ثبت شد",
        "tp_hit": "تارگت خورده",
        "stop_loss_hit": "حد ضرر خورده",
        "full_target": "تارگت کامل",
        "closed": "بسته شد",
        "reversed": "معکوس شد",
        "sl_update": "تغییر حد ضرر",
        "tp_update": "تغییر تارگت",
        "unknown_update": "به‌روزرسانی نامشخص",
    }
    if event is None:
        return labels[None]
    return labels.get(event, event)
