"""Deterministic, offline-only classification and parsing of Telegram text.

Design rules:
* Persian and English labels are handled by the same deterministic patterns.
* Prices are ``Decimal``; binary floating point is never used.
* Missing fields stay missing. Ranges are stored as low/high, not silently
  collapsed into an invented midpoint.
* Every uncertain field/relationship produces an explicit ambiguity and a
  ``needs_review`` parse status rather than a guessed financial value.
* Links and instructions embedded in a message are never followed or executed.

The parser has no network, exchange, or trading imports.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation

from .channel_format import (
    follow_up_event_fa,  # noqa: F401  (re-exported)
    parse_structured,
)
from .model import (
    QUOTE_ASSETS,
    MarketType,
    ParsedCandidate,
    ParseStatus,
    Side,
    SignalClassification,
    TargetPrice,
)

#: Parser version 1: generic heuristic candidate parser.
#: Parser version 2: v1 + structured support for the observed channel format
#: (see ``analysis/channel_format.py``). Only messages that do not match the
#: structured format fall back to the version-1 heuristics.
ANALYSIS_PARSER_VERSION = 2

_NON_SYMBOL_WORDS = {
    "BUY", "SELL", "LONG", "SHORT", "ENTRY", "TARGET", "TARGETS", "STOP", "LOSS", "PRICE",
    "AMOUNT", "QUANTITY", "MARKET", "LIMIT", "SIGNAL", "UPDATE", "CANCEL", "CLOSE", "TAKE",
    "PROFIT", "ALERT", "SPOT", "ORDER", "ZONE", "RANGE", "TP", "SL", "USD", "RESULT",
    "HIT", "DONE", "REACHED", "BOOKED", "NOW", "NEW", "TO", "AND", "ALL", "MSG", "MESSAGE",
}

_NUM = r"\d[\d,]*(?:\.\d+)?"
_RANGE_SEP = r"(?:\s*(?:-|–|—|~|TO|تا|الی)\s*)"
_RANGE_AT_RE = re.compile(rf"^\s*({_NUM}){_RANGE_SEP}({_NUM})", re.IGNORECASE)
_BETWEEN_AT_RE = re.compile(
    rf"^\s*(?:(?:BETWEEN|بین)\s+)?({_NUM})\s+(?:AND|و)\s+({_NUM})", re.IGNORECASE
)
_NUMBER_AT_RE = re.compile(rf"^\s*({_NUM})(?:\s*([KkMm]))?")
_LEADING_UPDATE_WORD_RE = re.compile(
    r"^(?:(?:MOVED?|MOVE|TO|NOW|UPDATE[D]?|NEW|CHANGE[D]?|REVISED?|RAISE[D]?|LOWER(?:ED)?|"
    r"را|به|جدید|تغییر|انتقال|بالا|پایین|بروز|آپدیت)\s*)+",
    re.IGNORECASE,
)

_ENTRY_LABEL_RE = re.compile(
    r"(?<![\w])(?:ENTRY(?:\s*(?:PRICE|ZONE|RANGE|AT))?|BUY\s*AT|SELL\s*AT|"
    r"ورود|نقطه\s*ورود|محدوده\s*ورود|قیمت\s*ورود|انتری)(?![\w])",
    re.IGNORECASE,
)
_TP_LABEL_RE = re.compile(
    r"(?<![\w])(?:"
    r"(?:TP|TARGETS?|TAKE\s*PROFIT)(?:(\d{1,2}))?(?:\s*(\d{1,2})(?=\s*[:=]))?"
    r"|(?:تارگت|هدف|تی\s*پی|حد\s*سود)"
    r"(?:\s*(اول|دوم|سوم|چهارم|پنجم))?(?:\s*(\d{1,2})(?=\s*[:=]))?"
    r")(?![\w])",
    re.IGNORECASE,
)
_SL_LABEL_RE = re.compile(
    r"(?<![\w])(?:SL|STOP\s*LOSS|STOPLOSS|STOP|حد\s*ضرر|حد\s*زیان|"
    r"استاپ\s*لاس|استاپ|اس\s*ال)(?![\w])",
    re.IGNORECASE,
)
_CANCEL_WORD_RE = re.compile(
    r"(?<![\w])(?:CANCEL(?:LED|ED)?|INVALID|IGNORE|ABORT|CLOSE[SD]?|"
    r"لغو|کنسل|باطل|منتفی|حذف|پاک)(?![\w])",
    re.IGNORECASE,
)
_RESULT_RE = re.compile(
    r"(?<![\w])(?:TP\s*\d*\s*(?:HIT|DONE|REACHED)|HIT\s+TP\s*\d*|"
    r"TARGETS?\s*(?:HIT|REACHED)|PROFIT\s+BOOKED|BOOKED\s+PROFIT|RESULT)(?![\w])"
    r"|(?:تارگت|هدف|تی\s*پی|حد\s*سود)\s*(?:\d{1,2}|اول|دوم|سوم|چهارم|پنجم)?\s*"
    r"(?:خورد|زد|رسید|لمس)"
    r"|نتیجه|گزارش\s*(?:سیگنال|معامله)|سود\s*(?:گرفت|کسب|بوک)",
    re.IGNORECASE,
)
_BUY_RE = re.compile(
    r"(?<![\w])(?:BUY|BUYING|BOUGHT)(?![\w])|خرید|بخر(?:ید)?|بخرید", re.IGNORECASE
)
_SELL_RE = re.compile(
    r"(?<![\w])(?:SELL|SELLING|SOLD)(?![\w])|فروش|بفروش(?:ید)?", re.IGNORECASE
)
_LONG_RE = re.compile(r"(?<![\w])(?:LONG|لانگ)(?![\w])", re.IGNORECASE)
_SHORT_RE = re.compile(r"(?<![\w])(?:SHORT|شورت)(?![\w])", re.IGNORECASE)
_NON_SPOT_RE = re.compile(
    r"(?<![\w])(?:LONG|SHORT|LEVERAGE|LEVERAGED|FUTURES?|PERPETUAL|PERP|MARGIN|"
    r"لانگ|شورت|اهرم|فیوچرز|فیوچر|آتی|مارجین)(?![\w])"
    r"|(?<![\d.])\d{1,3}(?:\.\d+)?\s*[Xx](?![\w])",
    re.IGNORECASE,
)
_SPOT_RE = re.compile(r"(?<![\w])(?:SPOT|اسپات|نقدی)(?![\w])", re.IGNORECASE)
_MARKET_ORDER_RE = re.compile(r"(?<![\w])(?:MARKET|مارکت)(?![\w])", re.IGNORECASE)
_LIMIT_ORDER_RE = re.compile(r"(?<![\w])(?:LIMIT|لیمیت)(?![\w])", re.IGNORECASE)
_MODIFICATION_RE = re.compile(
    r"(?<![\w])(?:UPDATE[D]?|CORRECTION|CORRECTED|REVISE[D]?|CHANGE[D]?|EDIT(?:ED)?)(?![\w])"
    r"|اصلاح|ویرایش|به\s*روز|بروزرسانی|آپدیت|تغییر",
    re.IGNORECASE,
)
_SL_UPDATE_RE = re.compile(
    r"(?<![\w])(?:MOVE[D]?|TRAIL(?:ING)?|RAISE[D]?|LOWER(?:ED)?)\s+(?:THE\s+)?"
    r"(?:SL|STOP(?:\s*LOSS)?)(?![\w])"
    r"|(?:SL|STOP(?:\s*LOSS)?)\s*(?:MOVE[D]?|TO|NOW|UPDATE[D]?|NEW)(?![\w])"
    r"|(?:حد\s*ضرر|حد\s*زیان|استاپ(?:\s*لاس)?)\s*(?:را\s*)?"
    r"(?:انتقال|تغییر|جدید|بالا|پایین|بروز|آپدیت)",
    re.IGNORECASE,
)
_TP_UPDATE_RE = re.compile(
    r"(?<![\w])(?:TP|TARGETS?|TAKE\s*PROFIT)\s*\d*\s*"
    r"(?:MOVE[D]?|TO|NOW|UPDATE[D]?|NEW|CHANGE[D]?)(?![\w])"
    r"|(?:NEW|UPDATED?|REVISED?)\s+(?:TP|TARGETS?)(?![\w])"
    r"|(?:تارگت|هدف|تی\s*پی|حد\s*سود)\s*(?:را\s*)?"
    r"(?:جدید|تغییر|انتقال|بالا|پایین|بروز|آپدیت)",
    re.IGNORECASE,
)
_HISTORICAL_RE = re.compile(
    r"(?<![\w])(?:YESTERDAY|PREVIOUS(?:LY)?|HISTORICAL|LAST\s+(?:WEEK|MONTH|SIGNAL))(?![\w])"
    r"|دیروز|قبلی|گذشته|هفته\s*گذشته|ماه\s*گذشته|تاریخی",
    re.IGNORECASE,
)
_QUOTED_RE = re.compile(r"^\s*>|نقل\s*قول|گفته\s*بود", re.IGNORECASE)
_URL_RE = re.compile(r"(?:https?://|t\.me/|www\.)\S+", re.IGNORECASE)
_EMBEDDED_INSTRUCTION_RE = re.compile(
    r"(?:ignore|disregard)\s+(?:all\s+)?(?:previous|prior)\s+instructions"
    r"|system\s*prompt|api\s*key|private\s*key|seed\s*phrase|send\s+.*(?:key|seed|password)"
    r"|دستور(?:های)?\s*قبلی|کلید\s*خصوصی|پسورد|رمز\s*عبور",
    re.IGNORECASE,
)
_ALL_RE = re.compile(r"(?<![\w])ALL(?![\w])|همه", re.IGNORECASE)
_EXPLICIT_REFERENCE_RE = re.compile(
    r"(?<![\w])(?:#|MSG\s*#?|MESSAGE\s*#?|SIGNAL\s*#?|ID\s*#?|"
    r"پیام\s*#?|سیگنال\s*#?)\s*(\d{1,12})(?![\w])",
    re.IGNORECASE,
)
_ORDINAL_INDEX = {"اول": 1, "دوم": 2, "سوم": 3, "چهارم": 4, "پنجم": 5}
_DIGIT_TRANSLATION = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")
_KEEP_PUNCTUATION = set("/-_.,:$%#@()[]~+=!?*&'\"|")


def normalize_text(text: str) -> str:
    """Normalize digits, Arabic/Persian letters, emoji and whitespace.

    The result is deterministic and is used for matching only. The original
    Telegram text remains untouched in ``messages``.
    """

    text = text.translate(_DIGIT_TRANSLATION)
    for src, dst in (
        ("\u00a0", " "),
        ("\u200c", " "),  # ZWNJ: "به\u200cروز" -> "به روز"
        ("\u200e", ""),
        ("\u200f", ""),
        ("\u202a", ""),
        ("\u202b", ""),
        ("\u202c", ""),
        ("٫", "."),
        ("٬", ","),
        ("ي", "ی"),
        ("ك", "ک"),
        ("ۀ", "ه"),
        ("ـ", ""),
        ("٪", "%"),
    ):
        text = text.replace(src, dst)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    out: list[str] = []
    for ch in text:
        if ch.isalnum() or ch.isspace() or ch in _KEEP_PUNCTUATION:
            out.append(ch)
        else:
            out.append(" ")
    return re.sub(r"\s+", " ", "".join(out)).strip().upper()


@dataclass(frozen=True)
class _NumberValue:
    value: Decimal
    end: int
    scaled: bool = False


@dataclass(frozen=True)
class _LabeledValue:
    values: tuple[Decimal, ...]
    start: int
    end: int
    index: int | None = None
    is_range: bool = False
    scaled: bool = False
    error: str | None = None


def _to_decimal(raw: str, suffix: str | None = None) -> Decimal:
    cleaned = raw.replace(",", "").replace(" ", "")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid number {raw!r}") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError(f"non-positive or non-finite value {raw!r}")
    if suffix and suffix.upper() == "K":
        value *= Decimal(1000)
    elif suffix and suffix.upper() == "M":
        value *= Decimal(1_000_000)
    return value


def _number_at(tail: str) -> _NumberValue | None:
    m = _NUMBER_AT_RE.match(tail)
    if not m:
        return None
    after = tail[m.end():]
    if after.lstrip().startswith("%"):
        raise ValueError("percentage is not a price")
    value = _to_decimal(m.group(1), m.group(2))
    return _NumberValue(value=value, end=m.end(), scaled=bool(m.group(2)))


def _collect_labeled(text: str, pattern: re.Pattern[str]) -> list[_LabeledValue]:
    found: list[_LabeledValue] = []
    for match in pattern.finditer(text):
        tail = text[match.end(): match.end() + 140]
        stripped = len(tail) - len(tail.lstrip(" \t:=-–—|•،؛"))
        tail = tail[stripped:]
        update_prefix = _LEADING_UPDATE_WORD_RE.match(tail)
        if update_prefix:
            tail = tail[update_prefix.end():].lstrip(" \t:=-–—|•،؛")
        if not tail:
            continue
        index = _label_index(match)
        try:
            between = _BETWEEN_AT_RE.match(tail)
            range_match = _RANGE_AT_RE.match(tail)
            if between:
                low = _to_decimal(between.group(1))
                high = _to_decimal(between.group(2))
                found.append(_LabeledValue((low, high), match.start(), match.end() + stripped + between.end(),
                                           index=index, is_range=True))
                continue
            if range_match:
                low = _to_decimal(range_match.group(1))
                high = _to_decimal(range_match.group(2))
                found.append(_LabeledValue((low, high), match.start(), match.end() + stripped + range_match.end(),
                                           index=index, is_range=True))
                continue
            number = _number_at(tail)
            if number is not None:
                found.append(_LabeledValue((number.value,), match.start(), match.end() + stripped + number.end,
                                           index=index, scaled=number.scaled))
        except (ValueError, InvalidOperation) as exc:
            found.append(_LabeledValue((), match.start(), match.end(), index=index, error=str(exc)))
    return found


def _label_index(match: re.Match[str]) -> int | None:
    for group in match.groups():
        if not group:
            continue
        token = str(group).strip()
        if token.isdigit():
            value = int(token)
            return value if value > 0 else None
        if token in _ORDINAL_INDEX:
            return _ORDINAL_INDEX[token]
    return None


def _extract_side(text: str) -> tuple[Side | None, list[str], str]:
    buys = len(_BUY_RE.findall(text))
    sells = len(_SELL_RE.findall(text))
    longs = len(_LONG_RE.findall(text))
    shorts = len(_SHORT_RE.findall(text))
    ambiguities: list[str] = []
    confidence = "high"

    buy_intent = buys + longs
    sell_intent = sells + shorts
    if longs and not buys:
        confidence = "medium"  # LONG is non-Spot; recorded for review, never traded
    if shorts and not sells:
        confidence = "medium"
    if buy_intent and sell_intent:
        ambiguities.append("both_buy_and_sell_directions")
        return None, ambiguities, "low"
    if buy_intent:
        return Side.BUY, ambiguities, confidence
    if sell_intent:
        return Side.SELL, ambiguities, confidence
    return None, ambiguities, "low"


def _valid_symbol_base(base: str) -> bool:
    return (
        bool(re.search(r"[A-Z]", base))
        and base not in _NON_SYMBOL_WORDS
        and not base.startswith(("TP", "SL"))
    )


def _extract_symbols(text: str) -> tuple[list[str], list[str], str]:
    candidates: list[tuple[str, str]] = []
    confidence = "high"

    for m in re.finditer(r"(?<![\w])([A-Z0-9]{2,15})\s*/\s*([A-Z0-9]{2,10})(?![\w])", text):
        base, quote = m.group(1), m.group(2)
        if quote in QUOTE_ASSETS and _valid_symbol_base(base) and base + quote not in _NON_SYMBOL_WORDS:
            candidates.append((base + quote, quote))

    for m in re.finditer(r"(?<![\w])([A-Z0-9]{2,15})\s*[-_]\s*([A-Z]{2,10})(?![\w])", text):
        base, quote = m.group(1), m.group(2)
        if quote in QUOTE_ASSETS and _valid_symbol_base(base) and base + quote not in _NON_SYMBOL_WORDS:
            candidates.append((base + quote, quote))

    for quote in sorted(QUOTE_ASSETS, key=len, reverse=True):
        for m in re.finditer(rf"(?<![\w])([A-Z0-9]{{2,15}})({quote})(?![\w])", text):
            base = m.group(1)
            full = base + quote
            if _valid_symbol_base(base) and full not in _NON_SYMBOL_WORDS:
                candidates.append((full, quote))

    for m in re.finditer(r"\$([A-Z][A-Z0-9]{1,14})(?![\w])", text):
        base = m.group(1)
        if base not in QUOTE_ASSETS and _valid_symbol_base(base):
            candidates.append((base + "USDT", "USDT"))
            confidence = "medium"  # quote asset was inferred from the dollar prefix

    seen: list[tuple[str, str]] = []
    for item in candidates:
        if item not in seen:
            seen.append(item)
    return [s for s, _ in seen], [q for _, q in seen], confidence


def _extract_reference(text: str, reply_to_message_id: int | None) -> tuple[int | None, list[str]]:
    ambiguities: list[str] = []
    refs: list[int] = []
    for m in _EXPLICIT_REFERENCE_RE.finditer(text):
        try:
            value = int(m.group(1))
        except ValueError:
            continue
        if value > 0 and value not in refs:
            refs.append(value)
    if len(refs) > 1 or (reply_to_message_id is not None and refs and reply_to_message_id not in refs):
        ambiguities.append("conflicting_message_references")
        return None, ambiguities
    if reply_to_message_id is not None:
        return reply_to_message_id, ambiguities
    return (refs[0] if refs else None), ambiguities


def _is_cancel(text: str) -> bool:
    # Words inside a URL (for example ".../invalid") are not instructions.
    text = _URL_RE.sub(" ", text)
    match = _CANCEL_WORD_RE.search(text)
    if match is None:
        return False
    token = match.group(0).upper()
    # "Ignore previous instructions" is prompt injection, not an order cancel.
    if token == "IGNORE" and _EMBEDDED_INSTRUCTION_RE.search(text):
        return False
    if match.start() == 0:
        return True
    prefix = text[: match.start()]
    # "price may close above resistance" is commentary, not a cancel
    # instruction. Explicit words such as CANCEL are not suppressed by a
    # nearby target/entry label in the same sentence.
    if token in {"CLOSE", "CLOSED", "CLOSES"} and any(
        label.search(prefix) for label in (_ENTRY_LABEL_RE, _TP_LABEL_RE, _SL_LABEL_RE)
    ):
        return False
    if _ALL_RE.search(text) or _extract_symbols(text)[0]:
        return True
    return bool(
        re.search(
            r"(?:CANCEL|CLOSE|لغو|کنسل).{0,40}(?:SIGNAL|ORDER|POSITION|سیگنال|سفارش|پوزیشن)",
            text,
            re.IGNORECASE,
        )
    )


def _dedupe_targets(values: list[TargetPrice]) -> tuple[tuple[TargetPrice, ...], list[str]]:
    seen: set[tuple[int | None, str]] = set()
    out: list[TargetPrice] = []
    notes: list[str] = []
    for item in values:
        key = (item.index, format(item.price, "f"))
        if key in seen:
            notes.append("duplicate_target_ignored")
            continue
        seen.add(key)
        out.append(item)
    return tuple(out), notes


def parse_message(
    text: str | None,
    *,
    is_edit: bool = False,
    forwarded: bool = False,
    media_only: bool = False,
    reply_to_message_id: int | None = None,
) -> ParsedCandidate:
    """Classify and parse one already-persisted message.

    Version 2 first tries the structured channel-format parser; messages
    that do not use it fall back to the (unchanged) generic heuristics of
    version 1, so synthetic/older corpora keep parsing identically.
    """

    if text is not None and text.strip():
        structured = parse_structured(text)
        if structured is not None:
            return replace(
                structured,
                text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
                field_confidence=dict(structured.field_confidence),
            )
    return parse_message_generic(
        text,
        is_edit=is_edit,
        forwarded=forwarded,
        media_only=media_only,
        reply_to_message_id=reply_to_message_id,
    )


def parse_message_generic(
    text: str | None,
    *,
    is_edit: bool = False,
    forwarded: bool = False,
    media_only: bool = False,
    reply_to_message_id: int | None = None,
) -> ParsedCandidate:
    """Generic (version-1) heuristic classification of one message."""

    raw = text or ""
    normalized = normalize_text(raw)
    # URLs are never followed, and their path segments must not be mistaken
    # for symbols or instructions.
    normalized = _URL_RE.sub(" ", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    text_sha256 = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    if not normalized:
        empty_reasons = (
            ("media without text; image/OCR content is never guessed",)
            if media_only
            else ("empty message",)
        )
        empty_ambiguities = ("media_without_text",) if media_only else ()
        return ParsedCandidate(
            classification=SignalClassification.NON_SIGNAL,
            market_type=MarketType.UNKNOWN,
            symbol=None,
            quote_asset=None,
            side=None,
            entry_price=None,
            entry_low_price=None,
            entry_high_price=None,
            stop_loss=None,
            targets=(),
            order_type=None,
            referenced_message_id=None,
            parse_status=(
                ParseStatus.NEEDS_REVIEW if empty_ambiguities else ParseStatus.PARSED
            ),
            field_confidence={"classification": "high"},
            ambiguities=empty_ambiguities,
            reasons=empty_reasons,
            normalized_text="",
            text_sha256=text_sha256,
        )

    ambiguities: list[str] = []
    reasons: list[str] = []
    confidence: dict[str, str] = {}

    side, side_ambiguities, side_conf = _extract_side(normalized)
    ambiguities.extend(side_ambiguities)
    confidence["side"] = side_conf

    symbols, quotes, symbol_conf = _extract_symbols(normalized)
    symbol: str | None = symbols[0] if len(symbols) == 1 else None
    quote_asset: str | None = quotes[0] if len(quotes) == 1 else None
    confidence["symbol"] = symbol_conf
    if len(symbols) > 1:
        ambiguities.append("multiple_symbols:" + ",".join(symbols[:4]))

    entry_values = _collect_labeled(normalized, _ENTRY_LABEL_RE)
    entry_price: Decimal | None = None
    entry_low: Decimal | None = None
    entry_high: Decimal | None = None
    valid_entries = [item for item in entry_values if item.values]
    for item in entry_values:
        if item.error:
            ambiguities.append(f"invalid_entry:{item.error}")
    distinct_entries = {tuple(format(v, "f") for v in item.values) for item in valid_entries}
    if len(distinct_entries) > 1:
        ambiguities.append("conflicting_entry_prices")
    elif valid_entries:
        item = valid_entries[0]
        if item.is_range and len(item.values) >= 2:
            low, high = item.values[0], item.values[1]
            if low > high:
                low, high = high, low
                ambiguities.append("entry_range_reversed")
            entry_low, entry_high = low, high
            if low == high:
                entry_price = low
        else:
            entry_price = item.values[0]
        confidence["entry"] = "medium" if item.scaled else "high"

    target_values = _collect_labeled(normalized, _TP_LABEL_RE)
    targets: list[TargetPrice] = []
    for item in target_values:
        if item.error:
            ambiguities.append(f"invalid_target:{item.error}")
            continue
        for value in item.values:
            targets.append(TargetPrice(index=item.index, price=value))
        if item.is_range:
            ambiguities.append("target_range_requires_review")
        confidence["targets"] = "medium" if item.scaled or item.is_range else "high"
    targets_tuple, target_notes = _dedupe_targets(targets)
    ambiguities.extend(target_notes)

    stop_values = _collect_labeled(normalized, _SL_LABEL_RE)
    stop_loss: Decimal | None = None
    distinct_stops = {format(item.values[0], "f") for item in stop_values if item.values}
    for item in stop_values:
        if item.error:
            ambiguities.append(f"invalid_stop_loss:{item.error}")
    if len(distinct_stops) > 1:
        ambiguities.append("conflicting_stop_loss_prices")
    elif stop_values:
        item = stop_values[0]
        if item.is_range:
            ambiguities.append("stop_loss_range_requires_review")
        elif item.values:
            stop_loss = item.values[0]
            confidence["stop_loss"] = "medium" if item.scaled else "high"

    explicit_spot = bool(_SPOT_RE.search(normalized))
    non_spot = bool(_NON_SPOT_RE.search(normalized))
    if non_spot:
        market_type = MarketType.NON_SPOT
        ambiguities.append("non_spot_signal_not_actionable")
        confidence["market_type"] = "high"
        reasons.append("non-Spot signal (futures/short/leverage is out of scope)")
    elif explicit_spot:
        market_type = MarketType.SPOT
        confidence["market_type"] = "high"
    elif symbol is not None:
        market_type = MarketType.SPOT
        confidence["market_type"] = "medium"
        reasons.append("Spot inferred from pair format; no futures/leverage marker")
    else:
        market_type = MarketType.UNKNOWN
        confidence["market_type"] = "low"

    order_type: str | None = None
    market_order = bool(
        _MARKET_ORDER_RE.search(normalized)
        and (
            side is not None
            or re.search(r"(?:MARKET|مارکت)\s*(?:ORDER|سفارش)", normalized, re.IGNORECASE)
        )
    )
    limit_order = bool(
        _LIMIT_ORDER_RE.search(normalized)
        and (
            side is not None
            or re.search(r"(?:LIMIT|لیمیت)\s*(?:ORDER|سفارش)", normalized, re.IGNORECASE)
        )
    )
    if market_order:
        order_type = "market"
        confidence["order_type"] = "high"
    elif limit_order:
        order_type = "limit"
        confidence["order_type"] = "high"

    referenced_message_id, ref_ambiguities = _extract_reference(normalized, reply_to_message_id)
    ambiguities.extend(ref_ambiguities)

    result = bool(_RESULT_RE.search(normalized))
    cancel = _is_cancel(normalized)
    sl_update = bool(_SL_UPDATE_RE.search(normalized))
    tp_update = bool(_TP_UPDATE_RE.search(normalized))
    modification = bool(_MODIFICATION_RE.search(normalized))
    # An "UPDATE <symbol> TP 115" / "UPDATE <symbol> SL 96" message changes a
    # single field even without the words "moved"/"new". Keep the more precise
    # classification only when no other signal field is being changed.
    if modification and not sl_update and not tp_update:
        single_field = (
            side is None
            and entry_price is None
            and entry_low is None
        )
        if single_field and target_values and not stop_values:
            tp_update = True
        elif single_field and stop_values and not target_values:
            sl_update = True
    historical = bool(_HISTORICAL_RE.search(normalized))
    has_new_signal_shape = bool(
        side is not None
        and symbol is not None
        and (entry_price is not None or entry_low is not None or order_type == "market")
    )
    signalish = bool(
        side is not None
        or symbol is not None
        or entry_values
        or target_values
        or stop_values
        or order_type is not None
        or non_spot
    )

    if forwarded:
        classification = SignalClassification.NON_SIGNAL
        reasons.append("forwarded content is context, not a new instruction")
    elif result and cancel:
        classification = SignalClassification.AMBIGUOUS
        ambiguities.append("result_and_cancel_in_same_message")
    elif result:
        classification = SignalClassification.RESULT
        reasons.append("outcome/result report")
    elif cancel:
        classification = SignalClassification.CANCELLATION
    elif sl_update and (stop_loss is not None or is_edit or stop_values):
        classification = SignalClassification.STOP_LOSS_UPDATE
    elif tp_update:
        classification = SignalClassification.TARGET_UPDATE
    elif non_spot:
        classification = SignalClassification.AMBIGUOUS
    elif (is_edit or modification) and signalish:
        classification = SignalClassification.MODIFICATION
    elif has_new_signal_shape:
        classification = SignalClassification.NEW_SIGNAL
    elif signalish:
        classification = SignalClassification.AMBIGUOUS
    else:
        classification = SignalClassification.NON_SIGNAL
        reasons.append("no trading instruction found")

    if historical:
        ambiguities.append("historical_context_requires_review")
        reasons.append("historical wording detected; not treated as a new instruction")

    if _QUOTED_RE.search(raw):
        ambiguities.append("quoted_context_requires_review")
    if _EMBEDDED_INSTRUCTION_RE.search(normalized):
        ambiguities.append("embedded_instruction_ignored")
    if _URL_RE.search(raw):
        reasons.append("link present but never followed")

    if classification == SignalClassification.NEW_SIGNAL:
        if order_type != "market" and entry_price is None and entry_low is None:
            ambiguities.append("missing_entry_price")
        if not targets_tuple:
            ambiguities.append("missing_take_profit")
        if stop_loss is None:
            ambiguities.append("missing_stop_loss")
    elif classification == SignalClassification.MODIFICATION:
        if not symbols:
            ambiguities.append("missing_symbol_for_update")
        if not targets_tuple and stop_loss is None and entry_price is None and entry_low is None:
            ambiguities.append("update_has_no_changed_field")
    elif classification == SignalClassification.TARGET_UPDATE:
        if not targets_tuple:
            ambiguities.append("missing_target_for_update")
        if not symbols:
            ambiguities.append("missing_symbol_for_update")
    elif classification == SignalClassification.STOP_LOSS_UPDATE:
        if stop_loss is None:
            ambiguities.append("missing_stop_loss_for_update")
        if not symbols:
            ambiguities.append("missing_symbol_for_update")
    elif classification == SignalClassification.CANCELLATION:
        if not symbols and not _ALL_RE.search(normalized):
            ambiguities.append("cancellation_without_symbol_or_all")
    elif classification == SignalClassification.AMBIGUOUS:
        if non_spot:
            pass
        elif side is None and symbol is None:
            ambiguities.append("no_direction_or_symbol")
        elif side is None:
            ambiguities.append("missing_direction")
        elif symbol is None:
            ambiguities.append("missing_symbol")
        elif entry_price is None and entry_low is None and order_type != "market":
            ambiguities.append("missing_entry_price")

    if classification in (
        SignalClassification.NEW_SIGNAL,
        SignalClassification.MODIFICATION,
    ):
        if side is not None and stop_loss is not None:
            reference = entry_low if side == Side.BUY else entry_high
            reference = reference or entry_price
            if reference is not None:
                if side == Side.BUY and stop_loss >= reference:
                    ambiguities.append("stop_loss_not_below_entry")
                if side == Side.SELL and stop_loss <= reference:
                    ambiguities.append("stop_loss_not_above_entry")
        if side is not None and targets_tuple:
            reference = entry_high if side == Side.BUY else entry_low
            reference = reference or entry_price
            if reference is not None:
                if side == Side.BUY and any(t.price <= reference for t in targets_tuple):
                    ambiguities.append("target_not_above_entry")
                if side == Side.SELL and any(t.price >= reference for t in targets_tuple):
                    ambiguities.append("target_not_below_entry")

    confidence["classification"] = "low" if classification == SignalClassification.AMBIGUOUS else "high"
    parse_status = ParseStatus.NEEDS_REVIEW if ambiguities else ParseStatus.PARSED

    return ParsedCandidate(
        classification=classification,
        market_type=market_type,
        symbol=symbol,
        quote_asset=quote_asset,
        side=side,
        entry_price=entry_price,
        entry_low_price=entry_low,
        entry_high_price=entry_high,
        stop_loss=stop_loss,
        targets=targets_tuple,
        order_type=order_type,
        referenced_message_id=referenced_message_id,
        parse_status=parse_status,
        field_confidence=confidence,
        ambiguities=tuple(dict.fromkeys(ambiguities)),
        reasons=tuple(dict.fromkeys(reasons)),
        normalized_text=normalized,
        text_sha256=text_sha256,
    )


class SignalAnalysisParser:
    """Thin, versioned facade used by the replay pipeline."""

    version = ANALYSIS_PARSER_VERSION

    def parse(
        self,
        text: str | None,
        *,
        is_edit: bool = False,
        forwarded: bool = False,
        media_only: bool = False,
        reply_to_message_id: int | None = None,
    ) -> ParsedCandidate:
        return parse_message(
            text,
            is_edit=is_edit,
            forwarded=forwarded,
            media_only=media_only,
            reply_to_message_id=reply_to_message_id,
        )
