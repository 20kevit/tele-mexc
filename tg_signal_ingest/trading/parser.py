"""Deterministic signal parser for plain-text Telegram channel messages.

The parser never guesses missing financial fields: anything it cannot parse
confidently becomes ``ambiguous`` (and is rejected by the risk layer) rather
than an order.  The bot only executes Spot BUY orders.  Channel messages that
describe a LONG direction are treated as a BUY; SHORT/SELL directions are
parsed so they can be reported, but the risk layer refuses to execute them.

Follow-up posts ("TP1 hit", "entry activated", "position closed", profit
reports) are classified as results and are never executable, even if they
contain a direction word or a price.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

PARSER_VERSION = 2

QUOTE_ASSETS = (
    "USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "BTC", "ETH", "EUR", "TRY", "BRZ", "USD",
)
_NON_SYMBOL_WORDS = {
    "BUY", "SELL", "LONG", "SHORT", "ENTRY", "TARGET", "TARGETS", "STOP", "LOSS", "PRICE",
    "AMOUNT", "QUANTITY", "MARKET", "LIMIT", "SIGNAL", "UPDATE", "CANCEL", "CLOSE", "TAKE",
    "PROFIT", "ALERT", "SPOT", "ORDER", "ZONE", "RANGE", "TP", "SL", "USD", "TIME", "DEPOSIT",
    "LEV", "SUGGESTED", "MAX", "FULL",
}

_NUM = r"(\d[\d,]*(?:\.\d+)?)"
_RANGE = rf"{_NUM}\s*(?:-|–|—|TO)\s*{_NUM}"
_PRICE = rf"(?:\$\s*)?{_NUM}"

_CANCEL_RE = re.compile(r"^(?:CANCEL|CANCELLED|CANCELED|INVALID|IGNORE|ABORT|CLOSE|CLOSED)\b", re.I)
_RESULT_RE = re.compile(
    r"\b(?:TP\s*\d*\s*(?:LONG|SHORT)?\s*(?:HIT|DONE|REACHED)|HIT\s+TP|"
    r"TARGET\s*\d*\s*(?:HIT|REACHED)|PROFIT\s+BOOKED|BOOKED\s+PROFIT|PROFIT\s*:|"
    r"POSITION\s+(?:CLOSED|OPENED|ENTERED|ACTIVATED)|"
    r"ENTRY\s+(?:ACTIVATED|TRIGGERED|FILLED)|TP\s*\d*\s*✅|✅\s*TP)\b"
    r"|(?:تارگت|هدف)\s*\d*[^.\n]{0,25}?(?:خورد|رسید|فعال|شد|مبارک)"
    r"|ورود[\s\S]{0,40}?(?:تریگر|ثبت|فعال|انجام|وارد)"
    r"|(?:بستن|بسته)\s*(?:پوزیشن|SHORT|LONG)?"
    r"|سود\s*:",
    re.I,
)
_BUY_RE = re.compile(r"\b(?:BUY|LONG)\b", re.I)
_SELL_RE = re.compile(r"\b(?:SELL|SHORT)\b", re.I)
_NON_SPOT_RE = re.compile(
    r"\b(?:FUTURES?|PERPETUAL|PERP|MARGIN|LEVERAGED)\b|\bLEVERAGE\b|\b\d{1,3}(?:\.\d+)?\s*[Xx]\b",
    re.I,
)
_ENTRY_RE = re.compile(
    rf"\b(?:ENTRY(?:\s*(?:PRICE|ZONE|RANGE|AT))?|BUY\s*(?:AT|@)|SELL\s*(?:AT|@)|"
    rf"ورود(?:\s*(?:در|با))?\s*(?:قیمت)?)\s*[:=]?\s*(?:{_RANGE}|{_PRICE})",
    re.I,
)
_TARGET_RE = re.compile(rf"\b(?:TP|TARGET|TAKE\s*PROFIT)\s*\d*\s*[:=]?\s*{_PRICE}", re.I)
_STOP_RE = re.compile(rf"\b(?:SL|STOP\s*LOSS|STOPLOSS|STOP|حد\s*ضرر)\s*[:=]?\s*{_PRICE}", re.I)
_AMOUNT_LABEL_RE = re.compile(
    rf"\b(?:AMOUNT|QTY|QUANTITY|SIZE)\s*[:=]?\s*(\$?\s*{_NUM}\s*[KkMm]?\s*[A-Z]{{0,10}})", re.I
)
_DOLLAR_AMOUNT_RE = re.compile(rf"\$\s*{_NUM}\s*([KkMm]?)")
_EXCHANGE_PREFIX_RE = re.compile(r"\b[A-Z0-9]{2,16}\s*:\s*(?=[A-Z])")
_PERP_SUFFIX_RE = re.compile(r"(?<=[A-Z0-9])\.P(?:\b|\(|\s|$)", re.I)


@dataclass(frozen=True)
class ParsedSignal:
    kind: str  # alert | update | cancel | result | info | ambiguous
    action: str | None  # buy | sell
    symbol: str | None
    entry_price: Decimal | None
    entry_low_price: Decimal | None
    entry_high_price: Decimal | None
    target_prices: tuple[Decimal, ...]
    stop_loss: Decimal | None
    order_type: str | None
    amount_text: str | None
    raw_text: str
    normalized_text: str
    fingerprint_sha256: str
    reasons: tuple[str, ...] = ()

    @property
    def actionable(self) -> bool:
        return self.kind in ("alert", "update") and self.action is not None and self.symbol is not None

    @property
    def reason_text(self) -> str:
        return "; ".join(self.reasons)


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\u00a0", " ")).strip().upper()


def _num(raw: str, *, suffix: str = "") -> Decimal:
    cleaned = raw.replace(",", "").replace("$", "").replace(" ", "")
    try:
        value = Decimal(cleaned)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid number {raw!r}") from exc
    factor = suffix.upper()
    if factor == "K":
        value *= Decimal(1000)
    elif factor == "M":
        value *= Decimal(1_000_000)
    return value


def _clean_symbol_text(text: str) -> str:
    # Social handles at the bottom of channel templates (e.g. @example_handle)
    # must never be mistaken for a trading pair.
    cleaned = re.sub(r"@[A-Za-z0-9_]+", " ", text)
    cleaned = re.sub(r"https?://\S+", " ", cleaned)
    cleaned = _EXCHANGE_PREFIX_RE.sub(" ", cleaned)
    cleaned = _PERP_SUFFIX_RE.sub("", cleaned)
    return cleaned


def _extract_symbol(text: str) -> str | None:
    symbol_text = _clean_symbol_text(text)
    m = re.search(r"\b(?=[A-Z0-9]*[A-Z])([A-Z0-9]{2,15})\s*/\s*([A-Z0-9]{2,10})\b", symbol_text)
    if m:
        return m.group(1) + m.group(2)
    m = re.search(r"\b(?=[A-Z0-9]*[A-Z])([A-Z0-9]{2,15})\s*[-_]\s*([A-Z]{2,10})\b", symbol_text)
    if m:
        return m.group(1) + m.group(2)
    m = re.search(r"\$([A-Z][A-Z0-9]{1,14})\b", symbol_text)
    if m:
        base = m.group(1)
        if base in QUOTE_ASSETS or base in _NON_SYMBOL_WORDS:
            return None
        return base + "USDT"
    for quote in sorted(QUOTE_ASSETS, key=len, reverse=True):
        m = re.search(rf"\b(?=[A-Z0-9]*[A-Z])([A-Z0-9]{{2,15}}){quote}\b", symbol_text)
        if (
            m
            and m.group(0) not in _NON_SYMBOL_WORDS
            and m.group(1) not in _NON_SYMBOL_WORDS
        ):
            return m.group(1) + quote
    return None


def _extract_entry(text: str) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    m = _ENTRY_RE.search(text)
    if not m:
        return None, None, None
    groups = [g for g in m.groups() if g is not None]
    numbers = [_num(g) for g in groups]
    if len(numbers) >= 2:
        low, high = numbers[0], numbers[1]
        if low > high:
            low, high = high, low
        return (low + high) / Decimal(2), low, high
    return numbers[0], None, None


def _extract_targets(text: str) -> tuple[Decimal, ...]:
    out: list[Decimal] = []
    for m in _TARGET_RE.finditer(text):
        try:
            out.append(_num(m.group(1)))
        except ValueError:
            continue
    return tuple(out)


def _extract_stop(text: str) -> tuple[Decimal | None, str | None]:
    m = _STOP_RE.search(text)
    if not m:
        return None, None
    try:
        return _num(m.group(1)), None
    except ValueError:
        return None, "invalid stop-loss value"


def _extract_amount(text: str) -> str | None:
    m = _AMOUNT_LABEL_RE.search(text)
    if m:
        return m.group(1).strip()
    m = _DOLLAR_AMOUNT_RE.search(text)
    if m:
        return f"${m.group(1)}{m.group(2)}"
    return None


def _fingerprint(normalized: str) -> str:
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


class SignalParser:
    version = PARSER_VERSION

    def parse(self, text: str | None, *, is_edit: bool = False) -> ParsedSignal:
        raw = text or ""
        normalized = normalize_text(raw)
        fingerprint = _fingerprint(normalized)
        default_kind = "update" if is_edit else "alert"

        if not normalized:
            return ParsedSignal(
                "info", None, None, None, None, None, (), None, None, None,
                raw, normalized, fingerprint,
            )

        cancel = bool(_CANCEL_RE.match(normalized))
        result = bool(_RESULT_RE.search(normalized)) and not cancel
        action = self._action(normalized)
        symbol = _extract_symbol(normalized)
        reasons: list[str] = []

        if cancel:
            return ParsedSignal(
                "cancel", action, symbol, None, None, None, (), None, None, None,
                raw, normalized, fingerprint,
            )

        if result:
            return ParsedSignal(
                "result", action, symbol, None, None, None, (), None, None, None,
                raw, normalized, fingerprint,
            )

        non_spot = bool(_NON_SPOT_RE.search(normalized))
        if non_spot:
            reasons.append("unsupported Futures/perpetual/leveraged instruction (Spot BUY only)")

        entry, low, high = _extract_entry(normalized)
        targets = _extract_targets(normalized)
        stop, stop_error = _extract_stop(normalized)
        amount = _extract_amount(normalized)
        order_type = "market" if re.search(r"\bMARKET\b", normalized) else None
        if stop_error:
            reasons.append(stop_error)

        if action is None:
            reasons.append("no buy/long or sell/short direction found")
        if symbol is None:
            reasons.append("no Spot symbol found")
        if action and symbol and entry is None and order_type != "market":
            reasons.append("no entry price found (limit orders require one)")

        if reasons:
            if action is None and symbol is None and not targets and stop is None:
                return ParsedSignal(
                    "info", None, None, entry, low, high, targets, stop, order_type, amount,
                    raw, normalized, fingerprint, ("not an actionable signal",),
                )
            return ParsedSignal(
                "ambiguous", action, symbol, entry, low, high, targets, stop, order_type, amount,
                raw, normalized, fingerprint, tuple(reasons),
            )

        return ParsedSignal(
            default_kind, action, symbol, entry, low, high, targets, stop, order_type, amount,
            raw, normalized, fingerprint,
        )

    @staticmethod
    def _action(text: str) -> str | None:
        buys = len(_BUY_RE.findall(text))
        sells = len(_SELL_RE.findall(text))
        if buys and not sells:
            return "buy"
        if sells and not buys:
            return "sell"
        if buys and sells:
            return None  # "long then close short" style text is too ambiguous
        return None
