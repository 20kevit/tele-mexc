"""Recognize the channel's entry template. Trading policy belongs to the engine."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal

PARSER_VERSION = 3
_PRICE = r"[0-9]+(?:\.[0-9]+)?"
SIGNAL_PATTERN = re.compile(
    rf"""
    \s*(?:🆔\s*)?\#\s*(?P<signal_id>[0-9]+)\s+
    (?:⏰\s*)?(?P<date>\d{{4}}[-/.]\d{{1,2}}[-/.]\d{{1,2}})\s+
    TIME\s*[:=]\s*(?P<time>(?:[01]\d|2[0-3]):[0-5]\d)\s+
    \*\s*(?P<direction>LONG|SHORT)\s*\*(?:\s*[🟢🔴])?\s*
    (?:⚡️?\s*)?TOOBIT\s*:\s*(?P<symbol>(?a:[A-Z0-9]+))\s*\.P\s*\(\s*15\s*\)\s*
    (?:✅\s*)?Entry\s*[:=]\s*(?P<entry>{_PRICE})\s+
    (?:🔰\s*)?Stop\s*Loss\s*[:=]\s*(?P<stop>{_PRICE})\s+
    """
    + r"\s+".join(rf"(?:📌\s*)?TP\s*{i}\s*[:=]\s*(?P<tp{i}>{_PRICE})" for i in range(1, 5))
    # Ignore only recognized footer fields, not arbitrary result posts. Free
    # text belongs to its own footer line (including the rest of that line).
    + rf"(?:\s+(?:[-–—]+|(?:🤑\s*)?FULL\s+Target\s*[:=]\s*{_PRICE}|"
    r"(?:💰\s*)?Deposit\b[^\r\n]*+|(?:💠\s*)?MAX\s+LEV\b[^\r\n]*+|[💬⭐][^\r\n]*+))*\s*",
    re.VERBOSE | re.IGNORECASE,
)


@dataclass(frozen=True)
class ParsedSignal:
    kind: str  # signal | additional
    raw_text: str
    normalized_text: str
    fingerprint_sha256: str
    signal_id: str | None = None  # printed channel ID, not the database key
    date: str | None = None
    time: str | None = None
    action: str | None = None  # LONG -> buy, SHORT -> sell
    symbol: str | None = None
    entry_price: Decimal | None = None
    stop_loss: Decimal | None = None
    target_prices: tuple[Decimal, ...] = ()
    # RiskManager also validates directly constructed range-only inputs.
    entry_low_price: Decimal | None = None
    entry_high_price: Decimal | None = None


class SignalParser:
    version = PARSER_VERSION

    def parse(self, text: str | None) -> ParsedSignal:
        raw = text or ""
        normalized = " ".join(raw.split()).upper()
        fingerprint = hashlib.sha256(normalized.encode()).hexdigest()
        match = SIGNAL_PATTERN.fullmatch(raw)
        if match is None:
            return ParsedSignal("additional", raw, normalized, fingerprint)
        return ParsedSignal(
            "signal",
            raw,
            normalized,
            fingerprint,
            signal_id=match["signal_id"],
            date=match["date"],
            time=match["time"],
            action="buy" if match["direction"].upper() == "LONG" else "sell",
            symbol=match["symbol"].upper(),
            entry_price=Decimal(match["entry"]),
            stop_loss=Decimal(match["stop"]),
            target_prices=tuple(Decimal(match[f"tp{i}"]) for i in range(1, 5)),
        )
