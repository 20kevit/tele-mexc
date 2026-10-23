"""Canonical, offline-only signal representation.

The model is deliberately independent of the trading engine and the exchange
client. Financial values are ``Decimal`` instances; when persisted they are
stored as canonical decimal strings, never binary floats. Missing values stay
``None``: the parser never fills a price, side, symbol, stop loss, or target
just to make a candidate look complete.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any


class SignalClassification(StrEnum):
    NEW_SIGNAL = "new_signal"
    MODIFICATION = "modification"
    CANCELLATION = "cancellation"
    TARGET_UPDATE = "target_update"
    STOP_LOSS_UPDATE = "stop_loss_update"
    RESULT = "result"
    NON_SIGNAL = "non_signal"
    AMBIGUOUS = "ambiguous"


class MarketType(StrEnum):
    SPOT = "spot"
    NON_SPOT = "non_spot"
    UNKNOWN = "unknown"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Direction(StrEnum):
    """Source-facing direction token of a futures signal.

    ``LONG`` maps to a Spot buy side and ``SHORT`` is retained for historical
    review only: it must never trigger an order, a sell, or any trading action.
    """

    LONG = "long"
    SHORT = "short"


class FollowUpEvent(StrEnum):
    """Structured follow-up event of an update/result message."""

    ENTRY_ACTIVATED = "entry_activated"
    TP_HIT = "tp_hit"
    STOP_LOSS_HIT = "stop_loss_hit"
    FULL_TARGET = "full_target"
    CLOSED = "closed"
    REVERSED = "reversed"
    SL_UPDATE = "sl_update"
    TP_UPDATE = "tp_update"
    UNKNOWN = "unknown_update"


#: Quote currencies recognized when splitting a raw pair into base/quote.
#: Shared by the structured format parser and the generic heuristic parser.
QUOTE_ASSETS = (
    "USDT", "USDC", "FDUSD", "BUSD", "TUSD", "DAI", "BTC", "ETH", "EUR", "TRY", "BRZ", "USD",
)


class ParseStatus(StrEnum):
    PARSED = "parsed"
    NEEDS_REVIEW = "needs_review"
    FAILED = "failed"


class ReviewStatus(StrEnum):
    UNREVIEWED = "unreviewed"
    CONFIRMED = "confirmed"
    CORRECTED = "corrected"
    REJECTED = "rejected"


class CandidateStatus(StrEnum):
    CURRENT = "current"
    SUPERSEDED = "superseded"
    SOURCE_DELETED = "source_deleted"


class Relationship(StrEnum):
    NONE = "none"
    REPLY = "reply"
    EXPLICIT_REFERENCE = "explicit_reference"
    UNIQUE_SYMBOL = "unique_symbol"
    ALL_SYMBOLS = "all_symbols"
    AMBIGUOUS = "ambiguous"


UPDATE_CLASSIFICATIONS = frozenset(
    {
        SignalClassification.MODIFICATION,
        SignalClassification.CANCELLATION,
        SignalClassification.TARGET_UPDATE,
        SignalClassification.STOP_LOSS_UPDATE,
    }
)

ACTIONABLE_CLASSIFICATIONS = frozenset(
    {
        SignalClassification.NEW_SIGNAL,
        SignalClassification.MODIFICATION,
    }
)


@dataclass(frozen=True)
class TargetPrice:
    """One take-profit target. ``index`` is the label number when present."""

    index: int | None
    price: Decimal

    def as_json(self) -> dict[str, Any]:
        return {"index": self.index, "price": format(self.price, "f")}


@dataclass(frozen=True)
class ParsedCandidate:
    classification: SignalClassification
    market_type: MarketType
    symbol: str | None
    quote_asset: str | None
    side: Side | None
    entry_price: Decimal | None
    entry_low_price: Decimal | None
    entry_high_price: Decimal | None
    stop_loss: Decimal | None
    targets: tuple[TargetPrice, ...]
    order_type: str | None
    referenced_message_id: int | None
    parse_status: ParseStatus
    field_confidence: dict[str, str] = field(default_factory=dict)
    ambiguities: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    normalized_text: str = ""
    text_sha256: str = ""
    # --- structured-format fields (parser v2). All optional; the legacy
    # --- heuristic parser keeps them unset so persisted rows stay comparable.
    direction: Direction | None = None
    logical_id: int | None = None
    exchange: str | None = None
    symbol_raw: str | None = None
    timeframe: int | None = None
    full_target: Decimal | None = None
    deposit_pct: Decimal | None = None
    suggested_leverage: Decimal | None = None
    leverage_pct: Decimal | None = None
    follow_up_event: FollowUpEvent | None = None
    followed_logical_id: int | None = None
    target_index: int | None = None
    profit_pct: Decimal | None = None
    event_price: Decimal | None = None
    source_date_text: str | None = None
    source_time_text: str | None = None
    source_timestamp_iso: str | None = None

    @property
    def actionable(self) -> bool:
        """Machine-only flag; it is not permission to trade.

        A SHORT signal (or any other message classified as a follow-up) is
        never actionable: historical review only.
        """

        if self.direction == Direction.SHORT:
            return False
        return (
            self.classification in ACTIONABLE_CLASSIFICATIONS
            and self.side is not None
            and self.symbol is not None
        )

    @property
    def ambiguity_text(self) -> str:
        return "; ".join(self.ambiguities)

    def with_relationship(self, *, parse_status: ParseStatus, ambiguities: tuple[str, ...]) -> ParsedCandidate:
        return ParsedCandidate(
            classification=self.classification,
            market_type=self.market_type,
            symbol=self.symbol,
            quote_asset=self.quote_asset,
            side=self.side,
            entry_price=self.entry_price,
            entry_low_price=self.entry_low_price,
            entry_high_price=self.entry_high_price,
            stop_loss=self.stop_loss,
            targets=self.targets,
            order_type=self.order_type,
            referenced_message_id=self.referenced_message_id,
            parse_status=parse_status,
            field_confidence=dict(self.field_confidence),
            ambiguities=ambiguities,
            reasons=self.reasons,
            normalized_text=self.normalized_text,
            text_sha256=self.text_sha256,
            direction=self.direction,
            logical_id=self.logical_id,
            exchange=self.exchange,
            symbol_raw=self.symbol_raw,
            timeframe=self.timeframe,
            full_target=self.full_target,
            deposit_pct=self.deposit_pct,
            suggested_leverage=self.suggested_leverage,
            leverage_pct=self.leverage_pct,
            follow_up_event=self.follow_up_event,
            followed_logical_id=self.followed_logical_id,
            target_index=self.target_index,
            profit_pct=self.profit_pct,
            event_price=self.event_price,
            source_date_text=self.source_date_text,
            source_time_text=self.source_time_text,
            source_timestamp_iso=self.source_timestamp_iso,
        )


@dataclass(frozen=True)
class CandidateRecord:
    """A persisted machine candidate plus source identity."""

    candidate_id: str
    channel_id: int
    message_id: int
    edit_version: int
    source_datetime: str
    source_text_sha256: str
    parser_version: int
    parsed: ParsedCandidate
    relationship: Relationship
    parent_candidate_id: str | None
    idempotency_key: str


def decimal_text(value: Decimal | None) -> str | None:
    """Canonical, non-exponential decimal text for SQLite."""

    return format(value, "f") if value is not None else None


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 - malformed persisted data is reported upstream
        return None
