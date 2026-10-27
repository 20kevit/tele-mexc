"""Typed order-book snapshot parsing with Decimal arithmetic.

Official reference (verified 2026-10-23):
- https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/order-book
  (``GET /api/v3/depth``, weight 3; ``lastUpdateId``, ``bids``/``asks`` as
  [price, qty] string pairs; snapshot only — no incremental guarantee)

All monetary values are Decimal strings; binary floats never touch them.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


class DepthValidationError(ValueError):
    """The depth payload did not match the documented structure."""


def _level(pair: Any, index: int, side: str) -> tuple[Decimal, Decimal]:
    if (
        not isinstance(pair, (list, tuple))
        or len(pair) != 2
        or not isinstance(pair[0], str)
        or not isinstance(pair[1], str)
    ):
        raise DepthValidationError(f"depth {side}[{index}] is not a [string, string] pair")
    try:
        return Decimal(pair[0]), Decimal(pair[1])
    except InvalidOperation as exc:
        raise DepthValidationError(f"depth {side}[{index}] contains a non-Decimal value") from exc


@dataclass(frozen=True)
class OrderBookSnapshot:
    symbol: str
    last_update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]  # best-first per exchange order
    asks: tuple[tuple[Decimal, Decimal], ...]

    @property
    def best_bid(self) -> Decimal | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    def crossed(self) -> bool:
        """True when the snapshot is inconsistent (ask below bid)."""

        if self.best_bid is None or self.best_ask is None:
            return False
        return self.best_ask < self.best_bid


def parse_depth(raw: Any, *, symbol: str = "") -> OrderBookSnapshot:
    """Parse the documented /api/v3/depth response, validating every level."""

    if not isinstance(raw, dict):
        raise DepthValidationError("depth response is not a JSON object")
    bids_raw = raw.get("bids")
    asks_raw = raw.get("asks")
    if not isinstance(bids_raw, list) or not isinstance(asks_raw, list):
        raise DepthValidationError("depth response is missing bids/asks lists")
    try:
        last_update_id = int(str(raw.get("lastUpdateId")))
    except (TypeError, ValueError) as exc:
        raise DepthValidationError("depth response has no integer lastUpdateId") from exc
    bids = tuple(_level(pair, i, "bids") for i, pair in enumerate(bids_raw))
    asks = tuple(_level(pair, i, "asks") for i, pair in enumerate(asks_raw))
    snap = OrderBookSnapshot(
        symbol=symbol.upper(), last_update_id=last_update_id, bids=bids, asks=asks
    )
    if snap.crossed():
        raise DepthValidationError("depth snapshot is crossed (ask below bid)")
    return snap
