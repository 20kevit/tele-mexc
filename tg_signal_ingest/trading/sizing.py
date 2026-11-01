"""Owner-selected order sizing.

The only position-size input for BUY orders is the owner's fixed quote
notional.  Signal deposit percentages, balance percentages and leverage
suggestions are never used.  Sell sizing exists for completeness but the
production signal path is Spot BUY only.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .risk import RiskDecision


class SizingError(Exception):
    """Sizing cannot be represented safely; the message is safe to display."""


def _dec(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def apply_sizing(
    *,
    side: str,
    decision: RiskDecision,
    meta: Any | None,
    buy_quote_usd: float | None,
    sell_pct_free: float | None,
    free_base: Decimal | None,
    max_quote: Decimal | None = None,
) -> RiskDecision:
    """Return a sized decision or raise :class:`SizingError`.

    ``max_quote`` is accepted only for API compatibility and is not applied;
    the fixed amount itself is authoritative.  ``meta`` is required for
    quantity precision.
    """

    del max_quote
    price = decision.limit_price or decision.reference_price
    if side == "buy":
        if buy_quote_usd is None:
            raise SizingError("fixed order amount is not configured")
        quote = _dec(buy_quote_usd)
        if quote is None or quote <= 0:
            raise SizingError("fixed order amount must be positive")
        if meta is None:
            raise SizingError("symbol metadata is required to size a buy")
        if decision.order_type == "market":
            base = None
        else:
            if price is None or price <= 0:
                raise SizingError("a limit price is required to size a buy")
            base = meta.quantize_quantity(quote / price)
            if base <= 0:
                raise SizingError("buy amount is smaller than the symbol quantity step")
            quote = base * price
        meta.validate_order(
            side="BUY",
            order_type=decision.order_type.upper(),
            quantity=base,
            quote_amount=quote,
            price=decision.limit_price,
        )
        return RiskDecision(
            approved=True,
            reasons=decision.reasons,
            order_type=decision.order_type,
            reference_price=decision.reference_price,
            limit_price=decision.limit_price,
            quote_amount=quote,
            base_quantity=base,
        )

    if side == "sell":
        if sell_pct_free is None:
            return decision
        pct = _dec(sell_pct_free)
        if pct is None or not (Decimal(0) < pct <= Decimal(100)):
            raise SizingError("sell percentage must be between 0 and 100")
        if free_base is None:
            raise SizingError("free base-asset balance is unavailable")
        if free_base < 0:
            raise SizingError("free base-asset balance is negative")
        if meta is None:
            raise SizingError("symbol metadata is required to size a sell")
        base = meta.quantize_quantity(free_base * pct / Decimal(100))
        if base <= 0:
            raise SizingError("percentage of free balance is below the symbol quantity step")
        if price is None or price <= 0:
            raise SizingError("no valid price is available to value the sell")
        quote = base * price
        meta.validate_order(
            side="SELL",
            order_type=decision.order_type.upper(),
            quantity=base,
            quote_amount=quote,
            price=decision.limit_price,
        )
        return RiskDecision(
            approved=True,
            reasons=decision.reasons,
            order_type=decision.order_type,
            reference_price=decision.reference_price,
            limit_price=decision.limit_price,
            quote_amount=quote,
            base_quantity=base,
        )

    raise SizingError(f"unsupported side {side!r}")
