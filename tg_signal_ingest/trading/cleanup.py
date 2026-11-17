"""Inspect MEXC open orders without cancelling or modifying them.

The legacy cleanup command and ``apply`` argument are retained for compatibility;
small notional alone never authorizes cancellation. Inspection is audited.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

CLEANUP_KEY = "trading.initial_open_order_cleanup"
INFLIGHT_KEY = "trading.initial_open_order_cleanup_inflight"
DEFAULT_THRESHOLD_USD = Decimal("5")
STABLE_QUOTE_ASSETS = {"USDT", "USD"}


@dataclass
class CleanupCandidate:
    symbol: str
    order_id: str
    side: str
    order_type: str
    remaining_qty: Decimal
    price: Decimal
    value_usd: Decimal | None
    quote_asset: str
    eligible: bool
    reason: str


@dataclass
class CleanupReport:
    already_completed: bool = False
    inspected: int = 0
    candidates: list[CleanupCandidate] = field(default_factory=list)
    canceled_symbols: list[str] = field(default_factory=list)
    left_untouched: list[str] = field(default_factory=list)
    uncertain_symbols: list[str] = field(default_factory=list)
    detail: str = ""

    @property
    def canceled_orders(self) -> int:
        return sum(len([c for c in self.candidates if c.symbol == s]) for s in self.canceled_symbols)

    def render(self) -> str:
        lines = [
            "Initial MEXC open-order cleanup",
            f"  inspected open orders: {self.inspected}",
            f"  canceled symbols/orders: {len(self.canceled_symbols)} / {self.canceled_orders}",
            f"  left untouched: {len(self.left_untouched)}",
            f"  uncertain: {len(self.uncertain_symbols)}",
        ]
        for candidate in self.candidates:
            value = "UNVALUED" if candidate.value_usd is None else f"{candidate.value_usd:.8f} USD"
            lines.append(
                f"  - {candidate.symbol} {candidate.order_id} {candidate.side} {candidate.order_type} "
                f"remaining={candidate.remaining_qty} price={candidate.price} value={value} "
                f"eligible={candidate.eligible} ({candidate.reason})"
            )
        if self.uncertain_symbols:
            lines.append("  uncertain symbols: " + ", ".join(self.uncertain_symbols))
        if self.detail:
            lines.append(f"  {self.detail}")
        return "\n".join(lines)


def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
        return result if result.is_finite() else None
    except (InvalidOperation, TypeError, ValueError):
        return None


def _quote_asset(symbol: str) -> str:
    upper = symbol.upper()
    for quote in ("USDT", "USDC", "TUSD", "FDUSD", "BTC", "ETH", "MX", "USD"):
        if upper.endswith(quote) and len(upper) > len(quote):
            return quote
    return ""


def _candidate(order: dict[str, Any], threshold: Decimal) -> CleanupCandidate:
    symbol = str(order.get("symbol") or "").upper()
    order_id = str(order.get("orderId") or order.get("order_id") or "")
    side = str(order.get("side") or "")
    order_type = str(order.get("type") or order.get("orderType") or "")
    status = str(order.get("status") or order.get("state") or "NEW").upper()
    if status in ("FILLED", "CANCELED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED"):
        return CleanupCandidate(
            symbol, order_id, side, order_type, Decimal("0"), Decimal("0"), None, "", False, "not an open order"
        )
    orig = _decimal(order.get("origQty") or order.get("quantity"))
    executed = _decimal(order.get("executedQty") or order.get("dealtQty")) or Decimal("0")
    price = _decimal(order.get("price"))
    quote = _quote_asset(symbol)
    remaining = max(Decimal("0"), (orig or Decimal("0")) - executed)
    if not order_id or not symbol:
        return CleanupCandidate(
            symbol,
            order_id,
            side,
            order_type,
            remaining,
            price or Decimal("0"),
            None,
            quote,
            False,
            "missing order identity",
        )
    if orig is None or remaining <= 0:
        return CleanupCandidate(
            symbol,
            order_id,
            side,
            order_type,
            remaining,
            price or Decimal("0"),
            None,
            quote,
            False,
            "no reliable remaining quantity",
        )
    if price is None or price <= 0:
        return CleanupCandidate(
            symbol, order_id, side, order_type, remaining, Decimal("0"), None, quote, False, "no reliable limit price"
        )
    if quote not in STABLE_QUOTE_ASSETS:
        return CleanupCandidate(
            symbol,
            order_id,
            side,
            order_type,
            remaining,
            price,
            None,
            quote,
            False,
            f"quote {quote or '?'} has no reliable USD conversion",
        )
    value = remaining * price
    if value >= threshold:
        return CleanupCandidate(
            symbol, order_id, side, order_type, remaining, price, value, quote, False, "value is at or above threshold"
        )
    return CleanupCandidate(symbol, order_id, side, order_type, remaining, price, value, quote, True, "below threshold")


def _group_orders(orders: list[dict[str, Any]], threshold: Decimal) -> dict[str, list[CleanupCandidate]]:
    grouped: dict[str, list[CleanupCandidate]] = {}
    for order in orders:
        if not isinstance(order, dict):
            continue
        candidate = _candidate(order, threshold)
        grouped.setdefault(candidate.symbol or "?", []).append(candidate)
    return grouped


def _summary(
    candidates: list[CleanupCandidate],
    canceled: list[str],
    uncertain: list[str],
    left: list[str],
    threshold: Decimal,
) -> str:
    return json.dumps(
        {
            "threshold_usd": str(threshold),
            "inspected": len(candidates),
            "canceled_symbols": canceled,
            "uncertain_symbols": uncertain,
            "left_untouched_symbols": left,
            "orders": [
                {
                    "symbol": c.symbol,
                    "order_id": c.order_id,
                    "side": c.side,
                    "type": c.order_type,
                    "remaining_qty": str(c.remaining_qty),
                    "price": str(c.price),
                    "value_usd": None if c.value_usd is None else str(c.value_usd),
                    "eligible": c.eligible,
                    "reason": c.reason,
                }
                for c in candidates
            ],
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def run_initial_open_order_cleanup(
    client: Any,
    store: Any,
    *,
    apply: bool,
    threshold_usd: Decimal = DEFAULT_THRESHOLD_USD,
) -> CleanupReport:
    """Inspect (and optionally cancel) qualifying open orders exactly once."""

    if store.get_app_state(CLEANUP_KEY):
        return CleanupReport(already_completed=True, detail="initial cleanup already completed")

    if not callable(getattr(client, "open_orders", None)):
        return CleanupReport(detail="exchange client cannot list open orders; nothing changed")
    if apply and not callable(getattr(client, "cancel_all_orders", None)):
        return CleanupReport(detail="exchange client cannot cancel open orders; nothing changed")

    try:
        orders = client.open_orders()
    except Exception as exc:  # noqa: BLE001 - cleanup must not guess
        return CleanupReport(detail=f"open-order query failed ({exc.__class__.__name__}); nothing changed")
    if not isinstance(orders, list):
        return CleanupReport(detail="open-order query returned an incomplete result; nothing changed")

    grouped = _group_orders(orders, threshold_usd)
    report = CleanupReport(inspected=len(orders))
    report.candidates = [c for group in grouped.values() for c in group]

    # F-07: the bot must never cancel an order it cannot positively attribute.
    # The historical implementation cancelled every open order below 5 USD,
    # including bot-owned exit legs and manual orders.  Inspection is still
    # useful, cancellation is not performed here at all.
    report.detail = (
        "inspection only: automatic order cancellation is permanently disabled. "
        "No order was cancelled or modified; unattributed and manual orders are "
        "left untouched by design."
    )
    for symbol in sorted(grouped):
        report.left_untouched.append(symbol)
    store.audit(
        "initial-open-order-cleanup",
        "lifecycle",
        "open_order_cleanup_inspection",
        _summary(report.candidates, [], [], report.left_untouched, threshold_usd)[:2000],
        event_type="cleanup.initial_open_orders",
        outcome="inspected",
        payload={"canceled_symbols": [], "left_untouched": report.left_untouched},
    )
    if apply:
        report.detail += " (apply requested but ignored)"
    return report
