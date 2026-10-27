"""Pre-trade risk controls.

The policy is deliberately narrow: Spot BUY only, fixed owner-configured quote
notional, exchange-native symbol validation.  There are no application-level
daily/open-order caps, signal-age caps, price-deviation thresholds, mandatory
stop-loss rules, or manually maintained symbol allowlists.  Genuinely invalid
or unsupported signals still fail closed.

Every check fails closed. Decimal is used for all financial arithmetic; no
float reaches an exchange quantity or price.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import TradingConfig
from .parser import ParsedSignal


@dataclass(frozen=True)
class RiskDecision:
    approved: bool
    reasons: tuple[str, ...]
    order_type: str
    reference_price: Decimal | None
    limit_price: Decimal | None
    quote_amount: Decimal | None
    base_quantity: Decimal | None

    @property
    def reason(self) -> str:
        return "; ".join(self.reasons)


class RiskManager:
    def __init__(self, cfg: TradingConfig, store: Any):
        self.cfg = cfg
        self.store = store

    def evaluate(
        self,
        signal: ParsedSignal,
        *,
        message_datetime: str | None,
        reference_price: Decimal | None,
        base_asset: str | None = None,
        now: Any = None,
        symbol_meta: Any = None,
    ) -> RiskDecision:
        del message_datetime, base_asset, now  # no age/replay policy here
        cfg = self.cfg
        reasons: list[str] = []

        if cfg.emergency_stop:
            reasons.append("emergency stop is active")
        if cfg.mode not in ("sim", "testonly", "live"):
            reasons.append(f"invalid trading mode {cfg.mode!r}")
        if signal.kind not in ("alert", "update"):
            reasons.append(f"signal kind {signal.kind!r} is not executable")
        if signal.action is None:
            reasons.append("signal direction is missing or ambiguous")
        elif signal.action != "buy":
            reasons.append("SHORT/SELL signals are not executed; this bot is Spot BUY only")
        if not signal.symbol:
            reasons.append("signal symbol is missing")
        if signal.entry_price is not None and signal.entry_price <= 0:
            reasons.append("entry price must be positive")
        if signal.stop_loss is not None and signal.stop_loss <= 0:
            reasons.append("stop loss must be positive when present")
        if (
            signal.entry_low_price is not None
            and signal.entry_high_price is not None
            and signal.entry_low_price > signal.entry_high_price
        ):
            reasons.append("entry range is inverted")

        if reference_price is not None and reference_price <= 0:
            reference_price = None
        order_type = self._order_type(signal, reasons)
        limit_price: Decimal | None = None
        if order_type == "limit":
            if signal.entry_price is None:
                reasons.append("limit order requires an entry price")
            else:
                limit_price = signal.entry_price
        elif reference_price is None:
            reasons.append("no valid reference price is available for a market order")

        quote_amount: Decimal | None = None
        base_quantity: Decimal | None = None
        if cfg.order_quote_amount is None:
            reasons.append(
                "fixed order amount is not configured; set an owner-approved "
                "trading.order_quote_amount (never inferred from the signal)"
            )
        else:
            try:
                quote_amount = Decimal(str(cfg.order_quote_amount))
            except (InvalidOperation, ValueError):
                reasons.append("configured fixed order amount is invalid")
                quote_amount = None
            if quote_amount is not None and quote_amount <= 0:
                reasons.append("configured fixed order amount must be positive")
                quote_amount = None
            if quote_amount is not None:
                price = limit_price or reference_price
                if price is not None and price > 0:
                    base_quantity = quote_amount / price
                else:
                    reasons.append("no valid price is available to size the order")

        if symbol_meta is not None and quote_amount is not None:
            try:
                symbol_meta.validate_order(
                    side="BUY",
                    order_type=order_type.upper(),
                    quantity=base_quantity,
                    quote_amount=quote_amount,
                    price=limit_price,
                )
            except Exception as exc:  # noqa: BLE001 - validation messages are safe
                reasons.append(f"symbol validation failed: {exc}")

        return RiskDecision(
            approved=not reasons,
            reasons=tuple(reasons),
            order_type=order_type,
            reference_price=reference_price,
            limit_price=limit_price,
            quote_amount=quote_amount,
            base_quantity=base_quantity,
        )

    def _order_type(self, signal: ParsedSignal, reasons: list[str]) -> str:
        policy = self.cfg.order_type_policy
        if signal.order_type == "market":
            if policy == "limit":
                reasons.append("market order blocked by order_type_policy=limit")
                return "limit"
            return "market"
        if signal.order_type == "limit":
            return "limit"
        # No explicit order type: an entry price means a limit entry (the
        # signal's own semantics are preserved); otherwise use market only when
        # the owner policy explicitly allows it.
        if signal.entry_price is not None:
            return "limit"
        return "market" if policy == "market" else "limit"
