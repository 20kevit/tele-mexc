"""Owner sizing: fixed-USD buys and percentage-of-free-base sells."""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal

import pytest

from tg_signal_ingest.trading.risk import RiskDecision
from tg_signal_ingest.trading.sizing import SizingError, apply_sizing


class Meta:
    base_asset = "BTC"
    quote_asset = "USDT"
    step = Decimal("0.0001")

    def quantize_quantity(self, quantity):
        return (Decimal(quantity) / self.step).to_integral_value(rounding=ROUND_DOWN) * self.step

    def validate_order(self, **kwargs):
        if kwargs.get("quantity") is not None and kwargs["quantity"] <= 0:
            raise ValueError("non-positive quantity")


def _decision(order_type="limit", price="100", reference="100"):
    return RiskDecision(
        approved=True,
        reasons=("ok",),
        order_type=order_type,
        reference_price=Decimal(reference) if reference else None,
        limit_price=Decimal(price) if price else None,
        quote_amount=None,
        base_quantity=None,
    )


def test_fixed_usd_buy_quantizes_and_tracks_actual_notional():
    result = apply_sizing(
        side="buy",
        decision=_decision(),
        meta=Meta(),
        buy_quote_usd=10.0,
        sell_pct_free=None,
        free_base=None,
        max_quote=Decimal("10"),
    )
    assert result.base_quantity == Decimal("0.1000")
    assert result.quote_amount == Decimal("10.0000")


def test_market_buy_uses_quote_budget_not_a_synthetic_price():
    result = apply_sizing(
        side="buy",
        decision=_decision(order_type="market", price=None),
        meta=Meta(),
        buy_quote_usd=10.0,
        sell_pct_free=None,
        free_base=None,
        max_quote=Decimal("10"),
    )
    assert result.base_quantity is None
    assert result.quote_amount == Decimal("10.0")


def test_buy_has_no_arbitrary_cap_but_rejects_below_quantity_step():
    result = apply_sizing(
        side="buy",
        decision=_decision(),
        meta=Meta(),
        buy_quote_usd=11.0,
        sell_pct_free=None,
        free_base=None,
        max_quote=Decimal("10"),  # accepted for compatibility; not enforced
    )
    assert result.quote_amount == Decimal("11.0000")

    with pytest.raises(SizingError, match="smaller than the symbol quantity step"):
        apply_sizing(
            side="buy",
            decision=_decision(price="1000000", reference="1000000"),
            meta=Meta(),
            buy_quote_usd=1.0,
            sell_pct_free=None,
            free_base=None,
            max_quote=Decimal("10"),
        )


def test_sell_uses_percentage_of_free_base_balance_without_a_quote_cap():
    result = apply_sizing(
        side="sell",
        decision=_decision(),
        meta=Meta(),
        buy_quote_usd=None,
        sell_pct_free=25.0,
        free_base=Decimal("2"),
        max_quote=Decimal("100"),
    )
    assert result.base_quantity == Decimal("0.5000")
    assert result.quote_amount == Decimal("50.0000")

    uncapped = apply_sizing(
        side="sell",
        decision=_decision(),
        meta=Meta(),
        buy_quote_usd=None,
        sell_pct_free=25.0,
        free_base=Decimal("2"),
        max_quote=Decimal("10"),
    )
    assert uncapped.quote_amount == Decimal("50.0000")


@pytest.mark.parametrize("pct", [0, -1, 100.1])
def test_sell_rejects_invalid_percentage(pct):
    with pytest.raises(SizingError, match="between 0 and 100"):
        apply_sizing(
            side="sell",
            decision=_decision(),
            meta=Meta(),
            buy_quote_usd=None,
            sell_pct_free=pct,
            free_base=Decimal("1"),
            max_quote=Decimal("100"),
        )


def test_sell_fails_closed_without_balance_or_metadata():
    with pytest.raises(SizingError, match="free base-asset balance"):
        apply_sizing(
            side="sell", decision=_decision(), meta=Meta(), buy_quote_usd=None,
            sell_pct_free=25.0, free_base=None, max_quote=Decimal("100"),
        )
    with pytest.raises(SizingError, match="symbol metadata"):
        apply_sizing(
            side="sell", decision=_decision(), meta=None, buy_quote_usd=None,
            sell_pct_free=25.0, free_base=Decimal("1"), max_quote=Decimal("100"),
        )
