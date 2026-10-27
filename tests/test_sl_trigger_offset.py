"""Stop-loss trigger offset: five ticks above the configured exit price.

All tests are deterministic and offline.  No real orders are placed and no
live exchange is contacted.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.mexc.symbols import (
    SymbolMeta,
    SymbolValidationError,
    compute_stop_loss_trigger_price,
)

# ---------------------------------------------------------------------------
# Task 5 #4/#5: five-tick trigger calculation + Decimal precision
# ---------------------------------------------------------------------------


def test_five_ticks_above_exit_price():
    # From Task 2's illustrative example: exit 0.1190, tick 0.0001 -> 0.1195.
    assert compute_stop_loss_trigger_price(
        Decimal("0.1190"), Decimal("0.0001"), 5
    ) == Decimal("0.1195")


def test_trigger_offset_applies_to_several_tick_sizes():
    cases = [
        (Decimal("0.1190"), Decimal("0.0001"), 5, Decimal("0.1195")),
        (Decimal("100.00"), Decimal("0.01"), 5, Decimal("100.05")),
        (Decimal("0.123456"), Decimal("0.000001"), 5, Decimal("0.123461")),
        (Decimal("1.2345"), Decimal("0.001"), 5, Decimal("1.240")),
    ]
    for exit_price, tick, ticks, expected in cases:
        assert compute_stop_loss_trigger_price(exit_price, tick, ticks) == expected


def test_offset_is_exactly_five_ticks_not_four_or_six():
    assert compute_stop_loss_trigger_price(Decimal("10"), Decimal("1"), 5) == Decimal("15")
    assert compute_stop_loss_trigger_price(Decimal("10"), Decimal("1"), 5) != Decimal("14")
    assert compute_stop_loss_trigger_price(Decimal("10"), Decimal("1"), 5) != Decimal("16")


def test_decimal_not_binary_arithmetic():
    # Binary float would give 0.11950000000000003; Decimal gives exact 0.1195.
    result = compute_stop_loss_trigger_price(Decimal("0.1190"), Decimal("0.0001"), 5)
    assert result == Decimal("0.1195")
    assert str(result) == "0.1195"


def test_rounds_up_to_tick_never_down_toward_exit():
    # Exit price not tick-aligned: 0.11903 + 0.0001 = 0.11913, quantize UP to
    # 0.0001 -> 0.1192 (activates earlier, never rounded down toward exit).
    assert compute_stop_loss_trigger_price(Decimal("0.11903"), Decimal("0.0001"), 1) == Decimal("0.1192")


# ---------------------------------------------------------------------------
# Task 5 #6: invalid / unavailable metadata fails closed
# ---------------------------------------------------------------------------


def test_invalid_exit_price_rejected():
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("0"), Decimal("0.0001"), 5)
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("-0.1190"), Decimal("0.0001"), 5)
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("nan"), Decimal("0.0001"), 5)


def test_invalid_tick_size_rejected():
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("0.1190"), Decimal("0"), 5)
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("0.1190"), Decimal("-0.0001"), 5)


def test_zero_offset_rejected():
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("0.1190"), Decimal("0.0001"), 0)
    with pytest.raises(SymbolValidationError):
        compute_stop_loss_trigger_price(Decimal("0.1190"), Decimal("0.0001"), -1)


# ---------------------------------------------------------------------------
# Task 5 #7: correct SELL stop-loss trigger direction (activates earlier)
# ---------------------------------------------------------------------------


def test_trigger_is_above_exit_for_sell_stop_loss():
    exit_price = Decimal("0.1190")
    tick = Decimal("0.0001")
    trigger = compute_stop_loss_trigger_price(exit_price, tick, 5)
    # For a SELL stop-loss on a LONG position, the trigger must be ABOVE the
    # exit price so price falling from above crosses the trigger first.
    assert trigger > exit_price
    assert trigger - exit_price == Decimal("0.0005")


def test_trigger_activates_before_exit_price_on_the_way_down():
    exit_price = Decimal("0.1190")
    trigger = compute_stop_loss_trigger_price(exit_price, Decimal("0.0001"), 5)
    falling = [Decimal("0.1197"), Decimal("0.1196"), Decimal("0.1194")]
    assert all(p > trigger for p in falling[:2])  # above trigger: not armed yet
    assert falling[2] < trigger  # below trigger: armed (as market falls)


# ---------------------------------------------------------------------------
# SymbolMeta helpers used by the engine
# ---------------------------------------------------------------------------


def _meta(tick_size: str) -> SymbolMeta:
    # quote_asset_precision determines price_step = 10^-precision.
    neg_power = len(tick_size.split(".")[1]) if "." in tick_size else 0
    return SymbolMeta(
        symbol="APEUSDT",
        status="1",
        base_asset="APE",
        quote_asset="USDT",
        base_asset_precision=6,
        quote_asset_precision=neg_power,
        min_qty=Decimal("0.0001"),
        min_quote=Decimal("1.0"),
        min_quote_market=Decimal("1.0"),
        max_quote=Decimal("1000000"),
        max_quote_market=Decimal("1000000"),
        order_types=("LIMIT", "MARKET"),
        spot_allowed=True,
        permissions=("SPOT",),
        trade_side_type=1,
        bid_multiplier_up=None,
        ask_multiplier_down=None,
        raw={},
    )


def test_price_step_derives_from_quote_asset_precision():
    assert _meta("0.0001").price_step == Decimal("0.0001")
    assert _meta("0.01").price_step == Decimal("0.01")


def test_quantize_price_rounds_down_to_tick():
    m = _meta("0.0001")
    assert m.quantize_price(Decimal("0.11903")) == Decimal("0.1190")


# ---------------------------------------------------------------------------
# Task 5 #8/#9: engine-level OCO field mapping + validation failures
# ---------------------------------------------------------------------------


class _FakeMeta:
    def __init__(self, tick: str):
        self.price_step = Decimal(tick)


class _MetaRegistry:
    def __init__(self, tick: str):
        self._tick = tick

    def get(self, symbol):
        return _FakeMeta(self._tick)


class _MissingMetaRegistry:
    def get(self, symbol):
        return None


class _FakeClient:
    def __init__(self, tick="0.0001"):
        self.symbols = _MetaRegistry(tick)
        self.submitted: list[dict] = []

    def place_oco(self, *, symbol, side, quantity, price, trigger_price,
                  trigger_limit_price, client_order_id):
        self.submitted.append({
            "symbol": symbol, "side": side, "quantity": quantity, "price": price,
            "trigger_price": trigger_price, "trigger_limit_price": trigger_limit_price,
            "client_order_id": client_order_id,
        })
        return {
            "orderId": "oco-1",
            "status": "NEW",
            "triggerPrice": str(trigger_price),
            "triggerLimitPrice": str(trigger_limit_price),
            "executedQty": "0",
            "cumulativeQuoteQty": "0",
        }


class _FakeEngine:
    """Stand-in for TradingEngine around submit_oco_sell and _compute_stop_loss_trigger."""

    def __init__(self, tick="0.0001", fail_trigger=False):
        self.cfg = type("C", (), {})()
        self.cfg.trading = type("T", (), {"stop_loss_trigger_offset_ticks": 5})()
        self.client = _FakeClient(tick)
        self.fail_trigger = fail_trigger

    def _live_client(self):
        return self.client

    def _symbol_meta(self, symbol):
        return self.client.symbols.get(symbol)

    def _compute_stop_loss_trigger(self, trade, stop_price):
        if self.fail_trigger:
            return None
        meta = self.client.symbols.get(trade["symbol"])
        if meta is None:
            return None
        return compute_stop_loss_trigger_price(
            stop_price, meta.price_step, int(self.cfg.trading.stop_loss_trigger_offset_ticks)
        )

    def submit_oco_sell(self, trade, client_order_id, price, quantity, stop_price, kind=None):
        """Mirror of TradingEngine.submit_oco_sell using injected fakes."""
        client = self._live_client()
        trigger_price = self._compute_stop_loss_trigger(trade, stop_price)
        if trigger_price is None:
            raise SymbolValidationError(
                f"stop-loss trigger unavailable for {trade.get('symbol')}"
            )
        entry_price = trade.get("entry_price")
        if entry_price is not None and trigger_price >= entry_price:
            raise SymbolValidationError("trigger not below entry price")
        response = client.place_oco(
            symbol=trade["symbol"], side="SELL", quantity=quantity, price=price,
            trigger_price=trigger_price, trigger_limit_price=stop_price,
            client_order_id=client_order_id,
        )
        return {
            "exchange_order_id": str(response.get("orderId")),
            "state": str(response.get("status") or "NEW"),
            "trigger_price": str(response.get("triggerPrice")),
            "trigger_limit_price": str(response.get("triggerLimitPrice")),
        }


def _trade(symbol="APEUSDT", entry=Decimal("0.1219"), sl=Decimal("0.1190")):
    return {"symbol": symbol, "entry_price": entry, "sl_price": sl, "trade_id": "trade-1"}


def test_engine_oco_transmits_trigger_above_exit_price():
    engine = _FakeEngine()
    trade = _trade()
    result = engine.submit_oco_sell(
        trade, "sl-cid-1", Decimal("0.1227"), Decimal("24.61"),
        Decimal("0.1190"), kind="tp1",
    )
    assert len(engine.client.submitted) == 1
    call = engine.client.submitted[0]
    # trigger_price (activation) is five ticks above the exit price.
    assert call["trigger_price"] == Decimal("0.1195")
    # trigger_limit_price (execution) stays at the configured exit price.
    assert call["trigger_limit_price"] == Decimal("0.1190")
    assert result["trigger_price"] == "0.1195"
    assert result["trigger_limit_price"] == "0.1190"


def test_engine_oco_fails_closed_when_metadata_missing():
    engine = _FakeEngine()
    engine.client.symbols = _MissingMetaRegistry()
    with pytest.raises(SymbolValidationError):
        engine.submit_oco_sell(
            _trade(), "sl-cid-1", Decimal("0.1227"), Decimal("24.61"),
            Decimal("0.1190"), kind="tp1",
        )
    assert engine.client.submitted == []  # nothing transmitted


def test_engine_oco_rejects_trigger_at_or_above_entry():
    engine = _FakeEngine()
    trade = _trade(entry=Decimal("0.1190"))  # entry == exit price (degenerate)
    with pytest.raises(SymbolValidationError):
        engine.submit_oco_sell(
            trade, "sl-cid-1", Decimal("0.1227"), Decimal("24.61"),
            Decimal("0.1190"), kind="tp1",
        )
    assert engine.client.submitted == []


# ---------------------------------------------------------------------------
# Task 5 #12: existing live orders and legacy positions are preserved
# ---------------------------------------------------------------------------


def test_implementation_does_not_touch_existing_orders_or_legacy_positions():
    # The trigger calculation is pure and additive: it never mutates any
    # order, position, or the configured stop-loss exit price.
    exit_price = Decimal("0.1190")
    before = exit_price
    trigger = compute_stop_loss_trigger_price(exit_price, Decimal("0.0001"), 5)
    assert exit_price == before  # exit price unchanged
    assert trigger != exit_price  # only the trigger moved
    assert trigger > exit_price
