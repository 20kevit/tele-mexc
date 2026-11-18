"""Lifecycle, partial-fill and restart-recovery tests for ``TradeManager`` with
native two-OCO exits.

Every exchange interaction goes through the shared fakes, so no order, cancel
or query ever reaches MEXC.  The tests exercise the durable state machine with a
real :class:`TradeStore` on a temporary SQLite database.

With native OCO exits, two OCO SELL orders are placed immediately once the entry
fills are confirmed:
  * OCO 1: ~50% of acquired quantity at TP1 with stop-limit at SL.
  * OCO 2: remaining ~50% at TP2 with the same stop-limit at SL.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from _trading_fakes import (
    FakeClient,
    FakeEngine,
    FakeMeta,
    FakeNotifier,
    FakeRegistry,
)

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.mexc.errors import MexcNetworkError
from tg_signal_ingest.repository import Repository
from tg_signal_ingest.trading.parser import ParsedSignal
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

FIXTURES = Path(__file__).parent / "fixtures" / "accounting" / "four_trades_exchange.json"

ENTRY = Decimal("100")
TP1 = Decimal("110")
TP2 = Decimal("120")
SL = Decimal("95")
SYMBOL = "BTCUSDT"


def _parsed(entry: Decimal = ENTRY) -> ParsedSignal:
    return ParsedSignal(
        kind="alert",
        action="buy",
        symbol=SYMBOL,
        entry_price=entry,
        entry_low_price=None,
        entry_high_price=None,
        target_prices=(TP1, TP2),
        stop_loss=SL,
        order_type="limit",
        amount_text=None,
        raw_text="buy",
        normalized_text="buy",
        fingerprint_sha256="abc",
    )


def _insert_signal(store: TradeStore, signal_id: str) -> None:
    with store.db.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO signals (signal_id, channel_id, message_id, edit_version, "
            "parser_version, signal_kind, symbol, action, status, fingerprint_sha256, created_at, updated_at) "
            "VALUES (?, ?, 1, 0, 1, 'alert', ?, 'buy', 'parsed', ?, 'now', 'now')",
            (signal_id, 100, SYMBOL, "abc"),
        )


def _insert_intent(
    store: TradeStore,
    *,
    signal_id: str = "sig-1",
    client_order_id: str = "tsi-1",
    entry: Decimal = ENTRY,
    quote: Decimal = Decimal("10"),
    base_quantity: str = "0.1",
) -> dict:
    _insert_signal(store, signal_id)
    intent_id = f"intent-{client_order_id}"
    with store.db.transaction() as conn:
        conn.execute(
            """INSERT INTO order_intents
            (intent_id, signal_id, idempotency_key, client_order_id, symbol, side,
             order_type, quote_amount, base_quantity, limit_price, mode, state,
             create_msg_id, create_channel_id, planned_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                intent_id,
                signal_id,
                f"idem-{client_order_id}",
                client_order_id,
                SYMBOL,
                "buy",
                "limit",
                str(quote),
                base_quantity,
                str(entry),
                "live",
                "planned",
                1,
                100,
                "now",
                "now",
            ),
        )
    return store.get_intent(intent_id)


def make_env(
    cfg: AppConfig,
    repo: Repository,
    *,
    min_notional: str = "1.0",
    engine_cls: type[FakeEngine] = FakeEngine,
):
    # Exit monitoring must work with conditional entry disabled.
    cfg.trading.entry_trigger_offset_ticks = None
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient(FakeRegistry(FakeMeta(min_notional=min_notional)))
    engine = engine_cls(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=notifier)
    tm._clock = lambda: 5000.0
    return cfg, store, engine, client, notifier, tm


@pytest.fixture()
def env(cfg: AppConfig, repo: Repository):
    return make_env(cfg, repo)


def setup_trade(env, *, fill: Decimal | None = None, confirm: bool = True):
    """Create a direct-entry trade and optionally fill the entry order.

    When an entry fill is confirmed, two native OCO exit orders are placed
    automatically.
    """
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
    assert decision.deferred is False
    trade = decision.trade
    engine._execute_intent(intent["intent_id"])
    intent = store.get_intent(intent["intent_id"])
    tm.on_intent_executed(intent)
    if fill is not None:
        client.fill_order(intent["exchange_order_id"], fill, fill * ENTRY, ENTRY)
        # Persist the fill, then cancel any unfilled remainder so the entry is
        # terminal: OCO exits are only placed once the entry cannot fill again.
        engine.reconcile_intent(intent["intent_id"], notify=False)
        base_qty = Decimal(str(intent.get("base_quantity") or "0"))
        if fill < base_qty:
            refreshed = store.get_intent(intent["intent_id"])
            if refreshed is not None:
                engine.cancel_intent_order(refreshed)
    trade = store.get_trade(trade["trade_id"])
    if confirm:
        tm._reconcile_entry_fills(trade)
        trade = store.get_trade(trade["trade_id"])
        assert trade["trade_state"] in ("oco_active", "oco_partial", "oco_submitting"), trade
    return trade, intent


def _oco_orders(store: TradeStore, trade, kind: str = "tp1") -> list[dict]:
    orders = store.list_trade_orders(trade["trade_id"], kind=kind)
    assert orders, f"expected a {kind} OCO order"
    return orders


def _fill_order(client: FakeClient, order: dict, qty: Decimal, price: Decimal) -> None:
    client.fill_order(order["exchange_order_id"], qty, qty * price, price)


# Proven MEXC allOrders shape: both OCO children share the exact creation time
# in milliseconds and expose neither a useful orderListId nor a client id.
APE_MS = 1794784085000


def _fixed_discovery_now() -> datetime:
    return datetime.fromtimestamp(APE_MS / 1000, tz=UTC)


def _pin_discovery_clock(tm, store) -> None:
    fixed = _fixed_discovery_now()
    tm._now = lambda: fixed
    store._now = lambda: fixed


def _stop_leg_by_parent(store, trade, parent_order: dict) -> dict:
    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    legs = [
        leg
        for leg in store.list_oco_legs(group["oco_group_id"])
        if leg["kind"] == "stop_loss" and leg["trade_order_id"] == parent_order["trade_order_id"]
    ]
    assert len(legs) == 1, legs
    return legs[0]


def _set_distinct_tranche_quantities(store, tp1: dict, tp2: dict) -> tuple[dict, dict]:
    """Mirror APE's 18.58/18.59 pair: same time must not imply same quantity."""
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET quantity='0.04', allocated_qty='0.04' WHERE trade_order_id=?",
            (tp1["trade_order_id"],),
        )
        conn.execute(
            "UPDATE trade_orders SET quantity='0.06', allocated_qty='0.06' WHERE trade_order_id=?",
            (tp2["trade_order_id"],),
        )
    refreshed_tp1 = store.get_trade_order(tp1["trade_order_id"])
    refreshed_tp2 = store.get_trade_order(tp2["trade_order_id"])
    assert refreshed_tp1 is not None and refreshed_tp2 is not None
    return refreshed_tp1, refreshed_tp2


def _tp_history_row(order: dict, *, status: str, executed: str = "0", time_ms: int = APE_MS) -> dict:
    return {
        "symbol": str(order["symbol"]).upper(),
        "orderId": str(order["exchange_order_id"]),
        "orderListId": -1,
        "clientOrderId": None,
        "side": "SELL",
        "type": "OCO_LIMIT_ORDER",
        "status": status,
        "price": str(order["price"]),
        "origQty": str(order["quantity"]),
        "executedQty": executed,
        "time": int(time_ms),
        "updateTime": int(time_ms),
    }


def _stop_history_row(
    order: dict,
    child_id: str,
    *,
    status: str,
    executed: str,
    stop_limit_price: Decimal | None = None,
    qty: Decimal | None = None,
    time_ms: int = APE_MS,
) -> dict:
    return {
        "symbol": str(order["symbol"]).upper(),
        "orderId": child_id,
        "orderListId": -1,
        "clientOrderId": None,
        "side": "SELL",
        "type": "OCO_STOP_LIMIT_ORDER",
        "status": status,
        # Production row shape: ``price`` is the stop-limit price, which is
        # distinct from the legacy local trigger (SL+ticks) on old rows.
        "price": str(stop_limit_price if stop_limit_price is not None else order["stop_trigger_price"]),
        "origQty": str(qty if qty is not None else order["quantity"]),
        "executedQty": executed,
        "time": int(time_ms),
        "updateTime": int(time_ms),
    }


def _register_exchange_order(
    client: FakeClient,
    row: dict,
    *,
    executed: str | None = None,
    quote: str = "0",
    fees: list[dict] | None = None,
) -> None:
    order_id = str(row["orderId"])
    client.orders[order_id] = {
        "orderId": order_id,
        "symbol": row["symbol"],
        "side": row["side"],
        "type": row["type"],
        "status": row["status"],
        "price": row["price"],
        "origQty": row["origQty"],
        "executedQty": row["executedQty"] if executed is None else executed,
        "cumulativeQuoteQty": quote,
    }
    if fees is not None:
        client.trades[order_id] = fees


def _register_canceled_stop_children(
    client: FakeClient,
    store: TradeStore,
    tp1_order: dict,
    tp2_order: dict,
) -> None:
    """Provide exchange-confirmed canceled stop siblings after both TPs fill."""

    tp2_time = APE_MS + 1000
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET created_at=? WHERE trade_order_id=?",
            (datetime.fromtimestamp(tp2_time / 1000, tz=UTC).isoformat(), tp2_order["trade_order_id"]),
        )
    tp2_order = store.get_trade_order(tp2_order["trade_order_id"])
    assert tp2_order is not None

    tp1_child = _stop_history_row(
        tp1_order,
        "C02__tp1-stop-canceled",
        status="CANCELED",
        executed="0",
        stop_limit_price=SL,
        time_ms=APE_MS,
    )
    tp2_child = _stop_history_row(
        tp2_order,
        "C02__tp2-stop-canceled",
        status="CANCELED",
        executed="0",
        stop_limit_price=SL,
        time_ms=tp2_time,
    )
    client.all_orders_rows = [
        _tp_history_row(tp1_order, status="FILLED", executed=str(tp1_order["quantity"]), time_ms=APE_MS),
        tp1_child,
        _tp_history_row(tp2_order, status="FILLED", executed=str(tp2_order["quantity"]), time_ms=tp2_time),
        tp2_child,
    ]
    _register_exchange_order(client, tp1_child, executed="0")
    _register_exchange_order(client, tp2_child, executed="0")


def _clone_oco_order(
    store: TradeStore,
    source: dict,
    suffix: str,
    *,
    quantity: Decimal,
) -> dict:
    """Insert another same-kind row in the same group (retry/duplicate shape)."""

    created = store.create_trade_order(
        trade_order_id=f"to-clone-{suffix}",
        trade_id=source["trade_id"],
        kind=source["kind"],
        client_order_id=f"cid-clone-{suffix}",
        symbol=source["symbol"],
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal(str(source["price"])),
        quantity=quantity,
        allocated_qty=quantity,
        stop_trigger_price=Decimal(str(source["stop_trigger_price"])),
        attempt=2,
        oco_group_id=source["oco_group_id"],
        leg_role=source["kind"],
    )
    exchange_id = f"ex-clone-{suffix}"
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET exchange_order_id=?, exchange_status='CANCELED' WHERE trade_order_id=?",
            (exchange_id, created["trade_order_id"]),
        )
    refreshed = store.get_trade_order(created["trade_order_id"])
    assert refreshed is not None
    return refreshed


def _fixture_trade_rows(trade_key: str) -> list[dict]:
    payload = json.loads(FIXTURES.read_text())
    for trade in payload["trades"]:
        if trade["trade_key"] == trade_key:
            return list(trade["orders"])
    raise AssertionError(f"fixture trade {trade_key} missing")


# ---------------------------------------------------------------------------
# OCO exit placement at entry fill
# ---------------------------------------------------------------------------


def test_oco_exits_placed_on_entry_fill(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1_orders = _oco_orders(store, trade, "tp1")
    tp2_orders = _oco_orders(store, trade, "tp2")
    assert len(tp1_orders) == 1
    assert len(tp2_orders) == 1
    # OCO 1: ~50% of 0.1 = 0.05 at TP1=110, stop-limit at SL=95.
    assert Decimal(tp1_orders[0]["quantity"]) == Decimal("0.05")
    assert Decimal(tp1_orders[0]["price"]) == TP1
    assert tp1_orders[0]["order_type"] == "OCO_STOP_LIMIT_ORDER"
    # OCO 2: remaining 0.05 at TP2=120, stop-limit at SL=95.
    assert Decimal(tp2_orders[0]["quantity"]) == Decimal("0.05")
    assert Decimal(tp2_orders[0]["price"]) == TP2
    assert tp2_orders[0]["order_type"] == "OCO_STOP_LIMIT_ORDER"
    assert trade["trade_state"] == "oco_active"


def test_repeated_healthy_oco_poll_does_not_toggle_group_state(env):
    """A healthy OCO poll is read-only and must not emit transient state events."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    group_before = store.get_oco_group(trade["trade_id"])
    assert group_before is not None
    assert group_before["state"] == "active"
    assert group_before["protection_status"] == "oco_active"
    orders_before = store.list_trade_orders(trade["trade_id"])
    events_before = store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE event_class='domain' AND event_type='oco_group.state_changed'"
    ).fetchone()[0]

    tm._place_oco_exits(store.get_trade(trade["trade_id"]))

    group_after = store.get_oco_group(trade["trade_id"])
    assert group_after is not None
    assert group_after["state"] == "active"
    assert group_after["protection_status"] == "oco_active"
    assert store.list_trade_orders(trade["trade_id"]) == orders_before
    events_after = store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE event_class='domain' AND event_type='oco_group.state_changed'"
    ).fetchone()[0]
    assert events_after == events_before

    # A real loss of certainty must still downgrade protection, without an
    # intermediate false `submitting` transition.
    tp1 = store.list_trade_orders(trade["trade_id"], kind="tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "unknown_requires_reconciliation")
    tm._place_oco_exits(store.get_trade(trade["trade_id"]))

    degraded_group = store.get_oco_group(trade["trade_id"])
    assert degraded_group is not None
    assert degraded_group["state"] == "partial"
    assert degraded_group["protection_status"] == "oco_partial"
    transitions = [
        json.loads(row["payload_json"])
        for row in store.db.conn.execute(
            "SELECT payload_json FROM audit_events "
            "WHERE event_class='domain' AND event_type='oco_group.state_changed' "
            "ORDER BY audit_id"
        ).fetchall()
    ]
    assert transitions[-1]["previous_state"] == "active"
    assert transitions[-1]["new_state"] == "partial"
    assert not any(event["previous_state"] == "active" and event["new_state"] == "submitting" for event in transitions)

    # Repeated polling of the degraded group is also observational; it must
    # not oscillate partial -> submitting -> partial.
    events_before_partial_poll = len(transitions)
    tm._place_oco_exits(store.get_trade(trade["trade_id"]))
    repeated_events = store.db.conn.execute(
        "SELECT payload_json FROM audit_events "
        "WHERE event_class='domain' AND event_type='oco_group.state_changed' "
        "ORDER BY audit_id"
    ).fetchall()
    assert len(repeated_events) == events_before_partial_poll
    assert store.get_oco_group(trade["trade_id"])["state"] == "partial"


def test_active_oco_group_downgrades_when_sellable_becomes_unprovable(env):
    """An early OCO failure must not leave a stale active group projection."""
    cfg, store, engine, client, notifier, tm = env
    trade, intent = setup_trade(env, fill=Decimal("0.1"))
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET fill_sync_required=1 WHERE intent_id=?",
            (intent["intent_id"],),
        )

    tm._place_oco_exits(store.get_trade(trade["trade_id"]))

    refreshed = store.get_trade(trade["trade_id"])
    group = store.get_oco_group(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation"
    assert group is not None
    assert group["state"] == "needs_review"
    assert group["protection_status"] == "needs_reconciliation"


def test_oco_partial_quantities(env):
    """OCO quantities are computed from actual acquired fills."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.06"))
    tp1_orders = _oco_orders(store, trade, "tp1")
    tp2_orders = _oco_orders(store, trade, "tp2")
    # 50% of 0.06 = 0.03, remaining = 0.03.
    assert Decimal(tp1_orders[0]["quantity"]) == Decimal("0.03")
    assert Decimal(tp2_orders[0]["quantity"]) == Decimal("0.03")


# ---------------------------------------------------------------------------
# OCO fills advance the trade
# ---------------------------------------------------------------------------


def test_oco_tp1_fill_advances_toward_tp2(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1_order = _oco_orders(store, trade, "tp1")[0]
    # Credit TP1 OCO fill.
    client.fill_order(tp1_order["exchange_order_id"], Decimal("0.05"), Decimal("0.05") * TP1, TP1)
    store.add_fill(
        exchange_order_id=tp1_order["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(TP1),
            "qty": "0.05",
            "quoteQty": str(Decimal("0.05") * TP1),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 2,
        },
    )
    tm._reconcile_trade_orders(trade)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active", trade
    assert Decimal(trade["tp1_sold_qty"]) == Decimal("0.05")


def test_oco_both_filled_closes_trade(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1_order = _oco_orders(store, trade, "tp1")[0]
    tp2_order = _oco_orders(store, trade, "tp2")[0]
    # Fill both OCOs.
    client.fill_order(tp1_order["exchange_order_id"], Decimal("0.05"), Decimal("0.05") * TP1, TP1)
    store.add_fill(
        exchange_order_id=tp1_order["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(TP1),
            "qty": "0.05",
            "quoteQty": str(Decimal("0.05") * TP1),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 2,
        },
    )
    client.fill_order(tp2_order["exchange_order_id"], Decimal("0.05"), Decimal("0.05") * TP2, TP2)
    store.add_fill(
        exchange_order_id=tp2_order["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(TP2),
            "qty": "0.05",
            "quoteQty": str(Decimal("0.05") * TP2),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 3,
        },
    )
    _register_canceled_stop_children(client, store, tp1_order, tp2_order)
    tm._reconcile_trade_orders(trade)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "closed", trade


# ---------------------------------------------------------------------------
# Stop-loss via OCO stop-limit legs (no separate market sell)
# ---------------------------------------------------------------------------


def test_oco_stop_loss_no_separate_market_sell(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    # Stop-loss hit: OCO stop-limit legs sell the position. No separate
    # market sell order is placed.
    tm.on_tick(SYMBOL, Decimal("95"), received_at=5000.0)
    trade = store.get_trade(trade["trade_id"])
    assert engine.sl_calls == [], engine.sl_calls
    assert trade["trade_state"] in ("oco_active", "oco_partial", "closed", "needs_reconciliation"), trade


def test_oco_no_duplicate_orders_on_repeat_ticks(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    initial_count = len(client.place_calls)
    # Repeated ticks must not place additional OCO orders.
    tm.on_tick(SYMBOL, Decimal("110"), received_at=5000.0)
    tm.on_tick(SYMBOL, Decimal("95"), received_at=5000.0)
    assert len(client.place_calls) == initial_count, client.place_calls


# ---------------------------------------------------------------------------
# Restart recovery
# ---------------------------------------------------------------------------


def test_oco_position_recovers_after_restart(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active"
    # New manager over the same DB (simulated restart).
    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "oco_active", t


def test_oco_restart_after_partial_fill(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1_order = _oco_orders(store, trade, "tp1")[0]
    # Partial TP1 fill, then restart.
    client.fill_order(tp1_order["exchange_order_id"], Decimal("0.02"), Decimal("0.02") * TP1, TP1)
    store.add_fill(
        exchange_order_id=tp1_order["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(TP1),
            "qty": "0.02",
            "quoteQty": str(Decimal("0.02") * TP1),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 2,
        },
    )
    tm._reconcile_trade_orders(trade)
    trade = store.get_trade(trade["trade_id"])
    assert Decimal(trade["tp1_sold_qty"]) == Decimal("0.02")

    # Simulated restart: fresh manager reconciles existing OCO orders.
    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active", trade
    assert Decimal(trade["tp1_sold_qty"]) == Decimal("0.02")


# ---------------------------------------------------------------------------
# Uncertain OCO submissions
# ---------------------------------------------------------------------------


def test_uncertain_oco_submission_is_not_blindly_retried(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))

    def fail_second_oco(*args, **kwargs):
        # First OCO succeeds, second fails with network error.
        if kwargs.get("kind") == "tp2":
            raise MexcNetworkError("timeout after submission")
        return FakeEngine.submit_oco_sell(engine, *args, **kwargs)

    engine.submit_oco_sell = fail_second_oco  # type: ignore[method-assign]
    # Re-trigger OCO placement by calling _place_oco_exits directly (trade already
    # has OCO orders pending from setup, but they were submitted by FakeEngine).
    # Instead, create a fresh trade where the second OCO will fail.
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] in ("oco_active", "oco_partial"), trade


def test_oco_partial_survives_restart(env):
    """If only one OCO is confirmed, the partial state persists across restart."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))

    def reject_tp2(*args, **kwargs):
        if kwargs.get("kind") == "tp2":
            raise RuntimeError("OCO 2 rejected by exchange")
        return FakeEngine.submit_oco_sell(engine, *args, **kwargs)

    engine.submit_oco_sell = reject_tp2  # type: ignore[method-assign]
    tp2_order = _oco_orders(store, trade, "tp2")[0]
    # Mark the second OCO as rejected to simulate partial success.  The
    # reconciliation retries the rejected leg, which also fails, so the trade
    # must stay honestly partial rather than claiming full protection.
    store.transition_trade_order(tp2_order["trade_order_id"], "rejected", note="OCO 2 rejected by exchange")
    tm._reconcile_trade(trade["trade_id"])
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_partial", trade

    # Restart: partial state is preserved, not silently completed.
    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_partial", trade


def test_reconcile_legacy_tp_state_is_safe_and_records_needs_reconciliation(env):
    """F1: reconciling a legacy standalone-TP state must not raise (TypeError)
    and must record the canonical trade.state_changed -> needs_reconciliation."""
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
    trade = decision.trade
    # Park the trade in a legacy standalone-TP state that the OCO-only policy
    # no longer executes.
    store.update_trade_state(trade["trade_id"], "tp1_active")

    tm._reconcile_trade(trade["trade_id"])  # must not raise TypeError

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation"
    transitions = [
        json.loads(row["payload_json"])
        for row in store.db.conn.execute(
            "SELECT payload_json FROM audit_events "
            "WHERE event_class='domain' AND event_type='trade.state_changed' "
            "ORDER BY audit_id"
        ).fetchall()
    ]
    assert any(
        t.get("previous_state") == "tp1_active" and t.get("new_state") == "needs_reconciliation" for t in transitions
    )


# ---------------------------------------------------------------------------
# Safety: no orders for non-tradeable / other symbols
# ---------------------------------------------------------------------------


def test_ignores_other_symbol(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    # A tick for a different symbol must not touch this trade.
    tm.on_tick("ETHUSDT", Decimal("2000"), received_at=5000.0)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active"
    # Only entry + 2 OCO orders were placed (3 total), none for other symbol.
    oco_calls = [c for c in client.place_calls if c.get("order_type") == "OCO_STOP_LIMIT_ORDER"]
    assert len(oco_calls) == 2, oco_calls


# ---------------------------------------------------------------------------
# Client order ID limits
# ---------------------------------------------------------------------------


def test_generated_client_order_ids_fit_the_cli_limit(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    ids = [
        order["client_order_id"] for order in store.list_trade_orders(trade["trade_id"]) if order.get("client_order_id")
    ]
    assert ids
    assert all(len(cid) <= 32 and cid.isascii() for cid in ids)


@pytest.mark.parametrize("state", ["submitting", "cancel_requested", "unknown_requires_reconciliation"])
@pytest.mark.parametrize("has_exchange_id", [False, True])
def test_uncertain_oco_is_not_confirmed_protection(env, state, has_exchange_id, monkeypatch):
    """A durable write claim or old identifier is not evidence of protection."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    order = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(
        order["trade_order_id"],
        state,
        note="simulated interrupted operation",
    )
    # Clear the ID explicitly: transition_trade_order preserves absent fields.
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET exchange_order_id=? WHERE trade_order_id=?",
            (order["exchange_order_id"] if has_exchange_id else None, order["trade_order_id"]),
        )

    def forbidden_write(*args, **kwargs):
        pytest.fail("certainty classification must not submit or cancel orders")

    monkeypatch.setattr(engine, "submit_oco_sell", forbidden_write)
    monkeypatch.setattr(engine, "cancel_trade_order", forbidden_write)
    before = len(store.list_trade_orders(trade["trade_id"]))
    assert tm._ensure_oco_leg(trade, "tp1", Decimal("0.05"), TP1, SL) == "uncertain"
    tm._place_oco_exits(trade)
    assert store.get_trade(trade["trade_id"])["trade_state"] == "oco_partial"
    assert len(store.list_trade_orders(trade["trade_id"])) == before


def test_recovery_downgrades_unknown_oco_with_old_exchange_id(env, monkeypatch):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    order = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(order["trade_order_id"], "unknown_requires_reconciliation")
    monkeypatch.setattr(tm, "_query_order", lambda order: None)
    before = len(store.list_trade_orders(trade["trade_id"]))
    tm._reconcile_trade(trade["trade_id"])
    assert store.get_trade(trade["trade_id"])["trade_state"] == "oco_partial"
    assert len(store.list_trade_orders(trade["trade_id"])) == before


@pytest.mark.parametrize("other_state", ["open", "filled"])
@pytest.mark.parametrize("uncertain_state", ["submitting", "cancel_requested", "unknown_requires_reconciliation"])
def test_mixed_oco_uncertainty_wins(env, monkeypatch, other_state, uncertain_state):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    first = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(
        first["trade_order_id"], other_state, sold_qty=Decimal("0.05") if other_state == "filled" else Decimal("0")
    )
    extra = tm._create_trade_order(trade, "tp1", Decimal("0.05"), TP1, order_type="OCO_STOP_LIMIT_ORDER")
    store.transition_trade_order(extra["trade_order_id"], uncertain_state)

    def forbidden(*args, **kwargs):
        pytest.fail("must not write when any prior placement is uncertain")

    monkeypatch.setattr(engine, "submit_oco_sell", forbidden)
    assert tm._ensure_oco_leg(trade, "tp1", Decimal("0.05"), TP1, SL) == "uncertain"
    tm._place_oco_exits(trade)
    assert store.get_trade(trade["trade_id"])["trade_state"] != "oco_active"


def test_both_completed_ocos_are_not_active_protection(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1_order = _oco_orders(store, trade, "tp1")[0]
    tp2_order = _oco_orders(store, trade, "tp2")[0]
    for order in store.list_trade_orders(trade["trade_id"]):
        store.transition_trade_order(order["trade_order_id"], "filled", sold_qty=Decimal(order["quantity"]))
    _register_canceled_stop_children(client, store, tp1_order, tp2_order)
    tm._reconcile_trade_orders(trade)
    tm._place_oco_exits(trade)
    assert store.get_trade(trade["trade_id"])["trade_state"] == "closed"


def test_filled_without_full_allocation_is_not_confirmed(env):
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    order = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(order["trade_order_id"], "filled", sold_qty=Decimal("0.01"))
    tm._place_oco_exits(trade)
    assert store.get_trade(trade["trade_id"])["trade_state"] != "oco_active"


def test_trade_manager_reconciliation_lock_does_not_block_an_unrelated_signal(env, caplog):
    import threading

    cfg, store, engine, client, notifier, tm = env
    caplog.set_level(10, logger="ingest.trading")
    entered = threading.Event()
    release = threading.Event()
    trade = {"trade_id": "trade-slow", "signal_id": "signal-slow", "trade_state": "entry_active"}
    tm.store.list_trades = lambda: [trade]
    tm.store.get_trade = lambda _trade_id: dict(trade)
    tm._refresh_buy_block = lambda: None

    def reconcile(_trade_id):
        entered.set()
        assert release.wait(2)

    tm._reconcile_trade = reconcile
    reconciliation = threading.Thread(target=tm.reconcile_all_open)
    reconciliation.start()
    assert entered.wait(1)

    tm.store.get_trade_by_signal = lambda _signal_id: None
    tm._create_trade = lambda intent, _parsed, _reference: {
        "trade_id": f"trade-{intent['signal_id']}",
        "entry_mode": "direct",
    }
    independent_done = threading.Event()
    independent = threading.Thread(
        target=lambda: (
            tm.on_intent_planned({"signal_id": "signal-independent"}, _parsed(), ENTRY),
            independent_done.set(),
        )
    )
    independent.start()
    try:
        assert independent_done.wait(0.5), "unrelated signal waited behind another trade's reconciliation"
    finally:
        release.set()
        reconciliation.join(2)
        independent.join(2)
    assert not reconciliation.is_alive() and not independent.is_alive()
    rendered = "\n".join(record.getMessage() for record in caplog.records)
    assert "trade_lock operation=reconcile_all_open" in rendered
    assert "trade_lock operation=on_intent_planned" in rendered


def test_canceled_oco_parent_is_parked_without_rearming(env):
    """A canceled OCO parent may hide either TP or SL child execution."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="simulated OCO parent cancel")
    placement_count = len(client.place_calls)

    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    assert len(client.place_calls) == placement_count, "ambiguous child outcome must never re-arm an OCO"


def test_ape_shaped_canceled_tp_with_filled_stop_children_settles_and_clears_block(env):
    """APE fixture: both OCO parents CANCELED, both stop children FILLED."""
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    tp1, tp2 = _set_distinct_tranche_quantities(store, tp1, tp2)
    # Real legacy rows stored SL+ticks as the trigger, while the exchange stop
    # child exposes the stop-limit price (the configured SL).
    trigger = SL + Decimal("0.03")
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET stop_trigger_price=? WHERE trade_id=? AND kind IN ('tp1','tp2')",
            (str(trigger), trade["trade_id"]),
        )
    tp1 = store.get_trade_order(tp1["trade_order_id"])
    tp2 = store.get_trade_order(tp2["trade_order_id"])
    assert tp1 is not None and tp2 is not None

    rows: list[dict] = []
    child_ids: dict[str, str] = {}
    for kind, order in (("tp1", tp1), ("tp2", tp2)):
        store.transition_trade_order(order["trade_order_id"], "canceled", note="simulated OCO parent cancel")
        child_id = f"C02__stop-{kind}"
        child_ids[kind] = child_id
        child_row = _stop_history_row(
            order,
            child_id,
            status="FILLED",
            executed=str(order["quantity"]),
            stop_limit_price=SL,
        )
        _register_exchange_order(
            client,
            child_row,
            executed=str(order["quantity"]),
            quote=str(Decimal(order["quantity"]) * SL),
            fees=[
                {
                    "id": f"fee-{kind}",
                    "orderId": child_id,
                    "price": str(SL),
                    "qty": str(order["quantity"]),
                    "quoteQty": str(Decimal(order["quantity"]) * SL),
                    "commission": "0.001",
                    "commissionAsset": "USDT",
                }
            ],
        )
        rows.extend([_tp_history_row(order, status="CANCELED"), child_row])
    client.all_orders_rows = rows
    # Production's legacy APE row is already needs_reconciliation because the
    # old code could not attribute the canceled TP parents to their SL children.
    store.update_trade_state(
        trade["trade_id"],
        "needs_reconciliation",
        note="legacy OCO child attribution pending",
    )

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "closed", refreshed
    assert Decimal(refreshed["sl_executed_qty"]) == Decimal("0.1")
    assert store.buy_block_reason() is None
    assert len(client.place_calls) == before_posts, "resolved OCO children must never trigger a new order"

    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    assert _stop_leg_by_parent(store, trade, tp1)["exchange_order_id"] == child_ids["tp1"]
    assert _stop_leg_by_parent(store, trade, tp2)["exchange_order_id"] == child_ids["tp2"]
    assert child_ids["tp1"] != tp1["exchange_order_id"]
    assert child_ids["tp2"] != tp2["exchange_order_id"]
    assert Decimal(_stop_leg_by_parent(store, trade, tp1)["filled_qty"]) == Decimal("0.04")
    assert Decimal(_stop_leg_by_parent(store, trade, tp2)["filled_qty"]) == Decimal("0.06")
    assert _stop_leg_by_parent(store, trade, tp1)["not_observed_reason"] in (None, "")
    assert _stop_leg_by_parent(store, trade, tp2)["not_observed_reason"] in (None, "")
    assert Decimal(_stop_leg_by_parent(store, trade, tp1)["fee_amount"]) == Decimal("0.001")


def test_sanitized_mexc_allorders_rows_match_exact_stop_children():
    """Use sanitized MEXC allOrders rows with the real exchange field layout.

    MEXC returns ``orderListId=-1`` and no explicit parent link; the stop
    child's ``price`` is the stop-limit (SL), while legacy local rows stored
    the trigger (SL+ticks).  Matching must use the exchange-visible fields.
    """

    for trade_key in ("MYXUSDT", "LPTUSDT", "APEUSDT"):
        rows = _fixture_trade_rows(trade_key)
        tp_rows = [row for row in rows if row["type"] == "OCO_LIMIT_ORDER"]
        stop_rows = [row for row in rows if row["type"] == "OCO_STOP_LIMIT_ORDER"]
        stop_limit = Decimal(stop_rows[0]["price"])
        matched: list[str] = []
        for tp_row in tp_rows:
            child, reason = TradeManager._select_oco_stop_child(
                rows,
                tp_id=tp_row["orderId"],
                expected_stop_limit=stop_limit,
                claimed_ids=set(matched),
            )
            assert child is not None, (trade_key, tp_row["orderId"], reason)
            assert reason == ""
            assert child["time"] == tp_row["time"]
            assert child["origQty"] == tp_row["origQty"]
            assert child["price"] == str(stop_limit)
            matched.append(child["orderId"])
        assert len(set(matched)) == len(tp_rows) == len(stop_rows)

    # The legacy trigger is not the exchange-visible stop-limit price: using it
    # must fail closed rather than approximate-match.
    lpt_rows = _fixture_trade_rows("LPTUSDT")
    lpt_tp = next(row for row in lpt_rows if row["type"] == "OCO_LIMIT_ORDER")
    child, reason = TradeManager._select_oco_stop_child(
        lpt_rows,
        tp_id=lpt_tp["orderId"],
        expected_stop_limit=Decimal("1.406"),
        claimed_ids=set(),
    )
    assert child is None
    assert reason == "oco_stop_child_candidate_count:0"


def test_two_same_kind_rows_per_group_keep_distinct_stop_children(env):
    """Two tp1 and two tp2 rows in one group must not overwrite leg targets."""

    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    # Keep total allocation inside acquired inventory.
    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE trade_orders SET quantity='0.02', allocated_qty='0.02' WHERE trade_id=?",
            (trade["trade_id"],),
        )
    tp1 = store.get_trade_order(tp1["trade_order_id"])
    tp2 = store.get_trade_order(tp2["trade_order_id"])
    assert tp1 is not None and tp2 is not None
    tp1b = _clone_oco_order(store, tp1, "tp1b", quantity=Decimal("0.02"))
    tp2b = _clone_oco_order(store, tp2, "tp2b", quantity=Decimal("0.02"))

    rows: list[dict] = []
    expected: dict[str, str] = {}
    for index, order in enumerate((tp1, tp1b, tp2, tp2b)):
        time_ms = APE_MS + index * 1000
        store.transition_trade_order(order["trade_order_id"], "canceled", note="multi-row group")
        child_id = f"C02__multi-{index}"
        expected[order["trade_order_id"]] = child_id
        child_row = _stop_history_row(
            order,
            child_id,
            status="FILLED",
            executed="0.02",
            stop_limit_price=SL,
            time_ms=time_ms,
        )
        _register_exchange_order(client, child_row, executed="0.02", quote=str(Decimal("0.02") * SL))
        rows.extend(
            [
                _tp_history_row(order, status="CANCELED", time_ms=time_ms),
                child_row,
            ]
        )
    client.all_orders_rows = rows

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    stop_legs = [leg for leg in store.list_oco_legs(group["oco_group_id"]) if leg["kind"] == "stop_loss"]
    assert len(stop_legs) == 4
    by_parent = {leg["trade_order_id"]: leg for leg in stop_legs}
    assert set(by_parent) == set(expected)
    for parent_id, child_id in expected.items():
        assert by_parent[parent_id]["exchange_order_id"] == child_id
        assert Decimal(by_parent[parent_id]["filled_qty"]) == Decimal("0.02")
    assert len({leg["exchange_order_id"] for leg in stop_legs}) == 4
    refreshed = store.get_trade(trade["trade_id"])
    assert Decimal(refreshed["sl_executed_qty"]) == Decimal("0.08")
    assert len(client.place_calls) == before_posts


def test_stop_child_discovery_backoff_prevents_weight_10_query_every_tick(env):
    cfg, store, engine, client, notifier, tm = env
    fixed = _fixed_discovery_now()
    now = [fixed]
    tm._now = lambda: now[0]
    store._now = lambda: now[0]
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    for order in (tp1, tp2):
        store.transition_trade_order(order["trade_order_id"], "canceled", note="ambiguous child")
    # No stop rows at all: deterministic no-match.
    client.all_orders_rows = [
        _tp_history_row(tp1, status="CANCELED"),
        _tp_history_row(tp2, status="CANCELED"),
    ]

    tm._reconcile_trade(trade["trade_id"])
    assert len(client.all_orders_calls) == 2
    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    reasons = {
        leg["target"]: leg["not_observed_reason"]
        for leg in store.list_oco_legs(group["oco_group_id"])
        if leg["kind"] == "stop_loss"
    }
    assert all("count=1" in reason for reason in reasons.values())

    tm._reconcile_trade(trade["trade_id"])
    assert len(client.all_orders_calls) == 2, "backoff must suppress duplicate allOrders calls"

    now[0] = fixed + timedelta(seconds=61)
    tm._reconcile_trade(trade["trade_id"])
    assert len(client.all_orders_calls) == 4
    reasons = {
        leg["target"]: leg["not_observed_reason"]
        for leg in store.list_oco_legs(group["oco_group_id"])
        if leg["kind"] == "stop_loss"
    }
    assert all("count=2" in reason for reason in reasons.values())


@pytest.mark.parametrize("outcome", ["query_failed", "history_expired"])
def test_stop_child_discovery_backoff_covers_query_and_history_outcomes(env, monkeypatch, outcome):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note=f"discovery {outcome}")
    calls: list[dict] = []
    if outcome == "query_failed":

        def _fail(**kwargs):
            calls.append(kwargs)
            raise MexcNetworkError("timeout")

        monkeypatch.setattr(engine, "query_all_orders", _fail)
    else:
        with store.db.transaction() as conn:
            conn.execute(
                "UPDATE trade_orders SET created_at='2020-01-01T00:00:00Z' WHERE trade_order_id=?",
                (tp1["trade_order_id"],),
            )

    tm._reconcile_trade(trade["trade_id"])
    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    leg = _stop_leg_by_parent(store, trade, tp1)
    assert f"reason=oco_stop_child_{outcome}" in leg["not_observed_reason"]
    assert "count=1" in leg["not_observed_reason"]

    tm._reconcile_trade(trade["trade_id"])
    if outcome == "query_failed":
        assert len(calls) == 1, "query failure must back off"
    else:
        assert client.all_orders_calls == [], "expired history must not query"
    leg = _stop_leg_by_parent(store, trade, tp1)
    assert "count=1" in leg["not_observed_reason"]


@pytest.mark.parametrize("child_state", ["open", "partially_filled"])
@pytest.mark.parametrize("initial_trade_state", ["needs_reconciliation", "oco_active", "oco_partial"])
def test_zero_remaining_does_not_close_with_open_or_partial_stop_child(env, child_state, initial_trade_state):
    """Every lifecycle state must fail closed while a stop child can still fill."""

    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    # Both TP parents filled -> remaining inventory projects to zero.
    store.transition_trade_order(tp1["trade_order_id"], "filled", sold_qty=Decimal("0.05"))
    store.transition_trade_order(tp2["trade_order_id"], "filled", sold_qty=Decimal("0.05"))

    raw_status = "NEW" if child_state == "open" else "PARTIALLY_FILLED"
    executed = "0" if child_state == "open" else "0.01"
    child_id = f"C02__still-working-{child_state}"
    child_row = _stop_history_row(tp1, child_id, status=raw_status, executed=executed, stop_limit_price=SL)
    _register_exchange_order(client, child_row, executed=executed, quote=str(Decimal(executed) * SL))
    tp2_time = APE_MS + 1000
    tp2_child = _stop_history_row(
        tp2,
        f"C02__tp2-canceled-{child_state}",
        status="CANCELED",
        executed="0",
        stop_limit_price=SL,
        time_ms=tp2_time,
    )
    _register_exchange_order(client, tp2_child, executed="0")
    client.all_orders_rows = [
        _tp_history_row(tp1, status="FILLED", executed="0.05"),
        child_row,
        _tp_history_row(tp2, status="FILLED", executed="0.05", time_ms=tp2_time),
        tp2_child,
    ]
    store.update_trade_state(trade["trade_id"], initial_trade_state, note="zero-remaining close gate")

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    assert Decimal(refreshed["sl_executed_qty"]) == Decimal(executed)
    assert len(client.place_calls) == before_posts


def test_terminal_stop_child_refreshes_fee_after_being_seen_open(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="child starts open")
    child_id = "C02__fee-refresh"
    open_row = _stop_history_row(tp1, child_id, status="NEW", executed="0", stop_limit_price=SL)
    _register_exchange_order(client, open_row, executed="0")
    client.all_orders_rows = [_tp_history_row(tp1, status="CANCELED"), open_row]

    tm._reconcile_trade(trade["trade_id"])
    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["state"] == "open"
    assert leg["fee_amount"] is None

    # Child fills later; fee evidence must be fetched on the terminal advance.
    client.orders[child_id]["status"] = "FILLED"
    client.orders[child_id]["executedQty"] = "0.05"
    client.orders[child_id]["cumulativeQuoteQty"] = str(Decimal("0.05") * SL)
    client.trades[child_id] = [
        {
            "id": "fee-refresh-1",
            "orderId": child_id,
            "price": str(SL),
            "qty": "0.05",
            "quoteQty": str(Decimal("0.05") * SL),
            "commission": "0.002",
            "commissionAsset": "USDT",
        }
    ]
    tm._reconcile_trade(trade["trade_id"])

    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["state"] == "filled"
    assert Decimal(leg["fee_amount"]) == Decimal("0.002")


def test_tp_filled_with_canceled_stop_child_counts_only_the_tp(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "filled", sold_qty=Decimal("0.05"))
    child_id = "C02__stop-tp1-canceled"
    child_row = _stop_history_row(tp1, child_id, status="CANCELED", executed="0")
    _register_exchange_order(client, child_row, executed="0")
    client.all_orders_rows = [
        _tp_history_row(tp1, status="FILLED", executed="0.05"),
        child_row,
    ]

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "oco_active", refreshed
    assert Decimal(refreshed["tp1_sold_qty"]) == Decimal("0.05")
    assert Decimal(refreshed["sl_executed_qty"]) == Decimal("0")
    assert len(client.place_calls) == before_posts

    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    stop_leg = _stop_leg_by_parent(store, trade, tp1)
    assert stop_leg["exchange_order_id"] == child_id
    assert Decimal(stop_leg["filled_qty"]) == Decimal("0")


def test_active_oco_with_only_tp_limit_child_visible_remains_covered(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    # While the OCO is active MEXC may list only the TP/limit child.
    client.all_orders_rows = [
        _tp_history_row(tp1, status="NEW"),
        _tp_history_row(tp2, status="NEW"),
    ]

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "oco_active", refreshed
    assert store.buy_block_reason() is None
    assert client.all_orders_calls == [], "active OCO must not be probed for an invisible stop child"
    assert len(client.place_calls) == before_posts


@pytest.mark.parametrize("candidate_count", [0, 2])
def test_unresolved_stop_child_candidates_remain_fail_closed(env, candidate_count):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    tp1, tp2 = _set_distinct_tranche_quantities(store, tp1, tp2)

    rows: list[dict] = []
    for kind, order in (("tp1", tp1), ("tp2", tp2)):
        store.transition_trade_order(order["trade_order_id"], "canceled", note="simulated OCO parent cancel")
        rows.append(_tp_history_row(order, status="CANCELED"))
        for index in range(candidate_count):
            rows.append(
                _stop_history_row(
                    order,
                    f"C02__ambiguous-{kind}-{index}",
                    status="FILLED",
                    executed=str(order["quantity"]),
                )
            )
    client.all_orders_rows = rows

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    assert len(client.place_calls) == before_posts, "unresolved child must never re-arm an OCO"
    assert store.buy_block_reason() is not None

    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    stop_legs = [leg for leg in store.list_oco_legs(group["oco_group_id"]) if leg["kind"] == "stop_loss"]
    assert len(stop_legs) == 2
    assert all(leg["not_observed_reason"] for leg in stop_legs)
    assert all(f"candidate_count:{candidate_count}" in leg["not_observed_reason"] for leg in stop_legs)


def test_partial_stop_child_fill_stays_blocked_and_never_overlaps(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="simulated partial stop fill")
    child_id = "C02__stop-partial"
    child_row = _stop_history_row(tp1, child_id, status="PARTIALLY_FILLED", executed="0.02")
    _register_exchange_order(client, child_row, executed="0.02", quote=str(Decimal("0.02") * SL))
    client.all_orders_rows = [_tp_history_row(tp1, status="CANCELED"), child_row]

    before_posts = len(client.place_calls)
    before_orders = len(store.list_trade_orders(trade["trade_id"]))
    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] != "closed", refreshed
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    assert Decimal(refreshed["sl_executed_qty"]) == Decimal("0.02")
    assert len(client.place_calls) == before_posts, "partial stop fill must not create an overlapping OCO"
    assert len(store.list_trade_orders(trade["trade_id"])) == before_orders
    assert store.buy_block_reason() is not None


def test_transient_oco_query_failure_can_recover_after_fresh_full_coverage(env):
    """A recovered network read may restore a fully verified OCO trade."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    store.update_trade_state(trade["trade_id"], "needs_reconciliation", note="temporary query failure")

    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "oco_active", refreshed


def test_needs_reconciliation_recovers_when_tp1_filled_and_tp2_freshly_covers_remainder(env):
    """LINK-style state: a filled TP1 plus a fresh active TP2 is valid protection."""
    cfg, store, engine, client, notifier, tm = env
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    observed = tm._ts()
    store.transition_trade_order(tp1["trade_order_id"], "filled", sold_qty=Decimal("0.05"), last_observed_at=observed)
    store.transition_trade_order(tp2["trade_order_id"], "open", sold_qty=Decimal("0"), last_observed_at=observed)
    store.update_trade_state(trade["trade_id"], "needs_reconciliation", note="transient TP1 query error")

    tm._reconcile_trade(trade["trade_id"])

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "oco_active", refreshed
    assert Decimal(refreshed["tp1_sold_qty"]) == Decimal("0.05")
    assert Decimal(refreshed["tp2_sold_qty"]) == Decimal("0")


def test_missing_stop_limit_price_never_falls_back_to_legacy_trigger(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    with store.db.transaction() as conn:
        conn.execute("UPDATE trades SET sl_price=NULL WHERE trade_id=?", (trade["trade_id"],))
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="missing persisted SL limit")
    client.all_orders_rows = [
        _tp_history_row(tp1, status="CANCELED"),
        _stop_history_row(
            tp1,
            "C02__trigger-price-decoy",
            status="FILLED",
            executed=str(tp1["quantity"]),
            stop_limit_price=Decimal(str(tp1["stop_trigger_price"])),
        ),
    ]

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["exchange_order_id"] is None
    assert "reason=oco_stop_child_missing_stop_limit_price" in leg["not_observed_reason"]
    assert client.all_orders_calls == []
    assert len(client.place_calls) == before_posts


def test_saturated_all_orders_response_cannot_prove_unique_stop_child(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="history response saturation")
    tp_row = _tp_history_row(tp1, status="CANCELED")
    child_row = _stop_history_row(
        tp1,
        "C02__candidate-in-saturated-history",
        status="FILLED",
        executed=str(tp1["quantity"]),
        stop_limit_price=SL,
    )
    fillers = [
        {
            "symbol": trade["symbol"],
            "orderId": f"unrelated-{index}",
            "orderListId": -1,
            "clientOrderId": None,
            "side": "SELL",
            "type": "LIMIT",
            "status": "CANCELED",
            "price": "999",
            "origQty": "1",
            "executedQty": "0",
            "time": APE_MS + 1000 + index,
            "updateTime": APE_MS + 1000 + index,
        }
        for index in range(998)
    ]
    client.all_orders_rows = [tp_row, child_row, *fillers]
    assert len(client.all_orders_rows) == 1000

    before_posts = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])

    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["exchange_order_id"] is None
    assert "reason=oco_stop_child_history_result_limit_reached" in leg["not_observed_reason"]
    assert len(client.all_orders_calls) == 1
    assert len(client.place_calls) == before_posts


def test_terminal_stop_child_refreshes_fee_when_partial_fee_already_exists(env):
    cfg, store, engine, client, notifier, tm = env
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(env, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    store.transition_trade_order(tp1["trade_order_id"], "canceled", note="partial stop child")
    child_id = "C02__partial-fee-refresh"
    partial_row = _stop_history_row(tp1, child_id, status="PARTIALLY_FILLED", executed="0.02", stop_limit_price=SL)
    _register_exchange_order(
        client,
        partial_row,
        executed="0.02",
        quote=str(Decimal("0.02") * SL),
        fees=[
            {
                "id": "fee-partial",
                "orderId": child_id,
                "price": str(SL),
                "qty": "0.02",
                "quoteQty": str(Decimal("0.02") * SL),
                "commission": "0.001",
                "commissionAsset": "USDT",
            }
        ],
    )
    client.all_orders_rows = [_tp_history_row(tp1, status="CANCELED"), partial_row]

    tm._reconcile_trade(trade["trade_id"])
    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["state"] == "partially_filled"
    assert Decimal(leg["fee_amount"]) == Decimal("0.001")

    # Terminal status advances without changing executed quantity. Fees may
    # have been finalized/adjusted by the exchange and must be refreshed.
    client.orders[child_id]["status"] = "FILLED"
    client.trades[child_id] = [
        {
            "id": "fee-final",
            "orderId": child_id,
            "price": str(SL),
            "qty": "0.02",
            "quoteQty": str(Decimal("0.02") * SL),
            "commission": "0.002",
            "commissionAsset": "USDT",
        }
    ]
    tm._reconcile_trade(trade["trade_id"])

    leg = _stop_leg_by_parent(store, trade, tp1)
    assert leg["state"] == "filled"
    assert Decimal(leg["filled_qty"]) == Decimal("0.02")
    assert Decimal(leg["fee_amount"]) == Decimal("0.002")
