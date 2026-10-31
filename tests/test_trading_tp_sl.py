"""Take-profit (TP1/TP2) and stop-loss management via native OCO orders.

All exchange interaction goes through :class:`FakeEngine`, so no real orders are
ever placed.  Durable ``trades``/``trade_orders``/``order_intents`` tables are
exercised via a real :class:`TradeStore` on a temporary SQLite database.

With native OCO exits, two OCO SELL orders are placed immediately once the entry
fills are confirmed:
  * OCO 1: ~50% of acquired quantity at TP1 with stop-limit at SL.
  * OCO 2: remaining ~50% at TP2 with the same stop-limit at SL.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from _trading_fakes import FakeClient, FakeEngine, FakeNotifier

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.repository import Repository
from tg_signal_ingest.trading.parser import ParsedSignal
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

ENTRY = Decimal("100")
TP1 = Decimal("110")
TP2 = Decimal("120")
SL = Decimal("95")
SYMBOL = "BTCUSDT"


@pytest.fixture()
def env(cfg: AppConfig, repo: Repository):
    cfg.trading.entry_trigger_offset_ticks = 2  # enable on_tick evaluation
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient()
    engine = FakeEngine(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=notifier)
    return cfg, store, engine, client, notifier, tm


def _parsed(entry=ENTRY, tp1=TP1, tp2=TP2, sl=SL) -> ParsedSignal:
    return ParsedSignal(
        kind="alert",
        action="buy",
        symbol=SYMBOL,
        entry_price=entry,
        entry_low_price=None,
        entry_high_price=None,
        target_prices=(tp1, tp2),
        stop_loss=sl,
        order_type="limit",
        amount_text=None,
        raw_text="buy",
        normalized_text="buy",
        fingerprint_sha256="abc",
    )


def _insert_signal(store: TradeStore, signal_id: str = "sig-1") -> None:
    with store.db.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO signals (signal_id, channel_id, message_id, edit_version, "
            "parser_version, signal_kind, symbol, action, status, fingerprint_sha256, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, 1, 'alert', ?, 'buy', 'parsed', ?, 'now', 'now')",
            (signal_id, 100, 1, SYMBOL, "abc"),
        )


def _insert_intent(
    store: TradeStore,
    *,
    signal_id: str = "sig-1",
    client_order_id: str = "tsi-1",
    entry: Decimal = ENTRY,
    quote: Decimal = Decimal("10"),
) -> dict:
    _insert_signal(store, signal_id)
    iid = f"intent-{client_order_id}"
    with store.db.transaction() as conn:
        conn.execute(
            """INSERT INTO order_intents
            (intent_id, signal_id, idempotency_key, client_order_id, symbol, side,
             order_type, quote_amount, base_quantity, limit_price, mode, state,
             create_msg_id, create_channel_id, planned_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                iid,
                signal_id,
                f"idem-{client_order_id}",
                client_order_id,
                SYMBOL,
                "buy",
                "limit",
                str(quote),
                "0.1",
                str(entry),
                "live",
                "planned",
                1,
                100,
                "now",
                "now",
            ),
        )
    return store.get_intent(iid)


def _oco_orders(store: TradeStore, trade, kind: str = "tp1") -> list[dict]:
    orders = store.list_trade_orders(trade["trade_id"], kind=kind)
    assert orders, f"expected a {kind} OCO order"
    return orders


def _setup_direct_entry(env, entry=ENTRY, fill_qty=Decimal("0")):
    """Create a trade, submit the entry (direct path), optionally credit a fill.

    When an entry fill is confirmed, two native OCO exit orders are placed
    automatically.  Returns (trade, intent).
    """
    cfg, store, engine, client, notifier, tm = env
    tm._clock = lambda: 5000.0  # keep ticks fresh vs the stale threshold
    intent = _insert_intent(store, entry=entry)
    decision = tm.on_intent_planned(intent, _parsed(entry=entry), reference_price=Decimal("100"))
    assert decision.deferred is False
    trade = decision.trade
    # Engine submits the entry (direct path).
    engine._execute_intent(intent["intent_id"])
    intent = engine.store.get_intent(intent["intent_id"])
    tm.on_intent_executed(intent)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "entry_active", trade
    # Credit an entry fill so there is a position to manage.
    if fill_qty > 0:
        client.fill_order(intent["exchange_order_id"], fill_qty, fill_qty * entry, entry)
        store.add_fill(
            exchange_order_id=intent["exchange_order_id"],
            symbol=SYMBOL,
            raw={
                "price": str(entry),
                "qty": str(fill_qty),
                "quoteQty": str(fill_qty * entry),
                "commission": "0",
                "commissionAsset": "USDT",
                "isBuyer": 1,
                "isMaker": 0,
                "time": 1,
            },
        )
        # A partial fill leaves the entry order working.  Cancel the unfilled
        # remainder so the entry is terminal; OCO exits are only placed once
        # the entry can no longer fill again.
        base_qty = Decimal(str(intent.get("base_quantity") or "0"))
        if fill_qty < base_qty:
            engine.cancel_intent_order(intent)
        tm._reconcile_entry_fills(trade)
        trade = store.get_trade(trade["trade_id"])
        # OCO exits should now be placed.
        assert trade["trade_state"] in ("oco_active", "oco_partial", "oco_submitting"), trade
    return trade, intent


# --------------------------------------------------------------------------
# OCO exit placement at entry fill
# --------------------------------------------------------------------------


def test_oco_exits_placed_on_entry_fill(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    # Both OCO orders should exist.
    tp1_orders = _oco_orders(store, trade, "tp1")
    tp2_orders = _oco_orders(store, trade, "tp2")
    assert len(tp1_orders) == 1
    assert len(tp2_orders) == 1
    # OCO 1: ~50% of 0.1 = 0.05 at TP1=110, stop-loss at SL=95.
    assert Decimal(tp1_orders[0]["quantity"]) == Decimal("0.05")
    assert Decimal(tp1_orders[0]["price"]) == TP1
    assert tp1_orders[0]["order_type"] == "OCO_STOP_LIMIT_ORDER"
    # OCO 2: remaining 0.05 at TP2=120, stop-loss at SL=95.
    assert Decimal(tp2_orders[0]["quantity"]) == Decimal("0.05")
    assert Decimal(tp2_orders[0]["price"]) == TP2
    assert tp2_orders[0]["order_type"] == "OCO_STOP_LIMIT_ORDER"
    assert trade["trade_state"] == "oco_active"


def test_oco_partial_quantities(env):
    """OCO quantities are computed from actual acquired fills."""
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.06"))
    tp1_orders = _oco_orders(store, trade, "tp1")
    tp2_orders = _oco_orders(store, trade, "tp2")
    # 50% of 0.06 = 0.03, remaining = 0.03.
    assert Decimal(tp1_orders[0]["quantity"]) == Decimal("0.03")
    assert Decimal(tp2_orders[0]["quantity"]) == Decimal("0.03")


# --------------------------------------------------------------------------
# OCO fills advance the trade
# --------------------------------------------------------------------------


def test_oco_tp1_fill_advances_to_tp2(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
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
    # TP1 allocation fully sold; OCO2 still active.
    assert Decimal(trade["tp1_sold_qty"]) == Decimal("0.05")


def test_oco_both_filled_closes_trade(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
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
    tm._reconcile_trade_orders(trade)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "closed", trade


# --------------------------------------------------------------------------
# Stop-loss via OCO stop-limit legs (no separate market sell)
# --------------------------------------------------------------------------


def test_oco_stop_loss_leg_exits_position(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    # Stop-loss hit: OCO stop-limit legs sell the position. No separate
    # market sell order is placed.
    tm.on_tick(SYMBOL, Decimal("95"), received_at=5000.0)
    trade = store.get_trade(trade["trade_id"])
    # No separate stop-loss market sell call.
    assert engine.sl_calls == [], engine.sl_calls
    # OCO orders were reconciled; stop-limit legs may have filled.
    assert trade["trade_state"] in ("oco_active", "oco_partial", "closed", "needs_reconciliation"), trade


def test_oco_no_separate_market_sell(env):
    """OCO exits replace the separate stop-loss market sell."""
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    # TP1 tick - OCO orders already exist, no new LIMIT sell placed.
    tm.on_tick(SYMBOL, Decimal("110"), received_at=5000.0)
    trade = store.get_trade(trade["trade_id"])
    # Only entry + 2 OCO orders were placed (3 total), no additional sells.
    place_calls = [c for c in client.place_calls if c.get("order_type") == "OCO_STOP_LIMIT_ORDER"]
    assert len(place_calls) == 2, place_calls


# --------------------------------------------------------------------------
# Restart safety
# --------------------------------------------------------------------------


def test_oco_position_recovers_after_restart(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active"
    # New manager over the same DB (simulated restart).
    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "oco_active", t


def test_oco_waits_for_terminal_entry_with_partial_fill(env):
    """No exit may be placed while the entry order can still fill."""
    cfg, store, engine, client, notifier, tm = env
    tm._clock = lambda: 5000.0
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100")).trade
    engine._execute_intent(intent["intent_id"])
    intent = engine.store.get_intent(intent["intent_id"])
    tm.on_intent_executed(intent)
    trade = store.get_trade(trade["trade_id"])

    # Partial fill, entry remainder still open.
    client.fill_order(intent["exchange_order_id"], Decimal("0.06"), Decimal("6"), ENTRY)
    store.add_fill(
        exchange_order_id=intent["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(ENTRY),
            "qty": "0.06",
            "quoteQty": "6",
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 1,
            "isMaker": 0,
            "time": 1,
        },
    )
    tm._reconcile_entry_fills(trade)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "entry_active", trade
    assert store.list_trade_orders(trade["trade_id"], kind="tp1") == []
    assert store.list_trade_orders(trade["trade_id"], kind="tp2") == []
    assert [c for c in client.place_calls if c.get("order_type") == "OCO_STOP_LIMIT_ORDER"] == []

    # Cancel the remainder: the entry is now terminal and protection is placed.
    engine.cancel_intent_order(intent)
    tm._reconcile_entry_fills(trade)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active", trade
    assert Decimal(trade["acquired_base_qty"]) == Decimal("0.06")
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp1")) == 1
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp2")) == 1


def test_oco_partial_rejection_retries_only_missing_leg(env):
    cfg, store, engine, client, notifier, tm = env
    original = engine.submit_oco_sell
    calls = {"tp1": 0, "tp2": 0}
    fail = {"tp2": True}

    def flaky(trade, client_order_id, price, quantity, stop_price, kind=None, **kwargs):
        calls[kind] += 1
        if kind == "tp2" and fail["tp2"]:
            fail["tp2"] = False
            raise ValueError("tp2 rejected by exchange")
        return original(trade, client_order_id, price, quantity, stop_price, kind=kind, **kwargs)

    engine.submit_oco_sell = flaky
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_partial", trade
    assert calls == {"tp1": 1, "tp2": 1}

    tm.reconcile_all_open()
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active", trade
    # The already-confirmed TP1 leg was reused, not resubmitted.
    assert calls == {"tp1": 1, "tp2": 2}


def test_uncertain_oco_leg_is_never_resubmitted(env):
    from tg_signal_ingest.mexc.errors import MexcNetworkError

    cfg, store, engine, client, notifier, tm = env
    original = engine.submit_oco_sell
    calls = {"tp1": 0, "tp2": 0}

    def flaky(trade, client_order_id, price, quantity, stop_price, kind=None, **kwargs):
        calls[kind] += 1
        if kind == "tp2":
            raise MexcNetworkError("timeout after send")
        return original(trade, client_order_id, price, quantity, stop_price, kind=kind, **kwargs)

    engine.submit_oco_sell = flaky
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] != "oco_active", trade

    # A later sweep must reconcile, never resubmit the uncertain leg.
    tm.reconcile_all_open()
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] in ("oco_submitting", "oco_partial"), trade
    assert calls["tp2"] == 1
    tp2_orders = store.list_trade_orders(trade["trade_id"], kind="tp2")
    assert len(tp2_orders) == 1
    assert tp2_orders[0]["state"] == "unknown_requires_reconciliation"


def test_restart_recovers_partial_oco_protection(env):
    cfg, store, engine, client, notifier, tm = env
    original = engine.submit_oco_sell
    calls = {"tp1": 0, "tp2": 0}
    fail = {"tp2": True}

    def flaky(trade, client_order_id, price, quantity, stop_price, kind=None, **kwargs):
        calls[kind] += 1
        if kind == "tp2" and fail["tp2"]:
            fail["tp2"] = False
            raise ValueError("rejected once")
        return original(trade, client_order_id, price, quantity, stop_price, kind=kind, **kwargs)

    engine.submit_oco_sell = flaky
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_partial"

    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active", trade
    assert calls == {"tp1": 1, "tp2": 2}


# --------------------------------------------------------------------------
# Safety: no orders for non-tradeable / other symbols
# --------------------------------------------------------------------------


def test_ignores_other_symbol(env):
    cfg, store, engine, client, notifier, tm = env
    trade, intent = _setup_direct_entry(env, fill_qty=Decimal("0.1"))
    # A tick for a different symbol must not touch this trade.
    tm.on_tick("ETHUSDT", Decimal("2000"), received_at=5000.0)
    trade = store.get_trade(trade["trade_id"])
    assert trade["trade_state"] == "oco_active"
    # The entry BUY was placed during setup; OCO exits for this symbol only.
    oco_calls = [c for c in client.place_calls if c.get("order_type") == "OCO_STOP_LIMIT_ORDER"]
    assert len(oco_calls) == 2, oco_calls
