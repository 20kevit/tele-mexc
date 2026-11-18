"""P0 characterization freeze: the per-tranche native OCO model.

Audit model being frozen: every confirmed entry-fill tranche gets exactly two
native OCO SELL orders — OCO1 = ``tp1_allocation_pct`` of the tranche at TP1,
OCO2 = the remainder at TP2 — and each native OCO carries a TP limit leg plus
an SL stop-limit leg.  Both pending order rows are persisted atomically
(``TradeStore.create_trade_order_pair``) before either is submitted.  A
multi-tranche position therefore holds ``2 x tranche_count`` OCO orders, each
tranche computed from the incremental fill, never by amending an existing one.

No exchange calls: the FakeClient records every submission.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from _trading_fakes import FakeClient, FakeEngine, FakeNotifier
from test_trading_tp_sl import _insert_intent, _parsed

from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore


@pytest.fixture()
def env(cfg, repo):
    """Same deterministic harness tuple as the OCO exit test suite."""
    cfg.trading.entry_trigger_offset_ticks = 2  # enable on_tick evaluation
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient()
    engine = FakeEngine(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=notifier)
    return cfg, store, engine, client, notifier, tm


ENTRY = Decimal("100")
TP1 = Decimal("110")
TP2 = Decimal("120")
SL = Decimal("95")
SYMBOL = "BTCUSDT"


def _credit_first_fill(harness, trade, intent, qty: Decimal) -> None:
    """Credit the first entry fill and let the manager place tranche 1."""
    cfg, store, engine, client, notifier, tm = harness
    client.fill_order(intent["exchange_order_id"], qty, qty * ENTRY, ENTRY)
    store.add_fill(
        exchange_order_id=intent["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(ENTRY),
            "qty": str(qty),
            "quoteQty": str(qty * ENTRY),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 1,
            "isMaker": 0,
            "time": 1,
        },
    )
    tm.on_intent_executed(store.get_intent(intent["intent_id"]))


def _credit_later_fill(harness, trade_id: str, exchange_order_id: str, qty: Decimal, time: int) -> None:
    """Credit an incremental entry fill while the entry remainder works."""
    cfg, store, engine, client, notifier, tm = harness
    store.add_fill(
        exchange_order_id=exchange_order_id,
        symbol=SYMBOL,
        raw={
            "price": str(ENTRY),
            "qty": str(qty),
            "quoteQty": str(qty * ENTRY),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 1,
            "isMaker": 0,
            "time": time,
        },
    )
    tm._reconcile_trade(trade_id)


def _tranches(store: TradeStore, trade_id: str) -> dict[str, list[dict]]:
    """Map tranche oco_group_id -> its tp1/tp2 order rows."""
    orders = store.list_trade_orders(trade_id, kind="tp1") + store.list_trade_orders(trade_id, kind="tp2")
    tranches: dict[str, list[dict]] = {}
    for order in orders:
        tranches.setdefault(str(order["oco_group_id"]), []).append(order)
    return tranches


def test_p0_multi_tranche_position_has_two_native_ocos_per_tranche(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=ENTRY).trade
    assert trade is not None
    engine._execute_intent(intent["intent_id"])
    acknowledged = store.get_intent(intent["intent_id"])
    assert acknowledged is not None and acknowledged.get("exchange_order_id")

    # Three incremental entry fills: 0.06 + 0.02 + 0.02 = 0.10 acquired.
    _credit_first_fill(env, trade, acknowledged, Decimal("0.06"))
    _credit_later_fill(env, trade["trade_id"], acknowledged["exchange_order_id"], Decimal("0.02"), 2)
    _credit_later_fill(env, trade["trade_id"], acknowledged["exchange_order_id"], Decimal("0.02"), 3)

    refreshed = store.get_trade(trade["trade_id"])
    assert Decimal(refreshed["acquired_base_qty"]) == Decimal("0.10")

    # 3 tranches x 2 native OCOs = 6 OCO parent orders (never a fixed 2).
    tranches = _tranches(store, trade["trade_id"])
    assert len(tranches) == 3
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp1")) == 3
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp2")) == 3

    # Each tranche: OCO1 = 50% of the INCREMENTAL fill at TP1; OCO2 = the
    # remainder at TP2 (cumulative-allocation bugs change these numbers).
    expected = {
        Decimal("0.06"): (Decimal("0.03"), Decimal("0.03")),
        Decimal("0.02"): (Decimal("0.01"), Decimal("0.01")),
    }
    for group_orders in tranches.values():
        assert len(group_orders) == 2
        by_kind = {order["kind"]: order for order in group_orders}
        assert set(by_kind) == {"tp1", "tp2"}
        tp1, tp2 = by_kind["tp1"], by_kind["tp2"]
        # Prices: OCO1 at TP1, OCO2 at TP2, both with the SL stop trigger.
        assert Decimal(tp1["price"]) == TP1
        assert Decimal(tp2["price"]) == TP2
        assert Decimal(tp1["stop_trigger_price"]) == SL
        assert Decimal(tp2["stop_trigger_price"]) == SL
        # The (OCO1, OCO2) pair must be exactly the incremental-fill
        # allocation; only the correct multiset can satisfy this per tranche.
        tranche_total = Decimal(tp1["quantity"]) + Decimal(tp2["quantity"])
        assert tranche_total in expected, tranche_total
        assert (Decimal(tp1["quantity"]), Decimal(tp2["quantity"])) == expected[tranche_total]

    # Distinct tranche identity and distinct client order ids everywhere.
    cids = [str(order["client_order_id"]) for group in tranches.values() for order in group]
    assert len(set(cids)) == 6

    # Each native OCO carries a TP limit leg plus its own SL stop-limit leg:
    # 4 legs per tranche (2 take_profit + 2 stop placeholders), 12 in total.
    group = store.get_oco_group(trade["trade_id"])
    assert group is not None
    legs = store.list_oco_legs(group["oco_group_id"])
    assert len(legs) == 12
    take_profit_legs = [leg for leg in legs if leg["kind"] == "take_profit"]
    stop_legs = [leg for leg in legs if leg["kind"] == "stop_loss"]
    assert len(take_profit_legs) == 6
    assert len(stop_legs) == 6
    # The stop legs are placeholders of the native OCO, not separate orders:
    # their exchange child is not independently visible while the OCO works.
    assert all(leg.get("not_observed_reason") == "oco_stop_child_not_yet_visible" for leg in stop_legs)

    # Full acquired quantity is covered by working OCO parents.
    assert tm._active_oco_coverage(refreshed) == Decimal("0.10")

    # Reconciliation never duplicates an existing tranche.
    before = len(client.place_calls)
    tm._reconcile_trade(trade["trade_id"])
    assert len(_tranches(store, trade["trade_id"])) == 3
    assert len(client.place_calls) == before


def test_p0_tranche_order_pairs_persist_atomically_before_any_submission(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=ENTRY).trade
    assert trade is not None
    trade_id = trade["trade_id"]
    assert store.list_trade_orders(trade_id, kind="tp1") == []
    assert store.list_trade_orders(trade_id, kind="tp2") == []
    assert client.place_calls == []

    # Spec shape mirrors the production `_create_oco_tranche` builder.
    tranche_id = "oco-trade-pair:t0001"

    def spec(kind: str, price: Decimal) -> dict:
        return {
            "trade_order_id": f"to-pair-{kind}",
            "trade_id": trade_id,
            "kind": kind,
            "client_order_id": f"cid-pair-{kind}",
            "symbol": SYMBOL,
            "side": "sell",
            "order_type": "OCO_STOP_LIMIT_ORDER",
            "price": price,
            "quantity": Decimal("0.01"),
            "allocated_qty": Decimal("0.01"),
            "stop_trigger_price": SL,
            "attempt": 1,
            "oco_group_id": tranche_id,
            "leg_role": kind,
        }

    tp1_spec = spec("tp1", TP1)
    tp2_spec = spec("tp2", TP2)

    # The store API refuses any spec list that is not exactly {tp1, tp2}.
    invalid_spec_lists = [
        [tp1_spec],
        [tp1_spec, tp2_spec, tp1_spec],
        [spec("tp1", TP1), spec("tp1", TP1)],
    ]
    for specs in invalid_spec_lists:
        with pytest.raises(ValueError, match="exactly one tp1 and one tp2"):
            store.create_trade_order_pair(specs)

    # Nothing was persisted by the failed calls.
    assert store.list_trade_orders(trade_id, kind="tp1") == []
    assert store.list_trade_orders(trade_id, kind="tp2") == []

    # The valid pair persists both pending rows in one transaction, with no
    # exchange write at all (create_trade_order_pair never submits).
    stored = store.create_trade_order_pair([tp1_spec, tp2_spec])
    assert len(stored) == 2
    assert {order["kind"] for order in stored} == {"tp1", "tp2"}
    assert all(order["state"] == "pending" for order in stored)
    assert len({order["client_order_id"] for order in stored}) == 2
    assert len({order["trade_order_id"] for order in stored}) == 2
    for kind in ("tp1", "tp2"):
        rows = store.list_trade_orders(trade_id, kind=kind)
        assert len(rows) == 1
        assert rows[0]["state"] == "pending"
    assert len(client.place_calls) == 0


def test_p0_submission_failure_after_pair_persistence_leaves_both_rows(env, monkeypatch):
    """Both pending rows must exist before the first leg reaches the exchange."""
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=ENTRY).trade
    assert trade is not None
    engine._execute_intent(intent["intent_id"])
    acknowledged = store.get_intent(intent["intent_id"])
    assert acknowledged is not None and acknowledged.get("exchange_order_id")
    _credit_first_fill(env, trade, acknowledged, Decimal("0.06"))

    # Tranche 1 submitted normally: 1 entry BUY + 2 native OCO parents
    # (OCO1 at TP1 and OCO2 at TP2, each one exchange order with a TP limit
    # leg and an SL stop-limit leg).
    assert len(client.place_calls) == 3
    sell_calls = [call for call in client.place_calls if call["side"] == "SELL"]
    assert len(sell_calls) == 2
    assert all(call["order_type"] == "OCO_STOP_LIMIT_ORDER" for call in sell_calls)
    assert sorted(Decimal(str(call["price"])) for call in sell_calls) == [TP1, TP2]

    # A second fill arrives, but the exchange now rejects every submission.
    attempts: list[dict] = []

    def failing_place(**kwargs):
        attempts.append(kwargs)
        raise RuntimeError("exchange outage during OCO submission")

    monkeypatch.setattr(client, "place_order", failing_place)
    _credit_later_fill(env, trade["trade_id"], acknowledged["exchange_order_id"], Decimal("0.02"), 2)

    # Both tranche legs were attempted (each leg fails independently); every
    # attempt carries the client order id of a PERSISTED pending row — the
    # pair was written to the store before either exchange submission.
    assert len(attempts) == 2
    assert all(call["symbol"] == SYMBOL for call in attempts)
    assert all(call["side"] == "SELL" for call in attempts)

    # Both pending rows of the second tranche are durably persisted even
    # though the exchange rejected their submissions.
    tranches = _tranches(store, trade["trade_id"])
    assert len(tranches) == 2
    second = [
        group
        for group in tranches.values()
        if all(Decimal(o["quantity"]) == Decimal("0.01") for o in group) and len(group) == 2
    ]
    assert len(second) == 1
    second_orders = second[0]
    assert {order["kind"] for order in second_orders} == {"tp1", "tp2"}
    persisted_cids = {str(order["client_order_id"]) for order in second_orders}
    assert persisted_cids == {str(call["client_order_id"]) for call in attempts}
    assert len(persisted_cids) == 2
    for order in second_orders:
        # The failed legs are not silently open: they are parked for
        # reconciliation or still pending — never presented as protected.
        assert order["state"] in ("pending", "submitting", "unknown_requires_reconciliation", "rejected")
    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] in ("oco_submitting", "oco_partial", "needs_reconciliation", "unprotected_critical")
