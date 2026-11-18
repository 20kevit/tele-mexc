"""P0 characterization freeze: unattributed OCO parent cancellation.

Incident (MYX 2026-10-15): both native OCO SELL parents of a live position
were found CANCELED on the exchange while their stop children were never
independently attributed.  The required behaviour is:

* the trade is parked in ``needs_reconciliation`` (never re-armed);
* the global new-BUY block activates with a reason naming the symbol/trade;
* unrelated symbols/trades stay tradeable (per-symbol independence);
* stop-child discovery retries on a bounded exponential backoff;
* the block clears only after unique child attribution backed by exchange
  evidence (exact allOrders field matching), never by guesswork.

Reuses the deterministic lifecycle-recovery harness; no exchange calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from test_trading_lifecycle_recovery import (
    APE_MS,
    SL,
    _oco_orders,
    _pin_discovery_clock,
    _register_exchange_order,
    _stop_history_row,
    _stop_leg_by_parent,
    _tp_history_row,
    make_env,
    setup_trade,
)

from tg_signal_ingest.trading.gate import SubmissionGate

SYMBOL = "BTCUSDT"  # harness trade symbol


def _advance(tm, store, seconds: float) -> None:
    """Move the pinned discovery clock forward deterministically."""
    fixed = datetime.fromtimestamp((APE_MS + seconds * 1000) / 1000, tz=UTC)
    tm._now = lambda: fixed
    store._now = lambda: fixed


def _cancel_parent(store, order: dict) -> None:
    store.transition_trade_order(order["trade_order_id"], "canceled", note="simulated OCO parent cancel")


def test_p0_canceled_parents_park_trade_and_block_new_buys_with_reason(cfg, repo):
    harness = make_env(cfg, repo)
    cfg, store, engine, client, notifier, tm = harness
    trade, _ = setup_trade(harness, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]

    # Both native OCO parents vanish on the exchange (the incident shape):
    # no stop child is attributed, nothing proves what executed.
    _cancel_parent(store, tp1)
    _cancel_parent(store, tp2)
    placement_count = len(client.place_calls)

    tm._reconcile_trade(trade["trade_id"])
    tm._refresh_buy_block()

    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    # Coverage is NOT falsely verified: no re-arm, no new orders at all.
    assert len(client.place_calls) == placement_count, "ambiguous child outcome must never re-arm an OCO"

    # The global new-BUY block is active and names the symbol and the trade.
    reason = store.buy_block_reason()
    assert reason is not None
    assert SYMBOL in reason
    assert trade["trade_id"] in reason

    # The incident response is durably recorded: close is REFUSED while the
    # unsold base quantity is unproven (fail-closed close), the discovery
    # failures are audited with their backoff reason, and the BUY block is
    # persisted as a risk audit event.
    close_refusals = store.db.conn.execute(
        "SELECT detail FROM audit_events WHERE event='needs_reconciliation' AND detail LIKE 'close refused:%'"
    ).fetchall()
    assert any("close refused: 0.1 base quantity remains unsold" in str(row["detail"]) for row in close_refusals)
    unresolved = store.db.conn.execute(
        "SELECT detail FROM audit_events WHERE event='oco_stop_child_unresolved'"
    ).fetchall()
    assert any("oco_stop_child_tp_not_found" in str(row["detail"]) for row in unresolved)
    blocked = store.db.conn.execute("SELECT detail FROM audit_events WHERE event='new_buys_blocked'").fetchall()
    assert any(SYMBOL in str(row["detail"]) and trade["trade_id"] in str(row["detail"]) for row in blocked)


def test_p0_block_is_global_for_buys_but_names_only_the_incident_trade(cfg, repo):
    """The persisted block gates every new BUY; nothing else is collateral damage."""
    harness = make_env(cfg, repo)
    cfg, store, engine, client, notifier, tm = harness
    trade, _ = setup_trade(harness, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    _cancel_parent(store, tp1)
    _cancel_parent(store, tp2)

    tm._reconcile_trade(trade["trade_id"])
    tm._refresh_buy_block()

    gate = SubmissionGate(repo.db, cfg)
    with repo.db.transaction() as conn:
        # New BUYs on any symbol are blocked while the incident is unresolved...
        entry_block = gate.check(conn, purpose="entry")
        assert entry_block is not None
        assert "unprotected position(s)" in entry_block
        # ...and the reason names exactly the incident symbol/trade, never an
        # unrelated one.
        assert SYMBOL in entry_block
        assert trade["trade_id"] in entry_block
        assert "APEUSDT" not in entry_block
        assert "LPTUSDT" not in entry_block
        # Protection/exit submissions are never blocked by the incident.
        assert gate.check(conn, purpose="protection") is None


def test_p0_stop_child_discovery_backs_off_within_the_incident(cfg, repo):
    harness = make_env(cfg, repo)
    cfg, store, engine, client, notifier, tm = harness
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(harness, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    _cancel_parent(store, tp1)
    _cancel_parent(store, tp2)
    # allOrders has no rows at all: the TP parent itself is not found.
    client.all_orders_rows = []
    refreshed_trade = store.get_trade(trade["trade_id"])
    assert refreshed_trade is not None

    def leg_for(parent: dict) -> dict:
        leg = _stop_leg_by_parent(store, refreshed_trade, parent)
        assert leg is not None
        return leg

    # Attempt 1: failure recorded with the exact fail-closed reason (and the
    # same event the close-refusal path later keys on).
    tm._discover_oco_stop_children(refreshed_trade)
    leg = leg_for(tp1)
    assert leg["not_observed_reason"].startswith("OCO_STOP_CHILD_DISCOVERY:count=1:")
    assert "oco_stop_child_tp_not_found" in leg["not_observed_reason"]
    first_remaining = tm._oco_discovery_backoff_remaining(leg)
    # The clock is pinned to the failure instant: elapsed is exactly zero, so
    # the remaining window equals the full scheduled delay.
    assert first_remaining == 60.0

    # An immediate retry is parked by the backoff (streak must not advance).
    tm._discover_oco_stop_children(refreshed_trade)
    leg = leg_for(tp1)
    assert "count=1:" in leg["not_observed_reason"]

    # Now allOrders lists the canceled TP parents but still no stop child:
    # the next attempt must fail closed with the candidate-count reason.
    _advance(tm, store, 61)
    client.all_orders_rows = [
        _tp_history_row(tp1, status="CANCELED", time_ms=APE_MS),
        _tp_history_row(tp2, status="CANCELED", time_ms=APE_MS),
    ]
    tm._discover_oco_stop_children(refreshed_trade)
    leg = leg_for(tp1)
    assert "count=2:" in leg["not_observed_reason"], leg["not_observed_reason"]
    assert "oco_stop_child_candidate_count:0" in leg["not_observed_reason"]

    # The bounded exponential schedule: 60 * 2^(streak-1), capped at 900s.
    # Times are cumulative from the incident start so each attempt lands
    # after the full delay of the previous streak.
    for seconds, streak, delay in ((182, 3, 240.0), (423, 4, 480.0), (904, 5, 900.0)):
        _advance(tm, store, seconds)
        tm._discover_oco_stop_children(refreshed_trade)
        leg = leg_for(tp1)
        assert f"count={streak}:" in leg["not_observed_reason"], (seconds, leg["not_observed_reason"])
        remaining = tm._oco_discovery_backoff_remaining(leg)
        assert remaining == delay, (seconds, streak, remaining, delay)

    # The cap holds: past the cap the delay stays 900 seconds.
    _advance(tm, store, 1805)
    tm._discover_oco_stop_children(refreshed_trade)
    leg = leg_for(tp1)
    assert "count=6:" in leg["not_observed_reason"]
    assert tm._oco_discovery_backoff_remaining(leg) <= 900.0


def test_p0_block_clears_only_after_unique_child_attribution_with_exchange_evidence(cfg, repo):
    harness = make_env(cfg, repo)
    cfg, store, engine, client, notifier, tm = harness
    _pin_discovery_clock(tm, store)
    trade, _ = setup_trade(harness, fill=Decimal("0.1"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    _cancel_parent(store, tp1)
    _cancel_parent(store, tp2)
    tm._reconcile_trade(trade["trade_id"])
    tm._refresh_buy_block()
    assert store.get_trade(trade["trade_id"])["trade_state"] == "needs_reconciliation"
    assert store.buy_block_reason() is not None

    # Exchange evidence arrives for TP2 only: its stop child FILLED with the
    # exact allOrders field shape (same creation time, stop-limit price).
    tp2_row = _tp_history_row(tp2, status="CANCELED", time_ms=APE_MS)
    tp2_child = _stop_history_row(
        tp2, "C02__tp2-stop-filled", status="FILLED", executed=str(tp2["quantity"]), stop_limit_price=SL
    )
    client.all_orders_rows = [tp2_row, tp2_child]
    _register_exchange_order(
        client,
        tp2_child,
        executed=str(tp2["quantity"]),
        quote=str(Decimal(str(tp2["quantity"])) * SL),
        fees=[
            {
                "id": "fee-tp2",
                "orderId": "C02__tp2-stop-filled",
                "price": str(SL),
                "qty": str(tp2["quantity"]),
                "quoteQty": str(Decimal(str(tp2["quantity"])) * SL),
                "commission": "0.001",
                "commissionAsset": "USDT",
            }
        ],
    )
    # Discovery retries obey the bounded backoff: the next attempt must land
    # after the first failure's 60-second window.
    _advance(tm, store, 61)
    tm._reconcile_trade(trade["trade_id"])
    tm._refresh_buy_block()

    # Partial attribution is NOT enough: TP1's child is still unproven, so the
    # trade stays parked and the global block stays active.
    assert store.get_trade(trade["trade_id"])["trade_state"] == "needs_reconciliation"
    assert store.buy_block_reason() is not None

    # TP1's stop child evidence completes the unique per-parent attribution:
    # both children match their parent exactly (same time, same quantity).
    tp1_row = _tp_history_row(tp1, status="CANCELED", time_ms=APE_MS)
    tp1_child = _stop_history_row(
        tp1, "C02__tp1-stop-filled", status="FILLED", executed=str(tp1["quantity"]), stop_limit_price=SL
    )
    client.all_orders_rows = [tp1_row, tp1_child, tp2_row, tp2_child]
    _register_exchange_order(
        client,
        tp1_child,
        executed=str(tp1["quantity"]),
        quote=str(Decimal(str(tp1["quantity"])) * SL),
        fees=[
            {
                "id": "fee-tp1",
                "orderId": "C02__tp1-stop-filled",
                "price": str(SL),
                "qty": str(tp1["quantity"]),
                "quoteQty": str(Decimal(str(tp1["quantity"])) * SL),
                "commission": "0.001",
                "commissionAsset": "USDT",
            }
        ],
    )
    _advance(tm, store, 182)
    tm._reconcile_trade(trade["trade_id"])
    tm._refresh_buy_block()

    # Now — and only now — the incident is fully attributed and the block
    # clears with exchange evidence (no invented ids, no ID arithmetic).
    cleared = store.get_trade(trade["trade_id"])
    assert cleared["trade_state"] != "needs_reconciliation", cleared
    assert store.buy_block_reason() is None


def test_p0_no_new_tranche_while_a_parent_cancel_is_unattributed(cfg, repo):
    """`_place_oco_exits` must refuse a new tranche for unattributed cancels."""
    harness = make_env(cfg, repo)
    cfg, store, engine, client, notifier, tm = harness
    trade, _ = setup_trade(harness, fill=Decimal("0.06"))
    tp1 = _oco_orders(store, trade, "tp1")[0]
    tp2 = _oco_orders(store, trade, "tp2")[0]
    # Both OCO parents canceled with zero child evidence: unattributed.
    _cancel_parent(store, tp1)
    _cancel_parent(store, tp2)
    assert tm._unattributed_oco_parent_cancellations(store.get_trade(trade["trade_id"]))
    placement_count = len(client.place_calls)

    # A later entry fill tries to create the next tranche.
    store.add_fill(
        exchange_order_id=trade["entry_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(Decimal("100")),
            "qty": "0.02",
            "quoteQty": "2",
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 1,
            "isMaker": 0,
            "time": 2,
        },
    )
    tm._reconcile_trade(trade["trade_id"])

    # No new tranche may be submitted while the cancel is unattributed: the
    # reconcile parks the trade on the close refusal for the now-unproven
    # 0.08 position and audits both discovery failures.
    assert len(client.place_calls) == placement_count, "unattributed parent cancel must block new tranche submissions"
    refreshed = store.get_trade(trade["trade_id"])
    assert refreshed["trade_state"] == "needs_reconciliation", refreshed
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp1")) == 1
    assert len(store.list_trade_orders(trade["trade_id"], kind="tp2")) == 1
    close_refusals = store.db.conn.execute(
        "SELECT detail FROM audit_events WHERE event='needs_reconciliation' AND detail LIKE 'close refused:%'"
    ).fetchall()
    assert any("close refused: 0.08 base quantity remains unsold" in str(row["detail"]) for row in close_refusals)
    unresolved = store.db.conn.execute(
        "SELECT detail FROM audit_events WHERE event='oco_stop_child_unresolved'"
    ).fetchall()
    assert len(unresolved) >= 2
    assert all("oco_stop_child_tp_not_found" in str(row["detail"]) for row in unresolved)
    assert store.buy_block_reason() is not None
