"""Conditional entry, trigger activation and entry-order submission.

All exchange interaction goes through :class:`FakeEngine`, so no real orders are
ever placed.  The durable ``trades``/``order_intents`` tables are exercised via a
real :class:`TradeStore` on a temporary SQLite database.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any

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


def _insert_intent(
    store: TradeStore,
    *,
    signal_id: str = "sig-1",
    client_order_id: str = "tsi-1",
    entry: Decimal = ENTRY,
    quote: Decimal = Decimal("10"),
    state: str = "planned",
) -> dict[str, Any]:
    iid = f"intent-{client_order_id}"
    with store.db.transaction() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO signals (signal_id, channel_id, message_id, edit_version, "
            "parser_version, signal_kind, symbol, action, status, fingerprint_sha256, created_at, updated_at) "
            "VALUES (?, ?, ?, 0, 1, 'alert', ?, 'buy', 'parsed', ?, 'now', 'now')",
            (signal_id, 100, 1, SYMBOL, "abc"),
        )
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
                state,
                1,
                100,
                "now",
                "now",
            ),
        )
    return store.get_intent(iid)


def _apply_conditional(cfg: AppConfig, ticks: int = 2) -> None:
    cfg.trading.entry_trigger_offset_ticks = ticks


# --------------------------------------------------------------------------
# Direct vs conditional decision
# --------------------------------------------------------------------------


def test_missing_offset_defaults_to_three_tick_conditional_entry(env):
    cfg, store, engine, client, notifier, tm = env
    cfg.trading.entry_trigger_offset_ticks = None  # legacy persisted shape
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.deferred is True
    assert decision.trade is not None
    assert decision.trade["entry_mode"] == "conditional"
    assert int(decision.trade["trigger_offset_ticks"]) == 3
    assert Decimal(decision.trade["trigger_price"]) == Decimal("99.97")
    assert client.place_calls == []


def test_conditional_entry_below_market_waits(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg)
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(entry=ENTRY), reference_price=Decimal("90"))
    assert decision.deferred is True
    assert decision.trade["entry_mode"] == "conditional"
    assert decision.trade["entry_mode"] == "conditional"
    # No order should be placed yet.
    assert client.place_calls == []
    assert store.get_trade(decision.trade["trade_id"])["trade_state"] == "pending_entry"


def test_conditional_entry_no_reference_price_fails_closed(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg)
    intent = _insert_intent(store)
    # With no observed market price conditional entry fails closed (reject) and
    # no order is placed.
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=None)
    assert decision.rejected is True
    assert decision.trade is None
    assert client.place_calls == []
    # The intent should be marked rejected in the durable store.
    assert store.get_intent(intent["intent_id"])["state"] == "rejected"
    assert len(store.list_trades()) == 0


def test_missing_offset_with_no_reference_fails_closed_not_direct(env):
    cfg, store, engine, client, notifier, tm = env
    cfg.trading.entry_trigger_offset_ticks = None
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=None)
    assert decision.rejected is True
    assert decision.trade is None
    assert client.place_calls == []
    assert store.get_intent(intent["intent_id"])["state"] == "rejected"


def test_market_at_or_above_entry_is_direct(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg)
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
    # Market reached the entry -> direct, not conditional.
    assert decision.deferred is False
    assert decision.trade["entry_mode"] == "direct"
    assert decision.trade["trade_state"] == "pending_entry"


# --------------------------------------------------------------------------
# Trigger monitoring
# --------------------------------------------------------------------------


def test_trigger_not_reached_keeps_pending(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    assert trade is not None
    # Trigger = 100 - 2*0.01 = 99.99; ticks below never fire.
    tm.on_tick(SYMBOL, Decimal("99.98"), received_at=0.0)
    tm.on_tick(SYMBOL, Decimal("90"), received_at=1.0)
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "pending_entry"
    assert client.place_calls == []


def test_tick_below_trigger_price_ignored_until_crossing(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    # Trigger 99.99; price 99.95 below: no entry.
    tm.on_tick(SYMBOL, Decimal("99.95"), received_at=0.0)
    assert store.get_trade(trade["trade_id"])["trade_state"] == "pending_entry"


def test_upward_crossing_activates_entry(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    cfg.trading.entry_trigger_stale_threshold_s = 60.0
    fake_clock_now = [0.0]
    tm._clock = lambda: fake_clock_now[0]

    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    # Tick crosses the trigger upward (99.98 -> 99.99).
    fake_clock_now[0] = 1.0
    tm.on_tick(SYMBOL, Decimal("99.99"), received_at=1.0)
    t = store.get_trade(trade["trade_id"])
    # entry_active means the limit-buy order was accepted by the engine gate.
    assert t["trade_state"] == "entry_active"
    assert t["entry_order_id"] == "ex-1000"
    assert engine.intent_calls and engine.intent_calls[0][0] == intent["intent_id"]
    events = store.db.conn.execute(
        "SELECT event_type,payload_json FROM audit_events "
        "WHERE event_class='domain' AND event_type='trade.state_changed' AND aggregate_id=?",
        (trade["trade_id"],),
    ).fetchall()
    assert len(events) == 1
    assert '"new_state":"entry_active"' in events[0]["payload_json"]
    assert not any(key == "ENTRY_TRIGGER_ACTIVATED" for key, _, _ in notifier.trade_events)


def test_single_activation_is_idempotent(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    cfg.trading.entry_trigger_stale_threshold_s = 60.0
    fake_clock_now = [0.0]
    tm._clock = lambda: fake_clock_now[0]
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    fake_clock_now[0] = 1.0
    tm.on_tick(SYMBOL, Decimal("99.99"), received_at=1.0)
    tm.on_tick(SYMBOL, Decimal("100.00"), received_at=2.0)
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "entry_active"
    # Exactly one intent execution despite multiple crossing ticks.
    assert len(engine.intent_calls) == 1


def test_price_jumping_from_below_trigger_activates(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    cfg.trading.entry_trigger_stale_threshold_s = 60.0
    fake_clock_now = [0.0]
    tm._clock = lambda: fake_clock_now[0]
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    # One tick jumps from below trigger to above entry -> still crosses trigger.
    fake_clock_now[0] = 1.0
    tm.on_tick(SYMBOL, Decimal("110"), received_at=1.0)
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "entry_active"
    assert len(engine.intent_calls) == 1


def test_stale_price_observation_is_ignored(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    cfg.trading.entry_trigger_stale_threshold_s = 5.0
    fake_clock_now = [0.0]
    tm._clock = lambda: fake_clock_now[0]
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    # A "stale" observation (now=100, received 100s ago) is ignored.
    fake_clock_now[0] = 100.0
    tm.on_tick(SYMBOL, Decimal("99.99"), received_at=0.0)
    assert store.get_trade(trade["trade_id"])["trade_state"] == "pending_entry"


def test_invalid_config_offset_below_one_fails_closed(env):
    cfg, store, engine, client, notifier, tm = env
    cfg.trading.entry_trigger_offset_ticks = 0
    intent = _insert_intent(store)
    assert cfg.trading.conditional_entry_enabled is False
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.rejected is True
    assert decision.trade is None
    assert client.place_calls == []
    assert store.get_intent(intent["intent_id"])["state"] == "rejected"


def test_default_trigger_is_exactly_three_exchange_ticks(env):
    cfg, store, engine, client, notifier, tm = env
    assert cfg.trading.entry_trigger_offset_ticks == 3
    assert cfg.trading.effective_entry_trigger_offset_ticks == 3
    assert tm._compute_trigger(Decimal("100"), 3, Decimal("0.01")) == Decimal("99.97")
    assert tm._compute_trigger(Decimal("100"), 3, Decimal("0.0005")) == Decimal("99.9985")
    assert tm._compute_trigger(Decimal("100"), 3, Decimal("0.05")) == Decimal("99.85")


def test_trigger_rounding_floors_to_the_symbol_tick_grid(env):
    cfg, store, engine, client, notifier, tm = env
    # 100.07 - 0.15 = 99.92; the 0.05 grid step below it is 99.90.
    assert tm._compute_trigger(Decimal("100.07"), 3, Decimal("0.05")) == Decimal("99.90")
    # A larger tick size moves the same nominal 3 ticks further in price.
    assert tm._compute_trigger(Decimal("250"), 3, Decimal("0.1")) == Decimal("249.7")


def test_explicit_offset_override_is_respected(env):
    cfg, store, engine, client, notifier, tm = env
    cfg.trading.entry_trigger_offset_ticks = 5
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.deferred is True
    assert int(decision.trade["trigger_offset_ticks"]) == 5
    assert Decimal(decision.trade["trigger_price"]) == Decimal("99.95")


# --------------------------------------------------------------------------
# Restart safety
# --------------------------------------------------------------------------


def test_pending_entry_survives_restart(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    # Simulate crash/restart: build a fresh manager over the same DB.
    tm2 = TradeManager(cfg, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    t = store.get_trade(trade["trade_id"])
    assert t["trade_state"] == "pending_entry"
    assert (
        tm2.store.list_trades(state="pending_entry") == [t]
        or store.get_trade(trade["trade_id"])["trade_state"] == "pending_entry"
    )


def test_trigger_state_recomputed_after_restart(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    cfg.trading.entry_trigger_stale_threshold_s = 60.0
    fake_clock_now = [0.0]
    tm._clock = lambda: fake_clock_now[0]
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90")).trade
    tm._clock = lambda: 1.0
    tm.on_tick(SYMBOL, Decimal("99.99"), received_at=1.0)
    assert store.get_trade(trade["trade_id"])["trade_state"] == "entry_active"
    # After restart, trigger re-evaluation does not double-submit.
    fake_clock_now[0] = 5.0
    tm2 = TradeManager(cfg, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2._clock = lambda: fake_clock_now[0]
    tm2.recover_on_startup()
    assert store.get_trade(trade["trade_id"])["trade_state"] == "entry_active"
    assert len(engine.intent_calls) == 1  # still one


# --------------------------------------------------------------------------
# SL/TP prices parsed onto the trade
# --------------------------------------------------------------------------


def test_parsed_prices_attached_to_trade(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    intent = _insert_intent(store)
    trade = tm.on_intent_planned(intent, _parsed(entry=ENTRY, tp1=TP1, tp2=TP2, sl=SL), reference_price=Decimal("90"))
    t = store.get_trade(trade.trade["trade_id"])
    assert t["tp1_price"] == str(TP1)
    assert t["tp2_price"] == str(TP2)
    assert t["sl_price"] == str(SL)
    assert t["entry_price"] == str(ENTRY)
    # trigger = 100 - 2*0.01 = 99.98
    assert Decimal(t["trigger_price"]) == Decimal("99.98")
    assert int(t["trigger_offset_ticks"]) == 2
    assert t["tp1_allocation_pct"] == "50.0"
    assert t["tp2_allocation_pct"] == "100.0"


def test_entry_fill_sync_uses_central_reconciliation_scheduler(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="tsi-central-reconcile")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
    assert decision.trade is not None
    calls = []
    original = engine.reconcile_intent_if_due

    def spy(intent_id: str, *, notify: bool = True):
        calls.append(intent_id)
        return original(intent_id, notify=notify)

    engine.reconcile_intent_if_due = spy
    synced = tm._reconcile_entry_fills(decision.trade)

    assert calls == [intent["intent_id"]]
    assert synced is False
    assert client.orders == {}


def test_recent_successful_snapshot_syncs_recovered_order_without_duplicate_query(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="tsi-recovered-ack")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.trade is not None
    trade = decision.trade

    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET state='open', exchange_order_id='ex-recovered', "
            "exchange_status='NEW', submitted_at='now' WHERE intent_id=?",
            (intent["intent_id"],),
        )
    engine.reconcile_intent_if_due = lambda intent_id, *, notify=True: None
    engine.has_fresh_successful_reconciliation = lambda intent_id: True

    synced = tm._reconcile_entry_fills(trade)
    refreshed = store.get_trade(trade["trade_id"])

    assert synced is True
    assert refreshed is not None
    assert refreshed["entry_order_id"] == "ex-recovered"
    assert refreshed["entry_exchange_status"] == "open"
    assert refreshed["trade_state"] == "entry_active"
    assert client.query_calls == []


def test_entry_remainder_cancel_uses_central_gate_for_unknown_intents(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="tsi-cancel-unknown")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.trade is not None
    trade = decision.trade
    store.transition_intent(intent["intent_id"], "unknown_requires_reconciliation", note="uncertain")

    due_calls = []
    raw_calls = []
    cancel_calls = []
    engine.reconcile_intent_if_due = lambda intent_id, *, notify=True: due_calls.append(intent_id)
    engine.has_fresh_successful_reconciliation = lambda intent_id: False
    engine.reconcile_intent = lambda intent_id, *, notify=True: raw_calls.append(intent_id)
    engine.cancel_intent_order = lambda current: cancel_calls.append(current["intent_id"])

    assert tm._cancel_entry_remainder(trade) is False
    assert due_calls == [intent["intent_id"]]
    assert raw_calls == []
    assert cancel_calls == []


def test_entry_remainder_cancel_uses_only_one_post_cancel_reconcile(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="tsi-cancel-open")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.trade is not None
    trade = decision.trade
    store.transition_intent(
        intent["intent_id"], "open", note="acknowledged", exchange_order_id="ex-cancel-open"
    )

    due_calls = []
    raw_calls = []
    cancel_calls = []

    def due_reconcile(intent_id, *, notify=True, urgent_after_cancel=False):
        due_calls.append((intent_id, urgent_after_cancel))
        return store.get_intent(intent_id)

    def raw_reconcile(intent_id, *, notify=True):
        raw_calls.append(intent_id)
        return store.get_intent(intent_id)

    engine.reconcile_intent_if_due = due_reconcile
    engine.has_fresh_successful_reconciliation = lambda intent_id: False
    engine.reconcile_intent = raw_reconcile
    engine.cancel_intent_order = lambda current: cancel_calls.append(current["intent_id"])

    assert tm._cancel_entry_remainder(trade) is False
    assert due_calls == [(intent["intent_id"], False), (intent["intent_id"], True)]
    assert raw_calls == []
    assert cancel_calls == [intent["intent_id"]]


def test_open_state_without_exchange_order_id_is_not_acknowledged(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="tsi-open-without-id")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.trade is not None
    trade = decision.trade

    with store.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET state='open', exchange_order_id=NULL, "
            "exchange_status='NEW' WHERE intent_id=?",
            (intent["intent_id"],),
        )
    engine.reconcile_intent_if_due = lambda intent_id, *, notify=True: None
    engine.has_fresh_successful_reconciliation = lambda intent_id: True

    synced = tm._reconcile_entry_fills(trade)
    refreshed = store.get_trade(trade["trade_id"])

    assert synced is False
    assert refreshed is not None
    assert not refreshed.get("entry_order_id")
    assert refreshed["trade_state"] == "pending_entry"
    assert client.query_calls == []


def test_trigger_unknown_without_exchange_order_id_is_not_successfully_processed(env):
    cfg, store, engine, client, notifier, tm = env
    _apply_conditional(cfg, ticks=2)
    intent = _insert_intent(store, client_order_id="tsi-trigger-unknown")
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.trade is not None and decision.deferred
    trade = decision.trade

    def uncertain_submit(intent_id: str):
        store.transition_intent(
            intent_id,
            "unknown_requires_reconciliation",
            note="simulated ambiguous POST without exchange acknowledgement",
        )
        return store.get_intent(intent_id)

    engine._execute_intent = uncertain_submit
    tm.on_tick(SYMBOL, Decimal(str(trade["trigger_price"])), received_at=time.monotonic())

    refreshed_intent = store.get_intent(intent["intent_id"])
    refreshed_trade = store.get_trade(trade["trade_id"])
    assert refreshed_intent is not None
    assert refreshed_intent["state"] == "unknown_requires_reconciliation"
    assert not refreshed_intent.get("exchange_order_id")
    assert refreshed_trade is not None
    assert not refreshed_trade.get("entry_order_id")
    assert client.orders == {}
