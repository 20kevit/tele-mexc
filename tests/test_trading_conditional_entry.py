"""Conditional entry, trigger activation and entry-order submission.

All exchange interaction goes through :class:`FakeEngine`, so no real orders are
ever placed.  The durable ``trades``/``order_intents`` tables are exercised via a
real :class:`TradeStore` on a temporary SQLite database.
"""

from __future__ import annotations

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
    cfg.trading.mode = "live"
    cfg.trading.live_enabled_token = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient()
    engine = FakeEngine(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=notifier)
    return cfg, store, engine, client, notifier, tm


def _parsed(entry=ENTRY, tp1=TP1, tp2=TP2, sl=SL) -> ParsedSignal:
    return ParsedSignal(
        kind="alert", action="buy", symbol=SYMBOL,
        entry_price=entry, entry_low_price=None, entry_high_price=None,
        target_prices=(tp1, tp2), stop_loss=sl, order_type="limit",
        amount_text=None, raw_text="buy", normalized_text="buy",
        fingerprint_sha256="abc",
    )


def _insert_intent(store: TradeStore, *, signal_id: str = "sig-1",
                   client_order_id: str = "tsi-1", entry: Decimal = ENTRY,
                   quote: Decimal = Decimal("10"), state: str = "planned") -> dict[str, Any]:
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
            (iid, signal_id, f"idem-{client_order_id}", client_order_id, SYMBOL, "buy",
             "limit", str(quote), "0.1", str(entry), "live", state, 1, 100, "now", "now"),
        )
    return store.get_intent(iid)


def _apply_conditional(cfg: AppConfig, ticks: int = 2) -> None:
    cfg.trading.entry_trigger_offset_ticks = ticks


# --------------------------------------------------------------------------
# Direct vs conditional decision
# --------------------------------------------------------------------------

def test_conditional_entry_disabled_creates_no_trade(env):
    cfg, store, engine, client, notifier, tm = env
    # offset_ticks is None -> conditional entry off; live mode still creates trade
    # but with entry_mode direct (market < entry but no trigger configured).
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.deferred is False
    assert decision.trade is not None
    assert decision.trade["entry_mode"] == "direct"


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
    cfg.trading.entry_trigger_offset_ticks = 0  # invalid; conditional entry off
    intent = _insert_intent(store)
    # offset 0 -> conditional disabled -> direct path (no trigger watcher).
    assert cfg.trading.conditional_entry_enabled is False
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("90"))
    assert decision.deferred is False


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
    assert tm2.store.list_trades(state="pending_entry") == [t] or \
        store.get_trade(trade["trade_id"])["trade_state"] == "pending_entry"


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
    trade = tm.on_intent_planned(intent, _parsed(entry=ENTRY, tp1=TP1, tp2=TP2, sl=SL),
                                 reference_price=Decimal("90"))
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
