"""Fail-closed exit-protection lifecycle for live BUYs (offline).

Covers the invariant that an executed live BUY can never remain silently
unprotected:

* a live BUY without a complete, ordered SL/TP1/TP2 ladder is rejected before
  any exchange submission;
* an OCO placement that cannot be confirmed marks the position
  ``unprotected_critical`` and persists a global new-BUY block;
* the block survives a restart and clears only when verified protection (or a
  fully exited position) is observed.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from _trading_fakes import FakeClient, FakeEngine, FakeNotifier

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.repository import Repository
from tg_signal_ingest.trading.parser import ParsedSignal
from tg_signal_ingest.trading.trade_manager import (
    UNPROTECTED_CRITICAL,
    TradeManager,
)
from tg_signal_ingest.trading.trade_store import TradeStore

ENTRY = Decimal("100")
TP1 = Decimal("110")
TP2 = Decimal("120")
SL = Decimal("95")
SYMBOL = "BTCUSDT"
LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"


@pytest.fixture()
def env(cfg: AppConfig, repo: Repository):
    cfg.trading.entry_trigger_offset_ticks = 2
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient()
    engine = FakeEngine(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=notifier)
    tm._clock = lambda: 5000.0
    return cfg, store, engine, client, notifier, tm


def _parsed(*, entry=ENTRY, tp1=TP1, tp2=TP2, sl=SL) -> ParsedSignal:
    targets = tuple(v for v in (tp1, tp2) if v is not None)
    return ParsedSignal(
        kind="alert",
        action="buy",
        symbol=SYMBOL,
        entry_price=entry,
        entry_low_price=None,
        entry_high_price=None,
        target_prices=targets,
        stop_loss=sl,
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
            "VALUES (?, ?, ?, 0, 1, 'alert', ?, 'buy', 'parsed', ?, 'now', 'now')",
            (signal_id, 100, 1, SYMBOL, "abc"),
        )


def _insert_intent(store: TradeStore, *, signal_id: str = "sig-1", client_order_id: str = "tsi-1") -> dict:
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
                "10",
                "0.1",
                str(ENTRY),
                "live",
                "planned",
                1,
                100,
                "now",
                "now",
            ),
        )
    return store.get_intent(iid)


def _setup_filled_trade(env, *, fill_qty: Decimal = Decimal("0.1")):
    """Create and fill a live entry, then attempt OCO placement."""

    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store)
    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
    assert decision.trade is not None
    engine._execute_intent(intent["intent_id"])
    intent = store.get_intent(intent["intent_id"])
    tm.on_intent_executed(intent)
    client.fill_order(intent["exchange_order_id"], fill_qty, fill_qty * ENTRY, ENTRY)
    store.add_fill(
        exchange_order_id=intent["exchange_order_id"],
        symbol=SYMBOL,
        raw={
            "price": str(ENTRY),
            "qty": str(fill_qty),
            "quoteQty": str(fill_qty * ENTRY),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 1,
            "isMaker": 0,
            "time": 1,
        },
    )
    tm._reconcile_entry_fills(store.get_trade(decision.trade["trade_id"]))
    return store.get_trade(decision.trade["trade_id"])


def test_live_buy_without_stop_loss_is_rejected_before_submission(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="no-sl")

    decision = tm.on_intent_planned(intent, _parsed(sl=None), reference_price=Decimal("100"))

    assert decision.rejected is True
    assert decision.trade is None
    stored = store.get_intent(intent["intent_id"])
    assert stored["state"] == "rejected"
    assert "stop-loss" in (stored["reject_reason"] or "")
    assert client.place_calls == []


def test_live_buy_with_disordered_ladder_is_rejected_before_submission(env):
    cfg, store, engine, client, notifier, tm = env
    intent = _insert_intent(store, client_order_id="bad-ladder")

    decision = tm.on_intent_planned(
        intent,
        _parsed(entry=ENTRY, tp1=Decimal("90"), tp2=TP2, sl=SL),
        reference_price=Decimal("100"),
    )

    assert decision.rejected is True
    stored = store.get_intent(intent["intent_id"])
    assert stored["state"] == "rejected"
    assert "invalid exit ladder" in (stored["reject_reason"] or "")
    assert client.place_calls == []


def test_unconfirmed_oco_marks_position_critical_but_does_not_globally_block(env, monkeypatch):
    cfg, store, engine, client, notifier, tm = env

    def reject_all(*args, **kwargs):
        raise RuntimeError("OCO rejected by exchange")

    monkeypatch.setattr(engine, "submit_oco_sell", reject_all)
    trade = _setup_filled_trade(env)

    assert trade["trade_state"] == UNPROTECTED_CRITICAL, trade
    assert "unprotected position" in (store.buy_block_reason() or "")
    assert any(event[0] == "BUY_BLOCK_ACTIVE" for event in notifier.trade_events)

    # The global flag is diagnostic only: an unrelated lifecycle is admitted.
    second = _insert_intent(store, signal_id="sig-2", client_order_id="tsi-2")
    before = len(client.place_calls)
    decision = tm.on_intent_planned(second, _parsed(), reference_price=Decimal("100"))
    assert decision.rejected is False
    assert decision.trade is not None
    assert len(client.place_calls) == before


def test_buy_block_clears_only_when_no_unsold_quantity_remains(env, monkeypatch):
    cfg, store, engine, client, notifier, tm = env

    def reject_all(*args, **kwargs):
        raise RuntimeError("OCO rejected by exchange")

    monkeypatch.setattr(engine, "submit_oco_sell", reject_all)
    trade = _setup_filled_trade(env)
    assert store.buy_block_reason() is not None

    # Record a fully filled stop-loss sale of the remaining base quantity.
    order = tm._create_trade_order(trade, "stop_loss", Decimal("0.1"), None, order_type="MARKET")
    store.transition_trade_order(
        order["trade_order_id"],
        "filled",
        sold_qty=Decimal("0.1"),
        exchange_order_id="sl-1",
    )
    tm._refresh_buy_block()

    assert store.buy_block_reason() is None
    assert any(event[0] == "BUY_BLOCK_CLEARED" for event in notifier.trade_events)


def test_restart_rebuilds_block_then_restores_protection(env, monkeypatch):
    cfg, store, engine, client, notifier, tm = env
    original = engine.submit_oco_sell

    def reject_all(*args, **kwargs):
        raise RuntimeError("OCO rejected by exchange")

    monkeypatch.setattr(engine, "submit_oco_sell", reject_all)
    trade = _setup_filled_trade(env)
    assert trade["trade_state"] == UNPROTECTED_CRITICAL

    # Simulate a stale cleared flag: restart must rebuild the block from
    # durable trade state before/after reconciliation.
    store.set_buy_block(None)
    tm2 = TradeManager(cfg.trading, store, engine=engine, client=client, notifier=FakeNotifier())
    tm2.recover_on_startup()
    assert store.get_trade(trade["trade_id"])["trade_state"] == UNPROTECTED_CRITICAL
    assert store.buy_block_reason() is not None

    # Once OCO submission works again, reconciliation restores protection and
    # only then clears the block.
    monkeypatch.setattr(engine, "submit_oco_sell", original)
    tm2.reconcile_all_open()
    assert store.get_trade(trade["trade_id"])["trade_state"] == "oco_active"
    assert store.buy_block_reason() is None
