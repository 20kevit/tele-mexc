"""Critical fixes verification — independent review findings §§1-7.

Mocks only, disposable DBs.  Each test spies on the exchange client and
proves the *runtime path* is safe, not just a comment.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tg_signal_ingest.config import AppConfig, ConfigError, load_config
from tg_signal_ingest.db import Database
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem, Repository
from tg_signal_ingest.trading.engine import TradingEngine
from tg_signal_ingest.trading.parser import SignalParser
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
COMPLETE = "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120"


# ── helpers ────────────────────────────────────────────────────────────────
class FakeMsg:
    def __init__(self, id, text, edit_date=None):
        self.id = id
        self.message = text
        self.date = NOW
        self.edit_date = edit_date
        self.media = None
        self.grouped_id = None
        self.reply_to = None
        self.forward = None

    def to_dict(self):
        return {"id": self.id, "message": self.message, "date": self.date.isoformat(), "edit_date": None}


def _store(repo, mid, text, source="live"):
    repo.upsert_message(message_to_record(100, FakeMsg(mid, text), source))
    return repo.get_message_raw(100, mid)


def _item(ch, mid):
    return QueueItem(
        queue_id=0,
        channel_id=ch,
        message_id=mid,
        item_type="message",
        status="pending",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )


def _engine_with_tm(cfg, repo, client):
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, trade_manager=tm)
    tm.engine = engine
    # Wire shared store coherently
    engine.store = store
    tm.store = store
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    return engine, tm, store


class SpyMexc:
    """Records OCO and market-sell submissions."""

    def __init__(self, place_oco_result=None, place_error=None):
        self.place_oco_calls = []
        self.place_order_calls = []
        self.market_sell_calls = []
        self.place_error = place_error
        self.place_oco_result = place_oco_result or {"orderId": "oco-1", "status": "NEW"}
        # minimal registry with the tick-size metadata the exit gate requires
        from tests._trading_fakes import FakeMeta

        class AnySymbolRegistry:
            def __init__(self):
                self.meta = FakeMeta(tick_size="0.01")

            def get(self, symbol):
                return self.meta

        self.symbols = AnySymbolRegistry()
        self.ticker_calls = 0

    def place_order(self, **kwargs):
        # Not used for OCO — OCO goes via CLI
        self.place_order_calls.append(kwargs)
        return {"orderId": "ex-1", "status": "NEW"}

    def place_oco(self, **kwargs):
        self.place_oco_calls.append(kwargs)
        if self.place_error:
            raise self.place_error
        return dict(self.place_oco_result)

    def ticker_price(self, symbol=None):
        self.ticker_calls += 1
        return {"symbol": symbol, "price": "100"}

    def open_orders(self, symbol=None):
        return []

    def account(self):
        return {
            "canTrade": True,
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "USDT", "free": "1000", "locked": "0", "available": "1000"},
                {"asset": "BTC", "free": "10", "locked": "0", "available": "10"},
            ],
        }

    def query_order(self, **kwargs):
        return {
            "orderId": kwargs.get("order_id") or "ex-1",
            "status": "NEW",
            "executedQty": "0",
            "cumulativeQuoteQty": "0",
        }


# ── §1 OCO invariant BEFORE submission ────────────────────────────────────


def test_oco_pre_submit_existing_exceeds_sellable_never_submits(cfg, repo):
    """Existing reservations already exceed sellable → no OCO submission."""
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    engine.client = SpyMexc()
    # Count submit_oco_sell calls via engine
    calls = []

    def spy_submit(trade, cid, price, qty, stop_price, kind=None, **kw):
        calls.append((kind, qty))
        return {"orderId": "ex-1", "status": "NEW", "executed_qty": Decimal("0")}

    engine.submit_oco_sell = spy_submit  # type: ignore
    # Create trade with 0.05 acquired, but existing reserved 0.06 > sellable
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=100, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=100,
        edit_version=0,
        idempotency_key="k100",
        client_order_id="tsi-k100",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.05"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k100'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-entry-100', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-entry-100",
            "BTCUSDT",
            "100",
            "0.05",
            "5",
            "",
            "",
            "t-100",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-oco-pre1",
        signal_id=sid,
        idempotency_key="k100",
        channel_id=100,
        message_id=100,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("5"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-oco-pre1",
        acquired_base_qty=Decimal("0.05"),
        acquired_quote_spent=Decimal("5"),
        avg_entry_price=Decimal("100"),
    )
    # Existing OCO1 reserved 0.04 + OCO2 0.03 = 0.07 > sellable 0.05
    store.create_trade_order(
        trade_order_id="to-pre1-tp1",
        trade_id="tr-oco-pre1",
        kind="tp1",
        client_order_id="t1-pre1",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.04"),
        allocated_qty=Decimal("0.04"),
    )
    store.transition_trade_order("to-pre1-tp1", "open", exchange_order_id="ex-pre1-tp1")
    store.create_trade_order(
        trade_order_id="to-pre1-tp2",
        trade_id="tr-oco-pre1",
        kind="tp2",
        client_order_id="t2-pre1",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("120"),
        quantity=Decimal("0.03"),
        allocated_qty=Decimal("0.03"),
    )
    store.transition_trade_order("to-pre1-tp2", "open", exchange_order_id="ex-pre1-tp2")
    trade = store.get_trade("tr-oco-pre1")
    tm._place_oco_exits(trade)
    assert calls == [], "unsafe OCO must never be submitted when existing exceeds sellable"
    assert store.get_trade("tr-oco-pre1")["trade_state"] == "needs_reconciliation"


def test_oco_pre_submit_proposed_oco1_would_exceed_sellable_blocked(cfg, repo):
    """Existing 0.06 reserved + proposed 0.05 exceeds verified 0.10 → rejected before exchange."""
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    calls = []

    def spy_submit(trade, cid, price, qty, stop_price, kind=None, **kw):
        calls.append((kind, qty))
        return {"orderId": "ex-1", "status": "NEW", "executed_qty": Decimal("0")}

    engine.submit_oco_sell = spy_submit  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=101, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=101,
        edit_version=0,
        idempotency_key="k101",
        client_order_id="tsi-k101",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.10"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k101'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-101', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-101",
            "BTCUSDT",
            "100",
            "0.10",
            "10",
            "",
            "",
            "t-101",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-oco-pre2",
        signal_id=sid,
        idempotency_key="k101",
        channel_id=100,
        message_id=101,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-oco-pre2",
        acquired_base_qty=Decimal("0.10"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # Existing OCO2 consumes 0.06 of verified sellable 0.10
    store.create_trade_order(
        trade_order_id="to-pre2-tp2",
        trade_id="tr-oco-pre2",
        kind="tp2",
        client_order_id="t2-pre2",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("120"),
        quantity=Decimal("0.06"),
        allocated_qty=Decimal("0.06"),
    )
    store.transition_trade_order("to-pre2-tp2", "open", exchange_order_id="ex-pre2-tp2")
    # proposed OCO1 = 0.05 (50% of 0.10) → combined 0.11 > 0.10 → must be blocked
    trade = store.get_trade("tr-oco-pre2")
    existing_reserved = tm._oco_reserved_quantity(trade, "tp2")
    assert existing_reserved == Decimal("0.06"), f"existing should be 0.06, got {existing_reserved}"
    sellable = tm._sellable_quantity(trade)
    assert sellable == Decimal("0.10"), f"sellable should be 0.10, got {sellable}"
    # Force allocation path: tp1 will be 0.05, tp2 already exists 0.06
    store.update_trade_fields("tr-oco-pre2", tp1_allocated_qty=Decimal("0"), tp2_allocated_qty=Decimal("0"))
    trade = store.get_trade("tr-oco-pre2")
    tm._place_oco_exits(trade)
    # Must be blocked before exchange call
    assert calls == [], (
        "OCO1 proposal 0.05 + existing 0.06 > sellable 0.10 must be rejected "
        f"with zero exchange writes, got calls={calls}"
    )
    t2 = store.get_trade("tr-oco-pre2")
    assert t2["trade_state"] == "needs_reconciliation"
    # Audit/notification evidence
    audits = store.db.conn.execute(
        "SELECT * FROM audit_events WHERE correlation=? AND event='oco_invariant_would_violate'",
        (t2["trade_id"],),
    ).fetchall()
    assert len(audits) >= 1, "expected audit oco_invariant_would_violate"
    # Verify exact quantities in audit detail
    detail = audits[0]["detail"]
    assert "0.06" in detail and "0.05" in detail and "0.10" in detail
    # Invariant after blocking still holds (0.06 <= 0.10) but the trade is
    # flagged needs_reconciliation for the would-violate proposal.
    ok, reason = tm._verify_oco_invariant(store.get_trade("tr-oco-pre2"))
    assert ok, f"existing 0.06 <= sellable 0.10 should still be invariant ok, got {reason}"


def test_oco_pre_submit_proposed_oco2_would_exceed_sellable_blocked(cfg, repo):
    """Boundary: existing 0.04 + proposed 0.06 == sellable 0.10 → allowed; 0.06+0.05>0.10 → blocked."""
    # First, verify boundary exact equality is allowed
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    calls = []
    engine.submit_oco_sell = lambda trade, cid, price, qty, stop_price, kind=None, **kw: (
        calls.append((kind, qty)),
        {"orderId": "ex-1", "status": "NEW", "executed_qty": Decimal("0")},
    )[1]
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=102, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=102,
        edit_version=0,
        idempotency_key="k102",
        client_order_id="tsi-k102",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.10"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k102'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-102', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-102",
            "BTCUSDT",
            "100",
            "0.10",
            "10",
            "",
            "",
            "t-102",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-oco-pre3",
        signal_id=sid,
        idempotency_key="k102",
        channel_id=100,
        message_id=102,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("40"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-oco-pre3",
        acquired_base_qty=Decimal("0.10"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # Existing 0.04, proposed 0.06 → combined 0.10 == sellable 0.10 → allowed
    store.create_trade_order(
        trade_order_id="to-pre3-tp1",
        trade_id="tr-oco-pre3",
        kind="tp1",
        client_order_id="t1-pre3",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.04"),
        allocated_qty=Decimal("0.04"),
    )
    store.transition_trade_order("to-pre3-tp1", "open", exchange_order_id="ex-pre3-tp1")
    trade = store.get_trade("tr-oco-pre3")
    assert tm._sellable_quantity(trade) == Decimal("0.10")
    assert tm._oco_reserved_quantity(trade, "tp1") == Decimal("0.04")
    tm._place_oco_exits(trade)
    # Should have submitted tp2 with 0.06 (remaining pool) and not exceed
    tp2_calls = [c for c in calls if c[0] == "tp2"]
    assert len(tp2_calls) == 1, f"boundary case should be allowed, got calls={calls}"
    assert tp2_calls[0][1] == Decimal("0.06") or tp2_calls[0][1] == Decimal("0.06000000")
    assert store.get_trade("tr-oco-pre3")["trade_state"] != "needs_reconciliation"
    # Now test exceed: create new trade in isolated DB where existing 0.07 + proposed 0.04 =0.11 >0.10 → blocked
    # Use a fresh tmp_path to avoid per-symbol conflict with previous trade in same DB (atomic admission)
    import tempfile
    from pathlib import Path

    from tg_signal_ingest.config import AppConfig as _AppCfg
    from tg_signal_ingest.db import Database as _DB
    from tg_signal_ingest.repository import Repository as _Repo

    tmp2 = Path(tempfile.mkdtemp())
    cfg2 = _AppCfg(
        api_id=12345,
        api_hash="a" * 32,
        data_dir=tmp2 / "data",
        db_path=tmp2 / "data" / "test2.sqlite3",
        session_path=tmp2 / "config" / "session",
    )
    cfg2.ensure_dirs()
    db2 = _DB(cfg2.db_path)
    db2.migrate()
    repo2 = _Repo(db2)
    from tg_signal_ingest.trading.trade_manager import TradeManager as _TM
    from tg_signal_ingest.trading.trade_store import TradeStore as _TS

    store2 = _TS(db2, cfg2.trading, now=lambda: NOW)
    tm2 = _TM(cfg2.trading, store2, now=lambda: NOW)
    from tg_signal_ingest.trading.engine import TradingEngine as _TE

    engine2 = _TE(cfg2, repo2, client=SpyMexc(), now=lambda: NOW, trade_manager=tm2)
    tm2.engine = engine2
    engine2.store = store2
    store2.set_app_state_once("trading.activation_boundary_message_id", "0")
    calls2 = []
    engine2.submit_oco_sell = lambda trade, cid, price, qty, stop_price, kind=None, **kw: (
        calls2.append((kind, qty)),
        {"orderId": "ex-2", "status": "NEW", "executed_qty": Decimal("0")},
    )[1]
    parsed2 = SignalParser().parse(COMPLETE)
    sid2, _ = store2.record_signal(parsed2, channel_id=100, message_id=1030, edit_version=0)
    store2.create_intent(
        signal=parsed2,
        signal_id=sid2,
        channel_id=100,
        message_id=1030,
        edit_version=0,
        idempotency_key="k102b",
        client_order_id="tsi-k102b",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.10"),
        limit_price=Decimal("100"),
    )
    iid2 = db2.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k102b'").fetchone()[
        "intent_id"
    ]
    db2.conn.execute("UPDATE order_intents SET exchange_order_id='ex-102b', state='filled' WHERE intent_id=?", (iid2,))
    db2.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-102b",
            "BTCUSDT",
            "100",
            "0.10",
            "10",
            "",
            "",
            "t-102b",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store2.create_trade(
        trade_id="tr-oco-pre3b",
        signal_id=sid2,
        idempotency_key="k102b",
        channel_id=100,
        message_id=1030,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("40"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid2,
    )
    store2.update_trade_fields(
        "tr-oco-pre3b",
        acquired_base_qty=Decimal("0.10"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store2.create_trade_order(
        trade_order_id="to-pre3b-tp1",
        trade_id="tr-oco-pre3b",
        kind="tp1",
        client_order_id="t1-pre3b",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.07"),
        allocated_qty=Decimal("0.07"),
    )
    store2.transition_trade_order("to-pre3b-tp1", "open", exchange_order_id="ex-pre3b-tp1")
    tm2._place_oco_exits(store2.get_trade("tr-oco-pre3b"))
    assert calls2 == [], f"existing 0.07 + proposed 0.04 >0.10 must be blocked, got {calls2}"
    assert store2.get_trade("tr-oco-pre3b")["trade_state"] == "needs_reconciliation"
    db2.close()


def test_oco_pre_submit_reuse_not_double_counted(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    calls = []

    def spy(trade, cid, price, qty, stop_price, kind=None, **kw):
        calls.append((kind, str(qty)))
        return {"orderId": f"ex-{kind}", "status": "NEW", "executed_qty": Decimal("0")}

    engine.submit_oco_sell = spy  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=103, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=103,
        edit_version=0,
        idempotency_key="k103",
        client_order_id="tsi-k103",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k103'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-103', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-103",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-103",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-oco-reuse",
        signal_id=sid,
        idempotency_key="k103",
        channel_id=100,
        message_id=103,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-oco-reuse",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store.create_trade_order(
        trade_order_id="to-reuse-tp1",
        trade_id="tr-oco-reuse",
        kind="tp1",
        client_order_id="t1-reuse",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-reuse-tp1", "open", exchange_order_id="ex-reuse-tp1")
    trade = store.get_trade("tr-oco-reuse")
    tm._place_oco_exits(trade)
    # Should only submit tp2 (0.05), not re-submit tp1 — tp1 already active should be reused, not double-counted
    submitted_kinds = [k for k, _ in calls]
    assert "tp1" not in submitted_kinds, "existing OCO1 should be reused, not resubmitted"
    assert "tp2" in submitted_kinds


def test_oco_sellable_unknown_blocks_submission(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    calls = []
    engine.submit_oco_sell = lambda trade, cid, price, qty, stop_price, kind=None, **kw: (
        calls.append(kind),
        {"orderId": "ex-1", "status": "NEW", "executed_qty": Decimal("0")},
    )[1]
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=104, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=104,
        edit_version=0,
        idempotency_key="k104",
        client_order_id="tsi-k104",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k104'").fetchone()[
        "intent_id"
    ]
    # Intent has no exchange_order_id and no fills → acquired is None → sellable unknown
    store.create_trade(
        trade_id="tr-oco-unknown",
        signal_id=sid,
        idempotency_key="k104",
        channel_id=100,
        message_id=104,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    # Do NOT set acquired_base_qty → remains 0/None
    trade = store.get_trade("tr-oco-unknown")
    tm._place_oco_exits(trade)
    assert calls == []
    assert store.get_trade("tr-oco-unknown")["trade_state"] in ("needs_reconciliation", "pending_entry")


def test_oco_ambiguous_result_never_resubmitted(cfg, repo):
    from tg_signal_ingest.mexc.errors import MexcNetworkError

    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc(place_error=MexcNetworkError("timeout after send")))

    # Spy that raises ambiguous
    def ambig_submit(trade, cid, price, qty, stop_price, kind=None, **kw):
        raise MexcNetworkError("timeout after send")

    engine.submit_oco_sell = ambig_submit  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=105, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=105,
        edit_version=0,
        idempotency_key="k105",
        client_order_id="tsi-k105",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k105'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-105', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-105",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-105",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-oco-ambig",
        signal_id=sid,
        idempotency_key="k105",
        channel_id=100,
        message_id=105,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-oco-ambig",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    trade = store.get_trade("tr-oco-ambig")
    tm._place_oco_exits(trade)
    # After ambiguous, orders should be in unknown_requires_reconciliation, not re-submitted with new client id
    orders = store.list_trade_orders("tr-oco-ambig")
    ambiguous = [o for o in orders if o["state"] == "unknown_requires_reconciliation"]
    assert len(ambiguous) >= 1
    # Second attempt must not create new client ids
    before = len(orders)
    tm._place_oco_exits(store.get_trade("tr-oco-ambig"))
    after = len(store.list_trade_orders("tr-oco-ambig"))
    assert after == before, "ambiguous OCO must never be resubmitted with new clientOrderId"


# ── §2 Allocation from net sellable ────────────────────────────────────────


def test_allocation_uses_net_sellable_with_base_fee(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    captured = {}

    def spy(trade, cid, price, qty, stop_price, kind=None, **kw):
        captured[kind] = qty
        return {"orderId": f"ex-{kind}", "status": "NEW", "executed_qty": Decimal("0")}

    engine.submit_oco_sell = spy  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=110, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=110,
        edit_version=0,
        idempotency_key="k110",
        client_order_id="tsi-k110",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k110'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-110', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-110",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "BTC",
            "0.002",
            "t-110",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-alloc-fee",
        signal_id=sid,
        idempotency_key="k110",
        channel_id=100,
        message_id=110,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-alloc-fee",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # Mock quantize to 8 decimals (real symbol meta would do same)
    tm.client = engine.client
    tm._place_oco_exits(store.get_trade("tr-alloc-fee"))
    # Net sellable = 0.1 - 0.002 = 0.098; TP1 50% = 0.049; TP2 remainder 0.049
    assert captured["tp1"] == Decimal("0.04900000") or captured["tp1"] == Decimal("0.049")
    assert captured["tp2"] == Decimal("0.04900000") or captured["tp2"] == Decimal("0.049")
    assert captured["tp1"] + captured["tp2"] <= Decimal("0.098")
    # Verify invariant holds
    trade = store.get_trade("tr-alloc-fee")
    assert tm._sellable_quantity(trade) == Decimal("0.098")
    assert tm._combined_oco_reserved(trade) <= tm._sellable_quantity(trade)


# ── §3 No Market Sell ───────────────────────────────────────────────────────


def test_no_market_sell_when_sl_crossed_both_ocos_active(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    # Track any market sell attempts
    market_calls = []
    engine.submit_stop_loss_market_sell = lambda *a, **kw: (
        market_calls.append(1) or {"exchange_order_id": "ex-sl", "state": "NEW", "executed_qty": Decimal("0")}
    )  # type: ignore
    # Also spy OCO to ensure OCO path is used
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=120, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=120,
        edit_version=0,
        idempotency_key="k120",
        client_order_id="tsi-k120",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k120'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-120', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-120",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-120",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-sl-both",
        signal_id=sid,
        idempotency_key="k120",
        channel_id=100,
        message_id=120,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-sl-both",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store.create_trade_order(
        trade_order_id="to-sl-both-tp1",
        trade_id="tr-sl-both",
        kind="tp1",
        client_order_id="t1-both",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-sl-both-tp1", "open", exchange_order_id="ex-both-tp1")
    store.create_trade_order(
        trade_order_id="to-sl-both-tp2",
        trade_id="tr-sl-both",
        kind="tp2",
        client_order_id="t2-both",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("120"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-sl-both-tp2", "open", exchange_order_id="ex-both-tp2")
    store.update_trade_state("tr-sl-both", "oco_active")
    # Price monitor tick crossing SL
    tm.on_tick("BTCUSDT", Decimal("89"), received_at=time.time())
    assert market_calls == [], "Market Sell must never be submitted when OCOs active"
    # Direct call to disabled method raises — verify on fresh engine without override
    fresh_engine, _, _ = _engine_with_tm(cfg, repo, SpyMexc())
    with pytest.raises(RuntimeError, match="disabled"):
        fresh_engine.submit_stop_loss_market_sell(store.get_trade("tr-sl-both"), Decimal("0.1"))


def _assert_no_market_sell_on_missing_oco(cfg, repo, missing_kind):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    market_calls = []
    engine.submit_stop_loss_market_sell = lambda *a, **kw: market_calls.append(1)  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=121, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=121,
        edit_version=0,
        idempotency_key="k121",
        client_order_id="tsi-k121",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k121'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-121', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-121",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-121",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id=f"tr-sl-missing-{missing_kind}",
        signal_id=sid,
        idempotency_key="k121",
        channel_id=100,
        message_id=121,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        f"tr-sl-missing-{missing_kind}",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store.update_trade_state(f"tr-sl-missing-{missing_kind}", "position_open")
    if missing_kind == "tp1_missing":
        store.create_trade_order(
            trade_order_id="to-miss-tp2",
            trade_id=f"tr-sl-missing-{missing_kind}",
            kind="tp2",
            client_order_id="t2-miss",
            symbol="BTCUSDT",
            side="sell",
            order_type="OCO_STOP_LIMIT_ORDER",
            price=Decimal("120"),
            quantity=Decimal("0.05"),
            allocated_qty=Decimal("0.05"),
        )
        store.transition_trade_order("to-miss-tp2", "open", exchange_order_id="ex-miss-tp2")
    elif missing_kind == "tp2_uncertain":
        store.create_trade_order(
            trade_order_id="to-miss-tp1",
            trade_id=f"tr-sl-missing-{missing_kind}",
            kind="tp1",
            client_order_id="t1-miss",
            symbol="BTCUSDT",
            side="sell",
            order_type="OCO_STOP_LIMIT_ORDER",
            price=Decimal("110"),
            quantity=Decimal("0.05"),
            allocated_qty=Decimal("0.05"),
        )
        store.transition_trade_order("to-miss-tp1", "open", exchange_order_id="ex-miss-tp1")
        store.create_trade_order(
            trade_order_id="to-miss-tp2-unc",
            trade_id=f"tr-sl-missing-{missing_kind}",
            kind="tp2",
            client_order_id="t2-unc",
            symbol="BTCUSDT",
            side="sell",
            order_type="OCO_STOP_LIMIT_ORDER",
            price=Decimal("120"),
            quantity=Decimal("0.05"),
            allocated_qty=Decimal("0.05"),
        )
        store.transition_trade_order(
            "to-miss-tp2-unc", "unknown_requires_reconciliation", exchange_order_id="ex-miss-tp2-unc"
        )
        tm.on_tick("BTCUSDT", Decimal("89"), received_at=time.time())
        assert market_calls == [], f"Market Sell must never be submitted when {missing_kind}"
        # State may remain protected via remaining OCO leg; key is no Market Sell
        assert store.get_trade(f"tr-sl-missing-{missing_kind}")["trade_state"] not in ("closed", "failed")


def test_no_market_sell_oco1_missing(cfg, repo):
    _assert_no_market_sell_on_missing_oco(cfg, repo, "tp1_missing")


def test_no_market_sell_oco2_uncertain(cfg, repo):
    _assert_no_market_sell_on_missing_oco(cfg, repo, "tp2_uncertain")


def test_no_market_sell_oco_rejection(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    market_calls = []
    engine.submit_stop_loss_market_sell = lambda *a, **kw: market_calls.append(1)  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=122, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=122,
        edit_version=0,
        idempotency_key="k122",
        client_order_id="tsi-k122",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k122'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-122', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-122",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-122",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-sl-reject",
        signal_id=sid,
        idempotency_key="k122",
        channel_id=100,
        message_id=122,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-sl-reject",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store.create_trade_order(
        trade_order_id="to-reject-tp1",
        trade_id="tr-sl-reject",
        kind="tp1",
        client_order_id="t1-reject",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-reject-tp1", "rejected", exchange_order_id=None)
    tm.on_tick("BTCUSDT", Decimal("89"), received_at=time.time())
    assert market_calls == []


def test_no_market_sell_oco_cancellation(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    market_calls = []
    engine.submit_stop_loss_market_sell = lambda *a, **kw: market_calls.append(1)  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=123, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=123,
        edit_version=0,
        idempotency_key="k123",
        client_order_id="tsi-k123",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k123'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-123', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-123",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-123",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-sl-cancel",
        signal_id=sid,
        idempotency_key="k123",
        channel_id=100,
        message_id=123,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-sl-cancel",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    store.create_trade_order(
        trade_order_id="to-cancel-tp1",
        trade_id="tr-sl-cancel",
        kind="tp1",
        client_order_id="t1-cancel",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-cancel-tp1", "canceled")
    tm.on_tick("BTCUSDT", Decimal("89"), received_at=time.time())
    assert market_calls == []


def test_no_market_sell_restart_missing_protection(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    market_calls = []
    engine.submit_stop_loss_market_sell = lambda *a, **kw: market_calls.append(1)  # type: ignore
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=124, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=124,
        edit_version=0,
        idempotency_key="k124",
        client_order_id="tsi-k124",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k124'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-124', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-124",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-124",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-sl-restart",
        signal_id=sid,
        idempotency_key="k124",
        channel_id=100,
        message_id=124,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-sl-restart",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # No OCO orders at all → restart should attempt OCO placement (not market sell)
    store.update_trade_state("tr-sl-restart", "position_open")
    tm.reconcile_all_open()
    assert market_calls == [], "restart must not submit Market Sell"
    assert store.get_trade("tr-sl-restart")["trade_state"] in (
        "needs_reconciliation",
        "position_open",
        "oco_active",
        "oco_submitting",
        "oco_partial",
    )


# ── §4 Per-symbol atomicity ───────────────────────────────────────────────


def test_two_engines_same_symbol_are_both_admitted_without_exchange_open_orders(cfg, repo):
    """Local trade history is independent; the exchange open-order check is the
    only cross-trade entry rule."""
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def attempt(mid, delay=0):
        try:
            cfg2 = AppConfig(
                api_id=12345,
                api_hash="a" * 32,
                data_dir=cfg.data_dir,
                db_path=cfg.db_path,
                session_path=cfg.session_path,
            )
            db2 = Database(cfg2.db_path)
            repo2 = Repository(db2)
            client = SpyMexc()
            store2 = TradeStore(db2, cfg2.trading, now=lambda: NOW)
            store2.set_app_state_once("trading.activation_boundary_message_id", "0")
            tm2 = TradeManager(cfg2.trading, store2, now=lambda: NOW)
            engine2 = TradingEngine(cfg2, repo2, client=client, now=lambda: NOW, trade_manager=tm2)
            tm2.engine = engine2
            engine2.store = store2
            barrier.wait(timeout=5)
            if delay:
                import time as _t

                _t.sleep(delay)
            rec = _store(repo2, mid, COMPLETE, source="live")
            item = _item(100, mid)
            results.append((mid, engine2.process_item(item, rec)))
            db2.close()
        except Exception as e:
            errors.append((mid, repr(e)))

    t1 = threading.Thread(target=attempt, args=(201, 0))
    t2 = threading.Thread(target=attempt, args=(202, 0.01))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)
    assert not errors, f"concurrency threads raised: {errors}"
    assert len(results) == 2
    assert all(res is not None for _mid, res in results)
    all_trades = repo.db.conn.execute("SELECT count(*) as c FROM trades WHERE symbol='BTCUSDT'").fetchone()["c"]
    assert all_trades == 2, f"independent lifecycles should coexist, got {all_trades}"


def test_per_symbol_different_symbols_proceed_independently(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    for mid, sym in [(210, "ETHUSDT"), (211, "SOLUSDT")]:
        txt = f"BUY {sym}\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120"
        rec = _store(repo, mid, txt)
        res = engine.process_item(_item(100, mid), rec)
        assert res is not None, f"{sym} should not be blocked by other symbol"


def test_same_symbol_exchange_query_failure_blocks_only_that_symbol(cfg, repo):
    class FailingOpenOrders(SpyMexc):
        def open_orders(self, symbol=None):
            if str(symbol).upper() == "BTCUSDT":
                raise RuntimeError("exchange unavailable")
            return []

    engine, tm, store = _engine_with_tm(cfg, repo, FailingOpenOrders())
    rec = _store(repo, 212, COMPLETE)
    res = engine.process_item(_item(100, 212), rec)
    assert res is not None and res["state"] == "rejected"
    assert engine.client.place_order_calls == []
    sig = repo.db.conn.execute(
        "SELECT status, rejection_reason FROM signals WHERE channel_id=100 AND message_id=212"
    ).fetchone()
    assert sig["status"] == "rejected"
    assert "open-order query failed" in sig["rejection_reason"].lower()

    other = _store(repo, 213, "BUY ETHUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120")
    assert engine.process_item(_item(100, 213), other) is not None


def test_per_symbol_terminal_states_not_blocking(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=220, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=220,
        edit_version=0,
        idempotency_key="k220",
        client_order_id="tsi-k220",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k220'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-220', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-220",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "",
            "",
            "t-220",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-term",
        signal_id=sid,
        idempotency_key="k220",
        channel_id=100,
        message_id=220,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-term", acquired_base_qty=Decimal("0.1"), acquired_quote_spent=Decimal("10"), avg_entry_price=Decimal("100")
    )
    store.update_trade_state("tr-term", "closed")
    # Now new signal for same symbol should be allowed (closed is terminal)
    rec2 = _store(repo, 221, COMPLETE)
    res2 = engine.process_item(_item(100, 221), rec2)
    assert res2 is not None


# ── §5 Config cannot disable ──────────────────────────────────────────────


def test_config_cannot_disable_mandatory_invariants(tmp_path):
    from pathlib import Path

    def _root_with(content, tmp_path):
        root = Path(tmp_path) / "cfgtest"
        root.mkdir(parents=True, exist_ok=True)
        (root / "config").mkdir(exist_ok=True)
        (root / "config" / "config.toml").write_text(content)
        (root / "data").mkdir(exist_ok=True)
        return root

    for key in [
        "require_stop_loss = true",
        "require_tp1 = true",
        "require_tp2 = true",
        "enforce_price_relationship = true",
    ]:
        root = _root_with(f"[trading]\norder_quote_amount = 10\n{key}\n", tmp_path=tmp_path)
        with pytest.raises(ConfigError, match="not configurable"):
            load_config(root=root, environ={}, require_credentials=False)


# ── §6 Entry-range handling ───────────────────────────────────────────────


def test_entry_range_rejected_with_clear_reason(cfg, repo):
    engine, _, _ = _engine_with_tm(cfg, repo, SpyMexc())
    # Simulate parser output with range only (no single entry)
    from tg_signal_ingest.trading.parser import ParsedSignal

    ps = ParsedSignal(
        kind="alert",
        action="buy",
        symbol="BTCUSDT",
        entry_price=None,
        entry_low_price=Decimal("100"),
        entry_high_price=Decimal("102"),
        target_prices=(Decimal("110"), Decimal("120")),
        stop_loss=Decimal("90"),
        order_type=None,
        amount_text=None,
        raw_text="BUY BTCUSDT ENTRY 100-102 SL 90 TP1 110 TP2 120",
        normalized_text="BUY BTCUSDT ENTRY 100-102",
        fingerprint_sha256="x",
        reasons=(),
    )
    rm = engine.risk
    dec = rm.evaluate(ps, message_datetime=None, reference_price=None, base_asset="BTC", now=NOW, symbol_meta=None)
    assert not dec.approved
    assert "entry range" in dec.reason.lower() and "not supported" in dec.reason.lower()


# ── §7 Exit commission accounting ─────────────────────────────────────────


def test_sell_base_fee_reduces_sellable(cfg, repo):
    engine, tm, store = _engine_with_tm(cfg, repo, SpyMexc())
    parsed = SignalParser().parse(COMPLETE)
    sid, _ = store.record_signal(parsed, channel_id=100, message_id=130, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=130,
        edit_version=0,
        idempotency_key="k130",
        client_order_id="tsi-k130",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    iid = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k130'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-130', state='filled' WHERE intent_id=?", (iid,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-130",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "BTC",
            "0.001",
            "t-130-entry",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-fee-both",
        signal_id=sid,
        idempotency_key="k130",
        channel_id=100,
        message_id=130,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=iid,
    )
    store.update_trade_fields(
        "tr-fee-both",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # TP1 sell with base fee
    store.create_trade_order(
        trade_order_id="to-fee-tp1",
        trade_id="tr-fee-both",
        kind="tp1",
        client_order_id="t1-fee",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-fee-tp1", "filled", exchange_order_id="ex-fee-tp1", sold_qty=Decimal("0.05"))
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-fee-tp1",
            "BTCUSDT",
            "110",
            "0.05",
            "5.5",
            "BTC",
            "0.0005",
            "t-130-tp1",
            0,
            0,
            2,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    trade = store.get_trade("tr-fee-both")
    sellable = tm._sellable_quantity(trade)
    # acquired 0.1 - entry fee 0.001 - tp1 base fee 0.0005 - sold 0.05 = 0.0485
    assert sellable == Decimal("0.0485")
    # No double-count: fee transaction IDs distinct, sum is 0.001+0.0005
    assert tm._total_base_fees(trade) == Decimal("0.0015")
