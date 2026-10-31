"""Deterministic tests for owner-confirmed strategy §§2.1-2.4, §3-§5, §7, §9.

No network, no live credentials — FakeMexc/FakeRegistry only.
Each test verifies a mandatory behavior at the execution boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem, Repository
from tg_signal_ingest.trading.engine import TradingEngine
from tg_signal_ingest.trading.parser import SignalParser
from tg_signal_ingest.trading.store import TradingStore
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


# ── helpers ──────────────────────────────────────────────────────────────
class FakeMsg:
    def __init__(self, id: int, text: str, edit_date=None):
        self.id = id
        self.message = text
        self.date = NOW
        self.edit_date = edit_date
        self.media = None
        self.grouped_id = None
        self.reply_to = None
        self.forward = None

    def to_dict(self):
        return {
            "id": self.id,
            "message": self.message,
            "date": self.date.isoformat(),
            "edit_date": self.edit_date.isoformat() if self.edit_date else None,
        }


def _store(repo: Repository, mid: int, text: str, source="live"):
    repo.upsert_message(message_to_record(100, FakeMsg(mid, text), source))
    return repo.get_message_raw(100, mid)


def _engine(cfg: AppConfig, repo: Repository, client=None, mode=None):
    del mode  # one automatic Spot behaviour; tests use a fake exchange
    cfg.trading.order_quote_amount = 10.0
    if client is None:
        client, registry = _fake_mexc_with_registry()
    else:
        registry = getattr(client, "symbols", None)
    engine = TradingEngine(cfg, repo, client=client, registry=registry, now=lambda: NOW)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    return engine


def _fake_mexc_with_registry():
    # import the fake from existing test file to reuse FakeRegistry
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.FakeMexc(), mod.FakeRegistry()


# ── §4 Mandatory validation at execution boundary ────────────────────────

COMPLETE_LONG = "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120"


def test_missing_symbol_rejects(cfg, repo):
    # Parser will produce kind=ambiguous for missing symbol, engine rejects before risk
    engine = _engine(cfg, repo)
    rec = _store(repo, 1, "BUY \nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=1,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )
    sig = repo.db.conn.execute("SELECT status FROM signals WHERE channel_id=100 AND message_id=1").fetchone()
    assert sig["status"] == "rejected"


def test_missing_entry_rejects(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 2, "BUY BTCUSDT\nSL: 90\nTP1: 110\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=2,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )


def test_missing_sl_rejects(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 3, "BUY BTCUSDT\nENTRY: 100\nTP1: 110\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=3,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )
    # distinct reason contains SL
    row = repo.db.conn.execute("SELECT rejection_reason FROM signals WHERE channel_id=100 AND message_id=3").fetchone()
    assert "SL" in row["rejection_reason"] or "stop" in row["rejection_reason"].lower()


def test_missing_tp1_rejects(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 4, "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=4,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )


def test_missing_tp2_rejects(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 5, "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=5,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )


def test_invalid_price_relationship_rejects(cfg, repo):
    engine = _engine(cfg, repo)
    # SL >= Entry
    rec = _store(repo, 6, "BUY BTCUSDT\nENTRY: 100\nSL: 100\nTP1: 110\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=6,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec,
        )
        is None
    )
    # Entry >= TP1
    rec2 = _store(repo, 7, "BUY BTCUSDT\nENTRY: 110\nSL: 90\nTP1: 110\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=7,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec2,
        )
        is None
    )
    # TP1 > TP2
    rec3 = _store(repo, 8, "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 130\nTP2: 120")
    assert (
        engine.process_item(
            QueueItem(
                queue_id=0,
                channel_id=100,
                message_id=8,
                item_type="message",
                status="pending",
                targets_version=0,
                attempts=0,
                available_at="",
                created_at="",
            ),
            rec3,
        )
        is None
    )


def test_valid_long_passes(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 9, COMPLETE_LONG)
    intent = engine.process_item(
        QueueItem(
            queue_id=0,
            channel_id=100,
            message_id=9,
            item_type="message",
            status="pending",
            targets_version=0,
            attempts=0,
            available_at="",
            created_at="",
        ),
        rec,
    )
    assert intent is not None
    assert intent["state"] in ("submitted", "open")
    assert intent["exchange_order_id"]


# ── §3 Independent execution ──────────────────────────────────────────────


def test_per_signal_idempotency_no_duplicate_buy(cfg, repo):
    engine = _engine(cfg, repo)
    rec = _store(repo, 10, COMPLETE_LONG)
    item = QueueItem(
        queue_id=0,
        channel_id=100,
        message_id=10,
        item_type="message",
        status="pending",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )
    first = engine.process_item(item, rec)
    second = engine.process_item(item, rec)
    assert first is not None and second is not None
    assert first["intent_id"] == second["intent_id"]
    # only one intent
    assert len(engine.store.list_intents(limit=10)) == 1


def test_per_symbol_concurrency_allows_different_symbol(cfg, repo):
    engine = _engine(cfg, repo)
    rec1 = _store(repo, 11, "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120")
    rec2 = _store(repo, 12, "BUY ETHUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120")
    a = engine.process_item(
        QueueItem(
            queue_id=0,
            channel_id=100,
            message_id=11,
            item_type="message",
            status="pending",
            targets_version=0,
            attempts=0,
            available_at="",
            created_at="",
        ),
        rec1,
    )
    b = engine.process_item(
        QueueItem(
            queue_id=0,
            channel_id=100,
            message_id=12,
            item_type="message",
            status="pending",
            targets_version=0,
            attempts=0,
            available_at="",
            created_at="",
        ),
        rec2,
    )
    assert a is not None and b is not None
    assert a["symbol"] != b["symbol"]


def test_per_symbol_concurrency_blocks_same_symbol_via_trade(cfg, repo):
    # Live mode with TradeManager: same symbol active trade blocks second BUY
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    FakeMexc = mod.FakeMexc
    client = FakeMexc(
        place_response={"orderId": "ex-1", "status": "NEW"},
        query_results=[
            {
                "orderId": "ex-1",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "LIMIT",
                "price": "100",
                "origQty": "0.1",
                "executedQty": "0",
                "cumulativeQuoteQty": "0",
                "status": "NEW",
                "time": 1,
            }
        ],
    )
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, trade_manager=tm)
    tm.engine = engine
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")

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

    first_rec = _store(repo, 20, COMPLETE_LONG)
    first = engine.process_item(_item(100, 20), first_rec)
    assert first is not None and first["state"] == "open"
    # second same symbol should be blocked by per-symbol concurrency (trade pending_entry/entry_active)
    second_rec = _store(repo, 21, COMPLETE_LONG)
    second = engine.process_item(_item(100, 21), second_rec)
    assert second is None
    sig = repo.db.conn.execute(
        "SELECT status, rejection_reason FROM signals WHERE channel_id=100 AND message_id=21"
    ).fetchone()
    assert sig["status"] == "rejected"
    assert "per-symbol concurrency" in sig["rejection_reason"].lower()


# ── §5 OCO invariant ─────────────────────────────────────────────────────


def test_combined_oco_invariant_holds_for_valid_allocation(cfg, repo):
    # Construct trade with 0.1 acquired, 50% each, sellable 0.1, reserved 0.05+0.05 =0.1 OK
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    # minimal registry
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    tm.client = mod.FakeMexc()
    parsed = SignalParser().parse(COMPLETE_LONG)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=30, edit_version=0)
    # create intent and trade manually
    intent_id = "intent-test-30"
    store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=30,
        edit_version=0,
        idempotency_key="k30",
        client_order_id="tsi-k30",
        mode="sim",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    # simulate fill
    # directly create trade
    store.create_trade(
        trade_id="tr-test-oco-ok",
        signal_id=signal_id,
        idempotency_key="k30",
        channel_id=100,
        message_id=30,
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
        entry_intent_id=intent_id,
    )
    # set acquired
    store.update_trade_fields(
        "tr-test-oco-ok",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    # create OCO legs reserved
    store.create_trade_order(
        trade_order_id="to-t1-ok",
        trade_id="tr-test-oco-ok",
        kind="tp1",
        client_order_id="t1-ok",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-t1-ok", "open", exchange_order_id="ex-t1-ok")
    store.create_trade_order(
        trade_order_id="to-t2-ok",
        trade_id="tr-test-oco-ok",
        kind="tp2",
        client_order_id="t2-ok",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("120"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-t2-ok", "open", exchange_order_id="ex-t2-ok")
    trade = store.get_trade("tr-test-oco-ok")
    ok, detail = tm._verify_oco_invariant(trade)
    assert ok, detail


def test_combined_oco_invariant_violation_persists_needs_reconciliation(cfg, repo):
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    tm.client = mod.FakeMexc()
    parsed = SignalParser().parse(COMPLETE_LONG)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=31, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=31,
        edit_version=0,
        idempotency_key="k31",
        client_order_id="tsi-k31",
        mode="sim",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    store.create_trade(
        trade_id="tr-test-oco-bad",
        signal_id=signal_id,
        idempotency_key="k31",
        channel_id=100,
        message_id=31,
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
        entry_intent_id="intent-31",
    )
    store.update_trade_fields(
        "tr-test-oco-bad",
        acquired_base_qty=Decimal("0.05"),
        acquired_quote_spent=Decimal("5"),
        avg_entry_price=Decimal("100"),
    )
    # Reserved 0.04+0.04=0.08 > sellable 0.05 → violation
    store.create_trade_order(
        trade_order_id="to-t1-bad",
        trade_id="tr-test-oco-bad",
        kind="tp1",
        client_order_id="t1-bad",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.04"),
        allocated_qty=Decimal("0.04"),
    )
    store.transition_trade_order("to-t1-bad", "open", exchange_order_id="ex-t1-bad")
    store.create_trade_order(
        trade_order_id="to-t2-bad",
        trade_id="tr-test-oco-bad",
        kind="tp2",
        client_order_id="t2-bad",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("120"),
        quantity=Decimal("0.04"),
        allocated_qty=Decimal("0.04"),
    )
    store.transition_trade_order("to-t2-bad", "open", exchange_order_id="ex-t2-bad")
    trade = store.get_trade("tr-test-oco-bad")
    ok, detail = tm._verify_oco_invariant(trade)
    assert not ok
    assert "OCO1+OCO2 reserved" in detail or "reserved" in detail.lower()
    trade2 = store.get_trade("tr-test-oco-bad")
    assert trade2["trade_state"] == "needs_reconciliation"


def test_base_fee_reduces_sellable(cfg, repo):
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    fake = mod.FakeMexc()
    tm.client = fake
    # need symbol meta with base_asset
    fake.symbols = mod.FakeRegistry()
    parsed = SignalParser().parse(COMPLETE_LONG)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=32, edit_version=0)
    # create intent with exchange_order_id ex-fee
    store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=32,
        edit_version=0,
        idempotency_key="k32",
        client_order_id="tsi-k32",
        mode="sim",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    # manually set exchange_order_id and add fill with base fee via direct SQL (TradingStore.add_fill requires raw dict)
    iid2 = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k32'").fetchone()[
        "intent_id"
    ]
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-fee', state='filled' WHERE intent_id=?", (iid2,)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-fee",
            "BTCUSDT",
            "100",
            "0.1",
            "10",
            "BTC",
            "0.001",
            "t-fee-1",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    store.create_trade(
        trade_id="tr-test-fee",
        signal_id=signal_id,
        idempotency_key="k32",
        channel_id=100,
        message_id=32,
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
        entry_intent_id=repo.db.conn.execute(
            "SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k32'"
        ).fetchone()["intent_id"],
    )
    store.update_trade_fields(
        "tr-test-fee",
        acquired_base_qty=Decimal("0.1"),
        acquired_quote_spent=Decimal("10"),
        avg_entry_price=Decimal("100"),
    )
    trade = store.get_trade("tr-test-fee")
    sellable = tm._sellable_quantity(trade)
    # sellable should be acquired - base_fee - sold = 0.1 -0.001 =0.099
    assert sellable == Decimal("0.099")


# ── §7 No auto market sell ───────────────────────────────────────────────


def test_no_auto_market_sell_on_oco_fail(cfg, repo):
    # When OCO placement fails (no allocation), system goes to needs_reconciliation, not market sell
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, now=lambda: NOW)
    import importlib.util

    spec = importlib.util.spec_from_file_location("engine_test", "tests/test_trading_engine.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    tm.client = mod.FakeMexc()
    tm.engine = object()  # dummy engine — config rejection does not need real client
    parsed = SignalParser().parse(COMPLETE_LONG)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=33, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=33,
        edit_version=0,
        idempotency_key="k33",
        client_order_id="tsi-k33",
        mode="sim",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    store.create_trade(
        trade_id="tr-test-nomarket",
        signal_id=signal_id,
        idempotency_key="k33",
        channel_id=100,
        message_id=33,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="immediate",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=None,
        tp2_price=None,
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=None,
        entry_intent_id="intent-33",
    )
    store.update_trade_fields("tr-test-nomarket", acquired_base_qty=Decimal("0.1"))
    # try to place OCO with missing prices — should go to needs_reconciliation, not market sell
    tm._place_oco_exits(store.get_trade("tr-test-nomarket"))
    trade2 = store.get_trade("tr-test-nomarket")
    assert trade2["trade_state"] == "needs_reconciliation"
    # no market sell order created
    assert len(store.list_trade_orders("tr-test-nomarket", kind="stop_loss")) == 0


# ── §9 Fill-level accounting decimal-safe ────────────────────────────────


def test_fill_level_pnl_decimal_safe(cfg, repo):
    store = TradingStore(repo.db, cfg.trading, now=lambda: NOW)
    # create intent and two fills with decimal quantities
    parsed = SignalParser().parse(COMPLETE_LONG)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=40, edit_version=0)
    store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=40,
        edit_version=0,
        idempotency_key="k40",
        client_order_id="tsi-k40",
        mode="sim",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    intent = repo.db.conn.execute("SELECT intent_id FROM order_intents WHERE client_order_id='tsi-k40'").fetchone()
    repo.db.conn.execute(
        "UPDATE order_intents SET exchange_order_id='ex-pnl', state='filled' WHERE intent_id=?", (intent["intent_id"],)
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-pnl",
            "BTCUSDT",
            "100.12345678",
            "0.03333333",
            "3.337777",
            "USDT",
            "0.003",
            "t-pnl-1",
            1,
            0,
            1,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    repo.db.conn.execute(
        (
            "INSERT INTO order_fills (exchange_order_id, symbol, price, quantity, "
            "quote_amount, fee_asset, fee_amount, exchange_trade_id, is_buyer, "
            "is_maker, trade_time_ms, source, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ),
        (
            "ex-pnl",
            "BTCUSDT",
            "100.12345678",
            "0.03333333",
            "3.337777",
            "BTC",
            "0.00002",
            "t-pnl-2",
            1,
            0,
            2,
            "reconciliation",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        ),
    )
    fills = store.fills_for_order("ex-pnl")
    assert len(fills) == 2
    total_quote = sum(Decimal(f["quote_amount"]) for f in fills)
    assert total_quote == Decimal("6.675554")
    # fees preserved with asset
    assert fills[0]["fee_asset"] == "USDT"
    assert fills[1]["fee_asset"] == "BTC"
