"""Deterministic, offline simulation harness for the trade lifecycle.

Purpose
-------
Provide operators and reviewers a repeatable, zero-network way to exercise the
conditional-entry / TP1-TP2 / stop-loss state machine end to end.  Every
exchange call is routed through an in-process fake, so this simulates MEXC Spot
behaviour without ever placing a real order.

The harness mirrors the production wiring: ``TradingEngine`` -> ``TradeManager``
-> ``TradeStore``, driven by simulated price ticks rather than a live feed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from tests._trading_fakes import FakeClient, FakeEngine, FakeMeta, FakeNotifier, FakeRegistry
from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.repository import Database, Repository
from tg_signal_ingest.trading.parser import ParsedSignal
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"


@dataclass
class SimConfig:
    symbol: str = "BTCUSDT"
    entry: Decimal = Decimal("100")
    tp1: Decimal = Decimal("110")
    tp2: Decimal = Decimal("120")
    sl: Decimal = Decimal("95")
    offset_ticks: int = 2
    quote_amount: Decimal = Decimal("10")
    base_quantity: Decimal = Decimal("0.1")
    stale_threshold_s: float = 60.0
    tick_size: str = "0.01"


@dataclass
class TradeSim:
    """Runs one trade through the lifecycle using deterministic fakes."""

    cfg: SimConfig = field(default_factory=SimConfig)
    events: list[str] = field(default_factory=list)

    # Built on first :meth:`start`.
    store: TradeStore | None = None
    engine: FakeEngine | None = None
    tm: TradeManager | None = None
    client: FakeClient | None = None
    notifier: FakeNotifier | None = None
    intent: dict[str, Any] | None = None
    trade: dict[str, Any] | None = None

    # ------------------------------------------------------------------ setup

    def start(self, app_cfg: AppConfig | None = None) -> TradeSim:
        app_cfg = app_cfg or _app_config()
        app_cfg.trading.entry_trigger_offset_ticks = self.cfg.offset_ticks
        app_cfg.trading.entry_trigger_stale_threshold_s = self.cfg.stale_threshold_s

        repo = Repository(Database(":memory:"))
        repo.db.migrate()
        self.store = TradeStore(repo.db, app_cfg.trading)
        self.client = FakeClient(registry=FakeRegistry(meta=FakeMeta(tick_size=self.cfg.tick_size)))
        self.engine = FakeEngine(app_cfg, self.store, client=self.client)
        self.notifier = FakeNotifier()
        self.tm = TradeManager(
            app_cfg.trading, self.store, engine=self.engine, client=self.client, notifier=self.notifier
        )
        self.events.clear()
        return self

    # ------------------------------------------------------------------ steps

    def submit_signal(self, reference_price: Decimal | None = None) -> None:
        """Create the trade for a buy signal at the given reference market price."""
        assert self.tm is not None and self.store is not None
        self._insert_signal_and_intent()
        assert self.intent is not None
        decision = self.tm.on_intent_planned(
            self.intent,
            ParsedSignal(
                kind="alert",
                action="buy",
                symbol=self.cfg.symbol,
                entry_price=self.cfg.entry,
                entry_low_price=None,
                entry_high_price=None,
                target_prices=(self.cfg.tp1, self.cfg.tp2),
                stop_loss=self.cfg.sl,
                order_type="limit",
                amount_text=None,
                raw_text="buy",
                normalized_text="buy",
                fingerprint_sha256="sim",
            ),
            reference_price=reference_price,
        )
        self.trade = decision.trade
        assert self.trade is not None
        if decision.deferred:
            self.events.append(f"conditional_entry_waiting trigger={self.trade['trigger_price']}")
        elif decision.rejected:
            self.events.append(f"rejected: {decision.reason}")
        else:
            self.events.append("direct_entry")
            assert self.engine is not None
            assert self.trade is not None
            self.engine._execute_intent(self.intent["intent_id"])
            executed_intent = self.engine.store.get_intent(self.intent["intent_id"])
            assert executed_intent is not None
            self.tm.on_intent_executed(executed_intent)
            self.intent = self.engine.store.get_intent(self.intent["intent_id"])
            self.trade = self.store.get_trade(self.trade["trade_id"])
            assert self.trade is not None
            assert self.trade["trade_state"] == "entry_active"
            self.events.append("entry_active")

    def fill_entry(self, qty: Decimal | None = None) -> None:
        """Credit an entry fill so a position exists to manage."""
        assert (
            self.engine is not None
            and self.intent is not None
            and self.client is not None
            and self.store is not None
            and self.tm is not None
            and self.trade is not None
        )
        qty = qty or self.cfg.base_quantity
        oid = self.intent["exchange_order_id"]
        self.client.fill_order(oid, qty, qty * self.cfg.entry, self.cfg.entry)
        self.store.add_fill(
            exchange_order_id=oid,
            symbol=self.cfg.symbol,
            raw={
                "price": str(self.cfg.entry),
                "qty": str(qty),
                "quoteQty": str(qty * self.cfg.entry),
                "commission": "0",
                "commissionAsset": "USDT",
                "isBuyer": 1,
                "isMaker": 0,
                "time": 1,
            },
        )
        self.tm._reconcile_entry_fills(self.trade)
        self.trade = self.store.get_trade(self.trade["trade_id"])
        self.events.append(f"entry_filled qty={qty}")

    def tick(self, symbol: str, price: Decimal, received_at: float = 1.0) -> None:
        """Deliver one market tick to the trade manager."""
        assert self.tm is not None and self.store is not None and self.trade is not None
        self.tm._clock = lambda: received_at
        self.tm.on_tick(symbol, price, received_at)
        traded = self.store.get_trade(self.trade["trade_id"])
        assert traded is not None
        self.trade = traded
        self.events.append(f"tick {symbol}@{price} -> {traded['trade_state']}")

    def reconcile(self) -> None:
        assert self.tm is not None and self.store is not None and self.trade is not None
        self.tm.reconcile_all_open()
        traded = self.store.get_trade(self.trade["trade_id"])
        assert traded is not None
        self.trade = traded
        self.events.append(f"reconciled -> {traded['trade_state']}")

    def state(self) -> str:
        assert self.trade is not None
        return self.trade["trade_state"]

    def trace(self) -> list[str]:
        return list(self.events)

    # ------------------------------------------------------------------ internal

    def _insert_signal_and_intent(self) -> None:
        assert self.store is not None
        sid = "sim-sig-1"
        cid = "sim-tsi-1"
        iid = f"intent-{cid}"
        with self.store.db.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO signals (signal_id, channel_id, message_id, edit_version, "
                "parser_version, signal_kind, symbol, action, status, fingerprint_sha256, created_at, updated_at) "
                "VALUES (?, ?, ?, 0, 1, 'alert', ?, 'buy', 'parsed', 'sim', 'now', 'now')",
                (sid, 100, 1, self.cfg.symbol),
            )
            conn.execute(
                """INSERT INTO order_intents
                (intent_id, signal_id, idempotency_key, client_order_id, symbol, side,
                 order_type, quote_amount, base_quantity, limit_price, mode, state,
                 create_msg_id, create_channel_id, planned_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    iid,
                    sid,
                    f"idem-{cid}",
                    cid,
                    self.cfg.symbol,
                    "buy",
                    "limit",
                    str(self.cfg.quote_amount),
                    str(self.cfg.base_quantity),
                    str(self.cfg.entry),
                    "live",
                    "planned",
                    1,
                    100,
                    "now",
                    "now",
                ),
            )
        self.intent = self.store.get_intent(iid)


# --------------------------------------------------------------------------- scenarios


def _app_config() -> AppConfig:
    from pathlib import Path

    base = Path("/tmp/tsim-data")
    cfg = AppConfig(
        api_id=1,
        api_hash="a" * 32,
        data_dir=base,
        db_path=base / "t.sqlite3",
        session_path=base / "session",
    )
    cfg.ensure_dirs()
    return cfg


def scenario_full_tp_sl() -> TradeSim:
    """Full lifecycle: direct entry -> fill -> TP1 -> TP2 -> closed.

    Demonstrates that TP1 sells 50% of the acquired position and TP2 sells the
    remainder, then the trade closes once both targets fill.
    """
    sim = TradeSim().start()
    assert sim.engine is not None and sim.store is not None and sim.client is not None
    sim.submit_signal(reference_price=Decimal("100"))
    assert sim.trade is not None
    sim.fill_entry()
    sim.tick(sim.cfg.symbol, sim.cfg.tp1)
    tp1_order = sim.store.list_trade_orders(sim.trade["trade_id"], kind="tp1")[0]
    sim.client.fill_order(
        tp1_order["exchange_order_id"],
        sim.cfg.base_quantity / 2,
        (sim.cfg.base_quantity / 2) * sim.cfg.tp1,
        sim.cfg.tp1,
    )
    sim.store.add_fill(
        exchange_order_id=tp1_order["exchange_order_id"],
        symbol=sim.cfg.symbol,
        raw={
            "price": str(sim.cfg.tp1),
            "qty": str(sim.cfg.base_quantity / 2),
            "quoteQty": str((sim.cfg.base_quantity / 2) * sim.cfg.tp1),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 2,
        },
    )
    sim.reconcile()  # tp1_active -> tp2_active
    sim.tick(sim.cfg.symbol, sim.cfg.tp2)
    tp2_order = sim.store.list_trade_orders(sim.trade["trade_id"], kind="tp2")[0]
    sim.client.fill_order(
        tp2_order["exchange_order_id"],
        sim.cfg.base_quantity / 2,
        (sim.cfg.base_quantity / 2) * sim.cfg.tp2,
        sim.cfg.tp2,
    )
    sim.store.add_fill(
        exchange_order_id=tp2_order["exchange_order_id"],
        symbol=sim.cfg.symbol,
        raw={
            "price": str(sim.cfg.tp2),
            "qty": str(sim.cfg.base_quantity / 2),
            "quoteQty": str((sim.cfg.base_quantity / 2) * sim.cfg.tp2),
            "commission": "0",
            "commissionAsset": "USDT",
            "isBuyer": 0,
            "isMaker": 1,
            "time": 3,
        },
    )
    sim.reconcile()  # tp2_active -> closed
    assert sim.state() == "closed", sim.state()
    return sim


def scenario_stop_loss() -> TradeSim:
    """Entry filled then stop-loss hits: entry+TP cancelled, market-sell exits."""
    sim = TradeSim().start()
    assert sim.engine is not None and sim.store is not None and sim.client is not None
    sim.submit_signal(reference_price=Decimal("100"))
    assert sim.trade is not None
    sim.fill_entry()
    sim.tick(sim.cfg.symbol, sim.cfg.tp1)  # arm TP1
    sim.tick(sim.cfg.symbol, sim.cfg.sl)  # SL wins; entry+TP cancelled, market sell
    assert sim.state() in ("stop_loss_executing", "exit_pending", "closed"), sim.state()
    assert sim.engine.cancel_intent_calls
    assert sim.engine.cancel_trade_calls
    assert sim.engine.sl_calls
    return sim


def scenario_conditional_entry_never_submits_before_trigger() -> TradeSim:
    """Below-entry market waits for the upward trigger; no order is placed."""
    sim = TradeSim().start()
    assert sim.engine is not None and sim.engine.client is not None
    sim.submit_signal(reference_price=Decimal("90"))
    assert sim.trade is not None and sim.trade["trade_state"] == "pending_entry"
    assert sim.engine.client.place_calls == []
    sim.tick(sim.cfg.symbol, Decimal("99.97"))  # below trigger -> still waiting
    assert sim.state() == "pending_entry"
    assert sim.engine.client.place_calls == []
    sim.tick(sim.cfg.symbol, Decimal("99.98"))  # reaches trigger -> entry activated
    assert sim.state() == "entry_active"
    assert len(sim.engine.intent_calls) == 1
    return sim
