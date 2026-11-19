"""P0 characterization freeze: symbol-metadata failures must fail closed.

Incident: when symbol metadata is unavailable at planning time (transient
exchange failure) or fails deterministic validation immediately before
submission, no BUY may ever reach the exchange.  Both production paths must
reject durably, record the rejection reason, notify the owner, and leave no
order behind.

These tests exercise the real decision logic on both paths:

* planning path — the engine ``symbol_metadata`` stage in ``process_item``
  and the ``TradeManager`` trade-creation stage (exit-ladder validation);
* pre-submit revalidation — ``TradingEngine.execute_intent`` ->
  ``_symbol_meta`` + ``meta.validate_order`` -> ``_reject_before_submit``.

No production code is modified and no network/exchange call is made.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from _trading_fakes import FakeClient, FakeEngine, FakeMeta, FakeNotifier, FakeRegistry
from conftest import FakeMsg

from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.mexc.errors import MexcSymbolError
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem, Repository
from tg_signal_ingest.trading.engine import TradingEngine
from tg_signal_ingest.trading.parser import ParsedSignal
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

SYMBOL = "BTCUSDT"
ENTRY = Decimal("100")
ALERT = "BUY BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120"
NOW = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
# Market reference used to defer entry into the conditional wait: 90 is below
# trigger 98.5 (entry 100 - 3 ticks * 0.5) and below the 100 entry.
_REFERENCE_PRICE = Decimal("90")


# ---------------------------------------------------------------------------
# Failing metadata sources
# ---------------------------------------------------------------------------


class _TransientRegistry:
    """First metadata lookup raises (e.g. MEXC 5xx), later ones recover."""

    def __init__(self) -> None:
        self.calls = 0

    def get(self, symbol: str):
        self.calls += 1
        if self.calls == 1:
            raise MexcSymbolError("symbol metadata temporarily unavailable (HTTP 500)")
        return FakeMeta()


class _DeterministicRegistry:
    """Every metadata lookup fails with the same deterministic symbol error."""

    def __init__(self) -> None:
        self.calls = 0

    def get(self, symbol: str):
        self.calls += 1
        raise MexcSymbolError("MEXC returned HTTP 400: invalid symbol parameter")


class _BrokenValidateMeta(FakeMeta):
    """Metadata resolves but order validation deterministically fails."""

    def validate_order(self, **kwargs: Any) -> None:
        raise ValueError("quantity below minimum 1.0")


class _EngineFakeClient(FakeClient):
    """FakeClient plus the read-only account/ticker views the engine reads.

    ``query_order``/``open_orders`` are inherited from ``FakeClient``.
    """

    def ticker_price(self, symbol=None):
        return {"symbol": symbol, "price": str(_REFERENCE_PRICE)}

    def account(self):
        return {
            "canTrade": True,
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "BTC", "free": "1000", "locked": "0", "available": "1000"},
                {"asset": "USDT", "free": "1000", "locked": "0", "available": "1000"},
            ],
        }


# ---------------------------------------------------------------------------
# Planning path helpers (TradeManager trade-creation stage)
# ---------------------------------------------------------------------------


def _planning_env(cfg: AppConfig, repo: Repository, registry: Any):
    store = TradeStore(repo.db, cfg.trading)
    client = FakeClient(registry)
    engine = FakeEngine(cfg, store, client=client)
    notifier = FakeNotifier()
    tm = TradeManager(cfg.trading, store, engine=engine, client=client)
    return store, engine, client, notifier, tm


def _insert_planned_intent(store: TradeStore, *, signal_id: str = "sig-1", client_order_id: str = "meta-1") -> dict:
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
    intent = store.get_intent(iid)
    assert intent is not None
    return intent


def _parsed() -> ParsedSignal:
    return ParsedSignal(
        kind="alert",
        action="buy",
        symbol=SYMBOL,
        entry_price=ENTRY,
        entry_low_price=None,
        entry_high_price=None,
        target_prices=(Decimal("110"), Decimal("120")),
        stop_loss=Decimal("95"),
        order_type="limit",
        amount_text=None,
        raw_text="buy",
        normalized_text="buy",
        fingerprint_sha256="abc",
    )


def test_p0_transient_metadata_failure_at_planning_rejects_fail_closed(cfg, repo):
    registry = _TransientRegistry()
    store, engine, client, notifier, tm = _planning_env(cfg, repo, registry)
    intent = _insert_planned_intent(store)

    decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))

    # Fail-closed: no trade is created and the decision is a rejection.
    assert decision.deferred is False
    assert decision.trade is None
    assert decision.rejected is True

    # The intent is durably rejected naming the missing metadata, and the
    # signal is rejected too (no partial pipeline state left behind).
    refreshed = store.get_intent(intent["intent_id"])
    assert refreshed is not None
    assert refreshed["state"] == "rejected"
    assert refreshed["reject_reason"] == "symbol metadata unavailable for exit-ladder validation"
    signal = store.get_signal(intent["signal_id"])
    assert signal is not None
    assert signal["status"] == "rejected"

    # The rejection is durably journaled with its precise reason; delivery is
    # handled independently by the domain-event worker.
    rejection = store.db.conn.execute(
        "SELECT payload_json,severity FROM audit_events WHERE event_class='domain' "
        "AND event_type='intent.state_changed' AND aggregate_id=?",
        (intent["intent_id"],),
    ).fetchone()
    assert rejection is not None and rejection["severity"] == "ERROR"
    assert "symbol metadata unavailable for exit-ladder validation" in rejection["payload_json"]

    # No order reached the fake exchange.
    assert client.place_calls == []
    assert store.list_trades() == []


def test_p0_metadata_recovery_allows_planning_after_transient_failure(cfg, repo):
    registry = _TransientRegistry()
    store, engine, client, notifier, tm = _planning_env(cfg, repo, registry)
    first = _insert_planned_intent(store, signal_id="sig-1", client_order_id="meta-a")

    first_decision = tm.on_intent_planned(first, _parsed(), reference_price=Decimal("100"))
    assert first_decision.rejected is True
    assert client.place_calls == []

    # Next planning round (fresh signal/intent): metadata lookup recovers.
    second = _insert_planned_intent(store, signal_id="sig-2", client_order_id="meta-b")
    second_decision = tm.on_intent_planned(second, _parsed(), reference_price=Decimal("100"))
    assert second_decision.rejected is False
    assert second_decision.deferred is False
    assert second_decision.trade is not None
    assert second_decision.trade["entry_mode"] == "direct"
    assert registry.calls == 2

    # The recovered planning round submits exactly one LIMIT BUY.
    engine._execute_intent(second["intent_id"])
    assert len(client.place_calls) == 1
    call = client.place_calls[0]
    assert call["symbol"] == SYMBOL
    assert call["side"] == "BUY"
    assert call["order_type"] == "LIMIT"
    assert Decimal(str(call["price"])) == ENTRY

    # The first, failed round was never submitted later.
    refreshed_first = store.get_intent(first["intent_id"])
    assert refreshed_first is not None
    assert refreshed_first["state"] == "rejected"


def test_p0_deterministic_symbol_error_keeps_every_attempt_fail_closed(cfg, repo):
    registry = _DeterministicRegistry()
    store, engine, client, notifier, tm = _planning_env(cfg, repo, registry)

    decisions = []
    for round_index in range(2):
        intent = _insert_planned_intent(store, signal_id=f"sig-{round_index}", client_order_id=f"meta-d{round_index}")
        decision = tm.on_intent_planned(intent, _parsed(), reference_price=Decimal("100"))
        decisions.append(decision)

    # Every attempt fails closed with the same deterministic reason.
    assert all(d.rejected is True for d in decisions)
    assert all(d.trade is None for d in decisions)
    assert registry.calls == 2
    assert client.place_calls == []
    assert store.list_trades() == []

    # Both rejections are durably recorded; no retry bypasses the gate.
    recorded = store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE event_class='domain' "
        "AND event_type='intent.state_changed' AND severity='ERROR' "
        "AND json_extract(payload_json, '$.new_state')='rejected'"
    ).fetchone()[0]
    assert recorded == 2


def test_p0_metadata_unavailable_at_engine_planning_rejects_signal_without_intent(cfg, repo):
    """Engine planning stage (``process_item``) fails closed on missing metadata.

    Production checks symbol metadata right after the reference-price stage
    (engine.py ``symbol_metadata`` stage); an unavailable registry must reject
    the signal durably without ever creating an intent or an order.
    """

    client = _EngineFakeClient()
    engine = _engine(cfg, repo, client)
    store = engine.store
    client.symbols = _DeterministicRegistry()

    record = _store_message(repo, 4, ALERT)
    intent = engine.process_item(_item(100, 4), record)

    # No intent is created at all and nothing reaches the exchange.
    assert intent is None
    assert client.place_calls == []
    assert store.list_trades() == []
    rows = store.db.conn.execute("SELECT status, rejection_reason FROM signals ORDER BY rowid DESC LIMIT 1").fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "rejected"
    assert rows[0]["rejection_reason"] == "symbol metadata unavailable; refusing to trade without exchange constraints"
    intents = store.db.conn.execute("SELECT COUNT(*) AS n FROM order_intents").fetchone()
    assert intents["n"] == 0

    # The rejection is audited in the validate stage.
    audits = store.db.conn.execute(
        "SELECT category, event, detail FROM audit_events WHERE category='validate' AND event='rejected'"
    ).fetchall()
    assert any("symbol metadata unavailable" in str(row["detail"]) for row in audits)


# ---------------------------------------------------------------------------
# Pre-submit revalidation path helpers (real TradingEngine)
# ---------------------------------------------------------------------------
#
# The auto pipeline executes a direct entry inside ``process_item``; the
# dedicated pre-submit gate (``TradingEngine.execute_intent``) is the entry
# point a deferred conditional entry takes when its trigger fires.  A
# conditional intent therefore reaches the gate with ``state == "planned"``
# exactly like production, and a transient metadata failure at trigger time
# must fail closed there.


def _engine(cfg: AppConfig, repo: Repository, client: Any) -> TradingEngine:
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.order_quote_amount_source = "test"
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    # Wire the real trade-lifecycle coordinator over the same store (the
    # production composition), so conditional planning reaches the gate.
    engine.trade_manager = TradeManager(cfg.trading, engine.store, engine=engine, client=client)
    return engine


def _item(channel_id: int, message_id: int) -> QueueItem:
    return QueueItem(
        queue_id=0,
        channel_id=channel_id,
        message_id=message_id,
        item_type="message",
        status="processing",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )


def _store_message(repo: Repository, mid: int, text: str) -> dict:
    msg = FakeMsg(mid, text, date=NOW - timedelta(seconds=20))
    repo.upsert_message(message_to_record(100, msg, "live"))
    record = repo.get_message_raw(100, mid)
    assert record is not None
    return record


def _planned_conditional_intent(engine: TradingEngine, repo: Repository, message_id: int) -> dict:
    """Plan a conditional entry that is still waiting for its trigger."""
    record = _store_message(repo, message_id, ALERT)
    intent = engine.process_item(_item(100, message_id), record)
    assert intent is not None
    assert intent["state"] == "planned", intent
    trade = engine.store.get_trade_by_intent(intent["intent_id"])
    assert trade is not None
    assert trade["trade_state"] == "pending_entry"
    assert trade["entry_mode"] == "conditional"
    return intent


def _audit_event_keys(store: Any) -> list[str]:
    return [
        str(row["event_type"] or row["event"])
        for row in store.db.conn.execute("SELECT event_type, event FROM audit_events ORDER BY audit_id").fetchall()
    ]


def test_p0_metadata_unavailable_at_presubmit_rejects_without_any_order(cfg, repo):
    client = _EngineFakeClient()
    client.symbols = FakeRegistry(FakeMeta(tick_size="0.5"))
    engine = _engine(cfg, repo, client)
    store = engine.store
    intent = _planned_conditional_intent(engine, repo, 1)

    # Metadata becomes unavailable between planning and the trigger firing.
    client.symbols = _DeterministicRegistry()
    result = engine.execute_intent(intent["intent_id"])

    # Fail closed at the pre-submit gate with the exact reason.
    assert result["state"] == "rejected"
    assert result["reject_reason"] == "symbol metadata is unavailable"
    refreshed = store.get_intent(intent["intent_id"])
    assert refreshed is not None
    assert refreshed["state"] == "rejected"
    assert refreshed["reject_reason"] == "symbol metadata is unavailable"

    # Not one order reached the exchange: the conditional trade row still
    # waits unacknowledged in pending_entry with no entry order attached.
    assert client.place_calls == []
    trades = store.list_trades()
    assert len(trades) == 1
    assert trades[0]["trade_state"] == "pending_entry"
    assert trades[0]["entry_order_id"] is None

    # The signal is rejected and the durable domain trail records the gate.
    signal = store.get_signal(intent["signal_id"])
    assert signal is not None
    assert signal["status"] == "rejected"
    assert any("pre_submit_revalidation_rejected" in key for key in _audit_event_keys(store))


def test_p0_metadata_recovery_at_presubmit_rejects_original_and_processes_new_round(cfg, repo):
    client = _EngineFakeClient()
    client.symbols = FakeRegistry(FakeMeta(tick_size="0.5"))
    engine = _engine(cfg, repo, client)
    store = engine.store
    intent = _planned_conditional_intent(engine, repo, 1)

    # Trigger fires while metadata is unavailable: rejected, nothing submitted.
    client.symbols = _DeterministicRegistry()
    rejected = engine.execute_intent(intent["intent_id"])
    assert rejected["state"] == "rejected"
    assert client.place_calls == []

    # Metadata recovers, but the rejected intent is terminal: re-executing it
    # must never submit an order (idempotent rejection, no silent retry).
    client.symbols = FakeRegistry(FakeMeta(tick_size="0.5"))
    again = engine.execute_intent(intent["intent_id"])
    assert again["state"] == "rejected"
    assert client.place_calls == []

    # A fresh alert after recovery plans and submits exactly one LIMIT BUY.
    record = _store_message(repo, 2, ALERT)
    next_intent = engine.process_item(_item(100, 2), record)
    assert next_intent is not None
    assert next_intent["state"] == "planned"
    assert next_intent["intent_id"] != intent["intent_id"]
    submitted = engine.execute_intent(next_intent["intent_id"])
    assert submitted["state"] == "open"
    assert len(client.place_calls) == 1
    call = client.place_calls[0]
    assert call["symbol"] == SYMBOL
    assert call["side"] == "BUY"
    assert call["order_type"] == "LIMIT"

    # The failed attempt stays rejected and was never retried silently.
    refreshed = store.get_intent(intent["intent_id"])
    assert refreshed is not None
    assert refreshed["state"] == "rejected"


def test_p0_order_validation_failure_at_presubmit_rejects_without_any_order(cfg, repo):
    client = _EngineFakeClient()
    client.symbols = FakeRegistry(FakeMeta(tick_size="0.5"))
    engine = _engine(cfg, repo, client)
    store = engine.store
    intent = _planned_conditional_intent(engine, repo, 3)

    # Metadata resolves at trigger time but order validation fails closed.
    client.symbols = FakeRegistry(_BrokenValidateMeta())
    result = engine.execute_intent(intent["intent_id"])

    assert result["state"] == "rejected"
    assert result["reject_reason"] == "symbol validation failed: quantity below minimum 1.0"
    refreshed = store.get_intent(intent["intent_id"])
    assert refreshed is not None
    assert refreshed["state"] == "rejected"
    assert refreshed["reject_reason"] == "symbol validation failed: quantity below minimum 1.0"
    assert client.place_calls == []
    trades = store.list_trades()
    assert len(trades) == 1
    assert trades[0]["trade_state"] == "pending_entry"
    assert trades[0]["entry_order_id"] is None
    assert any("pre_submit_revalidation_rejected" in key for key in _audit_event_keys(store))
