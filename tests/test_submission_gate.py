"""Deterministic pause/readiness tests at the actual submission boundary.

Every exchange is a fake.  The concurrency test uses barriers, not sleeps, to
prove that a pause acknowledgement cannot overtake an in-flight submission and
that no submission can start after the acknowledgement until resume.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tests._trading_fakes import FakeClient, FakeMeta, FakeNotifier, FakeRegistry
from tg_signal_ingest.config import AppConfig
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.repository import QueueItem, Repository
from tg_signal_ingest.trading.engine import TradingEngine
from tg_signal_ingest.trading.gate import (
    SubmissionBlocked,
    SubmissionGate,
)
from tg_signal_ingest.trading.parser import SignalParser
from tg_signal_ingest.trading.trade_manager import TradeManager
from tg_signal_ingest.trading.trade_store import TradeStore

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
ALERT = "BUY BTCUSDT\nENTRY: 100\nSL: 90\nTP1: 110\nTP2: 120"


class GatedClient(FakeClient):
    """Fake Spot client that can hold a submission at the exchange boundary."""

    def __init__(self):
        super().__init__(FakeRegistry(FakeMeta(tick_size="0.01")))
        self.entered = threading.Event()
        self.release = threading.Event()
        self.hold = False
        self.account_calls = 0

    def account(self):
        self.account_calls += 1
        return {
            "canTrade": True,
            "permissions": ["SPOT"],
            "balances": [
                {"asset": "USDT", "free": "1000", "locked": "0", "available": "1000"},
                {"asset": "BTC", "free": "10", "locked": "0", "available": "10"},
            ],
        }

    def ticker_price(self, symbol=None):
        return {"symbol": symbol, "price": "100"}

    def place_order(self, **kwargs):
        if self.hold:
            self.entered.set()
            assert self.release.wait(timeout=5), "test did not release the fake submission"
        return super().place_order(**kwargs)

    def place_oco(self, **kwargs):
        return self.place_order(
            symbol=kwargs.get("symbol"),
            side="SELL",
            order_type="OCO_STOP_LIMIT_ORDER",
            quantity=kwargs.get("quantity"),
            price=kwargs.get("price"),
            client_order_id=kwargs.get("client_order_id"),
        )


def _engine(cfg: AppConfig, repo: Repository, client: GatedClient) -> TradingEngine:
    cfg.trading.order_quote_amount = 10.0
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW)
    engine.store = store
    return engine


def _planned_intent(engine: TradingEngine, message_id: int) -> dict:
    parsed = SignalParser().parse(ALERT)
    sid, _ = engine.store.record_signal(parsed, channel_id=100, message_id=message_id, edit_version=0)
    intent, _ = engine.store.create_intent(
        signal=parsed,
        signal_id=sid,
        channel_id=100,
        message_id=message_id,
        edit_version=0,
        idempotency_key=f"idem-{message_id}",
        client_order_id=f"tsi-{message_id}",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    return intent


def _set_pause(repo: Repository, paused: bool) -> None:
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE control_setup SET paused = ?, pause_reason = ? WHERE id = 1",
            (1 if paused else 0, "owner paused execution" if paused else None),
        )


def test_pause_waits_for_inflight_then_blocks_every_new_submission(cfg, repo):
    client = GatedClient()
    engine = _engine(cfg, repo, client)
    first = _planned_intent(engine, 1)
    second = _planned_intent(engine, 2)

    client.hold = True
    paused_ack = threading.Event()
    errors: list[BaseException] = []

    def submit(intent_id: str) -> None:
        try:
            engine._execute_intent(intent_id)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion
            errors.append(exc)

    worker = threading.Thread(target=submit, args=(first["intent_id"],))
    worker.start()
    assert client.entered.wait(timeout=5), "submission never reached the fake exchange"

    def pause() -> None:
        gate = SubmissionGate(repo.db, cfg)
        with gate.exclusive(timeout_s=10):
            _set_pause(repo, True)
            paused_ack.set()

    pauser = threading.Thread(target=pause)
    pauser.start()
    # The pause acknowledgement must not be observable while the submission is
    # still inside the exchange call.
    assert not paused_ack.wait(timeout=0.2)
    client.release.set()
    worker.join(timeout=5)
    pauser.join(timeout=5)
    assert not errors, errors
    assert paused_ack.is_set()

    # After the acknowledgement no new submission may reach the exchange.
    client.hold = False
    before = len(client.place_calls)
    result = engine._execute_intent(second["intent_id"])
    assert result["state"] == "planned"
    assert len(client.place_calls) == before
    assert any(
        row["event"] == "submission_blocked"
        for row in repo.db.conn.execute(
            "SELECT event FROM audit_events WHERE correlation = ?", (second["intent_id"],)
        ).fetchall()
    )

    # Authorized resume (through the same exclusive gate) reopens the boundary.
    gate = SubmissionGate(repo.db, cfg)
    with gate.exclusive(timeout_s=10):
        _set_pause(repo, False)
    resumed = engine._execute_intent(second["intent_id"])
    assert resumed["state"] in ("submitting", "submitted", "open")
    assert len(client.place_calls) == before + 1


def test_resume_planned_intents_is_gated_and_conditional_entries_stay_deferred(cfg, repo):
    client = GatedClient()
    engine = _engine(cfg, repo, client)
    engine.trade_manager = TradeManager(cfg.trading, engine.store, engine=engine, client=client)
    intent = _planned_intent(engine, 3)
    _set_pause(repo, True)

    assert engine.resume_planned_intents() == 0
    assert client.place_calls == []

    _set_pause(repo, False)
    assert engine.resume_planned_intents() == 0
    # Without a trade lifecycle row it is deliberately not submitted.
    assert client.place_calls == []
    stored = engine.store.get_intent(intent["intent_id"])
    assert stored is not None and stored["state"] == "planned"


def _ready(repo: Repository) -> None:
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE control_setup SET telegram_state='authenticated', "
            "mexc_state='verified', selected_channel_id=100, "
            "learning_state='completed', paused=0 WHERE id=1"
        )


def _gate(cfg: AppConfig, repo: Repository) -> SubmissionGate:
    return SubmissionGate(repo.db, cfg, require_control_setup=True)


def _reason(cfg: AppConfig, repo: Repository) -> str | None:
    gate = _gate(cfg, repo)
    with repo.db.transaction() as conn:
        return gate.check(conn)


@pytest.mark.parametrize(
    ("column", "value", "expected"),
    [
        ("telegram_state", "unauthenticated", "Telegram"),
        ("mexc_state", "not_configured", "MEXC"),
        ("selected_channel_id", None, "channel"),
        ("learning_state", "in_progress", "learning"),
    ],
)
def test_readiness_prerequisites_are_checked_individually(cfg, repo, column, value, expected):
    _ready(repo)
    with repo.db.transaction() as conn:
        conn.execute(f"UPDATE control_setup SET {column} = ? WHERE id = 1", (value,))
    reason = _reason(cfg, repo)
    assert reason is not None and expected.lower() in reason.lower()


def test_readiness_combined_failures_and_state_transitions(cfg, repo):
    _ready(repo)
    assert _reason(cfg, repo) is None

    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE control_setup SET telegram_state='unauthenticated', mexc_state='not_configured' WHERE id = 1"
        )
    reason = _reason(cfg, repo)
    assert reason is not None and "MEXC" in reason  # first deterministic blocker

    _ready(repo)
    _set_pause(repo, True)
    assert "paused" in (_reason(cfg, repo) or "").lower()
    _set_pause(repo, False)
    assert _reason(cfg, repo) is None

    cfg.trading.emergency_stop = True
    assert "emergency" in (_reason(cfg, repo) or "").lower()
    cfg.trading.emergency_stop = False

    cfg.trading.execution_acknowledgement = "wrong"
    assert "acknowledgement" in (_reason(cfg, repo) or "").lower()


def test_unresolved_reconciliation_blocks_entries_but_not_protection(cfg, repo):
    _ready(repo)
    client = GatedClient()
    engine = _engine(cfg, repo, client)
    intent = _planned_intent(engine, 9)
    with repo.db.transaction() as conn:
        conn.execute(
            "UPDATE order_intents SET state='unknown_requires_reconciliation' WHERE intent_id=?",
            (intent["intent_id"],),
        )
    reason = _reason(cfg, repo)
    assert reason is not None and "reconciliation" in reason.lower()
    gate = _gate(cfg, repo)
    with repo.db.transaction() as conn:
        assert gate.check(conn, purpose="protection") is None


def test_unreadable_pause_state_fails_closed(cfg, repo):
    _ready(repo)
    repo.db.conn.execute("DROP TABLE control_setup")
    gate = _gate(cfg, repo)
    with pytest.raises(SubmissionBlocked, match="readiness check failed"), gate.submission(purpose="entry"):
        pass


def test_protection_and_entry_routes_share_the_pause_boundary(cfg, repo):
    client = GatedClient()
    cfg.trading.order_quote_amount = 10.0
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    tm = TradeManager(cfg.trading, store, engine=None, client=client, notifier=FakeNotifier())
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, trade_manager=tm)
    tm.engine = engine
    engine.store = store
    trade = store.create_trade(
        trade_id="trade-gate",
        signal_id="sig-gate",
        idempotency_key="idem-gate",
        channel_id=100,
        message_id=4,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="direct",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("100"),
        sl_price=Decimal("90"),
        entry_intent_id=None,
    )
    _set_pause(repo, True)
    for call in (
        lambda: engine.submit_sell_limit(trade, "tp-1", Decimal("110"), Decimal("0.05"), kind="tp1"),
        lambda: engine.submit_oco_sell(trade, "oco-1", Decimal("110"), Decimal("0.05"), Decimal("90"), kind="tp1"),
    ):
        with pytest.raises(SubmissionBlocked):
            call()
    assert client.place_calls == []


def test_runtime_callbacks_are_authoritative_over_persisted_state(cfg, repo):
    _ready(repo)
    gate = _gate(cfg, repo)

    # Persisted Telegram is authenticated, but the installed live callback is
    # false: the dimension must fail.
    gate.runtime["telegram"] = lambda: False
    with repo.db.transaction() as conn:
        assert "Telegram" in (gate.check(conn) or "")

    # Persisted MEXC is verified, but the installed live callback is false.
    gate.runtime.pop("telegram")
    gate.runtime["mexc"] = lambda: False
    with repo.db.transaction() as conn:
        assert "MEXC" in (gate.check(conn) or "")

    # Persisted channel is selected, but the installed live callback is false.
    gate.runtime.pop("mexc")
    gate.runtime["channel"] = lambda: False
    with repo.db.transaction() as conn:
        assert "channel" in (gate.check(conn) or "").lower()

    # Callback true is equally authoritative over stale negative persistence.
    for column in ("telegram_state", "mexc_state"):
        with repo.db.transaction() as conn:
            conn.execute(
                f"UPDATE control_setup SET {column} = ? WHERE id = 1",
                ("unauthenticated" if column == "telegram_state" else "not_configured",),
            )
    with repo.db.transaction() as conn:
        conn.execute("UPDATE control_setup SET selected_channel_id = NULL WHERE id = 1")
    gate.runtime["telegram"] = lambda: True
    gate.runtime["mexc"] = lambda: True
    gate.runtime["channel"] = lambda: True
    with repo.db.transaction() as conn:
        assert gate.check(conn) is None


@pytest.mark.parametrize("dimension", ["mexc", "telegram", "channel"])
def test_readiness_callback_exception_fails_closed(cfg, repo, dimension):
    _ready(repo)
    gate = _gate(cfg, repo)

    def boom():
        raise RuntimeError("readiness source unavailable")

    gate.runtime[dimension] = boom
    with repo.db.transaction() as conn:
        reason = gate.check(conn)
    assert reason is not None and "unreadable" in reason.lower()


def test_no_runtime_callback_uses_documented_persisted_fallback(cfg, repo):
    _ready(repo)
    gate = _gate(cfg, repo)
    with repo.db.transaction() as conn:
        assert gate.check(conn) is None

    with repo.db.transaction() as conn:
        conn.execute("UPDATE control_setup SET telegram_state='unauthenticated' WHERE id = 1")
    with repo.db.transaction() as conn:
        assert "Telegram" in (gate.check(conn) or "")


def test_runtime_readiness_flip_blocks_the_next_submission_immediately(cfg, repo):
    _ready(repo)
    gate = _gate(cfg, repo)
    state = {"telegram": True, "mexc": True, "channel": True}
    gate.runtime.update({name: (lambda n=name: state[n]) for name in state})

    with repo.db.transaction() as conn:
        assert gate.check(conn) is None
    state["telegram"] = False
    with repo.db.transaction() as conn:
        assert "Telegram" in (gate.check(conn) or "")

    # False -> true progresses only when every other prerequisite also passes.
    state["telegram"] = True
    with repo.db.transaction() as conn:
        assert gate.check(conn) is None
    with repo.db.transaction() as conn:
        conn.execute("UPDATE control_setup SET learning_state='in_progress' WHERE id = 1")
    with repo.db.transaction() as conn:
        assert "learning" in (gate.check(conn) or "").lower()


def test_engine_submission_flip_blocks_the_next_order(cfg, repo):
    _ready(repo)
    client = GatedClient()
    gate = SubmissionGate(repo.db, cfg, require_control_setup=True)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, submission_gate=gate)
    engine.store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    state = {"telegram": True, "mexc": True, "channel": True}
    gate.runtime.update({name: (lambda n=name: state[n]) for name in state})

    first = _planned_intent(engine, 70)
    assert engine._execute_intent(first["intent_id"])["state"] in (
        "submitting",
        "submitted",
        "open",
    )
    assert len(client.place_calls) == 1

    state["mexc"] = False
    second = _planned_intent(engine, 71)
    blocked = engine._execute_intent(second["intent_id"])
    assert blocked["state"] == "planned"
    assert len(client.place_calls) == 1


def test_mark_mexc_verified_false_does_not_remove_callback(cfg, repo):
    _ready(repo)  # persisted MEXC is verified
    client = GatedClient()
    gate = SubmissionGate(repo.db, cfg, require_control_setup=True)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, submission_gate=gate)
    engine.mark_mexc_verified(True)
    engine.mark_mexc_verified(False)
    assert "mexc" in engine.submission_gate.runtime
    with repo.db.transaction() as conn:
        assert "MEXC" in (engine.submission_gate.check(conn) or "")


def test_entry_submission_requires_complete_setup_at_the_claim(cfg, repo):
    client = GatedClient()
    cfg.trading.order_quote_amount = 10.0
    store = TradeStore(repo.db, cfg.trading, now=lambda: NOW)
    gate = SubmissionGate(repo.db, cfg, require_control_setup=True)
    engine = TradingEngine(cfg, repo, client=client, now=lambda: NOW, submission_gate=gate)
    engine.store = store
    intent = _planned_intent(engine, 60)

    blocked = engine._execute_intent(intent["intent_id"])
    assert blocked["state"] == "planned"
    assert client.place_calls == []
    audit = repo.db.conn.execute(
        "SELECT event, detail FROM audit_events WHERE correlation=? AND event='submission_blocked'",
        (intent["intent_id"],),
    ).fetchone()
    assert audit is not None and "MEXC" in audit["detail"]

    _ready(repo)
    submitted = engine._execute_intent(intent["intent_id"])
    assert submitted["state"] in ("submitting", "submitted", "open")
    assert len(client.place_calls) == 1


def test_entry_via_queue_item_is_paused_before_the_exchange(cfg, repo):
    client = GatedClient()
    engine = _engine(cfg, repo, client)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    repo.upsert_message(message_to_record(100, _Msg(50, ALERT), "live"))
    record = repo.get_message_raw(100, 50)
    _set_pause(repo, True)
    result = engine.process_item(_item(100, 50), record)
    assert result is not None and result["state"] == "planned"
    assert client.place_calls == []
    assert client.account_calls == 0


class _Msg:
    def __init__(self, mid: int, text: str):
        self.id = mid
        self.message = text
        self.date = NOW
        self.edit_date = None
        self.media = None

    def to_dict(self):
        return {}


def _item(channel_id: int, message_id: int) -> QueueItem:
    return QueueItem(
        queue_id=0,
        channel_id=channel_id,
        message_id=message_id,
        item_type="message",
        status="pending",
        targets_version=0,
        attempts=0,
        available_at="",
        created_at="",
    )
