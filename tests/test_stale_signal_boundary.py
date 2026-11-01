"""Stale-signal prevention across downtime and restarts.

Every scenario uses a disposable SQLite database and fake Telegram/MEXC
interfaces.  No test can reach a real exchange or place a real order.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import FakeMsg
from tests.test_service import FakeClient
from tests.test_submission_gate import GatedClient, _planned_intent
from tests.test_submission_gate import _engine as _gated_engine
from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.service import IngestService
from tg_signal_ingest.trading.gate import SubmissionGate
from tg_signal_ingest.trading.store import TradingStore

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


class _FakeTrading:
    def __init__(self, cfg, repo):
        self.store = TradingStore(repo.db, cfg.trading)
        self.notifier = None
        self.trade_manager = None
        self.runtime: dict = {}

    def set_runtime_readiness(self, **callbacks):
        self.runtime.update(callbacks)

    def reconcile_pending(self):
        return None

    def resume_planned_intents(self, limit=100):
        return 0


def _service(cfg, repo, client) -> IngestService:
    svc = IngestService(
        cfg,
        repo,
        client=client,
        backfill_runner=None,
        trading_engine=_FakeTrading(cfg, repo),
    )

    async def _stop_immediately() -> None:
        return None

    svc._start_background_work = lambda: None  # type: ignore[method-assign]
    svc._wait_until_stopped_or_disconnected = _stop_immediately  # type: ignore[method-assign]
    return svc


def _select(repo, channel_id: int = 100) -> None:
    repo.ensure_channel(channel_id, 123, f"sig{channel_id}", f"Sig {channel_id}")
    repo.select_channel(channel_id)


def _store(repo, channel_id: int, message_id: int, text: str = ALERT, source: str = "live", when=None):
    msg = FakeMsg(message_id, text)
    # Default to a message inside the signal-validity window; historical cases
    # must pass an explicit ``when``.
    msg.date = when if when is not None else NOW - timedelta(seconds=30)
    repo.upsert_message(message_to_record(channel_id, msg, source))
    return repo.get_message_raw(channel_id, message_id)


def test_boundaries_are_independent_per_channel(cfg, repo):
    for channel_id in (100, 200):
        repo.ensure_channel(channel_id, channel_id + 1, f"sig{channel_id}", f"Sig {channel_id}")

    store = TradingStore(repo.db, cfg.trading)
    assert store.ensure_activation_boundary(100, minimum=500) == 500
    assert store.ensure_activation_boundary(200, minimum=900) == 900
    assert store.activation_boundary(100) == 500
    assert store.activation_boundary(200) == 900

    engine = _engine(cfg, repo, FakeMexc())
    engine.store = store
    # Channel 100 message 501 is new; channel 200 message 901 is new.
    assert engine.process_item(_item(100, 500), _store(repo, 100, 500)) is None
    assert engine.process_item(_item(200, 900), _store(repo, 200, 900)) is None
    assert engine.store.list_intents(limit=10) == []


@pytest.mark.parametrize("downtime_hours", [1, 6, 24])
def test_multi_message_downtime_recovery_never_creates_an_intent(cfg, repo, downtime_hours):
    _select(repo)
    store = TradingStore(repo.db, cfg.trading)
    store.ensure_activation_boundary(100, minimum=100)  # last pre-downtime message
    started = NOW - timedelta(hours=downtime_hours)

    engine = _engine(cfg, repo, FakeMexc())
    engine.store = store
    for offset in range(1, 6):
        record = _store(repo, 100, 100 + offset, source="recovery", when=started + timedelta(minutes=offset))
        assert engine.process_item(_item(100, 100 + offset), record) is None

    assert engine.store.list_intents(limit=20) == []


@pytest.mark.parametrize("downtime_hours", [1, 6, 24])
def test_delayed_telegram_delivery_above_old_boundary_is_rejected(cfg, repo, downtime_hours):
    """A queued update delivered as ``live`` must be rejected when the startup
    boundary already covers Telegram's newest visible message id."""

    _select(repo)
    store = TradingStore(repo.db, cfg.trading)
    # Startup snapshot saw Telegram's latest id 110, even though messages
    # 101-110 were never stored before the handler was registered.
    store.ensure_activation_boundary(100, minimum=110)
    engine = _engine(cfg, repo, FakeMexc())
    engine.store = store
    delivered = NOW - timedelta(hours=downtime_hours)

    for message_id in range(101, 111):
        record = _store(repo, 100, message_id, when=delivered + timedelta(seconds=message_id))
        assert engine.process_item(_item(100, message_id), record) is None

    assert engine.store.list_intents(limit=20) == []


def test_backfill_during_startup_is_never_tradeable(cfg, repo):
    _select(repo)
    store = TradingStore(repo.db, cfg.trading)
    store.ensure_activation_boundary(100, minimum=100)
    engine = _engine(cfg, repo, FakeMexc())
    engine.store = store

    for message_id in range(101, 104):
        record = _store(repo, 100, message_id, source="backfill", when=NOW - timedelta(days=2))
        assert engine.process_item(_item(100, message_id), record) is None

    assert engine.store.list_intents(limit=20) == []


def test_historical_edit_and_duplicate_delivery_cannot_create_a_second_order(cfg, repo):
    _select(repo)
    engine = _engine(cfg, repo, FakeMexc())
    engine.store.ensure_activation_boundary(100, minimum=10)

    # Editing a historical message (id <= boundary) stays non-tradeable.
    record = _store(repo, 100, 10, when=NOW - timedelta(hours=2))
    assert engine.process_item(_item(100, 10, item_type="edit"), record) is None
    assert engine.store.list_intents(limit=10) == []

    # A genuinely new message can trade once, and a duplicate delivery cannot
    # create a second intent or a second exchange write.
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    engine.store.ensure_activation_boundary(100, minimum=10)
    fresh = _store(repo, 100, 11)
    first = engine.process_item(_item(100, 11), fresh)
    assert first is not None
    assert client.place_calls == 1
    second = engine.process_item(_item(100, 11), fresh)
    assert second is not None and second["intent_id"] == first["intent_id"]
    assert client.place_calls == 1
    assert len(engine.store.list_intents(limit=10)) == 1


def test_restart_with_planned_unsubmitted_intent_cancels_locally(cfg, repo):
    _select(repo)
    client = GatedClient()
    engine = _gated_engine(cfg, repo, client)
    intent = _planned_intent(engine, 1)
    # Boundary advanced by a restart after the plan was created.
    engine.store.ensure_activation_boundary(100, minimum=1)

    assert engine.resume_planned_intents() == 0
    stored = engine.store.get_intent(intent["intent_id"])
    assert stored is not None and stored["state"] == "canceled_local"
    assert client.place_calls == []
    assert engine.store.activation_boundary(100) == 1


def test_restart_with_uncertain_submission_never_resubmits(cfg, repo):
    from tg_signal_ingest.mexc.errors import MexcNetworkError

    _select(repo)
    client = FakeMexc(place_error=MexcNetworkError("timeout after send"))
    engine = _engine(cfg, repo, client)
    engine.store.ensure_activation_boundary(100, minimum=0)
    record = _store(repo, 100, 1)

    first = engine.process_item(_item(100, 1), record)
    assert first is not None and first["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 1

    # A restarted process may query authoritative state but must never resend.
    restarted = _engine(cfg, repo, client)
    restarted.store.ensure_activation_boundary(100, minimum=1)
    restarted.reconcile_pending()
    assert client.place_calls == 1
    stored = restarted.store.get_intent(first["intent_id"])
    assert stored is not None and stored["state"] != "planned"


def test_restart_with_open_order_reconciles_without_a_new_buy(cfg, repo):
    _select(repo)
    open_order = {
        "orderId": "ex-open-restart",
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
    client = FakeMexc(place_response={"orderId": "ex-open-restart", "status": "NEW"}, query_results=[open_order])
    engine = _engine(cfg, repo, client)
    engine.store.ensure_activation_boundary(100, minimum=0)
    created = engine.process_item(_item(100, 1), _store(repo, 100, 1))
    assert created is not None and client.place_calls == 1

    restarted = _engine(cfg, repo, client)
    restarted.store.ensure_activation_boundary(100, minimum=1)
    restarted.reconcile_pending()
    assert client.place_calls == 1
    stored = restarted.store.get_intent(created["intent_id"])
    assert stored is not None and stored["state"] in ("open", "submitted")


@pytest.mark.parametrize("failure", ["exception", "missing"])
async def test_startup_boundary_failure_fails_closed(cfg, repo, failure):
    _select(repo)

    class BrokenClient(FakeClient):
        async def get_messages(self, ref, limit=..., **kw):
            raise RuntimeError("telegram unavailable")

    class MissingClient(FakeClient):
        get_messages = None

    client = BrokenClient() if failure == "exception" else MissingClient()
    svc = _service(cfg, repo, client)
    await svc._run_once()

    assert svc._boundary_ready is False
    assert cfg  # silence unused fixture linters
    assert svc.trading.store.activation_boundary(100) is None
    callbacks = getattr(svc.trading, "runtime", {})
    assert "activation_boundary" in callbacks
    assert callbacks["activation_boundary"]() is False


def test_boundary_callback_does_not_create_a_global_gate(cfg, repo):
    from tests.test_submission_gate import _ready

    _ready(repo)
    gate = SubmissionGate(repo.db, cfg)
    gate.runtime["activation_boundary"] = lambda: False
    with repo.db.transaction() as conn:
        assert gate.check(conn) is None  # stale-message safety is enforced per message

    gate.runtime["activation_boundary"] = lambda: True
    with repo.db.transaction() as conn:
        assert gate.check(conn) is None
