"""Activation-boundary arming at service startup and reconnect (offline)."""

from __future__ import annotations

from tests.conftest import FakeMsg
from tests.test_service import FakeChannelEntity, FakeClient
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.service import IngestService
from tg_signal_ingest.trading.store import TradingStore


class _FakeTrading:
    def __init__(self, cfg, repo):
        self.store = TradingStore(repo.db, cfg.trading)
        self.trade_manager = None

    def reconcile_pending(self):
        return None


def _service(cfg, repo) -> tuple[IngestService, FakeClient]:
    cfg.trading.mode = "live"
    client = FakeClient(entity=FakeChannelEntity())
    svc = IngestService(
        cfg,
        repo,
        client=client,
        backfill_runner=None,
        trading_engine=_FakeTrading(cfg, repo),
    )

    def _no_background() -> None:
        return None

    async def _stop_immediately() -> None:
        return None

    svc._start_background_work = _no_background  # type: ignore[method-assign]
    svc._wait_until_stopped_or_disconnected = _stop_immediately  # type: ignore[method-assign]
    return svc, client


async def test_startup_arms_boundary_at_newest_stored_message(cfg, repo):
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    repo.upsert_message(message_to_record(100, FakeMsg(500, "old signal"), "live"))

    svc, _ = _service(cfg, repo)
    await svc._run_once()

    assert svc.trading.store.activation_boundary() == 500


async def test_reconnect_advances_boundary_forward_only(cfg, repo):
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    repo.upsert_message(message_to_record(100, FakeMsg(500, "old signal"), "live"))

    svc, _ = _service(cfg, repo)
    await svc._run_once()
    assert svc.trading.store.activation_boundary() == 500

    repo.upsert_message(message_to_record(100, FakeMsg(501, "while down"), "recovery"))
    await svc._run_once()
    assert svc.trading.store.activation_boundary() == 501

    # A persisted boundary that is already ahead must never be rewound.
    svc.trading.store.ensure_activation_boundary(100, minimum=900)
    await svc._run_once()
    assert svc.trading.store.activation_boundary() == 900


async def test_startup_boundary_does_not_skip_message_delivered_after_capture(cfg, repo):
    """A message stored by a live handler after the pre-handler snapshot stays tradeable."""

    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    repo.upsert_message(message_to_record(100, FakeMsg(500, "old signal"), "live"))

    svc, _ = _service(cfg, repo)
    # Simulate the tiny window between snapshot and handler delivery: the live
    # row is stored after the snapshot but before recovery/boundary arming.
    register = svc._register_handlers

    def register_then_deliver() -> None:
        register()
        repo.upsert_message(message_to_record(100, FakeMsg(501, "fresh live"), "live"))

    svc._register_handlers = register_then_deliver  # type: ignore[method-assign]
    await svc._run_once()

    assert svc.trading.store.activation_boundary() == 500
    assert svc.trading.store.activation_boundary() < 501


async def test_backfill_and_recovery_rows_above_boundary_stay_non_tradeable(cfg, repo):
    repo.ensure_channel(100, 123, "sigchan", "Sig Chan")
    repo.select_channel(100)
    repo.upsert_message(message_to_record(100, FakeMsg(500, "old signal"), "live"))

    svc, _ = _service(cfg, repo)
    await svc._run_once()
    boundary = svc.trading.store.activation_boundary()
    assert boundary == 500

    repo.upsert_message(message_to_record(100, FakeMsg(600, "backfill"), "backfill"))
    repo.upsert_message(message_to_record(100, FakeMsg(601, "recovered"), "recovery"))
    await svc._run_once()
    # The startup snapshot includes both, but source gating remains the
    # primary guard for recovered/backfilled rows.
    assert svc.trading.store.activation_boundary() == 601
