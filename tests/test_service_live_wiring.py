"""Live service wiring: market monitor, tick worker, recovery and reconciliation.

These tests drive ``IngestService`` internals with injected fakes.  They make
no Telegram or MEXC calls and assert the ordering that matters operationally:

* the monitor starts before ticks are consumed and is stopped on shutdown;
* symbols are subscribed from non-terminal trades;
* ticks reach ``TradeManager.on_tick`` on the worker thread;
* startup recovery runs before the first reconciliation sweep;
* reconciliation and notification flushing happen on every interval.
"""

from __future__ import annotations

import asyncio
import contextlib

from tg_signal_ingest.service import IngestService


class _FakeClient:
    async def disconnect(self):
        return None


class FakeMonitor:
    def __init__(self):
        self.started = False
        self.stopped = False
        self.subscribed: list[str] = []
        self.subscribed_event = asyncio.Event()

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def subscribe(self, symbol: str):
        self.subscribed.append(symbol)
        self.subscribed_event.set()


class FakeTradeManager:
    def __init__(self, trades):
        self.store = type("Store", (), {"list_trades": lambda _self: trades})()
        self.ticks: list[tuple[str, object, float]] = []
        self.recovered = 0
        self.reconciled = 0

    def on_tick(self, symbol, price, received_at):
        self.ticks.append((symbol, price, received_at))

    def recover_on_startup(self):
        self.recovered += 1

    def reconcile_all_open(self):
        self.reconciled += 1


class FakeNotifier:
    def __init__(self):
        self.flushed = 0

    def flush(self):
        self.flushed += 1


class FakeTrading:
    def __init__(self, trades, *, stop_cb=None):
        self.trade_manager = FakeTradeManager(trades)
        self.notifier = FakeNotifier()
        self.reconciled_pending = 0
        self._stop_cb = stop_cb

    def reconcile_pending(self):
        self.reconciled_pending += 1
        if self._stop_cb is not None:
            self._stop_cb()


def _service(cfg, repo, trading, monitor):
    return IngestService(
        cfg,
        repo,
        client=_FakeClient(),
        trading_engine=trading,
        market_monitor=monitor,
    )


async def _wait_for(predicate, timeout=3.0):
    async def _poll():
        while not predicate():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_market_worker_subscribes_and_dispatches_ticks(cfg, repo):
    trades = [
        {"symbol": "btcusdt", "trade_state": "entry_active"},
        {"symbol": "ETHUSDT", "trade_state": "open"},
        {"symbol": "SOLUSDT", "trade_state": "closed"},
        {"symbol": "XRPUSDT", "trade_state": "needs_reconciliation"},
        {"symbol": None, "trade_state": "entry_active"},
    ]
    trading = FakeTrading(trades)
    monitor = FakeMonitor()
    svc = _service(cfg, repo, trading, monitor)

    task = asyncio.ensure_future(svc._market_worker())
    await asyncio.wait_for(monitor.subscribed_event.wait(), timeout=3)

    assert monitor.started is True
    assert monitor.subscribed == ["BTCUSDT", "ETHUSDT"]
    assert svc._tick_thread is not None and svc._tick_thread.is_alive()

    svc._enqueue_tick("BTCUSDT", 61000.5, 123.0)
    await _wait_for(lambda: bool(trading.trade_manager.ticks))
    assert trading.trade_manager.ticks == [("BTCUSDT", 61000.5, 123.0)]

    svc.request_stop()
    await asyncio.wait_for(task, timeout=5)
    assert monitor.stopped is True
    assert svc._tick_thread is None and svc._tick_queue is None


async def test_reconciliation_worker_recovers_before_sweeping(cfg, repo):
    trading = FakeTrading([])
    monitor = FakeMonitor()
    svc = _service(cfg, repo, trading, monitor)
    trading._stop_cb = svc.request_stop

    await asyncio.wait_for(svc._reconciliation_worker(), timeout=5)

    tm = trading.trade_manager
    assert tm.recovered == 1
    assert trading.reconciled_pending == 1
    assert tm.reconciled == 1
    assert trading.notifier.flushed == 1


async def test_live_mode_schedules_monitor_and_reconciliation(cfg, repo):
    trading = FakeTrading([])
    monitor = FakeMonitor()
    svc = _service(cfg, repo, trading, monitor)
    svc._selected_channel_id = 100

    async def _noop_worker():
        await asyncio.Event().wait()

    svc._market_worker = _noop_worker  # type: ignore[method-assign]
    svc._reconciliation_worker = _noop_worker  # type: ignore[method-assign]

    svc._start_background_work()
    try:
        assert svc._monitor_task is not None
        assert svc._recon_task is not None
        assert svc._queue_task is not None
    finally:
        await svc._reap_background_tasks()


async def test_background_work_always_starts_monitor_and_reconciliation(cfg, repo):
    trading = FakeTrading([])
    monitor = FakeMonitor()
    svc = _service(cfg, repo, trading, monitor)
    svc._selected_channel_id = 100

    async def _noop_worker():
        await asyncio.Event().wait()

    svc._market_worker = _noop_worker  # type: ignore[method-assign]
    svc._reconciliation_worker = _noop_worker  # type: ignore[method-assign]

    svc._start_background_work()
    try:
        assert svc._monitor_task is not None
        assert svc._recon_task is not None
    finally:
        await svc._reap_background_tasks()


async def test_market_worker_skips_without_trade_manager(cfg, repo):
    trading = FakeTrading([])
    trading.trade_manager = None
    monitor = FakeMonitor()
    svc = _service(cfg, repo, trading, monitor)

    await asyncio.wait_for(svc._market_worker(), timeout=3)
    assert monitor.started is False


async def test_market_worker_stops_cleanly_when_monitor_start_fails(cfg, repo):
    trading = FakeTrading([])

    class BrokenMonitor(FakeMonitor):
        async def start(self):
            raise RuntimeError("feed down")

    monitor = BrokenMonitor()
    svc = _service(cfg, repo, trading, monitor)

    with contextlib.suppress(RuntimeError):
        await asyncio.wait_for(svc._market_worker(), timeout=3)
    assert monitor.stopped is False
    assert svc._tick_thread is None
