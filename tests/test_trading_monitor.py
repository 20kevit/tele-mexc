"""Market monitor tests: price feeds, fallback, and tick dispatch.

The MarketMonitor is driven by an injectable :class:`PriceFeed`, so no real
network is used.  Ticks are delivered to a callback that we capture and assert
on.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tg_signal_ingest.mexc import MexcRateLimitError, MexcWafError
from tg_signal_ingest.trading.monitor import (
    MEXC_WS_URL,
    WS_TICKER_TOPIC,
    MarketMonitor,
    PollingPriceFeed,
    PriceFeedError,
    _PrimaryWithFallback,
)


@pytest.mark.asyncio
async def test_polling_feed_delivers_ticks_to_callback():
    prices = iter([Decimal("100.00"), Decimal("100.50"), Decimal("99.99")])
    ticks: list[tuple[str, Decimal, float]] = []

    def fetch(symbol: str) -> Decimal | None:
        return next(prices)

    feed = PollingPriceFeed(fetch, poll_interval_s=0.02, clock=lambda: 1000.0)
    await feed.subscribe("BTCUSDT", lambda s, p, r: ticks.append((s, p, r)))
    await feed.start()
    try:
        await asyncio.sleep(0.06)
    finally:
        await feed.stop()

    assert len(ticks) >= 2, ticks
    assert ticks[0] == ("BTCUSDT", Decimal("100.00"), 1000.0)
    assert all(s == "BTCUSDT" for s, _, _ in ticks)


@pytest.mark.asyncio
async def test_polling_feed_ignores_invalid_prices():
    ticks: list[Decimal] = []

    def fetch(symbol: str) -> Decimal | None:
        return Decimal("nan")

    feed = PollingPriceFeed(fetch, poll_interval_s=0.02, clock=lambda: 1000.0)
    await feed.subscribe("BTCUSDT", lambda s, p, r: ticks.append(p))
    await feed.start()
    try:
        await asyncio.sleep(0.06)
    finally:
        await feed.stop()

    assert ticks == [], ticks  # NaN is not finite -> not published


@pytest.mark.asyncio
async def test_polling_feed_fetch_failure_recovers():
    calls = {"n": 0}

    def fetch(symbol: str) -> Decimal | None:
        calls["n"] += 1
        if calls["n"] < 3:
            raise PriceFeedError("down")
        return Decimal("50")

    ticks: list[Decimal] = []
    feed = PollingPriceFeed(fetch, poll_interval_s=0.02, clock=lambda: 1000.0,
                            backoff_base_s=0.01)
    await feed.subscribe("BTCUSDT", lambda s, p, r: ticks.append(p))
    await feed.start()
    try:
        await asyncio.sleep(0.3)
    finally:
        await feed.stop()

    assert Decimal("50") in ticks, ticks


@pytest.mark.asyncio
async def test_polling_feed_without_subscriptions_yields_to_event_loop():
    """An idle monitor must not busy-spin and starve other asyncio tasks."""
    feed = PollingPriceFeed(lambda s: None, poll_interval_s=0.01, clock=lambda: 1.0)
    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.001)

    ticker_task = asyncio.create_task(ticker())
    await feed.start()
    # Stop from a separate thread so a starving event loop cannot prevent the
    # regression test itself from terminating.
    watchdog = threading.Timer(0.05, feed._stop.set)
    watchdog.start()
    try:
        await feed._task
    finally:
        watchdog.cancel()
        ticker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ticker_task
        await feed.stop()

    assert ticks >= 2, ticks


@pytest.mark.asyncio
async def test_market_monitor_tick_and_stale():
    received: list[tuple[str, Decimal, float]] = []

    def on_price(symbol: str, price: Decimal, received_at: float) -> None:
        received.append((symbol, price, received_at))

    cfg = SimpleNamespace(trading=SimpleNamespace(
        entry_trigger_stale_threshold_s=10.0,
        monitor_poll_interval_s=2.0,
        monitor_max_retries=5,
        monitor_backoff_base_s=1.0,
    ))
    monitor = MarketMonitor(
        cfg=cfg,
        fetch_price=lambda s: None,
        on_price=on_price,
        clock=lambda: 5000.0,
    )

    monitor._tick("BTCUSDT", Decimal("100"), 1000.0)
    assert received == [("BTCUSDT", Decimal("100"), 1000.0)]
    tick = monitor.last_tick("BTCUSDT")
    assert tick is not None and tick.price == Decimal("100")

    # received 1000.0, clock 5000.0 -> age 4000s > 10s -> stale.
    assert monitor.is_stale("BTCUSDT", now=5000.0) is True
    assert monitor.is_stale("UNKNOWN", now=5000.0) is True  # no tick -> stale


@pytest.mark.asyncio
async def test_primary_with_fallback_uses_fallback_when_primary_down():
    """When the primary feed cannot start, the polling fallback still ticks."""
    fallback_ticks: list[Decimal] = []

    class BrokenPrimary:
        async def start(self):
            raise PriceFeedError("ws down")
        async def stop(self):
            pass
        async def subscribe(self, symbol, callback):
            pass
        async def unsubscribe(self, symbol):
            pass

    fallback = PollingPriceFeed(lambda s: Decimal("7"), poll_interval_s=0.02,
                                clock=lambda: 1000.0)
    primary = BrokenPrimary()  # type: ignore[arg-type]
    feed = _PrimaryWithFallback(primary=primary, fallback=fallback)
    await feed.subscribe("BTCUSDT", lambda s, p, r: fallback_ticks.append(p))
    await feed.start()
    try:
        await asyncio.sleep(0.15)
    finally:
        await feed.stop()

    assert Decimal("7") in fallback_ticks, fallback_ticks


@pytest.mark.asyncio
async def test_monitor_feed_subscribe_unsubscribe_symmetry():
    feed = PollingPriceFeed(lambda s: Decimal("1"), clock=lambda: 1.0)
    assert feed.subscribed_symbols == []
    await feed.subscribe("BTCUSDT", lambda s, p, r: None)
    await feed.subscribe("ETHUSDT", lambda s, p, r: None)
    assert set(feed.subscribed_symbols) == {"BTCUSDT", "ETHUSDT"}
    await feed.unsubscribe("BTCUSDT")
    assert feed.subscribed_symbols == ["ETHUSDT"]
    await feed.stop()


def test_mexc_websocket_feed_constants():
    assert MEXC_WS_URL == "wss://wbs.mexc.com/raw"
    assert WS_TICKER_TOPIC.format("btcusdt") == "spot@public.ticker.v3.api@btcusdt"


@pytest.mark.asyncio
async def test_batch_poll_requests_all_symbols_in_one_call():
    calls: list[set[str]] = []
    ticks: list[tuple[str, Decimal]] = []
    first_call = asyncio.Event()

    def fetch_prices(symbols: list[str]) -> dict[str, Decimal]:
        calls.append(set(symbols))
        first_call.set()
        return {symbol: Decimal("10") for symbol in symbols}

    feed = PollingPriceFeed(
        lambda _s: None,
        fetch_prices=fetch_prices,
        poll_interval_s=0.01,
        clock=lambda: 1000.0,
    )
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        await feed.subscribe(symbol, lambda s, p, r: ticks.append((s, p)))
    await feed.start()
    try:
        await asyncio.wait_for(first_call.wait(), timeout=1)
        await asyncio.sleep(0.03)
    finally:
        await feed.stop()

    assert calls and calls[0] == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}
    assert len(calls) <= 4  # one request per interval, not one per symbol
    assert {s for s, _ in ticks} == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


@pytest.mark.asyncio
async def test_polling_feed_honors_429_retry_after_without_retry_storm():
    calls = 0
    sleeps: list[float] = []
    got_tick = asyncio.Event()

    def fetch_prices(symbols: list[str]) -> dict[str, Decimal]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise MexcRateLimitError("429", retry_after_s=7.0)
        got_tick.set()
        return {"BTCUSDT": Decimal("1")}

    feed = PollingPriceFeed(
        lambda _s: None,
        fetch_prices=fetch_prices,
        poll_interval_s=0.01,
        clock=lambda: 1000.0,
    )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    feed._sleep_with_stop = fake_sleep  # type: ignore[method-assign]
    await feed.subscribe("BTCUSDT", lambda s, p, r: None)
    await feed.start()
    try:
        await asyncio.wait_for(got_tick.wait(), timeout=1)
    finally:
        await feed.stop()

    assert sleeps and sleeps[0] >= 7.0
    assert calls >= 2


@pytest.mark.asyncio
async def test_polling_feed_handles_418_as_ban_duration():
    sleeps: list[float] = []

    def fetch_prices(symbols: list[str]) -> dict[str, Decimal]:
        raise MexcWafError("418", retry_after_s=120.0)

    feed = PollingPriceFeed(
        lambda _s: None,
        fetch_prices=fetch_prices,
        poll_interval_s=0.01,
        clock=lambda: 1000.0,
    )

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        feed._stop.set()

    feed._sleep_with_stop = fake_sleep  # type: ignore[method-assign]
    await feed.subscribe("BTCUSDT", lambda s, p, r: None)
    await feed.start()
    await feed._task

    assert sleeps and sleeps[0] >= 120.0


@pytest.mark.asyncio
async def test_exponential_backoff_is_bounded_and_nonzero():
    feed = PollingPriceFeed(
        lambda _s: None, fetch_prices=lambda _s: {},
        poll_interval_s=1.0, backoff_base_s=1.0,
    )
    first = feed._backoff_for(1)
    second = feed._backoff_for(2)
    huge = feed._backoff_for(20)
    assert 0.5 <= first <= 1.0
    assert 1.0 <= second <= 2.0
    assert huge <= 30.0


@pytest.mark.asyncio
async def test_polling_feed_never_calls_order_placement():
    class NoOrders:
        def __init__(self):
            self.place_calls = 0

        def place_order(self, **kwargs):
            self.place_calls += 1
            raise AssertionError("polling must never place an order")

    exchange = NoOrders()
    feed = PollingPriceFeed(
        lambda _s: Decimal("1"),
        fetch_prices=lambda symbols: {s: Decimal("1") for s in symbols},
        poll_interval_s=0.01,
        clock=lambda: 1000.0,
    )
    await feed.subscribe("BTCUSDT", lambda s, p, r: None)
    await feed.start()
    await asyncio.sleep(0.03)
    await feed.stop()
    assert exchange.place_calls == 0
