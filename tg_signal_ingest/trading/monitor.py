"""Reliable market-data monitoring for conditional entry, TP and SL triggers.

Design goals
------------
* Never activate an order from a stale, disconnected, malformed or
  inconsistent price observation.
* Prefer WebSocket market data for timely trigger detection, with a documented
  REST polling fallback so a single interrupted WebSocket connection never
  stalls monitoring forever.
* Bounded retries with exponential backoff + jitter; respect throttling.
* Deterministic in tests: the monitor is driven by an injectable
  :class:`PriceFeed`, and the :class:`TradeManager` is driven by direct
  ``on_tick`` calls so every price stream, gap and disconnect is simulated.

MEXC Spot interfaces used (official, documented):
* ``GET /api/v3/ticker/price`` — last price by symbol (weight 1, unauthenticated).
* WebSocket ``wss://wbs.mexc.com/raw`` — ``spot@public.ticker.v3.api@<SYMBOL>``
  topic (best effort; falls back to polling when unavailable).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import ssl
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import AppConfig

log = logging.getLogger("ingest.trading.monitor")

# Official MEXC Spot WebSocket endpoint (market data).  Reconnect semantics are
# handled by this module.
MEXC_WS_URL = "wss://wbs.mexc.com/raw"

# Spot ticker v3 topic pattern: spot@public.ticker.v3.api@<SYMBOL>
WS_TICKER_TOPIC = "spot@public.ticker.v3.api@{}"

PriceCallback = Callable[[str, Decimal, float], None]


@dataclass
class PriceTick:
    """A single observed market price for a symbol."""

    symbol: str
    price: Decimal
    received_at: float  # time.monotonic() timestamp

    @property
    def age_s(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return max(0.0, now - self.received_at)


class PriceFeedError(Exception):
    """Base class for price feed failures."""


class PriceFeed:
    """Abstract market-data feed.

    Implementations push :class:`PriceTick` objects to a caller-supplied
    callback.  The monitor itself never opens a second connection for the same
    symbol.
    """

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def subscribe(self, symbol: str, callback: PriceCallback) -> None:
        raise NotImplementedError

    async def unsubscribe(self, symbol: str) -> None:
        raise NotImplementedError


class PollingPriceFeed(PriceFeed):
    """REST ``/api/v3/ticker/price`` polling feed.

    Reliable, dependency-free and easy to mock in tests.  This is the
    documented fallback when WebSocket monitoring is unavailable.
    """

    def __init__(
        self,
        fetch_price: Callable[[str], Decimal | None],
        *,
        fetch_prices: Callable[[list[str]], dict[str, Decimal]] | None = None,
        poll_interval_s: float = 2.0,
        max_retries: int = 5,
        backoff_base_s: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] | None = None,
    ):
        self._fetch_price = fetch_price
        self._fetch_prices = fetch_prices
        self._poll_interval_s = poll_interval_s
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._clock = clock
        self._sleep = sleep or time.sleep
        self._subscriptions: dict[str, PriceCallback] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._failures: dict[str, int] = {}
        self._last_price: dict[str, PriceTick] = {}

    # --------------------------------------------------------------- API

    @property
    def subscribed_symbols(self) -> list[str]:
        return list(self._subscriptions)

    def last_tick(self, symbol: str) -> PriceTick | None:
        return self._last_price.get(symbol)

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def subscribe(self, symbol: str, callback: PriceCallback) -> None:
        self._subscriptions[symbol] = callback
        # Push the most recent known tick immediately so the consumer can
        # evaluate state that was missed while offline.
        tick = self._last_price.get(symbol)
        if tick is not None:
            callback(tick.symbol, tick.price, tick.received_at)

    async def unsubscribe(self, symbol: str) -> None:
        self._subscriptions.pop(symbol, None)

    # --------------------------------------------------------------- internals

    async def _run_loop(self) -> None:
        if self._fetch_prices is not None:
            await self._run_batch_loop()
            return
        while not self._stop.is_set():
            symbols = list(self._subscriptions)
            if not symbols:
                # With nothing to poll, do not spin: the queue consumer and
                # Telegram listener share this event loop and must be able to
                # make progress while the monitor is idle.
                await self._sleep_with_stop(self._poll_interval_s)
                continue
            now = self._clock()
            stop_cycle = False
            for symbol in symbols:
                if self._stop.is_set():
                    break
                try:
                    price = await asyncio.to_thread(self._fetch_price, symbol)
                except Exception as exc:  # noqa: BLE001 - one bad fetch must not stop the feed
                    failures = self._failures.get(symbol, 0) + 1
                    self._failures[symbol] = failures
                    stop_cycle = await self._sleep_after_failure(
                        f"poll for {symbol}", failures, exc
                    )
                    if stop_cycle:
                        break
                    continue
                self._failures.pop(symbol, None)
                if price is not None and price.is_finite() and price > 0:
                    tick = PriceTick(symbol=symbol, price=price, received_at=now)
                    self._last_price[symbol] = tick
                    callback = self._subscriptions.get(symbol)
                    if callback is not None:
                        callback(symbol, price, now)
                await self._sleep_with_stop(self._poll_interval_s)
            if stop_cycle:
                await self._sleep_with_stop(self._poll_interval_s)

    async def _run_batch_loop(self) -> None:
        """One all-symbol ticker request per interval (official weight 10)."""

        fetch_prices = self._fetch_prices
        assert fetch_prices is not None
        failures = 0
        while not self._stop.is_set():
            symbols = list(self._subscriptions)
            if not symbols:
                await self._sleep_with_stop(self._poll_interval_s)
                continue
            try:
                prices = await asyncio.to_thread(fetch_prices, symbols)
            except Exception as exc:  # noqa: BLE001 - backoff, never crash
                failures += 1
                await self._sleep_after_failure("batch ticker poll", failures, exc)
                continue
            failures = 0
            now = self._clock()
            for symbol, price in prices.items():
                if symbol not in self._subscriptions:
                    continue
                if price is None or not price.is_finite() or price <= 0:
                    continue
                tick = PriceTick(symbol=symbol, price=price, received_at=now)
                self._last_price[symbol] = tick
                callback = self._subscriptions.get(symbol)
                if callback is not None:
                    callback(symbol, price, now)
            await self._sleep_with_stop(self._poll_interval_s)

    async def _sleep_after_failure(self, context: str, failures: int, exc: Exception) -> bool:
        """Sleep after a failure; return True when the whole cycle must stop."""

        retry_after = getattr(exc, "retry_after_s", None)
        if retry_after is not None and float(retry_after) >= 0:
            # 429/418: the exchange's instruction wins over local backoff, with
            # a one-second floor so a missing/zero header cannot hot-loop.
            delay = max(1.0, float(retry_after)) + random.uniform(0, 0.5)
            log.warning(
                "%s rate-limited (%s); honoring Retry-After %.1fs",
                context, exc.__class__.__name__, delay,
            )
            await self._sleep_with_stop(delay)
            return True
        delay = self._backoff_for(failures)
        log.warning(
            "%s failed (attempt %s, %s); backing off %.1fs",
            context, failures, exc.__class__.__name__, delay,
        )
        await self._sleep_with_stop(delay)
        return False

    def _backoff_for(self, failures: int) -> float:
        # Bounded exponential backoff with additive jitter (never zero).
        base = self._backoff_base_s
        cap = min(base * (2 ** min(max(0, failures - 1), 6)), 30.0)
        return random.uniform(cap / 2, cap) if cap > 0 else 0.0

    async def _sleep_with_stop(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, seconds))


# ---------------------------------------------------------------------------
# WebSocket feed (preferred, best effort)
# ---------------------------------------------------------------------------


def base64_key() -> str:
    import base64
    import os

    return base64.b64encode(os.urandom(16)).decode("ascii")


class MexcWebSocketFeed(PriceFeed):
    """MEXC Spot public WebSocket ticker feed (raw WebSocket over TLS).

    Subscribes to ``spot@public.ticker.v3.api@SYMBOL`` per symbol and pushes
    price ticks to the registered callbacks.  Any failure (connect, subscribe,
    frame parse, disconnect) degrades to the polling fallback; this feed never
    blocks trading while it is down.

    Implemented with the standard library only (no extra dependency).  The wire
    format follows the public MEXC Spot WebSocket documentation; behaviour
    against the real exchange has not been exercised in this tree, so failures
    degrade to polling rather than halting.
    """

    def __init__(
        self,
        *,
        url: str = MEXC_WS_URL,
        connect_timeout_s: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
        open_connection: Callable[..., Any] | None = None,
    ):
        self._url = url
        self._connect_timeout_s = connect_timeout_s
        self._clock = clock
        self._open_connection = open_connection or asyncio.open_connection
        self._subscriptions: dict[str, PriceCallback] = {}
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._id_lock = asyncio.Lock()

    # --------------------------------------------------------------- API

    async def start(self) -> None:
        self._stop.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop.set()
        self._close()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def subscribe(self, symbol: str, callback: PriceCallback) -> None:
        async with self._lock:
            self._subscriptions[symbol] = callback
        # Resubscribe on the live connection if we are already connected.
        async with self._id_lock:
            if self._writer is not None and not self._writer.is_closing():
                await self._send_subscribe(symbol)

    async def unsubscribe(self, symbol: str) -> None:
        async with self._lock:
            self._subscriptions.pop(symbol, None)

    # --------------------------------------------------------------- internals

    async def _run_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_and_read()
                # Clean disconnect: reset backoff and retry.
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - never kill the monitor
                log.warning("websocket feed interrupted (%s); polling fallback active", exc.__class__.__name__)
                delay = min(backoff, 30.0) + random.uniform(0, backoff / 4)
                backoff = min(backoff * 2, 30.0)
                await self._sleep_with_stop(delay)

    async def _connect_and_read(self) -> None:
        reader, writer = await asyncio.wait_for(
            self._open_connection("wbs.mexc.com", 443, ssl=ssl.create_default_context()),
            timeout=self._connect_timeout_s,
        )
        self._reader, self._writer = reader, writer
        try:
            await self._perform_handshake()
            async with self._id_lock:
                for symbol in list(self._subscriptions):
                    await self._send_subscribe(symbol)
            await self._read_frames()
        finally:
            self._close()

    async def _perform_handshake(self) -> None:
        key = base64_key()
        request = (
            "GET /raw HTTP/1.1\r\n"
            "Host: wbs.mexc.com\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        assert self._writer is not None
        self._writer.write(request.encode("ascii"))
        await self._writer.drain()
        reader = self._reader
        assert reader is not None
        status = await asyncio.wait_for(reader.readline(), timeout=self._connect_timeout_s)
        if not status.startswith(b"HTTP/1.1 101"):
            raise PriceFeedError(f"websocket handshake failed: {status[:80]!r}")
        # Skip remaining handshake headers.
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=self._connect_timeout_s)
            if not line or line in (b"\r\n", b"\n"):
                break

    async def _read_frames(self) -> None:
        assert self._reader is not None
        while not self._stop.is_set():
            frame = await self._read_frame()
            if frame is None:
                return  # closed cleanly
            await self._dispatch(frame)

    async def _read_frame(self) -> bytes | None:
        reader = self._reader
        assert reader is not None
        header = await asyncio.wait_for(reader.readexactly(2), timeout=self._connect_timeout_s)
        b0, b1 = header[0], header[1]
        fin = bool(b0 & 0x80)
        opcode = b0 & 0x0F
        masked = bool(b1 & 0x80)
        length = b1 & 0x7F
        if length == 126:
            length = int.from_bytes(await reader.readexactly(2), "big")
        elif length == 127:
            length = int.from_bytes(await reader.readexactly(8), "big")
        mask = await reader.readexactly(4) if masked else b""
        payload = await reader.readexactly(length) if length else b""
        if masked:
            payload = bytes(p ^ mask[i % 4] for i, p in enumerate(payload))
        if opcode == 0x8:  # close
            return None
        if opcode == 0x9:  # ping
            assert self._writer is not None
            self._writer.write(bytes([0x8A, 0]) + payload)
            await self._writer.drain()
            return b""
        if not fin or opcode != 0x1:
            return b""
        return payload

    async def _dispatch(self, payload: bytes) -> None:
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError:
            return
        try:
            message = json.loads(text)
        except (ValueError, TypeError):
            return
        if not isinstance(message, dict):
            return
        topic = str(message.get("topic") or "")
        symbol = self._symbol_from_topic(topic)
        if not symbol:
            return
        data = message.get("data")
        if not isinstance(data, list):
            data = [data]
        for entry in data:
            if not isinstance(entry, dict):
                continue
            price = self._price_from_entry(entry)
            if price is None:
                continue
            tick = PriceTick(symbol=symbol, price=price, received_at=self._clock())
            async with self._lock:
                callback = self._subscriptions.get(symbol)
            if callback is not None:
                callback(symbol, price, tick.received_at)

    @staticmethod
    def _symbol_from_topic(topic: str) -> str | None:
        # ``spot@public.ticker.v3.api@SYMBOL``
        if not topic.startswith("spot@public.ticker.v3.api@"):
            return None
        symbol = topic.split("@", 3)[-1]
        return symbol or None

    @staticmethod
    def _price_from_entry(entry: dict[str, Any]) -> Decimal | None:
        for key in ("lastPrice", "last", "p", "c", "price"):
            value = entry.get(key)
            if value is None:
                continue
            try:
                price = Decimal(str(value))
            except (InvalidOperation, ValueError, TypeError):
                continue
            if price.is_finite() and price > 0:
                return price
        return None

    def _close(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            with contextlib.suppress(OSError):
                self._writer.close()
        self._reader = None
        self._writer = None

    async def _send_subscribe(self, symbol: str) -> None:
        """Send one subscription frame on the live WebSocket connection."""
        topic = WS_TICKER_TOPIC.format(symbol.upper())
        frame = {
            "method": "SUBSCRIPTION",
            "params": [topic],
            "id": 1,
        }
        assert self._writer is not None
        self._writer.write((json.dumps(frame) + "\n").encode("utf-8"))
        await self._writer.drain()

    async def _sleep_with_stop(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=max(0.0, seconds))


# ---------------------------------------------------------------------------
# Monitor: staleness tracking + dispatch to the trade manager
# ---------------------------------------------------------------------------


class MarketMonitor:
    """Watches symbols and dispatches fresh price ticks to a handler.

    The monitor owns a primary feed (WebSocket when configured/available) and
    a REST polling fallback.  It tracks the last-seen price and its timestamp
    per symbol so callers can reject triggers observed on stale data.
    """

    def __init__(
        self,
        *,
        cfg: AppConfig,
        fetch_price: Callable[[str], Decimal | None],
        on_price: Callable[[str, Decimal, float], None],
        fetch_prices: Callable[[list[str]], dict[str, Decimal]] | None = None,
        feed: PriceFeed | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.cfg = cfg
        self._on_price = on_price
        self._clock = clock
        self._stale_threshold_s = float(cfg.trading.entry_trigger_stale_threshold_s)
        self._last_tick: dict[str, PriceTick] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._feed: PriceFeed
        if feed is not None:
            self._feed = feed
        else:
            polling = PollingPriceFeed(
                fetch_price,
                fetch_prices=fetch_prices,
                poll_interval_s=float(cfg.trading.monitor_poll_interval_s),
                max_retries=int(cfg.trading.monitor_max_retries),
                backoff_base_s=float(cfg.trading.monitor_backoff_base_s),
                clock=clock,
            )
            ws = MexcWebSocketFeed(clock=clock)
            self._feed = _PrimaryWithFallback(primary=ws, fallback=polling)

    @property
    def feed(self) -> PriceFeed:
        return self._feed

    def last_tick(self, symbol: str) -> PriceTick | None:
        return self._last_tick.get(symbol)

    def is_stale(self, symbol: str, now: float | None = None) -> bool:
        tick = self._last_tick.get(symbol)
        if tick is None:
            return True
        now = self._clock() if now is None else now
        age = max(0.0, now - tick.received_at)
        return age > self._stale_threshold_s

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        await self._feed.start()

    async def stop(self) -> None:
        await self._feed.stop()
        self._started = False

    async def subscribe(self, symbol: str) -> None:
        def wrapped(symbol, price, received_at):
            return self._tick(symbol, price, received_at)
        await self._feed.subscribe(symbol, wrapped)

    async def unsubscribe(self, symbol: str) -> None:
        await self._feed.unsubscribe(symbol)

    def _tick(self, symbol: str, price: Decimal, received_at: float) -> None:
        tick = PriceTick(symbol=symbol, price=price, received_at=received_at)
        self._last_tick[symbol] = tick
        try:
            self._on_price(symbol, price, received_at)
        except Exception as exc:  # noqa: BLE001 - one bad trade must not kill the feed
            log.error("trade manager price handler failed for %s: %s", symbol, exc.__class__.__name__)


class _PrimaryWithFallback(PriceFeed):
    """Run a primary (WebSocket) feed with a polling fallback.

    The fallback is subscribed to the same symbols and starts publishing ticks
    immediately; the primary connects in the background and subscribes when it
    is healthy.  If the primary dies, the fallback keeps publishing so
    monitoring never stops.
    """

    def __init__(self, *, primary: PriceFeed, fallback: PriceFeed):
        self._primary = primary
        self._fallback = fallback
        self._stop = asyncio.Event()
        self._primary_task: asyncio.Task | None = None
        self._started = False

    async def start(self) -> None:
        self._stop.clear()
        await self._fallback.start()
        self._primary_task = asyncio.create_task(self._run_primary())
        self._started = True

    async def stop(self) -> None:
        self._stop.set()
        if self._primary_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._primary_task
        with contextlib.suppress(Exception):
            await self._primary.stop()
        await self._fallback.stop()

    async def subscribe(self, symbol: str, callback: PriceCallback) -> None:
        await self._fallback.subscribe(symbol, callback)
        await self._primary.subscribe(symbol, callback)

    async def unsubscribe(self, symbol: str) -> None:
        await self._fallback.unsubscribe(symbol)
        await self._primary.unsubscribe(symbol)

    async def _run_primary(self) -> None:
        while not self._stop.is_set():
            try:
                await self._primary.start()
                # Keep the primary running until stop is requested.
                while not self._stop.is_set():
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - fallback keeps running
                log.warning("primary price feed unavailable (%s); polling fallback active", exc.__class__.__name__)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=5.0)
