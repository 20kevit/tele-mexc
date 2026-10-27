"""Continuous ingestion service: live listener + backfill + recovery + queue.

Operational model (per connection cycle inside ``run``):
  1. Connect directly to Telegram and verify the stored session.
  2. Resolve and validate the selected channel entity.
  3. Attach live event handlers FIRST, so a concurrently running backfill can
     never shadow the live stream's newest messages.
  4. If a previous run stored ``live_last_seen_message_id``, walk the id gap
     upwards with a recovery sweep (bridges outages, upsert-deduplicated).
  5. Start the history import (backfill) in the background if it is pending,
     plus the downstream processing-queue consumer.
  6. Wait for either a stop signal or a Telegram disconnect. Disconnects are
     handled with bounded exponential backoff + jitter.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import queue
import random
import signal
import threading
from typing import Any

from telethon import TelegramClient, events
from telethon import utils as tg_utils
from telethon.errors import ChannelPrivateError, FloodWaitError, RPCError
from telethon.tl.types import PeerChannel, UpdateDeleteChannelMessages

from .backfill import BackfillError, BackfillReport, run_backfill
from .config import AppConfig
from .connectivity import ConnectivityError
from .normalize import message_to_record
from .processing import ProcessingEngine
from .repository import Repository
from .telethon import AuthRequired, build_client, connect_and_verify
from .trading import TradingEngine, build_trading_engine
from .utils import marked_channel_id

log = logging.getLogger("ingest.service")


class ChannelNotAccessible(Exception):
    """The configured channel cannot be read with the stored session."""


class IngestService:
    def __init__(
        self,
        cfg: AppConfig,
        repo: Repository,
        client: TelegramClient | None = None,
        backfill_runner=None,
        trading_engine: TradingEngine | None = None,
        control_bot: Any | None = None,
        market_monitor: Any | None = None,
    ):
        self.cfg = cfg
        self.repo = repo
        self._client = client or build_client(cfg)
        self._backfill_runner = backfill_runner or run_backfill
        self.proc_engine = ProcessingEngine(repo, poll_interval=cfg.processing_poll_interval_s)
        self.trading = trading_engine or build_trading_engine(cfg, repo)
        self._stop = asyncio.Event()
        self._backfill_task: asyncio.Task | None = None
        self._queue_task: asyncio.Task | None = None
        self._recon_task: asyncio.Task | None = None
        self._control_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._monitor: Any | None = market_monitor
        self._subscribed_symbols: set[str] = set()
        self._tick_queue: queue.Queue[tuple[str, Any, float] | None] | None = None
        self._tick_stop = threading.Event()
        self._tick_thread: threading.Thread | None = None
        self._control_bot = control_bot
        if control_bot is not None:
            control_bot.attach_engine(self.trading)
        self._entity: Any = None
        self._selected_channel_id: int | None = None
        self._selected_marked_id: int | None = None

    # ------------------------------------------------------------- control

    def request_stop(self) -> None:
        if not self._stop.is_set():
            log.info("stop requested; shutting down cleanly")
        self._stop.set()

    def install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is not None:
                with contextlib.suppress(NotImplementedError, RuntimeError):  # pragma: no cover
                    loop.add_signal_handler(sig, self.request_stop)

    # ------------------------------------------------------------ loop run

    async def run(self) -> int:
        """Run until stop; reconnect cycles on failures. Returns exit code."""

        backoff = 1.0
        exit_code = 0
        self.install_signal_handlers(asyncio.get_running_loop())
        if self._control_bot is not None:
            self._control_task = asyncio.create_task(self._control_worker())
            log.info("control bot task started")
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = 1.0  # a clean cycle resets the reconnect backoff
            except AuthRequired as exc:
                log.error("%s", exc)
                self.request_stop()
                exit_code = 3
            except ChannelNotAccessible as exc:
                log.error("%s", exc)
                self.request_stop()
                exit_code = 4
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                if isinstance(exc, (ConnectivityError, ConnectionError, OSError, FloodWaitError)):
                    log.error(
                        "%s during service run; retrying after backoff %.1fs over direct networking",
                        exc.__class__.__name__,
                        backoff,
                    )
                elif isinstance(exc, ChannelPrivateError):
                    log.error("channel is private/gone; verify selection with 'tg-ingest diagnostics telegram'")
                    exit_code = 4
                else:
                    log.error("unexpected service error (%s): %s", exc.__class__.__name__, str(exc)[:200])
                if self._stop.is_set():
                    break
                with contextlib.suppress(Exception):
                    await self._client.disconnect()
                self._build_fresh_client()
                if isinstance(exc, FloodWaitError):
                    # FloodWait is an instruction, not a hint: never reconnect
                    # earlier than Telegram explicitly allows.
                    delay = exc.seconds + random.uniform(0, 1)
                    backoff = 1.0
                else:
                    delay = min(backoff, self.cfg.reconnect_backoff_max_s) + random.uniform(0, backoff / 4)
                    backoff = min(backoff * 2, self.cfg.reconnect_backoff_max_s)
                await self._sleep_with_stop(delay)
        await self._shutdown_tasks()
        return exit_code

    def _build_fresh_client(self) -> None:
        self._client = build_client(self.cfg)

    async def _run_once(self) -> None:
        self.cfg.ensure_dirs()
        # Re-entrency guard: every new connection cycle must first terminate
        # tasks started by a previous cycle (a duplicate backfill worker or
        # queue consumer would leak FloodWait-prone loops and double claim).
        await self._reap_background_tasks()
        await connect_and_verify(self.cfg, self._client)
        recovered = self.repo.requeue_expired_claims()
        if recovered:
            log.info("re-queued %s item(s) left in 'processing' state by a previous crashed run", recovered)
        log.info("telegram session verified; transport=direct TCP/TLS")
        await self._resolve_channel_entity()
        self._register_handlers()
        await self._run_recovery_sweep_if_needed()
        self._start_background_work()
        await self._wait_until_stopped_or_disconnected()

    # --------------------------------------------------------------- setup

    async def _resolve_channel_entity(self) -> None:
        selected = self.repo.selected_channel()
        if selected is None:
            raise ChannelNotAccessible(
                "no channel selected; run 'tg-ingest channels list' and 'tg-ingest channel select'"
            )
        try:
            entity = await self._client.get_entity(PeerChannel(selected.channel_id))
        except (ChannelPrivateError, ValueError) as exc:
            raise ChannelNotAccessible(
                f"channel (id {selected.channel_id}) not accessible with this session over direct Telegram: "
                f"{exc.__class__.__name__}"
            ) from exc
        title = getattr(entity, "title", None) or selected.title
        self.repo.ensure_channel(
            selected.channel_id,
            int(getattr(entity, "access_hash", selected.access_hash) or selected.access_hash),
            getattr(entity, "username", None) or selected.username,
            str(title),
        )
        self._entity = entity
        self._selected_channel_id = selected.channel_id
        self._selected_marked_id = marked_channel_id(selected.channel_id)
        log.info("listening to channel: id=%s marked=%s title=%r", selected.channel_id, self._selected_marked_id, title)

    def _register_handlers(self) -> None:
        marked = self._selected_marked_id
        assert marked is not None

        def _is_target(chat_id: Any) -> bool:
            try:
                return int(chat_id) == marked
            except (TypeError, ValueError):
                peer = getattr(chat_id, "channel_id", None)
                return peer is not None and marked_channel_id(int(peer)) == marked

        @self._client.on(events.NewMessage())
        async def _on_new(event: events.NewMessage.Event) -> None:
            chat_id = getattr(event, "chat_id", None)
            if not _is_target(chat_id):
                return  # unrelated chat: never touched, message dropped
            await self._store_event_message(event.message, "live")

        @self._client.on(events.MessageEdited())
        async def _on_edited(event: events.MessageEdited.Event) -> None:
            chat_id = getattr(event, "chat_id", None)
            if not _is_target(chat_id):
                return
            await self._store_event_message(event.message, "live")

        @self._client.on(events.Raw(UpdateDeleteChannelMessages))
        async def _on_deleted(update: UpdateDeleteChannelMessages) -> None:
            marked = tg_utils.get_peer_id(PeerChannel(update.channel_id))
            if not _is_target(marked):
                return
            changed = self.repo.mark_deleted(self._selected_channel_id or 0, update.messages)
            log.info("deletion event: %s message(s) newly marked deleted", changed)

    async def _store_event_message(self, message: Any, source: str) -> None:
        assert self._selected_channel_id is not None
        try:
            rec = message_to_record(self._selected_channel_id, message, source)
            result = self.repo.upsert_message(rec)
            self.repo.record_live_event(self._selected_channel_id, int(message.id))
            if result.outcome.value == "inserted":
                log.info(
                    "live message %s stored (media=%s)", message.id, rec.media_type or "text-only"
                )
            elif result.outcome.value == "updated":
                log.info(
                    "live edit of %s stored (version %s -> %s)",
                    message.id, result.old_version, result.new_version,
                )
            await self._download_media_if_requested(message)
        except Exception as exc:  # noqa: BLE001
            log.error(
                "failed to store live message %s: %s", getattr(message, "id", "?"), exc.__class__.__name__
            )

    async def _download_media_if_requested(self, message: Any) -> None:
        """Optional, default-off media download (only when explicitly configured)."""

        if not self.cfg.download_media or getattr(message, "media", None) is None:
            return
        try:
            path = await self._client.download_media(message, file=str(self.cfg.media_dir))
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "media download failed for message %s: %s", getattr(message, "id", "?"),
                exc.__class__.__name__,
            )
            return
        if path:
            with contextlib.suppress(OSError):  # pragma: no cover
                os.chmod(path, 0o600)

    # ------------------------------------------------------------ recovery

    async def _run_recovery_sweep_if_needed(self) -> None:
        assert self._selected_channel_id is not None
        state = self.repo.sync_state(self._selected_channel_id)
        last = state.get("live_last_seen_message_id")
        if last is None:
            log.info("no previous live checkpoint; recovery sweep not needed this cycle")
            return
        caught = await self.sweep_gap(int(last))
        refreshed = await self.refresh_recent_window(int(last))
        self.repo.record_recovery(self._selected_channel_id)
        log.info("recovery sweep complete: %s gap message(s) fetched, %s recent refreshed", caught, refreshed)

    async def sweep_gap(self, last_id: int) -> int:
        """Fetch messages with id > ``last_id`` (posted while offline).

        Pages upward from the stored live watermark until no newer message
        remains; the watermark advances only across gap paging so repeated
        sweeps do not re-fetch the same window.
        """

        count = 0
        floor_id = last_id
        while not self._stop.is_set():
            page_ids: list[int] = []
            try:
                async for msg in self._client.iter_messages(
                    self._entity, reverse=True, min_id=floor_id, limit=self.cfg.recovery_sweep_size
                ):
                    self._store_message_direct(msg, "recovery")
                    page_ids.append(int(msg.id))
            except FloodWaitError as exc:
                wait = exc.seconds + 1
                log.warning("flood-wait %ss during gap sweep; sleeping then stopping this cycle", wait)
                await self._sleep_with_stop(wait)
                break
            if not page_ids:
                break
            count += len(page_ids)
            top_id = max(page_ids)
            if top_id > floor_id:
                self.repo.record_live_event(self._selected_channel_id or 0, top_id)
                floor_id = top_id
            if len(page_ids) < self.cfg.recovery_sweep_size:
                break
        return count

    async def refresh_recent_window(self, last_id: int) -> int:
        """Refresh the N most recent stored messages (catches offline edits)."""

        count = 0
        try:
            lower = max(1, last_id - self.cfg.recovery_sweep_size)
            batch = await self._client.get_messages(
                self._entity, limit=self.cfg.recovery_sweep_size, offset_id=last_id + 1, min_id=lower
            )
            for msg in batch:
                if msg is None:
                    continue
                self._store_message_direct(msg, "recovery")
                count += 1
        except FloodWaitError as exc:
            log.warning("flood-wait %ss during recent-window refresh", exc.seconds)
            await self._sleep_with_stop(exc.seconds + 1)
        return count

    def _store_message_direct(self, msg: Any, source: str) -> None:
        self.repo.upsert_message(message_to_record(self._selected_channel_id or 0, msg, source))

    # ---------------------------------------------------------- background

    def _start_background_work(self) -> None:
        assert self._selected_channel_id is not None
        state = self.repo.sync_state(self._selected_channel_id)
        status = state.get("backfill_status", "not_started")
        if status in ("not_started", "in_progress"):
            self._backfill_task = asyncio.create_task(self._backfill_worker())
            log.info("history import scheduled (status=%s)", status)
        else:
            log.info("history import already done; skipping backfill")
        self._register_trading_handlers()
        self._queue_task = asyncio.create_task(self.proc_engine.run(self._stop))
        if self.cfg.trading.mode == "live":
            self._recon_task = asyncio.create_task(self._reconciliation_worker())
            if self._trade_manager() is not None:
                self._monitor_task = asyncio.create_task(self._market_worker())

    def _register_trading_handlers(self) -> None:
        async def _handle(item, record):
            await asyncio.to_thread(self.trading.process_item, item, record)

        for item_type in ("message", "edit", "delete"):
            self.proc_engine.register_handler(item_type, _handle)

    async def _control_worker(self) -> None:
        """Keep the controller alive independently of Telegram reconnects."""

        assert self._control_bot is not None
        while not self._stop.is_set():
            try:
                await self._control_bot.run(self._stop)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - control failures must not stop ingestion
                log.error("control bot crashed (%s); retrying in 5s", exc.__class__.__name__)
                await self._sleep_with_stop(5)

    def _trade_manager(self) -> Any | None:
        return getattr(self.trading, "trade_manager", None)

    def _build_monitor(self) -> Any | None:
        """Create the live market monitor from the engine's price reader."""

        fetch_price = getattr(self.trading, "fetch_market_price", None)
        if fetch_price is None:
            log.error("live trading engine has no market price reader; monitor not started")
            return None
        try:
            from .trading.monitor import MarketMonitor

            return MarketMonitor(
                cfg=self.cfg,
                fetch_price=fetch_price,
                on_price=self._enqueue_tick,
            )
        except Exception as exc:  # noqa: BLE001 - monitoring is best effort
            log.error("market monitor construction failed (%s)", exc.__class__.__name__)
            return None

    def _enqueue_tick(self, symbol: str, price: Any, received_at: float) -> None:
        """Hand a tick to the worker thread without blocking the price feed."""

        tick_queue = self._tick_queue
        if tick_queue is None:
            return
        try:
            tick_queue.put_nowait((symbol, price, received_at))
        except queue.Full:
            # Prices are ephemeral: prefer the freshest tick over a backlog.
            with contextlib.suppress(queue.Empty):
                tick_queue.get_nowait()
            with contextlib.suppress(queue.Full):
                tick_queue.put_nowait((symbol, price, received_at))

    def _start_tick_worker(self, trade_manager: Any) -> None:
        if self._tick_thread is not None and self._tick_thread.is_alive():
            return
        self._tick_queue = queue.Queue(maxsize=1000)
        self._tick_stop = threading.Event()

        def _run() -> None:
            while not self._tick_stop.is_set():
                tick_queue = self._tick_queue
                if tick_queue is None:
                    return
                try:
                    item = tick_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:
                    return
                symbol, price, received_at = item
                try:
                    trade_manager.on_tick(symbol, price, received_at)
                except Exception as exc:  # noqa: BLE001 - never kill the worker
                    log.error(
                        "trade tick failed for %s (%s); continuing",
                        symbol, exc.__class__.__name__,
                    )

        self._tick_thread = threading.Thread(
            target=_run, name="trade-tick-worker", daemon=True
        )
        self._tick_thread.start()

    def _stop_tick_worker(self) -> None:
        self._tick_stop.set()
        if self._tick_queue is not None:
            with contextlib.suppress(queue.Full):
                self._tick_queue.put_nowait(None)
        thread = self._tick_thread
        self._tick_thread = None
        if thread is not None:
            thread.join(timeout=2.0)
        self._tick_queue = None

    async def _sync_monitor_symbols(self) -> None:
        """Subscribe the monitor to every symbol with a non-terminal trade."""

        monitor = self._monitor
        trade_manager = self._trade_manager()
        if monitor is None or trade_manager is None:
            return
        store = getattr(trade_manager, "store", None)
        if store is None:
            return
        try:
            trades = store.list_trades()
        except Exception as exc:  # noqa: BLE001 - keep the monitor alive
            log.error("trade symbol lookup failed (%s)", exc.__class__.__name__)
            return
        active = {
            str(trade.get("symbol") or "").upper()
            for trade in trades
            if trade.get("trade_state")
            not in ("closed", "tp2_completed", "needs_reconciliation")
            and trade.get("symbol")
        }
        for symbol in sorted(active - self._subscribed_symbols):
            try:
                await monitor.subscribe(symbol)
            except Exception as exc:  # noqa: BLE001
                log.error("subscribe failed for %s (%s)", symbol, exc.__class__.__name__)
                continue
            self._subscribed_symbols.add(symbol)

    async def _market_worker(self) -> None:
        """Run the price monitor and keep its subscriptions in sync."""

        trade_manager = self._trade_manager()
        if trade_manager is None:
            return
        monitor = self._monitor or self._build_monitor()
        if monitor is None:
            return
        self._monitor = monitor
        try:
            await monitor.start()
        except Exception as exc:  # noqa: BLE001 - trading still runs without ticks
            log.error("market monitor failed to start (%s)", exc.__class__.__name__)
            return
        log.info("market monitor started for conditional-entry/TP/SL lifecycle")
        self._start_tick_worker(trade_manager)
        try:
            while not self._stop.is_set():
                await self._sync_monitor_symbols()
                await self._sleep_with_stop(2.0)
        finally:
            self._stop_tick_worker()
            with contextlib.suppress(Exception):
                await monitor.stop()
            self._monitor = None
            self._subscribed_symbols.clear()

    async def _reconciliation_worker(self) -> None:
        interval = max(5.0, self.cfg.trading.recon_interval_s)
        trade_manager = self._trade_manager()
        if trade_manager is not None:
            try:
                # Rehydrate trades before any tick can act on them.  Uncertain
                # orders are queried only; they are never resubmitted here.
                await asyncio.to_thread(trade_manager.recover_on_startup)
            except Exception as exc:  # noqa: BLE001
                log.error("trade recovery failed (%s); continuing", exc.__class__.__name__)
        while not self._stop.is_set():
            try:
                await asyncio.to_thread(self.trading.reconcile_pending)
                if trade_manager is not None:
                    await asyncio.to_thread(trade_manager.reconcile_all_open)
                notifier = getattr(self.trading, "notifier", None)
                if notifier is not None:
                    await asyncio.to_thread(notifier.flush)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconciliation must never kill ingestion
                log.error("reconciliation sweep failed (%s); continuing", exc.__class__.__name__)
            await self._sleep_with_stop(interval)

    async def _backfill_worker(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            try:
                report: BackfillReport = await self._backfill_runner(
                    self._client, self.repo, self.cfg, channel_bare_id=self._selected_channel_id
                )
                if report.completed:
                    log.info(
                        "history import done: discovered=%s inserted=%s updated=%s unchanged=%s failed=%s",
                        report.discovered, report.inserted, report.updated, report.unchanged, report.failed,
                    )
                    return
                if report.abort_reason:
                    log.error("history import aborted: %s", report.abort_reason)
                    return
                log.warning(
                    "history import pass ended without completion; retrying (checkpoint preserved)"
                )
                await self._sleep_with_stop(30)
                continue
            except FloodWaitError as exc:
                attempt += 1
                # Honor the server-provided wait exactly; normal backoff caps do
                # not apply to FloodWait.
                delay = exc.seconds + random.uniform(0, 1)
                log.error(
                    "backfill attempt %s hit FloodWait; retrying in %.0fs", attempt, delay
                )
                await self._sleep_with_stop(delay)
                continue
            except (BackfillError, RPCError) as exc:
                attempt += 1
                delay = min(self.cfg.reconnect_backoff_max_s, 2 ** min(attempt, 9)) + random.uniform(0, 2)
                log.error("backfill attempt %s failed (%s); retrying in %.0fs", attempt, exc.__class__.__name__, delay)
                await self._sleep_with_stop(delay)
                continue
            except asyncio.CancelledError:
                return
            except Exception as exc:  # noqa: BLE001  (survivability never lets backfill kill the live loop)
                attempt += 1
                log.error("backfill crashed (%s) on attempt %s; retrying later", exc.__class__.__name__, attempt)
                await self._sleep_with_stop(min(300, 10 * attempt))

    # ------------------------------------------------------------ main loop

    async def _wait_until_stopped_or_disconnected(self) -> None:
        stop_task = asyncio.create_task(self._stop.wait())
        try:
            disconnect_task = asyncio.ensure_future(self._client.run_until_disconnected())
        except Exception:  # pragma: no cover
            stop_task.cancel()
            raise
        disconnect_done = asyncio.create_task(self._wait_for_task(disconnect_task))
        try:
            done, _ = await asyncio.wait(
                {stop_task, disconnect_done}, return_when=asyncio.FIRST_COMPLETED
            )
            if stop_task in done and self._stop.is_set():
                with contextlib.suppress(Exception):
                    await self._client.disconnect()
            elif disconnect_done in done:
                log.info("telegram connection dropped (will reconnect with backoff)")
        finally:
            for task in (stop_task, disconnect_done):
                if not task.done():
                    task.cancel()

    async def _wait_for_task(self, task: asyncio.Task) -> None:
        try:
            await task
        except (asyncio.CancelledError, Exception) as exc:  # noqa: BLE001
            if not self._stop.is_set():
                log.debug("telegram runner ended with %s", type(exc).__name__)

    async def _sleep_with_stop(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    # ------------------------------------------------------------- cleanup

    async def _reap_background_tasks(self) -> None:
        """Cancel and await tasks of a previous connection cycle (if any)."""

        for attr in ("_backfill_task", "_queue_task", "_recon_task", "_monitor_task"):
            task = getattr(self, attr, None)
            if task is None or task.done():
                setattr(self, attr, None)
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            setattr(self, attr, None)
        self._stop_tick_worker()

    async def _shutdown_tasks(self) -> None:
        await self._reap_background_tasks()
        task, self._control_task = self._control_task, None
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        with contextlib.suppress(Exception):
            await self._client.disconnect()
        log.info("ingest service stopped")
