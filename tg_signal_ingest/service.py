"""Continuous ingestion service: live listener + backfill + recovery + queue.

Operational model (per connection cycle inside ``run``):
  1. Connect through the configured proxy and verify the stored session.
  2. Resolve and validate the selected channel entity.
  3. Attach live event handlers FIRST, so a concurrently running backfill can
     never shadow the live stream's newest messages.
  4. If a previous run stored ``live_last_seen_message_id``, walk the id gap
     upwards with a recovery sweep (bridges outages, upsert-deduplicated).
  5. Start the history import (backfill) in the background if it is pending,
     plus the downstream processing-queue consumer.
  6. Wait for either a stop signal or a Telegram disconnect. Disconnects are
     handled with bounded exponential backoff + jitter; a proxy failure never
     degrades into a direct connection.
"""

from __future__ import annotations

import asyncio
import logging
import random
import signal
from typing import Any

from telethon import TelegramClient, events, utils as tg_utils
from telethon.errors import ChannelPrivateError, FloodWaitError, RPCError
from telethon.tl.types import PeerChannel, UpdateDeleteChannelMessages

from .backfill import BackfillError, BackfillReport, run_backfill
from .config import AppConfig
from .normalize import message_to_record
from .proxy import ProxyError
from .processing import ProcessingEngine
from .repository import Repository
from .telethon import AuthRequired, build_client, connect_and_verify
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
    ):
        self.cfg = cfg
        self.repo = repo
        self._client = client or build_client(cfg)
        self._backfill_runner = backfill_runner or run_backfill
        self.proc_engine = ProcessingEngine(repo, poll_interval=cfg.processing_poll_interval_s)
        self._stop = asyncio.Event()
        self._backfill_task: asyncio.Task | None = None
        self._queue_task: asyncio.Task | None = None
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
                try:
                    loop.add_signal_handler(sig, self.request_stop)
                except (NotImplementedError, RuntimeError):  # pragma: no cover
                    pass

    # ------------------------------------------------------------ loop run

    async def run(self) -> int:
        """Run until stop; reconnect cycles on failures. Returns exit code."""

        backoff = 1.0
        exit_code = 0
        self.install_signal_handlers(asyncio.get_running_loop())
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
                if isinstance(exc, (ProxyError, ConnectionError, OSError, FloodWaitError)):
                    log.error(
                        "%s during service run; retrying after backoff %.1fs "
                        "(no direct Telegram fallback is used)",
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
                try:
                    await self._client.disconnect()
                except Exception:  # noqa: BLE001
                    pass
                self._build_fresh_client()
                delay = min(backoff, self.cfg.reconnect_backoff_max_s) + random.uniform(0, backoff / 4)
                backoff = min(backoff * 2, self.cfg.reconnect_backoff_max_s)
                await self._sleep_with_stop(delay)
        await self._shutdown_tasks()
        return exit_code

    def _build_fresh_client(self) -> None:
        self._client = build_client(self.cfg)

    async def _run_once(self) -> None:
        self.cfg.ensure_dirs()
        await connect_and_verify(self.cfg, self._client)
        log.info("telegram session verified; transport=%s", self._describe_proxy())
        await self._resolve_channel_entity()
        self._register_handlers()
        await self._run_recovery_sweep_if_needed()
        self._start_background_work()
        await self._wait_until_stopped_or_disconnected()

    # --------------------------------------------------------------- setup

    def _describe_proxy(self) -> str:
        p = self.cfg.proxy
        return f"{p.type}://{p.host}:{p.port}"

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
                f"channel (id {selected.channel_id}) not accessible with this session via proxy: "
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
        async def _on_new(event: events.NewMessage.Event) -> None:  # type: ignore[name-defined]
            chat_id = getattr(event, "chat_id", None)
            if not _is_target(chat_id):
                return  # unrelated chat: never touched, message dropped
            self._store_event_message(event.message, "live")

        @self._client.on(events.MessageEdited())
        async def _on_edited(event: events.MessageEdited.Event) -> None:  # type: ignore[name-defined]
            chat_id = getattr(event, "chat_id", None)
            if not _is_target(chat_id):
                return
            self._store_event_message(event.message, "live")

        @self._client.on(events.Raw(UpdateDeleteChannelMessages))
        async def _on_deleted(update: UpdateDeleteChannelMessages) -> None:  # type: ignore[name-defined]
            marked = tg_utils.get_peer_id(PeerChannel(update.channel_id))
            if not _is_target(marked):
                return
            changed = self.repo.mark_deleted(self._selected_channel_id or 0, update.messages)
            log.info("deletion event: %s message(s) newly marked deleted", changed)

    def _store_event_message(self, message: Any, source: str) -> None:
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
        except Exception as exc:  # noqa: BLE001
            log.error(
                "failed to store live message %s: %s", getattr(message, "id", "?"), exc.__class__.__name__
            )

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
                wait = min(exc.seconds, 120)
                log.warning("flood-wait %ss during gap sweep; sleeping then stopping this cycle", wait)
                await asyncio.sleep(wait)
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
            await asyncio.sleep(min(exc.seconds, 60))
        return count

    def _store_message_direct(self, msg: Any, source: str) -> None:
        upsert_result = self.repo.upsert_message(message_to_record(self._selected_channel_id or 0, msg, source))

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
        self._queue_task = asyncio.create_task(self.proc_engine.run(self._stop))

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
            except (BackfillError, FloodWaitError, RPCError) as exc:
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
                try:
                    await self._client.disconnect()
                except Exception:  # noqa: BLE001
                    pass
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
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------- cleanup

    async def _shutdown_tasks(self) -> None:
        for task in (self._backfill_task, self._queue_task):
            if task is None or task.done():
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass
        try:
            await self._client.disconnect()
        except Exception:  # noqa: BLE001
            pass
        log.info("ingest service stopped")
