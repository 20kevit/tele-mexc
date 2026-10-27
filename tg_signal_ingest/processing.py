"""Durable downstream processing queue consumer.

A future signal parser registers an async handler per item type

    engine.register_handler("message", my_handle_message)
    engine.register_handler("edit", my_handle_signal_edit)

and items move pending -> processing -> processed (or pending/failed with
bounded exponential backoff). Messages are only processed once per stored
version; a ``reprocess`` command re-enqueues the current version explicitly so
a parser update can walk the corpus.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .repository import QueueItem, Repository

log = logging.getLogger("ingest.processing")

HandlerFn = Callable[[QueueItem, dict[str, Any] | None], Awaitable[None]]


@dataclass
class ProcessingStats:
    processed: int = 0
    retried: int = 0
    failed: int = 0
    superseded: int = 0


class ProcessingEngine:
    def __init__(self, repo: Repository, poll_interval: float = 2.0, batch_size: int = 50):
        self.repo = repo
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._handlers: dict[str, HandlerFn] = {}
        self._edit_handlers_tracked: bool = False

    def register_handler(self, item_type: str, fn: HandlerFn) -> None:
        if item_type not in ("message", "edit", "delete"):
            raise ValueError("item_type must be 'message', 'edit' or 'delete'")
        self._handlers[item_type] = fn

    def unregister_handler(self, item_type: str) -> None:
        self._handlers.pop(item_type, None)

    async def _handle_item(self, item: QueueItem) -> tuple[bool, str | None]:
        record = self.repo.get_message_raw(item.channel_id, item.message_id)
        if record is None:
            if item.item_type == "delete":
                return True, "message never stored; deletion recorded only in queue"
            # The message row vanished (e.g. manual DB cleanup); retry briefly.
            return False, "message record missing while queue item exists"

        version_ok = item.targets_version == int(record.get("processing_version", 0))
        if not version_ok and item.item_type == "edit":
            # A newer version superseded this notification: nothing to do.
            return True, f"superseded by version {record.get('processing_version')}"

        handler = self._handlers.get(item.item_type)
        if handler is None:
            return True, None  # default: no-op consumer (parser not installed yet)
        await handler(item, record)
        return True, None

    async def run_once(self, max_items: int | None = None) -> ProcessingStats:
        stats = ProcessingStats()
        limit = max_items if max_items is not None else self.batch_size
        for item in self.repo.claim_pending(limit=limit):
            try:
                success, detail = await self._handle_item(item)
            except Exception as exc:  # noqa: BLE001
                self.repo.finish(item, False, f"{exc.__class__.__name__}: {str(exc)[:300]}")
                stats.failed += 1
                log.warning("processing failed for %s/%s(%s): %s",
                            item.channel_id, item.message_id, item.item_type, exc.__class__.__name__)
                continue
            if success:
                self.repo.finish(item, True)
                if detail and detail.startswith("superseded"):
                    stats.superseded += 1
                else:
                    stats.processed += 1
                    log.debug("handled %s/%s (%s)", item.channel_id, item.message_id, item.item_type)
            else:
                self.repo.finish(item, False, detail or "processing error")
                stats.retried += 1
        return stats

    async def run(self, stop_event: asyncio.Event) -> ProcessingStats:
        """Continuous consumer loop used by the ingest service.

        Survives unexpected errors: a single failure neither kills the loop
        (which would strand the backlog) nor breaks claimed items — claimed
        work is requeued before the loop continues.
        """

        total = ProcessingStats()
        while not stop_event.is_set():
            try:
                stats = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - single consumer: errors must not kill the loop
                log.error(
                    "queue consumer error (%s); requeueing claimed items and continuing",
                    exc.__class__.__name__,
                )
                with contextlib.suppress(Exception):
                    self.repo.requeue_expired_claims()
                stats = ProcessingStats()
            total.processed += stats.processed
            total.retried += stats.retried
            total.failed += stats.failed
            total.superseded += stats.superseded
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.poll_interval)
                break
            except TimeoutError:
                pass
        log.info(
            "processing loop finished: processed=%s retried=%s failed=%s superseded=%s",
            total.processed, total.retried, total.failed, total.superseded,
        )
        return total


class InterfaceTestHandler:
    """Minimal working consumer for the downstream processing interface test."""

    def __init__(self) -> None:
        self.handled: list[tuple[str, int, int, int]] = []

    def handler(self) -> HandlerFn:
        async def fn(item: QueueItem, record: dict[str, Any] | None) -> None:
            self.handled.append(
                (item.item_type, item.channel_id, item.message_id, item.targets_version)
            )
        return fn
