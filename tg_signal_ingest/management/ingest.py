"""Resumable, duplicate-safe full-history import for the management bot.

The worker is deliberately single-channel and data-only. It never invokes the
signal parser, processing queue, risk manager, exchange client, or trading
engine. Progress is checkpointed after every bounded page so a crash or a
cancelled job can be resumed without creating duplicate messages.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import InputChannel, PeerChannel

from ..config import AppConfig
from ..normalize import message_to_record
from ..repository import Repository
from .store import ManagementStore

log = logging.getLogger("ingest.management")

MAX_FLOOD_RETRIES = 6
MAX_TRANSIENT_RETRIES = 4


class ChannelAccessError(Exception):
    """The owner's session cannot access the selected channel."""


@dataclass
class IngestReport:
    status: str
    discovered: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0
    total_stored: int = 0
    error: str | None = None
    completed: bool = False
    bounded: bool = False


ProgressCallback = Callable[[dict[str, Any]], Awaitable[None]]


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


class IngestionWorker:
    def __init__(
        self,
        cfg: AppConfig,
        repo: Repository,
        store: ManagementStore,
        *,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ):
        self.cfg = cfg
        self.repo = repo
        self.store = store
        self._sleep = sleep

    async def run(
        self,
        client: Any,
        job_id: str,
        *,
        on_progress: ProgressCallback | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> IngestReport:
        job = self.store.claim_job(job_id)
        if job is None:
            current = self.store.get_job(job_id)
            assert current is not None
            return IngestReport(
                status=str(current["status"]),
                discovered=int(current["discovered"]),
                inserted=int(current["inserted"]),
                updated=int(current["updated"]),
                unchanged=int(current["unchanged"]),
                failed=int(current["failed"]),
                total_stored=int(current["total_stored"]),
                error=current["error"],
                completed=current["status"] == "completed",
            )

        channel_id = int(job["channel_id"])
        max_messages = (
            int(job["max_messages"]) if job.get("max_messages") is not None else None
        )
        bounded = max_messages is not None
        report = IngestReport(
            status="running",
            discovered=int(job["discovered"]),
            inserted=int(job["inserted"]),
            updated=int(job["updated"]),
            unchanged=int(job["unchanged"]),
            failed=int(job["failed"]),
            total_stored=int(job["total_stored"]),
            bounded=bounded,
        )
        entity = self._channel_entity(channel_id)
        batch_size = max(10, min(int(self.cfg.backfill_batch_size), 500))
        exhausted = False
        last_notice = 0.0

        try:
            await self._ensure_channel(client, entity, channel_id)
            newest_id = await self._newest_id(client, entity, job)
            offset = job.get("checkpoint_next_max_id")
            offset = int(offset) if offset is not None else (newest_id + 1 if newest_id is not None else 0)
            # A bounded validation sample is not a full backfill. It
            # deliberately avoids sync_state so a later live/backfill start
            # cannot mistake the sample for a completed history import.
            if not bounded:
                with self.repo.db.transaction() as conn:
                    conn.execute(
                        """
                        INSERT INTO sync_state
                            (channel_id, backfill_target_mode, backfill_since, backfill_status,
                             backfill_next_max_id, backfill_started_at, updated_at)
                        VALUES (?, 'full', NULL, 'in_progress', ?, ?, ?)
                        ON CONFLICT (channel_id) DO UPDATE SET
                            backfill_target_mode = 'full',
                            backfill_since = NULL,
                            backfill_status = 'in_progress',
                            backfill_next_max_id = excluded.backfill_next_max_id,
                            backfill_started_at = COALESCE(
                                sync_state.backfill_started_at,
                                excluded.backfill_started_at
                            ),
                            backfill_completed_at = NULL,
                            updated_at = excluded.updated_at
                        """,
                        (channel_id, offset, _now(), _now()),
                    )
            self.store.update_job(job_id, newest_seen_id=newest_id, checkpoint_next_max_id=offset)

            while True:
                if stop_event is not None and stop_event.is_set():
                    report.status = "interrupted"
                    report.error = "worker stopped; checkpoint preserved"
                    break
                current = self.store.get_job(job_id)
                if current is None:
                    report.status = "failed"
                    report.error = "job disappeared"
                    break
                if int(current.get("cancel_requested") or 0):
                    report.status = "cancelled"
                    report.error = "cancelled by owner; checkpoint preserved"
                    break

                remaining: int | None = None
                if max_messages is not None:
                    remaining = max_messages - report.discovered
                if remaining is not None and remaining <= 0:
                    exhausted = True
                    break
                fetch_limit = (
                    batch_size
                    if remaining is None
                    else max(1, min(batch_size, remaining))
                )
                try:
                    page = await self._get_page(client, entity, offset, fetch_limit)
                except FloodWaitError:
                    raise
                except RPCError as exc:
                    report.status = "partial" if report.inserted or report.discovered else "failed"
                    report.error = f"Telegram denied further history ({exc.__class__.__name__})"
                    break

                if not page:
                    exhausted = True
                    break
                if len(page) < fetch_limit:
                    exhausted = True

                oldest = min(int(message.id) for message in page)
                failed_before = report.failed
                for message in page:
                    if max_messages is not None and report.discovered >= max_messages:
                        break
                    report.discovered += 1
                    try:
                        record = message_to_record(channel_id, message, "backfill")
                        outcome = self.repo.upsert_message(record, enqueue_processing=False).outcome.value
                        if outcome == "inserted":
                            report.inserted += 1
                        elif outcome == "updated":
                            report.updated += 1
                        else:
                            report.unchanged += 1
                    except Exception as exc:  # noqa: BLE001
                        report.failed += 1
                        log.error(
                            "failed to persist history message id=%s (%s)",
                            getattr(message, "id", "?"),
                            exc.__class__.__name__,
                        )

                offset = oldest
                report.total_stored = self.repo.message_counts(channel_id).total
                self.store.update_job(
                    job_id,
                    checkpoint_next_max_id=offset,
                    oldest_seen_id=oldest,
                    discovered=report.discovered,
                    inserted=report.inserted,
                    updated=report.updated,
                    unchanged=report.unchanged,
                    failed=report.failed,
                    total_stored=report.total_stored,
                )
                if failed_before != report.failed:
                    # Do not spin forever on a persistently corrupt row; the
                    # checkpoint still advances and the job is marked partial.
                    log.warning("history import encountered %s row failure(s)", report.failed)
                if not bounded:
                    self.repo.backfill_checkpoint(
                        channel_id, next_max_id=offset, oldest_id_seen=oldest, count_delta=len(page)
                    )
                if max_messages is not None and report.discovered >= max_messages:
                    exhausted = True
                    break

                loop_now = asyncio.get_running_loop().time()
                if on_progress is not None and loop_now - last_notice >= 15.0:
                    last_notice = loop_now
                    self.store.update_job(job_id, last_notified_at=_now())
                    try:
                        await on_progress(self.store.get_job(job_id) or {})
                    except Exception as exc:  # noqa: BLE001 - notifications are best effort
                        log.warning("progress notification failed (%s)", exc.__class__.__name__)

                if exhausted:
                    break

            if report.status == "running":
                if report.failed:
                    report.status = "partial"
                    report.error = f"{report.failed} message(s) could not be persisted"
                elif exhausted:
                    report.status = "completed"
                else:  # pragma: no cover - loop only exits above
                    report.status = "interrupted"
                    report.error = "history range ended without an explicit completion signal"

            report.total_stored = self.repo.message_counts(channel_id).total
            report.completed = report.status == "completed"
            report.bounded = bounded
            self.store.update_job(
                job_id,
                discovered=report.discovered,
                inserted=report.inserted,
                updated=report.updated,
                unchanged=report.unchanged,
                failed=report.failed,
                total_stored=report.total_stored,
                checkpoint_next_max_id=None if report.completed else offset,
            )
            if report.completed and not bounded:
                self.repo.complete_backfill(channel_id, oldest_id_seen=offset)
            self.store.finish_job(
                job_id,
                report.status,
                error=report.error,
                checkpoint_next_max_id=None if report.completed else offset,
            )
            if bounded:
                self.store.audit(
                    f"validation_import_{report.status}",
                    f"discovered={report.discovered} inserted={report.inserted} "
                    f"failed={report.failed} total={report.total_stored}",
                )
            else:
                self.store.audit(
                    f"ingestion_{report.status}",
                    f"inserted={report.inserted} total={report.total_stored}",
                )
            return report
        except FloodWaitError as exc:
            report.status = "interrupted"
            report.error = f"Telegram flood wait ({int(exc.seconds)}s); retry later"
        except ChannelAccessError as exc:
            report.status = "failed"
            report.error = str(exc)
        except (ConnectionError, TimeoutError, OSError) as exc:
            report.status = "failed"
            report.error = f"network failure ({exc.__class__.__name__}); checkpoint preserved"
        except Exception as exc:  # noqa: BLE001 - mark a durable, resumable failure
            report.status = "failed"
            report.error = f"unexpected ingestion failure ({exc.__class__.__name__})"
            if bounded:
                # Do not attach a traceback that could contain message or channel
                # details to a validation-job log line.
                log.error(
                    "bounded validation ingestion failed (%s)", exc.__class__.__name__
                )
            else:
                log.exception("management ingestion failed")
        finally:
            # Exception paths (network loss, flood wait, channel denial) must not
            # leave a durable job stuck in 'running' forever.
            current = self.store.get_job(job_id)
            if current and current["status"] == "running":
                final = report.status
                if final == "running":
                    final = "interrupted" if stop_event is not None and stop_event.is_set() else "failed"
                self.store.finish_job(
                    job_id,
                    final,
                    error=report.error,
                    checkpoint_next_max_id=current.get("checkpoint_next_max_id"),
                )
                report.status = final
        return report

    async def _get_page(self, client: Any, entity: Any, offset: int, limit: int) -> list[Any]:
        transient = 0
        while True:
            try:
                raw = await client.get_messages(entity, limit=limit, offset_id=offset, min_id=0)
                return [message for message in raw if message is not None]
            except FloodWaitError as exc:
                wait = exc.seconds + random.uniform(0, 1)
                log.warning("flood wait during management history fetch; sleeping %.0fs", wait)
                await self._sleep(wait)
                continue
            except (ConnectionError, TimeoutError, OSError) as exc:
                transient += 1
                if transient > MAX_TRANSIENT_RETRIES:
                    raise
                wait = min(60.0, 2.0 ** transient) + random.uniform(0, 0.5)
                log.warning(
                    "transient history fetch failure (%s); retrying over direct networking in %.0fs",
                    exc.__class__.__name__,
                    wait,
                )
                await self._sleep(wait)

    def _channel_entity(self, channel_id: int) -> Any:
        row = self.repo.db.conn.execute(
            "SELECT access_hash FROM channels WHERE channel_id = ?", (int(channel_id),)
        ).fetchone()
        access_hash = int(row["access_hash"] or 0) if row is not None else 0
        if access_hash:
            return InputChannel(int(channel_id), access_hash)
        return PeerChannel(int(channel_id))

    async def _ensure_channel(self, client: Any, entity: Any, channel_id: int) -> None:
        try:
            entity = await client.get_entity(entity)
        except Exception as exc:  # noqa: BLE001 - owner-facing code classifies it
            raise ChannelAccessError(
                f"channel access failed ({exc.__class__.__name__}); verify membership and retry"
            ) from None
        self.repo.ensure_channel(
            channel_id,
            int(getattr(entity, "access_hash", 0) or 0),
            getattr(entity, "username", None),
            str(getattr(entity, "title", None) or f"channel {channel_id}"),
        )

    async def _newest_id(self, client: Any, entity: Any, job: dict[str, Any]) -> int | None:
        stored = job.get("newest_seen_id")
        if stored is not None:
            return int(stored)
        try:
            raw = await client.get_messages(entity, limit=1)
        except FloodWaitError:
            raise
        except RPCError:
            raise
        return int(raw[0].id) if raw else 0
