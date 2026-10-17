"""Historical message import (backfill) with resumable checkpoints.

Design:
  * Pages history with ``get_messages`` using ``offset_id`` pagination
    (descending ids), never loading the whole history into memory.
  * Commits every batch, then stores the next ``offset_id`` checkpoint in
    ``sync_state`` so a terminated import resumes where it left off; the
    (channel_id, message_id) upsert makes seam retries harmless.
  * ``since`` mode filters by date in code after resolving the id boundary,
    so calendar correctness does not depend on Telegram's ``offset_date``
    semantics.
  * Runs concurrently with live monitoring; id-based upserts merge both.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from telethon.errors import FloodWaitError

from .config import AppConfig
from .normalize import message_to_record
from .repository import Repository
from .utils import marked_channel_id

log = logging.getLogger("ingest.backfill")

SAFETY_BOUND_MESSAGES = 250_000
MAX_FLOOD_RETRIES = 6


@dataclass
class BackfillReport:
    discovered: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    failed: int = 0
    completed: bool = False
    oldest_id_seen: int | None = None
    abort_reason: str | None = None


class BackfillError(Exception):
    pass


class _Page:
    def __init__(self, messages: list[Any], bottom: bool, oldest_ok: int | None):
        self.messages = messages
        self.bottom = bottom
        self.oldest_ok = oldest_ok


async def _fetch_page(
    client: Any, entity_ref: int, offset_id: int, limit: int, min_bound: int
) -> list[Any]:
    for _attempt in range(MAX_FLOOD_RETRIES):
        try:
            raw = await client.get_messages(entity_ref, limit=limit, offset_id=offset_id, min_id=min_bound)
            return [m for m in raw if m is not None]
        except FloodWaitError as exc:
            wait = min(exc.seconds, 120) + random.uniform(0, 1)
            log.warning("flood-wait during history fetch; sleeping %.1fs then retrying", wait)
            await asyncio.sleep(wait)
    raise BackfillError("history fetch kept hitting FloodWait; will retry later")


def _resolve_since_dt(since: str | None) -> datetime | None:
    if not since:
        return None
    try:
        return datetime.fromisoformat(since).replace(tzinfo=UTC)
    except ValueError as exc:
        raise BackfillError(f"invalid backfill_since value {since!r}") from exc


async def _resolve_min_bound(client: Any, entity_ref: int, mode: str, since_dt: datetime | None) -> int:
    """Id boundary below which history is out of the requested scope.

    Full mode: 0 (import everything available). Since mode: exclusive top of
    the page strictly older than ``since`` (the exact date boundary is also
    enforced in code, so Telegram date-paging quirks stay harmless).
    """

    if mode != "since" or since_dt is None:
        return 0
    msgs = await client.get_messages(entity_ref, limit=1, offset_date=since_dt)
    if not msgs or msgs[0] is None:
        return 0
    boundary = msgs[0]
    if boundary.date and boundary.date >= since_dt:
        return 0
    return int(boundary.id)


def _store_page_messages(
    repo: Repository, messages: list[Any], channel_id: int, since_dt: datetime | None, report: BackfillReport
) -> None:
    for msg in messages:
        report.discovered += 1
        if since_dt is not None and getattr(msg, "date", None) and msg.date < since_dt:
            continue  # date filtered (since mode)
        try:
            record = message_to_record(channel_id, msg, "backfill")
            outcome = repo.upsert_message(record).outcome.value
            if outcome == "inserted":
                report.inserted += 1
            elif outcome == "updated":
                report.updated += 1
            else:
                report.unchanged += 1
        except Exception as exc:  # noqa: BLE001
            report.failed += 1
            log.error("failed to store history message id=%s: %s", getattr(msg, "id", "?"), exc.__class__.__name__)


async def run_backfill(
    client: Any,
    repo: Repository,
    cfg: AppConfig,
    channel_bare_id: int | None = None,
    max_pages: int | None = None,
) -> BackfillReport:
    """Run (or resume) the history import for the selected channel."""

    selected = repo.selected_channel()
    if selected is None:
        raise BackfillError("no channel selected; run 'tg-ingest channel select' first")
    if channel_bare_id is not None and int(channel_bare_id) != selected.channel_id:
        raise BackfillError(
            f"channel mismatch: selected id {selected.channel_id} != requested {channel_bare_id}"
        )
    channel_id = selected.channel_id
    entity_ref = marked_channel_id(channel_id)

    report = BackfillReport()
    state = repo.sync_state(channel_id)
    status = state.get("backfill_status", "not_started")
    if status == "done":
        report.completed = True
        return report

    since_dt = _resolve_since_dt(cfg.backfill_since if cfg.backfill_scope == "since" else None)
    min_bound = await _resolve_min_bound(client, entity_ref, cfg.backfill_scope, since_dt)
    entity_probe = await client.get_messages(entity_ref, limit=1)
    newest_available = int(entity_probe[0].id) if entity_probe else 0

    if status == "in_progress":
        saved = state.get("backfill_next_max_id")
        offset_id = int(saved) if saved is not None else (newest_available + 1)
        log.info("resuming history import: offset_id=%s min_bound=%s", offset_id, min_bound)
    else:
        offset_id = newest_available + 1
        now = _utcnow()
        with repo.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (channel_id, backfill_target_mode, backfill_since,
                    backfill_status, backfill_started_at, updated_at)
                VALUES (?, ?, ?, 'in_progress', ?, ?)
                ON CONFLICT (channel_id) DO UPDATE SET
                    backfill_target_mode = excluded.backfill_target_mode,
                    backfill_since = excluded.backfill_since,
                    backfill_status = 'in_progress',
                    backfill_started_at = sync_state.backfill_started_at,
                    backfill_completed_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (channel_id, cfg.backfill_scope, cfg.backfill_since, now, now),
            )
        log.info(
            "history import starting: mode=%s since=%s offset_id=%s",
            cfg.backfill_scope, cfg.backfill_since, offset_id,
        )

    pages = 0
    while True:
        if max_pages is not None and pages >= max_pages:
            break
        msgs = await _fetch_page(client, entity_ref, offset_id, cfg.backfill_batch_size, min_bound)
        pages += 1
        if not msgs:
            break
        oldest_fetched = min(int(m.id) for m in msgs)
        report.oldest_id_seen = (
            oldest_fetched if report.oldest_id_seen is None else min(report.oldest_id_seen, oldest_fetched)
        )
        _store_page_messages(repo, msgs, channel_id, since_dt, report)
        repo.backfill_checkpoint(
            channel_id, next_max_id=oldest_fetched, oldest_id_seen=report.oldest_id_seen, count_delta=len(msgs)
        )
        offset_id = oldest_fetched
        if oldest_fetched <= max(min_bound, 1):
            break
    if max_pages is not None and pages >= max_pages:
        return report

    # Completion: the last full page was exhausted, or everything below min_bound filtered out.
    reached_bottom = oldest_available_bottom(
        min_bound=min_bound, oldest_id_seen=report.oldest_id_seen, since_dt=since_dt
    )
    if reached_bottom:
        now = _utcnow()
        with repo.db.transaction() as conn:
            conn.execute(
                "UPDATE sync_state SET backfill_status = 'done', backfill_completed_at = "
                " COALESCE(backfill_completed_at, ?), backfill_next_max_id = NULL, updated_at = ? WHERE channel_id = ?",
                (now, now, channel_id),
            )
        report.completed = True
        log.info(
            "history import complete: discovered=%s inserted=%s updated=%s unchanged=%s failed=%s",
            report.discovered, report.inserted, report.updated, report.unchanged, report.failed,
        )
    else:
        log.info("history import pass ended; will continue later (checkpoint offset_id=%s)", offset_id)
    return report


def oldest_available_bottom(min_bound: int, oldest_id_seen: int | None, since_dt: datetime | None) -> bool:
    if since_dt is not None and min_bound > 0:
        return True  # we iterated down to the id boundary computed from the date
    return max(min_bound, 1) == 1 or (oldest_id_seen is not None and oldest_id_seen == 1)


def _utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
