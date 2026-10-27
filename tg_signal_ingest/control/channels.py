"""Eligible Telegram channel discovery for the control bot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from telethon.tl.types import Channel


@dataclass(frozen=True)
class ChannelChoice:
    channel_id: int
    access_hash: int
    title: str
    username: str | None
    kind: str
    last_activity: str | None = None


def _eligible(entity: Any) -> bool:
    # Telethon's iter_dialogs returns raw TL entities. Channel objects cover
    # both broadcast channels and supergroups, while Chat covers ordinary
    # small groups and User covers private chats. Raw Channel objects do not
    # expose convenience attributes such as ``is_channel``.
    if not isinstance(entity, Channel):
        return False
    return not getattr(entity, "deleted", False)


def _label(entity: Any, title: str) -> str:
    return "گروه" if getattr(entity, "megagroup", False) else "کانال"


async def fetch_channels(
    client: Any, *, offset: int = 0, limit: int = 10, query: str | None = None
) -> tuple[list[ChannelChoice], bool]:
    """Return up to ``limit`` eligible dialogs and whether more remain.

    Only broadcast channels and supergroups are eligible; users, bots, and
    ordinary small groups are never listed. No backfill or trading is started.
    """

    needle = (query or "").strip().lstrip("@").casefold()
    found: list[tuple[int, datetime, ChannelChoice]] = []
    sequence = 0
    async for dialog in client.iter_dialogs(limit=None):
        entity = dialog.entity
        if not _eligible(entity):
            continue
        title = str(dialog.name or getattr(entity, "title", "") or "")
        username = getattr(entity, "username", None)
        if needle and needle not in title.casefold() and needle not in str(username or "").casefold():
            continue
        activity = getattr(dialog, "date", None)
        if isinstance(activity, datetime):
            activity_dt = activity if activity.tzinfo else activity.replace(tzinfo=UTC)
        else:
            activity_dt = datetime.min.replace(tzinfo=UTC)
        found.append(
            (
                sequence,
                activity_dt,
                ChannelChoice(
                    channel_id=int(entity.id),
                    access_hash=int(getattr(entity, "access_hash", 0) or 0),
                    title=title or f"channel {entity.id}",
                    username=str(username) if username else None,
                    kind=_label(entity, title),
                    last_activity=activity_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
                    if activity_dt.year > 1
                    else None,
                ),
            )
        )
        sequence += 1
    # Explicit activity ordering; stable sequence preserves deterministic
    # behavior when Telegram omits a dialog date in tests or older clients.
    found.sort(key=lambda item: (item[1], -item[0]), reverse=True)
    ordered = [item[2] for item in found]
    total = len(ordered)
    collected = ordered[offset : offset + limit]
    return collected, total > offset + len(collected)


async def find_channel(client: Any, channel_id: int) -> ChannelChoice | None:
    """Revalidate a callback-supplied id against the live dialog list."""

    async for dialog in client.iter_dialogs(limit=None):
        entity = dialog.entity
        if _eligible(entity) and int(entity.id) == int(channel_id):
            title = str(dialog.name or getattr(entity, "title", "") or "")
            username = getattr(entity, "username", None)
            return ChannelChoice(
                channel_id=int(channel_id),
                access_hash=int(getattr(entity, "access_hash", 0) or 0),
                title=title or f"channel {channel_id}",
                username=str(username) if username else None,
                kind=_label(entity, title),
            )
    return None
