"""Durable owner/developer Telegram notifications for the trading pipeline.

Notifications are written to an outbox *before* delivery is attempted.  The
outbox is independent from order execution: retrying a failed message can
never re-submit an order.  Secrets are never included in notification text.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from ..control.bot_api import BotApiError
from .store import TradingStore

log = logging.getLogger("ingest.trading.notify")

# Telegram's Bot API limit is 4096 characters.  We keep a little headroom for
# chunk numbering and send longer notification bodies as ordered chunks instead
# of truncating the original Telegram signal text.
TELEGRAM_CHUNK_SIZE = 4000


class TradingNotifier:
    def __init__(
        self,
        store: TradingStore,
        bot: Any | None,
        *,
        owner_id: int | None,
        developer_id: int | None,
    ):
        self.store = store
        self.bot = bot
        self.recipients: list[tuple[str, int]] = []
        if owner_id:
            self.recipients.append(("owner", int(owner_id)))
        if developer_id:
            self.recipients.append(("developer", int(developer_id)))

    @property
    def configured(self) -> bool:
        return bool(self.bot and self.recipients)

    def _targets(self, roles: Iterable[str] | None) -> list[tuple[str, int]]:
        if roles is None:
            return list(self.recipients)
        allowed = {str(role) for role in roles}
        return [(role, chat_id) for role, chat_id in self.recipients if role in allowed]

    def enqueue(
        self,
        *,
        event_key: str,
        text: str,
        signal_id: str | None = None,
        intent_id: str | None = None,
        roles: Iterable[str] | None = None,
    ) -> int:
        """Persist one event for every selected recipient.

        ``roles`` is the central routing boundary: owner-only, developer-only,
        or both.  The outbox uniqueness key is ``(event_key, role)`` so retries
        and restarts cannot duplicate an event.
        """

        targets = self._targets(roles)
        if not targets:
            return 0
        # Chunk at enqueue time, one durable outbox row per chunk.  A retry
        # after a partial Telegram failure therefore resends only the chunks
        # that were not yet delivered, never the whole message again.
        chunks = self._chunks(text)
        for role, chat_id in targets:
            for index, chunk in enumerate(chunks, 1):
                chunk_key = event_key if len(chunks) == 1 else f"{event_key}#{index}"
                self.store.enqueue_notification(
                    event_key=chunk_key,
                    signal_id=signal_id,
                    intent_id=intent_id,
                    role=role,
                    chat_id=chat_id,
                    text=chunk,
                )
        return len(targets)

    def notify(
        self,
        *,
        event_key: str,
        text: str,
        signal_id: str | None = None,
        intent_id: str | None = None,
        roles: Iterable[str] | None = None,
    ) -> None:
        """Enqueue + best-effort immediate delivery (never raises to caller)."""

        self.enqueue(
            event_key=event_key,
            text=text,
            signal_id=signal_id,
            intent_id=intent_id,
            roles=roles,
        )
        try:
            self.flush()
        except Exception as exc:  # noqa: BLE001 - notifications never break trading
            log.warning("notification flush failed (%s)", exc.__class__.__name__)

    def notify_trade(
        self,
        trade: dict[str, Any] | None,
        event_key: str,
        text: str,
        **extra: Any,
    ) -> None:
        """Route a trade-lifecycle event to both owner and developer."""
        signal_id = trade.get("signal_id") if trade else None
        intent_id = trade.get("entry_intent_id") if trade else None
        self.notify(event_key=event_key, text=text, signal_id=signal_id, intent_id=intent_id)

    def notify_intent(
        self, intent: dict[str, Any], event_key: str, text: str
    ) -> None:
        """Route an entry-intent event to both owner and developer."""
        self.notify(
            event_key=event_key, text=text,
            signal_id=intent.get("signal_id"), intent_id=intent.get("intent_id"),
        )

    @staticmethod
    def _chunks(text: str) -> list[str]:
        if len(text) <= TELEGRAM_CHUNK_SIZE:
            return [text]
        return [
            text[offset : offset + TELEGRAM_CHUNK_SIZE]
            for offset in range(0, len(text), TELEGRAM_CHUNK_SIZE)
        ]

    def flush(self, limit: int = 100) -> int:
        """Attempt pending/failed deliveries; return the number sent."""

        if self.bot is None:
            return 0
        sent = 0
        for row in self.store.pending_notifications(limit=limit):
            try:
                self.bot.send_message(int(row["chat_id"]), str(row["text"]))
            except BotApiError as exc:
                self.store.mark_notification_failed(
                    int(row["notification_id"]), f"BotApiError: {exc}"
                )
            except Exception as exc:  # noqa: BLE001 - transport boundary
                self.store.mark_notification_failed(
                    int(row["notification_id"]), f"{exc.__class__.__name__}"
                )
            else:
                self.store.mark_notification_sent(int(row["notification_id"]))
                sent += 1
        return sent

    def pending_count(self) -> int:
        return len(self.store.pending_notifications(limit=1000))
