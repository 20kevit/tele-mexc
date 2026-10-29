"""Durable owner/developer Telegram notifications for the trading pipeline.

Notifications are written to an outbox *before* delivery is attempted.  The
outbox is independent from order execution: retrying a failed message can
never re-submit an order.  Secrets are never included in notification text.

Delivery outcomes:
  * ``sent`` — Telegram accepted the message (``ok``).
  * ``retry`` — transient network failure; schedule a later attempt.
  * ``ambiguous`` — the process ended mid-delivery or Telegram could not
    confirm acceptance; never retried automatically.
  * ``suppressed`` — the chat is blocked/deactivated or the owner disabled the
    category; recorded but never retried.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from ..control.bot_api import BotApiError, BotApiForbiddenError, BotApiRateLimitError
from .store import TradingStore

log = logging.getLogger("ingest.trading.notify")

# Telegram's Bot API limit is 4096 characters.  We keep a little headroom for
# chunk numbering and send longer notification bodies as ordered chunks instead
# of truncating the original Telegram signal text.
TELEGRAM_CHUNK_SIZE = 4000

# Backoff ceiling in seconds.  Retry-after values from Telegram are honored
# and capped here so a stuck notification never starves the whole outbox.
MAX_BACKOFF_S = 24 * 3600


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
        category: str = "signal_decision",
        event_type: str = "signal.decision",
        compact_text: str | None = None,
    ) -> int:
        """Persist one event for every selected recipient.

        ``roles`` is the central routing boundary: owner-only, developer-only,
        or both.  The outbox uniqueness key is ``(event_key, role)`` so retries
        and restarts cannot duplicate an event.
        """

        targets = self._targets(roles)
        if not targets:
            return 0
        # Route by persisted preference (falling back to code defaults).  The
        # owner explicitly opts out of routine local signal rejections; the
        # developer receives everything.
        allowed_targets = [
            (role, chat_id)
            for role, chat_id in targets
            if self.store.notification_enabled(role, category)
        ]
        if not allowed_targets:
            return 0
        # Chunk at enqueue time, one durable outbox row per chunk.  A retry
        # after a partial Telegram failure therefore resends only the chunks
        # that were not yet delivered, never the whole message again.
        chunks = self._chunks(text)
        enqueued = 0
        for role, chat_id in allowed_targets:
            for index, chunk in enumerate(chunks, 1):
                chunk_key = event_key if len(chunks) == 1 else f"{event_key}#{index}"
                inserted = self.store.enqueue_notification(
                    event_key=chunk_key,
                    signal_id=signal_id,
                    intent_id=intent_id,
                    role=role,
                    chat_id=chat_id,
                    text=chunk,
                    category=category[:80],
                    event_type=event_type[:120],
                    compact_text=compact_text,
                )
                if inserted:
                    enqueued += 1
        return enqueued

    def notify(
        self,
        *,
        event_key: str,
        text: str,
        signal_id: str | None = None,
        intent_id: str | None = None,
        roles: Iterable[str] | None = None,
        category: str = "signal_decision",
        event_type: str = "signal.decision",
        compact_text: str | None = None,
    ) -> None:
        """Enqueue + best-effort immediate delivery (never raises to caller)."""

        self.enqueue(
            event_key=event_key,
            text=text,
            signal_id=signal_id,
            intent_id=intent_id,
            roles=roles,
            category=category,
            event_type=event_type,
            compact_text=compact_text,
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
        self.notify(
            event_key=event_key,
            text=text,
            signal_id=signal_id,
            intent_id=intent_id,
            category="order_lifecycle",
        )

    def notify_intent(
        self, intent: dict[str, Any], event_key: str, text: str
    ) -> None:
        """Route an entry-intent event to both owner and developer."""
        self.notify(
            event_key=event_key, text=text,
            signal_id=intent.get("signal_id"), intent_id=intent.get("intent_id"),
            category="order_lifecycle",
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
        """Attempt pending/failed deliveries; return the number sent.

        Ambiguous (in-flight) deliveries from a crashed run are recovered to
        ``ambiguous`` so they are never retried automatically.
        """

        if self.bot is None:
            return 0
        if hasattr(self.store, "recover_ambiguous_sending"):
            try:
                recovered = self.store.recover_ambiguous_sending()
                if recovered:
                    log.info("recovered %d ambiguous notifications", recovered)
            except Exception:  # noqa: BLE001
                log.warning("ambiguous-notification recovery failed", exc_info=True)
        sent = 0
        for row in self.store.pending_notifications(limit=limit):
            try:
                self._deliver(row)
                self.store.finish_notification(
                    int(row["notification_id"]),
                    outcome="sent",
                    telegram_message_id=None,
                )
                sent += 1
            except BotApiForbiddenError as exc:
                # The owner/developer blocked or deactivated the chat.  Never
                # retry; record suppression and keep the outbox clean.
                log.warning("notification suppressed for chat %s (%s)", row["chat_id"], exc)
                self.store.finish_notification(
                    int(row["notification_id"]), outcome="suppressed", error=str(exc)
                )
            except BotApiRateLimitError as exc:
                # Honor Telegram's ``retry_after`` but cap it so one chat cannot
                # starve the whole outbox forever.
                retry_after = max(1, min(int(exc.retry_after or 30), MAX_BACKOFF_S))
                log.warning("rate limited notifying chat %s; retry in %ss", row["chat_id"], retry_after)
                self.store.finish_notification(
                    int(row["notification_id"]),
                    outcome="retry",
                    error=f"rate_limited retry_after={retry_after}",
                    retry_after_s=retry_after,
                )
            except BotApiError as exc:
                self.store.finish_notification(
                    int(row["notification_id"]),
                    outcome="failed",
                    error=f"BotApiError: {exc}",
                )
            except Exception as exc:  # noqa: BLE001 - transport boundary
                self.store.finish_notification(
                    int(row["notification_id"]),
                    outcome="retry",
                    error=f"{exc.__class__.__name__}",
                )
        return sent

    def _deliver(self, row: dict[str, Any]) -> None:
        if self.bot is None:
            return
        try:
            self.bot.send_message(int(row["chat_id"]), str(row["text"]))
        except TypeError:
            # Fakes and older clients accept only (chat_id, text).
            self.bot.send_message(int(row["chat_id"]), str(row["text"]))

    def pending_count(self) -> int:
        return len(self.store.pending_notifications(limit=1000))
