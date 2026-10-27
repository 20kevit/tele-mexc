"""Signal -> intent -> exchange order -> reconciliation pipeline.

Safety invariants:
* Every intent exists in SQLite before any exchange call.
* The idempotency key is derived from the Telegram message identity (not the
  edit version), so editing a signal can never place a second order.
* A submit whose outcome is uncertain becomes
  ``unknown_requires_reconciliation`` and is *never* retried automatically.
* Live mode is explicit; simulation and official test-order modes never call
  the matching engine.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import AppConfig
from ..mexc.errors import (
    AccidentalOrderRiskError,
    MexcDuplicateOrder,
    MexcError,
    MexcNetworkError,
    MexcOrderNotFound,
    MexcRateLimitError,
    MexcServerError,
    MexcUnexpectedResponse,
    MexcWafError,
)
from ..repository import QueueItem, Repository
from .balance import BalanceError, parse_account, validate_balance
from .parser import ParsedSignal, SignalParser
from .risk import RiskDecision, RiskManager
from .store import TERMINAL_INTENT_STATES, RiskLimitExceeded, TradingStore, dec_str

log = logging.getLogger("ingest.trading")

LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
EXCHANGE_TERMINAL = {"FILLED", "CANCELED", "PARTIALLY_CANCELED"}

# Central notification routing.  Genuine signals go to both recipients; only
# non-signal diagnostics go to the developer alone.
NOTIFY_BOTH = ("owner", "developer")
NOTIFY_DEVELOPER = ("developer",)


@dataclass
class ReconcileStats:
    checked: int = 0
    resolved: int = 0
    ambiguous: int = 0
    discrepancies: int = 0
    not_found: int = 0
    errors: int = 0


class TradingEngine:
    def __init__(
        self,
        cfg: AppConfig,
        repo: Repository,
        *,
        client: Any | None = None,
        registry: Any | None = None,
        price_provider: Any | None = None,
        parser: SignalParser | None = None,
        now: Any | None = None,
        monotonic: Any | None = None,
        notifier: Any | None = None,
        trade_manager: Any | None = None,
    ):
        self.cfg = cfg
        self.repo = repo
        self._now = now or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self.store = TradingStore(repo.db, cfg.trading, now=self._now)
        self.parser = parser or SignalParser()
        self.risk = RiskManager(cfg.trading, self.store)
        self.client = client
        self.registry = registry
        self._price_provider = price_provider
        # Durable owner/developer reporting; never a prerequisite for an order.
        self.notifier = notifier
        # Trade lifecycle coordinator (conditional entry, TP1/TP2, SL).  When
        # present and mode is live it owns entry submission for live intents.
        self.trade_manager = trade_manager
        # The processing queue is normally single-consumer, but concurrent
        # delivery/retry can call this from more than one thread. Serializing
        # the whole decision keeps the DB reservation and execution atomic.
        self._process_lock = threading.RLock()

    # ------------------------------------------------------------ ingestion

    def process_item(self, item: QueueItem, record: dict[str, Any]) -> dict[str, Any] | None:
        """Handle one queue item. Returns an intent dict when one is created."""

        with self._process_lock:
            return self._process_item(item, record)

    def _process_item(
        self, item: QueueItem, record: dict[str, Any]
    ) -> dict[str, Any] | None:
        if item.item_type == "delete" or int(record.get("deleted") or 0):
            self.handle_delete(item.channel_id, item.message_id)
            return None

        text = record.get("text") or record.get("caption")
        version = int(record.get("processing_version") or 0)
        parsed = self.parser.parse(text, is_edit=item.item_type == "edit" or version > 0)
        signal_id, inserted = self.store.record_signal(
            parsed,
            channel_id=item.channel_id,
            message_id=item.message_id,
            edit_version=version,
        )

        # Historical backfill and any message at/below the startup boundary
        # are stored and audited but are completely silent and non-tradeable.
        # This check runs before every notification/order path so a downtime
        # backlog can never flood recipients or become fresh orders.
        historical = self._is_historical(item, record)
        if historical:
            if parsed.kind in ("alert", "update", "ambiguous", "cancel"):
                boundary = self.store.activation_boundary()
                if record.get("source") == "backfill":
                    reason = "historical backfill message; only new Telegram messages can trade"
                elif boundary is None:
                    reason = "trading activation boundary is not set; refusing to trade"
                else:
                    reason = (
                        f"message is at or below the activation boundary {boundary}; "
                        "cannot be distinguished from historical replay"
                    )
                self.store.update_signal(signal_id, "rejected", reason)
                self.store.audit(signal_id, "lifecycle", "rejected_replay", reason)
            else:
                self.store.audit(signal_id, "parse", parsed.kind, parsed.reason_text)
            return None

        # Classify once, after the historical/replay guard.  Genuine trading
        # signals always go to both recipients; ordinary channel messages are
        # developer-only NOT_A_SIGNAL diagnostics.
        genuine = self._is_genuine_signal(parsed)
        if not genuine:
            self._notify_non_signal(signal_id, parsed, record=record)
            if parsed.kind in ("info", "result"):
                self.store.audit(signal_id, "parse", parsed.kind, parsed.reason_text)
                return None
            if parsed.kind == "cancel":
                self.store.update_signal(signal_id, "parsed", "cancel signal")
                if (
                    self.cfg.trading.mode in ("testonly", "live")
                    and record.get("source") == "backfill"
                ):
                    reason = "historical cancel imported by backfill; not applied to live orders"
                    self.store.update_signal(signal_id, "rejected", reason)
                    self.store.audit(signal_id, "lifecycle", "cancel_ignored_backfill", reason)
                    return None
                self._handle_cancel(parsed, signal_id, item.channel_id, item.message_id)
                return None
            reason = "; ".join(parsed.reasons) or "not an actionable trading signal"
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "parse", "not_a_signal", reason)
            return None

        # A genuine BUY/SELL candidate that the parser could not turn into an
        # executable alert (missing entry/symbol, Futures instruction, ...) is
        # a local rejection and is reported to both recipients.
        if parsed.kind == "ambiguous":
            reason = "; ".join(parsed.reasons) or "ambiguous signal"
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "parse", "rejected_ambiguous", reason)
            self._notify_rejected(signal_id, parsed, reason, record=record)
            return None

        # Historical/backfill traffic and anything at or below the one-time
        # activation boundary is never tradeable.  This is an identity/replay
        # guard, not an age-based rejection.
        replay_reason: str | None = None
        if (
            self.cfg.trading.mode in ("testonly", "live")
            and record.get("source") == "backfill"
        ):
            replay_reason = "historical backfill message; only new Telegram messages can trade"
        elif self.cfg.trading.mode in ("testonly", "live"):
            boundary = self.store.activation_boundary()
            if boundary is None:
                replay_reason = "trading activation boundary is not set; refusing to trade"
            elif item.message_id <= boundary:
                replay_reason = (
                    f"message is at or below the activation boundary {boundary}; "
                    "cannot be distinguished from historical replay"
                )
        if replay_reason is not None:
            self.store.update_signal(signal_id, "rejected", replay_reason)
            self.store.audit(signal_id, "lifecycle", "rejected_replay", replay_reason)
            if not self._is_historical(item, record):
                self._notify_rejected(signal_id, parsed, replay_reason, record=record)
            return None

        if self.store.has_later_cancel(item.channel_id, parsed.symbol, item.message_id):
            reason = "a later cancel signal for this symbol was already processed"
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "lifecycle", "rejected_after_cancel", reason)
            self._notify_rejected(signal_id, parsed, reason, record=record)
            return None

        idempotency_key = self.idempotency_key(item.channel_id, item.message_id, parsed)
        client_order_id = "tsi" + idempotency_key[:28]
        existing_intent = self.store.intent_for_message(item.channel_id, item.message_id)
        if existing_intent is not None and existing_intent["idempotency_key"] != idempotency_key:
            reason = (
                "Telegram message already has an order intent with a different "
                "action/symbol; refusing to place a second order"
            )
            self.store.update_signal(signal_id, "superseded", reason)
            self.store.audit(signal_id, "lifecycle", "conflicting_edit", reason)
            self._notify_intent_decision(
                existing_intent,
                status="DUPLICATE_IGNORED",
                reason=reason,
                record=record,
                signal_id=signal_id,
            )
            return existing_intent

        reference = self._reference_price(parsed)
        meta = self._symbol_meta(parsed.symbol)
        if self.cfg.trading.mode in ("testonly", "live") and meta is None:
            reason = "symbol metadata unavailable; refusing to trade without exchange constraints"
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "validate", "rejected", reason)
            self._notify_rejected(signal_id, parsed, reason, record=record)
            return None
        decision = self.risk.evaluate(
            parsed,
            message_datetime=record.get("datetime"),
            reference_price=reference,
            base_asset=getattr(meta, "base_asset", None),
            now=self._now(),
            symbol_meta=meta,
        )
        if not decision.approved:
            self.store.update_signal(signal_id, "rejected", decision.reason)
            self.store.audit(signal_id, "risk", "rejected", decision.reason)
            self._notify_rejected(signal_id, parsed, decision.reason, record=record)
            return None

        quantized = self._quantize_decision(decision, meta, parsed.action or "")
        if quantized is None:
            reason = "quantized order violates symbol constraints"
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "validate", "rejected", reason)
            self._notify_rejected(signal_id, parsed, reason, record=record)
            return None
        decision = quantized

        self.store.update_signal(signal_id, "validated")
        try:
            intent, inserted = self.store.create_intent(
                signal=parsed,
                signal_id=signal_id,
                channel_id=item.channel_id,
                message_id=item.message_id,
                edit_version=version,
                idempotency_key=idempotency_key,
                client_order_id=client_order_id,
                mode=self.cfg.trading.mode,
                order_type=decision.order_type,
                quote_amount=decision.quote_amount or Decimal(0),
                base_quantity=decision.base_quantity,
                limit_price=decision.limit_price,
            )
        except RiskLimitExceeded as exc:
            reason = str(exc)
            self.store.update_signal(signal_id, "rejected", reason)
            self.store.audit(signal_id, "risk", "aggregate_budget_rejected", reason)
            self._notify_rejected(signal_id, parsed, reason, record=record)
            return None

        if not inserted:
            reason = f"intent already exists for Telegram message ({intent['intent_id']})"
            self.store.update_signal(signal_id, "superseded", reason)
            self.store.audit(signal_id, "lifecycle", "duplicate_intent", reason)
            return self._resume_planned_intent(intent, parsed, reference)

        self.store.audit(signal_id, "lifecycle", "intent_planned", intent["intent_id"])
        # Trade lifecycle coordinator: conditional entry, TP1/TP2, SL.
        # Only intervenes in live mode; sim/testonly keep their existing path.
        defer = False
        if self.trade_manager is not None:
            trade_decision = self.trade_manager.on_intent_planned(intent, parsed, reference)
            if trade_decision.rejected:
                # Fail-closed: do NOT submit an entry order.
                return intent
            if trade_decision.deferred and trade_decision.trade is not None:
                defer = True
                self.store.audit(
                    signal_id, "lifecycle", "conditional_entry_waiting",
                    f"trade {trade_decision.trade['trade_id']} waiting for upward trigger "
                    f"at {trade_decision.trade['trigger_price']}",
                )
                self._notify(
                    event_key=f"{intent['intent_id']}:conditional_entry_waiting",
                    signal_id=signal_id,
                    intent_id=intent["intent_id"],
                    text=(
                        f"{intent['symbol']}: entry {intent['limit_price']} above the market "
                        f"({reference}); waiting for upward trigger "
                        f"{trade_decision.trade['trigger_price']} before submitting a Limit Buy "
                        f"(max price = entry)."
                    ),
                )
        if defer:
            return intent
        result = self._execute_intent(intent["intent_id"])
        if self.trade_manager is not None and result is not None:
            self.trade_manager.on_intent_executed(result)
        return result

    # -------------------------------------------------------------- helpers

    def _resume_planned_intent(
        self,
        intent: dict[str, Any],
        parsed: ParsedSignal,
        reference: Decimal | None,
    ) -> dict[str, Any]:
        """Resume a ``planned`` intent left behind by a crash or failed retry.

        Only ``planned`` is resumable: it proves no exchange call was claimed.
        ``submitting``/``unknown_requires_reconciliation`` are never retried
        automatically.  A conditional-entry trade stays deferred until its
        trigger fires.
        """

        current = self.store.get_intent(intent["intent_id"]) or intent
        if current.get("state") != "planned":
            return current
        self.store.audit(
            current["intent_id"], "lifecycle", "planned_intent_resumed",
            "retry/duplicate delivery resumed a planned intent that was never submitted",
        )
        if self.trade_manager is not None:
            decision = self.trade_manager.on_intent_planned(current, parsed, reference)
            if decision.rejected:
                return current
            if decision.deferred:
                self.store.audit(
                    current["intent_id"], "lifecycle", "conditional_entry_waiting",
                    "resumed intent remains a conditional entry waiting for its trigger",
                )
                return current
        result = self._execute_intent(current["intent_id"])
        if self.trade_manager is not None and result is not None:
            self.trade_manager.on_intent_executed(result)
        return result

    def _is_at_or_below_boundary(self, item: QueueItem) -> bool:
        if self.cfg.trading.mode not in ("testonly", "live"):
            return False
        boundary = self.store.activation_boundary()
        return boundary is None or item.message_id <= boundary

    def _is_historical(self, item: QueueItem, record: dict[str, Any]) -> bool:
        """True for backfill or any message at/below the activation boundary."""

        return record.get("source") == "backfill" or self._is_at_or_below_boundary(item)

    @staticmethod
    def idempotency_key(channel_id: int, message_id: int, signal: ParsedSignal) -> str:
        raw = f"{channel_id}:{message_id}:{signal.action}:{signal.symbol}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _reference_price(self, signal: ParsedSignal) -> Decimal | None:
        if self._price_provider is not None:
            try:
                return Decimal(str(self._price_provider(signal.symbol)))
            except (InvalidOperation, TypeError, ValueError, MexcError):
                return None
        if self.cfg.trading.mode == "sim":
            return signal.entry_price
        if self.client is None or not signal.symbol:
            return None
        return self.fetch_market_price(signal.symbol)

    def fetch_market_price(self, symbol: str) -> Decimal | None:
        """Read the current Spot price for the market monitor.

        Returns None on any failure; the monitor treats a missing/short price
        as stale and never triggers an order from it.
        """

        if self.client is None or not symbol:
            return None
        try:
            ticker = self.client.ticker_price(symbol=symbol)
            price = ticker.get("price") if isinstance(ticker, dict) else None
            if price is None:
                return None
            value = Decimal(str(price))
        except (MexcError, InvalidOperation, TypeError, ValueError):
            return None
        return value if value.is_finite() and value > 0 else None

    def _symbol_meta(self, symbol: str | None) -> Any | None:
        if self.cfg.trading.mode == "sim" or not symbol:
            return None
        registry = self.registry or getattr(self.client, "symbols", None)
        if registry is None:
            return None
        try:
            return registry.get(symbol)
        except Exception as exc:  # noqa: BLE001 - converted to a risk rejection
            log.warning("symbol metadata lookup failed for %s: %s", symbol, exc.__class__.__name__)
            return None

    @staticmethod
    def _quantize_decision(decision: RiskDecision, meta: Any | None, side: str) -> RiskDecision | None:
        if meta is None:
            return decision
        try:
            qty = meta.quantize_quantity(decision.base_quantity) if decision.base_quantity else None
            price = meta.quantize_price(decision.limit_price) if decision.limit_price else None
            quote = decision.quote_amount
            if decision.order_type == "limit" and qty is not None and price is not None:
                quote = qty * price
            meta.validate_order(
                side=side.upper(),
                order_type=decision.order_type.upper(),
                quantity=qty,
                quote_amount=quote,
                price=price,
            )
        except Exception:  # noqa: BLE001 - constraints are fail-closed
            return None
        return RiskDecision(
            approved=True,
            reasons=(),
            order_type=decision.order_type,
            reference_price=decision.reference_price,
            limit_price=price,
            quote_amount=quote,
            base_quantity=qty,
        )

    # --------------------------------------------------------- notifications

    def _notify(
        self,
        *,
        event_key: str,
        text: str,
        signal_id: str | None = None,
        intent_id: str | None = None,
        roles: Iterable[str] | None = None,
    ) -> None:
        """Best-effort durable notification; never affects order handling."""

        if self.notifier is None:
            return
        try:
            self.notifier.notify(
                event_key=event_key,
                text=text,
                signal_id=signal_id,
                intent_id=intent_id,
                roles=roles,
            )
        except Exception as exc:  # noqa: BLE001 - reporting must not break execution
            log.warning("notification enqueue failed (%s)", exc.__class__.__name__)

    @staticmethod
    def _is_genuine_signal(parsed: ParsedSignal) -> bool:
        """A genuine trading signal has a recognizable BUY or SELL direction.

        ``ambiguous`` is included when the parser detected a direction: those
        are real signals that failed local validation (missing entry/symbol,
        unsupported Futures instruction, ...), not ordinary chatter.
        """

        return parsed.kind in ("alert", "update", "ambiguous") and parsed.action in ("buy", "sell")

    def _source_ref(
        self,
        *,
        record: dict[str, Any] | None = None,
        signal_id: str | None = None,
        intent: dict[str, Any] | None = None,
    ) -> str:
        if record is not None:
            return (
                f"channel {record.get('channel_id')} / message {record.get('message_id')} "
                f"(v{record.get('processing_version')})"
            )
        if intent is not None:
            return (
                f"channel {intent.get('create_channel_id')} / "
                f"message {intent.get('create_msg_id')} (signal {intent.get('signal_id')})"
            )
        if signal_id:
            parts = signal_id.split(":")
            if len(parts) >= 2:
                return f"channel {parts[0]} / message {parts[1]}"
        return signal_id or "unknown source"

    def _original_text(
        self,
        *,
        record: dict[str, Any] | None = None,
        signal_id: str | None = None,
        intent: dict[str, Any] | None = None,
    ) -> str:
        """Return the exact source text, never normalized or rewritten."""

        if record is None:
            if intent is not None:
                try:
                    record = self.store.get_message(
                        int(intent["create_channel_id"]), int(intent["create_msg_id"])
                    )
                except (KeyError, TypeError, ValueError):
                    record = None
            elif signal_id:
                parts = signal_id.split(":")
                if len(parts) >= 2:
                    try:
                        record = self.store.get_message(int(parts[0]), int(parts[1]))
                    except (TypeError, ValueError):
                        record = None
        if record is not None:
            text = record.get("text")
            caption = record.get("caption")
            if text:
                return str(text)
            if caption:
                return str(caption)
            return f"[no text content; media={record.get('media_type') or 'unknown'}]"
        return "[original message text unavailable]"

    def _notify_decision(
        self,
        *,
        event_key: str,
        status: str,
        symbol: str | None,
        direction: str | None,
        reason: str,
        classification: str | None = None,
        record: dict[str, Any] | None = None,
        signal_id: str | None = None,
        intent: dict[str, Any] | None = None,
        submission_attempted: bool = False,
        diagnostics: str | None = None,
        roles: Iterable[str] | None = NOTIFY_BOTH,
    ) -> None:
        """Central formatter for signal decisions and diagnostics."""

        lines = [
            f"status: {status}",
            f"source: {self._source_ref(record=record, signal_id=signal_id, intent=intent)}",
            f"symbol: {symbol or '—'}",
            f"direction: {str(direction or '').upper() or '—'}",
        ]
        if classification:
            lines.append(f"classification: {classification}")
        lines.append(f"decision: {reason}")
        lines.append(
            f"exchange submission attempted: {'yes' if submission_attempted else 'no'}"
        )
        if diagnostics:
            lines.append(f"diagnostics: {diagnostics}")
        lines.append("original message:")
        lines.append(self._original_text(record=record, signal_id=signal_id, intent=intent))
        self._notify(
            event_key=event_key,
            signal_id=signal_id or (intent.get("signal_id") if intent else None),
            intent_id=intent.get("intent_id") if intent else None,
            text="\n".join(lines),
            roles=roles,
        )

    def _notify_non_signal(
        self,
        signal_id: str,
        parsed: ParsedSignal,
        *,
        record: dict[str, Any] | None = None,
    ) -> None:
        """Developer-only diagnostic for a message that is not a trading signal."""

        if parsed.kind == "result":
            reason = "result/take-profit follow-up, not a new trading signal"
        elif parsed.kind == "cancel":
            reason = "cancel/control message, not a new trading signal"
        elif parsed.kind == "info":
            reason = parsed.reason_text or "ordinary channel message, not an actionable trading signal"
        else:
            reason = parsed.reason_text or "not an actionable trading signal"
        self._notify_decision(
            event_key=f"{signal_id}:not_a_signal",
            status="NOT_A_SIGNAL",
            symbol=parsed.symbol,
            direction=parsed.action,
            reason=reason,
            classification=parsed.kind,
            record=record,
            signal_id=signal_id,
            diagnostics=parsed.reason_text or None,
            roles=NOTIFY_DEVELOPER,
        )

    def _notify_rejected(
        self,
        signal_id: str,
        parsed: ParsedSignal,
        reason: str,
        *,
        record: dict[str, Any] | None = None,
    ) -> None:
        """Report a genuine signal rejected locally, to owner and developer."""

        if parsed.action == "sell":
            status = "IGNORED_SELL_SPOT_BUY_ONLY"
            headline = "سیگنال فروش نادیده گرفته می‌شود❌ — این ربات فقط Spot BUY است"
            detail = (
                "intentionally ignored: this bot executes Spot BUY only; "
                "no exchange order was submitted. " + reason
            )
        else:
            status = "REJECTED_LOCALLY"
            headline = "🚫 سیگنال رد شد — سفارشی ارسال نشد"
            detail = f"no exchange order was submitted. {reason}"
        self._notify_decision(
            event_key=f"{signal_id}:decision:{status}",
            status=status,
            symbol=parsed.symbol,
            direction=parsed.action,
            reason=f"{headline} | {detail}",
            classification=parsed.kind,
            record=record,
            signal_id=signal_id,
            diagnostics=parsed.reason_text or None,
        )

    def _notify_intent_decision(
        self,
        intent: dict[str, Any],
        *,
        status: str,
        reason: str,
        record: dict[str, Any] | None = None,
        signal_id: str | None = None,
    ) -> None:
        """Report a local intent decision (no exchange submission) to both."""

        self._notify_decision(
            event_key=f"{intent['intent_id']}:decision:{status}",
            status=status,
            symbol=intent.get("symbol"),
            direction=intent.get("side"),
            reason=f"no exchange order was submitted. {reason}",
            classification="ORDER_INTENT",
            record=record,
            signal_id=signal_id or intent.get("signal_id"),
            intent=intent,
            submission_attempted=False,
        )

    @staticmethod
    def _order_status_label(intent: dict[str, Any]) -> str:
        """Truthful status derived from state, exchange status and fill data."""

        state = str(intent.get("state") or "")
        exchange_status = str(intent.get("exchange_status") or "").upper()
        requested = _optional_decimal(intent.get("base_quantity")) or Decimal(0)
        executed = _optional_decimal(intent.get("executed_qty")) or Decimal(0)
        partial = requested > 0 and executed > 0 and executed < requested
        if state == "filled" or exchange_status == "FILLED":
            return "PARTIALLY_FILLED" if partial else "FILLED"
        if exchange_status == "PARTIALLY_FILLED":
            return "PARTIALLY_FILLED"
        if state == "canceled":
            return "CANCELED_WITH_PARTIAL_FILL" if executed > 0 else "CANCELED"
        if state == "unknown_requires_reconciliation":
            return "UNCERTAIN_AWAITING_RECONCILIATION"
        if state == "rejected":
            return "SUBMISSION_REJECTED" if intent.get("mode") == "live" else "REJECTED_LOCALLY"
        if state in ("submitted", "open", "reconciling"):
            return "ACCEPTED_OPEN"
        if state == "submitting":
            return "SUBMISSION_IN_PROGRESS"
        if state == "canceled_local":
            return "CANCELED_LOCALLY"
        return state.upper() or "UNKNOWN"

    @staticmethod
    def _headline_for_status(status: str) -> str:
        headlines = {
            "FILLED": "✅ سفارش واقعی در MEXC ثبت و پر شد",
            "PARTIALLY_FILLED": "🟡 سفارش واقعی در MEXC — پر شده به‌صورت جزئی",
            "ACCEPTED_OPEN": "✅ سفارش واقعی در MEXC ثبت شد (پر نشده)",
            "CANCELED_WITH_PARTIAL_FILL": "⚠️ سفارش در MEXC لغو شد — پر شده به‌صورت جزئی",
            "CANCELED": "⚠️ سفارش در MEXC لغو/بسته شد",
            "UNCERTAIN_AWAITING_RECONCILIATION": (
                "⚠️ وضعیت سفارش نامشخص است — نیاز به تطبیق دستی"
            ),
            "SUBMISSION_REJECTED": "❌ سفارش در MEXC رد شد — ارسال موفقیت‌آمیز تایید نشد",
        }
        return headlines.get(status, f"وضعیت سفارش: {status}")

    def _intent_text(
        self,
        intent: dict[str, Any],
        headline: str,
        *,
        submission_attempted: bool,
        status_label: str | None = None,
    ) -> str:
        """Format an actual order result for owner/developer notifications."""

        status = status_label or self._order_status_label(intent)
        lines = [
            headline,
            f"status: {status}",
            f"source: {self._source_ref(intent=intent)}",
            f"symbol: {intent.get('symbol') or '—'}",
            f"side: {str(intent.get('side') or '').upper() or '—'} (Spot)",
            f"type: {str(intent.get('order_type') or '').upper() or '—'}",
            f"fixed quote: {intent.get('quote_amount') or '—'} USDT",
            f"quantity: {intent.get('base_quantity') or '—'}",
            f"limit price: {intent.get('limit_price') or '—'}",
            f"state: {intent.get('state')}",
            f"exchange order id: {intent.get('exchange_order_id') or '—'}",
            f"exchange status: {intent.get('exchange_status') or '—'}",
            f"executed qty: {intent.get('executed_qty') or '—'}",
            f"cumulative quote: {intent.get('cumulative_quote') or '—'}",
            f"exchange submission attempted: {'yes' if submission_attempted else 'no'}",
        ]
        try:
            requested = Decimal(str(intent.get("base_quantity") or "0"))
            executed = Decimal(str(intent.get("executed_qty") or "0"))
            if requested > 0:
                lines.append(
                    f"remaining quantity: {format(max(Decimal(0), requested - executed), 'f')}"
                )
        except (InvalidOperation, ValueError):
            pass
        fills = []
        order_id = intent.get("exchange_order_id")
        if order_id:
            try:
                fills = self.store.fills_for_order(str(order_id))
            except Exception:  # noqa: BLE001 - reporting only
                fills = []
        if fills:
            fee_parts = [
                f"{row.get('fee_amount')} {row.get('fee_asset') or ''}".strip()
                for row in fills
                if row.get("fee_amount") not in (None, "")
            ]
            if fee_parts:
                lines.append("fees: " + ", ".join(fee_parts))
            prices = [Decimal(str(row["price"])) * Decimal(str(row["quantity"])) for row in fills]
            total_qty = sum((Decimal(str(row["quantity"])) for row in fills), Decimal(0))
            if total_qty > 0:
                avg = sum(prices, Decimal(0)) / total_qty
                lines.append(f"average fill price: {format(avg, 'f')}")
        if intent.get("reject_reason"):
            lines.append(f"reason: {intent['reject_reason']}")
        summary = self._budget_summary()
        if summary is not None:
            committed = summary["spent"] + summary["reserved"]
            lines.append(
                "cumulative tracked spend: "
                f"{format(committed, 'f')} USDT "
                f"(reference {summary['limit']} USDT; not an order cap)"
            )
        lines.append("original message:")
        lines.append(self._original_text(intent=intent))
        return "\n".join(lines)

    def _budget_summary(self) -> dict[str, Decimal] | None:
        try:
            return self.store.budget_summary()
        except Exception:  # noqa: BLE001 - reporting must never break trading
            return None

    def balance_check(self, intent: dict[str, Any]) -> str | None:
        """Public fail-closed balance preflight used by the approval UI."""

        return self._balance_preflight(intent)

    def execute_approved_intent(self, intent_id: str) -> dict[str, Any]:
        """Revalidate immediately before submission, then execute.

        Approval is never treated as a bypass of risk, age, symbol, emergency
        stop, or balance checks. Any failed revalidation leaves the intent
        rejected and no exchange call is made.
        """

        intent = self.store.get_intent(intent_id)
        if intent is None:
            raise RuntimeError(f"intent {intent_id} not found")
        if intent["state"] != "planned":
            return intent
        signal = self.store.get_signal(intent["signal_id"])
        if signal is None or signal["status"] not in ("planned", "validated"):
            return self._reject_before_submit(
                intent, "signal is no longer active for this order"
            )
        if signal.get("symbol") != intent.get("symbol") or signal.get("action") != intent.get("side"):
            return self._reject_before_submit(
                intent, "signal fields changed after the intent was planned"
            )
        if self.cfg.trading.emergency_stop:
            return self._reject_before_submit(intent, "emergency stop is active")
        symbol = str(intent.get("symbol") or "")
        meta = self._symbol_meta(symbol)
        if self.cfg.trading.mode in ("testonly", "live") and meta is None:
            return self._reject_before_submit(intent, "symbol metadata is unavailable")
        if meta is not None:
            try:
                meta.validate_order(
                    side=str(intent["side"]).upper(),
                    order_type=str(intent["order_type"]).upper(),
                    quantity=_optional_decimal(intent.get("base_quantity")),
                    quote_amount=_optional_decimal(intent.get("quote_amount")),
                    price=_optional_decimal(intent.get("limit_price")),
                )
            except Exception as exc:  # noqa: BLE001 - exchange constraints fail closed
                return self._reject_before_submit(intent, f"symbol validation failed: {exc}")
        return self._execute_intent(intent_id)

    def _reject_before_submit(self, intent: dict[str, Any], reason: str) -> dict[str, Any]:
        self.store.transition_intent(
            intent["intent_id"], "rejected", note=f"pre-submit revalidation failed: {reason}",
            reject_reason=reason, category="risk",
        )
        self.store.update_signal(intent["signal_id"], "rejected", reason)
        self.store.audit(intent["intent_id"], "risk", "pre_submit_revalidation_rejected", reason)
        result = self.store.get_intent(intent["intent_id"])
        assert result is not None
        self._notify_intent_decision(
            result,
            status="REJECTED_LOCALLY",
            reason=f"pre-submit revalidation failed: {reason}",
        )
        return result

    def _execute_intent(self, intent_id: str) -> dict[str, Any]:
        intent = self.store.get_intent(intent_id)
        if intent is None:
            raise RuntimeError(f"intent {intent_id} disappeared")
        if intent["state"] != "planned":
            return intent
        # Exactly one delivery may turn a planned intent into a submission.
        # A crash between planning and submission leaves the row ``planned`` and
        # a retried queue item can safely resume it; a duplicate delivery that
        # arrives after the claim can only observe the existing state.
        if not self.store.claim_intent_for_submission(
            intent_id, note="submission claimed by this delivery"
        ):
            result = self.store.get_intent(intent_id)
            assert result is not None
            return result

        mode = intent["mode"]
        try:
            if mode == "sim":
                return self._execute_sim(intent)
            if mode == "testonly":
                return self._execute_testonly(intent)
            if mode == "live":
                return self._execute_live(intent)
        except AccidentalOrderRiskError as exc:
            self.store.transition_intent(
                intent_id, "rejected", note=f"live gate blocked submission: {exc}",
                reject_reason=str(exc), category="risk",
            )
            raise
        self.store.transition_intent(intent_id, "rejected", note=f"unknown mode {mode!r}")
        result = self.store.get_intent(intent_id)
        assert result is not None
        return result

    def _execute_sim(self, intent: dict[str, Any]) -> dict[str, Any]:
        price = Decimal(intent["limit_price"]) if intent.get("limit_price") else None
        qty = Decimal(intent["base_quantity"]) if intent.get("base_quantity") else None
        quote = Decimal(intent["quote_amount"]) if intent.get("quote_amount") else Decimal(0)
        if price is None and qty and qty > 0:
            price = quote / qty
        if qty is None and price and price > 0:
            qty = quote / price
        self.store.transition_intent(intent["intent_id"], "submitting", note="simulated submission")
        self.store.transition_intent(
            intent["intent_id"], "filled",
            note="simulation only; no exchange order was sent",
            exchange_order_id=f"sim-{intent['client_order_id']}",
            exchange_status="SIMULATED_FILLED",
            executed_qty=qty,
            cumulative_quote=quote,
        )
        result = self.store.get_intent(intent["intent_id"])
        assert result is not None
        self._notify(
            event_key=f"{intent['intent_id']}:result:SIMULATED",
            signal_id=intent.get("signal_id"),
            intent_id=intent["intent_id"],
            text=self._intent_text(
                result,
                "SIMULATION ONLY — no exchange order was sent",
                submission_attempted=False,
                status_label="SIMULATED_FILLED (NO REAL ORDER)",
            ),
        )
        return result

    def _order_payload(self, intent: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": intent["symbol"],
            "side": intent["side"].upper(),
            "order_type": intent["order_type"].upper(),
            "client_order_id": intent["client_order_id"],
        }
        qty = Decimal(intent["base_quantity"]) if intent.get("base_quantity") else None
        price = Decimal(intent["limit_price"]) if intent.get("limit_price") else None
        quote = Decimal(intent["quote_amount"]) if intent.get("quote_amount") else None
        if intent["order_type"] == "market" and intent["side"] == "buy":
            payload["quote_order_qty"] = quote
        else:
            payload["quantity"] = qty
        if intent["order_type"] == "limit":
            payload["price"] = price
        return payload

    # -------------------------------------------------------- TP/SL protection

    def _protection_prices(
        self, intent: dict[str, Any]
    ) -> tuple[Decimal | None, Decimal | None]:
        """Derive the first take-profit and stop-loss for an attached order.

        Only limit BUY entries on a backend that supports exchange-side
        protection are eligible.  A direction that would be rejected by the
        exchange is dropped locally and audited; it is never silently sent.
        """

        if intent.get("side") != "buy" or intent.get("order_type") != "limit":
            return None, None
        if not getattr(self.client, "supports_attached_protection", False):
            return None, None
        signal_id = intent.get("signal_id")
        if not signal_id:
            return None, None
        signal = self.store.get_signal(str(signal_id))
        if signal is None:
            return None, None
        entry = _optional_decimal(intent.get("limit_price"))
        if entry is None or entry <= 0:
            return None, None

        tp: Decimal | None = None
        raw_targets = str(signal.get("target_prices") or "")
        for raw in raw_targets.split(","):
            candidate = _optional_decimal(raw)
            if candidate is not None and candidate > entry:
                tp = candidate
                break

        sl = _optional_decimal(signal.get("stop_loss"))
        if sl is not None and sl >= entry:
            self.store.audit(
                intent["intent_id"], "lifecycle", "protection_sl_dropped",
                f"stop-loss {sl} is not below the BUY entry {entry}",
            )
            sl = None
        return tp, sl

    def _record_protection_intent(
        self, intent: dict[str, Any], tp: Decimal | None, sl: Decimal | None
    ) -> None:
        if tp is None and sl is None:
            return
        self.store.upsert_protection(
            intent_id=intent["intent_id"],
            client_order_id=intent["client_order_id"],
            symbol=intent["symbol"],
            take_profit_price=tp,
            stop_loss_price=sl,
            protection_mode="attached",
            status="unverified",
        )

    def _verify_protection(self, intent_id: str) -> None:
        """Confirm exchange-side TP/SL, never assuming the request succeeded."""

        protection = self.store.get_protection(intent_id)
        if protection is None:
            return
        intent = self.store.get_intent(intent_id)
        order_id = (intent or {}).get("exchange_order_id") or protection.get("entry_order_id")
        if not order_id:
            return
        verify = getattr(self.client, "verify_protection", None)
        if verify is None:
            self.store.update_protection(
                intent_id, status="unverified",
                last_error="execution backend cannot verify attached protection",
            )
            return
        try:
            result = verify(symbol=protection["symbol"], order_id=str(order_id))
        except MexcError as exc:
            self.store.update_protection(
                intent_id, status="unverified",
                last_error=f"protection verification failed: {exc}",
            )
            return
        except Exception as exc:  # noqa: BLE001 - verification must never undo a landed order
            self.store.update_protection(
                intent_id, status="unverified",
                last_error=f"protection verification error ({exc.__class__.__name__})",
            )
            return
        if not result.get("verified"):
            self.store.update_protection(
                intent_id, status="unverified",
                last_error="exchange detail did not echo the requested TP/SL",
            )
            return

        mismatches: list[str] = []
        for local_key, remote_key in (
            ("take_profit_price", "take_profit"),
            ("stop_loss_price", "stop_loss"),
        ):
            wanted = _optional_decimal(protection.get(local_key))
            if wanted is None:
                continue
            observed = _optional_decimal(result.get(remote_key))
            if observed is None or observed != wanted:
                mismatches.append(f"{remote_key}: wanted {wanted}, observed {observed}")
        if mismatches:
            self.store.update_protection(
                intent_id, status="unverified",
                last_error="; ".join(mismatches)[:500],
            )
            return
        self.store.update_protection(intent_id, status="active")


    def _execute_testonly(self, intent: dict[str, Any]) -> dict[str, Any]:
        if self.client is None:
            self.store.transition_intent(intent["intent_id"], "failed", note="no MEXC client configured")
            result = self.store.get_intent(intent["intent_id"])
            assert result is not None
            return result
        self.store.transition_intent(intent["intent_id"], "submitting", note="test-order validation")
        try:
            self.client.place_test_order(**self._order_payload(intent))
        except MexcError as exc:
            self.store.transition_intent(
                intent["intent_id"], "rejected", note=f"test order rejected: {exc}", reject_reason=str(exc)
            )
        else:
            self.store.transition_intent(
                intent["intent_id"], "submitted",
                note="official order/test accepted; no real order placed",
                exchange_status="TEST_ONLY_ACCEPTED",
            )
            self.store.transition_intent(
                intent["intent_id"], "canceled_local",
                note="test-only intent terminal (never reached the matching engine)",
                exchange_status="TEST_ONLY_ACCEPTED",
            )
        result = self.store.get_intent(intent["intent_id"])
        assert result is not None
        status_label = (
            "TEST_ONLY_REJECTED (NO REAL ORDER)"
            if result["state"] == "rejected"
            else "TEST_ONLY_ACCEPTED (NO REAL ORDER)"
        )
        self._notify(
            event_key=f"{intent['intent_id']}:result:{status_label}",
            signal_id=intent.get("signal_id"),
            intent_id=intent["intent_id"],
            text=self._intent_text(
                result,
                "OFFICIAL TEST ORDER ONLY — no matching-engine order",
                submission_attempted=False,
                status_label=status_label,
            ),
        )
        return result

    def _execute_live(self, intent: dict[str, Any]) -> dict[str, Any]:
        if self.cfg.trading.mode != "live" or self.cfg.trading.live_enabled_token != LIVE_TOKEN:
            raise AccidentalOrderRiskError("live order blocked: live mode is not explicitly armed")
        if self.client is None:
            raise AccidentalOrderRiskError("live order blocked: no MEXC client configured")

        intent_id = intent["intent_id"]
        # Spend tracking is informational only: it never caps the owner's
        # per-order amount.  Pre-migration/legacy rows get a ledger row before
        # any exchange call.
        self.store.ensure_budget_reservation(intent_id)
        # Re-validate the exact payload immediately before the exchange call.
        meta = self._symbol_meta(intent.get("symbol"))
        if meta is None:
            return self._reject_before_submit(intent, "symbol metadata is unavailable")
        try:
            meta.validate_order(
                side=str(intent["side"]).upper(),
                order_type=str(intent["order_type"]).upper(),
                quantity=_optional_decimal(intent.get("base_quantity")),
                quote_amount=_optional_decimal(intent.get("quote_amount")),
                price=_optional_decimal(intent.get("limit_price")),
            )
        except Exception as exc:  # noqa: BLE001 - exchange constraints fail closed
            return self._reject_before_submit(intent, f"symbol validation failed: {exc}")
        balance_reason = self._balance_preflight(intent)
        if balance_reason is not None:
            note = f"balance preflight rejected order: {balance_reason}"
            self.store.transition_intent(
                intent_id, "rejected", note=note, reject_reason=balance_reason, category="risk"
            )
            self.store.update_signal(intent["signal_id"], "rejected", balance_reason)
            self.store.audit(intent_id, "risk", "balance_preflight_rejected", balance_reason)
            result = self.store.get_intent(intent_id)
            assert result is not None
            self._notify_intent_decision(
                result,
                status="REJECTED_LOCALLY",
                reason=f"balance preflight rejected the order: {balance_reason}",
            )
            return result
        tp_price, sl_price = self._protection_prices(intent)
        protection_requested = tp_price is not None or sl_price is not None
        if protection_requested:
            # Persist the *intent* to protect before submitting.  The row is
            # 'unverified' until an exchange query echoes the trigger prices.
            self._record_protection_intent(intent, tp_price, sl_price)

        self.store.transition_intent(intent_id, "submitting", note="submitting to MEXC Spot")
        payload = self._order_payload(intent)
        if protection_requested:
            payload["take_profit_price"] = tp_price
            payload["stop_loss_price"] = sl_price
        try:
            response = self.client.place_order(**payload)
        except (
            MexcDuplicateOrder,
            MexcNetworkError, MexcServerError, MexcUnexpectedResponse,
            MexcRateLimitError, MexcWafError,
        ) as exc:
            self.store.transition_intent(
                intent_id, "unknown_requires_reconciliation",
                note=f"submission outcome uncertain ({exc.__class__.__name__}); no retry will be attempted",
                reject_reason=str(exc),
                category="mexc_request",
            )
            if protection_requested:
                self.store.update_protection(
                    intent_id, status="unknown_requires_reconciliation",
                    last_error=str(exc)[:500],
                )
        except MexcError as exc:
            self.store.transition_intent(
                intent_id, "rejected", note=f"submission rejected: {exc}", reject_reason=str(exc),
                category="mexc_request",
            )
            if protection_requested:
                self.store.update_protection(
                    intent_id, status="rejected", last_error=str(exc)[:500]
                )
        except Exception as exc:  # noqa: BLE001 - after submitting, assume uncertainty
            self.store.transition_intent(
                intent_id, "unknown_requires_reconciliation",
                note=f"unexpected submit failure ({exc.__class__.__name__}); no retry will be attempted",
                reject_reason=str(exc),
                category="mexc_request",
            )
            if protection_requested:
                self.store.update_protection(
                    intent_id, status="unknown_requires_reconciliation",
                    last_error=str(exc)[:500],
                )
        else:
            order_id = response.get("orderId")
            if order_id is None:
                self.store.transition_intent(
                    intent_id, "unknown_requires_reconciliation",
                    note="order response had no orderId; reconciling by client order id",
                    category="mexc_request",
                )
                if protection_requested:
                    self.store.update_protection(
                        intent_id, status="unknown_requires_reconciliation",
                        last_error="placement response had no order id",
                    )
            else:
                self.store.upsert_exchange_order(intent_id=intent_id, raw=response, discovered_in="submit")
                self.store.transition_intent(
                    intent_id, "submitted",
                    note=f"exchange accepted order {order_id}",
                    exchange_order_id=str(order_id),
                    exchange_status=str(response.get("status") or "NEW"),
                    executed_qty=_optional_decimal(response.get("executedQty")),
                    cumulative_quote=_optional_decimal(response.get("cumulativeQuoteQty")),
                    category="mexc_request",
                )
                if protection_requested:
                    self.store.upsert_protection(
                        intent_id=intent_id,
                        client_order_id=intent["client_order_id"],
                        symbol=intent["symbol"],
                        entry_order_id=str(order_id),
                        take_profit_price=tp_price,
                        stop_loss_price=sl_price,
                        protection_mode="attached",
                        status="unverified",
                    )
                    self._verify_protection(intent_id)
                self.reconcile_intent(intent_id, notify=False)

        result = self.store.get_intent(intent_id)
        assert result is not None
        status = self._order_status_label(result)
        self._notify(
            event_key=f"{intent_id}:result:{status}",
            signal_id=intent.get("signal_id"),
            intent_id=intent_id,
            text=self._intent_text(
                result,
                self._headline_for_status(status),
                submission_attempted=True,
            ),
        )
        return result

    def _balance_preflight(self, intent: dict[str, Any]) -> str | None:
        """Return a rejection reason when live balance checks cannot pass."""

        meta = self._symbol_meta(intent.get("symbol"))
        if meta is None:
            return "symbol metadata unavailable for balance preflight"
        base_asset = getattr(meta, "base_asset", None)
        quote_asset = getattr(meta, "quote_asset", None)
        if not base_asset or not quote_asset:
            return "symbol metadata is missing base/quote assets"
        client = self.client
        if client is None or not hasattr(client, "account"):
            return "MEXC client does not support account balance queries"
        try:
            raw = client.account()
            snapshot = parse_account(raw, fetched_at=self._monotonic())
            outstanding = self.store.outstanding_balance_usage(
                intent["symbol"], exclude_intent_id=intent["intent_id"]
            )
            validate_balance(
                snapshot,
                cfg=self.cfg.trading,
                side=intent["side"],
                base_asset=str(base_asset),
                quote_asset=str(quote_asset),
                base_quantity=(
                    Decimal(intent["base_quantity"]) if intent.get("base_quantity") else None
                ),
                quote_amount=(
                    Decimal(intent["quote_amount"]) if intent.get("quote_amount") else None
                ),
                outstanding_base=outstanding["base"],
                outstanding_quote=outstanding["quote"],
                now=self._monotonic(),
            )
        except BalanceError as exc:
            return str(exc)
        except MexcError as exc:
            return f"balance query failed ({exc.__class__.__name__}): {exc}"
        return None

    # ---------------------------------------------------------- cancellation

    def _live_client(self):
        """Fail closed: live armed + client present. Returns the client."""
        if self.cfg.trading.mode != "live" or self.cfg.trading.live_enabled_token != LIVE_TOKEN:
            raise AccidentalOrderRiskError("live order blocked: live mode is not explicitly armed")
        if self.client is None:
            raise AccidentalOrderRiskError("live order blocked: no MEXC client configured")
        return self.client

    def cancel_intent_order(self, intent: dict[str, Any]) -> None:
        """Cancel an entry intent's open exchange order (idempotent on missing id)."""
        exchange_id = intent.get("exchange_order_id")
        client_order_id = intent.get("client_order_id")
        if not exchange_id and not client_order_id:
            return
        client = self._live_client()
        if intent.get("state") not in ("open", "partially_filled", "submitted"):
            return
        with contextlib.suppress(MexcOrderNotFound):
            client.cancel_order(
                symbol=intent["symbol"],
                order_id=exchange_id,
                orig_client_order_id=client_order_id if not exchange_id else None,
            )

    def cancel_trade_order(self, order: dict[str, Any]) -> None:
        """Cancel an open TP/SL trade order on the exchange."""
        exchange_id = order.get("exchange_order_id")
        client_order_id = order.get("client_order_id")
        if not exchange_id and not client_order_id:
            return
        client = self._live_client()
        client.cancel_order(
            symbol=order["symbol"],
            order_id=exchange_id,
            orig_client_order_id=client_order_id if not exchange_id else None,
        )

    def submit_sell_limit(
        self,
        trade: dict[str, Any],
        client_order_id: str,
        price: Decimal,
        quantity: Decimal,
        kind: str | None = None,
    ) -> dict[str, Any]:
        """Submit a LIMIT sell for a TP target. Returns an exchange result dict."""
        client = self._live_client()
        response = client.place_order(
            symbol=trade["symbol"],
            side="SELL",
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
        )
        order_id = response.get("orderId")
        return {
            "exchange_order_id": str(order_id) if order_id is not None else None,
            "state": str(response.get("status") or "NEW"),
            "executed_qty": _optional_decimal(response.get("executedQty")),
            "cumulative_quote": _optional_decimal(response.get("cumulativeQuoteQty")),
        }

    def submit_oco_sell(
        self,
        trade: dict[str, Any],
        client_order_id: str,
        price: Decimal,
        quantity: Decimal,
        stop_price: Decimal,
        kind: str | None = None,
    ) -> dict[str, Any]:
        """Submit a native OCO SELL pair (limit leg + stop-limit leg) via the CLI.

        The limit leg is the take-profit (price); the stop-limit leg is the
        stop-loss (trigger_price == trigger_limit_price == stop_price).  When one
        leg activates the exchange cancels the other, giving a true
        one-cancels-the-other exit without managing two independent orders.
        """

        client = self._live_client()
        response = client.place_oco(
            symbol=trade["symbol"],
            side="SELL",
            quantity=quantity,
            price=price,
            trigger_price=stop_price,
            trigger_limit_price=stop_price,
            client_order_id=client_order_id,
        )
        order_id = response.get("orderId")
        return {
            "exchange_order_id": str(order_id) if order_id is not None else None,
            "state": str(response.get("status") or "NEW"),
            "executed_qty": _optional_decimal(response.get("executedQty")),
            "cumulative_quote": _optional_decimal(response.get("cumulativeQuoteQty")),
        }

    def submit_stop_loss_market_sell(
        self,
        trade: dict[str, Any],
        quantity: Decimal,
        *,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit a MARKET sell for the remaining position (stop-loss policy)."""
        client = self._live_client()
        cid = client_order_id or f"sl-{trade['trade_id'][-24:]}"
        response = client.place_order(
            symbol=trade["symbol"],
            side="SELL",
            order_type="MARKET",
            quote_order_qty=None,
            quantity=quantity,
            price=None,
            client_order_id=cid,
        )
        order_id = response.get("orderId")
        return {
            "exchange_order_id": str(order_id) if order_id is not None else None,
            "state": str(response.get("status") or "NEW"),
            "executed_qty": _optional_decimal(response.get("executedQty")),
            "cumulative_quote": _optional_decimal(response.get("cumulativeQuoteQty")),
        }

    def query_trade_order(self, order: dict[str, Any]) -> dict[str, Any]:
        """Query the exchange status of a TP/SL trade order.

        When the local row has no exchange id yet (crash after submission, or a
        response that omitted it), resolve it by ``clientOrderId``.  ``-2011``
        is reported as ``NOT_FOUND`` and is never treated as a terminal local
        rejection by the caller.
        """
        client = self._live_client()
        exchange_id = order.get("exchange_order_id")
        client_order_id = order.get("client_order_id")
        if not exchange_id and not client_order_id:
            return {"state": "NOT_FOUND", "executed_qty": Decimal("0")}
        try:
            raw = client.query_order(
                symbol=order["symbol"],
                order_id=exchange_id,
                orig_client_order_id=client_order_id if not exchange_id else None,
            )
        except MexcOrderNotFound:
            return {"state": "NOT_FOUND", "executed_qty": Decimal("0")}
        order_id = raw.get("orderId") or exchange_id
        return {
            "state": str(raw.get("status") or "UNKNOWN").upper(),
            "executed_qty": _optional_decimal(raw.get("executedQty")),
            "cumulative_quote": _optional_decimal(raw.get("cumulativeQuoteQty")),
            "exchange_order_id": str(order_id) if order_id is not None else None,
        }

    def _handle_cancel(
        self, parsed: ParsedSignal, signal_id: str, channel_id: int, message_id: int
    ) -> None:
        symbol = parsed.symbol
        reason = f"cancel signal {signal_id}"
        if symbol is None and "ALL" not in parsed.normalized_text:
            self.store.audit(
                signal_id, "lifecycle", "cancel_ignored",
                "cancel signal has no symbol and did not explicitly say ALL",
            )
            return
        # A queued/retried old cancel must never cancel an order created by a
        # newer Telegram message (message ids are monotonic per channel).
        intents = self.store.cancel_signals(
            symbol, reason, channel_id=channel_id, before_message_id=message_id
        )
        self.store.audit(signal_id, "lifecycle", "cancel_received", f"symbol={symbol} intents={len(intents)}")
        for intent in intents:
            self._cancel_intent(intent, reason)

    def handle_delete(self, channel_id: int, message_id: int) -> None:
        reason = f"Telegram message {channel_id}/{message_id} deleted"
        intents = self.store.cancel_signals_for_message(channel_id, message_id, reason)
        for intent in intents:
            self._cancel_intent(intent, reason)

    def _cancel_intent(self, intent: dict[str, Any], reason: str) -> None:
        intent_id = intent["intent_id"]
        state = intent["state"]
        if state not in ("open", "submitted", "reconciling", "submitting", "unknown_requires_reconciliation"):
            self.store.transition_intent(intent_id, "canceled_local", note=reason)
            return
        if self.cfg.trading.mode != "live" or self.client is None:
            self.store.transition_intent(intent_id, "canceled_local", note=f"{reason} (no live exchange call)")
            return
        try:
            self.client.cancel_order(
                symbol=intent["symbol"],
                order_id=intent.get("exchange_order_id"),
                orig_client_order_id=intent.get("client_order_id"),
            )
        except MexcOrderNotFound:
            self.reconcile_intent(intent_id)
            return
        except MexcError as exc:
            self.store.audit(
                intent_id, "mexc_request", "cancel_failed",
                f"{exc.__class__.__name__}: {exc}. Order state left unchanged for reconciliation.",
            )
            return
        self.reconcile_intent(intent_id)

    # -------------------------------------------------------- reconciliation

    def reconcile_intent(self, intent_id: str, *, notify: bool = True) -> dict[str, Any]:
        intent = self.store.get_intent(intent_id)
        if intent is None:
            raise RuntimeError(f"intent {intent_id} not found")
        if intent["mode"] != "live" or self.client is None:
            self.store.record_reconciliation(
                intent_id=intent_id, client_order_id=intent.get("client_order_id"),
                local_state=intent["state"], exchange_state=None,
                outcome="consistent", detail="not a live intent; nothing to reconcile",
            )
            return intent

        if (
            intent["state"] in TERMINAL_INTENT_STATES
            and int(intent.get("fill_sync_required") or 0) == 1
        ):
            return self._resync_terminal_fills(intent, intent_id, notify=notify)

        try:
            raw = self.client.query_order(
                symbol=intent["symbol"],
                order_id=intent.get("exchange_order_id"),
                orig_client_order_id=intent.get("client_order_id"),
            )
        except MexcOrderNotFound:
            outcome = "not_found"
            # -2011 can be a live order that is not query-visible yet, or one
            # canceled/filled outside our view. Never turn non-terminal state
            # into a terminal local failure: that would lose tracking of a real
            # order. Keep it in the reconciliation set indefinitely.
            if intent["state"] not in TERMINAL_INTENT_STATES:
                note = (
                    "exchange returned -2011 (not found); order may still exist. "
                    "State remains uncertain and is NEVER retried automatically"
                )
                self.store.transition_intent(
                    intent_id, "unknown_requires_reconciliation", note=note,
                    category="reconcile",
                )
            self.store.record_reconciliation(
                intent_id=intent_id, client_order_id=intent.get("client_order_id"),
                local_state=intent["state"], exchange_state=None, outcome=outcome,
                detail="exchange returned -2011",
            )
            result = self.store.get_intent(intent_id)
            assert result is not None
            if notify and result["state"] != intent["state"]:
                status = self._order_status_label(result)
                self._notify(
                    event_key=f"{intent_id}:reconcile:{result['state']}:NOT_FOUND",
                    signal_id=intent.get("signal_id"),
                    intent_id=intent_id,
                    text=self._intent_text(
                        result,
                        self._headline_for_status(status),
                        submission_attempted=True,
                    ),
                )
            return result
        except (MexcNetworkError, MexcUnexpectedResponse, MexcServerError) as exc:
            self.store.record_reconciliation(
                intent_id=intent_id, client_order_id=intent.get("client_order_id"),
                local_state=intent["state"], exchange_state=None, outcome="ambiguous",
                detail=f"{exc.__class__.__name__}: {exc}",
            )
            result = self.store.get_intent(intent_id)
            assert result is not None
            if notify:
                status = self._order_status_label(result)
                self._notify(
                    event_key=f"{intent_id}:reconcile:{result['state']}:QUERY_AMBIGUOUS",
                    signal_id=intent.get("signal_id"),
                    intent_id=intent_id,
                    text=self._intent_text(
                        result,
                        self._headline_for_status(status)
                        + " — reconciliation query failed; status may change",
                        submission_attempted=True,
                        status_label=(
                            "ACCEPTED_OPEN_RECONCILIATION_PENDING"
                            if status == "ACCEPTED_OPEN"
                            else status
                        ),
                    ),
                )
            return result

        exchange_status = str(raw.get("status") or "UNKNOWN").upper()
        executed = _optional_decimal(raw.get("executedQty"))
        cumulative = _optional_decimal(raw.get("cumulativeQuoteQty"))
        local_state = intent["state"]
        new_state, outcome, note = self._map_exchange_state(local_state, exchange_status)

        order_id = str(raw.get("orderId") or intent.get("exchange_order_id") or "")
        if order_id:
            self.store.upsert_exchange_order(intent_id=intent_id, raw=raw, discovered_in="reconciliation")
            protection = self.store.get_protection(intent_id)
            if protection is not None and protection["status"] in (
                "unverified", "unknown_requires_reconciliation",
            ):
                self._verify_protection(intent_id)

        fills_expected = self._fills_expected(exchange_status, executed)
        if new_state is not None:
            # Set the durable fill-sync flag in the SAME transaction that makes
            # the order terminal. If the process dies before fills are read,
            # reconciliation still knows the intent is not fully reconciled.
            self.store.transition_intent(
                intent_id, new_state, note=note,
                exchange_order_id=order_id or None,
                exchange_status=exchange_status,
                executed_qty=executed,
                cumulative_quote=cumulative,
                category="reconcile",
                fill_sync_required=fills_expected,
            )
        elif fills_expected and int(intent.get("fill_sync_required") or 0) == 0:
            self.store.set_fill_sync_required(
                intent_id, True, note="partial fills observed before terminal status"
            )

        if order_id and fills_expected:
            fills_ok, fills_detail = self._record_fills(intent, order_id, executed)
            if fills_ok:
                self.store.set_fill_sync_required(
                    intent_id, False, note=f"fills synced: {fills_detail}"
                )
            else:
                # The order state is known, but fill history is not: keep the
                # intent in the reconciliation set and report ambiguity.
                outcome = "ambiguous"
                note = f"{note}; fill sync pending: {fills_detail}"

        self.store.record_reconciliation(
            intent_id=intent_id, client_order_id=intent.get("client_order_id"),
            local_state=local_state, exchange_state=exchange_status, outcome=outcome, detail=note,
        )
        result = self.store.get_intent(intent_id)
        assert result is not None
        if notify and result["state"] != local_state:
            fills = self.store.fills_for_order(str(order_id)) if order_id else []
            status = self._order_status_label(result)
            self._notify(
                event_key=(
                    f"{intent_id}:reconcile:{result['state']}:{exchange_status}:"
                    f"{len(fills)}"
                ),
                signal_id=intent.get("signal_id"),
                intent_id=intent_id,
                text=self._intent_text(
                    result,
                    self._headline_for_status(status)
                    + f" — reconciled (local {local_state} -> {result['state']}, "
                    f"exchange {exchange_status}, outcome={outcome})",
                    submission_attempted=True,
                ),
            )
        return result

    def _resync_terminal_fills(
        self, intent: dict[str, Any], intent_id: str, *, notify: bool = True
    ) -> dict[str, Any]:
        """Recover fill history for a terminal order without touching state."""

        order_id = intent.get("exchange_order_id")
        if not order_id:
            self.store.record_reconciliation(
                intent_id=intent_id, client_order_id=intent.get("client_order_id"),
                local_state=intent["state"], exchange_state=intent.get("exchange_status"),
                outcome="ambiguous",
                detail="fill sync required but no exchange order id is available",
            )
            return intent
        expected = _optional_decimal(intent.get("executed_qty"))
        fills_ok, fills_detail = self._record_fills(intent, str(order_id), expected)
        if fills_ok:
            self.store.set_fill_sync_required(
                intent_id, False, note=f"terminal fills recovered: {fills_detail}"
            )
            outcome = "resolved"
        else:
            outcome = "ambiguous"
        self.store.record_reconciliation(
            intent_id=intent_id, client_order_id=intent.get("client_order_id"),
            local_state=intent["state"], exchange_state=intent.get("exchange_status"),
            outcome=outcome, detail=f"terminal fill resync: {fills_detail}",
        )
        result = self.store.get_intent(intent_id)
        assert result is not None
        if notify and fills_ok:
            status = self._order_status_label(result)
            fills = self.store.fills_for_order(str(order_id))
            self._notify(
                event_key=f"{intent_id}:fills:{result['state']}:{len(fills)}",
                signal_id=intent.get("signal_id"),
                intent_id=intent_id,
                text=self._intent_text(
                    result,
                    self._headline_for_status(status) + " — fill history recovered",
                    submission_attempted=True,
                ),
            )
        return result

    @staticmethod
    def _fills_expected(exchange_status: str, executed: Decimal | None) -> bool:
        if exchange_status in ("FILLED", "PARTIALLY_FILLED", "PARTIALLY_CANCELED"):
            return True
        return executed is not None and executed > 0

    def _record_fills(
        self, intent: dict[str, Any], order_id: str, expected_executed: Decimal | None = None
    ) -> tuple[bool, str]:
        """Persist fills idempotently. False means "not fully reconciled"."""

        client = self.client
        if client is None or not hasattr(client, "my_trades"):
            return False, "client does not expose myTrades"
        try:
            trades = client.my_trades(symbol=intent["symbol"], order_id=order_id, limit=1000)
        except MexcError as exc:
            return False, f"{exc.__class__.__name__}: {exc}"
        if not isinstance(trades, list):
            return False, "myTrades response was not a list"
        recorded = 0
        for trade in trades:
            if not isinstance(trade, dict):
                return False, "myTrades contained a malformed entry"
            if trade.get("qty") in (None, "") or trade.get("price") in (None, ""):
                return False, "myTrades entry missing qty/price"
            try:
                self.store.add_fill(
                    exchange_order_id=order_id,
                    symbol=intent["symbol"],
                    raw=trade,
                    source="rest_query",
                )
            except (InvalidOperation, TypeError, ValueError) as exc:
                return False, f"invalid fill record: {exc.__class__.__name__}"
            recorded += 1
        total = self.store.fill_quantity_total(order_id)
        if expected_executed is not None and expected_executed > 0 and total < expected_executed:
            return False, f"persisted fills {total} < executed {expected_executed}"
        return True, f"{recorded} trade(s) scanned, {total} total quantity persisted"

    @staticmethod
    def _map_exchange_state(local_state: str, exchange_status: str) -> tuple[str | None, str, str]:
        if exchange_status == "NEW" or exchange_status == "PARTIALLY_FILLED":
            mapped = "open"
        elif exchange_status == "FILLED":
            mapped = "filled"
        elif exchange_status in ("CANCELED", "PARTIALLY_CANCELED"):
            mapped = "canceled"
        else:
            return None, "ambiguous", f"unrecognised exchange status {exchange_status!r}; state left unchanged"

        if local_state == "filled" and mapped != "filled":
            return "filled", "discrepancy", (
                f"local state filled but exchange reports {exchange_status}; refusing to un-fill local record"
            )
        if local_state in ("failed", "rejected", "canceled", "canceled_local") and mapped not in (
            "canceled", "filled",
        ):
            return mapped, "discrepancy", (
                f"local terminal state {local_state} disagrees with exchange status {exchange_status}"
            )
        if local_state == mapped:
            return mapped, "consistent", f"exchange status {exchange_status} matches local state"
        return mapped, "resolved", f"local {local_state} -> exchange {exchange_status}"

    def reconcile_pending(self, limit: int = 50) -> ReconcileStats:
        stats = ReconcileStats()
        for intent in self.store.pending_reconciliation(limit=limit):
            stats.checked += 1
            try:
                result = self.reconcile_intent(intent["intent_id"])
            except Exception as exc:  # noqa: BLE001 - one bad intent must not stop the sweep
                stats.errors += 1
                self.store.record_reconciliation(
                    intent_id=intent["intent_id"], client_order_id=intent.get("client_order_id"),
                    local_state=intent.get("state"), exchange_state=None, outcome="ambiguous",
                    detail=f"reconciliation crashed: {exc.__class__.__name__}: {exc}",
                )
                continue
            outcomes = [
                r["outcome"]
                for r in self.repo.db.conn.execute(
                    "SELECT outcome FROM reconciliation_results WHERE intent_id = ? "
                    "ORDER BY recon_id DESC LIMIT 1",
                    (intent["intent_id"],),
                )
            ]
            outcome = outcomes[0] if outcomes else "ambiguous"
            if outcome == "resolved":
                stats.resolved += 1
            elif outcome == "discrepancy":
                stats.discrepancies += 1
            elif outcome == "not_found":
                stats.not_found += 1
            else:
                stats.ambiguous += 1
            del result
        return stats


def _parse_signal_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def string_or_none(value: Decimal | None) -> str | None:
    return dec_str(value)
