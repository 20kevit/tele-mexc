"""Trade lifecycle management: conditional entry, TP1/TP2 and stop-loss.

The :class:`TradeManager` is the single owner of a trade's state machine.  It is
synchronous and serialised by an internal lock so concurrent queue deliveries
and background reconciliation cannot race each other.

Entry modes
-----------
* ``direct`` — the current market is already at/above the signal entry price
  (or conditional entry is disabled): submit a Limit Buy at the entry price
  immediately, subject to risk/balance controls.
* ``conditional`` — the current market is below the entry price and
  ``entry_trigger_offset_ticks`` is configured: create a durable pending entry
  and wait for an upward crossing of ``entry_price - offset * tick_size`` before
  submitting the Limit Buy.  The Limit Buy always respects the entry price as a
  hard maximum; it is never converted to a market order.

Take-profit: TP1 sells ``tp1_allocation_pct_of_acquired`` of the ACTUAL acquired
position; TP2 sells ``tp2_allocation_pct_of_remaining`` of what remains after
TP1 *actually sold* — not merely what was allocated.  Partial TP1 fills are
therefore carried into TP2 instead of being stranded.

Stop-loss: one per trade; triggers a market-sell of the remaining bot-owned
quantity after cancelling the entry remainder and any open TP orders.  Every
exit order is durable and reconciled by ``clientOrderId`` before another order
can be submitted, so a restart, timeout or duplicate tick can never blindly
oversell the position.
"""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..mexc.errors import (
    MexcDuplicateOrder,
    MexcNetworkError,
    MexcRateLimitError,
    MexcServerError,
    MexcUnexpectedResponse,
)
from .parser import ParsedSignal
from .store import TERMINAL_INTENT_STATES
from .trade_store import TradeStore

log = logging.getLogger("ingest.trading.trade_manager")


@dataclass
class TradeDecision:
    """Outcome of the trade manager's review of a freshly-planned intent."""

    trade: dict[str, Any] | None
    deferred: bool  # True => do not execute the entry yet (conditional entry)
    rejected: bool = False  # True => fail closed; do not execute at all
    reason: str | None = None


# Terminal trade states: no further action required.
TERMINAL_TRADE_STATES = (
    "closed",
    "tp2_completed",
    "needs_reconciliation",
)

# Order states that still represent exposure on the exchange (or an unknown
# outcome that must be resolved before placing another sell).
UNCONFIRMED_ORDER_STATES = (
    "pending",
    "submitting",
    "open",
    "partially_filled",
    "cancel_requested",
    "unknown_requires_reconciliation",
)
TERMINAL_ORDER_STATES = ("filled", "canceled", "rejected")
# Write failures that do not prove the request was rejected.  They must never
# be followed by an automatic resubmission of a *different* client order id.
_UNCERTAIN_ERRORS = (
    MexcNetworkError,
    MexcUnexpectedResponse,
    MexcServerError,
    MexcRateLimitError,
)


def _dec(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _first(values: list[Decimal | str] | tuple[Decimal, ...] | None) -> Decimal | None:
    if not values:
        return None
    for v in values:
        d = _dec(v)
        if d is not None:
            return d
    return None


def _second(values: list[Decimal | str] | tuple[Decimal, ...] | None) -> Decimal | None:
    if not values:
        return None
    seen = 0
    for v in values:
        d = _dec(v)
        if d is None:
            continue
        seen += 1
        if seen == 2:
            return d
    return None


class TradeManager:
    """Coordinates a trade from signal intent through TP1/TP2 and stop-loss."""

    def __init__(
        self,
        cfg: Any,
        store: TradeStore,
        *,
        engine: Any | None = None,
        client: Any | None = None,
        notifier: Any | None = None,
        price_provider: Any | None = None,
        now: Any | None = None,
        clock: Any | None = None,
    ):
        self.cfg = cfg
        self.store = store
        self.engine = engine
        self.client = client
        self._notifier = notifier
        self._price_provider = price_provider
        self._now = now or (lambda: datetime.now(UTC))
        self._clock = clock or time.monotonic
        self._lock = threading.RLock()

    @property
    def notifier(self) -> Any:
        """Notifier attached to the engine after construction (lazy)."""
        if self._notifier is not None:
            return self._notifier
        if self.engine is not None:
            return getattr(self.engine, "notifier", None)
        return None

    # ===================================================================
    # Public entry points
    # ===================================================================

    def on_intent_planned(
        self, intent: dict[str, Any], parsed: ParsedSignal, reference_price: Decimal | None
    ) -> TradeDecision:
        """Create the trade for a freshly-planned intent and decide entry mode.

        Returns a :class:`TradeDecision`. ``deferred`` is True when entry is
        conditional and the intent must NOT be executed yet (the monitor will
        activate it later).  ``rejected`` is True when the trade setup fails
        closed and the engine must not execute the intent at all.  ``trade`` is
        the created trade row or ``None``.
        """
        with self._lock:
            mode = intent.get("mode")
            if mode != "live":
                # sim / testonly / non-tradeable: let the engine handle execution.
                return TradeDecision(trade=None, deferred=False, reason="not live mode")
            existing = self.store.get_trade_by_signal(intent["signal_id"])
            if existing is not None:
                if (
                    existing.get("entry_mode") == "conditional"
                    and existing.get("trade_state") == "pending_entry"
                ):
                    return TradeDecision(
                        trade=existing, deferred=True,
                        reason="existing conditional entry still waiting for trigger",
                    )
                return TradeDecision(trade=existing, deferred=False, reason="trade already exists")
            # Conditional entry requires a verified market price to decide whether to
            # wait for the upward trigger.  Missing/unverified prices fail closed.
            if self.cfg.conditional_entry_enabled and reference_price is None:
                self._reject_trade_intent(
                    intent, "conditional entry enabled but no market price to evaluate"
                )
                return TradeDecision(
                    trade=None, deferred=False, rejected=True,
                    reason="no reference price for conditional entry",
                )
            trade = self._create_trade(intent, parsed, reference_price)
            if trade is None:
                return TradeDecision(
                    trade=None, deferred=False, rejected=True,
                    reason="trade creation rejected",
                )
            if trade["entry_mode"] == "conditional":
                return TradeDecision(
                    trade=trade, deferred=True,
                    reason="conditional entry waiting for trigger",
                )
            return TradeDecision(trade=trade, deferred=False, reason="direct entry")

    def on_intent_executed(self, intent: dict[str, Any]) -> None:
        """Called by the engine after ``_execute_intent`` placed the entry order."""
        intent_id = intent.get("intent_id")
        if intent_id is None:
            return
        with self._lock:
            trade = self.store.get_trade_by_intent(intent_id)
            if trade is None:
                return
            if trade["trade_state"] not in ("pending_entry", "entry_active"):
                return
            self.store.update_trade_fields(
                trade["trade_id"],
                entry_order_id=intent.get("exchange_order_id"),
                entry_exchange_status=intent.get("state"),
                entry_filled_qty=_dec(intent.get("executed_qty")) or Decimal("0"),
            )
            intent_state = intent.get("state")
            if intent_state in ("open", "partially_filled", "submitted"):
                self.store.update_trade_state(trade["trade_id"], "entry_active", note="entry order open")
                return
            refreshed = self.store.get_trade(trade["trade_id"])
            if refreshed is not None:
                trade = refreshed
            self._reconcile_entry_fills(trade)
            refreshed = self.store.get_trade(trade["trade_id"])
            if refreshed is not None:
                trade = refreshed
            self._settle_entry_state(trade)

    def on_tick(self, symbol: str, price: Decimal, received_at: float) -> None:
        """Evaluate every active trade for ``symbol`` against a new tick.

        TP1/TP2/stop-loss evaluation is always active.  Conditional-entry
        triggers only exist for trades whose ``entry_mode`` is ``conditional``;
        disabling conditional entry must never disable exit monitoring.
        """
        if price is None or not price.is_finite() or price <= 0:
            return
        with self._lock:
            trades = self.store.list_trades()
            for trade in trades:
                if trade["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                if trade["symbol"].upper() != symbol.upper():
                    continue
                self._evaluate_trade_tick(trade, price, received_at)

    def recover_on_startup(self) -> None:
        """Rehydrate active trades from durable state after a restart.

        Each non-terminal trade is reconciled with the exchange: the entry
        intent and any open TP/SL orders are queried, fills are recorded, and
        interrupted stop-loss/exit states are resumed.  Uncertain submissions
        are only queried — never resubmitted with a new client order id.
        """
        with self._lock:
            for trade in self.store.list_trades():
                if trade["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                try:
                    self._reconcile_trade(trade["trade_id"])
                except Exception as exc:  # noqa: BLE001 - never block recovery
                    log.error("recovery reconcile failed for %s: %s", trade["trade_id"], exc)
                    self._record_error(trade, f"recovery failed: {exc}", "reconciliation")

    # ===================================================================
    # Trade creation + entry decision
    # ===================================================================

    def _create_trade(
        self, intent: dict[str, Any], parsed: ParsedSignal, reference_price: Decimal | None
    ) -> dict[str, Any] | None:
        symbol_meta = self._symbol_meta_symbol(intent["symbol"])
        entry_price = _dec(parsed.entry_price)
        if entry_price is None or entry_price <= 0:
            self._reject_trade_intent(intent, "signal has no valid entry price")
            return None

        tick_size = symbol_meta.price_step if symbol_meta else None
        offset = self.cfg.entry_trigger_offset_ticks
        trigger_price = None
        entry_mode = "direct"
        current = reference_price
        # Conditional entry is enabled when offset >= 1 (see TradingConfig).  We
        # already verified current is not None; only engage the watcher when the
        # market is below the entry and we can build a valid downward trigger.
        if (
            self.cfg.conditional_entry_enabled
            and tick_size is not None
            and current is not None
            and current.is_finite()
            and current > 0
            and current < entry_price
        ):
            trigger_price = self._compute_trigger(entry_price, offset, tick_size)
            if trigger_price is None or trigger_price <= 0 or trigger_price >= entry_price:
                self._notify_stale_config(intent["symbol"], "computed trigger price invalid")
                self._reject_trade_intent(intent, "trigger price invalid; refusing conditional entry")
                return None
            entry_mode = "conditional"

        tp1_price = _first(parsed.target_prices)
        tp2_price = _second(parsed.target_prices)
        sl_price = _dec(parsed.stop_loss)

        client_order_id = intent.get("client_order_id")
        trade_id = f"trade-{client_order_id}"
        quote_amount = _dec(intent.get("quote_amount")) or Decimal("0")

        return self.store.create_trade(
            trade_id=trade_id,
            signal_id=intent["signal_id"],
            idempotency_key=intent["idempotency_key"],
            channel_id=int(intent["create_channel_id"]),
            message_id=int(intent["create_msg_id"]),
            symbol=str(intent["symbol"]),
            side=str(intent["side"]),
            entry_mode=entry_mode,
            requested_quote=quote_amount,
            entry_price=entry_price,
            trigger_offset_ticks=offset,
            trigger_price=trigger_price,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            tp1_allocation_pct=Decimal(str(self.cfg.tp1_allocation_pct_of_acquired)),
            tp2_allocation_pct=Decimal(str(self.cfg.tp2_allocation_pct_of_remaining)),
            sl_price=sl_price,
            entry_intent_id=intent["intent_id"],
        )

    def _compute_trigger(self, entry_price: Decimal, ticks: int, tick_size: Decimal) -> Decimal | None:
        """Trigger = entry_price - ticks * tick_size, quantized down to tick size."""
        raw = entry_price - (Decimal(ticks) * tick_size)
        try:
            return raw.quantize(tick_size, rounding="ROUND_DOWN")
        except InvalidOperation:
            return None

    def _reject_trade_intent(self, intent: dict[str, Any], reason: str) -> None:
        self.store.transition_intent(
            intent["intent_id"], "rejected", note=f"trade setup: {reason}",
            reject_reason=reason, category="risk",
        )
        self.store.update_signal(intent["signal_id"], "rejected", reason)
        self._notify_intent(intent, "REJECTED_TRADE", reason)

    # ===================================================================
    # Price monitoring (called by the MarketMonitor feed callback)
    # ===================================================================

    def _evaluate_trade_tick(self, trade: dict[str, Any], price: Decimal, received_at: float) -> None:
        """Dispatch a tick to the appropriate handler for a trade state."""
        if self._is_stale(received_at):
            return
        state = trade["trade_state"]
        try:
            if state == "pending_entry":
                self._check_entry_trigger(trade, price)
            elif state in ("entry_active", "position_open"):
                self._check_tp_sl(trade, price)
            elif state in ("tp1_active", "tp1_completed"):
                self._check_tp2_or_sl(trade, price)
            elif state == "tp2_active":
                self._check_sl_and_tp2_remainder(trade, price)
            elif state in (
                "stop_loss_executing", "exit_pending", "needs_reconciliation",
                "oco_submitting", "oco_active", "oco_partial",
            ):
                self._reconcile_trade(trade["trade_id"])
        except Exception as exc:  # noqa: BLE001 - never let one tick kill the monitor
            log.error("trade tick evaluation failed for %s: %s", trade["trade_id"], exc)

    def _is_stale(self, received_at: float) -> bool:
        threshold = float(self.cfg.entry_trigger_stale_threshold_s)
        age = max(0.0, self._clock() - received_at)
        if age > threshold:
            log.warning("ignoring stale price observation (age %.1fs > %.1fs)", age, threshold)
            return True
        return False

    def _check_entry_trigger(self, trade: dict[str, Any], price: Decimal) -> None:
        """Activate a pending entry when price crosses the trigger upward."""
        if trade.get("entry_mode") != "conditional":
            return
        trigger = _dec(trade.get("trigger_price"))
        if trigger is None or price < trigger:
            return
        self._submit_entry_order(trade)

    def _exchange_protection_active(self, trade: dict[str, Any]) -> bool:
        """True when the exchange owns a verified TP/SL for this entry."""
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return False
        getter = getattr(self.store, "get_protection", None)
        if getter is None:
            return False
        try:
            protection = getter(intent_id)
        except Exception:  # noqa: BLE001 - never block the lifecycle on a lookup
            return False
        if protection and protection.get("status") == "active":
            log.info(
                "exchange-side TP/SL is active for trade %s; skipping local exit",
                trade.get("trade_id"),
            )
            return True
        return False

    def _check_tp_sl(self, trade: dict[str, Any], price: Decimal) -> None:
        if self._exchange_protection_active(trade):
            return
        # Native OCO exits carry their own TP + stop-limit legs; do not place
        # conflicting separate TP/SL orders while OCOs are active.
        if self._ocos_active(trade):
            return
        sl = _dec(trade.get("sl_price"))
        tp1 = _dec(trade.get("tp1_price"))
        # SL wins the trade-state transition: when both are crossed in one tick
        # the downward stop takes priority over a TP fill.
        if sl is not None and price <= sl:
            self._enter_stop_loss(trade, price)
            return
        if tp1 is not None and price >= tp1:
            self._enter_tp1(trade)

    def _check_tp2_or_sl(self, trade: dict[str, Any], price: Decimal) -> None:
        if self._exchange_protection_active(trade):
            return
        # Native OCO exits carry their own TP + stop-limit legs; do not place
        # conflicting separate TP/SL orders while OCOs are active.
        if self._ocos_active(trade):
            return
        sl = _dec(trade.get("sl_price"))
        if sl is not None and price <= sl:
            self._enter_stop_loss(trade, price)
            return
        tp2 = _dec(trade.get("tp2_price"))
        if tp2 is not None and price >= tp2:
            self._enter_tp2(trade)
            return
        # A partially filled/canceled TP1 may leave an unsold remainder.  Re-arm
        # it while price is still at/above the first target.
        tp1 = _dec(trade.get("tp1_price"))
        if (
            tp1 is not None
            and price >= tp1
            and not self._has_unconfirmed_order(trade, "tp1")
            and (
                self._target_remaining(trade, "tp1") > 0
                or self._tp_never_started(trade, "tp1")
            )
        ):
            self._do_tp_sale(trade, "tp1")

    def _check_sl_and_tp2_remainder(self, trade: dict[str, Any], price: Decimal) -> None:
        if self._exchange_protection_active(trade):
            return
        # Native OCO exits carry their own TP + stop-limit legs; do not place
        # conflicting separate TP/SL orders while OCOs are active.
        if self._ocos_active(trade):
            return
        sl = _dec(trade.get("sl_price"))
        if sl is not None and price <= sl:
            self._enter_stop_loss(trade, price)
            return
        tp2 = _dec(trade.get("tp2_price"))
        if (
            tp2 is not None
            and price >= tp2
            and not self._has_unconfirmed_order(trade, "tp2")
            and (
                self._target_remaining(trade, "tp2") > 0
                or self._tp_never_started(trade, "tp2")
            )
        ):
            self._do_tp_sale(trade, "tp2")

    # ===================================================================
    # Entry submission (direct or conditional trigger hit)
    # ===================================================================

    def _submit_entry_order(self, trade: dict[str, Any]) -> None:
        """Submit the Limit Buy (conditional trigger hit) through the engine gate."""
        store = self.store
        now = self._ts()
        with store.db.transaction() as conn:
            row = conn.execute(
                "SELECT trade_state, entry_intent_id FROM trades WHERE trade_id = ?",
                (trade["trade_id"],),
            ).fetchone()
            if row is None:
                return
            if row[0] != "pending_entry":
                return  # idempotent: already activated
            # Atomically flip pending_entry -> entry_active. If submission
            # then fails the reconciler parks/retries the trade safely.
            conn.execute(
                "UPDATE trades SET trade_state='entry_active', "
                "activated_at=COALESCE(activated_at, ?), updated_at=? WHERE trade_id=?",
                (now, now, trade["trade_id"]),
            )
        intent = store.get_intent(row[1])
        if intent is None:
            store.update_trade_state(trade["trade_id"], "needs_reconciliation", note="entry intent missing")
            return
        refreshed = store.get_trade(trade["trade_id"])
        if refreshed is not None:
            trade = refreshed
        self._notify_trade(trade, "ENTRY_TRIGGER_ACTIVATED", "submitting Limit Buy at entry price")
        if self.engine is None:
            self._record_error(trade, "engine unavailable", "runtime")
            return
        # Reuse the engine's full live gate: budget + balance preflight + order.
        try:
            self.engine._execute_intent(intent["intent_id"])
        except Exception as exc:  # noqa: BLE001
            self._record_error(trade, f"entry submission failed: {exc}", "exchange")
            self._notify_trade(trade, "ENTRY_SUBMISSION_FAILED", str(exc)[:500])
            # A failed submission may be a definitive local rejection (no
            # exchange order) or an uncertain outcome.  Reconcile the intent so
            # the trade does not stay stuck in entry_active forever.
            self._reconcile_entry_fills(trade)
            refreshed = store.get_trade(trade["trade_id"])
            if refreshed is not None:
                self._settle_entry_state(refreshed)
            return
        # Sync the exchange order id/state that _execute_intent just recorded.
        executed_intent = store.get_intent(intent["intent_id"])
        if executed_intent is not None:
            self.on_intent_executed(executed_intent)
        refreshed = store.get_trade(trade["trade_id"])
        if refreshed is not None:
            trade = refreshed
        self._notify_trade(trade, "ENTRY_SUBMITTED", "Limit Buy submitted at entry price")

    def _settle_entry_state(self, trade: dict[str, Any]) -> None:
        """Advance an entry-state trade once the entry intent is terminal.

        An OCO exit may only be placed after the entry intent is terminal
        (filled, canceled with a known partial fill, or definitively rejected)
        and the fill history is fully synced.  Placing protection while the
        entry is still open would leave later fills unprotected.
        """
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return
        intent = self.store.get_intent(intent_id)
        if intent is None:
            return
        state = trade["trade_state"]
        if state not in ("pending_entry", "entry_active", "position_open"):
            return
        intent_state = intent.get("state")
        if int(intent.get("fill_sync_required") or 0) == 1:
            return  # fill history pending; never size exits from stale data
        terminal = intent_state in TERMINAL_INTENT_STATES
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if not terminal:
            if state == "pending_entry" and intent_state in (
                "submitted", "open", "partially_filled", "reconciling",
            ):
                self.store.update_trade_state(
                    trade["trade_id"], "entry_active", note="entry order is working"
                )
            elif intent_state == "planned" and state == "entry_active":
                # Never submit autonomously from here.  A retried queue item can
                # atomically resume a planned intent; an activation that has no
                # exchange order is escalated instead.
                self.store.update_trade_state(
                    trade["trade_id"], "needs_reconciliation",
                    note="entry activated but no submission was claimed; operator review required",
                )
                self._notify_trade(
                    trade, "ENTRY_NOT_SUBMITTED",
                    "entry activation did not record a submission; manual reconciliation required",
                )
            return
        if acquired > 0:
            if state in ("pending_entry", "entry_active"):
                self.store.update_trade_state(
                    trade["trade_id"], "position_open", note="entry fills confirmed"
                )
            # Place the two native OCO exit orders once the position is open.
            trade = self._refresh(trade)
            self._place_oco_exits(trade)
            return
        if intent_state in ("canceled", "canceled_local", "rejected", "failed"):
            self.store.update_trade_state(
                trade["trade_id"], "closed",
                note=f"entry intent {intent_state} with no fills; no position opened",
                sl_state="dormant",
            )
            refreshed = self.store.get_trade(trade["trade_id"])
            self._notify_trade(
                refreshed or trade, "ENTRY_NOT_FILLED",
                f"entry {intent_state}; no position opened, no exit orders placed",
            )
        elif intent_state == "filled":
            # Filled according to the exchange but no fill records: never treat
            # this as a zero position.  Fail closed for manual reconciliation.
            self.store.update_trade_state(
                trade["trade_id"], "needs_reconciliation",
                note="entry reports filled but no fill records were persisted",
            )
            self._notify_trade(
                trade, "ENTRY_FILLS_MISSING",
                "entry filled but fills could not be persisted; manual reconciliation required",
            )

    # ===================================================================
    # Take-profit handling
    # ===================================================================

    def _enter_tp1(self, trade: dict[str, Any]) -> None:
        """Enter TP1: cancel entry remainder, reconcile fills, sell the target."""
        if not self._transition_state(trade, "tp1_active", ("entry_active", "position_open")):
            return
        trade = self._refresh(trade)
        if not self._cancel_entry_remainder(trade):
            self._record_error(trade, "entry cancel unconfirmed; TP1 held", "exchange")
            return
        if not self._reconcile_entry_fills(trade):
            self._record_error(trade, "entry fills unconfirmed; TP1 held", "reconciliation")
            return
        self._do_tp_sale(self._refresh(trade), "tp1")

    def _enter_tp2(self, trade: dict[str, Any]) -> None:
        """Enter TP2: cancel any TP1 remainder, then sell what is actually left."""
        if not self._transition_state(
            trade, "tp2_active", ("tp1_active", "tp1_completed", "position_open")
        ):
            return
        trade = self._refresh(trade)
        if not self._cancel_orders_of_kind(trade, "tp1"):
            self._record_error(trade, "TP1 cancel unconfirmed; TP2 held", "exchange")
            return
        if not self._reconcile_entry_fills(trade):
            self._record_error(trade, "entry fills unconfirmed; TP2 held", "reconciliation")
            return
        self._reconcile_trade_orders(trade)
        self._do_tp_sale(self._refresh(trade), "tp2")

    def _transition_state(
        self, trade: dict[str, Any], new_state: str, allowed_from: tuple[str, ...]
    ) -> bool:
        """Atomically move trade state. Returns True if transitioned."""
        now = self._ts()
        with self.store.db.transaction() as conn:
            row = conn.execute(
                "SELECT trade_state FROM trades WHERE trade_id = ?", (trade["trade_id"],)
            ).fetchone()
            if row is None:
                return False
            state = row[0]
            if state in (
                "tp2_completed", "closed", "needs_reconciliation",
                "stop_loss_executing", "exit_pending",
            ):
                return False
            if state not in allowed_from:
                return False
            conn.execute(
                "UPDATE trades SET trade_state=?, updated_at=? WHERE trade_id=?",
                (new_state, now, trade["trade_id"]),
            )
        refreshed = self.store.get_trade(trade["trade_id"])
        if refreshed is not None:
            trade = refreshed
        self._notify_trade(trade, "STATE_TRANSITION", f"trade -> {new_state}")
        return True

    def _do_tp_sale(self, trade: dict[str, Any], target: str) -> None:
        """Reconcile fills, then submit/reuse a TP sell order for the remainder."""
        if self.engine is None:
            self._record_error(trade, "no engine available for TP sale", "runtime")
            return
        # A TP2 sell must never coexist with an open TP1 remainder.  This is
        # repeated here (not only in _enter_tp2) so restart/reconciliation
        # retries cannot skip the cancellation.
        if target == "tp2" and not self._cancel_orders_of_kind(trade, "tp1"):
            self._record_error(trade, "TP1 cancel unconfirmed; TP2 held", "exchange")
            return
        # A TP sell must never coexist with a still-fillable entry remainder.
        if not self._cancel_entry_remainder(trade):
            self._record_error(trade, "entry cancel unconfirmed; TP held", "exchange")
            return
        synced = self._reconcile_entry_fills(trade)
        trade = self._refresh(trade)
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)

        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if acquired <= 0:
            if not synced:
                self._record_error(
                    trade,
                    "entry fills unconfirmed; refusing to mark TP complete",
                    "reconciliation",
                )
                return
            self._advance_tp_if_empty(trade, target)
            return

        # Never place a second sell while this target still has an order that
        # may be live.  The reconciler has just refreshed its state.  A
        # ``pending`` row was never sent and is reused below with a safe qty.
        if self._has_live_order(trade, target):
            return

        cap = max(1, int(getattr(self.cfg, "stop_loss_max_retries", 3) or 3))
        rejected = [
            order for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
            if order["state"] == "rejected"
        ]
        if len(rejected) >= cap:
            self._record_error(
                trade, f"{target} retry cap reached ({cap}); not submitting another sell",
                "exchange",
            )
            return

        pending = [
            order for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
            if order["state"] == "pending"
        ]
        if pending:
            order: dict[str, Any] | None = pending[0]
            assert order is not None
            price = _dec(order.get("price")) or self._tp_price(trade, target)
            requested = _dec(order.get("quantity")) or Decimal("0")
            allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
            target_capacity = (
                max(Decimal("0"), allocated - self._sold_for_kind(trade, target))
                if allocated > 0
                else requested
            )
            # A late fill can arrive after this row was created.  It was never
            # sent, so base the submitted quantity on current fills rather than
            # the stale row.
            qty = min(requested, target_capacity, self._remaining_base(trade))
            qty = self._quantize_qty(trade, qty)
            if qty <= 0:
                self.store.transition_trade_order(
                    order["trade_order_id"], "canceled",
                    note="not sent; no remaining quantity",
                )
                if self._remaining_base(trade) <= 0:
                    self._finalize_trade(trade)
                else:
                    self._record_residual(
                        trade, target, requested,
                        f"{target} remainder below lot size after rebase",
                    )
                    self._advance_tp_if_empty(trade, target)
                return
            if qty < requested:
                # Never sent: replace it with a safely reduced quantity and
                # shrink the allocation so completion can be observed.
                self.store.transition_trade_order(
                    order["trade_order_id"], "canceled",
                    note=f"not sent; quantity rebased {requested}->{qty}",
                )
                order = None
                new_allocated = self._sold_for_kind(trade, target) + qty
                if allocated > new_allocated:
                    if target == "tp1":
                        self.store.update_trade_fields(
                            trade["trade_id"], tp1_allocated_qty=new_allocated
                        )
                    else:
                        self.store.update_trade_fields(
                            trade["trade_id"], tp2_allocated_qty=new_allocated
                        )
                    trade = self._refresh(trade)
        else:
            order = None
            allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
            if allocated <= 0:
                if target == "tp1":
                    allocated = acquired * (
                        _dec(trade.get("tp1_allocation_pct")) or Decimal("100")
                    ) / Decimal("100")
                else:
                    base = max(
                        Decimal("0"),
                        acquired - self._sold_for_kind(trade, "tp1"),
                    )
                    allocated = base * (
                        _dec(trade.get("tp2_allocation_pct")) or Decimal("100")
                    ) / Decimal("100")
                allocated = self._quantize_qty(trade, allocated)
                if allocated <= 0:
                    self._record_residual(trade, target, acquired, f"{target} allocation below lot size")
                    self._advance_tp_if_empty(trade, target)
                    return
                if target == "tp1":
                    self.store.update_trade_fields(
                        trade["trade_id"], tp1_allocated_qty=allocated
                    )
                else:
                    self.store.update_trade_fields(
                        trade["trade_id"], tp2_allocated_qty=allocated
                    )
                trade = self._refresh(trade)
            qty = max(Decimal("0"), allocated - self._sold_for_kind(trade, target))
            qty = min(qty, self._compute_available(trade))
            qty = self._quantize_qty(trade, qty)
            price = self._tp_price(trade, target)

        if price is None or price <= 0:
            self._record_error(trade, f"{target} has no valid price; target skipped", "config")
            self._advance_tp_if_empty(trade, target)
            return
        price = self._quantize_price(trade, price)

        if qty <= 0:
            self._record_residual(
                trade, target, self._target_remaining(trade, target),
                f"{target} remainder below lot size",
            )
            self._advance_tp_if_empty(trade, target)
            return

        reason = self._validate_sell(trade, qty, price, order_type="LIMIT")
        if reason:
            self._record_residual(trade, target, qty, f"{target}: {reason}")
            self._notify_trade(trade, "TP_MINIMUM_NOT_MET", f"{target}: {reason}")
            self._advance_tp_if_empty(trade, target)
            return

        if order is None:
            order = self._create_trade_order(trade, target, qty, price, order_type="LIMIT")
        self._submit_tp_order(trade, target, order, qty, price)

    def _tp_price(self, trade: dict[str, Any], target: str) -> Decimal | None:
        price = _dec(trade.get(f"{target}_price"))
        if price is not None and price > 0:
            return price
        return None

    def _target_remaining(self, trade: dict[str, Any], target: str) -> Decimal:
        allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
        if allocated <= 0:
            return Decimal("0")
        return max(Decimal("0"), allocated - self._sold_for_kind(trade, target))

    def _tp_never_started(self, trade: dict[str, Any], target: str) -> bool:
        """True when a reached TP never reached its allocation/order step.

        This is the durable restart case: the trade state says the target was
        reached, but the process died before ``_do_tp_sale`` persisted the
        allocation or created an order.  A ``pending`` row was never sent, so
        retrying it is safe.
        """
        if (_dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")) > 0:
            return False
        orders = self.store.list_trade_orders(trade["trade_id"], kind=target)
        return not orders or all(order["state"] == "pending" for order in orders)

    def _has_pending_order(self, trade: dict[str, Any], target: str) -> bool:
        return any(
            order["state"] == "pending"
            for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
        )

    def _compute_available(self, trade: dict[str, Any]) -> Decimal:
        """Bot-owned base quantity available to sell right now."""
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        sold = (
            self._sold_for_kind(trade, "tp1")
            + self._sold_for_kind(trade, "tp2")
            + self._sold_for_kind(trade, "stop_loss")
        )
        outstanding = Decimal("0")
        for order in self.store.list_trade_orders(trade["trade_id"]):
            if order["state"] in UNCONFIRMED_ORDER_STATES:
                qty = _dec(order.get("quantity")) or Decimal("0")
                filled = _dec(order.get("sold_qty")) or Decimal("0")
                outstanding += max(Decimal("0"), qty - filled)
        return max(Decimal("0"), acquired - sold - outstanding)

    def _create_trade_order(
        self,
        trade: dict[str, Any],
        kind: str,
        qty: Decimal,
        price: Decimal | None,
        *,
        order_type: str,
    ) -> dict[str, Any]:
        attempt = len(self.store.list_trade_orders(trade["trade_id"], kind=kind)) + 1
        client_order_id = self._client_order_id(trade["trade_id"], kind, attempt)
        return self.store.create_trade_order(
            trade_order_id=f"to-{client_order_id}",
            trade_id=trade["trade_id"],
            kind=kind,
            client_order_id=client_order_id,
            symbol=trade["symbol"],
            side="sell",
            order_type=order_type,
            price=price,
            quantity=qty,
            allocated_qty=qty,
        )

    def _submit_tp_order(
        self, trade: dict[str, Any], target: str, order: dict[str, Any], qty: Decimal, price: Decimal
    ) -> None:
        engine = self.engine
        if engine is None:
            self._record_error(trade, "no engine available for TP sale", "runtime")
            return
        self.store.transition_trade_order(
            order["trade_order_id"], "submitting", note="submission started"
        )
        try:
            result = engine.submit_sell_limit(
                trade, order["client_order_id"], price, qty, kind=target
            )
        except MexcDuplicateOrder as exc:
            # The earlier attempt may have landed.  Never submit a second
            # order; resolve this one by client order id.
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="duplicate client order id; earlier attempt may have landed",
            )
            self._record_error(trade, f"TP duplicate client id: {exc}", "exchange")
            self._reconcile_trade_orders(trade)
            return
        except _UNCERTAIN_ERRORS as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note=f"uncertain submission: {exc}"[:500],
            )
            self._record_error(trade, f"TP submission outcome unknown: {exc}", "exchange")
            return
        except Exception as exc:  # noqa: BLE001 - definitive rejection before submission
            self.store.transition_trade_order(
                order["trade_order_id"], "rejected", note=str(exc)[:500]
            )
            self._record_error(trade, f"TP rejected: {exc}", "exchange")
            self._notify_trade(trade, "TP_REJECTED", str(exc)[:500])
            return

        exchange_id = result.get("exchange_order_id") or result.get("order_id")
        executed = _dec(result.get("executed_qty")) or Decimal("0")
        if not exchange_id:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="no exchange order id returned; reconcile by client order id",
            )
            self._record_error(trade, "TP submission returned no order id", "exchange")
            return
        self.store.transition_trade_order(
            order["trade_order_id"], "open", note="submitted",
            exchange_order_id=str(exchange_id),
            exchange_status=result.get("state") or result.get("status"),
            sold_qty=executed,
        )
        label = "TP_SUBMITTED" if target == "tp1" else "TP2_SUBMITTED"
        self._notify_trade(trade, label, f"{target} sell {qty} @ {price}")
        # Refresh immediately so a fast fill is not lost if the process exits.
        self._reconcile_trade_orders(trade)

    # ===================================================================
    # Native two-OCO exit placement
    # ===================================================================

    def _oco_orders(self, trade: dict[str, Any], kind: str) -> list[dict[str, Any]]:
        """Return trade orders for a given OCO target (tp1 / tp2)."""
        return self._list_trade_orders(trade["trade_id"], kind=kind)

    def _list_trade_orders(
        self, trade_id: str, kind: str | None = None
    ) -> list[dict[str, Any]]:
        """Defensive wrapper around store.list_trade_orders (mock stores may lack it)."""
        list_orders = getattr(self.store, "list_trade_orders", None)
        if list_orders is None:
            return []
        return list_orders(trade_id=trade_id, kind=kind)

    def _oco_order_active(self, trade: dict[str, Any], kind: str) -> bool:
        """True when an OCO order for ``kind`` is still live or uncertain."""
        return any(
            order["state"] in UNCONFIRMED_ORDER_STATES
            for order in self._oco_orders(trade, kind)
        )

    def _ocos_active(self, trade: dict[str, Any]) -> bool:
        """True when either OCO exit order is still live or uncertain."""
        return self._oco_order_active(trade, "tp1") or self._oco_order_active(trade, "tp2")

    def _ocos_terminal(self, trade: dict[str, Any]) -> bool:
        """True when both OCO orders have reached a terminal state."""
        orders = self._list_trade_orders(trade["trade_id"])
        oco_orders = [o for o in orders if o.get("kind") in ("tp1", "tp2")]
        if not oco_orders:
            return False
        return all(o["state"] in TERMINAL_ORDER_STATES for o in oco_orders)

    def _place_oco_exits(self, trade: dict[str, Any]) -> None:
        """Place two native OCO SELL exit orders once entry fills are confirmed.

        OCO 1: ~50% of acquired quantity at TP1 with stop-loss at SL.
        OCO 2: the remainder at TP2 with the same stop-loss.

        Idempotency is per leg, not per trade: an already confirmed leg is never
        resubmitted, a ``pending`` leg (never sent) is safely reused/rebased, a
        bounded number of outright rejections is retried, and an uncertain
        submission is only reconciled — never resent with a new client id.
        Protection is reported as active only when every required leg is
        confirmed by the exchange.
        """
        if self.engine is None:
            self._record_error(trade, "no engine available for OCO exits", "runtime")
            return

        previous_state = trade.get("trade_state")
        # Never protect a position whose entry is still working or whose fill
        # history is not fully synced.
        intent_id = trade.get("entry_intent_id")
        if intent_id:
            intent = self.store.get_intent(intent_id)
            if intent is not None:
                if intent.get("state") not in TERMINAL_INTENT_STATES:
                    return
                if int(intent.get("fill_sync_required") or 0) == 1:
                    return

        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if acquired <= 0:
            self._record_error(trade, "no acquired quantity; cannot place OCO exits", "runtime")
            return

        sl_price = _dec(trade.get("sl_price"))
        tp1_price = _dec(trade.get("tp1_price"))
        tp2_price = _dec(trade.get("tp2_price"))
        entry_price = _dec(trade.get("entry_price")) or _dec(trade.get("avg_entry_price"))

        problem: str | None = None
        if sl_price is None or sl_price <= 0:
            problem = "no stop-loss price; OCO exit requires a stop-loss"
        elif tp1_price is None or tp1_price <= 0:
            problem = "no TP1 price; OCO exit requires TP1"
        elif tp2_price is None or tp2_price <= 0:
            problem = "no TP2 price; OCO exit requires TP2"
        elif entry_price is None or entry_price <= 0:
            problem = "entry price is unknown; cannot order the exit ladder"
        elif not (sl_price < entry_price < tp1_price <= tp2_price):
            problem = (
                f"invalid exit ladder: stop-loss {sl_price} < entry {entry_price} < "
                f"TP1 {tp1_price} <= TP2 {tp2_price} is required"
            )
        if problem is not None:
            self.store.update_trade_state(
                trade["trade_id"], "needs_reconciliation",
                note=f"OCO not placed: {problem}",
            )
            self._notify_trade(trade, "OCO_CONFIG_REJECTED", problem)
            return

        # The validation above guarantees a usable, ordered exit ladder.
        assert sl_price is not None and tp1_price is not None and tp2_price is not None

        # Allocations are computed once from the actual acquired position.
        # A restart must never shrink or re-expand an existing allocation.
        tp1_pct = _dec(trade.get("tp1_allocation_pct")) or Decimal("50")
        tp2_pct = _dec(trade.get("tp2_allocation_pct")) or Decimal("100")
        tp1_allocated = _dec(trade.get("tp1_allocated_qty")) or Decimal("0")
        tp2_allocated = _dec(trade.get("tp2_allocated_qty")) or Decimal("0")
        updates: dict[str, Any] = {}
        if tp1_allocated <= 0:
            tp1_allocated = self._quantize_qty(trade, acquired * tp1_pct / Decimal("100"))
            updates["tp1_allocated_qty"] = tp1_allocated
        if tp2_allocated <= 0:
            pool = max(Decimal("0"), acquired - tp1_allocated)
            tp2_allocated = self._quantize_qty(trade, pool * tp2_pct / Decimal("100"))
            updates["tp2_allocated_qty"] = tp2_allocated
        if updates:
            self.store.update_trade_fields(trade["trade_id"], **updates)
            trade = self._refresh(trade)

        required: dict[str, tuple[Decimal, Decimal]] = {
            "tp1": (tp1_allocated, tp1_price),
            "tp2": (tp2_allocated, tp2_price),
        }
        required = {k: v for k, v in required.items() if v[0] > 0}
        if not required:
            self.store.update_trade_state(
                trade["trade_id"], "needs_reconciliation",
                note="OCO allocations are below the symbol lot size; position remains unprotected",
            )
            self._notify_trade(
                trade, "OCO_ALLOCATION_TOO_SMALL",
                "acquired quantity is below the lot size; no OCO exit could be placed",
            )
            return

        results: dict[str, str] = {}
        for kind, (qty, price) in required.items():
            results[kind] = self._ensure_oco_leg(trade, kind, qty, price, sl_price)

        confirmed = [k for k, v in results.items() if v in ("confirmed", "filled")]
        uncertain = [k for k, v in results.items() if v == "uncertain"]
        failed = [k for k, v in results.items() if v in ("rejected", "failed", "canceled")]
        trade = self._refresh(trade)

        def _publish(
            new_state: str, note: str, event_key: str, text: str
        ) -> None:
            self.store.update_trade_state(trade["trade_id"], new_state, note=note)
            if new_state != previous_state:
                self._notify_trade(trade, event_key, text)

        if len(confirmed) == len(required):
            _publish(
                "oco_active",
                "all required OCO exit legs confirmed",
                "OCO_PROTECTION_ACTIVE",
                f"{len(confirmed)} OCO exit leg(s) confirmed",
            )
        elif confirmed:
            _publish(
                "oco_partial",
                "partial OCO protection: confirmed="
                f"{','.join(confirmed) or '-'}; uncertain={','.join(uncertain) or '-'}; "
                f"failed={','.join(failed) or '-'}",
                "OCO_PROTECTION_PARTIAL",
                f"only {','.join(confirmed)} confirmed; incomplete protection",
            )
        elif uncertain:
            _publish(
                "oco_submitting",
                "OCO submission outcome uncertain; reconcile, never resubmit",
                "OCO_PROTECTION_UNCERTAIN",
                "OCO submission outcome uncertain; reconciliation required",
            )
        else:
            _publish(
                "needs_reconciliation",
                f"no OCO exit could be confirmed (failed={','.join(failed) or '-'})",
                "OCO_PROTECTION_FAILED",
                "no OCO exit could be confirmed; position is unprotected and needs review",
            )

    def _ensure_oco_leg(
        self,
        trade: dict[str, Any],
        kind: str,
        wanted_qty: Decimal,
        price: Decimal,
        stop_price: Decimal,
    ) -> str:
        """Return the per-leg state: confirmed/filled/uncertain/rejected/failed/canceled."""

        orders = self._list_trade_orders(trade["trade_id"], kind=kind)
        if any(order["state"] == "filled" for order in orders):
            return "filled"
        if any(order["state"] in (
            "submitting", "cancel_requested", "unknown_requires_reconciliation",
        ) for order in orders):
            # A write claim or a previously assigned exchange id is not proof
            # of current protection. Reconcile these rows; never resend them.
            return "uncertain"
        if any(order["state"] in ("open", "partially_filled") for order in orders):
            return "confirmed"
        if any(order["state"] == "canceled" for order in orders):
            # An OCO leg can only disappear because it was canceled/replaced on
            # the exchange; resubmitting automatically could oversell.
            return "canceled"

        rejected = [order for order in orders if order["state"] == "rejected"]
        cap = max(1, int(getattr(self.cfg, "stop_loss_max_retries", 3) or 3))
        if len(rejected) >= cap:
            return "failed"

        qty = self._quantize_qty(trade, min(wanted_qty, self._remaining_base(trade)))
        price_q = self._quantize_price(trade, price)
        if qty <= 0:
            self._record_residual(trade, kind, wanted_qty, f"{kind} remainder below lot size")
            return "failed"
        reason = self._validate_sell(trade, qty, price_q, order_type="LIMIT")
        if reason:
            self._record_residual(trade, kind, qty, f"{kind}: {reason}")
            self._notify_trade(trade, "OCO_MINIMUM_NOT_MET", f"{kind}: {reason}")
            return "failed"

        pending = [order for order in orders if order["state"] == "pending"]
        next_order: dict[str, Any] | None = pending[0] if pending else None
        if next_order is not None:
            existing_qty = _dec(next_order.get("quantity")) or Decimal("0")
            existing_price = _dec(next_order.get("price")) or Decimal("0")
            if existing_qty != qty or existing_price != price_q:
                # Never sent: replace it with the corrected amount/price.
                self.store.transition_trade_order(
                    next_order["trade_order_id"], "canceled",
                    note=f"not sent; rebased {existing_qty}->{qty} @ {price_q}",
                )
                next_order = None
        if next_order is None:
            next_order = self._create_trade_order(
                trade, kind, qty, price_q, order_type="OCO_STOP_LIMIT_ORDER"
            )

        result = self._submit_oco_order(trade, kind, next_order, qty, price_q, stop_price)
        refreshed = self.store.get_trade_order(next_order["trade_order_id"])
        if result is not None and result.get("exchange_order_id"):
            return "confirmed"
        state = (refreshed or {}).get("state")
        if state == "unknown_requires_reconciliation":
            return "uncertain"
        if state == "rejected":
            return "rejected"
        if state == "filled":
            return "filled"
        return "failed"

    def _target_qty(self, trade: dict[str, Any], target: str) -> Decimal:
        """Return the allocated quantity for an OCO target."""
        return _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")

    def _submit_oco_order(
        self,
        trade: dict[str, Any],
        target: str,
        order: dict[str, Any],
        qty: Decimal,
        price: Decimal,
        stop_price: Decimal,
    ) -> dict[str, Any] | None:
        """Submit one OCO exit order and persist its exchange identifier."""
        engine = self.engine
        if engine is None:
            self._record_error(trade, "no engine available for OCO submission", "runtime")
            return None
        self.store.transition_trade_order(
            order["trade_order_id"], "submitting", note="OCO submission started"
        )
        try:
            result = engine.submit_oco_sell(
                trade, order["client_order_id"], price, qty, stop_price, kind=target
            )
        except MexcDuplicateOrder as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="duplicate client order id; earlier OCO attempt may have landed",
            )
            self._record_error(trade, f"{target} OCO duplicate client id: {exc}", "exchange")
            self._reconcile_trade_orders(trade)
            return None
        except _UNCERTAIN_ERRORS as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note=f"uncertain OCO submission: {exc}"[:500],
            )
            self._record_error(trade, f"{target} OCO submission outcome unknown: {exc}", "exchange")
            return None
        except Exception as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "rejected", note=str(exc)[:500]
            )
            self._record_error(trade, f"{target} OCO rejected: {exc}", "exchange")
            self._notify_trade(trade, "OCO_REJECTED", f"{target}: {str(exc)[:500]}")
            return None

        exchange_id = result.get("orderId") or result.get("exchange_order_id")
        executed = _dec(result.get("executed_qty")) or Decimal("0")
        if not exchange_id:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="OCO submission returned no order id; reconcile by client order id",
            )
            self._record_error(trade, f"{target} OCO returned no order id", "exchange")
            return None
        self.store.transition_trade_order(
            order["trade_order_id"], "open", note="OCO submitted",
            exchange_order_id=str(exchange_id),
            exchange_status=result.get("state") or result.get("status"),
            sold_qty=executed,
        )
        self._notify_trade(trade, f"OCO_SUBMITTED_{target.upper()}", f"{target} OCO {qty} @ {price} SL @ {stop_price}")
        self._reconcile_trade_orders(trade)
        return {
            "exchange_order_id": str(exchange_id),
            "executed_qty": executed,
        }


    def _advance_tp_if_empty(self, trade: dict[str, Any], target: str) -> None:
        """If the sell quantity is zero, advance the target without inventing fills."""
        now = self._ts()
        with self.store.db.transaction() as conn:
            row = conn.execute(
                "SELECT trade_state FROM trades WHERE trade_id = ?", (trade["trade_id"],)
            ).fetchone()
            if row is None:
                return
            state = row[0]
            new_state = "tp1_completed" if target == "tp1" else "tp2_completed"
            if state in (f"{target}_active", "needs_reconciliation"):
                conn.execute(
                    "UPDATE trades SET trade_state=?, updated_at=? WHERE trade_id=?",
                    (new_state, now, trade["trade_id"]),
                )
            else:
                return
        refreshed = self.store.get_trade(trade["trade_id"])
        if refreshed is not None:
            trade = refreshed
        self._notify_trade(trade, "TP_SKIPPED", f"{target} qty below minimum; marked completed")

    # ===================================================================
    # Stop-loss handling
    # ===================================================================

    def _enter_stop_loss(self, trade: dict[str, Any], trigger_price: Decimal) -> None:
        """Execute SL: atomically transition, then resume the durable exit."""
        now = self._ts()
        with self.store.db.transaction() as conn:
            row = conn.execute(
                "SELECT trade_state, sl_state FROM trades WHERE trade_id = ?", (trade["trade_id"],)
            ).fetchone()
            if row is None:
                return
            state, sl_state = row[0], row[1]
            if state in ("stop_loss_executing", "exit_pending", "closed"):
                return
            if sl_state == "completed":
                return
            conn.execute(
                "UPDATE trades SET trade_state='stop_loss_executing', sl_state='executing', "
                "updated_at=? WHERE trade_id=?",
                (now, trade["trade_id"]),
            )
        refreshed = self.store.get_trade(trade["trade_id"])
        if refreshed is not None:
            trade = refreshed
        self._notify_trade(trade, "SL_TRIGGERED", f"stop-loss activated at {trigger_price}")
        self._resume_stop_loss(trade)

    def _resume_stop_loss(self, trade: dict[str, Any]) -> None:
        """Cancel conflicts, confirm entry fills, and sell the actual remainder.

        This method is intentionally idempotent: it runs on the first SL tick,
        on every recovery/reconciliation sweep, and on restart.  It never
        submits a second exit while an earlier exit order is live or uncertain.
        """
        if self.engine is None:
            self._record_error(trade, "no engine available for stop-loss", "runtime")
            return

        # 1. Resolve any earlier stop-loss order first.  A market sell may have
        #    landed even when the process died before recording the response.
        #    A ``pending`` row was never sent, so cancel it locally and let the
        #    normal path create a new order for the confirmed remainder.
        for order in self.store.list_trade_orders(trade["trade_id"], kind="stop_loss"):
            if order["state"] == "pending":
                self.store.transition_trade_order(
                    order["trade_order_id"], "canceled",
                    note="never sent before restart; replaced by a new exit order",
                )
                continue
            if order["state"] not in TERMINAL_ORDER_STATES:
                self._reconcile_single_order(trade, order)
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)

        live_exits = [
            order for order in self.store.list_trade_orders(trade["trade_id"], kind="stop_loss")
            if order["state"] in UNCONFIRMED_ORDER_STATES
        ]
        if live_exits:
            self._record_error(
                trade,
                "stop-loss exit order is still open/uncertain; not submitting another",
                "exchange",
            )
            return

        cap = max(1, int(getattr(self.cfg, "stop_loss_max_retries", 3) or 3))
        rejected_exits = [
            order for order in self.store.list_trade_orders(trade["trade_id"], kind="stop_loss")
            if order["state"] == "rejected"
        ]
        if len(rejected_exits) >= cap:
            self._record_error(
                trade, f"stop-loss retry cap reached ({cap}); not submitting another sell",
                "exchange",
            )
            return

        # 2. Stop any further entry fills, then cancel conflicting TP sells.
        if not self._cancel_entry_remainder(trade):
            self._record_error(trade, "entry cancel unconfirmed; stop-loss held", "exchange")
            return
        tp_ok = self._cancel_orders_of_kind(trade, "tp1") and self._cancel_orders_of_kind(
            trade, "tp2"
        )
        if not tp_ok:
            self._record_error(trade, "TP cancel unconfirmed; stop-loss held", "exchange")
            return

        # 3. Capture final fills after cancellation.  Without confirmed fill
        #    accounting, selling could oversell (or miss the real position).
        if not self._reconcile_entry_fills(trade):
            self._record_error(trade, "entry fills unconfirmed; stop-loss held", "reconciliation")
            return
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)
        if any(
            order["state"] in UNCONFIRMED_ORDER_STATES
            for order in self.store.list_trade_orders(trade["trade_id"])
        ):
            self._record_error(
                trade, "a conflicting sell order is still unconfirmed; stop-loss held", "exchange"
            )
            return

        remaining = self._remaining_base(trade)
        if remaining <= 0:
            self._finalize_trade(trade)
            return

        qty = self._quantize_qty(trade, remaining)
        if qty <= 0:
            self._record_residual(trade, "stop_loss", remaining, "remainder below lot size")
            self._finalize_trade(trade, residual=remaining, reason="dust below minimum after stop-loss")
            return
        reason = self._validate_sell(trade, qty, None, order_type="MARKET")
        if reason:
            self._record_residual(trade, "stop_loss", qty, f"stop-loss: {reason}")
            self._finalize_trade(trade, residual=qty, reason=f"unsellable after stop-loss: {reason}")
            return
        self._submit_stop_loss_order(trade, qty)

    def _submit_stop_loss_order(self, trade: dict[str, Any], qty: Decimal) -> None:
        engine = self.engine
        if engine is None:
            self._record_error(trade, "no engine available for stop-loss", "runtime")
            return
        order = self._create_trade_order(
            trade, "stop_loss", qty, None, order_type="MARKET"
        )
        self.store.transition_trade_order(
            order["trade_order_id"], "submitting", note="stop-loss submission started"
        )
        try:
            result = engine.submit_stop_loss_market_sell(
                trade, qty, client_order_id=order["client_order_id"]
            )
        except MexcDuplicateOrder as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="duplicate client order id; earlier exit may have landed",
            )
            self._record_error(trade, f"stop-loss duplicate client id: {exc}", "exchange")
            self._reconcile_single_order(trade, order)
            return
        except _UNCERTAIN_ERRORS as exc:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note=f"uncertain stop-loss submission: {exc}"[:500],
            )
            self._record_error(trade, f"stop-loss outcome unknown: {exc}", "exchange")
            self._notify_trade(trade, "SL_SUBMISSION_UNCERTAIN", str(exc)[:500])
            return
        except Exception as exc:  # noqa: BLE001 - definitive rejection before submission
            self.store.transition_trade_order(
                order["trade_order_id"], "rejected", note=str(exc)[:500]
            )
            self._record_error(trade, f"stop-loss rejected: {exc}", "exchange")
            self._notify_trade(trade, "SL_SUBMISSION_FAILED", str(exc)[:500])
            return

        exchange_id = result.get("exchange_order_id") or result.get("order_id")
        executed = _dec(result.get("executed_qty")) or Decimal("0")
        if exchange_id:
            self.store.update_trade_fields(trade["trade_id"], sl_order_id=str(exchange_id))
            self.store.transition_trade_order(
                order["trade_order_id"], "open", note="stop-loss submitted",
                exchange_order_id=str(exchange_id),
                exchange_status=result.get("state") or result.get("status"),
                sold_qty=executed,
            )
        else:
            self.store.transition_trade_order(
                order["trade_order_id"], "unknown_requires_reconciliation",
                note="stop-loss returned no order id; reconcile by client order id",
            )
            self._record_error(trade, "stop-loss submission returned no order id", "exchange")
            return

        self._notify_trade(trade, "SL_SUBMITTED", f"market sell {qty}")
        # Query the fresh exit order once.  A market order is usually terminal
        # immediately; partial/unknown outcomes stay in exit_pending.
        self._reconcile_single_order(trade, order)
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)
        if self._remaining_base(trade) <= 0:
            self._finalize_trade(trade)
            return
        self.store.update_trade_state(
            trade["trade_id"], "exit_pending", note="stop-loss exit awaiting confirmed fill"
        )

    def _remaining_base(self, trade: dict[str, Any]) -> Decimal:
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        sold = (
            self._sold_for_kind(trade, "tp1")
            + self._sold_for_kind(trade, "tp2")
            + self._sold_for_kind(trade, "stop_loss")
        )
        return max(Decimal("0"), acquired - sold)

    def _finalize_trade(
        self,
        trade: dict[str, Any],
        *,
        residual: Decimal = Decimal("0"),
        reason: str = "",
    ) -> None:
        if residual > 0:
            self._record_residual(trade, "stop_loss", residual, reason or "unsellable remainder")
        remaining = self._remaining_base(trade)
        if remaining > 0 and residual <= 0:
            # Never silently close a trade that still owns base quantity.
            self.store.update_trade_state(
                trade["trade_id"], "needs_reconciliation",
                note=f"close refused: {remaining} base quantity remains unsold",
            )
            self._notify_trade(
                trade, "TRADE_CLOSE_REFUSED",
                f"{remaining} base quantity remains; manual reconciliation required",
            )
            return
        self.store.update_trade_state(
            trade["trade_id"], "closed", note="position fully exited", sl_state="completed"
        )
        self._notify_trade(trade, "TRADE_CLOSED", "trade fully closed")

    # ===================================================================
    # Cancellation
    # ===================================================================

    def _cancel_entry_remainder(self, trade: dict[str, Any]) -> bool:
        """Cancel the entry order and confirm a terminal intent state."""
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return True
        engine = self.engine
        reconciler = getattr(engine, "reconcile_intent", None) if engine else None
        for _ in range(4):
            intent = self.store.get_intent(intent_id)
            if intent is None:
                return False
            if intent.get("state") in TERMINAL_INTENT_STATES:
                return True
            if reconciler is None:
                # Legacy/test engine without reconciliation: request the cancel
                # and treat a non-raising call as confirmed.
                if engine is None:
                    return False
                try:
                    engine.cancel_intent_order(intent)
                except Exception as exc:  # noqa: BLE001
                    log.warning("entry cancel failed for %s: %s", trade["trade_id"], exc)
                    return False
                return True
            # Query before cancelling: the order may have filled while the tick
            # was being processed, and an unconditional cancel would discard
            # the fill evidence we need for quantity accounting.
            state = intent.get("state")
            if state in ("open", "partially_filled") and intent.get("exchange_order_id"):
                if engine is None:
                    return False
                try:
                    engine.cancel_intent_order(intent)
                except Exception as exc:  # noqa: BLE001 - reconcile below decides
                    log.warning("entry cancel failed for %s: %s", trade["trade_id"], exc)
            try:
                reconciler(intent_id, notify=False)
            except Exception as exc:  # noqa: BLE001
                log.warning("entry reconcile failed for %s: %s", trade["trade_id"], exc)
                return False
            intent = self.store.get_intent(intent_id)
            if intent is None:
                return False
            if intent.get("state") in TERMINAL_INTENT_STATES:
                return True
        return False

    def _cancel_orders_of_kind(self, trade: dict[str, Any], kind: str) -> bool:
        ok = True
        for order in self.store.list_trade_orders(trade["trade_id"], kind=kind):
            if order["state"] in TERMINAL_ORDER_STATES:
                continue
            if order["state"] == "pending":
                # Created but never submitted; cancelling locally is exact.
                self.store.transition_trade_order(
                    order["trade_order_id"], "canceled", note="not submitted"
                )
                continue
            if not self._cancel_trade_order(trade, order):
                ok = False
        return ok

    def _cancel_trade_order(self, trade: dict[str, Any], order: dict[str, Any]) -> bool:
        if order["state"] in TERMINAL_ORDER_STATES:
            return True
        if order["state"] == "pending":
            self.store.transition_trade_order(
                order["trade_order_id"], "canceled", note="not submitted"
            )
            return True
        if self.engine is None:
            return False
        self.store.transition_trade_order(
            order["trade_order_id"], "cancel_requested", note="cancel requested"
        )
        for _ in range(2):
            try:
                self.engine.cancel_trade_order(order)
            except Exception as exc:  # noqa: BLE001 - reconcile below decides
                log.warning("order cancel failed for %s: %s", order["trade_order_id"], exc)
            fresh = self._query_order(order)
            if fresh is None:
                return False
            mapped = self._map_order_state(fresh.get("state") or fresh.get("status"))
            sold = _dec(fresh.get("executed_qty")) or Decimal("0")
            self._persist_order(order, mapped, fresh, sold)
            if mapped in TERMINAL_ORDER_STATES:
                return True
            if mapped not in ("open", "partially_filled", "submitting"):
                return False
        return False

    # ===================================================================
    # Reconciliation
    # ===================================================================

    def reconcile_all_open(self) -> None:
        """Reconcile every non-terminal trade (called after reconnect/restart)."""
        with self._lock:
            for trade in self.store.list_trades():
                if trade["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                try:
                    self._reconcile_trade(trade["trade_id"])
                except Exception as exc:  # noqa: BLE001
                    log.error("reconcile failed for %s: %s", trade["trade_id"], exc)

    def _reconcile_trade(self, trade_id: str) -> None:
        """Sync fills from the exchange and advance the trade state machine."""
        store = self.store
        trade = store.get_trade(trade_id)
        if trade is None:
            return
        state = trade["trade_state"]
        if state in ("stop_loss_executing", "exit_pending"):
            self._resume_stop_loss(trade)
            return
        if state == "needs_reconciliation":
            # Only refresh facts; never auto-submit a new order from this state.
            self._reconcile_entry_fills(trade)
            self._reconcile_trade_orders(trade)
            return

        self._reconcile_entry_fills(trade)
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)
        if trade is None:
            return
        state = trade["trade_state"]

        if state in ("pending_entry", "entry_active", "position_open"):
            intent = (
                store.get_intent(trade["entry_intent_id"])
                if trade.get("entry_intent_id") else None
            )
            if (
                state == "pending_entry"
                and intent is not None
                and intent.get("state") in ("submitted", "open", "partially_filled", "reconciling")
            ):
                store.update_trade_state(
                    trade_id, "entry_active", note="entry was already submitted before restart"
                )
                trade = self._refresh(trade) or trade
            self._settle_entry_state(trade)
            return

        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if state == "tp1_active":
            if self._tp_done(trade, "tp1"):
                if _dec(trade.get("tp2_price")) is not None:
                    self._enter_tp2(trade)
                else:
                    # TP1 done but no TP2 target: keep the rest protected by SL.
                    store.update_trade_state(
                        trade_id, "tp1_completed",
                        note="TP1 filled; no TP2 price, remaining position kept under SL",
                    )
            elif self._tp_never_started(trade, "tp1") or self._has_pending_order(trade, "tp1"):
                # Restart/recovery: TP1 was reached but no allocation/order was
                # persisted, or an unsent pending order survived the restart.
                self._do_tp_sale(trade, "tp1")
            return
        if state == "tp1_completed":
            if _dec(trade.get("tp2_price")) is not None:
                self._enter_tp2(trade)
            return
        if state == "tp2_active":
            if self._tp_done(trade, "tp2") and self._remaining_base(trade) <= 0:
                self._finalize_trade(trade)
            elif self._tp_done(trade, "tp2") and acquired > 0:
                # Allocated target sold but a residual remains (for example a
                # TP2 allocation that deliberately left a dust balance).
                remaining = self._remaining_base(trade)
                qty = self._quantize_qty(trade, remaining)
                if qty <= 0 and remaining > 0:
                    self._finalize_trade(
                        trade, residual=remaining, reason="dust below minimum after TP2"
                    )
            elif self._tp_never_started(trade, "tp2") or self._has_pending_order(trade, "tp2"):
                # Restart/recovery: TP2 was reached but the TP1 cancellation or
                # allocation step was interrupted, or an unsent pending order
                # survived the restart.
                self._do_tp_sale(trade, "tp2")
            return
        if state in ("oco_submitting", "oco_active", "oco_partial"):
            # Reconcile OCO exit orders against the exchange.  All required legs
            # must confirm before protection is considered active; an uncertain
            # leg is reconciled but never resubmitted.
            if self._ocos_terminal(trade):
                self._finalize_trade(trade)
                return
            required = [
                kind for kind in ("tp1", "tp2")
                if self._target_qty(trade, kind) > 0
            ]
            confirmed = [
                kind for kind in required
                if any(
                    order["state"] in ("filled", "open", "partially_filled")
                    for order in self._oco_orders(trade, kind)
                )
                and not any(
                    order["state"] in (
                        "submitting", "cancel_requested", "unknown_requires_reconciliation",
                    )
                    for order in self._oco_orders(trade, kind)
                )
            ]
            if state == "oco_active" and len(confirmed) == len(required):
                return
            if state == "oco_active" and len(confirmed) < len(required):
                self.store.update_trade_state(
                    trade["trade_id"], "oco_partial",
                    note="an OCO leg is no longer confirmed; protection is partial",
                )
                trade = self._refresh(trade)
            # Reuse pending/confirmed legs and retry only bounded rejected legs.
            self._place_oco_exits(trade)
            return

    def _tp_done(self, trade: dict[str, Any], target: str) -> bool:
        allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
        sold = self._sold_for_kind(trade, target)
        return allocated > 0 and sold >= allocated

    def _reconcile_entry_fills(self, trade: dict[str, Any]) -> bool:
        """Record entry fills; return True only when accounting is trustworthy.

        When the engine exposes ``reconcile_intent`` (the production engine),
        the entry intent is queried read-only *before* any TP/SL quantity is
        computed.  A network/ambiguous result returns False so callers fail
        closed rather than acting on stale local fills.
        """
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return True
        intent = self.store.get_intent(intent_id)
        if intent is None:
            return False
        synced = True
        reconciler = getattr(self.engine, "reconcile_intent", None) if self.engine else None
        if (
            reconciler is not None
            and intent.get("mode") == "live"
            and (intent.get("exchange_order_id") or intent.get("client_order_id"))
        ):
            needs_sync = (
                intent.get("state") not in TERMINAL_INTENT_STATES
                or int(intent.get("fill_sync_required") or 0) == 1
            )
            if needs_sync:
                try:
                    reconciler(intent_id, notify=False)
                except Exception as exc:  # noqa: BLE001
                    log.warning("entry fill sync failed for %s: %s", trade["trade_id"], exc)
                    synced = False
                intent = self.store.get_intent(intent_id) or intent
                if (
                    intent.get("state") in ("unknown_requires_reconciliation", "reconciling")
                    or int(intent.get("fill_sync_required") or 0) == 1
                ):
                    synced = False

        exchange_order_id = intent.get("exchange_order_id")
        fills = self.store.fills_for_order(exchange_order_id) if exchange_order_id else []
        executed_qty = Decimal("0")
        cumulative_quote = Decimal("0")
        weighted = Decimal("0")
        fees = Decimal("0")
        for fill in fills:
            qty = _dec(fill.get("quantity")) or Decimal("0")
            quote = _dec(fill.get("quote_amount")) or Decimal("0")
            price = _dec(fill.get("price")) or Decimal("0")
            fee = _dec(fill.get("fee_amount")) or Decimal("0")
            executed_qty += qty
            cumulative_quote += quote
            weighted += price * qty
            fees += fee
        avg = (weighted / executed_qty) if executed_qty > 0 else None
        self.store.update_trade_fields(
            trade["trade_id"],
            acquired_base_qty=executed_qty,
            acquired_quote_spent=cumulative_quote,
            avg_entry_price=avg,
            fees_paid=fees,
            entry_filled_qty=_dec(intent.get("executed_qty")) or Decimal("0"),
            entry_exchange_status=intent.get("state"),
        )
        # Once fills are confirmed and the entry intent is terminal, settle the
        # entry state and place OCO exits.  An open/partially filled entry is
        # never treated as a finished position.
        if executed_qty > 0 and synced:
            refreshed = self.store.get_trade(trade["trade_id"])
            if refreshed is not None:
                self._settle_entry_state(refreshed)
        return synced

    def _reconcile_trade_orders(self, trade: dict[str, Any], *, advance: bool = True) -> None:
        """Query TP/SL order status and recompute sold totals across ALL orders."""
        if not self.engine:
            return
        for order in self.store.list_trade_orders(trade["trade_id"]):
            if order["state"] in TERMINAL_ORDER_STATES:
                continue
            if order["state"] == "pending":
                continue  # never sent; the submit path will send it
            updated = self._reconcile_single_order(trade, order)
            if (
                updated is not None
                and order["state"] == "cancel_requested"
                and updated["state"] in ("open", "partially_filled")
            ):
                # The exchange still shows the order; request the cancel again.
                try:
                    self.engine.cancel_trade_order(updated)
                except Exception as exc:  # noqa: BLE001
                    log.warning("cancel retry failed for %s: %s", updated["trade_order_id"], exc)
        self._refresh_sold_fields(trade)
        if advance:
            self._advance_trade_after_reconcile(trade)

    def _advance_trade_after_reconcile(self, trade: dict[str, Any]) -> None:
        """Advance TP states after a fill is recorded (idempotent)."""
        trade = self._refresh(trade)
        state = trade["trade_state"]
        if state == "tp1_active" and self._tp_done(trade, "tp1"):
            if _dec(trade.get("tp2_price")) is not None:
                self._enter_tp2(trade)
            else:
                self.store.update_trade_state(
                    trade["trade_id"], "tp1_completed",
                    note="TP1 filled; no TP2 price, remaining position kept under SL",
                )
            return
        if state == "tp2_active" and self._tp_done(trade, "tp2"):
            remaining = self._remaining_base(trade)
            if remaining <= 0:
                self._finalize_trade(trade)
            else:
                qty = self._quantize_qty(trade, remaining)
                if qty <= 0:
                    self._finalize_trade(
                        trade, residual=remaining, reason="dust below minimum after TP2"
                    )
        if state in ("oco_active", "oco_partial") and self._ocos_terminal(trade):
            self._finalize_trade(trade)

    def _reconcile_single_order(
        self, trade: dict[str, Any], order: dict[str, Any]
    ) -> dict[str, Any] | None:
        if order["state"] in TERMINAL_ORDER_STATES or order["state"] == "pending":
            return order
        fresh = self._query_order(order)
        if fresh is None:
            self._record_error(
                trade, f"order query failed for {order['trade_order_id']}", "exchange"
            )
            return None
        raw_state = fresh.get("state") or fresh.get("status")
        mapped = self._map_order_state(raw_state)
        sold = _dec(fresh.get("executed_qty")) or Decimal("0")
        old_sold = _dec(order.get("sold_qty")) or Decimal("0")
        # Fills are monotonic: an exchange query must never reduce them.
        sold = max(sold, old_sold)
        if mapped == "not_found":
            if order["state"] == "pending":
                return order
            mapped = "unknown_requires_reconciliation"
            raw_state = "NOT_FOUND"
        # FILLED with no executed quantity is not trustworthy enough to size a
        # later order from.  Fail closed and require another reconciliation.
        if mapped == "filled":
            expected = _dec(order.get("quantity")) or Decimal("0")
            if expected > 0 and sold <= 0:
                mapped = "unknown_requires_reconciliation"
        self._persist_order(
            order, mapped, fresh, sold,
            raw_state=str(raw_state) if raw_state is not None else None,
        )
        updated = self.store.get_trade_order(order["trade_order_id"])
        return updated

    def _query_order(self, order: dict[str, Any]) -> dict[str, Any] | None:
        if not self.engine:
            return None
        try:
            return self.engine.query_trade_order(order)
        except Exception as exc:  # noqa: BLE001
            log.warning("trade order query failed %s: %s", order["trade_order_id"], exc)
            return None

    def _persist_order(
        self,
        order: dict[str, Any],
        mapped: str,
        fresh: dict[str, Any],
        sold: Decimal,
        raw_state: str | None = None,
    ) -> None:
        exchange_id = fresh.get("exchange_order_id") or fresh.get("order_id")
        # NOT_FOUND never proves a terminal local rejection; persist the
        # constrained "unknown" state instead of an invalid raw value.
        if mapped == "not_found":
            mapped = "unknown_requires_reconciliation"
        self.store.transition_trade_order(
            order["trade_order_id"], mapped, note="reconciled",
            exchange_order_id=str(exchange_id) if exchange_id else None,
            exchange_status=str(raw_state) if raw_state is not None else None,
            sold_qty=sold,
        )

    def _refresh_sold_fields(self, trade: dict[str, Any]) -> None:
        tp1 = self._sold_for_kind(trade, "tp1")
        tp2 = self._sold_for_kind(trade, "tp2")
        sl = self._sold_for_kind(trade, "stop_loss")
        self.store.update_trade_fields(
            trade["trade_id"], tp1_sold_qty=tp1, tp2_sold_qty=tp2, sl_executed_qty=sl
        )

    def _sold_for_kind(self, trade: dict[str, Any], kind: str) -> Decimal:
        total = Decimal("0")
        for order in self.store.list_trade_orders(trade["trade_id"], kind=kind):
            total += _dec(order.get("sold_qty")) or Decimal("0")
        return total

    def _has_unconfirmed_order(self, trade: dict[str, Any], kind: str) -> bool:
        return any(
            order["state"] in UNCONFIRMED_ORDER_STATES
            for order in self.store.list_trade_orders(trade["trade_id"], kind=kind)
        )

    def _has_live_order(self, trade: dict[str, Any], kind: str) -> bool:
        """True for an order that may exist at the exchange.

        ``pending`` means "created locally, never sent"; it must not block the
        submit path because that path reuses/rebases the row.
        """
        return any(
            order["state"] in UNCONFIRMED_ORDER_STATES and order["state"] != "pending"
            for order in self.store.list_trade_orders(trade["trade_id"], kind=kind)
        )

    @staticmethod
    def _map_order_state(state: str | None) -> str:
        if not state:
            return "unknown_requires_reconciliation"
        s = str(state).upper()
        if s in ("NEW", "SUBMITTING", "INACTIVE"):
            return "open"
        if s in ("PARTIALLY_FILLED", "PARTIAL_FILLED"):
            return "partially_filled"
        if s == "FILLED":
            return "filled"
        if s in ("CANCELED", "CANCELLED", "PARTIALLY_CANCELED", "PARTIALLY_CANCELLED", "EXPIRED"):
            return "canceled"
        if s == "REJECTED":
            return "rejected"
        if s in ("NOT_FOUND", "UNKNOWN"):
            return "not_found"
        return "unknown_requires_reconciliation"

    # ===================================================================
    # Helpers
    # ===================================================================

    @staticmethod
    def _client_order_id(trade_id: str, kind: str, attempt: int) -> str:
        """Stable, exchange-safe client id (max 32 chars, ASCII)."""
        digest = hashlib.sha256(
            f"{trade_id}:{kind}:{attempt}".encode("ascii")
        ).hexdigest()[:16]
        prefix = {"tp1": "t1", "tp2": "t2", "stop_loss": "sl"}[kind]
        return f"{prefix}-{digest}"

    def _refresh(self, trade: dict[str, Any]) -> dict[str, Any]:
        refreshed = self.store.get_trade(trade["trade_id"])
        return refreshed if refreshed is not None else trade

    def _symbol_meta_symbol(self, symbol: str) -> Any | None:
        registry = getattr(self.client, "symbols", None) if self.client else None
        if registry is None:
            return None
        try:
            return registry.get(symbol)
        except Exception as exc:  # noqa: BLE001
            log.warning("symbol metadata lookup failed for %s: %s", symbol, exc)
            return None

    def _symbol_meta(self, trade: dict[str, Any]) -> Any | None:
        return self._symbol_meta_symbol(trade["symbol"])

    def _quantize_qty(self, trade: dict[str, Any], qty: Decimal) -> Decimal:
        if qty <= 0:
            return Decimal("0")
        meta = self._symbol_meta(trade)
        if meta is not None:
            try:
                return meta.quantize_quantity(qty)
            except Exception:  # noqa: BLE001
                pass
        return qty.quantize(Decimal("0.00000001"), rounding="ROUND_DOWN")

    def _quantize_price(self, trade: dict[str, Any], price: Decimal) -> Decimal:
        meta = self._symbol_meta(trade)
        if meta is not None:
            try:
                return meta.quantize_price(price)
            except Exception:  # noqa: BLE001
                pass
        return price.quantize(Decimal("0.00000001"), rounding="ROUND_DOWN")

    def _validate_sell(
        self,
        trade: dict[str, Any],
        qty: Decimal,
        price: Decimal | None,
        *,
        order_type: str,
    ) -> str | None:
        meta = self._symbol_meta(trade)
        if meta is None:
            return None
        try:
            meta.validate_order(
                side="SELL",
                order_type=order_type,
                quantity=qty,
                quote_amount=(qty * price) if price is not None else None,
                price=price,
            )
        except Exception as exc:  # noqa: BLE001 - local validation failure
            return str(exc)[:300]
        return None

    def _record_residual(
        self, trade: dict[str, Any], target: str, qty: Decimal, reason: str
    ) -> None:
        if qty <= 0:
            return
        self.store.update_trade_fields(
            trade["trade_id"],
            unsold_residual_qty=qty,
            unsold_residual_reason=f"{target}: {reason}"[:500],
        )

    def _record_error(self, trade: dict[str, Any], message: str, category: str) -> None:
        retry = int(trade.get("retry_count") or 0) + 1
        self.store.update_trade_fields(
            trade["trade_id"], retry_count=retry,
            last_error=message[:500], last_error_category=category,
        )
        self._notify_trade(trade, "TRADE_ERROR", message[:500])

    # ===================================================================
    # Notifications
    # ===================================================================

    def _notify_trade(
        self, trade: dict[str, Any] | None, event_key: str, text: str, **extra: Any
    ) -> None:
        """Notify via the notifier when a trade transitions. Never blocks trading."""
        if self.notifier is None:
            return
        try:
            self.notifier.notify_trade(trade, event_key, text, **extra)
        except Exception as exc:  # noqa: BLE001 - notifications never block trading
            log.warning("notify_trade failed (%s); trading continues", exc)

    def _notify_intent(self, intent: dict[str, Any], event_key: str, text: str) -> None:
        if self.notifier is None:
            return
        try:
            self.notifier.notify_intent(intent, event_key, text)
        except Exception as exc:  # noqa: BLE001 - notifications never block trading
            log.warning("notify_intent failed (%s)", exc)

    def _notify_stale_config(self, symbol: str, reason: str) -> None:
        self._notify_trade(None, "STALE_CONFIG", f"{symbol}: {reason}")

    def _ts(self) -> str:
        return self._now().isoformat()
