"""Trade lifecycle management: conditional entry, TP1/TP2 and stop-loss.

The :class:`TradeManager` is the single owner of a trade's state machine.  It is
synchronous and serialised by an internal lock so concurrent queue deliveries
and background reconciliation cannot race each other.

Entry modes
-----------
* ``direct`` — the current market is already at/above the signal entry price:
  submit a Limit Buy at the entry price immediately, subject to risk/balance
  controls.
* ``conditional`` — the current market is below the entry price: create a
  durable pending entry and wait for an upward crossing of
  ``entry_price - offset * tick_size`` before submitting the Limit Buy.  The
  default offset is 3 ticks and is configurable; a missing offset resolves to 3
  rather than direct entry.  The Limit Buy always respects the entry price as a
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
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ..mexc.errors import (
    MexcAuthError,
    MexcConfigError,
    MexcDuplicateOrder,
    MexcNetworkError,
    MexcPermissionError,
    MexcPrecisionError,
    MexcRateLimitError,
    MexcServerError,
    MexcSymbolError,
    MexcUnexpectedResponse,
)
from .parser import ParsedSignal
from .store import TERMINAL_INTENT_STATES, TradingError
from .trade_store import TradeStore

log = logging.getLogger("ingest.trading.trade_manager")


@dataclass
class TradeDecision:
    """Outcome of the trade manager's review of a freshly-planned intent."""

    trade: dict[str, Any] | None
    deferred: bool  # True => do not execute the entry yet (conditional entry)
    rejected: bool = False  # True => fail closed; do not execute at all
    reason: str | None = None


# Terminal trade states: genuinely closed / successfully completed — no further
# action required.  ``needs_reconciliation`` is explicitly NOT terminal:
# it is a fail-closed, reconcilable state that must remain eligible for
# safe read-only reconciliation during startup recovery and periodic
# reconciliation (see _reconcile_trade).  Historical audit evidence
# (docs/TRADING_RULES_COMPLIANCE_AUDIT.md @71a952b) described stop-loss
# Market Sell as current — current implementation (1a27058+) has no automatic
# Market Sell fallback; SL protection is solely via verified native OCO legs.
TERMINAL_TRADE_STATES = (
    "closed",
    "tp2_completed",
)

# A position with no verified exit coverage.  Unlike ``needs_reconciliation``
# (manual review), this state is retried by reconciliation so the bot can
# restore protection, while the global new-BUY block remains in force.
UNPROTECTED_CRITICAL = "unprotected_critical"

# Reconciliation-required state — not terminal, not successfully completed.
# Trades in this state must:
#  1. remain eligible for safe read-only reconciliation (on_tick,
#     recover_on_startup, reconcile_all_open),
#  2. never be auto-submitted (no BUY/SELL/OCO/replacement),
#  3. never auto-transition back to ordinary active trading on a bare
#     reconciliation attempt,
#  4. retain needs_reconciliation with reason/notify if still unknown,
#  5. transition to an explicit auditable state (e.g. closed) only when
#     exchange outcome is conclusively established.
# Owner-agreed OCO retry policy (F-02): one initial attempt plus eight retries.
OCO_RETRY_DELAYS_S: tuple[int, ...] = (5, 10, 15, 30, 30, 30, 30, 30)
MAX_OCO_ATTEMPTS = 1 + len(OCO_RETRY_DELAYS_S)

# Deterministic exchange validations that can never succeed by resubmitting
# the same payload.  They are terminal for the leg (no retry, no replacement).
_NON_RETRYABLE_TOKENS = (
    "price needs to be",
    "limit price",
    "stop price",
    "trigger price",
    "invalid price",
    "invalid quantity",
    "quantity",
    "minimum",
    "precision",
    "not supported",
    "symbol",
    "permission",
)

RECONCILIATION_REQUIRED_STATE = "needs_reconciliation"
RECONCILIABLE_TRADE_STATES = (RECONCILIATION_REQUIRED_STATE,)

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


def oco_retry_delay_s(attempt: int) -> int | None:
    """Delay before the next OCO attempt, or None when the budget is spent.

    ``attempt`` is zero-based: 0 = the initial submission.
    """

    if attempt < 0:
        return OCO_RETRY_DELAYS_S[0]
    if attempt >= len(OCO_RETRY_DELAYS_S):
        return None
    return OCO_RETRY_DELAYS_S[attempt]


def _dec_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _is_retryable_oco_error(exc: BaseException) -> bool:
    """False for deterministic rejections that must never be retried."""

    if isinstance(exc, (MexcPrecisionError, MexcSymbolError, MexcPermissionError, MexcConfigError, MexcAuthError)):
        return False
    text = str(exc).lower()
    return not any(token in text for token in _NON_RETRYABLE_TOKENS)


def _dec(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _optional_decimal(value: Any) -> Decimal | None:
    """Return a Decimal or None, without converting empty/invalid to zero."""
    if value in (None, ""):
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
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
            existing = self.store.get_trade_by_signal(intent["signal_id"])
            if existing is not None:
                if existing.get("entry_mode") == "conditional" and existing.get("trade_state") == "pending_entry":
                    return TradeDecision(
                        trade=existing,
                        deferred=True,
                        reason="existing conditional entry still waiting for trigger",
                    )
                return TradeDecision(trade=existing, deferred=False, reason="trade already exists")
            # Conditional entry requires a verified market price to decide whether to
            # wait for the upward trigger.  Missing/unverified prices fail closed.
            if self.cfg.conditional_entry_enabled and reference_price is None:
                self._reject_trade_intent(intent, "conditional entry enabled but no market price to evaluate")
                return TradeDecision(
                    trade=None,
                    deferred=False,
                    rejected=True,
                    reason="no reference price for conditional entry",
                )
            trade = self._create_trade(intent, parsed, reference_price)
            if trade is None:
                return TradeDecision(
                    trade=None,
                    deferred=False,
                    rejected=True,
                    reason="trade creation rejected",
                )
            if trade["entry_mode"] == "conditional":
                return TradeDecision(
                    trade=trade,
                    deferred=True,
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
            # Establish the fail-closed block from durable state before any
            # reconciliation call can fail, then refresh it again afterwards.
            self._refresh_buy_block()
            try:
                for trade in self.store.list_trades():
                    if trade["trade_state"] in TERMINAL_TRADE_STATES:
                        continue
                    try:
                        self._reconcile_trade(trade["trade_id"])
                    except Exception as exc:  # noqa: BLE001 - never block recovery
                        log.error("recovery reconcile failed for %s: %s", trade["trade_id"], exc)
                        self._record_error(trade, f"recovery failed: {exc}", "reconciliation")
            finally:
                self._refresh_buy_block()

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

        # A complete, ordered exit ladder and a working OCO path are still
        # required before a BUY.  Historical global new-BUY blocks are not an
        # entry gate; unrelated symbols remain independent.
        tick_size = getattr(symbol_meta, "price_step", None) if symbol_meta else None
        offset = self.cfg.effective_entry_trigger_offset_ticks
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 1:
            self._reject_trade_intent(
                intent,
                "entry trigger offset is missing or invalid; refusing direct entry",
            )
            return None
        trigger_price = None
        entry_mode = "direct"
        current = reference_price
        # Conditional entry is the canonical strategy.  When the market is
        # already at/above Entry, a Limit Buy at Entry is submitted directly;
        # a missing/invalid offset can never select that path.
        if (
            tick_size is not None
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

        problem = self._pre_buy_exit_problem(entry_price, tp1_price, tp2_price, sl_price, symbol_meta)
        if problem is not None:
            self._reject_trade_intent(intent, problem)
            return None

        client_order_id = intent.get("client_order_id")
        trade_id = f"trade-{client_order_id}"
        quote_amount = _dec(intent.get("quote_amount")) or Decimal("0")

        try:
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
        except Exception:
            raise

    def _compute_trigger(self, entry_price: Decimal, ticks: int, tick_size: Decimal) -> Decimal | None:
        """Trigger = entry_price - ticks * tick_size, floored to the tick grid."""

        raw = entry_price - (Decimal(ticks) * tick_size)
        try:
            steps = (raw / tick_size).to_integral_value(rounding="ROUND_DOWN")
            return steps * tick_size
        except (InvalidOperation, ZeroDivisionError):
            return None

    # ===================================================================
    # Exit-protection gate (before BUY and across restarts)
    # ===================================================================

    def _pre_buy_exit_problem(
        self,
        entry_price: Decimal,
        tp1_price: Decimal | None,
        tp2_price: Decimal | None,
        sl_price: Decimal | None,
        symbol_meta: Any | None,
    ) -> str | None:
        """Return why a live BUY must not be submitted, or None when safe.

        The live strategy is a two-leg native OCO exit.  A BUY without a
        complete, ordered ladder or without a working OCO submission path can
        fill and then remain unprotected, so it is rejected *before* entry.
        """

        if self.engine is None:
            return "no execution engine available for OCO exits"
        if not callable(getattr(self.engine, "submit_oco_sell", None)):
            return "execution engine cannot submit OCO exits"
        if symbol_meta is None:
            return "symbol metadata unavailable for exit-ladder validation"
        if sl_price is None or sl_price <= 0:
            return "signal has no stop-loss; live BUY requires verified exit protection"
        if tp1_price is None or tp1_price <= 0:
            return "signal has no TP1; native OCO exits require TP1 and TP2"
        if tp2_price is None or tp2_price <= 0:
            return "signal has no TP2; native OCO exits require TP1 and TP2"
        if not (sl_price < entry_price < tp1_price <= tp2_price):
            return (
                f"invalid exit ladder: stop-loss {sl_price} < entry {entry_price} "
                f"< TP1 {tp1_price} <= TP2 {tp2_price} is required"
            )
        return None

    def _exit_ladder_problem(self, trade: dict[str, Any]) -> str | None:
        """Return an invalid-ladder reason for an open position, or None."""

        sl_price = _dec(trade.get("sl_price"))
        tp1_price = _dec(trade.get("tp1_price"))
        tp2_price = _dec(trade.get("tp2_price"))
        entry_price = _dec(trade.get("entry_price")) or _dec(trade.get("avg_entry_price"))
        if sl_price is None or sl_price <= 0:
            return "no stop-loss price; OCO exit requires a stop-loss"
        if tp1_price is None or tp1_price <= 0:
            return "no TP1 price; OCO exit requires TP1"
        if tp2_price is None or tp2_price <= 0:
            return "no TP2 price; OCO exit requires TP2"
        if entry_price is None or entry_price <= 0:
            return "entry price is unknown; cannot order the exit ladder"
        if not (sl_price < entry_price < tp1_price <= tp2_price):
            return (
                f"invalid exit ladder: stop-loss {sl_price} < entry {entry_price} < "
                f"TP1 {tp1_price} <= TP2 {tp2_price} is required"
            )
        return None

    def _buy_block_reason(self) -> str | None:
        getter = getattr(self.store, "buy_block_reason", None)
        if getter is None:
            return None
        try:
            return getter()
        except Exception as exc:  # noqa: BLE001 - fail closed, never crash planning
            log.error("could not read the new-BUY block; refusing new BUYs: %s", exc)
            return "new-BUY safety state is unreadable"

    def _open_trades_for_block(self) -> list[dict[str, Any]] | None:
        """List trades that still need protection, or None when unreadable."""

        lister = getattr(self.store, "list_non_terminal_trades", None)
        if lister is not None:
            try:
                return list(lister())
            except Exception as exc:  # noqa: BLE001
                log.error("could not list open trades for the new-BUY block: %s", exc)
                return None
        try:
            return list(self.store.list_trades(limit=1000))
        except Exception as exc:  # noqa: BLE001
            log.error("could not list trades for the new-BUY block: %s", exc)
            return None

    def _verified_exit_coverage(self, trade: dict[str, Any]) -> Decimal:
        """Working, exchange-confirmed sell quantity covering the remainder."""

        remaining = self._remaining_base(trade)
        if remaining is None or remaining <= 0:
            return Decimal("0")
        if self._exchange_protection_active(trade):
            return remaining
        covered = Decimal("0")
        for kind in ("tp1", "tp2", "stop_loss"):
            for order in self._list_trade_orders(trade["trade_id"], kind=kind):
                if order.get("state") not in ("open", "partially_filled"):
                    continue
                qty = _dec(order.get("quantity")) or Decimal("0")
                sold = _dec(order.get("sold_qty")) or Decimal("0")
                covered += max(Decimal("0"), qty - sold)
        return covered

    def _is_unprotected_critical(self, trade: dict[str, Any]) -> bool:
        """True when this trade owns base quantity without verified exit cover."""

        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if acquired <= 0:
            return False
        remaining = self._remaining_base(trade)
        if remaining is None or remaining <= 0:
            return False
        return self._verified_exit_coverage(trade) < remaining

    def _refresh_buy_block(self) -> None:
        """Persist the global new-BUY block from verified exit coverage.

        The block is never cleared by time.  It clears only when every open
        bot-owned position is fully sold or has exchange-confirmed working
        coverage.  This is deliberately independent of the trade's exact state
        so a restart cannot hide an unprotected position behind a stale label.
        """

        try:
            trades = self._open_trades_for_block()
        except Exception as exc:  # noqa: BLE001 - never crash a tick on the guard
            log.error("new-BUY block evaluation failed: %s", exc)
            return
        if trades is None:
            # A failed read must never clear an existing block.
            return

        unprotected = [trade for trade in trades if self._is_unprotected_critical(trade)]
        if unprotected:
            labels = ", ".join(f"{trade.get('symbol')}:{trade.get('trade_id')}" for trade in unprotected[:5])
            if len(unprotected) > 5:
                labels += f" (+{len(unprotected) - 5} more)"
            reason: str | None = f"unprotected position(s): {labels}"
        else:
            reason = None

        getter = getattr(self.store, "buy_block_reason", None)
        previous: str | None = None
        if getter is not None:
            try:
                previous = getter()
            except Exception:  # noqa: BLE001
                previous = None

        setter = getattr(self.store, "set_buy_block", None)
        if setter is not None:
            try:
                setter(reason)
            except Exception as exc:  # noqa: BLE001 - keep the in-memory guard usable
                log.error("could not persist the new-BUY block: %s", exc)

        if reason and reason != previous:
            self.store.audit(
                "buy-block",
                "risk",
                "new_buys_blocked",
                reason,
                event_type="trading.buy_block",
                outcome="blocked",
                dedupe_key=f"buy-block:{reason}",
            )
            # The reason is part of the event key so a *different* block (a new
            # unprotected position) is reported instead of being swallowed by
            # the outbox uniqueness constraint.
            self._notify_trade(
                None,
                f"BUY_BLOCK_ACTIVE_{hashlib.sha1(reason.encode('utf-8')).hexdigest()[:8]}",
                f"new BUYs blocked (enforced by the submission gate): {reason}",
            )
        elif not reason and previous:
            self.store.audit(
                "buy-block",
                "risk",
                "new_buys_unblocked",
                "every open position has verified exit protection",
                event_type="trading.buy_block",
                outcome="unblocked",
                dedupe_key="buy-block:cleared",
            )
            self._notify_trade(
                None,
                "BUY_BLOCK_CLEARED",
                "new BUYs allowed: every open position has verified exit protection",
            )

    def _reject_trade_intent(self, intent: dict[str, Any], reason: str) -> None:
        self.store.transition_intent(
            intent["intent_id"],
            "rejected",
            note=f"trade setup: {reason}",
            reject_reason=reason,
            category="risk",
        )
        self.store.update_signal(intent["signal_id"], "rejected", reason)
        self._notify_intent(intent, "REJECTED_TRADE", reason)

    # ===================================================================
    # Price monitoring (called by the MarketMonitor feed callback)
    # ===================================================================

    def _evaluate_trade_tick(self, trade: dict[str, Any], price: Decimal, received_at: float) -> None:
        """Handle one price tick.

        Only two things are price-driven now:

        * a conditional entry waiting for its trigger;
        * an unprotected position crossing its stop-loss (alert only, never an
          order).

        Exits are native OCO orders held by MEXC, so OCO/fill/order state is
        reconciled exclusively by the periodic worker.  The historical design
        issued a REST order query per leg on every tick, which produced ~57k
        checks per leg for a single stuck trade.
        """
        if self._is_stale(received_at):
            return
        state = trade["trade_state"]
        try:
            if state == "pending_entry":
                self._check_entry_trigger(trade, price)
            elif state == UNPROTECTED_CRITICAL:
                self._check_unprotected_price(trade, price)
        except Exception as exc:  # noqa: BLE001 - never let one tick kill the monitor
            log.error("trade tick evaluation failed for %s: %s", trade["trade_id"], exc)

    def _check_unprotected_price(self, trade: dict[str, Any], price: Decimal) -> None:
        """Alert (never trade) when an unprotected position crosses its SL."""

        sl = _dec(trade.get("sl_price"))
        if sl is not None and price <= sl:
            self._handle_unprotected_sl(trade, price, context="tick: position unprotected at SL")

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

    def _handle_unprotected_sl(self, trade: dict[str, Any], trigger_price: Decimal, context: str) -> None:
        """Owner-confirmed §7: native OCO SL legs are the *only* authorized SL.

        When price crosses SL but OCO protection is missing/uncertain/rejected,
        do NOT submit a Market Sell.  Persist critical unprotected state,
        notify owner+developer with exact signal/symbol/qty/OCO state and
        last reconciliation evidence, apply narrowest safety restriction
        (per-symbol needs_reconciliation, not global), and continue
        read-only reconciliation.
        """
        sellable = self._sellable_quantity(trade)
        # Gather OCO state for the notification
        oco1_orders = self._oco_orders(trade, "tp1")
        oco2_orders = self._oco_orders(trade, "tp2")
        oco1_state = ",".join(o["state"] for o in oco1_orders) if oco1_orders else "missing"
        oco2_state = ",".join(o["state"] for o in oco2_orders) if oco2_orders else "missing"
        last_error = trade.get("last_error") or "no prior error"
        trade_state = trade.get("trade_state")
        detail = (
            f"CRITICAL UNPROTECTED: price {trigger_price} crossed SL {trade.get('sl_price')} "
            f"for {trade.get('symbol')} signal {trade.get('signal_id')} "
            f"trade {trade.get('trade_id')} qty sellable={sellable} "
            f"OCO1=[{oco1_state}] OCO2=[{oco2_state}] trade_state={trade_state} "
            f"context={context} last_error={last_error}; "
            "native OCO protection missing/uncertain — NO Market Sell submitted; "
            "manual intervention required (narrow per-symbol restriction)"
        )
        # Persist critical state (fail-closed, not terminal)
        self.store.update_trade_state(
            trade["trade_id"],
            "needs_reconciliation",
            note=detail[:2000],
        )
        self.store.update_trade_fields(
            trade["trade_id"],
            last_error=detail[:500],
            last_error_category="protection",
        )
        self.store.audit(trade["trade_id"], "risk", "sl_unprotected_critical", detail[:2000])
        self._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail)
        # Narrowest restriction: per-symbol (trade already in needs_reconciliation
        # is treated as conflicting for that symbol via CONFLICTING_TRADE_STATES)

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
        # Reuse the engine's full live gate: reservation + balance preflight + order.
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
                "submitted",
                "open",
                "partially_filled",
                "reconciling",
            ):
                self.store.update_trade_state(trade["trade_id"], "entry_active", note="entry order is working")
            if acquired > 0 and intent_state in ("open", "partially_filled"):
                # Partial fill: protect what was actually bought right away.
                # The unfilled remainder keeps working and is cancelled later if
                # the acquired position is exited (F-05).
                self.store.update_trade_state(
                    trade["trade_id"], "oco_submitting", note="partial fill: protecting acquired quantity"
                )
                self._place_oco_exits(self._refresh(trade) or trade)
            elif intent_state == "planned" and state == "entry_active":
                # Never submit autonomously from here.  A retried queue item can
                # atomically resume a planned intent; an activation that has no
                # exchange order is escalated instead.
                self.store.update_trade_state(
                    trade["trade_id"],
                    "needs_reconciliation",
                    note="entry activated but no submission was claimed; operator review required",
                )
                self._notify_trade(
                    trade,
                    "ENTRY_NOT_SUBMITTED",
                    "entry activation did not record a submission; manual reconciliation required",
                )
            return
        if acquired > 0:
            if state in ("pending_entry", "entry_active"):
                self.store.update_trade_state(trade["trade_id"], "position_open", note="entry fills confirmed")
            # Place the two native OCO exit orders once the position is open.
            trade = self._refresh(trade)
            self._place_oco_exits(trade)
            return
        if intent_state in ("canceled", "canceled_local", "rejected", "failed"):
            self.store.update_trade_state(
                trade["trade_id"],
                "closed",
                note=f"entry intent {intent_state} with no fills; no position opened",
                sl_state="dormant",
            )
            refreshed = self.store.get_trade(trade["trade_id"])
            self._notify_trade(
                refreshed or trade,
                "ENTRY_NOT_FILLED",
                f"entry {intent_state}; no position opened, no exit orders placed",
            )
        elif intent_state == "filled":
            # Filled according to the exchange but no fill records: never treat
            # this as a zero position.  Fail closed for manual reconciliation.
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note="entry reports filled but no fill records were persisted",
            )
            self._notify_trade(
                trade,
                "ENTRY_FILLS_MISSING",
                "entry filled but fills could not be persisted; manual reconciliation required",
            )

    # ===================================================================
    # Take-profit handling
    # ===================================================================

    def _enter_tp1(self, trade: dict[str, Any]) -> None:
        """Disabled: standalone take-profit orders are not part of the product."""

        self._disable_legacy_exit(trade, "tp1")
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
        """Disabled: standalone take-profit orders are not part of the product."""

        self._disable_legacy_exit(trade, "tp2")
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

    def _transition_state(self, trade: dict[str, Any], new_state: str, allowed_from: tuple[str, ...]) -> bool:
        """Atomically move trade state. Returns True if transitioned."""
        now = self._ts()
        with self.store.db.transaction() as conn:
            row = conn.execute("SELECT trade_state FROM trades WHERE trade_id = ?", (trade["trade_id"],)).fetchone()
            if row is None:
                return False
            state = row[0]
            if state in (
                "tp2_completed",
                "closed",
                "needs_reconciliation",
                "stop_loss_executing",
                "exit_pending",
                UNPROTECTED_CRITICAL,
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

    def _disable_legacy_exit(self, trade: dict[str, Any], target: str) -> None:
        """Fail closed instead of submitting a standalone take-profit order."""

        detail = (
            f"standalone {target} exit requested but disabled (OCO-only policy); "
            "no order was submitted — manual reconciliation required"
        )
        self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
        self._notify_trade(trade, "LEGACY_EXIT_DISABLED", detail)

    def _do_tp_sale(self, trade: dict[str, Any], target: str) -> None:
        """Disabled: standalone take-profit orders are not part of the product."""

        self._disable_legacy_exit(trade, target)
        return

    def _do_tp_sale_unused(self, trade: dict[str, Any], target: str) -> None:
        """Retained historical implementation (unreachable); see _do_tp_sale."""
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
            order
            for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
            if order["state"] == "rejected"
        ]
        if len(rejected) >= cap:
            self._record_error(
                trade,
                f"{target} retry cap reached ({cap}); not submitting another sell",
                "exchange",
            )
            return

        pending = [
            order
            for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
            if order["state"] == "pending"
        ]
        if pending:
            order: dict[str, Any] | None = pending[0]
            assert order is not None
            price = _dec(order.get("price")) or self._tp_price(trade, target)
            requested = _dec(order.get("quantity")) or Decimal("0")
            allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
            target_capacity = (
                max(Decimal("0"), allocated - self._sold_for_kind(trade, target)) if allocated > 0 else requested
            )
            # A late fill can arrive after this row was created.  It was never
            # sent, so base the submitted quantity on current fills rather than
            # the stale row.
            remaining = self._remaining_base(trade)
            if remaining is None:
                detail = f"{target} sellable unknown — cannot rebase pending quantity; needs_reconciliation"
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self.store.audit(trade["trade_id"], "risk", "oco_invariant_unknown", detail[:2000])
                return
            qty = min(requested, target_capacity, remaining)
            qty = self._quantize_qty(trade, qty)
            if qty <= 0:
                self.store.transition_trade_order(
                    order["trade_order_id"],
                    "canceled",
                    note="not sent; no remaining quantity",
                )
                remaining2 = self._remaining_base(trade)
                if remaining2 is None:
                    detail = f"{target} sellable unknown after rebase; needs_reconciliation"
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                    self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                    return
                if remaining2 <= 0:
                    self._finalize_trade(trade)
                else:
                    self._record_residual(
                        trade,
                        target,
                        requested,
                        f"{target} remainder below lot size after rebase",
                    )
                    self._advance_tp_if_empty(trade, target)
                return
            if qty < requested:
                # Never sent: replace it with a safely reduced quantity and
                # shrink the allocation so completion can be observed.
                self.store.transition_trade_order(
                    order["trade_order_id"],
                    "canceled",
                    note=f"not sent; quantity rebased {requested}->{qty}",
                )
                order = None
                new_allocated = self._sold_for_kind(trade, target) + qty
                if allocated > new_allocated:
                    if target == "tp1":
                        self.store.update_trade_fields(trade["trade_id"], tp1_allocated_qty=new_allocated)
                    else:
                        self.store.update_trade_fields(trade["trade_id"], tp2_allocated_qty=new_allocated)
                    trade = self._refresh(trade)
        else:
            order = None
            allocated = _dec(trade.get(f"{target}_allocated_qty")) or Decimal("0")
            if allocated <= 0:
                if target == "tp1":
                    allocated = acquired * (_dec(trade.get("tp1_allocation_pct")) or Decimal("100")) / Decimal("100")
                else:
                    base = max(
                        Decimal("0"),
                        acquired - self._sold_for_kind(trade, "tp1"),
                    )
                    allocated = base * (_dec(trade.get("tp2_allocation_pct")) or Decimal("100")) / Decimal("100")
                allocated = self._quantize_qty(trade, allocated)
                if allocated <= 0:
                    self._record_residual(trade, target, acquired, f"{target} allocation below lot size")
                    self._advance_tp_if_empty(trade, target)
                    return
                if target == "tp1":
                    self.store.update_trade_fields(trade["trade_id"], tp1_allocated_qty=allocated)
                else:
                    self.store.update_trade_fields(trade["trade_id"], tp2_allocated_qty=allocated)
                trade = self._refresh(trade)
            qty = max(Decimal("0"), allocated - self._sold_for_kind(trade, target))
            avail = self._compute_available(trade)
            if avail is None:
                detail = f"{target} sellable unknown — cannot compute available quantity; needs_reconciliation"
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self.store.audit(trade["trade_id"], "risk", "oco_invariant_unknown", detail[:2000])
                return
            qty = min(qty, avail)
            qty = self._quantize_qty(trade, qty)
            price = self._tp_price(trade, target)

        if price is None or price <= 0:
            self._record_error(trade, f"{target} has no valid price; target skipped", "config")
            self._advance_tp_if_empty(trade, target)
            return
        price = self._quantize_price(trade, price)

        if qty <= 0:
            self._record_residual(
                trade,
                target,
                self._target_remaining(trade, target),
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
            order["state"] == "pending" for order in self.store.list_trade_orders(trade["trade_id"], kind=target)
        )

    def _compute_available(self, trade: dict[str, Any]) -> Decimal | None:
        """Bot-owned base quantity available to sell right now (fee-aware), or None if unknown."""
        base_sellable = self._sellable_quantity(trade)
        if base_sellable is None:
            return None
        outstanding = Decimal("0")
        for order in self.store.list_trade_orders(trade["trade_id"]):
            if order["state"] in UNCONFIRMED_ORDER_STATES:
                qty = _dec(order.get("quantity")) or Decimal("0")
                filled = _dec(order.get("sold_qty")) or Decimal("0")
                outstanding += max(Decimal("0"), qty - filled)
        return max(Decimal("0"), base_sellable - outstanding)

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
            attempt=attempt,
            oco_group_id=self._oco_group_id(trade),
            leg_role=kind,
        )

    def _submit_tp_order(
        self, trade: dict[str, Any], target: str, order: dict[str, Any], qty: Decimal, price: Decimal
    ) -> None:
        """Refuse standalone TP submission; exits are native OCO pairs only."""

        raise TradingError(
            "standalone take-profit orders are disabled; exits must be native OCO pairs"
        )
        engine = self.engine  # noqa: F841 - unreachable, kept for historical context
        if engine is None:
            self._record_error(trade, "no engine available for TP sale", "runtime")
            return
        self.store.transition_trade_order(order["trade_order_id"], "submitting", note="submission started")
        try:
            result = engine.submit_sell_limit(
                trade,
                order["client_order_id"],
                price,
                qty,
                kind=target,
                trade_order_id=order["trade_order_id"],
            )
        except MexcDuplicateOrder as exc:
            # The earlier attempt may have landed.  Never submit a second
            # order; resolve this one by client order id.
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note="duplicate client order id; earlier attempt may have landed",
            )
            self._record_error(trade, f"TP duplicate client id: {exc}", "exchange")
            self._reconcile_trade_orders(trade)
            return
        except _UNCERTAIN_ERRORS as exc:
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note=f"uncertain submission: {exc}"[:500],
            )
            self._record_error(trade, f"TP submission outcome unknown: {exc}", "exchange")
            return
        except Exception as exc:  # noqa: BLE001 - definitive rejection before submission
            self.store.transition_trade_order(order["trade_order_id"], "rejected", note=str(exc)[:500])
            self._record_error(trade, f"TP rejected: {exc}", "exchange")
            self._notify_trade(trade, "TP_REJECTED", str(exc)[:500])
            return

        exchange_id = result.get("exchange_order_id") or result.get("order_id")
        executed = _dec(result.get("executed_qty")) or Decimal("0")
        if not exchange_id:
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note="no exchange order id returned; reconcile by client order id",
            )
            self._record_error(trade, "TP submission returned no order id", "exchange")
            return
        self.store.transition_trade_order(
            order["trade_order_id"],
            "open",
            note="submitted",
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
        """Trade orders representing the OCO leg for ``kind`` (tp1/tp2)."""

        return self._list_trade_orders(trade["trade_id"], kind=kind)

    # ---------------------------------------------------------- OCO evidence

    def _oco_group_id(self, trade: dict[str, Any]) -> str | None:
        group = self.store.get_oco_group(trade["trade_id"])
        return str(group["oco_group_id"]) if group else None

    def _sync_oco_legs(self, trade: dict[str, Any]) -> str | None:
        """Project both legs of each OCO order into ``oco_groups``/``oco_legs``.

        A native OCO order carries a take-profit leg *and* a stop-loss leg.  The
        historical projection only stored the take-profit leg, so a stop-loss
        execution looked indistinguishable from a cancellation.  Both legs are
        now persisted with group attribution so MEXC-confirmed state can be
        compared with what the bot believes.
        """

        group_id = self._oco_group_id(trade)
        if group_id is None:
            return None

        def _project(order: dict[str, Any], leg_role: str) -> None:
            self.store.upsert_oco_leg(
                group_id=group_id,
                trade=trade,
                order=order,
                state=str(order.get("state") or "unknown"),
                leg_role=leg_role,
                exchange_status=order.get("exchange_status"),
                cancel_reason=order.get("cancel_reason"),
                canceled_by_leg=order.get("canceled_by_leg"),
                evidence_id=order.get("evidence_id"),
                not_observed_reason=order.get("not_observed_reason"),
                filled_quote=_dec(order.get("filled_quote")),
                fee_amount=_dec(order.get("fee_amount")),
                fee_asset=order.get("fee_asset"),
                attempt=int(str(order.get("attempt") or 0)),
                verified=order.get("not_observed_reason") in (None, ""),
            )

        for kind in ("tp1", "tp2"):
            for order in self._oco_orders(trade, kind):
                _project(order, kind)
                stop_order = dict(order)
                stop_order["target"] = f"{kind}_sl"
                _project(stop_order, f"{kind}_sl")
        return group_id

    def _sync_oco_group_state(self, trade: dict[str, Any], *, note: str | None = None) -> None:
        mapping = {
            "oco_submitting": "submitting",
            "oco_active": "active",
            "oco_partial": "partial",
            "needs_reconciliation": "needs_review",
            UNPROTECTED_CRITICAL: "needs_review",
            "tp1_completed": "triggered",
            "tp2_completed": "completed",
            "closed": "completed",
            "stop_loss_executing": "triggered",
            "exit_pending": "triggered",
        }
        refreshed = self.store.get_trade(trade["trade_id"]) or trade
        state = mapping.get(str(refreshed.get("trade_state")), "intended")
        self.store.set_oco_group_state(
            trade["trade_id"],
            state,
            protection_status=str(refreshed.get("trade_state")),
            note=note,
            closed=state == "completed",
        )

    def _attribute_oco_siblings(self, trade: dict[str, Any]) -> dict[str, str]:
        """Attribute a canceled OCO leg to a filled sibling.

        This is observation only: it records *why* a leg disappeared.  It never
        resubmits an exit and never marks a partially exited trade as closed.
        """

        group_id = self._oco_group_id(trade)
        if group_id is None:
            return {}
        filled = [kind for kind in ("tp1", "tp2") if any(o["state"] == "filled" for o in self._oco_orders(trade, kind))]
        attributed: dict[str, str] = {}
        if len(filled) != 1:
            return attributed
        winner = filled[0]
        for kind in ("tp1", "tp2"):
            if kind == winner:
                continue
            for order in self._oco_orders(trade, kind):
                if (
                    order["state"] == "canceled"
                    and not order.get("canceled_by_leg")
                    and (_dec(order.get("sold_qty")) or Decimal("0")) <= 0
                ):
                    self.store.transition_trade_order(
                        order["trade_order_id"],
                        "canceled",
                        note=f"OCO sibling {winner} filled; {kind} canceled by the exchange",
                        cancel_reason="oco_sibling_filled",
                        canceled_by_leg=winner,
                    )
                    self.store.attribute_sibling_cancel(group_id, filled_target=winner, canceled_target=kind)
                    attributed[kind] = winner
        return attributed

    def _list_trade_orders(self, trade_id: str, kind: str | None = None) -> list[dict[str, Any]]:
        """Defensive wrapper around store.list_trade_orders (mock stores may lack it)."""
        list_orders = getattr(self.store, "list_trade_orders", None)
        if list_orders is None:
            return []
        return list_orders(trade_id=trade_id, kind=kind)

    def _oco_order_active(self, trade: dict[str, Any], kind: str) -> bool:
        """True when an OCO order for ``kind`` is still live or uncertain."""
        return any(order["state"] in UNCONFIRMED_ORDER_STATES for order in self._oco_orders(trade, kind))

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
            self._refresh_buy_block()
            return

        previous_state = trade.get("trade_state")
        # Protection may be placed as soon as a quantity is confirmed (F-05).
        # The entry order itself may still be working: the remainder is tracked
        # separately and cancelled if the acquired position is exited.  Only an
        # unsynchronised fill history blocks protection, because it would be
        # sized from unverified quantity.
        intent_id = trade.get("entry_intent_id")
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if intent_id:
            intent = self.store.get_intent(intent_id)
            if intent is not None and int(intent.get("fill_sync_required") or 0) == 1 and acquired <= 0:
                self._refresh_buy_block()
                return
        if acquired <= 0:
            self._record_error(trade, "no acquired quantity; cannot place OCO exits", "runtime")
            self._refresh_buy_block()
            return

        sl_price = _dec(trade.get("sl_price"))
        tp1_price = _dec(trade.get("tp1_price"))
        tp2_price = _dec(trade.get("tp2_price"))
        entry_price = _dec(trade.get("entry_price")) or _dec(trade.get("avg_entry_price"))

        problem = self._exit_ladder_problem(trade)
        if problem is not None:
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note=f"OCO not placed: {problem}",
            )
            self._notify_trade(trade, "OCO_CONFIG_REJECTED", problem)
            self._refresh_buy_block()
            return

        # The validation above guarantees a usable, ordered exit ladder.
        assert sl_price is not None and tp1_price is not None and tp2_price is not None

        # If the existing exit legs already sold the whole position, close it
        # before trying to allocate new orders.  Otherwise sellable==0 makes
        # the allocator reject and a fully completed trade is mislabelled.
        remaining = self._remaining_base(trade)
        if remaining is not None and remaining <= 0:
            existing = self._list_trade_orders(trade["trade_id"])
            if any(order.get("kind") in ("tp1", "tp2") and order.get("state") == "filled" for order in existing):
                self._finalize_trade(trade, reason="all allocated exits already filled")
                return

        group_id = self.store.ensure_oco_group(
            trade,
            intended_config={
                "entry_price": str(entry_price),
                "tp1_price": str(tp1_price),
                "tp2_price": str(tp2_price),
                "stop_loss_price": str(sl_price),
                "tp1_allocation_pct": str(trade.get("tp1_allocation_pct")),
                "tp2_allocation_pct": str(trade.get("tp2_allocation_pct")),
            },
            protection_status="placing",
        )
        self.store.set_oco_group_state(trade["trade_id"], "submitting")
        trade = self._refresh(trade)
        self.store.audit(
            trade["trade_id"],
            "lifecycle",
            "oco_group_created",
            f"intended {len(('tp1', 'tp2'))} legs for {trade['symbol']}",
            event_type="oco.intended",
            trade_id=trade["trade_id"],
            intent_id=trade.get("entry_intent_id"),
            symbol=trade["symbol"],
            payload={"oco_group_id": group_id, "stop_loss": str(sl_price)},
            dedupe_key=f"oco-intended:{group_id}",
        )

        # Allocations from *net* sellable quantity (owner-confirmed §5.2).
        # Gross acquired is incorrect when base-asset commission reduces
        # inventory.  Use sellable = acquired - base_fees - already_sold.
        # Allocations are computed once from the actual acquired position.
        # A restart must never shrink or re-expand an existing allocation.
        sellable_for_allocation = self._sellable_quantity(trade)
        if sellable_for_allocation is None:
            detail = (
                f"OCO allocation unprovable: sellable quantity is unknown for "
                f"{trade.get('trade_id')} {trade.get('symbol')} "
                f"(acquired={trade.get('acquired_base_qty')}); reconciliation required"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            return
        if sellable_for_allocation <= 0:
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note="sellable quantity is zero; cannot allocate OCO exits",
            )
            self._notify_trade(trade, "OCO_ALLOCATION_TOO_SMALL", "sellable quantity is zero")
            return
        tp1_pct = _dec(trade.get("tp1_allocation_pct")) or Decimal("50")
        tp2_pct = _dec(trade.get("tp2_allocation_pct")) or Decimal("100")
        tp1_allocated = _dec(trade.get("tp1_allocated_qty")) or Decimal("0")
        tp2_allocated = _dec(trade.get("tp2_allocated_qty")) or Decimal("0")
        updates: dict[str, Any] = {}
        if tp1_allocated <= 0:
            tp1_allocated = self._quantize_qty(trade, sellable_for_allocation * tp1_pct / Decimal("100"))
            updates["tp1_allocated_qty"] = tp1_allocated
        if tp2_allocated <= 0:
            pool = max(Decimal("0"), sellable_for_allocation - tp1_allocated)
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
                trade["trade_id"],
                "needs_reconciliation",
                note="OCO allocations are below the symbol lot size; position remains unprotected",
            )
            self._notify_trade(
                trade,
                "OCO_ALLOCATION_TOO_SMALL",
                "acquired quantity is below the lot size; no OCO exit could be placed",
            )
            self._refresh_buy_block()
            return

        # Critical invariant pre-check BEFORE any exchange submission (§5.1).
        # 1. If the current invariant cannot be proven or is already violated,
        #    persist needs_reconciliation, notify, and *return without submitting
        #    or increasing any OCO reservation*.
        ok, detail = self._verify_oco_invariant(trade)
        if not ok:
            # _verify_oco_invariant already persisted needs_reconciliation and
            # notified.  Do not submit any OCO.
            return

        results: dict[str, str] = {}
        invariant_blocked = False
        for kind, (qty, price) in required.items():
            # 3-4. Hypothetical combined reservation check BEFORE submitting this leg.
            # Existing reservations for both OCOs, accounting for reuse.
            existing_other = self._oco_reserved_quantity(trade, "tp2" if kind == "tp1" else "tp1")
            existing_this = self._oco_reserved_quantity(trade, kind)
            sellable = self._sellable_quantity(trade)
            if sellable is None:
                detail = (
                    f"OCO invariant unprovable for {trade.get('trade_id')}: sellable is unknown; "
                    "cannot verify hypothetical reservation — aborting submission"
                )
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                results[kind] = "failed"
                continue
            if existing_this > 0:
                hypothetical = existing_other + existing_this
            else:
                hypothetical = existing_other + qty
            if hypothetical > sellable:
                detail = (
                    f"pre-submit OCO invariant would be violated: "
                    f"existing OCO1+OCO2 {existing_other}+{existing_this} + proposed {kind} {qty} "
                    f"= hypothetical {hypothetical} > sellable {sellable}; "
                    "OCO submission blocked, persisting needs_reconciliation"
                )
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                self._notify_trade(trade, "OCO_INVARIANT_WOULD_VIOLATE", detail)
                self.store.audit(trade["trade_id"], "risk", "oco_invariant_would_violate", detail[:2000])
                results[kind] = "failed"
                invariant_blocked = True
                continue
            # 5. Only then call the exchange submission method.
            results[kind] = self._ensure_oco_leg(trade, kind, qty, price, sl_price)
            # 6. After submission, _ensure_oco_leg reconciles actual exchange
            #    order identity and state via _submit_oco_order → _reconcile.
            # 7. Recalculate invariant after confirmed submission (and every
            #    subsequent fill/cancel/reconciliation is covered in
            #    _reconcile_trade_orders).
            # Re-verify after each leg (fee/lot-size rebasing can change sellable)
            post_ok, post_detail = self._verify_oco_invariant(self._refresh(trade))
            if not post_ok:
                # Stop submitting further legs — reconciliation required.
                for pending_kind in list(required.keys()):
                    if pending_kind not in results:
                        results[pending_kind] = "failed"
                break
            trade = self._refresh(trade)

        working = [k for k, v in results.items() if v == "confirmed"]
        completed = [k for k, v in results.items() if v == "filled"]
        confirmed = working + completed
        uncertain = [k for k, v in results.items() if v == "uncertain"]
        failed = [k for k, v in results.items() if v in ("rejected", "failed", "canceled")]
        trade = self._refresh(trade)
        self._sync_oco_legs(trade)

        def _publish(new_state: str, note: str, event_key: str, text: str) -> None:
            self.store.update_trade_state(trade["trade_id"], new_state, note=note)
            self._sync_oco_group_state(trade, note=note)
            self.store.audit(
                trade["trade_id"],
                "lifecycle",
                event_key.lower(),
                text,
                event_type="oco.state",
                trade_id=trade["trade_id"],
                symbol=trade["symbol"],
                outcome=new_state,
                payload={"legs": results},
            )
            if new_state != previous_state:
                self._notify_trade(trade, event_key, text)

        if invariant_blocked:
            # Any leg blocked due to would-violate invariant means the allocation
            # plan is inconsistent with verified sellable — position is unprotected
            # and requires reconciliation, even if other legs are already confirmed.
            refreshed = self._refresh(trade)
            if refreshed is not None and refreshed.get("trade_state") != "needs_reconciliation":
                self.store.update_trade_state(
                    trade["trade_id"],
                    "needs_reconciliation",
                    note=detail[:2000] if "detail" in locals() else "OCO invariant would-violate blocked submission",
                )
            return
        if len(completed) == len(required):
            self._finalize_trade(trade)
            self._refresh_buy_block()
            return
        coverage = sum(
            (
                max(
                    Decimal("0"),
                    (_dec(order.get("quantity")) or Decimal("0")) - (_dec(order.get("sold_qty")) or Decimal("0")),
                )
                for kind in working
                for order in self._oco_orders(trade, kind)
                if order["state"] in ("open", "partially_filled")
            ),
            Decimal("0"),
        )
        remaining_cov = self._remaining_base(trade)
        if remaining_cov is None:
            # Unknown sellable → cannot prove full coverage; treat as not fully covered
            remaining_cov = Decimal(
                "0"
            )  # will fail coverage check and go to partial, but also already handled via invariant
        if len(confirmed) == len(required) and coverage >= remaining_cov:
            _publish(
                "oco_active",
                "remaining position covered by working OCO allocations",
                "OCO_PROTECTION_ACTIVE",
                f"{len(working)} working OCO allocation(s); {len(completed)} completed",
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
        else:
            # No exchange-confirmed exit leg.  The position owns base quantity
            # without verified protection, so mark it critical (reconciliation
            # keeps retrying protection) and keep the global new-BUY block on.
            detail = f"uncertain={','.join(uncertain) or '-'}; failed={','.join(failed) or '-'}"
            _publish(
                UNPROTECTED_CRITICAL,
                f"no OCO exit is confirmed; position is unprotected ({detail})",
                "OCO_PROTECTION_UNVERIFIED",
                "no OCO exit is confirmed; position is unprotected and new BUYs are blocked",
            )
        self._refresh_buy_block()

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
        if any(
            order["state"]
            in (
                "submitting",
                "cancel_requested",
                "unknown_requires_reconciliation",
            )
            for order in orders
        ):
            # A write claim or a previously assigned exchange id is not proof
            # of current protection. Reconcile these rows; never resend them.
            return "uncertain"
        filled = [order for order in orders if order["state"] == "filled"]
        active = [order for order in orders if order["state"] in ("open", "partially_filled")]
        if filled:
            # Completion is not active protection. Contradictory overlapping
            # orders or incomplete execution evidence must be reviewed.
            sold = sum((_dec(order.get("sold_qty")) or Decimal("0") for order in orders), Decimal("0"))
            return "filled" if not active and sold >= wanted_qty else "uncertain"
        if len(active) > 1:
            return "uncertain"
        if active:
            return "confirmed"
        if any(order["state"] == "canceled" for order in orders):
            # An OCO leg can only disappear because it was canceled/replaced on
            # the exchange; resubmitting automatically could oversell.
            return "canceled"

        rejected = [order for order in orders if order["state"] == "rejected"]
        if rejected:
            # A deterministic rejection is terminal for this leg: retrying the
            # same payload can never succeed and must not look like a transient
            # failure to the operator.
            retryable_flags = [
                int(str(order.get("retryable") if order.get("retryable") is not None else 1)) for order in rejected
            ]
            if 0 in retryable_flags:
                return "failed"
            if len(rejected) >= MAX_OCO_ATTEMPTS:
                return "failed"
            next_at = _dec_ts(rejected[-1].get("next_attempt_at"))
            if next_at is not None and self._now() < next_at:
                return "retry_pending"

        remaining_oco = self._remaining_base(trade)
        if remaining_oco is None:
            detail = f"{kind} sellable unknown — cannot size OCO; needs_reconciliation"
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self.store.audit(trade["trade_id"], "risk", "oco_invariant_unknown", detail[:2000])
            return "failed"
        qty = self._quantize_qty(trade, min(wanted_qty, remaining_oco))
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
                    next_order["trade_order_id"],
                    "canceled",
                    note=f"not sent; rebased {existing_qty}->{qty} @ {price_q}",
                )
                next_order = None
        if next_order is None:
            next_order = self._create_trade_order(trade, kind, qty, price_q, order_type="OCO_STOP_LIMIT_ORDER")

        self._submit_oco_order(trade, kind, next_order, qty, price_q, stop_price)
        refreshed = self.store.get_trade_order(next_order["trade_order_id"])
        state = (refreshed or {}).get("state")
        if state in ("open", "partially_filled"):
            return "confirmed"
        if state in ("unknown_requires_reconciliation", "submitting", "cancel_requested"):
            return "uncertain"
        if state == "rejected":
            return "rejected"
        if state == "filled":
            sold = _dec((refreshed or {}).get("sold_qty")) or Decimal("0")
            return "filled" if sold >= wanted_qty else "uncertain"
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
        self.store.transition_trade_order(order["trade_order_id"], "submitting", note="OCO submission started")
        try:
            result = engine.submit_oco_sell(
                trade,
                order["client_order_id"],
                price,
                qty,
                stop_price,
                kind=target,
                trade_order_id=order["trade_order_id"],
            )
        except MexcDuplicateOrder as exc:
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note="duplicate client order id; earlier OCO attempt may have landed",
            )
            self._record_error(trade, f"{target} OCO duplicate client id: {exc}", "exchange")
            self._reconcile_trade_orders(trade)
            return None
        except _UNCERTAIN_ERRORS as exc:
            attempt = int(str(order.get("attempt") or 0))
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note=f"uncertain OCO submission: {exc}"[:500],
                attempt=attempt,
            )
            self._record_error(trade, f"{target} OCO submission outcome unknown: {exc}", "exchange")
            self._notify_trade(
                trade,
                f"OCO_ATTEMPT_{attempt + 1}_UNCERTAIN",
                f"{target} attempt {attempt + 1}/{MAX_OCO_ATTEMPTS} outcome unknown: {str(exc)[:400]}; "
                "read-only reconciliation will decide, no replacement is submitted",
            )
            return None
        except Exception as exc:
            attempt = int(str(order.get("attempt") or 0))
            retryable = _is_retryable_oco_error(exc)
            delay = oco_retry_delay_s(attempt)
            next_at = (
                (self._now() + timedelta(seconds=delay)).isoformat()
                if retryable and delay is not None
                else None
            )
            self.store.transition_trade_order(
                order["trade_order_id"],
                "rejected",
                note=str(exc)[:500],
                attempt=attempt,
                next_attempt_at=next_at,
                retryable=retryable,
            )
            self._record_error(trade, f"{target} OCO rejected: {exc}", "exchange")
            remaining_attempts = 0 if not retryable else max(0, MAX_OCO_ATTEMPTS - (attempt + 1))
            self._notify_trade(
                trade,
                f"OCO_ATTEMPT_{attempt + 1}_REJECTED",
                f"{target} attempt {attempt + 1}/{MAX_OCO_ATTEMPTS} rejected: {str(exc)[:400]}; "
                f"retryable={retryable} remaining_attempts={remaining_attempts}",
            )
            return None

        exchange_id = result.get("orderId") or result.get("exchange_order_id")
        executed = _dec(result.get("executed_qty")) or Decimal("0")
        if not exchange_id:
            self.store.transition_trade_order(
                order["trade_order_id"],
                "unknown_requires_reconciliation",
                note="OCO submission returned no order id; reconcile by client order id",
            )
            self._record_error(trade, f"{target} OCO returned no order id", "exchange")
            return None
        self.store.transition_trade_order(
            order["trade_order_id"],
            "open",
            note="OCO submitted",
            exchange_order_id=str(exchange_id),
            exchange_status=result.get("state") or result.get("status"),
            sold_qty=executed,
            stop_trigger_price=_dec(result.get("trigger_price")),
        )
        attempt = int(order.get("attempt") or 0)
        self._notify_trade(
            trade,
            f"OCO_SUBMITTED_{target.upper()}_ATTEMPT_{attempt + 1}",
            f"{target} attempt {attempt + 1}/{MAX_OCO_ATTEMPTS} accepted: {qty} @ {price} "
            f"SL @ {stop_price} trigger @ {result.get('trigger_price')}",
        )
        self._reconcile_trade_orders(trade)
        return {
            "exchange_order_id": str(exchange_id),
            "executed_qty": executed,
        }

    def _advance_tp_if_empty(self, trade: dict[str, Any], target: str) -> None:
        """If the sell quantity is zero, advance the target without inventing fills."""
        now = self._ts()
        with self.store.db.transaction() as conn:
            row = conn.execute("SELECT trade_state FROM trades WHERE trade_id = ?", (trade["trade_id"],)).fetchone()
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
        """Disabled per §7 — Market Sell stop-loss is *not* an authorized automatic path.

        Native OCO SL legs are the only authorized automatic stop-loss mechanism.
        This method is retained only to prove no Market Sell is submitted: it
        delegates to the unprotected handler which persists critical state and
        never calls the exchange.  Search for `submit_stop_loss_market_sell`
        must show zero reachable call sites (verified via grep).
        """
        self._handle_unprotected_sl(
            trade, trigger_price, context="_enter_stop_loss disabled — Market Sell not authorized"
        )

    def _resume_stop_loss(self, trade: dict[str, Any]) -> None:
        """Disabled per §7 — never resumes a Market Sell. No SELL submitted."""
        try:
            self._reconcile_entry_fills(trade)
            self._reconcile_trade_orders(trade)
        except Exception:
            pass
        sellable = self._sellable_quantity(trade)
        detail = (
            f"_resume_stop_loss disabled — Market Sell not authorized; "
            f"trade {trade.get('trade_id')} {trade.get('symbol')} sellable={sellable} "
            "requires native OCO SL protection; manual intervention required"
        )
        self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
        self._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail)

    def _submit_stop_loss_order(self, trade: dict[str, Any], qty: Decimal) -> None:
        """Disabled per §7 — Market Sell submission is not authorized.

        This method must never reach engine.submit_stop_loss_market_sell.
        Search must show zero reachable call sites.
        """
        detail = (
            f"_submit_stop_loss_order disabled — Market Sell not authorized; "
            f"trade {trade.get('trade_id')} {trade.get('symbol')} qty={qty} "
            "native OCO SL legs only; persisting critical unprotected state"
        )
        self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
        self._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail)

    # ----------------------------------------------------------------
    # Sellable inventory — owner-confirmed §5.2 / critical invariant §5.1
    # ----------------------------------------------------------------
    def _base_asset_for_trade(self, trade: dict[str, Any]) -> str | None:
        try:
            meta = self._symbol_meta(trade)
            base = getattr(meta, "base_asset", None) if meta is not None else None
            if base:
                return str(base).upper()
        except Exception:
            pass
        # Fallback for tests / when symbol registry unavailable: infer base
        # asset from symbol string (e.g., BTCUSDT → BTC).  This keeps fee
        # accounting deterministic even with mocked clients.
        try:
            symbol = str(trade.get("symbol") or "")
            for quote in ("USDT", "USDC", "USDD", "BTC", "ETH", "BNB", "MX"):
                if symbol.endswith(quote) and len(symbol) > len(quote):
                    return symbol[: -len(quote)].upper()
            # Last resort: first 3 chars
            return symbol[:3].upper() if symbol else None
        except Exception:
            return None

    def _total_base_fees(self, trade: dict[str, Any]) -> Decimal | None:
        """Sum commissions paid in the base asset across entry + exit fills.

        Strictly fail-closed: a confirmed absence of applicable fills may
        contribute zero, but any failed DB query, unavailable order history,
        incomplete fill synchronization, malformed fee record, or otherwise
        uncertain result must NOT be interpreted as zero.  Returns None
        (unknown) when fees cannot be proven, propagating via _sellable_quantity
        to block OCO submission and any exact-inventory operation.
        Quote-asset fees do not reduce base sellable; third-token fees are
        preserved as unvalued.  Idempotency is via exchange trade IDs; missing
        IDs for base-asset fees make the result unprovable.
        """
        base_asset = self._base_asset_for_trade(trade)
        if not base_asset:
            # Cannot determine base asset → cannot prove fee asset match
            return None
        # Incomplete fill sync → fee data incomplete
        intent_id = trade.get("entry_intent_id")
        if intent_id:
            try:
                intent = self.store.get_intent(intent_id)
            except Exception:
                return None
            if intent is not None and int(intent.get("fill_sync_required") or 0) == 1:
                return None
        seen: set[str] = set()
        total = Decimal("0")
        # Entry fills
        if intent_id:
            try:
                intent = self.store.get_intent(intent_id)
            except Exception:
                return None
            exch_id = (intent or {}).get("exchange_order_id") if intent else None
            if exch_id:
                try:
                    fills = self.store.fills_for_order(exch_id)
                except Exception:
                    return None
                for fill in fills:
                    fee_asset = str(fill.get("fee_asset") or "").upper()
                    if fee_asset == base_asset:
                        # Base-asset fee must have a parseable amount and a dedup ID
                        fee_raw = fill.get("fee_amount")
                        if fee_raw is None or fee_raw == "":
                            return None
                        dec_fee = _dec(fee_raw)
                        if dec_fee is None:
                            return None
                        tid = str(fill.get("exchange_trade_id") or fill.get("trade_id") or "")
                        if not tid:
                            return None
                        if tid in seen:
                            continue
                        seen.add(tid)
                        total += dec_fee
                    else:
                        # Track non-base fills for idempotency but don't add
                        tid = str(fill.get("exchange_trade_id") or fill.get("trade_id") or "")
                        if tid:
                            if tid in seen:
                                continue
                            seen.add(tid)
                        # confirmed absence of base fee → 0, no uncertainty
                # confirmed absence (no fills) → 0 is valid, already handled
        # Exit fills (TP1/TP2/stop_loss OCO legs) — base-asset commissions on SELL
        try:
            orders = self.store.list_trade_orders(trade["trade_id"])
        except Exception:
            return None
        for order in orders:
            exch_id = order.get("exchange_order_id")
            if not exch_id:
                continue  # never sent → confirmed zero
            try:
                fills = self.store.fills_for_order(exch_id)
            except Exception:
                return None
            for fill in fills:
                fee_asset = str(fill.get("fee_asset") or "").upper()
                if fee_asset == base_asset:
                    fee_raw = fill.get("fee_amount")
                    if fee_raw is None or fee_raw == "":
                        return None
                    dec_fee = _dec(fee_raw)
                    if dec_fee is None:
                        return None
                    tid = str(fill.get("exchange_trade_id") or fill.get("trade_id") or "")
                    if not tid:
                        return None
                    if tid in seen:
                        continue
                    seen.add(tid)
                    total += dec_fee
                else:
                    tid = str(fill.get("exchange_trade_id") or fill.get("trade_id") or "")
                    if tid:
                        if tid in seen:
                            continue
                        seen.add(tid)
        return total

    def _sellable_quantity(self, trade: dict[str, Any]) -> Decimal | None:
        """Bot-owned base quantity actually available to allocate/sell.

        Returns None when sellable cannot be proven (e.g., acquired not yet
        recorded, fill sync pending, fee query failed, malformed fee record) —
        caller must treat as unknown, persist needs_reconciliation, and NOT submit.
        """
        raw_acquired = trade.get("acquired_base_qty")
        if raw_acquired is None or raw_acquired == "":
            return None
        acquired = _dec(raw_acquired)
        if acquired is None:
            return None
        # Check for incomplete fill sync at intent level (authoritative)
        intent_id = trade.get("entry_intent_id")
        if intent_id:
            try:
                intent = self.store.get_intent(intent_id)
                if intent is not None and int(intent.get("fill_sync_required") or 0) == 1:
                    return None
            except Exception:
                return None
        base_fees = self._total_base_fees(trade)
        if base_fees is None:
            return None
        # _sold_for_kind is durable derived from trade_orders.sold_qty, but if
        # any order's sold_qty is unknown due to reconciliation, sellable remains
        # calculable from known sold; fee unknown already handled above.
        sold = (
            self._sold_for_kind(trade, "tp1")
            + self._sold_for_kind(trade, "tp2")
            + self._sold_for_kind(trade, "stop_loss")
        )
        return max(Decimal("0"), acquired - base_fees - sold)

    def _oco_reserved_quantity(self, trade: dict[str, Any], kind: str) -> Decimal:
        total = Decimal("0")
        for order in self._oco_orders(trade, kind):
            if order["state"] in (
                "open",
                "partially_filled",
                "submitting",
                "cancel_requested",
                "unknown_requires_reconciliation",
            ):
                qty = _dec(order.get("quantity")) or Decimal("0")
                filled = _dec(order.get("sold_qty")) or Decimal("0")
                total += max(Decimal("0"), qty - filled)
        return total

    def _combined_oco_reserved(self, trade: dict[str, Any]) -> Decimal:
        return self._oco_reserved_quantity(trade, "tp1") + self._oco_reserved_quantity(trade, "tp2")

    def _verify_oco_invariant(self, trade: dict[str, Any]) -> tuple[bool, str]:
        sellable = self._sellable_quantity(trade)
        if sellable is None:
            sold_total = (
                self._sold_for_kind(trade, "tp1")
                + self._sold_for_kind(trade, "tp2")
                + self._sold_for_kind(trade, "stop_loss")
            )
            detail = (
                f"OCO invariant unprovable: sellable quantity is unknown for "
                f"{trade.get('trade_id')} {trade.get('symbol')} "
                f"(acquired={trade.get('acquired_base_qty')} "
                f"base_fees={self._total_base_fees(trade)} "
                f"sold={sold_total}); "
                "reconciliation required before (re)submitting OCO legs"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self.store.audit(trade["trade_id"], "risk", "oco_invariant_unknown", detail[:2000])
            return False, detail
        reserved = self._combined_oco_reserved(trade)
        if reserved <= sellable:
            return True, f"invariant ok: reserved {reserved} <= sellable {sellable}"
        detail = (
            f"critical OCO invariant violated: OCO1+OCO2 reserved {reserved} "
            f"> bot-owned sellable {sellable} "
            f"(acquired={_dec(trade.get('acquired_base_qty')) or Decimal('0')} "
            f"base_fees={self._total_base_fees(trade)} "
            f"sold_tp1={self._sold_for_kind(trade, 'tp1')} "
            f"sold_tp2={self._sold_for_kind(trade, 'tp2')} "
            f"sold_sl={self._sold_for_kind(trade, 'stop_loss')}); "
            "reconciliation required before (re)submitting OCO legs"
        )
        self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
        self._notify_trade(trade, "OCO_INVARIANT_VIOLATED", detail)
        self.store.audit(trade["trade_id"], "risk", "oco_invariant_violated", detail[:2000])
        return False, detail

    def _remaining_base(self, trade: dict[str, Any]) -> Decimal | None:
        """Sellable remaining base, or None if unknown (fail-closed)."""
        return self._sellable_quantity(trade)

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
        if remaining is None:
            # Unknown inventory → cannot prove fully exited; fail closed
            detail = f"close refused: sellable unknown for {trade.get('trade_id')} — fee or fill data unprovable"
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
            self._notify_trade(trade, "TRADE_CLOSE_REFUSED_UNKNOWN", detail)
            self.store.audit(trade["trade_id"], "risk", "close_refused_unknown", detail[:2000])
            return
        if remaining > 0 and residual <= 0:
            # Never silently close a trade that still owns base quantity.
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note=f"close refused: {remaining} base quantity remains unsold",
            )
            self._notify_trade(
                trade,
                "TRADE_CLOSE_REFUSED",
                f"{remaining} base quantity remains; manual reconciliation required",
            )
            self._refresh_buy_block()
            return
        self.store.update_trade_state(trade["trade_id"], "closed", note="position fully exited", sl_state="completed")
        self._sync_oco_legs(trade)
        self._sync_oco_group_state(trade, note="position fully exited")
        self.store.audit(
            trade["trade_id"],
            "lifecycle",
            "trade_closed",
            "position fully exited",
            event_type="trade.closed",
            trade_id=trade["trade_id"],
            symbol=trade["symbol"],
            intent_id=trade.get("entry_intent_id"),
            outcome="closed",
        )
        self._notify_trade(trade, "TRADE_CLOSED", "trade fully closed")
        self._refresh_buy_block()

    # ===================================================================
    # Cancellation
    # ===================================================================

    def _cancel_entry_remainder_when_exited(self, trade: dict[str, Any]) -> None:
        """Cancel a still-working entry remainder once the position is exited.

        F-05: with a partial fill the exchange still holds an unfilled entry
        quantity.  If the acquired part leaves the position (TP1 or stop-loss),
        that remainder must not be allowed to fill later and open an unwanted,
        unprotected position.
        """

        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return
        intent = self.store.get_intent(intent_id)
        if intent is None or intent.get("state") in TERMINAL_INTENT_STATES:
            return
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if acquired <= 0:
            return
        remaining = self._remaining_base(trade)
        exited = self._tp_done(trade, "tp1") or (remaining is not None and remaining <= 0)
        if not exited:
            return
        if not self._cancel_entry_remainder(trade):
            self._record_error(trade, "entry remainder cancel unconfirmed", "exchange")
            return
        detail = (
            "acquired position exited while the entry order still had an unfilled "
            "remainder; the remainder was cancelled so it cannot open a new position"
        )
        self.store.audit(trade["trade_id"], "lifecycle", "entry_remainder_cancelled", detail)
        self._notify_trade(trade, "ENTRY_REMAINDER_CANCELLED", detail)
        self._refresh_buy_block()

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
                self.store.transition_trade_order(order["trade_order_id"], "canceled", note="not submitted")
                continue
            if not self._cancel_trade_order(trade, order):
                ok = False
        return ok

    def _cancel_trade_order(self, trade: dict[str, Any], order: dict[str, Any]) -> bool:
        if order["state"] in TERMINAL_ORDER_STATES:
            return True
        if order["state"] == "pending":
            self.store.transition_trade_order(order["trade_order_id"], "canceled", note="not submitted")
            return True
        if self.engine is None:
            return False
        self.store.transition_trade_order(order["trade_order_id"], "cancel_requested", note="cancel requested")
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
            try:
                for trade in self.store.list_trades():
                    if trade["trade_state"] in TERMINAL_TRADE_STATES:
                        continue
                    try:
                        self._reconcile_trade(trade["trade_id"])
                    except Exception as exc:  # noqa: BLE001
                        log.error("reconcile failed for %s: %s", trade["trade_id"], exc)
            finally:
                self._refresh_buy_block()

    def _reconcile_trade(self, trade_id: str) -> None:
        """Reconcile one trade and refresh the durable new-BUY block."""

        try:
            self._reconcile_trade_inner(trade_id)
        finally:
            self._refresh_buy_block()

    def _reconcile_trade_inner(self, trade_id: str) -> None:
        """Sync fills from the exchange and advance the trade state machine."""
        store = self.store
        trade = store.get_trade(trade_id)
        if trade is None:
            return
        state = trade["trade_state"]
        if state in ("stop_loss_executing", "exit_pending"):
            # Legacy Market Sell states — Market Sell path is disabled per §7.
            # Treat as unprotected: reconcile read-only, persist critical.
            self._reconcile_entry_fills(trade)
            self._reconcile_trade_orders(trade)
            self.store.update_trade_state(
                trade_id,
                "needs_reconciliation",
                note=(
                    "legacy stop_loss_executing/exit_pending without OCO — "
                    "Market Sell disabled; manual reconciliation required"
                ),
            )
            self._notify_trade(
                trade,
                "SL_UNPROTECTED_CRITICAL",
                "legacy Market Sell state encountered after disablement — no Market Sell submitted",
            )
            return
        if state == RECONCILIATION_REQUIRED_STATE:
            # Safe read-only reconciliation only — never auto-submit BUY/SELL/
            # OCO/replacement.  Must not auto-transition back to ordinary active
            # trading.  If still unknown, retain needs_reconciliation with reason;
            # if conclusively closed, use explicit auditable transition to closed.
            self._reconcile_entry_fills(trade)
            self._reconcile_trade_orders(trade)
            refreshed = self._refresh(trade)
            if refreshed is None:
                return
            # Determine if conclusively established as closed:
            #  - no unknown order states, no unknown intent, sellable is known
            #  - all orders terminal and remaining base is 0
            try:
                orders = self._list_trade_orders(refreshed["trade_id"])
                has_unknown = any(
                    o["state"] in ("unknown_requires_reconciliation", "submitting", "cancel_requested") for o in orders
                )
                intent_id = refreshed.get("entry_intent_id")
                intent = store.get_intent(intent_id) if intent_id else None
                intent_unknown = False
                if intent is not None and (
                    intent.get("state") in ("unknown_requires_reconciliation", "reconciling")
                    or int(intent.get("fill_sync_required") or 0) == 1
                ):
                    intent_unknown = True
                sellable = self._sellable_quantity(refreshed)
                # Unknown evidence → retain
                if has_unknown or intent_unknown or sellable is None:
                    # Record revisit without duplicating user notifications
                    # (initial entry already notified SL_UNPROTECTED_CRITICAL /
                    # OCO_INVARIANT_*).  Use audit only, idempotent.
                    self.store.audit(
                        trade_id,
                        "reconcile",
                        "needs_reconciliation_revisited",
                        "read-only reconciliation; still unresolved "
                        f"(has_unknown={has_unknown} intent_unknown={intent_unknown} "
                        f"sellable={sellable})",
                    )
                    return
                all_terminal = all(o["state"] in TERMINAL_ORDER_STATES for o in orders) if orders else True
                # Conclusive close: sellable 0 and all orders terminal
                if all_terminal and sellable == Decimal("0"):
                    # Explicit auditable transition — not an automatic active resumption
                    self.store.update_trade_state(
                        trade_id,
                        "closed",
                        note=(
                            "reconciliation: exchange outcome conclusively established; "
                            "position fully exited (remaining 0, all orders terminal)"
                        ),
                        sl_state="completed",
                    )
                    refreshed2 = self._refresh(refreshed)
                    self._notify_trade(
                        refreshed2 or refreshed,
                        "TRADE_CLOSED_RECONCILED",
                        "trade conclusively closed via read-only reconciliation",
                    )
                    self.store.audit(
                        trade_id, "lifecycle", "closed_via_reconciliation", "all orders terminal, sellable 0"
                    )
                    return
                # Otherwise retain needs_reconciliation — do not auto-transition to active
                self.store.audit(
                    trade_id,
                    "reconcile",
                    "needs_reconciliation_retained",
                    f"sellable={sellable} all_terminal={all_terminal} "
                    f"orders={len(orders)} — manual review still required",
                )
                return
            except Exception as exc:  # noqa: BLE001 - never block reconciliation on audit
                log.warning("needs_reconciliation post-reconcile check failed for %s: %s", trade_id, exc)
                return

        self._reconcile_entry_fills(trade)
        self._reconcile_trade_orders(trade)
        trade = self._refresh(trade)
        if trade is None:
            return
        state = trade["trade_state"]

        if state == UNPROTECTED_CRITICAL:
            # A position owns base quantity without verified exit cover.  Keep
            # retrying protection (idempotently, never blindly) and leave the
            # global new-BUY block in force until coverage is confirmed.
            self._sync_oco_legs(trade)
            self._sync_oco_group_state(trade)
            remaining = self._remaining_base(trade)
            if remaining is not None and remaining <= 0:
                self._finalize_trade(trade)
                return
            self._place_oco_exits(trade)
            return

        if state in ("pending_entry", "entry_active", "position_open"):
            intent = store.get_intent(trade["entry_intent_id"]) if trade.get("entry_intent_id") else None
            if (
                state == "pending_entry"
                and intent is not None
                and intent.get("state") in ("submitted", "open", "partially_filled", "reconciling")
            ):
                store.update_trade_state(trade_id, "entry_active", note="entry was already submitted before restart")
                trade = self._refresh(trade) or trade
            self._settle_entry_state(trade)
            return

        if state in ("tp1_active", "tp1_completed", "tp2_active", "position_open", "entry_active"):
            # Legacy standalone-TP states are no longer executed.  Exits are
            # native OCO only (F-01): such a trade is parked for manual review
            # instead of silently falling back to a separate TP order.
            self.store.update_trade_state(
                trade_id,
                "needs_reconciliation",
                note=(
                    "legacy standalone take-profit state; automatic exit disabled "
                    "(OCO-only policy) — manual reconciliation required"
                ),
            )
            self._notify_trade(
                trade,
                "LEGACY_EXIT_DISABLED",
                "legacy TP state encountered; no standalone exit order was submitted",
            )
            return
        if state in ("oco_submitting", "oco_active", "oco_partial"):
            # Reconcile OCO exit orders against the exchange.  All required legs
            # must confirm before protection is considered active; an uncertain
            # leg is reconciled but never resubmitted.
            attributed = self._attribute_oco_siblings(trade)
            if attributed:
                # Observation only: attribution never re-arms an exit.
                self.store.audit(
                    trade["trade_id"],
                    "lifecycle",
                    "oco_sibling_canceled",
                    "a filled OCO sibling canceled the other leg: "
                    + ", ".join(f"{k}<-{v}" for k, v in attributed.items()),
                    event_type="oco.sibling_canceled",
                    trade_id=trade["trade_id"],
                    symbol=trade["symbol"],
                    payload={"attributed": attributed},
                )
            self._sync_oco_legs(trade)
            self._sync_oco_group_state(trade)
            self._cancel_entry_remainder_when_exited(trade)
            if self._ocos_terminal(trade):
                self._finalize_trade(trade)
                return
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
                intent.get("state") not in TERMINAL_INTENT_STATES or int(intent.get("fill_sync_required") or 0) == 1
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
        # §5.3 — recompute combined OCO invariant after every reconciliation.
        try:
            refreshed = self._refresh(trade)
            # Only verify when OCO legs exist; otherwise a pre-OCO position
            # (e.g. entry_active) would spuriously look violated due to 0 sellable.
            if self._list_trade_orders(refreshed["trade_id"]):
                self._verify_oco_invariant(refreshed)
        except Exception:  # noqa: BLE001 - invariant check must not block reconciliation
            pass
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
                    trade["trade_id"],
                    "tp1_completed",
                    note="TP1 filled; no TP2 price, remaining position kept under SL",
                )
            return
        if state == "tp2_active" and self._tp_done(trade, "tp2"):
            remaining = self._remaining_base(trade)
            if remaining is None:
                self.store.audit(
                    trade["trade_id"], "reconcile", "tp2_remaining_unknown_advance", "sellable unknown; cannot finalize"
                )
                return
            if remaining <= 0:
                self._finalize_trade(trade)
            else:
                qty = self._quantize_qty(trade, remaining)
                if qty <= 0:
                    self._finalize_trade(trade, residual=remaining, reason="dust below minimum after TP2")
        if state in ("oco_active", "oco_partial") and self._ocos_terminal(trade):
            self._finalize_trade(trade)

    def _reconcile_single_order(self, trade: dict[str, Any], order: dict[str, Any]) -> dict[str, Any] | None:
        if order["state"] in TERMINAL_ORDER_STATES or order["state"] == "pending":
            return order
        fresh = self._query_order(order)
        if fresh is None:
            self.store.mark_trade_order_not_observed(
                order["trade_order_id"], "exchange query returned no usable result"
            )
            self._record_error(trade, f"order query failed for {order['trade_order_id']}", "exchange")
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
            order,
            mapped,
            fresh,
            sold,
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
            order["trade_order_id"],
            mapped,
            note="reconciled",
            exchange_order_id=str(exchange_id) if exchange_id else None,
            exchange_status=str(raw_state) if raw_state is not None else None,
            sold_qty=sold,
            last_observed_at=self._ts(),
            not_observed_reason=None,
        )

    def _refresh_sold_fields(self, trade: dict[str, Any]) -> None:
        tp1 = self._sold_for_kind(trade, "tp1")
        tp2 = self._sold_for_kind(trade, "tp2")
        sl = self._sold_for_kind(trade, "stop_loss")
        self.store.update_trade_fields(trade["trade_id"], tp1_sold_qty=tp1, tp2_sold_qty=tp2, sl_executed_qty=sl)

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
        digest = hashlib.sha256(f"{trade_id}:{kind}:{attempt}".encode("ascii")).hexdigest()[:16]
        prefix = {"tp1": "t1", "tp2": "t2", "stop_loss": "sl"}[kind]
        return f"{prefix}-{digest}"

    def _refresh(self, trade: dict[str, Any]) -> dict[str, Any]:
        refreshed = self.store.get_trade(trade["trade_id"])
        return refreshed if refreshed is not None else trade

    def _symbol_meta_symbol(self, symbol: str) -> Any | None:
        client = self.client or (getattr(self.engine, "client", None) if self.engine else None)
        registry = getattr(client, "symbols", None) if client else None
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

    def _record_residual(self, trade: dict[str, Any], target: str, qty: Decimal, reason: str) -> None:
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
            trade["trade_id"],
            retry_count=retry,
            last_error=message[:500],
            last_error_category=category,
        )
        self._notify_trade(trade, "TRADE_ERROR", message[:500])

    # ===================================================================
    # Notifications
    # ===================================================================

    def _notify_trade(self, trade: dict[str, Any] | None, event_key: str, text: str, **extra: Any) -> None:
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
