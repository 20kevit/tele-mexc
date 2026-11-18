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

import contextlib
import hashlib
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from ..locking import KeyedRLockPool
from ..mexc.errors import (
    MexcAuthError,
    MexcConfigError,
    MexcDuplicateOrder,
    MexcNetworkError,
    MexcOrderNotFound,
    MexcOrderStateUncertain,
    MexcPermissionError,
    MexcPrecisionError,
    MexcRateLimitError,
    MexcServerError,
    MexcSymbolError,
    MexcUnexpectedResponse,
)
from ..telemetry import SLOW_LOCK_HOLD_MS, SLOW_LOCK_WAIT_MS, level_for
from .domain_events import DomainEventConflict, build_domain_event
from .parser import ParsedSignal
from .parsing import parse_decimal as _dec
from .parsing import parse_iso_utc as _dec_ts
from .store import TERMINAL_INTENT_STATES, submission_acknowledged
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
    "ignored_legacy",
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

# OCO child discovery.  MEXC returns the TP and stop children of a resolved
# native OCO as separate allOrders rows with ``orderListId = -1`` and null
# client ids.  There is no exchange parent/child link, and the OCO trigger is
# not exposed (``stopPrice`` is null/absent), so a sibling is only accepted
# when every observable field matches exactly and the result is unique:
# symbol, side, type, ``time`` (same millisecond), ``origQty``, and the
# stop-limit ``price``.  ``price`` on the stop child is the stop-limit price,
# which equals ``trades.sl_price``; it is *not* the trigger stored in
# ``trade_orders.stop_trigger_price`` for legacy rows (historically SL+ticks).
OCO_CHILD_DISCOVERY_MARGIN_MS = 10 * 60 * 1000
OCO_CHILD_DISCOVERY_HISTORY_MS = 7 * 24 * 60 * 60 * 1000
OCO_CHILD_DISCOVERY_BACKOFF_PREFIX = "OCO_STOP_CHILD_DISCOVERY:"
OCO_CHILD_DISCOVERY_BACKOFF_MAX_S = 900.0
OCO_CHILD_DISCOVERY_QUERY_LIMIT = 1000

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
    # -2011/-2013 answering a create request: the order may have landed.
    MexcOrderStateUncertain,
    MexcOrderNotFound,
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


def _int_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_retryable_oco_error(exc: BaseException) -> bool:
    """False for deterministic rejections that must never be retried."""

    if isinstance(
        exc,
        (
            MexcPrecisionError,
            MexcSymbolError,
            MexcPermissionError,
            MexcConfigError,
            MexcAuthError,
            MexcOrderNotFound,
            MexcOrderStateUncertain,
        ),
    ):
        return False
    text = str(exc).lower()
    return not any(token in text for token in _NON_RETRYABLE_TOKENS)


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
        self._trade_locks = KeyedRLockPool()
        self._buy_block_lock = threading.Lock()

    @contextlib.contextmanager
    def _hold_trade_lock(self, key: str, operation: str) -> Any:
        requested = self._clock()
        with self._trade_locks.hold(key):
            acquired = self._clock()
            outcome = "ok"
            try:
                yield
            except Exception:
                outcome = "error"
                raise
            finally:
                released = self._clock()
                wait_ms = int(round(max(0.0, acquired - requested) * 1000))
                hold_ms = int(round(max(0.0, released - acquired) * 1000))
                level = level_for((wait_ms, SLOW_LOCK_WAIT_MS), (hold_ms, SLOW_LOCK_HOLD_MS))
                log.log(
                    level,
                    "trade_lock operation=%s signal_or_trade=%s wait_ms=%s hold_ms=%s outcome=%s",
                    operation,
                    key,
                    wait_ms,
                    hold_ms,
                    outcome,
                )

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
        with self._hold_trade_lock(str(intent["signal_id"]), "on_intent_planned"):
            existing = self.store.get_trade_by_signal(intent["signal_id"])
            if existing is not None:
                trade_state = str(existing.get("trade_state") or "")
                if trade_state != "pending_entry":
                    # Duplicate Telegram delivery reaches this method through
                    # _resume_planned_intent(), outside the periodic resume guard.
                    # Fail closed for every lifecycle state except pending_entry.
                    return TradeDecision(
                        trade=existing,
                        deferred=False,
                        rejected=True,
                        reason=(f"existing trade state {trade_state!r} is not eligible for planned entry submission"),
                    )
                if existing.get("entry_mode") == "conditional":
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
        """Called only after a durable MEXC order acknowledgement."""
        if not submission_acknowledged(intent):
            return
        intent_id = intent.get("intent_id")
        if intent_id is None:
            return
        trade = self.store.get_trade_by_intent(intent_id)
        if trade is None:
            return
        with self._hold_trade_lock(str(trade.get("signal_id") or trade["trade_id"]), "on_intent_executed"):
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
            if intent_state in (
                "open",
                "partially_filled",
                "submitted",
                "reconciling",
                "unknown_requires_reconciliation",
            ):
                self.store.update_trade_state(
                    trade["trade_id"],
                    "entry_active",
                    note=(
                        "entry order acknowledged; exchange state requires reconciliation"
                        if intent_state in ("reconciling", "unknown_requires_reconciliation")
                        else "entry order open"
                    ),
                )
                # The acknowledgement can already include a partial execution.
                # Sync myTrades now and protect the confirmed acquired quantity;
                # do not wait for the next 60-second reconciliation sweep.
                refreshed = self.store.get_trade(trade["trade_id"])
                if refreshed is not None:
                    self._reconcile_entry_fills(refreshed)
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
        for trade in self.store.list_trades():
            if trade["trade_state"] in TERMINAL_TRADE_STATES:
                continue
            if trade["symbol"].upper() != symbol.upper():
                continue
            key = str(trade.get("signal_id") or trade["trade_id"])
            with self._hold_trade_lock(key, "on_tick"):
                current = self.store.get_trade(trade["trade_id"])
                if current is None or current["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                self._evaluate_trade_tick(current, price, received_at)

    def recover_on_startup(self) -> None:
        """Rehydrate active trades from durable state after a restart.

        Each non-terminal trade is reconciled with the exchange: the entry
        intent and any open TP/SL orders are queried, fills are recorded, and
        interrupted stop-loss/exit states are resumed.  Uncertain submissions
        are only queried — never resubmitted with a new client order id.
        """
        # Establish the fail-closed block before reconciliation. Each trade is
        # reconciled under its own lock; unrelated signals remain admissible.
        self._refresh_buy_block()
        try:
            for trade in self.store.list_trades():
                if trade["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                key = str(trade.get("signal_id") or trade["trade_id"])
                with self._hold_trade_lock(key, "recover_on_startup"):
                    current = self.store.get_trade(trade["trade_id"])
                    if current is None or current["trade_state"] in TERMINAL_TRADE_STATES:
                        continue
                    try:
                        self._reconcile_trade(current["trade_id"])
                    except Exception as exc:  # noqa: BLE001 - never block recovery
                        log.error("recovery reconcile failed for %s: %s", current["trade_id"], exc)
                        self._record_error(current, f"recovery failed: {exc}", "reconciliation")
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
            # The initial reference price may already be at/above the computed
            # trigger while still below Entry. Treat that as an already-fired
            # trigger and submit the normal LIMIT at Entry immediately; waiting
            # for another upward crossing would miss the valid entry window.
            if current < trigger_price:
                entry_mode = "conditional"
            else:
                trigger_price = None

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

    def _active_oco_coverage(self, trade: dict[str, Any]) -> Decimal:
        """Quantity covered by currently working native OCO parent orders."""
        covered = Decimal("0")
        for kind in ("tp1", "tp2"):
            for order in self._oco_orders(trade, kind):
                if order.get("state") not in ("open", "partially_filled"):
                    continue
                quantity = _dec(order.get("quantity")) or Decimal("0")
                sold = _dec(order.get("sold_qty")) or Decimal("0")
                covered += max(Decimal("0"), quantity - sold)
        return covered

    def _entry_remainder_outgrows_oco_coverage(self, trade: dict[str, Any]) -> bool:
        """Detect later entry fills that exceed the already-active OCO sizes."""
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return False
        intent = self.store.get_intent(intent_id)
        if intent is None or intent.get("state") not in ("open", "partially_filled"):
            return False
        if self._unattributed_oco_parent_cancellations(trade):
            return False  # the parent/child containment path owns this case
        sellable = self._remaining_base(trade)
        if sellable is None or sellable <= 0:
            return False
        return self._active_oco_coverage(trade) < sellable

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
        with self._buy_block_lock:
            self._refresh_buy_block_locked()

    def _refresh_buy_block_locked(self) -> None:
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
        """Submit a triggered LIMIT entry; activate only after durable MEXC ack.

        A blocked submission leaves the trade pending so a later fresh tick can
        retry after Pause/new-BUY gates clear. A claimed but uncertain submission
        is parked for reconciliation and is never blindly retried.
        """
        store = self.store
        with store.db.transaction() as conn:
            row = conn.execute(
                "SELECT trade_state, entry_intent_id FROM trades WHERE trade_id = ?",
                (trade["trade_id"],),
            ).fetchone()
        if row is None or row[0] != "pending_entry":
            return
        intent_id = row[1]
        intent = store.get_intent(intent_id) if intent_id else None
        if intent is None:
            store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note="conditional entry intent missing before submission",
            )
            return
        if self.engine is None:
            self._record_error(trade, "engine unavailable", "runtime")
            return

        try:
            # TradingEngine claims planned -> submitting atomically inside its
            # pause/readiness gate. Do not mutate the trade before that claim.
            self.engine._execute_intent(intent["intent_id"])
        except Exception as exc:  # noqa: BLE001
            self._record_error(trade, f"entry submission failed: {exc}", "exchange")
            self._notify_trade(trade, "ENTRY_SUBMISSION_FAILED", str(exc)[:500])
            current = store.get_intent(intent["intent_id"])
            if current is not None and submission_acknowledged(current):
                self.on_intent_executed(current)
            elif current is not None and current.get("state") in (
                "submitting",
                "reconciling",
                "unknown_requires_reconciliation",
            ):
                store.update_trade_state(
                    trade["trade_id"],
                    "needs_reconciliation",
                    note="entry submission was claimed but its exchange outcome is uncertain",
                )
            elif current is not None and current.get("state") in TERMINAL_INTENT_STATES:
                refreshed = store.get_trade(trade["trade_id"])
                if refreshed is not None:
                    self._settle_entry_state(refreshed)
            # A still-planned intent was not claimed (for example, Pause or a
            # new-BUY block). Keep pending_entry; the next fresh trigger can retry.
            return

        current = store.get_intent(intent["intent_id"])
        if current is None:
            store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note="entry intent disappeared after submission attempt",
            )
            return
        if submission_acknowledged(current):
            # This is the only path that advances pending_entry to entry_active.
            self.on_intent_executed(current)
        elif current.get("state") == "planned":
            # The engine's atomic submission gate declined the claim. Preserve
            # the waiting state; do not make a planned intent look submitted.
            return
        elif current.get("state") in TERMINAL_INTENT_STATES:
            refreshed = store.get_trade(trade["trade_id"])
            if refreshed is not None:
                self._settle_entry_state(refreshed)
        else:
            # A claimed intent without durable acknowledgement is not retryable.
            store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note=f"entry submission state {current.get('state')} is not a durable acknowledgement",
            )

    def _settle_entry_state(self, trade: dict[str, Any]) -> None:
        """Advance an entry and protect confirmed acquired quantity.

        Wait for fill synchronization before sizing OCO exits. A working entry
        with confirmed partial fills may receive protection before it is terminal;
        subsequent fills and any remaining coverage are handled by reconciliation.
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
        elif intent_state == "filled":
            # Filled according to the exchange but no fill records: never treat
            # this as a zero position.  Fail closed for manual reconciliation.
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note="entry reports filled but no fill records were persisted",
            )

    # ===================================================================
    # Take-profit handling
    # ===================================================================

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

    def _disable_legacy_exit(self, trade: dict[str, Any], target: str) -> None:
        """Fail closed instead of submitting a standalone take-profit order."""

        detail = (
            f"standalone {target} exit requested but disabled (OCO-only policy); "
            "no order was submitted — manual reconciliation required"
        )
        self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])

    def _do_tp_sale(self, trade: dict[str, Any], target: str) -> None:
        """Disabled: standalone take-profit orders are not part of the product."""

        self._disable_legacy_exit(trade, target)
        return

    def _create_trade_order(
        self,
        trade: dict[str, Any],
        kind: str,
        qty: Decimal,
        price: Decimal | None,
        *,
        order_type: str,
        tranche_id: str | None = None,
        attempt: int = 1,
        stop_trigger_price: Decimal | None = None,
    ) -> dict[str, Any]:
        group_id = tranche_id or self._oco_group_id(trade)
        client_order_id = self._client_order_id(trade["trade_id"], kind, attempt, tranche_id=tranche_id)
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
            stop_trigger_price=stop_trigger_price,
            attempt=attempt,
            oco_group_id=group_id,
            leg_role=kind,
        )

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

    def _list_oco_legs(self, group_id: str | None) -> list[dict[str, Any]]:
        """Defensive wrapper around store.list_oco_legs (mock stores may lack it)."""

        if group_id is None:
            return []
        lister = getattr(self.store, "list_oco_legs", None)
        if lister is None:
            return []
        try:
            return list(lister(group_id))
        except Exception as exc:  # noqa: BLE001 - projection must not crash a tick
            log.warning("OCO leg lookup failed for %s: %s", group_id, exc)
            return []

    def _oco_leg_targets(
        self,
        order: dict[str, Any],
        group_id: str,
        kind: str,
        *,
        siblings: list[dict[str, Any]] | None = None,
        existing_legs: dict[str, dict[str, Any]] | None = None,
    ) -> tuple[str, str]:
        """Return unique (TP, SL) projection targets for one OCO order row.

        Legacy single-tranche groups keep the historical ``tp1``/``tp1_sl``
        targets.  Whenever a group holds more than one row of the same kind
        (retry attempts or several same-kind rows), every row after the legacy
        owner gets a stable per-parent suffix derived from ``trade_order_id`` so
        two stop children can never overwrite each other in the same group.
        """

        siblings = list(siblings if siblings is not None else [order])
        existing_legs = existing_legs or {}
        legacy_tp, legacy_sl = kind, f"{kind}_sl"
        order_id = str(order.get("trade_order_id") or "")

        def _owned_by(order_leg: dict[str, Any] | None) -> bool:
            if order_leg is None:
                return False
            owner = str(order_leg.get("trade_order_id") or "")
            if owner:
                return bool(order_id) and owner == order_id
            # A legacy projection without an owner can only be claimed when
            # there is exactly one row of this kind to claim it.
            return len(siblings) == 1

        if _owned_by(existing_legs.get(legacy_tp)) or _owned_by(existing_legs.get(legacy_sl)):
            return legacy_tp, legacy_sl
        if len(siblings) == 1 and str(order.get("oco_group_id") or group_id) == group_id:
            return legacy_tp, legacy_sl
        order_key = order_id or str(order.get("client_order_id") or order.get("exchange_order_id") or "")
        if not order_key:
            # ``trade_order_id`` is the storage primary key; without a stable
            # identity a unique target cannot be derived, so keep the legacy
            # name rather than inventing a non-deterministic key.
            return legacy_tp, legacy_sl
        return f"{kind}:{order_key}", f"{kind}_sl:{order_key}"

    @staticmethod
    def _stop_leg_is_resolved(leg: dict[str, Any] | None, tp_order: dict[str, Any]) -> bool:
        """True when the stop leg is an independently observed child order."""

        if not leg:
            return False
        if str(leg.get("kind") or "") != "stop_loss":
            return False
        child_id = str(leg.get("exchange_order_id") or "")
        tp_id = str(tp_order.get("exchange_order_id") or "")
        if not child_id:
            return False
        if tp_id and child_id == tp_id:
            # Legacy projection copied the TP parent identity into the stop row.
            return False
        leg_owner = str(leg.get("trade_order_id") or "")
        tp_owner = str(tp_order.get("trade_order_id") or "")
        if leg_owner and tp_owner and leg_owner != tp_owner:
            # Never let a stop row belonging to another parent satisfy this one.
            return False
        if str(leg.get("not_observed_reason") or "").strip():
            return False
        if not leg.get("last_observed_at"):
            return False
        return str(leg.get("state") or "") in ("open", "partially_filled", "filled", "canceled", "rejected")

    def _unsettled_oco_stop_child_reason(self, trade: dict[str, Any]) -> str | None:
        """Describe a stop child that may still fill, so no replacement overlaps it."""

        group_id = self._oco_group_id(trade)
        if group_id is None:
            return None
        for leg in self._list_oco_legs(group_id):
            if str(leg.get("kind") or "") != "stop_loss":
                continue
            if str(leg.get("not_observed_reason") or "").strip():
                continue
            child_id = str(leg.get("exchange_order_id") or "")
            if not child_id:
                continue
            state = str(leg.get("state") or "")
            filled = _dec(leg.get("filled_qty")) or Decimal("0")
            intended = _dec(leg.get("intended_qty")) or Decimal("0")
            if state in ("open", "partially_filled"):
                return f"{child_id} state={state} filled={filled}/{intended}"
            if filled > 0 and intended > 0 and filled < intended:
                return f"{child_id} partial fill {filled}/{intended}"
        return None

    def _oco_stop_children_settled(self, trade: dict[str, Any]) -> bool:
        """True only when terminal OCO parents have proven terminal stop children.

        Active parents may legitimately keep an invisible stop placeholder while
        the native OCO is working.  Once a parent is terminal, however, its
        independently identified stop child must exist and be terminal before
        the trade can close.  A missing, ambiguous, open, or partial child fails
        closed.
        """

        group_id = self._oco_group_id(trade)
        if group_id is None:
            return True
        legs = {str(leg.get("target") or ""): leg for leg in self._list_oco_legs(group_id)}
        parents = self._list_trade_orders(trade["trade_id"])
        parents_by_id = {
            str(order.get("trade_order_id") or ""): order for order in parents if order.get("kind") in ("tp1", "tp2")
        }

        # Every terminal TP parent must have its own independently observed
        # stop child.  A missing leg or a legacy TP-id projection is unresolved.
        for kind in ("tp1", "tp2"):
            siblings = self._oco_orders(trade, kind)
            for order in siblings:
                if str(order.get("state") or "") not in TERMINAL_ORDER_STATES:
                    continue
                _tp_target, sl_target = self._oco_leg_targets(
                    order, group_id, kind, siblings=siblings, existing_legs=legs
                )
                if not self._stop_leg_is_resolved(legs.get(sl_target), order):
                    return False

        for leg in legs.values():
            if str(leg.get("kind") or "") != "stop_loss":
                continue
            reason = str(leg.get("not_observed_reason") or "").strip()
            if reason:
                # Only an active parent may keep this harmless placeholder.
                # A terminal parent with the same reason is not settled.
                if reason == "oco_stop_child_not_yet_visible" and not leg.get("exchange_order_id"):
                    parent = parents_by_id.get(str(leg.get("trade_order_id") or ""))
                    if parent and str(parent.get("state") or "") in ("open", "partially_filled"):
                        continue
                return False
            if str(leg.get("state") or "") not in TERMINAL_ORDER_STATES:
                return False
        return True

    def _sync_oco_legs(self, trade: dict[str, Any]) -> str | None:
        """Project every tranche OCO parent with unique TP and SL evidence keys."""
        group_id = self._oco_group_id(trade)
        if group_id is None:
            return None
        existing = {str(leg.get("target") or ""): leg for leg in self._list_oco_legs(group_id)}

        def _project(
            order: dict[str, Any],
            target: str,
            leg_role: str,
            *,
            filled_qty: Decimal | None = None,
            not_observed_reason: str | None = None,
            verified: bool | None = None,
        ) -> None:
            projected = dict(order)
            projected["target"] = target
            self.store.upsert_oco_leg(
                group_id=group_id,
                trade=trade,
                order=projected,
                state=str(order.get("state") or "unknown"),
                leg_role=leg_role,
                exchange_status=order.get("exchange_status"),
                cancel_reason=order.get("cancel_reason"),
                canceled_by_leg=order.get("canceled_by_leg"),
                evidence_id=order.get("evidence_id"),
                not_observed_reason=not_observed_reason,
                filled_qty=filled_qty,
                filled_quote=_dec(order.get("filled_quote")),
                fee_amount=_dec(order.get("fee_amount")),
                fee_asset=order.get("fee_asset"),
                attempt=int(str(order.get("attempt") or 0)),
                verified=(verified if verified is not None else order.get("not_observed_reason") in (None, "")),
            )

        for kind in ("tp1", "tp2"):
            siblings = self._oco_orders(trade, kind)
            for order in siblings:
                tp_target, sl_target = self._oco_leg_targets(
                    order, group_id, kind, siblings=siblings, existing_legs=existing
                )
                _project(order, tp_target, kind)
                existing_stop = existing.get(sl_target)
                if self._stop_leg_is_resolved(existing_stop, order):
                    # An independently discovered child owns this row.  Never
                    # overwrite its identity/state with the TP parent.
                    continue
                if existing_stop is not None and str(existing_stop.get("not_observed_reason") or "").startswith(
                    OCO_CHILD_DISCOVERY_BACKOFF_PREFIX
                ):
                    # Preserve the discovery retry clock; re-projecting the TP
                    # parent here would otherwise reset ``last_observed_at`` on
                    # every reconcile tick and park the retry forever.
                    continue
                state = str(order.get("state") or "unknown")
                if state in ("open", "partially_filled"):
                    # MEXC may not list the stop child until the OCO resolves.
                    # Keep an explicit placeholder, but never project the TP
                    # fill quantity into the stop leg.
                    placeholder = dict(order)
                    placeholder["exchange_order_id"] = None
                    placeholder["exchange_status"] = None
                    placeholder["state"] = "open"
                    placeholder["filled_quote"] = None
                    placeholder["fee_amount"] = None
                    placeholder["fee_asset"] = None
                    _project(
                        placeholder,
                        sl_target,
                        f"{kind}_sl",
                        filled_qty=Decimal("0"),
                        not_observed_reason="oco_stop_child_not_yet_visible",
                        verified=False,
                    )
                else:
                    # A terminal TP parent without a discovered child is
                    # unresolved.  Preserve the explicit reason and never copy
                    # the TP identity/fill into the stop leg.
                    unresolved = dict(existing_stop) if existing_stop else dict(order)
                    unresolved["target"] = sl_target
                    if str(unresolved.get("exchange_order_id") or "") == str(order.get("exchange_order_id") or ""):
                        unresolved["exchange_order_id"] = None
                    unresolved["state"] = "unknown_requires_reconciliation"
                    self.store.upsert_oco_leg(
                        group_id=group_id,
                        trade=trade,
                        order=unresolved,
                        state="unknown_requires_reconciliation",
                        leg_role=f"{kind}_sl",
                        exchange_status=unresolved.get("exchange_status"),
                        not_observed_reason=(unresolved.get("not_observed_reason") or "oco_stop_child_unresolved"),
                        filled_qty=Decimal("0"),
                        verified=False,
                    )
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
        """Do not infer TP1/TP2 as siblings; each is a separate native OCO parent.

        The true sibling is the stop child inside that same parent. Its execution
        must be attributed from exchange child-order/fill evidence, not another
        TP parent (especially when several fill tranches exist).
        """
        del trade
        return {}

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

    def _unattributed_oco_parent_cancellations(self, trade: dict[str, Any]) -> list[dict[str, Any]]:
        """Return canceled native-OCO parent rows whose executed child is unknown.

        MEXC can report the OCO parent as CANCELED when either its TP or SL
        child executes.  The parent row alone cannot identify the executed child
        or its fills, so it is unsafe to re-arm that allocation.  A canceled
        parent is cleared only after its stop child has been uniquely matched in
        allOrders and its status was queryable by the exact child id.
        """

        group_id = self._oco_group_id(trade)
        legs = {str(leg.get("target") or ""): leg for leg in self._list_oco_legs(group_id)} if group_id else {}
        unresolved: list[dict[str, Any]] = []
        siblings_by_kind = {kind: self._oco_orders(trade, kind) for kind in ("tp1", "tp2")}
        for order in self._list_trade_orders(trade["trade_id"]):
            if order.get("kind") not in ("tp1", "tp2") or order.get("state") != "canceled":
                continue
            if group_id is not None:
                kind = str(order.get("kind"))
                _tp_target, sl_target = self._oco_leg_targets(
                    order,
                    group_id,
                    kind,
                    siblings=siblings_by_kind.get(kind, []),
                    existing_legs=legs,
                )
                if self._stop_leg_is_resolved(legs.get(sl_target), order):
                    continue
            unresolved.append(order)
        return unresolved

    # ------------------------------------------------- OCO child discovery

    def _discover_oco_stop_children(self, trade: dict[str, Any]) -> None:
        """Find stop children for terminal TP parents using exact allOrders rows.

        Discovery is deliberately limited to terminal (FILLED/CANCELED) TP
        parents: MEXC does not reliably list the stop child while the OCO is
        still active, and the TP parent already provides coverage then.
        """

        engine = self.engine
        if engine is None or not hasattr(engine, "query_all_orders"):
            return
        group_id = self._oco_group_id(trade)
        if group_id is None:
            return
        legs = {str(leg.get("target") or ""): leg for leg in self._list_oco_legs(group_id)}
        all_orders = self._list_trade_orders(trade["trade_id"])
        tp_parent_ids = {
            str(order.get("exchange_order_id"))
            for order in all_orders
            if order.get("kind") in ("tp1", "tp2") and order.get("exchange_order_id")
        }
        claimed: dict[str, str] = {
            str(leg.get("exchange_order_id")): str(leg.get("trade_order_id") or "")
            for leg in legs.values()
            if str(leg.get("kind") or "") == "stop_loss"
            and leg.get("exchange_order_id")
            and str(leg.get("exchange_order_id")) not in tp_parent_ids
        }
        for kind in ("tp1", "tp2"):
            siblings = self._oco_orders(trade, kind)
            for order in siblings:
                if str(order.get("state") or "") not in ("filled", "canceled"):
                    continue
                if not order.get("exchange_order_id"):
                    continue
                _tp_target, sl_target = self._oco_leg_targets(
                    order, group_id, kind, siblings=siblings, existing_legs=legs
                )
                existing = legs.get(sl_target)
                if self._stop_leg_is_resolved(existing, order):
                    child_id = str((existing or {}).get("exchange_order_id") or "")
                    if child_id:
                        claimed[child_id] = str(order.get("trade_order_id") or "")
                    self._refresh_discovered_oco_stop_child(trade, existing, group_id, sl_target)
                    continue
                if existing is not None and self._oco_discovery_backoff_remaining(existing) > 0:
                    log.debug(
                        "OCO stop-child discovery parked for %s after %s",
                        order.get("trade_order_id"),
                        existing.get("not_observed_reason"),
                    )
                    continue
                discovered_id = self._discover_one_oco_stop_child(
                    trade, order, kind, sl_target, group_id, existing=existing, claimed=claimed
                )
                if discovered_id:
                    claimed[discovered_id] = str(order.get("trade_order_id") or "")

    @staticmethod
    def _oco_discovery_streak(reason: str) -> int:
        prefix = OCO_CHILD_DISCOVERY_BACKOFF_PREFIX
        if not reason.startswith(prefix):
            return 0
        for part in reason[len(prefix) :].split(":"):
            if part.startswith("count="):
                try:
                    return max(0, int(part.split("=", 1)[1]))
                except ValueError:
                    return 0
        return 0

    def _oco_discovery_backoff_remaining(self, leg: dict[str, Any]) -> float:
        """Reuse the bounded exponential backoff policy for discovery retries."""

        streak = self._oco_discovery_streak(str(leg.get("not_observed_reason") or ""))
        if streak <= 0:
            return 0.0
        last = _dec_ts(leg.get("last_observed_at"))
        if last is None:
            return 0.0
        delay = min(60.0 * (2 ** (streak - 1)), OCO_CHILD_DISCOVERY_BACKOFF_MAX_S)
        elapsed = max(0.0, (self._now() - last).total_seconds())
        return max(0.0, delay - elapsed)

    @staticmethod
    def _oco_claimed_for_parent(claimed_ids: dict[str, str] | None, parent_order_id: str) -> set[str]:
        """Exclude other parents' children, but allow this parent's prior candidate.

        A stop child whose exact status query failed is persisted with its
        order id plus an unresolved reason.  On retry that same id is still the
        parent's candidate and must not be filtered out as "already claimed".
        """

        return {child_id for child_id, owner in (claimed_ids or {}).items() if owner != parent_order_id}

    @staticmethod
    def _select_oco_stop_child(
        rows: list[dict[str, Any]],
        *,
        tp_id: str,
        expected_stop_limit: Decimal | None,
        claimed_ids: set[str],
    ) -> tuple[dict[str, Any] | None, str]:
        """Return the single exact stop child for one TP parent, or a reason.

        No order-id arithmetic and no fuzzy fallback is permitted.  A candidate
        must agree on every observable field, and a row already claimed by
        another parent is not eligible.  Anything else fails closed.
        """

        if expected_stop_limit is None:
            return None, "oco_stop_child_missing_stop_limit_price"
        tp_row = next(
            (row for row in rows if isinstance(row, dict) and str(row.get("orderId")) == tp_id),
            None,
        )
        if tp_row is None:
            return None, "oco_stop_child_tp_not_found"
        if str(tp_row.get("type") or "").upper() != "OCO_LIMIT_ORDER":
            return None, "oco_stop_child_tp_type_mismatch"
        tp_time = _int_ms(tp_row.get("time"))
        tp_orig = _dec(tp_row.get("origQty"))
        if tp_time is None or tp_orig is None:
            return None, "oco_stop_child_tp_row_incomplete"
        symbol = str(tp_row.get("symbol") or "").upper()
        side = str(tp_row.get("side") or "").upper()
        if not symbol or not side:
            return None, "oco_stop_child_tp_row_incomplete"

        candidates: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("orderId")) == tp_id:
                continue
            if str(row.get("orderId") or "") in claimed_ids:
                continue
            if str(row.get("type") or "").upper() != "OCO_STOP_LIMIT_ORDER":
                continue
            if str(row.get("symbol") or "").upper() != symbol:
                continue
            if str(row.get("side") or "").upper() != side:
                continue
            if _int_ms(row.get("time")) != tp_time:
                continue
            if _dec(row.get("origQty")) != tp_orig:
                continue
            if _dec(row.get("price")) != expected_stop_limit:
                continue
            candidates.append(row)

        if len(candidates) != 1:
            return None, f"oco_stop_child_candidate_count:{len(candidates)}"
        child = candidates[0]
        child_id = str(child.get("orderId") or "")
        if not child_id or child_id == tp_id:
            return None, "oco_stop_child_invalid_order_id"
        return child, ""

    def _discover_one_oco_stop_child(
        self,
        trade: dict[str, Any],
        tp_order: dict[str, Any],
        kind: str,
        sl_target: str,
        group_id: str,
        *,
        existing: dict[str, Any] | None = None,
        claimed: dict[str, str] | None = None,
    ) -> str | None:
        engine = self.engine
        if engine is None or not hasattr(engine, "query_all_orders"):
            self._discover_stop_unresolved(
                trade, tp_order, kind, sl_target, group_id, "oco_stop_child_engine_unavailable", existing
            )
            return None
        tp_id = str(tp_order.get("exchange_order_id") or "")
        trigger_price = _dec(tp_order.get("stop_trigger_price"))
        # ``price`` on an OCO_STOP_LIMIT_ORDER row is the stop-limit price, not
        # the trigger.  The limit is the trade's configured SL; legacy local
        # rows stored the trigger (SL+ticks) in ``stop_trigger_price``.
        # The trigger is not a safe substitute for the stop-limit price.
        # If the persisted SL limit is missing, discovery must fail closed.
        stop_limit_price = _dec(trade.get("sl_price"))
        if not tp_id:
            self._discover_stop_unresolved(
                trade, tp_order, kind, sl_target, group_id, "oco_stop_child_missing_tp_order_id", existing
            )
            return None
        if stop_limit_price is None:
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                "oco_stop_child_missing_stop_limit_price",
                existing,
            )
            return None
        created = _dec_ts(tp_order.get("created_at"))
        if created is None:
            self._discover_stop_unresolved(
                trade, tp_order, kind, sl_target, group_id, "oco_stop_child_missing_creation_time", existing
            )
            return None
        created_ms = int(created.timestamp() * 1000)
        now_ms = int(self._now().timestamp() * 1000)
        if created_ms < now_ms - OCO_CHILD_DISCOVERY_HISTORY_MS:
            self._discover_stop_unresolved(
                trade, tp_order, kind, sl_target, group_id, "oco_stop_child_history_expired", existing
            )
            return None
        start_ms = max(0, created_ms - OCO_CHILD_DISCOVERY_MARGIN_MS)
        end_ms = created_ms + OCO_CHILD_DISCOVERY_MARGIN_MS
        try:
            rows = engine.query_all_orders(
                symbol=str(trade["symbol"]),
                start_time_ms=start_ms,
                end_time_ms=end_ms,
                limit=OCO_CHILD_DISCOVERY_QUERY_LIMIT,
                trade=trade,
                trade_order_id=tp_order.get("trade_order_id"),
            )
        except Exception as exc:  # noqa: BLE001 - history read is fail-closed
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                f"oco_stop_child_query_failed:{exc.__class__.__name__}",
                existing,
            )
            return None
        if not isinstance(rows, list):
            self._discover_stop_unresolved(
                trade, tp_order, kind, sl_target, group_id, "oco_stop_child_query_not_a_list", existing
            )
            return None
        if len(rows) >= OCO_CHILD_DISCOVERY_QUERY_LIMIT:
            # allOrders has a hard 1000-row ceiling and no pagination cursor.
            # A saturated response cannot prove candidate uniqueness.
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                "oco_stop_child_history_result_limit_reached",
                existing,
                stop_trigger_price=trigger_price,
                stop_limit_price=stop_limit_price,
            )
            return None

        child, reason = self._select_oco_stop_child(
            rows,
            tp_id=tp_id,
            expected_stop_limit=stop_limit_price,
            claimed_ids=self._oco_claimed_for_parent(claimed, str(tp_order.get("trade_order_id") or "")),
        )
        if child is None:
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                reason,
                existing,
                stop_trigger_price=trigger_price,
                stop_limit_price=stop_limit_price,
            )
            return None
        return self._query_and_persist_oco_stop_child(
            trade,
            tp_order,
            kind,
            sl_target,
            group_id,
            child,
            stop_limit_price=stop_limit_price,
            stop_trigger_price=trigger_price,
            previous=existing,
        )

    def _query_and_persist_oco_stop_child(
        self,
        trade: dict[str, Any],
        tp_order: dict[str, Any],
        kind: str,
        sl_target: str,
        group_id: str,
        child: dict[str, Any],
        *,
        stop_limit_price: Decimal,
        stop_trigger_price: Decimal | None,
        previous: dict[str, Any] | None = None,
    ) -> str | None:
        child_id = str(child.get("orderId") or "")
        child_order = {
            "trade_order_id": tp_order.get("trade_order_id"),
            "trade_id": trade.get("trade_id"),
            "symbol": trade.get("symbol"),
            "exchange_order_id": child_id,
            "client_order_id": None,
        }
        fresh = self._query_order(child_order)
        if fresh is None:
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                "oco_stop_child_status_query_failed",
                existing=previous,
                exchange_order_id=child_id,
                child=child,
                stop_trigger_price=stop_trigger_price,
                stop_limit_price=stop_limit_price,
            )
            return None
        raw_state = str(fresh.get("state") or fresh.get("status") or "")
        mapped = self._map_order_state(raw_state)
        sold = _dec(fresh.get("executed_qty")) or Decimal("0")
        reported = _dec(child.get("executedQty")) or Decimal("0")
        sold = max(sold, reported)
        if mapped == "filled" and sold <= 0:
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                "oco_stop_child_filled_without_quantity",
                existing=previous,
                exchange_order_id=child_id,
                child=child,
                stop_trigger_price=stop_trigger_price,
                stop_limit_price=stop_limit_price,
            )
            return None
        if mapped not in ("open", "partially_filled", "filled", "canceled", "rejected"):
            self._discover_stop_unresolved(
                trade,
                tp_order,
                kind,
                sl_target,
                group_id,
                f"oco_stop_child_status_{mapped}",
                existing=previous,
                exchange_order_id=child_id,
                child=child,
                stop_trigger_price=stop_trigger_price,
                stop_limit_price=stop_limit_price,
            )
            return None
        fee_amount: Decimal | None = None
        fee_asset: str | None = None
        if sold > 0:
            fee_amount, fee_asset = self._query_oco_child_fees(trade, child_id)
        self.store.upsert_oco_leg(
            group_id=group_id,
            trade=trade,
            order={
                "trade_order_id": tp_order.get("trade_order_id"),
                "target": sl_target,
                "symbol": trade.get("symbol"),
                "price": child.get("price") or str(stop_limit_price),
                "stop_trigger_price": (
                    str(stop_trigger_price) if stop_trigger_price is not None else tp_order.get("stop_trigger_price")
                ),
                "quantity": child.get("origQty"),
                "exchange_order_id": child_id,
            },
            state=mapped,
            leg_role=f"{kind}_sl",
            exchange_status=raw_state or str(child.get("status") or ""),
            filled_qty=sold,
            filled_quote=_dec(fresh.get("cumulative_quote")),
            fee_amount=fee_amount,
            fee_asset=fee_asset,
            not_observed_reason="",
            verified=True,
        )
        return child_id

    def _refresh_discovered_oco_stop_child(
        self,
        trade: dict[str, Any],
        leg: dict[str, Any] | None,
        group_id: str,
        sl_target: str,
    ) -> None:
        """Refresh fills for a discovered stop child that is still working."""

        if not leg:
            return
        if str(leg.get("state") or "") not in ("open", "partially_filled"):
            return
        child_id = str(leg.get("exchange_order_id") or "")
        if not child_id:
            return
        parent_order_id = leg.get("trade_order_id")
        fresh = self._query_order(
            {
                "trade_order_id": parent_order_id,
                "trade_id": trade.get("trade_id"),
                "symbol": trade.get("symbol"),
                "exchange_order_id": child_id,
                "client_order_id": None,
            }
        )
        if fresh is None:
            return
        raw_state = str(fresh.get("state") or fresh.get("status") or "")
        mapped = self._map_order_state(raw_state)
        if mapped not in ("open", "partially_filled", "filled", "canceled", "rejected"):
            return
        previous_filled = _dec(leg.get("filled_qty")) or Decimal("0")
        sold = _dec(fresh.get("executed_qty")) or Decimal("0")
        sold = max(sold, previous_filled)
        fee_amount = _dec(leg.get("fee_amount"))
        fee_asset = leg.get("fee_asset")
        # Fee evidence is only complete once fills stop advancing.  Refresh it
        # when the child becomes terminal (or its fill quantity advances) so a
        # child first seen open cannot keep a null/partial fee forever.
        if sold > 0 and (mapped in TERMINAL_ORDER_STATES or sold > previous_filled or fee_amount is None):
            refreshed_fee, refreshed_asset = self._query_oco_child_fees(trade, child_id)
            if refreshed_fee is not None:
                fee_amount, fee_asset = refreshed_fee, refreshed_asset
        self.store.upsert_oco_leg(
            group_id=group_id,
            trade=trade,
            order={
                "trade_order_id": parent_order_id,
                "target": sl_target,
                "symbol": trade.get("symbol"),
                "price": leg.get("intended_price"),
                "stop_trigger_price": leg.get("intended_trigger_price"),
                "quantity": leg.get("intended_qty"),
                "exchange_order_id": child_id,
            },
            state=mapped,
            leg_role="stop_loss",
            exchange_status=raw_state,
            filled_qty=sold,
            filled_quote=_dec(fresh.get("cumulative_quote")),
            fee_amount=fee_amount,
            fee_asset=fee_asset,
            not_observed_reason="",
            verified=True,
        )

    def _discover_stop_unresolved(
        self,
        trade: dict[str, Any],
        tp_order: dict[str, Any],
        kind: str,
        sl_target: str,
        group_id: str,
        reason: str,
        existing: dict[str, Any] | None = None,
        *,
        exchange_order_id: str | None = None,
        child: dict[str, Any] | None = None,
        stop_trigger_price: Decimal | None = None,
        stop_limit_price: Decimal | None = None,
    ) -> None:
        """Persist a failed discovery outcome and arm the bounded retry clock."""

        previous_reason = str((existing or {}).get("not_observed_reason") or "")
        streak = self._oco_discovery_streak(previous_reason) + 1
        tagged = f"{OCO_CHILD_DISCOVERY_BACKOFF_PREFIX}count={streak}:reason={reason}"
        self._persist_oco_stop_unresolved(
            trade,
            tp_order,
            kind,
            sl_target,
            group_id,
            tagged,
            exchange_order_id=exchange_order_id,
            child=child,
            stop_trigger_price=stop_trigger_price,
            stop_limit_price=stop_limit_price,
        )

    def _persist_oco_stop_unresolved(
        self,
        trade: dict[str, Any],
        tp_order: dict[str, Any],
        kind: str,
        sl_target: str,
        group_id: str,
        reason: str,
        *,
        exchange_order_id: str | None = None,
        child: dict[str, Any] | None = None,
        stop_trigger_price: Decimal | None = None,
        stop_limit_price: Decimal | None = None,
    ) -> None:
        try:
            self.store.upsert_oco_leg(
                group_id=group_id,
                trade=trade,
                order={
                    "trade_order_id": tp_order.get("trade_order_id"),
                    "target": sl_target,
                    "symbol": trade.get("symbol"),
                    "price": (
                        (child or {}).get("price")
                        or (str(stop_limit_price) if stop_limit_price is not None else None)
                        or tp_order.get("price")
                    ),
                    "stop_trigger_price": (
                        str(stop_trigger_price)
                        if stop_trigger_price is not None
                        else tp_order.get("stop_trigger_price")
                    ),
                    "quantity": (child or {}).get("origQty") or tp_order.get("quantity"),
                    "exchange_order_id": exchange_order_id,
                },
                state="unknown_requires_reconciliation",
                leg_role=f"{kind}_sl",
                not_observed_reason=reason,
                filled_qty=Decimal("0"),
                verified=False,
            )
        except Exception as exc:  # noqa: BLE001 - evidence persistence must not crash
            log.warning("could not persist unresolved OCO stop child for %s: %s", trade.get("trade_id"), exc)
        try:
            self.store.audit(
                trade["trade_id"],
                "reconcile",
                "oco_stop_child_unresolved",
                reason,
            )
        except Exception as exc:  # noqa: BLE001 - audit is advisory
            log.warning("could not audit unresolved OCO stop child for %s: %s", trade.get("trade_id"), exc)

    def _query_oco_child_fees(self, trade: dict[str, Any], child_id: str) -> tuple[Decimal | None, str | None]:
        """Aggregate myTrades fees for one child id; unknown stays unknown."""

        if self.engine is None or not hasattr(self.engine, "query_order_trades"):
            return None, None
        try:
            rows = self.engine.query_order_trades(
                symbol=str(trade["symbol"]),
                exchange_order_id=child_id,
                limit=1000,
                trade=trade,
            )
        except Exception:  # noqa: BLE001 - fee evidence is advisory only
            return None, None
        if not isinstance(rows, list):
            return None, None
        totals: dict[str, Decimal] = {}
        for row in rows:
            if not isinstance(row, dict):
                return None, None
            amount = _dec(row.get("commission"))
            asset = row.get("commissionAsset")
            if amount is None or not asset:
                continue
            key = str(asset)
            totals[key] = totals.get(key, Decimal("0")) + amount
        if len(totals) == 1:
            asset, amount = next(iter(totals.items()))
            return amount, asset
        return None, None

    def _ocos_terminal(self, trade: dict[str, Any]) -> bool:
        """True when both OCO orders have reached a terminal state."""
        orders = self._list_trade_orders(trade["trade_id"])
        oco_orders = [o for o in orders if o.get("kind") in ("tp1", "tp2")]
        if not oco_orders:
            return False
        return all(o["state"] in TERMINAL_ORDER_STATES for o in oco_orders)

    def _oco_tranche_ids(self, trade: dict[str, Any]) -> list[str]:
        base = self._oco_group_id(trade)
        if base is None:
            return []
        ids = {
            str(order.get("oco_group_id") or base)
            for order in self._list_trade_orders(trade["trade_id"])
            if order.get("kind") in ("tp1", "tp2")
        }
        return sorted(ids, key=lambda value: (value != base, value))

    def _planned_oco_reserved(self, trade: dict[str, Any], group_id: str) -> Decimal:
        """Latest intended/live allocation per tranche target, including pending rows."""
        latest: dict[tuple[str, str], dict[str, Any]] = {}
        for order in self._list_trade_orders(trade["trade_id"]):
            if order.get("kind") not in ("tp1", "tp2"):
                continue
            tranche = str(order.get("oco_group_id") or group_id)
            key = (tranche, str(order.get("kind")))
            previous = latest.get(key)
            rank = (int(str(order.get("attempt") or 0)), str(order.get("created_at") or ""))
            previous_rank = (
                (int(str(previous.get("attempt") or 0)), str(previous.get("created_at") or "")) if previous else None
            )
            if previous is None or (previous_rank is not None and rank >= previous_rank):
                latest[key] = order
        reserving = {
            "pending",
            "submitting",
            "open",
            "partially_filled",
            "cancel_requested",
            "unknown_requires_reconciliation",
            "rejected",
        }
        total = Decimal("0")
        for order in latest.values():
            if order.get("state") not in reserving:
                continue
            qty = _dec(order.get("quantity")) or Decimal("0")
            sold = _dec(order.get("sold_qty")) or Decimal("0")
            total += max(Decimal("0"), qty - sold)
        return total

    def _next_oco_tranche_id(self, trade: dict[str, Any], group_id: str) -> str:
        prefix = f"{group_id}:t"
        highest = 0
        for tranche_id in self._oco_tranche_ids(trade):
            if tranche_id.startswith(prefix):
                suffix = tranche_id[len(prefix) :]
                if suffix.isdigit():
                    highest = max(highest, int(suffix))
        return f"{prefix}{highest + 1:04d}"

    def _create_oco_tranche(
        self,
        trade: dict[str, Any],
        tranche_id: str,
        allocations: dict[str, Decimal],
        prices: dict[str, Decimal],
        stop_price: Decimal,
    ) -> list[dict[str, Any]]:
        specs: list[dict[str, Any]] = []
        for kind in ("tp1", "tp2"):
            client_order_id = self._client_order_id(trade["trade_id"], kind, 1, tranche_id=tranche_id)
            qty = allocations[kind]
            specs.append(
                {
                    "trade_order_id": f"to-{client_order_id}",
                    "trade_id": trade["trade_id"],
                    "kind": kind,
                    "client_order_id": client_order_id,
                    "symbol": trade["symbol"],
                    "side": "sell",
                    "order_type": "OCO_STOP_LIMIT_ORDER",
                    "price": prices[kind],
                    "quantity": qty,
                    "allocated_qty": qty,
                    "stop_trigger_price": stop_price,
                    "attempt": 1,
                    "oco_group_id": tranche_id,
                    "leg_role": kind,
                }
            )
        create_pair = getattr(self.store, "create_trade_order_pair", None)
        if callable(create_pair):
            return create_pair(specs)
        # Compatibility for small test doubles; production TradeStore persists
        # the two pending rows atomically before either exchange write.
        return [self.store.create_trade_order(**spec) for spec in specs]

    def _place_oco_exits(self, trade: dict[str, Any]) -> None:
        """Protect each incremental Entry-fill tranche with two independent OCOs."""
        if self.engine is None:
            self._record_error(trade, "no engine available for OCO exits", "runtime")
            self._refresh_buy_block()
            return

        intent_id = trade.get("entry_intent_id")
        acquired = _dec(trade.get("acquired_base_qty")) or Decimal("0")
        if intent_id:
            intent = self.store.get_intent(intent_id)
            if intent is not None and int(intent.get("fill_sync_required") or 0) == 1:
                detail = (
                    "entry fill synchronization is incomplete; existing OCO coverage "
                    "cannot be treated as complete until MEXC fills are reconciled"
                )
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self._sync_oco_group_state(trade, note=detail)
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
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=f"OCO not placed: {problem}")
            self._refresh_buy_block()
            return
        assert sl_price is not None and tp1_price is not None and tp2_price is not None

        sellable = self._sellable_quantity(trade)
        if sellable is None:
            detail = (
                f"OCO tranche sizing unknown for {trade.get('trade_id')} {trade.get('symbol')}; fill/fee sync required"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self._refresh_buy_block()
            return
        if sellable <= 0:
            if any(o.get("state") == "filled" for o in self._list_trade_orders(trade["trade_id"])):
                self._finalize_trade(trade, reason="all acquired tranche allocations exited")
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
                "allocation_policy": "per_entry_fill_tranche",
            },
            protection_status="placing",
        )
        current_group = self.store.get_oco_group(trade["trade_id"])
        if current_group is None or str(current_group.get("state")) == "intended":
            self.store.set_oco_group_state(trade["trade_id"], "submitting")
        trade = self._refresh(trade)

        if self._unattributed_oco_parent_cancellations(trade):
            detail = (
                "an OCO parent is canceled with child execution not independently "
                "attributed; no new tranche may be submitted"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self._sync_oco_group_state(trade, note=detail)
            return

        unsettled_child = self._unsettled_oco_stop_child_reason(trade)
        if unsettled_child is not None:
            detail = (
                f"an OCO stop child is only partially resolved ({unsettled_child}); "
                "remaining inventory cannot be re-protected without overlapping an "
                "order that may still fill — no new OCO submitted"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self._sync_oco_group_state(trade, note=detail)
            return

        ok, detail = self._verify_oco_invariant(trade)
        if not ok:
            self._sync_oco_group_state(trade, note=detail[:2000])
            return

        for order in self._list_trade_orders(trade["trade_id"]):
            if order.get("kind") not in ("tp1", "tp2") or order.get("state") != "filled":
                continue
            allocated = _dec(order.get("allocated_qty")) or _dec(order.get("quantity")) or Decimal("0")
            sold = _dec(order.get("sold_qty")) or Decimal("0")
            if allocated > 0 and sold < allocated:
                detail = (
                    f"OCO parent {order.get('trade_order_id')} is FILLED but executed "
                    f"quantity {sold} is below allocation {allocated}; tranche sizing is ambiguous"
                )
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self._sync_oco_group_state(trade, note=detail)
                return

        # Complete a legacy single-tranche group that has exactly one target
        # row. Reuse its existing size and create only the missing counterpart;
        # an oversized target is never resized or reinterpreted as a new tranche.
        existing_tranches = self._oco_tranche_ids(trade)
        if existing_tranches == [group_id]:
            by_kind = {
                kind: [
                    order
                    for order in self._oco_orders(trade, kind)
                    if str(order.get("oco_group_id") or group_id) == group_id
                ]
                for kind in ("tp1", "tp2")
            }
            present = [kind for kind, rows in by_kind.items() if rows]
            missing = [kind for kind, rows in by_kind.items() if not rows]
            if len(present) == 1 and len(missing) == 1:
                present_kind, missing_kind = present[0], missing[0]
                existing = by_kind[present_kind][0]
                if existing.get("state") not in ("open", "partially_filled", "pending", "rejected"):
                    detail = (
                        "legacy OCO tranche has only one target with a terminal/uncertain "
                        "outcome; missing target cannot be inferred"
                    )
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
                tp1_pct = _dec(trade.get("tp1_allocation_pct")) or Decimal("50")
                tp2_pct = _dec(trade.get("tp2_allocation_pct")) or Decimal("100")
                expected_tp1 = self._quantize_qty(trade, sellable * tp1_pct / Decimal("100"))
                expected_tp2 = self._quantize_qty(
                    trade, max(Decimal("0"), sellable - expected_tp1) * tp2_pct / Decimal("100")
                )
                expected = {"tp1": expected_tp1, "tp2": expected_tp2}
                existing_qty = _dec(existing.get("quantity")) or Decimal("0")
                if existing_qty != expected[present_kind]:
                    detail = (
                        f"legacy {present_kind} allocation {existing_qty} does not match "
                        f"expected tranche allocation {expected[present_kind]} for sellable "
                        f"inventory {sellable}; refusing resize"
                    )
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self.store.audit(trade["trade_id"], "risk", "oco_invariant_would_violate", detail)
                    self._notify_trade(trade, "OCO_INVARIANT_VIOLATED", detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
                missing_price = tp1_price if missing_kind == "tp1" else tp2_price
                missing_qty = expected[missing_kind]
                reason = (
                    self._validate_sell(
                        trade, missing_qty, self._quantize_price(trade, missing_price), order_type="LIMIT"
                    )
                    if missing_qty > 0
                    else "missing target allocation is below lot size"
                )
                if reason:
                    detail = f"legacy OCO tranche missing {missing_kind} cannot be completed: {reason}"
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self._notify_trade(trade, "OCO_MINIMUM_NOT_MET", detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
                self._create_trade_order(
                    trade,
                    missing_kind,
                    missing_qty,
                    self._quantize_price(trade, missing_price),
                    order_type="OCO_STOP_LIMIT_ORDER",
                    tranche_id=group_id,
                    attempt=1,
                    stop_trigger_price=sl_price,
                )
                self.store.update_trade_fields(
                    trade["trade_id"], tp1_allocated_qty=expected_tp1, tp2_allocated_qty=expected_tp2
                )
                trade = self._refresh(trade)

        planned_reserved = self._planned_oco_reserved(trade, group_id)
        if planned_reserved > sellable:
            detail = (
                f"planned OCO tranche allocations {planned_reserved} exceed "
                f"sellable inventory {sellable}; no further OCO writes"
            )
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
            self._notify_trade(trade, "OCO_INVARIANT_VIOLATED", detail)
            self._sync_oco_group_state(trade, note=detail)
            return

        uncovered = max(Decimal("0"), sellable - planned_reserved)
        if uncovered > 0:
            tp1_pct = _dec(trade.get("tp1_allocation_pct")) or Decimal("50")
            tp2_pct = _dec(trade.get("tp2_allocation_pct")) or Decimal("100")
            tp1_qty = self._quantize_qty(trade, uncovered * tp1_pct / Decimal("100"))
            tp2_pool = max(Decimal("0"), uncovered - tp1_qty)
            tp2_qty = self._quantize_qty(trade, tp2_pool * tp2_pct / Decimal("100"))
            allocations = {"tp1": tp1_qty, "tp2": tp2_qty}
            prices = {"tp1": self._quantize_price(trade, tp1_price), "tp2": self._quantize_price(trade, tp2_price)}
            for kind in ("tp1", "tp2"):
                qty = allocations[kind]
                reason = (
                    self._validate_sell(trade, qty, prices[kind], order_type="LIMIT")
                    if qty > 0
                    else "allocation below lot size"
                )
                if reason:
                    detail = f"new OCO tranche cannot be fully allocated ({kind}={qty}): {reason}"
                    self._record_residual(trade, kind, uncovered, detail)
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                    self._notify_trade(trade, "OCO_MINIMUM_NOT_MET", detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
            tranche_id = self._next_oco_tranche_id(trade, group_id)
            try:
                self._create_oco_tranche(trade, tranche_id, allocations, prices, sl_price)
            except Exception as exc:
                detail = f"could not durably create both OCO rows for tranche {tranche_id}: {exc}"
                self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail[:2000])
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self._sync_oco_group_state(trade, note=detail)
                return
            self.store.update_trade_fields(
                trade["trade_id"],
                tp1_allocated_qty=(_dec(trade.get("tp1_allocated_qty")) or Decimal("0")) + tp1_qty,
                tp2_allocated_qty=(_dec(trade.get("tp2_allocated_qty")) or Decimal("0")) + tp2_qty,
            )
            trade = self._refresh(trade)

        tranche_ids = self._oco_tranche_ids(trade)
        required: list[tuple[str, str, Decimal, Decimal]] = []
        for tranche_id in tranche_ids:
            for kind, price in (("tp1", tp1_price), ("tp2", tp2_price)):
                rows = [
                    order
                    for order in self._oco_orders(trade, kind)
                    if str(order.get("oco_group_id") or group_id) == tranche_id
                ]
                if not rows:
                    detail = (
                        f"OCO tranche {tranche_id} is missing required {kind} order row; refusing inferred replacement"
                    )
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
                initial = min(
                    rows, key=lambda row: (int(str(row.get("attempt") or 0)), str(row.get("created_at") or ""))
                )
                qty = _dec(initial.get("allocated_qty")) or _dec(initial.get("quantity")) or Decimal("0")
                if qty <= 0:
                    detail = f"OCO tranche {tranche_id} has invalid {kind} quantity"
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self._sync_oco_group_state(trade, note=detail)
                    return
                required.append((tranche_id, kind, qty, self._quantize_price(trade, price)))

        results: dict[str, str] = {}
        for tranche_id, kind, qty, price in required:
            results[f"{tranche_id}:{kind}"] = self._ensure_oco_leg(
                trade, kind, qty, price, sl_price, tranche_id=tranche_id
            )
            post_ok, post_detail = self._verify_oco_invariant(self._refresh(trade))
            if not post_ok:
                self._sync_oco_group_state(trade, note=post_detail[:2000])
                return
            trade = self._refresh(trade)

        working = [key for key, value in results.items() if value == "confirmed"]
        completed = [key for key, value in results.items() if value == "filled"]
        confirmed = working + completed
        uncertain = [key for key, value in results.items() if value in ("uncertain", "retry_pending")]
        failed = [key for key, value in results.items() if value in ("rejected", "failed", "canceled")]
        trade = self._refresh(trade)
        self._sync_oco_legs(trade)
        coverage = self._active_oco_coverage(trade)
        remaining_cov = self._remaining_base(trade)
        if remaining_cov is None:
            detail = "sellable quantity became unknown after OCO tranche submission"
            self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
            self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
            self._sync_oco_group_state(trade, note=detail)
            return
        if remaining_cov <= 0:
            self._finalize_trade(trade, reason="all tranche allocations filled")
            return
        if len(confirmed) == len(required) and coverage >= remaining_cov:
            self.store.update_trade_state(
                trade["trade_id"],
                "oco_active",
                note=f"{len(tranche_ids)} fill tranche(s) protected by independent OCO allocations",
            )
            self._sync_oco_group_state(trade, note="all fill tranches protected")
            self.store.audit(
                trade["trade_id"],
                "lifecycle",
                "oco_tranches_active",
                f"{len(tranche_ids)} tranche(s); {len(working)} working and {len(completed)} completed OCO target(s)",
                event_type="oco.state",
                trade_id=trade["trade_id"],
                symbol=trade["symbol"],
                payload={
                    "tranches": tranche_ids,
                    "results": results,
                    "coverage": str(coverage),
                    "sellable": str(remaining_cov),
                },
            )
        elif confirmed:
            detail = (
                f"partial OCO tranche protection; confirmed={','.join(confirmed)}; "
                f"uncertain={','.join(uncertain) or '-'}; failed={','.join(failed) or '-'}"
            )
            self.store.update_trade_state(trade["trade_id"], "oco_partial", note=detail[:2000])
            self._sync_oco_group_state(trade, note=detail)
            self._notify_trade(trade, "OCO_PROTECTION_PARTIAL", detail)
        else:
            detail = (
                f"no OCO tranche is fully confirmed; uncertain={','.join(uncertain) or '-'}; "
                f"failed={','.join(failed) or '-'}"
            )
            self.store.update_trade_state(trade["trade_id"], UNPROTECTED_CRITICAL, note=detail[:2000])
            self._sync_oco_group_state(trade, note=detail)
            self._notify_trade(trade, "OCO_PROTECTION_UNVERIFIED", detail)
        self._refresh_buy_block()

    def _ensure_oco_leg(
        self,
        trade: dict[str, Any],
        kind: str,
        wanted_qty: Decimal,
        price: Decimal,
        stop_price: Decimal,
        *,
        tranche_id: str | None = None,
    ) -> str:
        """Ensure one target order for one tranche; never confuse other tranches."""
        base_group_id = self._oco_group_id(trade)
        if tranche_id is None:
            candidate_groups = {
                str(order.get("oco_group_id") or base_group_id) for order in self._oco_orders(trade, kind)
            }
            if len(candidate_groups) > 1:
                return "uncertain"
            group_id = next(iter(candidate_groups), base_group_id)
        else:
            group_id = tranche_id
        if group_id is None:
            return "failed"
        orders = [
            order
            for order in self._oco_orders(trade, kind)
            if str(order.get("oco_group_id") or base_group_id) == group_id
        ]
        if any(
            order["state"] in ("submitting", "cancel_requested", "unknown_requires_reconciliation") for order in orders
        ):
            return "uncertain"
        active = [order for order in orders if order["state"] in ("open", "partially_filled")]
        if len(active) > 1:
            return "uncertain"
        filled = [order for order in orders if order["state"] == "filled"]
        if filled:
            sold = sum((_dec(order.get("sold_qty")) or Decimal("0") for order in orders), Decimal("0"))
            return "filled" if not active and sold >= wanted_qty else "uncertain"
        if active:
            return "confirmed"
        if any(order["state"] == "canceled" for order in orders):
            return "canceled"

        rejected = [order for order in orders if order["state"] == "rejected"]
        if rejected:
            retryable_flags = [
                int(str(order.get("retryable") if order.get("retryable") is not None else 1)) for order in rejected
            ]
            if 0 in retryable_flags or len(rejected) >= MAX_OCO_ATTEMPTS:
                return "failed"
            last_rejected = max(rejected, key=lambda row: int(str(row.get("attempt") or 0)))
            next_at = _dec_ts(last_rejected.get("next_attempt_at"))
            if next_at is not None and self._now() < next_at:
                return "retry_pending"
            attempt = max(int(str(row.get("attempt") or 0)) for row in orders) + 1
            next_order = self._create_trade_order(
                trade,
                kind,
                wanted_qty,
                price,
                order_type="OCO_STOP_LIMIT_ORDER",
                tranche_id=group_id,
                attempt=attempt,
                stop_trigger_price=stop_price,
            )
        else:
            pending = [order for order in orders if order["state"] == "pending"]
            if pending:
                next_order = max(pending, key=lambda row: int(str(row.get("attempt") or 0)))
                existing_qty = _dec(next_order.get("quantity")) or Decimal("0")
                existing_price = _dec(next_order.get("price")) or Decimal("0")
                if existing_qty != wanted_qty or existing_price != price:
                    detail = (
                        f"pending OCO row mismatch for {kind}/{group_id}: "
                        f"{existing_qty}@{existing_price} != {wanted_qty}@{price}"
                    )
                    self.store.update_trade_state(trade["trade_id"], "needs_reconciliation", note=detail)
                    self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                    return "failed"
            else:
                next_order = self._create_trade_order(
                    trade,
                    kind,
                    wanted_qty,
                    price,
                    order_type="OCO_STOP_LIMIT_ORDER",
                    tranche_id=group_id,
                    attempt=1,
                    stop_trigger_price=stop_price,
                )

        self._submit_oco_order(trade, kind, next_order, wanted_qty, price, stop_price)
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
            return None
        except Exception as exc:
            attempt = int(str(order.get("attempt") or 0))
            retryable = _is_retryable_oco_error(exc)
            delay = oco_retry_delay_s(attempt)
            next_at = (self._now() + timedelta(seconds=delay)).isoformat() if retryable and delay is not None else None
            self.store.transition_trade_order(
                order["trade_order_id"],
                "rejected",
                note=str(exc)[:500],
                attempt=attempt,
                next_attempt_at=next_at,
                retryable=retryable,
            )
            self._record_error(trade, f"{target} OCO rejected: {exc}", "exchange")
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
        self._reconcile_trade_orders(trade)
        return {
            "exchange_order_id": str(exchange_id),
            "executed_qty": executed,
        }

    # ===================================================================
    # Stop-loss handling
    # ===================================================================

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
            self._refresh_buy_block()
            return
        if self._oco_group_id(trade) is not None and (
            not self._ocos_terminal(trade) or not self._oco_stop_children_settled(trade)
        ):
            child_reason = self._unsettled_oco_stop_child_reason(trade)
            detail = (
                f"close refused: OCO parent/stop-child lifecycle is not settled for "
                f"{trade.get('symbol')} trade {trade.get('trade_id')}; "
                f"{child_reason or 'terminal parent/child evidence is incomplete'}"
            )
            previous_state = str(trade.get("trade_state") or "")
            self.store.update_trade_state(
                trade["trade_id"],
                "needs_reconciliation",
                note=detail[:2000],
            )
            self._sync_oco_group_state(trade, note=detail)
            self.store.audit(
                trade["trade_id"],
                "risk",
                "trade_close_refused_oco_unsettled",
                detail[:2000],
                event_type="trading.trade_close_refused",
                outcome="blocked",
                dedupe_key=f"trade-close-refused-oco:{trade['trade_id']}",
            )
            if previous_state != "needs_reconciliation":
                self._notify_trade(trade, "OCO_PROTECTION_UNVERIFIED", detail)
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
        self._refresh_buy_block()

    # ===================================================================
    # Cancellation
    # ===================================================================

    def _cancel_entry_remainder_when_exited(self, trade: dict[str, Any], *, force: bool = False) -> None:
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
        tp1_tranche_exited = any(
            order.get("state") == "filled"
            and (_dec(order.get("sold_qty")) or Decimal("0"))
            >= (_dec(order.get("allocated_qty")) or _dec(order.get("quantity")) or Decimal("0"))
            for order in self._oco_orders(trade, "tp1")
        )
        exited = tp1_tranche_exited or self._tp_done(trade, "tp1") or (remaining is not None and remaining <= 0)
        if force and self._unattributed_oco_parent_cancellations(trade):
            # A canceled OCO parent may mean TP or SL child execution. Either
            # way, do not allow the still-working BUY remainder to create a new
            # position while the child outcome is unresolved.
            exited = True
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
        """Cancel a confirmed working entry and verify it without query bursts."""
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return True
        engine = self.engine
        if engine is None:
            return False
        intent = self.store.get_intent(intent_id)
        if intent is None:
            return False
        if intent.get("state") in TERMINAL_INTENT_STATES:
            return True

        due_reconciler = getattr(engine, "reconcile_intent_if_due", None)
        fresh_snapshot = getattr(engine, "has_fresh_successful_reconciliation", None)
        reconciler = getattr(engine, "reconcile_intent", None)
        try:
            if callable(due_reconciler):
                checked = due_reconciler(intent_id, notify=False)
                if checked is None and not (callable(fresh_snapshot) and fresh_snapshot(intent_id)):
                    return False
            elif callable(reconciler):
                # Compatibility for legacy/test engines without the central gate.
                reconciler(intent_id, notify=False)
            else:
                return False
        except Exception as exc:  # noqa: BLE001
            log.warning("entry reconcile failed for %s: %s", trade["trade_id"], exc)
            return False

        intent = self.store.get_intent(intent_id)
        if intent is None:
            return False
        if intent.get("state") in TERMINAL_INTENT_STATES:
            return True
        if intent.get("state") not in ("open", "partially_filled") or not intent.get("exchange_order_id"):
            # Unknown, planned, submitting, or unacknowledged intents are never
            # queried in a tight retry loop and are never cancelled by guess.
            return False

        # One immediate reconciliation after the actual cancel request is
        # needed for safety. If it remains non-terminal, later sweeps use the
        # persisted scheduler/backoff rather than four GET bursts.
        try:
            engine.cancel_intent_order(intent)
        except Exception as exc:  # noqa: BLE001 - the refreshed state decides
            log.warning("entry cancel failed for %s: %s", trade["trade_id"], exc)
        try:
            if callable(due_reconciler):
                due_reconciler(intent_id, notify=False, urgent_after_cancel=True)
            elif callable(reconciler):
                reconciler(intent_id, notify=False)
        except Exception as exc:  # noqa: BLE001 - cancellation verification is fail-closed
            log.warning("entry cancel verification failed for %s: %s", trade["trade_id"], exc)
        refreshed = self.store.get_intent(intent_id)
        return bool(refreshed and refreshed.get("state") in TERMINAL_INTENT_STATES)

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
        """Reconcile non-terminal trades independently, without a global API lock."""
        try:
            for trade in self.store.list_trades():
                if trade["trade_state"] in TERMINAL_TRADE_STATES:
                    continue
                key = str(trade.get("signal_id") or trade["trade_id"])
                with self._hold_trade_lock(key, "reconcile_all_open"):
                    current = self.store.get_trade(trade["trade_id"])
                    if current is None or current["trade_state"] in TERMINAL_TRADE_STATES:
                        continue
                    try:
                        self._reconcile_trade(current["trade_id"])
                    except Exception as exc:  # noqa: BLE001
                        log.error("reconcile failed for %s: %s", current["trade_id"], exc)
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
                    o["state"] in ("unknown_requires_reconciliation", "submitting", "cancel_requested")
                    or bool(o.get("not_observed_reason"))
                    or (o.get("state") in ("open", "partially_filled") and not o.get("last_observed_at"))
                    for o in orders
                )
                has_ambiguous_oco_parent = bool(self._unattributed_oco_parent_cancellations(refreshed))
                stop_children_settled = self._oco_stop_children_settled(refreshed)
                intent_id = refreshed.get("entry_intent_id")
                intent = store.get_intent(intent_id) if intent_id else None
                intent_unknown = False
                if intent is not None and (
                    intent.get("state") in ("planned", "submitting", "unknown_requires_reconciliation", "reconciling")
                    or int(intent.get("fill_sync_required") or 0) == 1
                ):
                    intent_unknown = True
                sellable = self._sellable_quantity(refreshed)
                # Unknown evidence → retain
                if has_unknown or has_ambiguous_oco_parent or intent_unknown or sellable is None:
                    # Record revisit without duplicating user notifications
                    # (initial entry already notified SL_UNPROTECTED_CRITICAL /
                    # OCO_INVARIANT_*).  Use audit only, idempotent.
                    self.store.audit(
                        trade_id,
                        "reconcile",
                        "needs_reconciliation_revisited",
                        "read-only reconciliation; still unresolved "
                        f"(has_unknown={has_unknown} has_ambiguous_oco_parent={has_ambiguous_oco_parent} "
                        f"stop_children_settled={stop_children_settled} "
                        f"intent_unknown={intent_unknown} sellable={sellable})",
                    )
                    return
                all_terminal = all(o["state"] in TERMINAL_ORDER_STATES for o in orders) if orders else True
                # Conclusive close: sellable 0, all orders terminal, and every
                # known stop child is terminal/unambiguous.
                if all_terminal and sellable == Decimal("0") and stop_children_settled:
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
                    self.store.audit(
                        trade_id, "lifecycle", "closed_via_reconciliation", "all orders terminal, sellable 0"
                    )
                    return
                # Recover only from a transient observation failure when both
                # OCO parent orders have just been freshly observed as working,
                # the complete remaining inventory is covered, and the combined
                # reservation invariant is satisfied. Any terminal/ambiguous
                # parent remains parked for explicit child attribution.
                oco_orders = [o for o in orders if o.get("kind") in ("tp1", "tp2")]
                relevant_oco = [o for o in oco_orders if o.get("state") in ("open", "partially_filled", "filled")]
                active_oco = [o for o in relevant_oco if o.get("state") in ("open", "partially_filled")]
                fresh_active = (
                    bool(active_oco)
                    and bool(relevant_oco)
                    and all(o.get("last_observed_at") and not o.get("not_observed_reason") for o in relevant_oco)
                )
                active_coverage = sum(
                    (
                        max(
                            Decimal("0"),
                            (_dec(o.get("quantity")) or Decimal("0")) - (_dec(o.get("sold_qty")) or Decimal("0")),
                        )
                        for o in active_oco
                    ),
                    Decimal("0"),
                )
                reserved = self._combined_oco_reserved(refreshed)
                if (
                    fresh_active
                    and sellable > 0
                    and active_coverage >= sellable
                    and reserved <= sellable
                    and not has_ambiguous_oco_parent
                ):
                    self.store.update_trade_state(
                        trade_id,
                        "oco_active",
                        note=(
                            "read-only reconciliation recovered transient observation failure; "
                            "both OCO parents freshly confirmed and full sellable quantity is covered"
                        ),
                    )
                    recovered = self._refresh(refreshed)
                    self._sync_oco_group_state(recovered or refreshed, note="fresh OCO coverage re-confirmed")
                    self.store.audit(
                        trade_id,
                        "reconcile",
                        "needs_reconciliation_recovered",
                        "both OCO parents freshly confirmed; full remaining quantity covered; no ambiguous child state",
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
            if self._unattributed_oco_parent_cancellations(trade):
                # Do not infer TP-vs-SL from the parent OCO status and do not
                # create a replacement OCO. First stop any live entry remainder,
                # then park the trade for child/fill attribution.
                self._cancel_entry_remainder_when_exited(trade, force=True)
                detail = (
                    "native OCO parent is canceled but MEXC child execution and fills are not "
                    "independently attributed; entry remainder was canceled where safely "
                    "confirmable and OCO re-arming is blocked"
                )
                self.store.update_trade_state(trade_id, "needs_reconciliation", note=detail)
                self._notify_trade(trade, "OCO_INVARIANT_UNKNOWN", detail)
                self._sync_oco_group_state(trade, note=detail)
                self.store.audit(trade_id, "reconcile", "oco_parent_child_unresolved", detail)
                return
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

        When the engine exposes ``reconcile_intent_if_due`` (the production
        engine), reconciliation is routed through its single persisted
        backoff/eligibility gate before any TP/SL quantity is computed. A skipped
        check is reusable only when a fresh successful exchange snapshot, durable
        order acknowledgement, and completed fill sync are already persisted.
        Otherwise callers fail closed rather than acting on stale local fills.
        """
        intent_id = trade.get("entry_intent_id")
        if not intent_id:
            return True
        intent = self.store.get_intent(intent_id)
        if intent is None:
            return False
        synced = True
        reconciler = getattr(self.engine, "reconcile_intent_if_due", None) if self.engine else None
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
                    checked = reconciler(intent_id, notify=False)
                    if checked is None:
                        fresh_snapshot = getattr(self.engine, "has_fresh_successful_reconciliation", None)
                        synced = bool(fresh_snapshot and fresh_snapshot(intent_id))
                except Exception as exc:  # noqa: BLE001
                    log.warning("entry fill sync failed for %s: %s", trade["trade_id"], exc)
                    synced = False
                intent = self.store.get_intent(intent_id) or intent
                if (
                    intent.get("state") in ("planned", "submitting", "unknown_requires_reconciliation", "reconciling")
                    or int(intent.get("fill_sync_required") or 0) == 1
                ):
                    synced = False

        exchange_order_id = intent.get("exchange_order_id")
        if intent.get("state") in ("submitted", "open", "partially_filled") and not submission_acknowledged(intent):
            synced = False
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
            entry_order_id=exchange_order_id,
        )
        if (
            submission_acknowledged(intent)
            and intent.get("state") in ("submitted", "open", "partially_filled")
            and trade.get("trade_state") == "pending_entry"
        ):
            self.store.update_trade_state(
                trade["trade_id"], "entry_active", note="entry order acknowledgement reconciled"
            )
        # Once fills are confirmed and the entry intent is terminal, settle the
        # entry state and place OCO exits.  An open/partially filled entry is
        # never treated as a finished position.
        if executed_qty > 0 and synced:
            refreshed = self.store.get_trade(trade["trade_id"])
            if refreshed is not None:
                self._settle_entry_state(refreshed)
        return synced

    @staticmethod
    def _trade_order_not_found_streak(reason: str) -> int:
        prefix = "MEXC_ORDER_NOT_FOUND:"
        if not reason.startswith(prefix):
            return 0
        for part in reason[len(prefix) :].split(":"):
            if part.startswith("count="):
                try:
                    return max(0, int(part.split("=", 1)[1]))
                except ValueError:
                    return 0
        return 0

    def _trade_order_not_found_backoff_remaining(self, order: dict[str, Any]) -> float:
        streak = self._trade_order_not_found_streak(str(order.get("not_observed_reason") or ""))
        if streak <= 0:
            return 0.0
        last = _dec_ts(order.get("last_observed_at"))
        if last is None:
            return 0.0
        delay = min(60.0 * (2 ** (streak - 1)), 900.0)
        elapsed = max(0.0, (self._now() - last).total_seconds())
        return max(0.0, delay - elapsed)

    def _reconcile_trade_orders(self, trade: dict[str, Any], *, advance: bool = True) -> None:
        """Query TP/SL order status and recompute sold totals across ALL orders."""
        if not self.engine:
            return
        for order in self.store.list_trade_orders(trade["trade_id"]):
            if order["state"] in TERMINAL_ORDER_STATES and not order.get("not_observed_reason"):
                continue
            if order["state"] == "pending":
                continue  # never sent; the submit path will send it
            remaining = self._trade_order_not_found_backoff_remaining(order)
            if remaining > 0:
                log.debug(
                    "trade-order reconciliation parked for %s after MEXC order-not-found; retry in %.1fs",
                    order["trade_order_id"],
                    remaining,
                )
                continue
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
        try:
            self._discover_oco_stop_children(trade)
        except Exception as exc:  # noqa: BLE001 - discovery must not break reconciliation
            log.warning("OCO stop child discovery failed for %s: %s", trade.get("trade_id"), exc)
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
            return
        if state in ("needs_reconciliation", UNPROTECTED_CRITICAL) and self._ocos_terminal(trade):
            # A legacy OCO can enter needs_reconciliation solely because its TP
            # child was CANCELED while the stop sibling identity was unknown.
            # Once the stop child is independently observed and its fills are
            # included in sl_executed_qty, a zero remaining balance is enough
            # to close this trade.  Require real OCO rows and no unresolved
            # canceled parent; an empty/unknown trade must never be closed here.
            oco_orders = [
                order for order in self._list_trade_orders(trade["trade_id"]) if order.get("kind") in ("tp1", "tp2")
            ]
            remaining = self._remaining_base(trade)
            if (
                oco_orders
                and remaining is not None
                and remaining <= 0
                and not self._unattributed_oco_parent_cancellations(trade)
                and self._oco_stop_children_settled(trade)
            ):
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
            error_code = fresh.get("error_code")
            if error_code in (-2011, -2013):
                previous = str(order.get("not_observed_reason") or "")
                streak = self._trade_order_not_found_streak(previous) + 1
                fresh = dict(fresh)
                fresh["not_observed_reason"] = f"MEXC_ORDER_NOT_FOUND:count={streak}:code={error_code}"
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
            not_observed_reason=fresh.get("not_observed_reason"),
        )

    def _refresh_sold_fields(self, trade: dict[str, Any]) -> None:
        tp1 = self._sold_for_kind(trade, "tp1")
        tp2 = self._sold_for_kind(trade, "tp2")
        sl = self._sold_for_kind(trade, "stop_loss")
        self.store.update_trade_fields(trade["trade_id"], tp1_sold_qty=tp1, tp2_sold_qty=tp2, sl_executed_qty=sl)

    def _sold_for_kind(self, trade: dict[str, Any], kind: str) -> Decimal:
        total = Decimal("0")
        covered_ids: set[str] = set()
        for order in self.store.list_trade_orders(trade["trade_id"], kind=kind):
            total += _dec(order.get("sold_qty")) or Decimal("0")
            if order.get("exchange_order_id"):
                covered_ids.add(str(order["exchange_order_id"]))
        if kind != "stop_loss":
            return total
        # Actual native OCO stop children are persisted in oco_legs, not as
        # separate trade_orders rows.  Count only independently discovered
        # children: never a legacy projection that copied the TP identity, and
        # never an unresolved placeholder.
        group_id = self._oco_group_id(trade)
        if group_id is None:
            return total
        tp_parent_ids = {
            str(order.get("exchange_order_id"))
            for tp_kind in ("tp1", "tp2")
            for order in self.store.list_trade_orders(trade["trade_id"], kind=tp_kind)
            if order.get("exchange_order_id")
        }
        counted_ids: set[str] = set()
        for leg in self._list_oco_legs(group_id):
            if str(leg.get("kind") or "") != "stop_loss":
                continue
            if str(leg.get("not_observed_reason") or "").strip():
                continue
            leg_id = str(leg.get("exchange_order_id") or "")
            if not leg_id or leg_id in tp_parent_ids or leg_id in covered_ids:
                continue
            if leg_id in counted_ids:
                continue
            counted_ids.add(leg_id)
            total += _dec(leg.get("filled_qty")) or Decimal("0")
        return total

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
    def _client_order_id(trade_id: str, kind: str, attempt: int, *, tranche_id: str | None = None) -> str:
        """Stable, exchange-safe client id (max 32 chars, ASCII)."""
        identity = f"{trade_id}:{kind}:{attempt}" if tranche_id is None else f"{trade_id}:{tranche_id}:{kind}:{attempt}"
        digest = hashlib.sha256(identity.encode("ascii")).hexdigest()[:16]
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
        """Persist critical protection alerts as domain events; never blocks trading."""
        critical_keys = {
            "SL_UNPROTECTED_CRITICAL",
            "OCO_INVARIANT_UNKNOWN",
            "OCO_INVARIANT_WOULD_VIOLATE",
            "OCO_INVARIANT_VIOLATED",
            "TRADE_CLOSE_REFUSED_UNKNOWN",
        }
        try:
            if event_key in critical_keys or event_key.startswith("BUY_BLOCK_ACTIVE_"):
                trade_id = str((trade or {}).get("trade_id") or "global")
                symbol = (trade or {}).get("symbol")
                # Stable for identical repeated observations, distinct when the
                # evidence text changes. This preserves alert-on-change semantics.
                event_id = uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"tele-mexc:trade-protection:{trade_id}:{event_key}:{text}",
                ).hex
                with contextlib.suppress(DomainEventConflict):
                    self.store.record_domain_event(
                        build_domain_event(
                            "trade.protection_critical",
                            aggregate_id=trade_id,
                            correlation_id=trade_id,
                            symbol=str(symbol) if symbol else None,
                            event_id=event_id,
                            severity="CRITICAL",
                            payload={"event_key": event_key, "detail": str(text)[:2000]},
                        )
                    )
                return
            if self.notifier is not None:
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
