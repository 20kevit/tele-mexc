"""Durable persistence for trades, TP orders and stop-loss orders.

Extends the existing ``order_intents`` / ``order_status_history`` model with
a full trade lifecycle table and a per-trade order table.  One trade is created
per executable signal; the entry intent still lives in ``order_intents`` and is
referenced by ``trades.entry_intent_id``.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from .audit import redact_payload
from .store import SymbolConflictError, TradingStore, _row, dec_str


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


class TradeError(Exception):
    """Trade persistence error."""


class TradeStore(TradingStore):
    """TradingStore extended with the trade lifecycle tables (migration 13)."""

    # ---------------------------------------------------------------- trades

    def create_trade(
        self,
        *,
        trade_id: str,
        signal_id: str,
        idempotency_key: str,
        channel_id: int,
        message_id: int,
        symbol: str,
        side: str,
        entry_mode: str,
        requested_quote: Decimal,
        entry_price: Decimal,
        trigger_offset_ticks: int | None,
        trigger_price: Decimal | None,
        tp1_price: Decimal | None,
        tp2_price: Decimal | None,
        tp1_allocation_pct: Decimal,
        tp2_allocation_pct: Decimal,
        sl_price: Decimal | None,
        entry_intent_id: str | None = None,
    ) -> dict[str, Any]:
        """Insert a trade row in ``pending_entry`` state.

        Returns the inserted row.  Raises if a trade with the same idempotency
        key already exists (idempotent re-creation returns the existing row).
        """
        now = self._ts()
        # Columns must match migration 13's ``trades`` table exactly.
        cols = [
            "trade_id", "signal_id", "idempotency_key", "channel_id", "message_id",
            "symbol", "side", "entry_mode", "requested_quote", "entry_price",
            "trigger_offset_ticks", "trigger_price",
            "trade_state", "tp1_price", "tp2_price",
            "tp1_allocation_pct", "tp2_allocation_pct",
            "acquired_base_qty", "acquired_quote_spent", "avg_entry_price", "fees_paid",
            "tp1_allocated_qty", "tp1_sold_qty", "tp2_allocated_qty", "tp2_sold_qty",
            "sl_price", "sl_state", "sl_order_id", "sl_executed_qty",
            "unsold_residual_qty", "unsold_residual_reason",
            "entry_intent_id",
            "entry_order_id", "entry_exchange_status", "entry_filled_qty",
            "retry_count", "last_error", "last_error_category",
            "activated_at", "closed_at", "created_at", "updated_at",
        ]
        placeholders = ", ".join("?" for _ in cols)
        values = (
            trade_id, signal_id, idempotency_key, channel_id, message_id,
            symbol, side, entry_mode, dec_str(requested_quote),
            dec_str(entry_price),
            None if trigger_offset_ticks is None else str(trigger_offset_ticks),
            dec_str(trigger_price),
            "pending_entry", dec_str(tp1_price), dec_str(tp2_price),
            dec_str(tp1_allocation_pct), dec_str(tp2_allocation_pct),
            "0", "0", None, "0",
            "0", "0", "0", "0",
            dec_str(sl_price), "dormant", None, "0",
            None, None,
            entry_intent_id,
            None, None, "0", 0, None, None,
            None, None,
            now, now,
        )
        assert len(values) == len(cols), f"{len(values)} values for {len(cols)} columns"
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM trades WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                return dict(existing)
            # Atomic per-symbol check inside same DB transaction as INSERT.
            # Prevents two concurrent intents for same symbol (both passed their
            # create_intent check before either trade existed) from both inserting
            # trades. BEGIN IMMEDIATE ensures second transaction sees first commit.
            if symbol:
                placeholders_c = ", ".join("?" for _ in self.CONFLICTING_TRADE_STATES)
                conflict = conn.execute(
                    f"SELECT trade_id, trade_state FROM trades WHERE symbol = ? AND trade_state IN ({placeholders_c}) LIMIT 1",
                    (symbol.upper(), *self.CONFLICTING_TRADE_STATES),
                ).fetchone()
                if conflict is not None:
                    raise SymbolConflictError(
                        f"per-symbol concurrency: {symbol.upper()} already has an active trade "
                        f"{conflict['trade_id']} in state {conflict['trade_state']}; "
                        "overlapping BUY not admitted"
                    )
            conn.execute(
                f"INSERT INTO trades ({', '.join(cols)}) VALUES ({placeholders})", values
            )
            row = conn.execute("SELECT * FROM trades WHERE trade_id = ?", (trade_id,)).fetchone()
            assert row is not None
            return dict(row)

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute("SELECT * FROM trades WHERE trade_id = ?", (trade_id,)).fetchone()
        )

    def get_trade_by_signal(self, signal_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute("SELECT * FROM trades WHERE signal_id = ?", (signal_id,)).fetchone()
        )

    def get_trade_by_intent(self, intent_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM trades WHERE entry_intent_id = ?", (intent_id,)
            ).fetchone()
        )

    def list_trades(self, state: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if state is not None:
            rows = self.db.conn.execute(
                "SELECT * FROM trades WHERE trade_state = ? ORDER BY updated_at DESC LIMIT ?",
                (state, limit),
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM trades ORDER BY updated_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def list_non_terminal_trades(self) -> list[dict[str, Any]]:
        """Every trade that still needs lifecycle attention.

        Used by the global new-BUY block so a protection failure cannot be
        missed just because the trade is older than the UI list limit.
        """

        rows = self.db.conn.execute(
            "SELECT * FROM trades WHERE trade_state NOT IN "
            "('closed', 'tp2_completed') "
            "ORDER BY updated_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def update_trade_state(
        self,
        trade_id: str,
        new_state: str,
        *,
        note: str = "",
        sl_state: str | None = None,
        now: str | None = None,
    ) -> dict[str, Any] | None:
        now = now or self._ts()
        sets = ["trade_state = ?", "updated_at = ?"]
        params: list[Any] = [new_state, now]
        if sl_state is not None:
            sets.append("sl_state = ?")
            params.append(sl_state)
        if new_state == "closed":
            sets.append("closed_at = ?")
            params.append(now)
        if new_state in ("position_open", "tp1_active", "stop_loss_executing", "exit_pending"):
            sets.append("activated_at = COALESCE(activated_at, ?)")
            params.append(now)
        params.append(trade_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE trades SET {', '.join(sets)} WHERE trade_id = ?", params
            )
            conn.execute(
                "INSERT INTO audit_events (correlation, category, event, detail, created_at) "
                "VALUES (?, 'lifecycle', ?, ?, ?)",
                (trade_id, new_state, note[:2000], now),
            )
        return self.get_trade(trade_id)

    def update_trade_fields(
        self,
        trade_id: str,
        *,
        acquired_base_qty: Decimal | None = None,
        acquired_quote_spent: Decimal | None = None,
        avg_entry_price: Decimal | None = None,
        fees_paid: Decimal | None = None,
        tp1_allocated_qty: Decimal | None = None,
        tp1_sold_qty: Decimal | None = None,
        tp2_allocated_qty: Decimal | None = None,
        tp2_sold_qty: Decimal | None = None,
        sl_executed_qty: Decimal | None = None,
        sl_order_id: str | None = None,
        unsold_residual_qty: Decimal | None = None,
        unsold_residual_reason: str | None = None,
        entry_order_id: str | None = None,
        entry_exchange_status: str | None = None,
        entry_filled_qty: Decimal | None = None,
        retry_count: int | None = None,
        last_error: str | None = None,
        last_error_category: str | None = None,
    ) -> dict[str, Any] | None:
        now = self._ts()
        sets = ["updated_at = ?"]
        params: list[Any] = [now]
        if acquired_base_qty is not None:
            sets.append("acquired_base_qty = ?")
            params.append(dec_str(acquired_base_qty))
        if acquired_quote_spent is not None:
            sets.append("acquired_quote_spent = ?")
            params.append(dec_str(acquired_quote_spent))
        if avg_entry_price is not None:
            sets.append("avg_entry_price = ?")
            params.append(dec_str(avg_entry_price))
        if fees_paid is not None:
            sets.append("fees_paid = ?")
            params.append(dec_str(fees_paid))
        if tp1_allocated_qty is not None:
            sets.append("tp1_allocated_qty = ?")
            params.append(dec_str(tp1_allocated_qty))
        if tp1_sold_qty is not None:
            sets.append("tp1_sold_qty = ?")
            params.append(dec_str(tp1_sold_qty))
        if tp2_allocated_qty is not None:
            sets.append("tp2_allocated_qty = ?")
            params.append(dec_str(tp2_allocated_qty))
        if tp2_sold_qty is not None:
            sets.append("tp2_sold_qty = ?")
            params.append(dec_str(tp2_sold_qty))
        if sl_executed_qty is not None:
            sets.append("sl_executed_qty = ?")
            params.append(dec_str(sl_executed_qty))
        if sl_order_id is not None:
            sets.append("sl_order_id = ?")
            params.append(sl_order_id)
        if unsold_residual_qty is not None:
            sets.append("unsold_residual_qty = ?")
            params.append(dec_str(unsold_residual_qty))
        if unsold_residual_reason is not None:
            sets.append("unsold_residual_reason = ?")
            params.append(unsold_residual_reason)
        if entry_order_id is not None:
            sets.append("entry_order_id = ?")
            params.append(entry_order_id)
        if entry_exchange_status is not None:
            sets.append("entry_exchange_status = ?")
            params.append(entry_exchange_status)
        if entry_filled_qty is not None:
            sets.append("entry_filled_qty = ?")
            params.append(dec_str(entry_filled_qty))
        if retry_count is not None:
            sets.append("retry_count = ?")
            params.append(retry_count)
        if last_error is not None:
            sets.append("last_error = ?")
            params.append(last_error[:500])
        if last_error_category is not None:
            sets.append("last_error_category = ?")
            params.append(last_error_category)
        params.append(trade_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE trades SET {', '.join(sets)} WHERE trade_id = ?", params
            )
        return self.get_trade(trade_id)

    # ----------------------------------------------------------- trade orders

    def create_trade_order(
        self,
        *,
        trade_order_id: str,
        trade_id: str,
        kind: str,
        client_order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        price: Decimal | None,
        quantity: Decimal | None,
        allocated_qty: Decimal | None,
        stop_trigger_price: Decimal | None = None,
    ) -> dict[str, Any]:
        """Insert a TP/SL order row in ``pending`` state.

        Idempotent on client_order_id: returns the existing row if present.
        ``stop_trigger_price`` records the stop-loss activation trigger that
        was transmitted to the exchange, distinct from ``price`` (the
        take-profit limit price) and from the trade's configured stop-loss
        exit price.
        """
        now = self._ts()
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM trade_orders WHERE client_order_id = ?", (client_order_id,)
            ).fetchone()
            if existing is not None:
                return dict(existing)
            cols = [
                "trade_order_id", "trade_id", "kind", "client_order_id", "symbol",
                "side", "order_type", "price", "quantity", "allocated_qty",
                "sold_qty", "state", "exchange_order_id", "exchange_status",
                "stop_trigger_price", "created_at", "updated_at",
            ]
            placeholders = ", ".join("?" for _ in cols)
            values = (
                trade_order_id, trade_id, kind, client_order_id, symbol, side,
                order_type, dec_str(price), dec_str(quantity), dec_str(allocated_qty),
                "0", "pending", None, None, dec_str(stop_trigger_price),
                now, now,
            )
            conn.execute(
                f"INSERT INTO trade_orders ({', '.join(cols)}) VALUES ({placeholders})",
                values,
            )
            row = conn.execute("SELECT * FROM trade_orders WHERE trade_order_id = ?", (trade_order_id,)).fetchone()
            assert row is not None
            return dict(row)

    def get_trade_order(self, trade_order_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM trade_orders WHERE trade_order_id = ?", (trade_order_id,)
            ).fetchone()
        )

    def get_trade_order_by_exchange(self, exchange_order_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM trade_orders WHERE exchange_order_id = ?", (exchange_order_id,)
            ).fetchone()
        )

    def list_trade_orders(
        self, trade_id: str | None = None, kind: str | None = None,
        state: str | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if trade_id is not None:
            where.append("trade_id = ?")
            params.append(trade_id)
        if kind is not None:
            where.append("kind = ?")
            params.append(kind)
        if state is not None:
            where.append("state = ?")
            params.append(state)
        sql = "SELECT * FROM trade_orders"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC"
        rows = self.db.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    # ── Per-symbol concurrency (owner-confirmed §3.2) ──────────────

    CONFLICTING_TRADE_STATES: tuple[str, ...] = (
        "pending_entry",
        "entry_active",
        "position_open",
        "oco_submitting",
        "oco_active",
        "oco_partial",
        "tp1_active",
        "tp1_completed",
        "tp2_active",
        "stop_loss_executing",
        "exit_pending",
        "needs_reconciliation",
    )
    """Any non-terminal bot-owned trade for the same symbol blocks a new BUY.

    Terminal states ``closed`` and ``tp2_completed`` do not conflict.
    ``needs_reconciliation`` is treated as conflicting because protection is
    unverified — a new overlapping trade could oversell or confuse
    reconciliation.
    """

    def has_active_trade_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        """Return the first conflicting trade for *symbol*, or None."""
        if not symbol:
            return None
        placeholders = ", ".join("?" for _ in self.CONFLICTING_TRADE_STATES)
        rows = self.db.conn.execute(
            f"SELECT * FROM trades WHERE symbol = ? AND trade_state IN ({placeholders}) ORDER BY updated_at DESC LIMIT 1",
            (symbol.upper(), *self.CONFLICTING_TRADE_STATES),
        ).fetchall()
        return dict(rows[0]) if rows else None

    def transition_trade_order(
        self,
        trade_order_id: str,
        new_state: str,
        *,
        note: str = "",
        exchange_order_id: str | None = None,
        exchange_status: str | None = None,
        sold_qty: Decimal | None = None,
        allocated_qty: Decimal | None = None,
        stop_trigger_price: Decimal | None = None,
        evidence_id: str | None = None,
        last_observed_at: str | None = None,
        not_observed_reason: str | None = None,
        cancel_reason: str | None = None,
        canceled_by_leg: str | None = None,
        filled_quote: Decimal | None = None,
        fee_amount: Decimal | None = None,
        fee_asset: str | None = None,
        oco_group_id: str | None = None,
        leg_role: str | None = None,
    ) -> dict[str, Any] | None:
        now = self._ts()
        sets = ["state = ?", "updated_at = ?"]
        params: list[Any] = [new_state, now]
        if exchange_order_id is not None:
            sets.append("exchange_order_id = ?")
            params.append(exchange_order_id)
        if exchange_status is not None:
            sets.append("exchange_status = ?")
            params.append(exchange_status)
        if sold_qty is not None:
            sets.append("sold_qty = ?")
            params.append(dec_str(sold_qty))
        if allocated_qty is not None:
            sets.append("allocated_qty = ?")
            params.append(dec_str(allocated_qty))
        if stop_trigger_price is not None:
            sets.append("stop_trigger_price = ?")
            params.append(dec_str(stop_trigger_price))
        for column, value in (
            ("evidence_id", evidence_id),
            ("last_observed_at", last_observed_at),
            ("cancel_reason", cancel_reason),
            ("canceled_by_leg", canceled_by_leg),
            ("fee_asset", fee_asset),
            ("oco_group_id", oco_group_id),
            ("leg_role", leg_role),
        ):
            if value is not None:
                sets.append(f"{column} = ?")
                params.append(value)
        if not_observed_reason is not None or last_observed_at is not None:
            sets.append("not_observed_reason = ?")
            params.append(not_observed_reason)
        for column, decimal in (
            ("filled_quote", filled_quote),
            ("fee_amount", fee_amount),
        ):
            if decimal is not None:
                sets.append(f"{column} = ?")
                params.append(dec_str(decimal))
        params.append(trade_order_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE trade_orders SET {', '.join(sets)} WHERE trade_order_id = ?", params
            )
        return self.get_trade_order(trade_order_id)

    # ------------------------------------------------------------ OCO groups

    def ensure_oco_group(
        self,
        trade: dict[str, Any],
        *,
        intended_config: dict[str, Any] | None = None,
        protection_status: str | None = None,
    ) -> str:
        """Create (or return) the explicit OCO lifecycle group for a trade."""

        now = self._ts()
        group_id = str(trade.get("oco_group_id") or f"oco-{trade['trade_id']}")
        with self.db.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM oco_groups WHERE trade_id = ?", (trade["trade_id"],)
            ).fetchone()
            if existing is not None:
                return str(existing["oco_group_id"])
            conn.execute(
                "INSERT INTO oco_groups (oco_group_id, trade_id, intent_id, symbol, state, "
                "intended_config, protection_status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'intended', ?, ?, ?, ?)",
                (
                    group_id, trade["trade_id"], trade.get("entry_intent_id"),
                    str(trade["symbol"]).upper(), redact_payload(intended_config),
                    protection_status, now, now,
                ),
            )
            conn.execute(
                "UPDATE trades SET oco_group_id = COALESCE(oco_group_id, ?), updated_at = ? "
                "WHERE trade_id = ?",
                (group_id, now, trade["trade_id"]),
            )
        return group_id

    def set_oco_group_state(
        self,
        trade_id: str,
        state: str,
        *,
        protection_status: str | None = None,
        note: str | None = None,
        closed: bool = False,
    ) -> None:
        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE oco_groups SET state = ?, protection_status = COALESCE(?, protection_status), "
                "updated_at = ?, closed_at = CASE WHEN ? THEN ? ELSE closed_at END "
                "WHERE trade_id = ?",
                (state, protection_status, now, 1 if closed else 0, now, trade_id),
            )
            if note:
                conn.execute(
                    "UPDATE trades SET protection_config_json = COALESCE(protection_config_json, ?), "
                    "updated_at = ? WHERE trade_id = ?",
                    (redact_payload({"last_oco_note": note}), now, trade_id),
                )

    def get_oco_group(self, trade_id: str) -> dict[str, Any] | None:
        return _row(
            self.db.conn.execute(
                "SELECT * FROM oco_groups WHERE trade_id = ?", (trade_id,)
            ).fetchone()
        )

    def upsert_oco_leg(
        self,
        *,
        group_id: str,
        trade: dict[str, Any],
        order: dict[str, Any],
        state: str,
        leg_role: str | None = None,
        exchange_status: str | None = None,
        cancel_reason: str | None = None,
        canceled_by_leg: str | None = None,
        evidence_id: str | None = None,
        not_observed_reason: str | None = None,
        filled_qty: Decimal | None = None,
        filled_quote: Decimal | None = None,
        fee_amount: Decimal | None = None,
        fee_asset: str | None = None,
    ) -> str:
        """Upsert the per-leg projection for one OCO target (tp1/tp2)."""

        now = self._ts()
        target = str(order.get("target") or order.get("kind") or leg_role or "unknown")
        kind = (
            "stop_loss"
            if target == "stop_loss"
            else ("take_profit" if target in ("tp1", "tp2", "take_profit") else "take_profit")
        )
        leg_id = f"{group_id}:{target}"
        with self.db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO oco_legs (
                    leg_id, oco_group_id, trade_id, trade_order_id, kind, target,
                    client_order_id, exchange_order_id, intended_price,
                    intended_trigger_price, intended_qty, state, exchange_status,
                    filled_qty, filled_quote, fee_amount, fee_asset, cancel_reason,
                    canceled_by_target, evidence_id, last_observed_at,
                    not_observed_reason, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(leg_id) DO UPDATE SET
                    trade_order_id = COALESCE(excluded.trade_order_id, oco_legs.trade_order_id),
                    client_order_id = COALESCE(excluded.client_order_id, oco_legs.client_order_id),
                    exchange_order_id = COALESCE(excluded.exchange_order_id, oco_legs.exchange_order_id),
                    intended_price = COALESCE(excluded.intended_price, oco_legs.intended_price),
                    intended_trigger_price = COALESCE(excluded.intended_trigger_price, oco_legs.intended_trigger_price),
                    intended_qty = COALESCE(excluded.intended_qty, oco_legs.intended_qty),
                    state = excluded.state,
                    exchange_status = COALESCE(excluded.exchange_status, oco_legs.exchange_status),
                    filled_qty = CASE
                        WHEN CAST(excluded.filled_qty AS REAL) >= CAST(oco_legs.filled_qty AS REAL)
                        THEN excluded.filled_qty ELSE oco_legs.filled_qty END,
                    filled_quote = COALESCE(excluded.filled_quote, oco_legs.filled_quote),
                    fee_amount = COALESCE(excluded.fee_amount, oco_legs.fee_amount),
                    fee_asset = COALESCE(excluded.fee_asset, oco_legs.fee_asset),
                    cancel_reason = COALESCE(excluded.cancel_reason, oco_legs.cancel_reason),
                    canceled_by_target = COALESCE(excluded.canceled_by_target, oco_legs.canceled_by_target),
                    evidence_id = COALESCE(excluded.evidence_id, oco_legs.evidence_id),
                    last_observed_at = COALESCE(excluded.last_observed_at, oco_legs.last_observed_at),
                    not_observed_reason = COALESCE(excluded.not_observed_reason, oco_legs.not_observed_reason),
                    updated_at = excluded.updated_at
                """,
                (
                    leg_id, group_id, trade["trade_id"], order.get("trade_order_id"),
                    kind, target, order.get("client_order_id"), order.get("exchange_order_id"),
                    dec_str(_optional_decimal(order.get("price"))),
                    dec_str(_optional_decimal(order.get("stop_trigger_price"))),
                    dec_str(_optional_decimal(order.get("quantity"))),
                    state, exchange_status,
                    dec_str(filled_qty or _optional_decimal(order.get("sold_qty")) or Decimal("0")),
                    dec_str(filled_quote), dec_str(fee_amount), fee_asset,
                    cancel_reason, canceled_by_leg, evidence_id, now,
                    not_observed_reason, now, now,
                ),
            )
        return leg_id

    def list_oco_legs(self, group_id: str) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM oco_legs WHERE oco_group_id = ? ORDER BY target", (group_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_trade_order_not_observed(self, trade_order_id: str, reason: str) -> None:
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE trade_orders SET not_observed_reason = ?, updated_at = ? "
                "WHERE trade_order_id = ?",
                (reason[:500], self._ts(), trade_order_id),
            )

    def attribute_sibling_cancel(
        self, group_id: str, *, filled_target: str, canceled_target: str
    ) -> None:
        """Record that an OCO sibling was canceled *because* its peer filled."""

        now = self._ts()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE oco_legs SET cancel_reason = 'oco_sibling_filled', "
                "canceled_by_target = ?, updated_at = ? "
                "WHERE oco_group_id = ? AND target = ?",
                (filled_target, now, group_id, canceled_target),
            )
            conn.execute(
                "UPDATE trade_orders SET cancel_reason = 'oco_sibling_filled', "
                "canceled_by_leg = ?, updated_at = ? "
                "WHERE oco_group_id = ? AND kind = ?",
                (filled_target, now, group_id, canceled_target),
            )
