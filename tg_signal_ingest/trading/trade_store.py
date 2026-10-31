"""Durable persistence for trades, TP orders and stop-loss orders.

Extends the existing ``order_intents`` / ``order_status_history`` model with
a full trade lifecycle table and a per-trade order table.  One trade is created
per executable signal; the entry intent still lives in ``order_intents`` and is
referenced by ``trades.entry_intent_id``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from .store import TradingStore, _row, dec_str


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
        params.append(trade_order_id)
        with self.db.transaction() as conn:
            conn.execute(
                f"UPDATE trade_orders SET {', '.join(sets)} WHERE trade_order_id = ?", params
            )
        return self.get_trade_order(trade_order_id)
