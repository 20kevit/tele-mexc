# Data Completeness and Auditability — Evidence Matrix

**Purpose:** Prove every signal/trade/order/fill is durably auditable; document what is persisted, where, and what remains UNVERIFIED.

## 1. Source of Truth

- **Exchange is authoritative** for orders/fills/balances (`engine.py:reconcile_intent`, `trade_manager.py:_query_order`).
- **DB is authoritative** for intent/trade lifecycle and audit history (event-sourced, immutable).

## 2. Persisted Tables

| Table | Key Columns | Invariant |
|-------|-------------|-----------|
| `signals` | signal_id (channel:message:edit_version), channel_id, message_id, edit_version, symbol, action, entry_price, sl_price, tp1_price, tp2_price, status, rejection_reason, fingerprint, raw_text, normalized_text, created_at, updated_at | One row per Telegram edit_version; status `rejected` preserves exact reason |
| `order_intents` | intent_id, signal_id, idempotency_key (sha256), client_order_id (tsi+28), symbol, side, order_type, quote_amount, base_quantity, limit_price, mode (sim/testonly/live), state, exchange_order_id, executed_qty, cumulative_quote, fill_sync_required, created_at | Created BEFORE exchange call; terminal states never retried automatically |
| `order_status_history` | intent_id, from_state, to_state, note, created_at | Append-only; every transition recorded |
| `order_fills` | fill_id, exchange_order_id, exchange_trade_id (unique), symbol, side, price, quantity, quote_amount, fee_asset, fee_amount, is_buyer, is_maker, trade_time_ms, created_at | Fill-level accounting; upsert on exchange_trade_id prevents double-count |
| `trades` | trade_id (tr-sha16), signal_id, idempotency_key, symbol, trade_state, entry_intent_id, requested_quote, entry_price, trigger_price, tp1_price, tp2_price, tp1_allocation_pct, tp2_allocation_pct, acquired_base_qty, acquired_quote_spent, avg_entry_price, fees_paid, tp1_allocated_qty, tp1_sold_qty, tp2_allocated_qty, tp2_sold_qty, sl_price, sl_state, sl_order_id, sl_executed_qty, unsold_residual_qty, retry_count, last_error, created_at, updated_at | One trade per signal; acquired_* from fills, not requested |
| `trade_orders` | trade_order_id (to-clientOrderId), trade_id, kind (tp1/tp2/stop_loss), client_order_id (t1/sl-...), symbol, side, order_type (OCO_STOP_LIMIT_ORDER/LIMIT/MARKET), price, quantity, allocated_qty, sold_qty, state, exchange_order_id, exchange_status, created_at | Two OCO legs + optional stop-loss; state machine with pending→submitting→open→filled |
| `audit_events` | correlation (signal/intent/trade), category (lifecycle/risk/exchange/balance), event, detail (≤2000), created_at | Immutable history; every rejection, submission, invariant violation logged |
| `trading_budget_ledger` | intent_id, reserved_quote, spent_quote, fee_quote, active, state, note, created_at | Atomic reservation inside create_intent transaction |
| `mexc_account_snapshots` | snapshot_id, raw_json, fetched_at, created_at | Periodic snapshots; used for global halt decision |
| `app_state` | key, value | activation_boundary_message_id, trading.halted etc. |

## 3. What Is Captured per Execution

| Field | Source | Persisted Where |
|-------|--------|-----------------|
| Executed quantity, quote value, price, commission asset/amount, exchange fill ID, timestamp | `myTrades` / `order_fills` | `order_fills.quantity`, `quote_amount`, `price`, `fee_asset`, `fee_amount`, `exchange_trade_id`, `trade_time_ms` |
| Realized vs unrealized PNL | Derived: realized = sum(exit quote) - acquired_quote_spent - fees; unrealized = (mark_price - avg_entry) * (sellable) | Computed in `trade_manager.py` (not separately persisted — derived on read, marked unvalued if fees unvalued) |
| Base-asset fee reduction | `_total_base_fees` sums where fee_asset == base_asset | Subtracted in `_sellable_quantity` |
| Quantization | `quantize(ROUND_DOWN)` per symbol_meta | `store.py:create_intent` + `trade_manager.py:_quantize_qty/price` |

## 4. Completeness Gaps (UNVERIFIED)

| Gap | Impact | Mitigation |
|-----|--------|------------|
| No live account snapshot | Global halt trigger `canTrade!=true` never observed live | Mocked in `test_trading_engine.py::test_global_halt` but UNVERIFIED live |
| No live OCO execution (TP1+SL simultaneously) | OCO lifecycle verified via mocks + CLI docs only | Manual CLI `place-oco` dry-run; live small-quote test pending owner auth |
| No production restart recovery | `reconcile_all_open` on restart never observed with live orders | Unit test via `test_trading_lifecycle_recovery.py` with FakeMexc; UNVERIFIED live |
| No third-token fee valuation | `unvalued_fees` for non-USDT/non-base fees | Displayed as incomplete, not invented |

## 5. Auditability Proof

```sql
-- All rejections with exact reason
SELECT signal_id, symbol, status, rejection_reason FROM signals WHERE status='rejected';

-- Full lifecycle of one signal
SELECT correlation, category, event, detail, created_at FROM audit_events
WHERE correlation IN ('100:52:0','intent-tsi...','tr-...') ORDER BY created_at;

-- Fill-level P&L for one trade
SELECT exchange_trade_id, quantity, quote_amount, price, fee_asset, fee_amount
FROM order_fills WHERE exchange_order_id='ex-123';

-- Inventory invariant at any point
SELECT trade_id, acquired_base_qty, tp1_allocated_qty, tp2_allocated_qty,
       tp1_sold_qty, tp2_sold_qty, sl_executed_qty
FROM trades WHERE trade_id='tr-...';
-- plus trade_orders sold_qty for reserved vs sellable
```

All queries are supported by existing indexes (`signals(channel_id,message_id)`, `order_intents(idempotency_key)`, `trade_orders(trade_id)`, `audit_events(correlation)`).

## 6. Retention

- `audit_events` and `order_status_history` are **never deleted** (no TTL).
- `order_fills` retained indefinitely (P&L history).
- `mexc_account_snapshots` pruned after 30 days (not used for P&L).
