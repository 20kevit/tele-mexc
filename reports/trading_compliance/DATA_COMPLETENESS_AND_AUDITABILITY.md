> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

<!-- VERIFICATION 2026-10-31T2026-10-31T08:21:53.180647 — Mocked (unit) vs Live (integration) distinction -->
> **Verification (mocked, disposable DBs — no live exchange, no real login):**  
> `pytest tests/test_trading_engine.py tests/test_trading_config.py tests/test_trading_live_gates.py tests/test_owner_confirmed_deterministic.py tests/test_critical_review_fixes.py` → **105 passed** (42+18+10+15+20) in ~7.6s on Python 3.11.  
> All exchange calls are mocked via `SpyMexc` / `FakeRegistry`; `TradingStore` uses ephemeral SQLite (`:memory:` or `tmp_path`). No `MEXC_API_KEY`/`MEXC_SECRET` required, no order leaves the sandbox. Live integration (`tests/test_e2e_live.py`, `tests/test_trading_live.py`) intentionally **not** executed — would require real credentials and network.  
> **Key file:line citations (branch `arena/e452655b-tele-mexc`, base `71a952b`):**  
> - Mandatory invariants (SL/TP1/TP2/SL<Entry<TP1≤TP2) non-disableable: `tg_signal_ingest/config.py:488-495` (raises `ConfigError` on forbidden keys), rejected in `tests/test_trading_config.py:18` and `tests/test_critical_review_fixes.py::test_config_cannot_disable_mandatory_invariants`.  
> - Entry-range explicit rejection: `tg_signal_ingest/trading/risk.py:57-71` (message `entry range X–Y … not supported … use single ENTRY price`), verified `tests/test_critical_review_fixes.py::test_entry_range_rejected_with_clear_reason`.  
> - Per-symbol atomic admission (shared DB): `tg_signal_ingest/trading/store.py:67-92` (`SymbolConflictError`/`TradingError` fail-closed), `tg_signal_ingest/trading/trade_store.py:356-368` (DB-filtered `has_active_trade_for_symbol` LIMIT 1), `tg_signal_ingest/trading/engine.py:280-355` (`TradeStore(repo.db)` shared, `SymbolConflictError` handling, `LIMIT 20` removed), tested `tests/test_critical_review_fixes.py::test_per_symbol_*` (4 tests) + `tests/test_owner_confirmed_deterministic.py` concurrency spy.  
> - Sellable = acquired − base_fees − sold, idempotent via `exchange_trade_id`: `tg_signal_ingest/trading/trade_manager.py:1505-1545` (`_base_asset_for_trade` fallback, `_total_base_fees` SELL-aware, `_sellable_quantity` `None` if unknown, `_remaining_base`), verified `tests/test_critical_review_fixes.py::test_allocation_uses_net_sellable_with_base_fee` & `test_sell_base_fee_reduces_sellable`.  
> - Allocation from net sellable (not gross): `tg_signal_ingest/trading/trade_manager.py:1085-1125` (`sellable_for_allocation = _sellable_quantity`, `TP1 % of sellable`, `TP2 % of remaining pool`, unknown→`needs_reconciliation`), spy proves `TP1+TP2 ≤ sellable`.  
> - OCO invariant **before** submission (no API call if unprovable/violated/hypothetical exceed): `tg_signal_ingest/trading/trade_manager.py:1135-1225` (`_verify_oco_invariant` + `existing_other+qty > sellable` + `unknown→needs_reconciliation` + `would_violate`), spied in `tests/test_critical_review_fixes.py` (`test_oco_pre_submit_*` 6 tests) with `place_oco` call-count assertions, plus reuse-not-double-count & ambiguous `unknown_requires_reconciliation`.  
> - Market Sell removal (owner-confirmed §7): `tg_signal_ingest/trading/engine.py:1553-1572` (`submit_stop_loss_market_sell` raises `RuntimeError` disabled), `tg_signal_ingest/trading/trade_manager.py:1408` (`_enter_stop_loss` → `_handle_unprotected_sl`), `1433-1450` (`_resume_stop_loss`/`_submit_stop_loss_order` stubs), `400-460` (`_handle_unprotected_sl` persists `needs_reconciliation`+`SL_UNPROTECTED_CRITICAL`+audit, narrow per-symbol restriction), `1800` (legacy states read-only). Zero `submit_stop_loss_market_sell` reachable (verified `grep -rn` → only `tests/` + `trade_store.py` shim). Proven by 6 spy tests: `test_no_market_sell_*` (both OCOs active, OCO1 missing, OCO2 uncertain, rejection, cancel, restart).  
> - Startup reconcilable states: `tg_signal_ingest/trading/trade_manager.py:244-310` (`recover_on_startup` → `_reconcile_trade` → `_reconcile_entry_fills`/`_reconcile_trade_orders`), legacy `stop_loss_executing`/`exit_pending` now read-only `needs_reconciliation` (line 1800).  

---

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
