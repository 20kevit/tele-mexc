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

# Developer Diagnostics Guide — Event-Sourced Trading System

**Audience:** Developers debugging live or replay failures.  
**Prerequisites:** `docs/trading.md`, `docs/data-model.md`, `DATA_COMPLETENESS_AND_AUDITABILITY.md`.  
**Invariant:** All state changes are audited atomically; `audit_events` is append-only.

## 1. Correlation IDs

| Entity | ID | Example |
|--------|----|---------|
| Telegram signal | `signal_id = f"{channel_id}:{message_id}:{edit_version}"` | `100:52:0` |
| Intent (BUY) | `intent_id = f"intent-tsi{idempotency_key[:28]}"` | `<intent-id>...` |
| Idempotency key | `sha256(channel:message:action:symbol)` | `48a1cff...` |
| Trade | `trade_id = f"tr-{idempotency_key[:16]}"` | `tr-48a1cff...` |
| Trade order (TP/SL/OCO) | `trade_order_id = f"to-{clientOrderId}"` | `to-t1-abc...` |
| OCO child | `client_order_id = f"{prefix}-{sha256(trade:kind:attempt)[:16]}"` | `t1-abc...` |
| Audit event | `rowid` + `correlation` (intent/trade/signal) | — |

Join via: `signals.signal_id = order_intents.signal_id = trades.signal_id`; `trade_orders.trade_id = trades.trade_id`; `audit_events.correlation` in (`signal_id`, `intent_id`, `trade_id`).

## 2. Where to Look

| Question | Query |
|----------|-------|
| Why was signal rejected? | `SELECT rejection_reason FROM signals WHERE signal_id=?` + `SELECT * FROM audit_events WHERE correlation=? AND category='risk'` |
| Was BUY submitted? | `SELECT state, exchange_order_id, client_order_id FROM order_intents WHERE signal_id=?` + `order_status_history` |
| What fills? | `SELECT * FROM order_fills WHERE exchange_order_id=?` (quantity, quote_amount, price, fee_asset/amount, exchange_trade_id, is_buyer, is_maker, trade_time_ms) |
| OCO legs? | `SELECT kind, state, quantity, price, allocated_qty, sold_qty, exchange_order_id FROM trade_orders WHERE trade_id=? ORDER BY kind` |
| Invariant violation? | `SELECT * FROM audit_events WHERE event='oco_invariant_violated'` + `trades.last_error` + `trade_state='needs_reconciliation'` |
| Balance reservation? | `SELECT * FROM trading_budget_ledger WHERE intent_id=?` + `SELECT outstanding_balance_usage(symbol)` |

## 3. State Machines

### Intent (order_intents.state)
`planned → submitting → submitted/open → filled/canceled/failed/rejected/canceled_local` plus `unknown_requires_reconciliation` (never auto-retried). Terminal: `filled, canceled, failed, rejected, canceled_local`.

**Key guard:** Only `planned` is resumable (`engine.py:_resume_planned_intent:360`). `submitting`/`unknown_requires_reconciliation` require `reconcile_intent` first.

### Trade (trades.trade_state)
`pending_entry → entry_active → position_open → oco_submitting/co_active/oco_partial → tp1_active → tp1_completed → tp2_active → tp2_completed → closed` plus `stop_loss_executing/exit_pending → closed` and `needs_reconciliation` (fail-closed).

**Critical:** `needs_reconciliation` never auto-submits new orders (`trade_manager.py:_reconcile_trade:1830`).

### Trade Order (trade_orders.state)
`pending (never sent, safe to reuse) → submitting → open/partially_filled → filled/canceled/rejected` plus `unknown_requires_reconciliation`, `cancel_requested`. `pending` is **not** `UNCONFIRMED_ORDER_STATES` for blocking (safe to reuse/rebased).

## 4. Reconciliation Flow

1. `reconcile_pending()` → `reconcile_intent(intent_id, notify=False)` → `client.query_order` by `exchange_order_id` or `client_order_id`.
2. `record_fill` upserts `order_fills` (idempotent on `exchange_trade_id`).
3. `trades.acquired_base_qty` recomputed from `order_fills` sum (not `executed_qty` estimate).
4. `trades.entry_filled_qty` synced; `fill_sync_required` cleared when terminal.
5. OCO invariant re-verified (`trade_manager.py:_reconcile_trade_orders:1950`).

**Idempotency:** `record_fill` ON CONFLICT DO NOTHING on `exchange_trade_id`; `create_trade_order` ON CONFLICT `client_order_id` returns existing.

## 5. Common Failure Signatures

| Symptom | Root Cause | Diagnostics |
|---------|------------|-------------|
| `needs_reconciliation` after OCO | Invariant violated: `OCO1+OCO2 > sellable` due to base-fee or lot-size | `audit_events` detail shows `reserved X > sellable Y (acquired, base_fees, sold_tp1...)` + `trade_manager.py:_verify_oco_invariant` |
| `unknown_requires_reconciliation` on BUY | Network timeout after send; duplicate clientOrderId | `order_status_history` + `audit_events` category `exchange`; check `client.place_error` is `MexcNetworkError` |
| `TP_MINIMUM_NOT_MET` | Allocated qty < lot size after quantize down | `trade_manager.py:_validate_sell` + `symbol_meta.validate_order` |
| `OCO_CONFIG_REJECTED` | Missing SL/TP1/TP2 or SL<Entry<TP1≤TP2 violated | `trade_manager.py:_place_oco_exits:1000` problem string |
| `STALE_CONFIG` | `symbol_meta` stale/missing | `audit_events` category `lifecycle` |

## 6. Concurrency Controls — Implementation

- **Per-signal idempotency:** `engine.py:idempotency_key` stable; `store.py:claim_intent_for_submission` transitions `planned→submitting` atomically via `Database.transaction()` + `SingleInstanceLock` (`db.py:80`).
- **Per-symbol concurrency:** `trade_store.py:CONFLICTING_TRADE_STATES` (12 states) + `engine.py:275` checks `trades` table only (not intents) to avoid masking balance semantics. `needs_reconciliation` is conflicting.
- **Balance reservations:** `store.py:_reserve_budget` inside same transaction as `create_intent`; `outstanding_balance_usage(symbol)` per-symbol (quote shared via ledger, not via per-symbol intent sum — see `store.py:826` note: conservative).
- **Global halt:** Only when `account.canTrade != true` or missing `balances` (`balance.py:validate_balance:120`). Documented halt triggers in `docs/trading.md` §3.4; no unconditional global lock.

## 7. Invariant Recomputation Checklist (§5.3)

`_verify_oco_invariant` must be called after:
- `_reconcile_entry_fills` (new fill → sellable changed)
- `_place_oco_exits` per leg (new reservation)
- `_cancel_trade_order` / `_cancel_orders_of_kind` (reservation released)
- `_reconcile_trade_orders` (execution/cancel confirmed)
- `reconcile_all_open` (restart)

Current coverage: `trade_manager.py:1180,1850,1950,2020`.

## 8. Logging & Notification

- **Logger:** `ingest.trading` (INFO trades, WARNING cancellations, ERROR reconciles)
- **Notifier:** `TradingNotifier.notify_trade(trade, event_key, text)` — never blocks trading (`trade_manager.py:_notify_trade:2100` try/except).
- **No secrets:** `client_order_id` logged, `api_key` never.

## 9. How to Reproduce

```bash
# Unit (mocks only)
python -m pytest tests/test_mandatory_validation.py tests/test_oco_invariant.py -xvs

# Full suite (mocks)
python -m pytest tests/test_trading_engine.py tests/test_trading_live_gates.py -q

# Manual OCO dry-run (no live orders)
python -m tg_signal_ingest.mexc.cli --dry-run place-oco --symbol BTCUSDT --quantity 0.001
```

## 10. Files to Patch for New Symbol

- `tg_signal_ingest/mexc/symbols.py` — add `get(symbol)` metadata (lot size, tick size)
- `tg_signal_ingest/trading/trade_manager.py:_symbol_meta_symbol` — already generic
- No deny-list exists — §6.1 `grep -rn PENDLE` negative confirmed
