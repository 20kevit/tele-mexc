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

# OCO and Inventory Invariant Audit — Critical §5

**Invariant:** `OCO1_reserved + OCO2_reserved ≤ bot_owned_sellable`  
**Enforcement:** Decimal-safe, recomputed after every relevant event, persisted+notified if unprovable.

## 1. Definitions

| Term | Formula | Source |
|------|---------|--------|
| `acquired` | SUM(`order_fills.quantity` for entry intent's `exchange_order_id`) | `trade_manager.py:_reconcile_entry_fills:1920` |
| `base_fees` | SUM(`order_fills.fee_amount` where `fee_asset == base_asset`) | `trade_manager.py:_total_base_fees:1480` |
| `sold` | SUM(`trade_orders.sold_qty` where kind in (tp1,tp2,stop_loss) and state terminal) | `_sold_for_kind` |
| `sellable` | `max(0, acquired - base_fees - sold)` | `_sellable_quantity:1500` |
| `OCO1_reserved` | SUM(`trade_orders.quantity - sold_qty` for kind=tp1 where state in (open,partially_filled,submitting,cancel_requested,unknown_requires_reconciliation)) | `_oco_reserved_quantity:1520` |
| `OCO2_reserved` | Same for kind=tp2 | `_combined_oco_reserved:1530` |

**Precision:** All via `Decimal`, `quantize(ROUND_DOWN)` per `symbol_meta` (`_quantize_qty:2050`). Never uses float.

**Example (BTCUSDT, entry 100, acquired 0.1 BTC, base fee 0.0001 BTC, TP1 sold 0.02):**
```
sellable = 0.1 - 0.0001 - 0.02 = 0.0799
OCO1_reserved = 0.03 (open TP1 remaining)
OCO2_reserved = 0.04 (open TP2)
combined = 0.07 ≤ 0.0799 → OK
```

## 2. Where Recomputed

| Trigger | Location | Verification |
|---------|----------|--------------|
| After entry fill sync | `trade_manager.py:_reconcile_entry_fills` → `_settle_entry_state` → `_place_oco_exits` | Acquired changes → sellable recomputed |
| Before OCO submission | `trade_manager.py:_place_oco_exits:1070` | Allocations from acquired; invariant checked before submit |
| After each OCO leg submission | `_place_oco_exits:1130` loop `post_ok = _verify_oco_invariant(refresh)` | Prevents incremental overflow |
| After TP/SL cancel/reconcile | `trade_manager.py:_reconcile_trade_orders:1950` → `_verify_oco_invariant` | Reservation released → invariant rechecked |
| On restart | `reconcile_all_open:1800` iterates trades → `_reconcile_trade` → `_reconcile_trade_orders` → `_verify` | Recalcs from persisted fills/orders |
| After `reconcile_intent` | `engine.py:reconcile_intent` → `trade_manager._reconcile_entry_fills` | Fills authoritative |

## 3. Failure Mode (Unprovable)

If `reserved > sellable`:

```python
# trade_manager.py:_verify_oco_invariant:1540
detail = f"critical OCO invariant violated: OCO1+OCO2 reserved {reserved} > bot-owned sellable {sellable} (acquired=..., base_fees=..., sold_tp1=..., sold_tp2=..., sold_sl=...)"
self.store.update_trade_state(trade_id, "needs_reconciliation", note=detail)
self._notify_trade(trade, "OCO_INVARIANT_VIOLATED", detail)
self.store.audit(trade_id, "risk", "oco_invariant_violated", detail)
return False, detail
```

Caller (`_place_oco_exits`) **does not submit or increase** affected leg; marks remaining required legs as failed; trade stays in `needs_reconciliation` until manual reconciliation (cancel or execution confirms new sellable).

**Notification:** Both owner and developer (TradingNotifier).

## 4. Two-OCO Structure (Owner-Confirmed)

- **OCO1:** TP1 Limit + SL Stop-Limit (OCO_STOP_LIMIT_ORDER via `engine.submit_oco_sell` → `mexc-ai/cli` `spot.trade.place-oco@v0.0.22`)
- **OCO2:** TP2 Limit + SL Stop-Limit (independent OCO group)
- Allocation: `tp1_allocation_pct_of_acquired=50%` (`config.py`), `tp2_allocation_pct_of_remaining=100%` of remainder. Quantized down, never exceeds acquired.

**Verified vs Mocked:**

| Aspect | Status | Evidence |
|--------|--------|----------|
| MEXC Spot supports OCO via `place-oco` | **VERIFIED** | `@mexc-ai/cli` docs + `tg_signal_ingest/mexc/client.py:place_oco` (dry-run) |
| Two independent OCO groups per symbol allowed | **VERIFIED** | CLI docs: multiple OCOs per symbol supported; no global OCO cap observed |
| Quantity reservation semantics (locked base) | **UNVERIFIED live** | Inferred from docs; not observed on live account — mocked via `FakeMexc` |
| OCO child IDs (TP/SL legs) distinct | **UNVERIFIED live** | Mocked as `t1-...`/`t2-...`; live IDs unknown until real OCO placed |
| Partial fills for OCO legs | **MOCKED** | `test_oco_invariant.py::test_partial_fill_reduces_sellable` via manual fill injection |

**Distinction preserved:** No test claims live OCO placement succeeded.

## 5. Edge Cases Handled

| Case | Handling |
|------|----------|
| Multiple BUY fills (partial entry) | `acquired` sums all `order_fills` rows; `sellable` recomputed after each reconcile |
| Base-asset fee (e.g., BTC fee for BTCUSDT) | `_total_base_fees` identifies fee_asset == base_asset; quote-asset fee does not reduce base sellable |
| Third-token fee (e.g., MX fee) | Preserved as `unvalued_fees`; not subtracted from sellable, PNL marked incomplete |
| Precision/step-size | `quantize down` ensures never exceeds symbol constraints; dust (< lot size) → `unsold_residual_qty` |
| TP1 executes → remaining sellable shrinks | `_reconcile_trade_orders` recomputes; second OCO's remaining still covers sellable (or needs_reconciliation if not) |
| Stale OCO exceeding remaining after TP1 | Invariant violation → `needs_reconciliation` → owner notified, narrowest restriction |
| Delayed cancellation confirmation | `cancel_requested` still counted as reserved until terminal; prevents oversell |
| Process restart mid-OCO | `reconcile_all_open` restores identities via `exchange_order_id`/`clientOrderId`, recalc invariant |
| Manual user orders | Filtered by persisted `exchange_order_id` — never counted toward bot sellable/reserved (see §6) |

## 6. Test Coverage (Deterministic)

| Test | Verifies |
|------|----------|
| `test_oco_invariant.py::test_combined_does_not_exceed_sellable` | Happy path: 0.1 acquired, 0.05+0.05 reserved ≤ 0.1 |
| `test_oco_invariant.py::test_base_fee_reduces_sellable` | 0.1 acquired -0.0001 fee → sellable 0.0999, 0.05+0.05 would violate → needs_reconciliation |
| `test_oco_invariant.py::test_violation_persists_needs_reconciliation` | Reserved 0.12 > sellable 0.1 → trade state needs_reconciliation, OCO not increased |
| `test_oco_invariant.py::test_partial_tp1_execution_recomputes` | TP1 partial fill reduces sellable, OCO2 still valid |
| `test_oco_invariant.py::test_cancel_releases_reservation` | Cancel OCO1 → reserved drops, invariant passes |

All mocked; live OCO execution marked UNVERIFIED.

## 7. Known Limitation

`_total_base_fees` currently sums only entry fills' fees (via `entry_intent_id`→`exchange_order_id`). Exit fills' fees (TP/SL) are not yet subtracted from sellable — they reduce quote PNL, not base sellable, so impact is minor. Documented as **OPEN_001** in `OPEN_ISSUES_AND_OWNER_DECISIONS.md`.
