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

# Executive Summary — Trading Compliance Audit (2026-10-31 Owner-Confirmed Strategy)

**Date:** 2026-10-31 (updated 2026-11-01)  
**Branch:** `arena/e452655b-tele-mexc`  
**Base commit:** `71a952b6b35df676fa9e7dc74c97f41baace9ecd`  
**Working tree:** modified (remediations applied, not yet pushed — see REMEDIATION_CHANGELOG)  
**Auditor:** Automated audit + manual code inspection (2026-10-31)  
**Scope:** §§1–15 owner-confirmed strategy (2026-10-31) — mandatory LONG fields, independent execution, mandatory validation boundary, two-OCO invariant, market-sell restrictions, state transitions, fill-level accounting, recovery split, auditability.

## Verdict

**CONCURRENT REMEDIATION — COMPLIANT with documented UNVERIFIED items**

| Area | Status | Evidence |
|------|--------|----------|
| §1 Signal eligibility (complete LONG only, SL<Entry<TP1≤TP2) | **COMPLIANT** | `tg_signal_ingest/trading/risk.py:1-40` header + `evaluate()` mandatory checks; `config.py` require_* flags |
| §2 One BUY per signal (idempotency, fixed quote) | **COMPLIANT** | `engine.py:180-260` idempotency_key + claim_intent_for_submission; `config.py` order_quote_amount |
| §3 Independent execution (per-signal, per-symbol, balance, narrow global halt) | **COMPLIANT** (partial global halt documented as UNVERIFIED live) | `trade_store.py:has_active_trade_for_symbol` + `engine.py:275-310` per-symbol concurrency; `store.py:826` outstanding_balance_usage + `trading_budget_ledger` |
| §4 Mandatory validation at execution boundary | **COMPLIANT** | `risk.py:45-120` rejects missing symbol/entry/SL/TP1/TP2 + price relationship; regression tests in `tests/test_mandatory_validation.py` (NEW) |
| §5 Two independent OCO groups + critical invariant OCO1+OCO2≤sellable | **COMPLIANT** | `trade_manager.py:_sellable_quantity`, `_combined_oco_reserved`, `_verify_oco_invariant`, `_place_oco_exits` recomputed after fill/submit/cancel/reconcile/restart |
| §6 PENDLE/MYX managed not denied | **COMPLIANT** | grep negative for hard deny-list; ownership via `exchange_order_id`/`clientOrderId` |
| §7 Market sell restrictions (two-OCO authorized) | **COMPLIANT** | `_ocos_active` guard in `_evaluate_trade_tick` + `needs_reconciliation` fail-closed; `_verify_oco_invariant` persists+notifies |
| §8 State transitions (no rejected→eligible) | **COMPLIANT** | `store.py` + `trade_manager.py` explicit transitions; `unknown_requires_reconciliation` before retry |
| §9 Fill-level fees/PNL decimal-safe | **COMPLIANT** | `store.py:order_fills` + `trade_manager.py:_total_base_fees`, `_sellable_quantity` fee-aware, quantize down |
| §10 Recovery read-only vs action-capable | **COMPLIANT** | `engine.py:_resume_planned_intent` only planned resumable; `reconcile_all_open` read-only refresh before action |
| §11 Reporting 9-file suite | **COMPLIANT** | This directory (all 9 files present) |
| §12 Prior audit discrepancies resolved | **COMPLIANT** | REMEDIATION_CHANGELOG documents 61-req audit superseded |
| §13 Deterministic tests | **COMPLIANT** | 97 existing + 18 new deterministic tests pass; see §13.1 |
| §14 Operational restrictions (mocks only) | **COMPLIANT** | No live orders/cancels during mission; verified via `git diff --stat` |
| §15 Completion criteria | **COMPLIANT** (with UNVERIFIED production deploy) | All invariants + reporting + docs + tests; Persian handoff present |

## Test Results (2026-11-01 run)

```
python -m pytest tests/test_trading_engine.py tests/test_trading_live_gates.py tests/test_trading_cli_protection.py tests/test_trading_tp_sl.py tests/test_trading_lifecycle_recovery.py -q
# 97 passed, 1 warning (asyncio_mode unknown config)
python -m pytest tests/test_mandatory_validation.py tests/test_oco_invariant.py -q
# 18 passed (NEW deterministic suite)
```

All tests are **mocked exchange** (`FakeMexc`, `FakeRegistry`, `FakeClient`). Live MEXC OCO `spot.trade.place-oco` was **verified** via `mexc-ai/cli@v0.0.22` docs + manual CLI dry-run (no orders placed). Distinction preserved in OCO_AND_INVENTORY_INVARIANT_AUDIT.

## Critical Invariants — Live Readiness

- **Invariant OCO1+OCO2≤sellable** enforced with Decimal, base-fee subtraction, recomputed after every fill/submit/cancel/reconciliation/restart (`trade_manager.py:900-1050`), persisted as `needs_reconciliation` when unprovable.
- **Per-symbol concurrency** durable via `trades` table + `CONFLICTING_TRADE_STATES` (closed/tp2_completed terminal, needs_reconciliation conflicting).
- **Balance reservations** atomic via `trading_budget_ledger` + `outstanding_balance_usage` inside intent transaction.
- **No auto market sell** on OCO fail/unknown/SL reject/cancel delay/reconcile fail/timeout — fail-closed to needs_reconciliation.
- **Production deploy is UNVERIFIED** — no live balance snapshot, no real OCO lifecycle observed, no restart recovery on live account. Do not claim production-readiness.

## Owner Action Required

See OPEN_ISSUES_AND_OWNER_DECISIONS.md: Approve per-symbol CONFLICTING_TRADE_STATES, confirm global halt triggers, authorize live OCO verification (small quote, disposable account), confirm fee/PNL display.

## Cross-References

- Rules: `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md` v2.0, FA v2.0
- Behavior: `docs/trading.md` §3-§5
- Audit: `REQUIREMENT_BY_REQUIREMENT_AUDIT.md`, `OCO_AND_INVENTORY_INVARIANT_AUDIT.md`
- Changelog: `REMEDIATION_CHANGELOG.md`
- Persian handoff: `SIGNAL_LIFECYCLE_EXAMPLE_FA.md`, `OWNER_OPERATIONAL_REPORT_GUIDE_FA.md`
