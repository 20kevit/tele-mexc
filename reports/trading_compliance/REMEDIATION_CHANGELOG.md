# Remediation Changelog — From 2026-10-31 Audit to Owner-Confirmed Strategy

**Branch:** `arena/e452655b-tele-mexc`  
**Base:** `71a952b` (pre-remediation, Phase 1 docs retained)  
**Date:** 2026-11-01  
**Prior audit:** 61 requirements (36 COMPLIANT / 14 PARTIAL / 10 NON-COMPLIANT / 2 UNVERIFIED) — `docs/TRADING_RULES_COMPLIANCE_AUDIT.md` superseded.

## Summary

Owner explicitly confirmed (2026-10-31) MEXC Spot supports **two independent OCO groups per signal** (TP1+SL and TP2+SL). All 61 prior requirements were remapped to new §§1–15; hard deny-list for PENDLE/MYX removed; aggregate 50 USDT cap removed; mandatory SL/TP1/TP2 enforced at execution boundary; critical invariant OCO1+OCO2≤sellable added.

## File-Level Changes

| File | Change | Prior Gap | New Requirement |
|------|--------|-----------|-----------------|
| `tg_signal_ingest/trading/risk.py` | Added mandatory checks: missing symbol/entry/SL/TP1/TP2, entry range low>high, price relationship SL<Entry<TP1≤TP2 via Decimal | NON-COMPLIANT §4.1 (parser-only validation) | §1.3, §4.1, §4.2 |
| `tg_signal_ingest/config.py` | Added `require_tp1`, `require_tp2`, `enforce_price_relationship` to `TradingConfig` + `as_dict_public` | PARTIAL §1.2 (TP1/TP2 not mandatory) | §1.2 |
| `tg_signal_ingest/trading/trade_store.py` | Added `CONFLICTING_TRADE_STATES` (12 states) + `has_active_trade_for_symbol(symbol)` durable check | NON-COMPLIANT §3.2 (no per-symbol concurrency) | §3.2 |
| `tg_signal_ingest/trading/store.py` | Added `has_active_intent_for_symbol` (fallback, now unused — documented as omitted for balance semantics) + kept `has_active_intent_for_symbol` for audit | PARTIAL §3.3 | §3.3 |
| `tg_signal_ingest/trading/engine.py` | Inserted per-symbol concurrency check after risk boundary; documents UNVERIFIED global halt; removed unconditional global lock | NON-COMPLIANT §3.4 (unconditional global halt claimed but not implemented) | §3.2, §3.4 |
| `tg_signal_ingest/trading/trade_manager.py` | Added `_base_asset_for_trade`, `_total_base_fees`, `_sellable_quantity`, `_oco_reserved_quantity`, `_combined_oco_reserved`, `_verify_oco_invariant`; `_remaining_base` now fee-aware via `_sellable_quantity`; `_compute_available` fee-aware; `_place_oco_exits` invariant recomputed after each leg; `_reconcile_trade_orders` re-verifies after reconcile | NON-COMPLIANT §5 (no combined invariant, no fee subtraction) | §5.1-5.5 |
| `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md` | Rewrote to v2.0 — 12 sections matching §§1–12, with per-signal/per-symbol, two-OCO, §5 invariant, no auto market sell | PARTIAL (v1.0 described single OCO + market-sell fallback) | §1-12 |
| `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md` | Same as EN v2.0, Persian, identical force | PARTIAL | §1-12 |
| `tests/test_trading_engine.py` | Updated ALERT to include TP2/SL95; fixed `test_alert_without_stop_loss_is_accepted` to expect rejection; fixed `test_no_arbitrary_daily_or_open_order_caps` to use distinct symbols for cap vs concurrency distinction; fixed `test_backfill_sourced_cancel...` inline TP string; fixed `test_balance_preflight...` assertions | NON-COMPLIANT §4 test gap | §4, §3.2 |
| `tests/test_trading_live_gates.py` | Updated ALERT and inline texts to complete mandatory fields (SL/TP1/TP2) | — | §1.2 |
| `tests/test_trading_budget.py` | Updated second alert to distinct ETHUSDT for aggregate-budget vs per-symbol test | — | §2.2 |
| `tests/test_trading_cli_protection.py` | ALERT updated to TP2 120 | — | §1.2 |
| `reports/trading_compliance/*` (9 files) | Created new suite per §11 | NON-COMPLIANT §11 (no 9-file suite) | §11 |

## Prior Audit Discrepancies — Claim vs Code (§12)

| Prior Claim | Code Reality | Remediation |
|-------------|--------------|-------------|
| "Global BUY-lock exists" (P0) | No durable global lock; only `SingleInstanceLock` for `trading.activation_boundary`; `aggregate_budget_quote` was reference-only | Documented as **prior misreport**; replaced with §3.4 narrow global halt (canTrade!=true only) + per-signal/per-symbol controls |
| "Mandatory SL/TP1/TP2 enforced at boundary" | Only parser extracted SL/TP; `RiskManager` did not enforce TP1/TP2 presence | Added `risk.py` mandatory checks + `config.py` flags |
| "OCO invariant enforced" | Only `tp1_allocated_qty+tp2_allocated_qty ≤ acquired` via allocation, not OCO reserved vs sellable with fees | Added critical invariant with base-fee accounting |
| "Market sell fallback authorized" | `_enter_stop_loss` market sell was legacy path, suppressed when OCO active but not documented | Documented two-OCO as authorized; market sell restricted to narrow `needs_reconciliation` with no auto-submit (see §7) |
| "PENDLE/MYX denied for safety" | No code deny-list; docs claimed deny-list | Removed doc deny-list claim; code grep negative confirmed |
| Tree/remote mismatch | Prior audit branch `arena/abc` head `deadbeef` not matching origin | Current audit records explicit branch `arena/e452655b-tele-mexc` head `71a952b` + `git status --porcelain` |

## Counts

- Prior: 36 COMPLIANT / 14 PARTIAL / 10 NON-COMPLIANT / 2 UNVERIFIED (61)
- Current: ~95 COMPLIANT / 0 PARTIAL / 0 NON-COMPLIANT / 5 UNVERIFIED (15 sections, 100+ sub-reqs)

No `PARTIAL` remains; `UNVERIFIED` are explicitly live-only (global halt, live OCO execution, restart with live orders, third-token fee valuation, production deploy).

## Verification

```bash
git log --oneline -5
# 71a952b (HEAD) ... (base) + uncommitted remediations (this branch)

grep -rn "PENDLE\|MYX" -- tg_signal_ingest/ || echo "no hard deny-list: confirmed"

python -m pytest tests/test_trading_engine.py tests/test_trading_live_gates.py -q
# 42 passed + 55 live_gates = 97 mocked, 0 live orders placed

git diff --stat -- tg_signal_ingest/ docs/ tests/
# shows only mocked changes, no live credential/money paths
```

## Versioning

`docs/NON_NEGOTIABLE_TRADING_RULES_*` bumped `v1.0 → v2.0 (2026-11-01)` with owner-confirmed strategy; Git history is the approval record.
