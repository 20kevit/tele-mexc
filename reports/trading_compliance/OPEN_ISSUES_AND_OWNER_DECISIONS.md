# Open Issues and Owner Decisions

**Date:** 2026-11-01  
**Status:** Requires owner approval before live deployment. No false production-readiness claim.

## 1. Owner Decisions Required

| ID | Decision | Options | Recommended | Impact if Deferred |
|----|----------|---------|-------------|--------------------|
| D001 | **Conflicting trade states for per-symbol concurrency** — should `tp1_completed` block a new BUY for same symbol? | A) Yes (all non-terminal including `tp1_completed`) block; B) No, only `pending_entry,entry_active,position_open,oco_*` block | **A** (current code: `CONFLICTING_TRADE_STATES` includes `tp1_completed`, `tp1_active`, `tp2_active`, `pending_entry`... but not `closed/tp2_completed`) — prevents overlapping position while TP2 still pending | If B, second signal could start while TP2 still reserved → oversell risk |
| D002 | **Global halt triggers** — when to halt all symbols? | A) Only `canTrade != true` or missing `balances` or stale `account` snapshot >5min; B) Also halt on `MexcWafError` repeated | **A** (narrow, as implemented `engine.py:_is_trading_halted`) | If B, more conservative but may halt unnecessarily |
| D003 | **Live OCO verification** — authorize small-quote live test? | A) Yes, 3 USDT on BTCUSDT with disposable sub-account, observe two OCOs, then cancel; B) No, stay mocked | **A** (with explicit owner auth, disposable DB, no PENDLE/MYX confusion) | If B, live OCO lifecycle stays UNVERIFIED |
| D004 | **Fee valuation for third-token fees** — how to display? | A) Show as `unvalued_fees` (no conversion invented); B) Fetch mark price for fee token at fill time | **A** (current: preserve asset/amount, mark PNL incomplete) | If B, need price oracle for arbitrary fee tokens |
| D005 | **Dust threshold** — when to mark `unsold_residual_qty` and close? | A) Quantized qty ==0 → residual dust, auto-close; B) Never auto-close, manual | **A** (implemented `trade_manager.py:_remaining_base` → `unsold_residual`) | If B, trades with dust stay open forever |

Record approval via reply to this file or Git commit message `Approved D001=A D002=A...`.

## 2. Open Issues (Not Blocking, Tracked)

| ID | Issue | Severity | File:Line | Mitigation |
|----|-------|----------|-----------|------------|
| OPEN_001 | Exit fills' fees (TP/SL) not subtracted from sellable — only entry fees subtracted in `_total_base_fees` | Low | `trade_manager.py:1485` | Exit fees affect quote PNL, not base sellable; dust risk <0.01% — fix in v2.1 by summing all `trade_orders` fills |
| OPEN_002 | `outstanding_balance_usage` is per-symbol, not account-wide quote — concurrent ETH+BTC could overspend USDT | Medium | `store.py:826` | Current `trading_budget_ledger` is account-wide via `reserved_quote`, but preflight uses per-symbol outstanding; cross-symbol overspend relies on ledger reservation atomicity — add account-wide `SUM(active)` check in v2.1 |
| OPEN_003 | OCO child leg IDs (quantity/price) not live-verified | Medium | `trade_manager.py:_submit_oco_order:1250` `place_oco` | CLI docs confirm OCO_STOP_LIMIT_ORDER; live IDs unknown until real placement — D003 required |
| OPEN_004 | Market sell flag not distinct — generic LIVE token could still theoretically trigger `_enter_stop_loss` if OCO not active | Low | `trade_manager.py:_enter_stop_loss:1400` | `_ocos_active` guard + `needs_reconciliation` fail-closed prevent auto-sell, but add explicit `enable_stop_loss_market_sell` flag in v2.1 |
| OPEN_005 | No Persian operational dashboard UI — owner must query SQLite directly | Low | `OWNER_OPERATIONAL_REPORT_GUIDE_FA.md` | Future: add read-only `operations.md` dashboard queries |

## 3. UNVERIFIED Items — Not Claimed as COMPLIANT

| Item | Why UNVERIFIED | Next Step |
|------|----------------|-----------|
| Live global halt (`canTrade != true`) | Never observed on live account | Mocked test only; live observation after D003 |
| Live two-OCO placement | Mocked via FakeMexc | Live 3 USDT test after D003 |
| Production restart with live open orders | No live restart occurred | Simulate with disposable live sub-account (owner auth) |
| Third-token fee live valuation | No such fee observed | Keep unvalued until encountered |

## 4. Operational Restrictions Acknowledgment

During this mission: **no live login, no orders placed, no cancels, no credential/DB/production replay, no deploy/restart without auth** (§14). Verified via `git diff` (only `tg_signal_ingest/`+`tests/`+`docs/`+`reports/` mocked changes) and `block_internet_sockets` fixture.

## 5. How to Approve

Comment in PR: `Approved D001=A D002=A D003=A D004=A D005=A — proceed to live 3 USDT verification` and push will be done only to `arena/e452655b-tele-mexc` after your explicit GitHub reconnect.

## 6. Prior Audit Unverified (2)

Prior audit's 2 UNVERIFIED (live balance snapshot staleness, Futures deny-list) are now **COMPLIANT (mocked)** and documented as UNVERIFIED live above — no hidden gap.
