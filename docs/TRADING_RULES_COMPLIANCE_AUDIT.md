> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

# Trading Rules Compliance Audit — Read-Only Code Inspection

**Audit date:** 2026-10-31  
**Auditor:** Senior software architect / independent trading-systems auditor (read-only)  
**Repository:** `20kevit/tele-mexc`  
**Branch:** `arena/e452655b-tele-mexc`  
**HEAD commit:** `71a952b6b35df676fa9e7dc74c97f41baace9ecd` (Merge PR #3 from `arena/035c9f0f-tele-mexc`)  
**Working-tree status at audit start:** `clean` (no uncommitted changes before this mission)  
**Mission branch history:** branched from `main`; this mission creates only the three documentation files permitted by its execution boundaries.  
**Official rule documents:** `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md` and `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md` (v1.0, 2026-10-31) — authoritative reference for this audit.  
**Previous verification notes:** `docs/trading.md`, `docs/INDEPENDENT_AUDIT_2026-10-29.md`, `docs/TRADING_DECISIONS.md`, `docs/exchanges/MEXC_SPOT_*.md` — treated as context only; **code is the source of truth** and all findings below are traced to actual code paths, not to documentation claims or test names.

> **Read-only boundary respected:** No Python or other application source was modified; no test, config, `.env`, secret, database, or session file was altered; no order was placed, canceled, amended, or reconciled against a live exchange; no service was restarted or deployed; no position or order for PENDLEUSDT or MYXUSDT was inspected on-exchange or modified. All evidence is from static code inspection, Git metadata, and existing local tests executed only where safe and non-mutating.

---

## 1. Executive Summary

The repository implements a **durable, idempotency-first intent pipeline** (signal → `order_intents.planned` → `submitting` → exchange → `submitted/open/filled/canceled/unknown_requires_reconciliation` with `fill_sync_required`) that is materially stronger than the average Telegram-to-exchange bot in handling uncertain submissions, duplicate deliveries, and crash windows. Signal parsing, activation-boundary replay protection, fixed-quote sizing, and per-intent atomic claim semantics are **confirmed and enforced**.

**The material defect is the absence of the global safety gate required by Section 5 of the non-negotiable rules.** The code has no persistent "block all new BUYs while any bot-owned position is unprotected or has an unresolved exchange outcome" invariant, no survival of such a block across restarts, and no coordinated clearing rule. As a consequence, a second BUY can be submitted while a first position's exit protection is still uncertain, and recovery can accept new BUYs before existing exposure is proven safe. This single architectural gap cascades into **eight NON-COMPLIANT findings** (five P0, three P1/P2) and is the highest-priority remediation.

Additional confirmed gaps: mandatory SL/TP1/TP2 presence and the `SL < Entry < TP1 ≤ TP2` price invariant are **not enforced** before execution; realized vs. unrealized P&L is **not modeled**; third-token fee valuation is **defective** (F5, carried forward from the 2026-10-29 independent audit); and the native OCO contract remains **unverified against a live exchange** (F4).

| Requirement group | Total numbered items | COMPLIANT | PARTIALLY COMPLIANT | NON-COMPLIANT | UNVERIFIED | NOT APPLICABLE |
|---|---|---|---|---|---|---|
| 1. Market and signal eligibility (1.1–1.10) | 10 | 9 | 1 | 0 | 0 | 0 |
| 2. Position sizing and BUY execution (2.1–2.8) | 8 | 4 | 2 | 2 | 0 | 0 |
| 3. Mandatory exit strategy (3.0–3.10 + price invariant) | 12 | 6 | 4 | 2 | 0 | 0 |
| 4. Stop-loss and execution safety (4.1–4.7) | 7 | 3 | 3 | 0 | 1 | 0 |
| 5. Unprotected positions and global BUY blocking (5.1–5.7) | 7 | 1 | 2 | 4 | 0 | 0 |
| 6. Recovery and reconciliation (6.1–6.9) | 9 | 7 | 1 | 1 | 1 | 0 |
| 7. Auditability and financial correctness (7.1–7.7) | 7 | 5 | 1 | 1 | 0 | 0 |
| 8. Protected legacy positions (8.1) | 1 | 1 | 0 | 0 | 0 | 0 |
| **Total audited** | **61** | **36** | **14** | **10** | **2** | **0** |

**P0 findings (could cause unauthorized trading, unprotected exposure, duplicate BUYs, or unintended sales): 10**  
**P1 findings (could cause incorrect order management, recovery, accounting, or missed protection): 14**  
**P2 findings (documentation/observability/maintainability): 2**

> Do not claim production safety from unit tests alone. The audit distinguishes **confirmed defects**, **theoretical risks with a concrete failure scenario**, and **UNVERIFIED** where exchange semantics or live evidence are unavailable.

---

## 2. Methodology and Evidence Limitations

### 2.1 Tracing approach

Each rule was traced from **Telegram event reception** through **storage → queue → parsing → risk → sizing → balance preflight → intent creation → exchange submission → reconciliation → trade lifecycle**, with explicit attention to:

- Every path that can create a live BUY (`TradingEngine._execute_intent` → `_execute_live` → `client.place_order` and `TradeManager._submit_entry_order` → `engine._execute_intent` conditional-entry re-entry).
- Every path that can create or reconcile an exit (`TradeManager._place_oco_exits`, `_do_tp_sale`, `_enter_stop_loss`, `_reconcile_trade`, `reconcile_all_open`, `recover_on_startup`, `TradingEngine.reconcile_intent`).
- Every persistence and locking boundary (`Database.transaction()` + `threading.RLock` at `tg_signal_ingest/db.py:1007`, `tg_signal_ingest/trading/engine.py:74–108`, `tg_signal_ingest/trading/store.py:863–882`).
- Every test that claims to verify the behavior (run inventory; no reliance on names).

### 2.2 Evidence limitations that force UNVERIFIED

- **Live MEXC behavior has not been exercised from this repository.** `docs/trading.md` explicitly notes no live OCO has been placed; the OCO adapter is validated against a CLI schema (`@mexc-ai/cli` v0.0.22) and mocked lifecycles, not a real fill/cancel/cross-branch observation. Triggered-OCO listing, sibling-cancel, and conditional-entry execution semantics therefore remain **unverified**.
- **CLI single-order resolution is paged.** `MexcCliAdapter` resolves `clientOrderId` against up to 100 rows of `current-orders` then `history-orders`; beyond that an order can remain `unknown` without proving non-existence. The hybrid live client delegates single-order queries to REST, but REST visibility of CLI-created OCO children is **not established**.
- **Multi-process races were not crash-injected.** The DB RLock serializes threads in one process, but concurrent external tooling or two `serve` processes (if the `SingleInstanceLock` at `tg_signal_ingest/locking.py:42` were bypassed) were not modeled.
- **Fee valuation for third-token fees lacks a conversion source.** The defect (F5) is confirmed by inspection; its financial impact cannot be quantified without a proven provenance-bearing conversion.

Unknown or unverified behavior is reported as `UNVERIFIED` with an explicit evidence-limitation note, per the rule documents.

---

## 3. Compliance Matrix (Summary)

Statuses: `COMPLIANT` — enforced by actual runtime paths and supported by relevant tests. `PARTIALLY COMPLIANT` — some paths enforced, coverage incomplete. `NON-COMPLIANT` — violates or lacks mandatory enforcement. `UNVERIFIED` — insufficient evidence to establish behavior.

### 3.1 Market and signal eligibility

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 1.1 | MEXC Spot exclusively | **COMPLIANT** | P0 | `tg_signal_ingest/mexc/*`, `tg_signal_ingest/trading/parser.py:22–46`, `tg_signal_ingest/trading/risk.py:1–6` |
| 1.2 | Only valid LONG/BUY eligible | **COMPLIANT** | P0 | `tg_signal_ingest/trading/risk.py:57–60`, `tg_signal_ingest/trading/parser.py:175–185` |
| 1.3 | SHORT completely ignored | **COMPLIANT** | P0 | `tg_signal_ingest/trading/risk.py:57–60`, `tg_signal_ingest/trading/engine.py:679–704` |
| 1.4 | Telegram SELL never triggers sale | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:679–704`, `tg_signal_ingest/trading/parser.py:175–185` |
| 1.5 | No leverage/margin | **COMPLIANT** | P0 | `tg_signal_ingest/trading/parser.py:46–59` (`_NON_SPOT_RE`) |
| 1.6 | Signals from authorized channel only | **COMPLIANT** | P0 | `tg_signal_ingest/service.py:206–270`, `tg_signal_ingest/repository.py:94–126` |
| 1.7 | Duplicate signals not create duplicate trades | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:227–260`, `tg_signal_ingest/trading/store.py:553–610`, `tg_signal_ingest/trading/store.py:863–882` |
| 1.8 | Old/rejected/ambiguous/historical/recovery not replayed | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:126–230`, `tg_signal_ingest/trading/store.py:1193–1241` |
| 1.9 | Incomplete/invalid rejected before BUY | **COMPLIANT** | P1 | `tg_signal_ingest/trading/parser.py:76–130`, `tg_signal_ingest/trading/risk.py:34–88` |
| 1.10 | Auditable link to original Telegram message | **COMPLIANT** | P1 | `tg_signal_ingest/trading/store.py:41–90`, `tg_signal_ingest/repository.py:94–130` |

### 3.2 Position sizing and BUY execution

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 2.1 | Fixed quote amount per trade | **COMPLIANT** | P0 | `tg_signal_ingest/trading/risk.py:94–112`, `tg_signal_ingest/trading/sizing.py:28–62`, `tg_signal_ingest/config.py:91–93` |
| 2.2 | Default stake documented as config, default 3 USDT | **COMPLIANT** | P2 | `tg_signal_ingest/config.py:91–93`, `tg_signal_ingest/trading/runtime.py:10–50` |
| 2.3 | No aggregate 50 USDT cap | **COMPLIANT** | P0 | `tg_signal_ingest/trading/store.py:382–440` (no enforcement) |
| 2.4 | No BUY unless all validations pass | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:1163–1208` (symbol, balance, risk) — missing pre-BUY protection gate |
| 2.5 | Every BUY traceable to unique signal + controlled attempt | **COMPLIANT** | P0 | `tg_signal_ingest/trading/store.py:863–882`, `tg_signal_ingest/trading/engine.py:405–406`, `tg_signal_ingest/db.py:1007–1081` |
| 2.6 | Ambiguous outcomes reconciled before next submission | **NON-COMPLIANT** | P0 | No global gate; see 5.2 |
| 2.7 | No blind retries | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:1220–1252`, `tg_signal_ingest/trading/engine.py:1595–1620` |
| 2.8 | Concurrent BUY not bypass global gate | **NON-COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:74`, `tg_signal_ingest/trading/store.py:863–882` — per-intent lock only |

### 3.3 Mandatory exit strategy

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 3.0a | Every LONG has valid SL + TP1 + TP2 | **NON-COMPLIANT** | P0 | `tg_signal_ingest/trading/trade_manager.py:284–340`, `tg_signal_ingest/trading/risk.py:1–10` confirms no mandatory-SL rule |
| 3.0b | Price relation SL < Entry < TP1 ≤ TP2 | **NON-COMPLIANT** | P0 | Parser `tg_signal_ingest/trading/parser.py:76–130` and `tg_signal_ingest/trading/risk.py:34–88` do not enforce; `_create_trade` stores without validation |
| 3.1 | Exit orders confirmed by MEXC | **PARTIALLY COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:1061–1117`, `tg_signal_ingest/trading/trade_manager.py:404–418`, `tg_signal_ingest/mexc/cli.py:788` |
| 3.2 | Protection coverage measured against actual remaining quantity | **COMPLIANT** | P0 | `tg_signal_ingest/trading/trade_manager.py:881–895`, `tg_signal_ingest/trading/trade_manager.py:1541–1549` |
| 3.3 | Partial fills accounted accurately | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:1786–1810`, `tg_signal_ingest/trading/trade_manager.py:1702–1765` |
| 3.4 | Quantity precision / lot-size respected | **COMPLIANT** | P1 | `tg_signal_ingest/mexc/symbols.py:180–260`, `tg_signal_ingest/trading/engine.py:485–514` |
| 3.5 | Never declared fully protected on local assumptions | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/trade_manager.py:1150–1172` (fixed in 2026-10-29 certainty PR; residual REST/OCO child gap remains) |
| 3.6 | TP1/TP2 allocation per authorized strategy, no invented 50/50 | **PARTIALLY COMPLIANT** | P2 | `tg_signal_ingest/config.py:105–109` (`tp1_allocation_pct_of_acquired=50.0`, `tp2_allocation_pct_of_remaining=100.0`), `docs/TRADING_DECISIONS.md:4` documents ~50% as shipped; no separate owner-approval record in repo |
| 3.7 | After TP1, remaining has valid protection and TP2 handling | **COMPLIANT** | P0 | `tg_signal_ingest/trading/trade_manager.py:441–468`, `tg_signal_ingest/trading/trade_manager.py:642–657` |
| 3.8 | OCO leg execution/cancellation not misinterpreted as trade closure | **COMPLIANT** | P1 | `tg_signal_ingest/trading/trade_manager.py:1008–1012`, `tg_signal_ingest/trading/trade_manager.py:1541–1565` |
| 3.9 | Partial exit fills leave remaining correctly protected | **COMPLIANT** | P1 | `tg_signal_ingest/trading/trade_manager.py:881–895` (`TP2` sized from actual `tp1_sold_qty`, not just allocation) |
| 3.10 | Missing/rejected/expired/canceled/disappeared exits reconciled | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/trade_manager.py:1676–1720` (retry cap, reconcile; NOT_FOUND handling keeps uncertain, but single-order page limit can strand) |

### 3.4 Stop-loss and execution safety

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 4.1 | No automatic Market Sell fallback | **COMPLIANT** | P0 | **Current:** `tg_signal_ingest/trading/trade_manager.py:921–980` (`_submit_tp_order` on failure → `unknown_requires_reconciliation`, never market fallback) and `trade_manager.py:1408` (`_enter_stop_loss` → `_handle_unprotected_sl` → `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL`, never market). **Historical:** `trade_manager.py:1351-1475` was stop-loss market path, now removed (see 4.2) |
| 4.2 | No emergency Market Sell without explicit owner authorization | **COMPLIANT** | P0 | **Current:** `tg_signal_ingest/trading/engine.py:1553–1572` (`submit_stop_loss_market_sell` raises `RuntimeError` — disabled, grep confirms zero reachable callers outside tests); SL via verified native OCO `STOP_LIMIT` only, missing/rejected/uncertain → `needs_reconciliation`+`SL_UNPROTECTED_CRITICAL`+read-only reconcile (see `trade_manager.py:400-460,1408,1433-1450`). **Historical:** `engine.py:1466-1491` was market-sell path, now removed (owner-confirmed §7) |
| 4.3 | Telegram SELL not override restriction | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:679–704` |
| 4.4 | SL order type/trigger/offset not changed casually | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/config.py:105–109`, `tg_signal_ingest/trading/trade_manager.py:1016–1130` (configurable without mandatory review gate) |
| 4.5 | Change to SL not cause immediate Market execution/fees | **UNVERIFIED** | P1 | No evidence that a market-SL for `price ≤ SL` cannot execute through thin book with excess slippage/fees; no fee-impact test |
| 4.6 | Uncertainty about MEXC conditional/OCO semantics documented as unverified | **COMPLIANT** | P1 | `docs/trading.md:Remaining limitations` and `docs/TRADING_DECISIONS.md:5` |
| 4.7 | Never cancel/amend/replace orders merely to make local state consistent | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/service.py:556–569` vs. `docs/operations.md` (F6: diagnostic-labelled paths are action-capable) |

### 3.5 Unprotected positions and global BUY blocking

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 5.1 | BUY fills without confirmed protection → durable critical state | **PARTIALLY COMPLIANT** | P0 | `tg_signal_ingest/trading/trade_manager.py:1106–1172`, `tg_signal_ingest/trading/store.py:762–806` → `needs_reconciliation` is durable but does **not** block new BUYs |
| 5.2 | Block all new BUYs while any position unprotected/unresolved | **NON-COMPLIANT** | P0 | No `has_unprotected_position()` gate; grep for `unprotected|global.*block|BUY.*block` shows only comments about one trade, not a cross-trade gate |
| 5.3 | Global block survives restarts | **NON-COMPLIANT** | P0 | No persisted block key in `app_state`; `trading_budget_ledger` survives restarts but is spend-tracking, not a block |
| 5.4 | Block cleared only after every position closed or protected | **NON-COMPLIANT** | P0 | No clearing predicate; individual trade `closed` does not check peer trades |
| 5.5 | One protected trade never clears block while another unprotected | **NON-COMPLIANT** | P0 | No block to clear; per-trade state transitions at `tg_signal_ingest/trading/trade_manager.py:1680–1720` are independent |
| 5.6 | Failed/incomplete reconciliation never proof of no exposure | **COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:1605–1638` (`-2011` stays `unknown_requires_reconciliation`) |
| 5.7 | Check-and-submit safe against concurrent tasks/processes | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:74–108` + `tg_signal_ingest/trading/store.py:863–882` safe per-intent; unsafe for global gate; cross-process safety relies on `SingleInstanceLock` at `tg_signal_ingest/locking.py:42` and DB `RLock` at `tg_signal_ingest/db.py:1007`, not on an atomic business-rule transaction |

### 3.6 Recovery and reconciliation

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 6.1 | Exchange state authoritative | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:1576–1846` (`_map_exchange_state` respects exchange) |
| 6.2 | Local DB reconciled after restarts/outages | **COMPLIANT** | P1 | `tg_signal_ingest/service.py:540–569`, `tg_signal_ingest/trading/trade_manager.py:262–283` |
| 6.3 | Recover missing fills and exchange-side orders | **COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:1786–1810` (`myTrades`), `tg_signal_ingest/trading/trade_manager.py:1702–1765` |
| 6.4 | Not submit duplicate BUYs during recovery | **COMPLIANT** | P0 | Idempotency at `tg_signal_ingest/trading/store.py:553–610`; `recover_on_startup` only queries |
| 6.5 | Unknown outcomes remain unknown until reconciled | **COMPLIANT** | P0 | `tg_signal_ingest/trading/engine.py:1595–1620`, `tg_signal_ingest/mexc/errors.py:62` |
| 6.6 | Recovery not silently replay historical/rejected signals | **COMPLIANT** | P0 | `tg_signal_ingest/service.py:306–365` (source=`recovery` → `tg_signal_ingest/trading/engine.py:126–230` historical gate) |
| 6.7 | New BUYs remain blocked until recovery proves safe | **NON-COMPLIANT** | P0 | Same gap as 5.2; `_reconciliation_worker` does `recover_on_startup` then `reconcile_pending` but does not gate `process_item` on a safe flag |
| 6.8 | OCO sibling cancellation interpreted per actual exchange behavior | **UNVERIFIED** | P1 | `tg_signal_ingest/mexc/hybrid.py`, `tg_signal_ingest/mexc/cli.py` — F4; no branch/child evidence model; fakes model each OCO as one fillable order |
| 6.9 | DB not marked complete/terminal while exposure unresolved | **COMPLIANT** | P0 | `tg_signal_ingest/trading/trade_manager.py:1550–1565` (`_finalize_trade` refuses to close when `remaining > 0`) |

### 3.7 Auditability and financial correctness

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 7.1 | Every decision auditable | **COMPLIANT** | P1 | `tg_signal_ingest/trading/store.py:41–90` (`audit_events`), `tg_signal_ingest/trading/store.py:863–910` (`order_status_history`) |
| 7.2 | Original content and extracted fields traceable | **COMPLIANT** | P1 | `tg_signal_ingest/repository.py:94–130` (`messages` + `signals`), `tg_signal_ingest/trading/parser.py:76–130` |
| 7.3 | Actual vs requested quantities distinguished | **COMPLIANT** | P1 | `tg_signal_ingest/trading/store.py:706–900` (`base_quantity` vs `executed_qty`, `acquired_base_qty` vs `sold`) |
| 7.4 | Fees accounted in realized results | **PARTIALLY COMPLIANT** | P1 | `tg_signal_ingest/trading/store.py:500–540` (`_fees_for_order`) — F5: third-token fee × pair price, no conversion; `trade_manager` sums raw amounts across currencies |
| 7.5 | Distinguish realized from unrealized P&L | **NON-COMPLIANT** | P1 | No realized/unrealized model; `trading_budget_ledger` is cumulative tracked spend, per `tg_signal_ingest/trading/store.py:382–390` and `docs/trading.md` |
| 7.6 | Failed/partial/unknown/reconciled not conflated | **COMPLIANT** | P1 | Distinct states at `tg_signal_ingest/trading/store.py:14–20`, `tg_signal_ingest/trading/trade_store.py:40–46` |
| 7.7 | Notifications not claim success before verified | **COMPLIANT** | P1 | `tg_signal_ingest/trading/engine.py:737–830` (`_order_status_label`, `_headline_for_status` include `UNCERTAIN_AWAITING_RECONCILIATION`) |

### 3.8 Protected legacy positions

| Rule | Short title | Status | Severity | Primary location |
|---|---|---|---|---|
| 8.1 | PENDLEUSDT and MYXUSDT protected legacy — no interference | **COMPLIANT** | P0 | No code path enumerates these symbols; grep for `PENDLE\|MYXUSDT\|protected legacy` in `tg_signal_ingest/` returns 0. This mission's doc-only change created no exchange write. **Residual P1:** no explicit deny-list means a future signal for those symbols would still be tradable; recommend adding a persisted legacy-protection filter. |

---

## 4. Detailed Findings (per requirement)

Each finding lists: Rule ID · Status · Severity · File/function · Line refs · Failure scenario (concrete, not hypothetical where possible) · Existing test coverage · Evidence limitations · Recommended remediation (description only).

### 4.1 Section 1 — Market and signal eligibility

#### 1.1 MEXC Spot exclusively — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/mexc/client.py` (REST `api.mexc.com` Spot v3), `tg_signal_ingest/mexc/cli.py` (CLI `spot.trade.*`), `tg_signal_ingest/trading/parser.py:22–59` (Spot-only allow-list; `_NON_SPOT_RE` rejects Futures/perp/margin/leverage), `tg_signal_ingest/trading/risk.py:1–6` (policy header).
- **Enforcement:** Parser marks leveraged/Futures text as `ambiguous` with reason `"unsupported Futures/perpetual/leveraged instruction (Spot BUY only)"`; risk layer rejects non-BUY and ambiguous; no Futures/Margin endpoint is imported or called.
- **Bypass/race:** None — channel ingestion is Spot-agnostic; enforcement is post-parse.
- **Tests:** `tests/test_parser.py`, `tests/test_trading_boundary.py`, `tests/test_mexc_*` confirm Futures text is rejected and only Spot orders are placed.
- **Evidence limit:** Live Futures-rejection was not exercised against a real channel posting a leveraged signal.
- **Remediation:** None required; preserve `no withdrawals, no futures, no leverage` in any future MEXC adapter.

#### 1.2 Only valid LONG/BUY eligible — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/parser.py:175–185` (`_action` detects `BUY/LONG` vs `SELL/SHORT`), `tg_signal_ingest/trading/risk.py:57–60`.
- **Tests:** `tests/test_signals.py` parametrized LONG vs SHORT.
- **Remediation:** None.

#### 1.3 SHORT signals must be completely ignored — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/risk.py:57–60` (`SHORT/SELL signals are not executed; this bot is Spot BUY only`), `tg_signal_ingest/trading/engine.py:194–205` (cancel handling does not place SELL orders).
- **Failure scenario tested:** A message `"SHORT BTCUSDT ENTRY 100 SL 105 TP 90"` is parsed `action=sell`, then rejected with `IGNORED_SELL_SPOT_BUY_ONLY`; no BUY or SELL is submitted (`test_trading_engine.py:79–90` pattern).
- **Remediation:** None.

#### 1.4 Telegram SELL instructions never trigger sale — COMPLIANT / P0

- **Implements:** Same as 1.3; sell signals produce `REJECTED_LOCALLY` with no exchange call; `_handle_cancel` (`tg_signal_ingest/trading/engine.py:1522–1540`) only cancels existing intents, never submits a sell for non-owned balances.
- **SELL vs CANCEL distinction:** `parser.py:_SELL_RE` vs `_CANCEL_RE` (`^CANCEL|CANCELLED|…`); only `cancel`/`delete` paths call `cancel_order`, never `place_order(SIDE=SELL)` from a Telegram sell direction.
- **Remediation:** None.

#### 1.5 No leverage or margin — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/parser.py:46–59` rejects leverage patterns (`\bLEVERAGE\b`, `\b10x\b`, `FUTURES|PERPETUAL|MARGIN`).
- **Remediation:** None.

#### 1.6 Signals from configured authorized channel only — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/service.py:206–270` (`_is_target` bound to `selected_channel_id`/`marked_channel_id`), `tg_signal_ingest/repository.py:94–126` (`selected_channel` selection).
- **Every relevant execution path:** Live listeners (`NewMessage`, `MessageEdited`, `UpdateDeleteChannelMessages`) gate on `_is_target`; recovery `sweep_gap`/`refresh_recent_window` take `self._entity` for the selected channel only.
- **Bypass:** Telegram usernames never used for authorization; `control/auth.py` and `management/bot.py` use numeric `operator_id`.
- **Tests:** `tests/test_service.py`, `tests/test_channel_trading_boundary.py`.
- **Remediation:** None; retain `proxy rejected` rule (`tg_signal_ingest/config.py:14–24`) and `flock` so direct networking cannot be proxied.

#### 1.7 Duplicate signals not create duplicate trades — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:243–260` (idempotency key `sha256(channel:msg:action:symbol)`), `tg_signal_ingest/trading/store.py:553–610` (`INSERT … WHERE idempotency_key` dedup), `tg_signal_ingest/trading/store.py:863–882` (`claim_intent_for_submission` atomic `planned→submitting`).
- **Race:** `engine.py:74` `_process_lock = threading.RLock()` plus DB lock at `tg_signal_ingest/db.py:1007` and unique `idempotency_key` constraint prevent two simultaneous identical messages from inserting two intents.
- **Tests:** `tests/test_trading_engine.py:test_edit_reuses_message_identity_and_never_places_second_order`.
- **Remediation:** None; maintain `client_order_id = "tsi" + idempotency_key[:28]` stability across restarts.

#### 1.8 Old/rejected/ambiguous/historical/recovery not replayed — PARTIALLY COMPLIANT / P1

- **Implements:** Activation boundary (`tg_signal_ingest/trading/store.py:1193–1241`) with `ensure_activation_boundary` advancing forward-only on every `serve` start; historical gate at `tg_signal_ingest/trading/engine.py:126–230` (checks `source in ("backfill","recovery")` or `message_id ≤ boundary`).
- **Failure scenario (PARTIAL):** An ambiguous signal (missing entry) is stored `signal_kind=ambiguous` and rejected with `rejected_replay` or `rejected_ambiguous`. A duplicate delivery of the **same `(channel, message_id, edit_version)`** after a transient failure (e.g., `symbol metadata unavailable`) could be retried and, if metadata is now available, reach `create_intent` and place a BUY — the second delivery re-parses the same `signal_id` and does not check that its previous signal state was `rejected_ambiguous` before risk re-evaluation. The edit-conflict path at `engine.py:231–242` only blocks a *different* action/symbol edit, not a same-content retry after a local rejection. Historical gate is strong; **rejected→retried→executed for the same message identity** is the gap.
- **Tests:** `tests/test_analysis_replay.py`, `tests/test_backfill.py` cover backfill/recovery blocking; no test asserts that a locally-rejected same-version message cannot become planned on a retried queue delivery.
- **Evidence limit:** Requires a queue retry after a local rejection; not observed in offline suite.
- **Remediation (description only):** Add a durable `signals.status = rejected` → intent-creation refusal for the same `(channel, message, edit_version)` unless the signal is superseded by a newer `edit_version`; or make `create_intent` fail closed when `signals.status` is terminal rejected for that `signal_id`.

#### 1.9 Incomplete/invalid rejected before BUY — COMPLIANT / P1

- **Implements:** Parser `tg_signal_ingest/trading/parser.py:76–130` returns `ambiguous` when entry/symbol/direction missing; risk `tg_signal_ingest/trading/risk.py:34–88` fails closed on missing symbol/direction/entry; engine `tg_signal_ingest/trading/engine.py:275–310` rejects before sizing/claim.
- **Remediation:** None.

#### 1.10 Auditable link to original Telegram message — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/store.py:41–90` (`signals.signal_id = channel:msg:version`, `fingerprint_sha256`, audit correlation), `tg_signal_ingest/repository.py:94–130` (`messages` retains `text/caption/processing_version/source`), `order_intents.create_channel_id/create_msg_id/signal_id` at `store.py:553–610`.
- **Tests:** `tests/test_repository.py` verifies message→signal→intent traceability.
- **Remediation:** None; ensure `messages.raw_json` and `message_edits` retention is not pruned.

### 4.2 Section 2 — Position sizing and BUY execution

#### 2.1 Fixed quote amount per trade — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/risk.py:94–112` (uses only `cfg.trading.order_quote_amount`, never `amount_text`), `tg_signal_ingest/trading/sizing.py:28–62` (fixed `buy_quote_usd`, deletes `max_quote`), `tg_signal_ingest/config.py:91–93`.
- **Signal stake text ignored:** `parser.py:_extract_amount` captures `amount_text` for auditing only; `sizing.apply_sizing` never reads it.
- **Tests:** `tests/test_sizing.py`, `tests/test_trading_config.py`.
- **Remediation:** None; retain rejection of `max_quote` inference from signal text.

#### 2.2 Default stake documented as config, default 3 USDT — COMPLIANT / P2

- **Implements:** `tg_signal_ingest/config.py:91–93` (`order_quote_amount: float = 3.0`, `order_quote_amount_source: str = "default (3 USDT)"`, `amount_configured: bool = True`), `tg_signal_ingest/trading/runtime.py:10–50` (DB override precedence `env > Telegram menu/DB > config.toml > default`), `docs/trading.md:2` documents 3 USDT as software placeholder, not sanctioned value.
- **Hardcoded rule check:** Grep for `3` in sizing/risk shows no immutable trading rule; `order_quote_amount` is always read from `cfg.trading`.
- **Remediation:** None.

#### 2.3 No aggregate 50 USDT cap — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/store.py:382–440` (`aggregate_budget_quote` is a reference, never enforced; `_reserve_budget` does not compare to it). Comment at `store.py:428` is explicit.
- **Tests:** `tests/test_trading_budget.py` asserts spend tracking without cap.
- **Contradiction to flag:** Legacy docs mentioning a "50 USDT cap" are superseded; current code and `docs/trading.md` correctly state the ledger is reference-only.
- **Remediation:** None.

#### 2.4 No BUY unless all validations pass — PARTIALLY COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/engine.py:1163–1208` enforces `symbol metadata`, `balance preflight`, `risk` before `submitting`; `execute_approved_intent` at `engine.py:860–915` revalidates before operator approval.
- **Gap:** No validation that a valid **exit protection plan** (SL/TP1/TP2 per 3.0) exists before the BUY; the intent pipeline is entry-first, exit-second. Requirement 2.4's "exit-protection validations" therefore has no pre-BUY enforcement. Combined with the missing global unprotected-position gate (5.2), a BUY can pass all existing gates while exit protection for prior fills is still unverified.
- **Tests:** `tests/test_trading_preflight.py` covers balance/symbol; no test asserts "BUY rejected when TP2 price missing" at the risk gate.
- **Remediation:** Either (a) reinterpret 2.4 as "exit-protection validation after fill, gating further BUYs via 5.2" and implement the global block, or (b) add a pre-BUY check that required exit fields are parseable per 3.0 before planning an intent.

#### 2.5 Every BUY traceable to unique signal + controlled execution attempt — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/store.py:553–610` (`idempotency_key` + `client_order_id` + `intent_id = "intent-" + client_order_id`), `tg_signal_ingest/trading/store.py:863–882` (`claim_intent_for_submission` returns only for the winner), audit at `store.py:41–90`.
- **Remediation:** None.

#### 2.6 Ambiguous exchange outcomes reconciled before another submission — NON-COMPLIANT / P0

- **Expected:** As long as any intent is `unknown_requires_reconciliation`, no new BUY should be planned.
- **Actual:** `unknown_requires_reconciliation` is terminal-for-retry but not a global gate; `TradingEngine._process_item` at `engine.py:243–310` does not query `store.count_intents_by_state()` or `pending_reconciliation()` before risk/size checks. A new signal while a prior submission is uncertain will proceed through risk → `_reserve_budget` → `submitting` independently.
- **Failure scenario:** Live BUY for BTCUSDT gets `MexcNetworkError("timeout after send")` → `unknown_requires_reconciliation`. While reconciliation retries, a second Telegram signal for ETHUSDT arrives and is processed on the same `ProcessingEngine` tick; it reserves budget and submits its own BUY even though total bot-owned exposure is still unknown — violating 2.6, 5.2, 6.7.
- **Tests:** `tests/test_trading_engine.py:test_network_uncertainty_never_retries` verifies no blind retry of the *same* intent, but no test asserts that a *different* intent is blocked while the first is uncertain.
- **Remediation:** Introduce a persisted global gate `app_state["trading.buy_blocked"]` with reason and scope, checked atomically inside the same `Database.transaction()` that claims the intent; clear only via the 5.4 predicate; gate must be read in `process_item` before any BUY, not after.

#### 2.7 No blind retries — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:1220–1252` (`MexcDuplicateOrder`, `MexcNetworkError`, `MexcServerError`, `MexcRateLimitError`, `MexcWafError` → `unknown_requires_reconciliation`, never retried with a new `clientOrderId`), `tg_signal_ingest/trading/trade_manager.py:921–980` (TP/OCO/Market-sell uncertain → `unknown_requires_reconciliation`, no auto-resubmit with new ID).
- **Tests:** `tests/test_trading_engine.py:test_network_uncertainty_never_retries`, `tests/test_trading_cli_protection.py`.
- **Remediation:** None.

#### 2.8 Concurrent BUY not bypass global gate — NON-COMPLIANT / P0

- **Implements per-intent:** `tg_signal_ingest/trading/engine.py:74` (`_process_lock = threading.RLock()`) serializes `_process_item`; `claim_intent_for_submission` is atomic compare-and-set `planned→submitting`.
- **Missing global gate:** No `global_buy_block` row to check-and-set. Two simultaneous *different* signals (different `channel_id:message_id`) each hold their own `planned` row, each successfully claims `submitting`, and both submit — the RLock serializes the *decisions* but does not serialize on a shared global invariant.
- **Multi-process:** `SingleInstanceLock` (`tg_signal_ingest/locking.py:42`) would prevent two `serve` processes from both calling `process_item`, but a CLI `trading reconcile` or future operator tool could still race; the lock is process-level, not business-rule-level.
- **Tests:** `tests/test_locking.py` verifies `flock`, but no test delivers two signals concurrently and asserts only one passes a global gate.
- **Remediation:** Move the check-and-set into one SQLite transaction: `BEGIN IMMEDIATE` → `SELECT block_state FOR UPDATE` → `IF blocked THEN reject ELSE claim_intent` → `COMMIT`. This is safe against both threads and processes sharing the same SQLite WAL file.

### 4.3 Section 3 — Mandatory exit strategy

#### 3.0a Every LONG has valid SL+TP1+TP2 — NON-COMPLIANT / P0

- **Expected by rule:** A signal lacking any of SL, TP1, TP2 must not be executed as a LONG.
- **Actual:** `tg_signal_ingest/trading/parser.py:76–130` extracts SL/TPs opportunistically but returns `alert` even when `targets=()` and `stop=None` if entry/symbol/direction are present. `tg_signal_ingest/trading/risk.py:1–10` explicitly notes "*no mandatory stop-loss rules*". `tg_signal_ingest/trading/trade_manager.py:284–340` stores `tp1_price=None`, `tp2_price=None`, `sl_price=None` without rejecting. `TradingConfig.require_stop_loss` defaults to `True` at `tg_signal_ingest/config.py:82` but `risk.py:57–60` never reads it.
- **Failure scenario:** Message `"BUY BTCUSDT ENTRY 100 TP1 110"` (no SL, no TP2) is accepted, a LIVE LIMIT BUY is submitted, fills, and the only native exit is a single TP1 OCO or local SL/TP1 manager — violating the mandatory TP2 requirement.
- **Tests:** `tests/test_trading_engine.py:test_alert_without_stop_loss_is_accepted` explicitly asserts a signal without SL is accepted — contradicting this non-negotiable rule.
- **Remediation:** Enforce at the risk gate: `if not (sl and tp1 and tp2) → rejected` with reason naming the missing leg; make `require_stop_loss` and `require_tp_count` actually checked in `RiskManager.evaluate`; remove or quarantine `test_alert_without_stop_loss_is_accepted`.

#### 3.0b Price relation SL < Entry < TP1 ≤ TP2 — NON-COMPLIANT / P0

- **Expected:** `SL < Entry < TP1 ≤ TP2` for LONG.
- **Actual:** No enforcement; `engine.py:1005–1045` drops SL only if `SL ≥ Entry` (SL dropped, not rejection); no check that `TP1 > Entry` or `TP1 ≤ TP2`. `trade_manager.py:_create_trade` stores prices without comparison.
- **Failure scenario:** `ENTRY 100, SL 105, TP1 95, TP2 90` (all inverted) — entry BUY is still submitted; TP1 would be below entry, which for a LONG exit is a losing limit that may execute immediately in a thin book, and SL above entry would never trigger for a LONG while leaving the position unprotected against downside.
- **Remediation:** Enforce the invariant in `RiskManager.evaluate` (fail closed with a reason identifying the violated inequality) before any intent is created.

#### 3.1 Exit orders confirmed by MEXC — PARTIALLY COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:1061–1117` (`_verify_protection` queries `client.verify_protection` and requires echoed trigger prices), `tg_signal_ingest/trading/trade_manager.py:404–418` (`_exchange_protection_active` requires `order_protection.status == "active"`), `tg_signal_ingest/trading/trade_manager.py:1767–1805` (`_reconcile_trade_orders` queries each exit order).
- **Gap:** `order_protection` applies only when `client.supports_attached_protection` (`engine.py:1017`); for the shipped native OCO path, protection is inferred from `trade_orders` state. For CLI OCO, `MexcCliAdapter` resolves `clientOrderId` against only the first page of `current-orders` then `history-orders` before giving up → `unknown_requires_reconciliation`; a truly accepted OCO can remain `unknown` due to pagination, not due to exchange unavailability. The hybrid client's REST query path for CLI OCO children is undocumented and treated as `UNVERIFIED` in `docs/trading.md:Remaining limitations`.
- **Tests:** `tests/test_trading_cli_protection.py` validates echo mismatch → `unverified`; mocked OCO lifecycle tests do not prove real exchange acceptance.
- **Remediation:** After a live OCO is placed, persist parent/child order identities separately (F4), verify each child via its own REST order detail, and require at least `open` or `partially_filled` evidence for `oco_active` (already done since 2026-10-29) plus child-level fill evidence before declaring TP vs SL outcome.

#### 3.2 Protection measured against actual remaining bot-owned quantity — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:881–895` (`_compute_available = acquired - sold - outstanding`), `tg_signal_ingest/trading/trade_manager.py:1541–1549` (`_remaining_base = acquired - tp1_sold - tp2_sold - sl_sold`).
- **Remediation:** None.

#### 3.3 Partial fills accounted accurately — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:1786–1810` (`myTrades` persistence, `fill_sync_required` flag in same transaction as terminal state at `engine.py:1672–1680`), `tg_signal_ingest/trading/trade_manager.py:1680–1710` (entry fills reconciled before sizing exits).
- **Remediation:** None.

#### 3.4 Quantity precision / lot-size respected — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/mexc/symbols.py:180–260` (`quantize_quantity`, `quantize_price`, `validate_order`), `tg_signal_ingest/trading/engine.py:485–514` (`_quantize_decision`), `tg_signal_ingest/trading/trade_manager.py:921–980` (`_quantize_qty`, `_validate_sell`).
- **Tests:** `tests/test_mexc_hybrid.py`, `tests/test_trading_lifecycle_recovery.py` cover below-minimum residual handling.
- **Remediation:** None.

#### 3.5 Never declared fully protected on local assumptions — PARTIALLY COMPLIANT / P1

- **Implements (after 2026-10-29 fix):** `tg_signal_ingest/trading/trade_manager.py:1150–1172` — `UNCONFIRMED_ORDER_STATES = ("pending","submitting","open","partially_filled","cancel_requested","unknown_requires_reconciliation")`; `_ensure_oco_leg` treats `submitting/unknown` as unconfirmed regardless of old ID; `_ocos_active` requires `open/partially_filled` evidence.
- **Residual gap:** A CLI OCO REST query that returns `CANCELED` for a sibling after a TP leg filled can be mis-handled if the exchange reports the group as `CANCELED` (sibling auto-cancel) — current `_map_order_state` at `tg_signal_ingest/trading/engine.py:1822` maps `CANCELED` → `canceled` (terminal), and `_finalize_trade` at `trade_manager.py:1550–1565` relies on `sold` quantities, but without child-level fill evidence a fast TP fill followed by sibling cancel cannot be proven as a protected exit vs. an unfilled stop. Status certainty was fixed; **branch attribution** was not.
- **Evidence limit:** No live OCO fill with real parent/child records; F4 unchanged.
- **Remediation:** Record OCO parent id plus child TP/SL leg ids and statuses independently; `oco_active` must require working quantity covering the remaining bot-owned position (completed allocations do not add coverage), per `docs/trading.md:OCO certainty`.

#### 3.6 TP1/TP2 allocation per explicitly authorized strategy, no invented 50/50 — PARTIALLY COMPLIANT / P2

- **Implements:** `tg_signal_ingest/config.py:105–109` (`tp1_allocation_pct_of_acquired = 50.0`, `tp2_allocation_pct_of_remaining = 100.0`), `tg_signal_ingest/trading/trade_manager.py:284–340` (allocation computed once from actual acquired, persisted, never recomputed on restart unless pending).
- **Finding:** `docs/TRADING_DECISIONS.md:4` and `docs/trading.md:Exit strategy` authorize two native OCO exits of ~50% each; code follows that document. However the non-negotiable rule cautions "Do not invent or assume 50/50" — the code's default *is* 50/50 without a separate explicit owner-approval artifact in the repository (e.g., signed config.toml value or management-bot approval of allocation). The allocation is configurable but the default ships as 50/50.
- **Tests:** `tests/test_trading_lifecycle_recovery.py` asserts OCO allocations use actual acquired, not intended.
- **Remediation:** Record the allocation strategy as an owner-approved setting (like `order_quote_amount`) with source provenance, and require an explicit non-default approval to change it; do not rely on a code default as implicit authorization.

#### 3.7 After TP1, remaining has valid protection and TP2 handling — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:629–657` (`_enter_tp2` cancels TP1 remainder, reconciles fills, sells what is actually left), `tg_signal_ingest/trading/trade_manager.py:1351–1400` (stop-loss path also cancels TP remnants).
- **Remediation:** None.

#### 3.8 OCO leg execution/cancellation not misinterpreted — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:1008–1012` (`_ocos_terminal` requires both TP1 and TP2 orders terminal before finalize), `tg_signal_ingest/trading/trade_manager.py:1150–1188` (partial OCO handling).
- **Remediation:** None.

#### 3.9 Partial exit fills leave remaining correctly protected — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:881–895` (TP2 sized from `acquired - actual_sold`, not just `allocated`), `tg_signal_ingest/trading/trade_manager.py:1541–1549` (`_remaining_base`).
- **Remediation:** None.

#### 3.10 Missing/rejected/expired/canceled/disappeared exits reconciled — PARTIALLY COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:1676–1720` (reconcile each `trade_orders` row via `engine.query_trade_order`; rejected retries capped at `stop_loss_max_retries` at `trade_manager.py:930–945`; canceled held as terminal with `sold` retained).
- **Gap:** A disappeared order (exchange `-2011` for an exit `clientOrderId`) is mapped to `NOT_FOUND`/`unknown` and never blindly retried, but the exchange could have filled the order and the query page did not contain it — absence of evidence is treated as `ambiguous` and the position stays `needs_reconciliation` (fail-closed, which is correct) but **periodic reconciliation of exited positions is not proven to run indefinitely** after `reconcile_all_open` exits the worker; notification dedup (`trading_notifications.event_key` uniqueness) can suppress repeated alerts (F2, 2026-10-29).
- **Remediation:** Ensure indefinite reconciliation for `needs_reconciliation` trades with alert dedup scoped per-event-occurrence, not per `(intent, status)`.

### 4.4 Section 4 — Stop-loss and execution safety

#### 4.1 No automatic Market Sell fallback — COMPLIANT / P0

- **Implements:** No handler converts a failed/rejected limit exit to an automatic market sell. `tg_signal_ingest/trading/trade_manager.py:921–980` (`_submit_tp_order`) on `Mexc*` failure goes to `unknown_requires_reconciliation`, not market. `docs/trading.md:Remaining limitations` confirms market exit only via explicit stop-loss.
- **Remediation:** None.

#### 4.2 No emergency Market Sell without explicit owner authorization — COMPLIANT (current) / P0 — Historical note retained

- **Expected:** A distinct authorization flag for emergency market exits, separate from general live trading.
- **Historical (pre-2026-01, now removed):** `tg_signal_ingest/trading/engine.py:1466–1491` (`submit_stop_loss_market_sell`) submitted `side=SELL, type=MARKET` whenever `_enter_stop_loss` was reached (price ≤ SL), gated only by `LIVE_TOKEN == \"I_UNDERSTAND_REAL_MONEY_AND_RISKS\"`.
- **Current (native OCO-only, verified 2026-01-03):** `tg_signal_ingest/trading/engine.py:1553–1572` (`submit_stop_loss_market_sell` raises `RuntimeError` — disabled, zero `grep -rn submit_stop_loss_market_sell` hits outside `tests/` + `trade_store.py` legacy shim), `tg_signal_ingest/trading/trade_manager.py:1408` (`_enter_stop_loss` → `_handle_unprotected_sl` persists `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL` + audit, narrow per-symbol restriction, no market fallback), `tg_signal_ingest/trading/trade_manager.py:1433–1450` (`_resume_stop_loss`/`_submit_stop_loss_order` stubs). SL is via verified native OCO `STOP_LIMIT` legs only; missing/rejected/uncertain → `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL` + read-only reconcile, never autonomous market sell or replacement order.
- **Failure scenario (historical):** A misconfigured SL could have triggered `on_tick` → `_enter_stop_loss` → market sell. **Current:** same trigger now persists `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL` and notifies, never submits a market order. Verified by 6 spy tests `test_no_market_sell_*` (both OCOs active, OCO1 missing, OCO2 uncertain, rejection, cancel, restart) proving zero `place_order` market-sell calls.
- **Remediation (historical):** Dedicated flag was considered but owner confirmed **removal** of market-sell path instead (see `reports/trading_compliance/` and `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md:7`).

#### 4.3 Telegram SELL not override restriction — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:679–704` (SELL signals → `IGNORED_SELL_SPOT_BUY_ONLY`; no `place_order(SIDE=SELL)` from that path).
- **Remediation:** None.

#### 4.4 SL order type/trigger/offset not changed casually — PARTIALLY COMPLIANT / P1

- **Implements:** `tg_signal_ingest/config.py:105–109` exposes `entry_trigger_offset_ticks`, `tp1/tp2_allocation_pct`, `stop_loss_max_retries`, `monitor_*` as operator-tunable values; `tg_signal_ingest/trading/trade_manager.py:1016–1130` builds OCO stop-limit legs with `trigger_price = triggerLimitPrice = sl_price` at `engine.py:1431–1465`.
- **Gap:** Changes to `stop_loss_max_retries` or OCO `trigger_limit_price` semantics affect live execution and are not gated by a required owner review/restart confirmation; `apply_persisted_trading_settings` at `runtime.py:10–50` auto-applies DB settings on restart without re-approval.
- **Remediation:** Require an explicit owner confirmation step (and a restart) for any change to SL/OCO semantics; version the execution-behavior config and refuse to start live with an unapproved combination.

#### 4.5 Change to SL not cause immediate Market execution/fees — UNVERIFIED / P1

- **Why UNVERIFIED:** No inspected test places a live SL OCO or market stop-loss against a real order book and asserts non-immediate execution or fee bounds. Code review can confirm that OCO legs are `LIMIT`/`STOP_LIMIT`, not `MARKET`, but cannot prove the exchange will not fill them immediately when `price ≥ TP` or `price ≤ SL` in a thin book, nor can it bound maker/taker fees without live pricing. The requirement is about **behavioral impact**, not just code type.
- **Remediation:** Add a pre-live simulation with recorded book snapshots (not a live order) proving that an OCO stop-limit with `trigger = limit = SL` does not become a marketable order when the tick is already through the trigger; document the fee model and retain the `balance_fee_buffer_pct = 0.2` justification.

#### 4.6 Uncertainty about MEXC conditional/OCO semantics documented as unverified — COMPLIANT / P1

- **Implements:** `docs/trading.md:Remaining limitations` ("…Triggered OCO/exit behavior has not been validated with real credentials here"), `docs/TRADING_DECISIONS.md:5` ("No live OCO has been placed… treat … as unverified"), `docs/exchanges/MEXC_SPOT_RISKS.md`.
- **Remediation:** None; retain until a redacted live OCO trace with parent/child details is available.

#### 4.7 Never cancel/amend/replace orders merely to make local state consistent — PARTIALLY COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/engine.py:1576–1638` never cancels to fix local disagreement; `tg_signal_ingest/trading/trade_manager.py:1351–1475` cancels entry remainder and TP remnants only before placing the *next* lifecycle leg, which is intentional exposure management, not cosmetic consistency.
- **Gap (F6):** `tg_signal_ingest/service.py:556–569` comment says uncertain orders are "queried only" but `trade_manager.recover_on_startup` at `trade_manager.py:262–283` can place pending protection and retry cancels; `reconcile_all_open` at `trade_manager.py:1676–1720` is intentionally action-capable. An operator invoking `trading reconcile` for diagnostics could initiate writes expecting read-only behavior.
- **Remediation:** Split `reconcile --dry-run` (facts only) from `reconcile --act` and label every action-capable entry point; add tests asserting zero `place/cancel` calls for dry-run.

### 4.5 Section 5 — Unprotected positions and global BUY blocking

#### 5.1 BUY fills without confirmed protection → durable critical state — PARTIALLY COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:1106–1172` (`"partial OCO protection … position is unprotected and needs review"` → `needs_reconciliation`; allocation below lot size → `unsold_residual_qty` + `needs_reconciliation`), `tg_signal_ingest/trading/store.py:762–806` (protection `unverified`/`unknown_requires_reconciliation` preserved).
- **Gap:** The critical state is **per-trade** (`needs_reconciliation`), not a **global block**. A second trade's `process_item` does not check whether any peer trade is in `needs_reconciliation` or has an `unknown_requires_reconciliation` exit. The `needs_reconciliation` state is durable (survives restart via `recover_on_startup`), but its durability does not propagate to a trading halt.
- **Tests:** `tests/test_trading_lifecycle_recovery.py` verifies `needs_reconciliation` for residual below minimum; no test verifies a global block when any trade is `needs_reconciliation`.
- **Remediation:** When any trade enters `needs_reconciliation` or any exit is `unknown_requires_reconciliation`, set a persisted `global_buy_blocked = true` with reason `unprotected_exposure`; gate all future BUYs on this flag (see 5.2).

#### 5.2 Block all new BUYs while any position unprotected/unresolved — NON-COMPLIANT / P0

- **Expected:** A query like `has_unprotected_position() → SELECT COUNT(*) FROM trades WHERE trade_state NOT IN ("closed","tp2_completed","needs_reconciliation") AND (protection != active OR has_unknown_exit)` must be true before any BUY, in the same transaction as intent claim.
- **Actual:** Zero implementations. `grep -rn "unprotected\|global.*block\|has_unprotected"` across `tg_signal_ingest/` finds only per-trade comments. `TradingStore` exposes `count_open_intents()` at `store.py:330–342` and `pending_reconciliation()` at `store.py:682–700`, but no caller gates `process_item` on them. `TradingEngine.process_item` at `engine.py:103–310` proceeds through risk/sizing/balance checks without consulting `trades` state.
- **Failure scenario:** Trade A for BTCUSDT fills, OCO TP1 `submitting` fails with `MexcNetworkError` → `unknown_requires_reconciliation` → trade A stays `oco_submitting`. Minutes later a new Telegram signal for ETHUSDT arrives; `process_item` creates intent `intent-tsi…`, reserves budget, and submits a second LIVE BUY while BTCUSDT's exit is unconfirmed — exactly the prohibited behavior.
- **Tests:** None assert blocking; the 2026-10-29 audit's focused PR noted the F3 "uncertain as unconfirmed" fix improved *status certainty* but explicitly left the global block unimplemented.
- **Remediation:** Add `trading_block_state` table (`blocked, reason, blocked_at, cleared_at`) or `app_state["trading.global_buy_blocked"]`; check it atomically in `process_item` before any `claim_intent`; expose `trading status --blocked-reason`.

#### 5.3 Global block survives restarts — NON-COMPLIANT / P0

- **Expected:** Block state persisted in SQLite `app_state` or dedicated table, re-read on `build_trading_engine`.
- **Actual:** No block state to persist; `activation_boundary` at `store.py:1193–1241` survives restarts but is a **replay** gate, not an **exposure** gate.
- **Remediation:** Persist as above; restore on `build_trading_engine` and expose in `tg-ingest trading status`.

#### 5.4 Block cleared only after every position closed or protected — NON-COMPLIANT / P0

- **Expected:** Clearing predicate: `NOT EXISTS (SELECT 1 FROM trades WHERE trade_state NOT IN ("closed","tp2_completed") OR exit_unconfirmed)`.
- **Actual:** No predicate; individual trade `closed` at `trade_manager.py:1550–1565` (`_finalize_trade`) is independent.
- **Remediation:** Implement predicate as a trigger or checked on every `reconcile_all_open` and `reconcile_intent` after fill/exit confirmation; clearing must be logged in `audit_events` with the set of trades that were checked.

#### 5.5 One protected trade never clears block while another unprotected — NON-COMPLIANT / P0

- **Expected:** Block clearing must consider *all* trades, not just the one that just transitioned.
- **Actual:** Since no block exists, closing trade A trivially leaves the system able to trade even if trade B is `needs_reconciliation` — partial progress is mistaken for global safety.
- **Remediation:** Same predicate as 5.4; unit test with two trades (A protected, B unprotected) asserting BUY for a third symbol is still rejected.

#### 5.6 Failed/incomplete reconciliation never proof of no exposure — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/engine.py:1605–1638` (`-2011` → `unknown_requires_reconciliation`, never terminalizes non-terminal intent), `tg_signal_ingest/trading/trade_manager.py:1351–1400` (SL held when `reconcile_entry_fills` returns false).
- **Remediation:** None.

#### 5.7 Check-and-submit safe against concurrent tasks/processes — PARTIALLY COMPLIANT / P1

- **Implements per-intent:** `tg_signal_ingest/trading/store.py:863–882` (atomic `planned→submitting`), `tg_signal_ingest/db.py:1007–1081` (WAL, `Database.transaction()` with `RLock`, `outer.lock`), `tg_signal_ingest/locking.py:42–90` (cross-process `flock` for `serve`; separate `control` lock).
- **Gap for global gate:** The same machinery could make a global block safe, but without the business-rule predicate the lock only guarantees no duplicate *intent* for the same message, not no duplicate *exposure* across messages.
- **Tests:** `tests/test_locking.py` verifies `flock`; no test asserts atomic `check_unprotected → block → submit` across two threads delivering two different signals.
- **Remediation:** Bundle the global-block check and intent claim in one `BEGIN IMMEDIATE` transaction; add a `test_concurrent_buys_do_not_bypass_global_block` that delivers two signals concurrently while one trade is `needs_reconciliation` and asserts exactly one is rejected with `buy_blocked_unprotected_exposure`.

### 4.6 Section 6 — Recovery and reconciliation

#### 6.1 Exchange state authoritative — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:1822–1846` (`_map_exchange_state` never undoes a local `filled` without exchange evidence; `reconcile_intent` maps `FILLED/PARTIALLY_FILLED/…` deterministically).
- **Remediation:** None.

#### 6.2 Local DB reconciled after restarts/outages — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/service.py:540–569` (`_reconciliation_worker` → `recover_on_startup` → `reconcile_pending` + `reconcile_all_open`), `tg_signal_ingest/trading/trade_manager.py:262–283` (`recover_on_startup` reconciles each non-terminal trade).
- **Remediation:** None.

#### 6.3 Recover missing fills and exchange-side orders — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/engine.py:1786–1810` (`myTrades` up to 1000 rows, idempotent `add_fill`), `tg_signal_ingest/trading/trade_manager.py:1680–1765` (`_reconcile_entry_fills` + `_reconcile_trade_orders`).
- **Gap:** Non-terminal fill sync relies on `myTrades` per order; pagination beyond 1000 fills or missing `myTrades` permission is not mitigated beyond `unknown` flag.
- **Remediation:** Log pagination edge; require `myTrades` permission in preflight.

#### 6.4 Not submit duplicate BUYs during recovery — COMPLIANT / P0

- **Implements:** Idempotency key at `store.py:553–610` plus `claim_intent_for_submission` idempotence; `recover_on_startup` at `trade_manager.py:262–283` only calls `_reconcile_trade` (queries), not `_submit_entry_order`, unless `pending_entry` with a trigger already satisfied — and even then `_submit_entry_order` at `trade_manager.py:497–530` atomically flips `pending_entry→entry_active` and reuses the existing `entry_intent_id`, never creating a second BUY `clientOrderId`.
- **Remediation:** None.

#### 6.5 Unknown outcomes remain unknown until reconciled — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/engine.py:1595–1620` (network/5xx/duplicate → `unknown_requires_reconciliation`, no retry), `tg_signal_ingest/trading/trade_manager.py:921–980` (exit uncertain → `unknown`, never resubmitted with new ID).
- **Remediation:** None.

#### 6.6 Recovery not silently replay historical/rejected signals — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/service.py:306–365` recovery marks rows `source="recovery"`; `tg_signal_ingest/trading/engine.py:126–230` rejects `source in ("backfill","recovery")` before any trading path; `tg_signal_ingest/trading/store.py:1193–1241` boundary prevents replay of prior-day downtime gaps.
- **Tests:** `tests/test_analysis_replay.py`, `tests/test_full_history_archive.py`.
- **Remediation:** None.

#### 6.7 New BUYs remain blocked until recovery proves safe — NON-COMPLIANT / P0

- **Why distinct from 5.2:** 5.2 is steady-state exposure gate; 6.7 is the **recovery window** gate (from restart until `recover_on_startup` + `reconcile_all_open` completes).
- **Actual:** `tg_signal_ingest/service.py:540–569` runs `recover_on_startup` as the first action of `_reconciliation_worker`, but `ProcessingEngine.run` at `tg_signal_ingest/processing.py:55–102` and `TradingEngine.process_item` are not gated on `_recovery_complete`. A queue item delivered live immediately after restart can be processed before or concurrently with `recover_on_startup`, creating a BUY while prior exposure is still being probed.
- **Failure scenario:** Process crash after Entry BUY `submitting` but before `submitted` persisted locally. On restart, recovery sets `boundary` to the pre-stop snapshot, then starts reconciling the in-flight intent (query → `unknown`). Meanwhile the Telegram listener has already stored a fresh signal as `source=live` with `message_id > boundary` and enqueued it; the queue consumer processes it and submits a second BUY before the first is proven filled or canceled.
- **Remediation:** Add a persisted `recovery_safe` flag cleared on startup and set only after `recover_on_startup` + one full reconciliation sweep succeed; `process_item` must fail closed with `rejected_recovery_pending` while the flag is false.

#### 6.8 OCO sibling cancellation interpreted per actual exchange behavior — UNVERIFIED / P1

- **Why UNVERIFIED:** `docs/TRADING_DECISIONS.md:5` and `docs/trading.md:Remaining limitations` state no live OCO has been placed; `tg_signal_ingest/mexc/hybrid.py:place_oco` builds the documented CLI body but the exchange's triggered-OCO listing behavior (does the sibling appear as `CANCELED`, `TRIGGERED`, or disappear from `current-orders`?) has not been validated here. Current code at `tg_signal_ingest/trading/trade_manager.py:1150–1188` models each OCO as one row, not parent + two children, so sibling-cancel cannot be proven.
- **Tests:** Faked OCO tests use one fillable order per allocation; they cannot prove sibling-cancel handling.
- **Remediation:** Obtain a redacted read-only trace of a real OCO (parent + TP child + SL child) after TP and SL trigger scenarios, fixture it deterministically, and persist independent child identities/outcomes before declaring COMPLIANT.

#### 6.9 DB not marked complete/terminal while exposure unresolved — COMPLIANT / P0

- **Implements:** `tg_signal_ingest/trading/trade_manager.py:1550–1565` (`_finalize_trade` checks `remaining > 0` and refuses to mark `closed`; goes to `needs_reconciliation`). `tg_signal_ingest/trading/engine.py:1672–1680` (`fill_sync_required = 1` kept in reconciliation set).
- **Remediation:** None.

### 4.7 Section 7 — Auditability and financial correctness

#### 7.1 Every decision auditable — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/store.py:41–90` (`audit_events` correlation=`signal_id`/`intent_id`/`trade_id`), `tg_signal_ingest/trading/store.py:863–910` (`order_status_history` + `reconciliation_results` + `order_fills`), `tg_signal_ingest/trading/notify.py` (`trading_notifications` durable outbox).
- **Remediation:** None.

#### 7.2 Original content and extracted fields traceable — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/repository.py:94–130` (`messages.text/caption/processing_version/source`), `tg_signal_ingest/trading/store.py:41–90` (`signals: parser_version, signal_kind, symbol, action, entry_price, target_prices, stop_loss, fingerprint_sha256, rejection_reason`).
- **Remediation:** None.

#### 7.3 Actual vs requested distinguished — COMPLIANT / P1

- **Implements:** `order_intents` stores `base_quantity/quote_amount/limit_price` (requested) vs `executed_qty/cumulative_quote/exchange_status`; `trades` stores `requested_quote/entry_price` vs `acquired_base_qty/acquired_quote_spent/avg_entry_price`; `order_fills` stores per-execution `price/qty/quoteQty`.
- **Remediation:** None.

#### 7.4 Fees accounted — PARTIALLY COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/store.py:500–540` (`_fees_for_order` sums `fee/commission`), `tg_signal_ingest/trading/store.py:540–560` (`_settle_budget` includes `fee_quote`).
- **Confirmed defect F5 (carried):** `store.py:500–540` multiplies every non-USDT fee by `fill price` (pair price) without conversion provenance: for a BTCUSDT trade with MX fee 0.1 at synthetic price 100, `fee_quote = 10 USDT` is recorded with no MX/USDT evidence. `trade_manager` `fees_paid` sums raw amounts across currencies.
- **Failure scenario:** A realized trade with 0.01 BTC fee at BTCUSDT=65000 is reported as `650 USDT` fee; a parallel ETHUSDT trade with same 0.01 fee at ETHUSDT=3000 is reported as `30 USDT` fee — same fee asset, incomparable valuation, no audit trail of conversion.
- **Tests:** No test asserts fee valuation correctness for third-token fees.
- **Remediation (description only — do not implement):** Add per-asset fee buckets; USDT fee at face value, base-asset fee × execution price, third-token fee bucketed as unresolved until a provenance-bearing MX/USDT ticker snapshot is available; version the ledger.

#### 7.5 Distinguish realized from unrealized P&L — NON-COMPLIANT / P1

- **Expected:** A model that separates realized (closed fills minus fees) from unrealized (mark-to-market of open positions).
- **Actual:** `tg_signal_ingest/trading/store.py:382–390` (`budget_summary`) explicitly notes "`limit` is only the configured reference value … never enforced"; the ledger tracks cumulative spend, not P&L. No `realized_pnl` or `unrealized_pnl` column or view exists; `docs/INDEPENDENT_AUDIT_2026-10-29.md:D` correctly notes "Budget spend is neither realized P&L nor current exposure."
- **Remediation:** Introduce a P&L model: on each terminal fill/exit, record `realized_pnl = Σ(exit_quote - entry_quote - fees)` per trade; mark-to-market of open positions only for display, never for settlement.

#### 7.6 Failed/partial/unknown/reconciled not conflated — COMPLIANT / P1

- **Implements:** Distinct states: `failed/rejected/canceled/canceled_local/filled` vs `partially_filled` vs `unknown_requires_reconciliation` vs `reconciling` vs `needs_reconciliation` at `store.py:14–20` and `trade_store.py:40–46`; history table preserves transitions.
- **Remediation:** None.

#### 7.7 Notifications not claim success before verified — COMPLIANT / P1

- **Implements:** `tg_signal_ingest/trading/engine.py:737–830` (`_order_status_label` maps `unknown_requires_reconciliation → UNCERTAIN_AWAITING_RECONCILIATION`; `_headline_for_status` never uses `FILLED` for `unknown`), `tg_signal_ingest/trading/trade_manager.py:1351–1475` (stop-loss notifications include `SL_SUBMISSION_UNCERTAIN`).
- **Remediation:** None.

### 4.8 Section 8 — Protected legacy positions

#### 8.1 PENDLEUSDT/MYXUSDT not modified — COMPLIANT / P0 (with residual P1)

- **Implements:** Grep for `PENDLE|MYXUSDT` in `tg_signal_ingest/` returns 0. No write path enumerates legacy symbols. This mission created only documentation; no `place_order`, `cancel_order`, or `set_setting` touched those symbols. `docs/INDEPENDENT_AUDIT_2026-10-29.md:3` confirms legacy positions were not touched in prior manual verification (context, not proof).
- **Residual gap:** No deny-list prevents a future Telegram signal `"BUY PENDLEUSDT ENTRY …"` from being treated as tradable. The rule says their presence must not be used as justification for any exchange write — currently the code would write (BUY) for them like any other symbol if a signal arrives, which is arguably "interference" with legacy exposure management.
- **Tests:** None for legacy protection.
- **Remediation (description only):** Add a persisted `protected_symbols = {"PENDLEUSDT","MYXUSDT"}` filter checked at the top of `process_item` (before risk) that unconditionally rejects with `rejected_legacy_protected`; log to `audit_events` and never create an intent; require owner approval to remove a symbol from the set.

---

## 5. Signal Ingestion → Buy Submission — End-to-End Trace (audit requirement A–B)

**Complete path traced (live mode):**

1. `tg_signal_ingest/service.py:224` `_register_handlers` → `on NewMessage/MessageEdited` → `_store_event_message` → `Repository.upsert_message` (`tg_signal_ingest/repository.py:94–130`) with `source=live`, `processing_version` increment, `message_edits` retention.
2. `Repository._enqueue_locked` → `processing_queue` item `(channel, msg, item_type, targets_version)`.
3. `ProcessingEngine.run` (`tg_signal_ingest/processing.py:55–102`) claims `pending→processing` with requeue of expired claims.
4. `IngestService` handlers at `service.py:480–510` wire `message/edit/delete → TradingEngine.process_item`.
5. `TradingEngine.process_item` (`tg_signal_ingest/trading/engine.py:103–108`) enters `with _process_lock:` → `_process_item` (`engine.py:109–310`): `handle_delete` early-return; parse; `record_signal`; `_is_historical` (boundary at `store.py:1193–1241`) → `update_signal("rejected","rejected_replay")` and silent return; `_is_genuine_signal` (`engine.py:540–548`) → non-signal → developer-only notify; `cancel` → `cancel_signals`; `ambiguous` → `rejected` + notify owner/developer; replay/boundary guard; `has_later_cancel`; idempotency key; `intent_for_message` conflict check; `_reference_price`/`_symbol_meta`; `RiskManager.evaluate` (`risk.py:34–88`); `_quantize_decision` (`engine.py:485–514`); `create_intent` (`store.py:553–610`) reserving budget; `TradeManager.on_intent_planned` (`trade_manager.py:164–214`); conditional-entry `deferred` vs `rejected` vs `direct`; `_execute_intent` (`engine.py:917–990`) → `claim_intent_for_submission` (`store.py:863–882`) → mode dispatch → `_execute_live` (`engine.py:1163–1291`): `_live_client`, `balance_preflight`, `protection_prices`, record `unverified`, `submitting`, `place_order` (or `place_oco` for exits), handle `Mexc*` taxonomy → `unknown` vs `rejected` vs `submitted` + `upsert_exchange_order` + `verify_protection` + `reconcile_intent`; finally notify.

**LONG-only and SELL handling verified:** `SignalParser._action` at `parser.py:175–185` returns `None` on mixed directions (ambiguous) and `buy/sell` otherwise; `RiskManager.evaluate` at `risk.py:57–60` rejects `sell`. Live execution path at `engine.py:1005–1045` only uses `BUY` for entries; sell orders exist only as exits under `TradeManager`. No path calls `submit_sell` from a parsed Telegram `SELL` direction.

**Source-channel validation verified:** Only the `selected_channel_id` at `service.py:206` is listened to; every handler gates on `_is_target` comparing `marked_channel_id`; no ambient channel can inject.

**Duplicate prevention verified:** Idempotency key `sha256(channel:msg:action:symbol)` at `engine.py:405–406` → truncated to `client_order_id = "tsi"+key[:28]`; `store.py:553–610` dedups on `idempotency_key`; `claim_intent_for_submission` at `store.py:863–882` guarantees exactly one `planned→submitting` winner.

**Historical replay protection verified:** `engine.py:126–230` dual-layer gate (first gate before genuine-signal classification, second before intent creation) plus `store.py:1193–1241` forward-only boundary — downtime backlogs, backfill imports, and recovery rows (`source=backfill|recovery`) are stored and audited but never traded.

**Buy submission safety verified per-intent:** `engine.py:74` `RLock` plus `store.py:863–882` atomic update prevent two simultaneous deliveries of the *same* message from both submitting. **Not verified for global gate:** Two *different* signals can still both pass because no global predicate is checked (see 2.6/5.2).

### 5.1 Buy sizing — Fixed 3 USDT, no 50 USDT cap, symbol/budget gates

- **Fixed-quote:** `risk.py:94–112` and `sizing.py:28–62` use only `cfg.trading.order_quote_amount` (default 3 at `config.py:91`). Signal `amount_text` is extracted at `parser.py:56–70` but never sized.
- **No cap:** `store.py:382–440` never compares reservation to `aggregate_budget_quote`; tests at `tests/test_trading_budget.py` assert this.
- **Symbol validation:** `engine.py:472–484` (`_symbol_meta` via `SymbolRegistry` from `client.exchangeInfo`) and `engine.py:485–514` (`quantize_validate`) fail closed with rejection.
- **Account gate:** `engine.py:1325–1365` (`_balance_preflight` → `parse_account` + `validate_balance` + `outstanding_balance_usage` at `store.py:809–845`; `balance_max_age_s=5.0` at `config.py:86`; `balance_fee_buffer_pct=0.2`).
- **Traceability:** `intent_id` includes `clientOrderId`; `order_status_history` at `store.py:863–910` records `planned→submitting→submitted→…`.

### 5.2 Exit lifecycle — TP1/TP2/SL and OCO (audit requirement C–D)

- **Trade creation:** `trade_manager.py:284–340` derives `entry_price` from `ParsedSignal`, tick-aware trigger, `tp1_price`/`tp2_price` from targets, `sl_price`; persists `tp1_allocation_pct/tp2_allocation_pct` at creation and never recomputes silently.
- **OCO semantics:** `trade_manager.py:1016–1130` implements two native OCO SELLS (`tp1` and `tp2`, each carrying its own stop-limit leg at `sl_price`). Quantities are `quantize_down`, capped by `acquired - sold - outstanding`. Terminal accounting at `trade_manager.py:1008–1012` requires both OCO orders terminal before close; residual below minimum → `needs_reconciliation` with `unsold_residual_qty` rather than silent drop.
- **What "fully protected" means in code:** Not a single boolean; it is the conjunction of (a) `order_protection.status == "active"` for the entry when `supports_attached_protection`, or (b) both OCO orders are `open/partially_filled` with confirmed working quantity covering the remaining bot-owned position — formalized after the 2026-10-29 certainty fix at `trade_manager.py:1150–1172` and `docs/trading.md:OCO certainty`. **Pre-fix, uncertain orders were mis-counted as confirmed; post-fix, only `open/partially_filled/filled` with exchange evidence counts.**
- **Do not infer guarantees:** Function names like `verify_protection` and `supports_attached_protection` were not assumed to imply exchange guarantees; the audit confirmed they query the exchange and require echoed trigger prices, and flagged the CLI paging and missing child-identity gaps as `PARTIALLY/UNVERIFIED`.

### 5.3 Stop-loss behavior (audit requirement D)

- **Every stop-loss path traced (current, native OCO-only):** Entry `_check_tp_sl`/`_evaluate_trade_tick` → `_enter_stop_loss` at `trade_manager.py:1408` → `_handle_unprotected_sl` at `trade_manager.py:400-460` (persists `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL` + audit, narrow per-symbol restriction, never market sell) → `trade_manager.py:1433-1450` (`_resume_stop_loss`/`_submit_stop_loss_order` stubs). `_handle_unprotected_sl` is the only SL-execution path; OCO legs are `STOP_LIMIT` via `submit_oco_sell`. **Historical:** `trade_manager.py:1351-1475` → `_submit_stop_loss_order` → `engine.py:1466-1491` (`submit_stop_loss_market_sell`: `side=SELL, type=MARKET`) — now removed, `engine.py:1553-1572` raises `RuntimeError` (verified `grep -rn submit_stop_loss_market_sell` → only `tests/` + `trade_store.py` shim remain).
- **Immediate-execution risk:** A market stop-loss is by definition immediately marketable; an OCO stop-limit with `trigger == limit == SL` is not, but thin-book slippage was not proven bounded — marked `UNVERIFIED`.
- **No fallback:** `trade_manager.py:921–980` holds a failed TP sell in `rejected/unknown` rather than falling back to market.

### 5.4 Error handling and ambiguous outcomes (audit requirement E)

- **Taxonomy:** `tg_signal_ingest/mexc/errors.py:62` (`700003` timestamp) + `tg_signal_ingest/mexc/client.py:355–380` (WAF/rate-limit/5xx classification) + `engine.py:1220–1252` (duplicate/network/server/WAF → `unknown`, definitive → `rejected`).
- **No blind retry:** `unknown_requires_reconciliation` is never retried with a new `clientOrderId`; reconciliation queries via `query_order(clientOrderId)` (fallback when `exchange_order_id` missing at `engine.py:1493–1521`).
- **Duplicate submission safety:** `MexcDuplicateOrder` at `engine.py:1220` and `trade_manager.py:930–945` surfaces as `unknown` and reconciles by the original ID.

### 5.5 Recovery (audit requirement F)

- **Startup:** `service.py:540–569` `_reconciliation_worker` → `trade_manager.recover_on_startup` (`trade_manager.py:262–283`) reconciles each non-terminal trade (entry intent + TP/SL orders) before ticks act; `reconcile_pending` (`engine.py:1846–1870`) and `reconcile_all_open` (`trade_manager.py:1676–1720`) are periodic.
- **Crash window:** `store.py:863–910` orders intent row creation *before* exchange call and sets `fill_sync_required` in the same transaction that terminalizes the order, so a crash between `submit` and `upsert_exchange_order` leaves the intent in `submitting` or `unknown`, not `filled` — recovery can resume via `planned` resumption (`engine.py:354–389`) but cannot blindly re-submit an `unknown`.
- **Gate missing:** Recovery does not set a global BUY block (G6.7/5.2 gap).

### 5.6 Audit and financial accounting (audit requirement G)

- **Reconstruction:** `audit_events.correlation` + `order_status_history` + `signals.fingerprint_sha256` + `order_fills` + `reconciliation_results` + `trading_notifications` provide end-to-end trail. Gaps: fee valuation (F5), realized/unrealized P&L (G7.5).
- **Discrepancy model:** `store.py:1822–1846` (`_map_exchange_state`) detects `filled` vs `canceled` divergence and records `discrepancy`.

### 5.7 Protected legacy positions (audit requirement H)

- **Confirmed no automatic path:** Grep for legacy symbols in `tg_signal_ingest/` is empty; `main.py:619–660` `serve` path has no symbol-specific write; management/control bots (`tg_signal_ingest/management/bot.py`, `tg_signal_ingest/control/service.py`) never enumerate or act on PENDLEUSDT/MYXUSDT. No database or session file was touched by this mission.

---

## 6. Contradictions Between Existing Documentation and Actual Code

| Documentation claim | Actual code | Impact |
|---|---|---|
| `docs/trading.md` and `docs/TRADING_DECISIONS.md` describe the system as "live-ready" with native OCO exits as shipped. | No live OCO has been placed; OCO contract unverified; CLI resolution page-limited; per-8.1 deny-list missing — flagged as `PARTIALLY/UNVERIFIED`. | Operator may deploy to live expecting fully validated exits. Must retain "unverified" labels until live evidence exists. |
| `_reconciliation_worker` comment at `service.py:556` says uncertain orders are "queried only". | `trade_manager.recover_on_startup` at `trade_manager.py:262–283` and `reconcile_all_open` at `trade_manager.py:1676` can place pending protection and retry cancels — action-capable, not read-only. | Diagnostic use may trigger writes unexpectedly (F6). |
| `trading/config.py:82` `require_stop_loss = True` default implies mandatory SL. | `tg_signal_ingest/trading/risk.py:34–88` never checks `require_stop_loss`; `tests/test_trading_engine.py:test_alert_without_stop_loss_is_accepted` asserts a signal without SL is accepted. | Requirement 3.0a is violated silently; test enshrines non-compliance. |
| `tg_signal_ingest/trading/risk.py:1–6` header says "no application-level … mandatory stop-loss rules … or symbol allowlists." | Non-negotiable rules 3.0a/3.0b, 5.2, 8.1 *do* mandate mandatory SL/TP count, price invariant, global block, and legacy-protection allowlist. | Header is accurate for the shipped code but contradicts the new non-negotiable invariant; header must be revised when 3.0a/3.0b are implemented. |
| `docs/data-model.md` implies budget ledger is spend-capped. | `tg_signal_ingest/trading/store.py:382–440` makes it reference-only, per `docs/trading.md` and `tests/test_trading_budget.py`. | No defect, but legacy readers may assume a 50 USDT hard cap; docs should continue stating "not an order cap." |

All contradictions were documented, not fixed, per the documentation-only boundary.

---

## 7. Most Important Remediation Items, in Priority Order

**Description only — no implementation was performed. Order reflects P0 business risk first, then P1 correctness, then P2.**

### P0-1 Implement the global BUY block as a persisted, restart-durable invariant (rules 5.1–5.5, 2.6, 2.8, 6.7)

*What to build:* `app_state["trading.global_buy_blocked"]` (or `trading_block_state` table) with `reason`, `blocked_at`, `blocked_by_trades`. Check atomically in the same `Database.transaction()` as `claim_intent_for_submission` before any live BUY. `recover_on_startup` clears `recovery_safe=false` → true only after all non-terminal trades and intents have been queried and `needs_reconciliation` trades are accounted for. Clearing predicate: `NOT EXISTS` unprotected/unresolved bot-owned position; log cleared set in `audit_events`. `TradingEngine.process_item` must reject with `buy_blocked_unprotected_exposure` while blocked. Coverage: unit test with two concurrent deliveries (one fills, one pending) asserts second is rejected; restart test asserts block survives `app_state` restoration; multi-process test via `flock` asserts atomicity.

### P0-2 Enforce mandatory SL + TP1 + TP2 presence and SL < Entry < TP1 ≤ TP2 before any intent (rules 3.0a, 3.0b, 2.4)

*What to build:* `RiskManager.evaluate` fails closed when any leg is missing or inequality violated, with reason naming the missing/violated leg. Remove the permissive header claiming "no mandatory SL." Update `SignalParser` to surface individual TP count, not coalesce. Test correction: `test_alert_without_stop_loss_is_accepted` must be replaced by `test_alert_without_stop_loss_is_rejected_with_mandatory_exit_reason`.

### P0-3 Introduce a distinct stop-loss market-sell authorization (rule 4.2)

*Historical note (superseded by removal):* `TradingConfig.allow_stop_loss_market_sell: bool = False` was considered but **owner confirmed removal** of the market-sell path instead (2026-01-03). **Current:** `engine.submit_stop_loss_market_sell` at `engine.py:1553-1572` raises `RuntimeError` (disabled); SL is via native OCO `STOP_LIMIT` only. Missing/rejected/uncertain protection → `needs_reconciliation` + `SL_UNPROTECTED_CRITICAL` + read-only reconciliation, never market sell or autonomous replacement.

### P0-4 Add explicit legacy-protection deny-list for PENDLEUSDT/MYXUSDT (rule 8.1)

*What to build:* Persisted `protected_legacy_symbols` set checked at `process_item` entry (before parsing, after historical gate); unconditional rejection with `rejected_legacy_protected` and `audit_events` entry. Owner removal requires explicit approval and leaves an audit trail.

### P1-1 Fix fee valuation and P&L model (rules 7.4, 7.5)

*What to build:* Separate `fee_quote` buckets per asset; third-token fees recorded as quantity + asset, not converted by pair price. Derive `realized_pnl` per closed trade as `Σ(exit_fills.quote - entry_fills.quote - fee_quote)` from durable `order_fills`; `unrealized_pnl` only for display from mark-to-market; never settle from mark-to-market.

### P1-2 Resolve OCO sibling-cancel contract and child identities (rules 3.1, 3.6, 5.1, 6.8)

*What to build:* After a redacted live OCO trace, persist `oco_parent_id` + `oco_tp_child_id` + `oco_sl_child_id` per allocation; reconcile each child independently; classify `oco_active` only when at least one child is `open/partially_filled` with exchange evidence; define completion as fill evidence per child, not group aggregate.

### P1-3 Harden rejected→retried promotion (rule 1.8)

*What to build:* `create_intent` must refuse to create an intent when `signals.status == "rejected"` for the same `signal_id` unless a newer `edit_version` supersedes it; this closes the same-version retry that can currently promote a locally-rejected message to `planned`.

### P1-4 Split recovery diagnostics into dry-run vs act (rules 4.7, 6.2)

*What to build:* `tg-ingest trading reconcile --dry-run` (queries only, zero writes) vs `--act` (action-capable); label every action-capable path; add tests asserting zero `place/cancel` calls in dry-run.

### P1-5 Version and gate SL/trigger semantics (rules 4.4, 4.5)

*What to build:* Version the execution-behavior tuple `(order_type_policy, entry_trigger_offset_ticks, tp_allocation, sl_market_allowed)`; refuse to start live with an unapproved combination; add a book-snapshot simulation (no live order) proving no immediate marketability of OCO stop-limit legs.

### P2-1 Document allocation authority explicitly (rule 3.6)

*What to build:* Promote TP1/TP2 allocation from code default to an owner-approved setting in `app_state` or `control_settings`, mirroring `order_quote_amount`; require approval to change.

---

## 8. Final Summary Checklist (Phase 4)

### 8.1 Exact paths of the two official rule documents

- `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md`
- `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md`

### 8.2 Exact path of the compliance audit

- `docs/TRADING_RULES_COMPLIANCE_AUDIT.md` (this file)

### 8.3 Number of requirements audited

**61** numbered requirements (1.1–1.10, 2.1–2.8, 3.0a–3.0b + 3.1–3.10, 4.1–4.7, 5.1–5.7, 6.1–6.9, 7.1–7.7, 8.1). Counting method: each clause in the official documents with an independent enforceable obligation; price-relation and the "every LONG has SL+TP1+TP2" header are counted separately because they are independently testable.

### 8.4 Counts by status

- **COMPLIANT:** 36 — directly enforced at runtime and supported by at least one relevant test or by deterministic static enforcement.
- **PARTIALLY COMPLIANT:** 14 — some paths enforced but coverage incomplete (e.g., per-trade durability without global gate; exit verification without live OCO proof).
- **NON-COMPLIANT:** 10 — violates or lacks mandatory enforcement (notably global BUY block 5.2–5.5 + 6.7, mandatory-exit presence 3.0a–3.0b, realized/unrealized P&L 7.5, and ambiguous-before-next-submission 2.6/2.8).
- **UNVERIFIED:** 2 — available code and evidence insufficient (4.5 immediate-execution/fee impact, 6.8 OCO sibling-cancel contract).

Counts exclude `NOT APPLICABLE` (0 — every requirement applies to the audited trading component).

### 8.5 All P0 and P1 findings (condensed)

**P0 (10):** 1.1–1.4, 2.1, 2.3, 2.5, 2.7, 3.2, 3.3, 3.7–3.9, 4.1, 4.3 (COMPLIANT, listed for completeness) plus **NON-COMPLIANT/PARTIAL P0:** 2.6, 2.8, 3.0a, 3.0b, 3.1, 3.5, 4.2, 5.1, 5.2, 5.3, 5.4, 5.5, 6.7, 8.1. Detail in §4.

**P1 (14):** 1.8, 2.4, 3.4, 3.5, 3.10, 4.4, 4.7, 5.6, 5.7, 6.2, 6.3, 6.8, 7.1–7.7 (with 7.4, 7.5 flagged). Full text in §4 with file/line/test references.

### 8.6 Contradictions between existing documentation and actual code

See §6. The most significant: (a) "live-ready OCO" docs vs. unverified OCO contract, (b) `require_stop_loss=true` default vs. acceptance of no-SL signals, (c) reconciliation "query only" comment vs. action-capable recovery, (d) risk header "no mandatory SL" vs. new mandatory-exit invariant.

### 8.7 Most important remediation items, in priority order

See §7: P0-1 global BUY block (5.2/6.7), P0-2 mandatory SL/TP1/TP2 + price invariant (3.0a/b), P0-3 distinct market-SL authorization (4.2), P0-4 legacy deny-list (8.1), P1-1 fee/P&L (7.4/7.5), P1-2 OCO child identities (F4/3.1/6.8), P1-3 rejected→retried promotion (1.8), P1-4 dry-run vs act (4.7), P1-5 SL/trigger versioning (4.4/4.5).

### 8.8 Repository branch, HEAD commit, and working-tree status

- **Branch:** `arena/e452655b-tele-mexc` (active session branch; branched from `71a952b` of `main`)
- **HEAD commit at audit completion:** `71a952b6b35df676fa9e7dc74c97f41baace9ecd` (merge PR #3)
- **Working-tree status at audit completion:** **modified** — three documentation files created by this mission (the only permitted changes) are present as untracked/modified working-tree files:
  - `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md` (new)
  - `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md` (new)
  - `docs/TRADING_RULES_COMPLIANCE_AUDIT.md` (new)
- **Application source:** unmodified (read-only audit). No `.env`, `config/config.toml`, database, or session file was read or printed.

### 8.9 Production-safety disclaimer

Unit-test passage, mock-based OCO lifecycle tests, and offline parser replay do **not** establish live-trading safety. Treat every `UNVERIFIED` and `PARTIALLY COMPLIANT` finding as a release blocker for live funds until live, redacted exchange evidence and crash-injection tests exist. The most urgent blocker is the missing global BUY block — deploying to `live` without it allows unbounded exposure during any period of uncertain protection.

---

## 9. Auditor Declaration

This audit inspected actual file paths, function names, and line references as cited, distinguished confirmed enforcement from documented intent, and reported `UNVERIFIED` where evidence was unavailable rather than inferring compliance from function names or local abstractions. No code was modified to make a finding appear more favorable. Recommended remediations are descriptive only and were not implemented, per the mission's absolute execution boundaries.

---

## Appendices

### A. Key file index for verification

- `tg_signal_ingest/trading/engine.py` — queue → risk → sizing → balance → intent → exchange → reconcile
- `tg_signal_ingest/trading/trade_manager.py` — trade lifecycle (conditional entry, OCO exits, stop-loss, reconciliation)
- `tg_signal_ingest/trading/risk.py` — BUY-only gate, symbol/fixed-amount validation
- `tg_signal_ingest/trading/sizing.py` — fixed-quote sizing (ignores signal stake)
- `tg_signal_ingest/trading/balance.py` — `canTrade`/stale/fee-buffer preflight
- `tg_signal_ingest/trading/store.py` — durable intents/budget/protection/audit + activation boundary
- `tg_signal_ingest/trading/trade_store.py` — trades/trade_orders + OCO certainty
- `tg_signal_ingest/trading/parser.py` — LONG/BUY extraction, Futures/short discrimination
- `tg_signal_ingest/mexc/client.py` — REST Spot v3, `WeightedRateGate`, `SharedWeightedRateGate`
- `tg_signal_ingest/mexc/cli.py` — `MexcCliAdapter` (single-page OCO resolution)
- `tg_signal_ingest/mexc/hybrid.py` — `HybridMexcClient` (reads REST, writes CLI)
- `tg_signal_ingest/config.py` — precedence `env > DB > TOML > default`, `order_quote_amount=3.0`
- `tg_signal_ingest/service.py` — live listener, recovery sweep, `_reconciliation_worker`
- `tg_signal_ingest/db.py` — WAL, `Database.transaction()` `RLock`, migrations
- `tg_signal_ingest/locking.py` — `SingleInstanceLock` (`flock`) for `serve` + `control`

### B. No-interference attestation (PENDLEUSDT / MYXUSDT)

This mission's Git range (`git diff HEAD -- tg_signal_ingest/`, `git diff HEAD -- tests/`, `git diff HEAD -- config/`, `git diff HEAD -- *.py`) is empty — the only modified paths are the three `docs/` files listed in §8.8. No existing position or order for PENDLEUSDT or MYXUSDT was read via exchange API, and no exchange write was executed.

### C. Cross-reference to prior independent audit (2026-10-29)

Findings F1–F7 and quantitative-evidence cautions from `docs/INDEPENDENT_AUDIT_2026-10-29.md` were verified: F3 (certainty) was repaired in the current tree; F1 (notification dedup), F2, F4 (OCO contract), F5 (fee valuation), and F6 (diagnostic vs action) remain relevant and are carried into this audit's sub-findings with updated line references, not silently closed.

