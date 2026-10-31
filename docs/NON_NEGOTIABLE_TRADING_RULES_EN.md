# Non-Negotiable Trading Rules — Official Reference

**Status:** Active and binding  
**Version:** 2.0 — Owner-confirmed MEXC Spot strategy (2026-10-31)  
**Date:** 2026-10-31 (updated 2026-11-01 for owner-confirmed strategy)  
**Authority:** Project Owner — explicit confirmation that MEXC API supports two independent OCO groups per signal  
**Branch:** `arena/e452655b-tele-mexc`  
**Commit:** `71a952b6b35df676fa9e7dc74c97f41baace9ecd` + remediations of 2026-10-31 audit  
**Languages:** English (this document) and Persian (`docs/NON_NEGOTIABLE_TRADING_RULES_FA.md`) — both complete and equivalent. Neither language omits, weakens, reinterprets, or introduces additional permissions.

---

## Preamble — Nature and Precedence

These rules are **mandatory system invariants**.

1. They take precedence over convenience, optimization, signal-channel instructions, and implementation shortcuts.
2. No code change may weaken or bypass them without explicit owner authorization.
3. Passing automated tests does not establish compliance unless tests actually verify the relevant behavior.
4. Unknown or unverified behavior must never be presented as compliant — use `UNVERIFIED`.
5. These documents are the authoritative reference for all current and future trading-system behavior.

---

## 1. Signal Eligibility — Complete LONG Only

**1.1.** Only complete and valid LONG signals are eligible for execution.

**1.2.** Every eligible signal **must** include:
- A supported trading symbol.
- LONG direction.
- A valid entry price or supported entry range.
- Stop Loss (SL).
- Take Profit 1 (TP1).
- Take Profit 2 (TP2).

**1.3.** For LONG signals the required invariant is:

```
SL < Entry < TP1 <= TP2
```

Entry may be a single price or a validated range (e.g., `low–high`); the range midpoint is the execution reference. An entry range with `low > high` is invalid.

**1.4.** Missing or invalid mandatory information **means rejection before BUY submission**. Never infer missing SL, TP1, or TP2 values.

**1.5.** Never execute SHORT signals.

**1.6.** Telegram-originated SELL instructions must **never independently trigger** exchange sells.

**1.7.** No leverage or margin trading is permitted; Futures/perpetual/leveraged instructions are rejected.

**1.8.** Signals must originate from the **configured and authorized Telegram channel** only.

**1.9.** Duplicate signals must not create duplicate trades (per-signal idempotency, durable).

**1.10.** Every accepted signal must retain an **auditable link** to its original Telegram message (channel, message_id, edit_version, fingerprint, raw text).

---

## 2. One BUY per Signal — Independent, Configurable, Idempotent

**2.1.** Each accepted signal represents an **independent trade**. The bot must submit **one BUY** per eligible signal, subject to idempotency, per-symbol concurrency, balance, and risk controls.

**2.2.** The authorized **fixed quote amount per trade** remains configurable through the existing authorized configuration (`trading.order_quote_amount`, default 3 USDT, precedence env > DB > TOML > default). Do not introduce channel-provided leverage, percentage sizing, or a new aggregate cap. There is **no aggregate 50 USDT trading cap**.

**2.3.** Every BUY submission must be **traceable** to one unique signal and one controlled execution attempt (stable idempotency key, `clientOrderId`, persisted intent, audit event).

**2.4.** No BUY unless **all mandatory signal validations pass** at the execution boundary (risk gate), including symbol support, LONG price relationship, symbol metadata, balance preflight, and per-symbol concurrency.

**2.5.** Ambiguous exchange outcomes must be **reconciled before retry**; no blind retries of order-creating requests.

**2.6.** Concurrent BUY operations must respect the documented concurrency controls (per-signal idempotency, per-symbol, balance reservations — see §3).

---

## 3. Independent Execution Controls — No Unconditional Global Lock

An unresolved BTC signal **must not automatically prevent** an otherwise valid ETH signal. The owner explicitly requires independent signals to be processed independently.

Implement **three distinct controls**:

**3.1. Per-signal idempotency**
- Every Telegram signal has a stable identity `sha256(channel_id:message_id:action:symbol)` → `clientOrderId`.
- A signal must never submit duplicate BUYs due to retries, repeated updates, restarts, or ambiguous API responses.
- Ambiguous submission (`unknown_requires_reconciliation`) must be reconciled before retry.
- Preserve every submission attempt and its result in `order_status_history` and `audit_events`.

**3.2. Per-symbol concurrency**
- Prevent unsafe overlapping trades on the **same symbol**.
- Define conflicting trade states: any existing non-terminal bot-owned trade for the same symbol (e.g., `pending_entry`, `entry_active`, `position_open`, `oco_submitting`, `oco_active`, `oco_partial`, `tp1_active`, `exit_pending`) blocks a new BUY for that symbol. Terminal states `closed`/`tp2_completed` do not conflict; `needs_reconciliation` is treated as conflicting because protection is unverified.
- Do not block unrelated symbols.
- Decisions are durable (DB `trades` table) and safe across multiple workers and restarts (`Database.transaction()` + `threading.RLock` + `SingleInstanceLock`).

**3.3. Balance reservations**
- Concurrent signals must not spend the same available quote balance.
- Reservations are atomic and durable (`trading_budget_ledger` + `outstanding_balance_usage` inside the intent-creation transaction).
- Reconcile reservations with actual exchange balances and fills (`myTrades` + `fill_sync_required` flag in same transaction as terminalization).
- Release reservations only when the corresponding outcome is known (terminal state or reconciled fill).

**3.4. Global safety halt (narrow, documented)**
- A global halt is permitted **only** for a genuine account-wide unsafe condition: unreliable account-wide balance state (e.g., `canTrade != true`, missing `balances`, stale `account` snapshot, or confirmed inability to synchronize account state).
- Document **every halt trigger and recovery condition** in code and in `docs/trading.md` / audit reports. No unconditional account-wide BUY lock for per-signal or per-symbol issues.

---

## 4. Mandatory Validation at the Execution Boundary

Enforce signal completeness in the **actual risk/execution boundary**, not just in the parser.

**4.1.** No BUY submission is permitted unless **all mandatory fields and LONG price relationships are valid**:
- Missing symbol, unsupported symbol, missing direction, SHORT direction, missing entry, missing SL, missing TP1, missing TP2, invalid price relationship (`SL < Entry < TP1 <= TP2`), malformed or unsupported entry range.

**4.2.** Each missing field must independently prevent BUY submission with an **exact persisted rejection reason** visible in owner and developer reports (signal status `rejected`, `audit_events` category `risk`).

**4.3.** No generic retry or parser reprocessing may bypass this validation. A permanently rejected signal must not become eligible through a generic retry; only a newer `edit_version` with corrected content may be re-evaluated.

---

## 5. Two Independent OCO Groups — With Critical Combined-Quantity Invariant

For every executed signal:

- Create one BUY.
- Determine the **actual filled and sellable quantity** (actual BUY fills — see 5.x).
- Divide that quantity according to the existing **authorized TP allocation strategy** (`tp1_allocation_pct_of_acquired = 50%`, `tp2_allocation_pct_of_remaining = 100%` of remainder — documented in `docs/trading.md` §4 and `config.py`).
- Create **two independent OCO groups**:
  - **OCO 1:** TP1 + associated SL.
  - **OCO 2:** TP2 + associated SL.

The owner has explicitly confirmed the current MEXC API supports this arrangement; implementation must use the actual supported API (`spot.trade.place-oco` via CLI, verified in `@mexc-ai/cli` v0.0.22) and verify quantity, reservation, and lifecycle semantics.

**5.1. Critical combined-quantity invariant**

```
OCO1_reserved_quantity + OCO2_reserved_quantity ≤ bot_owned_sellable_quantity
```

**5.2. Sellable quantity** for a signal must account for:
- Actual BUY fills (one or multiple `order_fills` rows).
- Multiple fill events (each `order_fills` row with execution price/quote).
- Base-asset commissions (fee asset == base asset reduces sellable; quote-asset fees do not reduce base sellable; third-token fees are preserved as unvalued).
- Quantity precision and step-size rounding (`quantize down`).
- Previously executed TP or SL quantities.
- Existing active reservations belonging to the same bot-owned signal.
- Confirmed cancellations and remaining open orders.

Never count the same inventory twice. Never use requested BUY quantity as a substitute for actual acquired sellable quantity. Do not count unrelated manually held assets.

**5.3.** Recalculate the invariant after **every relevant fill, partial fill, order submission, cancellation, execution, reconciliation, and restart** using decimal-safe arithmetic.

**5.4.** If the invariant cannot be proven, **do not submit or increase** the affected OCO quantities. Persist the condition (trade state `needs_reconciliation`, `last_error` + `audit_events`), reconcile the exchange, and notify the owner immediately.

**5.5.** For each signal maintain complete identities and state for:
- Telegram signal, BUY order, every BUY fill, OCO group 1, OCO group 1 TP child, OCO group 1 SL child, OCO group 2, OCO group 2 TP child, OCO group 2 SL child, every execution and cancellation event.

Handle explicitly:
- Partial BUY fills, multiple BUY fills, base-asset fees, partial TP fills, full TP1/TP2 execution, SL execution, delayed exchange events, rejected OCO submissions, ambiguous OCO responses, delayed cancellation confirmation, process restarts, exchange reconciliation.
- After TP1 executes, recompute remaining bot-owned quantity and required coverage of the other group; never leave stale OCO quantities exceeding remaining sellable inventory.
- Do not assume a successful API response proves the OCO is still active — verify actual exchange state.

---

## 6. PENDLEUSDT and MYXUSDT — Managed, Not Denied

**6.1.** Remove any hardcoded deny-list that rejects these symbols **solely because of their names**. They must be eligible for normal signal processing under the same validation and risk rules as other supported symbols.

**6.2.** Manage **only orders and positions created and recorded by this bot** (persisted `exchange_order_id`, `clientOrderId` where supported, `order_fills` execution records).

**6.3.** Never adopt or modify unrelated manually created orders, never cancel unrelated user orders, never close unrelated user positions.

**6.4.** No live exchange orders or positions may be accessed or modified during this documentation/testing mission.

---

## 7. Market Sell Restrictions — Two-OCO Is the Authorized Strategy

The authorized exit strategy is the two-OCO structure.

**7.1.** Do **not** automatically submit a Market Sell when:
- OCO submission fails.
- OCO status is unknown.
- A stop-loss order is rejected.
- Cancellation is delayed.
- Reconciliation fails.
- A network timeout occurs.

**7.2.** Instead: (1) persist the exact failure, (2) identify the affected signal, (3) reconcile actual bot-owned inventory and orders, (4) mark unverified protection as critical risk, (5) notify the owner immediately, (6) apply the narrowest appropriate restriction (per-signal/per-symbol, not global unless §3.4 applies), (7) do not invent a replacement exit order.

**7.3.** A generic LIVE token must **not authorize discretionary Market Sell** behavior. Stop-loss market execution, if ever introduced, requires a distinct explicit flag.

---

## 8. Rejected Signals, Retries, and State Transitions

**8.1.** Valid state transitions are explicit: `parsed → validated → planned → submitting → submitted/open → filled/canceled` etc., with `unknown_requires_reconciliation` for ambiguous submissions.

**8.2.** A permanently rejected signal must **not become eligible** through a generic retry. Only a newer `edit_version` may be re-evaluated; same `(channel, message, edit_version)` rejected stays rejected.

**8.3.** A transient infrastructure failure may be retried **only** when the previous operation is proven not to have executed (no exchange order evidence).

**8.4.** An ambiguous order submission must be **reconciled before another order can be submitted** for that signal.

**8.5.** Reprocessing a Telegram message after parser changes must **not create a second BUY** for an already executed signal (idempotency key stability).

**8.6.** Preserve all previous decisions and execution attempts in `signals` + `audit_events` + `order_status_history`.

---

## 9. Fees and P&L Accounting — Fill-Level, Asset-Aware

**9.1.** Persist fill-level accounting for every execution:
- Executed quantity, executed quote value, execution price, commission amount, commission asset, exchange fill/trade ID, execution timestamp.

**9.2.** Do not assume all commissions are paid in USDT. Preserve original amount and asset; if commission valuation is unavailable, mark P&L as incomplete (`unvalued_fees`) rather than inventing a conversion price.

**9.3.** Distinguish: realized P&L, unrealized P&L, trading fees, unvalued fees, remaining bot-owned inventory, reserved quantity, unprotected quantity.

**9.4.** Use decimal-safe arithmetic and actual exchange precision rules (`quantize down`).

---

## 10. Recovery and Reconciliation — Read-Only vs Action-Capable

**10.1.** Separate read-only inspection from action-capable order management. A worker described as query-only must never submit or cancel orders.

**10.2.** Startup recovery must:
- Restore persisted signal and order identities.
- Query actual bot-owned exchange orders.
- Reconcile fills and cancellations.
- Recalculate sellable inventory (including fees/precision).
- Recalculate the combined two-OCO quantity invariant.
- Verify actual protection.
- Detect missing or uncertain protection.
- Avoid duplicate submissions.
- Not replay old Telegram messages as new trades.
- Not submit new BUYs or discretionary SELLs during startup recovery.

**10.3.** Exchange state is authoritative for orders, fills, balances.

---

## 11. Comprehensive Reporting and Auditability

The owner and developer reporting system is a **core deliverable** (see `reports/trading_compliance/`). Every signal, trade, order, fill, cancellation, and reconciliation is durably auditable (event-sourced, immutable history, atomic state+audit transactions).

**Owner** sees operational truth per signal/trade (channel, message, timestamps, symbol, direction, entry/SL/TP1/TP2, parsing/validation result, BUY submission/fill details, sellable vs reserved vs unprotected, TP/SL execution, protection status, P&L, fees, final status — with clear categories: rejected, not attempted, unknown, partially filled, protected, partially protected, unprotected, etc.). Critical alerts for unknown submissions, missing protection, invariant violations, failed cancellations, mismatches, and any unresolved live-trade condition.

**Developer** sees raw evidence per event (unique event ID, signal/order relationships, local+exchange timestamps, source/type, state before/after, validation, config snapshot, correlation/client/exchange IDs, OCO/child IDs, HTTP operation, sanitized request/response, error codes, retry counts, reconciliation counts, reservation/sellable/invariant results, commission asset, duration, sanitized traceback). No secrets logged.

All claims cite exact file paths, line numbers, test names, and actual test results; mocked exchange behavior is distinguished from verified exchange behavior; implementation capability is distinguished from production deployment; unproven items are marked `UNVERIFIED`.

---

## 12. Protected Legacy Positions — Historical Context

Previous audits reported PENDLEUSDT/MYXUSDT as protected legacy positions that this mission must not interfere with. That protection was implemented as a documentation-level guarantee (no exchange writes during this mission) and remains honored. Per owner direction, no hardcoded name-based deny-list may block these symbols from normal validation in future code; ownership-based ordering (persisted order IDs) is the correct protection mechanism (see §6).

---

## Versioning

Any change requires a new version, updated date, and explicit owner approval in Git history. No natural-language paraphrase of a Telegram message may override a requirement here. On conflict with other docs, this document prevails unless superseded by a newer owner-authorized version.

## Cross-Reference

- Persian equivalent: `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md` (identical force)
- Compliance audit: `docs/TRADING_RULES_COMPLIANCE_AUDIT.md` (historical) and `reports/trading_compliance/` (current)
- Canonical behavior docs: `docs/trading.md`, `docs/data-model.md`, `docs/operations.md`
- Executable truth: `tg_signal_ingest/` and `tests/`
