# Codebase Simplification Audit — Tele-MEXC (Revision 2)

- **Date:** 2026-11-17 (Rev 2 — correction & evidence-validation pass + final consistency pass, same day)
- **Repo:** `20kevit/tele-mexc`, branch `arena/58c00512-tele-mexc`
- **Provenance (authoritative revision history):** baseline/source commit `9ac46c0` — the code under audit, **unchanged by any audit revision**. Revision 1 = commit `9f6526f` (audit document only). Revision 2 = commit `c9632f4` (audit document only; **supersedes Revision 1**). The final Rev 2 consistency pass is a further documentation-only commit on this branch. Neither revision modifies any source, test, config, migration, or runtime file: the entire diff from `9ac46c0` to the current head is this Markdown document. All `file:line` citations below refer to the `9ac46c0` code state.
- **Rev 2 scope (finalized by the consistency pass):** corrects the OCO terminology and the per-tranche protection invariant (§2, §3, §11, §A.1), rewrites F-05 / F-06 / F-07 / F-08 / F-09 / F-10 / F-12 / F-15 on re-verified evidence, replaces the un-auditable LOC claims (≈900–1,100 / 500–600) with an itemized, range-level register (§12), fixes stale dispatcher/monitor line citations, replaces the overlapping "20/8/8/10 = 26" classification totals with a single authoritative table that reconciles to the 20 unique findings (§7), and separates all recommendations into five change categories with prerequisites and acceptance criteria (§13). Sections whose substance did not change are carried from Rev 1 with citations re-spot-checked where touched.
- **Scope of the audit itself:** read-only. No code, config, DB, service, or deployment changes were made; no migrations were run; no exchange calls were performed; no services were restarted. This document authorizes nothing — no implementation, no deletion, no merge, no production deployment. Every action it describes requires its own review, tests, and owner approval.

---

## 1. Executive Summary

The codebase is **complex for defensible reasons, plus a smaller, precisely itemized layer of dead scar tissue**. The core trading path (parser → risk → intent → submission gate → exchange evidence → reconciliation → TradeManager OCO lifecycle) is deliberately fail-closed and evidence-first. Most of its size exists because MEXC's native OCO API exposes no parent/child link for stop children and because several real incidents (duplicate submissions, unprotected positions, global BUY freezes) are encoded as permanent invariants.

**Corrected OCO model (authoritative, supersedes Rev 1):** a *position* is not protected by a fixed number of OCO orders. Each **fill tranche** — a durable grouping of entry fills — is protected by **exactly two native MEXC OCO SELL orders** (OCO 1 = `tp1_allocation_pct`, default 50%, of *the tranche* at TP1; OCO 2 = the remainder of *the tranche* at TP2), and **each native OCO is one exchange order with two linked legs** (a TP limit leg and an SL stop-limit leg whose trigger and execution price are both the configured stop-loss). Positions therefore have `2 × (number of fill tranches)` native OCO orders. Tranches are independent — OCOs of different tranches are never siblings. Rev 1's "exactly two protective OCOs per position" was wrong; see §A.1 for the full model and evidence.

Key numbers (Rev 2, itemized in §12):

| Metric | Value |
|---|---|
| Findings catalogued | 20 unique (F-01 … F-20), one primary class each — §7 |
| KEEP (load-bearing complexity) | 5 findings (F-01, F-16, F-17, F-19, F-20) |
| Pure-deletion / merge candidates (zero runtime behavior change) | 6 findings (F-02, F-03, F-04, F-05 Tier-1 rows 1–11, F-13, F-18) — **162 LOC** pure-deletion candidates + duplicated-helper merges; a further **113 LOC** Tier-1 item follows the F-08 decision (§12.1) |
| DEFER (evidence-gated, nothing removed/disabled) | 5 findings (F-06, F-07, F-08, F-10, F-12) |
| Owner policy / consent decisions | 3 findings (F-09, F-14, F-15) |
| Provably production-unreachable code, itemized by exact ranges | **275 LOC total — of which only 162 LOC (rows 1–11) are pure-deletion candidates; the other 113 LOC (notify delivery half) is conditional on the F-08 decision** (§12.1) — **replaces Rev 1's "≈900–1,100"** |
| Live legacy-state handlers (deletion = compatibility behavior change, not pure deletion) | 27 LOC (§12.2) |
| Config-conditional code (inactive under current config, not proven dead) | ≈ 209 LOC by range, DEFER (§12.3) |
| Deferred ranges (WebSocket implementation, F-10) | 233 LOC, DEFER under documented owner decision N-03 (§12.4) |

**Top findings (Rev 2):**

1. **Notification architecture is a layered dual-ledger design with proven, localized duplicate visibility — not a wholesale "everything is written twice" defect** (F-08, rewritten): trade-layer critical alerts go *only* to the immutable domain ledger, routine trade alerts *only* to the legacy outbox; but specific rejection flows produce both a domain-row message and a decision message for the same business moment, and some flows emit two domain rows for one fact. The consolidation is a staged, owner-approved migration with a full compat case — **DEFER**, not a P2 simplification.
2. **275 LOC of provably production-unreachable code, itemized line-by-line** (F-05 Tier-1): zero- or test-only-referenced methods in `trade_manager.py`, `store.py`, `engine.py`, and the never-called delivery half of `TradingNotifier`. **Only 162 LOC of this (rows 1–11) are pure-deletion candidates**, and only after the P0 characterization freeze and per-item verification; the remaining **113 LOC** (the notify delivery half) is **not immediately deletable** — its removal follows the F-08 ledger decision. Several adjacent disabled methods are *live legacy-state handlers* and must not be deleted (§12.2).
3. **The three historical incidents are correctly encoded in current code** (§4): the entry edge case is *fixed*, the MYX metadata failure remains terminal *by design* (optional bounded retry = owner decision, F-14), and the APE-style global BUY freeze is now coverage-based and instrumented instead of time-based — its global scope is an owner policy question (F-15), on which Rev 2 makes **no recommendation**.
4. **Duplicated per-symbol conflict logic and decimal/backoff helpers** across `store.py`/`trade_store.py`/`trade_manager.py` (F-02, F-03, F-04) — pure maintenance risk, trivial merges.
5. **The hand-rolled WebSocket feed is a documented deferred decision (N-03 in `docs/DECISION_REGISTRY.md`), not dead weight to delete** (F-10, rewritten): it has never been exercised against the real exchange, so its removal is **DEFER** until production evidence proves it redundant; Rev 2 lists the exact evidence required.

**Guardrails:** §11 lists the **13** fund-protection invariants that must not be weakened by any simplification. Nothing in the P0–P3 plan touches them; P0 is a test-freeze step, not a code change.

---

## 2. Product Spec Baseline (authoritative contract)

The audit measured every finding against the behavior documented in `README.md`, `AGENTS.md`, and `docs/trading.md` (the authoritative behavior spec), cross-checked against `docs/architecture.md` and `docs/data-model.md`. The contract that constrains simplification:

1. Only Spot **BUY** signals execute; SHORT/SELL are parsed but refused before any REST call (`parser.py` `_action` classifies; refusal happens at parse/risk/planning layers, before submission).
2. Entry is a **LIMIT** order at the signal entry price (never converted to market; `risk.py`, `engine.py`). Conditional entry waits for an upward crossing of `entry − offset·tick_size` (offset default 3 ticks, `config.py:137-141`).
3. **Each confirmed entry-fill tranche is protected by exactly two native OCO SELL orders** (OCO 1 = `tp1_allocation_pct`, default 50%, of the tranche at TP1; OCO 2 = the tranche remainder at TP2); **each native OCO carries a TP limit leg and an SL stop-limit leg** whose trigger and execution price are both the configured SL. Positions with multiple tranches have proportionally more OCO orders (`docs/trading.md:455-474`; `trade_manager.py:_place_oco_exits`; `config.py` TP percentages).
4. A BUY may only be submitted when a complete ordered exit ladder (SL < Entry < TP1 ≤ TP2) and a working OCO path exist (`trade_manager.py:595-628 _pre_buy_exit_problem`).
5. Uncertain exchange outcomes are **never retried blindly**; they park as `unknown_requires_reconciliation` and are reconciled read-only (`engine.py:1511-1526`, `trade_manager.py:_UNCERTAIN_ERRORS`).
6. Notifications are durable (outbox before delivery), secrets-redacted, owner-visible for safety events; delivery is decoupled from trading (independent dispatcher worker).
7. No test/simulation product mode exists; every valid new signal is eligible for automatic execution subject to the gates (`config.py` explicitly rejects legacy mode selectors).

Deviation from spec items is a correctness bug, not a simplification. This audit proposes none.

---

## 3. Current Architecture & Data Flow (as verified)

```
Telegram (Telethon) ──► messages table ──► processing queue (single consumer)
                                              │
                                              ▼
                                   TradingEngine.process_item
                                   ├─ parse (parser.py, versioned)
                                   ├─ activation boundary check (per-channel, forward-only)
                                   ├─ reference price + symbol metadata (SymbolRegistry, TTL 300s)
                                   ├─ risk validation (risk.py: ladder, LIMIT-only, fixed quote)
                                   ├─ store.create_intent (idempotency key, budget reservation)
                                   ├─ TradeManager.on_intent_planned (creates trade,
                                   │        decides conditional vs direct entry)
                                   └─ _execute_intent
                                        ├─ SubmissionGate.check (emergency stop / pause /
                                        │        durable new-BUY block) inside the atomic
                                        │        planned→submitting claim
                                        ├─ pre-submit revalidation (metadata, risk)
                                        ├─ balance preflight (fresh account snapshot)
                                        ├─ same-symbol open-order check (exchange-backed,
                                        │        per-symbol lock held across check+submit)
                                        ├─ exchange_operations evidence row BEFORE the call
                                        └─ POST order via HybridMexcClient (writes → CLI,
                                                 reads → REST)
                                              │
              ┌───────────────────────────────┘
              ▼
   TradeManager (per-trade lock)
   ├─ entry fill sync (reconcile_intent_if_due, durable backoff)
   ├─ OCO tranche placement: per fill tranche → 2 native OCO SELL orders
   │   (each = TP limit leg + SL stop-limit leg), invariant checks
   │   (reserved ≤ sellable, sellable fail-closed)
   ├─ periodic reconciliation (60s): order states, fills, stop-child
   │   discovery via allOrders exact match, buy-block refresh
   └─ price monitor (polling 2s + best-effort WS): conditional trigger,
       unprotected-at-SL alert only (no market sell, §7 rule)

Audit trail: audit_events (+ immutable domain events, migration 25)
Deliveries: NotificationDispatcher (separate SQLite ledger + worker unit)
Control:    ControlBot (pause/resume, settings, setup) → same DB
```

Deploy units: `deploy/tg-ingest.service`, `deploy/tg-notification-worker.service`. Single-instance locks per DB (`locking.py`). Shared MEXC rate budget across processes via file-backed `SharedWeightedRateGate` (`mexc/client.py:150-230`).

---

## 4. Historical Incident Analysis — history vs verified current behavior

### 4.1 Entry edge case: entry 100 / market 99 → bot waited for 97 instead of placing a limit at 100

- **History (reported):** with market below entry but already above the computed trigger, the bot chose the conditional path and waited for a *fresh upward crossing* of the trigger, missing the valid entry window.
- **Verified current behavior (high confidence):** **fixed.** `_create_trade` computes the trigger and then explicitly handles this case: "The initial reference price may already be at/above the computed trigger while still below Entry. Treat that as an already-fired trigger and submit the normal LIMIT at Entry immediately" — `trade_manager.py:540-587`. When `current >= trigger_price`, `entry_mode` stays `direct` and `trigger_price` is nulled, so the LIMIT at Entry is submitted immediately. Only `current < trigger_price` selects the conditional path (`trade_manager.py:584-587`, evaluated per tick at `trade_manager.py:918-931 _check_entry_trigger`).
- **Residual risk:** none identified. A missing/invalid offset fails closed instead of silently choosing a path (`trade_manager.py:507-516`).

### 4.2 MYX metadata failure → terminal rejection

- **History (reported):** a transient symbol-metadata failure caused the signal to be rejected terminally even though a later retry would have succeeded.
- **Verified current behavior (high confidence):** the failure is still **terminal at two checkpoints**:
  1. Planning: `_process_item` rejects the signal with "symbol metadata unavailable; refusing to trade without exchange constraints" (`engine.py:489-497`) — a terminal `rejected` state.
  2. Pre-submit: `_execute_intent` re-validates and `_reject_before_submit` produces "pre-submit revalidation failed: symbol metadata is unavailable" (`engine.py:1164-1176`).
- **Mechanism (high confidence):** `SymbolRegistry.get` (`mexc/symbols.py:278-291`) serves a 300 s TTL cache; on miss it performs a live `GET /api/v3/exchangeInfo` and **propagates any error** (network, WAF, 5xx). Failed lookups are not negatively cached, so a *subsequent* signal retries the fetch — but the signal that hit the transient failure is lost.
- **Classification:** intentional fail-closed behavior (trading without exchange constraints is prohibited). It is safe (no position can become unprotected from a metadata failure because no BUY is ever placed), but it sacrifices availability on transient failures. Improvement candidate F-14 (bounded retry, owner decision) — not a correctness fix, an availability tweak requiring owner consent because it changes rejection statistics.

### 4.3 APE OCO-cancel → 26 h global new-BUY block on unrelated symbols

- **History (reported):** an OCO cancellation on one symbol froze new BUYs across all symbols for ~26 hours.
- **What the current code guarantees (high confidence):**
  - There is **no time-based and no historical-state global block**. The gate reads a single durable flag (`gate.py:123-140 _buy_block_reason`); the comment at `gate.py:155-159` states: "There is intentionally no account-wide/history-wide entry lock here. Historical trade reconciliation state is per-order/per-trade and must never disable unrelated symbols."
  - The flag is recomputed from **verified exit coverage**: `_refresh_buy_block` (`trade_manager.py:733-790`) lists non-terminal trades, computes `_is_unprotected_critical` (acquired > 0, remaining > 0, verified OCO coverage < remaining; `trade_manager.py:700-731`), and persists the block with a precise reason (`unprotected position(s): SYMBOL:trade_id…`) plus audit events `new_buys_blocked` / `new_buys_unblocked`. It is refreshed after every reconciliation sweep (`trade_manager.py:3301-3309`), after every OCO placement attempt, and around startup recovery.
  - A **canceled OCO parent whose stop-child execution is not independently attributed** parks the trade in `needs_reconciliation` (`trade_manager.py:1560-1591 _unattributed_oco_parent_cancellations`; handling at `trade_manager.py:3430-3455`) and therefore keeps coverage unverified → the block stays. Clearing requires the stop child to be uniquely matched in `allOrders` (exact field match, no id arithmetic, `trade_manager.py:1693-1758 _select_oco_stop_child`) and its status queried by exact id — with bounded discovery backoff (15-min cap, `trade_manager.py:1631-1665`) instead of hot loops.
- **Honest assessment:** the *mechanism* that made a 26 h freeze possible — "block until protection is proven, however long that takes" — is **still present by design** (`trade_manager.py:737-756`: "The block is never cleared by time. It clears only when every open bot-owned position is fully sold or has exchange-confirmed working coverage"). What changed versus the incident era: the reason is precise and audited, discovery is bounded and evidence-based, and readiness reporting marks the block as diagnostic context (`readiness.py:356-360`). Whether the historical 26 h came from the removed global-history blocking or from prolonged discovery failure cannot be established from this tree (**low confidence** on the original root cause; **high confidence** on current behavior). The *global scope* of the block is a policy question, not a bug — see §5.3 and F-15.

---

## 5. Global Entry Gates — inventory and the "why global" answers

All gates verified at the audited commit. For each: what it blocks, where it lives, and the required answer to *"why must this block every new BUY instead of only the affected trade or symbol?"*

| # | Gate | Scope | Blocks | Verified at |
|---|---|---|---|---|
| G1 | Emergency stop | global | everything (entry + protection) | `gate.py:146-148`, `config.py` `emergency_stop` |
| G2 | Owner pause | global | everything (entry + protection) | `gate.py:149-153` (`control_setup.paused`), atomic with the planned→submitting claim (`store.py:claim_intent_for_submission`) |
| G3 | Durable new-BUY block | global, **entry purpose only** | new BUYs only; protection submissions pass | `gate.py:154-162` + `store.py:set_buy_block/buy_block_reason`; source of truth recomputed from coverage (`trade_manager.py:733-790`) |
| G4 | Same-symbol open-order check | **per-symbol** (not global) | a second BUY on the same symbol while an exchange order is open | `engine.py:191-225`, applied under a per-symbol lock (`engine.py:163`, `locking.py KeyedRLockPool`) |
| G5 | Activation boundary | per-channel, forward-only | messages at/below boundary; `None` boundary ⇒ refuse all | `engine.py:342-377, 418-433, 777-784`, `store.py:ensure_activation_boundary` |
| G6 | Pre-buy exit-ladder/OCO-path check | per-trade | BUY without complete ladder or OCO path | `trade_manager.py:595-628` |
| G7 | Balance preflight | per-submission (account read) | insufficient spendable after outstanding intents | `engine.py:1639-1680`, `trading/balance.py` |
| — | Runtime readiness callbacks (Telegram connected, channel, boundary armed) | **diagnostic only** | nothing (owner policy: never block) | `gate.py:63-65` ("these never block entries… not read by `check`") |

### 5.1 G1 Emergency stop — why global?
It *is* the global kill switch by definition; its scope matches its purpose. Nothing to change.

### 5.2 G2 Owner pause — why global?
The control bot's pause means "stop trading entirely" (single product, two owners). A per-symbol pause would be a different feature that was never requested. The pause is read inside the same SQLite transaction as the atomic `planned → submitting` claim, so it cannot race a submission (`store.py:claim_intent_for_submission` with `precheck`). Keep.

### 5.3 G3 Durable new-BUY block — why must it block *every* new BUY?
**Policy answer (documented):** the block encodes the owner-confirmed invariant "never add exposure while any bot-owned position lacks verified exit protection" (`trade_manager.py:737-756`; comment states it is deliberately independent of a single trade row so a restart cannot hide an unprotected position behind a stale label).

**Engineering answer:** the root causes of "coverage cannot be verified" are frequently **systemic, not symbol-local**: entry fill synchronization (`fill_sync_required` gates OCO sizing, `trade_manager.py:2280-2298`), base-fee provability (`_total_base_fees` fail-closed across entry+exit fills, `trade_manager.py:2866-2967`), server-clock health for signed writes (`mexc/clock.py`), and the shared rate budget (`SharedWeightedRateGate`). A per-symbol block would assert that the failure on symbol A shares none of these causes with symbol B — an assertion the code cannot currently prove. Note also the block is already *asymmetric*: it stops entries only, never protection submissions, so it does not delay the fix of the unprotected position itself (`gate.py:139-144`).

**F-15 stance (Rev 2):** the global scope is an **owner policy decision**. This audit offers no argument for or against changing it and no default recommendation. Any scope change is a policy reversal of an owner-confirmed protection rule and requires explicit owner sign-off (see F-15).

---

## 6. Detailed Findings

Each finding: evidence, one primary classification with secondary tags where applicable (§7), risk of acting, risk of not acting. Confidence: high unless stated. **Rev 2 re-verified every citation in F-05, F-06, F-07, F-08, F-10, F-12, F-15 and the dispatcher/monitor/notify/OCO citations; other sections carry Rev 1 citations over the same code state.**

### F-01 — Native-OCO stop-child discovery subsystem is forced complexity — **KEEP**
- **Evidence:** `trade_manager.py:1272-2260` (≈ 1,000 LOC): `_oco_leg_targets`, `_sync_oco_legs`, `_oco_stop_children_settled`, `_discover_oco_stop_children`, `_select_oco_stop_child`, `_query_and_persist_oco_stop_child`. Root cause documented in-code (`trade_manager.py:112-127`): MEXC returns OCO children as separate `allOrders` rows with `orderListId = -1`, null client ids, and no exposed trigger; `mexc/client.py:all_orders` docstring repeats the constraint. Exact-match-only matching with a hard fail-closed path (`trade_manager.py:1705-1758`), bounded backoff (`trade_manager.py:1631-1665`), and a 7-day history window guard (`trade_manager.py:1763-1770`).
- **Why KEEP:** there is no exchange-native alternative; guessing parent/child links would risk double-selling or miscounted inventory. This is the highest-complexity, highest-necessity subsystem in the repo.
- **Risk of removal:** catastrophic (inventory misattribution). **Risk of keeping:** maintenance cost — mitigated by the P1 test freeze.

### F-02 — Duplicated per-symbol conflict logic — **SIMPLIFY-MERGE (P1)**
- **Evidence:** `store.py` defines `CONFLICTING_TRADE_STATES` + `has_active_trade_for_symbol` and `trade_store.py:411-440` redefines both **identically** in the subclass. The engine uses the base-store version; the override adds no behavior (same states, same query shape).
- **Action:** delete the duplicate in `trade_store.py` (keep the docstring in one place).
- **Risk:** trivial. **Risk of not acting:** the two definitions can drift; a state added to one but not the other silently changes concurrency semantics depending on which class a caller holds.

### F-03 — Duplicated decimal/timestamp helpers — **SIMPLIFY-MERGE (P1)**
- **Evidence:** `trade_manager.py:207-232` (`_dec`, `_optional_decimal`), `trade_store.py:23-31` (`_optional_decimal`, second copy), `store.py:dec_str`/`_parse_iso`, `trade_manager.py:_dec_ts/_int_ms`. Four near-identical parsers of Decimal/ISO strings.
- **Action:** consolidate into one internal module (e.g. `trading/decimals.py`).
- **Risk:** trivial (pure functions, fully covered by tests).

### F-04 — Two hand-rolled backoff-streak codecs — **SIMPLIFY-MERGE (P1)**
- **Evidence:** `_oco_discovery_streak`/`_oco_discovery_backoff_remaining` (`trade_manager.py:1631-1665`) and `_trade_order_not_found_streak`/`_trade_order_not_found_backoff_remaining` (`trade_manager.py:3621-3650`) both encode retry state into `not_observed_reason` strings (`PREFIX:count=N:reason=…`) and re-implement identical parsing plus exponential-delay math (60·2^(n−1), 900 s cap). `_persist_oco_stop_unresolved` and `mark_trade_order_not_observed` are the corresponding writers.
- **Action:** extract one shared codec + delay function; keep the persisted string format for backward compatibility with existing rows.
- **Risk:** low. **Risk of not acting:** two retry clocks drift (e.g. one gets a jitter fix, the other doesn't).

### F-05 — Disabled legacy exit machinery in TradeManager — **REMOVE (Tier-1 only) + DEFER (Tier-2 legacy-state handling)** *(rewritten in Rev 2)*

Rev 2 splits this finding into three tiers with different reachability bases. The reachability basis for every item is a repo-wide grep over `tg_signal_ingest/` **and** `tests/` (definitions excluded), at the audited commit; "production-unreachable" means no call site outside the method's own body and disabled siblings, i.e. the runtime can never reach it.

**Tier 1 — provably production-unreachable; pure deletion, zero runtime behavior change** (exact ranges in §12.1):

| Item | Range | Refs (non-definition) | Basis |
|---|---|---|---|
| `trade_manager._target_qty` | `trade_manager.py:2705-2707` | **0** anywhere | unreferenced |
| `trade_manager._transition_state` | `trade_manager.py:1130-1176` | 1 (test only: `test_trading_lifecycle_recovery.py:892`) | unreferenced in production |
| `trade_manager._compute_available` | `trade_manager.py:1193-1204` | 2 (test only: `test_focused_followup_repair.py:876-877`) | unreferenced in production |
| `trade_manager._enter_tp1` | `trade_manager.py:1101-1113` | 1 (test only: `test_trading_cli_protection.py:209`) | unreferenced in production |
| `trade_manager._submit_tp_order` | `trade_manager.py:1237-1245` | 1 (test only: `test_trading_cli_protection.py:215-218`) | unreferenced in production; raises unconditionally |
| `trade_manager._enter_stop_loss` | `trade_manager.py:2799-2810` | 0 call sites (its own name appears only in its own f-string) | unreferenced |
| `trade_manager._resume_stop_loss` | `trade_manager.py:2812-2826` | 0 call sites (name appears only in its own f-string) | unreferenced |
| `trade_manager._submit_stop_loss_order` | `trade_manager.py:2828-2844` | 0 call sites (name appears only in its own f-string) | unreferenced |
| `store.pending_protection_verification` | `store.py:1414-1420` | **0** anywhere | unreferenced |
| `store.clock_health_state` (function only) | `store.py:40-48` | **0** anywhere; the `CLOCK_SKEW_*` **constants** (`store.py:35-37`) ARE used (`main.py:785-787`) and stay | unreferenced |
| `engine.submit_stop_loss_market_sell` | `engine.py:1902-1919` | 4 (comments only) | unreferenced; raises `RuntimeError` |
| `TradingNotifier` delivery half (`flush`/`_report_ambiguous`/`_deliver`/`pending_count`) | `notify.py:225-337` | production callers **0** (tests only, 28 call sites) | unreferenced in production — but deletion is sequenced behind the F-08 ledger decision, so its *removal class* is DEFER (F-07) |

Rows 1–11 are behavior-neutral at runtime **because nothing can reach them**; the required work is test-side (update/remove the tests that exercise them directly) plus the P0 characterization freeze — they are pure-deletion **candidates**, subject to per-item verification. Row 12 is equally unreachable but is **not a P1 deletion candidate**: its removal is sequenced behind the F-08 ledger decision (see F-07).

**Tier 2 — live legacy-state handling; deletion = behavior change, not deletion** (§12.2):

| Item | Range | Live caller |
|---|---|---|
| `trade_manager._enter_tp2` | `trade_manager.py:1115-1128` | `_advance_trade_after_reconcile` (`trade_manager.py:3717`) — for a legacy `tp1_active` trade whose TP1 completed, calls `_enter_tp2`, which **fails closed** into `needs_reconciliation` via `_disable_legacy_exit` |
| `trade_manager._do_tp_sale` | `trade_manager.py:1187-1191` | `_enter_tp1`/`_enter_tp2` bodies |
| `trade_manager._disable_legacy_exit` | `trade_manager.py:1178-1185` | `_do_tp_sale` |

These methods are the **fail-closed parking behavior** for pre-OCO / legacy trade rows (`tp1_active`, `tp2_active`, legacy stop states): when reconciliation advances such a row, the current code routes it to `needs_reconciliation` with an explanatory note instead of submitting a standalone TP/Market-Sell order. **Rev 1's suggestion to "re-route those branches to the existing needs_reconciliation parking" is itself a behavior change** — the parking *is* what these methods do, and re-routing/deleting them alters what happens to legacy DB rows during reconciliation. Any change here requires: (a) an inventory of which legacy `trade_state` values can reach `_advance_trade_after_reconcile` (from a production DB snapshot, not from code alone); (b) a written decision on the desired handling of each legacy state; (c) characterization tests that pin the current parking behavior first. Until then: **DEFER — do not delete.**

**Tier 3 — §7 "no Market Sell" proof surface.** `_enter_stop_loss`/`_resume_stop_loss`/`_submit_stop_loss_order` and `engine.submit_stop_loss_market_sell` are Tier-1-unreachable, but they also *document* the §7 policy ("Native OCO SL legs are the only authorized automatic stop-loss mechanism") in code. Deleting them is behavior-neutral; keeping a single assertion-style test that proves no Market Sell call site exists preserves the guarantee with less surface. This is a presentation choice for the owner, not a safety change either way.

- **Risk of acting (Tier 1):** low; the only real hazard is deleting a Tier-2/Tier-3 item by name-similarity — hence the itemized register. **Risk of not acting:** readers must mentally execute dead code to conclude the strategy; the disabled bodies are a standing temptation and a maintenance trap.

### F-06 — Attached-protection path is inactive under the current production backend — **DEFER (runtime evidence required)** *(revised in Rev 2)*

Rev 2 downgrades "dead in production" to the precise claim: **inactive under current configuration**, which is not the same as deletable.

- **Evidence:** `HybridMexcClient.supports_attached_protection = False` (`mexc/hybrid.py:20`); the engine attached-protection code (`engine.py:1298-1420` region: `_protection_prices`, `_record_protection_intent`, `_verify_protection`, `upsert_protection`/`update_protection` writers at `engine.py:1342-1547`, call sites `engine.py:1476, 1481, 1590, 2334`) therefore never executes its exchange path on the only production backend (`docs/trading.md:921` confirms the hybrid backend does not use the attached path). The `order_protection` table (migration 7, `db.py`) and store methods (`store.py:1335-1420`) exist for it; `pending_protection_verification` has zero references (F-05 Tier 1); `trade_manager._exchange_protection_active` (`trade_manager.py:934-955`) reads a table that is never written for live intents under the current backend.
- **Why DEFER, not REMOVE:** the writers are config-conditional, not unreferenced — a future backend with `supports_attached_protection = True` reactivates the whole path. Removal requires runtime evidence that the path is operationally orphaned, not just statically inactive: (1) `SELECT COUNT(*) FROM order_protection` in the production DB (expected 0 or legacy-only rows); (2) owner confirmation that no CLI-backend/attached-protection roadmap exists; (3) a decision on whether historical `order_protection` rows (if any) are retained for audit. Nothing is removed or disabled by this audit.
- **Action if evidence confirms orphanhood:** delete the engine path and unused store method; keep the DB table and historical rows; keep `_exchange_protection_active` (harmless defense if a future backend sets the flag).
- **Risk of not acting:** two protection models coexist in the reader's mind.

### F-07 — `TradingNotifier` delivery half is unreachable in production — **DEFER (sequenced behind F-08)** *(revised in Rev 2)*
- **Evidence:** `notify.py:225-337` (`flush`, `_report_ambiguous`, `_deliver`, `pending_count`) implement Bot-API delivery, claim, and ambiguity recovery for the legacy outbox, but the producer path intentionally never flushes (`notify.py:138-146` docstring: the synchronous `flush()` is intentionally removed from the producer path), and `service.py`/`main.py` deliver exclusively through `NotificationDispatcher` (`main.py:cmd_notifications_worker`). Repo-wide grep: production callers of `flush` = **0** (33 test call sites only). Consequence: in production, legacy outbox rows sit at `status='pending'` until the dispatcher's `relay_legacy_once` bridges them (`notification_dispatcher.py:1245-1316`).
- **Why DEFER rather than REMOVE:** the delivery half is the only in-process sender for the legacy outbox; whether it can be deleted depends on the F-08 ledger decision (if the legacy relay/outbox is retained indefinitely for outage recovery or operator requeue semantics, the delivery half's fate follows it). Deleting it now is behavior-neutral but forecloses an option the F-08 review has not yet decided.
- **Action:** decide together with F-08; keep `enqueue` either way (the legacy outbox is a relay *source*).
- **Risk:** low either way. **Risk of not acting:** two delivery implementations invite accidental reuse.

### F-08 — Notification architecture: layered dual-ledger design with proven, localized duplicate visibility — **DEFER (staged consolidation, owner-approved, full migration case required)** *(fully rewritten in Rev 2)*

Rev 1 claimed "every notification is persisted twice and relayed twice." **That claim is false**, and the required event mapping below replaces it.

**The two ledgers and their complete producer/dispatcher mapping (all verified at the audited commit):**

**Ledger B — immutable domain events** (`audit_events`, `event_class='domain'`, migration-25 append-only triggers; written **inside the business transaction** by `store.record_domain_event`, `store.py:312-336`):
- `signal.received` (`store.py:693`), `signal.rejected` (`store.py:737`; suppressible per call via `emit_rejection_event=False`),
- `intent.created` (`store.py:1228`), `intent.state_changed` (`store.py:1522` on claim; `store.py:1611-1621` on transitions — severity **CRITICAL** for `unknown_requires_reconciliation`, **ERROR** for `rejected`/`failed`, else INFO), `intent.submission_accepted` (`store.py:1644`),
- `fill.recorded` (`store.py:1796`), `reconciliation.outcome` (`store.py:1885`), `exchange.write_outcome` (`store.py:573`),
- `trade.state_changed` (`store.py` trade mutation path; `trade_store.py:251`), `trade_order.state_changed` (`trade_store.py:693`), `oco_group.state_changed` (`trade_store.py:794`), `oco_leg.state_changed` (`trade_store.py:917`), `trade.protection_critical` (`trade_manager.py:4016`; deterministic uuid5 `event_id` for alert-on-change semantics, `DomainEventConflict`-suppressed, severity CRITICAL),
- default audience `(owner, owner_b, developer)` (`domain_events.py:36`).

**Ledger A — legacy formatted outbox** (`trading_notifications`, one row per `(event_key#chunk, role)`; writer-side dedupe on the `(event_key, role)` uniqueness per `notify.py:130-141`): written by `TradingNotifier.notify/notify_trade/notify_intent` (`notify.py:157-218`) from engine/trade-manager call sites — signal decisions (`engine.py:467, 1086, 1112, 1243, 1272`), conditional-entry waiting (`engine.py:588`), startup recovery (`engine.py:763`), OCO placement failures (`trade_manager.py`, e.g. `OCO_MINIMUM_NOT_MET`, `OCO_INVARIANT_UNKNOWN`), and `Notify.notify` generic alerts (`engine.py:913, 1059`).

**The trade layer deliberately routes, it does not duplicate** (`trade_manager._notify_trade`, `trade_manager.py:3997-4030`): `SL_UNPROTECTED_CRITICAL`, `OCO_INVARIANT_UNKNOWN/WOULD_VIOLATE/VIOLATED`, `TRADE_CLOSE_REFUSED_UNKNOWN`, and `BUY_BLOCK_ACTIVE_*` go **only** to Ledger B as `trade.protection_critical` (CRITICAL); everything else goes **only** to Ledger A. This is an either/or router, not a double writer.

**Dispatcher (separate SQLite delivery ledger; source DB opened read-only via `PRAGMA query_only=ON`, `notification_dispatcher.py:1166`):** every cycle, `run_once` (`notification_dispatcher.py:1387-1400`) runs **both** relays — `relay_once` (Ledger B, cursor on `audit_id`, `:1197-1218` → `relay_batch` `:775-890`: renders text, applies audience — CRITICAL bypasses audience and reaches every configured recipient — chunks, per-recipient `delivery_decision`, `INSERT OR IGNORE` dedupe on `relayed_events.event_id` + `deliveries UNIQUE(event_id, recipient_role, part_no)`) and `relay_legacy_once` (Ledger A, monotonic `legacy_notification_cursor`, `:1245-1316`: `_legacy_severity` mapping `:1219-1244`, `delivery_decision`, chunk inserts, `relayed_legacy_notifications` bookkeeping). Delivery then goes through one pipeline: `deliver_one` (`:1326`), `MAX_ATTEMPTS = 10` shared per `(event_id, recipient_role)` via sibling-attempt summation (`:30`, claim `:927-971`, finish `:974-1069`), dead-letter on budget exhaustion, ambiguous sends never auto-retried, stale-claim recovery (`:898-925`), operator-only requeue for ambiguous/dead-letter (`:1088-1135`), Retry-After honored. Suppression: the only content rule is the routine `oco_group.state_changed` `active↔submitting` flip (`delivery_decision`, `:216-226`); owner-safety bypass guarantees CRITICAL and `signal.rejected_locally`-class events reach owners regardless of preferences (`:138-165`); suppression decisions are persisted, so operator history stays complete.

**Where duplicate *user-visible* messages ARE proven (code-level):**
1. **Submission-blocked flow:** `execute_intent`'s `SubmissionBlocked` handler (`engine.py:1253-1281`) runs `transition_intent(rejected)` → Ledger B emits `intent.state_changed` with severity **ERROR** (`store.py:1611-1621`) — and then calls `_notify_intent_decision(SUBMISSION_BLOCKED)` (`engine.py:1272`) → Ledger A writes a `signal.rejected_locally` decision message, which **bypasses owner preferences** (`notification_dispatcher.py:158-165`). Under default routing both rows deliver to the owners: **two Telegram messages, one business fact, different texts.**
2. **Pre-submit expiry flow:** `_reject_before_submit` expiry path (`engine.py:1209-1219`) emits `intent.state_changed` (ERROR) *and* `update_signal(rejected)` → `signal.rejected` (INFO) — two domain rows for one decision, both rendered → **two messages** from Ledger B alone.
3. **Planning rejections:** engine rejection sites that do NOT pass `emit_rejection_event=False` (e.g. `engine.py:413, 437, 443, 453, 480, 490, 503, 515, 545, 555, 1172, 1216, 1452`) emit `signal.rejected` rows; sites that pair the rejection with a decision message use `emit_rejection_event=False` (`engine.py:379`) specifically to avoid the double message — **the codebase already knows about this hazard and handles it inconsistently.**

**Where duplication is NOT present:** the trade layer (router, above), trade/order/OCO state events (Ledger B only), and recovery/decision messages without a matching domain emit (Ledger A only). Ledger A's direct delivery half is never invoked in production (F-07), so outbox rows cannot double-send via two paths.

- **Revised classification:** architectural overlap + migration risk, with proven duplicate visibility for the rejection class — **not** "two parallel systems doing the same thing."
- **Action (staged, owner-approved; nothing executed by this audit):** Phase 0 — map every Ledger-A `event_key` to either a Ledger-B `event_type` or an explicit "Ledger-A-only" marker (the taxonomy exists in `domain_events.py:EVENT_REGISTRY`; the mapping table itself must be written and reviewed). Phase 1 — make rejection flows single-ledger (extend the `emit_rejection_event=False` pattern or route rejections exclusively through one ledger), eliminating the proven duplicates without touching the trade-layer router. Phase 2 — only after a defined soak window and a documented outage-recovery/compat case (backlog drain, `relayed_legacy_notifications`/cursor semantics, operator requeue expectations, retention of delivery history): decide whether the legacy relay and outbox write path retire (which would complete F-07) or remain as the formatted-message ledger. **No path is deleted by this audit, and no phase is authorized without the full migration + compatibility case (backlog behavior, outage recovery, recipient-specific severity/audience behavior, retry/requeue semantics).**
- **Risk:** medium (notification behavior change; the trade-layer CRITICAL routing and owner-safety bypass must be preserved verbatim — `notification failures must not block trading` is unaffected since both ledgers are already decoupled from trading). **Risk of not acting:** permanent inconsistent rejection messaging (2–3 messages for one fact in some flows), two vocabularies (`_legacy_severity` `:1219-1244`) and two relay cursors maintained forever.

### F-09 — Notification volume reduction: decided by the owner; design questions remain — **OWNER-DECISION (design only)** *(aligned with the recorded owner decision in the Rev 2 final pass)*
- **Owner decision (recorded in the owner's Notion decision document for the Tele-MEXC notification redesign, last edited 2026-11-17):** notification volume must be significantly reduced; messages must be concise, meaningful, and action-oriented; repetitive status transitions, polling updates, unnecessary technical details, and internal retry noise must not generate needless user-facing messages. Internal event and delivery history may remain complete (filtering affects delivery, not recording). CRITICAL and genuine safety-unknown alerts remain protected. Per-recipient severity settings and the existing maximum of 10 automatic delivery attempts (`MAX_ATTEMPTS`, `notification_dispatcher.py:30`) remain valid unless superseded by a newer explicit decision. **Whether volume should be reduced is therefore settled and is no longer an open question in this audit.**
- **Evidence (scale, from the producer enumeration above):** one full trade emits ≈ 15–30 Telegram messages across recipients: `signal.received`, `intent.created`, every `intent.state_changed`, every `trade.state_changed`, per-leg `trade_order.state_changed`, `oco_group.state_changed`, per-leg `oco_leg.state_changed`, **per fill** `fill.recorded`, **per exchange write** `exchange.write_outcome`, and `reconciliation.outcome` on every change. Exactly one content-suppression rule exists (routine `active↔submitting` group flip, `delivery_decision`, `notification_dispatcher.py:216-226`); threading (`event_thread_key`, `:543-580`) groups replies but does not reduce count.
- **Remaining open items (design only; owner-approved before any implementation):** (1) which lifecycle events are combined into per-trade summaries (candidates: per-fill `fill.recorded`, INFO `oco_group`/`oco_leg`/`trade_order` state flips, repeated `reconciliation.outcome` rows); (2) which messages require independent delivery rather than summary folding (e.g. decision messages, first-of-class protection alerts); (3) the exact coalescing and suppression rules that preserve the owner-safety bypass (`notification_dispatcher.py:138-165`), CRITICAL/safety-unknown delivery, and per-recipient severity settings verbatim.
- **This audit implements nothing and proposes no numerical message target**; the coalescing/suppression design is the owner's to approve.
- **Risk:** notification behavior change → requires the owner-approved design plus tests. **Risk of not implementing:** the decided volume reduction does not materialize (alert fatigue persists; genuinely critical messages lose signal value in noise).

### F-10 — Unexercised hand-rolled WebSocket feed — **DEFER until production evidence proves redundancy** *(rewritten in Rev 2)*
- **Evidence:** `MexcWebSocketFeed` (`monitor.py:267-499`) implements a from-scratch WebSocket client (handshake `:359`, frame parser `:384-392`, subscribe `:479-499` — raw JSON `{"method": "SUBSCRIPTION", …}` frames to `wss://wbs.mexc.com/raw`, `:41`) for which its own docstring says behavior against the real exchange "has not been exercised in this tree." Repo-wide grep: **zero references** to `MexcWebSocketFeed` outside `monitor.py` (production, tests, docs). `_PrimaryWithFallback` (`:583-634`) is referenced only by `tests/test_trading_monitor.py:25,169` and starts the polling fallback immediately, so a working WS would add duplicate ticks, never latency wins. The 2 s polling requirement is spec-compliant (`config.py monitor_poll_interval_s`).
- **Documented decision status:** `docs/DECISION_REGISTRY.md` N-03 lists "MEXC WebSocket market data" as **runtime-confirmed / deferred**, and its preamble states deferred items are deliberate owner decisions, not unfinished work. Deleting the feed would therefore contradict a recorded owner decision, not just remove unexercised code. Separately, the raw-JSON subscribe framing is *suspected* to be incompatible with MEXC's documented WebSocket protocol (which requires a protobuf-based handshake/topic format) — this audit flags that as an **unverified claim** that only a runtime test can settle.
- **Why DEFER, not REMOVE:** lack of references is not lack of operational value — the feed is the designated vehicle for a latency improvement the owner explicitly deferred, and "never worked" can only be proven at runtime, not statically. **Nothing is removed or disabled by this audit.**
- **Required evidence before any removal decision (all must be recorded):** (1) a runtime connectivity test of `MexcWebSocketFeed` against `wbs.mexc.com/raw` with logs showing handshake, subscribe acceptance, and ticker frames (or a documented protocol analysis showing the framing cannot work); (2) if it connects: a measured latency comparison vs the 2 s poller on conditional-entry trigger handling; (3) an owner statement on whether sub-2 s trigger latency is a product requirement (the spec currently allows polling); (4) if both feeds ever run concurrently: a dedupe design for `_PrimaryWithFallback`'s duplicate ticks; (5) an updated N-03 registry entry recording the outcome.
- **Risk of acting without evidence:** loses the only latency-improvement vehicle and reverses a documented decision. **Risk of not acting:** ~233 LOC (by range `monitor.py:267-499`) of unexercised network code remains a latent-failure surface — acceptable while deferred and never enabled.

### F-11 — `cleanup.py` inspection-only machinery — **SIMPLIFY (P2)**
- **Evidence:** `trading/cleanup.py:212-219`: "inspection only: automatic order cancellation is permanently disabled… (apply requested but ignored)". The ~180 LOC of `CleanupCandidate` eligibility/threshold logic (`:29-140`) feeds a report that never cancels anything; the `apply` flag is ignored by design.
- **Action:** reduce to open-order inspection + audit (or fold into a `diagnostics` command); delete eligibility/apply machinery.
- **Risk:** low (CLI output changes shape only). **Risk of not acting:** the module's name and `apply` parameter suggest capabilities it deliberately lacks.

### F-12 — Approval/learning-era schema residue — **DEFER (documentation now; archival/drop only after the dependency inventory below)** *(corrected in Rev 2)*

**Rev 1 wrongly listed five live control tables as residue.** The control bot uses them in production: `control_flow` (`control/store.py:214-224`), `control_updates` (`:248`), `control_audit` (`:261`), `control_notices` (`:270`), `control_secrets` (`:285-313`). They are **not** simplification candidates.

Corrected inventory at the audited commit (all "unreferenced" claims = repo-wide grep over `tg_signal_ingest/` + `tests/`, non-`db.py`):

| Table(s) | Created at | Non-`db.py` refs | Status |
|---|---|---|---|
| `control_setup`, `control_settings`, `control_flow`, `control_updates`, `control_secrets`, `control_audit`, `control_notices` | migrations 5+ | `control/service.py`, `control/store.py` | **LIVE — keep** |
| `control_learning_samples` (`db.py:960`), `control_approvals` (`db.py:980`) | migration 5 | **0** | residue — defer |
| `trading_daily_spend` (`db.py:890`) | migration 2 | **0** | residue — defer (daily caps are an explicitly rejected config knob, `config.py` obsolete list) |
| `signal_candidates` (`db.py:1181`), `signal_candidate_reviews` (`:1231`), `analysis_runs` (`:1244`), `analysis_failures` (`:1256`), `channel_signals` (`:1364`), `signal_message_links` (`:1412`) | migrations | **0** | residue — defer |
| `mgmt_*` family (`db.py:1039-1313`, 12 tables) | migrations | **0** | residue — defer |
| `order_protection` (migration 7) | migration 7 | writers `store.py:1335-1420` (called only from the F-06 inactive-under-config engine path); reader `pending_protection_verification` unreferenced | **config-conditional — see F-06, defer** |
| `control_setup.execution_mode CHECK ('approval','auto')` | migration 5 | constraint only | misleading CHECK constraint; harmless at runtime |

- **Why DEFER, not REMOVE:** absence of code references is not proof of no operational value. These tables may hold historical records (audit, retention), reporting dependencies outside this repo, or migration-history value. Dropping them is a **data migration** with a retention policy decision, not a code deletion.
- **Required evidence/inventory before any archival-or-drop migration:** (1) row counts per table in the production DB; (2) owner confirmation of retention policy (archive-then-drop vs keep); (3) confirmation no external reporting/backup tooling reads them; (4) a written note that `sqlite_sequence`/migration history keeps recording them if dropped (migrations are append-only); (5) a restore path if any table turns out to be needed. Phase 1 (now): document as legacy — this section. Phase 2 (later, owner-approved): one-off archival+drop migration per table group.
- **Risk:** medium (data loss if a hidden consumer exists). **Risk of not acting:** schema surface confuses every future audit; `execution_mode` CHECK still suggests an approval product that does not exist.

### F-13 — Misleading readiness-failure log — **SIMPLIFY (P1, log-only)**
- **Evidence:** `service.py:313-320` logs "MEXC account readiness could not be verified; **new entries will be blocked**" when `verify_mexc_readiness` fails — but `gate.py:63-65` is explicit that runtime readiness callbacks "never block entries… are not read by `check`" (owner policy). The actual behavior: the failure is recorded; entries proceed subject to the real gates.
- **Action:** correct the log text (and the comment in `service.py:_arm_trading_boundary`, which says "the submission gate stays closed" — the boundary is enforced per-message in `engine.process_item` (`engine.py:364-377`: boundary `None` ⇒ refuse), not by the gate; behavior is fail-closed either way, but the mechanism description is wrong).
- **Risk:** none. **Risk of not acting:** an operator reading logs during an incident will mis-predict gate behavior.

### F-14 — Metadata failure: optional bounded retry — **OWNER DECISION** *(reclassified from SIMPLIFY-DEFER; no default recommendation)*
- **Evidence:** §4.2. `SymbolRegistry.get` performs one live call on cache miss and propagates errors (`mexc/symbols.py:278-291`).
- **Action:** none by default. If the owner accepts fewer terminal rejections as a policy change: one immediate retry on `MexcNetworkError`/`MexcServerError` (not on deterministic `MexcSymbolError`) inside `SymbolRegistry.get` before failing; keep both checkpoints terminal afterward. Alternatively a short negative-result cooldown to protect the rate budget.
- **Risk:** low; changes rejection statistics (fewer terminal rejections); the fail-closed property is preserved because deterministic errors remain terminal and retries are read-only. **Risk of not acting:** transient 5 s exchange hiccup permanently discards a tradeable signal — the MYX incident class.

### F-15 — Global scope of the new-BUY block — **OWNER POLICY DECISION ONLY** *(restated in Rev 2: no recommendation)*
- **Evidence:** §5.3. All inputs are per-trade (`trade_manager.py:700-731`); the global scope is documented as deliberate (`trade_manager.py:737-756`, `store.py:set_buy_block` docstring).
- **Action:** **none. This audit makes no recommendation for or against narrowing the scope, and silence here must not be read as approval of any variant.** The existing behavior (global, entry-only, coverage-derived, never time-cleared) stands. If the owner independently chooses to review the policy, the review must include: the systemic-failure modes listed in §5.3 (fill-sync, fee provability, clock health, rate budget), the existing per-symbol protections (G4/G6/G7), the 7-day `allOrders` discovery window (`trade_manager.py:1763-1770`) after which discovery fails permanently with `oco_stop_child_history_expired`, and whether the owner-visible escalation notification for that exhaustion exists beyond the audit event.
- **Risk:** policy change to fund protection — outside this audit's authority.

### F-16 — Idempotency + evidence chain — **KEEP**
- **Evidence:** sha256 idempotency keys over `channel:message:action:symbol` (`engine.py` planning), `client_order_id` scheme "tsi"+28 (entry) and digest-based OCO ids (`trade_manager.py:4011-4019`), `exchange_operations` insert-before-send (`store.py:begin_exchange_operation`, used by `trading/evidence.py:144-233`), `order_status_history` on every transition, immutable domain events (migration 25 triggers). Complexity is the incident scar tissue that prevents duplicate orders and proves outcomes. No simplification proposed.

### F-17 — Centralized reconciliation gate — **KEEP**
- **Evidence:** `reconcile_intent_if_due` with durable per-intent backoff (`engine.py:2150+`; not_found 60 s→15 min cap, ambiguous 5 s→60 s, success coalesced; in-flight POST grace ≥ 15 s) replaced the historical pattern of per-tick REST queries — the trade-manager docstring records that the old design produced "≈57k checks per leg for a single stuck trade" (`trade_manager.py:875-884`). This is a prior, successful simplification; treat further removals in this area with the same care.

### F-18 — CLI surface — **SIMPLIFY (P3)**
- **Evidence:** `main.py` (1,821 LOC) exposes ~30 subcommands; `diagnostics audit|orders|reconcile|clock` (`main.py:cmd_diag_audit/orders/reconcile/clock`) each re-query the same store tables that `trading status|intents|signals|notifications|readiness` already cover.
- **Action:** fold the four diag commands into `trading status --verbose` (keep the JSON outputs for automation). Also remove the dead `stop_loss_trigger_offset_ticks` knob (config accepts only 0 and rejects anything else, `config.py:573-586` — a knob that cannot be turned is noise).
- **Risk:** low (CLI ergonomics only).

### F-19 — Exchange/clock/rate-limit machinery — **KEEP**
- **Evidence:** `ServerClock` refuses signed writes without a fresh low-RTT sample (`mexc/clock.py:292+`, `client.py:_timestamp_for(for_write=)`), `recvWindow` bounds enforced locally (`client.py:170-176`), typed error taxonomy maps −2011/−2013 by method context (`client.py:385-392`: a create-order −2011 is *uncertain*, a query −2011 is *not found* — subtle and correct), shared UID/IP/order gates across processes (`client.py:150-230`). All complexity is correctness-bearing.

### F-20 — Two-process deployment + independent dispatcher worker — **KEEP**
- **Evidence:** `deploy/tg-ingest.service` and `deploy/tg-notification-worker.service`; the dispatcher reads the trading DB read-only (`notification_dispatcher.py:1166`, `PRAGMA query_only=ON`) and owns a separate delivery ledger, so Telegram I/O can never hold a trading transaction. Deliberate, correct, cheap.

---

## 7. Authoritative Classification Table (Rev 2 — one row per unique finding)

**This table replaces all Rev 1 classification totals** (Rev 1's "20 findings, 8 KEEP, 8 SIMPLIFY, 10 REMOVE/DEFER" summed to 26 and double-counted findings across categories; several findings also appeared in multiple tables). Each finding has **exactly one primary class**; secondary tags are listed separately and are not counted in the totals.

| ID | Finding (short) | Primary class | Secondary tags |
|---|---|---|---|
| F-01 | OCO stop-child discovery subsystem | **KEEP** | — |
| F-02 | Duplicated per-symbol conflict logic | **SIMPLIFY** (merge) | pure-deletion |
| F-03 | Duplicated decimal/ISO helpers | **SIMPLIFY** (merge) | pure-deletion |
| F-04 | Two backoff-streak codecs | **SIMPLIFY** (merge) | — |
| F-05 | Disabled legacy exit machinery | **REMOVE** (Tier 1 only) | DEFER (Tier 2 legacy-state handling); behavior-equivalence refactor (Tier 3 presentation) |
| F-06 | Attached-protection path | **DEFER** | production-evidence-required |
| F-07 | `TradingNotifier` delivery half | **DEFER** | sequenced behind F-08 |
| F-08 | Dual notification ledgers | **DEFER** | staged consolidation; compat-data migration in phase 2; behavior change in phase 1 |
| F-09 | Notification volume reduction (decided); coalescing design open | **OWNER-DECISION** | design questions only (2026-11-17 owner decision) |
| F-10 | WebSocket feed | **DEFER** | production-evidence-required; documented owner deferral (N-03) |
| F-11 | `cleanup.py` inspection-only machinery | **SIMPLIFY** | — |
| F-12 | Approval/learning-era schema residue | **DEFER** | compat-data migration |
| F-13 | Misleading readiness log | **SIMPLIFY** | log-only |
| F-14 | Metadata bounded retry | **OWNER-DECISION** | small behavior change |
| F-15 | Global scope of new-BUY block | **OWNER-DECISION** | policy; no recommendation offered |
| F-16 | Idempotency + evidence chain | **KEEP** | — |
| F-17 | Centralized reconciliation gate | **KEEP** | — |
| F-18 | CLI surface | **SIMPLIFY** | — |
| F-19 | Exchange/clock/rate-limit machinery | **KEEP** | — |
| F-20 | Two-process deployment | **KEEP** | — |

**Totals (reconcile exactly with the 20 unique findings):** KEEP **5** · SIMPLIFY **6** · REMOVE **1** · DEFER **5** · OWNER-DECISION **3** → **20**.

Merge/split notes vs Rev 1: F-05 is split into Tier 1 (REMOVE) / Tier 2 (DEFER) / Tier 3 (presentation choice) — the row keeps the primary class of its largest actionable tier and tags the rest. F-07 was REV-1 "REMOVE-DEFER" and is now DEFER-primary because its deletion is sequenced behind the F-08 decision. F-14 moved from SIMPLIFY to OWNER-DECISION because it changes rejection statistics (a policy-visible behavior change), not just mechanics. No finding was merged or dropped; the count remains 20.

## 8. Classification: KEEP (justified complexity)

| Item | Where | Why it must stay |
|---|---|---|
| OCO stop-child discovery | `trade_manager.py:1272-2260` | MEXC API exposes no parent/child link; guessing misattributes fills (F-01) |
| Fail-closed sellable-quantity/fee provability | `trade_manager.py:2866-3058` | Prevents overselling; `None` ⇒ no submission |
| OCO invariant (reserved ≤ sellable) | `trade_manager.py:3022-3058` | §11.3 critical invariant; re-verified after every reconcile |
| Uncertain-never-retried semantics | `engine.py:1511-1526`, `trade_manager.py:108-124` | Duplicate-order protection; core incident scar tissue |
| Evidence-before-write chain | `trading/evidence.py`, `store.py:begin_exchange_operation` | Crash between send and response must remain provable |
| Atomic claim + gate precheck in one transaction | `store.py:claim_intent_for_submission`, `gate.py` | Pause/block cannot race submission |
| Activation boundary (forward-only, per-channel) | `engine.py:342-433`, `store.py` boundary methods | Downtime backlog must never replay as fresh orders |
| Server clock / recvWindow / rate gates | `mexc/clock.py`, `mexc/client.py` | Signed-write correctness; WAF/rate-limit compliance |
| Reconciliation single-gate + backoff | `engine.py:2150+` | Prevents query bursts (F-17) |
| Domain-event immutability triggers | `db.py` migration 25 | Audit integrity for the notification pipeline |
| Dual deployment units | `deploy/*` | I/O isolation (F-20) |
| Parser determinism (never guesses) | `trading/parser.py` | Ambiguous ⇒ rejected, not traded |
| Trade-layer notification router (critical→domain-only) | `trade_manager.py:3997-4030` | Guarantees CRITICAL protection alerts are immutable and alert-on-change (F-08) |

## 9. Classification: SIMPLIFY / MERGE

| ID | Item | Effort | Behavior change |
|---|---|---|---|
| F-02 | Deduplicate `CONFLICTING_TRADE_STATES`/`has_active_trade_for_symbol` | trivial | none |
| F-03 | Consolidate decimal/ISO helpers | trivial | none |
| F-04 | One backoff-streak codec | small | none (persisted format preserved) |
| F-11 | `cleanup.py` → inspection only | small | CLI output only |
| F-13 | Fix readiness log/comment | trivial | none |
| F-18 | Fold diag CLI commands; drop dead config knob | small | CLI ergonomics |

## 10. Classification: REMOVE / DEFER / OWNER-DECISION

| ID | Item | LOC basis | Prerequisite |
|---|---|---|---|
| F-05 | Tier-1 deletions, itemized | 162 pure-deletion candidates + 113 F-08-conditional (exact ranges, §12.1) | P0 characterization tests; test updates for the test-referenced items; the 113 LOC row follows the F-08 decision |
| F-05 | Tier-2 legacy-state handling | ≈ 27 + re-routing logic | Legacy-state inventory from production DB; written handling decision; parking-behavior characterization tests — **DEFER** |
| F-06 | Attached-protection path | ≈ 209 by range (config-conditional) | Runtime evidence (§12.3 list) — **DEFER** |
| F-07 | `TradingNotifier` delivery half | ≈ 111 (`notify.py:225-337`) | F-08 phase-2 decision — **DEFER** |
| F-08 | Ledger consolidation | n/a (migration design) | Owner approval; full mapping + compat case — **DEFER** |
| F-09 | Notification volume reduction (coalescing design) | n/a | Volume reduction decided by owner (2026-11-17); coalescing/suppression design owner-approved before implementation — **OWNER-DECISION** |
| F-10 | `MexcWebSocketFeed` | ≈ 233 (`monitor.py:267-499`) | Runtime evidence list (F-10) + updated N-03 — **DEFER** |
| F-12 | Legacy schema archival/drop | n/a (schema) | Row counts, retention policy, external-consumer check — **DEFER** |
| F-14 | Metadata bounded retry | small | Owner consent — **OWNER-DECISION** |
| F-15 | Global-block scope | n/a | **Owner policy decision — no recommendation offered** |
| — | `_legacy_severity` vocabulary | ≈ 26 (`:1219-1244`) | Only after F-08 phase 2 |
| — | `cancel_all_account_orders` kill switch | keep | Documented destructive switch; retain |

---

## 11. Fund-Protection Invariants — 13 rules (must not be weakened by any simplification)

1. Every BUY is preceded by a verified, ordered exit ladder and a working OCO path (`trade_manager.py:595-628`).
2. **Each fill tranche is protected by exactly two native OCO SELL orders (OCO 1 at TP1, OCO 2 at TP2), each OCO being one exchange order with a TP limit leg and an SL stop-limit leg; both pending order rows are persisted atomically before either exchange write; tranches are independent and their OCOs are never siblings** (`docs/trading.md:455-474`; `trade_store.py:create_trade_order_pair`, `trade_store.py:448+`; `trade_manager.py:_create_oco_tranche`, `trade_manager.py:2228+`, allocation policy `per_entry_fill_tranche` `:2325`). The position-level OCO count is therefore `2 × tranche count`, not a fixed two.
3. OCO reservation can never exceed sellable inventory; sellable is `None` (unknown) ⇒ no submission (`trade_manager.py:3022-3058`, `:2969-3002`).
4. Uncertain write outcomes are never followed by automatic resubmission with a different client id (`engine.py:1511-1526`; `trade_manager.py:108-124`).
5. Exchange evidence rows exist before the request (`store.py:begin_exchange_operation`).
6. Fills are monotonic; an exchange query never reduces recorded fills; a local `filled` is never un-filled (`trade_manager.py:_reconcile_single_order`, engine `_map_exchange_state`).
7. Entry remainders are cancelled once the acquired position exits (F-05 rule, `trade_manager.py:3139-3181`).
8. Terminal OCO parents require independently observed, terminal stop children before a trade may close (`trade_manager.py:1366-1412`).
9. The activation boundary is forward-only; backfill/recovery rows are never tradeable; an unarmed boundary refuses everything (`engine.py:364-377, 777-784`).
10. Idempotency: one intent per (channel, message, action, symbol) version; one trade per signal; retry-safe client ids.
11. Protection submissions are never blocked by the entry-only BUY block (`gate.py:139-144`) — simplifications must preserve this asymmetry.
12. Notifications never execute orders and never block trading; outbox-before-delivery is preserved (`notify.py`, `notification_dispatcher.py`).
13. Restart behavior: persisted tranches are reused (never shrunk or re-expanded); no new tranche is created while any submission is unresolved/UNKNOWN (`docs/trading.md:641-667`).

---

## 12. Dead & Legacy Code Register — itemized, auditable (Rev 2)

**Method (rev 2):** for every item, the exact line range at the audited commit, the reachability basis (unreachable = no production call site; unreferenced = zero non-definition references; inactive-under-config = live code path gated by a runtime flag that is currently false), and how references were checked (repo-wide grep over `tg_signal_ingest/` + `tests/`, definitions excluded). **Rev 1's aggregate "≈900–1,100 LOC provably dead" is withdrawn** — it mixed reachability classes and was not auditable. No LOC figure in this revision exceeds the sum of its itemized ranges, and no range is counted twice.

### 12.1 Tier 1 — provably production-unreachable: 162 LOC pure-deletion candidates + 113 LOC conditional on F-08 (275 itemized total)

| # | Location | Item | Range | Lines | Refs (non-def) | Basis |
|---|---|---|---|---|---|---|
| 1 | `trade_manager.py` | `_target_qty` | 2705-2707 | 3 | 0 | unreferenced |
| 2 | `trade_manager.py` | `_transition_state` | 1130-1176 | 47 | 1 (test) | production-unreachable |
| 3 | `trade_manager.py` | `_compute_available` | 1193-1204 | 12 | 2 (tests) | production-unreachable |
| 4 | `trade_manager.py` | `_enter_tp1` | 1101-1113 | 13 | 1 (test) | production-unreachable |
| 5 | `trade_manager.py` | `_submit_tp_order` | 1237-1245 | 9 | 1 (test) | production-unreachable; raises |
| 6 | `trade_manager.py` | `_enter_stop_loss` | 2799-2810 | 12 | 0 | unreferenced |
| 7 | `trade_manager.py` | `_resume_stop_loss` | 2812-2826 | 15 | 0 | unreferenced |
| 8 | `trade_manager.py` | `_submit_stop_loss_order` | 2828-2844 | 17 | 0 | unreferenced |
| 9 | `store.py` | `pending_protection_verification` | 1414-1420 | 7 | 0 | unreferenced |
| 10 | `store.py` | `clock_health_state` (fn only; constants `:35-37` stay) | 40-48 | 9 | 0 | unreferenced |
| 11 | `engine.py` | `submit_stop_loss_market_sell` | 1902-1919 | 18 | 4 (comments) | production-unreachable; raises |
| 12 | `notify.py` | delivery half (`flush`/`_report_ambiguous`/`_deliver`/`pending_count`) | 225-337 | 113 | 0 prod / 33 tests | production-unreachable — **removal sequenced behind F-08 (F-07)** |

**Σ itemized: 275 lines, in two groups with different deletion status — the total is NOT an immediately approved deletion amount:**
- **162 LOC — rows 1–11: pure-deletion candidates.** Deletable with zero runtime behavior change only after the P0 characterization freeze, per-item re-verification of the ranges at the working-tree HEAD, and the listed test updates. A candidate list, not an approval.
- **113 LOC — row 12 (`notify.py` delivery half): production-unreachable but NOT immediately deletable.** Its removal is conditional on the F-08 ledger decision (F-07) and is sequenced into P2/P3, not P1.

LOC arithmetic: 3+47+12+13+9+12+15+17+7+9+18 = 162; 162 + 113 = 275.

### 12.2 Tier 2 — live legacy-state handling (deletion = behavior change; NOT pure deletion)

| # | Location | Item | Range | Lines | Live caller |
|---|---|---|---|---|---|
| 1 | `trade_manager.py` | `_enter_tp2` | 1115-1128 | 14 | `_advance_trade_after_reconcile` `:3717` (legacy `tp1_active` + TP1 done) |
| 2 | `trade_manager.py` | `_do_tp_sale` | 1187-1191 | 5 | `_enter_tp1`/`_enter_tp2` |
| 3 | `trade_manager.py` | `_disable_legacy_exit` | 1178-1185 | 8 | `_do_tp_sale` |

**Σ: 27 lines** — these fail-closed into `needs_reconciliation` for legacy rows; **deletion changes compatibility behavior** for legacy DB rows during reconciliation, so this is not pure deletion (F-05 Tier 2 discussion).

### 12.3 Tier 3 — inactive-under-config (live code paths gated by a runtime flag; not proven dead, not provably deletable)

| # | Location | Item | Range (approx.) | Lines | Gate |
|---|---|---|---|---|---|
| 1 | `engine.py` | attached-protection region (`_protection_prices`, `_record_protection_intent`, `_verify_protection` + call sites) | 1298-1420 | ≈123 | `supports_attached_protection=False` (`mexc/hybrid.py:20`) |
| 2 | `store.py` | `order_protection` store methods (writers) | 1335-1420 | ≈86 | same gate via engine callers |

**Σ by range ≈ 209** — qualified, not a deletion claim; DEFER behind F-06's evidence list.

### 12.4 Deferred-range register (other)

| Location | Item | Range | Lines | Status |
|---|---|---|---|---|
| `monitor.py` | `MexcWebSocketFeed` | 267-499 | 233 | DEFER (F-10; N-03 documented decision) |
| `trading/cleanup.py` | cancel-eligibility machinery | 29-140 | ≈112 | SIMPLIFY (F-11, CLI-shape only) |
| `config.py` | `stop_loss_trigger_offset_ticks` (must be 0) | 573-586 | knob | SIMPLIFY (F-18) |
| `db.py` | legacy tables (F-12) | migrations 2, 5-17 | schema | DEFER (retention/migration decision) |
| `mexc/client.py` | `cancel_all_account_orders` | — | — | **KEEP** (documented destructive kill switch) |

### 12.5 Merge-simplification LOC (refactors, net reduction smaller than gross)

| ID | Item | Gross duplicated lines | Note |
|---|---|---|---|
| F-02 | `trade_store.py:411-440` duplicate conflict logic | ≈30 | delete duplicate |
| F-03 | decimal/ISO helper copies | ≈40 | consolidate |
| F-04 | second backoff codec `trade_manager.py:3621-3650` | ≈30 | one codec; persisted format unchanged |

**Bottom line (Rev 2 final):** provably production-unreachable, itemized = **275 LOC total — 162 pure-deletion candidates (after P0 + individual verification) + 113 conditional on the F-08 decision (not immediately deletable)**. Live legacy-state handlers = **27 LOC** (deletion changes compatibility behavior). Inactive-under-config = **≈209 LOC by range** (not proven dead; deferred). Deferred WebSocket implementation = **233 LOC** (F-10). Rev 1's "500–600 LOC P1" and "900–1,100 LOC provably dead" are **withdrawn as un-auditable**.

---

## 13. Change-Category Separation & Prioritized Plan (Rev 2)

**Five change categories.** Every action item below belongs to exactly one category; mixing categories in one change is prohibited.

- **A. Pure deletion / zero behavior change** — reachable proof exists; runtime behavior identical; tests updated only where they called the dead code directly.
- **B. Behavior-equivalence refactors** — code moves/merges; external behavior identical by construction (pure functions, preserved persisted formats); characterization tests pin equivalence.
- **C. Compat/data migrations** — schema or persisted-state changes (F-12 archival, F-08 phase 2); require retention policy, rollback path, and owner approval; never "just delete."
- **D. Production-evidence-required changes** — no change until a specific evidence list is satisfied at runtime (F-06, F-10; F-08 phase 2 soak); static analysis cannot settle them.
- **E. Owner policy decisions** — behavior/product choices no engineering review can authorize (F-09, F-14, F-15). This audit recommends nothing in this category.

**P0 — Prerequisite (no code changes): characterization-test freeze.**
Before any A/B change, pin the three incident paths with explicit tests (most infrastructure exists in the 72-file offline suite):
1. market ≥ trigger while < entry ⇒ direct LIMIT at entry (§4.1 path, `trade_manager.py:540-587`);
2. transient metadata failure ⇒ terminal reject + owner notification, deterministic symbol error ⇒ identical outcome (§4.2);
3. canceled-OCO-with-unattributed-child ⇒ trade parked, global block asserted with precise reason, discovery retried under bounded backoff, block clears when the child is attributed (§4.3).
Also pin §11 invariants 3, 4, 8, 11, 13 with direct assertions, **plus the OCO tranche model itself: two native OCOs per tranche, TP1/TP2 allocations of the tranche, atomic pair persistence, tranche independence** (invariant 2).
*Acceptance criteria:* all pinned tests green on the audited code state; each incident path has a named test; §11 invariant assertions exist and fail if the invariant is broken in isolation. Effort ~1–2 days. Risk: none.

**P1 — Category A + B, zero runtime behavior change.**
1. F-02 deduplicate conflict logic (A); 2. F-03 decimal helpers (B); 3. F-04 backoff codec, persisted format preserved (B); 4. F-05 Tier-1 rows 1–11 of §12.1 — the 162 LOC candidate set (A), with row 12 (113 LOC) and the Tier-2 rows **explicitly untouched** and Tier-3 presentation decided separately; 5. F-13 log/comment fix (A — log text only); 6. F-18's dead config knob (A) if done ahead of the CLI consolidation.
*Prerequisites:* P0 green; per-item test updates committed together; §12.1 ranges re-verified at the working-tree HEAD before each deletion (line numbers drift).
*Acceptance criteria:* full test suite green; grep proof at the new HEAD that each deleted symbol has zero references; diff contains no change outside the listed ranges and their tests; §11 invariants unaffected.

**P2 — Evidence- and decision-gated structural work (categories C/D, owner-informed).**
1. F-08 phase 1 (category D→B): eliminate the proven duplicate rejection messages by making each rejection flow single-ledger (extend the `emit_rejection_event=False` pattern or route rejections through one ledger), preserving the trade-layer CRITICAL router and owner-safety bypass verbatim. *Prerequisites:* the Ledger-A↔Ledger-B mapping table written and reviewed; owner approval of the message-text changes. *Acceptance criteria:* scripted replay of a rejection produces exactly one owner-visible message per business fact; CRITICAL routing unchanged (asserted by test); no historical row re-sent (cursors respected).
2. F-06 / F-10 evidence collection (category D, read-only): run the evidence lists in F-06 and F-10 (DB row counts, runtime WS test, latency comparison). *Acceptance criteria:* evidence recorded in the decision registry; no code change in this step.
3. F-11 `cleanup.py` slimming (category A/B, CLI output change — owner-informed).
4. F-14 metadata retry (category E→B) **only if** the owner approves the rejection-statistics change.
*Nothing in P2 removes the WebSocket feed, the attached-protection path, or any table.*

**P3 — Owner-visible / deferred (categories C/E).**
1. F-08 phase 2 ledger consolidation (category C) — only after phase-1 soak and the full compat case (backlog drain, outage recovery, operator requeue, delivery-history retention); completes or retires F-07 with it. 2. F-09 notification volume reduction (E): implement only the owner-approved coalescing/suppression design per the 2026-11-17 decision — which lifecycle events fold into summaries, which messages stay independent, exact rules preserving the owner-safety bypass, CRITICAL delivery, and per-recipient severity; no numerical target is set by this audit. 3. F-12 archival/drop migration per table group (C) after the §F-12 evidence inventory. 4. F-18 CLI consolidation (A/B, ergonomics). 5. F-15 (E) — owner policy review only; the audit offers no recommendation. 6. F-10 removal (D) — only if the F-10 evidence list concludes the feed is redundant; otherwise it stays deferred, documented in N-03.

**Explicitly not recommended for change:** the OCO discovery subsystem, the sellable/invariant math, the uncertain-outcome policy, the evidence chain, the gate/claim transaction pairing, the trade-layer notification router, and the activation/replay boundaries (F-01, F-16, F-17, F-19, F-20). Their complexity is load-bearing.

---

## 14. Unresolved Questions (owner/reviewer input required)

1. **F-08:** is the legacy formatted outbox a permanent product surface (some messages — e.g. the Persian recovery text at `engine.py:763-773` and decision formats — exist only there), or a bridge to be drained? This determines whether phase 2 is consolidation or cutover, and whether F-07's delivery half is deleted or kept.
2. **F-08:** which rejection flows' owner-visible text is authoritative — the formatted decision message or the rendered domain event? The proven duplicates (§F-08) cannot be resolved without this choice.
3. **F-05 Tier 2:** which legacy `trade_state` values actually exist in the production DB (static analysis can only enumerate what *can* reach `_advance_trade_after_reconcile`)? And is the current fail-closed parking the desired permanent handling for those rows?
4. **F-06:** does any backend roadmap rely on `supports_attached_protection = True`, and is `order_protection` empty in production?
5. **F-10:** does `MexcWebSocketFeed` connect and receive frames against `wbs.mexc.com/raw` at all (the raw-JSON framing vs MEXC's documented protocol is an unverified claim), and is sub-2 s trigger latency a requirement?
6. **F-12:** are the residue tables empty in production, and does any external reporting/backup tooling read them? What is the retention policy?
7. **F-15:** does the owner want to reconfirm the global-block policy at all? (No change is proposed; the question is only whether a review is ever scheduled.)
8. **F-09 (design, post-decision):** which lifecycle events fold into summaries, which messages require independent delivery, and what exact coalescing/suppression rules apply — under the owner's 2026-11-17 volume-reduction decision (whether to reduce volume is no longer open).

---

## 15. Audit Method (Rev 2)

1. **Static read of the full execution path** at the audited commit (`9ac46c0`): `docs/` (README, AGENTS, trading spec, architecture, data-model, DECISION_REGISTRY), `engine.py` (2,617 LOC, complete), `trade_manager.py` (4,044, complete), `store.py` (2,389, complete), `trade_store.py` (984, complete), `gate.py`, `risk.py`, `monitor.py`, `readiness.py`, `notify.py`, `domain_events.py`, `evidence.py`, `audit.py`, `balance.py`, `cleanup.py`, `parser.py`, `service.py`, `processing.py`, `notification_dispatcher.py` (1,488), `main.py`, `config.py`, `repository.py`, `db.py` (migrations 1–25), `mexc/client.py`, `mexc/hybrid.py`, `mexc/symbols.py`, `mexc/clock.py` (structure), `mexc/errors.py` (structure), `mexc/cli.py` (structure), `control/service.py` (structure), `control/store.py`, `locking.py`, `telemetry.py`, `runtime.py`, `deploy/*`.
2. **Call-site verification** for every dead-code/ledger claim via repo-wide grep (definitions excluded), covering both `tg_signal_ingest` and `tests/`; reachability basis stated per item in §12.
3. **Rev 2 re-verification pass:** every citation in the rewritten sections (F-05, F-06, F-07, F-08, F-10, F-12, F-15, §A.1, §12, dispatcher/notify/monitor ranges, OCO tranche model incl. `docs/trading.md:455-474/641-667/921`, `trade_manager.py` tranche machinery `:2174/:2218/:2228/:2325`, `trade_store.py:448+`) was re-read line-by-line at the audited code state. Citations in carried sections (F-01..F-04, F-09, F-11, F-13, F-14, F-16..F-18, §4-§5) were spot-checked where touched; stale dispatcher citations from Rev 1 (e.g. relay loops at `:947-971`/`:1086-1153`, suppression `:316-324`, owner bypass `:117-143`, query-only `:899-905`) were corrected to the verified locations (`:1197-1218`, `:1245-1316`, `:216-226`, `:138-165`, `:1166`), and the Rev 1 monitor ranges (`:236-456`, `:480-530`) were corrected to `:267-499` / `:583-634`.
4. **Incident replay against code:** each historical incident was traced to its current enforcement points; where history could not be reconstructed from the tree (the 26 h duration's original trigger), that limitation is stated rather than inferred.
5. **Spec conformance check:** every finding was evaluated against the authoritative behavior spec; anything that would alter entry/exit correctness or fund protection was excluded from actionable recommendations and routed to owner decisions (F-09, F-14, F-15).
6. **Constraints honored:** read-only audit; no orders placed/cancelled/tested; no `serve`/`auth login`/`trading verify`/`trading reconcile` executed; no migrations run; no services restarted; no interaction with MEXC or live orders; no secrets, DB contents, or code dumps in this report. **This document is an audit only: it authorizes nothing — no implementation, no deletion, no merge, no production deployment. Its recommendations all require separate review, tests, and owner approval before anything is acted upon; passing tests alone authorize nothing.**

---

## Appendix A.1 — The authoritative OCO model (corrected terminology, Rev 2)

Vocabulary, per `docs/trading.md` (L455-474, L641-667, L921) and the implementation:

- **Position:** one trade's aggregate acquired base quantity.
- **Fill tranche:** a durable grouping of entry fills (`allocation_policy = "per_entry_fill_tranche"`, `trade_manager.py:2325`; tranche id persisted in `trade_orders.oco_group_id`, `trade_manager.py:2218-2228`). A tranche is created when incremental acquired quantity is confirmed and its fill history is synchronized — including from the acknowledgement callback of an already-partial entry while the entry remainder is still working. Fills discovered together in one fresh reconciliation snapshot may be combined into one tranche (`docs/trading.md:470-472`).
- **Native OCO order:** one MEXC OCO SELL order (`spot.trade.place-oco`) with **two linked legs**: a TP limit leg and an SL stop-limit leg (trigger and execution price both = configured SL). When one leg activates, the exchange cancels only its own linked sibling.
- **Per tranche exactly two native OCO orders:** OCO 1 = `tp1_allocation_pct` (default 50%) **of the tranche** at TP1; OCO 2 = the tranche remainder at TP2 (`trade_manager.py:_create_oco_tranche`, `:2228+`; `_place_oco_exits`, `:2265+`; engine `submit_oco_sell`/`_submit_oco_sell_locked`, `engine.py:1816-1900`, which refuses inverted exits and sets SL legs to the configured SL). Both pending order rows are written atomically before either submission (`trade_store.py:create_trade_order_pair`, `:448+`).
- **Consequences:** a position's native-OCO count is `2 × tranche count`, not a fixed two; OCOs of different tranches are never siblings; the bot never edits or cancels/replaces an active OCO; restarts reuse persisted tranches (never shrink or re-expand) and create no new tranche while a submission is unresolved/UNKNOWN (`docs/trading.md:641-667`); the aggregate `oco_groups` record remains one per trade.

Every statement in this audit that mentions OCO counts uses this per-tranche model. Rev 1's "exactly 2 protective OCOs per position" (Rev 1 exec summary, Rev 1 §2.3, Rev 1 §10.2 — Rev 1's own section numbering) was incorrect and is superseded by §11 invariant 2.
