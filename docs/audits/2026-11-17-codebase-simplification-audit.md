# Codebase Simplification Audit — Tele-MEXC

- **Date:** 2026-11-17
- **Repo:** `20kevit/tele-mexc`, branch `arena/58c00512-tele-mexc`, HEAD `9ac46c0` (clean tree)
- **Scope:** read-only audit of `tg_signal_ingest` (~29,000 LOC) + `tests/` (72 files, ~25,000 LOC, fully mocked/offline). No code, config, DB, service, or deployment changes were made; no exchange calls were performed.
- **Goal:** eliminate unnecessary complexity while preserving order correctness and fund protection, verified against the product spec (BUY signal → entry order → exactly 2 protective OCOs → monitoring → minimal notifications).
- **Evidence convention:** every finding cites `file:line` at HEAD `9ac46c0`. Confidence levels: **high** (read directly in this audit), **medium** (inferred from cross-references), **low** (historical, not reproducible in this tree). Historical incidents are explicitly separated from verified current behavior.

---

## 1. Executive Summary

The codebase is **complex for defensible reasons, plus a meaningful layer of dead scar tissue**. The core trading path (parser → risk → intent → submission gate → exchange evidence → reconciliation → TradeManager OCO lifecycle) is deliberately fail-closed and evidence-first; most of its size exists because MEXC's native OCO API exposes no parent/child link for stop children and because several real incidents (duplicate submissions, unprotected positions, global BUY freezes) are encoded as permanent invariants.

Key numbers:

| Metric | Value |
|---|---|
| Findings catalogued | 20 (F-01 … F-20) |
| KEEP (justified complexity) | 8 findings, majority of LOC |
| SIMPLIFY / MERGE | 8 findings |
| REMOVE / DEFER | 10 findings (several overlap) |
| Provably dead code identified | ≈ 900–1,100 LOC (dead methods, unreachable bodies, unused store methods, disabled-path helpers) |
| Deletable with **zero** behavior change (P1) | ≈ 500–600 LOC |
| Deletable with staged, low-risk refactors (P2) | ≈ 900–1,100 LOC + one duplicated notification ledger |
| Schema/CLI simplifications (P3) | approval-era tables, CLI diag consolidation, WebSocket feed |

**Top five findings:**

1. **Every notification is persisted twice and relayed twice** (F-08): domain events (in-transaction, relayed by `NotificationDispatcher.relay_once`) *and* a legacy `trading_notifications` outbox row (relayed by `relay_legacy_once`). Two independent delivery ledgers exist for the same facts. This is the largest architectural simplification available.
2. **≈300 LOC of disabled legacy exit machinery** in `TradeManager` (`_enter_tp1/_enter_tp2/_do_tp_sale/_submit_tp_order/_enter_stop_loss/_resume_stop_loss/_submit_stop_loss_order/...`), several containing unreachable code after unconditional `return` (F-05), plus the dead attached-protection path in the engine (F-06).
3. **The three historical incidents are correctly encoded in current code** (verified, §4): the entry edge case is *fixed*, the MYX metadata failure remains terminal *by design* (with an optional bounded-retry improvement, F-14), and the APE-style global BUY freeze is now coverage-based and instrumented instead of time-based — but it is still global in scope, which is a policy choice to reconfirm (F-15).
4. **Duplicated per-symbol conflict logic and decimal/backoff helpers** across `store.py`/`trade_store.py`/`trade_manager.py` (F-02, F-03, F-04) — pure maintenance risk, trivial merges.
5. **A hand-rolled WebSocket client (~230 LOC) that has never been exercised against the real exchange** runs in parallel with the always-on polling feed, adding at best duplicate ticks (F-10).

**Guardrails:** §10 lists the fund-protection invariants that must not be weakened by any simplification. Nothing in the P0–P3 plan touches them; P0 itself is a test-freeze step, not a code change.

---

## 2. Product Spec Baseline (authoritative contract)

The audit measured every finding against the behavior documented in `README.md`, `AGENTS.md`, and `docs/trading.md` (the authoritative behavior spec), cross-checked against `docs/architecture.md` and `docs/data-model.md`. The contract that constrains simplification:

1. Only Spot **BUY** signals execute; SHORT/SELL are parsed but refused before any REST call (`engine.py:580`, `parser.py` `_action`).
2. Entry is a **LIMIT** order at the signal entry price (never converted to market; `risk.py`, `engine.py`). Conditional entry waits for an upward crossing of `entry − offset·tick_size` (offset default 3 ticks, `config.py:137-141`).
3. Exactly **two protective native OCO orders** per position (TP1 50% of acquired, TP2 100% of remainder), stop-loss trigger = configured SL price (`trade_manager.py:_place_oco_exits`, `config.py` TP percentages).
4. A BUY may only be submitted when a complete ordered exit ladder (SL < Entry < TP1 ≤ TP2) and a working OCO path exist (`trade_manager.py:595-628 _pre_buy_exit_problem`).
5. Uncertain exchange outcomes are **never retried blindly**; they park as `unknown_requires_reconciliation` and are reconciled read-only (`engine.py:1490-1530`, `trade_manager.py:_UNCERTAIN_ERRORS`).
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
   ├─ OCO tranche placement: 2 × native OCO per fill tranche,
   │   invariant checks (reserved ≤ sellable, sellable fail-closed)
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
- **Verified current behavior (high confidence):** **fixed.** `_create_trade` computes the trigger and then explicitly handles this case: "The initial reference price may already be at/above the computed trigger while still below Entry. Treat that as an already-fired trigger and submit the normal LIMIT at Entry immediately" — `trade_manager.py:566-583`. When `current >= trigger_price`, `entry_mode` stays `direct` and `trigger_price` is nulled, so the LIMIT at Entry is submitted immediately. Only `current < trigger_price` selects the conditional path (`trade_manager.py:584-587`, evaluated per tick at `trade_manager.py:918-931 _check_entry_trigger`).
- **Residual risk:** none identified. A missing/invalid offset fails closed instead of silently choosing a path (`trade_manager.py:507-516`).

### 4.2 MYX metadata failure → terminal rejection

- **History (reported):** a transient symbol-metadata failure caused the signal to be rejected terminally even though a later retry would have succeeded.
- **Verified current behavior (high confidence):** the failure is still **terminal at two checkpoints**:
  1. Planning: `_process_item` rejects the signal with "symbol metadata unavailable; refusing to trade without exchange constraints" (`engine.py:489-497`) — a terminal `rejected` state.
  2. Pre-submit: `_execute_intent` re-validates and `_reject_before_submit` produces "pre-submit revalidation failed: symbol metadata is unavailable" (`engine.py:1164-1176`, revalidation near `engine.py:1425-1435`).
- **Mechanism (high confidence):** `SymbolRegistry.get` (`mexc/symbols.py:278-291`) serves a 300 s TTL cache; on miss it performs a live `GET /api/v3/exchangeInfo` and **propagates any error** (network, WAF, 5xx). Failed lookups are not negatively cached, so a *subsequent* signal retries the fetch — but the signal that hit the transient failure is lost.
- **Classification:** intentional fail-closed behavior (trading without exchange constraints is prohibited). It is safe (no position can become unprotected from a metadata failure because no BUY is ever placed), but it sacrifices availability on transient failures. Improvement candidate F-14 (bounded retry, P2) — not a correctness fix, an availability tweak requiring owner consent because it changes rejection statistics.

### 4.3 APE OCO-cancel → 26 h global new-BUY block on unrelated symbols

- **History (reported):** an OCO cancellation on one symbol froze new BUYs across all symbols for ~26 hours.
- **What the current code guarantees (high confidence):**
  - There is **no time-based and no historical-state global block**. The gate reads a single durable flag (`gate.py:123-140 _buy_block_reason`); the comment at `gate.py:155-159` states: "There is intentionally no account-wide/history-wide entry lock here. Historical trade reconciliation state is per-order/per-trade and must never disable unrelated symbols." The historical global-state blocking was explicitly removed (`engine.py:1425-1427`).
  - The flag is recomputed from **verified exit coverage**: `_refresh_buy_block` (`trade_manager.py:733-790`) lists non-terminal trades, computes `_is_unprotected_critical` (acquired > 0, remaining > 0, verified OCO coverage < remaining; `trade_manager.py:700-731`), and persists the block with a precise reason (`unprotected position(s): SYMBOL:trade_id…`) plus audit events `new_buys_blocked` / `new_buys_unblocked`. It is refreshed after every reconciliation sweep (`trade_manager.py:3302-3308`), after every OCO placement attempt, and around startup recovery.
  - A **canceled OCO parent whose stop-child execution is not independently attributed** parks the trade in `needs_reconciliation` (`trade_manager.py:1560-1591 _unattributed_oco_parent_cancellations`; handling at `trade_manager.py:3430-3455`) and therefore keeps coverage unverified → the block stays. Clearing requires the stop child to be uniquely matched in `allOrders` (exact field match, no id arithmetic, `trade_manager.py:1693-1758 _select_oco_stop_child`) and its status queried by exact id — with bounded discovery backoff (15-min cap, `trade_manager.py:1631-1665`) instead of hot loops.
- **Honest assessment:** the *mechanism* that made a 26 h freeze possible — "block until protection is proven, however long that takes" — is **still present by design** (`trade_manager.py:737-756`: "The block is never cleared by time. It clears only when every open bot-owned position is fully sold or has exchange-confirmed working coverage"). What changed versus the incident era: the reason is precise and audited, discovery is bounded and evidence-based, and readiness reporting marks the block as diagnostic context (`readiness.py:356-360`). Whether the historical 26 h came from the removed global-history blocking or from prolonged discovery failure cannot be established from this tree (**low confidence** on the original root cause; **high confidence** on current behavior). The *global scope* of the block is a policy question, not a bug — see §5.3 and F-15.

---

## 5. Global Entry Gates — inventory and the "why global" answers

All gates verified at HEAD. For each: what it blocks, where it lives, and the required answer to *"why must this block every new BUY instead of only the affected trade or symbol?"*

| # | Gate | Scope | Blocks | Verified at |
|---|---|---|---|---|
| G1 | Emergency stop | global | everything (entry + protection) | `gate.py:146-148`, `config.py` `emergency_stop` |
| G2 | Owner pause | global | everything (entry + protection) | `gate.py:149-153` (`control_setup.paused`), atomic with the planned→submitting claim (`store.py:claim_intent_for_submission`) |
| G3 | Durable new-BUY block | global, **entry purpose only** | new BUYs only; protection submissions pass | `gate.py:154-162` + `store.py:set_buy_block/buy_block_reason`; source of truth recomputed from coverage (`trade_manager.py:733-790`) |
| G4 | Same-symbol open-order check | **per-symbol** (not global) | a second BUY on the same symbol while an exchange order is open | `engine.py:191-225`, applied at `engine.py:1462-1480` under a per-symbol lock (`engine.py:163`, `locking.py KeyedRLockPool`) |
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

**Simplification option (deferred, F-15):** all block inputs are per-trade; a per-symbol block is technically feasible. It is a **policy reversal** of an owner-confirmed protection rule, so this audit classifies it as DEFER requiring explicit owner sign-off, with a supporting argument: same-symbol overlap is already prevented by G4/G6, and balance overspend by G7; the residual risk a per-symbol policy accepts is "a new unrelated BUY inherits exposure while global machinery (fee/fill/clock) is in an unproven state."

---

## 6. Detailed Findings

Each finding: evidence, classification (KEEP / SIMPLIFY-MERGE / REMOVE-DEFER), risk of acting, risk of not acting. Confidence: high unless stated.

### F-01 — Native-OCO stop-child discovery subsystem is forced complexity — **KEEP**
- **Evidence:** `trade_manager.py:1272-2260` (≈ 1,000 LOC): `_oco_leg_targets`, `_sync_oco_legs`, `_oco_stop_children_settled`, `_discover_oco_stop_children`, `_select_oco_stop_child`, `_query_and_persist_oco_stop_child`. Root cause documented in-code (`trade_manager.py:112-127`): MEXC returns OCO children as separate `allOrders` rows with `orderListId = -1`, null client ids, and no exposed trigger; `mexc/client.py:all_orders` docstring repeats the constraint. Exact-match-only matching with a hard fail-closed path (`trade_manager.py:1705-1758`), bounded backoff (`trade_manager.py:1631-1665`), and a 7-day history window guard.
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

### F-05 — Disabled legacy exit machinery in TradeManager — **REMOVE (P1 for provably dead, P2 for call-site surgery)**
- **Evidence (high confidence, call-site greps included tests):**
  - `_enter_tp1` (`trade_manager.py:1101`), `_enter_tp2` (`:1115`), `_do_tp_sale` (`:1187`): first statement disables and `return`s; the remaining bodies are **unreachable**.
  - `_submit_tp_order` (`:1237`): unconditional raise.
  - `_disable_legacy_exit` (`:1178`): reachable only from the above.
  - `_target_qty`: **zero references** outside its definition.
  - `_compute_available` (`:1193`): referenced only from the disabled path.
  - `_transition_state` (`:1130`): one reference, from `_do_tp_sale`'s unreachable body.
  - `_enter_stop_loss` (`:2799`), `_resume_stop_loss` (`:2812`), `_submit_stop_loss_order` (`:2828`): explicitly disabled per §7 (native OCO SL legs only); they exist to *prove* no Market Sell path exists. `engine.submit_stop_loss_market_sell` (`engine.py:1902`) intentionally raises `RuntimeError`.
- **Action:** delete the unreachable bodies and the zero-reference methods; keep the §7 *guard* semantics by leaving the "legacy state ⇒ needs_reconciliation parking" in `_reconcile_trade_inner` (`trade_manager.py:3319-3337`, `:3384-3394`) so old DB rows are still handled safely. Keep one tiny `_enter_stop_loss`-style guard or an assertion test proving no Market Sell call site exists (the current grep-proof ritual).
- **Risk:** low-medium: `_advance_trade_after_reconcile` (`:3666+`) calls `_enter_tp2` for legacy `tp1_active`/`tp2_active` states — those branches must be re-routed to the existing needs_reconciliation parking rather than deleted outright. Requires the P0 characterization tests first. **Risk of not acting:** readers must mentally execute dead code to conclude the strategy; the disabled bodies are a standing temptation and a maintenance trap.

### F-06 — Attached-protection path is dead in production — **REMOVE-DEFER (P2)**
- **Evidence:** `HybridMexcClient.supports_attached_protection = False` (`mexc/hybrid.py:20`); engine attached-protection code (`engine.py:1298-1375 _protection_prices/_record_protection_intent/_verify_protection`, call sites `engine.py:1476, 1481, 1590, 2334`) therefore never executes on the only production backend (docs/trading.md confirms the backend has no attached TP/SL). The `order_protection` table (migration 7) and store methods exist for it; `pending_protection_verification` has **zero references**; `trade_manager._exchange_protection_active` (`trade_manager.py:934-955`) reads a table that is never written for live intents.
- **Action:** delete the engine path and unused store method; **keep the DB table and historical rows**; keep `_exchange_protection_active` or simplify it with a comment (it is harmless defense if a future backend sets the flag).
- **Risk:** low; must verify no CLI write backend roadmap depends on the flag (medium confidence on future plans). **Risk of not acting:** two protection models coexist in the reader's mind.

### F-07 — `TradingNotifier` delivery half is unreachable — **REMOVE-DEFER (P2)**
- **Evidence:** `notify.py:flush/_deliver` (~150 LOC of Bot API delivery, claim, ambiguity recovery) exist, but the producer path intentionally never flushes (`notify.py:138-146`: "The synchronous `flush()` is intentionally removed from the producer path"), and `service.py`/`main.py` deliver exclusively through `NotificationDispatcher` (`main.py:cmd_notifications_worker`). No production caller of `flush` (grep: tests only).
- **Action:** delete the delivery half; keep `enqueue` (the legacy outbox is still a relay *source*, see F-08 until cutover completes).
- **Risk:** low. **Risk of not acting:** two delivery implementations invite accidental reuse.

### F-08 — Dual notification ledgers (domain events + legacy outbox) — **SIMPLIFY-MERGE (P2, staged) — highest-value simplification**
- **Evidence:** every lifecycle fact is written twice:
  1. **Domain event** into `audit_events` (immutable, migration 25 triggers) inside the business transaction — `store.py:transition_intent` / `trade_store.py:update_trade_state` / `transition_trade_order` / `upsert_oco_leg` / `set_oco_group_state` all call `record_domain_event` with `conn=`.
  2. **Legacy outbox row** into `trading_notifications` via `TradingNotifier.notify/notify_trade/notify_intent` (`notify.py:147-186`) from engine/trade-manager call sites (e.g. `trade_manager.py:_notify_intent`, `_notify_trade` non-critical branch, `_record_error`).
  The dispatcher then runs **two** relay loops against two cursors: `relay_once` (domain events, `notification_dispatcher.py:947-971`) and `relay_legacy_once` (legacy rows, `:1086-1153`), each producing deliveries in its own ledger. Dedupe exists per row (`UNIQUE(event_id, recipient_role, part_no)`), but the *same fact* legitimately produces two deliveries under two ids.
- **Action (staged):** Phase 1 — stop writing legacy rows for events that already have a domain-event representation (the producers exist); keep `relay_legacy_once` for pre-cutover rows. Phase 2 — after drain, delete the legacy relay and the outbox write path (F-07 completes).
- **Risk:** medium (notification behavior change; must map every legacy `event_key` to a domain event first — the taxonomy is already frozen in `domain_events.py:EVENT_REGISTRY`, and the dispatcher's category map covers all current types, `notification_dispatcher.py:42-58`). **Risk of not acting:** permanent double persistence, double relay logic, and a second vocabulary (`_legacy_severity`, `:1031-1063`) that must be maintained in parallel forever.

### F-09 — Notification volume vs "minimal notifications" spec — **SIMPLIFY-DEFER (P3, owner-visible)**
- **Evidence (high confidence, from producer enumeration):** one full trade emits ≈ 15–30 Telegram messages across recipients: `signal.received`, `intent.created`, every `intent.state_changed`, every `trade.state_changed`, per-leg `trade_order.state_changed`, `oco_group.state_changed`, per-leg `oco_leg.state_changed`, **per fill** `fill.recorded`, **per exchange write** `exchange.write_outcome`, and `reconciliation.outcome` on every change. Exactly one suppression rule exists (routine `active↔submitting` group flip, `notification_dispatcher.py:316-324`); threading (`event_thread_key`, `:390-424`) groups replies but does not reduce count.
- **Action:** owner-approved coalescing (e.g. fold `fill.recorded` into intent summaries; suppress INFO group/leg state flips that carry no safety payload — the owner-safety bypass at `:117-143` already guarantees CRITICAL/uncertain events always arrive).
- **Risk:** owner-visible behavior change → requires explicit consent. **Risk of not acting:** alert fatigue; genuinely critical messages lose signal value in noise (this is the product's stated reason for the spec's "minimal notifications").

### F-10 — Unexercised hand-rolled WebSocket feed — **REMOVE-DEFER (P3)**
- **Evidence:** `monitor.py:236-456` implements a from-scratch WebSocket client (handshake, frame parser, ping/pong) for `wss://wbs.mexc.com/raw`; its own docstring says "behaviour against the real exchange has not been exercised in this tree". `_PrimaryWithFallback` (`:480-530`) subscribes the callback to **both** feeds and starts the polling fallback immediately — so a working WS only adds duplicate ticks (the polling feed never stops). Trigger latency requirement is 2 s polling (`config.py monitor_poll_interval_s`), which the spec allows.
- **Action:** delete `MexcWebSocketFeed` and the composite; run `PollingPriceFeed` alone. Keep `_is_stale` gating.
- **Risk:** low (loses a best-effort latency improvement that has never been validated). **Risk of not acting:** ~230 LOC of unexercised network code is a latent-failure surface (frame parsing bugs surface only if the WS ever connects in production).

### F-11 — `cleanup.py` inspection-only machinery — **SIMPLIFY (P2)**
- **Evidence:** `trading/cleanup.py:212-219`: "inspection only: automatic order cancellation is permanently disabled… (apply requested but ignored)". The ~180 LOC of `CleanupCandidate` eligibility/threshold logic (`:29-140`) feeds a report that never cancels anything; the `apply` flag is ignored by design.
- **Action:** reduce to open-order inspection + audit (or fold into a `diagnostics` command); delete eligibility/apply machinery.
- **Risk:** low (CLI output changes shape only). **Risk of not acting:** the module's name and `apply` parameter suggest capabilities it deliberately lacks.

### F-12 — Approval/learning-era schema residue — **REMOVE-DEFER (P3, needs migration)**
- **Evidence:** `db.py` migrations create and retain tables outside the live execution path: `control_setup.execution_mode CHECK ('approval','auto')` (migration 5) — the approval mode was removed from the product; `control_flow`, `control_learning_samples`, `control_approvals`, `control_updates`, `control_secrets`, `control_audit`, `control_notices`; `signal_candidates`, `signal_candidate_reviews`, `analysis_runs`, `analysis_failures`, `channel_signals`, `signal_message_links`; the `mgmt_*` family (state/auth_flow/updates/callbacks/snapshots/ingestions/audit/operator flows/import requests/review drafts); `trading_daily_spend` (migration 2) — the daily cap is an explicitly rejected config knob (`config.py` obsolete list includes `daily_max_orders`).
- **Action:** Phase 1: document as legacy (this report). Phase 2: a one-off archival+drop migration, only if no reporting depends on them (medium confidence: none of these tables appears in `store.py`/`engine.py`/`trade_manager.py` hot paths; `control/service.py` uses `control_setup`/`control_settings` only — medium confidence for the `mgmt_*` family, used by older operator tooling).
- **Risk:** medium (migration + historical data retention policy). **Risk of not acting:** schema surface confuses every future audit; `execution_mode` CHECK still suggests an approval product that does not exist.

### F-13 — Misleading readiness-failure log — **SIMPLIFY (P1, log-only)**
- **Evidence:** `service.py:313-320` logs "MEXC account readiness could not be verified; **new entries will be blocked**" when `verify_mexc_readiness` fails — but `gate.py:63-65` is explicit that runtime readiness callbacks "never block entries… are not read by `check`" (owner policy). The actual behavior: the failure is recorded; entries proceed subject to the real gates.
- **Action:** correct the log text (and the comment in `service.py:_arm_trading_boundary`, which says "the submission gate stays closed" — the boundary is enforced per-message in `engine.process_item` (`engine.py:364-377`: boundary `None` ⇒ refuse), not by the gate; behavior is fail-closed either way, but the mechanism description is wrong).
- **Risk:** none. **Risk of not acting:** an operator reading logs during an incident will mis-predict gate behavior.

### F-14 — Metadata failure: optional bounded retry — **SIMPLIFY-DEFER (P2, small behavior change)**
- **Evidence:** §4.2. `SymbolRegistry.get` performs one live call on cache miss and propagates errors (`mexc/symbols.py:278-291`).
- **Action (optional):** one immediate retry on `MexcNetworkError`/`MexcServerError` (not on deterministic `MexcSymbolError`) inside `SymbolRegistry.get` before failing; keep both checkpoints terminal afterward. Alternatively a short negative-result cooldown to protect the rate budget.
- **Risk:** low; changes rejection statistics (fewer terminal rejections); the fail-closed property is preserved because deterministic errors remain terminal and retries are read-only. **Risk of not acting:** transient 5 s exchange hiccup permanently discards a tradeable signal — the MYX incident class.

### F-15 — Global scope of the new-BUY block — **DEFER (owner decision)**
- **Evidence:** §5.3. All inputs are per-trade (`trade_manager.py:700-731`); the global scope is documented as deliberate (`trade_manager.py:737-756`, `store.py:set_buy_block` docstring).
- **Action:** none by default. If the owner reconfirms policy, a per-symbol variant is implementable with the existing inputs plus a systemic-failure override (unreadable store state ⇒ global block, already the behavior at `trade_manager.py:651-663`).
- **Risk:** policy change to fund protection — outside this audit's authority. **Risk of not acting:** unrelated symbols stay frozen while a single trade's discovery is stuck (bounded at the 7-day `allOrders` window, after which discovery fails permanently with `oco_stop_child_history_expired`, `trade_manager.py:1763-1770` — worth an owner-visible escalation notification, which currently exists only as an audit event).

### F-16 — Idempotency + evidence chain — **KEEP**
- **Evidence:** sha256 idempotency keys over `channel:message:action:symbol` (`engine.py` planning), `client_order_id` scheme "tsi"+28 (entry) and digest-based OCO ids (`trade_manager.py:4011-4019`), `exchange_operations` insert-before-send (`store.py:begin_exchange_operation`, used by `trading/evidence.py:144-233`), `order_status_history` on every transition, immutable domain events (migration 25 triggers, `db.py:_migration_25_domain_event_envelope`). Complexity is the incident scar tissue that prevents duplicate orders and proves outcomes. No simplification proposed.

### F-17 — Centralized reconciliation gate — **KEEP**
- **Evidence:** `reconcile_intent_if_due` with durable per-intent backoff (`engine.py:2150+`; not_found 60 s→15 min cap, ambiguous 5 s→60 s, success coalesced; in-flight POST grace ≥ 15 s) replaced the historical pattern of per-tick REST queries — the trade-manager docstring records that the old design produced "≈57k checks per leg for a single stuck trade" (`trade_manager.py:875-884`). This is a prior, successful simplification; treat further removals in this area with the same care.

### F-18 — CLI surface — **SIMPLIFY (P3)**
- **Evidence:** `main.py` (1,821 LOC) exposes ~30 subcommands; `diagnostics audit|orders|reconcile|clock` (`main.py:cmd_diag_audit/orders/reconcile/clock`) each re-query the same store tables that `trading status|intents|signals|notifications|readiness` already cover.
- **Action:** fold the four diag commands into `trading status --verbose` (keep the JSON outputs for automation). Also remove the dead `stop_loss_trigger_offset_ticks` knob (config accepts only 0 and rejects anything else, `config.py:573-586` — a knob that cannot be turned is noise).
- **Risk:** low (CLI ergonomics only).

### F-19 — Exchange/clock/rate-limit machinery — **KEEP**
- **Evidence:** `ServerClock` refuses signed writes without a fresh low-RTT sample (`mexc/clock.py:292+`, `client.py:_timestamp_for(for_write=)`), `recvWindow` bounds enforced locally (`client.py:170-176`), typed error taxonomy maps −2011/−2013 by method context (`client.py:385-392`: a create-order −2011 is *uncertain*, a query −2011 is *not found* — subtle and correct), shared UID/IP/order gates across processes (`client.py:150-230`). All complexity is correctness-bearing.

### F-20 — Two-process deployment + independent dispatcher worker — **KEEP**
- **Evidence:** `deploy/tg-ingest.service` and `deploy/tg-notification-worker.service`; the dispatcher reads the trading DB read-only (`notification_dispatcher.py:899-905`, `PRAGMA query_only=ON`) and owns a separate delivery ledger, so Telegram I/O can never hold a trading transaction. Deliberate, correct, cheap.

---

## 7. Classification: KEEP (justified complexity)

| Item | Where | Why it must stay |
|---|---|---|
| OCO stop-child discovery | `trade_manager.py:1272-2260` | MEXC API exposes no parent/child link; guessing misattributes fills (F-01) |
| Fail-closed sellable-quantity/fee provability | `trade_manager.py:2866-3002` | Prevents overselling; `None` ⇒ no submission |
| OCO invariant (reserved ≤ sellable) | `trade_manager.py:3022-3058` | §5.1 critical invariant; re-verified after every reconcile |
| Uncertain-never-retried semantics | `engine.py:1490-1530`, `trade_manager.py:108-124` | Duplicate-order protection; core incident scar tissue |
| Evidence-before-write chain | `trading/evidence.py`, `store.py:begin_exchange_operation` | Crash between send and response must remain provable |
| Atomic claim + gate precheck in one transaction | `store.py:claim_intent_for_submission`, `gate.py` | Pause/block cannot race submission |
| Activation boundary (forward-only, per-channel) | `engine.py:342-433`, `store.py` boundary methods | Downtime backlog must never replay as fresh orders |
| Server clock / recvWindow / rate gates | `mexc/clock.py`, `mexc/client.py` | Signed-write correctness; WAF/rate-limit compliance |
| Reconciliation single-gate + backoff | `engine.py:2150+` | Prevents query bursts (F-17) |
| Domain-event immutability triggers | `db.py` migration 25 | Audit integrity for the notification pipeline |
| Dual deployment units | `deploy/*` | I/O isolation (F-20) |
| Parser determinism (never guesses) | `trading/parser.py` | Ambiguous ⇒ rejected, not traded |

## 8. Classification: SIMPLIFY / MERGE

| ID | Item | Effort | Behavior change |
|---|---|---|---|
| F-02 | Deduplicate `CONFLICTING_TRADE_STATES`/`has_active_trade_for_symbol` | trivial | none |
| F-03 | Consolidate decimal/ISO helpers | trivial | none |
| F-04 | One backoff-streak codec | small | none (format preserved) |
| F-08 | Single notification ledger (staged) | medium | delivery-neutral after mapping |
| F-11 | `cleanup.py` → inspection only | small | CLI output only |
| F-13 | Fix readiness log/comment | trivial | none |
| F-18 | Fold diag CLI commands; drop dead config knob | small | CLI ergonomics |
| F-14 | Bounded metadata retry (optional) | small | fewer terminal rejections (owner consent) |

## 9. Classification: REMOVE / DEFER

| ID | Item | LOC | Prerequisite |
|---|---|---|---|
| F-05 | Dead exit stubs + unreachable bodies (`_target_qty`, `_compute_available`, `_transition_state`, bodies of `_enter_tp1/_enter_tp2/_do_tp_sale`) | ≈ 300 | P0 tests; re-route legacy-state branches to needs_reconciliation parking |
| F-06 | Attached-protection engine path + `pending_protection_verification` | ≈ 150 | Confirm no CLI-backend roadmap dependency |
| F-07 | `TradingNotifier` delivery half (flush/_deliver/claim/recover) | ≈ 150 | F-08 phase 2 |
| F-10 | `MexcWebSocketFeed` + composite feed | ≈ 230 | Owner ack that 2 s polling is sufficient (spec says it is) |
| F-12 | Approval-era schema (documentation first, migration later) | n/a (schema) | Archival policy |
| F-15 | Global-block scope narrowing | n/a | **Owner policy reversal — DEFER** |
| — | `_legacy_severity` vocabulary (after F-08 phase 2) | ≈ 40 | F-08 phase 2 |
| — | `cancel_all_account_orders` kill switch | keep | Documented destructive switch; retain (1 call path, engine-external) |

---

## 10. Fund-Protection Invariants (must not be weakened by any simplification)

1. Every BUY is preceded by a verified, ordered exit ladder and a working OCO path (`trade_manager.py:595-628`).
2. Exactly two native OCO legs per fill tranche; both pending rows are persisted atomically before either exchange write (`trade_store.py:create_trade_order_pair`).
3. OCO reservation can never exceed sellable inventory; sellable is `None` (unknown) ⇒ no submission (`trade_manager.py:3022-3058`, `:2969-3002`).
4. Uncertain write outcomes are never followed by automatic resubmission with a different client id (`engine.py:1490-1530`; `trade_manager.py:108-124`).
5. Exchange evidence rows exist before the request (`store.py:begin_exchange_operation`).
6. Fills are monotonic; an exchange query never reduces recorded fills; a local `filled` is never un-filled (`trade_manager.py:_reconcile_single_order`, engine `_map_exchange_state`).
7. Entry remainders are cancelled once the acquired position exits (F-05 rule, `trade_manager.py:3139-3181`).
8. Terminal OCO parents require independently observed, terminal stop children before a trade may close (`trade_manager.py:1366-1412`).
9. The activation boundary is forward-only; backfill/recovery rows are never tradeable; an unarmed boundary refuses everything (`engine.py:364-377, 777-784`).
10. Idempotency: one intent per (channel, message, action, symbol) version; one trade per signal; retry-safe client ids.
11. Protection submissions are never blocked by the entry-only BUY block (`gate.py:139-144`) — simplifications must preserve this asymmetry.
12. Notifications never execute orders and never block trading; outbox-before-delivery is preserved (`notify.py`, `notification_dispatcher.py`).

---

## 11. Notification & Observability Assessment

- **Correctness:** strong. Domain events are written in the business transaction; the dispatcher is independent, fenced against stale claims (`notification_dispatcher.py:912-925`), honors Retry-After, dead-letters after 10 shared attempts, and never auto-retries ambiguous sends. Owner-safety bypass guarantees uncertain/unprotected events reach owners regardless of preferences (`notification_dispatcher.py:117-143`).
- **Complexity:** the double ledger (F-08) is the main debt; the legacy severity vocabulary (`_legacy_severity`) and the dual relay cursors exist only for the bridge.
- **Volume:** ≈15–30 messages per trade (F-09) versus the spec's "minimal notifications" — a product-level tuning item, not a bug.
- **Observability:** audit journal + domain events + `exchange_operations` give complete post-hoc reconstruction; `diagnostics clock/audit/orders/reconcile` and `readiness` cover operations. The readiness report already treats the BUY block as diagnostic context (`readiness.py:356-360`).

---

## 12. Dead & Legacy Code Register (verified)

| Location | Item | Status |
|---|---|---|
| `trade_manager.py:1101-1130` | `_enter_tp1`, `_enter_tp2`, `_transition_state` | Disabled; bodies after `return` unreachable |
| `trade_manager.py:1178-1236` | `_disable_legacy_exit`, `_do_tp_sale`, `_compute_available`, `_submit_tp_order` | Disabled / unreachable / raise-only |
| `trade_manager.py` `_target_qty` | zero references | Dead |
| `trade_manager.py:2799-2845` | `_enter_stop_loss`, `_resume_stop_loss`, `_submit_stop_loss_order` | §7 guards (keep the guarantee, shrink the surface) |
| `engine.py:1298-1375` | `_protection_prices`, `_record_protection_intent`, `_verify_protection` | Dead in production (`supports_attached_protection=False`) |
| `store.py` `pending_protection_verification` | zero references | Dead |
| `store.py` `clock_health_state` | zero function references (constants used in `main.py` diag) | Semi-dead |
| `notify.py:150-260` | `flush`, `_deliver`, claim/recovery of legacy outbox | Unreachable in production |
| `config.py:573-586` | `stop_loss_trigger_offset_ticks` (must be 0) | Dead knob |
| `trading/cleanup.py:29-140` | cancel-eligibility machinery | Disabled by policy |
| `monitor.py:236-456` | `MexcWebSocketFeed` | Never validated against the exchange |
| `db.py` migrations 5–17 | approval/learning/mgmt-era tables | Legacy schema, out of hot path |
| `mexc/client.py:cancel_all_account_orders` | account-wide kill switch | Intentionally retained, documented destructive |

---

## 13. Prioritized Remediation Plan

**P0 — Prerequisite (no code changes): characterization-test freeze.**
Before any P1/P2 deletion, pin the three incident paths with explicit tests (most infrastructure exists in the 72-file offline suite):
1. market ≥ trigger while < entry ⇒ direct LIMIT at entry (§4.1 path, `trade_manager.py:566-587`);
2. transient metadata failure ⇒ terminal reject + owner notification, deterministic symbol error ⇒ identical outcome (§4.2);
3. canceled-OCO-with-unattributed-child ⇒ trade parked, global block asserted with precise reason, discovery retried under bounded backoff, block clears when the child is attributed (§4.3).
Also pin §10 invariants 3, 4, 8, 11 with direct assertions. Effort: ~1–2 days. Risk: none.

**P1 — Low-risk deletions/merges (zero behavior change; ≈ 500–600 LOC).**
1. F-02 deduplicate conflict logic; 2. F-03 decimal helpers; 3. F-04 backoff codec; 4. F-05 dead stubs + unreachable bodies (re-route the legacy-state branches in `_advance_trade_after_reconcile` to the existing needs_reconciliation parking — keep the §7 no-Market-Sell proof as a test); 5. F-13 log/comment fix; 6. delete `_target_qty`, `pending_protection_verification`, `clock_health_state`, and the dead config knob (F-18b). Each item is independently shippable with the P0 tests green.

**P2 — Staged structural simplifications (behavior-neutral at the user level, owner-informed).**
1. F-08 phase 1: stop double-writing legacy outbox rows for domain-event-backed facts; keep `relay_legacy_once` for the drain window; 2. F-06: remove the attached-protection engine path behind the existing flag; keep the table; 3. F-07: remove `TradingNotifier` delivery half after phase 1 soak; 4. F-11: slim `cleanup.py`; 5. F-14 (optional, owner consent): one bounded metadata retry for transient errors only.

**P3 — Owner-visible / deferred.**
1. F-09 notification coalescing (requires owner approval; the safety bypass already protects critical events); 2. F-10 remove the WebSocket feed; 3. F-12 legacy-schema documentation now, archival migration later; 4. F-18 CLI consolidation; 5. F-15 global-block scope review (explicit owner policy decision; include an escalation notification when discovery exhausts the 7-day history window).

**Explicitly not recommended:** touching the OCO discovery subsystem, the sellable/invariant math, the uncertain-outcome policy, the evidence chain, or the gate/claim transaction pairing (F-01, F-16, F-17, F-19). Their complexity is load-bearing.

---

## 14. Audit Method

1. **Static read of the full execution path** at HEAD `9ac46c0` on a clean tree: `docs/` (README, AGENTS, trading spec, architecture, data-model), `engine.py` (2,617 LOC, complete), `trade_manager.py` (4,044, complete), `store.py` (2,389, complete), `trade_store.py` (984, complete), `gate.py`, `risk.py`, `monitor.py`, `readiness.py`, `notify.py`, `domain_events.py`, `evidence.py`, `audit.py`, `balance.py`, `cleanup.py`, `parser.py`, `service.py`, `processing.py`, `notification_dispatcher.py`, `main.py`, `config.py`, `repository.py`, `db.py` (migrations 1–25), `mexc/client.py`, `mexc/hybrid.py`, `mexc/symbols.py`, `mexc/clock.py` (structure), `mexc/errors.py` (structure), `mexc/cli.py` (structure), `control/service.py` (structure), `locking.py`, `telemetry.py`, `runtime.py`, `deploy/*`.
2. **Call-site verification** for every dead-code claim via repo-wide grep (definitions excluded), covering both `tg_signal_ingest` and `tests/`.
3. **Incident replay against code:** each historical incident was traced to its current enforcement points; where history could not be reconstructed from the tree (the 26 h duration's original trigger), that limitation is stated rather than inferred.
4. **Spec conformance check:** every finding was evaluated against the authoritative behavior spec; anything that would alter entry/exit correctness or fund protection was excluded from actionable recommendations and routed to owner decisions (F-09, F-15).
5. **Constraints honored:** read-only audit; no orders placed/cancelled/tested; no `serve`/`auth login`/`trading verify`/`trading reconcile` executed; no secrets, DB contents, or code dumps in this report; exactly one new file committed (this report).
