# Final Report: tele-mexc Execution Flow Fixes

## 1. Root Cause of Approval Prompt / Unexpected Market Order

**Root cause: race between two independent, uncoordinated code paths.**

The trading engine (`tg_signal_ingest/trading/engine.py`, `process_item` → `_execute_intent`) **auto-executes every eligible signal immediately** after creating a "planned" intent. There is no owner-approval check in this path.

The control bot (`tg_signal_ingest/control/service.py`, `_sync_approvals` called from `tick()` every 3s) **independently created per-signal approval prompts** when `execution_mode == "approval"`. When an approval expired, `expire_approvals()` transitioned the intent to `canceled_local` and sent the Persian timeout message «مهلت تأیید به پایان رسید سفارش اجرا نشد.» (and its English equivalents).

**The defect**: The approval prompt misled the owner into believing their decision gated execution. In reality, the engine had already (or would independently) auto-submit the order. The owner observed: approval prompt → timeout message → Market order already in flight. The timeout message and the Market order came from two separate paths that were never coordinated:

- Approval timeout → intent → `canceled_local` (control bot)
- Signal arrival → intent → `_execute_intent()` → order submitted (engine)

Neither path consulted the other's state. An approval timeout never *caused* a Market order; the Market order was the engine's pre-existing auto-execution, racing with — and usually completing before — the approval UI.

### Why it was not conclusively reproducible from logs

The production logs show websocket/price-feed warnings and clock-drift (700003) errors, but no conclusive correlation between a specific approval timeout and a specific Market order submission. The database has 0 pending approvals and mixed intent states (canceled/filled/rejected), consistent with the race but not proof of a specific incident. The code-level defect is confirmed regardless.

---

## 2. Behavior Before vs After

| Aspect | Before | After |
|--------|--------|-------|
| Signal evaluation | Approval prompt sent per signal; owner expected to approve/reject | **Automatic evaluation by engine; no approval prompt** |
| Approval timeout | Intent → `canceled_local`; Persian timeout message sent | Expired approvals recorded in `control_approvals` only; **no intent state change** |
| Market-order fallback | Engine auto-executes independently of approval state | Same auto-execution, but **no misleading approval UI** |
| Legacy `ap:y:`/`ap:n:` callbacks | Executed order via `execute_approved_intent` | **Rejected with notice**; no order execution |
| Signal-approval prompts (`_sync_approvals`) | Created approval rows + Telegram buttons | **No-op**; no new approvals created |
| Admin controls (auto-mode 2-step, emergency stop, disconnect) | Preserved | **Preserved unchanged** |
| Uncertain exchange writes | `unknown_requires_reconciliation`, never auto-retried | **Unchanged** |

---

## 3. Entry Decision Rules (Limit vs Conditional)

Implemented in `tg_signal_ingest/trading/trade_manager.py` and verified by 14 deterministic tests in `tests/test_trading_conditional_entry.py`.

| Condition | Entry Strategy | Reason |
|-----------|---------------|--------|
| Market price ≥ entry price (BUY) | **Direct**: submit Limit Buy at entry price immediately | Market is already at/above intended entry; no waiting needed |
| Market price < entry price (BUY) + conditional enabled | **Conditional**: wait for upward trigger (`entry − offset × tick_size`), then submit Limit Buy at entry (hard max) | Never buy above entry; never convert to Market |
| Market price < entry + conditional disabled | **Direct**: submit Limit Buy at entry price | Conditional entry not configured |
| No valid entry price | **Declined**: trade rejected, no order | Fail closed; cannot size or price |
| No reference price (conditional enabled) | **Declined**: trade rejected | Cannot evaluate trigger without market price |
| Stale price observation | **Ignored**: tick skipped | Price too old to trust |
| Entry range | Evaluated explicitly via `entry_low_price`/`entry_high_price` | Not silently converted |
| Unsupported order type | Market blocked when `order_type_policy=limit` | Fail closed |

**No Market fallback exists for entry orders.** Market orders are used only for stop-loss exits (`submit_stop_loss_market_sell`), which is existing TP/SL policy and unchanged.

---

## 4. Event Trail and Telegram Reporting

Durable per-event records are written to `audit_events` (schema v2.0) and `trading_notifications` outbox, both transactional with state changes.

**Captured events include:**
- Message observed (channel ID, message ID, source, timestamps)
- Signal parsing result (kind, action, symbol, entry price, targets, stop loss)
- Validation and risk decisions (each rejection reason)
- Reference price, source, timestamp; selected entry strategy; rationale
- Intent created/declined (intent ID, idempotency key)
- Order submission attempt (exact parameters, exchange write attempted)
- Exchange response identifiers (orderId, status, executedQty)
- Success / rejection / partial fill / fill / uncertain outcome
- TP/SL state transitions, conditional-entry transitions, cancellations, closure
- Approval expiration/rejection (legacy workflow only)
- Recovery, reconciliation, duplicate detection, stale-data rejection
- Notification delivery success/failure (queued/attempted/sent/failed/ambiguous)

**Deduplication:** Scoped to event identity (dedupe_key per intent/category/outcome), not global. Retries do not duplicate events.

**Secret redaction:** Bot tokens, API keys, signatures, and other secrets are redacted before persistence (`tg_signal_ingest/trading/audit.py`).

**Example event flow:**
1. `signal.rejected_locally` — SELL signal ignored (Spot BUY only)
2. `lifecycle.intent_planned` — intent created with idempotency key
3. `order_lifecycle.conditional_entry_waiting` — waiting for upward trigger
4. `order_lifecycle.ENTRY_SUBMITTED` — Limit Buy submitted (entry price, hard max)
5. `reconcile.outcome` — reconciliation result (consistent/ambiguous/not_found)
6. `lifecycle.fills_synced` — fill history persisted

Telegram notifications distinguish **queued**, **attempted**, and **successfully delivered** states. An outbox record alone does not claim delivery.

---

## 5. `/version` Output Example

From `tg_signal_ingest/version.py`, owner-only, read-only:

```
📋 نسخه اجرا (فقط مالک/توسعه‌دهنده)
• نسخه برنامه: 0.1.0
• commit SHA: 61e66a4cb5a8a104933f9c813275425c3e080ccd
• شاخه: fix/sl-stop-loss-trigger-offset
• تغییرات غیرCommit‌شده: خیر
• زمان ساخت: unknown
• زمان استقرار: unknown
• زمان اجرا (uptime): 00h00m00s
• حالت معامله: sim
• مجوز لایو: خیر
• تطبیق با نسخه انتظاردهی: yes
```

**Design:** Prefers `.deploy-meta.json` (deployment metadata) over `git` commands. Unproven fields show `"unknown"`. Restricted to the owner (`CONTROL_OWNER_USER_ID`). No credentials, tokens, or private paths exposed. The command is read-only.

The /version module was verified via unit test (commit SHA, branch, dirty state, trading mode, live authorization, revision match). Live Telegram delivery was not directly tested because the bot token and owner user ID are secrets not accessible in this context.

---

## 6. Git State

| Item | Value |
|------|-------|
| Canonical branch | `fix/sl-stop-loss-trigger-offset` |
| Commit SHA | `61e66a4cb5a8a104933f9c813275425c3e080ccd` |
| Working tree | Clean (0 uncommitted changes) |
| PR #2 changes | Merged from `public/main` (5aba737, 0e76cb0, 2cea664 + merge 391eb6a) |
| `ui-audit` branch | Preserved (unrelated history, no common ancestor, worktree at `/opt/projects/akha1-ui-audit`) |
| Local `main` branch | Preserved (foundational commits, behind current branch) |
| Local `public-review` branch | Preserved (merge base `a3a10b5`, ancestor of current) |
| Remote branches | Untouched (no force-push, no shared history rewrite) |

**Branch integration decisions:**
- `fix/sl-stop-loss-trigger-offset` (current): canonical branch; contains all Phase 2–5 fixes + PR #2 changes + a79e6a9/71ba231/9aea236/a3a10b5
- `public/main` (391eb6a): merged into canonical branch via non-fast-forward merge (no conflicts); PR #2 changes integrated
- `ui-audit` (1d9ce53): **not merged** — completely unrelated history (no common ancestor with current branch); preserved in separate worktree; unique commits (df8c003, 13cbf3d, 5bdf1da, 1d9ce53, and 13 base infrastructure commits) retained
- `main` (cbb5e77): preserved; behind current branch; its foundational work is already represented in the current codebase
- `public-review` (a3a10b5): preserved; merge base; ancestor of current branch

---

## 7. Files Changed

**New files:**
- `tg_signal_ingest/version.py` — runtime version info for `/version`
- `tests/test_trading_audit_store.py` — audit store tests (pre-existing, committed)
- `tests/test_trading_evidence.py` — evidence tests (pre-existing, committed)
- `docs/evidence/*.md` — forensic evidence docs (pre-existing, committed)

**Modified files:**
- `tg_signal_ingest/control/service.py` — removed `_sync_approvals` signal prompts, removed intent cancellation on expiration, removed `_approval_action`/`_reject_intent`/`_execute_approval`/`_approval_text`, added `_show_version` + `/version` command + `set_my_commands` entry
- `tests/test_control_service.py` — rewrote approval tests (no-prompt, no-timeout-cancel, legacy-callback-rejected), added `/version` tests (owner-only, output fields, secret redaction), added admin-control test
- Plus all pre-existing committed changes: `trading/engine.py`, `trading/store.py`, `trading/trade_manager.py`, `trading/audit.py`, `trading/evidence.py`, `trading/notify.py`, `trading/trade_store.py`, `db.py`, `main.py`, `service.py`, `control/store.py`, `control/bot_api.py`, `config.py`, `mexc/symbols.py`, docs, and tests

---

## 8. Test, Lint, and Type-Check Results

| Check | Result |
|-------|--------|
| Full test suite (`pytest`) | **827 passed** (0 failed) |
| Ruff (`ruff check`) | Clean on all modified/new files (pre-existing issues in `main.py`/`engine.py` unchanged) |
| MyPy (`mypy tg_signal_ingest`) | Clean on all modified/new files (2 pre-existing issues in `main.py` unchanged) |

No regressions introduced. Pre-existing lint/type issues in `main.py` (unused `TradeEngine` import, missing type annotation) and `engine.py` (line too long) were not introduced by this work.

---

## 9. Production Service Verification

**Service:** `tg-ingest` (systemd), `WorkingDirectory=/opt/projects/akha1`, `ExecStart=.venv/bin/tg-ingest serve`

**Restart evidence (from `journalctl -u tg-ingest`):**
- Old PID 481262 stopped cleanly: `processing loop finished: processed=14 retried=0 failed=0 superseded=0`
- New PID 485996 started: `control bot task started`, `telegram session verified; transport=direct TCP/TLS`, `listening to channel: id=1530389001`, `trading activation boundary armed at message id 19356`, `recovery sweep complete: 0 gap message(s) fetched, 500 recent refreshed`, `history import already done; skipping backfill`, `market monitor started for conditional-entry/TP/SL lifecycle`

**Verification results:**
- ✅ Service is active and enabled
- ✅ Running new code (SHA 61e66a4)
- ✅ No exchange orders placed during restart
- ✅ No trading configuration changed (mode, stake size, live authorization preserved)
- ✅ Telegram session verified over direct TCP/TLS (no proxy)
- ✅ Trading activation boundary armed (historical messages not tradeable)
- ✅ Websocket feed fallback warnings are pre-existing (not related to this work)

**Not directly verified:** Live `/version` delivery via Telegram (requires sending a message to the control bot; bot token and owner user ID are production secrets not accessible in this context). The `/version` module logic was verified via unit test.

---

## 10. Remaining Limitations and Actions Requiring Authorization

**Limitations:**
1. **Live `/version` delivery not end-to-end verified**: The `/version` Telegram command handler is implemented and unit-tested, but live delivery through the control bot was not tested because the bot token and owner user ID are production secrets. To verify, an authorized operator should send `/version` to the control bot and confirm the output matches the expected format.

2. **ui-audit branch not merged**: The `ui-audit` branch has completely unrelated history (no common ancestor) with the canonical branch. Its unique commits (structured channel parser v2, UI audit findings, management bot UTF-16 limits, etc.) are preserved in the separate worktree at `/opt/projects/akha1-ui-audit` but not integrated. Merging would require manual conflict resolution across unrelated histories.

3. **Pre-existing lint/type issues**: Two mypy errors and several ruff warnings remain in `main.py` and `engine.py`. These predate this work and were not introduced or worsened by it.

**Actions requiring explicit authorization:**
1. **Live `/version` verification**: Send `/version` to the production control bot and confirm output. Requires access to the bot token/owner chat (not accessible in this context).
2. **ui-audit integration**: If the unique ui-audit commits (channel parser v2, UI audit findings, etc.) are needed in the canonical branch, manual integration across unrelated histories is required.
3. **Future service restarts**: The service has been restarted once to deploy these changes. Any further restarts require explicit authorization.
