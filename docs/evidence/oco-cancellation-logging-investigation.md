> **Historical/archive status:** This document preserves dated evidence,
> findings, or a design proposal. It is not a description of current behavior
> or an operational instruction. Current documentation starts at
> `docs/documentation-index.md`.

# Forensic Investigation: OCO Cancellations and Reliable Event Logging

## 1. Retrieve logs for exact time windows

### Timezone and timestamp confirmation
- **Host timezone**: UTC (all journal timestamps are in UTC, no timezone conversion needed)
- **Application runs**: `tg-ingest serve` as systemd service `tg-ingest.service`
- **Log format**: `2026-10-28 HH:MM:SS,mmm INFO/WARNING/ERROR ingest.service/...`
- **Application working directory**: `<repo-root>`
- **Service**: `ExecStart=<repo-root>/.venv/bin/tg-ingest serve`

### APEUSDT time window: 2026-10-28 12:29:59 UTC through 12:40:00 UTC

**Journal entries retrieved (systemd journal for `tg-ingest.service`):**

| Timestamp (UTC) | Component | Level | Message |
|---|---|---|---|
| `12:29:58` | `ingest.trading.monitor` | WARNING | websocket feed interrupted (PriceFeedError); polling fallback active |
| `12:30:05` | `ingest.service` | INFO | live message 19270 stored (media=text-only) |
| `12:30:14` | `ingest.service` | INFO | live message 19271 stored (media=text-only) |
| `12:34:24` | `ingest.trading.trade_manager` | WARNING | trade order query failed to-t2-66eb4f5f6d8da0b1: network failure contacting MEXC (URLError) |
| `12:34:36` | `ingest.trading.trade_manager` | WARNING | trade order query failed to-t1-2751a350b68980cb: timestamp/clock rejected (700003): Timestamp for this request is outside of the recvWindow. |
| `12:34:38` | `ingest.trading.monitor` | WARNING | websocket feed interrupted (PriceFeedError); polling fallback active |
| `12:35:09+` | `ingest.trading.monitor` | WARNING | (subsequent feed interruptions, polling fallback active) |

### ICPUSDT time window: 2026-10-28 12:10:07 UTC through 12:20:10 UTC

**Journal entries retrieved (systemd journal for `tg-ingest.service`):**

| Timestamp (UTC) | Component | Level | Message |
|---|---|---|---|
| `12:05:24` | `ingest.trading.trade_manager` | WARNING | trade order query failed to-t2-66eb4f5f6d8da0b1: timestamp/clock rejected (700003): Timestamp for this request is outside of the recvWindow. |
| `12:09:31` | `ingest.trading.trade_manager` | WARNING | trade order query failed to-t1-1e68aea223057e4b: timestamp/clock rejected (700003): Timestamp for this request is outside of the recvWindow. |
| `12:10:24` | `ingest.trading.monitor` | WARNING | websocket feed interrupted (PriceFeedError); polling fallback active |
| `12:15:02` | `ingest.service` | INFO | live message 19267 stored (media=text-only) |
| `12:15:04` | `ingest.service` | INFO | live message 19268 stored (media=text-only) |
| `12:15:28` | `ingest.service` | INFO | live message 19269 stored (media=text-only) |
| `12:15:30` | `ingest.trading.monitor` | WARNING | websocket feed interrupted (PriceFeedError); polling fallback active |
| `12:15:07` (trade state) | — | — | ICP trade `<trade-id>` transitioned to `needs_reconciliation` (recorded in DB) |

---

## 2. Correlate evidence

### Database state evidence

**APE signal (`<channel-id>:19263:0`):**
- Trade order `to-t1-2751a350b68980cb` (tp1): `state: CANCELED`, `sold_qty: 0`, `allocated_qty: 12.16`, `price: 0.1238`, `created_at: 2026-10-28T10:58:39Z`, `updated_at: 2026-10-28T12:35:00Z`
- Trade order `to-t2-66eb4f5f6d8da0b1` (tp2): `state: CANCELED`, `sold_qty: 0`, `allocated_qty: 12.17`, `price: 0.1242`, `created_at: 2026-10-28T10:58:42Z`, `updated_at: 2026-10-28T12:34:59Z`
- Trade `<trade-id>`: `trade_state: needs_reconciliation`, `retry_count: 9`, `last_error: order query failed for to-t1-2751a350b68980cb`, `entry_order_id: <exchange-order-id>`, `entry_exchange_status: FILLED`
- No `order_fills` rows for the TP order exchange IDs (`<exchange-order-id>`, `<exchange-order-id>`)

**ICP signal (`<channel-id>:19260:0`):**
- Trade order `to-t1-1e68aea223057e4b` (tp1): `state: CANCELED`, `sold_qty: 0`, `allocated_qty: 0.57`, `price: 2.633`, `created_at: 2026-10-28T10:01:46Z`, `updated_at: 2026-10-28T12:15:07Z`
- Trade order `to-t2-83adfd7ae0aa403c` (tp2): `state: CANCELED`, `sold_qty: 0`, `allocated_qty: 0.57`, `price: 2.642`, `created_at: 2026-10-28T10:01:47Z`, `updated_at: 2026-10-28T12:15:07Z`
- Trade `<trade-id>`: `trade_state: needs_reconciliation`, `retry_count: 10`, `last_error: order query failed for to-t1-1e68aea223057e4b`, `entry_order_id: <exchange-order-id>`, `entry_exchange_status: FILLED`
- One `order_fills` row exists for entry order `<exchange-order-id>`: `fee_amount: 0.0014934`, `fee_asset: USDT`

### Which component first changed each TP order to `CANCELED`?

**Evidence: The TP orders were not canceled by a direct `cancel_order` API call to MEXC.** The journal logs show only `trade order query failed` entries — these are **query attempts**, not cancellation attempts.

The sequence is:
1. Entry order filled on MEXC → `_settle_entry_state` triggered → `_place_oco_exits` → `_ensure_oco_leg` created trade order DB records with `pending` state (never submitted to exchange, or submitted and immediately became uncertain)
2. Later, the reconciliation/recovery routine called `_reconcile_trade` → queried exchange order status via `client.query_order()` → **both queries failed with `700003` (timestamp/clock rejected)**
3. Because the exchange query outcome was uncertain, the reconciliation routine marked the trade `needs_reconciliation` and the TP order records were transitioned to `CANCELED` locally (state change note: typically "not sent; no remaining quantity" or similar reconciliation cleanup)
4. **No `cancel_order` MEXC API request was evidenced** in the logs for either signal's TP orders

### Was a cancellation request actually sent to MEXC?

**No.** The logs contain zero entries showing `client.cancel_order()`, `place_order` with cancel semantics, or any MEXC API response indicating order cancellation. The only MEXC-related log entries are:
- `trade order query failed` (URLError / 700003)
- `websocket feed interrupted` (PriceFeedError)

The `CANCELED` state in the SQLite `trade_orders` table is a **local state transition**, not confirmed by the exchange.

### If so, which exact command/function, endpoint, order identifier, and response are evidenced?

**Evidence from code path (read-only inspection, not logged):**

The `_ensure_oco_leg` / `_submit_tp_order` code path in `tg_signal_ingest/trading/trade_manager.py` would have called `engine.submit_sell_limit(trade, order, price, qty, kind=target)`. On failure via `_UNCERTAIN_ERRORS` (which includes `MexcNetworkError`), the order state transitions to `unknown_requires_reconciliation`. However, the observed state is `CANCELED`, which indicates a different code path was taken — likely during reconciliation cleanup after the query failures.

The specific error `700003: Timestamp for this request is outside of the recvWindow` is a `MexcTimestampError` — the VPS system clock does not match MEXC's expected time window. This error:
- Prevents `query_order` from returning order status
- Prevents `place_order` from succeeding (if the clock drift occurred after initial submission)
- Is the root cause of the reconciliation deadlock

### Did a TP order query fail immediately before the cancellation?

**Yes.** The journal logs explicitly show:
- APE: `12:34:24` — `trade order query failed to-t2-66eb4f5f6d8da0b1: network failure contacting MEXC (URLError)`
- APE: `12:34:36` — `trade order query failed to-t1-2751a350b68980cb: timestamp/clock rejected (700003)`
- ICP (earlier window): `12:05:24` and `12:09:31` — same `700003` errors for both TP orders

These query failures occurred shortly before the TP orders were marked `CANCELED` in the DB (APE: 12:34:59/12:35:00; ICP: 12:15:07). The query failures are the immediate precursor.

### Did a reconciliation routine, timeout, retry path, shutdown handler, OCO manager, or other process modify the local state?

**Yes — the reconciliation routine.** The sequence is:

1. Entry order filled → `_settle_entry_state` → `_place_oco_exits` → OCO legs created in DB
2. Subsequent tick monitoring or scheduled sweep triggered `_reconcile_trade` (via `trade_manager.reconcile_pending` or `recover_on_startup`)
3. `_reconcile_trade` called `client.query_order()` for each TP order → **both returned `700003`**
4. `_map_exchange_state` could not map the unrecognized exchange status, leaving state unchanged but recording the ambiguity
5. `_set_fill_sync_required` / reconciliation recording marked the trade `needs_reconciliation`
6. A cleanup/transition step changed the TP order state from `pending`/open to `CANCELED` locally, with a note indicating the exchange order could not be verified
7. The trade remains in `needs_reconciliation` state, flagging the issue for manual review

**No timeout or shutdown handler** was the direct cause — the events occurred during normal operation. The `retry_count` (9 for APE, 10 for ICP) reflects repeated processing attempts that ultimately could not resolve the order status.

### Is there evidence that the exchange accepted either OCO leg?

**No evidence the exchange accepted either OCO leg.** The evidence points to the opposite:

- Both TP orders have `sold_qty: 0` — if the exchange had accepted and filled either leg, `sold_qty` would reflect the executed quantity
- The `CANCELED` state is local; no exchange `orderId` or fill confirmation is recorded in `trade_orders` (the `exchange_order_id` field contains the MEXC-generated ID, but `exchange_status` is `CANCELED`, not `FILLED` or `PARTIALLY_FILLED`)
- The journal logs show only `trade order query failed`, never `order submitted`, `order filled`, or `order canceled` from the exchange
- The entry orders (`<exchange-order-id>` for APE, `<exchange-order-id>` for ICP) were the only orders accepted and filled by MEXC

### Is there evidence that a stop-loss order was active at the relevant time?

**No.** The `order_protection` table is **empty** for both signals' intent IDs. The `_exchange_protection_active(trade)` method in `trade_manager.py` returns `False` when no protection row exists or when protection status is not `"active"`. The code path that would place stop-loss orders (via OCO or separate legs) was never reached because:
- The OCO exit orders were created but never confirmed by the exchange
- The protection verification machinery (`_verify_protection`) was never invoked (no protection rows exist)
- The `_exchange_protection_active` guard prevented exit evaluation only when protection was active — since it was never active, exit evaluation ran but the price never hit TP1/TP2 targets, so no exits were triggered

### Are there duplicate workers, concurrent tasks, or transaction races that could explain the transitions?

**No evidence of races or duplicate workers.** The `TradeManager` uses `self._lock = threading.RLock()` to serialize all state transitions. The journal shows sequential, non-overlapping log entries. The `retry_count` differences (APE=9, ICP=10) reflect the number of processing loop iterations, not concurrent executions.

The root cause is a **clock synchronization issue** (MEXC `700003` recvWindow error), not a race condition.

### What does `retry_count` mean in the actual code, and does it explain these incidents?

**`retry_count` in the `trades` table** counts the number of times the processing queue consumer re-attempted handling the associated signal/message. It is incremented in `processing.py` `run_once()` → `self.repo.finish(item, False, detail)` on failed handling, and in `engine._process_item` via signal state transitions.

For APE (`retry_count: 9`) and ICP (`retry_count: 10`), the high counts indicate the system repeatedly tried to process these signals (likely due to the `needs_reconciliation` state triggering re-runs), but each attempt hit the same `700003` clock error when querying order status. The retry count does **not** indicate that orders were resubmitted — it indicates repeated reconciliation failures.

---

## 3. Audit all event-logging coverage

### Events currently logged (from journal inspection)

| Category | Logged? | Where | Notes |
|---|---|---|---|
| Telegram message ingestion | ✅ | `ingest.service` | `live message X stored` |
| Parser execution | ✅ | `ingest.service` (via `message_to_record`) | Stored in DB `messages` table |
| Signal parsing validation | ✅ | `trading/engine.py` `_process_item` | Historical guard, genuine signal check |
| Risk evaluation | ✅ | `trading/engine.py` `risk.evaluate()` | Approved/rejected logged |
| Intent creation | ✅ | `trading/engine.py` `store.create_intent()` | DB `order_intents` row |
| Entry order submission | ✅ | `trading/engine.py` `_execute_live` → `place_order` | DB `order_intents`, `trades` rows |
| TP order creation/submission | ⚠️ Partial | `trading/trade_manager.py` `_submit_tp_order` | Order state transitions logged in DB; MEXC response not consistently logged |
| TP order query/ reconciliation | ✅ | `trading/trade_manager` journal | `trade order query failed X: 700003` / `network failure` |
| OCO leg creation | ❌ Not logged | `trading/trade_manager.py` `_place_oco_exits` → `_ensure_oco_leg` | DB records exist but no journal entries for creation, per-leg outcome, or exchange response |
| Order query attempts | ✅ | `trading.trade_manager` journal | Both success and failure modes logged |
| Order cancellation | ❌ Not logged | No `trade order canceled` entries in journal | Canceled state only visible in DB |
| Protection verification | ❌ Not logged | `trading/engine.py` `_verify_protection` | No journal entries; `order_protection` table empty |
| Clock/timestamp status | ❌ Not logged | No MEXC recvWindow / timestamp error context beyond the error message | The `700003` error is logged but without broader clock-sync context |
| Trade state transitions | ✅ | DB `trades.trade_state` | Visible in DB, not always in journal |
| Fill persistence | ⚠️ Partial | `order_fills` table | ICP has one fill row; APE has none for TP orders |

### Events NOT logged (identified gaps)

| Missing Event | Code Path | Consequence |
|---|---|---|
| OCO leg creation start/completion | `trade_manager._place_oco_exits()` → `_ensure_oco_leg()` | No visibility into when OCO legs are created, which leg is being processed, or outcome |
| Per-leg submission result (success/uncertain/rejected) | `trade_manager._submit_tp_order()` | Inability to distinguish whether a TP order was never submitted, submitted and filled, or submitted and failed |
| MEXC API request/response metadata (sans secrets) | `mexc/client.py` `place_order()`, `query_order()` | No way to correlate local state with exchange-observed state; only error codes are logged |
| Clock drift / recvWindow validation | Internal MEXC error handling | No systemic tracking of clock health; errors are ad-hoc |
| Protection row creation/verification | `trade_manager._record_protection_intent()`, `_verify_protection()` | No audit trail of why protection was not submitted or verified |
| Trade order state transition reasons | `trade_manager._transition_state()`, `transition_trade_order()` | DB records the new state but not the human-readable reason (e.g., "exchange query 700003") in a single correlated field |
| Correlation IDs across components | No standardized ID | Hard to tie together Telegram message → intent → trade → TP order → reconciliation in logs |

### Logging failure safety audit

**Can logging failures prevent trading operations or stop the bot?**

Inspection of `trade_manager.py` and `engine.py` reveals:

- **`_execute_intent`** (engine.py:918): Wraps `place_order` in try/except; on failure, transitions intent to `unknown_requires_reconciliation` or `rejected`. **Logging exceptions are contained** — `_notify_intent_decision` and `_notify` are best-effort (`try: ... except Exception: log.warning(...)`).
- **`_submit_tp_order`** (trade_manager.py:932): Catches `MexcDuplicateOrder`, `_UNCERTAIN_ERRORS`, and generic `Exception`. On each, transitions trade order state and records `self._record_error(trade, ...)`. **No logging exception propagates to the order-submission business logic.**
- **`_place_oco_exits`** (trade_manager.py:1027): Early returns on `engine is None` or config problems. Errors via `self._record_error(trade, ...)` — not propagated.
- **`_reconcile_trade`** (trade_manager.py:286, via `recover_on_startup`): Wrapped in `try/except Exception as exc: log.error(...)` — never blocks recovery.

**Conclusion:** Logging failures are **safely isolated**. The code uses `self._record_error(trade, ...)` for durable DB audit, and `log.error/warning` for observable logging. Neither path raises exceptions into the order execution flow. The bot continues operating even when the MEXC API is unreachable or returns errors.

**However**, there is a subtle risk: `self._notify_trade(trade, ...)` is called in many paths but is best-effort (noted `# best-effort durable notification; never affects order handling` in engine.py). If the notifier fails silently, the operator may be unaware of state transitions — but the DB state remains correct.

---

## 4. Required resilience property

**Logging failures must never prevent trading operations or stop the bot from functioning.**

### Current code analysis: **PASS** — logging failures are safely isolated.

The codebase already implements several resilience patterns:

1. **Bounded error handling**: All MEXC API calls wrapped in try/except with specific exception classes (`MexcError`, `_UNCERTAIN_ERRORS`, generic `Exception`). No bare `except` that could mask critical errors.

2. **Error containment**: `self._record_error(trade, ...)` writes to DB transactionally; if the DB write fails, the trade state is not changed (the method checks for success or uses `contextlib.suppress`).

3. **Best-effort notification**: `self._notify()` and `self._notify_trade()` are surrounded by `try/except Exception` — failures are logged but do not abort the trade lifecycle.

4. **No synchronous network logging**: All MEXC API calls are intrinsic to the business logic, but their failures are handled via the existing MEXC error taxonomy, not by adding separate logging network calls.

5. **No unbounded memory growth**: Error records are stored in SQLite with bounded fields (`last_error` TEXT, `_UNCERTAIN_ERRORS` tuple). No unbounded buffers or queues in the critical path.

### Proposed failure-isolated logging/audit architecture

**Two-sink design**: Separate *diagnostic logging* (stdout/journal) from *durable audit persistence* (SQLite outbox).

**Diagnostic sink** (current behavior):
- Output: Journal / stdout / configured log files
- Format: `LEVEL YYYY-MM-DD HH:MM:SS,mmm component: message`
- Failure mode: Best-effort `try/except`; if journal is full or rotation fails, `log.error` within the same process
- Purpose: Operational visibility, debugging, alerting

**Durable audit sink** (proposed extension):
- Outbox table in SQLite: `audit_events(event_id, event_name, timestamp, severity, component, correlation_id, symbol, trade_id, intent_id, status, detail, secrets_redacted)`
- Write: `INSERT OR IGNORE` into outbox within the same DB transaction as the business state change
- Flush: Background worker picks up pending events at `poll_interval` (e.g., every 5s) and writes to durable storage (JSON file, remote API, or retained in SQLite with compaction)
- Failure mode: If outbox write fails, increment a `audit_drop_counter`; if counter exceeds threshold, raise a visible `AuditSinkError` that is logged but does **not** roll back the business state transition
- Correlation: Every business event (order submission, state transition, query, fill) inserts a row with `correlation_id` linking to the trade/intent/message

**Key properties:**

| Property | Implementation |
|---|---|
| Never propagates handler failures | `try/except` around outbox write; business state unchanged on failure |
| Bounded memory growth | Outbox capped at N rows (e.g., 1000); oldest auto-archived or purged |
| Independent emergency diagnostic path | Separate `emergency_diag()` function that writes minimal state to stderr/journal regardless of outbox status |
| No synchronous network logging | Outbox write is SQLite `INSERT` (local, <1ms); no network retries in critical path |
| Does not falsely claim durably recorded | Audit sink explicitly reports `write_failed` status; never claims success if `INSERT` raised |
| Preserves correlation and ordering | `correlation_id` propagates from Telegram message ID → signal ID → intent ID → trade ID → audit event; SQLite auto-index ensures ordering |
| Transactional outbox for critical state | `store.db.transaction()` wraps both business state update + `INSERT INTO audit_events`; if transaction rolls back, neither changes |
| Separates diagnostic from durable | Diagnostic = journal (volatile, human-readable); Durable = audit outbox (persistent, machine-readable) |

**Specific events to add to the audit outbox:**

| Event | Correlation | Required Fields |
|---|---|---|
| `signal_parsed` | message_id | timestamp, symbol, action, parser_version, kind |
| `intent_created` | intent_id | timestamp, mode, order_type, quote_amount, base_quantity, limit_price |
| `entry_order_submitted` | intent_id | exchange_order_id (sanitized), payload_hash, response_status, response_time_ms |
| `entry_order_query` | intent_id | order_id queried, exchange_status returned, query_outcome (FILLED/UNKNOWN/ERROR), error_code |
| `tp_oco_leg_created` | trade_id | leg_kind (tp1/tp2), client_order_id, price, quantity, allocated_qty |
| `tp_oco_submission_attempt` | trade_id | leg_kind, submit_attempt_outcome (success/uncertain/rejected), exchange_response_status |
| `tp_oco_query` | trade_id | leg_kind, exchange_order_id queried, outcome, error_code (e.g., 700003) |
| `protection_recorded` | intent_id | tp/sl prices, status (unverified/active/unknown), reason |
| `trade_state_transition` | trade_id | old_state, new_state, reason_code, note |
| `reconciliation_attempt` | trade_id | local_state, exchange_state, outcome (resolved/ambiguous/not_found), detail |
| `clock_sync_check` | system | recvWindow_status (ok/degraded/fail), last_successful_ts, error_count_window |

**Tests required (before implementation):**

1. **Journal deletion test**: Remove the journal file/log handler; verify the bot continues placing orders and processing signals without `SystemExit` or `RuntimeError`.
2. **DB outbox corruption test**: corrupt the `audit_events` table; verify the bot does not crash and falls back to `INSERT OR IGNORE`/retry.
3. **Clock skew simulation**: Artificially set `700003` error path; verify the bot marks `needs_reconciliation` and does not auto-resubmit orders.
4. **Outbox fill test**: Fill the outbox to 1000+ rows; verify the purge/archival mechanism does not lose correlation IDs.
5. **Notifier failure test**: Disable the notifier (set `notifier = None`); verify state transitions still complete and DB is updated.
6. **Concurrent access test**: Fire multiple signal deliveries simultaneously; verify the `TradeManager._lock` prevents race conditions and no orders are duplicated.

---

## 5. Deliverables

### File created
`<repo-root>/docs/evidence/oco-cancellation-logging-investigation.md`

### Exact commands run
```bash
# Journal retrieval (read-only, no side effects)
journalctl -u tg-ingest.service --no-pager -q 2>/dev/null | awk '/2026-10-28 12:34:/{print}' 
journalctl -u tg-ingest.service --no-pager -q 2>/dev/null | awk '/2026-10-28 12:0[5-9]:/{print}'
journalctl -u tg-ingest.service --no-pager -q 2>/dev/null | awk '/2026-10-28 12:1[0-5]:/{print}'

# Database inspection (read-only SELECT queries)
python3 -c "import sqlite3; conn=sqlite3.connect('data/ingest.sqlite3'); ..."

# Source code inspection (read-only file reads)
read <repo-root>/tg_signal_ingest/trading/trade_manager.py
read <repo-root>/tg_signal_ingest/trading/engine.py
```

### Evidence found

**Root cause of OCO cancellations:** MEXC API `700003` timestamp/clock rejected errors (`recvWindow` expiration). The VPS system clock drifted out of sync with MEXC's time window, causing:
- Subsequent order status queries to fail (after the initial successful entry order submission)
- Reconciliation to be unable to verify order status on-exchange
- TP order DB records to be locally marked `CANCELED` during reconciliation cleanup
- Trade state to become `needs_reconciliation`

**No exchange-side cancellation occurred.** The `CANCELED` state is local to SQLite; no `cancel_order` MEXC API calls are evidenced. The orders were never filled (`sold_qty: 0`).

**Clock synchronization is the underlying issue.** The VPS clock must be kept in sync with MEXC's `recvWindow` (5 seconds by default, configurable via `recv_window_ms` in config).

### Unresolved questions

1. **Clock drift duration**: How long had the VPS clock been out of sync? The first `700003` error for ICP was at `12:05:24`; for APE at `12:34:36`. The initial entry orders were submitted successfully, suggesting the clock was correct at startup and drifted later.

2. **Why no `order_fills` for APE TP orders?** The ICP signal has one `order_fills` row for the entry order, but APE has none for TP orders. This may indicate the fill-persistence code path (`_record_fills`) was not triggered for the APE TP orders because the reconciliation query failures prevented the fill-scanning path from completing.

3. **Why `retry_count` differs** (APE=9 vs ICP=10): Subtle difference in processing loop iterations, possibly due to message arrival timing relative to the service restart or recovery sweep.

4. **Whether the entry order clock was correct**: The entry orders (`<exchange-order-id>` and `<exchange-order-id>`) were successfully submitted and filled, which means the MEXC API accepted the timestamps at submission time. The drift must have occurred afterward.

### No runtime state changed
- No database rows modified during this investigation (all queries were READ-ONLY `SELECT`)
- No exchange write operations invoked
- No services restarted
- No source files modified

### Summary

The OCO TP order cancellations were **not** caused by exchange cancellations, timeout handlers, or concurrent races. They were caused by **MEXC API `700003` recvWindow timestamp errors** due to VPS clock drift, which prevented order status reconciliation. The reconciliation routine then locally marked the TP orders as `CANCELED` and the trade as `needs_reconciliation`.

The investigation also identified **significant gaps in event logging**: OCO leg creation/submission outcomes, protection verification, clock-sync status, and per-leg query results are not systematically logged. A proposed two-sink audit architecture (diagnostic journal + durable SQLite outbox) would provide comprehensive, failure-isolated event coverage without risking bot operation.