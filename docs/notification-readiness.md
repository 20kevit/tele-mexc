# Notification architecture and Phase 2 readiness

Status: **release candidate ready for controlled rollout; not yet deployed**.
This document records the implementation, validation evidence, and remaining
post-deployment checks. It does not claim that the live worker is already running.

Source of truth remains code and tests:

- `tg_signal_ingest/trading/domain_events.py` — typed event envelope/registry.
- `tg_signal_ingest/notification_dispatcher.py` — independent relay + delivery.
- `tg_signal_ingest/control/bot_api.py` — direct Bot API transport/errors.
- `deploy/tg-notification-worker.service` — worker unit.
- `tests/test_domain_events.py`, `tests/test_notification_dispatcher.py`,
  `tests/test_notification_policy.py`.

## Boundary

Trading producers write only local SQLite state. Telegram delivery runs in the
separate `tg-ingest notifications worker` process and uses a separate delivery
database (`data/notification-delivery.sqlite3`). The dispatcher opens the
trading database **read-only** (`mode=ro`, `query_only=ON`) and never writes it.

No trading module, engine lock, service tick, startup/readiness path, or CLI
performs a Telegram send or an outbox flush. `TradingNotifier.notify()` is
enqueue-only. The dormant legacy `flush()` helper remains for compatibility and
is not called by any producer; a static call-graph test enforces this.

Trade execution, sizing, entry/trigger/tick logic, TP/SL/OCO, fills, cancel,
reconciliation, accounting, pause, and order state are unchanged by this work.
Evolution of trading state remains governed by `docs/trading.md` and the
non-negotiable trading rules.

## Severity policy

| Level | Meaning | Owner A/B | Programmer |
| --- | --- | --- | --- |
| DEBUG | chatter, not-a-signal, news/analysis, parser diagnostics | never | only with diagnostic category enabled |
| INFO | valid signal received/accepted, confirmed submit with orderId, open/fill, protection active, TP/SL lifecycle, trade closed | yes, subject to category/minimum | yes, subject to category/minimum |
| WARNING | incomplete/ambiguous/rejected genuine trading signal, scheduled retry/delay, recoverable discrepancy | yes; owner-safety cases bypass; others subject to category/minimum | subject to category/minimum |
| ERROR | definitive exchange rejection, protection failure after retries, persistent API/DB/recovery failure, serious mismatch | yes; safety-unknown cases bypass; others subject to category/minimum | subject to category/minimum |
| CRITICAL | unprotected live trade, UNKNOWN/duplicate-risk submission, dangerous discrepancy, critical service interruption | yes; **always** | yes; **always** |

Routing precedence for one recipient:

1. `CRITICAL` → delivered to every configured recipient, ignoring audience,
   category preference, and minimum severity.
2. `DEBUG` → owners always suppressed; programmer delivered only when the
   diagnostic category is enabled and the programmer's own minimum threshold
   permits it.
3. Owner safety alert → delivered to owners regardless of category/minimum.
   This covers `signal.rejected`, `trade.protection_critical`,
   `unknown_requires_reconciliation`, `needs_reconciliation`,
   `unprotected_critical`, uncertain/discrepancy reconciliation outcomes, and
   unprotected OCO groups.
4. Otherwise → category preference **and** per-recipient minimum severity.

Owner B falls back to Owner A's category preferences until its own preference
rows exist, but an explicit Owner B row takes precedence. Minimum severity is
stored per recipient in `control_settings['notification_min_severity.<role>']`,
with the legacy shared setting used only as a fallback. The Telegram settings
menu lets Owner A choose any configured recipient; Owner B can change only its
own threshold; the programmer remains read-only. Suppressed events remain in
delivery history with rendered text and a suppression reason.

## Delivery semantics

- Idempotency is the exact `event_id`. Distinct lifecycle events are never
  coalesced, even when they represent the same state twice. Reusing an
  `event_id` with a different immutable envelope is rejected.
- One row per `(event_id, recipient_role, part_no)`; long messages preserve
  order and are retried fragment by fragment.
- Automatic per-recipient attempt budget is 10 total sends including the initial
  one. An explicit operator requeue can authorize one additional send; the
  shared event/recipient budget is checked across message fragments first.
- Only definitely-not-sent failures are retried: DNS/connect failures, HTTP 429
  (structured `parameters.retry_after`), and explicit retryable transport
  failures raised before transmission.
- A timeout/read failure after possible transmission, an unreadable response, a
  Telegram 5xx response, or a worker crash after claim is `ambiguous`/UNKNOWN and
  is **never** retried automatically.
- Permanent 4xx responses are `dead_letter` immediately; they are never retried.
- Backoff is exponential full jitter with a five-minute maximum; 429 schedules
  the next attempt instead of sleeping. Other recipients and events continue.
- Claims use a lease (`claim_token`, `claimed_at`) and `finish()` fences stale
  workers with a token/status check. A stale claim becomes `ambiguous`, never a
  blind resend.
- `telegram_message_id` is persisted after definitive success. The first
  lifecycle message per `(recipient, trade/intent thread)` becomes that
  recipient's root; later lifecycle messages reply to their own root. CRITICAL
  alerts are standalone.
- Requeue is an explicit operator action
  (`tg-ingest notifications requeue <delivery_id>`), allowed only for
  `ambiguous`/`dead_letter` rows. It preserves a monotonic action history and
  grants one final send attempt only while the shared event/recipient budget
  has room; otherwise it returns `budget_exhausted` without changing the row.

### Exactly-once is impossible

The Telegram Bot API has no idempotency key for `sendMessage`. A transport
failure after the request reaches Telegram can leave the local ledger uncertain.
This implementation chooses **at-most-once for ambiguous outcomes**: it never
blindly retries, preserves the uncertainty in history, and requires an operator
to reconcile. The tradeoff is a small chance that one message was actually
delivered while the ledger says UNKNOWN. That is preferable to duplicate trading
alerts or duplicate owner actions.

## Observability

```bash
.venv/bin/tg-ingest notifications status            # counts, depth, heartbeat, oldest age
.venv/bin/tg-ingest notifications status --json
.venv/bin/tg-ingest notifications list --status dead_letter --limit 50
.venv/bin/tg-ingest notifications requeue <delivery_id>
```

The heartbeat is `dispatcher_state.last_cycle_at` in the delivery database. A
missing heartbeat means the worker never ran; a heartbeat older than 60 seconds
means it is dead or hung. `systemctl is-active` does not prove delivery health.
`trading readiness` reports the dispatcher ledger/heartbeat separately from the
legacy compatibility outbox.

## Findings revalidation (F-01 … F-20)

| ID | Finding | Candidate status | Evidence |
| --- | --- | --- | --- |
| F-01 | Direct Telegram sends inside the trading path | Fixed | `TradingNotifier.notify` enqueue-only; AST/static call-graph test; no `send_message` in trading producers. |
| F-02 | Synchronous flush in ControlBot tick | Fixed | `ControlBot.tick` performs no recovery/flush; delivery is worker-owned. |
| F-03 | Ambiguous outcome marked failed/retried; missing permanent errors/budget | Fixed | `BotApiTransportError`/5xx → `ambiguous`, never retried; DNS/connect → retry; 4xx → dead-letter; 10-attempt budget and tests. |
| F-04 | Preferences/severity not enforced | Fixed | Central `delivery_decision`; per-role category/minimum; CRITICAL and owner-safety bypass; tests. |
| F-05 | Callback store not end-to-end/single-use | Partially fixed / not wired | Legacy callback tokens are opaque, role-bound, expiring, and atomically single-use; no notification callback is emitted by the dispatcher, so no end-to-end claim is made. |
| F-06 | `message_id` discarded | Fixed | `deliveries.telegram_message_id` and attempt rows persist it; threading uses it. |
| F-07 | Event enqueue atomicity/recovery | Fixed for domain events | Events are inserted on the caller's connection inside the business transaction; cursor + relay are atomic; tests prove rollback and exactly-one-event commits. |
| F-08 | Non-signal routes to all recipients | Fixed | Non-signal legacy rows classify DEBUG, owners hard-suppressed, programmer category-gated. |
| F-09 | Overbroad dedupe | Fixed | Only exact `event_id`; control/service events use fresh UUIDs; distinct repeated lifecycle events produce distinct deliveries. |
| F-10 | Taxonomy/envelope/correlation | Fixed for implemented producers | Versioned registry, canonical JSON, UUID IDs, correlation/causation, unknown-event fallback; deferred producer families documented. |
| F-11 | Structured `retry_after` | Fixed | `parameters.retry_after` parsed and scheduled; no sleep; capped at five minutes. |
| F-12 | Lease/fencing | Fixed | Claim token + status check on finish; stale recovery → ambiguous; tests for stale/concurrent workers. |
| F-13 | CLI/control visibility | Fixed | `notifications status/list/requeue`; queue depth, oldest age, heartbeat, dead-letter, ambiguous counts. |
| F-14 | Persian escaping/redaction | Fixed | All payload text redacted, bounded, control-character filtered; safe unknown fallback. |
| F-15 | `_sync_auto_notices` bypass | Fixed | Removed; control tick no longer scans/sends auto notices. |
| F-16 | Readiness flush claim | Fixed | Readiness no longer claims a startup flush; it reports dispatcher heartbeat and state. |
| F-17 | Schema/docs mismatch | Fixed in this change | `docs/data-model.md`, `docs/trading.md`, `docs/operations.md` updated; migration head 25. |
| F-18 | HTTP connection reuse/status | Partially fixed | Error/status classification is explicit; connections are deliberately not pooled (`Connection: close`) to keep ambiguous-outcome reasoning provable. Reuse is an optimization, not required for correctness. |
| F-19 | Chunking/order/fragments | Fixed | Deterministic ordered chunks, per-fragment rows, part-order claim guard, tests. |
| F-20 | Owner A/B authorization conflation | Verified/fixed | Distinct recipient roles and chat IDs; both owners get owner-level events; programmer is read-only; callback/control tests. |

## Candidate verification (2026-11-13, offline)

| Command | Result |
| --- | --- |
| `.venv/bin/pytest -q` | 870 passed (138.17s) |
| focused notification/policy/control/trading suite | 101 passed (14.44s) |
| `.venv/bin/ruff check .` | pass |
| `.venv/bin/mypy tg_signal_ingest` | pass (51 files) |
| `.venv/bin/python -m compileall -q tg_signal_ingest tests` | pass |
| `git diff --check` | pass |
| `systemd-analyze verify deploy/tg-ingest.service deploy/tg-notification-worker.service` | pass (static only) |

No MEXC order/balance API or test-message send was performed. The controlled
rollout did restart the production `tg-ingest.service` once at 15:05:50 UTC;
migration 25 completed and the main service returned active with PID 244919
and `NRestarts=0`. The worker initially raced the migration and logged a
transient `aggregate_type` schema error; after migration it recovered and
successfully delivered five real system-notification messages (all recorded
with Telegram message IDs). No historical events or legacy notifications were
replayed. The worker was then stopped deliberately while a startup schema gate
was added. The main trading service remains active; no MEXC orders or balances
were changed. The existing ControlBot configuration was not changed.

## Residual gaps / release prerequisites

- **Runtime systemd dependency behavior was verified in an isolated harness on 2026-11-13.** Temporary test units proved: (1) worker crash triggers worker restart while the main service remains active; (2) worker start-limit exhaustion leaves the main service active; (3) stopping the main service stops the worker. All temporary units were removed and `tg-ingest.service` remained active with unchanged PID `196659` and `NRestarts=0`. This validates systemd dependency behavior, not real Telegram delivery or the production rollout itself.
- The worker waits up to 180 seconds for the required `audit_events` domain
  columns and source tables before opening its long-lived read-only connection.
  This prevents the systemd `After=` ordering from racing the main service's
  migration. The dispatcher itself never migrates the trading DB.
- The online SQLite backup and reviewed source/unit copy are complete. Migration
  25 is applied in Production. The main service is active; the worker is
  intentionally stopped pending deployment of the startup schema gate. Copy the
  follow-up `main.py`, start the worker, and verify fresh heartbeat/no new schema
  errors before declaring the notification path fully operational.
- Deploy the worker unit with the real deployment path/user; the candidate unit
  uses the same `/opt/projects/akha1` paths as `tg-ingest.service`.
- Notification callback buttons (`expand`/`source`/`ack`) are not emitted by the
  dispatcher. If they are added later they must use the single-use callback store
  and remain strictly read-only.
- `notifications requeue` is an operator write to the delivery ledger only; it
  never touches trading state or the exchange.
- Legacy `trading_notifications` rows written before first worker startup are
  deliberately not replayed (independent no-replay cursor).
