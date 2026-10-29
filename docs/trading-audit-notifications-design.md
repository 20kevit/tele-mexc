# Trading Audit, Notifications, and Read-Only Investigation — Design

**Status:** proposed design only; not implemented.  It is not a description of
current runtime behavior.

**Repository basis:** source and tests at the current branch, with SQLite schema
head 16. Code/tests take precedence over existing documentation. No database,
credentials, Telegram session, or message contents were read. No tests, live
commands, exchange calls, Telegram calls, deployments, or operational changes
were run.

This document is an implementation package for OpenCode: it defines scope,
event vocabulary, storage and migration shape, state semantics, notification
policy and Persian copy, reconciliation rules, security requirements, staged
file changes, and verification evidence. It deliberately does **not** implement
any feature or arm trading.

## 1. Objective and hard boundaries

Build three separable capabilities:

1. **Durable audit journal:** record meaningful source, local decision,
   operation, acknowledgement, exchange observation, fill, lifecycle, and
   reconciliation facts even when notifications are disabled or fail.
2. **Configurable notifications:** route selected event categories to the
   configured owner and developer independently, with durable retries and
   Persian messages. A mute or category opt-out only affects delivery; it never
   suppresses the journal.
3. **Read-only investigation:** show a correlated timeline and sanitized
   evidence without submitting, canceling, testing, or changing an exchange
   order. An investigation detail button is a read action, not an operation.

The design is additive to the existing MEXC Spot execution pipeline. It must
not change the existing trade strategy or order limits as a side effect.

### Non-negotiable invariants

- MEXC **Spot** only. Signal-driven entries are BUY/LONG only. Telegram SELL or
  SHORT instructions are ignored/rejected locally; the existing bot-managed
  Spot exits remain SELL orders. Telegram cancel/delete handling is a distinct
  instruction and must be audited separately from a SELL signal.
- Keep the fixed quote amount (currently **3 USDT by default**, owner
  configurable); never size from signal text. Do not turn the existing
  configurable 50 USDT aggregate-budget *reference* into a hard cap.
- Do not adopt, repair, protect, cancel, or close pre-existing
  `PENDLEUSDT`/`MYXUSDT` positions. Reconciliation is scoped to exact
  bot-created IDs and intents. Never infer ownership by symbol, balance, price,
  or approximate timestamp.
- Do not replay backfill/recovery/boundary traffic, rejected or ambiguous
  signals, or duplicate signals into new intents. An existing same-intent
  `planned` recovery is distinct from creating a second signal/order and may
  only use the existing single-claim proof that no write was claimed; this
  journal/reconciliation feature must never invoke signal processing or that
  resume path. An uncertain write is never retried automatically.
- Distinguish, in storage and wording: local eligibility/rejection, a persisted
  intent, a write attempt, an exchange response/acknowledgement, a query-observed
  exchange state, fill evidence, and reconciled position evidence. An attempt
  is not an acknowledgement; an acknowledgement is not an open-order query; an
  open order is not a fill; an order fill is not by itself a fully closed trade.
- Keep OCO group, TP leg, stop leg, sibling-cancel status, fills, remaining
  bot-tracked position, and trade outcome separate. An executed leg plus a
  confirmed canceled sibling is an **exit**, not a canceled trade. A partial
  fill or canceled OCO group does not prove the full position is flat.
- Audit writes are independent of Telegram delivery. Notification settings
  never disable journal writes. Local investigation is read-only and does not
  construct an exchange client.
- Do not add exchange endpoints or assume undocumented MEXC/CLI response
  fields. If the current adapter cannot establish a fact, store/report
  `unknown`/`not_observed`, not a guessed status.
- No real test order without explicit, specific owner approval. This design,
  its unit tests, and its read-only investigation routes must not issue one.
  Do not change live authorization, control locks, current environment/config,
  service units, or deployment state as part of this design task.

## 2. Repository assessment (what exists today)

### 2.1 Existing durable objects

`tg_signal_ingest/db.py` contains migrations 1–16. Relevant current tables are:

| Current table | What it already represents | Important limit for this design |
| --- | --- | --- |
| `messages`, `message_edits`, `processing_queue` | Message identity, current content, prior text/caption when an edit is observed, and versioned queue work | `messages.raw_json` is overwritten on edit. `message_edits` keeps prior text/caption, not a complete immutable copy of every source field. A backfill snapshot is not proof of the original message revision. |
| `signals` | Parsed signal rows keyed by `(channel_id, message_id, edit_version)`, parser version, fingerprint and local status | It does not provide an append-only decision/event timeline or consistently separate source, occurrence, and record times. |
| `order_intents`, `order_status_history` | Idempotent intent, client order ID, current intent state and state-history rows | `submitting`/`submission_claimed` is a local claim before the remote call; it is not proof that bytes reached MEXC. History notes are free text. |
| `exchange_orders`, `order_fills`, `reconciliation_results` | Latest normalized order view, idempotent fills, and reconciliation outcomes | `exchange_orders` is a current projection, not a request/response ledger. `reconciliation_results` has no durable run/operation envelope or structured evidence snapshot. |
| `trades`, `trade_orders`, `order_protection` | Trade workflow, TP/SL-like order records and legacy attached-protection verification | There is no dedicated OCO group/child-leg model or independent position/outcome projection. |
| `audit_events` | Sparse free-text rows (`correlation`, category, event, detail, created time) | Useful legacy breadcrumbs, not a structured forensic journal. Existing code sometimes writes a transition and audit row in the same transaction, but other decisions and notifications are separate. |
| `trading_notifications` | Durable Telegram outbox with `(event_key, role)` uniqueness and `pending`/`sent`/`failed` states | Routing is hard-coded; enqueue is generally after the business transition, not in the same transaction; no per-recipient policy, lease, scheduled retry, delivery-attempt history, or saved Telegram message ID. |

`TradingStore.claim_intent_for_submission()` and `transition_intent()` write
some state/history/audit rows atomically. `TradeStore.update_trade_state()` also
writes a legacy audit row in its transaction. By contrast, `update_trade_fields()`,
`transition_trade_order()`, some signal decisions, and notification enqueueing
do not create a complete atomic event/outbox record. A remote MEXC effect can
never be in the same SQLite transaction; the design must make the before/after
evidence explicit instead of claiming distributed atomicity.

### 2.2 Current execution and evidence boundaries

- `TradingEngine.process_item()` stores/classifies signals, rejects prohibited
  or invalid inputs, checks replay/boundary and duplicate/conflicting edits,
  runs symbol/risk checks, and persists an intent before execution.
- A live submission claims `planned -> submitting` before the network call.
  Definite exchange rejections are distinguished from network/5xx/malformed or
  duplicate outcomes. Uncertain writes become
  `unknown_requires_reconciliation`; the existing code does not blindly retry
  those. A duplicate queue delivery can find an already-existing `planned`
  intent; the journal feature must never initiate that signal path itself.
- MEXC reads use the REST adapter in the live hybrid client; live writes use the
  official CLI adapter. The CLI uses argv (not a shell), `--no-retry` for
  writes, does not auto-confirm with `--yes`, strips API-key/secret and proxy
  environment variables, and treats a non-JSON nonzero write result as
  uncertain. Do not persist CLI argv, environment, raw stdout/stderr, signed
  parameters, secrets, or whole raw response objects.
- Supported local read interfaces include existing order query by exchange
  order/client order ID and `my_trades` by known order ID. The CLI client-ID
  lookup is limited to its existing current/history listing behavior; an
  incomplete listing is not evidence that an order does not exist. Do not add a
  guessed endpoint or turn “not found” into “not submitted.”
- `MexcCliAdapter._normalize_place()` and `_normalize_oco()` can normalize a
  missing/unknown returned status to `NEW`; record the raw-field-presence and
  response provenance in the new evidence layer. Do not present that fallback
  as an independently queried `NEW` state. An order ID in a successful write
  response is an acknowledgement fact, not fill proof.
- Existing fills are keyed by exchange order/trade ID and terminal fill sync is
  retried until complete. Preserve the distinction between order status and
  fill rows.
- Live recovery already queries uncertain entries rather than resubmitting
  them. Existing startup recovery may perform the trading lifecycle's own
  bounded actions for certain persisted trade states; the new investigator and
  notification callbacks must not call those recovery/write methods.

### 2.3 Notifications and routing today

`TradingNotifier` persists an outbox row before its best-effort send, has
idempotency by `(event_key, role)`, chunks longer messages, and retries failed
rows without submitting another order. Those are valuable existing properties;
the design replaces neither the journal nor the outbox with transient Telegram
messages.

Routing is nevertheless hard-coded. The current routing tests assert that both
owner and developer receive genuine signal notifications, local BUY rejects,
ignored Telegram SELL/SHORT instructions, and order/trade lifecycle updates;
`NOT_A_SIGNAL` diagnostics go to the developer only. The requested default is
different: developers receive meaningful decisions/rejects/diagnostics, while
the owner defaults to real live exchange activity and verified trading
lifecycle changes, not routine local rejects or ignored signals. Update those
tests when implementing configurable policy.

The notifier's `notify()` calls `flush()` synchronously after enqueue. Delivery
errors do not fail trading, but a slow Bot API call can still delay the caller.
The live reconciliation worker also flushes the outbox; `BotApiClient` does not
currently expose Telegram `retry_after` as a typed rate-limit value. The new
dispatcher must be independent of order execution and honor Telegram rate
limits/FloodWait-like retry instructions.

Default notification text currently includes the original message text via
`TradingEngine._original_text()`. That resolver can read the current message
row, not necessarily the exact revision that produced the event. New default
messages should be concise and avoid quoting user-supplied source text; a
recipient-scoped read-only detail route may show the correct stored revision.

### 2.4 Telegram control/management routing discrepancy

At this branch, `main.py::cmd_serve()` calls `_build_control_runtime()` when
`cfg.control.enabled`, passes the `ControlBot` and trading engine to
`IngestService`, and `IngestService.run()` starts `_control_worker()`. The
current `ControlBot` polls `getUpdates` and authorizes its existing controls
against the owner numeric Telegram ID only. `ManagementBot` is a separate
owner/developer polling application, deliberately without trading, and uses the
same control/serve locks. Do **not** add a second `getUpdates` consumer.

`AGENTS.md`, `docs/architecture.md`, and focused bot documentation contain
statements that the legacy control bot is not embedded in `serve`; those
statements disagree with the executable `main.py`/`service.py` path. Code and
tests are authoritative. The implementation stage must reconcile the stale
documentation without changing existing locks or enabling concurrent polling.

The Bot API client already provides direct verified HTTPS, redacts the token in
its representation/network errors, and exposes `sendMessage`, `editMessageText`,
`answerCallbackQuery`, and `getUpdates`. The current control callback handler
is owner-only. Add new notification/investigation permissions through a
role-aware, narrowly scoped router; do not give the developer owner trading
controls as a side effect.

The control module also has persisted approval rows/callbacks and defines
`ControlBot.should_require_approval()`. In the inspected source, that method has
no call site in the trading path: after an intent is planned,
`TradingEngine.process_item()` proceeds to `_execute_intent()` unless the
conditional-entry manager defers it. Do not describe an intent as “owner
approved” merely because the control bot or approval UI exists. Audit an
approval request/decision only when a persisted `control_approvals` row and its
actual decision are linked to the intent; verify any proposed approval-gate
wiring independently rather than silently adding it in this audit feature.

### 2.5 OCO and position semantics today

`TradeManager._place_oco_exits()` submits two native OCO SELL operations after
entry fills are reconciled: one allocation at TP1 and the other at TP2, both
using the configured stop price. Today each submission is stored as one
`trade_orders` row keyed by `tp1`/`tp2`; that row holds one client/order ID and a
group-like state. It is not two child-leg rows. The adapter's normalized
detail is order-level (one ID/status/quantity); current persistence does not
track separate TP/SL child IDs, per-leg observations, or sibling cancellation.

Adapter comments/docs describe OCO intent, but the repository has no verified
live child-leg response fixture establishing the exact exchange-side status
mapping. Therefore:

- Treat group acknowledgement, group query status, child TP status, child stop
  status, and sibling cancellation as different facts.
- Create local *desired* TP and stop legs for the two configured groups, but
  leave exchange child states `not_observed` unless the existing adapter
  returns a specific supported field that is covered by a fixture/test.
- Do not infer which leg executed or that a sibling canceled from one group ID,
  order-level `FILLED`, or a partial fill. Do not create a new MEXC endpoint.
- Keep `trade_state`, OCO group state, each leg state, fill totals, remaining
  bot-tracked base, residual status, and trade outcome separate. A fully-flat
  outcome requires complete linked fill evidence and no unresolved/open exit;
  otherwise show partial, residual, or unknown.

### 2.6 Existing operating constraints relevant to this feature

The parser/risk path is Spot-only and rejects short/sell entries. The default
fixed quote amount is 3 USDT and remains owner-configurable. The tracked
aggregate-budget number is a configurable reference, not a hard 50 USDT order
cap. External/pre-existing orders are not adopted/canceled/counted as local
orders. Historical/backfill/recovery traffic and the persisted activation
boundary are non-tradeable. Preserve these invariants; do not infer extra risk
limits or exchange capabilities from older docs/config fields.

## 3. Core event and timestamp contract

### 3.1 Event envelope

Every new journal event uses a versioned, structured envelope:

| Field | Meaning |
| --- | --- |
| `event_id` | Generated stable ID for this journal row. |
| `event_key` | Unique idempotency key for this logical event; repeated delivery/reconciliation must not create a second identical state-transition event. A distinct network attempt gets its own operation/event ID. |
| `event_type`, `category`, `schema_version` | Stable machine-readable event vocabulary and payload version. Never use rendered Persian text as the event type. |
| `correlation_id`, `causation_event_id` | End-to-end chain and immediate causal event, where known. For an initial signal use a stable signal/message correlation key; for children use the event that caused the transition. |
| Entity refs | Nullable `channel_id`, `message_id`, `revision_no`, `signal_id`, `intent_id`, `trade_id`, `trade_order_id`, `oco_group_id`, `exchange_operation_id`, `exchange_order_id`. These are references, not claims that every object exists. |
| `source_system`, `actor_role` | Examples: `telegram_mtproto`, `trading_engine`, `mexc_rest`, `mexc_cli`, `telegram_bot_api`; `system`, `owner`, `developer`, or `unknown`. Do not store credentials or arbitrary usernames. |
| `occurred_at_utc` | UTC time the application observed/decided the event. Not the source message time. |
| `recorded_at_utc` | UTC time the journal transaction recorded it. Separate from occurrence time. |
| `source_timestamp_raw`, `source_timestamp_kind`, `source_timestamp_utc` | Original upstream timestamp/text, its meaning (for example `telegram_message_date` or `telegram_edit_date`), and a normalized UTC value only when conversion is certain. Never substitute it for `occurred_at_utc`. |
| `exchange_timestamp_ms`, `exchange_timestamp_utc` | Exact exchange timestamp field if returned, plus a normalized UTC rendering when valid. Null if unavailable; never manufacture an exchange time from local receive time. |
| `payload_json` | Compact, allow-listed, versioned, sanitized factual data. No raw HTTP bodies, credentials, raw CLI output, signed params, Telegram `raw_json`, or unbounded exception text. |

Serialize application UTC instants consistently as ISO-8601 with `Z` and
sub-second precision. Use monotonic clocks only for elapsed durations; if needed
persist `duration_ms` separately, never as an absolute timestamp.

The Telegram delivery timeline has its own fields: `notification_created_at_utc`,
`last_attempt_at_utc`, `sent_at_utc`, plus the Telegram `message_id` returned by
the Bot API when available. A successful local `sendMessage` response is a
Telegram delivery acknowledgement; it is not an exchange event.

### 3.2 Event transaction rule

For a local business transition, write the updated projection, one
`trading_audit_events` row, recipient-routing decisions/outbox rows, and any
status-history row in the **same SQLite transaction**. Commit before any
network call. No Telegram/MEXC call occurs while a SQLite write transaction is
open.

For an exchange write, commit a sanitized operation row and
`exchange.write_attempted` event before entering the adapter. After the call,
commit the classified response/uncertainty and its event in a new transaction.
A process crash between these boundaries leaves an honest `attempted`/unknown
operation for reconciliation; SQLite cannot make an exchange write atomic.

For exchange queries, record the operation and response evidence. Update a
current projection only from exact, validated query/fill evidence, and append
the state-change event in the same transaction as that projection update.
Repeated identical polls may create operation evidence, but must not emit
repeated lifecycle notifications when no material state changed.

## 4. Event catalog

The journal records meaningful events even if no recipient is configured, a
category is off, or Telegram is unavailable. Do not emit one durable event for
every price tick or every historical row; record relevant boundaries, decisions,
operations, state changes, anomalies, and reconciliation observations.

| Event family / event types | Required factual payload | Developer default | Owner default |
| --- | --- | --- | --- |
| `message.observed`, `message.revision_observed`, `message.deleted` | Message/revision refs; source (`live`/`recovery`/`backfill`); source and observed timestamps; content hash; no raw Telegram object | Record always. Notify for meaningful **live** parser/decision outcomes; do not flood with bulk backfill/recovery. | Audit only unless it causes a live exchange operation or action-needed lifecycle change. |
| `processing.claimed`, `processing.failed`, `processing.retry_scheduled`, `processing.boundary_suppressed` | Queue ID/type/version, attempt count, safe failure code, whether any trading action was attempted | Yes for failures, repeated retries, and decision-relevant suppression; dedupe routine success. | Only action-needed service failures; no routine retry chatter. |
| `signal.classified`, `signal.eligible`, `signal.rejected_local`, `signal.ignored`, `signal.duplicate_suppressed`, `signal.superseded`, `signal.cancel_instruction_observed` | `signal_id`, parser version, revision, signal kind/action/symbol if present, safe risk/parse codes, local outcome, `submission_attempted=false` where applicable; when used, record reference-price/source/time, symbol-metadata version/hash, fixed quote amount/source, policy version, and risk-gate results | Yes, including live ignored/non-signal classifications, local rejects, SHORT/SELL ignores, and diagnostics. | No routine rejects, ignored signals, non-signals, duplicate/superseded notices, or simulated activity. |
| `approval.requested`, `approval.approved`, `approval.denied`, `approval.expired` | Linked approval/intent IDs, exact decision actor/role and timestamps; no sensitive input. Emit only from an actual persisted approval state transition. | Yes when present. | Yes for an actual owner decision; no inferred approval. |
| `intent.created`, `intent.deferred`, `intent.submission_claimed`, `intent.local_abort`, `intent.cancel_requested` | Intent/idempotency refs, fixed quote/quantity/price snapshot, mode, local decision code, preflight result, trigger evidence if used | Yes. | Only when a live external action is about to occur or a live position needs attention; local planned/deferred status is opt-in. |
| `exchange.operation_prepared`, `exchange.write_attempted`, `exchange.write_acknowledged`, `exchange.write_rejected`, `exchange.write_uncertain`, `exchange.cancel_acknowledged` | Operation ID, operation kind, backend, sanitized request summary/hash, call/response times, exact response-field presence, error class/code, exchange order/client IDs if returned | Yes, for all meaningful order/exit writes and read-only reconciliation calls. | Yes for live write attempts and their outcomes, including uncertain and definitive exchange rejection. Do not route `/order/test` as a real order; test-only is opt-in. |
| `order.status_observed`, `fill.observed`, `fill_sync.pending`, `fill_sync.completed`, `fill_sync.discrepancy` | Query/operation ref, observed status/source/time, exact quantities, fees/assets, fill identity, sync completeness | Yes for material changes, incomplete sync, discrepancies. | Yes for live observed status changes/fills, unresolved fill sync, and discrepancies; no unchanged polling. |
| `trade.state_changed`, `trade.position_changed`, `trade.residual_recorded`, `trade.outcome_confirmed` | Trade workflow state, independent position state/remaining qty, source fills, OCO refs, residual, outcome evidence and certainty | Yes. | Yes for verified live lifecycle changes and any partial/residual/unknown position. |
| `oco.group_prepared`, `oco.group_write_attempted`, `oco.group_acknowledged`, `oco.group_observed`, `oco.leg_observed`, `oco.sibling_cancel_observed` | Group/local leg refs; intended TP/stop parameters; group order ID; observed field provenance; per-leg evidence; explicit sibling cancellation evidence or `unknown` | Yes, including unknown/partial protection. | Yes for live OCO attempt/ack/state and any protection gap; never call an unobserved leg “active.” |
| `reconciliation.started`, `reconciliation.consistent`, `reconciliation.resolved`, `reconciliation.discrepancy`, `reconciliation.ambiguous`, `reconciliation.not_found` | Run and operation IDs, local snapshot refs, observed exchange fields, exact comparison result, unresolved gaps | Yes for discrepancies, ambiguity, not-found, and material resolution; successful unchanged polls may be summarized. | Yes for unresolved/changed live lifecycle or position; ordinary unchanged success is quiet. |
| `notification.policy_changed`, `notification.suppressed`, `notification.delivery_attempted`, `notification.sent`, `notification.retry_scheduled`, `notification.dead_letter`, `notification.delivery_uncertain` | Event/recipient role, policy version/category, attempt/outcome/retry-after, Telegram message ID; never token | Journal always; dispatcher errors and dead letters notify developer. | A failure to deliver to the owner is action-needed; delivery of the owner's own preference change can be acknowledged. |
| `control.setting_changed`, `control.paused`, `control.resumed`, `control.callback_denied`, `control.detail_viewed` | Safe setting name/old/new non-secret value or redacted marker; actor role; event ref; authorization result | Yes for meaningful operational/security changes and repeated denials. | Yes for owner trading-control changes and security/action-needed events. |

Notification delivery events are non-recursive: journal every attempt/result,
but never notify the same failed recipient about that recipient's send failure.
At most send one deduplicated action-needed warning to the other configured
recipient; if both recipients are unavailable/muted, leave the failure visible
in the local read-only report. Never create an unbounded chain of
“notification about notification” messages.

Events that are always journaled but need not be sent include repeated identical
status polls, raw routine price ticks, normal callback acknowledgements,
individual historical/backfill suppression rows, and notification routing
suppression for every ordinary low-priority event. Aggregate repeated noise by
window and retain counts/first/last times, without losing the underlying
business event or unresolved exception.

## 5. Proposed schema and migration plan

Current schema head is 16. Add ordered, additive migrations only; do not rewrite
an applied migration. The table definitions below specify columns and invariants
for implementation. SQLite stores decimal quantities/prices as canonical
strings; do not use binary float for money/quantity comparisons.

Target relationship map (existing tables retained unless marked new):

```text
messages ── message_revisions ── signals ── order_intents ── exchange_orders ── order_fills
                                            │                                      │
                                            ├── exchange_operations ── reconciliation_observations
                                            │                                      ▲
                                            └── trades ── trade_orders ── oco_groups ── oco_legs
                                                          │            │
                                                          └── exact exchange IDs ────────┘
                                                                 │
order_fills ── exact bot-owned IDs ── trade_position_projection ── trade_outcome_projection

reconciliation_runs ── reconciliation_observations
trading_audit_events ──< references/correlation/causation for all of the above
        │
        └── notification_outbox (one routing/delivery row per event × role)
                    └── notification_attempts
```

`order_fills` remains the canonical exchange fill store; a fill is associated
to a trade only through an exact bot-owned exchange order ID. The event journal
uses soft entity references plus stable correlation/causation IDs so a missing
projection does not erase evidence. The outbox has a hard foreign key to its
journal event.

### 5.1 Migration 17 — source revisions and structured journal

#### `message_revisions`

One metadata row per locally observed source revision. Do not duplicate
`messages.raw_json` or store a second copy of message text. Resolve text/caption
through the existing `messages` current row or `message_edits` row using
`(channel_id, message_id, revision_no)`; `content_sha256` supports checking the
resolved content when available.

| Column | Type / rule |
| --- | --- |
| `revision_id` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `channel_id`, `message_id`, `revision_no` | `INTEGER NOT NULL`; unique `(channel_id, message_id, revision_no)`. `revision_no` is this repository's local `processing_version`, not a claimed Telegram edit counter. |
| `source` | `TEXT NOT NULL CHECK IN ('live','recovery','backfill','legacy_backfill')` |
| `capture_basis` | `TEXT NOT NULL CHECK IN ('live_event','recovery_snapshot','backfill_snapshot','legacy_snapshot')`; makes a fetched current snapshot distinct from a captured live edit. |
| `telegram_message_at_raw`, `telegram_edit_at_raw` | Nullable source timestamp strings. |
| `telegram_message_at_utc`, `telegram_edit_at_utc` | Nullable normalized UTC values only if timezone conversion is certain. |
| `observed_at_utc`, `recorded_at_utc` | Observation and DB-record times, kept distinct. |
| `content_sha256` | Nullable for migrated history whose exact hash was not previously computed; for new revisions compute SHA-256 over canonical UTF-8 JSON with exactly the nullable `text` and `caption` keys (sorted keys, compact separators, `ensure_ascii=False`). |
| `source_fields_mask` | Optional compact versioned list of which parser-relevant source fields were present; not raw message JSON. |

Indexes: `(channel_id, message_id, revision_no)` (unique already),
`(source, observed_at_utc)`, and `(recorded_at_utc)`. Insert the initial revision
when a message is first stored and each new local revision in the same
`Repository.upsert_message()` transaction as the current-row/edit snapshot and
queue row. For legacy rows, backfill metadata from `messages` plus
`message_edits`, preserve the available `first_seen`/`captured_at`/edit time, set
`capture_basis='legacy_snapshot'`, and leave uncomputable hashes/timestamps
null. Do not describe a backfill snapshot as an original pre-edit capture.

#### `trading_audit_events`

| Column | Type / rule |
| --- | --- |
| `event_seq` | `INTEGER PRIMARY KEY AUTOINCREMENT`; internal stable ordering only. |
| `event_id` | `TEXT NOT NULL UNIQUE`; generated ID. |
| `event_key` | `TEXT NOT NULL UNIQUE`; idempotent logical event key. |
| `event_type`, `category` | `TEXT NOT NULL`; stable vocabulary from §4. |
| `schema_version` | `INTEGER NOT NULL DEFAULT 1`. |
| `correlation_id` | `TEXT NOT NULL`; join key across the signal/trade workflow. |
| `causation_event_id` | Nullable event ID. |
| Entity refs | Nullable `channel_id`, `message_id`, `revision_no`, `signal_id`, `intent_id`, `trade_id`, `trade_order_id`, `oco_group_id`, `exchange_operation_id`, `exchange_order_id`. Soft references intentionally preserve history if a projection is unavailable. |
| `source_system`, `actor_role` | `TEXT NOT NULL` / nullable `TEXT`; enum-like values documented in code. |
| `occurred_at_utc`, `recorded_at_utc` | `TEXT NOT NULL`; separate UTC instants. |
| `source_timestamp_raw`, `source_timestamp_kind`, `source_timestamp_utc` | Nullable original source timestamp, its declared meaning, and certain normalized UTC equivalent. |
| `exchange_timestamp_ms`, `exchange_timestamp_utc` | Nullable raw exchange epoch and certain UTC representation. |
| `payload_json` | `TEXT NOT NULL`; versioned allow-listed facts, bounded in application code (suggested maximum 32 KiB). |

Indexes: `(correlation_id,event_seq)`, `(event_type,occurred_at_utc)`,
`(category,occurred_at_utc)`, `(channel_id,message_id,revision_no)`, and entity
lookup indexes for `intent_id`, `trade_id`, and `exchange_order_id`.

Journal APIs expose `INSERT` only. Existing `audit_events`,
`order_status_history`, and `reconciliation_results` remain intact and
queryable as **legacy/unstructured** sources; do not fabricate complete write
attempts or exchange responses from them. A read-only report may union them into
a timeline only with explicit provenance labels. SQLite is durable local
storage, not WORM storage or cryptographic proof against an administrator who
can edit the file; do not describe it as tamper-proof.

### 5.2 Migration 18 — exchange-operation and reconciliation evidence

#### `exchange_operations`

One row per external MEXC operation attempt/observation, not one row per trade.
The state column is a current summary; every state transition is also an
append-only journal event.

| Column | Type / rule |
| --- | --- |
| `operation_id` | `TEXT PRIMARY KEY`. |
| `operation_key` | `TEXT NOT NULL UNIQUE`; dedupe key for this single operation attempt. Separate calls have separate keys. |
| `operation_kind` | `TEXT NOT NULL`; allow-listed values such as `place_entry`, `place_oco`, `cancel_order`, `query_order`, `query_fills`, `account_preflight`, `test_order`. |
| `backend` | `TEXT NOT NULL CHECK IN ('rest','cli','hybrid','unknown')`; no profile/credential string. |
| Entity refs | Nullable intent/trade/trade-order/OCO/order/client-order IDs and symbol/side/type when applicable. |
| `operation_state` | `TEXT NOT NULL CHECK IN ('prepared','attempted','acknowledged','exchange_rejected','uncertain','query_observed','query_not_found','query_ambiguous','local_failed')`. `acknowledged` does not mean filled/open/canceled. |
| `request_summary_json` | `TEXT NOT NULL`; allow-listed symbol, side, type, exact decimal strings, client ID, and safe control fields only. |
| `request_sha256` | `TEXT NOT NULL`; hash of the canonical sanitized summary, not signed/raw request bytes. |
| `response_summary_json` | Nullable sanitized allow-list of returned IDs/status/quantities/time and field-presence flags. Never store `raw` CLI response. |
| `response_sha256` | Nullable hash of canonical sanitized response summary. |
| `error_class`, `exchange_error_code`, `safe_error_code` | Nullable normalized classification; no raw traceback/header/body. |
| `request_started_at_utc`, `response_received_at_utc`, `recorded_at_utc` | Separate UTC instants; response time nullable on crash/timeout. |
| `exchange_timestamp_ms` | Nullable exact exchange time. |
| `duration_ms` | Nullable elapsed duration. |

Indexes: `(operation_state,request_started_at_utc)`, `(intent_id,request_started_at_utc)`,
`(trade_id,request_started_at_utc)`, `(client_order_id)`, and
`(exchange_order_id)`.

#### `reconciliation_runs` and `reconciliation_observations`

`reconciliation_runs`: `run_id TEXT PRIMARY KEY`, `run_kind`, `started_at_utc`,
`finished_at_utc`, `status` (`running`/`completed`/`partial`/`failed`), safe
scope JSON, initiator role/source. A runtime run is **read-only to MEXC**; it
may persist local observations/projections. A local-only investigation does
not insert a run row because it must not write to the inspected database.

`reconciliation_observations`: integer primary key, `run_id`, nullable
`operation_id`, `entity_type`, `entity_id`, `local_state`, nullable
`exchange_state`, `outcome` (`consistent`/`resolved`/`discrepancy`/`ambiguous`/
`not_found`), `detail_code`, sanitized evidence JSON, `exchange_timestamp_ms`,
`observed_at_utc`, and `recorded_at_utc`. Preserve the existing
`reconciliation_results` table as a compatibility projection; new observations
are the structured evidence.

### 5.3 Migration 19 — recipient policy, outbox, and safe callbacks

#### `notification_policy_overrides`

`recipient_role TEXT CHECK IN ('owner','developer')`, `category TEXT`,
`enabled INTEGER CHECK IN (0,1)`, `policy_version TEXT`,
`updated_by_user_id INTEGER`, `updated_at_utc TEXT`, primary key
`(recipient_role,category)`. Missing row means use the versioned code default.
Never seed a value that changes the default silently during migration.

#### `notification_recipient_state`

`recipient_role TEXT PRIMARY KEY`, `muted INTEGER NOT NULL DEFAULT 0 CHECK IN
(0,1)`, nullable `muted_until_utc`, `updated_by_user_id`, `updated_at_utc`.
Mute applies only to delivery. Keep recipient IDs in configuration; they are not
policy keys.

#### `notification_outbox`

One routing decision per journal event and recipient role, including a durable
suppression result when applicable. This makes “not sent because muted/off”
explainable without changing the journal.

| Column | Type / rule |
| --- | --- |
| `notification_id` | `INTEGER PRIMARY KEY AUTOINCREMENT`. |
| `event_id` | `TEXT NOT NULL REFERENCES trading_audit_events(event_id)`. |
| `recipient_role`, `chat_id`, `category`, `policy_version` | Role is owner/developer; `chat_id` nullable if unconfigured; category/default snapshot retained. |
| `routing_decision` | `queued` / `suppressed_policy` / `suppressed_muted` / `recipient_unconfigured`. |
| `delivery_status` | `pending` / `leased` / `sent` / `retry` / `dead_letter` / `delivery_uncertain` / `suppressed_policy` / `suppressed_muted` / `recipient_unconfigured` / `recipient_changed`. |
| `template_key`, `template_version`, `rendered_text` | Exact sanitized Persian rendering to deliver; keep text concise. No raw message text by default. |
| `attempt_count`, `created_at_utc`, `next_attempt_at_utc`, `last_attempt_at_utc`, `sent_at_utc` | Delivery state and separate notification times. |
| `telegram_message_id` | Nullable returned Telegram message ID. |
| `lease_owner`, `lease_expires_at_utc` | Nullable worker lease preventing simultaneous sends. |
| `last_error_code`, `retry_after_s` | Safe code and server-directed delay; never raw token-bearing URL. |
| uniqueness | `UNIQUE(event_id,recipient_role)`; a policy change does not create another delivery for the old event. |

#### `notification_attempts` and `notification_callbacks`

`notification_attempts`: attempt ID, notification ID, attempt number, start/end
UTC times, outcome (`sent`/`retryable`/`permanent`/`uncertain`), safe error
code, `retry_after_s`, and Telegram returned message ID(s) if any. Do not
persist unredacted Bot API request/response bodies.

`notification_callbacks`: hash (not raw value) of a random short callback
token, journal event ID, recipient role, action limited to `detail` or
`preferences`, expiry/creation/used UTC times. Callback data stays under
Telegram's 64-byte limit. The handler resolves role/user/chat from current
configuration and the stored token; never trust an ID, role, or chat address
embedded in callback data.

Indexes: outbox `(delivery_status,next_attempt_at_utc)`,
`(recipient_role,created_at_utc)`, attempts `(notification_id,attempt_no)`, and
callbacks `(expires_at_utc,used_at_utc)`.

Do not auto-replay legacy `trading_notifications` rows into the new outbox.
That could send stale/historical messages. Leave the legacy table readable and
show its pending/failed count as legacy until an owner-authorized, explicit
migration/handling plan exists.

### 5.4 Migration 20 — OCO groups/legs and position outcome projection

#### `oco_groups`

One row per native `place_oco` submission; current `trade_orders` remains the
compatibility/local submission row.

`oco_group_id TEXT PRIMARY KEY`, `trade_id TEXT NOT NULL`,
`trade_order_id TEXT UNIQUE NOT NULL`, `allocation_kind` (`tp1`/`tp2`),
`client_order_id`, nullable `exchange_order_id`, allocated quantity as decimal
text, desired TP price, desired stop trigger and stop-limit price, local
submission state, nullable normalized exchange group status, status provenance
(`local_intent`/`write_response`/`exchange_query`/`legacy_backfill`), last
observed UTC/exchange time, created/updated UTC times. Distinguish
`acknowledged` from `open_observed`; use `unknown` where no query evidence
exists.

#### `oco_legs` and `oco_leg_observations`

Create two **desired** local leg rows per group: `take_profit` and `stop_loss`.
Each row has a local leg ID, group ID, `leg_kind`, desired price/trigger/limit,
desired quantity, nullable exchange child ID, current `observed_state`
(`not_observed`/`unknown`/`open`/`partially_filled`/`filled`/`canceled`/
`rejected`/`expired`/`other`), `sibling_cancel_state`
(`unknown`/`not_requested`/`request_attempted`/`acknowledged_unconfirmed`/
`confirmed`/`not_applicable`), last observation time, and provenance.

`oco_leg_observations` is append-only: observation ID, leg ID, nullable
operation ID, normalized state, source-field name/version, raw exchange time
when supplied, observed/recorded UTC times, and event ID. No child ID/status is
invented. If the existing response only identifies the group/order, leave both
child states `not_observed` and sibling status `unknown`.

#### `trade_position_projection` and `trade_outcome_projection`

Keep remaining position and trade outcome as separate projections; neither is
`trades.trade_state` (the existing workflow state).

`trade_position_projection`: one row per trade with `trade_id PRIMARY KEY`,
`position_state` (`no_confirmed_position`/`open`/`partially_exited`/
`flat_confirmed`/`residual_below_minimum`/`unknown`/`legacy_unverified`),
confirmed acquired and disposed base amounts, nullable remaining base amount,
fill-sync completeness/quality, and last evidence/reconciliation UTC time.

`trade_outcome_projection`: one row per trade with `trade_id PRIMARY KEY`,
`outcome_state` (`in_progress`/`partial_exit`/`closed_confirmed`/
`closed_with_residual`/`unknown`/`legacy_unverified`),
`outcome_reason` (`take_profit`/`stop_loss`/`other_confirmed`/`unknown`),
nullable realized quote PnL and its calculation-quality code, evidence event ID,
and determined/updated UTC times. `closed_confirmed` is allowed only when
position state is `flat_confirmed`; if workflow completion leaves a known
below-minimum amount, use `closed_with_residual`, not a full-close outcome. If
fill completeness, fee denomination, or exchange linkage is unknown, use
`unknown` and leave realized PnL null. Record both projection changes in
`trading_audit_events`.

Derive position only from fills linked to this bot's exact entry/exit orders and
verified fill-completeness evidence. Apply a base-asset fee adjustment only
when the adapter's fee semantics are established; otherwise position quality
is `unknown`. Do not use account balance to attribute a position to a bot
trade; it may include unrelated/manual holdings. `flat_confirmed` requires
complete relevant fill sync, exact remaining quantity zero (not merely below
minimum), and no unresolved/open bot exit. A below-minimum amount is a recorded
residual, not a flat position. Keep `trades.trade_state` as the existing
workflow projection during rollout; never display `closed` as “fully flat” if
the position projection is residual/unknown.

#### Legacy OCO/position backfill

Backfill OCO groups only from existing `trade_orders` whose local type is the
known OCO type. Use `legacy_backfill` provenance, group state
`legacy_unverified`, and per-leg `not_observed`/sibling `unknown`; do not
populate child exchange IDs. Initialize both existing position and outcome
projections as `legacy_unverified`/unknown, not flat/open/closed based on a
terminal workflow label alone. Reconcile only exact bot-owned existing order
IDs if the established runtime is later explicitly authorized to do so; do not
scan/adopt external orders or PENDLEUSDT/MYXUSDT holdings.

## 6. Distinct state machines and claims

### 6.1 Source/signal decision

Conceptual lifecycle (the existing `signals.status` remains the compatibility
projection):

```text
observed_revision -> parsed/classified -> eligible_local -> intent_planned
       |                    |                 |               |
       |                    +-> ignored       +-> rejected_local
       |                    +-> rejected_local
       |                    +-> duplicate_suppressed / superseded
       +-> deleted_observed -> cancel_instruction_observed (if applicable)
```

Each edit is a new local revision/signal version; do not mutate an earlier
signal's decision history. `eligible_local` means only that local parsing/risk
checks passed. It does not mean the exchange accepted an order. `ignored`
(non-signal, SHORT/SELL, result/info, etc.) is different from a local risk
rejection. A message deletion or cancel signal is a source instruction
observation and, if applicable, a later cancel operation; it is not proof an
exchange order canceled.

### 6.2 Intent, write attempt, exchange state, fills, reconciliation

Store these as separate facets, not one ambiguous `status`:

| Facet | Proposed values / progression | Evidence rule |
| --- | --- | --- |
| Local decision | `planned`, `deferred`, `rejected_local`, `canceled_local`, `superseded`, `duplicate_suppressed` | Written by application decision code. No network claim. |
| Approval | `not_required`, `pending`, `approved`, `denied`, `expired`, `unknown` | Separate facet, populated only from a linked persisted approval record/decision. The current engine path does not call `should_require_approval()`; never infer `approved` from an intent or control-bot menu. |
| Write operation | `not_attempted -> prepared -> attempted -> acknowledged` OR `exchange_rejected` OR `uncertain` | `attempted` event commits immediately before adapter call. A crash after it is conservatively uncertain. Use explicit error classification; duplicate-client-ID errors remain uncertain despite exception inheritance. |
| Exchange order projection | `unobserved`, `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, `PARTIALLY_CANCELED`, `UNKNOWN` (plus exact adapter-supported values) | Keep `status_source` (`write_response` vs `query`) and response field-presence. Only query evidence is an independent observed state. Unknown/unrecognized stays unknown. |
| Fill sync | `not_required`, `pending`, `complete`, `discrepancy` | Reconcile exact fill rows; order status alone does not create a fill row or make fill sync complete. |
| Reconciliation | `not_due`, `pending`, `consistent`, `resolved`, `discrepancy`, `ambiguous`, `not_found` | `not_found`/ambiguous do not terminalize a possibly existing order. |

`order_intents.state` remains for compatibility and current engine transitions;
the report should present the facets above. `submitting` or `submission_claimed`
means the local single-submit claim was taken, not that the exchange was reached.
A successful response with an order ID is `acknowledged`; it is not “filled” and
is not an open-order observation unless the source proves that status. A cancel
response acknowledges a cancel request, not a completed cancellation. Only a
later exact query/recognized response can set `CANCELED`.

For `sim`, report `simulated` and `exchange_operation=none`. For `testonly`,
record a `test_order` validation call and label it **no matching-engine order**;
never phrase it as a placed/real order. Never include an exchange order ID that
was not actually returned/observed.

### 6.3 Exchange operation

```text
prepared -> attempted -> acknowledged -> query_observed
                    |                -> query_not_found / query_ambiguous
                    +-> exchange_rejected
                    +-> uncertain -> query_observed / remains uncertain
```

`prepared` proves the local request summary was persisted. `attempted` means the
call boundary was entered; it is intentionally conservative if the process
crashes before bytes leave. `acknowledged` means the adapter returned a
successful acknowledgement fact. `exchange_rejected` is allowed only for a
classified definitive reject. Network timeout, 5xx, malformed write response,
duplicate order/client ID, and unknown CLI exit are `uncertain`. An uncertain
write gets read-only reconciliation only; no new order ID or blind retry.

Read operations have their own attempt/result evidence and do not mutate an
exchange order. A request to cancel is also a write operation; only an observed
exchange state confirms cancellation.

### 6.4 OCO and trade outcome

| Dimension | Independent state |
| --- | --- |
| OCO group | Local `prepared`/`attempted`; response `acknowledged`; query `open_observed`/`partially_executed`/`executed`/`canceled`/`rejected`; or `uncertain`/`legacy_unverified`. |
| TP child / stop child | Individually `not_observed`, `unknown`, or a status backed by an explicit supported child field/ID and an adapter fixture. Never derive a child status from the group's single order status. |
| Sibling cancellation | `unknown` until an explicit sibling-cancel observation; distinguish request attempted/acknowledged-unconfirmed/confirmed. |
| Trade workflow | Existing `trades.trade_state` (`pending_entry`, `entry_active`, `position_open`, `oco_*`, TP/SL, `exit_pending`, `closed`, `needs_reconciliation`) remains a workflow state. |
| Bot-tracked position | `no_confirmed_position`, `open`, `partially_exited`, `residual_below_minimum`, `flat_confirmed`, `unknown`, `legacy_unverified`, with exact remaining qty/quality. |
| Trade outcome | `in_progress`, `partial_exit`, `closed_confirmed`, `closed_with_residual`, `unknown`, `legacy_unverified`; outcome reason/PnL only when supported by linked complete evidence. |

An executed OCO leg with a canceled sibling can be an **exit**; it does not
mean the trade's entire position is closed. The two current `place_oco`
submissions are separate groups. Do not infer that an execution in one group
cancels another group. Compute remaining bot-owned position from complete linked
fills, not the last OCO state. Keep outstanding sell reservations separate from
physical remaining base so one is not mistaken for the other.

### 6.5 Notification delivery

```text
routing_decision -> pending -> leased -> sent
                                  |-> retry (scheduled/backoff)
                                  |-> dead_letter (permanent/exhausted)
                                  +-> delivery_uncertain (send may have succeeded)
policy-off/muted/unconfigured -> suppressed_* (journal event remains)
```

Telegram `sendMessage` has no local exactly-once guarantee: a timeout can occur
after Telegram accepted a message. Use a lease to stop concurrent workers and
persist each attempt. Honor Bot API `retry_after` exactly (with safe positive
jitter only after the mandated wait). Retry definite retryable failures with
capped exponential backoff; permanent recipient/permission failures go to a
visible dead-letter state. A send whose result is ambiguous becomes
`delivery_uncertain`, not an automatic rapid resend; a human-visible duplicate
risk must be explicit if an operator later chooses to requeue it. None of these
states affect order execution or the event journal.

## 7. Notification defaults, policy, and Persian templates

### 7.1 Effective default policy

Use a versioned code default plus per-role/category overrides. A missing policy
row means the default below. Keep `owner` and `developer` separate; do not
interpret “both configured” as “both receive everything.” The numeric Telegram
IDs continue to come from existing configuration.

| Category | Developer default | Owner default |
| --- | --- | --- |
| Live signal classification, parse diagnostics, local eligibility/rejection, ignored BUY/SELL/SHORT/non-signal result, duplicate/superseded diagnosis | **On** for meaningful live classifications and diagnostic outcomes | **Off** for routine local decisions, ignored signals, and non-signals |
| Live exchange write attempts/acknowledgements/definitive exchange rejects/uncertain writes/cancel requests | **On** | **On** when `mode=live`; wording reflects the exact stage. `testonly` is separate and off unless enabled. |
| Exchange-observed order status, fills, fill-sync gap/discrepancy | **On** for material change/problem | **On** for live material changes, unresolved gaps, and discrepancies |
| Live trade/OCO/position lifecycle, partial exit, residual, outcome | **On** | **On** when evidence is material; always surface partial/residual/unknown/needs-attention |
| Reconciliation ambiguity/not-found/discrepancy/material resolution | **On** | **On** for live unresolved or changed state; quiet on identical routine polls |
| System diagnostics, processing failures, notification delivery failure | **On**, with repeated identical errors aggregated | Critical/action-needed only; owner-delivery failures are visible to the owner |
| Historical/backfill/recovery rows, routine market ticks, unchanged status polls | Audit only by default; optionally enable aggregated diagnostics | Audit only by default |
| Policy/security changes and repeated unauthorized access | Journal always; developer sees meaningful alerts | Owner sees policy/control changes and critical security alerts |
| Simulation | **On** for meaningful decisions, clearly labelled simulated | **Off** by default (configurable) |

“Developer on” means all meaningful categories, including local rejects,
ignored live signal classifications and diagnostics; it does **not** mean
unbounded raw price ticks, bulk historical replay, or arbitrary raw Telegram
updates. Repeated parser/service noise must be rate-aggregated without dropping
its counts or first/last timestamps. The owner default emphasizes actual live
exchange activity and verified trading lifecycle changes; routine rejects and
ignored signals remain off unless the owner opts in.

Each recipient may change **their own** category overrides and mute state. The
owner's existing trading/control settings are not notification preferences.
The developer may view events delivered to the developer and change only the
developer's notification settings; this does not grant owner trading controls.
Both roles can view only their own delivered-event detail unless a later
explicitly authorized role policy expands that scope. Missing developer ID
means no developer delivery, not an error that suppresses journal events.

### 7.2 Safe Persian templates

Render plain text (no HTML/Markdown parse mode by default), with a short summary
and optional `🔎 جزئیات` callback. Escape/strip control characters from untrusted
fields; keep messages below Telegram's length limit. Use a safe Persian
reason-code map; unknown errors display a short class/code, not an exception
trace or raw response. Examples (placeholders are generated from validated
fields):

**Local reject (developer by default; owner opt-in)**

```text
🛠 تصمیم محلی — رد شد
نماد: {symbol_or_dash}
دلیل: {safe_reason_fa}
درخواست به MEXC ارسال نشد.
مرجع: پیام {message_id}، نسخه {revision_no}
```

**Write attempt with uncertain outcome (owner + developer for live)**

```text
⚠️ نتیجهٔ درخواست سفارش نامشخص است
نماد: {symbol} — جهت: خرید (Spot)
مقدار درخواستی: {quantity_or_quote}
وضعیت: تلاش برای ارسال ثبت شد؛ پاسخ قطعی در دست نیست.
ثبت، اجرا یا لغو سفارش تأیید نشده است؛ ارسال مجدد خودکار انجام نمی‌شود.
مرجع: {intent_id} · عملیات: {operation_id}
```

**Successful write response / acknowledgement (not a fill)**

```text
📨 پاسخ سفارش از MEXC دریافت شد
نماد: {symbol} — خرید (Spot)
شناسهٔ برگشتی: {exchange_order_id_or_unknown}
وضعیت داخل پاسخ: {response_status_or_unknown}
این پاسخ به‌تنهایی بازبودن یا پرشدن سفارش را ثابت نمی‌کند.
```

**Exchange-observed status / fill**

```text
🔎 وضعیت سفارش از MEXC مشاهده شد
منبع: {query_or_response_source} · زمان مشاهده: {observed_at_utc}
وضعیت مشاهده‌شده: {exchange_status}
اجرای ثبت‌شده: {executed_qty} از {requested_qty}
وضعیت همگام‌سازی پرشدن: {complete_or_pending_or_discrepancy}
پرشدن سفارش به‌تنهایی به معنی بسته‌شدن موقعیت نیست.
```

**Partial exit / residual**

```text
🟡 خروج جزئی — موقعیت هنوز کاملاً بسته نیست
نماد: {symbol}
مقدار فروشِ تأییدشده: {sold_qty}
باقی‌ماندهٔ ربات: {remaining_qty_or_unknown}
وضعیت موقعیت: {partially_exited_or_residual_or_unknown}
```

**Reconciliation discrepancy**

```text
🔍 تطبیق نیازمند بررسی است
مرجع: {intent_or_trade_id}
وضعیت محلی: {local_state}
مشاهدهٔ MEXC: {exchange_state_or_unknown}
نتیجه: {safe_discrepancy_code_fa}
در این بررسی هیچ سفارش جدید یا درخواست لغوی ارسال نشد.
```

**OCO state**

```text
🧩 وضعیت گروه خروج OCO
گروه: {oco_group_id} · تخصیص: {tp1_or_tp2}
گروه/سفارش: {group_state}
پای TP: {tp_leg_state}
پای حدضرر: {sl_leg_state}
لغو پای همتا: {confirmed_or_unknown}
باقی‌ماندهٔ موقعیت: {remaining_qty_or_unknown}
وضعیت کل معامله از وضعیت گروه به‌تنهایی نتیجه‌گیری نمی‌شود.
```

A “fully flat/closed” Persian message is allowed only when
`position_state=flat_confirmed`; otherwise use partial/residual/unknown language.
The default notification must not quote the original Telegram body. A detail
view resolves the exact `revision_no` and access-checks the recipient.

## 8. Read-only investigation and reconciliation/recovery algorithms

### 8.1 Strictly local read-only investigation

Add an event/timeline query path that:

1. Opens the existing SQLite file with `Database(..., read_only=True)` (the
   repository already uses this pattern for `manage status`); it must not call
   `migrate()`, create a DB, or write a “last viewed” field into a trading
   projection.
2. Does not construct `TradingEngine`, `TradeManager`, `MEXC` REST/CLI clients,
   Telegram MTProto clients, or a signal-queue worker; no network access.
3. Reads a bounded page using indexed event/intent/trade/message references;
   joins the journal with current projections, order history, fills, legacy
   reconciliation results, and legacy audit rows with explicit provenance.
4. Renders each fact as “local decision”, “write attempted”, “response
   acknowledged”, “exchange state observed at …”, “fill evidence”, or
   “reconciliation outcome”; missing evidence is shown as “not recorded” or
   “unknown,” never inferred.
5. Does not invoke `process_item()`, queue retry/reprocess, `_execute_intent()`,
   `recover_on_startup()`, order/cancel/test methods, or an exchange query.

The read-only report is an investigation, not a live account snapshot. It must
say when current status comes from a stored observation and how old that
observation is.

### 8.2 Runtime reconciliation (exchange reads only; local journal writes)

This is distinct from local-only investigation. It may call only existing
read/query interfaces and persist observations/projections. It may **not** place,
cancel, replace, test, or adopt orders.

1. Candidate scope is local bot-owned intents/trades with an attempted/uncertain
   operation, non-terminal exchange projection, `fill_sync_required`, missing
   OCO confirmation, or a prior discrepancy. Require exact known
   `intent_id`/`client_order_id`/`exchange_order_id`; exclude historical/recovery
   signals, unrelated exchange orders, external balances, and legacy positions.
2. Start a `reconciliation_run`; snapshot the relevant local state/version and
   append a start event. Use the existing shared rate gates/backoff. No network
   call is made inside a database transaction.
3. Query by known exact order/client ID using the existing supported
   `query_order` path. Fetch fills by known exchange order ID only through the
   existing `my_trades` method. For CLI search, respect its existing listing
   window; a result outside the available window is `unknown/not_found`, not
   proof the write never reached MEXC.
4. Validate identity fields returned by the adapter (symbol, side, client/order
   IDs, type/quantity where available) before association. If none/multiple
   candidates or fields conflict, append `discrepancy`; do not fuzzy-match by
   price/time/balance and do not adopt the candidate.
5. `NEW`/partial/filled/canceled states are observations only if the returned
   field is explicit and the adapter normalization is tested. If a field is
   absent/unrecognized, store `UNKNOWN` plus `field_present=false`; do not inherit
   a default `NEW` as verified state.
6. On a query timeout/5xx/malformed response, store an ambiguous operation and
   observation; leave the prior projection unchanged except for its
   reconciliation marker. On `-2011` not found, keep a non-terminal/uncertain
   intent unresolved and never submit again. Absence from a bounded listing is
   also not proof of no order.
7. Insert fills idempotently by the existing stable fill key; compare exact
   decimal fill totals with exchange executed quantity. Keep sync pending when
   the persisted fills are short, and mark discrepancy when they conflict.
   Never synthesize a fill solely from an order-level status.
8. For OCO, record the best order/group-level query available through existing
   interfaces. Do not query a guessed OCO-leg endpoint. Populate child state
   only from exact response fields/IDs that are present and have fixtures;
   otherwise preserve `not_observed`/`unknown`, including sibling cancellation.
9. Recompute the bot-tracked position from complete linked fills. Track open
   sell reservations separately. If fill completeness, fee treatment, or OCO
   state is uncertain, position outcome is `unknown`; a residual below minimum
   remains a residual, not flat. `flat_confirmed` additionally requires no
   unresolved/open bot exit.
10. In one DB transaction, append each material observation/event and update
    projections/reconciliation compatibility rows. Enqueue a notification only
    if policy selects that material change. Repeated identical reads do not
    emit repeated lifecycle notifications.

### 8.3 Restart/recovery boundaries

- A persisted `planned` intent with proof no exchange operation was attempted
  may be eligible for the **existing** single-claim recovery path, subject to
  the original live-message/boundary/idempotency/risk checks. The audit/read-only
  feature itself never resumes it or parses a message again. If any evidence
  shows an attempt, or the source is historical/rejected/duplicate/ambiguous,
  do not submit.
- A `submitting`/`unknown_requires_reconciliation` intent with an uncertain or
  legacy-incomplete attempt is query-only. Never replace it with a new client
  ID and never assume a missing operation row proves that no request was sent.
- A terminal order with `fill_sync_required` is fill-query-only until complete;
  no fill-based position result is final while sync is pending.
- An OCO row with no known child evidence remains group-level/unknown; no
  additional OCO or market order is created by an investigator.
- Existing trading recovery behavior remains in `TradeManager`; this design
  instruments it but does not ask the read-only callback or audit CLI to run it.
- Notification retry/dead-letter handling is isolated from execution. A retry
  may call Telegram Bot API only; it cannot re-submit any MEXC operation.

### 8.4 Read-only Telegram detail and preferences

Add Persian `جزئیات` and `تنظیم اعلان‌ها` callbacks to the **existing** control
poller. No new `getUpdates` loop. The current `serve`/control/management locks
remain the single-poller safety boundary.

- Authorize from Telegram's actual numeric `from.id`, configured owner/developer
  IDs, private `chat.id == from.id`, callback token hash, expiry, and role. Never
  trust `callback_data` for a user ID, chat ID, role, symbol, SQL fragment, or
  exchange order ID.
- The developer can use only the new developer notification preferences and
  read-only event details delivered to that developer. Do not authorize the
  developer into existing owner pause/resume, sizing, MEXC credential, or
  approval-control handlers.
- The owner can use owner notification preferences and read-only details in
  the owner's scope. Existing owner control commands remain separate; the new
  detail/policy routes must not call or mutate their trading-setting flows.
- `detail` resolves the event from the local DB, verifies a corresponding
  delivered/routed row for that recipient, applies redaction, and responds via
  Bot API. It performs no MEXC/Telegram-user-session calls and no order action.
  Audit the access event without storing the callback's raw token or source
  message body in logs.
- Preference changes update only notification preference tables, append a
  `notification.policy_changed` event, and show a Persian confirmation. They do
  not mutate `control_settings`, `app_state`, the fixed order amount, execution
  mode, emergency stop, credentials, or exchange orders.
- Keep callbacks short (opaque random token, under Telegram's 64-byte limit),
  expiring, recipient-bound, replay-safe, and limited to `detail`/`preferences`.
  A repeated detail callback can be allowed as a new audited read; a replayed
  expired/forged token is denied.

## 9. Sanitized exchange-operation evidence format

Persist one normalized operation envelope per relevant MEXC call:

```json
{
  "operation_id": "op-…",
  "kind": "place_entry | place_oco | cancel_order | query_order | query_fills | test_order",
  "backend": "cli | rest | hybrid",
  "mode": "sim | testonly | live",
  "entity_refs": {
    "signal_id": "…", "intent_id": "…", "trade_id": "…",
    "trade_order_id": "…", "oco_group_id": "…"
  },
  "identity": {
    "symbol": "BTCUSDT", "side": "BUY", "order_type": "LIMIT",
    "client_order_id": "…", "exchange_order_id": "…"
  },
  "request_summary": {
    "quantity": "0.01", "quote_amount": "3", "limit_price": "…",
    "take_profit_price": null, "stop_trigger_price": null,
    "stop_limit_price": null
  },
  "request_sha256": "sha256(canonical sanitized summary)",
  "request_started_at_utc": "…",
  "operation_state": "attempted | acknowledged | exchange_rejected | uncertain | query_observed | query_not_found | query_ambiguous",
  "response_summary": {
    "order_id_present": true, "status_field_present": false,
    "normalized_status": "UNKNOWN", "executed_qty": null,
    "exchange_timestamp_ms": null
  },
  "response_received_at_utc": "…",
  "exchange_timestamp_ms": null,
  "safe_error": {"class": "MexcNetworkError", "code": null, "reason_code": "timeout"}
}
```

This is a schema example, not a MEXC response contract. Exact field names are
adapter inputs and must be allow-listed against code/tests. Store exact decimal
strings and raw numeric timestamp values. For state-changing calls, persist the
`attempted` row/event immediately before the adapter call, then persist the
response classification separately. For reads, record the query, return shape,
field-presence, observed result and rate-limit/error class.

Never store: API key/secret, signature, signed URL/query, Bot API token, full
CLI argv/environment, proxy variables, full raw CLI stdout/stderr, raw HTTP
headers/body, Telegram session/auth data, or an unfiltered exception string.
For account preflight, retain only the pass/fail, snapshot freshness, safe
reason code, and requested amount needed to explain the local decision; do not
copy a full account/balance response into the journal. Do not hash the
secret-bearing raw request and claim that it is safe evidence; hash only the
canonical sanitized summary. Persist only response keys required to explain
state, and preserve `field_present=false` rather than filling missing fields
with guessed defaults.

## 10. Security, privacy, performance, and failure handling

### Security/privacy

- Use exact configured numeric Telegram IDs and role-specific scopes. Restrict
  detail/preferences to private chats. No username-based authorization.
- Keep the journal, revisions, order data, and message archive private. Detail
  callbacks only disclose the event/revision the recipient is entitled to see.
  Default notifications carry IDs and concise validated facts, not original
  Telegram text or full diagnostics.
- Treat Telegram text, callback data, MEXC responses, CLI output, and exception
  messages as untrusted. Render plain text, strip control characters, bound
  sizes, redact tokens/long credential-like values, and prefer safe error codes
  to error strings.
- Store no raw callback token. Do not log callback contents. Use a short
  one-time/random token lookup, role/user/chat binding, expiry, and replay
  handling. Keep developer permissions separate from owner control actions.
- Journal configuration changes by safe setting name and actor; never journal
  secret values, access codes, API keys, session strings, or secretbox keys.
- SQLite append-only behavior is an application invariant, not a claim of
  tamper-proof storage. Protect file permissions/backups and keep WAL/SHM copies
  sensitive. Do not add unbounded source text to event payloads.

### Performance/reliability

- Append one event per meaningful decision/transition/operation; do not write a
  journal row on every WebSocket tick. Record the price snapshot/reference and
  trigger decision when it actually affects a signal/entry/exit.
- Keep SQLite transactions short and never hold a lock across network I/O.
  Reuse current rate gates and existing REST/CLI paths. Do not add a second
  Telegram poller or per-symbol exchange polling loop.
- Move Telegram send/retry to a bounded background outbox worker. It must not
  block order submission, signal parsing, or SQLite transactions. Share the
  Bot API rate limiter with other sends for the same configured token; honor
  Telegram `retry_after` and cap delivery concurrency.
- Use indexed bounded pages and stable keyset/event-sequence pagination for
  investigations. Bound JSON/text payloads, queue batch size, API calls, and
  callback lifetime. Keep routine identical reconciliation quiet.
- Default retention is no automatic pruning, matching the current repository's
  no-retention policy. Add no automatic deletion of journal, fills, message
  revisions, outbox failures, or callback audit. Plan backups and monitor DB/WAL
  growth; any retention/purge feature requires a separate explicit policy and
  must not silently remove unresolved evidence.
- A migration that backfills revision metadata may hold a SQLite write lock and
  consume disk. Test it on a copied/synthetic v16 DB and verify rollback/backup
  procedures before any owner-authorized production migration. Do not run such
  a migration against a live DB as part of this design.

### Failure semantics

- If journal insertion fails, do not make a new remote write without durable
  pre-write evidence; fail closed before a new MEXC write. Preserve existing
  behavior only where the operation is already irrevocably underway.
- If Telegram is down, outbox rows remain; trading and journal commits proceed.
  A notification failure never deletes or changes the business event.
- If process crashes after an operation was marked attempted, reconcile the
  same known IDs; never infer that no request was sent.
- If a query is not supported, bounded, incomplete, or ambiguous, keep the
  state unknown and expose the gap. No fuzzy search/adoption.
- Current source and historical tests do not establish live native-OCO child
  response semantics or the fee accounting needed for every possible base-fee
  position calculation. Keep those facts unverified until there is a safe,
  sanitized, owner-authorized evidence source; do not place a test order to
  obtain one.

## 11. Staged file-by-file implementation plan

All stages remain on the existing branch. Keep the current runtime mode,
execution path, credentials, control locks, and exchange behavior unchanged
except for the explicit audit/notification semantics above. No deploy/restart
or real exchange test is part of implementation acceptance.

### Stage 1 — revision metadata and journal API

- `tg_signal_ingest/db.py`: add migration 17 only; preserve 1–16 byte-for-byte.
- `tg_signal_ingest/repository.py`: insert `message_revisions` in the same
  transaction as initial message, edit history, current row, and queue event;
  compute content hash over parser-relevant fields; preserve source time,
  observed time, and legacy snapshot provenance.
- New `tg_signal_ingest/trading/audit.py`: typed event envelope, event-key
  builder, UTC normalization, allow-list/sanitization, canonical JSON/decimal
  helpers; no network calls.
- `tg_signal_ingest/trading/store.py` and `trade_store.py`: transaction-aware
  append methods so transition + event are atomic; keep `audit_events` API for
  compatibility and label it legacy.
- Tests: `tests/test_db_migrations.py`, new `tests/test_message_revisions.py`,
  new `tests/test_trading_audit.py`. Use temp SQLite and synthetic records only.

### Stage 2 — operation evidence and correlation

- `tg_signal_ingest/db.py`: migration 18 for `exchange_operations` and
  reconciliation run/observation tables.
- `tg_signal_ingest/trading/engine.py`: instrument intent pre-write,
  acknowledgement, definite reject, uncertain, cancel, order-query, fill-sync,
  and reconciliation paths. Persist before remote writes; no new retry path.
- `tg_signal_ingest/trading/trade_manager.py` and `trade_store.py`: correlate
  entry/exit operation evidence and fill/recovery events.
- `tg_signal_ingest/mexc/errors.py`, `mexc/client.py`, `mexc/cli.py`,
  `mexc/hybrid.py`: expose only safe classification/provenance needed by the
  recorder; preserve existing endpoints and rate gates. Do not persist raw
  bodies/output. Remove/neutralize any missing-status-to-`NEW` ambiguity for
  evidence, with a compatibility test before changing projections.
- Tests: update engine/reconciliation/CLI adapter tests; add transaction-crash,
  timeout-after-send, duplicate-client-ID, missing-status and secret-redaction
  cases. Fake clients must assert uncertain attempts are never resubmitted.

### Stage 3 — configurable transactional notification outbox

- `tg_signal_ingest/db.py`: migration 19 for policy, recipient mute state,
  outbox, attempts, and callback records; do not replay legacy pending messages.
- `tg_signal_ingest/trading/notify.py`: accept journal event/category rather
  than hard-coded role lists; persist route decisions and outbox in the event
  transaction; render concise Persian templates with versioned keys.
- `tg_signal_ingest/trading/store.py`: policy lookup/override/mute and outbox
  lease/attempt APIs; journal every change/suppression decision.
- `tg_signal_ingest/control/bot_api.py`: typed Telegram error/rate-limit
  handling that exposes `retry_after` without leaking the token; retain direct
  HTTPS and response redaction.
- `tg_signal_ingest/trading/runtime.py` and `tg_signal_ingest/service.py`:
  attach a bounded notification dispatcher worker, use a shared token-level
  rate gate, and ensure notification work is not performed synchronously in the
  order execution path. Do not create a second `getUpdates` poller.
- Tests: revise `tests/test_trading_notify.py` and
  `tests/test_trading_notification_routing.py` for defaults, per-role override,
  mute-with-audit, policy version, retries/429, ambiguous delivery, lease
  concurrency, send failure independence, Persian template truthfulness, and no
  order/cancel calls from the dispatcher.

### Stage 4 — explicit OCO and position projections

- `tg_signal_ingest/db.py`: migration 20 for OCO group/legs/observations,
  separate position and outcome projections, with safe `legacy_unverified`
  backfill.
- `tg_signal_ingest/trading/trade_store.py`: create/update groups and desired
  legs atomically with existing `trade_orders`; persist leg observations only
  where returned fields support them.
- `tg_signal_ingest/trading/trade_manager.py`: project group, leg, sibling,
  fill, residual, and position outcome separately; leave unavailable statuses
  unknown. Never report full flat from a partial fill or one OCO state.
- `tg_signal_ingest/mexc/cli.py` and `mexc/hybrid.py`: parse only existing
  supported response fields; do not add a guessed MEXC capability. If current
  fixtures cannot demonstrate child-leg status, the child projection remains
  `not_observed`.
- Tests: extend `tests/test_trading_lifecycle_recovery.py` and adapter tests for
  TP leg, stop leg, confirmed/unknown sibling cancellation, partial fill,
  second OCO still open, residual, fee ambiguity, restart, and no false flat.
  Test-only fake fixtures are not evidence of live MEXC semantics.

### Stage 5 — local-only investigation and authorized detail/preferences

- New `tg_signal_ingest/trading/investigation.py`: bounded, read-only timeline
  and evidence report; use `Database(read_only=True)`; no engine/client imports
  in execution path.
- New `tg_signal_ingest/control/notification_interactions.py` (or equivalent
  narrow router): exact role checks, callback token hashes/expiry, read-only
  detail, and per-recipient preference/mute actions only.
- `tg_signal_ingest/control/service.py`: route the new prefix before/alongside
  existing owner controls without broadening developer authority; integrate
  with the existing poller only.
- `tg_signal_ingest/main.py`: add an explicitly local-only `audit`/timeline
  command that opens the DB read-only, does not migrate, and clearly states it
  makes no exchange/Telegram-user-session calls. Do not repurpose
  `trading reconcile`, which is a network query and persists reconciliation.
- Tests: add read-only SQLite write-failure assertions, zero-network/zero-order
  spies, exact role/chat/recipient scoping, unauthorized/expired/replayed
  callback cases, preference isolation from `control_settings`/`app_state`, and
  bounded rendering tests.

### Stage 6 — documentation and verification handoff

Update the canonical docs only once the behavior is implemented:

- `docs/data-model.md`: migrations 17–20, table/enum relationships, legacy
  provenance and retention.
- `docs/trading.md`: operation/ack/status/fill facets, OCO/position semantics,
  and what is still unverified.
- `docs/architecture.md`, `docs/operations.md`, `AGENTS.md`,
  `docs/CONTROL_BOT_FA.md`, `docs/MANAGEMENT_BOT_FA.md`: actual `serve` control
  poller wiring, single-poller locks, read-only investigation boundaries, and
  notification worker.
- `docs/documentation-index.md`: promote the implemented guides and remove
  stale status claims.
- Run offline unit/migration/lint/type checks on synthetic/temp data; do not
  execute `serve`, `manage run`, `control run`, `trading reconcile`, auth, live
  verification, or any real test/order command without separate explicit
  authorization.

## 12. Acceptance checklist and required evidence

### Data/migrations

- [ ] Migrations 17–20 are additive, ordered, idempotent under repository
      migration rules; v16 stays unchanged.
- [ ] Synthetic v16 migration tests preserve messages/edits, signals, intents,
      fills, reconciliation rows, notifications, and trade rows; old records are
      visibly `legacy`/`unverified`, not reinterpreted as known attempts or flat
      positions.
- [ ] Message revisions are one row per observed local version, correctly
      resolve to existing current/edit text, and keep Telegram/source, observed,
      recorded, exchange, and notification times distinct.
- [ ] Journal event + state projection + route/outbox decision are atomic for
      local transitions; no notification send or MEXC call occurs in a DB write
      transaction.
- [ ] Event idempotency prevents duplicate logical transitions, while distinct
      external attempts and exchange observations remain separately attributable.
- [ ] No automated purge; bounded/indexed queries use the expected indexes.

### Truthful trading/exchange evidence

- [ ] Local rejects/ignored/duplicate/superseded signals state
      `exchange_submission_attempted=false`; no MEXC write operation exists for
      them.
- [ ] Successful response/ID is rendered as acknowledgement, not fill; a
      write attempt alone never becomes “placed,” “open,” “canceled,” or
      “filled.” A query observation has source/time and exact field presence.
- [ ] Test-only is labelled validation-only; sim is labelled simulated; neither
      is described as a real order.
- [ ] Network/5xx/malformed/duplicate-ID ambiguous writes become uncertain and
      are query-only across queue retry/restart/reconciliation; a test spy proves
      exactly one write call and zero hidden retries.
- [ ] A cancel acknowledgement is not rendered as canceled until an exact
      exchange observation supports it. `-2011`/bounded listing absence remains
      unresolved and never triggers resubmission/adoption.
- [ ] Fill rows remain idempotent and fill completeness is independent from
      exchange order status.
- [ ] No order/position adoption by symbol/balance; tests prove unrelated
      external orders and the named legacy symbols are not attached, modified,
      canceled, or closed.
- [ ] Existing fixed quote configuration remains owner-configurable with the
      3 USDT default; the 50 USDT reference is not turned into a hard cap; no
      new signal replay path is introduced.

### OCO/position correctness

- [ ] OCO parent/group and two desired child legs are represented separately;
      each child observation has exact source/field provenance.
- [ ] Without supported child data, TP/stop/sibling states remain
      `not_observed`/`unknown`; no endpoint or response capability is invented.
- [ ] One filled child plus confirmed sibling cancellation is recorded as an
      exit, not a canceled trade. Separate OCO groups are not conflated.
- [ ] Partial fills, incomplete fill sync, fee ambiguity, open exits, and
      below-minimum residuals cannot become `flat_confirmed`.
- [ ] `closed` legacy workflow state cannot override an `unknown` or residual
      position in reports/notifications.

### Notifications and access

- [ ] Developer defaults include meaningful live accepted/rejected/ignored/
      duplicate/parser/diagnostic categories; owner defaults exclude routine
      local rejects/ignored signals and include live write attempts/outcomes,
      material observed lifecycle changes, and unresolved discrepancies.
- [ ] Per-role category toggles/mute affect only outbox routing; journal events
      still exist and routing suppression is explainable.
- [ ] Persian golden tests cover local reject, attempt/uncertain, acknowledgement,
      observed status/fill, cancel request vs confirmation, OCO unknown/partial,
      position residual, and reconciliation discrepancy language.
- [ ] Telegram retry-after is honored; definite retry failures are scheduled;
      delivery ambiguity is visible and not rapidly resent; outbox leases prevent
      concurrent duplicate sends. Notification failures never alter order state.
- [ ] No new `getUpdates` consumer. Unauthorized, wrong-chat, wrong-role,
      expired, and forged callbacks disclose no diagnostic detail and cannot
      call order/place/cancel/test/reconciliation-write functions.
- [ ] Developer detail access does not grant existing owner trading controls;
      preference changes cannot alter trade settings, credentials, execution
      mode, emergency stop, authorization, or locks.
- [ ] Notification default text does not include the original source body or
      secrets; detail resolves the correct revision and applies recipient scope.

### Read-only evidence

- [ ] Local investigation uses a read-only SQLite connection and succeeds with
      network calls disabled; a write attempt through its DB handle fails.
- [ ] Call spies assert zero MEXC/CLI/Telethon calls and zero place/cancel/test
      invocations from local report and callback detail/preferences routes.
- [ ] Reports separately display local decision, operation attempt,
      acknowledgement, exchange-observed state, fill-sync, reconciliation,
      position remainder, provenance, and unknown gaps.
- [ ] `pytest`, `ruff`, and `mypy` results are recorded only after actually run.
      No live trading or real test order is part of the acceptance evidence.
- [ ] OCO child-leg behavior remains explicitly unverified unless an existing
      sanitized contract/fixture establishes it or a separately approved,
      non-order evidence source does. Do not place an order just to pass a test.

## 13. Risks, limitations, and unresolved owner questions

### Known risks / implementation dependencies

1. **External/local atomicity:** a remote write can succeed while the process
   dies before storing its response. The correct state is uncertain plus
   reconciliation, not retry.
2. **Historical completeness:** existing free-text audit, current message row,
   order projections, and status histories cannot reconstruct every original
   request, response, child OCO leg, source revision, or timing. Preserve legacy
   provenance; do not backfill invented facts.
3. **OCO contract gap:** source normalizes one OCO order ID and order-level state;
   it does not establish child-leg IDs/status/sibling cancellation. Keep child
   states unknown until supported evidence exists.
4. **Position/fee accounting:** remaining base depends on complete linked fills
   and correctly understood base-denominated fees. Unknown fee semantics mean
   unknown position, not flat.
5. **Notification delivery is at-least-once/uncertain:** Telegram has no
   exactly-once send key in this application. The outbox can prevent concurrent
   duplicates and avoid blind retries after ambiguous transport, but a manually
   requeued notification may still duplicate.
6. **Routing tests intentionally change:** current tests assert owner receives
   routine local rejects and ignored SELL/SHORT notices. The implementation
   must revise them to the requested default without weakening their no-order
   assertions.
7. **Poller/docs mismatch:** `serve` currently starts a `ControlBot` poller when
   enabled, notwithstanding stale repository docs. Any new callback route must
   attach to that single poller and preserve existing locks; docs need correction.
8. **SQLite growth/lock time:** event, revision, operation, observation, and
   outbox history adds data and writes. Keep transactions short, do not journal
   every price tick, and test copied-database migrations before owner-authorized
   operation.
9. **Provenance on legacy submissions:** prior `submitting`/history data cannot
   prove whether the request reached the network; represent it as legacy
   uncertain/unverified when the available evidence is insufficient.

### Owner questions

**None block the design or first implementation stage.** The requested recipient
defaults, audit independence, read-only boundary, Persian style, trading scope,
and no-live-test constraint are specified above. Retention defaults to the
repository's current no-pruning policy; owner-requested retention changes would
be a separate decision. Native OCO child semantics are a verification dependency,
not a request to place a test order or an unresolved design choice: absent safe
evidence, keep those states unknown.

## 14. References used for this assessment

Primary code/tests inspected (not executed):

- `tg_signal_ingest/db.py`, `repository.py`, `service.py`, `main.py`
- `tg_signal_ingest/trading/engine.py`, `store.py`, `trade_store.py`,
  `trade_manager.py`, `notify.py`, `runtime.py`, `monitor.py`, `parser.py`,
  `risk.py`, and `sizing.py`
- `tg_signal_ingest/mexc/client.py`, `cli.py`, `hybrid.py`, `errors.py`, and
  `symbols.py`
- `tg_signal_ingest/control/bot_api.py`, `service.py`, `store.py`, and `auth.py`
- `tg_signal_ingest/management/bot.py` and `store.py`
- `tests/test_db_migrations.py`, `tests/test_trading_notify.py`,
  `tests/test_trading_notification_routing.py`,
  `tests/test_trading_lifecycle_recovery.py`, MEXC adapter tests,
  control/config/auth tests, and management-bot tests.

Current docs read as context, subordinate to code/tests:
`AGENTS.md`, `docs/architecture.md`, `docs/data-model.md`, `docs/trading.md`,
`docs/operations.md`, and `docs/documentation-index.md`.
