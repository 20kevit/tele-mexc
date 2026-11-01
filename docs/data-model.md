# Data Model

SQLite schema and persistence invariants. Source of truth:
`tg_signal_ingest/db.py` (migrations) and the repository/store modules.

## Database basics

- Default path: `data/ingest.sqlite3`; overridable by `data_dir`/`db_path`.
- WAL journal mode, `foreign_keys=ON`, 10-second busy timeout, `BEGIN IMMEDIATE`
  transactions serialized by a process-wide lock.
- Embedded, append-only migrations applied in order and recorded in
  `schema_migrations`. Current maximum version: **21**.
- Commands that open a normal repository run migrations, including `status`,
  `backfill status`, `trading status`, and `control status`. Read-only inspection
  of an existing database requires an external SQLite viewer or a disposable
  copy; help/config commands that never open a repository do not migrate.
- There is no automatic retention, pruning, or archival job. The database grows
  with messages, edits, signals, queue rows, audit events, notifications, and
  trades.

The file (and its `-wal`/`-shm` sidecars) contains private message content,
channel metadata, order data, and audit detail. Treat every copy as sensitive.

## Migration map

| Version | Adds |
| --- | --- |
| 1 | `channels`, `messages`, `message_edits`, `processing_queue`, `sync_state`, `app_state` |
| 2 | `signals`, `order_intents`, `exchange_orders`, `order_status_history`, `order_fills`, `reconciliation_results`, `audit_events`, `trading_daily_spend` |
| 3 | signal/intent lookup indexes |
| 4 | `order_intents.fill_sync_required`, `fill_synced_at` |
| 5 | legacy `control_*` tables |
| 6 | historical management tables and message `sender_id`/`sender_name`/`post_author`/`entities_json` |
| 7 | historical per-operator management flows/channels |
| 8 | historical offline candidate/review tables |
| 9 | historical bounded-validation requests/drafts |
| 10 | structured channel-format candidate fields and logical index |
| 11 | `trading_budget_ledger` |
| 12 | `trades`, `trade_orders` |
| 13 | `order_protection` (exchange-side TP/SL intent and verification state) |
| 14 | `trading_notifications` |
| 15 | historical management flow stage |
| 16 | unique `(event_key, role)` notification outbox |
| 17 | `trade_orders.stop_trigger_price` (recorded; body extended later) |
| 18 | normalized `audit_events` v2 schema (event_type, dedupe_key, indexes) |
| 19 | `trade_orders` per-leg evidence columns; `trades` OCO/outcome columns; `reconciliation_results`/`messages`/`signals` `source_ref` |
| 20 | `exchange_operations`, `oco_groups`, `oco_legs`, `notification_preferences`, `notification_callbacks`, `notification_delivery_attempts` |
| 21 | notification outbox role check widened to `owner`, `owner_b`, `developer`; existing rows preserved |

## Table groups

### Telegram archive and sync

| Table | Key columns / purpose |
| --- | --- |
| `channels` | `channel_id` PK, access hash, title/username, one selected row |
| `messages` | PK `(channel_id, message_id)`; text/caption, media metadata, grouped/reply/forward fields, `raw_json`, `source`, `processing_version`, `deleted` |
| `message_edits` | PK `(channel_id, message_id, edit_seq)`; append-only edit snapshots |
| `processing_queue` | unique `(channel_id, message_id, item_type, targets_version)`; status, attempts, `available_at`, error |
| `sync_state` | per-channel backfill checkpoint, live watermark, last event/recovery times |
| `app_state` | key/value runtime state; per-channel forward-only activation boundary, one-time cleanup completion/in-flight markers, and diagnostic new-BUY block state |

`messages.source` is `backfill`, `live`, or `recovery`. Only `live` can be
tradeable, and only above the activation boundary.

### Signals and orders

| Table | Key columns / purpose |
| --- | --- |
| `signals` | unique `(channel_id, message_id, edit_version)`; parsed kind/action/symbol/prices, fingerprint, status |
| `order_intents` | unique idempotency key and client order id; side/type/mode/state, quote/base/limit price, exchange ids/status/executed quantities |
| `exchange_orders` | latest exchange view per order id, discovered on submit or reconciliation |
| `order_status_history` | append-only intent state transitions |
| `order_fills` | unique `(exchange_order_id, exchange_trade_id)`; price, quantity, quote, fee, trade time |
| `reconciliation_results` | append-only outcome (`consistent`, `resolved`, `discrepancy`, `ambiguous`, `not_found`) |
| `audit_events` | v2 structured timeline: `event_id`, `event_type` (`category.event`), `severity`, `occurred_at_utc`, `recorded_at_utc`, correlation/causation ids, `trade_id`/`intent_id`/`order_id`/`exchange_order_id`, `symbol`, `reason_code`, `outcome`, `source_ref`, `evidence_source`, `evidence_limitations`, `payload_json`, `dedupe_key`; redacted and bounded `detail` |
| `trading_daily_spend` | historical per-day bookkeeping table; retained for compatibility, no runtime writes or cap |

Relationships:

```
signals 1 ── * order_intents 1 ── * order_fills
                    │ 1
                    ├── exchange_orders (by exchange_order_id)
                    ├── reconciliation_results
                    └── trading_budget_ledger (1:1)
```

### Reservations and notifications

| Table | Purpose |
| --- | --- |
| `trading_budget_ledger` | one row per executable intent: reserved/spent/fee quote, active flag, state (`reserved`, `uncertain`, `settled`, `released`) |
| `trading_notifications` | durable outbox; unique `(event_key, role)`, status `pending`/`sending`/`sent`/`failed`/`ambiguous`/`suppressed`/`canceled`, `category`/`event_type`, attempts/error/next attempt |

The ledger is a technical per-intent reservation/idempotency tracker. It is not a portfolio budget and is never reset by restart. The legacy global notification severity value is retained only for compatibility and never suppresses the two owners' event stream; roles are `owner`, `owner_b`, and `developer`.
The notification outbox is independent of order state.

### Exchange evidence and OCO lifecycle

| Table | Purpose |
| --- | --- |
| `exchange_operations` | one row per exchange call, inserted **before** the request; `operation_class` (`read`/`write`), endpoint, payloads (redacted), response classification/code, error, `uncertainty` |
| `oco_groups` | one row per trade (`oco_group_id` PK, `trade_id` unique); `state` mirrors trade state, `intended_config`, `protection_status`, `closed_at` |
| `oco_legs` | one row per OCO target (`leg_id` PK, unique per group+target); `kind`, `target`, prices/qtys, state, exchange status, fills/fees, `cancel_reason`, `canceled_by_target` |
| `notification_preferences` | historical per-category preferences; no longer read or written. Current filtering uses one `notification_min_severity` in `control_settings` |
| `notification_callbacks` | single-use time-limited tokens for `expand`/`signal_source`/`ack` actions |
| `notification_delivery_attempts` | per-notification attempt history: outcome, error, message id, retry-after, `ambiguous` flag |

`exchange_operations.uncertainty`: `none`, `uncertain`, `reconciled`,
`read_only_uncertain`. An uncertain write is never auto-resubmitted.
`oco_groups.state`: `intended`, `submitting`, `active`, `partial`,
`triggered`, `completed`, `canceled`, `uncertain`, `needs_review`.
Sibling cancellation is attributed on the canceled leg via
`cancel_reason = 'oco_sibling_filled'` / `canceled_by_target`; it is observed,
never invented.

### Trade lifecycle


| Table | Purpose |
| --- | --- |
| `trades` | one row per live executed signal (`signal_id` unique); entry mode/price, trigger, TP1/TP2/SL prices and allocations, acquired/sold quantities, state, entry intent/order, retry/error fields |
| `trade_orders` | TP1/TP2/SL orders; unique client order id, exchange id, allocated/sold qty, state, plus `stop_trigger_price`, `oco_group_id`, `leg_role`, `last_observed_at`, `evidence_id`, `not_observed_reason`, `cancel_reason`, `canceled_by_leg`, `filled_quote`, `fee_amount`, `fee_asset` |
| `order_protection` | intended/verified exchange-side TP/SL for an entry intent: prices, mode, status, entry order id, verified timestamp, error |

`trades.trade_state`: `pending_entry`, `entry_active`, `position_open`,
`oco_submitting`, `oco_active`, `oco_partial`, `tp1_active`, `tp1_completed`,
`tp2_active`, `tp2_completed`, `stop_loss_executing`, `exit_pending`, `closed`,
`needs_reconciliation`.

`trade_orders.kind`: `tp1`, `tp2`, `stop_loss`.
`trade_orders.state`: `pending`, `submitting`, `open`, `partially_filled`,
`filled`, `cancel_requested`, `canceled`, `rejected`,
`unknown_requires_reconciliation`.

`order_protection.status`: `not_requested`, `unverified`, `active`, `triggered`,
`completed`, `rejected`, `unknown_requires_reconciliation`. Only `active`
suppresses local TP/SL management; `unverified` is explicitly not protection.
The current live hybrid backend advertises no attached-protection support and
uses native OCO exits instead; `order_protection` is retained for backends that
do support attached triggers.

### Historical / compatibility tables

These tables are retained so existing databases migrate forward without data
loss. The application features that wrote some of them have been removed.

| Group | Purpose |
| --- | --- |
| `channel_signals`, `signal_message_links` | historical logical signal index and link status; preserved for future browsing |
| `signal_candidates`, `signal_candidate_reviews`, `analysis_runs`, `analysis_failures` | historical offline-analysis records |
| `control_*` | owner setup/settings/flows/encrypted secrets/audit/notices plus historical learning and approval records |
| `mgmt_*` | historical two-operator management state, ingestion jobs, and bounded-validation requests |

No rows are deleted by the application on upgrade. Secrets in `control_secrets`
are encrypted with `CONTROL_SECRET_KEY`; losing the key makes them unrecoverable.

## Enum reference

| Field | Values |
| --- | --- |
| `messages.source` | `backfill`, `live`, `recovery` |
| `processing_queue.item_type` | `message`, `edit`, `delete` |
| `processing_queue.status` | `pending`, `processing`, `processed`, `failed` |
| `sync_state.backfill_status` | `not_started`, `in_progress`, `done` |
| `signals.signal_kind` | `alert`, `update`, `cancel`, `result`, `info`, `ambiguous` |
| `signals.status` | `parsed`, `rejected`, `validated`, `planned`, `superseded`, `canceled`, `deleted` |
| `order_intents.state` | `planned`, `submitting`, `submitted`, `reconciling`, `open`, `filled`, `canceled`, `failed`, `rejected`, `unknown_requires_reconciliation`, `canceled_local` |
| `order_intents.mode` | historical compatibility: `sim`, `testonly`, `live`; new rows are `live`, legacy non-live rows are never executable |
| `exchange_orders.status` | `NEW`, `FILLED`, `PARTIALLY_FILLED`, `CANCELED`, `PARTIALLY_CANCELED`, `UNKNOWN` |
| `order_fills.source` | `reconciliation`, `rest_query` |
| `audit_events.category` | `telegram`, `parse`, `validate`, `risk`, `mexc_request`, `lifecycle`, `reconcile`, `config`, `emergency` |
| `trading_budget_ledger.state` | `reserved`, `uncertain`, `settled`, `released` |
| `order_protection.status` | `not_requested`, `unverified`, `active`, `triggered`, `completed`, `rejected`, `unknown_requires_reconciliation` |
| `trading_notifications.status` | `pending`, `sending`, `sent`, `failed`, `ambiguous`, `suppressed`, `canceled` |

## Persistence invariants

1. A message row is keyed by Telegram identity; edits never create a second
   message row.
2. Queue work is versioned; a stale edit version is superseded, not applied.
3. A signal is unique per message/edit version. A live trade is unique per
   signal.
4. Idempotency key and client order id are unique. Intent planning precedes
   submission.
5. Every intent state change appends history; reconciliation outcomes and audit
   events are append-only.
6. Budget reservation is inserted in the same transaction as the intent.
7. Fills are idempotent by exchange trade id. `fill_sync_required` keeps terminal
   intents in the reconciliation set until fills match executed quantity.
8. Notifications are idempotent by `(event_key, role)` and never gate execution.
9. `app_state` activation boundaries are per channel and forward-only; each
   `serve` cycle snapshots Telegram's newest visible id plus the newest
   pre-handler stored id, and `source='recovery'/'backfill'` remains non-tradeable
   even above the boundary.
10. A planned intent at/below its channel boundary is canceled locally during
    startup recovery; it cannot become a submission after downtime.
11. `trading.initial_open_order_cleanup` records completion of the one-time
    <5 USD open-order cleanup; it is never repeated on normal restart.

## Backup and integrity

Recommended online backup (SQLite backup API, safe with WAL):

```bash
mkdir -p /var/backups/tg-signal
.venv/bin/python -c "import sqlite3; s=sqlite3.connect('data/ingest.sqlite3'); d=sqlite3.connect('/var/backups/tg-signal/ingest-backup.sqlite3'); s.backup(d); d.close(); s.close()"
```

For an offline copy, stop all writers, then copy the database and its sidecars.
Restore with the service stopped; remove stale `-wal`/`-shm` files; keep mode
`0600`. Verify with `tg-ingest status` and `tg-ingest trading status` before
starting `serve`.

Repository-opening status commands may initialize or migrate the selected
database:

```bash
.venv/bin/tg-ingest status
.venv/bin/tg-ingest backfill status
.venv/bin/tg-ingest queue list --status failed
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest trading intents
```
