# Architecture

Current behavior of the package. Code is authoritative; see
`docs/documentation-index.md` for the complete document map.

## System context

```
Telegram (MTProto, direct TLS)
        │  live events / backfill / recovery
        ▼
  ingest service ──► SQLite archive ──► processing queue ──► trading pipeline
        │                 │                                      ▼
        │                 └── historical archive          MEXC Spot v3 REST
        └── notification Bot API ──► Owner A + Owner B + programmer      (automatic MEXC Spot)
```

One Telegram account and one selected channel. SQLite is the only durable
store. There is no HTTP server and no inbound network listener.

## Process model

| Entry point | What runs | Locks |
| --- | --- | --- |
| `tg-ingest serve` | Telegram ingestion, backfill, queue consumer, trading engine, notifications, reconciliation/monitor, and — when `cfg.control.enabled` — the ControlBot task | `<db>.serve.lock` and, when control is enabled, `<db>.control.lock` |
| `tg-ingest control run` | Standalone two-owner/programmer controller; no ingestion, backfill, queue, or trading worker | `<db>.control.lock` |
| Other CLI commands | Short-lived DB/network checks | none |

Locks use kernel `flock`; they are released automatically on process exit or
crash. `serve` and `control run` must not target the same database at the same
time because both may own the control lock.

## `serve` startup sequence

Implemented in `tg_signal_ingest/service.py`.

1. Acquire the serve lock; when control is enabled, also acquire the control
   lock. Load config; open and migrate SQLite.
2. Build the trading engine and notification outbox.
3. Connect to Telegram directly, verify the saved session, resolve the selected
   channel.
4. Requeue queue items left in `processing` by a crashed run.
5. Verify Telegram/MEXC connectivity for startup; runtime readiness is diagnostic, not a global entry gate.
6. Snapshot Telegram's newest visible channel message id and the newest
   already-stored message id, before live handlers are attached.
7. Persist/advance the **per-channel** tradeable activation boundary to the
   higher of those ids (forward only), still before handlers are attached. A
   message delivered after the snapshot stays above the boundary. If Telegram's
   newest id cannot be established, the boundary is left unarmed and the
   submission gate fails closed.
8. Attach live new-message, edit, and delete handlers.
9. If a previous watermark exists, sweep the id gap for messages posted while
   offline; those rows are `source='recovery'` and non-tradeable; refresh the
   recent window to catch edits.
10. Start background workers:
   - history import if `not_started` or `in_progress`;
   - processing-queue consumer;
   - reconciliation sweep;
   - market monitor when a `TradeManager` exists;
   - the ControlBot task when `serve` was started with control enabled.
11. Wait for SIGINT/SIGTERM or a Telegram disconnect. Shutdown cancels and awaits
    queue, backfill, reconciliation, monitor, tick, and control tasks.

On disconnect, the service rebuilds the Telegram client and reconnects with
bounded exponential backoff plus jitter. FloodWait always wins: the process
sleeps at least the server-provided interval. A clean cycle resets backoff.

## Ingestion data flow

`normalize.message_to_record()` converts a Telethon message to the archive
shape. `Repository.upsert_message()` is the only writer for message rows.

| Event | Stored effect |
| --- | --- |
| New message | Insert/update `messages`, append queue item `message`, record live watermark |
| Edit | Update message text/metadata, increment `processing_version`, append `message_edits`, enqueue `edit` at the new version |
| Delete | Mark `messages.deleted`, enqueue `delete` |
| Recovery message | Same as new but `source='recovery'`; never directly tradeable |

Queue identity is `(channel_id, message_id, item_type, targets_version)`, so a
duplicate event for the same version is idempotent. A later edit makes older
queued versions superseded rather than processed out of order.

Backfill writes with `enqueue_processing=False`: historical messages are stored
and audited, never fed to the processing/trading path.

## Backfill and recovery

**Backfill** (`tg_signal_ingest/backfill.py`) pages from newest to oldest using
`backfill_next_max_id`, with `backfill_scope` = `full` or `since`. Each page
commits messages and a checkpoint. A resumed run continues from the checkpoint;
a completed run is a no-op.

**Live gap recovery** walks message ids above `live_last_seen_message_id` in
`recovery_sweep_size` pages and advances the watermark. `refresh_recent_window`
re-fetches the previous window to capture edits made while offline. Deletions
that happened while offline are not recoverable because Telegram only pushes
them to connected clients.

## Processing queue

`ProcessingEngine` claims a bounded batch, resolves the current message row,
and invokes the handler registered for `message`/`edit`/`delete`. `serve`
registers the trading engine for all three.

- Success moves the item to `processed`.
- Failure increments attempts and schedules exponential backoff
  `1, 2, 4, ...` seconds, capped at 60 seconds, until
  `processing_max_attempts` (default 8), then `failed`.
- Startup and consumer errors requeue expired `processing` claims.
- `queue retry` re-enqueues failed items; `queue reprocess` enqueues the current
  message version again.

## Trading pipeline

```
raw message text
  → SignalParser (deterministic, spot-only)
  → signals row
  → replay/activation-boundary guard
  → RiskManager (BUY only, fixed quote amount, symbol constraints)
  → order_intents row + atomic technical reservation
  → submission gate (pause + emergency stop) + exchange same-symbol open-order check
  → LIMIT-only MEXC Spot execution
  → exchange_orders, order_status_history, order_fills
  → reconciliation and durable notifications
  → TradeManager lifecycle (conditional entry, TP1/TP2, native OCO SL)
```

Key ordering guarantees:

- The signal and intent are persisted before any exchange call.
- Budget reservation and intent insertion share one transaction.
- One message maps to at most one idempotency key/intent.
- A process lock serializes the whole decision path, including concurrent
  queue retries.
- Notifications are an independent durable outbox; delivery retries cannot
  resubmit an order.

Details and state tables: `docs/trading.md`.

## MEXC adapter

`tg_signal_ingest/mexc/client.py` implements documented Spot v3 endpoints over
stdlib `urllib` in a worker thread. It provides:

- HMAC-SHA256 signing over the same sorted, URL-encoded parameter string that is
  sent; TLS verification stays on.
- Documented per-endpoint weights, file-backed shared IP/UID/order gates, one
  batch ticker request per monitor interval; 429/418/403 trigger a per-process
  backoff honoring `Retry-After`, with a conservative default.
- Typed errors; 5xx/network/unknown submit outcomes are ambiguous.
- Dynamic symbol metadata and precision validation from `exchangeInfo`.
- Decimal-only order-book parsing and depth validation.

No withdrawals, futures, leverage, or undocumented endpoints are implemented.

Live **write** operations (place, cancel, and native OCO exits) additionally
use the official `@mexc-ai/cli` through `mexc/cli.py`; reads stay on REST.
`mexc/hybrid.py` keeps reads on REST and writes on the CLI; the reason for the
split and its fail-closed rules are in `docs/trading.md`. The adapter never
receives API keys: it uses the CLI's own encrypted profile.

## Persistence and failure model

SQLite runs in WAL mode with `BEGIN IMMEDIATE` transactions and a process-wide
lock. Exchange uncertainty is a first-class state, not an exception to retry:
`unknown_requires_reconciliation` and `fill_sync_required` keep the record
visible until reconciliation resolves it. See `docs/data-model.md`.

## Control-bot lifecycle

- `control/` contains the two-owner/programmer controller, setup store, and Bot API
  client.
- `cmd_serve` builds the ControlBot when `cfg.control.enabled` is true, acquires
  both locks, and passes it to `IngestService`. The service runs
  `ControlBot.run()` as an asyncio task and attaches the same trading engine, so
  menu settings and pause/resume affect the live engine.
- `control run` builds the same bot without ingestion/backfill/queue/trading
  workers and acquires only the control lock.
- If control is disabled, the service still reuses only
  `control.bot_api.BotApiClient` for durable trading notifications.

## Trade-lifecycle wiring

Implemented and covered by tests:

- `TradeManager.on_tick()` evaluates only price-driven work: a conditional
  entry trigger and the stop-loss alert for an unprotected position. Exchange
  order/leg/fill state is never queried from the tick path; the periodic
  reconciliation sweep owns it (including OCO legs, see `docs/trading.md`).
  independent of whether conditional entry is enabled.
- `serve` subscribes symbols only for `pending_entry` / `unprotected_critical`
  trades at monitor startup and refreshes subscriptions every 5 s, so a newly
  created conditional entry is monitored and
  symbols with no active trade are unsubscribed.
- The monitor's REST fallback (`IngestService._fetch_price`) uses the MEXC
  client, not the Telethon client.
- `serve` runs `TradeManager.recover_on_startup()` once and calls
  `reconcile_all_open()` from the periodic reconciliation worker. These are
  action-capable: lifecycle recovery can place pending protection and retry
  requested cancellations; it is not a read-only diagnostic.
- Uncertain entry/exit submissions are reconciled by `clientOrderId`; they are
  not blindly resubmitted.

Remaining limits:

- `stop_loss_max_retries` caps definitive TP/SL rejection retries; recovery
  cadence comes from the tick/reconciliation loop.
- The standalone CLI adapter resolves a client order id only within the first
  page of `current-orders`, then `history-orders`. The live hybrid client
  delegates single-order queries to REST; unresolved results remain uncertain.
- Exchange-side attached TP/SL has not been validated against real credentials.

## Documentation verification scope

The behavior in this document was established by reading the implementation and
tests, running offline CLI help, and executing the offline test suite.

Not performed during this documentation pass: Telegram login, live channel
access, MEXC requests, order test/submission/reconciliation, `serve`, or
systemd deployment. Live-traffic behavior is inferred from code and
deterministic tests, not observed against the exchange.
