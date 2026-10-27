# Architecture

Current behavior of the package. Code is authoritative; see
`docs/documentation-index.md` for the complete document map.

## System context

```
Telegram (MTProto, direct TLS)
        │  live events / backfill / recovery
        ▼
  ingest service ──► SQLite archive ──► processing queue ──► trading pipeline
        │                 │                                      │
        │                 └── offline analysis (manual)          ▼
        │                                                MEXC Spot v3 REST
        └── notification Bot API ──► owner/developer      (sim/testonly/live)
```

One Telegram account and one selected channel. SQLite is the only durable
store. There is no HTTP server and no inbound network listener.

## Process model

| Entry point | What runs | Locks |
| --- | --- | --- |
| `tg-ingest serve` | Telegram ingestion, backfill, queue consumer, trading engine, notifications, live reconciliation/monitor | `<db>.serve.lock` |
| `tg-ingest manage run` | Two-operator management bot; data-only, bounded validation | `<db>.serve.lock` + `<db>.control.lock` |
| `tg-ingest control run` | Legacy owner controller only; no ingestion worker | `<db>.control.lock` |
| `tg-ingest analyze ...` | Offline analysis of stored messages | none |
| Other CLI commands | Short-lived DB/network checks | none |

Locks use kernel `flock`; they are released automatically on process exit or
crash. Control and management use the same Bot API token, so do not run them
concurrently.

## `serve` startup sequence

Implemented in `tg_signal_ingest/service.py`.

1. Acquire the serve lock; load config; open and migrate SQLite.
2. Build the trading engine and notification outbox.
3. Connect to Telegram directly, verify the saved session, resolve the selected
   channel.
4. Requeue queue items left in `processing` by a crashed run.
5. On the first connection cycle, read the channel's newest message id.
6. Persist/advance the tradeable activation boundary.
7. Attach live new-message, edit, and delete handlers.
8. If a previous watermark exists, sweep the id gap for messages posted while
   offline; refresh the recent window to catch edits.
9. Start background workers:
   - history import if `not_started` or `in_progress`;
   - processing-queue consumer;
   - durable notification flusher when a Bot API client and recipients exist;
   - in `live` mode only: reconciliation sweep and market monitor.
10. Wait for SIGINT/SIGTERM or a Telegram disconnect.

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
a completed run is a no-op. The management bot has a separate, bounded,
data-only ingestion worker that never touches the processing queue.

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
  → order_intents row + atomic budget reservation
  → sim / testonly / live execution
  → exchange_orders, order_status_history, order_fills
  → reconciliation and durable notifications
  → optional TradeManager lifecycle (conditional entry, TP1/TP2, SL)
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
- Separate weighted rate gates for general and order buckets; 429/418/403
  trigger a global backoff honoring `Retry-After`, with a conservative default.
- Typed errors; 5xx/network/unknown submit outcomes are ambiguous.
- Dynamic symbol metadata and precision validation from `exchangeInfo`.
- Decimal-only order-book parsing and depth validation.

No withdrawals, futures, leverage, or undocumented endpoints are implemented.

Live **write** operations (place/cancel/query and native OCO exits)
additionally use the official `@mexc-ai/cli` through `mexc/cli.py`.
`mexc/hybrid.py` keeps reads on REST and writes on the CLI; the reason for the
split and its fail-closed rules are in `docs/trading.md`. The adapter never
receives API keys: it uses the CLI's own encrypted profile.

## Persistence and failure model

SQLite runs in WAL mode with `BEGIN IMMEDIATE` transactions and a process-wide
lock. Exchange uncertainty is a first-class state, not an exception to retry:
`unknown_requires_reconciliation` and `fill_sync_required` keep the record
visible until reconciliation resolves it. See `docs/data-model.md`.

## Legacy and adjacent components

- `control/` is a legacy owner controller plus the shared Bot API client. It is
  not embedded in `serve`: `main.py` does not pass a control bot to
  `IngestService`, and the production service uses only
  `control.bot_api.BotApiClient` for notifications. (The service constructor
  still accepts an optional `control_bot` for tests/embedding.)
- `management/` is the current two-operator channel/preview/validation bot. It
  does not instantiate the trading engine, MEXC client, or processing queue.
- `analysis/` is a separate offline parser and review pipeline. It does not
  contact Telegram/MEXC and is not wired into `serve`.

## Trade-lifecycle wiring

Implemented and covered by tests:

- `TradeManager.on_tick()` evaluates TP1/TP2/stop-loss for every active trade,
  independent of whether conditional entry is enabled.
- `serve` subscribes symbols for active trades at monitor startup and refreshes
  subscriptions every 5 s, so trades created after startup are monitored and
  symbols with no active trade are unsubscribed.
- The monitor's REST fallback (`IngestService._fetch_price`) uses the MEXC
  client, not the Telethon client.
- `serve` runs `TradeManager.recover_on_startup()` once and calls
  `reconcile_all_open()` from the periodic reconciliation worker.
- Uncertain entry/exit submissions are reconciled by `clientOrderId`; they are
  not blindly resubmitted.

Remaining limits:

- `stop_loss_max_retries` caps definitive TP/SL rejection retries. The
  `stop_loss_retry_backoff_base_s` value is parsed but not applied; recovery
  cadence comes from the tick/reconciliation loop and is idempotent.
- The CLI resolves a client order id only within the first page of
  `current-orders`, then `history-orders`; an order outside that window remains
  `unknown_requires_reconciliation`.
- Exchange-side attached TP/SL has not been validated against real credentials.

## Documentation verification scope

The behavior in this document was established by reading the implementation and
tests, running offline CLI help, and executing the offline test suite.

Not performed during this documentation pass: Telegram login, live channel
access, MEXC requests, order test/submission/reconciliation, `serve`, or
systemd deployment. Live-traffic behavior is inferred from code and
deterministic tests, not observed against the exchange.
