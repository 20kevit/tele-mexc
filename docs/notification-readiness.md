# Notification delivery architecture

Status: implemented in code; this change has **not** been deployed or exercised
against production services. All verification in this branch is offline and
uses fake Telegram/MEXC clients.

## Architecture

Trading producers record typed, immutable events in `audit_events` within the
business transaction. `tg-ingest notifications worker` reads that journal
read-only and maintains delivery state in the separate
`data/notification-delivery.sqlite3` ledger. No trade-manager, signal-engine,
readiness, or startup path performs Telegram I/O. The old `TradingNotifier`,
formatted `trading_notifications` producer, and `trading notifications` CLI
path have been removed. Historical database migrations and old rows remain for
upgrade compatibility; the dispatcher no longer relays them.

For source of truth, see `tg_signal_ingest/trading/domain_events.py`,
`tg_signal_ingest/notification_dispatcher.py`, `tg_signal_ingest/control/bot_api.py`,
and the tests named below. Existing delivery-ledger databases may retain unused
thread-era columns/tables; current delivery messages are standalone.

## Routing and safety policy

- The dispatcher applies per-recipient category preferences and minimum
  severity. Owner B inherits Owner A's category preferences unless overridden;
  minimum-severity overrides are recipient-specific.
- Critical protection and uncertain-exchange/reconciliation events remain
  durable domain events and are not discarded by routine owner filters.
- Recipient delivery rows are independent, so one recipient's failure does not
  block another. Event IDs provide deduplication; identical IDs with conflicting
  envelopes are rejected.
- Long text is split into ordered chunks no longer than the Telegram limit.
- Each recipient delivery has at most 10 automatic attempts. Known-not-sent
  transport failures and rate limits are retryable with bounded backoff; an
  ambiguous send outcome is not blindly retried. Permanent failures are
  retained for inspection.
- The Telegram API has no send idempotency key. Consequently an ambiguous
  outcome is recorded as UNKNOWN rather than risking a duplicate alert.

The worker's output is informational only: it cannot place, cancel, or amend
MEXC orders. Trading execution, sizing, entry gates, OCO protection, fills,
UNKNOWN handling, reconciliation, and recovery remain owned by the trading
pipeline.

## Operations

```bash
.venv/bin/tg-ingest notifications status
.venv/bin/tg-ingest notifications status --json
.venv/bin/tg-ingest notifications list --status dead_letter --limit 50
.venv/bin/tg-ingest notifications requeue <delivery_id>
```

Status and list are read-only. Requeue is an explicit operator action for an
eligible delivery; it does not touch exchange state. `trading readiness` reports
the independent worker heartbeat. A recent heartbeat is not proof that a
message reached Telegram; inspect delivery rows for outcome.

## Verification

The offline suite covers event durability/deduplication, per-recipient routing,
severity and category policy, retry limits, ambiguous outcomes, and critical
protection alert recording. No production send, deployment, restart, database
migration, or real order was performed for this change.
