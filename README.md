# tg-signal-ingest

Telegram channel ingestion service for a single Telegram account and a single
selected signal channel. It persists every channel message (text, media,
edits, deletions, replies, forwards, albums) in a local SQLite database, parses
Spot signals, applies risk controls, and drives a durable order-intent state
machine against the official MEXC Spot API.

**Live trading is disabled by default.** Default mode is `sim` (no exchange
calls). `testonly` uses the official `/api/v3/order/test` endpoint;
`live` additionally requires an explicit acknowledgement token in the private
config. No withdrawals, no futures, no unofficial endpoints.

- Runtime: Python 3.12 + Telethon (MTProto) with direct Telegram networking.
- Storage: SQLite with reproducible migrations.
- Config: `.env` (secrets) + private `config.toml` (runtime paths/options).
- CLI: `tg-ingest` (setup, auth, channels, backfill, serve, status, diagnostics,
  trading status/signals/intents/parse/reconcile/repair-signals/test-order,
  offline `analyze` replay/summary/list/show/review/failures/corpus-check, owner
  control bot, and a two-operator `manage run`/`manage status` bot with developer
  self-authorized bounded validation).
- Exchange: MEXC Spot v3 REST (HMAC-SHA256 signing, documented endpoint weights,
  process-shared rate gates, Decimal arithmetic, idempotent intents,
  partial-fill tracking/reconciliation). Market monitoring batches all symbols
  into one ticker request per interval and honors `429`/`418` `Retry-After`.

See `docs/operations.md` for the full operator guide,
`docs/SIGNAL_ANALYSIS.md` for the offline, non-trading candidate/review
pipeline, `docs/CONTROL_BOT_FA.md` for the Persian owner guide,
`docs/MANAGEMENT_BOT_FA.md` for the two-operator management and bounded-validation
bot, and
`docs/LIVE_VERIFICATION.md` for the manual Telegram verification procedure.

## Reviewer quick start (offline)

The test suite is fully mocked: it needs no Telegram session, no MEXC
credentials, no network, and no production database.

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy tg_signal_ingest
```

For a real installation, copy `.env.example` to `.env` and fill placeholders, or
run `.venv/bin/tg-ingest config init`. Then use `.venv/bin/tg-ingest config check`,
`auth login` in a trusted SSH terminal, `channels list`, and
`channel select <id-or-@username>` before `serve`. The database is created and
migrated when a normal repository is opened; `tg-ingest status` is an offline
status view, but may create or migrate the configured database.

Production service commands (owner/operator only):

```bash
sudo systemctl start tg-ingest.service
sudo systemctl restart tg-ingest.service
systemctl status tg-ingest.service
journalctl -u tg-ingest.service -f
.venv/bin/tg-ingest status
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest queue list
```

## Verification status

- The parser and trading engine are tested with mocks and fixtures. Passing
  tests are **not** proof of live exchange execution.
- The structured parser covers the channel shape observed in the private
  development sample; it is not a universal parser for arbitrary Telegram
  signals.
- Entry submission, fill synchronization, and OCO exit wiring are implemented
  for MEXC Spot. Live signal-to-order and live OCO behavior remain unverified
  until a genuine signal is observed and its exchange result is checked.
- There is no withdrawal support, no futures/short selling, and no unofficial
  exchange endpoint.
- Two pre-existing exchange positions are intentionally outside the new trade
  lifecycle and are never adopted automatically.

## Trading safety model

- Every intent is written to SQLite before submission; the idempotency key is
  based on Telegram `(channel, message)` identity, so edits cannot duplicate a
  trade.
- An uncertain submission becomes `unknown_requires_reconciliation` and is
  never retried blindly. Reconciliation queries the exchange by order/client id.
- Spot BUY only: SHORT/SELL instructions, emergency stop, ambiguous/invalid
  signals, and unsupported symbols, precision, or notionals fail closed. There
  is no symbol allowlist, signal-age limit, price-deviation threshold,
  daily/open-order cap, or mandatory stop-loss rule.
- Confirmed fills get two native OCO SELL exits (TP1 and TP2, each with the
  stop-loss); partial or uncertain protection is reported honestly and
  reconciled, never blindly resubmitted.
- Live mode requires `mode = "live"` plus `live_enabled_token`; the default is
  `sim`. See `docs/operations.md`.
- The optional single-owner Telegram control bot defaults to human approval.
  It authenticates the personal session, verifies MEXC permissions, selects the
  channel, teaches from five confirmed examples, and presents nonce-bound,
  expiring approvals. It never enables trading, withdrawals, futures, or
  automatic mode by itself. Automatic mode requires a two-step confirmation and
  is cleared on every process restart. See `docs/CONTROL_BOT_FA.md`.
- The `manage` bot authorizes exactly two numeric operators (owner and
  developer), lists channels, and previews the latest ten messages read-only.
  It can also store one bounded sample of 10/25/50 messages after a developer
  request that the developer self-authorizes, followed by a separate developer
  start; the sample is then analysed offline for human confirm/correct/reject.
  The self-authorization is limited to bounded validation and does not grant
  unrelated owner-only controls. It never starts full history ingestion or live
  monitoring. The shared
  personal session is authenticated only from a trusted SSH terminal
  (`tg-ingest auth login`); the bot never asks for or accepts a login code in
  chat, because Telegram invalidates codes shared in Telegram messages. It does
  not instantiate the processing queue, exchange client, or trading engine,
  does not download media, and never places an order. Logout requires explicit
  confirmation and only removes the session after a successful server-side
  `logOut`.
- Offline candidate analysis (`tg-ingest analyze ...`) is independent of the
  trading parser/engine and is not wired into `serve`. It reads already-stored
  messages, never contacts Telegram or MEXC, and stores human corrections
  append-only. It requires an explicit manual command and never starts
  collection, backfill, or live monitoring. See `docs/SIGNAL_ANALYSIS.md`.

## Known limitations (platform-imposed)

- **Deletions during downtime** are not recoverable: Telegram only pushes
  delete events to connected clients. What we can do (and do): mark deletions
  received live, and re-fetch edited messages in the recovery window.
- **Old edits outside the recovery window**: edits made while the service was
  offline and older than `recovery_sweep_size` messages may not be re-fetched.
- **Media contents** (photos/documents) are stored as references and metadata
  only; a photo carrying a signal is marked `requires_ocr` in its media meta
  and is never treated as parsed text. Downloading media is optional and
  disabled by default.
- Delete notifications bound to a channel are used only for that channel;
  the service never accesses unrelated private conversations.
- Ambiguous/rejected signals are stored with a reason but never traded. A live
  order whose final exchange state cannot be determined remains flagged for
  operator reconciliation; there is no automatic resubmission.
