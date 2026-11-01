# tg-signal-ingest

Telegram channel ingestion service for a single Telegram account and a single
selected signal channel. It persists every channel message (text, media,
edits, deletions, replies, forwards, albums) in a local SQLite database, parses
Spot signals, applies risk controls, and drives a durable order-intent state
machine against the official MEXC Spot API.

**Automatic MEXC Spot execution.** The product has one execution behaviour:
valid LONG/BUY signals from the selected channel are executed automatically with
the owner's fixed quote amount. Futures/Perpetual/leveraged messages may be
ingested; eligible LONG signals are normalized to Spot BUY. SHORT/SELL signals
never execute. There is no SIM/TESTONLY/LIVE product-mode split, no symbol
allowlist, and no per-order approval. Entry orders are LIMIT-only. The pipeline
requires MEXC Spot credentials and the MEXC CLI write backend; there is no
acknowledgement token or mode gate. No withdrawals, no Futures/Margin
execution, and no unofficial endpoints.

- Pause/resume and readiness are enforced at the submission boundary with a
  kernel file lock and a transactional check, not only in the UI.
- Every process start establishes a durable **per-channel** activation boundary
  from Telegram's newest visible message id plus the newest already-stored row,
  before live handlers are attached. Messages at/below it, and every
  backfill/recovery row, are stored and audited but never traded. If the boundary
  cannot be established, new entries fail closed.
- A planned intent that predates the current start is canceled locally and is
  never submitted after downtime. Existing open entries, fills, and OCO exits
  are reconciled read-only; an uncertain submission is never resubmitted.

- Runtime: Python 3.12 + Telethon (MTProto) with direct Telegram networking.
- Storage: SQLite with reproducible migrations.
- Config: `.env` (secrets) + private `config.toml` (runtime paths/options).
- CLI: `tg-ingest` (setup, auth, channels, backfill, serve, status, diagnostics,
  trading status/signals/intents/parse/reconcile/repair-signals/readiness/cleanup-open-orders,
  owner control bot).
- Exchange: MEXC Spot v3 REST (HMAC-SHA256 signing, documented endpoint weights,
  process-shared rate gates, Decimal arithmetic, idempotent intents,
  partial-fill tracking/reconciliation). Market monitoring batches all symbols
  into one ticker request per interval and honors `429`/`418` `Retry-After`.

See `docs/operations.md` for the full operator guide,
`docs/CONTROL_BOT_FA.md` for the Persian owner guide, and
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
- There is no withdrawal support, no Futures/Margin order execution, no SHORT
  execution, and no unofficial exchange endpoint. Futures/Perpetual signal text
  is ingested; eligible LONG signals execute as Spot BUY only.
- Pre-existing external positions are outside the bot trade lifecycle and are
  never adopted automatically.

## Trading safety model

- Every intent is written to SQLite before submission; the idempotency key is
  based on Telegram `(channel, message)` identity, so edits cannot duplicate a
  trade.
- An uncertain submission becomes `unknown_requires_reconciliation` and is
  never retried blindly. Reconciliation queries the exchange by order/client id.
- Spot BUY only: SHORT/SELL instructions, emergency stop, ambiguous/invalid
  signals, and unsupported symbols, precision, or notionals fail closed. There
  is no symbol allowlist, signal-age limit, price-deviation threshold, or
  daily/open-order cap. SL, TP1 and TP2 are mandatory and cannot be disabled.
- Confirmed fills get two native OCO SELL exits (TP1 and TP2, each with the
  stop-loss); partial or uncertain protection is reported honestly and
  reconciled, never blindly resubmitted.
- The pipeline requires MEXC Spot credentials and a healthy MEXC CLI write
  backend. There is no acknowledgement token and no simulated/test-only mode.
- The only cross-trade entry restriction is an actual MEXC open-order check for
  the same symbol. Unresolved historical trades do not block unrelated symbols.
- The optional Telegram control bot authenticates the personal session, verifies
  MEXC permissions, and selects the channel. When `CONTROL_BOT_ENABLED` is true,
  `tg-ingest serve` runs it inside the service process and holds both the serve
  and control locks; `tg-ingest control run` is the standalone controller mode.
  Two configured owners (Owner A and Owner B) have full owner-level controls.
  The configured programmer has read-only technical/diagnostic access and is
  never an owner. Both owners receive every operational event; notifications are
  not filtered by severity or perceived importance. Its `/pause` and `/resume`
  controls are enforced at the order-submission boundary with a kernel file lock
  and a transactional readiness check. See `docs/CONTROL_BOT_FA.md`.
- Before the first controlled start, run the non-trading readiness report.
  The authorized one-time initialization cleanup is
  `tg-ingest trading cleanup-open-orders --apply` (open orders strictly below
  5 USD only; never sells positions; recorded so it is not repeated).

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
