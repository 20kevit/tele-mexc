# Operator Guide — tg-signal-ingest

A single-user Telegram ingestion service for one signal channel. It stores every
channel message (text, media, edits, deletions, replies, forwards, albums) in a
local SQLite database, parses Spot signals, applies risk controls, and drives a
durable order-intent state machine against the official MEXC Spot API.

**Live trading is disabled by default.** The default mode is `sim` (no exchange
calls). `testonly` uses the official `/api/v3/order/test` endpoint. `live`
requires the explicit acknowledgement token below. No withdrawals, no futures,
no unofficial endpoints.

All commands below are copy-paste-ready and were executed on this VPS
(Ubuntu 24.04, Python 3.12, project at `/opt/projects/akha1`).

---

## 1. Prerequisites

- Ubuntu Linux VPS with Python 3.12 (already the system python: `python3 --version`)
- Direct outbound DNS, TCP and TLS access to `api.telegram.org` and Telegram
  MTProto data centres on TCP 443. No proxy is required.
- A Telegram account that can read the signal channel, plus its owner's cooperation
  for the one-time interactive login

## 2. Installation

```bash
cd /opt/projects/akha1
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'      # or without [dev] for production
```

## 3. Obtain Telegram API credentials (one time)

1. Visit https://my.telegram.org in a browser (use the account owner's Telegram account)
2. Log in with the phone number, then open **API development tools**
3. Create an application (any short name; e.g. "signal-ingest" and its own purpose)
4. Note the shown **App api_id** (integer) and **App api_hash** (32 hex chars)
5. NEVER put these values in source control — they go into `.env` only (next step)

## 4. Configure the application

```bash
tg-ingest config init      # creates private .env and config/config.toml templates
nano /opt/projects/akha1/.env
```

Fill `.env`:

```
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=0123456789abcdef0123456789abcdef
```

Telegram traffic uses direct networking; do not add proxy variables. Optional
runtime settings live in `config/config.toml` (paths, backfill scope, media
download flag, sweep sizes). Both files are chmod 0600 and git-ignored.

Validate:

```bash
tg-ingest config check
```

## 5–6. First-run login (terminal only, interactive)

Run this in a trusted SSH terminal. **Never send the login code into a Telegram
chat or bot.** Telegram invalidates login codes shared in a Telegram chat, and a
shared code can be reported as `PHONE_CODE_EXPIRED` even though it was entered
correctly.

```bash
cd /opt/projects/akha1
.venv/bin/tg-ingest auth login
```

The command asks for:

1. **Phone number** with country code (e.g. `+15550101337`)
2. The **login code** Telegram sends to that account
3. The **2FA password**, only if two-step verification is enabled

The code and 2FA password are read with hidden input; the phone/code/password
are never printed or logged. The exact `phone_code_hash` returned by that
request is used for the submission. If Telegram rejects it, the real category
is shown (for example `PHONE_CODE_EXPIRED`, `PHONE_CODE_INVALID`,
`PHONE_CODE_HASH_EMPTY`, or `PHONE_NUMBER_INVALID`); the CLI does not silently
request another code. If the Telegram session is in use by `tg-manage`, the
command waits/fails with a clear lock message; the management service can stay
running.

The session is persisted locally (never printed, never uploaded). Verify
afterwards:

```bash
tg-ingest auth check
```

`PHONE_CODE_EXPIRED` can mean the code expired, was already used, or was
invalidated because it was shared in a Telegram chat. Do not try to evade the
protection by splitting or disguising the code; run a fresh login later when
you can enter the code only in the terminal.

## 7. List and select the destination channel

```bash
tg-ingest channels list
tg-ingest channel select <bare_id_or_@username_or_title>   # e.g. channel select 1946012345
```

Only channels/supergroups are candidates; the selection is persisted in the
database and is used by every subsequent run.

## 7b. Two-operator channel-management bot (read-only inspection)

The optional `manage` bot authenticates a personal Telegram session, lists
channels ten at a time, confirms a channel identity, and previews the latest ten
messages. It is deliberately separate from `serve` and never instantiates the
signal parser, processing queue, MEXC client, risk engine, or order execution.
Selecting a channel or previewing messages does **not** start ingestion, enqueue
anything, or download media.

```bash
tg-ingest manage status   # read-only; never creates/migrates the database
tg-ingest manage run      # two-operator bot over direct Telegram networking
```

Two numeric Telegram IDs are required and must differ:

```bash
CONTROL_BOT_TOKEN=...            # @BotFather token
CONTROL_OWNER_USER_ID=...        # business owner
CONTROL_DEVELOPER_USER_ID=...    # developer/operator
```

Authorization is private-chat-only and ID-based; usernames are never trusted.
Each operator has an independent login flow and channel snapshot. Only one
Telethon login/session operation can run at a time, guarded by an in-process
lock plus `flock` on `config/tg_session.session.lock`. After a bot restart an
incomplete login stage is abandoned (the challenge lives only in memory) while
a valid session file is preserved.

`manage run` holds the database and control locks, so do not run it alongside
`serve` or `control run`. Without the token and both IDs it refuses to start.
The `/logout` action always asks for confirmation, attempts a real server-side
`logOut`, and removes local session files only after Telegram confirms success.
See `docs/MANAGEMENT_BOT_FA.md`.

## 8. Import historical messages

Optional initial scope — choseFull default. To limit the initial import by date,
edit `config/config.toml` before starting:

```
backfill_scope = "since"
backfill_since = "2025-06-01"
```

Then either run explicitly, or let `serve` do it in the background:

```bash
tg-ingest backfill run        # foreground; resumable; prints progress counts
tg-ingest backfill status     # checkpoint/report view
```

The import resumes from its stored checkpoint after process termination.

## 9. Start continuous monitoring

```bash
tg-ingest serve               # live monitor + recovery sweeps + queue consumer
```

`serve` connects directly to Telegram, verifies the stored session, attaches
the live listener, bridges any offline gap with a recovery sweep, runs the
history import concurrently when pending, and drives the downstream processing
queue.
Press Ctrl+C (or send SIGTERM) for a clean shutdown.

## 10. Inspect stored messages and sync status

```bash
tg-ingest status                       # channel, backfill, counts, queue
tg-ingest backfill status              # history import checkpoint
tg-ingest messages list --limit 20     # recent messages (preview)
tg-ingest messages show 1234           # full record + edit history
tg-ingest queue list --status pending  # downstream queue snapshot
```

A quiet channel shows `no new messages`-like silence plus last-event timestamps;
that is explicitly normal, never a failure by itself.

## 11. Run persistently (systemd)

A service unit is provided at `deploy/tg-ingest.service`:

```bash
cp deploy/tg-ingest.service /etc/systemd/system/tg-ingest.service
systemctl daemon-reload
systemctl enable --now tg-ingest
systemctl status tg-ingest          # is the service running
journalctl -fu tg-ingest            # follow structured logs
```

The service exposes no network endpoints of any kind (no HTTP API is present).

The read-only two-operator management bot has a separate unit:

```bash
cp deploy/tg-manage.service /etc/systemd/system/tg-manage.service
systemctl daemon-reload
systemctl enable --now tg-manage
systemctl status tg-manage
journalctl -fu tg-manage
```

The unit runs as `root`, in `/opt/projects/akha1`, with the project virtualenv,
`Restart=always`, `RestartSec=10`, and `WantedBy=multi-user.target`, so it starts
on boot. Restart it only after an intentional code/configuration change:

```bash
systemctl restart tg-manage
systemctl is-active tg-manage
journalctl -u tg-manage -n 30 --no-pager
```

`tg-manage` and `tg-ingest` share locks deliberately; run only one of them at a
time. The management bot does not start an ingestion worker or backfill. Both
units strip ambient/legacy proxy variables and use direct Telegram networking.

## 12. Recover after reboot or network outage

Nothing to do by hand:

- On boot, systemd auto-restarts `tg-ingest`.
- On connect, the service runs a **recovery sweep** that imports every message
  posted while it was offline (bounded by `recovery_sweep_size` per pass), then
  refreshes the recent message window to catch edits made during the outage.
- Telethon reconnects automatically; session state is persisted locally, so no
  re-login is needed.
- Check quickly with `tg-ingest status`.

## 13. Reauthenticate after a revoked session

If Telegram terminates the session (e.g. via "active sessions → terminate"):

```bash
tg-ingest auth check     # will clearly report that no valid session exists
tg-ingest auth login     # repeat the terminal flow of section 5
```

The management bot can remain active. The CLI and the bot use the same session
lock, so do not stop/restart `tg-manage` merely to log in. If the lock is busy,
wait for the current operation to finish and retry. The login code must still be
entered only in the SSH terminal, never in a Telegram chat.

### Using a separate session for a different Telegram account

The default session file stays untouched when `TG_SESSION_PATH` points at a
different path. The two sessions have separate lock files and can therefore be
authenticated independently:

```bash
cd /opt/projects/akha1
TG_SESSION_PATH=config/tg_session_personal .venv/bin/tg-ingest auth login
TG_SESSION_PATH=config/tg_session_personal .venv/bin/tg-ingest auth check
TG_SESSION_PATH=config/tg_session_personal .venv/bin/tg-ingest auth check-channel
```

`auth check-channel` is read-only: it verifies that the selected account can
open the exact channel already chosen for the management workflow, prints no
channel id/title, and fetches no message history. Use
`deploy/tg-manage-personal.service` to run the management bot with that session;
it shares the same bot token and database as `tg-manage.service`, so stop one
before starting the other. Do not run both at once.

## 14. Stop monitoring and delete/revoke the session

```bash
tg-ingest auth logout    # revokes the Telegram session AND deletes local files
```

The service keeps using only legitimate account access; it never uploads
sessions or credentials anywhere and performs no other Telegram actions
(never posts, joins, or reacts).

## 15. Troubleshooting

| Symptom | Diagnosis |
| --- | --- |
| `DIRECT NETWORK FAILURE: ...` | check outbound DNS/TCP 443 and the default route; run `tg-ingest diagnostics network` |
| `DIRECT TELEGRAM FAILURE: ...` | verify Telegram endpoints are reachable directly, then retry |
| `No valid Telegram session...` | session missing/revoked: run `tg-ingest auth login` in a terminal (never send the code in chat) |
| `PHONE_CODE_EXPIRED` | code expired, was already used, or was invalidated because it was shared in Telegram. Do not resend/disguise it; retry later in the terminal only |
| `PHONE_CODE_HASH_EMPTY` / stale challenge | start a fresh terminal login; never reuse an old hash |
| session lock busy | wait for the bot/CLI operation to finish; do not stop the service just for the lock |
| `channel not accessible with this session` | account left the channel or channel deleted; re-run `channels list`, re-select |
| `history fetch kept hitting FloodWait` | Telegram throttling; temporarily restart later; the checkpoint preserves progress |
| queue `failed` > 0 | inspect `queue list --status failed`, then `queue retry` |
| DB locked warnings | a maintenance CLI ran while `serve` was busy; retry once |

Diagnostics, in the required order:

```bash
tg-ingest diagnostics network    # direct DNS/TCP/TLS (works without credentials)
tg-ingest diagnostics bot        # configured Bot API token, read-only getMe
tg-ingest diagnostics telegram   # direct network + session + channel access
```

## 16. How the signal parser consumes messages

The parser never needs Telethon:

- The service enqueues every stored message into `processing_queue`
  (`pending` -> `processing` -> `processed`; automatic bounded backoff, max
  attempts configurable; failed items can be retried with `queue retry`).
- Register an async handler per item type and drive it with `ProcessingEngine`:

  ```python
  from tg_signal_ingest.db import Database
  from tg_signal_ingest.repository import Repository
  from tg_signal_ingest.processing import ProcessingEngine

  engine = ProcessingEngine(Repository(Database(...)))
  engine.register_handler("message", handle_new_signal)   # plain new message
  engine.register_handler("edit",    handle_signal_edit)  # TP/SL modifications etc.
  engine.register_handler("delete",  handle_closing)
  stats = await engine.run_once()             # or await engine.run(stop_event)
  ```

- Each handler receives the full stored message row (id-based identity
  `(channel_id, message_id)` + `processing_version`), so edits are observable
  and deduplicated; use `queue reprocess --id <id>` (or `--all`) after a parser
  update to re-run old messages.
- Photos / documents are stored with `media_type` and metadata only;
  media messages carry `caption` and set no parseable text body — the parser
  treats media without OCR as non-actionable (never guesses text).

## 17. MEXC Spot trading pipeline

The service registers parser handlers automatically in `serve`. Configure it in
the private `config/config.toml` (credentials stay in `.env`):

```toml
[trading]
mode = "sim"                 # starter default; no exchange calls
allowed_symbols = ["BTCUSDT"] # EMPTY allowlist rejects every signal
max_quote_per_order = 10
daily_max_orders = 2
max_open_orders = 2
max_signal_age_s = 900
max_price_deviation_pct = 1
require_stop_loss = true
order_type_policy = "limit"
remaining_balance_reserve_pct = 20  # live only: never spend the last 20% of free funds
balance_fee_buffer_pct = 0.2        # live only: extra headroom for fees on top of the order
balance_max_age_s = 5               # live only: reject an account snapshot older than this
recon_interval_s = 30               # live only: background reconciliation sweep period
# emergency_stop = true
```

`tg-ingest serve` takes an advisory lock on `<db_path>.serve.lock`
(`SingleInstanceLock`, kernel `flock`). A second process exits with `already
holds the serve lock ...` instead of consuming queue items, and the lock is
released automatically if the service crashes or is killed.

Modes:

| Mode | Behaviour |
| --- | --- |
| `sim` | Parses/validates/plans and marks intents filled locally. No network. |
| `testonly` | Sends the order to official `POST /api/v3/order/test` only. No real order. |
| `live` | Sends a real Spot order. Requires `live_enabled_token = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"`. |

Inspect the pipeline without trading:

```bash
tg-ingest trading status
tg-ingest trading signals --limit 20
tg-ingest trading intents --limit 20
tg-ingest trading parse <message_id>
tg-ingest trading reconcile          # live only; queries non-terminal intents
tg-ingest trading test-order --symbol BTCUSDT --side BUY --quote 10
```

Safety invariants:

- Intent rows are written before any exchange call; each transition is in
  `order_status_history`/`audit_events`.
- Idempotency is keyed by Telegram `(channel_id, message_id, side, symbol)`.
  Editing a message cannot create a second order.
- Network/5xx/unknown submit outcomes become
  `unknown_requires_reconciliation` and are never retried automatically.
- Partial fills are preserved (`PARTIALLY_FILLED`/`PARTIALLY_CANCELED`), and
  fills are deduplicated in `order_fills`.
- A live intent whose final state cannot be established remains flagged. There
  is no blind resubmission; inspect with `trading intents` and reconcile again.
- Fills are durable: an order that reaches a terminal state while `myTrades` is
  unavailable is flagged `fill_sync_required` and re-fetched by later
  reconciliation sweeps (including after a restart) until its fills are stored.
- Every live submission runs a fail-closed balance preflight against a fresh
  account snapshot. It reserves `remaining_balance_reserve_pct` plus a
  `balance_fee_buffer_pct` fee buffer, adds outstanding local open intents, and
  rejects (never under-reserves) on missing, stale, or insufficient balances.

## 18. Single-owner Telegram control bot (optional)

The control bot is a controller for the existing pipeline, not a second trading
path. It uses the official Telegram Bot API for Telegram messages and the
existing Telethon session for channel access. It never creates a new order
path: every signal still passes the parser, risk manager, sizing, balance
preflight, durable intent state machine, and reconciliation.

### Configure

Add to the private `.env` (never commit; never paste values into chat):

```bash
# from @BotFather
CONTROL_BOT_TOKEN=123456:replace-me
# numeric Telegram user id of the only permitted owner
CONTROL_OWNER_USER_ID=123456789
# generate once, keep stable; encrypts MEXC credentials at rest
python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
CONTROL_SECRET_KEY=replace-with-output
# optional; default is true when token and owner are set
# CONTROL_BOT_ENABLED=true
# CONTROL_API_BASE=https://api.telegram.org
```

`CONTROL_OWNER_USER_ID` must be a positive numeric id. If
`CONTROL_BOT_ENABLED=true` is set explicitly, both the token and owner id are
required. `CONTROL_SECRET_KEY` is required to save MEXC credentials; without it
the setup flow fails closed rather than storing plaintext. The bot token and
the MEXC secret are never printed or logged.

### Run

`tg-ingest serve` starts the control bot automatically when it is configured,
alongside ingestion, using the same engine and approval/sizing hooks. It holds
`<db_path>.control.lock` so a second poller cannot consume the same bot updates.
The lock is released on exit or crash.

```bash
tg-ingest serve
# in another shell, inspect safely without network:
tg-ingest control status
```

`tg-ingest control run` runs only the controller (no ingestion/backfill); use it
for diagnostics or when another supervised process owns ingestion. Do not run
both `serve` and `control run` against the same database and bot token.

`/start` always resumes the persisted state. It never resets the Telegram
session, MEXC credentials, selected channel, learning progress, approval
history, or execution mode. A session that exists but is not recorded in the
database is reported as a discrepancy and requires explicit adoption; a missing
session is never silently logged out or replaced.

### Owner flow

1. `/start` → open **راهنمای ورود امن (ترمینال)**. In a trusted SSH terminal run
   `cd /opt/projects/akha1 && .venv/bin/tg-ingest auth login`; enter the phone
   number with country code, the login code, and the 2FA password if enabled
   only in that terminal. Never send a code into the chat: Telegram invalidates
   codes shared in Telegram messages. The service may stay running; the session
   lock prevents concurrent use.
2. Open **تنظیم و تأیید MEXC** and send a key restricted to **Spot read + Spot
   trade**. Keys with withdrawal permission are rejected. Verify the account
   label and permissions shown; the secret is never displayed again.
3. Open **انتخاب کانال سیگنال**; channels are listed 10 per page and can be
   searched. The existing ingestion channel is updated; no backfill starts
   automatically.
4. Open **ادامه یادگیری سیگنال** and review stored messages. Only actionable
   examples can be approved. Five consecutive approved examples complete
   learning; a rejected actionable example resets the streak. Learning is not
   permission to trade.
5. Configure **اندازه سفارش**: buys use a fixed USD notional (default `10`);
   sells use a percentage of the **free** base balance (default `25%`). Both
   remain subject to `max_quote_per_order`, reserve, fee buffer, precision,
   min-notional, open/daily caps, and fresh-balance checks.
6. Keep the default **تأیید انسانی**. Each planned intent produces one message
   with one-time, nonce-bound, expiring approve/reject buttons. `/pause` stops
   new execution immediately and returns to approval mode.

Automatic mode is available only after Telegram, MEXC, channel, and five-example
learning are ready. It requires reading a warning and pressing a second explicit
confirmation. It still enforces every risk and balance check, and it is cleared
on every process restart; the owner must confirm it again. The bot never enables
trading, live mode, withdrawals, futures, or an empty allowlist.

Automatic-mode result notices are best-effort. If Telegram is unavailable, the
order state in SQLite remains authoritative; use `tg-ingest trading intents` and
`tg-ingest trading reconcile` rather than assuming an order failed.

### Security notes

- The bot only responds to `CONTROL_OWNER_USER_ID`; all other messages and
  callbacks are rejected before revealing or changing state.
- Approval callbacks are bound to the owner, approval nonce, intent id, and
  intent version. Replays, expired approvals, stale nonces, and concurrent
  clicks are rejected atomically.
- MEXC credentials are encrypted with PBKDF2-HMAC-SHA256 and an HMAC-protected
  counter stream. Losing `CONTROL_SECRET_KEY` requires re-entering the MEXC
  credentials; it cannot be recovered from the database.
- The bot does not accept phone numbers, login codes, or 2FA passwords in chat.
  Telegram login is terminal-only; submitted codes can be invalidated by
  Telegram when shared in a chat. The terminal flow keeps these values out of
  SQLite, logs, and audit details.
- The explicit **قطع اتصال حساب تلگرام** action requires a second confirmation
  and logs out/deletes the local session. `/start` alone never does this.

## 19. Offline signal analysis and human review

`tg-ingest analyze ...` is a separate, offline pipeline for turning
**already-stored** messages into candidate signals. It does not connect to
Telegram or MEXC, does not import the trading engine or exchange client, and is
not registered in `serve`. Run it manually:

```bash
tg-ingest analyze replay --channel-id <id> --limit 100
tg-ingest analyze replay --channel-id <id> --limit 100 --dry-run
tg-ingest analyze summary
tg-ingest analyze list --parse-status needs_review
tg-ingest analyze show <candidate_id>          # no message text by default
tg-ingest analyze review <candidate_id> --decision confirm --note "checked"
tg-ingest analyze failures
```

Replay is deterministic and idempotent per
`(channel_id, message_id, edit_version, parser_version)`. Edits supersede older
candidates, deletions mark `source_deleted`, and a single bad row is isolated in
`analysis_failures` without stopping the batch. Source message text and media
are never rewritten; human corrections are append-only review rows.

Use `--dry-run`, `--limit`, `--since`, `--until`, or `--after-message-id` when
inspecting an unfamiliar database. Every DB-reading subcommand also accepts
`--db-path`, so a disposable copy can be checked without editing the production
config:

```bash
tg-ingest analyze summary --db-path /tmp/opencode/ingest-copy.sqlite3
```

## 20. Logical signal index (`analyze reindex`)

`tg-ingest analyze reindex` rebuilds the per-channel logical signal index
(`channel_signals` + `signal_message_links`) deterministically from the
already-stored messages. Verify a checksummed backup before applying, always
start with `--dry-run` aimed at a disposable copy, and re-run the apply once —
it is idempotent:

```bash
tg-ingest analyze reindex --channel-id <id> --dry-run --report-path /tmp/opencode/reindex-dry-run.json
tg-ingest analyze reindex --channel-id <id> --report-path /tmp/opencode/reindex-apply.json
tg-ingest analyze reindex --channel-id <id>   # second run must be a no-op
```

The apply records parser-v2 candidates through the shared immutable candidate
machinery and fills the two index tables in one transaction. It never rewrites
raw messages, never deletes unrelated rows, and never creates orders or orders
artefacts; `execution_status` stays `none`, SHORT signals are stored purely for
historical review, and the channel's deposit/leverage metadata never reaches
any trading surface. Link statuses (`linked` / `unresolved` / `conflict` /
`no_reference`) follow the association rules documented in
`docs/SIGNAL_ANALYSIS.md`.

For the management-bot read-only review menu «مرور سیگنال‌های اخیر» to appear,
the index must exist (run `analyze reindex` at least once).

See `docs/SIGNAL_ANALYSIS.md` for the schema, classifications, ambiguity codes,
corpus command, and the current limitation: the production database has no
messages yet, so real-format accuracy is unproven and must be validated on a
database copy after a separately authorized import.

## Operational safety

- Secrets, session files, and the database are outside version control
  (`.gitignore` enforces this; see `tests/test_gitignore.py`).
- Session files are chmod 600, parent dir 700.
- Logs never contain message contents, codes, passwords, or API values.
- Trading defaults to `sim`; live requires explicit configuration. The code
  never withdraws funds, uses futures, or touches unofficial endpoints.
