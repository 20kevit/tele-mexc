# Operations

Canonical operator guide. Trading-specific behavior is in `docs/trading.md`.
Commands assume `/opt/projects/akha1` and the project virtualenv.

## Safety posture

- One automatic MEXC Spot behaviour: no SIM/TESTONLY/LIVE product-mode split.
- Pipeline construction requires the exact owner acknowledgement, MEXC Spot
  credentials, a healthy MEXC CLI write backend, and a positive fixed
  `order_quote_amount`.
- No withdrawals, Futures/Margin order execution, leverage, short selling, symbol
  allowlist, or per-signal approval gate. Futures/Perpetual signal text may be
  ingested; eligible LONG signals execute as Spot BUY only.
- Entry orders are LIMIT-only; MARKET is rejected at configuration, risk, REST,
  CLI, and payload boundaries. Conditional entry waits for price to cross the
  configured trigger; the default is exactly 3 exchange ticks below Entry and
  a missing offset resolves to 3, never direct entry.
- `/pause` is enforced at the exchange-submission boundary; `/resume` rechecks
  readiness and cannot bypass emergency stop or reconciliation blockers.
- Both owners receive every operational event. The legacy minimum-severity
  setting is retained for compatibility but never suppresses the owners' stream.
  The programmer receives the same event stream plus technical diagnostics.
- Balance checks verify sufficiency for the exact order; no percentage of the
  account is reserved.
- The service opens no inbound ports and exposes no HTTP API.
- Telegram and MEXC use direct outbound TLS. Legacy Telegram proxy settings
  are rejected; service units unset ambient proxy variables.
- Trade lifecycle startup recovery and periodic reconciliation can place pending
  protective orders or retry requested cancellations. They are not read-only
  diagnostics. Uncertain submissions are not blindly resubmitted.

## Install and configure

```bash
cd /opt/projects/akha1
python3 -m venv .venv
.venv/bin/pip install -e '.[dev]'       # omit [dev] for production
.venv/bin/tg-ingest config init         # only creates missing files, mode 0600
```

Configuration precedence, highest first:

1. process environment variables;
2. `.env` at the project root (loaded only for keys not already in the process
   environment);
3. `config/config.toml`;
4. built-in defaults.

`.env` supplies secrets and optional environment overrides; `config.toml`
supplies runtime paths, backfill scope, and non-secret trading settings. Neither
file is committed. `TG_SESSION_PATH` selects an alternate session file.

Minimum Telegram values in `.env`:

```dotenv
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
```

MEXC values required for the automatic Spot pipeline:

```dotenv
MEXC_API_KEY=
MEXC_API_SECRET=
MEXC_BASE_URL=https://api.mexc.com
```

Optional trading overrides (environment wins over DB/TOML):

```dotenv
TRADING_EXECUTION_ACKNOWLEDGEMENT=I_UNDERSTAND_REAL_MONEY_AND_RISKS
TRADING_ORDER_QUOTE_AMOUNT=3
MEXC_CLI_BINARY=
MEXC_CLI_PROFILE=
```

`TRADING_MODE`, `TRADING_LIVE_TOKEN`, and `TRADING_ORDER_TYPE_POLICY` are
rejected. Obsolete TOML keys for allowlists, aggregate budgets, daily caps, or
SELL sizing are rejected; old environment names for those removed settings are
not read.

Validate without contacting anything:

```bash
.venv/bin/tg-ingest config check
```

It prints paths, credential presence, the acknowledgement state, and the
effective fixed amount — not secret values.

### Credential and permission matrix

| Credential | Where it belongs | Required permission/use |
| --- | --- | --- |
| `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` | private `.env` | Telegram application credentials for MTProto |
| Telegram session file | `config/tg_session.session` or `$TG_SESSION_PATH.session`, mode `0600` | Personal-account session; never commit or copy into chat |
| `CONTROL_BOT_TOKEN` | private `.env` | BotFather token for the single ControlBot |
| `CONTROL_OWNER_USER_ID` / `CONTROL_OWNER_B_USER_ID` / `CONTROL_DEVELOPER_USER_ID` | private `.env` | Numeric Telegram user IDs; all distinct. Owner A and Owner B have full owner controls; the programmer has read-only technical/diagnostic access and receives every event |
| `CONTROL_SECRET_KEY` | private `.env` | Encrypts MEXC credentials stored by the control menu |
| `MEXC_API_KEY` / `MEXC_API_SECRET` | private `.env` | Spot account read + Spot trade; never withdrawal |
| MEXC CLI profile | CLI-owned `~/.mexc/profiles` | Same Spot account/key as REST; used for writes |

REST reads (account, metadata, order queries, fills) and CLI writes (place,
cancel, OCO) must resolve to the same MEXC Spot account. `trading preflight
--network` performs the read-only identity check. Never put secret values in
`config.toml`, documentation, logs, or chat.


## Telegram login and channel selection

Run login only in a trusted SSH terminal. Never send a login code or 2FA password
through Telegram, a bot, a log, or a shell argument.

```bash
.venv/bin/tg-ingest auth login
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest channels list
.venv/bin/tg-ingest channel select <bare_id_or_@username_or_title>
```

- Login is interactive; the session is stored at `config/tg_session.session`
  (or `$TG_SESSION_PATH.session`), mode `0600`.
- A second account can use a separate session:
  `TG_SESSION_PATH=config/tg_session_alt .venv/bin/tg-ingest auth login`.
- `diagnostics telegram` confirms access to the selected channel without
  printing ids or fetching history.
- `auth logout` performs a server-side logout and removes local session files.
- A session lock prevents concurrent login/use by the bot and CLI.

## Backfill

```bash
.venv/bin/tg-ingest backfill run
.venv/bin/tg-ingest backfill status
```

`backfill_scope` is `full` or `since`; `since` requires `backfill_since` in
`config/config.toml`. The import is resumable: pages and checkpoints are
committed together. Re-running after completion is a no-op. Backfilled messages
are permanently non-tradeable.

`serve` starts the backfill automatically when status is `not_started` or
`in_progress`. `backfill run` is for foreground/diagnostic use.

## Run the ingestion service

```bash
.venv/bin/tg-ingest serve
```

`serve` performs live ingestion, recovery, backfill, queue processing, trading,
and notification delivery. It holds `<db_path>.serve.lock`; a second `serve`
exits with an `already holds ... serve.lock` message. Stop with Ctrl+C or
SIGTERM for a clean shutdown.

Foreground status and message inspection while it runs (normal repository
commands may initialize or migrate the configured database):

```bash
.venv/bin/tg-ingest status
.venv/bin/tg-ingest backfill status
.venv/bin/tg-ingest messages list --limit 20
.venv/bin/tg-ingest messages show <message_id>
.venv/bin/tg-ingest queue list --status pending
```

`queue retry` is a separate action that re-enqueues failed items, not an
inspection command. Run it only when reprocessing is intended.

Quiet periods are normal: `status` may show no recent live event without any
failure.

## systemd

```bash
cp deploy/tg-ingest.service /etc/systemd/system/tg-ingest.service
systemctl daemon-reload
systemctl enable --now tg-ingest
systemctl status tg-ingest
journalctl -fu tg-ingest
```

The unit runs as root in `/opt/projects/akha1`, uses `.venv/bin/tg-ingest
serve`, restarts on failure, and strips ambient proxy variables. It exposes no
ports.

The primary control bot can run embedded in `serve` or separately with
`control run`. It takes the control lock. Do not run a second controller against
the same database.

| Unit | Command | Locks |
| --- | --- | --- |
| `tg-ingest.service` | `tg-ingest serve` | serve + optional control |

Restart only after an intentional code/config change:

```bash
systemctl restart tg-ingest
systemctl is-active tg-ingest
journalctl -u tg-ingest -n 50 --no-pager
```

A restart establishes the **per-channel** activation boundary from Telegram's
newest visible message id plus the newest message already stored **before live
handlers are registered**. Everything at or below that boundary becomes
non-tradeable history. Messages recovered during the startup sweep remain above
the snapshot but are non-tradeable by `source='recovery'`; a genuinely new live
message delivered after the snapshot is still tradeable. If Telegram's newest id
cannot be established, new entries fail closed. Expect the boundary to advance on
every restart, and do not rewrite it manually. A planned intent that predates
the restart is canceled locally and is never submitted after downtime.

MEXC REST calls are protected by a shared, file-backed weight limiter. The
defaults are `60` IP weight/10 s and `100` UID weight/10 s (official limits are
`300` and `500`). The order/cancel gate is `12` requests/second. The limiter
state lives under `data/mexc-rate-limit-*.json`; do not delete it while a
process is running. A `429` or `418` is honored via `Retry-After`; do not retry
in a tight loop.

## Bots

### Control bot (integrated or standalone)

```bash
.venv/bin/tg-ingest control status
.venv/bin/tg-ingest control run
```

When `CONTROL_BOT_ENABLED` is true and the bot is fully configured,
`tg-ingest serve` starts the ControlBot inside the service process and holds
both `<db>.serve.lock` and `<db>.control.lock`. If control is disabled,
`serve` still uses the Bot API client only for durable trading notifications.

`control run` starts the same controller without ingestion, backfill, queue,
trading, or market-monitor workers and takes only `<db>.control.lock`. Never run
it against a database already served by `serve`. Authorization covers Owner A
(`CONTROL_OWNER_USER_ID`) and Owner B (`CONTROL_OWNER_B_USER_ID`) with full
owner-level access. The programmer (`CONTROL_DEVELOPER_USER_ID`) is not an
owner: it receives every operational event and technical diagnostics but can
only use read-only diagnostic commands (`/status`, `/version`, `/help`). Owner
guide: `docs/CONTROL_BOT_FA.md`.

### Telegram session revoked

```bash
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest auth login
```

The control process may stay running; the session lock serializes access. Never
send the code to a chat.

### Exchange uncertainty

An uncertain order remains `unknown_requires_reconciliation`; it is never
retried automatically.

```bash
.venv/bin/tg-ingest trading intents
.venv/bin/tg-ingest trading reconcile
```

`trading reconcile` inspects non-terminal intents (and terminal fills that still
need synchronization). It builds the configured MEXC client, so credentials and
the CLI backend must be available. Legacy `sim`/`testonly` rows are ignored.
Repeat after network recovery. Do not cancel or resubmit based only on a local
guess; inspect the exchange order id/status.

```bash
.venv/bin/tg-ingest trading notifications       # pending/failed outbox rows
.venv/bin/tg-ingest trading notifications --status ambiguous --limit 50
```

The notification outbox is independent of order state; reviewing it never
re-submits an order. Ambiguous deliveries (process ended mid-delivery) are
recovered once on startup and never retried automatically.

### Terminal-intent / signal consistency repair

Code now performs this synchronization inside the intent transition. For a row
written before that fix, the explicit command is:

```bash
.venv/bin/tg-ingest trading repair-signals           # read-only dry-run
.venv/bin/tg-ingest trading repair-signals --apply   # persist only active signal statuses
```

It maps `rejected`/`failed` intents to a `rejected` signal, never touches
`superseded`/`canceled`/`deleted` signals, never transitions an intent, and never
calls MEXC. Review the dry-run output before `--apply`. Prefer a maintenance
window with `serve` stopped; the update is transactional but the service can
still be processing unrelated messages.

## Diagnostics and troubleshooting

```bash
.venv/bin/tg-ingest diagnostics network    # DNS/TCP/TLS; no credentials
.venv/bin/tg-ingest diagnostics bot        # Bot API getMe; read-only
.venv/bin/tg-ingest diagnostics telegram   # network + real session + channel
.venv/bin/tg-ingest diagnostics clock      # MEXC server-clock snapshot/skew (requires live credentials)
.venv/bin/tg-ingest diagnostics audit      # audit journal health/counts
.venv/bin/tg-ingest diagnostics orders     # intent traces and state diagnostics
.venv/bin/tg-ingest diagnostics reconcile  # unresolved reconciliation diagnostics
```

Clock snapshot shows the anchored server time, RTT, sample count, trusted flag,
and last error. Writes fail closed when `trusted` is false.

| Symptom | Action |
| --- | --- |
| `DIRECT NETWORK FAILURE` | Check DNS, default route, outbound TCP 443; rerun `diagnostics network` |
| `DIRECT TELEGRAM FAILURE` | Verify Telegram reachability; retry |
| `No valid Telegram session` | Run `auth login` in a terminal |
| `PHONE_CODE_EXPIRED` | Code expired/used/shared in Telegram; retry later in terminal only |
| Session lock busy | Wait for the bot/CLI operation; do not stop the service just for the lock |
| `channel not accessible` | Account left channel or selection is stale; list and reselect |
| FloodWait | Honor the interval; checkpoints preserve progress |
| Queue `failed` > 0 | Inspect `queue list --status failed`, fix cause, `queue retry` |
| `database is locked` | Maintenance overlapped a long write; retry once |
| Stale balance rejection | Check clock/account snapshot freshness and free balance |
| Live order uncertain | Inspect intents, then `trading reconcile`; never guess/resubmit |
| `BUY_BLOCK_ACTIVE` / new BUYs blocked | Inspect `trading status` and `app_state`; an open position lacks verified exit protection. Run `trading reconcile` to restore OCO coverage; the block clears only when every position is fully sold or has exchange-confirmed working coverage |
| MEXC `700003` / `700005` timestamp | Read-only signed calls refresh the clock once and retry; writes never retry. Check `diagnostics clock` skew; a stale wall clock or rate-gate wait can push a write outside `recvWindow` |
| MEXC `429`/`418` | Honor `Retry-After`; check the shared limiter files/logs; never hammer the API |
| Terminal intent `rejected`, signal still active | Dry-run `trading repair-signals`, review the exact rows, then `--apply` |
| No tradeable message after restart | Expected: the activation boundary advanced or the row is recovery/backfill-sourced |

## Upgrade, migration and rollback

Pre-upgrade:

1. Confirm the target commit and read the release notes/diff.
2. Stop `tg-ingest` (and any standalone `control run`) cleanly so no writer is active.
3. Record `tg-ingest status`, `trading status`, active intents/trades, and any
   `unknown_requires_reconciliation` rows.
4. Do not upgrade while an order submission or reconciliation is in flight.

WAL-safe backup:

```bash
mkdir -p /var/backups/tg-signal
.venv/bin/python -c "import sqlite3; s=sqlite3.connect('data/ingest.sqlite3'); d=sqlite3.connect('/var/backups/tg-signal/pre-upgrade.sqlite3'); s.backup(d); d.close(); s.close()"
chmod 600 /var/backups/tg-signal/pre-upgrade.sqlite3
```

Update and migrate:

```bash
git fetch public
git switch opencode/tele-mexc-completion
git pull --ff-only public opencode/tele-mexc-completion
.venv/bin/pip install -e '.[dev]'   # or without [dev] for production
.venv/bin/tg-ingest config check
.venv/bin/tg-ingest status          # opens/migrates the configured DB
```

Start and verify:

```bash
.venv/bin/tg-ingest trading readiness --network   # must print READY; never places orders
systemctl start tg-ingest
systemctl is-active tg-ingest
journalctl -u tg-ingest -n 100 --no-pager
.venv/bin/tg-ingest status
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest trading intents
```

Rollback constraints:

- Migrations are append-only; application code rollback does not reverse schema
  changes. Keep the pre-upgrade database backup for a true data rollback.
- Stop the service before replacing the database or its `-wal`/`-shm` sidecars.
- Never roll back by deleting migration rows or editing historical records.
- If live positions or `unknown_requires_reconciliation` intents exist, do not
  roll back blindly: reconcile/close exposure first or keep the newer code and
  have the owner review every open order.
- The activation boundary advances on every restart; an old code/image rolled
  back may reject newly arrived messages until its boundary is re-established.

## Backup and restore

SQLite uses WAL. Use the SQLite backup API; do not copy only the main file while
the service is writing.

```bash
mkdir -p /var/backups/tg-signal
.venv/bin/python -c "import sqlite3; s=sqlite3.connect('data/ingest.sqlite3'); d=sqlite3.connect('/var/backups/tg-signal/ingest-backup.sqlite3'); s.backup(d); d.close(); s.close()"
chmod 600 /var/backups/tg-signal/ingest-backup.sqlite3
```

Restore:

1. Stop `tg-ingest` and `control` processes.
2. Preserve the current database and `-wal`/`-shm` files for rollback.
3. Replace `data/ingest.sqlite3` with the backup.
4. Remove stale `data/ingest.sqlite3-wal` and `-shm`.
5. Set mode `0600`, start one process, check `status` and `trading status`.

The database and backups contain message content, intents, and audit data. They
are sensitive and are not covered by the application's logging redaction.

## Secrets and file hygiene

- `.env`, `config/config.toml`, sessions, media, and the database are
  gitignored; verify with `git status --porcelain`.
- Session files are mode `0600`; parent directory `0700`.
- If an API key/token was exposed: revoke it at the source, create a new one, and
  re-enter it locally. Rotating `CONTROL_SECRET_KEY` makes stored MEXC secrets
  unreadable and requires re-entry.
- Logs and notifications omit credentials, tokens, and signed parameters. Do not
  add message text or secrets to logs.
