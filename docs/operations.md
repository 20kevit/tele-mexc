# Operations

Canonical operator guide. Trading-specific behavior is in `docs/trading.md`.
Commands assume `/opt/projects/akha1` and the project virtualenv.

## Safety posture

- Default `trading.mode = "sim"`: no exchange calls.
- `testonly` sends only to the official order-test endpoint.
- `live` requires an explicit token and a positive fixed `order_quote_amount`.
- No withdrawals, futures, leverage, short selling, or per-signal approval gate.
- The service opens no inbound ports and exposes no HTTP API.
- Telegram and MEXC use direct outbound TLS. Proxy variables are rejected.

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

MEXC values, only when needed:

```dotenv
MEXC_API_KEY=
MEXC_API_SECRET=
MEXC_BASE_URL=https://api.mexc.com
```

Validate without contacting anything:

```bash
.venv/bin/tg-ingest config check
```

It prints paths, mode, credential presence, and the effective fixed amount — not
secret values.

## Telegram login and channel selection

Run login only in a trusted SSH terminal. Never send a login code or 2FA password
through Telegram, a bot, a log, or a shell argument.

```bash
.venv/bin/tg-ingest auth login
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest channels list
.venv/bin/tg-ingest channel select <bare_id_or_@username_or_title>
.venv/bin/tg-ingest auth check-channel
```

- Login is interactive; the session is stored at `config/tg_session.session`
  (or `$TG_SESSION_PATH.session`), mode `0600`.
- A second account can use a separate session:
  `TG_SESSION_PATH=config/tg_session_personal .venv/bin/tg-ingest auth login`.
- `check-channel` confirms access to the already-selected channel without
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

Foreground inspection while it runs:

```bash
.venv/bin/tg-ingest status
.venv/bin/tg-ingest backfill status
.venv/bin/tg-ingest messages list --limit 20
.venv/bin/tg-ingest messages show <message_id>
.venv/bin/tg-ingest queue list --status pending
.venv/bin/tg-ingest queue retry
```

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

Management bot:

```bash
cp deploy/tg-manage.service /etc/systemd/system/tg-manage.service
systemctl daemon-reload
systemctl enable --now tg-manage
journalctl -fu tg-manage
```

`tg-manage-personal.service` is the same bot with
`TG_SESSION_PATH=config/tg_session_personal` and the same token/database. Run
only one of `tg-ingest`, `tg-manage`, `tg-manage-personal`, or `control run`
against a given database at a time as an operating rule.

| Unit | Command | Locks |
| --- | --- | --- |
| `tg-ingest.service` | `tg-ingest serve` | serve |
| `tg-manage.service` | `tg-ingest manage run` | serve + control |
| `tg-manage-personal.service` | `TG_SESSION_PATH=... manage run` | serve + control |

Restart only after an intentional code/config change:

```bash
systemctl restart tg-ingest
systemctl is-active tg-ingest
journalctl -u tg-ingest -n 50 --no-pager
```

A restart advances the trading activation boundary to the newest message
already stored **before live handlers are registered**. Everything at or below
that snapshot becomes non-tradeable history. Messages recovered during the
startup sweep remain above the snapshot but are non-tradeable by
`source='recovery'`; a genuinely new live message delivered after the snapshot
is still tradeable. Expect the boundary to advance on every restart, and do not
rewrite it manually.

MEXC REST calls are protected by a shared, file-backed weight limiter. The
defaults are `60` IP weight/10 s and `100` UID weight/10 s (official limits are
`300` and `500`). The order/cancel gate is `12` requests/second. The limiter
state lives under `data/mexc-rate-limit-*.json`; do not delete it while a
process is running. A `429` or `418` is honored via `Retry-After`; do not retry
in a tight loop.

## Bots and offline pipelines

### Management bot (current)

```bash
.venv/bin/tg-ingest manage status
.venv/bin/tg-ingest manage run
```

Requires `CONTROL_BOT_TOKEN`, `CONTROL_OWNER_USER_ID`, and
`CONTROL_DEVELOPER_USER_ID` (positive and different). It lists channels,
previews the latest ten messages, supports a developer-self-authorized bounded
validation sample, and performs safe logout. It never starts the parser, queue,
MEXC client, risk engine, or orders. UI details:
`docs/MANAGEMENT_BOT_FA.md`.

### Legacy control bot

```bash
.venv/bin/tg-ingest control status
.venv/bin/tg-ingest control run
```

This owner controller is **not embedded in `serve`**. `serve` reuses only its Bot
API client to deliver trading notifications. `control run` runs the controller
alone, takes `<db>.control.lock`, and performs no ingestion/backfill. Do not run
it alongside `serve` in normal operation. Authorization covers both the configured
owner (`CONTROL_OWNER_USER_ID`) and the configured developer
(`CONTROL_DEVELOPER_USER_ID`) with owner-equivalent access to every control-bot
command and administrative action. Owner guide: `docs/CONTROL_BOT_FA.md`.

### Offline analysis

```bash
.venv/bin/tg-ingest analyze summary
.venv/bin/tg-ingest analyze replay --channel-id <id> --limit 100 --dry-run
.venv/bin/tg-ingest analyze list --parse-status needs_review
.venv/bin/tg-ingest analyze show <candidate_id>
.venv/bin/tg-ingest analyze review <candidate_id> --decision confirm --note "checked"
.venv/bin/tg-ingest analyze failures
```

Analysis reads already-stored messages, never contacts Telegram/MEXC, and never
creates orders. Use `--db-path` for a disposable copy. Details:
`docs/SIGNAL_ANALYSIS.md`.

## MEXC credentials and verification

Store `MEXC_API_KEY`/`MEXC_API_SECRET` only in the private `.env` (mode `0600`),
never in `config.toml`, the systemd unit, chat, or command arguments. Use a key
with Spot read + Spot trade only, preferably IP-whitelisted; withdrawal
permission must be disabled.

```bash
.venv/bin/tg-ingest trading status      # presence + effective config/state
.venv/bin/tg-ingest trading verify      # read-only apiKeyInfo; no order
```

Verification changes nothing: trading stays in whatever mode the config says.
Changed `.env`/config values require a process restart.

### Official CLI profile and preflight

Live order *writes* use the official `@mexc-ai/cli` binary. Its credentials are
stored in the CLI's own encrypted profile and are never copied into `.env` or
this application. The CLI accepts the same standard MEXC Open API Access/Secret
key pair used by the REST client (Spot read + Spot trade; no withdrawal):

```bash
mexc profile set tg-ingest     # interactive; enter the key/secret in your terminal
mexc profile use tg-ingest
mexc profile list              # no credentials needed
.venv/bin/tg-ingest trading preflight            # local binary/version/schema/profile checks
.venv/bin/tg-ingest trading preflight --network  # + doctor, current-orders, account identity
```

`preflight` never places, cancels or modifies an order and prints no secrets.
`--network` fails without running the signed read when the CLI `doctor` reports a
problem, when the configured CLI profile is missing, or when the CLI profile and
the REST key read different Spot account balances. Live mode additionally
rejects `[mexc_cli] enabled = false` at config load. The minimum version is
`0.0.22`. Live mode arms non-interactive CLI writes (`--yes`) automatically when
`mexc_cli.confirm_noninteractive_writes` is absent; an explicit `false` remains a
fail-closed override and makes every live write reject locally.

## Recovery

### After reboot or network outage

No manual replay is needed. `serve` reconnects with backoff, sweeps the message
id gap above the stored watermark, refreshes the recent window for edits, and
requeues queue items left in `processing`. Backfill resumes from its checkpoint.

### Telegram session revoked

```bash
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest auth login
```

The management bot may stay running; the session lock serializes access. Never
send the code to a chat.

### Exchange uncertainty

An uncertain order remains `unknown_requires_reconciliation`; it is never
retried automatically.

```bash
.venv/bin/tg-ingest trading intents
.venv/bin/tg-ingest trading reconcile
```

`trading reconcile` inspects live intents (and terminal fills that still need
synchronization). It queries MEXC only when a MEXC client can be built, so run it
with the live/testonly credentials configured. In `sim` it is a local no-op for
live rows. Repeat after network recovery. Do not cancel or resubmit based only on
a local guess; inspect the exchange order id/status.

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
| Bounded validation unavailable | Run `analyze reindex` first and check management state |

## Backup and restore

SQLite uses WAL. Use the SQLite backup API; do not copy only the main file while
the service is writing.

```bash
mkdir -p /var/backups/tg-signal
.venv/bin/python -c "import sqlite3; s=sqlite3.connect('data/ingest.sqlite3'); d=sqlite3.connect('/var/backups/tg-signal/ingest-backup.sqlite3'); s.backup(d); d.close(); s.close()"
chmod 600 /var/backups/tg-signal/ingest-backup.sqlite3
```

Restore:

1. Stop `tg-ingest`, `tg-manage`, and `control` processes.
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
