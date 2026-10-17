# Operator Guide — tg-signal-ingest

A single-user Telegram ingestion service for one signal channel. It stores every
channel message (text, media, edits, deletions, replies, forwards, albums) in a
local SQLite database and exposes a durable downstream processing queue.
**This phase does not parse signals and does not trade.**

All commands below are copy-paste-ready and were executed on this VPS
(Ubuntu 24.04, Python 3.12, project at `/opt/projects/akha1`).

---

## 1. Prerequisites

- Ubuntu Linux VPS with Python 3.12 (already the system python: `python3 --version`)
- The proxy listener `http://127.0.0.1:1081` must be reachable
  (ProxyHub-managed xray HTTP inlet; verified on this host, do not modify)
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
# Proxy defaults below are already correct for this VPS; adjust only if ProxyHub changes
TELEGRAM_PROXY_TYPE=http
TELEGRAM_PROXY_HOST=127.0.0.1
TELEGRAM_PROXY_PORT=1081
```

Optional runtime settings live in `config/config.toml` (paths, backfill scope,
media download flag, sweep sizes). Both files are chmod 0600 and git-ignored.

Validate:

```bash
tg-ingest config check
```

## 5–6. First-run login (performed by the account owner, interactive)

```bash
tg-ingest auth login
```

The command asks for:

1. **Phone number** with country code (e.g. `+15550101337`)
2. The **login code** Telegram sends to that account (arrives as an app/phone
   message within seconds)
3. The **2FA password**, only if two-step verification is enabled

Rejected or expired codes are reported and can be re-entered; the session is
then persisted locally (never printed, never uploaded). Verify afterwards:

```bash
tg-ingest auth check
```

## 7. List and select the destination channel

```bash
tg-ingest channels list
tg-ingest channel select <bare_id_or_@username_or_title>   # e.g. channel select 1946012345
```

Only channels/supergroups are candidates; the selection is persisted in the
database and is used by every subsequent run.

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

`serve` connects through the proxy, verifies the stored session, attaches the
live listener, bridges any offline gap with a recovery sweep, runs the history
import concurrently when pending, and drives the downstream processing queue.
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
tg-ingest auth login     # repeat the interactive flow of section 5
```

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
| `PROXY FAILURE: TCP connect ...` | listener down: `ss -tlnp | grep 1081`; check ProxyHub |
| `HTTP CONNECT through proxy failed: ...` | check with `tg-ingest diagnostics proxy` (shows which District check failed) |
| `No valid Telegram session...` | session missing/revoked: `tg-ingest auth login` |
| `channel not accessible with this session` | account left the channel or channel deleted; re-run `channels list`, re-select |
| `history fetch kept hitting FloodWait` | Telegram throttling; temporarily restart later; the checkpoint preserves progress |
| queue `failed` > 0 | inspect `queue list --status failed`, then `queue retry` |
| DB locked warnings | a maintenance CLI ran while `serve` was busy; retry once |

Diagnostics, in the required order:

```bash
tg-ingest diagnostics proxy      # network level (works without credentials)
tg-ingest diagnostics telegram   # adds session + channel access verification
```

## 16. How the future signal parser consumes messages

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
  must treat media without OCR as `requires image analysis`.

## Operational safety

- Secrets, session files, and the database are outside version control
  (`.gitignore` enforces this; see `tests/test_gitignore.py`).
- Session files are chmod 600, parent dir 700.
- Logs never contain message contents, codes, passwords, or API values.
- No orders, exchange, or trading code exists in this phase.
