# Live Telegram verification procedure (manual, one time)

This is the documented **manual integration test** that requires real Telegram
credentials and cannot be part of the automated suite (which is fully mocked
and offline).

Owner of the Telegram account performs the interactive steps; the operator
records the outcomes. This project uses direct Telegram networking only; no
proxy listener or proxy variables are required.

## Completed on the new VPS without owner credentials (2026-10-22 audit)

| Check | Result |
| --- | --- |
| 1. `tg-ingest diagnostics network` | OK — direct DNS + TCP 443 to `api.telegram.org` and all four tested Telegram DCs; Bot API TLS 1.3 handshake verified |
| 2. Ambient proxy isolation (`HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` pointed at a dead local port) | diagnostics and Bot API `getMe` still succeeded; the variables are reported as ignored |
| 3. `tg-ingest diagnostics bot` | OK — read-only `getMe` over direct HTTPS |
| 4. `tg-ingest diagnostics telegram` | OK — direct Telethon MTProto encrypted RPC (`help.getConfig`); existing session is present but not authenticated |
| 5. Legacy `TELEGRAM_PROXY_*` variables / `[proxy]` config section | rejected with a clear direct-networking migration error |
| 6. `systemd-analyze verify deploy/tg-ingest.service deploy/tg-manage.service` | exit 0 |
| 7. Installed `tg-manage.service` | active; direct TCP connection to `api.telegram.org:443`; no proxy variables in its process environment |
| 8. Session path consistency (CLI vs systemd, arbitrary CWD) | `/opt/projects/akha1/config/tg_session.session` in all cases |
| 9. Session file permissions | `0600` (parent directory `0700`) |
| 10. SQLite integrity and preserving migration | `PRAGMA integrity_check` = `ok`; existing rows retained (currently 0 messages, 0 queue items) |
| 11. Automated suite | 273 tests passed; ruff and mypy clean |

The authenticated account identity and channel metadata checks remain blocked
until the account owner completes the Telegram session login. The existing
session file was **not** deleted, replaced, or logged out during this audit.

## Automated control-bot checks (2026 audit)

| Check | Result |
| --- | --- |
| `/start` resumes persisted setup; unauthorized users are rejected before state access | covered by `tests/test_control_service.py` |
| Configured developer receives owner-equivalent control-bot access (start/version/pause/resume); owner access preserved; strangers blocked | covered by `tests/test_control_service.py` |
| Existing valid session requires explicit adoption; in-chat login is disabled; stale challenges are discarded; codes/phone/passwords are never persisted | covered by `tests/test_control_auth.py`, service tests, and terminal-login tests |
| Five consecutive actionable approvals complete learning; invalid examples cannot be approved | covered by `tests/test_control_store.py` and service tests |
| Approval callbacks are owner-bound, nonce-checked, expiring, one-time, version-checked | covered by store and service tests |
| Automatic mode needs two confirmations, `/pause` revokes it, restart clears it | covered by service tests |
| Fixed-USD buy and percentage-of-free-base sell remain capped by `max_quote_per_order` | covered by `tests/test_sizing.py` |
| Encrypted MEXC storage and tamper detection | covered by `tests/test_control_store.py` |
| Bot API transport ignores ambient proxy variables and never exposes the token in errors | covered by `tests/test_control_bot_api.py` |
| Telethon client is constructed with no proxy and direct diagnostics fail closed | covered by `tests/test_connectivity.py` |

> Superseded on 2026-10-27: the per-order amount is no longer capped by
> `max_quote_per_order`. That key remains only a legacy config fallback for
> `order_quote_amount`; see `docs/trading.md`.

## Two-operator management-bot test (developer first, then owner)

**Not yet performed against Telegram.** These steps require a real account and
must be executed interactively by each operator; the automated suite only proves
the state machine and redaction logic.

The management bot runs as `tg-manage.service`. Both `CONTROL_OWNER_USER_ID` and
`CONTROL_DEVELOPER_USER_ID` must be present and different; only those numeric
IDs in a private chat receive responses.

| # | Step | Expected | Recorded |
| --- | --- | --- | --- |
| M1 | Developer sends `/start` in a private chat | status/menu identifies role `توسعه‌دهنده`; no channel selected | |
| M2 | Developer presses the secure-login help button | response gives the SSH command; **no code is requested in chat**; no phone/code/password is accepted | |
| M2b | Developer runs `.venv/bin/tg-ingest auth login` in a trusted SSH terminal and enters phone/code/optional 2FA only there | `Login succeeded ...`; SQLite/logs contain no phone/code/password; `tg-manage` may stay running and the session lock prevents overlap | |
| M3 | Developer presses **channel list** | 10 channels per page with title and stable id; next/previous/refresh/cancel | |
| M4 | Developer selects and confirms a channel | identity re-checked; message states no ingestion started | |
| M5 | Developer presses **preview latest 10** | ≤10 messages with id/UTC time; media-only shows a marker; no download/queue/ingestion | |
| M6 | Developer sends `/logout`, then confirms | server-side `logOut` attempted; session files removed only on success; both operators notified | |
| M7 | Owner sends `/start` and reviews status | owner sees the shared session state; no separate in-chat login flow exists | |
| M8 | Owner previews the real channel | read-only preview works; no ingestion/trading service started | |

The session is shared by both operators. To test a different owner account,
logout first, then have that owner run the terminal login; there is no per-chat
login challenge. Do not run `serve` (which uses the same session) while the bot
is using it. `tg-ingest auth login`/`auth logout` acquire the same advisory lock
on `config/tg_session.session.lock`, so a busy-lock message means wait and retry,
not that the service must be stopped.

## CLI owner-interactive steps (not part of the management-bot test)

The terminal login is now the primary path; the management service does not need
to be stopped for it because both use the session lock. The remaining table
covers ingestion/trading verification and is not required for the read-only
management-bot test.

| # | Step | Expected | Recorded |
| --- | --- | --- | --- |
| A | `tg-ingest config check` | `configuration OK`, credentials `set`, transport direct | |
| B | `tg-ingest auth login` (owner enters phone, code, optional 2FA) | `Login succeeded for ... Session persisted` | |
| C | `ls -l config/tg_session.session` | permissions `-rw-------` | |
| D | `tg-ingest auth check` | `Session status: valid` | |
| E | `tg-ingest channels list` | displays channels incl. target | |
| F | `tg-ingest channel select <id>` | `selected channel: ...` | |
| G | `tg-ingest backfill run` | progress counts and `history import complete` | |
| H | `tg-ingest backfill run` (re-run) | no duplicates (unchanged counts)  | |
| I | `tg-ingest backfill status` | `backfill: status=done` | |
| J | `systemctl enable --now tg-ingest` after a channel is selected and foreground `serve` is clean | `systemctl status tg-ingest` active; live test message stored | |
| K | owner posts a test message in the channel | `live message ... stored` in `journalctl -fu tg-ingest` | |
| L | owner edits the message | `live edit of ... stored (version 0 -> 1)`; `tg-ingest messages show <id>` lists edit history | |
| M | owner deletes that message (service online) | `deletion event: 1 message(s) newly marked deleted` | |
| N | stop daemon; wait 2+ min; restart; owner posts 1-2 messages while offline | `recovery sweep complete: N gap message(s) fetched` on the next start | |
| O | queue snapshot | `tg-ingest queue list` shows pending/processed counts matching captured messages | |
| P | temporarily deny outbound Telegram egress in a test environment | service logs a direct connectivity failure with bounded retry; no proxy fallback exists | |
| Q | `tg-ingest control status` | configured, state progresses `not_configured` → `ready`; token/secret are redacted | |
| R | owner sends `/start` to the bot | Persian status/menu; existing setup resumes; no login reset | |
| S | owner configures MEXC with a read+trade key | `verified`; withdrawal-enabled keys are rejected; secret never echoed | |
| T | owner confirms five actionable examples | learning `completed`; trading remains disabled and mode remains approval | |
| U | a simulated signal appears while in approval mode | one approval message; duplicate clicks cannot execute twice | |
| V | owner enables automatic mode via both warnings, then restarts | restart returns to human approval; no silent automatic execution | |
| W | `git status --short` (from repo root) | no `.env`, no `*.session`, no `*.sqlite3` | |

Ready-to-paste owner command block (steps A through F):

```bash
cd /opt/projects/akha1
.venv/bin/tg-ingest config check
.venv/bin/tg-ingest auth login       # enter secrets only in this terminal
ls -l config/tg_session.session
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest channels list
.venv/bin/tg-ingest channel select <ID-OR-@USERNAME>
# tg-manage may remain active; if the session lock is busy, wait and retry.
```

Notes:

- Deletion events (step M) are only received while the service is connected;
  deletions during downtime are not recoverable (documented limitation in
  `README.md`).
- Step P must be performed against a disposable test environment; do not
  interrupt production networking to test the failure path.
- The direct network diagnostics are read-only and safe to repeat:
  `tg-ingest diagnostics network`, `diagnostics bot`, `diagnostics telegram`.
