# Live Telegram verification procedure (manual, one time)

This is the documented **manual integration test** that requires real Telegram
credentials and cannot be part of the automated suite (which is fully mocked
and offline).

Owner of the Telegram account performs the interactive steps; the operator
records the outcomes.

## Already completed without owner credentials (2026 audit)

| Check | Result |
| --- | --- |
| 1. `tg-ingest diagnostics proxy` | OK — CONNECT tunnel to Telegram DC 149.154.167.51:443; direct egress blocked (expected) |
| 2. Wrong-port failure path (`TELEGRAM_PROXY_PORT=30999`) | clear `PROXY FAILURE`, exit 1, no silent direct fallback |
| 3. `systemd-analyze verify deploy/tg-ingest.service` | exit 0 |
| 4. Session path consistency (CLI vs systemd, arbitrary CWD) | `/opt/projects/akha1/config/tg_session.session` in all cases |
| 5. **Real MTProto handshake through the proxy** (Telethon `connect()` with throwaway session, no sign-in) | `connected=True, signed-in=False` — transport path fully verified, auth-key exchange with the DC succeeded through `127.0.0.1:1081` |
| 6. Automated suite | 68 tests passed, ruff + mypy clean |

## Owner-interactive steps (start here)

| # | Step | Expected | Recorded |
| --- | --- | --- | --- |
| A | `tg-ingest config check` (after filling `.env`) | `configuration OK`, credentials `set` | |
| B | `tg-ingest auth login` (owner enters phone, code, optional 2FA) | `Login succeeded for ... Session persisted` | |
| C | `ls -l config/tg_session.session` | permissions `-rw-------` | |
| D | `tg-ingest auth check` | `Session status: valid` | |
| E | `tg-ingest channels list` | displays channels incl. target | |
| F | `tg-ingest channel select <id>` | `selected channel: ...` | |
| G | `tg-ingest backfill run` | progress counts and `history import complete` | |
| H | `tg-ingest backfill run` (re-run) | no duplicates (unchanged counts)  | |
| I | `tg-ingest backfill status` | `backfill: status=done` | |
| J | `cp deploy/tg-ingest.service /etc/systemd/system/ && systemctl daemon-reload && systemctl enable --now tg-ingest` if a test channel exists — see note | `systemctl status tg-ingest` active; live test message stored | |
| K | owner posts a test message in the channel | `live message ... stored` in `journalctl -fu tg-ingest` | |
| L | owner edits the message | `live edit of ... stored (version 0 -> 1)`; `tg-ingest messages show <id>` lists edit history | |
| M | owner deletes that message (service online) | `deletion event: 1 message(s) newly marked deleted` | |
| N | stop daemon; wait 2+ min; restart; owner posts 1-2 messages while offline | `recovery sweep complete: N gap message(s) fetched` on the next start | |
| O | queue snapshot | `tg-ingest queue list` shows pending/processed counts matching captured messages | |
| P | stop proxy listener briefly (only with explicit ProxyHub admin authorization) — see note | service logs proxy failure and keeps retrying; never falls back to direct connection | |
| Q | `git status --short` (from repo root) | no `.env`, no `*.session`, no `*.sqlite3` | |

Ready-to-paste owner command block (steps A through F):

```bash
cd /opt/projects/akha1
.venv/bin/tg-ingest config check
.venv/bin/tg-ingest auth login
ls -l config/tg_session.session
.venv/bin/tg-ingest auth check
.venv/bin/tg-ingest channels list
.venv/bin/tg-ingest channel select <ID-OR-@USERNAME>
```

Note (step J): enable the systemd unit only after the channel is selected and a
short-lived foreground `serve` run has been observed without errors.

Notes:

- Step 15 requires coordination with ProxyHub administration; if temporarily not
  possible, validate the failure path by running with a wrong proxy port for a
  few seconds and confirm the log messages.
- Deletion events (step 13) are only received while the service is connected;
  deletions during downtime are not recoverable (documented limitation in
  `README.md`).
