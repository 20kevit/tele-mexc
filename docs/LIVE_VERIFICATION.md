# Live Telegram verification procedure (manual, one time)

This is the documented **manual integration test** that requires real Telegram
credentials and cannot be part of the automated suite (which is fully mocked
and offline).

Owner of the Telegram account performs the interactive steps; the operator
records the outcomes.

| # | Step | Expected | Recorded |
| --- | --- | --- | --- |
| 1 | `tg-ingest diagnostics proxy` | `proxy diagnostics: OK` + CONNECT tunnel to a Telegram DC + `direct egress blocked (expected)` | |
| 2 | `TELEGRAM_PROXY_PORT=30999 tg-ingest diagnostics proxy` | clear `PROXY FAILURE`, exit 1 | |
| 3 | `tg-ingest config check` (after filling `.env`) | `configuration OK` | |
| 4 | `tg-ingest auth login` (owner enters phone, code, optional 2FA) | `Login succeeded for ... Session persisted` | |
| 5 | `ls -l config/tg_session.session` | permissions `-rw-------` | |
| 6 | `tg-ingest auth check` | `Session status: valid` | |
| 7 | `tg-ingest channels list` | displays channels incl. target | |
| 8 | `tg-ingest channel select <id>` | `selected channel: ...` | |
| 9 | `tg-ingest backfill run` | progress counts and `history import complete` | |
| 10 | `tg-ingest backfill status` | `backfill: status=done` | |
| 11 | `tg-ingest serve` starts; owner posts a test message | `live message ... stored (media=text-only)` in logs | |
| 12 | owner edits the message | `live edit of ... stored (version 0 -> 1)` | |
| 13 | owner deletes the message (while service online) | `deletion event: 1 message(s) newly marked deleted` | |
| 14 | restart `tg-ingest serve` after 2+ min of downtime | `recovery sweep complete: N gap message(s) fetched` | |
| 15 | stop proxy listener briefly (with owner's ProxyHub admin) — see note | service logs proxy failure and keeps retrying; never falls back to direct connection | |
| 16 | `git status --short` (from repo root) | no `.env`, no `*.session`, no `*.sqlite3` | |

Notes:

- Step 15 requires coordination with ProxyHub administration; if temporarily not
  possible, validate the failure path by running with a wrong proxy port for a
  few seconds and confirm the log messages.
- Deletion events (step 13) are only received while the service is connected;
  deletions during downtime are not recoverable (documented limitation in
  `README.md`).
