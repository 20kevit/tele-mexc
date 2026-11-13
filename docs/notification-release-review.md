# Tele-MEXC Notification Release Review — 2026-11-13

## Verdict

**OPERATIONAL IN PRODUCTION.** The release worktree and follow-up startup-gate
patch passed the complete test and static-quality gates. Migration 25 is applied.
The first worker start exposed a migration race, then recovered and delivered
six real system-notification messages. The startup schema gate was subsequently
deployed; the restarted worker is active with a fresh heartbeat and no new
schema errors. No MEXC orders or balances were changed.

## Isolation and source

- Production checkout: `/opt/projects/akha1`, branch `production-zero-sl-20261102`,
  base `4e904ec45fb8ad064e56d7ff04c9f4de1b357bda`.
- Candidate source: `/tmp/tele-mexc-notification-phase2`.
- Release worktree: `/tmp/tele-mexc-notification-release-20261113`.
- Release worktree was created from the production base; no reset, clean, stash,
  or checkout was run in Production or the source Candidate.
- Before rollout, Production `tg-ingest.service` was active with MainPID
  `196659` and `NRestarts=0`. The controlled restart applied migration 25;
  current main PID is `244919`, still active with `NRestarts=0`.
- Production source files and unit files were backed up before copying. The
  pre-deploy SQLite backup passed `PRAGMA integrity_check` at schema version 24.
- The installed main unit differs from the reviewed unit only by the intended
  soft `Wants=tg-notification-worker.service` pull-in; no drop-ins exist.

## Included scope

- Typed domain-event registry and transactional event persistence in the
  existing trading database; migration 25 marks legacy/domain rows and adds
  immutable domain-event handling.
- Independent read-only source relay and separate delivery ledger, worker CLI,
  status/list/requeue commands, heartbeat, stale-claim fencing, and readiness.
- Bot API transport classification, structured 429 `retry_after`, definite
  not-sent retry, ambiguous no-blind-retry, dead-letter, max 10 automatic send
  attempts, monotonic attempt/action history, and explicit requeue budget guard.
- Five-level severity policy, Owner A/Owner B/Developer routing, per-recipient
  minimum-severity controls in Telegram, Owner B category overrides, and
  critical buy-block/protection alerts.
- Lifecycle event producers for signal, intent, exchange submission, fills,
  trade/order/OCO/TP/SL transitions, reconciliation, service, and control events.
- Worker/main systemd units and the corresponding operations/data-model/trading
  documentation and focused regression tests.

## Trade-execution boundary audit

- `engine.py`: changes are event-envelope creation and removal of direct
  synchronous notification sends. No order decision, price, sizing, validity,
  or exchange submission rule was intentionally changed.
- `trade_manager.py`: changes are domain-event recording and removal/replacement
  of synchronous notification calls. The currently approved Production
  conditional-entry trigger logic was explicitly preserved in the release.
  Candidate's unrelated `_settle_entry_state` fill-refresh change was excluded.
- `trade_store.py` and `trading/store.py`: domain-event writes are tied to the
  existing state-transition transaction; no order placement/cancel/TP/SL/OCO
  algorithm was added or rewritten.
- No new symbol allowlist, dry-run/live split, short entry, sell entry, KCEX
  integration, historical signal replay, or accounting remediation was added.
- The release does not touch MEXC orders or balances.

## Validation evidence

| Gate | Result |
| --- | --- |
| Full pytest suite after startup-gate fix | **870 passed in 138.17s** |
| Focused notification/policy/control/service suite | **101 passed in 14.44s** |
| Ruff | PASS |
| Mypy | PASS — 51 source files |
| Compileall / AST | PASS |
| `git diff --check` | PASS |
| `systemd-analyze verify` for both units | PASS |
| Migration 25 tests | PASS on disposable pytest databases; production DB not migrated |
| Isolated systemd dependency harness | PASS: worker crash/restart and start-limit exhaustion did not stop main; stopping main stopped worker; temporary units removed |

Additional regression coverage includes Owner B preference overrides, per-recipient
threshold settings, local signal rejection as WARNING, BUY_BLOCK_ACTIVE as
CRITICAL, retry-history sequence after 10 attempts, and refusal to requeue when
sibling message parts exhausted the shared event/recipient budget.

## Deliberately excluded / deferred

- Candidate's unrelated fill-refresh change in `_settle_entry_state` was not
  ported; the Production behavior is retained.
- No accounting/ledger remediation or unrelated dirty Production artifacts were
  copied into the release.
- Notification inline callback buttons (expand/source/ack) are not emitted.
  Callback-token storage is single-use and role-bound but remains an optional,
  unwired capability; no interactive-button claim is made.
- No synthetic Telegram test message or MEXC request was sent. Actual Telegram
  delivery was verified by six real system-notification deliveries, each with a recorded
  `telegram_message_id`; no historical event replay occurred.
- First worker start briefly raced migration 25 and logged `no such column:
  aggregate_type`. It recovered after the main migration completed. The worker
  was then stopped deliberately; `wait_for_notification_source_schema` now
  waits up to 180 seconds and checks required columns/tables before opening the
  long-lived connection. A regression test verifies old-schema rejection and
  readiness after migration.

## Post-deployment operational verification — 2026-11-13 15:11 UTC

| Check | Observed result |
| --- | --- |
| `tg-ingest.service` | active, PID 244919, NRestarts 0 |
| `tg-notification-worker.service` | active, PID 245563, NRestarts 0 |
| Worker heartbeat | 0.9 seconds, `health=ok` |
| Delivery ledger | 6 sent, 0 pending, 0 failed, 0 dead-letter, 0 ambiguous |
| Telegram message IDs | 6 stored; every delivery has one successful attempt |
| Historical replay | none; existing source/legacy cursors preserved |
| Worker errors after startup-gate deployment | none observed |

The worker remains a soft dependency (`Wants`) of the trading service. Its
failure does not stop the trading service; it follows the main service lifecycle
through `BindsTo`/`PartOf` and waits for the required DB schema before connecting.

## Rollout checklist

1. Create and verify a consistent SQLite backup of `data/ingest.sqlite3` using
   SQLite's backup API; do not copy only the main DB file while WAL is active.
2. Back up the exact reviewed source/unit files and preserve all existing dirty
   Production/untracked accounting artifacts.
3. Copy only the reviewed release files into `/opt/projects/akha1`.
4. Install `tg-notification-worker.service` and the reviewed `tg-ingest.service`,
   run `systemctl daemon-reload`, then perform one controlled restart of
   `tg-ingest.service` so migration 25 applies and the worker is pulled in.
5. Verify `tg-ingest.service` and `tg-notification-worker.service` are active,
   migration head is 25, delivery DB exists, heartbeat is fresh, and no
   unexpected restart/failure occurred. Do not send a test message without
   separate authorization.
6. If the main service fails to start, restore the backed-up reviewed files and
   original unit, daemon-reload, and restart the main service; do not touch
   exchange orders during rollback.

Release branch `codex/notification-release-20261113` is pushed to GitHub.
Initial release commit: `f36f3e0`; startup-gate follow-up: `e3fea1c`.
Production source files and units match the reviewed release; both services are
active and the notification path is operational.
