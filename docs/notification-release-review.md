# Tele-MEXC Notification Release Review — 2026-11-13

## Verdict

**GO for controlled production rollout. Not deployed yet.** The isolated release
worktree passed the complete test and static-quality gates. The production
service, production database, exchange orders, and live Telegram delivery have
not been changed or tested by this release review.

## Isolation and source

- Production checkout: `/opt/projects/akha1`, branch `production-zero-sl-20261102`,
  base `4e904ec45fb8ad064e56d7ff04c9f4de1b357bda`.
- Candidate source: `/tmp/tele-mexc-notification-phase2`.
- Release worktree: `/tmp/tele-mexc-notification-release-20261113`.
- Release worktree was created from the production base; no reset, clean, stash,
  or checkout was run in Production or the source Candidate.
- Production `tg-ingest.service` remained active with MainPID `196659` and
  `NRestarts=0`; no production files, database, or orders were changed.
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
| Full pytest suite | **869 passed in 141.83s** |
| Focused notification/policy/control/trading suite | **127 passed in 22.92s** |
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
- No live Telegram test message or MEXC request was sent. Actual delivery by the
  new worker remains unverified until the post-deployment worker heartbeat and
  first real delivery are observed.

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

**At the time this review was drafted, the release was not yet committed or
deployed. The release branch and rollout status are recorded in the final
release update.**
