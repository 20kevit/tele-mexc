# Project Decision Registry — Tele-MEXC

Durable, version-controlled record of every agreed audit decision and its
implementation status. This file is the human-readable view; the machine-readable
source of truth is [`decisions/registry.json`](decisions/registry.json).

- Baseline tag: `tele-mexc-production-baseline-2026-11-01`
- Baseline commit: `d987c7febb90065cb148f643fea97c054e325fee`
- Registry schema: `1.0`

## Evidence provenance (read this first)

Historical runtime evidence used by these decisions was produced between
2026-10-24 and 2026-10-31 by an **older build** (HEAD then was around
`71a952b` / `2cea664`), not by the baseline commit `d987c7f`. It describes what
that build did. It is context, never proof about current HEAD. Executable code
and tests are authoritative; see `AGENTS.md` for the source-of-truth order.

## Status vocabulary

`pending` · `verified-existing` · `fixed` · `tested` · `deferred` ·
`intentionally-not-fixed` · `blocked`

`deferred` means "decision recorded, implementation intentionally postponed by
the owner's decision"; it is not a defect left behind by accident.

## Registry

| ID | Title | Evidence | Status | Commit |
| --- | --- | --- | --- | --- |
| N-01 | Signal validity window and delayed/ambiguous entry submission | runtime-confirmed | fixed | 15856a7 |
| N-02 | Centralized MEXC API rate limiting | runtime-confirmed | fixed | 15856a7 |
| N-03 | MEXC WebSocket market data | runtime-confirmed | deferred | — |
| N-04 | Stale price handling in the tick queue | runtime-confirmed | fixed | 15856a7 |
| N-05 | Missed Telegram messages during downtime | runtime-confirmed | verified-existing | 15856a7 |
| N-06 | Same-symbol duplicate BUY protection | runtime-confirmed | verified-existing | 15856a7 |
| N-07 | BUY Block enforcement | runtime-confirmed | fixed | 15856a7 |
| N-08 | SQLite concurrency correctness | risk-only | fixed | 15856a7 |
| N-09 | Minimum order notional | risk-only | deferred | — |
| N-10 | Existing protective controls | code-confirmed | fixed | 15856a7 |
| F-01 | Standalone TP order path | code-confirmed | fixed | 15856a7 |
| F-02 | OCO retry policy and per-attempt reporting | runtime-confirmed | fixed | 15856a7 |
| F-04 | Excessive per-tick OCO leg polling | runtime-confirmed | fixed | 15856a7 |
| F-05 | Partial entry fill protection | not-observed | fixed | 15856a7 |
| F-06 | Manual entry cancellation | runtime-confirmed | verified-existing | 15856a7 |
| F-07 | Manual sales and unknown exchange orders | runtime-confirmed | fixed | 15856a7 |
| F-09 | 100-trade monitoring limit | not-observed | deferred | — |
| F-10 | Telegram notification reliability | runtime-confirmed | fixed | 15856a7 |
| F-12 | OCO local database consistency | runtime-confirmed | fixed | 15856a7 |
| F-13 | Incorrect MEXC error classification | runtime-confirmed | fixed | 15856a7 |

## Non-negotiable product policies (do not reopen)

Automatic execution of all configured-channel signals · MEXC Spot only · LONG
only (SHORT ignored) · LIMIT entry · OCO-only exit · no per-signal or per-order
approval · no sim/live split · no symbol allowlist · fixed owner-configurable
quote per order · channel percentages and leverage ignored · no automatic price
changes · no automatic Market Sell fallback · old messages never execute after a
restart · configurable signal validity (default 12h) · two notification
recipients · exactly one designated owner with administrative control.

## How to update this registry

1. Edit `docs/decisions/registry.json` (the machine-readable source).
2. Mirror the row status and commit in the table above.
3. Every status change to `fixed`/`tested` must name the commit SHA and the tests
   that prove it.
4. Never delete a decision or rewrite its history; add a `limitations` note
   instead.
5. `tests/test_decision_registry.py` enforces schema, ID coverage, allowed
   statuses and that every `fixed`/`tested` item has a commit and tests.

## Verification (remediation commit `15856a7`)

| Check | Result |
| --- | --- |
| `pytest` (full suite) | 748 passed |
| `ruff check .` | clean |
| `mypy tg_signal_ingest` | clean (48 files) |
| Migration 22 from v21 on a disposable copy | applied, data preserved, idempotent, `integrity_check=ok` |
| Production database | untouched; no migration executed against `data/ingest.sqlite3` |
| Production service | untouched; no restart, no deploy |
| Exchange | no order created, cancelled or amended |
| Telegram | no message sent from tests (fake Bot API only) |
| Secrets | none printed, committed or logged |

Deferred items are deliberate owner decisions, not unfinished work: WebSocket
(N-03), minimum-notional caution (N-09) and the 100-trade listing limit (F-09).
