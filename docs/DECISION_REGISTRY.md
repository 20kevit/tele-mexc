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
| N-01 | Signal validity window and delayed/ambiguous entry submission | runtime-confirmed | pending | — |
| N-02 | Centralized MEXC API rate limiting | runtime-confirmed | pending | — |
| N-03 | MEXC WebSocket market data | runtime-confirmed | deferred | — |
| N-04 | Stale price handling in the tick queue | runtime-confirmed | pending | — |
| N-05 | Missed Telegram messages during downtime | runtime-confirmed | verified-existing | — |
| N-06 | Same-symbol duplicate BUY protection | runtime-confirmed | verified-existing | — |
| N-07 | BUY Block enforcement | runtime-confirmed | pending | — |
| N-08 | SQLite concurrency correctness | risk-only | pending | — |
| N-09 | Minimum order notional | risk-only | deferred | — |
| N-10 | Existing protective controls | code-confirmed | pending | — |
| F-01 | Standalone TP order path | code-confirmed | pending | — |
| F-02 | OCO retry policy and per-attempt reporting | runtime-confirmed | pending | — |
| F-04 | Excessive per-tick OCO leg polling | runtime-confirmed | pending | — |
| F-05 | Partial entry fill protection | not-observed | pending | — |
| F-06 | Manual entry cancellation | runtime-confirmed | pending | — |
| F-07 | Manual sales and unknown exchange orders | runtime-confirmed | pending | — |
| F-09 | 100-trade monitoring limit | not-observed | deferred | — |
| F-10 | Telegram notification reliability | runtime-confirmed | pending | — |
| F-12 | OCO local database consistency | runtime-confirmed | pending | — |
| F-13 | Incorrect MEXC error classification | runtime-confirmed | pending | — |

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
