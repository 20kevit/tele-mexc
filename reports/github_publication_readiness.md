# GitHub Publication Readiness Report

**Project:** tele-mexc (`tg-signal-ingest`)  
**Repository:** `20kevit/tele-mexc`  
**Remote:** `public` → `git@github-akha1:20kevit/tele-mexc.git`  
**Date:** 2026-10-31  
**Status:** READY — awaiting owner authorization to push

---

## 1. Verified Local Main SHA

```
d7f39020612bc7c0013b20621ec2302e7476d400
```

This is the tip of the local `main` branch in the integration worktree
`/tmp/opencode/tele-mexc-integration` **before** the publication-readiness
report commit. After committing this report, the tip advances by one docs-only
commit (see §16).

## 2. Verified Remote Main SHA

```
71a952b6b35df676fa9e7dc74c97f41baace9ecd
```

Fetched with `git fetch --all --tags --prune`. It is the merge of PR #3
(`arena/035c9f0f-tele-mexc`, commit `5c7dd55` "Complete maintenance audit fixes")
into the previous public main `391eb6a`.

## 3. Exact Ahead/Behind Counts

Against `public/main` (`71a952b`):

```
git rev-list --left-right --count public/main...HEAD
0	43
```

* **Behind:** 0
* **Ahead:** 43 commits
* `git merge-base --is-ancestor public/main HEAD` → **YES** (fast-forward).

Local `main` is a descendant of the remote main through merge commit
`eda846d`, which has `71a952b` as its second parent. The tree of `eda846d` is
identical to the pre-merge local tree, so the merge records ancestry without
reintroducing remote files that the local tree already handles.

## 4. Consolidation Verification

| Check | Result |
| --- | --- |
| Expected consolidation commit `6299e15` exists | YES |
| `reports/git_branch_consolidation_report.md` present | YES |
| Commit is reachable from local `main` | YES |
| Original full history preserved | YES (root `2076ec9`) |
| Public squashed history preserved as merge parent | YES (`71a952b` ancestor) |

The previous consolidation commit was independently inspected. The feature
work it claimed was verified in source, not only in the report. The only
genuinely missing approved feature was RTT-aware/median clock synchronization;
it has now been implemented (see §7).

### Feature verification (Phase 2 checklist)

| Item | Status | Evidence |
| --- | --- | --- |
| Median-of-three server-time sampling | **Implemented** | `tg_signal_ingest/mexc/clock.py` `refresh()` computes `_median_int` over 3 samples |
| Half-RTT compensation | **Present** | `wall_midpoint = t0_wall + rtt/2` |
| RTT filtering | **Present** | `low_rtt = [s for s in samples if s.rtt_ms <= max_rtt_ms]` |
| Offset clamping | **Implemented** | `max_offset_deviation_ms` outlier rejection + `max_offset_ms` absolute clamp |
| Periodic synchronization | **Present (lazy)** | `now_ms()` refreshes when `age > read_max_age_s`; writes call `ensure_fresh_for_write()` |
| Debouncing | **Present** | `min_refresh_interval_s=1.0` returns the cached sample |
| Concurrency protection | **Present** | `threading.Lock` in `refresh()` |
| Timestamp after rate-gate wait | **Present** | `_acquire_bucket()` then `_send_once()` → `_timestamp_for()` |
| No write retry after timestamp errors | **Preserved** | `attempts = 2` only for signed read-only requests; writes `attempts = 1` |
| `unknown_requires_reconciliation` handling | **Preserved** | engine transitions, never resubmits; `-2011` never terminalizes |
| `recvWindow` default 5000 | **Preserved** | `MexcConfig.recv_window_ms = 5000`; `MexcSpotClient` enforces 1–60000 |
| Global buy block atomicity | **Present** | `TradingStore.set_buy_block()` single `BEGIN IMMEDIATE` transaction; `app_state` keys |
| Fail-closed buy block reads | **Present** | `_buy_block_reason()` returns an unreadable-state block on exception |
| Dry-run / read-only reconciliation | **Present** | `trading reconcile` only queries orders/fills; `needs_reconciliation` branch never submits |
| OCO uncertainty protection | **Present** | `2cea664`, `0e76cb0` cherry-picks |
| Tick-size-aware SL trigger offset | **Present** | `a79e6a9` cherry-pick; migration 17 |
| Audit / evidence modules | **Present** | `trading/audit.py`, `trading/evidence.py`, migrations 18–20 |
| Notification outbox | **Present** | `6029115` cherry-pick; migration 20 |
| Developer authorization | **Present** | `d31e26b` cherry-pick |
| Telegram UTF-16/byte limits | **Present** | `5bdf1da` cherry-pick |
| UI audit changes | **Present** | `1d9ce53` cherry-pick |
| Documentation & migrations | **Present** | migrations 1–20 verified from source |

## 5. Branch Comparison Results

| Branch / Ref | Tip | vs local main | Verdict |
| --- | --- | --- | --- |
| `public/main` | `71a952b` | ancestor (merged) | **Integrated** |
| `public/arena/035c9f0f-tele-mexc` (PR #3) | `5c7dd55` | cherry-picked as `d2e8fa6` | **Integrated** |
| `public/arena/8b00f400-tele-mexc` (PR #2) | `2cea664` | commits cherry-picked | **Integrated** |
| `public/arena/bbce7401-tele-mexc` (PR #1) | `5aba737` | cherry-picked as `0ffd4c6` | **Integrated** |
| `public/arena/e452655b-tele-mexc` (PR #4) | `ab507fe` | 3 unique commits, not merged | **Preserved / not merged** |
| `public/fix/sl-stop-loss-trigger-offset` | `d31e26b` | commits + uncommitted work applied | **Integrated** |
| `fix/sl-stop-loss-trigger-offset` (local) | `d31e26b` | same as above | **Integrated** |
| `ui-audit` (local) | `1d9ce53` | commits cherry-picked | **Integrated** |
| `pre-publication-backup` tag | `7243f0e` | ancestor | **Preserved** |

### Local worktree uncommitted work

* `/opt/projects/akha1` (`fix/sl-stop-loss-trigger-offset`): contained the
  RTT-aware clock and global buy-block implementation. **Integrated** into
  main as commits `57c0e79`, `1ee023f`, `d7f3902`, with tests and docs.
* `/opt/projects/akha1-ui-audit` (`ui-audit`): uncommitted files were
  inspected and found to be **older duplicate content** (missing
  `market_monitor`, approval gate, service notifications, `max_message_id`,
  etc. that main already has). **Not integrated**; the worktree was left
  untouched.

## 6. Open PR Findings

| PR | Source branch | Tip | Status | Finding |
| --- | --- | --- | --- | --- |
| #1 | `arena/bbce7401-tele-mexc` | `5aba737` | **Merged-equivalent in main** | Docs-only trading audit design; cherry-picked. |
| #2 | `arena/8b00f400-tele-mexc` | `2cea664` | **Merged-equivalent in main** | OCO uncertainty fixes; cherry-picked. |
| #3 | `arena/035c9f0f-tele-mexc` | `5c7dd55` | **Merged into remote main** | Maintenance audit; cherry-picked. |
| #4 | `arena/e452655b-tele-mexc` | `ab507fe` | **OPEN — reviewed, not merged** | 3 commits: mandatory SL/TP1/TP2, per-symbol concurrency, OCO invariant, net-sellable allocation, Market Sell removal, `needs_reconciliation` lifecycle, fee fail-closed, trade-level atomicity. |

### PR #4 review

PR #4 is 3 commits ahead of `71a952b` and 0 behind. It is a clean
fast-forward candidate for remote main but remains an **open PR under review**
(it includes a "follow-up repair" commit). It overlaps heavily with the
current `trade_manager.py` / `engine.py` / `store.py` safety model and makes
competing design choices (per-symbol concurrency instead of the global
new-BUY block, Market Sell removal instead of the existing disabled-path
documentation).

**Decision:** not merged into local `main` in this mission. Its branch and
history are preserved:
* Remote branch: `public/arena/e452655b-tele-mexc`
* Backup ref: `refs/backup/public-arena-e452655b` → `ab507fe`

The owner should review PR #4 and decide whether to merge it on GitHub or
request a rebase onto the current main. Its unique safety work is not lost.

## 7. Missing or Corrected Implementations

1. **RTT-aware / median-of-three clock synchronization** — was missing.
   Implemented in `tg_signal_ingest/mexc/clock.py` and integrated into
   `MexcSpotClient`.
   * `ServerClock` samples `GET /api/v3/time` three times, filters by RTT,
     computes the median offset, rejects offset outliers, clamps the final
     offset, and anchors to the monotonic clock.
   * Signed read-only requests refresh once and retry after `700003`/`10073`.
   * Signed writes fail closed (`MexcClockUnavailable`) when no trusted sample
     exists and are never retried automatically.
2. **Global new-BUY block** — implemented in `TradingStore` / `TradeManager` /
   `TradingEngine`, persisted in `app_state`, rebuilt on restart, and checked
   immediately before submission.
3. **Diagnostic clock command** — `cmd_diag_clock` now reads a real
   `clock_snapshot()` when MEXC credentials are present, and uses the
   module-level `MAX_RECV_WINDOW_MS` constant.
4. **Migration upgrade coverage** — added a v16 → v20 disposable-database test
   that verifies legacy audit/order/notification rows survive and new tables
   appear.

## 8. Full Test Results

Environment: Python 3.12.3, fresh venv
`/tmp/opencode/tele-mexc-integration/.venv`, `pip install -e '.[dev]'`
(telethon 1.45.0, pytest 9.2.0, pytest-asyncio 1.4.1, ruff 0.17.3, mypy 2.4.1).

```text
$ .venv/bin/pytest -q
900 passed in 93.47s
```

Included suites:

* MEXC official contract tests
* Trading engine, budget, preflight, live gates, lifecycle recovery
* OCO protection and unprotected-position lifecycle
* RTT clock and timestamp retry policy
* Migration and disposable-DB upgrade tests
* Authorization/control/notification tests
* Management bot and UI reliability tests
* Analysis, parser, backfill, repository, service tests

No failures, no skips, no missing optional dependencies in this environment.
The earlier `test_auth_logout` credential failure no longer occurs after
integrating the maintenance audit's offline test guard.

## 9. Ruff and Mypy Results

```text
$ .venv/bin/ruff check .
All checks passed!

$ .venv/bin/mypy tg_signal_ingest
Success: no issues found in 62 source files
```

## 10. Migration Verification

Verified directly from source:

```text
MIGRATIONS count: 20
versions: [1, 2, ..., 20]
types: 1–17 SQL strings, 18–20 Python callables
```

* `tests/test_db_migrations.py`: 6 passed, including a new
  `test_v16_database_upgrades_to_v20_and_preserves_rows` that:
  * applies migrations 1–16 to a disposable DB,
  * inserts legacy `audit_events`, `trades`, `trade_orders`, and
    `trading_notifications` rows,
  * applies 17–20,
  * asserts legacy rows are preserved, `stop_trigger_price` is `NULL`,
    `leg_role` is backfilled, notification `category/event_type` become
    `legacy`, and `exchange_operations`, `oco_groups`, `oco_legs`,
    `notification_preferences` exist,
  * asserts a rerun is a no-op.

## 11. Backup Verification

| Backup | Path | Verification |
| --- | --- | --- |
| Original pre-consolidation bundle | `/tmp/opencode/git-backup-20261031-134001/tele-mexc-all.bundle` | `git bundle verify` → complete history |
| Verification-phase bundle (with new remote refs) | `/tmp/opencode/git-backup-verify-20261031-144039/all-refs.bundle` | complete history |
| Final bundle | `/tmp/opencode/git-backup-final-20261031-145113/all-refs-final.bundle` | complete history |
| Uncommitted worktree diffs | `/tmp/opencode/git-backup-verify-20261031-144039/fix-sl-uncommitted.diff`, `-untracked.tar.gz`, `ui-audit-uncommitted.diff`, `ui-audit-untracked.tar.gz` | archived |

Backup refs in the repository:

```
refs/backup/initial-HEAD                  cbb5e77
refs/backup/initial-main                  cbb5e77
refs/backup/initial-fix/sl-...            d31e26b
refs/backup/initial-public-review         a3a10b5
refs/backup/initial-ui-audit              1d9ce53
refs/backup/pre-publication-main          d492112
refs/backup/remote-main-71a952b           71a952b
refs/backup/public-main                   391eb6a
refs/backup/public-arena-8b00f400         2cea664
refs/backup/public-arena-bbce7401         5aba737
refs/backup/public-arena-035c9f0f         5c7dd55
refs/backup/public-arena-e452655b         ab507fe
refs/backup/public-review                 a3a10b5
```

## 12. Exact Files and Commits Prepared for Publication

### New commits on top of remote main

```
eda846d merge: reconcile local consolidated main with GitHub main (fast-forward base)
d2e8fa6 Complete maintenance audit fixes
d492112 fix: use module-level MEXC recvWindow maximum in clock diagnostics
57c0e79 feat: add RTT-aware MEXC server clock and read-only timestamp retry
1ee023f feat: fail-closed exit-protection gate and durable global new-BUY block
d7f3902 feat: median-of-three clock offset with outlier rejection and clamping
268919e test: verify v16 to v20 migration upgrade preserves legacy evidence
6299e15 docs: update consolidation report with final HEAD and AGENTS.md change
241eaa6 docs: update AGENTS.md migration count to 1–20
fef64ad docs: add Git branch consolidation report
94c6436 style: fix ruff lint errors in diagnostic commands and engine note
8174475 docs: UI audit findings, fixes and residual risks (management bot)
eb16d61 management: enforce Telegram UTF-16/byte limits across every bot surface
0ffd4c6 docs: add trading audit and notification design
d822776 fix: grant control-bot owner-equivalent access to configured developer
fbdc531 docs: add final execution-flow fix report
f2c5461 fix: remove signal-level approval prompts; add /version command; harden entry and audit paths
017ea3f fix: tick-size-aware stop-loss trigger offset on OCO exits
b60582c fix: make mixed OCO uncertainty dominate completion evidence
586f17e fix: keep uncertain OCO protection explicitly unconfirmed
618837e fix: prevent MEXC 700003 after rate-limit waits
... plus the original pre-publication history back to 2076ec9
```

### Key files added/changed by this mission

* `tg_signal_ingest/mexc/clock.py` (new)
* `tg_signal_ingest/mexc/client.py`, `errors.py`, `__init__.py`
* `tg_signal_ingest/trading/store.py`, `trade_manager.py`, `trade_store.py`, `engine.py`
* `tg_signal_ingest/main.py` (clock diagnostics, buy-block status)
* `tests/test_mexc_clock.py` (new)
* `tests/test_trading_unprotected_lifecycle.py` (new)
* `tests/test_db_migrations.py` (upgrade coverage)
* `docs/trading.md`, `docs/operations.md`, `docs/documentation-index.md`
* `docs/MAINTENANCE_AUDIT_2026-10-29.md` (from PR #3)
* `reports/git_branch_consolidation_report.md`
* `reports/github_publication_readiness.md` (this file)
* `.gitignore`, `tests/conftest.py`, `tests/test_offline_guard.py` (from PR #3)

### Secret / sensitive-file check

* No tracked `.env`, `config/config.toml`, `*.session`, `*.sqlite`, or key
  material.
* Pattern scan for API keys, bot tokens, private keys, and bearer tokens over
  tracked non-fixture files returned no hits.
* Exchange forensic reports containing real balances/orders were **not**
  copied into the repository.

## 13. Exact Git Push Command

The remote is named `public` (not `origin`):

```bash
git push public main:main
```

No force flag. This is a normal fast-forward update.

## 14. Is the Update a Normal Fast-Forward?

**YES.**

* `git merge-base --is-ancestor 71a952b HEAD` → true.
* `git rev-list --left-right --count public/main...HEAD` → `0 43`.
* The remote main is a direct ancestor of local main through merge commit
  `eda846d`.

The merge commit was created with `git merge -s ours --allow-unrelated-histories
71a952b` after cherry-picking the only unique remote change (`5c7dd55`). It
records ancestry without rewriting public history and without reintroducing
files the local tree already handles. No force push is required or permitted.

## 15. Remaining Risks

1. **PR #4 is not integrated.** Its mandatory SL/TP, per-symbol concurrency,
   OCO invariant, fee fail-closed, and `needs_reconciliation` lifecycle work
   remain only on `public/arena/e452655b-tele-mexc` / `ab507fe`. The owner must
   review and decide. This is the largest outstanding design decision.
2. **Publishing restores pre-publication files.** Because local main preserves
   the original full history, a push will restore `AGENTS.md`, `deploy/`,
   `docs/CONTROLS_BOT_FA.md`, and many tests that the public squashed history
   had removed. This is intentional per the mission but should be confirmed by
   the owner.
3. **`ruff format --check` drift.** Formatting is not enforced repo-wide; a
   later dedicated `ruff format .` pass would be cosmetic only.
4. **Clock synchronization is lazily periodic.** There is no background clock
   thread; refreshes occur on read-age expiry or immediately before a write.
   This is sufficient for signed timestamps but is not a continuous daemon.
5. **Live behavior remains unverified.** No live MEXC orders, logins, or
   production databases were used. Real OCO behavior, global buy-block
   enforcement against a live exchange, and protection recovery remain
   unverified in production.
6. **Diagnostic clock command** performs a live read only when MEXC
   credentials are configured; without credentials it prints thresholds and a
   "no live credentials" note.
7. **Remote branch deletion / PR closure** was not performed and is out of
   scope.

## 16. Publication Gate

**STOP — no push has been executed.**

After committing this report, the local `main` tip will advance by one
docs-only commit. Re-run:

```bash
git rev-parse main
git rev-list --left-right --count public/main...main
git merge-base --is-ancestor public/main main && echo FF
```

Then, with explicit owner authorization:

```bash
git push public main:main
```
