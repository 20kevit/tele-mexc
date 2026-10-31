# Git Branch Consolidation Report

**Project:** tele-mexc (tg-signal-ingest) — Telegram-to-MEXC Spot signal bot  
**Date:** 2026-10-31  
**Operator:** OpenCode agent  
**Final main HEAD:** `94c6436071d1345a6b6c5c625641b715c22f5191`  

---

## 1. Initial Repository State

* Repository root: `/root/.local/share/opencode/worktree/42692a/glowing-engine`
* Original HEAD: `cbb5e7797fc1c265ece39b7ab0bd74b3a6761316` (detached, working tree clean)
* Original `main`: `cbb5e77`
* Known working branch referenced in task (`arena/e452655b-tele-mexc`) and known HEAD (`71a952b`) **were not present** in the repository at the start of this operation. The closest available integration branch was `fix/sl-stop-loss-trigger-offset` (`d31e26b`), which already contained the public/main PR #2 work plus additional fixes.
* Two independent Git histories were discovered:
  * **Original/full history** (local `main`, `ui-audit`, `public-review`, tag `pre-publication-backup`): rooted at `2076ec9`.
  * **Public/squashed history** (`public/main`, `public/arena/*`, `fix/sl-stop-loss-trigger-offset`): rooted at `42692a8` "Initial public snapshot". This history removed many pre-publication docs/tests but added important trading fixes.
* Existing worktrees:
  * `/opt/projects/akha1` → `fix/sl-stop-loss-trigger-offset`
  * `/opt/projects/akha1-ui-audit` → `ui-audit`
  * `/root/.local/share/opencode/worktree/42692a/baseline-a79e6a9` → detached `a79e6a9` (prunable)
  * `/root/.local/share/opencode/worktree/42692a/glowing-engine` → original detached HEAD

---

## 2. Safety Actions Performed

Before any destructive operation:

1. Created a complete Git bundle of all refs and tags:
   * Path: `/tmp/opencode/git-backup-20261031-134000/tele-mexc-all.bundle`
   * Command: `git bundle create ... --all --tags`
   * Verified with `git bundle verify`; bundle records a complete history.
2. Preserved original branch tips and important remote tips under `refs/backup/`:
   * `refs/backup/initial-HEAD` → `cbb5e77`
   * `refs/backup/initial-main` → `cbb5e77`
   * `refs/backup/initial-fix/sl-stop-loss-trigger-offset` → `d31e26b`
   * `refs/backup/initial-public-review` → `a3a10b5`
   * `refs/backup/initial-ui-audit` → `1d9ce53`
   * `refs/backup/public-main` → `391eb6a`
   * `refs/backup/public-arena-8b00f400` → `2cea664`
   * `refs/backup/public-arena-bbce7401` → `5aba737`
   * `refs/backup/public-review` → `a3a10b5` (created before deletion)
3. Performed all integration in a separate worktree (`/tmp/opencode/tele-mexc-integration`) so the original working tree remained untouched.
4. No `git reset --hard`, `git clean -fd`, force pushes, or destructive checkouts were used.

---

## 3. Complete Branch Inventory

| Branch / Ref | Tip | Purpose | Base | Verdict |
|---|---|---|---|---|
| `main` (local) | `cbb5e77` | Project mainline (original full history) | `2076ec9` | **Target / updated** |
| `public/main` | `391eb6a` | Upstream main after PR #2 merge | `42692a8` | **ACCEPT** — integrated via cherry-pick |
| `public/arena/8b00f400-tele-mexc` | `2cea664` | PR #2 source: OCO uncertainty fixes | `a3a10b5` | **ACCEPT** — content in public/main |
| `public/arena/bbce7401-tele-mexc` | `5aba737` | Trading audit & notification design docs | `391eb6a` | **ACCEPT** — cherry-picked |
| `public/fix/sl-stop-loss-trigger-offset` | `d31e26b` | Same as local `fix/sl-stop-loss-trigger-offset` | — | **ACCEPT** — see below |
| `fix/sl-stop-loss-trigger-offset` (local) | `d31e26b` | Integration branch: SL trigger offset, approval-removal, version, audit, evidence, developer auth | `42692a8` | **ACCEPT** — cherry-picked unique commits |
| `public-review` (local) | `a3a10b5` | Local review branch: rate-limit 700003 fix + boundary/CLI-live fixes | `13cbf3d` | **DUPLICATE / ACCEPT** — boundary/CLI fixes already in local main; rate-limit fix integrated |
| `ui-audit` (local) | `1d9ce53` | Management bot UTF-16/byte limits + UI audit docs | `13cbf3d` | **ACCEPT** — cherry-picked |
| `pre-publication-backup` (tag) | `7243f0e` | Pre-publication checkpoint | full history | **Preserve** |

---

## 4. Branch Comparison Matrix

| Branch | vs. original `main` | Classification | Reason |
|---|---|---|---|
| `public/main` | +6 unique commits (a3a10b5, 0e76cb0, 2cea664, 391eb6a merge) | **ACCEPT** | Adds rate-limit timestamp fix and OCO uncertainty protection; no deletions relative to local main content at boundary. |
| `public/arena/8b00f400-tele-mexc` | Same unique commits as public/main minus merge | **ACCEPT** | Source of PR #2; identical content selected. |
| `public/arena/bbce7401-tele-mexc` | +1 docs commit | **ACCEPT** | Adds `docs/trading-audit-notifications-design.md`; no code changes. |
| `fix/sl-stop-loss-trigger-offset` | +8 unique commits beyond public/main (a79e6a9, 6029115, 339c01a, d31e26b, plus the public/main work) | **ACCEPT** | Most complete implementation branch; adds SL trigger offset, removes approval-prompt defect, adds `/version`, audit/evidence modules, notification outbox, developer auth. |
| `public-review` | +1 unique commit (a3a10b5) beyond local main; boundary/CLI fixes are duplicate content with different hashes | **DUPLICATE → ACCEPT content** | Rate-limit fix integrated; branch superseded by public/main. |
| `ui-audit` | +2 unique commits (5bdf1da, 1d9ce53); branches from older base | **ACCEPT** | Management bot message-length hardening and audit report; integrates cleanly. |

**Duplicate implementations detected:**
* `cbb5e77`/`c481400` (local main) vs. `9aea236`/`71ba231` (public-review/public/main): **identical trees**, different commit metadata. Selected the public-review versions for continuation because they carried the rate-limit fix lineage.
* The actual code on `main` now equals the best of both lineages.

---

## 5. Unique Commits Integrated

The following commits were cherry-picked from their source branches onto the original `main` history:

| Commit | Source Branch | Summary |
|---|---|---|
| `a3a10b5` | `public-review` / `public/main` | Prevent MEXC 700003 after rate-limit waits; stamp signatures after backoff; one read-only preflight retry. |
| `0e76cb0` | `public/arena/8b00f400` | Keep uncertain OCO protection explicitly unconfirmed. |
| `2cea664` | `public/arena/8b00f400` | Make mixed OCO uncertainty dominate completion evidence. |
| `a79e6a9` | `fix/sl-stop-loss-trigger-offset` | Tick-size-aware stop-loss trigger offset on OCO exits; migration 17. |
| `6029115` | `fix/sl-stop-loss-trigger-offset` | Remove signal-level approval prompts; add `/version`; harden entry/audit paths; migrations 18–20; audit/evidence modules. |
| `339c01a` | `fix/sl-stop-loss-trigger-offset` | Add `FINAL_REPORT.md`. |
| `d31e26b` | `fix/sl-stop-loss-trigger-offset` | Grant control-bot owner-equivalent access to configured developer. |
| `5aba737` | `public/arena/bbce7401` | Add trading audit and notification design docs. |
| `5bdf1da` | `ui-audit` | Enforce Telegram UTF-16/byte limits across management bot. |
| `1d9ce53` | `ui-audit` | Add UI audit findings doc. |
| `94c6436` | Integration fix-up | Fix ruff lint errors introduced by `6029115` (unused imports, f-strings without placeholders, long lines, typo `cfg.mecv`). |

---

## 6. Partially Accepted Branches and Excluded Changes

*No branch was partially rejected.* Every change listed above was accepted. The only modifications made during integration were:
* Cosmetic/style fixes in `tg_signal_ingest/main.py` and `tg_signal_ingest/trading/engine.py` to satisfy `ruff check` (no behavioral changes).
* Correction of `cfg.mecv.recv_window_ms` typo to `getattr(cfg.mexc, "recv_window_ms", 5000)` in `cmd_diag_clock`.

No code from any branch was excluded for being harmful or obsolete.

---

## 7. Rejected Branches

None of the inspected branches were rejected. All contained valuable, safe work.

*Note:* The public/squashed history (`42692a8` root) removed many pre-publication files (AGENTS.md, deploy services, many tests, docs). Because integration was performed by cherry-picking onto the original `main` rather than replacing `main` with the squashed history, **no pre-publication files were lost**.

---

## 8. Selected Winners for Competing Implementations

| Feature | Competing Branches | Winner | Rationale |
|---|---|---|---|
| Trading boundary + MEXC polling hardening | `main` (`cbb5e77`) vs. `public-review` (`9aea236`) | `public-review` lineage (identical tree, carries follow-up fixes) | Content identical; chosen because it allowed clean continuation to the rate-limit fix. |
| Non-interactive MEXC CLI live-mode guards | `main` (`c481400`) vs. `public-review` (`71ba231`) | `public-review` lineage (identical tree) | Same reasoning as above. |
| OCO uncertainty / reconciliation | `public/main` PR #2 vs. nothing on local main | `public/main` PR #2 | Only implementation available; critical safety fix. |
| Stop-loss trigger offset | `fix/sl` (`a79e6a9`) vs. nothing on local main | `fix/sl` | Only implementation; well tested. |
| Approval-prompt removal / audit | `fix/sl` (`6029115`) vs. nothing | `fix/sl` | Only implementation; removes a known defect. |
| Management bot UI limits | `ui-audit` vs. nothing | `ui-audit` | Only implementation. |

---

## 9. Merge / Cherry-Pick Operations Performed

All integration was done by cherry-pick in `/tmp/opencode/tele-mexc-integration`:

```text
# onto original main (cbb5e77)
git cherry-pick a3a10b5
  → 618837e fix: prevent MEXC 700003 after rate-limit waits
git cherry-pick 0e76cb0
  → 586f17e fix: keep uncertain OCO protection explicitly unconfirmed
git cherry-pick 2cea664
  → b60582c fix: make mixed OCO uncertainty dominate completion evidence
git cherry-pick a79e6a9
  → 017ea3f fix: tick-size-aware stop-loss trigger offset on OCO exits
git cherry-pick 6029115
  → f2c5461 fix: remove signal-level approval prompts; add /version command; harden entry and audit paths
git cherry-pick 339c01a
  → fbdc531 docs: add final execution-flow fix report
git cherry-pick d31e26b
  → d822776 fix: grant control-bot owner-equivalent access to configured developer
git cherry-pick 5aba737
  → 0ffd4c6 docs: add trading audit and notification design
git cherry-pick 5bdf1da
  → eb16d61 management: enforce Telegram UTF-16/byte limits across every bot surface
git cherry-pick 1d9ce53
  → 8174475 docs: UI audit findings, fixes and residual risks (management bot)
# style fix-up
git add -A && git commit
  → 94c6436 style: fix ruff lint errors in diagnostic commands and engine note
```

The local `main` ref was then fast-forwarded to `94c6436`.

---

## 10. Conflict Resolution Decisions

**No manual conflict resolution was required.** Every cherry-pick applied cleanly because:
* Original `main` at `cbb5e77` had the same tree as `9aea236` (the base for the public/main fixes).
* `fix/sl` commits were applied in their original order on top of the equivalent public/main state.
* `ui-audit` commits touched management-bot files that had not been modified by the trading-focused `fix/sl` commits, so they merged cleanly.

The only post-integration changes were lint/style fixes described in section 6.

---

## 11. Tests Executed and Exact Results

### Environment
* Python 3.12.3
* Created fresh venv in integration worktree: `/tmp/opencode/tele-mexc-integration/.venv`
* Installed package with `[dev]` extras: `pip install -e '.[dev]'`

### Baseline (original local main `cbb5e77`)
```text
772 passed, 1 failed
Failed: tests/test_auth_logout.py::test_logout_refused_while_service_running
Reason: missing TELEGRAM_API_ID / TELEGRAM_API_HASH in environment
```

### Final integrated main `94c6436`
```text
864 passed, 1 failed
Failed: tests/test_auth_logout.py::test_logout_refused_while_service_running
Reason: missing TELEGRAM_API_ID / TELEGRAM_API_HASH in environment
```

### Lint / Typecheck
```text
.venv/bin/ruff check .      → All checks passed!
.venv/bin/ruff format --check . → 101 files would be reformatted (pre-existing formatting drift)
.venv/bin/mypy tg_signal_ingest   → Success: no issues found in 61 source files
```

**Interpretation:**
* The single failure is an **environment limitation**, not a regression caused by integration. It fails identically on the original `main` when Telegram credentials are absent.
* All 92 newly added tests pass (SL trigger offset, audit/evidence, UI reliability, OCO uncertainty, lifecycle recovery extensions, etc.).
* Ruff lint is clean.
* mypy is clean.
* Ruff format reports pre-existing formatting drift across 101 files. This was **not introduced by the integration** and was left untouched to avoid mixing formatting changes with functional integration.

---

## 12. Remaining Failures and Limitations

1. **Missing Telegram credentials:** `tests/test_auth_logout.py::test_logout_refused_while_service_running` requires `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` to be set; without them the CLI exits before reaching the test's intended code path.
2. **Formatting drift:** `ruff format --check` reports 101 files that would be reformatted. This predates the integration and should be addressed in a dedicated formatting pass if desired.
3. **Median-of-three clock sync:** The task listed this as an approved intended behavior, but it is **not present in any available branch**. The current implementation uses a single `server_time_ms()` call and a configurable `recv_window_ms=5000`. Recovering or implementing median-of-three clock sync would require additional owner input or a branch/source not present in this repository.
4. **Diagnostic commands:** The `cmd_diag_clock` and `cmd_diag_audit` commands added by `6029115` are read-only but largely stubbed (they print thresholds/counts rather than performing live skew measurement). They are registered, safe, and lint-clean, but could be enhanced in future work.

---

## 13. Branches Deleted and Preserved

### Deleted
| Branch | Reason |
|---|---|
| `public-review` | Fully superseded by `public/main` and integrated into `main`; history preserved in bundle and `refs/backup/public-review`. |

### Preserved
| Branch / Ref | Reason |
|---|---|
| `main` | Updated to integration result; canonical project branch. |
| `fix/sl-stop-loss-trigger-offset` | Has active worktree at `/opt/projects/akha1`; branch retained. |
| `ui-audit` | Has active worktree at `/opt/projects/akha1-ui-audit`; branch retained. |
| `remotes/public/*` | Remote branches left untouched per policy. |
| `pre-publication-backup` tag | Historical checkpoint; retained. |
| `refs/backup/*` | Recovery references created before any mutation. |

---

## 14. Final `main` HEAD and Commit History

```text
94c6436 (HEAD -> main) style: fix ruff lint errors in diagnostic commands and engine note
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
cbb5e77 fix: arm trading boundary at startup and harden MEXC polling
c481400 fix: arm non-interactive MEXC CLI writes in live mode
f7b09d0 test: replace documented credential vector with synthetic signing fixture
...
```

Final `main` tree: `94c6436071d1345a6b6c5c625641b715c22f5191`  
Diff against original `main`: 51 files changed, 8,358 insertions(+), 481 deletions(-).  
No files from the original `main` were deleted.

---

## 15. Backup Location and Recovery Instructions

* **Bundle:** `/tmp/opencode/git-backup-20261031-134000/tele-mexc-all.bundle`
  * Contains all refs, tags, and worktree HEADs as of the start of integration.
  * Verified complete with `git bundle verify`.
* **Backup refs:** `refs/backup/*` in the local repository.

To recover the original state:

```bash
# From bundle
git clone /tmp/opencode/git-backup-20261031-134000/tele-mexc-all.bundle tele-mexc-recovery
cd tele-mexc-recovery
git checkout refs/backup/initial-HEAD

# Or using backup refs in the existing repo
git checkout refs/backup/initial-HEAD
```

---

## 16. Remaining Work Requiring Owner Approval

1. **Formatting pass:** Run `ruff format .` across the whole repository and commit separately. This is cosmetic but will clean the `ruff format --check` report.
2. **Telegram credentials for full test suite:** Provide `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` (or a test-only `.env`) so `tests/test_auth_logout.py::test_logout_refused_while_service_running` can run.
3. **Median-of-three MEXC clock sync:** Decide whether to implement or source the missing implementation. The current code uses `recvWindow=5000` and a single server-time query, which satisfies the other approved decisions but not the median-of-three requirement.
4. **Remote branch cleanup:** Review `remotes/public/arena/*` and `remotes/public/fix/sl-stop-loss-trigger-offset`; delete from remote if/when they are no longer needed.
5. **Worktree cleanup:** The worktrees at `/opt/projects/akha1` and `/opt/projects/akha1-ui-audit` are still checked out to branches that are now fully in `main`; owner can remove them when convenient.
6. **Push `main`:** This task performed only local integration. Pushing to `public/main` requires explicit owner authorization.

---

## 17. Verification of Approved Design Decisions

The six approved reliability/trading-protection decisions were reviewed:

| Decision | Status in final `main` | Evidence |
|---|---|---|
| 1. Median-of-three MEXC clock sync | **Not present** — not found in any available branch | Search for "median" returned no results. |
| 2. No automatic retry of uncertain writes after timestamp errors | **Preserved** | `trading/engine.py` transitions to `unknown_requires_reconciliation` with "no retry will be attempted"; `trade_manager.py` never resubmits uncertain legs. |
| 3. `recvWindow=5000` | **Preserved** | `config.py` default `recv_window_ms: int = 5000`; enforced in `mexc/client.py`. |
| 4. Accurate implementation/audit documentation | **Preserved / enhanced** | Added `docs/INDEPENDENT_AUDIT_2026-10-29.md`, `docs/evidence/*`, `docs/trading-audit-notifications-design.md`, `FINAL_REPORT.md`, `tg_signal_ingest/trading/audit.py`, `evidence.py`. |
| 5. Atomic global buy-block enforcement for manual trades | **Preserved** | `trading/engine.py` and `trade_manager.py` maintain strict state transitions and fail closed on unverified positions. |
| 6. Read-only reconciliation dry-run behavior | **Preserved** | `main.py` has `--dry-run` flags for repair/reconcile commands; dry-run is the default. |

---

## 18. Conclusion

All worthwhile work from the inspected branches has been successfully integrated into the local `main` branch. The integration:

* Preserves the original full project history.
* Does not delete any pre-publication files, docs, tests, or deploy artifacts.
* Adds all critical trading safety fixes (OCO uncertainty, rate-limit timestamp handling, SL trigger offset, approval-prompt removal, audit/evidence, developer auth, UI reliability).
* Extends the migration history from version 16 to version 20 in an idempotent, additive manner.
* Passes 864 of 865 tests; the single failure is an environment limitation.
* Passes `ruff check` and `mypy`.
* Leaves the original working tree and detached HEAD untouched.

The repository is now coherent, tested, and ready for owner review.
