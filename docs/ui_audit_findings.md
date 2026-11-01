> **Status note:** This audit describes the removed two-operator `manage` bot.
> The bot and its bounded-import/review UI were removed; the findings remain as
> historical evidence only.

# UI/UX Audit — Management Bot Interface (findings & fixes)

Branch: `ui-audit` (worktree `/opt/projects/akha1-ui-audit`), commit `df64f72` on top of `1b6e8ac`.
Scope: every user-facing surface of the Telegram management bot
(`tg_signal_ingest/management/`), with focus on the `/signals` review feature;
the control bot (`tg_signal_ingest/control/`) was audited read-only.

## A. Context and method

* All limits were verified against the official Bot API contract:
  * `sendMessage` text: **1–4096 characters after entity parsing** — Telegram
    measures UTF-16 code units, so astral emoji count twice.
  * `InlineKeyboardButton`: label ≤ 64 UTF-16 units, `callback_data` ≤ 64
    **bytes**.
  * `CallbackQuery.message` may be absent when the containing message was
    deleted or is inaccessible.
* Defects were first reproduced numerically (aggregates over 16 related
  messages reached ≈ 11 630 UTF-16 units; huge `targets_json` reached ≈ 10 552).
* Baseline suite on the mirrored state: **557 passed**; after the fixes and the
  new regression tests: **590 passed** (33 new tests in
  `tests/test_ui_reliability.py`). `ruff` and `mypy` clean on changed files.
* No live Telegram, DB, exchange or running service was touched. All tests use
  mocked transports and offline SQLite fixtures.

## B. Inventory of surfaces

| Surface | File | Status |
| --- | --- | --- |
| Commands (`/start`, `/status`, login, logout, ingest, preview) | `management/bot.py` | audited, fixed various |
| Signal review menu/pagination (`/signals`) | `management/bot.py` | fixed (stale offset, limits) |
| Signal detail / extract / related / original | `management/bot.py`, `signal_review.py` | fixed (UTF-16 bounds, segmentation) |
| Keyboard builder | `management/bot.py::_btn` | fixed (UTF-16 label, 64-byte data guard) |
| Outbound send | `management/bot.py::_send` | fixed (size guard, failure visibility) |
| Raw review (`/rawreview`, developer-only) | `raw_review.py` | verified already UTF-16-correct |
| Callback token store (one-shot, TTL 900 s) | `store.py` | verified, no change needed |
| Control bot | `control/service.py`, `control/bot_api.py` | audited read-only, unfixed (see F) |

## C. Findings and fixes (management bot)

| ID | Sev | Finding | Fix |
| --- | --- | --- | --- |
| A | P1 | `_signal_related` sent up to 16 messages in one aggregate; ≈ 11 630 units > 4096 → Telegram rejects → **silent loss of entire review screen** | `render_related_messages` now returns bounded segments (intro on 1st, footer on last, `(بخش k/m)` markers, per-item shortening markers); delivery reports failed segments explicitly |
| B | P2 | `render_signal_extract` could exceed 4096 units with hostile `targets_json`/`ambiguities_json`/`symbol_raw` → silent drop | every free-form field bounded with explicit `…` markers; two-step overflow shed; final per-line guard |
| C | P2 | Preview measured in Python chars; emoji-heavy pages reached ≈ 5 738 units → silent loss | budgets now UTF-16 (`PREVIEW_ITEM_UTF16=550`, `PREVIEW_TOTAL_UTF16=3800`) |
| D | P3 | `_btn` cut labels by Python chars (astral labels could still exceed 64 units); oversized `callback_data` passed through | label bounded in UTF-16 cutting only on code points; `ValueError` if `callback_data` > 64 bytes |
| E | P2 | Stale/out-of-range pagination offset rendered a false empty channel | snaps back to the last non-empty page |
| F | P3 | Empty page states (empty channels list, empty signal index) were dead ends | refresh button added |
| G | P3 | Operator tapping a button whose containing message was deleted got the misleading «دسترسی مجاز نیست» | Telegram omits `message` exactly in that case; known operator now gets «این دکمه دیگر معتبر نیست …؛ /start»; group/stranger taps still refused |
| H | P3 | `_send` swallowed transport failures without size context; oversized sends were silently dropped | returns success bool; >4096-unit sends logged as `error`; failure warnings include operator + utf16 size |
| I | — | Markup-injection surface | verified safe: plain text only, no `parse_mode` is ever set |
| J | — | Callback one-shot/TTL semantics, `_answer` before dispatch | verified correct (token = `mg:` + 32-char urlsafe, 35 bytes ≤ 64) |

## D. Verified properties (no change needed)

* `raw_review.py` slicing is UTF-16-correct (`_slice_utf16`, `MAX_BATCH_UTF16=3600`);
  covered by `TestRawReviewBounds`.
* Persian calendar/ISO timestamps preserved verbatim; renderers never modify
  stored rows (shortening is display-only and marked).
* Callback query answering happens before dispatch; replays/expiry answer
  «منقضی …» without side effects.

## E. Regression coverage added

`tests/test_ui_reliability.py` (33 tests, all offline/mocked):

* `shorten_utf16` unit behavior (bounds, astral integrity, untouched originals).
* Extract renderer bounds with hostile values, and readable for sane values.
* Related-messages segmentation: 16 long messages always ≤ 4096 units, order
  preserved, footer only on last segment, single-huge-body case, empty case.
* Callback lifecycle: stale-tap on deleted message ≠ access-denied; stranger
  and group taps denied; replay/expiry refusal; malformed payload answering.
* Pagination boundaries: 3 pages forward/back/last, stale offset snap, single
  record, empty state.
* Keyboard limits: label ≤ 64 units, `callback_data` ≤ 64 bytes across real
  flows, oversized `callback_data` rejected.
* Original-message integrity: astral-heavy body chunked with **byte-for-byte
  reconstruction** of the stored text; short case delivered with keyboard;
  missing original reports absence.
* Preview stays deliverable with astral content; raw-review batch bounds.

## F. Control bot — findings (documented, NOT fixed here)

`control/` is untracked work owned by a parallel session; committing patches
would adopt their code. Recommended patches (each ~10 lines):

1. **P2** `control/service.py::_show_next_candidate` embeds unbounded
   `candidate.message_text` into the review screen → can exceed 4096 units →
   Telegram rejects and the operator sees nothing. Fix: bound the body with
   the same `shorten_utf16` helpers (now available in `signal_review`).
2. **P3** `control/bot_api.py::BotApiClient.send_message` silently truncates
   with `text[:4096]` in **Python chars**; astral-heavy text can still be
   rejected. Fix: measure/compact in UTF-16 and log when truncating.
3. **P3** `controlbot.candidate.message` handler lacks a top-level
   try/except; a single malformed update can abort the dispatch loop. Fix:
   wrap per-update handling like the management bot does.

## G. Deployment notes

* Only `tg_signal_ingest/management/{bot,signal_review}.py` need rollout (plus
  the two test files). No DB migration, no config change, no restart was
  performed by this audit.
* Behavior is strictly additive-safe: existing flows render identical output
  for ordinary-sized data (only very long values now visibly shortened).
* Control-bot patches should land separately by the owning session.

## H. Residual risks

* RTL/bidi rendering: mixed Latin symbols inside Persian lines may reorder
  visually on some clients (cosmetic, P3, unfixed).
* `shorten_utf16` options are char-count based; no word-boundary policy
  (acceptable for status screens).
* Segment count for pathological inputs scales with data volume (rate-limit
  budgeting is left to the transport layer as before).
* The worktree mirrors uncommitted parallel-session state; `ui-audit` commits
  contain only the four audit files (verified via `git show --stat`).
