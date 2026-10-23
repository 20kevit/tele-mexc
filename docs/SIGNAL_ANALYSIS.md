# Offline signal analysis and human review

This pipeline turns **already-persisted Telegram messages** into structured
candidate signals. It is deliberately independent of the trading engine, the
MEXC client, and the live `serve` loop.

> **Status: validated against the stored real channel corpus (2026-10-23).**
> The stored corpus of channel `1530389001` (ten messages) is fully classified
> by parsing version 2: 1 initial signal (`#205`, LONG, ICPUSDT.P) and 9
> follow-ups (entry activations / take-profit hits), 0 SHORT, 0 ambiguous,
> 0 parse failures. See `tests/fixtures/channel_signal_corpus.jsonl`
> (`source = real_channel_corpus`, `human_reviewed = true`) and the
> dry-run/apply evidence in the reindex section below. The synthetic redacted
> corpus remains as a regression guard for the generic (v1) parser. Only this
> one channel/format is validated; new channels need fresh observation before
> any format claim.

## Safety boundary

- No Telegram connection, no exchange connection, no HTTP request, no MEXC SDK,
  no API key, and no order execution. `analysis` does not import `trading` or
  `mexc`.
- It reads `messages`, `message_edits`, and `processing_queue` state only through
  existing repository/database code. It never rewrites `messages.raw_json`,
  `messages.text`, or `messages.caption`.
- It never starts collection, backfill, live monitoring, or the processing queue.
- The `actionable` flag on a parsed candidate is a machine-only classification
  hint. It is **not** permission to trade. A SHORT direction (or any
  follow-up/result classification) always yields `actionable = False`; the
  index stores SHORT signals as historical review data with
  `execution_status = 'none'`, and nothing in this pipeline ever creates an
  order intent, an order, or sets leverage. The channel's `Deposit %` and
  `MAX LEV Suggested` fields are stored as metadata and never reach order
  sizing, leverage, or any trading surface.

## Schema (migrations 8 and 10)

| Table | Purpose |
| --- | --- |
| `signal_candidates` | Immutable machine result per `(channel_id, message_id, edit_version, parser_version)`. Migration 10 adds nullable structured-format columns (`logical_id`, `followed_logical_id`, `direction`, `exchange`, `symbol_raw`, `timeframe`, `full_target`, `deposit_pct`, `suggested_leverage`, `leverage_pct`, `follow_up_event`, `target_index`, `profit_pct`, `event_price`, `source_date_text`, `source_time_text`, `source_timestamp_iso`). |
| `signal_candidate_reviews` | Append-only human decisions and corrections. |
| `analysis_runs` | One row per replay with scope and summary counts. |
| `analysis_failures` | Per-message failures with attempt count and resolution time. |
| `channel_signals` | Logical signal index: one row per `(channel_id, logical_id)` holding the extracted fields of the initial-signal message. `execution_status` starts and stays `'none'`; nothing in this pipeline changes it. |
| `signal_message_links` | Exactly one association row per source message: `linked`, `unresolved`, `conflict`, or `no_reference`, with method, reason, and conflict counterpart. |

Financial columns are canonical decimal **text**, never binary floats. A range
is stored as `entry_low_price`/`entry_high_price`; `entry_price` stays `NULL`
unless the source gave one value. Missing stop loss, targets, side, or symbol
remain missing. A failed Jalali calendar conversion keeps the original date
text, leaves `source_timestamp_iso` empty, and flags
`source_date_not_converted` — a Jalali date such as `1405/08/01` is never
silently misread as Gregorian (it is `2026-10-23T05:15:00Z` with the channel's
fixed `+03:30` offset).

Candidate statuses: `current`, `superseded` (newer edit/parser version),
`source_deleted`. Reviews never mutate the machine row or the source message.

## Classifications

`new_signal`, `modification`, `cancellation`, `target_update`,
`stop_loss_update`, `result`, `non_signal`, `ambiguous`.

Every uncertain field or relationship adds an explicit ambiguity code and sets
`parse_status = needs_review` instead of guessing. Examples:
`conflicting_entry_prices`, `target_range_requires_review`,
`non_spot_signal_not_actionable`, `multiple_symbols:...`,
`referenced_message_has_no_candidate`, `embedded_instruction_ignored`.

## Parser versions

`ANALYSIS_PARSER_VERSION = 2` dispatches first to the structured channel-format
parser (`analysis/channel_format.py`) and falls back to the unchanged generic
heuristic parser (`parse_message_generic`, v1 semantics) when no characteristic
marker is present. Detection is deliberately strict: a logical-ID header
(`🆔 #205`) plus an `EXCH:SYMBOL` line from a known exchange whitelist. Generic
messages such as `ENTRY: 100` are never claimed by the structured parser.

## Usage

Use a database copy or a selected channel; never point exploratory commands at
production while investigating. Every DB-reading `analyze` subcommand accepts
`--db-path` so a disposable copy can be inspected without editing config:

```bash
tg-ingest analyze replay --db-path /tmp/opencode/ingest-copy.sqlite3 \
  --channel-id <id> --limit 100 --dry-run
```

The copy is migrated to the current schema; the original database is not
touched.

```bash
# Deterministic, idempotent replay of already-stored rows.
tg-ingest analyze replay --channel-id <id> --limit 100

# Parse without writing candidates, runs, or failures.
tg-ingest analyze replay --channel-id <id> --dry-run --limit 100

# Resume a bounded batch, or narrow by time.
tg-ingest analyze replay --channel-id <id> --after-message-id 5000 --limit 100
tg-ingest analyze replay --channel-id <id> --since 2026-01-01T00:00:00Z

# Review queue.
tg-ingest analyze summary
tg-ingest analyze list --classification ambiguous --parse-status needs_review
tg-ingest analyze show <candidate_id>            # channel id redacted; no text
tg-ingest analyze show <candidate_id> --show-text

# Human decision (append-only).
tg-ingest analyze review <candidate_id> --decision confirm --note "checked"
tg-ingest analyze review <candidate_id> --decision correct \
  --set-symbol BTCUSDT --set-entry 100.5 --set-targets 110,120 --set-stop 95 \
  --note "range was a single entry"

# Failure inspection.
tg-ingest analyze failures
tg-ingest analyze failures --all
```

Replay properties:

- **Deterministic** for the same input text and parser version.
- **Idempotent** via `(channel, message, edit_version, parser_version)` and a
  unique idempotency key; a second run reports `duplicates`, not new rows.
- **Restartable/bounded** with `--since`, `--until`, `--limit`, and
  `--after-message-id`. The management validation workflow additionally uses an
  exact `message_id_min`/`message_id_max` replay scope, so only the imported
  sample is analysed.
- **Failure-isolated**: one malformed row is recorded and skipped; the rest of
  the run continues. Retrying a successful row resolves its failure record.
- **Edit-aware**: a new `processing_version` inserts a new candidate and marks
  the older one `superseded`.
- **Deletion-aware**: tombstones mark candidates `source_deleted`; the Telegram
  source row itself is unchanged.

## Logical signal index (reindex)

The per-channel logical-signal index is rebuilt offline with:

```bash
# always start from a verified backup; the real corpus is only what is stored
tg-ingest analyze reindex --channel-id <id> --dry-run --report-path report.json
tg-ingest analyze reindex --channel-id <id> --report-path report.json
```

- `--dry-run` classifies and plans **in memory only**: no candidates, no index
  rows, no links — a machine-readable JSON report is produced either way.
- On apply it first records parser-v2 candidates through the shared, immutable
  `AnalysisStore` machinery, then fills `channel_signals` and
  `signal_message_links` in a single transaction.
- **Association rules** (documented in `analysis/reindex.py` and tested):
  explicit logical ID alone (initial signal or known index entry), reply to a
  known signal-origin message alone, or both present and agreeing → `linked`
  (method `explicit_id` / `reply` / `both`); both present and disagreeing →
  `conflict` and **never auto-attached**; a reference whose original was never
  stored stays `unresolved` with the recorded reason; neither identifier →
  `no_reference`.
- Out-of-order ingestion is tolerated: a stored follow-up resolves in the same
  run even when the initial signal has a higher message ID.
- Duplicate logical signal IDs: the earliest origin message wins
  deterministically, later ones are recorded (`duplicate_signal_ids`,
  `status = duplicate_origin`), never silently dropped or overwritten.
- Idempotent: a second apply reports `signals_unchanged` and creates nothing
  new; raw messages are never rewritten and link rows are only updated when
  the derived data changed.

Observed real-corpus evidence (channel `1530389001`, dry-run + apply run
2026-10-23): 10 messages scanned, 1 initial signal (`#205` LONG ICPUSDT.P,
entry `2.779`, SL `2.719`, TP1–TP4 `2.797/2.811/2.824/2.834`, FULL `2.869`,
deposit `1.00%`, suggested leverage `8 (-17.39%)`, Jalali date `1405/08/01`
`08:45` → `2026-10-23T05:15:00Z`), 9 follow-ups, 0 SHORT, 0 ambiguous,
0 conflicts, 2 linked rows (the signal itself and its `both`-method follow-up),
8 `unresolved` rows for `#201…#204` (originals were never stored → reason
`logical_signal_id_not_in_index` or missing reply target), 0 parse failures,
`foreign_key_check` clean, and a fully idempotent second run.

## Management-bot human review

The management bot can show the real stored text of one bounded imported sample
next to its parsed candidate, in the developer's private chat only. Decisions
are `confirm`, `correct`, or `reject`; corrections use `field=value` lines (for
example `symbol=ETHUSDT` and `targets=110,120`). Every decision is appended to
`signal_candidate_reviews` and an audit event records the role, opaque request
id, opaque candidate id, and decision. Machine fields and Telegram source rows
are never changed by review, and no review decision reaches trading or MEXC.

The bot also offers «🗂 مرور سیگنال‌های اخیر» (`/signals`) for both operators
whenever the logical signal index tables exist and a channel is selected. It is
purely read-only over `channel_signals` / `signal_message_links`: a signal list
(datetime-ordered), a per-signal view with previous/next navigation, the
extracted-fields view, the related-messages view, and the untouched original
message text (chunked to Telegram's 4096 UTF-16 limit). See
`docs/MANAGEMENT_BOT_FA.md`.

## Relationship resolution

For the generic v1 parser, updates are linked to a parent signal by, in order:

1. Telegram reply target.
2. Explicit message reference (`#123`, `MSG 123`, `پیام 123`).
3. Exactly one active `new_signal`/`modification` for the same symbol earlier
   in the same channel.

Zero or multiple candidates produce `relationship = ambiguous` and
`needs_review`. A symbol mismatch with the parent is also flagged.

For structured (v2) messages `#N` is a **logical signal ID**, not a Telegram
reference, so it never feeds the v1 message-reference resolution — the
candidate keeps `referenced_message_id = None` and the logical association is
decided by the reindexer's documented rules (explicit ID / reply / both /
conflict) that fill `signal_message_links`.

## Corpus and tests

```bash
python -m tg_signal_ingest.main analyze corpus-check \
  --path tests/fixtures/redacted_signal_corpus.jsonl
.venv/bin/tg-ingest analyze corpus-check \
  --path tests/fixtures/channel_signal_corpus.jsonl
```

`redacted_signal_corpus.jsonl` is synthetic (`human_reviewed = false`) and
guards the generic parser. `channel_signal_corpus.jsonl` contains the ten real
stored messages verbatim (`source = real_channel_corpus`,
`human_reviewed = true`, cross-checked against the backup's message rows and
datetimes) plus minimum synthetic edge cases (`source =
synthetic_channel_format`) such as a SHORT signal, duplicate/unconvertible
date, and both-direction ambiguity. A zero mismatch count is a regression
check.

Focused tests live in:

- `tests/test_analysis_parser.py`
- `tests/test_analysis_replay.py`
- `tests/test_analysis_store.py`
- `tests/test_analysis_cli.py`
- `tests/test_analysis_corpus.py`
- `tests/test_analysis_boundary.py`
- `tests/test_channel_format_parser.py`
- `tests/test_signal_index.py`
- `tests/test_channel_trading_boundary.py`
- `tests/test_signal_review_bot.py`

The boundary tests assert no network/exchange/trading imports and no writes to
`processing_queue`, `signals`, `order_intents`, or `exchange_orders`; the
channel trading-boundary tests additionally prove SHORT signals, follow-ups,
candidate rows, and the index itself never produce order artefacts.

## Known limitations

- Only the observed TOOBIT futures channel format is validated; other channels
  or unusual phrasing may need new patterns or human correction.
- Image-only signals are not OCR'd; they stay `non_signal`/`needs_review`
  (`media_without_text`).
- Forwarded messages are context only and never become a new actionable signal.
- The parser is rule-based. Unusual phrasing, mixed languages, or unusual
  number formatting may need new patterns or human correction.
- The analysis stage is not wired into `serve`; running it is an explicit manual
  action.
- Human corrections are recorded for audit but are not automatically applied to
  downstream trading logic.
- The observed corpus contains a single initial signal; association of
  subsequent follow-ups relies on the stored originals (missing ones stay
  `unresolved` with recorded reasons).

## Next manual validation step

On a **copy** of the production database after a separately authorized,
bounded history import:

```bash
mkdir -p /tmp/opencode
python - <<'PY'
import sqlite3
source = sqlite3.connect("file:/opt/projects/akha1/data/ingest.sqlite3?mode=ro", uri=True)
target = sqlite3.connect("/tmp/opencode/ingest-analysis-check.sqlite3")
source.backup(target)
source.close()
target.close()
PY

.venv/bin/tg-ingest analyze replay \
  --db-path /tmp/opencode/ingest-analysis-check.sqlite3 \
  --channel-id <id> --limit 50 --dry-run
.venv/bin/tg-ingest analyze replay \
  --db-path /tmp/opencode/ingest-analysis-check.sqlite3 \
  --channel-id <id> --limit 50
.venv/bin/tg-ingest analyze list \
  --db-path /tmp/opencode/ingest-analysis-check.sqlite3 --limit 50
.venv/bin/tg-ingest analyze summary \
  --db-path /tmp/opencode/ingest-analysis-check.sqlite3
```

Then have a human compare the listed candidates against the original messages
and record `confirm`/`correct`/`reject` decisions. Only after that comparison
should the corpus gain `human_reviewed: true` examples.
