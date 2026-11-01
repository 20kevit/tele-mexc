# AGENTS.md

Repository guidance for coding agents and operators. Read this first, then the
focused document for the task.

## What this repository is

Single-account, single-channel Telegram ingestion service with a local SQLite
archive, a downstream processing queue, and
a durable MEXC Spot order-intent pipeline. One Python package, one CLI:
`tg-ingest`.

- Python 3.12, Telethon (MTProto), SQLite, stdlib HTTP client for MEXC.
- Telegram and MEXC traffic is direct; proxy configuration is rejected.
- No web server, no public network listener, no withdrawals, no futures,
  no short selling.
- MEXC Spot only, automatic execution of valid signals; there is no
  SIM/TESTONLY/LIVE product-mode split, no symbol allowlist, and no per-signal
  approval. Construction requires the exact owner acknowledgement, a fixed
  quote amount, and MEXC credentials plus the CLI write backend restricted to
  Spot.
- Pause is enforced by a kernel file lock at the order-submission boundary;
  readiness (Telegram, MEXC, channel, emergency stop, reconciliation)
  is checked in the same transaction as the submission claim.

## Source-of-truth order

1. Executable code and tests (`tg_signal_ingest/`, `tests/`).
2. `docs/trading.md`, `docs/data-model.md`, `docs/operations.md`.
3. `README.md` and the focused docs linked from `docs/documentation-index.md`.
4. Historical verification notes and exchange research are context, not a
   description of current behavior.

When docs and code disagree, treat the code as authoritative and fix the docs.
Do not silently change behavior to match an old doc.

## Safety rules for agents

- Never place, cancel, test, or reconcile a real exchange order.
- Never run `tg-ingest auth login`, `auth logout`, `serve`, `control run`,
  `diagnostics telegram`, `trading verify`, or
  `trading reconcile` unless the user explicitly asks and understands the
  side effects.
- Never read, print, copy, or commit `.env`, `config/config.toml` values,
  Telegram session files, the SQLite database, or message contents. Use the
  documented redacted status commands instead.
- Never reset, clean, checkout, or revert unrelated working-tree changes. This
  checkout routinely contains intentional modifications and untracked files.
- Do not change application logic when the task is documentation-only.
- Do not deploy, restart systemd units, or edit production configuration
  without explicit authorization.

## Repository map

| Path | Responsibility |
| --- | --- |
| `tg_signal_ingest/main.py` | CLI commands, process startup, locks, `serve` orchestration |
| `tg_signal_ingest/config.py` | process env > `.env` > TOML > defaults, validation, trading safety caps |
| `tg_signal_ingest/db.py` | SQLite connection, WAL, embedded migrations 1–21 |
| `tg_signal_ingest/repository.py` | message upsert, edits, queue transitions, sync state |
| `tg_signal_ingest/service.py` | live listener, recovery sweep, background workers |
| `tg_signal_ingest/backfill.py` | resumable history import and checkpoints |
| `tg_signal_ingest/processing.py` | bounded queue consumer with backoff |
| `tg_signal_ingest/trading/` | parser, risk, intents, reservations, execution, reconciliation, trade lifecycle, notifications |
| `tg_signal_ingest/mexc/` | MEXC Spot v3 client, typed errors, symbol metadata, depth parsing |
| `tg_signal_ingest/control/` | two-owner/programmer controller, setup store, and Bot API client |
| `docs/` | operator, developer, architecture, trading, data docs |
| `deploy/` | systemd unit for `serve` |
| `tests/` | pytest suite, fakes, parser fixtures |

## Runtime invariants

- Message identity is `(channel_id, message_id)`; edits increment
  `processing_version` and are appended to `message_edits`.
- Queue identity is `(channel_id, message_id, item_type, targets_version)`, so
  retries and duplicate edits cannot double-process one version.
- Order idempotency is SHA-256 of
  `(channel_id, message_id, action, symbol)`; one Telegram message can produce
  at most one intent, and a conflicting edit is refused.
- The activation boundary is persisted **per channel** under
  `app_state['trading.activation_boundary.channel.<channel_id>']` (with the
  legacy global key kept only as a migration fallback). Each `serve` start
  records Telegram's newest visible message id plus the newest stored id before
  registering live handlers; the value only moves forward. Messages at/below it,
  and every `source='backfill'`/`'recovery'` row, are stored and audited but never
  traded. If the boundary cannot be established, new entries fail closed.
- A `planned` intent that predates the current process start is canceled locally
  during restart recovery and is never submitted after downtime. Open entries,
  fills, and OCO exits are reconciled read-only; uncertain submissions are never
  resubmitted.
- Two owners (Owner A, Owner B) have owner-level controls. The programmer has
  read-only technical/diagnostic access and is never an owner. Every recorded
  operational event is routed to both owners and the programmer; severity or
  category never suppresses the owners' event stream.
- Every intent reserves its full quote notional plus the fee buffer in
  `trading_budget_ledger` inside the intent-creation transaction. This is a
  technical/idempotency reservation, not a portfolio budget or spending cap.
  The ledger is never reset by restart.
- Uncertain live submission outcomes become
  `unknown_requires_reconciliation`; they are never retried automatically.
  Exchange `-2011` never terminalizes a non-terminal local intent.
- Terminal fills are recovered through `fill_sync_required` until `myTrades`
  confirms the full executed quantity.
- `serve` holds `<db>.serve.lock`. When `cfg.control.enabled` is true, it also
  acquires `<db>.control.lock` and runs the ControlBot task inside the same
  process. `control run` is the standalone controller mode and takes only the
  control lock. Never run both against the same database.
- The live market-monitor → `TradeManager` path has wiring limitations
  documented in `docs/trading.md`. Do not assume TP/SL or conditional entries
  are production-ready without validation.

## Common commands

```bash
# Offline checks (safe)
.venv/bin/tg-ingest --help
.venv/bin/tg-ingest status
.venv/bin/tg-ingest backfill status
.venv/bin/tg-ingest trading status
.venv/bin/tg-ingest trading readiness          # non-trading, safe
.venv/bin/tg-ingest control status

# Quality gates
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/mypy tg_signal_ingest

# Setup / long-running (owner-authorized only)
.venv/bin/tg-ingest config init
.venv/bin/tg-ingest config check
.venv/bin/tg-ingest auth login
.venv/bin/tg-ingest channels list
.venv/bin/tg-ingest channel select <id-or-@username-or-title>
.venv/bin/tg-ingest serve                 # ingestion + optional integrated control bot
.venv/bin/tg-ingest control run           # standalone controller only
```

## Change guidelines

- Preserve API/state compatibility unless the task explicitly changes behavior.
- Add a new migration; never rewrite an applied migration. Migrations must stay
  idempotent and ordered.
- Use `Decimal` for prices, quantities, fees, and reservation arithmetic. Do not let
  binary floats reach an exchange call.
- Keep DB writes inside `Database.transaction()` and preserve the
  intent-before-submission ordering.
- Never add a code path that retries an uncertain exchange submission,
  introduces a portfolio spending cap, trusts Telegram usernames, or derives
  size from signal text.
- Add or update tests for behavior changes. Parser changes usually require a
  replay/reprocess test.
- Keep docs concise: link to authoritative detail instead of duplicating it.

## Documentation maintenance policy

Update docs in the same change as behavior:

| Behavior changed | Update |
| --- | --- |
| CLI commands/options, locks, service startup | `docs/operations.md`, `README.md` |
| Ingestion, recovery, process layout | `docs/architecture.md` |
| Trading risk, reservations, states, reconciliation | `docs/trading.md` |
| Schema, enums, migration, retention | `docs/data-model.md` |
| Test/lint/typecheck/setup workflow | `docs/development.md` |
| Doc ownership or status | `docs/documentation-index.md` |

Rules:

- Document implemented behavior only. Mark inference and unresolved gaps
  explicitly.
- Prefer tables, short bullets, exact file paths, and copy-paste commands.
- Do not document secrets, values from `.env`/`config.toml`, session data, or
  private message content.
- Do not copy generic skill instructions into this repository.
- Keep `docs/OPERATIONS.md` as a compatibility redirect; the canonical guide is
  `docs/operations.md`.
- At the end of a docs change, verify every referenced path and command, and
  scan for secrets and contradictions.
