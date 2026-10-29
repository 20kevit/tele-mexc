# Development

Setup, checks, and change workflow. Production operation is in
`docs/operations.md`; runtime design is in `docs/architecture.md`.

## Prerequisites

- Linux, Python 3.12 or newer, Git.
- Installing dependencies requires package-index access. Normal tests use
  fakes and temporary data; manual integration runs require network access.
- The config loader rejects legacy Telegram proxy settings; service units unset
  ambient proxy variables for direct networking.

## Local setup

```bash
# From the repository root; use a Python version >= 3.12.
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
```

`tg-ingest config init` creates a private `.env` and `config/config.toml` if they
do not exist, both mode `0600`. It never overwrites existing files. Do not commit
either file.

## Quality gates

```bash
.venv/bin/pytest                 # full suite
.venv/bin/pytest -q tests/test_trading_engine.py
.venv/bin/pytest -q -k "budget or reconcile"
.venv/bin/ruff check .           # lint + import order
.venv/bin/mypy tg_signal_ingest  # configured type check
```

`pyproject.toml` defines pytest `asyncio_mode = "auto"`, test path `tests/`,
ruff line length 120, and mypy Python 3.12 with `check_untyped_defs`.

Tests use temporary SQLite databases and fake clients/transports; they must not
contact Telegram or MEXC. Shared fakes live in `tests/conftest.py` and
`tests/_trading_fakes.py`. An autouse fixture blocks IPv4/IPv6 connect and
sendto calls in the pytest process while allowing Unix socket pairs for asyncio.
It is not an OS-level network sandbox and does not cover subprocesses or DNS;
keep subprocesses fake or separately isolated and never use real credentials.

Ruff formatting is not an existing quality gate: `ruff format --check .` can
be used to assess it, but the repository has substantial pre-existing formatter
differences. Avoid a whole-repository reformat in an unrelated change. Tool
versions have lower bounds rather than a lockfile, so results may vary with
newer releases.

## High-value test areas

| Area | Tests |
| --- | --- |
| Config precedence/caps | `test_config.py`, `test_trading_config.py` |
| Schema/migrations | `test_db_migrations.py` |
| Message/queue semantics | `test_repository.py`, `test_processing.py`, `test_backfill.py` |
| Live/recovery behavior | `test_service.py`, `test_live_recovery.py`, `test_channel_trading_boundary.py`, `test_service_activation_boundary.py` |
| Parser/risk/sizing | `test_signals.py`, `test_sizing.py` |
| Intent/budget/execution | `test_trading_engine.py`, `test_trading_budget.py`, `test_trading_live_gates.py`, `test_mexc_execution_gates.py` |
| Reconciliation/fills | `test_trading_live_gates.py`, `test_balance.py` |
| Trade lifecycle (simulated) | `test_trading_conditional_entry.py`, `test_trading_tp_sl.py`, `test_trading_monitor.py` |
| MEXC contract | `test_mexc_client.py`, `test_mexc_official_contract.py`, `test_symbols.py` |
| MEXC rate limits/hybrid | `test_mexc_client.py`, `test_mexc_hybrid.py`, `test_trading_monitor.py` |
| MEXC CLI adapter/preflight | `test_mexc_cli_adapter.py`, `test_trading_preflight.py`, `test_trading_cli_protection.py`, `test_config_mexc_cli.py` |
| Management/control | `test_management_*.py`, `test_control_*.py` |

Do not "fix" a failing test by weakening a safety assertion (budget, replay
boundary, no-retry-on-uncertainty) without an explicit product decision.

## Changing the schema

1. Append a new `(version, step)` tuple to `MIGRATIONS` in `tg_signal_ingest/db.py`.
   A `step` is either a SQL string or a Python callable `(conn) -> None`; use a
   callable when you need to converge from a partially/manual schema (idempotent
   `ALTER TABLE ... ADD COLUMN` for missing columns only). Never edit an applied
   migration.
2. Use `CREATE TABLE/INDEX IF NOT EXISTS` and idempotent `ALTER TABLE`.
3. Add repository/store methods and tests, including reopen-and-migrate.
4. Update `docs/data-model.md` with tables, enums, and invariants.

`Database.migrate()` applies versions inside `BEGIN IMMEDIATE` and records them
in `schema_migrations`. It refuses to migrate a read-only connection.

## Changing ingestion or parsing

- Preserve message identity `(channel_id, message_id)` and versioned queue
  identity.
- Parser changes affect new messages only. To re-evaluate old messages:
  - `tg-ingest queue reprocess --id <message_id>` or `--all` re-enqueues the
    current processing path;
  - `tg-ingest analyze replay ...` re-runs the separate offline analyzer.
- Backfill must keep `enqueue_processing=False`; historical traffic is never
  tradeable.
- Ingestion gap recovery does not make historical messages tradeable. Trade
  lifecycle recovery is not read-only: it can place pending protection and retry
  requested cancellations. Preserve existing uncertainty and retry semantics;
  any change requires separate safety review.

## Changing trading behavior

Treat these as invariants unless the task explicitly changes the safety model:

- Intent before submission; atomic budget reservation before any exchange call.
- One idempotency key per Telegram message.
- No blind retry after network/5xx/unknown submission.
- `-2011` leaves non-terminal state non-terminal and flagged.
- Spot BUY only; sell exists only for trade-lifecycle exits.
- Fixed `order_quote_amount`, never signal-derived size.
- Aggregate budget is a configurable reference (default 50 USDT); no hard ceiling.
- Fresh balance preflight for live submission.
- Durable fills and notifications.

Update `docs/trading.md` in the same change. If you touch the trade lifecycle,
add an end-to-end service-level test, not only a simulation test.

## Manual offline smoke checks

Safe without credentials or network:

```bash
.venv/bin/tg-ingest --help
.venv/bin/python -c 'from tg_signal_ingest.config import load_config; print("import ok")'
```

Use only disposable configuration and data when testing status or analysis
commands. `status`, `backfill status`, `trading status`, `control status`, and
analysis commands open the configured or selected database and run migrations;
these are not strictly read-only. `manage status` opens an existing database
read-only. Commands using SQLite may contend with the running service.

## Debugging notes

- Use `--verbose` for DEBUG logging. Logs deliberately omit message contents,
  tokens, credentials, and signed parameter strings.
- SQLite uses WAL and a 10-second busy timeout. A `database is locked` error
  usually means a long maintenance command overlapped `serve`.
- Never copy a live SQLite file while it is being written. Use the backup
  procedure in `docs/operations.md`.
- Network diagnostics: `tg-ingest diagnostics network`; it works without
  credentials. `diagnostics telegram` uses the real session and channel.
