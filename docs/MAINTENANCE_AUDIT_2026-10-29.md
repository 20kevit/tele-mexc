# Maintenance audit — 2026-10-29

## Scope and evidence

Baseline: branch `arena/035c9f0f-tele-mexc`, commit
`391eb6ab4813c489f7d89105a609548fd432e197`, clean tracked/untracked
working tree, origin `https://github.com/20kevit/tele-mexc.git`. Inventory:
152 tracked files, including 58 package files, 68 test/fixture files and 17
document files. The inventory, packaging, entry points, service units, test
setup, canonical docs, Git exclusions, and representative safety paths were
reviewed; this is **not** an exhaustive line-by-line certification of the
repository or production readiness. No private configuration, session,
database, production environment, or exchange account was inspected. No
Telegram or exchange request, order action, deployment, or service restart was
performed.

The project declares Python >=3.12. Because the initial sandbox interpreter
was 3.11.2, Python 3.12.10 was built locally with SQLite, TLS and libffi for
the supported-version checks. The baseline was tested from a `git archive` of
`HEAD` in a separate directory; Git commands in those tests used the original
Git directory and the archived worktree. The final suite ran in the working
checkout. A first 3.12 attempt could not collect 22 modules because the local
interpreter lacked `_ctypes`; this interpreter setup issue was fixed before the
runs reported below. Tests used temporary data, mocked services, and an additional external
`sitecustomize` guard against IPv4/IPv6 connect/connect_ex/sendto in Python
processes, including inherited Python subprocesses. That guard is not a full
OS network sandbox or DNS guard. Pytest plugin autoload was disabled and the
asyncio plugin explicitly enabled. No production configuration was present.

## Prioritized findings

1. **High — existing financial and notification risks, not changed.** Gross
   entry quantities are recorded as acquired quantities without deducting base
   fees (`trading/trade_manager.py:_reconcile_entry_fills`); non-USDT fees are
   valued using the traded pair's price (`trading/store.py:_fees_for_order`);
   lifecycle event keys are passed through without trade identity while outbox
   uniqueness is `(event_key, role)` (`trading/notify.py`, migration 16).
   The earlier [independent audit](INDEPENDENT_AUDIT_2026-10-29.md) documents
   synthetic examples and other unresolved OCO accounting/contract risks. These
   need separately approved accounting, persistence and notification work;
   no fix or current-account claim is made here.
2. **High — recovery was described as read-only.** The service invokes
   `recover_on_startup` and `reconcile_all_open`; lifecycle reconciliation can
   place pending OCO protection and retry requested cancellations. Corrected
   the service comment and operator/developer docs; implementation unchanged.
3. **Medium — private SQLite sidecars could escape ignore rules.** Outside
   already-ignored directories, `*.sqlite3-wal` and similar paths were not
   ignored. Added specific SQLite and session journal/WAL/SHM patterns and
   Git-based regression tests. This does not remove or modify existing data.
4. **Medium — unsafe/brittle test isolation.** The running-service logout test
   ignored its temporary config, read ambient config and failed without
   credentials. Injected the fixture and asserted logout is not attempted.
   Added an in-process Internet-socket guard and its tests. The guard is not a
   subprocess/DNS sandbox and deliberately allows Unix socket pairs.
5. **Medium — misleading operator reference.** Corrected the config precedence
   module docstring, CLI/REST query split, several trading defaults and retry
   descriptions, non-existent trade inspection commands, migration side effects
   of status commands, and the distinction between queue inspection and retry.
   Removed references to options not consumed by the current config loader.
6. **Low — quality workflow limitations.** There is no checked-in CI or
   lockfile. Runtime and dev dependencies use version ranges; `tomli` is
   conditional on Python <3.11 and hence inactive under the declared >=3.12
   requirement. It is retained: no packaging/dependency change was needed.
   Ruff formatting differs across much of the tree; no broad reformat, tooling
   dependency change, or new CI was introduced.

## File and dependency hygiene

- **Safe removal:** none established. The previous always-true
  `test_env_example_not_used_in_repo` assertion was replaced with a real
  `git check-ignore --no-index` assertion; no file or dependency was deleted.
- **Keep:** `docs/OPERATIONS.md` is an explicit compatibility redirect;
  `config/.gitkeep`, service units, CLI modules, existing regression fixtures,
  and the legacy control bot have documented or executable uses. Research and
  prior reports are historical evidence, not a source of current live state.
- **Uncertain — retain:** externally invoked modules, integrations, historical
  docs, and conditional dependencies cannot be proven unused solely from
  imports. No untracked work existed at baseline. Filename inventory found no
  tracked SQLite/session files or `.env`; it is not a content-level secret scan.
  Backups and arbitrary custom runtime filenames remain the operator's
  responsibility; ignore patterns do not protect already-tracked files.

## Maintenance patch

- `.gitignore`: narrow private sidecar exclusions.
- `tests/conftest.py`, `tests/test_offline_guard.py`: in-process socket guard
  and regression tests; `tests/test_auth_logout.py`: isolated logout guard;
  `tests/test_gitignore.py`: sidecar coverage and meaningful example-file test.
- `tg_signal_ingest/config.py`, `tg_signal_ingest/service.py`: documentation
  corrections only; no executed statements changed.
- `README.md`, `docs/architecture.md`, `docs/data-model.md`,
  `docs/development.md`, `docs/operations.md`, `docs/trading.md`: corrected
  status/migration effects, safety boundaries, defaults, commands and workflow.
- `docs/documentation-index.md`, this file: index and audit evidence.

No files or dependencies were moved, deleted, added to runtime requirements, or
upgraded. The two new files are this report and `tests/test_offline_guard.py`.

## Checks

Tools installed in an isolated virtualenv: pytest 9.2.0,
pytest-asyncio 1.4.1, Ruff 0.17.3, mypy 2.4.1, Telethon 1.45.0;
Python 3.12.10. Editable installation with `.[dev]` succeeded; no dependency
declarations changed. Below, `VENV=/home/user/.cache/audit-python/venv312` and
`OFFLINE=/home/user/.cache/audit-offline`; baseline commands ran from the
archived tree with `GIT_DIR` and `GIT_WORK_TREE` set as described above, and
final commands from the checkout.

| Command | Baseline | After maintenance |
| --- | --- | --- |
| `PYTHONPATH=$OFFLINE PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 $VENV/bin/pytest -p pytest_asyncio.plugin -q` | exit 1: **1 failed, 791 passed**; ambient-config logout test | exit 0: **807 passed** |
| `$VENV/bin/ruff check .` | exit 0: `All checks passed!` | exit 0: `All checks passed!` |
| `$VENV/bin/mypy tg_signal_ingest` | exit 0: `Success: no issues found in 58 source files` | exit 0: same |
| `$VENV/bin/ruff format --check .` | exit 1: **95 would be reformatted, 49 already formatted** | exit 1: **95 would be reformatted, 51 already formatted** |

The latest pytest-asyncio emits a pre-existing warning about the unset default
fixture loop scope. Ruff format is an assessment, not an established project
quality gate. `git diff --check` is also used to check patch whitespace. For a
normal clean checkout, setup and check commands are in `docs/development.md`.

To reconstruct the extra Python-process guard used for the comparison, create
`$OFFLINE/sitecustomize.py` before running pytest (the environment variable is
inherited by Python subprocesses):

```python
import socket

for name in ("connect", "connect_ex", "sendto"):
    original = getattr(socket.socket, name)

    def guarded(sock, *args, _original=original, **kwargs):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError("Internet sockets are disabled for the audit")
        return _original(sock, *args, **kwargs)

    setattr(socket.socket, name, guarded)
```

This additional guard has the same limitations noted above; for a stronger
boundary use OS-level network isolation and separately control subprocesses.

## Contract verification and limitations

The only runtime-package edits are a module docstring in `config.py` and a
comment in `service.py`. Comparing the parsed AST to `HEAD` after removing the
module docstring shows both files otherwise identical; all other runtime and
deployment files are unchanged. Fifteen root/subcommand `--help` outputs were
also compared between the archived baseline and checkout; all were identical
and exited 0. Thus CLI parsers/arguments, bot commands and
callbacks, authorization, defaults and secret precedence, migrations, trading
logic, recovery, retry, notifications, persistence and startup/shutdown code
were not edited. The offline test suite exercises representative contracts, not
all production behavior; no live equivalence, exchange contract, or complete
security audit is claimed. Configuration docstrings are introspectable and the
ignore rules and tests are intentionally changed.

## Follow-up

**Safe maintenance:** repeat the documented install and checks in a standard
Python 3.12+ environment with OS-level network isolation; consider a lockfile or
constraints and CI only after selecting supported tool versions; gradually
address formatting; extend subprocess/DNS isolation and review docs against
CLI help/config and historic audit findings. Review raw exception logging for
possible sensitive external error text before changing logging semantics.

**Separate approval required:** financial fee accounting and OCO parent/child
reconciliation, outbox event identity, recovery action/diagnostic separation,
changes to production logging or persistence, and any live integration or
upgrade. See the independent audit for evidence and proposed tests. Do not run
live diagnostics, mutate orders, or apply migrations to production as part of
this maintenance pass.
