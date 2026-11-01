# Live Verification Matrix

This file separates repository evidence from live-account verification. It is a
checklist, not proof that live trading has been exercised. Current behavior is
defined by `docs/trading.md`; do not treat a passing mocked test as exchange
verification.

## Evidence levels

| Level | Meaning |
| --- | --- |
| Unit-tested | Pure logic/parser/risk/state assertions with fakes |
| Integration-tested | Multi-component flow with disposable SQLite and mocked Telegram/MEXC |
| Verified against live Telegram | Performed interactively by the account owner with real credentials |
| Verified against live MEXC | Performed with real read/write credentials; requires explicit authorization |
| Not yet verified | No current evidence |

## 1. Offline / repository verification

Run from the repository root:

```bash
.venv/bin/pytest -q
.venv/bin/ruff check .
.venv/bin/mypy tg_signal_ingest
.venv/bin/python -m compileall -q tg_signal_ingest tests
```

| Check | Evidence level | Notes |
| --- | --- | --- |
| Parser, Futures/Perpetual wording, `.P` normalization, SHORT rejection | Unit-tested | `tests/test_signals.py`, `tests/test_futures_signals.py` |
| Risk requirements (Entry/SL/TP1/TP2, price relation, fixed quote) | Unit-tested | `tests/test_trading_engine.py`, `tests/test_trading_config.py` |
| LIMIT-only execution and MARKET rejection | Unit-tested/integration | config, risk, engine payload, REST/CLI client tests |
| Conditional entry default 3 ticks and symbol tick rounding | Unit-tested | `tests/test_trading_conditional_entry.py` |
| TP1/TP2/OCO lifecycle, partial fills, recovery, idempotency | Integration-tested with disposable SQLite and fakes | trading lifecycle/focused repair suites |
| Pause/resume and submission readiness | Integration-tested with fake exchange and file locks | `tests/test_submission_gate.py`, `tests/test_resume_atomicity.py` |
| Per-channel stale-signal boundary, 1/6/24h downtime, delayed delivery, backfill, edits/duplicates | Integration-tested with disposable SQLite and fake Telegram/MEXC | `tests/test_stale_signal_boundary.py`, `tests/test_service_activation_boundary.py` |
| Planned-intent quarantine after restart; open/uncertain lifecycle recovery | Integration-tested with fakes | `tests/test_stale_signal_boundary.py`, lifecycle/recovery suites |
| First-startup readiness report (non-trading) | Unit/integration-tested | `tests/test_readiness.py` |
| Schema/migration compatibility | Integration-tested on disposable DBs | `tests/test_db_migrations.py` (current migration 21) |
| CLI/config validation | Unit-tested | `tests/test_config*.py`, CLI parser tests |
| Two-owner + programmer notification routing; no severity suppression | Unit/integration-tested | `tests/test_trading_notify.py`, routing tests, `tests/test_control_service.py` |
| MEXC REST/CLI request shapes | Mocked/contract-tested | `tests/test_mexc_*`; not live exchange proof |

## 2. Non-trading live checks

These require real Telegram/MEXC access and owner authorization but place no
orders. They were **not re-run during the documentation-only pass**; results
must be recorded by the operator performing them.

| Check | Evidence level | Command/procedure |
| --- | --- | --- |
| First-startup readiness (prerequisites, blockers) | Offline report tested; live result recorded per deployment | `tg-ingest trading readiness` then `--network`; never places an order |
| One-time open-order cleanup (<5 USD, no position sales) | Unit-tested; live run recorded in deployment report | `tg-ingest trading cleanup-open-orders --apply` |
| Same-symbol exchange open-order entry rule | Unit/integration-tested with fakes | `tests/test_same_symbol_open_orders.py` |
| Direct DNS/TCP/TLS to Telegram | Not yet verified in this pass | `tg-ingest diagnostics network` |
| Telegram session validity | Not yet verified in this pass | `tg-ingest auth check` |
| Selected channel access without history download | Not yet verified in this pass | `tg-ingest diagnostics telegram` |
| Selected channel ingestion/backfill | Not yet verified in this pass | `tg-ingest status`, `tg-ingest backfill status`, `queue list` |
| ControlBot starts and authorizes both owners + programmer | Not yet verified in this pass | `tg-ingest control status`; `/start`; inspect logs |
| MEXC read-only credential verification | Not yet verified in this pass | `tg-ingest trading verify` |
| CLI profile/REST account identity | Not yet verified in this pass | `tg-ingest trading preflight --network` |
| Notification delivery | Not yet verified in this pass | Trigger one approved non-trading control event; inspect outbox |
| Database/worker health | Not yet verified in this pass | `status`, `trading status`, logs; no order action |

Historical read-only results recorded in this repository (2026-10-22/23) are
context only. They do not certify the current commit, account, or exchange
state.

## 3. Actions requiring explicit authorization

Do not perform these during a documentation, development, or ordinary
maintenance task. They require a specific owner-approved live test plan:

- Real order placement, including `/order/test` execution.
- Real order cancellation or reconciliation that mutates exchange state.
- Any check involving real funds, live positions, or withdrawal-capable keys.
- Production deployment or service restart while orders/positions are active.
- Changing `order_quote_amount`, pause state, or other owner-controlled settings in production.

## 4. Current live-verification status

| Area | Status |
| --- | --- |
| MEXC Spot live entry/exit/OCO | **Not verified against a live exchange by this repository pass** |
| MEXC CLI profile/account matching | **Not verified in this pass**; use `trading preflight --network` under owner authorization |
| Telegram live ingestion/ControlBot | **Not verified in this pass**; use the non-trading checks above |
| Mocked/offline behavior | Covered by the test suite, with the limits above |

A green test suite proves repository logic and mocked integration only. It does
not prove exchange acceptance, fill quality, fee behavior, account permissions,
or production readiness.

## 5. Code readiness vs live verification

| Area | Code status | Evidence level | Live verification |
| --- | --- | --- | --- |
| Stale-signal boundary and restart quarantine | Implemented | Integration-tested with mocks | Not verified against live Telegram downtime |
| Entry idempotency and uncertain submissions | Implemented | Unit/integration-tested with mocks | Not verified against a real MEXC timeout |
| Two native OCO exits | Implemented | Mocked lifecycle and CLI contract tests | **No live OCO has been placed from this repository** |
| Two-owner + programmer roles | Implemented | Unit/integration-tested | Not verified with the real Bot API/accounts |
| Durable event journal/outbox | Implemented | Unit/integration-tested | Delivery to real chats not verified in this pass |
| First-startup readiness report | Implemented | Offline tests pass | Owner must run `--network` before authorizing startup |

The `trading readiness` command is the authoritative pre-start gate for this
repository. A successful code/test pass does not replace its live account,
Telegram, CLI, and open-position checks.
