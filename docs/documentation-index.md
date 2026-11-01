# Documentation Index

Map of the repository documentation: audience, authority, and when to read it.
When a document contradicts executable code, the code wins; fix the document.

## Authority order

1. Executable code and tests: `tg_signal_ingest/`, `tests/`.
2. Canonical behavior docs: `docs/trading.md`, `docs/data-model.md`,
   `docs/operations.md`, `docs/architecture.md`.
3. `AGENTS.md`, `README.md`, and the focused guides listed below.
4. Manual verification notes and exchange research: context only, not a
   specification of current behavior.

## Core guides

| Document | Audience | Covers | Status |
| --- | --- | --- | --- |
| `AGENTS.md` | Coding agents, maintainers | Repository map, safety rules, runtime invariants, change and docs policy | Canonical agent entry point |
| `docs/architecture.md` | Developers, operators | Components, processes, locks, ingestion/recovery flow, queue, trading wiring | Canonical design |
| `docs/operations.md` | Operators | Install, configure, auth, channels, backfill, `serve`, `status`, `diagnostics`, systemd, troubleshooting | Canonical operator guide |
| `docs/trading.md` | Operators, developers | Parsing, risk, sizing, intent states, submission, reconciliation, lifecycle gaps | Canonical trading behavior |
| `docs/data-model.md` | Developers | Tables, enums, migrations, retention, persistence invariants | Canonical schema |
| `docs/development.md` | Developers | Setup, tests, lint, typecheck, migrations, change workflow | Canonical dev workflow |
| `docs/documentation-index.md` | Everyone | This map and doc ownership | Canonical index |

## Focused guides

| Document | Audience | Covers | Status |
| --- | --- | --- | --- |
| `docs/CONTROL_BOT_FA.md` | Owner and developer (Persian) | `control` bot, MEXC key storage, notifications, safe run | Current; bot can be separate from `serve` |
| `docs/LIVE_VERIFICATION.md` | Owner | Manual one-time Telegram integration check with real credentials | Manual procedure, not automated |
| `docs/OPERATIONS.md` | — | Compatibility redirect to `docs/operations.md` | Redirect only; do not extend |
| `docs/TRADING_DECISIONS.md` | Operators, developers | Canonical record of trading-scope, sizing, entry/exit, CLI-limitation, and notification decisions | Canonical decision record |
| `docs/trading-audit-notifications-design.md` | Maintainers, OpenCode implementers | Proposed structured audit journal, recipient policies, OCO/position evidence, and read-only investigation plan | Design only; not implemented and not a source of current runtime behavior |
| `docs/MAINTENANCE_AUDIT_2026-10-29.md` | Maintainers | Offline maintenance findings, check results, and verification limits | Historical audit, not live certification |

## Exchange research

Historical and design material under `docs/exchanges/`. Useful for rationale
and API details, but not a description of shipped behavior.

| Document | Covers |
| --- | --- |
| `docs/exchanges/MEXC_SPOT_API_RESEARCH.md` | MEXC Spot v3 API surface and constraints |
| `docs/exchanges/MEXC_SPOT_INTEGRATION.md` | Integration design notes |
| `docs/exchanges/MEXC_SPOT_RISKS.md` | Exchange and operational risk notes |

## Reading order

| Goal | Read in order |
| --- | --- |
| Operate the service | `README.md` → `docs/operations.md` → `docs/trading.md` |
| Understand the system | `docs/architecture.md` → `docs/data-model.md` → `docs/trading.md` |
| Change code | `AGENTS.md` → `docs/development.md` → relevant canonical guide |
| Design audit/notification work | `AGENTS.md` → `docs/trading-audit-notifications-design.md` → relevant canonical guides; verify every proposal against code/tests |
| Run the bot interface | `docs/CONTROL_BOT_FA.md` |
| Verify against real Telegram | `docs/LIVE_VERIFICATION.md` |

## Ownership rules

- Update the canonical guide in the same change as the behavior.
- Keep `docs/OPERATIONS.md` as a redirect; add new operator content to
  `docs/operations.md`.
- Mark inference and unresolved gaps explicitly instead of documenting intent as
  behavior.
- Link to authoritative detail rather than duplicating it.
- Never add secrets, `.env`/`config.toml` values, session data, database
  contents, or private message content.

## Design-note status

The implementation proceeds from the mission requirements plus `docs/evidence/*`
because `docs/trading-audit-notifications-design.md` (referenced by the design
task) was **not present** in the checkout. That absence is documented here; no
design file was invented. Implemented behavior is described in
`docs/trading.md`, `docs/data-model.md`, and `docs/operations.md`.
