# Documentation Index

Map of maintained documentation, authority, and historical material. When a
document contradicts executable code, verify the code and tests first, then fix
the document.

## Authority order

1. Executable code and tests: `tg_signal_ingest/`, `tests/`.
2. Canonical behavior: `docs/trading.md`, `docs/data-model.md`,
   `docs/operations.md`, `docs/architecture.md`.
3. Agent/onboarding: `AGENTS.md`, `README.md`, `docs/development.md`,
   `docs/CONTROL_BOT_FA.md`.
4. Exchange research and historical audits: context only.

## Canonical guides

| Document | Audience | Covers |
| --- | --- | --- |
| `AGENTS.md` | Agents, maintainers | Repository map, safety rules, runtime invariants, change/docs policy |
| `README.md` | Everyone | Product summary, quick start, safety model, limitations |
| `docs/architecture.md` | Developers, operators | Processes, locks, `serve`/`control` lifecycle, ingestion, queue, trading wiring |
| `docs/operations.md` | Operators | Install, credentials, service, backup, upgrade/rollback, recovery, troubleshooting |
| `docs/trading.md` | Operators, developers | Parsing, risk, fixed sizing, LIMIT execution, conditional entry, per-channel stale-signal boundary, two native OCO exits, restart recovery, reconciliation, readiness, notifications |
| `docs/data-model.md` | Developers | Active and historical tables, migrations, enums, persistence invariants |
| `docs/development.md` | Developers | Setup, test areas, lint/typecheck, migrations, change workflow |
| `docs/documentation-index.md` | Everyone | This index |

## Focused current guides

| Document | Audience | Covers |
| --- | --- | --- |
| `docs/CONTROL_BOT_FA.md` | Owner/developer (Persian) | ControlBot menus, MEXC setup, channel selection, BUY amount, notification severity, pause/resume, troubleshooting |
| `docs/LIVE_VERIFICATION.md` | Owner/operator | Evidence levels and non-trading live checklist, including `trading readiness`; no claim of current live exchange verification |
| `docs/TRADING_DECISIONS.md` | Owner/developers | Dated trading decision record; current sections are limited by later owner decisions |
| `docs/NON_NEGOTIABLE_TRADING_RULES_EN.md` | Owner/developers | Owner-confirmed safety rules (English) |
| `docs/NON_NEGOTIABLE_TRADING_RULES_FA.md` | Owner/developers | Owner-confirmed safety rules (Persian) |
| `docs/OPERATIONS.md` | — | Compatibility redirect to `docs/operations.md`; do not extend |

## Exchange documents

| Document | Status |
| --- | --- |
| `docs/exchanges/MEXC_SPOT_INTEGRATION.md` | Integration contract; current behavior is canonical in `docs/trading.md` |
| `docs/exchanges/MEXC_SPOT_RISKS.md` | Exchange/security risk assessment; dated, verify before live enablement |
| `docs/exchanges/MEXC_SPOT_API_RESEARCH.md` | Historical research snapshot (2026-10-23); not a current behavior spec |

## Historical / audit archive

These are retained for evidence and rationale. They may describe removed
features, old product modes, or past live state. Do not treat them as current
instructions.

| Path | Contents |
| --- | --- |
| `docs/FINAL_REPORT.md` | Historical implementation report |
| `FINAL_REPORT.md` | Historical root-level completion report |
| `docs/INDEPENDENT_AUDIT_2026-10-29.md` | Historical independent audit |
| `docs/MAINTENANCE_AUDIT_2026-10-29.md` | Historical maintenance audit |
| `docs/TRADING_RULES_COMPLIANCE_AUDIT.md` | Historical compliance audit |
| `docs/trading-audit-notifications-design.md` | Unimplemented design proposal |
| `docs/ui_audit_findings.md` | Historical audit of the removed management bot UI |
| `docs/evidence/` | Historical forensic dossiers; see `docs/evidence/README.md` |
| `reports/` | Historical investigations; see `reports/README.md` |

## Reading order

| Goal | Read in order |
| --- | --- |
| Operate the service | `README.md` → `docs/operations.md` → `docs/trading.md` |
| Understand the system | `docs/architecture.md` → `docs/data-model.md` → `docs/trading.md` |
| Change code | `AGENTS.md` → `docs/development.md` → relevant canonical guide |
| Run the ControlBot | `docs/CONTROL_BOT_FA.md` |
| Verify evidence | `docs/LIVE_VERIFICATION.md` |
| Research exchange API | `docs/exchanges/MEXC_SPOT_API_RESEARCH.md` plus current `mexc/` code |

## Ownership rules

- Update the canonical guide in the same change as behavior.
- Keep `docs/OPERATIONS.md` as a redirect.
- Mark historical/inference status explicitly; do not present proposals as
  current behavior.
- Never add secrets, `.env`/`config.toml` values, session data, database
  contents, or private message content.
- Do not duplicate long explanations; link to the canonical document.
