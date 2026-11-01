"""Non-trading first-startup readiness report.

The report is deliberately read-only with respect to the exchange: it never
places, replaces, cancels, or modifies an order.  It may perform ordinary
read-only authentication/account queries when ``network=True``; the default
offline report uses durable local state and marks network facts as
``NOT_VERIFIED`` rather than pretending they were checked.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import AppConfig
from ..repository import Repository
from .store import TradingStore

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"
NOT_VERIFIED = "NOT_VERIFIED"

LATEST_MIGRATION = 21
RECONCILIATION_BLOCKING_STATES = ("submitting", "reconciling", "unknown_requires_reconciliation")
UNPROTECTED_TRADE_STATES = ("needs_reconciliation", "unprotected_critical")


@dataclass
class Check:
    name: str
    status: str
    detail: str


@dataclass
class ReadinessReport:
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, status: str, detail: str) -> None:
        self.checks.append(Check(name=name, status=status, detail=detail))

    @property
    def blockers(self) -> list[Check]:
        return [check for check in self.checks if check.status == FAIL]

    @property
    def warnings(self) -> list[Check]:
        return [check for check in self.checks if check.status in (WARN, NOT_VERIFIED)]

    @property
    def ready(self) -> bool:
        return not self.blockers

    def verdict(self) -> str:
        return "READY FOR FIRST CONTROLLED STARTUP" if self.ready else "NOT READY"

    def render(self) -> str:
        widths = max((len(check.name) for check in self.checks), default=10)
        lines = ["Tele-MEXC first-startup readiness (no exchange writes)", ""]
        for check in self.checks:
            lines.append(f"[{check.status:<12}] {check.name:<{widths}}  {check.detail}")
        lines.append("")
        if self.blockers:
            lines.append("BLOCKERS:")
            lines.extend(f"  - {check.name}: {check.detail}" for check in self.blockers)
        else:
            lines.append("BLOCKERS: none")
        if self.warnings:
            lines.append("WARNINGS / NOT VERIFIED:")
            lines.extend(f"  - {check.name}: {check.detail}" for check in self.warnings)
        lines.append("")
        lines.append(f"RESULT: {self.verdict()}")
        return "\n".join(lines)


def _run_git(args: list[str]) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _count(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] if row is not None else 0)


def build_readiness_report(
    cfg: AppConfig,
    repo: Repository,
    *,
    network: bool = False,
    cli_checker: Any | None = None,
    account_checker: Any | None = None,
) -> ReadinessReport:
    """Build the readiness report from durable state.

    ``cli_checker`` and ``account_checker`` are injectable for deterministic
    tests.  They must be read-only; the production defaults only run CLI
    version/profile checks and signed read queries.
    """

    report = ReadinessReport()
    store = TradingStore(repo.db, cfg.trading)
    conn = repo.db.conn

    # ----------------------------------------------------------- repository
    sha = _run_git(["rev-parse", "HEAD"])
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    dirty = _run_git(["status", "--porcelain"])
    if sha:
        report.add("git commit", PASS, sha)
    else:
        report.add("git commit", WARN, "not a git checkout or git unavailable")
    if branch:
        report.add("git branch", PASS, branch)
    if dirty is None:
        report.add("working tree", WARN, "git status unavailable")
    elif dirty:
        report.add("working tree", FAIL, "uncommitted changes present; commit before the controlled startup")
    else:
        report.add("working tree", PASS, "clean")

    # -------------------------------------------------------------- database
    version = repo.db.schema_version()
    if version == LATEST_MIGRATION:
        report.add("database schema", PASS, f"migration {version} (current)")
    else:
        report.add(
            "database schema",
            WARN,
            f"migration {version}; normal startup/readiness applies pending migration {LATEST_MIGRATION}",
        )

    # ---------------------------------------------------------- configuration
    trading = cfg.trading
    if trading.order_quote_amount and trading.order_quote_amount > 0:
        report.add(
            "fixed quote amount",
            PASS,
            f"{trading.order_quote_amount:g} USDT per entry (source: {trading.order_quote_amount_source})",
        )
    else:
        report.add("fixed quote amount", FAIL, "order_quote_amount must be positive")
    if cfg.mexc.api_key and cfg.mexc.api_secret:
        report.add("MEXC REST credentials", PASS, "configured (values not printed)")
    else:
        report.add("MEXC REST credentials", FAIL, "MEXC_API_KEY/MEXC_API_SECRET are missing")
    if cfg.mexc.cli_enabled:
        report.add("MEXC CLI writes", PASS, "enabled")
    else:
        report.add("MEXC CLI writes", FAIL, "[mexc_cli] enabled must be true for automatic Spot writes")
    if cfg.mexc.cli_confirm_noninteractive_writes:
        report.add("CLI non-interactive writes", PASS, "armed")
    else:
        report.add(
            "CLI non-interactive writes",
            FAIL,
            "explicit confirm_noninteractive_writes=false makes every write fail closed",
        )

    # ---------------------------------------------------------------- owners
    control = cfg.control
    if control.bot_token:
        report.add("control bot token", PASS, "configured (value not printed)")
    else:
        report.add("control bot token", FAIL, "CONTROL_BOT_TOKEN is required for owner notifications")
    if control.owner_user_id:
        report.add("Owner A", PASS, str(control.owner_user_id))
    else:
        report.add("Owner A", FAIL, "CONTROL_OWNER_USER_ID is not configured")
    if control.owner_b_user_id:
        report.add("Owner B", PASS, str(control.owner_b_user_id))
    else:
        report.add("Owner B", PASS, "optional; Owner A is sufficient")
    if control.developer_user_id:
        report.add("programmer role", PASS, str(control.developer_user_id))
    else:
        report.add("programmer role", WARN, "CONTROL_DEVELOPER_USER_ID is not configured (technical diagnostics only)")
    if control.owner_user_id and control.owner_b_user_id and control.owner_user_id == control.owner_b_user_id:
        report.add("owner role separation", FAIL, "Owner A and Owner B must be different Telegram ids")
    if control.owner_user_id and control.developer_user_id and control.owner_user_id == control.developer_user_id:
        report.add("programmer role separation", FAIL, "the programmer must not be an owner")
    if control.configured:
        report.add("owner notification routing", PASS, "durable outbox configured for owners + programmer")
    else:
        report.add("owner notification routing", FAIL, "control bot/owner configuration is incomplete")

    # ------------------------------------------------------------ channel
    selected = repo.selected_channel()
    if selected is None:
        report.add("selected channel", FAIL, "no Telegram channel is selected")
    else:
        report.add("selected channel", PASS, f"id={selected.channel_id} title={selected.title!r}")
        boundary = store.activation_boundary(selected.channel_id)
        if boundary is None:
            report.add(
                "activation boundary",
                WARN,
                "not yet established; serve establishes it before registering live handlers",
            )
        else:
            report.add("activation boundary", PASS, f"channel {selected.channel_id} message id {boundary}")
        setup = conn.execute("SELECT telegram_state, selected_channel_id FROM control_setup WHERE id = 1").fetchone()
        if setup is not None and setup["selected_channel_id"]:
            report.add("channel selection (setup)", PASS, "persisted")
        else:
            report.add(
                "channel selection (setup)",
                WARN,
                "ControlBot setup state is stale; runtime uses channels.is_selected (authoritative)",
            )

    # ------------------------------------------------------------- signals
    planned = _count(conn, "SELECT COUNT(*) FROM order_intents WHERE state='planned'")
    if planned:
        report.add(
            "pre-restart planned intents",
            WARN,
            f"{planned} planned intent(s) will be canceled locally at startup and never submitted",
        )
    else:
        report.add("pre-restart planned intents", PASS, "none")
    unresolved = _count(
        conn,
        "SELECT COUNT(*) FROM order_intents WHERE state IN (?,?,?)",
        RECONCILIATION_BLOCKING_STATES,
    )
    if unresolved:
        report.add(
            "unresolved submissions",
            WARN,
            f"{unresolved} intent(s) are submitting/reconciling/unknown; tracked per-order, not a global entry block",
        )
    else:
        report.add("unresolved submissions", PASS, "none")
    fill_sync = _count(conn, "SELECT COUNT(*) FROM order_intents WHERE fill_sync_required=1")
    if fill_sync:
        report.add("fill synchronization", WARN, f"{fill_sync} intent(s) still require fill synchronization")
    else:
        report.add("fill synchronization", PASS, "complete")

    # -------------------------------------------------------------- trades
    open_trades = conn.execute(
        "SELECT trade_id, symbol, trade_state FROM trades WHERE trade_state NOT IN "
        "('closed','tp2_completed','canceled','rejected','failed') ORDER BY updated_at"
    ).fetchall()
    if open_trades:
        states = ", ".join(f"{row['symbol']}:{row['trade_state']}" for row in open_trades[:10])
        report.add("open trade lifecycles", PASS, f"{len(open_trades)} non-terminal trade(s): {states}")
    else:
        report.add("open trade lifecycles", PASS, "none")
    unprotected = conn.execute(
        "SELECT trade_id, symbol, trade_state FROM trades WHERE trade_state IN (?,?) ORDER BY updated_at",
        UNPROTECTED_TRADE_STATES,
    ).fetchall()
    if unprotected:
        detail = "; ".join(f"{row['trade_id']} {row['symbol']} {row['trade_state']}" for row in unprotected[:10])
        report.add(
            "unprotected / uncertain positions",
            WARN,
            f"tracked per-order; does not block unrelated symbols: {detail}",
        )
    else:
        report.add("unprotected / uncertain positions", PASS, "none")
    block_reason = store.buy_block_reason()
    if block_reason:
        report.add("new-BUY block", WARN, f"diagnostic only; global entry blocking is disabled: {block_reason}")
    else:
        report.add("new-BUY block", PASS, "cleared")
    oco_rows = conn.execute(
        "SELECT state, COUNT(*) AS n FROM oco_groups GROUP BY state ORDER BY state"
    ).fetchall()
    if oco_rows:
        report.add("OCO groups", PASS, ", ".join(f"{row['state']}={row['n']}" for row in oco_rows))
    else:
        report.add("OCO groups", PASS, "none")

    # --------------------------------------------------------- notification
    notif = conn.execute(
        "SELECT status, COUNT(*) AS n FROM trading_notifications GROUP BY status ORDER BY status"
    ).fetchall()
    notif_counts = {str(row["status"]): int(row["n"]) for row in notif}
    pending = notif_counts.get("pending", 0) + notif_counts.get("retry", 0)
    ambiguous = notif_counts.get("ambiguous", 0)
    failed = notif_counts.get("failed", 0)
    if ambiguous:
        report.add(
            "notification outbox",
            WARN,
            f"{ambiguous} ambiguous delivery(ies); delivery certainty is unresolved",
        )
    elif failed:
        report.add("notification outbox", WARN, f"{failed} definitively failed delivery(ies); inspect the outbox")
    elif pending:
        report.add("notification outbox", PASS, f"{pending} pending/retry notification(s) will flush on startup")
    else:
        report.add("notification outbox", PASS, "no pending/failed deliveries")

    audit_count = _count(conn, "SELECT COUNT(*) FROM audit_events")
    report.add("event journal", PASS, f"{audit_count} durable audit event(s) recorded")

    # ------------------------------------------------------------- controls
    setup = conn.execute("SELECT paused, pause_reason FROM control_setup WHERE id = 1").fetchone()
    if setup is None:
        report.add("pause state", FAIL, "control_setup row is missing")
    elif int(setup["paused"] or 0):
        report.add("pause state", WARN, f"paused: {setup['pause_reason'] or 'owner pause'}")
    else:
        report.add("pause state", PASS, "not paused")
    if cfg.trading.emergency_stop:
        report.add("emergency stop", FAIL, "emergency stop is active")
    else:
        report.add("emergency stop", PASS, "inactive")

    # ---------------------------------------------------- recovery readiness
    report.add(
        "startup recovery",
        PASS,
        "existing entries/fills/OCOs are reconciled read-only; stale planned intents are canceled locally",
    )

    # --------------------------------------------------------------- network
    _network_checks(cfg, report, network=network, cli_checker=cli_checker, account_checker=account_checker)
    return report


def _network_checks(
    cfg: AppConfig,
    report: ReadinessReport,
    *,
    network: bool,
    cli_checker: Any | None,
    account_checker: Any | None,
) -> None:
    """Read-only CLI/account checks.  Never submits or cancels anything."""

    if cli_checker is not None:
        try:
            result = cli_checker()
            if isinstance(result, dict):
                ok = bool(result.get("ok"))
                detail = str(result.get("detail") or result.get("version") or "")
            else:
                ok = bool(getattr(result, "ok", False))
                detail = "; ".join(getattr(result, "problems", []) or []) or "compatible"
            report.add("MEXC CLI availability", PASS if ok else FAIL, detail)
        except Exception as exc:  # noqa: BLE001 - readiness must report, not crash
            report.add("MEXC CLI availability", FAIL, f"check failed ({exc.__class__.__name__})")
    else:
        try:
            from ..mexc.cli import MexcCliAdapter

            adapter = MexcCliAdapter(
                binary=getattr(cfg.mexc, "cli_binary", None),
                profile=getattr(cfg.mexc, "cli_profile", None),
                timeout_s=float(getattr(cfg.mexc, "cli_timeout_s", 5.0)),
                min_version=getattr(cfg.mexc, "cli_min_version", "0.0.22"),
                confirm_writes=bool(getattr(cfg.mexc, "cli_confirm_noninteractive_writes", False)),
                symbols=None,
            )
            health = adapter.check()
            report.add(
                "MEXC CLI availability",
                PASS if health.ok else FAIL,
                "; ".join(health.problems) if health.problems else "compatible",
            )
        except Exception as exc:  # noqa: BLE001
            report.add("MEXC CLI availability", FAIL, f"check failed ({exc.__class__.__name__})")

    if not network:
        report.add(
            "Telegram authentication",
            NOT_VERIFIED,
            "offline report; run with --network to verify the stored session and channel access",
        )
        report.add(
            "REST/CLI account consistency",
            NOT_VERIFIED,
            "offline report; run with --network to compare read-only account identities",
        )
        return

    if account_checker is not None:
        try:
            result = account_checker()
            if isinstance(result, dict) and result.get("ok"):
                report.add("REST/CLI account consistency", PASS, str(result.get("detail") or "match"))
            else:
                account_detail: object = result.get("detail") if isinstance(result, dict) else "mismatch"
                report.add("REST/CLI account consistency", FAIL, str(account_detail))
        except Exception as exc:  # noqa: BLE001
            report.add("REST/CLI account consistency", FAIL, f"check failed ({exc.__class__.__name__})")
    else:
        try:
            from ..mexc.cli import MexcCliAdapter
            from ..mexc.credentials import validate_credentials
            from .runtime import build_mexc_client

            rest = build_mexc_client(cfg)
            info = validate_credentials(rest)
            if str(getattr(info, "status", "VALID")).upper() != "VALID":
                report.add("REST/CLI account consistency", FAIL, "REST key is not valid")
            else:
                adapter = MexcCliAdapter(
                    binary=getattr(cfg.mexc, "cli_binary", None),
                    profile=getattr(cfg.mexc, "cli_profile", None),
                    timeout_s=float(getattr(cfg.mexc, "cli_timeout_s", 5.0)),
                    min_version=getattr(cfg.mexc, "cli_min_version", "0.0.22"),
                    confirm_writes=bool(getattr(cfg.mexc, "cli_confirm_noninteractive_writes", False)),
                    symbols=None,
                )
                cli_account = adapter.account()
                rest_account = rest.account()
                left = {b.get("asset"): b.get("free") for b in cli_account.get("balances", [])}
                right = {b.get("asset"): b.get("free") for b in rest_account.get("balances", [])}
                if left != right:
                    report.add("REST/CLI account consistency", FAIL, "CLI and REST balances differ")
                else:
                    report.add("REST/CLI account consistency", PASS, "same Spot account")
        except Exception as exc:  # noqa: BLE001
            report.add("REST/CLI account consistency", FAIL, f"check failed ({exc.__class__.__name__})")

    try:
        import asyncio

        from ..telethon import connect_and_verify

        async def _verify() -> str:
            client = await connect_and_verify(cfg)
            try:
                me = await client.get_me()
                return str(getattr(me, "id", "verified"))
            finally:
                await client.disconnect()

        identity = asyncio.run(_verify())
        report.add("Telegram authentication", PASS, f"session valid (account id {identity})")
    except Exception as exc:  # noqa: BLE001
        report.add("Telegram authentication", FAIL, f"session verification failed ({exc.__class__.__name__})")


def report_as_json(report: ReadinessReport) -> str:
    return json.dumps(
        {
            "result": report.verdict(),
            "ready": report.ready,
            "checks": [check.__dict__ for check in report.checks],
            "blockers": [check.__dict__ for check in report.blockers],
            "warnings": [check.__dict__ for check in report.warnings],
        },
        ensure_ascii=False,
        indent=2,
    )
