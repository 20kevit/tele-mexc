"""Construction helpers shared by the service and CLI."""

from __future__ import annotations

import logging
import math
from typing import Any

from ..config import AppConfig, ConfigError
from ..mexc import MexcSpotClient, SharedWeightedRateGate, SymbolRegistry
from ..repository import Repository
from .engine import TradingEngine
from .gate import SubmissionGate
from .store import TradingStore
from .trade_manager import TradeManager
from .trade_store import TradeStore

log = logging.getLogger("ingest.trading.runtime")


def apply_persisted_trading_settings(cfg: AppConfig, repo: Repository) -> bool:
    """Apply owner settings stored by the Telegram menus.

    Precedence is environment > database > ``config.toml`` > built-in default.
    The environment variable is reserved for deployment-level overrides; once
    the owner sets an amount in the Telegram menu it must keep working after a
    restart even when ``config.toml`` also names an amount.  Returns True when a
    stored value was applied.
    """

    # Signal validity is independent of the order amount, so it is applied even
    # when an environment override owns the amount.
    _apply_persisted_signal_validity(cfg, repo)

    source = str(getattr(cfg.trading, "order_quote_amount_source", ""))
    if source == "env":
        return False
    try:
        row = repo.db.conn.execute("SELECT value FROM control_settings WHERE key = 'order_quote_amount'").fetchone()
        legacy = False
        if row is None:
            # Compatibility: older control menus wrote buy_quote_usd.  Read it
            # only when the canonical setting is absent; it is never written
            # again and the new menu stores order_quote_amount.
            row = repo.db.conn.execute("SELECT value FROM control_settings WHERE key = 'buy_quote_usd'").fetchone()
            legacy = row is not None
    except Exception:  # noqa: BLE001 - a missing/old schema must not block startup
        return False
    if row is None:
        return False
    try:
        value = float(str(row["value"]))
    except (TypeError, ValueError):
        return False
    if not math.isfinite(value) or value <= 0:
        return False
    cfg.trading.order_quote_amount = value
    cfg.trading.order_quote_amount_source = "legacy database (buy_quote_usd)" if legacy else "database (Telegram menu)"
    cfg.trading.amount_configured = True
    log.info("applied persisted order amount %.8g USDT from the control database", value)
    return True


def _apply_persisted_signal_validity(cfg: AppConfig, repo: Repository) -> bool:
    """Apply the owner's signal-validity window stored by the Telegram menu."""

    try:
        row = repo.db.conn.execute(
            "SELECT value FROM control_settings WHERE key = 'signal_validity_hours'"
        ).fetchone()
    except Exception:  # noqa: BLE001 - a missing/old schema must not block startup
        return False
    if row is None:
        return False
    try:
        hours = float(str(row["value"]))
    except (TypeError, ValueError):
        return False
    if not 0.5 <= hours <= 168:
        return False
    cfg.trading.signal_validity_s = hours * 3600.0
    log.info("applied persisted signal validity %.3g hours from the control database", hours)
    return True


def _build_notifier(cfg: AppConfig, store: TradingStore) -> Any | None:
    """Build the durable owner/developer notifier when Bot API is configured."""

    control = cfg.control
    if not control.bot_token or not (control.owner_user_id or control.owner_b_user_id or control.developer_user_id):
        return None
    try:
        from ..control.bot_api import BotApiClient
        from .notify import TradingNotifier

        bot = BotApiClient(control.bot_token, api_base=control.api_base)
        return TradingNotifier(
            store,
            bot,
            owner_id=control.owner_user_id,
            owner_b_id=control.owner_b_user_id,
            developer_id=control.developer_user_id,
        )
    except Exception as exc:  # noqa: BLE001 - notifications must never block trading
        log.warning("trading notifier unavailable (%s); trading continues", exc.__class__.__name__)
        return None


def build_mexc_client(cfg: AppConfig, *, for_live_writes: bool = False) -> Any:
    if not cfg.mexc.api_key or not cfg.mexc.api_secret:
        raise ConfigError(
            "MEXC credentials are missing; set MEXC_API_KEY and MEXC_API_SECRET "
            "in the private .env (never commit them)."
        )
    state_dir = cfg.data_dir
    state_dir.mkdir(parents=True, exist_ok=True)
    rest = MexcSpotClient(
        cfg.mexc.api_key,
        cfg.mexc.api_secret,
        base_url=cfg.mexc.base_url,
        recv_window_ms=cfg.mexc.recv_window_ms,
        connect_timeout_s=cfg.mexc.connect_timeout_s,
        request_timeout_s=cfg.mexc.request_timeout_s,
        # One IP budget and one UID budget shared by serve, reconcile CLI and
        # any other local process.  Official limits are 300/10s (IP) and
        # 500/10s (UID); the configured values stay well below them.
        general_gate=SharedWeightedRateGate(
            max(1, cfg.mexc.max_ip_weight_per_10s),
            10.0,
            state_path=state_dir / "mexc-rate-limit-ip.json",
        ),
        uid_gate=SharedWeightedRateGate(
            max(1, cfg.mexc.max_uid_weight_per_10s),
            10.0,
            state_path=state_dir / "mexc-rate-limit-uid.json",
        ),
        order_gate=SharedWeightedRateGate(
            12,
            1.0,
            state_path=state_dir / "mexc-rate-limit-order.json",
        ),
    )
    if for_live_writes:
        if not cfg.mexc.cli_enabled:
            raise ConfigError(
                "the MEXC CLI execution backend is required for automatic Spot writes; set [mexc_cli] enabled = true"
            )
        from ..mexc.cli import MexcCliAdapter
        from ..mexc.hybrid import HybridMexcClient

        cli = MexcCliAdapter(
            binary=cfg.mexc.cli_binary,
            profile=cfg.mexc.cli_profile,
            timeout_s=cfg.mexc.cli_timeout_s,
            min_version=cfg.mexc.cli_min_version,
            confirm_writes=cfg.mexc.cli_confirm_noninteractive_writes,
            symbols=rest.symbols,
        )
        health = cli.check()
        if not health.ok:
            raise ConfigError("MEXC CLI is not compatible with this build: " + "; ".join(health.problems))
        return HybridMexcClient(rest, cli)
    return rest


def build_trading_engine(
    cfg: AppConfig,
    repo: Repository,
    *,
    client: Any | None = None,
    require_client: bool = True,
    price_provider: Any | None = None,
) -> TradingEngine:
    """Build the single automatic MEXC Spot pipeline.

    Construction is fail-closed on credentials and the CLI write backend, but
    there is no acknowledgement token or product-mode gate: every valid new
    signal is eligible for real execution subject to the trading constraints.
    """

    if client is None:
        client = build_mexc_client(cfg, for_live_writes=True)
    # Owner-selected per-order amount persisted by the Telegram menu.  Env/TOML
    # values always win; the database only replaces the built-in default.
    apply_persisted_trading_settings(cfg, repo)
    registry = SymbolRegistry(client) if client is not None else None
    trade_store = TradeStore(repo.db, cfg.trading)
    gate = SubmissionGate(repo.db, cfg)
    engine = TradingEngine(
        cfg,
        repo,
        client=client,
        registry=registry,
        price_provider=price_provider,
        notifier=None,
        submission_gate=gate,
    )
    # Durable owner/developer reporting.  The notifier is attached to both the
    # engine and the trade manager so lifecycle events are never dropped.
    notifier = _build_notifier(cfg, engine.store)
    engine.notifier = notifier
    # Trade lifecycle coordinator wires into the engine.  It owns conditional
    # entry, TP1/TP2 and stop-loss for every intent.
    trade_manager = TradeManager(
        cfg.trading,
        trade_store,
        engine=engine,
        client=client,
        notifier=notifier,
        price_provider=price_provider,
    )
    engine.trade_manager = trade_manager
    return engine
