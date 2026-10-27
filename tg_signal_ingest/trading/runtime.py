"""Construction helpers shared by the service and CLI."""

from __future__ import annotations

import logging
import math
from typing import Any

from ..config import AppConfig, ConfigError
from ..mexc import MexcSpotClient, SymbolRegistry, WeightedRateGate
from ..repository import Repository
from .engine import TradingEngine
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

    source = str(getattr(cfg.trading, "order_quote_amount_source", ""))
    if source == "env":
        return False
    try:
        row = repo.db.conn.execute(
            "SELECT value FROM control_settings WHERE key = 'order_quote_amount'"
        ).fetchone()
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
    cfg.trading.order_quote_amount_source = "database (Telegram menu)"
    cfg.trading.amount_configured = True
    log.info("applied persisted order amount %.8g USDT from the Telegram menu", value)
    return True


def _build_notifier(cfg: AppConfig, store: TradingStore) -> Any | None:
    """Build the durable owner/developer notifier when Bot API is configured."""

    control = cfg.control
    if not control.bot_token or not (control.owner_user_id or control.developer_user_id):
        return None
    try:
        from ..control.bot_api import BotApiClient
        from .notify import TradingNotifier

        bot = BotApiClient(control.bot_token, api_base=control.api_base)
        return TradingNotifier(
            store,
            bot,
            owner_id=control.owner_user_id,
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
    rest = MexcSpotClient(
        cfg.mexc.api_key,
        cfg.mexc.api_secret,
        base_url=cfg.mexc.base_url,
        recv_window_ms=cfg.mexc.recv_window_ms,
        connect_timeout_s=cfg.mexc.connect_timeout_s,
        request_timeout_s=cfg.mexc.request_timeout_s,
        general_gate=WeightedRateGate(max(1, cfg.mexc.max_ip_weight_per_10s), 10.0),
    )
    if for_live_writes and cfg.mexc.cli_enabled:
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
            raise ConfigError(
                "MEXC CLI is not compatible with this build: " + "; ".join(health.problems)
            )
        return HybridMexcClient(rest, cli)
    return rest


def build_trading_engine(
    cfg: AppConfig,
    repo: Repository,
    *,
    client: Any | None = None,
    require_client: bool = False,
    price_provider: Any | None = None,
) -> TradingEngine:
    """Build the pipeline.

    ``sim`` never constructs a network client. ``testonly``/``live`` require
    credentials; live additionally needs the explicit configuration token.
    """

    if client is None and (require_client or cfg.trading.mode in ("testonly", "live")):
        client = build_mexc_client(cfg, for_live_writes=cfg.trading.mode == "live")
    # Owner-selected per-order amount persisted by the Telegram menu.  Env/TOML
    # values always win; the database only replaces the built-in default.
    apply_persisted_trading_settings(cfg, repo)
    registry = None
    if client is not None and cfg.trading.mode in ("testonly", "live"):
        registry = SymbolRegistry(client)
    trade_store = TradeStore(repo.db, cfg.trading)
    engine = TradingEngine(
        cfg,
        repo,
        client=client,
        registry=registry,
        price_provider=price_provider,
        notifier=None,
    )
    # Durable owner/developer reporting.  The notifier is attached to both the
    # engine and the trade manager so lifecycle events are never dropped.
    notifier = _build_notifier(cfg, engine.store)
    engine.notifier = notifier
    # Trade lifecycle coordinator wires into the engine.  In live mode it owns
    # conditional entry, TP1/TP2 and stop-loss; in other modes it is inert.
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
