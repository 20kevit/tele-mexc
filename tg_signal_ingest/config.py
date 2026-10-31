"""Configuration loading and validation.

Sources (highest precedence first):
  1. process environment variables
  2. ``config/config.toml`` (private, gitignored)
  3. ``.env`` at the project root (gitignored)
  4. built-in defaults

Secrets (Telegram API credentials) are supplied via environment or private
files and must never be logged. Telegram traffic uses direct networking.
"""

from __future__ import annotations

import math
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

API_ID_ENV = "TELEGRAM_API_ID"
API_HASH_ENV = "TELEGRAM_API_HASH"
SESSION_PATH_ENV = "TG_SESSION_PATH"

LEGACY_PROXY_ENV = (
    "TELEGRAM_PROXY_TYPE",
    "TELEGRAM_PROXY_HOST",
    "TELEGRAM_PROXY_PORT",
    "TELEGRAM_PROXY_USERNAME",
    "TELEGRAM_PROXY_PASSWORD",
)

MEXC_API_KEY_ENV = "MEXC_API_KEY"
MEXC_API_SECRET_ENV = "MEXC_API_SECRET"
MEXC_BASE_URL_ENV = "MEXC_BASE_URL"

# Conservative software defaults, NOT user-approved financial recommendations.
# Live trading always requires an explicit, separate user enablement step.
DEFAULT_MEXC_BASE_URL = "https://api.mexc.com"  # verified against official docs


class ConfigError(Exception):
    """Configuration is missing or invalid; message is safe to display."""


@dataclass
class MexcConfig:
    api_key: str | None = None
    api_secret: str | None = None
    base_url: str = DEFAULT_MEXC_BASE_URL
    connect_timeout_s: float = 5.0
    request_timeout_s: float = 10.0
    recv_window_ms: int = 5000  # official recommendation <5s; max 60000
    # UID-weighted budget: official shared bucket is 500 weight/10s; stay far below.
    max_uid_weight_per_10s: int = 100
    max_ip_weight_per_10s: int = 60  # official shared bucket 300/10s; conservative cap
    # Dedicated CLI execution backend (live writes only; reads stay on REST).
    cli_enabled: bool = True
    cli_binary: str | None = None
    cli_profile: str | None = None
    cli_timeout_s: float = 5.0
    cli_min_version: str = "0.0.22"
    cli_confirm_noninteractive_writes: bool = False


@dataclass
class TradingConfig:
    """All financial limits are user-configurable; defaults are conservative
    software placeholders only (documented as such, not sanctioned values)."""

    mode: str = "sim"  # "sim" | "testonly" | "live"; "live" never auto-enabled
    live_enabled_token: str | None = None  # must equal config.mexc_live_i_understand to arm live
    max_quote_per_order: float = 10.0
    daily_max_orders: int = 2
    max_open_orders: int = 2
    allowed_symbols: list[str] = field(default_factory=list)  # uppercase; empty = none allowed
    allowlist_mode: str = "deny"  # "deny" until user sets explicit allowlist
    max_signal_age_s: float = 900.0  # reject signals older than this
    max_price_deviation_pct: float = 1.0  # vs reference price at decision time
    require_stop_loss: bool = True  # reject alerts without an explicit SL
    order_type_policy: str = "limit"  # "limit" | "market"; market requires explicit config
    remaining_balance_reserve_pct: float = 20.0  # reserve when spending an asset
    balance_fee_buffer_pct: float = 0.2  # conservative fee buffer on top of order size
    balance_max_age_s: float = 5.0  # reject account snapshots older than this
    recon_interval_s: float = 30.0
    emergency_stop: bool = False  # app_state runtime flag; config initial value
    fake_price_for_tests: bool = False
    # Fixed per-order quote amount (owner-approved, never inferred from signal text).
    order_quote_amount: float = 3.0
    order_quote_amount_source: str = "default (3 USDT)"
    amount_configured: bool = True
    # Cumulative test/live budget reference (USDT). Configurable; no hard ceiling.
    aggregate_budget_quote: float = 50.0
    # Conditional entry: when set to an int >= 1, trades wait for an upward price
    # trigger before submission. None or 0 disables conditional entry.
    entry_trigger_offset_ticks: int | None = None
    entry_trigger_stale_threshold_s: float = 60.0
    # Take-profit allocation percentages.
    tp1_allocation_pct_of_acquired: float = 50.0
    tp2_allocation_pct_of_remaining: float = 100.0
    stop_loss_max_retries: int = 3
    # Stop-loss trigger offset: for new bot-initiated LONG spot positions the
    # stop-loss *activation trigger* is placed this many valid price ticks
    # above the configured stop-loss exit price so it can activate earlier as
    # the market falls. Zero disables the offset (trigger == exit price).
    # The configured stop-loss exit price itself is never changed.
    stop_loss_trigger_offset_ticks: int = 3
    # Price feed polling (monitor) configuration.
    monitor_poll_interval_s: float = 2.0
    monitor_max_retries: int = 5
    monitor_backoff_base_s: float = 1.0

    @property
    def conditional_entry_enabled(self) -> bool:
        """Conditional entry is active when a positive trigger offset is configured."""
        return self.entry_trigger_offset_ticks is not None and self.entry_trigger_offset_ticks >= 1

    @property
    def stop_loss_trigger_enabled(self) -> bool:
        """Stop-loss trigger offset is active when at least one tick is configured."""
        return self.stop_loss_trigger_offset_ticks is not None and self.stop_loss_trigger_offset_ticks >= 1

    def as_dict_public(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "max_quote_per_order": self.max_quote_per_order,
            "daily_max_orders": self.daily_max_orders,
            "max_open_orders": self.max_open_orders,
            "allowed_symbols": sorted(self.allowed_symbols),
            "allowlist_mode": self.allowlist_mode,
            "max_signal_age_s": self.max_signal_age_s,
            "max_price_deviation_pct": self.max_price_deviation_pct,
            "require_stop_loss": self.require_stop_loss,
            "order_type_policy": self.order_type_policy,
            "remaining_balance_reserve_pct": self.remaining_balance_reserve_pct,
            "balance_fee_buffer_pct": self.balance_fee_buffer_pct,
            "balance_max_age_s": self.balance_max_age_s,
            "recon_interval_s": self.recon_interval_s,
            "emergency_stop": self.emergency_stop,
            "order_quote_amount": self.order_quote_amount,
            "order_quote_amount_source": self.order_quote_amount_source,
            "amount_configured": self.amount_configured,
            "aggregate_budget_quote": self.aggregate_budget_quote,
            "entry_trigger_offset_ticks": self.entry_trigger_offset_ticks,
            "entry_trigger_stale_threshold_s": self.entry_trigger_stale_threshold_s,
            "tp1_allocation_pct_of_acquired": self.tp1_allocation_pct_of_acquired,
            "tp2_allocation_pct_of_remaining": self.tp2_allocation_pct_of_remaining,
            "stop_loss_max_retries": self.stop_loss_max_retries,
            "stop_loss_trigger_offset_ticks": self.stop_loss_trigger_offset_ticks,
            "monitor_poll_interval_s": self.monitor_poll_interval_s,
            "monitor_max_retries": self.monitor_max_retries,
            "monitor_backoff_base_s": self.monitor_backoff_base_s,
        }


@dataclass
class ControlConfig:
    """Telegram bot settings for one owner and one developer operator.

    The owner and developer are separate, explicitly configured numeric
    Telegram user IDs. Usernames are never used for authorization. Secrets are
    supplied through the process environment (or the private ``.env``); they
    are never written to config.toml, logs, or the database in plaintext.
    """

    enabled: bool = False
    bot_token: str | None = None
    owner_user_id: int | None = None
    developer_user_id: int | None = None
    secret_key: str | None = None  # encrypts MEXC credentials at rest
    api_base: str = "https://api.telegram.org"
    approval_ttl_s: int = 300
    poll_timeout_s: int = 25

    @property
    def configured(self) -> bool:
        return bool(self.bot_token and self.owner_user_id)

    @property
    def management_configured(self) -> bool:
        """The two-operator management bot needs both explicit numeric IDs."""

        return bool(self.bot_token and self.owner_user_id and self.developer_user_id)


@dataclass
class AppConfig:
    api_id: int | None = None
    api_hash: str | None = None

    data_dir: Path = PROJECT_ROOT / "data"
    db_path: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "ingest.sqlite3")
    session_path: Path = field(default_factory=lambda: PROJECT_ROOT / "config" / "tg_session")

    backfill_scope: str = "full"  # "full" or "since"
    backfill_since: str | None = None  # ISO date, used when scope == "since"
    backfill_batch_size: int = 200
    backfill_commit_every: int = 500

    recovery_sweep_size: int = 500
    recovery_sweep_interval_s: int = 300
    reconnect_backoff_max_s: int = 300

    processing_poll_interval_s: int = 2
    processing_max_attempts: int = 8

    download_media: bool = False
    media_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "media")

    mexc: MexcConfig = field(default_factory=MexcConfig)
    trading: TradingConfig = field(default_factory=TradingConfig)
    control: ControlConfig = field(default_factory=ControlConfig)

    log_level: str = "INFO"

    def ensure_dirs(self) -> None:
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.media_dir).mkdir(parents=True, exist_ok=True)
        self.session_path.parent.mkdir(parents=True, exist_ok=True)


def _read_kv_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if not line.startswith("export "):
                key, sep, val = line.partition("=")
                if not sep:
                    continue
            else:
                line = line[len("export ") :]
                key, sep, val = line.partition("=")
                if not sep:
                    continue
            key = key.strip()
            val = val.strip().strip("'\"")
            if key:
                values[key] = val
    except OSError as exc:
        raise ConfigError(f"Cannot read {path.name}: {exc.__class__.__name__}") from exc
    return values


def _read_toml(path: Path) -> dict[str, object]:
    if not path.is_file():
        return {}
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {path.name}.") from exc
    except OSError as exc:
        raise ConfigError(f"Cannot read {path.name}: {exc.__class__.__name__}") from exc


def load_config(
    root: Path = PROJECT_ROOT, environ: dict[str, str] | None = None, require_credentials: bool = True
) -> AppConfig:
    """Build a validated configuration from toml + .env + environment."""

    env = dict(os.environ if environ is None else environ)
    kv = _read_kv_file(root / ".env")
    for key, val in kv.items():
        env.setdefault(key, val)

    toml = _read_toml(root / "config" / "config.toml")

    legacy_proxy = [name for name in LEGACY_PROXY_ENV if env.get(name)]
    if legacy_proxy:
        raise ConfigError(
            "Legacy Telegram proxy configuration is no longer supported: "
            + ", ".join(legacy_proxy)
            + ". Remove these variables; Telegram now uses direct networking."
        )
    if "proxy" in toml:
        raise ConfigError(
            "The [proxy] section is no longer supported; Telegram uses direct networking. "
            "Remove it from config/config.toml."
        )

    def _get(mapping: dict[str, object], key: str) -> object | None:
        val = mapping.get(key)
        return val

    cfg = AppConfig()

    api_id_env = env.get(API_ID_ENV)
    api_id_toml = _get(toml, "api_id")
    if api_id_env:
        try:
            cfg.api_id = int(str(api_id_env).strip())
        except ValueError as exc:
            raise ConfigError("TELEGRAM_API_ID must be an integer.") from exc
    elif isinstance(api_id_toml, (int, str)):
        try:
            cfg.api_id = int(api_id_toml)
        except ValueError as exc:
            raise ConfigError("config: api_id must be an integer.") from exc

    api_hash_env = env.get(API_HASH_ENV)
    api_hash_toml = _get(toml, "api_hash")
    cfg.api_hash = (str(api_hash_env).strip() if api_hash_env else None) or (
        str(api_hash_toml).strip() if isinstance(api_hash_toml, str) and api_hash_toml else None
    )

    data_dir = _get(toml, "data_dir")
    if isinstance(data_dir, str) and data_dir:
        p = Path(data_dir).expanduser()
        cfg.data_dir = p if p.is_absolute() else (root / p)
    db_path = _get(toml, "db_path")
    if cfg.data_dir != PROJECT_ROOT / "data":
        cfg.db_path = cfg.data_dir / "ingest.sqlite3"
    if isinstance(db_path, str) and db_path:
        p = Path(db_path).expanduser()
        if cfg.data_dir != PROJECT_ROOT / "data":
            cfg.db_path = p if p.is_absolute() else (cfg.data_dir / p)
        else:
            cfg.db_path = p if p.is_absolute() else (root / p)
    # An explicit environment override makes it possible to run a second,
    # separate Telegram session (for a different account) without touching the
    # default/employer session file. Empty values keep the TOML/default path.
    session_env = env.get(SESSION_PATH_ENV)
    if session_env and session_env.strip():
        cfg.session_path = Path(session_env.strip()).expanduser()
    else:
        session = _get(toml, "session_path")
        if isinstance(session, str) and session:
            cfg.session_path = Path(session).expanduser()

    scope = _get(toml, "backfill_scope")
    if isinstance(scope, str) and scope:
        if scope not in ("full", "since"):
            raise ConfigError("backfill_scope must be 'full' or 'since'.")
        cfg.backfill_scope = scope
    since = _get(toml, "backfill_since")
    if isinstance(since, str) and since:
        cfg.backfill_since = since
    raw_opts = toml.get("options", {})
    opts: dict[str, object] = dict(raw_opts) if isinstance(raw_opts, dict) else {}
    sweep = opts.get("recovery_sweep_size")
    if isinstance(sweep, int) and sweep > 0:
        cfg.recovery_sweep_size = sweep
    poll = opts.get("processing_poll_interval_s")
    if isinstance(poll, (int, float)) and poll > 0:
        cfg.processing_poll_interval_s = int(poll)
    media_flag = opts.get("download_media")
    if isinstance(media_flag, bool):
        cfg.download_media = media_flag
    lvl = _get(toml, "log_level")
    if isinstance(lvl, str) and lvl.upper() in ("DEBUG", "INFO", "WARNING", "ERROR"):
        cfg.log_level = lvl.upper()

    _validate(cfg, require_credentials=require_credentials)
    _load_mexc_and_trading(cfg, toml, env)
    _load_control(cfg, toml, env)
    return cfg


def _optional_env(env: dict[str, str], toml: dict[str, object], env_key: str, cfg_key: str) -> str | None:
    val = env.get(env_key)
    if not val:
        toml_val = toml.get(cfg_key)
        val = str(toml_val) if toml_val not in (None, "") else None
    return val or None


def _load_mexc_and_trading(cfg: AppConfig, toml: dict[str, object], env: dict[str, str]) -> None:
    m = cfg.mexc
    key = _optional_env(env, toml, MEXC_API_KEY_ENV, "mexc_api_key")
    secret = _optional_env(env, toml, MEXC_API_SECRET_ENV, "mexc_api_secret")
    m.api_key = key if key and key.strip() else None
    m.api_secret = secret if secret and secret.strip() else None

    base = _optional_env(env, toml, MEXC_BASE_URL_ENV, "mexc_base_url")
    if base:
        if not base.startswith("https://"):
            raise ConfigError("mexc_base_url must be an https:// URL.")
        m.base_url = base.rstrip("/")

    # ---- MEXC CLI execution backend (live writes only) ----
    cli_toml = toml.get("mexc_cli", {})
    cli_raw: dict[str, object] = dict(cli_toml) if isinstance(cli_toml, dict) else {}

    cli_enabled = cli_raw.get("enabled")
    if isinstance(cli_enabled, bool):
        m.cli_enabled = cli_enabled

    cli_binary = _optional_env(env, cli_raw, "MEXC_CLI_BINARY", "binary")
    if cli_binary and cli_binary.strip():
        m.cli_binary = cli_binary.strip()

    cli_profile = _optional_env(env, cli_raw, "MEXC_CLI_PROFILE", "profile")
    if cli_profile and cli_profile.strip():
        m.cli_profile = cli_profile.strip()

    cli_timeout = cli_raw.get("timeout_s")
    if cli_timeout not in (None, ""):
        if (
            not isinstance(cli_timeout, (int, float))
            or isinstance(cli_timeout, bool)
            or not math.isfinite(float(cli_timeout))
            or float(cli_timeout) <= 0
            or float(cli_timeout) > 60
        ):
            raise ConfigError("mexc_cli.timeout_s must be a finite number in (0, 60].")
        m.cli_timeout_s = float(cli_timeout)

    cli_min_version = cli_raw.get("min_version")
    if cli_min_version not in (None, ""):
        if not isinstance(cli_min_version, str) or not cli_min_version.strip():
            raise ConfigError("mexc_cli.min_version must be a version string.")
        parts = str(cli_min_version).split(".")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            raise ConfigError("mexc_cli.min_version must be a dotted version (e.g. 0.0.22).")
        m.cli_min_version = str(cli_min_version)

    cli_confirm_present = "confirm_noninteractive_writes" in cli_raw
    cli_confirm = cli_raw.get("confirm_noninteractive_writes")
    if cli_confirm is not None and not isinstance(cli_confirm, bool):
        raise ConfigError(
            "mexc_cli.confirm_noninteractive_writes must be true or false."
        )
    if isinstance(cli_confirm, bool):
        m.cli_confirm_noninteractive_writes = cli_confirm

    trading_toml = toml.get("trading", {})
    tr_raw: dict[str, object] = dict(trading_toml) if isinstance(trading_toml, dict) else {}
    env_mode = env.get("TRADING_MODE")
    if env_mode:
        tr_raw["mode"] = env_mode

    t = cfg.trading
    mode = tr_raw.get("mode")
    if isinstance(mode, str) and mode:
        mode = mode.strip().lower()
        if mode not in ("sim", "testonly", "live"):
            raise ConfigError("trading.mode must be 'sim', 'testonly' or 'live'.")
        t.mode = mode
    token = _optional_env(env, tr_raw, "TRADING_LIVE_TOKEN", "live_enabled_token")
    if token:
        t.live_enabled_token = token

    if t.mode == "live" and t.live_enabled_token != "I_UNDERSTAND_REAL_MONEY_AND_RISKS":
        raise ConfigError(
            "trading.mode = 'live' refuses to start without live_enabled_token = "
            "'I_UNDERSTAND_REAL_MONEY_AND_RISKS' in [trading] config.toml (fail closed)."
        )
    if t.mode == "live" and not m.cli_enabled:
        raise ConfigError(
            "trading.mode = 'live' requires [mexc_cli] enabled = true in config.toml "
            "(the MEXC CLI backend is required for live Spot writes)."
        )
    if t.mode == "live" and not cli_confirm_present:
        # A service cannot answer the CLI's terminal prompt.  The explicit live
        # token above is the owner's authorization; without this default every
        # live signal would be rejected locally after validation.  An explicit
        # false is still honored as a fail-closed operator override.
        m.cli_confirm_noninteractive_writes = True

    positive_specs = (
        ("max_quote_per_order", float),
        ("max_open_orders", int),
        ("daily_max_orders", int),
        ("max_signal_age_s", float),
        ("max_price_deviation_pct", float),
    )
    for name, cast in positive_specs:
        val = tr_raw.get(name)
        if val is None or val == "":
            continue
        if (
            not isinstance(val, (int, float))
            or isinstance(val, bool)
            or not math.isfinite(float(val))
            or float(val) <= 0
        ):
            raise ConfigError(f"trading.{name} must be a positive finite number.")
        if cast is int and not float(val).is_integer():
            raise ConfigError(f"trading.{name} must be a whole number.")
        setattr(t, name, cast(val))

    symbols = tr_raw.get("allowed_symbols")
    if isinstance(symbols, list):
        bad = [s for s in symbols if not isinstance(s, str)]
        if bad:
            raise ConfigError("trading.allowed_symbols entries must be strings.")
        t.allowed_symbols = [s.strip().upper() for s in symbols if str(s).strip()]
        if t.allowed_symbols:
            t.allowlist_mode = "explicit_allowlist"

    require_sl = tr_raw.get("require_stop_loss")
    if isinstance(require_sl, bool):
        t.require_stop_loss = require_sl

    policy = tr_raw.get("order_type_policy")
    if isinstance(policy, str) and policy:
        policy = policy.strip().lower()
        if policy not in ("limit", "market"):
            raise ConfigError("trading.order_type_policy must be 'limit' or 'market'.")
        t.order_type_policy = policy

    reserve = tr_raw.get("remaining_balance_reserve_pct")
    if reserve not in (None, ""):
        if (
            not isinstance(reserve, (int, float))
            or isinstance(reserve, bool)
            or not math.isfinite(float(reserve))
            or not 0 <= float(reserve) < 100
        ):
            raise ConfigError("trading.remaining_balance_reserve_pct must be between 0 and 100.")
        t.remaining_balance_reserve_pct = float(reserve)

    fee_buffer = tr_raw.get("balance_fee_buffer_pct")
    if fee_buffer not in (None, ""):
        if (
            not isinstance(fee_buffer, (int, float))
            or isinstance(fee_buffer, bool)
            or not math.isfinite(float(fee_buffer))
            or not 0 <= float(fee_buffer) <= 5
        ):
            raise ConfigError("trading.balance_fee_buffer_pct must be between 0 and 5.")
        t.balance_fee_buffer_pct = float(fee_buffer)

    balance_age = tr_raw.get("balance_max_age_s")
    if balance_age not in (None, ""):
        if (
            not isinstance(balance_age, (int, float))
            or isinstance(balance_age, bool)
            or not math.isfinite(float(balance_age))
            or not 0 < float(balance_age) <= 300
        ):
            raise ConfigError("trading.balance_max_age_s must be a finite number in (0, 300].")
        t.balance_max_age_s = float(balance_age)

    recon = tr_raw.get("recon_interval_s")
    if recon not in (None, ""):
        if (
            not isinstance(recon, (int, float))
            or isinstance(recon, bool)
            or not math.isfinite(float(recon))
            or float(recon) < 5
        ):
            raise ConfigError("trading.recon_interval_s must be a finite number >= 5 seconds.")
        t.recon_interval_s = float(recon)

    estop = tr_raw.get("emergency_stop")
    if isinstance(estop, bool):
        t.emergency_stop = estop

    # ---- order_quote_amount (fixed per-order quote, owner-approved) ----
    # Precedence: env TRADING_ORDER_QUOTE_AMOUNT > TOML order_quote_amount
    #             > TOML legacy max_quote_per_order > built-in default 3.0.
    order_amount_env = env.get("TRADING_ORDER_QUOTE_AMOUNT")
    order_amount_val = None
    order_amount_source = None
    if order_amount_env and str(order_amount_env).strip():
        try:
            order_amount_val = float(order_amount_env)
        except (TypeError, ValueError) as exc:
            raise ConfigError("TRADING_ORDER_QUOTE_AMOUNT must be a finite number.") from exc
        order_amount_source = "env"
    else:
        toml_amount = tr_raw.get("order_quote_amount")
        if toml_amount not in (None, ""):
            try:
                order_amount_val = float(str(toml_amount))
            except (TypeError, ValueError) as exc:
                raise ConfigError("trading.order_quote_amount must be a finite number.") from exc
            order_amount_source = "config"
        else:
            legacy = tr_raw.get("max_quote_per_order")
            if legacy not in (None, ""):
                try:
                    order_amount_val = float(str(legacy))
                except (TypeError, ValueError) as exc:
                    raise ConfigError("trading.max_quote_per_order must be a finite number.") from exc
                order_amount_source = "legacy max_quote_per_order"

    if order_amount_val is not None:
        if (
            not isinstance(order_amount_val, (int, float))
            or isinstance(order_amount_val, bool)
            or not math.isfinite(order_amount_val)
        ):
            raise ConfigError("trading.order_quote_amount must be a finite number.")
        if order_amount_val <= 0:
            raise ConfigError("trading.order_quote_amount must be positive.")
        t.order_quote_amount = float(order_amount_val)
        t.order_quote_amount_source = order_amount_source or "default (3 USDT)"
        t.amount_configured = True
    else:
        t.order_quote_amount = 3.0
        t.order_quote_amount_source = "default (3 USDT)"
        t.amount_configured = True

    # ---- aggregate_budget_quote (cumulative budget reference) ----
    budget_val = tr_raw.get("aggregate_budget_quote")
    if budget_val not in (None, ""):
        if (
            not isinstance(budget_val, (int, float))
            or isinstance(budget_val, bool)
            or not math.isfinite(float(budget_val))
            or float(budget_val) <= 0
        ):
            raise ConfigError("trading.aggregate_budget_quote must be a positive finite number.")
        t.aggregate_budget_quote = float(budget_val)

    # ---- conditional entry trigger offset ----
    offset_val = tr_raw.get("entry_trigger_offset_ticks")
    if offset_val not in (None, ""):
        if (
            not isinstance(offset_val, int)
            or isinstance(offset_val, bool)
            or offset_val < 0
        ):
            raise ConfigError("trading.entry_trigger_offset_ticks must be a whole number >= 0.")
        t.entry_trigger_offset_ticks = int(offset_val)

    stale_val = tr_raw.get("entry_trigger_stale_threshold_s")
    if stale_val not in (None, ""):
        if (
            not isinstance(stale_val, (int, float))
            or isinstance(stale_val, bool)
            or not math.isfinite(float(stale_val))
            or float(stale_val) <= 0
        ):
            raise ConfigError("trading.entry_trigger_stale_threshold_s must be a finite number > 0.")
        t.entry_trigger_stale_threshold_s = float(stale_val)

    tp1_pct_val = tr_raw.get("tp1_allocation_pct_of_acquired")
    if tp1_pct_val not in (None, ""):
        if (
            not isinstance(tp1_pct_val, (int, float))
            or isinstance(tp1_pct_val, bool)
            or not math.isfinite(float(tp1_pct_val))
            or not 0 < float(tp1_pct_val) <= 100
        ):
            raise ConfigError("trading.tp1_allocation_pct_of_acquired must be between 0 and 100.")
        t.tp1_allocation_pct_of_acquired = float(tp1_pct_val)

    tp2_pct_val = tr_raw.get("tp2_allocation_pct_of_remaining")
    if tp2_pct_val not in (None, ""):
        if (
            not isinstance(tp2_pct_val, (int, float))
            or isinstance(tp2_pct_val, bool)
            or not math.isfinite(float(tp2_pct_val))
            or not 0 < float(tp2_pct_val) <= 100
        ):
            raise ConfigError("trading.tp2_allocation_pct_of_remaining must be between 0 and 100.")
        t.tp2_allocation_pct_of_remaining = float(tp2_pct_val)

    sl_retries_val = tr_raw.get("stop_loss_max_retries")
    if sl_retries_val not in (None, ""):
        if (
            not isinstance(sl_retries_val, int)
            or isinstance(sl_retries_val, bool)
            or sl_retries_val < 1
        ):
            raise ConfigError("trading.stop_loss_max_retries must be a whole number >= 1.")
        t.stop_loss_max_retries = int(sl_retries_val)

    sl_trigger_offset_val = tr_raw.get("stop_loss_trigger_offset_ticks")
    if sl_trigger_offset_val not in (None, ""):
        if (
            not isinstance(sl_trigger_offset_val, int)
            or isinstance(sl_trigger_offset_val, bool)
            or sl_trigger_offset_val < 0
        ):
            raise ConfigError(
                "trading.stop_loss_trigger_offset_ticks must be a whole number >= 0."
            )
        t.stop_loss_trigger_offset_ticks = int(sl_trigger_offset_val)

    poll_val = tr_raw.get("monitor_poll_interval_s")
    if poll_val not in (None, ""):
        if (
            not isinstance(poll_val, (int, float))
            or isinstance(poll_val, bool)
            or not math.isfinite(float(poll_val))
            or float(poll_val) <= 0
        ):
            raise ConfigError("trading.monitor_poll_interval_s must be a finite number > 0.")
        t.monitor_poll_interval_s = float(poll_val)

    mr_val = tr_raw.get("monitor_max_retries")
    if mr_val not in (None, ""):
        if not isinstance(mr_val, int) or isinstance(mr_val, bool) or mr_val < 1:
            raise ConfigError("trading.monitor_max_retries must be a whole number >= 1.")
        t.monitor_max_retries = int(mr_val)

    bb_val = tr_raw.get("monitor_backoff_base_s")
    if bb_val not in (None, ""):
        if (
            not isinstance(bb_val, (int, float))
            or isinstance(bb_val, bool)
            or not math.isfinite(float(bb_val))
            or float(bb_val) <= 0
        ):
            raise ConfigError("trading.monitor_backoff_base_s must be a finite number > 0.")
        t.monitor_backoff_base_s = float(bb_val)


def _load_control(cfg: AppConfig, toml: dict[str, object], env: dict[str, str]) -> None:
    c = cfg.control
    raw = toml.get("control", {})
    section: dict[str, object] = dict(raw) if isinstance(raw, dict) else {}

    token = env.get("CONTROL_BOT_TOKEN") or section.get("bot_token")
    owner = env.get("CONTROL_OWNER_USER_ID") or section.get("owner_user_id")
    developer = env.get("CONTROL_DEVELOPER_USER_ID") or section.get("developer_user_id")
    secret = env.get("CONTROL_SECRET_KEY") or section.get("secret_key")
    enabled = env.get("CONTROL_BOT_ENABLED")
    base = env.get("CONTROL_API_BASE") or section.get("api_base")

    c.bot_token = str(token).strip() if token else None
    c.secret_key = str(secret) if secret else None
    if c.bot_token and ":" not in c.bot_token:
        raise ConfigError(
            "CONTROL_BOT_TOKEN is malformed; expected the token issued by @BotFather (number:value)."
        )
    if c.secret_key and len(c.secret_key.strip().encode("utf-8")) < 16:
        raise ConfigError("CONTROL_SECRET_KEY must contain at least 16 bytes of entropy.")
    if owner not in (None, ""):
        try:
            c.owner_user_id = int(str(owner).strip())
        except ValueError as exc:
            raise ConfigError("CONTROL_OWNER_USER_ID must be a numeric Telegram user id.") from exc
    if developer not in (None, ""):
        try:
            c.developer_user_id = int(str(developer).strip())
        except ValueError as exc:
            raise ConfigError(
                "CONTROL_DEVELOPER_USER_ID must be a numeric Telegram user id."
            ) from exc
    if isinstance(enabled, str) and enabled:
        c.enabled = enabled.strip().lower() in ("1", "true", "yes", "on")
    elif c.configured:
        c.enabled = True
    if isinstance(base, str) and base:
        if not base.startswith("https://"):
            raise ConfigError("CONTROL_API_BASE must use https://")
        c.api_base = base.rstrip("/")
    ttl = section.get("approval_ttl_s", c.approval_ttl_s)
    try:
        ttl_i = int(str(ttl))
    except (TypeError, ValueError) as exc:
        raise ConfigError("control.approval_ttl_s must be an integer.") from exc
    if not 30 <= ttl_i <= 86400:
        raise ConfigError("control.approval_ttl_s must be between 30 and 86400 seconds.")
    c.approval_ttl_s = ttl_i
    if c.owner_user_id is not None and c.owner_user_id <= 0:
        raise ConfigError("CONTROL_OWNER_USER_ID must be a positive numeric Telegram user id.")
    if c.developer_user_id is not None and c.developer_user_id <= 0:
        raise ConfigError(
            "CONTROL_DEVELOPER_USER_ID must be a positive numeric Telegram user id."
        )
    if (
        c.owner_user_id is not None
        and c.developer_user_id is not None
        and c.owner_user_id == c.developer_user_id
    ):
        raise ConfigError(
            "CONTROL_OWNER_USER_ID and CONTROL_DEVELOPER_USER_ID must be different; "
            "roles must remain distinguishable in authorization and audit records."
        )
    if c.enabled and not c.configured:
        raise ConfigError(
            "control bot is enabled but CONTROL_BOT_TOKEN and CONTROL_OWNER_USER_ID are both required."
        )


def _validate(cfg: AppConfig, require_credentials: bool = True) -> None:
    if require_credentials and (cfg.api_id is None or cfg.api_hash is None):
        raise ConfigError(
            "Telegram API credentials are missing. "
            f"Set {API_ID_ENV} and {API_HASH_ENV} in a private .env file "
            "(see docs/OPERATIONS.md, section 'Obtain Telegram API credentials')."
        )
    if cfg.backfill_scope == "since" and not cfg.backfill_since:
        raise ConfigError(
            "backfill_scope = 'since' requires backfill_since = 'YYYY-MM-DD' in config.toml."
        )
