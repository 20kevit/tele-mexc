"""Configuration loading and validation.

Sources (highest precedence first):
  1. process environment variables
  2. ``config/config.toml`` (private, gitignored)
  3. ``.env`` at the project root (gitignored)
  4. built-in defaults

Secrets (Telegram API credentials, proxy auth) are supplied via environment or
private files and must never be logged.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

API_ID_ENV = "TELEGRAM_API_ID"
API_HASH_ENV = "TELEGRAM_API_HASH"

PROXY_HOST_ENV = "TELEGRAM_PROXY_HOST"
PROXY_PORT_ENV = "TELEGRAM_PROXY_PORT"
PROXY_TYPE_ENV = "TELEGRAM_PROXY_TYPE"
PROXY_USER_ENV = "TELEGRAM_PROXY_USERNAME"
PROXY_PASSWORD_ENV = "TELEGRAM_PROXY_PASSWORD"


class ConfigError(Exception):
    """Configuration is missing or invalid; message is safe to display."""


@dataclass
class ProxyConfig:
    enabled: bool = True
    type: str = "http"  # "http" (CONNECT) or "socks5"
    host: str = "127.0.0.1"
    port: int = 1081
    username: str | None = None
    password: str | None = None

    def as_dict_public(self) -> dict[str, object]:
        return {"enabled": self.enabled, "type": self.type, "host": self.host, "port": self.port}


@dataclass
class AppConfig:
    api_id: int | None = None
    api_hash: str | None = None

    data_dir: Path = PROJECT_ROOT / "data"
    db_path: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "ingest.sqlite3")
    session_path: Path = field(default_factory=lambda: PROJECT_ROOT / "config" / "tg_session")

    proxy: ProxyConfig = field(default_factory=ProxyConfig)

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
    session = _get(toml, "session_path")
    if isinstance(session, str) and session:
        cfg.session_path = Path(session).expanduser()

    proxy_cfg: dict[str, object] = {}
    raw_proxy = toml.get("proxy")
    if isinstance(raw_proxy, dict):
        proxy_cfg = raw_proxy

    host_env = env.get(PROXY_HOST_ENV)
    if isinstance(host_env, str) and host_env:
        proxy_cfg["host"] = host_env
    port_env = env.get(PROXY_PORT_ENV)
    if port_env:
        proxy_cfg["port"] = port_env
    type_env = env.get(PROXY_TYPE_ENV)
    if isinstance(type_env, str) and type_env:
        proxy_cfg["type"] = type_env
    user_env = env.get(PROXY_USER_ENV)
    if isinstance(user_env, str) and user_env:
        proxy_cfg["username"] = user_env
    pass_env = env.get(PROXY_PASSWORD_ENV)
    if isinstance(pass_env, str) and pass_env:
        proxy_cfg["password"] = pass_env

    if "enabled" in proxy_cfg and isinstance(proxy_cfg["enabled"], bool):
        cfg.proxy.enabled = proxy_cfg["enabled"]
    ptype = proxy_cfg.get("type")
    if isinstance(ptype, str):
        ptype = ptype.strip().lower()
        if ptype not in ("http", "socks5"):
            raise ConfigError("proxy.type must be 'http' or 'socks5' in config or env.")
        cfg.proxy.type = ptype
    host = proxy_cfg.get("host")
    if isinstance(host, str) and host:
        cfg.proxy.host = host
    port = proxy_cfg.get("port")
    if isinstance(port, (int, str)) and not isinstance(port, bool):
        try:
            cfg.proxy.port = int(port)
        except ValueError as exc:
            raise ConfigError("proxy.port must be an integer.") from exc
    username = proxy_cfg.get("username")
    if isinstance(username, str) and username:
        cfg.proxy.username = username
    password = proxy_cfg.get("password")
    if isinstance(password, str) and password:
        cfg.proxy.password = password

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
    return cfg


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
