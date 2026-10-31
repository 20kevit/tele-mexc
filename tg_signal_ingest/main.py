"""``tg-ingest`` command line interface.

One-time setup (account owner):
    tg-ingest config init
    tg-ingest auth login                       # interactive
    tg-ingest channels list
    tg-ingest channel select <id-or-title>

Recurring operation:
    tg-ingest serve                            # long-running ingestion
    tg-ingest status                           # offline health/sync view
    tg-ingest backfill run|status              # history import control
    tg-ingest diagnostics network|bot|telegram # connectivity checks
    tg-ingest queue ...                        # downstream processing queue
    tg-ingest auth logout                      # revoke + delete session
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sqlite3
import sys
from dataclasses import replace
from pathlib import Path

from .backfill import BackfillError, run_backfill
from .config import PROJECT_ROOT, ConfigError, load_config
from .connectivity import ConnectivityError, run_direct_diagnostics
from .control.bot_api import BotApiClient, BotApiError
from .db import Database
from .mexc.errors import MexcAuthError, MexcConfigError, MexcNetworkError, MexcPermissionError
from .repository import Repository
from .session_guard import SessionBusyError, SessionGuard
from .telethon import (
    AuthRequired,
    build_client,
    connect_and_verify,
    interactive_login,
    logout_and_delete_session,
    secure_session_files,
)
from .trading.runtime import build_mexc_client
from .utils import configure_logging, marked_channel_id

ENV_TEMPLATE = """\
# Telegram API credentials (https://my.telegram.org -> API development tools).
# Keep this file private; it must never be committed.
TELEGRAM_API_ID=
TELEGRAM_API_HASH=

# Telegram traffic uses direct networking; no proxy settings are required.

# Optional single-owner Telegram control bot.
# CONTROL_BOT_TOKEN=
# CONTROL_OWNER_USER_ID=
# CONTROL_SECRET_KEY=   # generate: python3 -c 'import secrets; print(secrets.token_urlsafe(48))'
# CONTROL_BOT_ENABLED=true
"""

CONFIG_TEMPLATE = """\
# tg-signal-ingest runtime configuration (private; never commit).
# backend = "sqlite"  # postgres support can be added later

# Optional storage overrides (paths may be relative to this file's directory):
# db_path = "data/ingest.sqlite3"
# session_path = "config/tg_session"

backfill_scope = "{backfill_scope}"  # "full" or "since"
backfill_since = "{backfill_since}"  # used only when scope = "since" (YYYY-MM-DD)

[options]
# recovery_sweep_size = 500
# processing_poll_interval_s = 2
# download_media = false

# MEXC Spot automation is simulation-only until explicitly configured.
# Credentials belong in .env (MEXC_API_KEY / MEXC_API_SECRET), never here.
[trading]
mode = "sim"          # "sim" | "testonly" | "live" (live also needs live_enabled_token)
# allowed_symbols = ["BTCUSDT"]   # EMPTY allowlist rejects every signal (fail closed)
# max_quote_per_order = 10
# daily_max_orders = 2
# max_open_orders = 2
# max_signal_age_s = 900
# max_price_deviation_pct = 1
# require_stop_loss = true
# order_type_policy = "limit"
# recon_interval_s = 30
# emergency_stop = false
# live_enabled_token = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"  # only after owner approval

[control]
# Optional owner control-bot timing (token/keys belong in .env, never here).
# approval_ttl_s = 300
"""

def _load_or_exit(require_credentials: bool = True):
    try:
        return load_config(require_credentials=require_credentials)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        raise SystemExit(2)  # noqa: B904 - deliberate non-traceable exit at CLI boundary


def _new_repo(cfg):
    db = Database(cfg.db_path)
    db.migrate()
    repo = Repository(db, processing_max_attempts=cfg.processing_max_attempts)
    return db, repo


def _report_mexc_verification(cfg, *, client=None) -> tuple[int, list[str]]:
    """Read-only MEXC API key verification.

    Classifies credential state without printing the key or secret and without
    placing any order. Uses ``GET /api/v3/apiKeyInfo`` only.

    Returns ``(exit_code, output_lines)``:

    * 0 — credentials valid
    * 1 — permission/auth failure
    * 2 — missing credentials (no api_key/api_secret configured)
    * 3 — network failure (could not reach MEXC)
    """
    import importlib

    lines: list[str] = []

    if client is None:
        if not cfg.mexc.api_key or not cfg.mexc.api_secret:
            lines.append("MEXC credentials are missing: set MEXC_API_KEY and MEXC_API_SECRET")
            lines.append("  api key: <not configured>")
            lines.append("  api secret: <not configured>")
            return 2, lines

        try:
            client = build_mexc_client(cfg)
        except ConfigError as exc:
            lines.append(f"MEXC credentials error: {exc}")
            return 2, lines

    _credentials_mod = importlib.import_module("tg_signal_ingest.mexc.credentials")
    validate_credentials = _credentials_mod.validate_credentials

    try:
        info = validate_credentials(client)
    except MexcPermissionError as exc:
        lines.append(f"MISSING PERMISSION: {exc}")
        lines.append("  The key lacks SPOT_ACCOUNT_READ and/or SPOT_DEAL_READ.")
        lines.append("  Never request withdrawal permissions.")
        return 1, lines
    except MexcAuthError as exc:
        lines.append(f"REJECTED BY MEXC: {exc}")
        lines.append("  The API key or secret was rejected. no order was attempted.")
        return 1, lines
    except MexcNetworkError as exc:
        lines.append(f"NETWORK FAILURE: {exc}")
        lines.append("  Could not reach MEXC. no order was attempted.")
        return 3, lines
    except MexcConfigError as exc:
        lines.append(f"MEXC configuration error: {exc}")
        return 1, lines

    lines.append("MEXC API key verification: VALID")
    lines.append("  read-only GET /api/v3/apiKeyInfo: OK")
    lines.append(f"  status: {info.status}")
    lines.append(f"  permissions: {', '.join(info.permissions)}")
    if info.permanent:
        lines.append("  expiry: permanent key")
    else:
        lines.append(f"  {info.remaining_validity} day(s) remaining")
    lines.append(f"  IP whitelist: {', '.join(info.ip_whitelist) or '(none)'}")
    lines.append(f"  note: {info.note or '(none)'}")
    lines.append("  no order placed.")
    return 0, lines


# ------------------------------------------------------------------ commands


def cmd_config_init(args) -> int:
    env_path = PROJECT_ROOT / ".env"
    cfg_path = PROJECT_ROOT / "config" / "config.toml"
    if not env_path.exists():
        env_path.write_text(ENV_TEMPLATE)
        env_path.chmod(0o600)
        print(f"created private {env_path.name}: add TELEGRAM_API_ID and TELEGRAM_API_HASH there")
    else:
        print(f"{env_path.name} already exists; credentials left untouched")
    if not cfg_path.exists():
        cfg_path.write_text(CONFIG_TEMPLATE.format(backfill_scope="full", backfill_since=""))
        cfg_path.chmod(0o600)
        print(f"created {cfg_path}: adjust backfill_scope / backfill_since if needed")
    else:
        print(f"{cfg_path} already exists; nothing changed")
    print("Next: fill .env, then run 'tg-ingest config check' and 'tg-ingest auth login'.")
    return 0


def _credential_fingerprint(cfg) -> str:
    """Safe, log-only description of the loaded credentials.

    Never exposes the api_hash; only its length and outer characters so the
    account owner can visually compare against my.telegram.org.
    """

    from .config import _read_kv_file  # local import: expose only what exists

    root = Path(__file__).resolve().parents[1]
    env_sources: list[str] = []
    hash_value: str | None = cfg.api_hash
    id_value: int | None = cfg.api_id

    # determine where each value came from (process env can silently win over .env)
    process_env_id = os.environ.get("TELEGRAM_API_ID")
    process_env_hash = os.environ.get("TELEGRAM_API_HASH")
    file_kv = _read_kv_file(root / ".env")
    file_id = file_kv.get("TELEGRAM_API_ID")

    if process_env_id:
        env_sources.append("api_id taken from PROCESS ENVIRONMENT (overrides .env value!)")
    if process_env_hash:
        env_sources.append("api_hash taken from PROCESS ENVIRONMENT (overrides .env value!)")
    if file_id and process_env_id and file_id != process_env_id:
        env_sources.append("WARNING: process env api_id DIFFERS from the value inside .env")

    parts = [f"api_id={'set' if id_value is not None else 'MISSING'}"]
    if hash_value:
        parts.append(f"api_hash=set (len={len(hash_value)})")
    else:
        parts.append("api_hash=MISSING")
    if hash_value and len(hash_value) != 32:
        parts.append(
            "WARNING: api_hash length is not 32 (my.telegram.org hashes are exactly 32 hex chars)"
        )
    if env_sources:
        parts.extend(env_sources)
    return "; ".join(parts)


def cmd_config_check(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)

    print("configuration OK")
    print(f"  credentials: {_credential_fingerprint(cfg)}")
    print("  transport: direct TCP/TLS (no proxy configuration)")
    print(f"  database: {cfg.db_path}")
    print(f"  session:  {cfg.session_path}.session")
    print(f"  backfill: scope={cfg.backfill_scope} since={cfg.backfill_since}")
    symbols = ", ".join(cfg.trading.allowed_symbols) if cfg.trading.allowed_symbols else "not enforced"
    mexc_state = "configured" if cfg.mexc.api_key and cfg.mexc.api_secret else "missing"
    print(f"  trading: mode={cfg.trading.mode} symbol allowlist={symbols} MEXC credentials={mexc_state}")
    cli_write_state = (
        "armed (--yes)" if cfg.mexc.cli_confirm_noninteractive_writes else "fail-closed (no --yes)"
    )
    print(f"  MEXC CLI: enabled={cfg.mexc.cli_enabled} live writes={cli_write_state}")
    print(f"  order amount: {cfg.trading.order_quote_amount:g} USDT ({cfg.trading.order_quote_amount_source})")
    return 0


async def cmd_auth_login(_args) -> int:
    cfg = _load_or_exit()
    print(
        "Enter all secrets only in this terminal. Do not send a login code to a "
        "Telegram chat or bot; Telegram invalidates codes shared there."
    )
    try:
        async with SessionGuard(cfg).use():
            message = await interactive_login(cfg)
    except SessionBusyError:
        print(
            "Login not completed: the Telegram session is in use by tg-manage or "
            "another process. Wait for the current operation to finish and retry; "
            "do not stop or restart the service just for this lock.",
            file=sys.stderr,
        )
        return 4
    except AuthRequired as exc:
        print(f"Login not completed: {exc}", file=sys.stderr)
        return 3
    print(message)
    return 0


async def cmd_auth_check(_args) -> int:
    cfg = _load_or_exit()
    try:
        async with SessionGuard(cfg).use():
            client = await connect_and_verify(cfg)
            try:
                me = await client.get_me()
                name = (getattr(me, "first_name", None) or "") + (
                    f" @{me.username}" if getattr(me, "username", None) else ""
                )
            finally:
                await client.disconnect()
    except SessionBusyError:
        print("Session status: busy (another process is using the session)")
        return 4
    except AuthRequired as exc:
        print(f"Session status: {exc}")
        return 3
    print(f"Session status: valid (account: {name or 'unknown'})")
    return 0


async def cmd_auth_check_channel(_args) -> int:
    """Verify the current session can access the exact management-selected channel.

    This is read-only: it opens the selected channel by its stored access hash,
    prints no channel id/title, and never fetches message history. Use the
    ``TG_SESSION_PATH`` environment variable to check a separate account
    session without touching the default/employer session.
    """

    from .management.store import ManagementStore
    from .telethon import resolve_session_file

    cfg = _load_or_exit()
    _, session_file = resolve_session_file(cfg)
    print(f"  session file: {'present' if session_file.is_file() else 'MISSING'}")
    if not Path(cfg.db_path).is_file():
        print("management database unavailable: not initialized", file=sys.stderr)
        return 1
    db = None
    try:
        try:
            db = Database(cfg.db_path, read_only=True)
            store = ManagementStore(db)
            owner = int(cfg.control.owner_user_id) if cfg.control.owner_user_id else None
            developer = (
                int(cfg.control.developer_user_id) if cfg.control.developer_user_id else None
            )
            selections: list[tuple[str, dict]] = []
            for role, operator_id in (("owner", owner), ("developer", developer)):
                if operator_id is None:
                    continue
                details = store.selected_channel_details(operator_id)
                if details.get("selected_channel_id"):
                    selections.append((role, details))
        except sqlite3.Error:
            print("management schema not initialized", file=sys.stderr)
            return 1
        if not selections:
            print("no management-selected channel found", file=sys.stderr)
            return 2
        channel_ids = {int(details["selected_channel_id"]) for _role, details in selections}
        if len(channel_ids) != 1:
            print(
                "owner and developer selections differ; refusing to choose a channel",
                file=sys.stderr,
            )
            return 2
        channel_id = channel_ids.pop()
        access_hash = next(
            (
                int(details["access_hash"])
                for _role, details in selections
                if details.get("access_hash")
            ),
            0,
        )
        if not access_hash:
            print("selected channel has no stored access hash", file=sys.stderr)
            return 2
        print(f"  management selections checked: {len(selections)}")

        try:
            async with SessionGuard(cfg).use():
                client = await connect_and_verify(cfg)
                try:
                    me = await client.get_me()
                    name = (
                        (getattr(me, "first_name", None) or "")
                        + " "
                        + (getattr(me, "last_name", None) or "")
                    ).strip()
                    username = getattr(me, "username", None)
                    account_id = int(getattr(me, "id", 0) or 0)
                    print(f"  account: {name[:1] + '***' if name else '(none)'}"
                          + (f" (@{username[:1]}***)" if username else ""))
                    print(
                        "  account matches: "
                        f"owner={bool(owner and account_id == owner)} "
                        f"developer={bool(developer and account_id == developer)}"
                    )
                    from telethon.tl.types import InputChannel

                    entity = await client.get_entity(InputChannel(channel_id, access_hash))
                    if getattr(entity, "megagroup", False):
                        channel_type = "megagroup"
                    elif getattr(entity, "broadcast", False):
                        channel_type = "broadcast"
                    else:
                        channel_type = "other"
                    print("  channel access: OK")
                    print(f"  channel type: {channel_type}")
                    print(f"  title present: {bool(getattr(entity, 'title', None))}")
                    print(f"  username present: {bool(getattr(entity, 'username', None))}")
                    print(
                        "  entity matches selection: "
                        f"{int(getattr(entity, 'id', 0)) == channel_id}"
                    )
                    return 0
                finally:
                    with contextlib.suppress(Exception):
                        await client.disconnect()
        except SessionBusyError:
            print("session status: busy (another process is using this session)", file=sys.stderr)
            return 4
        except AuthRequired as exc:
            print(f"session status: {exc}", file=sys.stderr)
            return 3
        except Exception as exc:  # noqa: BLE001 - classify access failures
            print(f"channel verification failed: {exc.__class__.__name__}", file=sys.stderr)
            return 4
    finally:
        if db is not None:
            db.close()


def _ingestion_process_running(cfg) -> str | None:
    """Describe a process that may be using the session, or None.

    Checks the deployed systemd unit and any live 'serve' process. Never
    inspects session contents; only process/service state.
    """

    import subprocess

    try:
        active = subprocess.run(
            ["systemctl", "is-active", "--quiet", "tg-ingest"],
            capture_output=True,
            timeout=10,
        )
        if active.returncode == 0:
            return "systemd unit 'tg-ingest' is active"
    except (OSError, subprocess.SubprocessError):
        pass  # systemd unavailable; fall through to the process scan
    try:
        hit = subprocess.run(
            ["pgrep", "-f", r"tg-ingest serve|tg_signal_ingest\.service"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if hit.returncode == 0 and hit.stdout.strip():
            return f"process(es) matching 'serve' are running (pid {hit.stdout.split()[0]})"
    except (OSError, subprocess.SubprocessError):
        pass
    return None


async def cmd_auth_logout(_args) -> int:
    cfg = _load_or_exit()
    offender = _ingestion_process_running(cfg)
    if offender:
        print(
            f"refusing logout: {offender} and may be using the session.\n"
            "Stop ingestion first, e.g. 'systemctl stop tg-ingest' "
            "(or stop the 'tg-ingest serve' process), then run logout again.",
            file=sys.stderr,
        )
        return 1
    try:
        async with SessionGuard(cfg).use():
            outcome = await logout_and_delete_session(cfg)
    except SessionBusyError:
        print(
            "refusing logout: the Telegram session is currently in use by tg-manage "
            "or another process. Wait for the current operation to finish and retry.",
            file=sys.stderr,
        )
        return 4
    print(outcome)
    if "revoked on Telegram servers" not in outcome:
        print(
            "note: the session key may still be valid on Telegram's side; "
            "it expires with 'Settings -> Devices -> Terminate session' if needed.",
            file=sys.stderr,
        )
    print("Verify with: tg-ingest auth check, then a different account can log in with: tg-ingest auth login")
    return 0


async def cmd_channels_list(_args) -> int:
    cfg = _load_or_exit()
    try:
        client = await connect_and_verify(cfg)
    except AuthRequired as exc:
        print(f"Session status: {exc}", file=sys.stderr)
        return 3
    db, repo = _new_repo(cfg)
    selected = repo.selected_channel()
    count = 0
    print(f"{'#':>3}  {'bare id':<15} {'marked id':<18} {'username':<22} title")
    async for dialog in client.iter_dialogs(limit=None):
        entity = dialog.entity
        if not getattr(entity, "is_channel", False) or getattr(entity, "is_user", False):
            continue
        role = "group" if getattr(entity, "megagroup", False) else "channel"
        bare = int(entity.id)
        marked = marked_channel_id(bare)
        username = getattr(entity, "username", None)
        user_cell = f"@{username}" if username else "-"
        tag = " <- selected" if selected and selected.channel_id == bare else ""
        print(f"{count:>3}  {bare:<15} {marked:<18} {user_cell:<22} {role}: {dialog.name}{tag}")
        repo.ensure_channel(bare, int(entity.access_hash), username, str(dialog.name))
        count += 1
    if count == 0:
        print("no visible channels; the account owner must join/mute the signal channel in Telegram first")
    await client.disconnect()
    if not selected:
        print("\nSelect one with: tg-ingest channel select <id or title>")
    return 0


async def cmd_channel_select(args) -> int:
    cfg = _load_or_exit()
    db, repo = _new_repo(cfg)
    target = str(args.target).strip()
    needle = target.lstrip("@#").lower()
    try:
        want_id = int(needle) if needle.isdigit() else None
    except ValueError:  # pragma: no cover
        want_id = None
    try:
        client = await connect_and_verify(cfg)
    except AuthRequired as exc:
        print(f"Session status: {exc}", file=sys.stderr)
        return 3
    candidates = []
    async for dialog in client.iter_dialogs(limit=None):
        entity = dialog.entity
        if getattr(entity, "is_user", False) or not getattr(entity, "is_channel", False):
            continue  # only channels/supergroups; private chats and users are never touched
        bare = int(entity.id)
        title = str(dialog.name)
        username = getattr(entity, "username", None)
        matched = (
            want_id is not None and (bare == want_id or marked_channel_id(bare) == want_id)
        ) or (username is not None and needle == username.lower()) or (
            not target.startswith("@") and needle != "" and needle in title.lower()
        )
        if matched:
            candidates.append((bare, entity, title, username))
        else:
            pass
    if len(candidates) == 0:
        print(f"no channel matches {target!r}; run 'tg-ingest channels list'", file=sys.stderr)
        await client.disconnect()
        return 1
    if len(candidates) > 1:
        print("ambiguous target:", file=sys.stderr)
        for bare, _e, title, u in candidates:
            print(f"  id {bare}  {u}  {title}", file=sys.stderr)
        await client.disconnect()
        return 1
    bare, entity, title, username = candidates[0]
    repo.ensure_channel(bare, int(entity.access_hash), username, title)
    repo.select_channel(bare)
    marked = marked_channel_id(bare)
    print(f"selected channel: id={bare} marked={marked} title={title!r} username={username}")
    print("history import will run on the first 'tg-ingest serve' or via 'tg-ingest backfill run'.")
    await client.disconnect()
    return 0


async def cmd_backfill_run(args) -> int:
    cfg = _load_or_exit()
    db, repo = _new_repo(cfg)
    try:
        client = await connect_and_verify(cfg)
    except AuthRequired as exc:
        print(f"Session status: {exc}", file=sys.stderr)
        return 3
    if repo.selected_channel() is None:
        print("no channel selected; run 'tg-ingest channel select' first", file=sys.stderr)
        await client.disconnect()
        return 1
    try:
        report = await run_backfill(
            client, repo, cfg, channel_bare_id=None, max_pages=args.max_pages
        )
    except BackfillError as exc:
        print(f"backfill failed: {exc}", file=sys.stderr)
        await client.disconnect()
        return 4
    state = "complete" if report.completed else "in progress"
    print(
        f"discovered={report.discovered} inserted={report.inserted} updated={report.updated} "
        f"unchanged={report.unchanged} failed={report.failed} oldest_id_seen={report.oldest_id_seen} status={state}"
    )
    await client.disconnect()
    return 0 if report.failed == 0 else 1


def cmd_backfill_status(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    sync = repo.sync_status()
    if sync.channel_id is None:
        print("no channel selected")
        return 1
    counts = repo.message_counts(sync.channel_id)
    print(f"channel: {sync.channel_title} (id={sync.channel_id}, marked={marked_channel_id(sync.channel_id)})")
    print(f"backfill: status={sync.backfill_status} mode={sync.backfill_target_mode} since={sync.backfill_since}")
    print(f"  checkpoint offset_id={sync.backfill_next_max_id} oldest_seen={sync.backfill_oldest_id_seen}")
    print(f"  stored so far: {counts.total} (media={counts.with_media}, deleted={counts.deleted})")
    return 0


async def cmd_serve(_args) -> int:
    from .locking import AlreadyRunningError, SingleInstanceLock
    from .service import IngestService

    cfg = _load_or_exit()
    lock = SingleInstanceLock.for_database(cfg.db_path)
    try:
        lock.acquire()
    except AlreadyRunningError as exc:
        print(f"serve refused to start: {exc}", file=sys.stderr)
        return 1
    control_lock = None
    db = None
    try:
        if cfg.control.enabled:
            control_lock = SingleInstanceLock.for_control(cfg.db_path)
            try:
                control_lock.acquire()
            except AlreadyRunningError as exc:
                print(f"serve refused to start: {exc}", file=sys.stderr)
                return 1
        db, repo = _new_repo(cfg)
        control_bot = None
        trading_engine = None
        if cfg.control.enabled:
            try:
                control_bot, trading_engine = _build_control_runtime(cfg, repo)
            except ConfigError as exc:
                print(f"Configuration error: {exc}", file=sys.stderr)
                return 2
        try:
            svc = IngestService(
                cfg, repo, trading_engine=trading_engine, control_bot=control_bot
            )
        except ConfigError as exc:
            print(f"Configuration error: {exc}", file=sys.stderr)
            return 2
        return await svc.run()
    finally:
        if control_lock is not None:
            control_lock.release()
        if db is not None:
            db.close()
        lock.release()


def _build_control_runtime(cfg, repo):
    from .control.bot_api import BotApiClient
    from .control.service import ControlBot
    from .trading import build_trading_engine

    if not (cfg.control.bot_token and cfg.control.owner_user_id):
        raise ConfigError(
            "control bot is not configured; set CONTROL_BOT_TOKEN and "
            "CONTROL_OWNER_USER_ID in the private .env"
        )
    bot = ControlBot(
        cfg,
        repo,
        BotApiClient(cfg.control.bot_token, api_base=cfg.control.api_base),
    )
    engine = build_trading_engine(cfg, repo)
    bot.attach_engine(engine)
    return bot, engine


def _install_stop_handlers(stop: asyncio.Event) -> None:
    import signal

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            with contextlib.suppress(NotImplementedError, RuntimeError):  # pragma: no cover
                loop.add_signal_handler(sig, stop.set)


def _read_only_repo(cfg):
    """Open an existing database without running migrations or creating files."""

    db = Database(cfg.db_path, read_only=True)
    return db, Repository(db, processing_max_attempts=cfg.processing_max_attempts)


def cmd_manage_status(_args) -> int:
    from .management.store import ManagementStore

    cfg = _load_or_exit(require_credentials=False)
    if not Path(cfg.db_path).is_file():
        print("management database: not initialized (run 'tg-ingest manage run' once)")
        return 1
    try:
        db, repo = _read_only_repo(cfg)
    except (FileNotFoundError, sqlite3.Error) as exc:
        print(f"management database unavailable: {exc.__class__.__name__}", file=sys.stderr)
        return 1
    try:
        try:
            store = ManagementStore(db)
            state = store.state()
        except sqlite3.Error:
            print("management schema not initialized (run 'tg-ingest manage run' once)")
            return 1
        print("Telegram management bot")
        print(f"  configured: {'yes' if cfg.control.management_configured else 'no'}")
        print(
            "  authorized operators: owner="
            f"{'set' if cfg.control.owner_user_id else 'MISSING'} developer="
            f"{'set' if cfg.control.developer_user_id else 'MISSING'}"
        )
        print(f"  bot token: {'set' if cfg.control.bot_token else 'MISSING'}")
        print("  transport: direct TCP/TLS")
        print(f"  telegram state: {state.get('telegram_state')}")
        print(f"  account label: {state.get('telegram_label') or '-'}")
        print(f"  selected channel: {state.get('selected_channel_title') or 'not selected'}"
              f" (id={state.get('selected_channel_id') or '-'})")
        try:
            print(f"  pending login stages: {store.pending_flow_count()}")
        except sqlite3.Error:
            print("  pending login stages: unavailable (older schema)")
        job = store.latest_job()
        if job is None:
            print("  ingestion: not started")
        else:
            counts = repo.message_counts(int(job["channel_id"]))
            print(f"  ingestion: {job['status']} channel={job['channel_id']}")
            print(f"    stored={counts.total} inserted={job['inserted']} updated={job['updated']} "
                  f"failed={job['failed']}")
            print(f"    checkpoint={job.get('checkpoint_next_max_id')} error={job.get('error') or '-'}")
            if job.get("max_messages") is not None:
                print(f"    bounded sample: {job['max_messages']} messages")
        if store.validation_available():
            print("  validation workflow: ready")
            request = store.latest_import_request()
            if request is not None:
                print(
                    "    request: "
                    f"{request['status']} limit={request['max_messages']} "
                    f"reviewed={request['reviewed_count']}/{request['candidate_count']}"
                )
        else:
            print("  validation workflow: schema not applied")
        print("  trading/MEXC: deliberately disabled in the management bot")
        return 0
    finally:
        db.close()


async def _cmd_manage_run() -> int:
    from .locking import AlreadyRunningError, SingleInstanceLock
    from .management.bot import ManagementBot

    cfg = _load_or_exit()
    if not cfg.control.management_configured:
        print(
            "management bot requires CONTROL_BOT_TOKEN, CONTROL_OWNER_USER_ID and "
            "CONTROL_DEVELOPER_USER_ID",
            file=sys.stderr,
        )
        return 2
    try:
        bot = BotApiClient(cfg.control.bot_token or "", api_base=cfg.control.api_base)
    except (ConfigError, BotApiError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    lock = SingleInstanceLock.for_database(cfg.db_path)
    control_lock = SingleInstanceLock.for_control(cfg.db_path)
    db = None
    try:
        try:
            lock.acquire()
            control_lock.acquire()
        except AlreadyRunningError as exc:
            print(f"management bot refused to start: {exc}", file=sys.stderr)
            return 1
        db, repo = _new_repo(cfg)
        from .trading.runtime import apply_persisted_trading_settings

        apply_persisted_trading_settings(cfg, repo)
        manager = ManagementBot(cfg, repo, bot)
        stop = asyncio.Event()
        _install_stop_handlers(stop)
        print(
            "Telegram management bot running "
            "(two operators; preview and developer self-authorized bounded validation; "
            "trading disabled)."
        )
        await manager.run(stop)
        return 0
    finally:
        if db is not None:
            db.close()
        control_lock.release()
        lock.release()


def cmd_manage_run(_args) -> int:
    return asyncio.run(_cmd_manage_run())


def cmd_control_status(_args) -> int:
    from .control.secretbox import SecretBox
    from .control.store import ControlStore

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    try:
        store = ControlStore(repo.db)
        setup = store.setup()
        print("Telegram control bot")
        print(f"  configured: {'yes' if cfg.control.configured else 'no'}")
        print(f"  enabled: {'yes' if cfg.control.enabled else 'no'}")
        print(f"  owner user id: {cfg.control.owner_user_id or 'MISSING'}")
        print(f"  bot token: {'set' if cfg.control.bot_token else 'MISSING'}")
        encryption = "ready" if SecretBox(cfg.control.secret_key).available else "MISSING CONTROL_SECRET_KEY"
        print(f"  credential encryption: {encryption}")
        print(f"  state: {setup['effective_state']} (stored: {setup['setup_state']})")
        print(f"  telegram: {setup['telegram_state']} — {setup['telegram_account_label'] or '-'}")
        print(f"  MEXC: {setup['mexc_state']} — {setup['mexc_label'] or '-'}")
        print(f"  channel: {setup['selected_channel_title'] or 'not selected'}")
        print(
            f"  learning: {setup['learning_state']} "
            f"({int(setup['learning_streak'] or 0)}/{store.get_int_setting('learning_target', 5)})"
        )
        print(
            f"  execution: {setup['execution_mode']}  paused={bool(int(setup['paused'] or 0))}"
        )
        print(f"  pending approvals: {len(store.list_approvals('pending', 100))}")
        print("  trading remains disabled until separately configured; the control bot never forces it on.")
    finally:
        db.close()
    return 0


async def cmd_control_run(_args) -> int:
    from .locking import AlreadyRunningError, SingleInstanceLock

    cfg = _load_or_exit(require_credentials=False)
    if not cfg.control.configured:
        print(
            "control bot is not configured; set CONTROL_BOT_TOKEN and CONTROL_OWNER_USER_ID",
            file=sys.stderr,
        )
        return 2
    if not cfg.control.enabled:
        print("control bot is disabled by CONTROL_BOT_ENABLED", file=sys.stderr)
        return 2
    lock = SingleInstanceLock.for_control(cfg.db_path)
    try:
        lock.acquire()
    except AlreadyRunningError as exc:
        print(f"control refused to start: {exc}", file=sys.stderr)
        return 1
    db = None
    try:
        db, repo = _new_repo(cfg)
        try:
            bot, _engine = _build_control_runtime(cfg, repo)
        except ConfigError as exc:
            print(f"Configuration error: {exc}", file=sys.stderr)
            return 2
        stop = asyncio.Event()
        _install_stop_handlers(stop)
        print("Telegram control bot running (controller only; no ingestion worker).")
        await bot.run(stop)
        return 0
    finally:
        if db is not None:
            db.close()
        lock.release()


def cmd_status(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    sync = repo.sync_status()
    counts = repo.message_counts(sync.channel_id or 0)
    queue = repo.queue_counts()
    session_file = Path(str(cfg.session_path))
    if session_file.suffix != ".session":
        session_file = session_file.with_suffix(".session")
    if not session_file.is_absolute():
        session_file = PROJECT_ROOT / session_file
    session_state = "present" if session_file.is_file() else "MISSING (auth login needed)"
    print("tg-signal-ingest status")
    print(f"  channel: {sync.channel_title or '-'} (id={sync.channel_id})")
    print(f"  session file: {session_state}")
    print(f"  backfill: {sync.backfill_status} (mode={sync.backfill_target_mode}, count={sync.backfill_message_count})")
    print(f"  stored messages: total={counts.total} media={counts.with_media} deleted={counts.deleted}")
    if counts.max_id is not None:
        print(f"  newest stored id: {counts.max_id} at {counts.max_datetime}")
    if sync.live_last_event_at:
        print(f"  last live event: {sync.live_last_event_at} (id={sync.live_last_seen_message_id})")
        print(f"  last recovery sync: {sync.last_sync_at or 'not run yet'}")
        print("  quiet channels show as silence; this is normal, not a failure")
    else:
        print("  no live messages recorded yet")
    print("  queue: pending={p} processing={r} processed={d} failed={f}".format(
        p=queue["pending"], r=queue["processing"], d=queue["processed"], f=queue["failed"]
    ))
    db.close()
    return 0


async def cmd_diag_network(_args) -> int:
    try:
        diag = run_direct_diagnostics()
    except ConnectivityError as exc:
        print(f"DIRECT NETWORK FAILURE: {exc}", file=sys.stderr)
        return 1
    print("direct network diagnostics: OK")
    print(f"  Bot API: {diag.bot_api.host}:{diag.bot_api.port} "
          f"addresses={','.join(diag.bot_api.addresses) or '-'}")
    print(f"    {diag.bot_api.detail}")
    for check in diag.telegram_dcs:
        status = "OK" if check.tcp_ok else "FAILED"
        print(f"  {check.name}: {check.host}:{check.port} {status}")
        if not check.tcp_ok:
            print(f"    {check.detail}")
    if diag.ambient_proxy_vars:
        print(
            "  ambient proxy variables present but ignored: "
            + ", ".join(diag.ambient_proxy_vars)
        )
    else:
        print("  ambient proxy variables: none")
    if diag.legacy_proxy_vars:
        print(
            "  warning: legacy project proxy variables are set but ignored; remove: "
            + ", ".join(diag.legacy_proxy_vars)
        )
    return 0


async def cmd_diag_bot(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    if not cfg.control.bot_token:
        print("Bot API identity check skipped: CONTROL_BOT_TOKEN is not configured", file=sys.stderr)
        return 2
    try:
        bot = BotApiClient(cfg.control.bot_token, api_base=cfg.control.api_base)
        me = bot.get_me()
    except (BotApiError, ConfigError) as exc:
        print(f"DIRECT BOT API FAILURE: {exc}", file=sys.stderr)
        return 1
    username = me.get("username")
    print(
        "direct Bot API identity: OK "
        f"(id={me.get('id', 'unknown')}, username=@{username})" if username
        else f"direct Bot API identity: OK (id={me.get('id', 'unknown')})"
    )
    return 0


async def cmd_diag_telegram(_args) -> int:
    """Check the direct network, then the real Telethon session and channel."""

    network_code = await cmd_diag_network(_args)
    if network_code != 0:
        print("Telegram check skipped: direct network must work first.", file=sys.stderr)
        return 1
    cfg = _load_or_exit()
    client = None
    try:
        from telethon.tl.functions.help import GetConfigRequest

        client = build_client(cfg)
        await client.connect()
        await client(GetConfigRequest())
        print("Telegram direct MTProto encrypted RPC: OK.")
        if not await client.is_user_authorized():
            print(
                "Telegram direct reachability OK; authentication state: no valid session. "
                "The account owner must run 'tg-ingest auth login'."
            )
            return 3
        secure_session_files(cfg)
        print("Telegram direct reachability OK; session VALID.")
        from telethon.tl.types import PeerChannel

        db, repo = _new_repo(cfg)
        try:
            selected = repo.selected_channel()
            if selected is not None:
                entity = await client.get_entity(PeerChannel(selected.channel_id))
                print(
                    f"channel access OK: id={selected.channel_id} "
                    f"title={getattr(entity, 'title', '-')!r}"
                )
            else:
                print("channel check skipped: no channel selected.")
        finally:
            db.close()
    except (ConnectionError, OSError) as exc:
        print(
            f"DIRECT TELEGRAM FAILURE ({exc.__class__.__name__}); "
            "run 'tg-ingest diagnostics network'.",
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"channel verification failed: {exc.__class__.__name__}", file=sys.stderr)
        return 4
    finally:
        if client is not None:
            await client.disconnect()
    return 0


def cmd_diag_clock(args) -> int:
    """Show clock health and skew diagnostics."""
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)

    # Check clock skew via MEXC client if available
    from tg_signal_ingest.trading.store import (
        CLOCK_SKEW_FRESH_MS,
        CLOCK_SKEW_STALE_MS,
        CLOCK_SKEW_UNHEALTHY_MS,
    )

    # Read current database state for clock health tracking
    sync = repo.sync_status()
    counts = repo.message_counts(sync.channel_id or 0)

    lines: list[str] = []
    lines.append("clock health diagnostics")
    lines.append("  VPS clock sync: NTP enabled (drift possible after startup)")
    recv_window = getattr(cfg.mexc, "recv_window_ms", 5000)
    lines.append(
        f"  recvWindow default: {cfg.trading.mode} mode uses {recv_window} ms"
    )
    # Note: actual skew measurement requires an active MEXC client with live/auth config
    lines.append("  (run with live credentials to measure actual skew)")
    lines.append("")
    lines.append("  clock health thresholds:")
    lines.append(f"    fresh: < {CLOCK_SKEW_FRESH_MS} ms")
    lines.append(f"    degraded: {CLOCK_SKEW_FRESH_MS}-{CLOCK_SKEW_STALE_MS} ms")
    lines.append(f"    unhealthy: >= {CLOCK_SKEW_UNHEALTHY_MS} ms (exceeds default recvWindow)")
    lines.append("")
    lines.append(f"  channel: {sync.channel_title or '-'} (id={sync.channel_id})")
    lines.append(f"  backfill: {sync.backfill_status} (mode={sync.backfill_target_mode})")
    lines.append(f"  stored messages: total={counts.total}")
    db.close()
    for line in lines:
        print(line)
    return 0


def cmd_diag_audit(args) -> int:
    """Show audit journal health and event counts."""
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)

    # Query audit event counts from the database
    total = repo.db.conn.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
    by_severity = {}
    by_category = {}

    rows = repo.db.conn.execute(
        "SELECT severity, COUNT(*) FROM audit_events GROUP BY severity"
    ).fetchall()
    for row in rows:
        by_severity[row[0]] = row[1]

    rows = repo.db.conn.execute(
        "SELECT category, COUNT(*) FROM audit_events GROUP BY category"
    ).fetchall()
    for row in rows:
        by_category[row[0]] = row[1]

    # Check for events with clock_health set
    clock_events = repo.db.conn.execute(
        "SELECT clock_health, COUNT(*) FROM audit_events WHERE clock_health IS NOT NULL GROUP BY clock_health"
    ).fetchall()
    by_clock_health = {}
    for row in clock_events:
        by_clock_health[row[0]] = row[1]

    lines: list[str] = []
    lines.append("audit journal health")
    lines.append(f"  total events: {total}")
    lines.append("")
    lines.append("  by severity:")
    for sev, count in sorted(by_severity.items()):
        lines.append(f"    {sev}: {count}")
    lines.append("")
    lines.append("  by category:")
    for cat, count in sorted(by_category.items()):
        lines.append(f"    {cat}: {count}")
    lines.append("")
    lines.append("  by clock_health state:")
    if by_clock_health:
        for state, count in sorted(by_clock_health.items()):
            lines.append(f"    {state}: {count}")
    else:
        lines.append("    (no events with clock_health set yet)")
    lines.append("")
    lines.append("  recent events (last 20):")
    rows = repo.db.conn.execute(
        "SELECT event_id, event, severity, occurred_at_utc, clock_health, component "
        "FROM audit_events ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    for row in rows:
        clock_str = f" clock={row[5]}" if row[5] else ""
        lines.append(f"    {row[0][:30]}{clock_str} | {row[1]} | {row[2]} | {row[3]}")
    db.close()
    for line in lines:
        print(line)
    return 0


def cmd_diag_orders(args) -> int:
    """Show order intent traces and state diagnostics."""
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)

    from .trading.store import TradingStore

    store = TradingStore(repo.db, cfg.trading)
    counts = store.count_intents_by_state()

    lines: list[str] = []
    lines.append("order intent state diagnostics")
    lines.append("  intents by state:")
    for state, count in sorted(counts.items()):
        lines.append(f"    {state}: {count}")
    lines.append("")

    # Show intents with unknown_requires_reconciliation
    unreconciled = store.pending_reconciliation(limit=100)
    lines.append(f"  pending reconciliation ({len(unreconciled)} intents):")
    for intent in unreconciled:
        reason = intent.get("reject_reason") or ""
        lines.append(
            f"    {intent['intent_id']} mode={intent['mode']} state={intent['state']} "
            f"sym={intent['symbol']} reason={reason[:80]}"
        )
    lines.append("")

    # Show budget summary
    budget = store.budget_summary()
    lines.append("  budget summary:")
    lines.append(f"    limit: {budget['limit']} USDT")
    lines.append(f"    reserved: {budget['reserved']} USDT")
    lines.append(f"    spent: {budget['spent']} USDT")
    lines.append(f"    remaining: {budget['remaining']} USDT")
    db.close()
    for line in lines:
        print(line)
    return 0


def cmd_diag_reconcile(args) -> int:
    """Show unresolved reconciliation diagnostics."""
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)

    from .trading.runtime import build_trading_engine

    # Build engine in sim mode (no client needed for read-only diagnostics)
    engine = build_trading_engine(cfg, repo, require_client=False)

    # Get pending reconciliation intents
    pending = engine.store.pending_reconciliation(limit=args.limit if hasattr(args, 'limit') else 50)

    lines: list[str] = []
    lines.append("unresolved reconciliation diagnostics")
    lines.append(f"  checked: {len(pending)} intents pending reconciliation")
    lines.append("")

    # Group by outcome
    outcomes: dict[str, int] = {}
    for intent in pending:
        # Check last reconciliation result
        rows = engine.repo.db.conn.execute(
            "SELECT outcome FROM reconciliation_results WHERE intent_id = ? "
            "ORDER BY recon_id DESC LIMIT 1",
            (intent["intent_id"],),
        ).fetchall()
        outcome = rows[0]["outcome"] if rows else "none"
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

    lines.append("  outcomes:")
    for outcome, count in sorted(outcomes.items()):
        lines.append(f"    {outcome}: {count}")
    lines.append("")

    # Show details of intents with uncertain outcomes
    uncertain_outcomes = {"ambiguous", "not_found", "discrepancy"}
    uncertain_intents = [i for i in pending if outcomes.get(i["intent_id"], "none") in uncertain_outcomes]

    lines.append(f"  uncertain intents ({len(uncertain_intents)}):")
    for intent in uncertain_intents[:20]:
        reason = intent.get("reject_reason") or ""
        lines.append(
            f"    {intent['intent_id']} state={intent['state']} "
            f"sym={intent['symbol']} mode={intent['mode']} reason={reason[:80]}"
        )
    if len(uncertain_intents) > 20:
        lines.append(f"    ... and {len(uncertain_intents) - 20} more")

    db.close()
    for line in lines:
        print(line)
    return 0


def cmd_messages_list(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    selected = repo.selected_channel()
    rows = repo.list_messages(selected.channel_id if selected else None, limit=args.limit, newest_first=not args.oldest)
    for r in rows:
        deleted = " [DELETED]" if r["deleted"] else ""
        media = f" media={r['media_type']}" if r["media_type"] else ""
        text_preview = (r["text_preview"] or "").replace("\n", " ")
        print(f"id={r['message_id']:<8} {r['datetime']:<20} v{r['processing_version']}"
              f"{media}{deleted} {text_preview!r}")
    db.close()
    return 0


def cmd_messages_show(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    selected = repo.selected_channel()
    if selected is None:
        print("no channel selected", file=sys.stderr)
        return 1
    raw = repo.get_message_raw(selected.channel_id, args.id)
    if raw is None:
        print("message not found", file=sys.stderr)
        return 1
    import json

    payload = dict(raw)
    payload.pop("raw_json", None)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    edits = repo.edit_history(selected.channel_id, args.id)
    if edits:
        print(f"edit history ({len(edits)} previous version(s)):")
        for e in edits:
            prev = (e["text"] or e["caption"] or "").replace("\n", " ")
            print(f"  seq={e['edit_seq']} edit_date={e['edit_datetime']}"
                  f" captured={e['captured_at']} text={prev[:80]!r}")
    db.close()
    return 0


def cmd_queue_list(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    counts = repo.queue_counts()
    print(
        f"pending={counts['pending']} processing={counts['processing']} "
        f"processed={counts['processed']} failed={counts['failed']}"
    )
    for row in repo.list_queue(status=args.status, limit=args.limit):
        print(
            f"q {row['queue_id']:<6} {row['status']:<10} {row['item_type']:<8} "
            f"v{row['targets_version']} tries={row['attempts']} msg={row['message_id']} {row['last_error'] or ''}"
        )
    db.close()
    return 0


def cmd_queue_retry(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    changed = repo.retry_failed()
    print(f"re-queued {changed} failed item(s)")
    db.close()
    return 0


def _trading_store_and_engine(cfg, repo, *, require_client: bool = False):
    from .trading import build_trading_engine
    from .trading.store import TradingStore

    engine = build_trading_engine(cfg, repo, require_client=require_client)
    return TradingStore(repo.db, cfg.trading), engine


def cmd_trading_status(_args) -> int:
    from .trading.store import TradingStore

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    store = TradingStore(repo.db, cfg.trading)
    counts = store.count_intents_by_state()
    recon = store.reconciliation_counts()
    t = cfg.trading
    print("MEXC Spot trading pipeline")
    print(f"  mode: {t.mode} (live requires explicit token; default is sim)")
    print(f"  order amount: {t.order_quote_amount:g} USDT (source: {t.order_quote_amount_source})")
    print("  risk gates: Spot BUY only; no daily/open-order caps, signal-age limit, or symbol allowlist")
    print(f"  order type policy: {t.order_type_policy}")
    print(f"  emergency_stop: {t.emergency_stop}")
    print(f"  MEXC credentials: {'set' if cfg.mexc.api_key and cfg.mexc.api_secret else 'missing'}")
    print(f"  intents by state: {counts or 'none'}")
    print(f"  reconciliation outcomes: {recon or 'none'}")
    print(f"  recorded fills: {store.fills_count()}")
    db.close()
    return 0


def cmd_trading_signals(args) -> int:
    from .trading.store import TradingStore

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    for row in TradingStore(repo.db, cfg.trading).list_signals(limit=args.limit):
        print(
            f"{row['signal_id']:<24} {row['signal_kind']:<9} {row['status']:<12} "
            f"{(row['action'] or '-'):<4} {(row['symbol'] or '-'):<12} {row['rejection_reason'] or ''}"
        )
    db.close()
    return 0


def cmd_trading_repair_signals(args) -> int:
    from .trading.store import TradingStore

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    store = TradingStore(repo.db, cfg.trading)
    rows = store.repair_inconsistent_terminal_signals(limit=args.limit, apply=args.apply)
    if not rows:
        print("no inconsistent terminal signals found")
        db.close()
        return 0
    for row in rows:
        print(
            f"{row['signal_id']:<24} signal={row['signal_status']:<10} "
            f"intent={row['intent_id']:<34} state={row['intent_state']}"
        )
    if args.apply:
        print(f"repaired {len(rows)} signal status(es); no exchange call was made")
    else:
        print("dry-run only; pass --apply to persist these signal status corrections")
    db.close()
    return 0


def cmd_trading_intents(args) -> int:
    from .trading.store import TradingStore

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    for row in TradingStore(repo.db, cfg.trading).list_intents(limit=args.limit):
        print(
            f"{row['intent_id']:<34} {row['mode']:<8} {row['state']:<30} {row['symbol']:<12} "
            f"{row['side']:<4} qty={row['base_quantity'] or '-'} quote={row['quote_amount'] or '-'} "
            f"ex={row['exchange_order_id'] or '-'} {row['reject_reason'] or ''}"
        )
    db.close()
    return 0


def cmd_trading_parse(args) -> int:
    import json

    from .trading.parser import SignalParser

    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    selected = repo.selected_channel()
    if selected is None:
        print("no channel selected", file=sys.stderr)
        return 1
    record = repo.get_message_raw(selected.channel_id, args.id)
    if record is None:
        print("message not found", file=sys.stderr)
        return 1
    parsed = SignalParser().parse(
        record.get("text") or record.get("caption"),
        is_edit=bool(record.get("edit_datetime")),
    )
    payload = {
        "kind": parsed.kind,
        "action": parsed.action,
        "symbol": parsed.symbol,
        "entry_price": str(parsed.entry_price) if parsed.entry_price is not None else None,
        "entry_low_price": str(parsed.entry_low_price) if parsed.entry_low_price is not None else None,
        "entry_high_price": str(parsed.entry_high_price) if parsed.entry_high_price is not None else None,
        "targets": [str(p) for p in parsed.target_prices],
        "stop_loss": str(parsed.stop_loss) if parsed.stop_loss is not None else None,
        "order_type": parsed.order_type,
        "amount_text": parsed.amount_text,
        "reasons": list(parsed.reasons),
        "fingerprint": parsed.fingerprint_sha256,
    }
    print(json.dumps(payload, indent=2))
    db.close()
    return 0


def cmd_trading_reconcile(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    try:
        _store, engine = _trading_store_and_engine(
            cfg, repo, require_client=cfg.trading.mode != "sim"
        )
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        db.close()
        return 2
    stats = engine.reconcile_pending(limit=args.limit)
    print(
        f"reconciliation: checked={stats.checked} resolved={stats.resolved} "
        f"ambiguous={stats.ambiguous} discrepancies={stats.discrepancies} "
        f"not_found={stats.not_found} errors={stats.errors}"
    )
    db.close()
    return 0 if stats.errors == 0 else 1


def cmd_trading_notifications(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    from .trading.store import TradingStore

    store = TradingStore(repo.db, cfg.trading)
    rows = store.pending_notifications(limit=args.limit)
    if args.status:
        rows = [r for r in rows if r["status"] == args.status]
    print(f"trading notifications ({len(rows)} pending/failed):")
    for r in rows:
        attempts = store.notification_attempts(int(r["notification_id"]))
        last = attempts[-1] if attempts else None
        print(
            f"  id={r['notification_id']} status={r['status']} role={r['role']} "
            f"category={r['category']} event_type={r['event_type']} "
            f"attempts={r['attempts']} next={r['next_attempt_at'] or '-'} "
            f"last={last['outcome'] if last else '-'} {last['error'] or '' if last else ''}"
        )
        if last:
            print(f"        attempt={last['attempt_no']} at={last['attempted_at']} "
                  f"ambiguous={last['ambiguous']} retry_after={last['retry_after_s']}")
    db.close()
    return 0


def cmd_trading_test_order(args) -> int:
    from decimal import Decimal

    cfg = _load_or_exit(require_credentials=False)
    if cfg.trading.mode not in ("testonly", "live"):
        print("official order/test requires trading.mode = 'testonly' or 'live'", file=sys.stderr)
        return 2
    from .trading.runtime import build_mexc_client

    try:
        client = build_mexc_client(cfg)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    meta = client.symbols.get(args.symbol)
    quote = Decimal(str(args.quote))
    ticker = client.ticker_price(symbol=meta.symbol)
    if not isinstance(ticker, dict):
        raise SystemExit(1)
    price = Decimal(str(ticker["price"]))
    qty = meta.quantize_quantity(quote / price)
    meta.validate_order(
        side=args.side.upper(), order_type="MARKET", quantity=qty, quote_amount=quote, price=None
    )
    if args.side.upper() == "BUY":
        client.place_test_order(
            symbol=meta.symbol, side="BUY", order_type="MARKET", quote_order_qty=quote
        )
    else:
        client.place_test_order(
            symbol=meta.symbol, side="SELL", order_type="MARKET", quantity=qty
        )
    print(f"official test-order accepted for {meta.symbol} {args.side.upper()} quote={quote} (NO real order placed)")
    return 0


def cmd_trading_verify(args) -> int:
    """Read-only MEXC API key verification command.

    Classifies credential problems without printing the key or secret and
    without placing any order.
    """
    cfg = _load_or_exit(require_credentials=False)
    code, lines = _report_mexc_verification(cfg)
    for line in lines:
        print(line)
    return code


def cmd_trading_preflight(args) -> int:
    """Preflight checks for the MEXC CLI execution backend.

    Verifies the CLI binary, version, schema, doctor status, credential
    profile, and (in live mode) account identity against REST. No orders are
    placed and no secrets are printed.
    """
    cfg = _load_or_exit(require_credentials=False)

    if not cfg.mexc.cli_enabled:
        print("MEXC CLI writes are disabled; preflight cannot proceed.")
        print("RESULT: FAIL")
        return 1

    from .mexc.cli import MexcCliAdapter

    adapter = MexcCliAdapter(
        binary=getattr(cfg.mexc, "cli_binary", None),
        profile=getattr(cfg.mexc, "cli_profile", None),
        timeout_s=float(getattr(cfg.mexc, "cli_timeout_s", 5.0)),
        min_version=getattr(cfg.mexc, "cli_min_version", "0.0.22"),
        confirm_writes=bool(getattr(cfg.mexc, "cli_confirm_noninteractive_writes", False)),
        symbols=None,
    )

    health = adapter.check()
    if not health.ok:
        print("MEXC CLI check failed:")
        for problem in health.problems:
            print(f"  - {problem}")
        print("RESULT: FAIL")
        return 2

    if args.network:
        doctor = adapter.doctor()
        if not doctor.get("ok", False):
            print("MEXC CLI doctor reported problems:")
            for check in doctor.get("checks", []):
                print(f"  - {check.get('name', '?')}: {check.get('message', '')}")
            print("RESULT: FAIL")
            return 2
        adapter.read_only_access()
    else:
        print("NOT CHECKED: --network not requested; skipping live doctor/read checks.")

    # CLI credential profile validation.
    profiles_fn = getattr(adapter, "profiles", None)
    if profiles_fn is not None:
        profiles = profiles_fn()
        active = profiles.get("active")
        profile_list = profiles.get("profiles", [])
        cli_profile = getattr(cfg.mexc, "cli_profile", None)
        if not profile_list:
            print("no CLI credential profile is saved.")
            print("  Run 'mexc profile set' to store a MEXC website-issued Open API key.")
            print("  The CLI profile uses the same MEXC website-issued Open API credentials.")
            print("RESULT: FAIL")
            return 1
        if active is None or active not in profile_list:
            print(f"no active CLI profile (saved: {', '.join(profile_list)}).")
            print("  Run 'mexc profile use <name>' to select a profile.")
            print("RESULT: FAIL")
            return 1
        if cli_profile is not None and cli_profile not in profile_list:
            print(f"'{cli_profile}' is not saved in the CLI credential profiles.")
            print("  Run 'mexc profile set' to store it, then 'mexc profile use' to select it.")
            print("RESULT: FAIL")
            return 1

    # Live mode: verify CLI and REST account identity match.
    if cfg.trading.mode == "live":
        from .mexc.credentials import validate_credentials
        from .trading.runtime import build_mexc_client

        try:
            rest = build_mexc_client(cfg)
            info = validate_credentials(rest)
            info_status = getattr(info, "status", "VALID")
            if info_status.upper() != "VALID":
                print(f"REST key not valid: status={info_status}")
                print("RESULT: FAIL")
                return 2
            print("REST key: VALID")
        except Exception as exc:  # noqa: BLE001
            print(f"REST credential check failed: {exc}")
            print("RESULT: FAIL")
            return 2

        cli_account_fn = getattr(adapter, "account", None)
        rest_account_fn = getattr(rest, "account", None)
        if cli_account_fn is not None and rest_account_fn is not None:
            cli_account = cli_account_fn()
            rest_account = rest_account_fn()
            cli_balances = {
                b.get("asset"): b.get("free") for b in cli_account.get("balances", [])
            }
            rest_balances = {
                b.get("asset"): b.get("free") for b in rest_account.get("balances", [])
            }
            if cli_balances != rest_balances:
                print("MISMATCH: CLI and REST account balances differ.")
                print("RESULT: FAIL")
                return 2
            print("account identity: MATCH")

    print("RESULT: PASS")
    return 0


def cmd_queue_reprocess(args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    db, repo = _new_repo(cfg)
    selected = repo.selected_channel()
    if selected is None:
        print("no channel selected", file=sys.stderr)
        return 1
    if args.all:
        count = 0
        for row in repo.list_messages(selected.channel_id, limit=10_000, newest_first=False, include_deleted=False):
            repo.reprocess_message(selected.channel_id, int(row["message_id"]))
            count += 1
        print(f"re-enqueued {count} message(s) for the current parser")
        db.close()
        return 0
    try:
        version = repo.reprocess_message(selected.channel_id, args.id)
    except KeyError:
        print("message not found", file=sys.stderr)
        return 1
    print(f"re-enqueued message {args.id} at version {version}")
    db.close()
    return 0


def _analysis_repo(db_path: str | None = None):
    cfg = _load_or_exit(require_credentials=False)
    if db_path:
        # Offline analysis may target a disposable copy. Never require the
        # operator to edit the production config just to run a dry-run.
        resolved = Path(db_path).expanduser()
        cfg = replace(cfg, db_path=resolved, data_dir=resolved.parent)
    db, repo = _new_repo(cfg)
    return cfg, db, repo


def _iso_argument(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    from datetime import datetime

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"{name} must be an ISO date or timestamp") from exc
    return value


def cmd_analyze_replay(args) -> int:
    from .analysis import ReplayScope, run_replay

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    selected = repo.selected_channel()
    channel_id = args.channel_id if args.channel_id is not None else (selected.channel_id if selected else None)
    if channel_id is None:
        print("no channel selected; pass --channel-id to replay a specific channel", file=sys.stderr)
        db.close()
        return 1
    scope = ReplayScope(
        channel_id=channel_id,
        since=_iso_argument(args.since, "--since"),
        until=_iso_argument(args.until, "--until"),
        limit=args.limit,
        after_message_id=args.after_message_id,
        include_deleted=not args.exclude_deleted,
        dry_run=args.dry_run,
    )
    try:
        stats = run_replay(repo, scope)
    except Exception as exc:  # noqa: BLE001 - CLI boundary reports safely
        print(f"analysis replay failed: {exc.__class__.__name__}", file=sys.stderr)
        db.close()
        return 1
    mode = "dry-run" if stats.dry_run else f"run={stats.run_id}"
    print(f"offline analysis replay ({mode})")
    print(
        f"  scanned={stats.scanned} inserted={stats.inserted} duplicates={stats.duplicates} "
        f"superseded={stats.superseded} needs_review={stats.needs_review} "
        f"failures={stats.failures} deleted_marked={stats.deleted_marked}"
    )
    print(f"  last_message_id={stats.last_message_id}")
    if stats.by_classification:
        print("  by classification: " + ", ".join(
            f"{key}={value}" for key, value in sorted(stats.by_classification.items())
        ))
    db.close()
    return 1 if stats.failures else 0


def cmd_analyze_reindex(args) -> int:
    from .analysis import AnalysisStore
    from .analysis.parser import SignalAnalysisParser
    from .analysis.reindex import rebuild_signal_index

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    selected = repo.selected_channel()
    channel_id = args.channel_id if args.channel_id is not None else (selected.channel_id if selected else None)
    if channel_id is None:
        print("no channel selected; pass --channel-id to reindex a specific channel", file=sys.stderr)
        db.close()
        return 1
    parser = SignalAnalysisParser()
    store = AnalysisStore(repo.db)
    try:
        stats = rebuild_signal_index(
            db,
            channel_id=channel_id,
            dry_run=args.dry_run,
            parser=parser,
            store=store,
        )
    except Exception as exc:  # noqa: BLE001 - CLI boundary reports safely
        print(f"signal index rebuild failed: {exc.__class__.__name__}", file=sys.stderr)
        db.close()
        return 1
    report: dict = stats.as_json()
    report["context"] = {
        "channel_id": channel_id,
        "parser_version": parser.version,
        "signals_in_index": db.conn.execute(
            "SELECT COUNT(*) AS c FROM channel_signals WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()["c"],
        "links_in_index": db.conn.execute(
            "SELECT COUNT(*) AS c FROM signal_message_links WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()["c"],
        "link_status_breakdown": {
            row["status"]: row["c"]
            for row in db.conn.execute(
                "SELECT status, COUNT(*) AS c FROM signal_message_links "
                "WHERE channel_id = ? GROUP BY status",
                (channel_id,),
            ).fetchall()
        },
        "conflicts": db.conn.execute(
            "SELECT COUNT(*) AS c FROM signal_message_links "
            "WHERE channel_id = ? AND status = 'conflict'",
            (channel_id,),
        ).fetchone()["c"],
    }
    mode = "dry-run (nothing written)" if stats.dry_run else "applied"
    print(f"logical signal index rebuild ({mode}) for channel {channel_id}")
    print(
        f"  messages={stats.messages_scanned} parse_failures={stats.parse_failures} "
        f"initial_signals={stats.initial_signals} follow_ups={stats.follow_ups} "
        f"long={stats.long_signals} short={stats.short_signals} ambiguous={stats.ambiguous}"
    )
    print(
        f"  signals_planned={stats.signals_planned} duplicate_signal_ids={stats.duplicate_signal_ids} "
        f"linked={stats.links_linked} unresolved={stats.links_unresolved} "
        f"conflicts={stats.links_conflict} no_reference={stats.links_no_reference}"
    )
    if not stats.dry_run:
        print(
            f"  applied: signals_created={stats.signals_created} "
            f"signals_updated={stats.signals_updated} signals_unchanged={stats.signals_unchanged} "
            f"links_created={stats.links_created} links_updated={stats.links_updated}"
        )
    if args.report_path:
        report_path = Path(args.report_path).expanduser()
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(f"  machine-readable report: {report_path}")
    db.close()
    return 0


def cmd_analyze_summary(args) -> int:
    from .analysis import AnalysisStore

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    store = AnalysisStore(repo.db)
    summary = store.summary()
    print("offline signal analysis summary (no exchange activity)")
    print(f"  candidates: total={summary['total']} status={summary['status'] or 'none'}")
    print(f"  current classification: {summary['classification'] or 'none'}")
    print(f"  current parse status:   {summary['parse_status'] or 'none'}")
    print(f"  current review status:  {summary['review_status'] or 'none'}")
    print(f"  unresolved failures:    {summary['unresolved_failures']}")
    latest = summary["latest_run"]
    if latest:
        print(
            f"  latest run: {latest['run_id']} status={latest['status']} "
            f"started={latest['started_at']} finished={latest['finished_at'] or '-'}"
        )
    else:
        print("  latest run: none")
    db.close()
    return 0


def cmd_analyze_list(args) -> int:
    from .analysis import AnalysisStore

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    rows = AnalysisStore(repo.db).list_candidates(
        classification=args.classification,
        parse_status=args.parse_status,
        review_status=args.review_status,
        include_superseded=args.include_superseded,
        limit=args.limit,
    )
    for row in rows:
        entry = (
            f"{row['entry_low_price']}-{row['entry_high_price']}"
            if row["entry_low_price"] and row["entry_high_price"]
            else (row["entry_price"] or "-")
        )
        targets = ",".join(str(t.get("price")) for t in row["targets"]) or "-"
        print(
            f"{row['candidate_id']:<25} msg={row['message_id']:<8} v{row['edit_version']} "
            f"{row['classification']:<15} {row['parse_status']:<12} {row['review_status']:<11} "
            f"{(row['symbol'] or '-'):<12} {(row['side'] or '-'):<4} entry={entry:<20} "
            f"tp={targets:<20} sl={row['stop_loss'] or '-'}"
        )
    db.close()
    return 0


def cmd_analyze_show(args) -> int:
    import json

    from .analysis import AnalysisStore

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    store = AnalysisStore(repo.db)
    row = store.get_candidate(args.candidate_id)
    if row is None:
        print("candidate not found", file=sys.stderr)
        db.close()
        return 1
    payload = dict(row)
    payload["channel_id"] = "<redacted-channel>"
    payload["reviews"] = store.list_reviews(args.candidate_id)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if args.show_text:
        source = repo.get_message_raw(int(row["channel_id"]), int(row["message_id"]))
        if source is None:
            print("source message not found", file=sys.stderr)
        else:
            print("source text (explicit --show-text):")
            print(source.get("text") or source.get("caption") or "<no text>")
    db.close()
    return 0


def cmd_analyze_review(args) -> int:
    from .analysis import AnalysisError, AnalysisStore, validate_correction

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    corrections: dict[str, object] = {}
    for source, target in (
        ("set_classification", "classification"),
        ("set_market_type", "market_type"),
        ("set_symbol", "symbol"),
        ("set_quote_asset", "quote_asset"),
        ("set_side", "side"),
        ("set_entry", "entry_price"),
        ("set_entry_low", "entry_low_price"),
        ("set_entry_high", "entry_high_price"),
        ("set_stop", "stop_loss"),
        ("set_targets", "targets"),
    ):
        value = getattr(args, source)
        if value is not None:
            corrections[target] = value
    try:
        corrected = validate_correction(corrections) if corrections else None
        review = AnalysisStore(repo.db).add_review(
            args.candidate_id,
            reviewer=args.reviewer or os.environ.get("USER") or "operator",
            decision=args.decision,
            corrected=corrected,
            note=args.note,
        )
    except KeyError:
        print("candidate not found", file=sys.stderr)
        db.close()
        return 1
    except AnalysisError as exc:
        print(f"review rejected: {exc}", file=sys.stderr)
        db.close()
        return 2
    print(
        f"review recorded: id={review.get('review_id')} decision={review.get('decision')} "
        f"candidate={args.candidate_id}"
    )
    db.close()
    return 0


def cmd_analyze_failures(args) -> int:
    from .analysis import AnalysisStore

    _cfg, db, repo = _analysis_repo(getattr(args, "db_path", None))
    rows = AnalysisStore(repo.db).list_failures(
        unresolved_only=not args.all, limit=args.limit
    )
    for row in rows:
        print(
            f"failure={row['failure_id']} msg={row['message_id']} v{row['edit_version']} "
            f"attempts={row['attempts']} type={row['error_type']} "
            f"last={row['last_failed_at']} resolved={row['resolved_at'] or '-'}"
        )
    print(f"unresolved failures: {AnalysisStore(repo.db).unresolved_failure_count()}")
    db.close()
    return 0


def cmd_analyze_corpus(args) -> int:
    from .analysis import CorpusError, check_corpus, load_corpus

    try:
        examples = load_corpus(args.path)
    except CorpusError as exc:
        print(f"corpus error: {exc}", file=sys.stderr)
        return 1
    report = check_corpus(examples)
    print(
        f"corpus check: total={report.total} matched={report.matched} "
        f"mismatched={report.mismatched} human_reviewed={report.human_reviewed}"
    )
    if report.human_reviewed == 0:
        print("warning: no human-reviewed real examples; corpus is synthetic and not accuracy evidence")
    for mismatch in report.mismatches or []:
        print(f"  mismatch id={mismatch['id']} field={mismatch['field']} language={mismatch['language']}")
    return 1 if report.mismatched else 0


# ------------------------------------------------------------------ assembly


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tg-ingest",
        description="Telegram signal-channel ingestion service (storage + downstream queue).",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="DEBUG logging")
    sub = parser.add_subparsers(dest="command", required=True)

    cfg = sub.add_parser("config", help="create/validate private configuration")
    cfg_sub = cfg.add_subparsers(dest="config_cmd", required=True)
    cfg_init = cfg_sub.add_parser("init", help="create .env template and config.toml")
    cfg_init.set_defaults(func=cmd_config_init)
    cfg_check = cfg_sub.add_parser("check", help="validate configuration (offline)")
    cfg_check.set_defaults(func=cmd_config_check)

    auth = sub.add_parser("auth", help="Telegram session lifecycle")
    auth_sub = auth.add_subparsers(dest="auth_cmd", required=True)
    auth_login = auth_sub.add_parser("login", help="interactive first-time login (account owner)")
    auth_login.set_defaults(func=cmd_auth_login)
    auth_check = auth_sub.add_parser("check", help="verify stored session validity")
    auth_check.set_defaults(func=cmd_auth_check)
    auth_check_channel = auth_sub.add_parser(
        "check-channel",
        help="verify stored session access to the management-selected channel",
    )
    auth_check_channel.set_defaults(func=cmd_auth_check_channel)
    auth_logout = auth_sub.add_parser("logout", help="revoke Telegram session, delete local files")
    auth_logout.set_defaults(func=cmd_auth_logout)

    channels = sub.add_parser("channels", help="list accessible channels")
    channels.set_defaults(func=cmd_channels_list)

    channel = sub.add_parser("channel", help="channel selection")
    ch_sub = channel.add_subparsers(dest="channel_cmd", required=True)
    ch_sel = ch_sub.add_parser("select", help="select/persist the ingestion channel by id or title")
    ch_sel.add_argument("target", help="channel id (bare or marked), @username, or title fragment")
    ch_sel.set_defaults(func=cmd_channel_select)

    backfill = sub.add_parser("backfill", help="historical import")
    bf_sub = backfill.add_subparsers(dest="backfill_cmd", required=True)
    bf_run = bf_sub.add_parser("run", help="start/continue history import")
    bf_run.add_argument("--max-pages", type=int, default=None, help="stop after N pages (diagnostics/tests)")
    bf_run.set_defaults(func=cmd_backfill_run)
    bf_status = bf_sub.add_parser("status", help="show history import/checkpoint state")
    bf_status.set_defaults(func=cmd_backfill_status)

    serve = sub.add_parser("serve", help="continuous ingestion (live monitoring + backfill + queue)")
    serve.set_defaults(func=cmd_serve)

    status = sub.add_parser("status", help="offline health/sync overview")
    status.set_defaults(func=cmd_status)

    manage = sub.add_parser("manage", help="owner channel-management bot (no trading)")
    manage_sub = manage.add_subparsers(dest="manage_cmd", required=True)
    manage_run = manage_sub.add_parser("run", help="run authentication/channel/backfill bot")
    manage_run.set_defaults(func=cmd_manage_run)
    manage_status = manage_sub.add_parser("status", help="read-only management status")
    manage_status.set_defaults(func=cmd_manage_status)

    control = sub.add_parser("control", help="Telegram owner control bot")
    control_sub = control.add_subparsers(dest="control_cmd", required=True)
    control_status = control_sub.add_parser("status", help="offline control setup/approval overview")
    control_status.set_defaults(func=cmd_control_status)
    control_run = control_sub.add_parser(
        "run", help="run the owner controller only (serve can embed it automatically)"
    )
    control_run.set_defaults(func=cmd_control_run)

    diag = sub.add_parser("diagnostics", help="direct connectivity diagnostics")
    diag_sub = diag.add_subparsers(dest="diagnostics_cmd", required=True)
    dn = diag_sub.add_parser("network", help="verify direct DNS/TCP/TLS to Telegram endpoints")
    dn.set_defaults(func=cmd_diag_network)
    dbot = diag_sub.add_parser("bot", help="verify the configured bot token with getMe")
    dbot.set_defaults(func=cmd_diag_bot)
    dt = diag_sub.add_parser("telegram", help="direct network + real session + channel access")
    dt.set_defaults(func=cmd_diag_telegram)
    dc = diag_sub.add_parser("clock", help="clock health and skew diagnostics")
    dc.set_defaults(func=cmd_diag_clock)
    da = diag_sub.add_parser("audit", help="audit journal health and event counts")
    da.set_defaults(func=cmd_diag_audit)
    do_ = diag_sub.add_parser("orders", help="order intent traces and state diagnostics")
    do_.set_defaults(func=cmd_diag_orders)
    dr = diag_sub.add_parser("reconcile", help="unresolved reconciliation diagnostics")
    dr.set_defaults(func=cmd_diag_reconcile)

    messages = sub.add_parser("messages", help="inspect stored messages")
    m_sub = messages.add_subparsers(dest="messages_cmd", required=True)
    m_list = m_sub.add_parser("list", help="list stored messages (preview)")
    m_list.add_argument("--limit", type=int, default=20)
    m_list.add_argument("--oldest", action="store_true")
    m_list.set_defaults(func=cmd_messages_list)
    m_show = m_sub.add_parser("show", help="show one message (with edit history)")
    m_show.add_argument("id", type=int)
    m_show.set_defaults(func=cmd_messages_show)

    queue = sub.add_parser("queue", help="downstream processing queue")
    q_sub = queue.add_subparsers(dest="queue_cmd", required=True)
    q_list = q_sub.add_parser("list", help="queue snapshot")
    q_list.add_argument("--status", choices=["pending", "processing", "processed", "failed"])
    q_list.add_argument("--limit", type=int, default=20)
    q_list.set_defaults(func=cmd_queue_list)
    q_retry = q_sub.add_parser("retry", help="re-enqueue all failed items")
    q_retry.set_defaults(func=cmd_queue_retry)
    q_rep = q_sub.add_parser("reprocess", help="re-enqueue after parser update")
    q_rep.add_argument("--id", type=int, default=None, help="message id")
    q_rep.add_argument("--all", action="store_true")
    q_rep.set_defaults(func=cmd_queue_reprocess)

    trading = sub.add_parser("trading", help="MEXC Spot signal->order pipeline (live disabled by default)")
    ts_sub = trading.add_subparsers(dest="trading_cmd", required=True)
    ts_status = ts_sub.add_parser("status", help="offline trading configuration and state overview")
    ts_status.set_defaults(func=cmd_trading_status)
    ts_signals = ts_sub.add_parser("signals", help="list parsed signals")
    ts_signals.add_argument("--limit", type=int, default=20)
    ts_signals.set_defaults(func=cmd_trading_signals)
    ts_intents = ts_sub.add_parser("intents", help="list order intents and states")
    ts_intents.add_argument("--limit", type=int, default=20)
    ts_intents.set_defaults(func=cmd_trading_intents)
    ts_repair = ts_sub.add_parser(
        "repair-signals",
        help="audit/repair signals left active by a terminal rejected/failed intent (no orders)",
    )
    ts_repair.add_argument("--limit", type=int, default=50)
    ts_repair.add_argument(
        "--apply", action="store_true",
        help="persist the status correction; default is a read-only dry-run",
    )
    ts_repair.set_defaults(func=cmd_trading_repair_signals)
    ts_parse = ts_sub.add_parser("parse", help="parse one stored message without trading")
    ts_parse.add_argument("id", type=int)
    ts_parse.set_defaults(func=cmd_trading_parse)
    ts_recon = ts_sub.add_parser("reconcile", help="reconcile non-terminal live intents")
    ts_recon.add_argument("--limit", type=int, default=50)
    ts_recon.set_defaults(func=cmd_trading_reconcile)
    ts_notify = ts_sub.add_parser("notifications", help="list trading notification outbox state")
    ts_notify.add_argument("--status", default=None, help="filter by status (pending, failed, ambiguous, sent)")
    ts_notify.add_argument("--limit", type=int, default=50)
    ts_notify.set_defaults(func=cmd_trading_notifications)
    ts_test = ts_sub.add_parser("test-order", help="validate an order via the official /order/test endpoint")
    ts_test.add_argument("--symbol", required=True)
    ts_test.add_argument("--side", choices=["BUY", "SELL"], required=True)
    ts_test.add_argument("--quote", required=True, help="quote amount, e.g. 10 for 10 USDT")
    ts_test.set_defaults(func=cmd_trading_test_order)

    ts_verify = ts_sub.add_parser("verify", help="read-only MEXC API key verification (no order placed)")
    ts_verify.set_defaults(func=cmd_trading_verify)

    ts_preflight = ts_sub.add_parser(
        "preflight",
        help="preflight checks for the MEXC CLI execution backend (no orders placed)",
    )
    ts_preflight.add_argument(
        "--network", action="store_true",
        help="run live doctor and read-only access checks (requires network)",
    )
    ts_preflight.set_defaults(func=cmd_trading_preflight)

    analyze = sub.add_parser(
        "analyze", help="offline signal analysis of persisted messages (no exchange calls)"
    )
    an_sub = analyze.add_subparsers(dest="analyze_cmd", required=True)
    an_common = argparse.ArgumentParser(add_help=False)
    an_common.add_argument(
        "--db-path", default=None,
        help="explicit SQLite path (use a disposable copy for validation)",
    )
    an_replay = an_sub.add_parser(
        "replay", parents=[an_common], help="deterministic, idempotent offline replay"
    )
    an_replay.add_argument("--channel-id", type=int, default=None,
                           help="defaults to the selected channel")
    an_replay.add_argument("--since", default=None, help="ISO date/timestamp lower bound")
    an_replay.add_argument("--until", default=None, help="ISO date/timestamp upper bound")
    an_replay.add_argument("--limit", type=int, default=None, help="bounded sample size")
    an_replay.add_argument("--after-message-id", type=int, default=None,
                           help="resume after this Telegram message id")
    an_replay.add_argument("--exclude-deleted", action="store_true",
                           help="skip tombstoned messages instead of marking candidates")
    an_replay.add_argument("--dry-run", action="store_true", help="parse only; write nothing")
    an_replay.set_defaults(func=cmd_analyze_replay)

    an_summary = an_sub.add_parser(
        "summary", parents=[an_common], help="counts by classification/review/failure"
    )
    an_summary.set_defaults(func=cmd_analyze_summary)

    an_reindex = an_sub.add_parser(
        "reindex", parents=[an_common],
        help="rebuild the per-channel logical signal index from stored messages",
    )
    an_reindex.add_argument("--channel-id", type=int, default=None,
                            help="defaults to the selected channel")
    an_reindex.add_argument("--dry-run", action="store_true",
                            help="classify and plan in memory only; write nothing")
    an_reindex.add_argument("--report-path", default=None,
                            help="write a JSON machine-readable report to this path")
    an_reindex.set_defaults(func=cmd_analyze_reindex)

    an_list = an_sub.add_parser(
        "list", parents=[an_common], help="list candidate signals (no message text)"
    )
    an_list.add_argument("--classification", default=None)
    an_list.add_argument("--parse-status", default=None,
                         choices=["parsed", "needs_review", "failed"])
    an_list.add_argument("--review-status", default=None,
                         choices=["unreviewed", "confirmed", "corrected", "rejected"])
    an_list.add_argument("--include-superseded", action="store_true")
    an_list.add_argument("--limit", type=int, default=50)
    an_list.set_defaults(func=cmd_analyze_list)

    an_show = an_sub.add_parser(
        "show", parents=[an_common], help="show one candidate and its review history"
    )
    an_show.add_argument("candidate_id")
    an_show.add_argument("--show-text", action="store_true",
                         help="also print the private source text (explicit only)")
    an_show.set_defaults(func=cmd_analyze_show)

    an_review = an_sub.add_parser(
        "review", parents=[an_common], help="append a human review/correction"
    )
    an_review.add_argument("candidate_id")
    an_review.add_argument("--decision", choices=["confirm", "correct", "reject"], required=True)
    an_review.add_argument("--reviewer", default=None)
    an_review.add_argument("--note", default=None)
    an_review.add_argument("--set-classification", default=None)
    an_review.add_argument("--set-market-type", default=None,
                           choices=["spot", "non_spot", "unknown"])
    an_review.add_argument("--set-symbol", default=None)
    an_review.add_argument("--set-quote-asset", default=None)
    an_review.add_argument("--set-side", default=None, choices=["buy", "sell"])
    an_review.add_argument("--set-entry", default=None)
    an_review.add_argument("--set-entry-low", default=None)
    an_review.add_argument("--set-entry-high", default=None)
    an_review.add_argument("--set-stop", default=None)
    an_review.add_argument("--set-targets", default=None,
                           help="comma-separated target prices")
    an_review.set_defaults(func=cmd_analyze_review)

    an_fail = an_sub.add_parser(
        "failures", parents=[an_common], help="inspect isolated parsing failures"
    )
    an_fail.add_argument("--all", action="store_true", help="include resolved failures")
    an_fail.add_argument("--limit", type=int, default=50)
    an_fail.set_defaults(func=cmd_analyze_failures)

    an_corpus = an_sub.add_parser("corpus-check", help="validate the redacted parser corpus")
    an_corpus.add_argument("--path", required=True)
    an_corpus.set_defaults(func=cmd_analyze_corpus)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging("DEBUG" if args.verbose else "INFO")
    coro_or_func = args.func(args)
    if asyncio.iscoroutine(coro_or_func):
        code = asyncio.run(coro_or_func)
    else:
        code = coro_or_func
    raise SystemExit(code)


if __name__ == "__main__":
    main()
