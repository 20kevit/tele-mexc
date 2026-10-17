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
    tg-ingest diagnostics proxy|telegram       # connectivity checks
    tg-ingest queue ...                        # downstream processing queue
    tg-ingest auth logout                      # revoke + delete session
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .backfill import BackfillError, run_backfill
from .config import PROJECT_ROOT, ConfigError, load_config
from .db import Database
from .proxy import ProxyError, run_proxy_diagnostics
from .repository import Repository
from .telethon import (
    AuthRequired,
    connect_and_verify,
    interactive_login,
    logout_and_delete_session,
)
from .utils import configure_logging, marked_channel_id

ENV_TEMPLATE = """\
# Telegram API credentials (https://my.telegram.org -> API development tools).
# Keep this file private; it must never be committed.
TELEGRAM_API_ID=
TELEGRAM_API_HASH=

# Proxy for ALL Telegram traffic (verified listener on this VPS):
TELEGRAM_PROXY_TYPE={type}
TELEGRAM_PROXY_HOST={host}
TELEGRAM_PROXY_PORT={port}
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
"""

DEFAULT_PROXY = {"type": "http", "host": "127.0.0.1", "port": 1081}


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


# ------------------------------------------------------------------ commands


def cmd_config_init(args) -> int:
    env_path = PROJECT_ROOT / ".env"
    cfg_path = PROJECT_ROOT / "config" / "config.toml"
    if not env_path.exists():
        env_path.write_text(ENV_TEMPLATE.format(**DEFAULT_PROXY))
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


def cmd_config_check(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    from .proxy import telethon_proxy_arg

    try:
        telethon_proxy_arg(cfg.proxy)
    except ProxyError as exc:
        print(f"proxy config invalid: {exc}", file=sys.stderr)
        return 2
    print("configuration OK")
    print("  api id / api hash: <present and redacted>")
    print(f"  proxy:  {cfg.proxy.type}://{cfg.proxy.host}:{cfg.proxy.port} enabled={cfg.proxy.enabled}")
    print(f"  database: {cfg.db_path}")
    print(f"  session:  {cfg.session_path} .session")
    print(f"  backfill: scope={cfg.backfill_scope} since={cfg.backfill_since}")
    return 0


async def cmd_auth_login(_args) -> int:
    cfg = _load_or_exit()
    try:
        message = await interactive_login(cfg)
    except AuthRequired as exc:
        print(f"Login not completed: {exc}", file=sys.stderr)
        return 3
    print(message)
    return 0


async def cmd_auth_check(_args) -> int:
    cfg = _load_or_exit()
    try:
        client = await connect_and_verify(cfg)
    except AuthRequired as exc:
        print(f"Session status: {exc}")
        return 3
    me = await client.get_me()
    name = (getattr(me, "first_name", None) or "") + (f" @{me.username}" if getattr(me, "username", None) else "")
    print(f"Session status: valid (account: {name or 'unknown'})")
    await client.disconnect()
    return 0


async def cmd_auth_logout(_args) -> int:
    cfg = _load_or_exit()
    outcome = await logout_and_delete_session(cfg)
    print(outcome)
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
    from .service import IngestService

    cfg = _load_or_exit()
    db, repo = _new_repo(cfg)
    svc = IngestService(cfg, repo)
    code = await svc.run()
    return code


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


async def cmd_diag_proxy(_args) -> int:
    cfg = _load_or_exit(require_credentials=False)
    try:
        diag = run_proxy_diagnostics(cfg)
    except ProxyError as exc:
        print(f"PROXY FAILURE: {exc}")
        print("Fix: check the proxy listener is running (ss -tlnp | grep 1081); do NOT remove proxy config.")
        return 1
    print("proxy diagnostics: OK")
    print(f"  listener: {diag.proxy_addr_public}")
    print(f"  {diag.detail}")
    direct = "blocked (expected; proxy is mandatory)" if diag.direct_egress_blocked else "NOT blocked (unexpected)"
    print(f"  direct Telegram egress from VPS: {direct}")
    return 0


async def cmd_diag_telegram(_args) -> int:
    """Distinguishes proxy failure from Telegram auth failure."""

    try:
        proxy_code = await cmd_diag_proxy(_args)
    except SystemExit:
        proxy_code = 1
    if proxy_code != 0:
        print("Telegram check skipped: proxy must work first.")
        return 1
    cfg = _load_or_exit()
    try:
        client = await connect_and_verify(cfg)
    except AuthRequired as exc:
        print(f"Telegram reachability OK through proxy; authentication state: {exc}")
        return 3
    except ProxyError as exc:
        print(f"PROXY FAILURE: {exc}")
        return 1
    print("Telegram reachability OK through proxy; session VALID.")
    selected = None
    try:
        from telethon.tl.types import PeerChannel

        db, repo = _new_repo(cfg)
        selected = repo.selected_channel()
        if selected is not None:
            entity = await client.get_entity(PeerChannel(selected.channel_id))
            print(f"channel access OK: id={selected.channel_id} title={getattr(entity, 'title', '-')!r}")
        await client.disconnect()
    except Exception as exc:  # noqa: BLE001
        print(f"channel verification failed: {exc.__class__.__name__}")
        return 4
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

    diag = sub.add_parser("diagnostics", help="connectivity diagnostics")
    diag_sub = diag.add_subparsers(dest="diagnostics_cmd", required=True)
    dp = diag_sub.add_parser("proxy", help="verify local proxy reaches Telegram DCs")
    dp.set_defaults(func=cmd_diag_proxy)
    dt = diag_sub.add_parser("telegram", help="proxy + Telegram session + channel access")
    dt.set_defaults(func=cmd_diag_telegram)

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
