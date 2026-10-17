# tg-signal-ingest

Telegram channel ingestion service for a single Telegram account and a single
selected signal channel. It persists every channel message (text, media,
edits, deletions, replies, forwards, albums) in a local SQLite database and
exposes a durable downstream processing queue for a future signal parser.

**This phase does not parse signals and does not trade.**

- Runtime: Python 3.12 + Telethon (MTProto) over a local HTTP CONNECT proxy.
- Storage: SQLite with reproducible migrations.
- Config: `.env` (secrets) + private `config.toml` (runtime paths/proxy).
- CLI: `tg-ingest` (setup, auth, channels, backfill, serve, status, diagnostics).

See `docs/OPERATIONS.md` for the full operator guide and
`docs/LIVE_VERIFICATION.md` for the manual Telegram verification procedure.
