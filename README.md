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

## Known limitations (platform-imposed)

- **Deletions during downtime** are not recoverable: Telegram only pushes
  delete events to connected clients. What we can do (and do): mark deletions
  received live, and re-fetch edited messages in the recovery window.
- **Old edits outside the recovery window**: edits made while the service was
  offline and older than `recovery_sweep_size` messages may not be re-fetched.
- **Media contents** (photos/documents) are stored as references and metadata
  only; a photo carrying a signal is marked `requires_ocr` in its media meta
  and is never treated as parsed text. Downloading media is optional and
  disabled by default.
- Delete notifications bound to a channel are used only for that channel;
  the service never accesses unrelated private conversations.
