"""SQLite database bootstrap and schema migrations.

Migrations are embedded, idempotent, and applied in order. The connection uses
WAL mode and a busy timeout so concurrent maintenance commands on a busy
database do not produce spurious failures.
"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Callable
from pathlib import Path

MigrationStep = str | Callable[[sqlite3.Connection], None]


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _add_columns(conn: sqlite3.Connection, table: str, columns: list[tuple[str, str]]) -> None:
    """Add only missing columns.

    SQLite has no ``ADD COLUMN IF NOT EXISTS``.  Several production databases
    were partially upgraded by hand before this migration system supported
    Python steps, so migrations must be able to converge from either the old or
    an intermediate shape instead of assuming a pristine table.
    """

    existing = _columns(conn, table)
    for name, declaration in columns:
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")
            existing.add(name)


def _migration_18_audit_journal(conn: sqlite3.Connection) -> None:
    """Normalize the structured audit journal and preserve legacy rows."""

    _add_columns(
        conn,
        "audit_events",
        [
            ("schema_version", "TEXT NOT NULL DEFAULT '2.0'"),
            ("event_id", "TEXT"),
            ("event_type", "TEXT"),
            ("severity", "TEXT NOT NULL DEFAULT 'INFO'"),
            ("occurred_at_utc", "TEXT"),
            ("recorded_at_utc", "TEXT"),
            ("monotonic_elapsed_ns", "INTEGER DEFAULT 0"),
            ("clock_health", "TEXT"),
            ("process_id", "INTEGER"),
            ("hostname", "TEXT"),
            ("instance_id", "TEXT"),
            ("application_version", "TEXT"),
            ("component", "TEXT"),
            ("operation", "TEXT"),
            ("correlation_id", "TEXT"),
            ("causation_id", "TEXT"),
            ("telegram_channel_id", "INTEGER"),
            ("telegram_message_id", "INTEGER"),
            ("message_edit_version", "INTEGER"),
            ("signal_id", "TEXT"),
            ("intent_id", "TEXT"),
            ("trade_id", "TEXT"),
            ("order_id", "TEXT"),
            ("exchange_order_id", "TEXT"),
            ("symbol", "TEXT"),
            ("reason_code", "TEXT"),
            ("outcome", "TEXT"),
            ("secrets_redacted", "INTEGER NOT NULL DEFAULT 1"),
            ("source_ref", "TEXT"),
            ("evidence_source", "TEXT"),
            ("evidence_limitations", "TEXT"),
            ("payload_json", "TEXT"),
            ("observed_at_utc", "TEXT"),
            ("actor", "TEXT"),
            ("dedupe_key", "TEXT"),
        ],
    )
    # Backfill only from evidence already present in the row.  A legacy row is
    # explicitly labelled as legacy; it is never upgraded into an
    # exchange-confirmed fact.
    conn.execute(
        "UPDATE audit_events SET "
        "event_type = COALESCE(event_type, event), "
        "occurred_at_utc = COALESCE(occurred_at_utc, created_at), "
        "recorded_at_utc = COALESCE(recorded_at_utc, created_at), "
        "component = COALESCE(component, category), "
        "operation = COALESCE(operation, event), "
        "correlation_id = COALESCE(correlation_id, correlation), "
        "event_id = COALESCE(event_id, 'legacy-' || audit_id), "
        "source_ref = COALESCE(source_ref, 'legacy:audit:' || audit_id), "
        "payload_json = COALESCE(payload_json, detail), "
        "secrets_redacted = COALESCE(secrets_redacted, 1)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_audit_event_id "
        "ON audit_events (event_id) WHERE event_id IS NOT NULL"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_audit_dedupe "
        "ON audit_events (dedupe_key) WHERE dedupe_key IS NOT NULL"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_events (correlation_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_category ON audit_events (category, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_events (event_type, severity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_occurred ON audit_events (occurred_at_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_symbol ON audit_events (symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_intent ON audit_events (intent_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_trade ON audit_events (trade_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_exchange_order ON audit_events (exchange_order_id)")


def _migration_19_trade_evidence_columns(conn: sqlite3.Connection) -> None:
    """Per-leg OCO evidence and the release that was already recorded as v17.

    Some deployments recorded migration 17 before its body was extended with
    ``trade_orders.stop_trigger_price``.  Rewriting an applied migration can
    never repair those databases, so the column is converged here.
    """

    _add_columns(
        conn,
        "trade_orders",
        [
            ("stop_trigger_price", "TEXT"),
            ("oco_group_id", "TEXT"),
            ("leg_role", "TEXT"),
            ("last_observed_at", "TEXT"),
            ("evidence_id", "TEXT"),
            ("not_observed_reason", "TEXT"),
            ("cancel_reason", "TEXT"),
            ("canceled_by_leg", "TEXT"),
            ("filled_quote", "TEXT"),
            ("fee_amount", "TEXT"),
            ("fee_asset", "TEXT"),
        ],
    )
    conn.execute(
        "UPDATE trade_orders SET leg_role = CASE "
        "WHEN kind = 'stop_loss' THEN 'stop_loss' ELSE 'take_profit' END "
        "WHERE leg_role IS NULL"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_orders_oco_group "
        "ON trade_orders (oco_group_id, kind)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_trade_orders_exchange "
        "ON trade_orders (exchange_order_id)"
    )
    _add_columns(
        conn,
        "trades",
        [
            ("oco_group_id", "TEXT"),
            ("protection_config_json", "TEXT"),
            ("position_state", "TEXT"),
            ("outcome", "TEXT"),
            ("outcome_reason", "TEXT"),
            ("evidence_status", "TEXT"),
        ],
    )
    conn.execute(
        "UPDATE trades SET position_state = COALESCE(position_state, trade_state), "
        "evidence_status = COALESCE(evidence_status, 'legacy_unverified')"
    )
    _add_columns(
        conn,
        "reconciliation_results",
        [
            ("trade_id", "TEXT"),
            ("order_id", "TEXT"),
            ("evidence_id", "TEXT"),
            ("scope", "TEXT"),
            ("observed_at", "TEXT"),
        ],
    )
    _add_columns(
        conn,
        "messages",
        [
            ("content_sha256", "TEXT"),
            ("source_ref", "TEXT"),
        ],
    )
    _add_columns(conn, "signals", [("source_ref", "TEXT")])


def _migration_20_audit_evidence_notifications(conn: sqlite3.Connection) -> None:
    """Exchange evidence, OCO group model, and delivery-ready notification outbox."""

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exchange_operations (
            operation_id       TEXT PRIMARY KEY,
            schema_version     TEXT NOT NULL DEFAULT '1.0',
            correlation_id     TEXT,
            causation_id       TEXT,
            intent_id          TEXT,
            trade_id           TEXT,
            trade_order_id     TEXT,
            signal_id          TEXT,
            symbol             TEXT,
            operation_type     TEXT NOT NULL,
            operation_class    TEXT NOT NULL CHECK (operation_class IN ('read', 'write')),
            purpose            TEXT,
            transport          TEXT,
            endpoint           TEXT,
            client_order_id    TEXT,
            exchange_order_id  TEXT,
            request_payload    TEXT,
            request_started_at TEXT NOT NULL,
            request_finished_at TEXT,
            write_attempted    INTEGER NOT NULL DEFAULT 0 CHECK (write_attempted IN (0, 1)),
            clock_health       TEXT,
            response_received  INTEGER NOT NULL DEFAULT 0 CHECK (response_received IN (0, 1)),
            response_classification TEXT,
            response_status    TEXT,
            exchange_code      TEXT,
            http_status        INTEGER,
            response_payload   TEXT,
            error_class        TEXT,
            error_message      TEXT,
            rate_limit_wait_ms INTEGER,
            uncertainty        TEXT NOT NULL DEFAULT 'none'
                CHECK (uncertainty IN ('none', 'uncertain', 'reconciled', 'read_only_uncertain')),
            recovery_attempts  INTEGER NOT NULL DEFAULT 0,
            observed_at        TEXT,
            evidence_source    TEXT,
            evidence_limitations TEXT,
            redacted           INTEGER NOT NULL DEFAULT 1 CHECK (redacted IN (0, 1)),
            created_at         TEXT NOT NULL,
            updated_at         TEXT NOT NULL
        )
        """
    )
    _add_columns(conn, "exchange_operations", [("clock_health", "TEXT")])
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_exchange_ops_intent ON exchange_operations (intent_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_exchange_ops_trade ON exchange_operations (trade_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_exchange_ops_client ON exchange_operations (client_order_id)",
        "CREATE INDEX IF NOT EXISTS idx_exchange_ops_exchange ON exchange_operations (exchange_order_id)",
        "CREATE INDEX idx_exchange_ops_uncertain ON exchange_operations (operation_class, uncertainty, created_at)",
    ):
        conn.execute(sql)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oco_groups (
            oco_group_id      TEXT PRIMARY KEY,
            trade_id          TEXT NOT NULL UNIQUE,
            intent_id         TEXT,
            symbol            TEXT NOT NULL,
            state             TEXT NOT NULL DEFAULT 'intended'
                CHECK (state IN ('intended', 'submitting', 'active', 'partial',
                                 'triggered', 'completed', 'canceled', 'uncertain',
                                 'needs_review')),
            intended_config   TEXT,
            protection_status TEXT,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            closed_at         TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_oco_groups_state ON oco_groups (state, updated_at)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS oco_legs (
            leg_id             TEXT PRIMARY KEY,
            oco_group_id       TEXT NOT NULL,
            trade_id           TEXT NOT NULL,
            trade_order_id     TEXT,
            kind               TEXT NOT NULL CHECK (kind IN ('take_profit', 'stop_loss')),
            target             TEXT NOT NULL,
            client_order_id    TEXT,
            exchange_order_id  TEXT,
            intended_price     TEXT,
            intended_trigger_price TEXT,
            intended_qty       TEXT,
            state              TEXT NOT NULL DEFAULT 'intended',
            exchange_status    TEXT,
            filled_qty         TEXT NOT NULL DEFAULT '0',
            filled_quote       TEXT,
            fee_amount         TEXT,
            fee_asset          TEXT,
            cancel_reason      TEXT,
            canceled_by_target TEXT,
            evidence_id        TEXT,
            last_observed_at   TEXT,
            not_observed_reason TEXT,
            created_at         TEXT NOT NULL,
            updated_at         TEXT NOT NULL,
            UNIQUE (oco_group_id, target)
        )
        """
    )
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_oco_legs_trade ON oco_legs (trade_id, kind)",
        "CREATE INDEX IF NOT EXISTS idx_oco_legs_state ON oco_legs (state)",
        "CREATE INDEX IF NOT EXISTS idx_oco_legs_exchange ON oco_legs (exchange_order_id)",
    ):
        conn.execute(sql)

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_preferences (
            role       TEXT NOT NULL,
            category   TEXT NOT NULL,
            enabled    INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
            updated_at TEXT NOT NULL,
            PRIMARY KEY (role, category)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_callbacks (
            token           TEXT PRIMARY KEY,
            notification_id INTEGER NOT NULL,
            role            TEXT NOT NULL,
            action          TEXT NOT NULL CHECK (action IN ('expand', 'signal_source', 'ack')),
            created_at      TEXT NOT NULL,
            expires_at      TEXT NOT NULL,
            used_at         TEXT,
            use_count       INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notification_callbacks_notification "
        "ON notification_callbacks (notification_id)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_delivery_attempts (
            attempt_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            notification_id  INTEGER NOT NULL,
            attempt_no       INTEGER NOT NULL,
            attempted_at     TEXT NOT NULL,
            outcome          TEXT NOT NULL,
            error            TEXT,
            telegram_message_id INTEGER,
            retry_after_s    INTEGER,
            ambiguous        INTEGER NOT NULL DEFAULT 0 CHECK (ambiguous IN (0, 1))
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notification_attempts "
        "ON notification_delivery_attempts (notification_id, attempt_no)"
    )

    columns = _columns(conn, "trading_notifications")
    if "category" not in columns:
        # The v16 CHECK constraint only allowed pending/sent/failed.  SQLite
        # cannot alter a CHECK constraint, so rebuild once while preserving
        # every outbox row and notification id.
        conn.execute("ALTER TABLE trading_notifications RENAME TO trading_notifications_v16")
        conn.execute(
            """
            CREATE TABLE trading_notifications (
                notification_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                event_key         TEXT NOT NULL,
                signal_id         TEXT,
                intent_id         TEXT,
                category          TEXT NOT NULL DEFAULT 'legacy',
                event_type        TEXT NOT NULL DEFAULT 'legacy',
                role              TEXT NOT NULL CHECK (role IN ('owner', 'developer')),
                chat_id           INTEGER NOT NULL,
                text              TEXT NOT NULL,
                compact_text      TEXT,
                expanded_text     TEXT,
                status            TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'sending', 'sent', 'failed',
                                      'ambiguous', 'suppressed', 'canceled')),
                attempts          INTEGER NOT NULL DEFAULT 0,
                last_error        TEXT,
                next_attempt_at   TEXT,
                claim_token       TEXT,
                claimed_at        TEXT,
                telegram_message_id INTEGER,
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL,
                sent_at           TEXT,
                UNIQUE (event_key, role)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO trading_notifications (
                notification_id, event_key, signal_id, intent_id, category, event_type,
                role, chat_id, text, compact_text, expanded_text, status, attempts,
                last_error, next_attempt_at, created_at, updated_at, sent_at
            )
            SELECT notification_id, event_key, signal_id, intent_id, 'legacy', 'legacy',
                   role, chat_id, text, text, NULL, status, attempts,
                   last_error, updated_at, created_at, updated_at, sent_at
            FROM trading_notifications_v16
            """
        )
        conn.execute("DROP TABLE trading_notifications_v16")
    else:
        _add_columns(
            conn,
            "trading_notifications",
            [
                ("event_type", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("compact_text", "TEXT"),
                ("expanded_text", "TEXT"),
                ("next_attempt_at", "TEXT"),
                ("claim_token", "TEXT"),
                ("claimed_at", "TEXT"),
                ("telegram_message_id", "INTEGER"),
            ],
        )
    for sql in (
        "CREATE INDEX IF NOT EXISTS idx_trading_notifications_status "
        "ON trading_notifications (status, next_attempt_at, notification_id)",
        "CREATE INDEX IF NOT EXISTS idx_trading_notifications_event_key "
        "ON trading_notifications (event_key)",
        "CREATE INDEX IF NOT EXISTS idx_trading_notifications_signal "
        "ON trading_notifications (signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_trading_notifications_intent "
        "ON trading_notifications (intent_id)",
    ):
        conn.execute(sql)

    # Developer defaults: every meaningful category.  Owner defaults: material
    # trading/exchange evidence only.  Code applies the same defaults, so a
    # missing row is never interpreted as "send everything".
    now = "2026-01-01T00:00:00Z"
    for role, category, enabled in (
        ("owner", "signal_decision", 0),
        ("owner", "signal_accepted", 1),
        ("owner", "exchange_operation", 1),
        ("owner", "order_lifecycle", 1),
        ("owner", "oco_lifecycle", 1),
        ("owner", "take_profit", 1),
        ("owner", "stop_loss", 1),
        ("owner", "reconciliation", 1),
        ("owner", "critical", 1),
        ("owner", "diagnostic", 0),
        ("owner", "notification_delivery", 0),
        ("owner", "recovery", 0),
        ("developer", "signal_decision", 1),
        ("developer", "signal_accepted", 1),
        ("developer", "exchange_operation", 1),
        ("developer", "order_lifecycle", 1),
        ("developer", "oco_lifecycle", 1),
        ("developer", "take_profit", 1),
        ("developer", "stop_loss", 1),
        ("developer", "reconciliation", 1),
        ("developer", "critical", 1),
        ("developer", "diagnostic", 1),
        ("developer", "notification_delivery", 1),
        ("developer", "recovery", 1),
    ):
        conn.execute(
            "INSERT OR IGNORE INTO notification_preferences (role, category, enabled, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (role, category, enabled, now),
        )


MIGRATIONS: list[tuple[int, MigrationStep]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id   INTEGER PRIMARY KEY,
            access_hash  INTEGER NOT NULL,
            username     TEXT,
            title        TEXT NOT NULL,
            is_selected  INTEGER NOT NULL DEFAULT 0 CHECK (is_selected IN (0, 1)),
            selected_at  TEXT,
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
            channel_id          INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            datetime            TEXT NOT NULL,
            edit_datetime       TEXT,
            text                TEXT,
            caption             TEXT,
            media_type          TEXT,
            media_meta          TEXT,
            grouped_id          INTEGER,
            reply_to_message_id INTEGER,
            fwd_from_name       TEXT,
            fwd_from_id         INTEGER,
            fwd_post_message_id INTEGER,
            raw_json            TEXT NOT NULL,
            source              TEXT NOT NULL CHECK (source IN ('backfill', 'live', 'recovery')),
            processing_version  INTEGER NOT NULL DEFAULT 0,
            deleted             INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0, 1)),
            deleted_at          TEXT,
            first_seen          TEXT NOT NULL,
            last_updated        TEXT NOT NULL,
            PRIMARY KEY (channel_id, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_messages_date ON messages (channel_id, datetime);
        CREATE INDEX IF NOT EXISTS idx_messages_version ON messages (channel_id, processing_version);

        CREATE TABLE IF NOT EXISTS message_edits (
            channel_id    INTEGER NOT NULL,
            message_id    INTEGER NOT NULL,
            edit_seq      INTEGER NOT NULL,
            edit_datetime TEXT,
            captured_at   TEXT NOT NULL,
            text          TEXT,
            caption       TEXT,
            source        TEXT,
            PRIMARY KEY (channel_id, message_id, edit_seq)
        );

        CREATE TABLE IF NOT EXISTS processing_queue (
            queue_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id      INTEGER NOT NULL,
            message_id      INTEGER NOT NULL,
            item_type       TEXT NOT NULL CHECK (item_type IN ('message', 'edit', 'delete')),
            status          TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
            targets_version INTEGER NOT NULL,
            attempts        INTEGER NOT NULL DEFAULT 0,
            available_at    TEXT NOT NULL,
            claimed_at      TEXT,
            last_error      TEXT,
            created_at      TEXT NOT NULL,
            processed_at    TEXT,
            UNIQUE (channel_id, message_id, item_type, targets_version)
        );
        CREATE INDEX IF NOT EXISTS idx_queue_status ON processing_queue (status, available_at);

        CREATE TABLE IF NOT EXISTS sync_state (
            channel_id                 INTEGER PRIMARY KEY,
            backfill_target_mode       TEXT CHECK (backfill_target_mode IN ('full', 'since')
                                      OR backfill_target_mode IS NULL),
            backfill_since             TEXT,
            backfill_status            TEXT NOT NULL DEFAULT 'not_started'
                CHECK (backfill_status IN ('not_started', 'in_progress', 'done')),
            backfill_next_max_id       INTEGER,
            backfill_oldest_id_seen    INTEGER,
            backfill_message_count     INTEGER NOT NULL DEFAULT 0,
            backfill_started_at        TEXT,
            backfill_completed_at      TEXT,
            live_last_seen_message_id  INTEGER,
            live_last_event_at         TEXT,
            live_last_sync_at          TEXT,
            updated_at                 TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_state (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        """
        CREATE TABLE IF NOT EXISTS signals (
            signal_id          TEXT PRIMARY KEY,
            channel_id         INTEGER NOT NULL,
            message_id         INTEGER NOT NULL,
            edit_version       INTEGER NOT NULL,
            parser_version     INTEGER NOT NULL,
            signal_kind        TEXT NOT NULL CHECK (signal_kind IN
                               ('alert', 'update', 'cancel', 'result', 'info', 'ambiguous')),
            symbol             TEXT,
            action             TEXT CHECK (action IN ('buy', 'sell') OR action IS NULL),
            market_type        TEXT NOT NULL DEFAULT 'spot',
            entry_price        TEXT,
            entry_low_price    TEXT,
            entry_high_price   TEXT,
            target_prices      TEXT,
            stop_loss          TEXT,
            order_type         TEXT,
            amount_text        TEXT,
            fingerprint_sha256 TEXT NOT NULL,
            status             TEXT NOT NULL CHECK (status IN
                               ('parsed', 'rejected', 'validated', 'planned', 'superseded',
                                'canceled', 'deleted')),
            rejection_reason   TEXT,
            created_at         TEXT NOT NULL,
            updated_at         TEXT NOT NULL,
            UNIQUE (channel_id, message_id, edit_version)
        );
        CREATE INDEX IF NOT EXISTS idx_signals_message ON signals (channel_id, message_id);
        CREATE INDEX IF NOT EXISTS idx_signals_status ON signals (status);

        CREATE TABLE IF NOT EXISTS order_intents (
            intent_id       TEXT PRIMARY KEY,
            signal_id       TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            client_order_id TEXT UNIQUE,
            symbol          TEXT NOT NULL,
            side            TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
            order_type      TEXT NOT NULL CHECK (order_type IN ('market', 'limit', 'test_only')),
            quote_amount    TEXT,
            base_quantity   TEXT,
            limit_price     TEXT,
            mode            TEXT NOT NULL CHECK (mode IN ('sim', 'testonly', 'live')),
            state           TEXT NOT NULL CHECK (state IN
                            ('planned', 'submitting', 'submitted', 'reconciling', 'open',
                             'filled', 'canceled', 'failed', 'rejected',
                             'unknown_requires_reconciliation', 'canceled_local')),
            exchange_order_id TEXT,
            exchange_status   TEXT,
            executed_qty      TEXT,
            cumulative_quote  TEXT,
            reject_reason     TEXT,
            create_msg_id     INTEGER NOT NULL,
            create_channel_id INTEGER NOT NULL,
            planned_at     TEXT NOT NULL,
            submitted_at   TEXT,
            terminal_at    TEXT,
            updated_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_intents_state ON order_intents (state);
        CREATE INDEX IF NOT EXISTS idx_intents_signal ON order_intents (signal_id);

        CREATE TABLE IF NOT EXISTS exchange_orders (
            exchange_order_id TEXT PRIMARY KEY,
            client_order_id   TEXT,
            intent_id         TEXT,
            symbol            TEXT NOT NULL,
            side              TEXT NOT NULL,
            order_type        TEXT NOT NULL,
            price             TEXT,
            orig_qty          TEXT,
            executed_qty      TEXT,
            cumulative_quote  TEXT,
            status            TEXT NOT NULL CHECK (status IN
                              ('NEW', 'FILLED', 'PARTIALLY_FILLED', 'CANCELED',
                               'PARTIALLY_CANCELED', 'UNKNOWN')),
            time_in_force     TEXT,
            exchange_time_ms  INTEGER,
            discovered_in     TEXT NOT NULL,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_exchange_orders_intent ON exchange_orders (intent_id);

        CREATE TABLE IF NOT EXISTS order_status_history (
            history_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id         TEXT,
            exchange_order_id TEXT,
            previous_state    TEXT,
            new_state         TEXT,
            note              TEXT NOT NULL,
            changed_at        TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_status_history_intent ON order_status_history (intent_id);

        CREATE TABLE IF NOT EXISTS order_fills (
            fill_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange_order_id TEXT NOT NULL,
            symbol            TEXT NOT NULL,
            price             TEXT NOT NULL,
            quantity          TEXT NOT NULL,
            quote_amount      TEXT,
            fee_asset         TEXT,
            fee_amount        TEXT,
            exchange_trade_id TEXT,
            is_buyer          INTEGER,
            is_maker          INTEGER,
            trade_time_ms     INTEGER,
            source            TEXT NOT NULL CHECK (source IN ('reconciliation', 'rest_query')),
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            UNIQUE (exchange_order_id, exchange_trade_id)
        );

        CREATE TABLE IF NOT EXISTS reconciliation_results (
            recon_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            intent_id      TEXT,
            client_order_id TEXT,
            local_state    TEXT,
            exchange_state TEXT,
            outcome        TEXT NOT NULL CHECK (outcome IN
                           ('consistent', 'resolved', 'discrepancy', 'ambiguous', 'not_found')),
            detail         TEXT,
            created_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS audit_events (
            audit_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            correlation  TEXT NOT NULL,
            category     TEXT NOT NULL CHECK (category IN
                         ('telegram', 'parse', 'validate', 'risk', 'mexc_request',
                          'lifecycle', 'reconcile', 'config', 'emergency')),
            event        TEXT NOT NULL,
            detail       TEXT,
            created_at   TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_audit_correlation ON audit_events (correlation);
        CREATE INDEX IF NOT EXISTS idx_audit_category ON audit_events (category, created_at);

        -- daily spending cap tracking (market-day key, e.g. '2026-10-19')
        CREATE TABLE IF NOT EXISTS trading_daily_spend (
            day_key    TEXT PRIMARY KEY,
            quote_spent TEXT NOT NULL DEFAULT '0',
            order_count INTEGER NOT NULL DEFAULT 0,
            updated_at  TEXT NOT NULL
        );
        """,
    ),
    (
        3,
        """
        CREATE INDEX IF NOT EXISTS idx_signals_fingerprint
            ON signals (channel_id, fingerprint_sha256, created_at);
        CREATE INDEX IF NOT EXISTS idx_intents_client_order
            ON order_intents (client_order_id);
        CREATE INDEX IF NOT EXISTS idx_intents_idempotency
            ON order_intents (idempotency_key);
        """,
    ),
    (
        4,
        """
        ALTER TABLE order_intents ADD COLUMN fill_sync_required INTEGER NOT NULL DEFAULT 0
            CHECK (fill_sync_required IN (0, 1));
        ALTER TABLE order_intents ADD COLUMN fill_synced_at TEXT;
        CREATE INDEX IF NOT EXISTS idx_intents_fill_sync
            ON order_intents (mode, fill_sync_required, state);
        """,
    ),
    (
        5,
        """
        CREATE TABLE IF NOT EXISTS control_setup (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            setup_state TEXT NOT NULL DEFAULT 'not_configured',
            telegram_state TEXT NOT NULL DEFAULT 'unauthenticated',
            telegram_authorized_at TEXT,
            telegram_account_label TEXT,
            mexc_state TEXT NOT NULL DEFAULT 'not_configured',
            mexc_verified_at TEXT,
            mexc_label TEXT,
            selected_channel_id INTEGER,
            selected_channel_title TEXT,
            learning_state TEXT NOT NULL DEFAULT 'not_started',
            learning_streak INTEGER NOT NULL DEFAULT 0,
            learning_completed_at TEXT,
            execution_mode TEXT NOT NULL DEFAULT 'approval'
                CHECK (execution_mode IN ('approval', 'auto')),
            auto_enabled_at TEXT,
            paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
            pause_reason TEXT,
            updated_at TEXT NOT NULL
        );
        INSERT OR IGNORE INTO control_setup (id, updated_at) VALUES (1, datetime('now'));

        CREATE TABLE IF NOT EXISTS control_settings (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS control_flow (
            id         INTEGER PRIMARY KEY CHECK (id = 1),
            state      TEXT NOT NULL DEFAULT 'idle',
            payload    TEXT,
            updated_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT OR IGNORE INTO control_flow (id, state, updated_at) VALUES (1, 'idle', datetime('now'));

        CREATE TABLE IF NOT EXISTS control_learning_samples (
            sample_id       TEXT PRIMARY KEY,
            channel_id      INTEGER NOT NULL,
            message_id      INTEGER NOT NULL,
            edit_version    INTEGER NOT NULL,
            message_text    TEXT,
            extracted_json  TEXT NOT NULL,
            parser_kind     TEXT NOT NULL,
            actionable      INTEGER NOT NULL CHECK (actionable IN (0, 1)),
            decision        TEXT NOT NULL DEFAULT 'pending'
                CHECK (decision IN ('pending', 'approved', 'rejected')),
            decision_reason TEXT,
            sequence        INTEGER NOT NULL,
            created_at      TEXT NOT NULL,
            decided_at      TEXT,
            UNIQUE (channel_id, message_id, edit_version)
        );
        CREATE INDEX IF NOT EXISTS idx_control_samples_decision
            ON control_learning_samples (decision, sequence);

        CREATE TABLE IF NOT EXISTS control_approvals (
            approval_id    TEXT PRIMARY KEY,
            intent_id      TEXT NOT NULL UNIQUE,
            signal_id      TEXT NOT NULL,
            intent_version TEXT NOT NULL,
            nonce          TEXT NOT NULL,
            status         TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'failed')),
            chat_id        INTEGER,
            message_id     INTEGER,
            created_at     TEXT NOT NULL,
            expires_at     TEXT NOT NULL,
            decided_at     TEXT,
            consumed_at    TEXT,
            result_detail  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_control_approvals_status
            ON control_approvals (status, expires_at);
        CREATE INDEX IF NOT EXISTS idx_control_approvals_intent
            ON control_approvals (intent_id);

        CREATE TABLE IF NOT EXISTS control_updates (
            update_id INTEGER PRIMARY KEY,
            seen_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS control_secrets (
            name       TEXT PRIMARY KEY,
            alg        TEXT NOT NULL,
            salt       TEXT NOT NULL,
            nonce      TEXT NOT NULL,
            ciphertext TEXT NOT NULL,
            mac        TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS control_audit (
            audit_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            event      TEXT NOT NULL,
            detail     TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS control_notices (
            intent_id  TEXT PRIMARY KEY,
            kind       TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """,
    ),
    (
        6,
        """
        ALTER TABLE messages ADD COLUMN sender_id INTEGER;
        ALTER TABLE messages ADD COLUMN sender_name TEXT;
        ALTER TABLE messages ADD COLUMN post_author TEXT;
        ALTER TABLE messages ADD COLUMN entities_json TEXT;

        CREATE TABLE IF NOT EXISTS mgmt_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            telegram_state TEXT NOT NULL DEFAULT 'unauthenticated'
                CHECK (telegram_state IN ('unauthenticated', 'pending', 'authenticated')),
            telegram_label TEXT,
            telegram_authorized_at TEXT,
            selected_channel_id INTEGER,
            selected_channel_title TEXT,
            selected_channel_username TEXT,
            selection_snapshot_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT OR IGNORE INTO mgmt_state (id, created_at, updated_at)
            VALUES (1, datetime('now'), datetime('now'));

        CREATE TABLE IF NOT EXISTS mgmt_auth_flow (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            state TEXT NOT NULL DEFAULT 'idle'
                CHECK (state IN ('idle', 'auth_phone', 'auth_code', 'auth_password')),
            updated_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT OR IGNORE INTO mgmt_auth_flow (id, state, updated_at)
            VALUES (1, 'idle', datetime('now'));

        CREATE TABLE IF NOT EXISTS mgmt_updates (
            update_id INTEGER PRIMARY KEY,
            seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mgmt_callbacks (
            token TEXT PRIMARY KEY,
            owner_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            payload_json TEXT,
            snapshot_id TEXT,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mgmt_callbacks_expiry
            ON mgmt_callbacks (expires_at);

        CREATE TABLE IF NOT EXISTS mgmt_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            channel_count INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mgmt_snapshot_channels (
            snapshot_id TEXT NOT NULL,
            position INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            access_hash INTEGER NOT NULL,
            title TEXT NOT NULL,
            username TEXT,
            kind TEXT NOT NULL,
            last_activity TEXT,
            PRIMARY KEY (snapshot_id, position),
            UNIQUE (snapshot_id, channel_id)
        );

        CREATE TABLE IF NOT EXISTS mgmt_ingestions (
            job_id TEXT PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            channel_title TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'running', 'completed', 'partial',
                                  'failed', 'cancelled', 'interrupted')),
            checkpoint_next_max_id INTEGER,
            oldest_seen_id INTEGER,
            newest_seen_id INTEGER,
            discovered INTEGER NOT NULL DEFAULT 0,
            inserted INTEGER NOT NULL DEFAULT 0,
            updated INTEGER NOT NULL DEFAULT 0,
            unchanged INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            total_stored INTEGER NOT NULL DEFAULT 0,
            cancel_requested INTEGER NOT NULL DEFAULT 0
                CHECK (cancel_requested IN (0, 1)),
            error TEXT,
            created_at TEXT NOT NULL,
            started_at TEXT,
            updated_at TEXT NOT NULL,
            completed_at TEXT,
            last_notified_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_mgmt_ingestions_status
            ON mgmt_ingestions (status, created_at);
        CREATE INDEX IF NOT EXISTS idx_mgmt_ingestions_channel
            ON mgmt_ingestions (channel_id, created_at);

        CREATE TABLE IF NOT EXISTS mgmt_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event TEXT NOT NULL,
            detail TEXT,
            created_at TEXT NOT NULL
        );
        """,
    ),
    (
        7,
        """
        -- Per-operator login stages. The legacy singleton mgmt_auth_flow table
        -- is intentionally left in place but is no longer read or written.
        CREATE TABLE IF NOT EXISTS mgmt_operator_flows (
            operator_id INTEGER PRIMARY KEY,
            state TEXT NOT NULL DEFAULT 'idle'
                CHECK (state IN ('idle', 'auth_phone', 'auth_code', 'auth_password')),
            updated_at TEXT NOT NULL,
            expires_at TEXT
        );

        -- Per-operator channel browsing state so one operator's stale page or
        -- selection cannot be replayed by the other operator.
        CREATE TABLE IF NOT EXISTS mgmt_operator_channels (
            operator_id INTEGER PRIMARY KEY,
            selected_channel_id INTEGER,
            selected_channel_title TEXT,
            selected_channel_username TEXT,
            selection_snapshot_id TEXT,
            updated_at TEXT NOT NULL
        );

        ALTER TABLE mgmt_snapshots ADD COLUMN operator_id INTEGER;
        CREATE INDEX IF NOT EXISTS idx_mgmt_snapshots_operator
            ON mgmt_snapshots (operator_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_mgmt_callbacks_owner
            ON mgmt_callbacks (owner_id, used_at);
        """,
    ),
    (
        8,
        """
        -- Offline, non-trading analysis of already-persisted Telegram messages.
        -- Machine candidates are immutable per (source message, edit version,
        -- parser version) and human corrections live in the append-only review
        -- table and never rewrite the Telegram source row or the machine
        -- candidate fields.
        CREATE TABLE IF NOT EXISTS signal_candidates (
            candidate_id        TEXT PRIMARY KEY,
            channel_id          INTEGER NOT NULL,
            message_id          INTEGER NOT NULL,
            edit_version        INTEGER NOT NULL,
            source_datetime     TEXT NOT NULL,
            source_text_sha256  TEXT NOT NULL,
            parser_version      INTEGER NOT NULL,
            classification      TEXT NOT NULL CHECK (classification IN
                                ('new_signal', 'modification', 'cancellation',
                                 'target_update', 'stop_loss_update', 'result',
                                 'non_signal', 'ambiguous')),
            market_type         TEXT NOT NULL CHECK (market_type IN
                                ('spot', 'non_spot', 'unknown')),
            symbol              TEXT,
            quote_asset         TEXT,
            side                TEXT CHECK (side IN ('buy', 'sell') OR side IS NULL),
            entry_price         TEXT,
            entry_low_price     TEXT,
            entry_high_price    TEXT,
            stop_loss           TEXT,
            targets_json        TEXT NOT NULL DEFAULT '[]',
            order_type          TEXT,
            referenced_message_id INTEGER,
            parent_candidate_id TEXT,
            relationship        TEXT NOT NULL DEFAULT 'none' CHECK (relationship IN
                                ('none', 'reply', 'explicit_reference', 'unique_symbol',
                                 'all_symbols', 'ambiguous')),
            parse_status        TEXT NOT NULL CHECK (parse_status IN
                                ('parsed', 'needs_review', 'failed')),
            field_confidence_json TEXT NOT NULL DEFAULT '{}',
            ambiguities_json    TEXT NOT NULL DEFAULT '[]',
            idempotency_key     TEXT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'current' CHECK (status IN
                                ('current', 'superseded', 'source_deleted')),
            review_status       TEXT NOT NULL DEFAULT 'unreviewed' CHECK (review_status IN
                                ('unreviewed', 'confirmed', 'corrected', 'rejected')),
            reviewed_at         TEXT,
            created_at          TEXT NOT NULL,
            updated_at          TEXT NOT NULL,
            UNIQUE (channel_id, message_id, edit_version, parser_version),
            UNIQUE (idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS idx_signal_candidates_message
            ON signal_candidates (channel_id, message_id);
        CREATE INDEX IF NOT EXISTS idx_signal_candidates_classification
            ON signal_candidates (classification, parse_status);
        CREATE INDEX IF NOT EXISTS idx_signal_candidates_review
            ON signal_candidates (review_status);

        CREATE TABLE IF NOT EXISTS signal_candidate_reviews (
            review_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id   TEXT NOT NULL,
            reviewer       TEXT NOT NULL,
            decision       TEXT NOT NULL CHECK (decision IN
                           ('confirm', 'correct', 'reject')),
            corrected_json TEXT,
            note           TEXT,
            created_at     TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_signal_reviews_candidate
            ON signal_candidate_reviews (candidate_id, review_id);

        CREATE TABLE IF NOT EXISTS analysis_runs (
            run_id         TEXT PRIMARY KEY,
            parser_version INTEGER NOT NULL,
            scope_json     TEXT NOT NULL,
            status         TEXT NOT NULL CHECK (status IN
                           ('running', 'completed', 'failed')),
            counts_json    TEXT NOT NULL DEFAULT '{}',
            started_at     TEXT NOT NULL,
            finished_at    TEXT,
            error          TEXT
        );

        CREATE TABLE IF NOT EXISTS analysis_failures (
            failure_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id     INTEGER NOT NULL,
            message_id     INTEGER NOT NULL,
            edit_version   INTEGER NOT NULL,
            parser_version INTEGER NOT NULL,
            error_type     TEXT NOT NULL,
            error_message  TEXT NOT NULL,
            attempts       INTEGER NOT NULL DEFAULT 1,
            first_failed_at TEXT NOT NULL,
            last_failed_at TEXT NOT NULL,
            resolved_at    TEXT,
            UNIQUE (channel_id, message_id, edit_version, parser_version)
        );
        CREATE INDEX IF NOT EXISTS idx_analysis_failures_unresolved
            ON analysis_failures (resolved_at, last_failed_at);
        """,
    ),
    (
        9,
        """
        -- Developer-initiated, owner-authorized validation import. The request
        -- is deliberately separate from the ingestion job: authorization
        -- never starts collection by itself, and an approved request can be
        -- used exactly once to create one bounded job.
        CREATE TABLE IF NOT EXISTS mgmt_import_requests (
            request_id TEXT PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            channel_title TEXT NOT NULL,
            channel_access_hash INTEGER,
            selection_snapshot_id TEXT,
            max_messages INTEGER NOT NULL CHECK (max_messages BETWEEN 1 AND 100),
            status TEXT NOT NULL CHECK (status IN (
                'requested', 'approved', 'denied', 'expired', 'started',
                'review', 'completed', 'failed', 'cancelled'
            )),
            requested_by INTEGER NOT NULL,
            requested_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            decided_by INTEGER,
            decided_at TEXT,
            job_id TEXT,
            imported_min_id INTEGER,
            imported_max_id INTEGER,
            candidate_count INTEGER NOT NULL DEFAULT 0,
            reviewed_count INTEGER NOT NULL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_mgmt_import_requests_status
            ON mgmt_import_requests (status, created_at);
        CREATE INDEX IF NOT EXISTS idx_mgmt_import_requests_job
            ON mgmt_import_requests (job_id);

        -- One in-progress correction draft per operator. Draft text is operator
        -- input, never Telegram message content, and expires automatically.
        CREATE TABLE IF NOT EXISTS mgmt_review_drafts (
            operator_id INTEGER PRIMARY KEY,
            request_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            page_index INTEGER NOT NULL DEFAULT 0,
            fields_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        ALTER TABLE mgmt_operator_channels
            ADD COLUMN selected_channel_access_hash INTEGER;

        ALTER TABLE mgmt_ingestions ADD COLUMN max_messages INTEGER;
        ALTER TABLE mgmt_ingestions ADD COLUMN request_id TEXT;
        CREATE INDEX IF NOT EXISTS idx_mgmt_ingestions_request
            ON mgmt_ingestions (request_id);
        """,
    ),
    (
        10,
        """
        -- Structured channel-format fields (parser version >= 2) on the
        -- immutable per-message candidates. All values stay nullable: missing
        -- means "not in the source", never an invented default.
        ALTER TABLE signal_candidates ADD COLUMN logical_id INTEGER;
        ALTER TABLE signal_candidates ADD COLUMN followed_logical_id INTEGER;
        ALTER TABLE signal_candidates ADD COLUMN direction TEXT;
        ALTER TABLE signal_candidates ADD COLUMN exchange TEXT;
        ALTER TABLE signal_candidates ADD COLUMN symbol_raw TEXT;
        ALTER TABLE signal_candidates ADD COLUMN timeframe INTEGER;
        ALTER TABLE signal_candidates ADD COLUMN full_target TEXT;
        ALTER TABLE signal_candidates ADD COLUMN deposit_pct TEXT;
        ALTER TABLE signal_candidates ADD COLUMN suggested_leverage TEXT;
        ALTER TABLE signal_candidates ADD COLUMN leverage_pct TEXT;
        ALTER TABLE signal_candidates ADD COLUMN follow_up_event TEXT;
        ALTER TABLE signal_candidates ADD COLUMN target_index INTEGER;
        ALTER TABLE signal_candidates ADD COLUMN profit_pct TEXT;
        ALTER TABLE signal_candidates ADD COLUMN event_price TEXT;
        ALTER TABLE signal_candidates ADD COLUMN source_date_text TEXT;
        ALTER TABLE signal_candidates ADD COLUMN source_time_text TEXT;
        ALTER TABLE signal_candidates ADD COLUMN source_timestamp_iso TEXT;
        CREATE INDEX IF NOT EXISTS idx_signal_candidates_logical
            ON signal_candidates (channel_id, logical_id);

        -- Logical signal index: one row per (channel, signal-ID) with the
        -- extracted fields of the initial signal message. This is historical
        -- review data: SHORT signals are stored normally but the execution
        -- status stays 'none' until a separate, explicit execution workflow
        -- (which must refuse SHORT signals) ever sets it.
        CREATE TABLE IF NOT EXISTS channel_signals (
            channel_id          INTEGER NOT NULL,
            logical_id          INTEGER NOT NULL,
            direction           TEXT CHECK (direction IN ('long', 'short') OR direction IS NULL),
            exchange            TEXT,
            symbol_raw          TEXT,
            symbol              TEXT,
            quote_asset         TEXT,
            timeframe           INTEGER,
            entry_price         TEXT,
            stop_loss           TEXT,
            targets_json        TEXT NOT NULL DEFAULT '[]',
            full_target         TEXT,
            deposit_pct         TEXT,
            suggested_leverage  TEXT,
            leverage_pct        TEXT,
            source_date_text    TEXT,
            source_time_text    TEXT,
            source_timestamp_iso TEXT,
            origin_message_id   INTEGER NOT NULL,
            edit_version        INTEGER NOT NULL DEFAULT 0,
            parser_version      INTEGER NOT NULL,
            candidate_id        TEXT,
            classification      TEXT,
            parse_status        TEXT,
            ambiguities_json    TEXT NOT NULL DEFAULT '[]',
            status              TEXT NOT NULL DEFAULT 'active'
                                CHECK (status IN
                                ('active', 'needs_review', 'duplicate_origin',
                                 'source_edited')),
            execution_status    TEXT NOT NULL DEFAULT 'none'
                                CHECK (execution_status IN
                                ('none', 'eligible_pending_review')),
            created_at          TEXT NOT NULL,
            updated_at          TEXT NOT NULL,
            PRIMARY KEY (channel_id, logical_id),
            FOREIGN KEY (channel_id, origin_message_id)
                REFERENCES messages (channel_id, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_channel_signals_origin
            ON channel_signals (channel_id, origin_message_id);
        CREATE INDEX IF NOT EXISTS idx_channel_signals_symbol
            ON channel_signals (channel_id, symbol);

        -- Message-to-signal association. One row per source message: either a
        -- resolved link ('linked'), a recorded but unresolved reference, or a
        -- recorded conflict (both identifiers present and disagreeing).
        -- Rebuilt idempotently by the reindexer with no duplicate relationships.
        CREATE TABLE IF NOT EXISTS signal_message_links (
            channel_id      INTEGER NOT NULL,
            message_id      INTEGER NOT NULL,
            edit_version    INTEGER NOT NULL DEFAULT 0,
            candidate_id    TEXT,
            logical_id      INTEGER,
            reply_to_message_id INTEGER,
            method          TEXT CHECK (method IN
                            ('explicit_id', 'reply', 'both', 'none')
                            OR method IS NULL),
            status          TEXT NOT NULL
                            CHECK (status IN ('linked', 'unresolved', 'conflict', 'no_reference')),
            conflict_with_logical_id INTEGER,
            reason          TEXT,
            parser_version  INTEGER,
            classification  TEXT,
            linked_at       TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            PRIMARY KEY (channel_id, message_id),
            FOREIGN KEY (channel_id, message_id)
                REFERENCES messages (channel_id, message_id)
        );
        -- Note: there is deliberately no foreign key from
        -- signal_message_links.logical_id to channel_signals. A link row for
        -- a reference whose original signal was never stored (status
        -- 'unresolved') must be recordable without an owning index row, so
        -- the reindexer guarantees that status='linked' rows always point at
        -- an existing channel_signals entry inside the same transaction.
        CREATE INDEX IF NOT EXISTS idx_signal_message_links_signal
            ON signal_message_links (channel_id, logical_id, message_id);
        CREATE INDEX IF NOT EXISTS idx_signal_message_links_status
            ON signal_message_links (status);
        """,
    ),
    (
        11,
        """
        CREATE TABLE IF NOT EXISTS trading_budget_ledger (
            intent_id    TEXT PRIMARY KEY,
            reserved_quote TEXT NOT NULL DEFAULT '0',
            spent_quote    TEXT NOT NULL DEFAULT '0',
            fee_quote      TEXT NOT NULL DEFAULT '0',
            active         INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
            state          TEXT NOT NULL DEFAULT 'reserved',
            note           TEXT,
            created_at     TEXT NOT NULL,
            updated_at     TEXT NOT NULL
        );
        """,
    ),
    (
        12,
        """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id             TEXT PRIMARY KEY,
            signal_id            TEXT NOT NULL,
            idempotency_key      TEXT NOT NULL UNIQUE,
            channel_id           INTEGER NOT NULL,
            message_id           INTEGER NOT NULL,
            symbol               TEXT NOT NULL,
            side                 TEXT NOT NULL,
            entry_mode           TEXT NOT NULL DEFAULT 'direct',
            requested_quote      TEXT NOT NULL DEFAULT '0',
            entry_price          TEXT,
            trigger_offset_ticks TEXT,
            trigger_price        TEXT,
            trade_state          TEXT NOT NULL DEFAULT 'pending_entry',
            tp1_price            TEXT,
            tp2_price            TEXT,
            tp1_allocation_pct   TEXT NOT NULL DEFAULT '0',
            tp2_allocation_pct   TEXT NOT NULL DEFAULT '0',
            acquired_base_qty    TEXT NOT NULL DEFAULT '0',
            acquired_quote_spent TEXT NOT NULL DEFAULT '0',
            avg_entry_price      TEXT,
            fees_paid            TEXT NOT NULL DEFAULT '0',
            tp1_allocated_qty    TEXT NOT NULL DEFAULT '0',
            tp1_sold_qty         TEXT NOT NULL DEFAULT '0',
            tp2_allocated_qty    TEXT NOT NULL DEFAULT '0',
            tp2_sold_qty         TEXT NOT NULL DEFAULT '0',
            sl_price             TEXT,
            sl_state             TEXT NOT NULL DEFAULT 'dormant',
            sl_order_id          TEXT,
            sl_executed_qty      TEXT NOT NULL DEFAULT '0',
            unsold_residual_qty  TEXT,
            unsold_residual_reason TEXT,
            entry_intent_id      TEXT,
            entry_order_id       TEXT,
            entry_exchange_status TEXT,
            entry_filled_qty     TEXT NOT NULL DEFAULT '0',
            retry_count          INTEGER NOT NULL DEFAULT 0,
            last_error           TEXT,
            last_error_category  TEXT,
            activated_at         TEXT,
            closed_at            TEXT,
            created_at           TEXT NOT NULL,
            updated_at           TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trades_signal_id ON trades(signal_id);
        CREATE INDEX IF NOT EXISTS idx_trades_idempotency_key ON trades(idempotency_key);
        CREATE INDEX IF NOT EXISTS idx_trades_entry_intent_id ON trades(entry_intent_id);
        CREATE INDEX IF NOT EXISTS idx_trades_trade_state ON trades(trade_state);

        CREATE TABLE IF NOT EXISTS trade_orders (
            trade_order_id   TEXT PRIMARY KEY,
            trade_id         TEXT NOT NULL,
            kind             TEXT NOT NULL,
            client_order_id  TEXT NOT NULL UNIQUE,
            symbol           TEXT NOT NULL,
            side             TEXT NOT NULL,
            order_type       TEXT NOT NULL,
            price            TEXT,
            quantity         TEXT,
            allocated_qty    TEXT NOT NULL DEFAULT '0',
            sold_qty         TEXT NOT NULL DEFAULT '0',
            state            TEXT NOT NULL DEFAULT 'pending',
            exchange_order_id TEXT,
            exchange_status  TEXT,
            created_at       TEXT NOT NULL,
            updated_at       TEXT NOT NULL,
            FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
        );
        CREATE INDEX IF NOT EXISTS idx_trade_orders_trade_id ON trade_orders(trade_id);
        CREATE INDEX IF NOT EXISTS idx_trade_orders_client_order_id ON trade_orders(client_order_id);
        CREATE INDEX IF NOT EXISTS idx_trade_orders_kind ON trade_orders(kind);
        CREATE INDEX IF NOT EXISTS idx_trade_orders_state ON trade_orders(state);
        """,
    ),
    (
        13,
        """
        CREATE TABLE IF NOT EXISTS order_protection (
            intent_id              TEXT PRIMARY KEY,
            client_order_id        TEXT NOT NULL,
            symbol                 TEXT NOT NULL,
            entry_order_id         TEXT,
            take_profit_price      TEXT,
            stop_loss_price        TEXT,
            protection_mode        TEXT NOT NULL DEFAULT 'attached',
            status                 TEXT NOT NULL DEFAULT 'unverified',
            last_error             TEXT,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL,
            exchange_tp_order_id   TEXT,
            exchange_sl_order_id   TEXT,
            verified_at            TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_order_protection_status ON order_protection(status);
        """,
    ),
    (
        14,
        """
        CREATE TABLE IF NOT EXISTS trading_notifications (
            notification_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key         TEXT NOT NULL,
            signal_id         TEXT,
            intent_id         TEXT,
            role              TEXT NOT NULL,
            chat_id           INTEGER NOT NULL,
            text              TEXT NOT NULL,
            status            TEXT NOT NULL DEFAULT 'pending',
            attempts          INTEGER NOT NULL DEFAULT 0,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            sent_at           TEXT,
            last_error        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_event_key ON trading_notifications(event_key);
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_status ON trading_notifications(status);
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_notification_id ON trading_notifications(notification_id);
        """,
    ),
    (
        15,
        """
        -- Expand the allowed flow states so the management bot can persist an
        -- order-amount input stage without hitting the CHECK constraint.
        CREATE TABLE IF NOT EXISTS mgmt_operator_flows_new (
            operator_id INTEGER PRIMARY KEY,
            state TEXT NOT NULL DEFAULT 'idle'
                CHECK (state IN ('idle', 'auth_phone', 'auth_code', 'auth_password', 'order_amount_input')),
            updated_at TEXT NOT NULL,
            expires_at TEXT
        );
        INSERT INTO mgmt_operator_flows_new (operator_id, state, updated_at, expires_at)
            SELECT operator_id, state, updated_at, expires_at FROM mgmt_operator_flows;
        DROP TABLE mgmt_operator_flows;
        ALTER TABLE mgmt_operator_flows_new RENAME TO mgmt_operator_flows;
        """,
    ),
    (
        16,
        """
        -- Make notification outbox idempotent: the same (event_key, role) must
        -- never be enqueued twice, so retries and restarts cannot duplicate
        -- notifications.
        CREATE TABLE IF NOT EXISTS trading_notifications_new (
            notification_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key         TEXT NOT NULL,
            signal_id         TEXT,
            intent_id         TEXT,
            role              TEXT NOT NULL,
            chat_id           INTEGER NOT NULL,
            text              TEXT NOT NULL,
            status            TEXT NOT NULL DEFAULT 'pending',
            attempts          INTEGER NOT NULL DEFAULT 0,
            created_at        TEXT NOT NULL,
            updated_at        TEXT NOT NULL,
            sent_at           TEXT,
            last_error        TEXT,
            UNIQUE (event_key, role)
        );
        INSERT INTO trading_notifications_new (
            notification_id, event_key, signal_id, intent_id, role, chat_id,
            text, status, attempts, created_at, updated_at, sent_at, last_error
        )
        SELECT notification_id, event_key, signal_id, intent_id, role, chat_id,
               text, status, attempts, created_at, updated_at, sent_at, last_error
        FROM trading_notifications;
        DROP TABLE trading_notifications;
        ALTER TABLE trading_notifications_new RENAME TO trading_notifications;
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_event_key ON trading_notifications(event_key);
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_status ON trading_notifications(status);
        CREATE INDEX IF NOT EXISTS idx_trading_notifications_notification_id ON trading_notifications(notification_id);
        """,
    ),
    (
        17,
        """
        -- Record the stop-loss activation trigger transmitted to MEXC on each
        -- OCO exit leg, distinct from the take-profit limit price and from the
        -- configured stop-loss exit price (trades.sl_price).  Additive only:
        -- existing rows keep NULL and are never rewritten.
        ALTER TABLE trade_orders ADD COLUMN stop_trigger_price TEXT;
        """,
    ),
    (18, _migration_18_audit_journal),
    (19, _migration_19_trade_evidence_columns),
    (20, _migration_20_audit_evidence_notifications),
]


class Database:
    """Thin wrapper over sqlite3 with embedded migrations."""

    def __init__(self, path: str | Path, *, read_only: bool = False):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.read_only = read_only
        if read_only:
            if not self.path.is_file():
                raise FileNotFoundError(self.path)
            self.conn = sqlite3.connect(
                f"file:{self.path}?mode=ro", uri=True, check_same_thread=False, timeout=10
            )
        else:
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(str(self.path), check_same_thread=False, timeout=30)
        self.conn.row_factory = sqlite3.Row
        if not read_only:
            self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.isolation_level = None  # autocommit; explicit BEGIN via lock

    def migrate(self) -> list[int]:
        if self.read_only:
            raise RuntimeError("cannot migrate a read-only database")
        applied: list[int] = []
        with self.lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                "version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            done = {r["version"] for r in self.conn.execute("SELECT version FROM schema_migrations")}
            for version, step in MIGRATIONS:
                if version in done:
                    continue
                with self.transaction():
                    if callable(step):
                        step(self.conn)
                    else:
                        # executescript would implicitly COMMIT; run statements one by one
                        for statement in step.split(";"):
                            if statement.strip():
                                self.conn.execute(statement)
                    self.conn.execute(
                        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, datetime('now'))",
                        (version,),
                    )
                applied.append(version)
        return applied

    def schema_version(self) -> int:
        with self.lock:
            try:
                row = self.conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
            except sqlite3.OperationalError:
                return 0
            return int(row["v"] or 0)

    def transaction(self):
        outer = self

        class _Tx:
            def __init__(self, conn):
                self.conn = conn

            def __enter__(self):
                outer.lock.acquire()
                try:
                    self.conn.execute("BEGIN IMMEDIATE")
                except Exception:
                    outer.lock.release()
                    raise
                return self.conn

            def __exit__(self, exc_type, exc, tb):
                try:
                    if exc_type is None:
                        self.conn.execute("COMMIT")
                    else:
                        self.conn.execute("ROLLBACK")
                finally:
                    outer.lock.release()
                return False

        return _Tx(self.conn)

    def close(self) -> None:
        with self.lock:
            self.conn.close()
