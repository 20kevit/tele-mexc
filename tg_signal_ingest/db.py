"""SQLite database bootstrap and schema migrations.

Migrations are embedded, idempotent, and applied in order. The connection uses
WAL mode and a busy timeout so concurrent maintenance commands on a busy
database do not produce spurious failures.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

MIGRATIONS: list[tuple[int, str]] = [
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
            for version, sql in MIGRATIONS:
                if version in done:
                    continue
                with self.transaction():
                    # executescript would implicitly COMMIT; run statements one by one
                    for statement in sql.split(";"):
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
