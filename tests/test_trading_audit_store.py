"""Audit journal dedupe, notification preferences, and OCO group/leg lifecycle.

Uses a real migrated SQLite database.  No network, no exchange.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.db import Database
from tg_signal_ingest.trading.trade_store import TradeStore


@pytest.fixture()
def store(cfg, tmp_path):
    db = Database(tmp_path / "data" / "audit_store.sqlite3")
    db.migrate()
    yield TradeStore(db, cfg.trading)
    db.close()


def test_audit_dedupe_idempotent(store):
    store.audit(
        "corr-1", "lifecycle", "intent_planned",
        "intent created", event_id="evt-1", dedupe_key="dedupe:1",
        intent_id="intent-1", symbol="BTCUSDT",
    )
    # Repeat with the same dedupe key: no duplicate row.
    store.audit(
        "corr-1", "lifecycle", "intent_planned",
        "intent created", event_id="evt-1", dedupe_key="dedupe:1",
        intent_id="intent-1", symbol="BTCUSDT",
    )
    rows = list(
        store.db.conn.execute("SELECT COUNT(*) AS c FROM audit_events WHERE dedupe_key = ?", ("dedupe:1",))
    )
    assert rows[0]["c"] == 1


def test_audit_event_type_and_redaction(store):
    store.audit(
        "corr-2", "mexc_request", "place_order",
        "order 123456789:ABC token", event_id="evt-2",
        intent_id="intent-1", symbol="BTCUSDT",
        payload={"api_key": "secret", "qty": "0.1"},
    )
    row = store.db.conn.execute(
        "SELECT event_type, payload_json, detail, secrets_redacted FROM audit_events WHERE event_id = ?",
        ("evt-2",),
    ).fetchone()
    assert row["event_type"] == "mexc_request.place_order"
    assert "[REDACTED]" in row["payload_json"]
    assert row["secrets_redacted"] == 1


def test_notification_preferences_defaults_and_toggle(store):
    # Missing rows fall back to code defaults.
    assert store.notification_enabled("owner", "signal_decision") is False
    assert store.notification_enabled("owner", "signal_accepted") is True
    assert store.notification_enabled("developer", "diagnostic") is True
    # Toggle persists.
    store.set_notification_preference("owner", "signal_decision", True)
    assert store.notification_enabled("owner", "signal_decision") is True
    rows = store.notification_preferences("owner")
    cats = {r["category"]: bool(r["enabled"]) for r in rows}
    assert cats["signal_decision"] is True


def test_oco_group_and_leg_lifecycle(store):
    now = store._ts()
    with store.db.transaction() as conn:
        conn.execute(
            "INSERT INTO trades (trade_id, signal_id, idempotency_key, channel_id, message_id, "
            "symbol, side, entry_mode, requested_quote, entry_price, trade_state, "
            "tp1_price, tp2_price, tp1_allocation_pct, tp2_allocation_pct, sl_price, "
            "acquired_base_qty, entry_intent_id, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'position_open', ?, ?, ?, ?, ?, '0.1', 'intent-1', ?, ?)",
            (
                "trade-1", "sig-1", "idem-1", 100, 1, "BTCUSDT", "buy", "limit",
                "10", "100", "110", "120", "50", "100", "95", now, now,
            ),
        )
        conn.execute(
            "INSERT INTO trade_orders (trade_order_id, trade_id, kind, client_order_id, "
            "symbol, side, order_type, price, quantity, allocated_qty, state, created_at, updated_at) "
            "VALUES ('to-1', 'trade-1', 'take_profit', 'cid-tp1', 'BTCUSDT', 'sell', 'LIMIT', "
            "'110', '0.05', '0.05', 'open', ?, ?)",
            (now, now),
        )

    group = store.ensure_oco_group(
        store.get_trade("trade-1"),
        intended_config={"tp1_price": "110", "tp2_price": "120"},
        protection_status="placing",
    )
    assert group == "oco-trade-1"
    assert store.get_oco_group("trade-1")["oco_group_id"] == group

    tp1 = store.list_trade_orders("trade-1", kind="take_profit")[0]
    with store.db.transaction() as conn:
        conn.execute(
            "INSERT INTO trade_orders (trade_order_id, trade_id, kind, client_order_id, "
            "symbol, side, order_type, price, quantity, allocated_qty, state, created_at, updated_at) "
            "VALUES ('to-2', 'trade-1', 'stop_loss', 'cid-sl', 'BTCUSDT', 'sell', 'MARKET', "
            "NULL, '0.05', '0.05', 'open', ?, ?)",
            (store._ts(), store._ts()),
        )
    tp2 = store.list_trade_orders("trade-1", kind="stop_loss")[0]

    store.upsert_oco_leg(
        group_id=group, trade=store.get_trade("trade-1"), order=tp1, state="filled",
        leg_role="take_profit", exchange_status="FILLED", filled_qty=Decimal("0.05"),
    )
    store.upsert_oco_leg(
        group_id=group, trade=store.get_trade("trade-1"), order=tp2, state="canceled",
        leg_role="take_profit", exchange_status="CANCELED", cancel_reason="oco_sibling_filled",
        canceled_by_leg="take_profit",
    )
    legs = store.list_oco_legs(group)
    assert len(legs) == 2
    assert {leg["state"] for leg in legs} == {"filled", "canceled"}

    store.attribute_sibling_cancel(group, filled_target="take_profit", canceled_target="stop_loss")
    store.set_oco_group_state("trade-1", "completed", closed=True)
    grp = store.get_oco_group("trade-1")
    assert grp["state"] == "completed"
    assert grp["closed_at"] is not None
