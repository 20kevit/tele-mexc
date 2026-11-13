"""Phase 2 domain-event envelope, atomicity, and producer decoupling."""

from __future__ import annotations

import json
import sqlite3
import uuid
from decimal import Decimal

import pytest

from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item, _store_message
from tg_signal_ingest.db import Database
from tg_signal_ingest.trading.domain_events import (
    DomainEventConflict,
    DomainEventError,
    build_domain_event,
)
from tg_signal_ingest.trading.trade_store import TradeStore


@pytest.fixture()
def store(cfg):
    db = Database(cfg.db_path)
    db.migrate()
    yield TradeStore(db, cfg.trading)
    db.close()


def _trade(store: TradeStore) -> dict:
    return store.create_trade(
        trade_id="trade-1",
        signal_id="100:1:0",
        idempotency_key="trade-idem-1",
        channel_id=100,
        message_id=1,
        symbol="BTCUSDT",
        side="buy",
        entry_mode="limit",
        requested_quote=Decimal("10"),
        entry_price=Decimal("100"),
        trigger_offset_ticks=None,
        trigger_price=None,
        tp1_price=Decimal("110"),
        tp2_price=Decimal("120"),
        tp1_allocation_pct=Decimal("50"),
        tp2_allocation_pct=Decimal("50"),
        sl_price=Decimal("90"),
        entry_intent_id="intent-1",
    )


def _domain_events(store: TradeStore) -> list[dict]:
    return [
        dict(row)
        for row in store.db.conn.execute(
            "SELECT * FROM audit_events WHERE event_class = 'domain' ORDER BY audit_id"
        ).fetchall()
    ]


def _claimed_intent(store: TradeStore, *, message_id: int, idempotency_key: str, client_order_id: str) -> dict:
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=message_id, edit_version=0)
    intent, inserted = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=message_id,
        edit_version=0,
        idempotency_key=idempotency_key,
        client_order_id=client_order_id,
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    assert inserted is True
    claimed, blocked = store.claim_intent_for_submission(intent["intent_id"], note="claim")
    assert claimed is True and blocked is None
    return intent


def test_registry_schema_and_deterministic_json():
    event = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"new_state": "closed", "previous_state": "exit_pending", "sl_state": None},
        occurred_at="2026-11-12T10:11:12.123456+00:00",
        event_id="11111111-1111-4111-8111-111111111111",
    )
    assert event.event_id == "11111111111141118111111111111111"
    assert event.event_id != event.event_type
    assert event.payload_json == '{"new_state":"closed","previous_state":"exit_pending"}'
    assert event.occurred_at_utc == "2026-11-12T10:11:12.123456Z"
    assert event.schema_version == 1
    assert event.aggregate_type == "trade"
    assert event.audience == ("owner", "owner_b", "developer")

    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload={"previous_state": "open", "new_state": Decimal("1.0")},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload={"previous_state": "open", "new_state": 1.5},
        )
    with pytest.raises(DomainEventError):
        build_domain_event("unknown.event", aggregate_id="x", payload={})
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload={"previous_state": "open"},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload={"previous_state": "open", "new_state": "closed", "raw": "not allowed"},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload={"previous_state": "open", "new_state": "closed"},
            occurred_at="2026-11-12T10:11:12",
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            payload=[],
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id=123,
            payload={"previous_state": "open", "new_state": "closed"},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            correlation_id=123,
            payload={"previous_state": "open", "new_state": "closed"},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            symbol=123,
            payload={"previous_state": "open", "new_state": "closed"},
        )
    with pytest.raises(DomainEventError):
        build_domain_event(
            "trade.state_changed",
            aggregate_id="trade-1",
            event_id=123,
            payload={"previous_state": "open", "new_state": "closed"},
        )


def test_signal_received_and_rejected_events_are_atomic_and_deduplicated(store):
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, inserted = store.record_signal(
        parsed, channel_id=100, message_id=1, edit_version=0, emit_event=True
    )
    assert inserted is True
    events = _domain_events(store)
    assert [row["event_type"] for row in events] == ["signal.received"]
    received = json.loads(events[0]["payload_json"])
    assert received["entry_price"] == "100"
    assert received["tp1_price"] == "110"
    assert received["tp2_price"] == "120"
    assert received["stop_loss_price"] == "95"

    duplicate_id, inserted = store.record_signal(
        parsed, channel_id=100, message_id=1, edit_version=0, emit_event=True
    )
    assert duplicate_id == signal_id and inserted is False
    assert len(_domain_events(store)) == 1

    store.update_signal(signal_id, "rejected", "risk check failed")
    events = _domain_events(store)
    assert [row["event_type"] for row in events] == ["signal.received", "signal.rejected"]
    rejected = json.loads(events[1]["payload_json"])
    assert rejected["previous_state"] == "parsed"
    assert rejected["new_state"] == "rejected"
    assert rejected["reason"] == "risk check failed"

    # Repeated rejection writes do not generate duplicate semantic events.
    store.update_signal(signal_id, "rejected", "same terminal state")
    assert len(_domain_events(store)) == 2


def test_exchange_write_outcomes_are_recorded_but_read_calls_are_not(store):
    read_id = store.begin_exchange_operation(
        operation_type="query_order", operation_class="read", intent_id="intent-1", symbol="BTCUSDT"
    )
    store.finish_exchange_operation(read_id, success=True, response_classification="success")
    assert _domain_events(store) == []

    write_id = store.begin_exchange_operation(
        operation_type="place_order", operation_class="write", intent_id="intent-1", symbol="BTCUSDT"
    )
    store.finish_exchange_operation(
        write_id,
        success=False,
        response_classification="uncertain",
        error_class="TimeoutError",
        uncertainty="uncertain",
    )
    events = _domain_events(store)
    assert [row["event_type"] for row in events] == ["exchange.write_outcome"]
    assert events[0]["severity"] == "CRITICAL"
    assert json.loads(events[0]["payload_json"]) == {
        "error_class": "TimeoutError",
        "operation_type": "place_order",
        "outcome": "uncertain",
    }


def test_reconciliation_events_emit_only_for_meaningful_outcome_changes(store):
    store.record_reconciliation(
        intent_id="intent-1", client_order_id="client-1", local_state="submitting",
        exchange_state=None, outcome="not_found", detail="first not-found observation",
    )
    events = _domain_events(store)
    assert [row["event_type"] for row in events] == ["reconciliation.outcome"]
    payload = json.loads(events[0]["payload_json"])
    assert payload == {"local_state": "submitting", "outcome": "not_found"}

    # Identical repeated polling result is recorded in reconciliation history,
    # but does not generate another user-facing domain event.
    store.record_reconciliation(
        intent_id="intent-1", client_order_id="client-1", local_state="submitting",
        exchange_state=None, outcome="not_found", detail="same observation again",
    )
    assert len(_domain_events(store)) == 1

    store.record_reconciliation(
        intent_id="intent-1", client_order_id="client-1", local_state="filled",
        exchange_state="FILLED", outcome="resolved", detail="exchange confirmed fill",
    )
    assert len(_domain_events(store)) == 2
    assert json.loads(_domain_events(store)[1]["payload_json"])["outcome"] == "resolved"

    # A later poll confirming the same already-resolved state as consistent
    # is not a new user-facing outcome and must not duplicate the notice.
    store.record_reconciliation(
        intent_id="intent-1", client_order_id="client-1", local_state="filled",
        exchange_state="FILLED", outcome="consistent", detail="same settled state",
    )
    assert len(_domain_events(store)) == 2


def test_historical_signal_can_be_stored_without_notification_events(store):
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, inserted = store.record_signal(
        parsed, channel_id=100, message_id=1, edit_version=0, emit_event=False
    )
    assert inserted is True
    store.update_signal(signal_id, "rejected", "historical replay", emit_rejection_event=False)
    assert _domain_events(store) == []


def test_same_event_id_is_idempotent_only_for_identical_envelope(store):
    _trade(store)
    event = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"previous_state": "pending_entry", "new_state": "position_open"},
        occurred_at="2026-11-12T10:00:00Z",
    )
    assert store.record_domain_event(event) == event.event_id
    assert store.record_domain_event(event) == event.event_id
    rows = _domain_events(store)
    assert len(rows) == 1
    assert rows[0]["dedupe_key"] is None
    assert rows[0]["source_ref"] is None
    assert rows[0]["event_class"] == "domain"

    conflicting = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"previous_state": "position_open", "new_state": "tp1_active"},
        occurred_at="2026-11-12T10:00:00Z",
        event_id=event.event_id,
    )
    with pytest.raises(DomainEventConflict):
        store.record_domain_event(conflicting)
    assert len(_domain_events(store)) == 1

    distinct = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"previous_state": "pending_entry", "new_state": "position_open"},
        occurred_at="2026-11-12T10:00:00Z",
    )
    assert distinct.event_id != event.event_id
    assert uuid.UUID(distinct.event_id)
    store.record_domain_event(distinct)
    assert len(_domain_events(store)) == 2


def test_event_and_business_transition_roll_back_together(store):
    _trade(store)
    event = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"previous_state": "pending_entry", "new_state": "closed"},
    )
    with pytest.raises(RuntimeError), store.db.transaction() as conn:
        conn.execute(
            "UPDATE trades SET trade_state = 'closed', closed_at = 'now' WHERE trade_id = 'trade-1'"
        )
        store.record_domain_event(event, conn=conn)
        raise RuntimeError("force rollback")

    assert store.get_trade("trade-1")["trade_state"] == "pending_entry"
    assert _domain_events(store) == []


def test_committed_transition_has_exactly_one_event(store):
    _trade(store)
    store.update_trade_state("trade-1", "position_open", note="entry filled")
    events = _domain_events(store)
    assert len(events) == 1
    assert events[0]["event_type"] == "trade.state_changed"
    assert events[0]["aggregate_type"] == "trade"
    assert events[0]["aggregate_id"] == "trade-1"
    assert json.loads(events[0]["payload_json"]) == {
        "new_state": "position_open",
        "previous_state": "pending_entry",
        "reason": "entry filled",
    }

    # A repeated same-state update is not a new lifecycle transition.
    store.update_trade_state("trade-1", "position_open", note="duplicate projection write")
    assert len(_domain_events(store)) == 1

    store.update_trade_state("trade-1", "tp1_active", note="TP1 active")
    assert len(_domain_events(store)) == 2
    assert _domain_events(store)[1]["event_id"] != _domain_events(store)[0]["event_id"]


def test_trade_order_transition_is_captured(store):
    _trade(store)
    store.create_trade_order(
        trade_order_id="to-1",
        trade_id="trade-1",
        kind="tp1",
        client_order_id="client-to-1",
        symbol="BTCUSDT",
        side="sell",
        order_type="limit",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
    )
    store.transition_trade_order("to-1", "submitting", note="OCO submission started")

    events = [row for row in _domain_events(store) if row["event_type"] == "trade_order.state_changed"]
    assert len(events) == 1
    assert events[0]["aggregate_type"] == "trade_order"
    assert events[0]["aggregate_id"] == "to-1"
    assert events[0]["correlation_id"] == "trade-1"
    assert events[0]["symbol"] == "BTCUSDT"
    assert json.loads(events[0]["payload_json"]) == {
        "attempt": 0,
        "client_order_id": "client-to-1",
        "kind": "tp1",
        "new_state": "submitting",
        "previous_state": "pending",
        "price": "110",
        "quantity": "0.05",
        "reason": "OCO submission started",
    }


def test_retryable_oco_rejection_records_next_attempt_in_same_transaction(store):
    _trade(store)
    store.create_trade_order(
        trade_order_id="to-retry",
        trade_id="trade-1",
        kind="tp1",
        client_order_id="client-to-retry",
        symbol="BTCUSDT",
        side="sell",
        order_type="limit",
        price=Decimal("110"),
        quantity=Decimal("0.05"),
        allocated_qty=Decimal("0.05"),
        stop_trigger_price=Decimal("98"),
    )
    store.transition_trade_order(
        "to-retry",
        "rejected",
        note="temporary exchange rejection",
        attempt=2,
        next_attempt_at="2026-11-12T12:00:05+00:00",
        retryable=True,
    )
    event = [row for row in _domain_events(store) if row["event_type"] == "trade_order.state_changed"][-1]
    payload = json.loads(event["payload_json"])
    assert event["severity"] == "WARNING"
    assert payload["attempt"] == 2
    assert payload["retryable"] is True
    assert payload["next_attempt_at"] == "2026-11-12T12:00:05+00:00"
    assert payload["reason"] == "temporary exchange rejection"
    assert payload["price"] == "110"
    assert payload["quantity"] == "0.05"
    assert payload["stop_trigger_price"] == "98"
    assert payload["client_order_id"] == "client-to-retry"


def test_oco_group_events_only_on_meaningful_changes(store):
    trade = _trade(store)
    group_id = store.ensure_oco_group(trade)

    store.set_oco_group_state("trade-1", "active", protection_status="active")
    events = [row for row in _domain_events(store) if row["event_type"] == "oco_group.state_changed"]
    assert len(events) == 1
    assert events[0]["aggregate_id"] == group_id
    assert events[0]["correlation_id"] == "trade-1"
    payload = json.loads(events[0]["payload_json"])
    assert payload["previous_state"] == "intended"
    assert payload["new_state"] == "active"
    assert payload["protection_status"] == "active"

    # Identical polling/projection writes do not create another event.
    store.set_oco_group_state("trade-1", "active", protection_status="active")
    store.set_oco_group_state("trade-1", "active", protection_status="active", note="poll")
    assert len([row for row in _domain_events(store) if row["event_type"] == "oco_group.state_changed"]) == 1

    store.set_oco_group_state("trade-1", "active", protection_status="partial")
    assert len([row for row in _domain_events(store) if row["event_type"] == "oco_group.state_changed"]) == 2

    store.set_oco_group_state("trade-1", "completed", protection_status="completed", closed=True)
    events = [row for row in _domain_events(store) if row["event_type"] == "oco_group.state_changed"]
    assert len(events) == 3
    assert json.loads(events[-1]["payload_json"])["closed"] is True

    store.set_oco_group_state("trade-1", "completed", protection_status="completed", closed=True)
    assert len([row for row in _domain_events(store) if row["event_type"] == "oco_group.state_changed"]) == 3


def test_oco_leg_events_only_on_meaningful_changes(store):
    trade = _trade(store)
    group_id = store.ensure_oco_group(trade)
    order = {
        "target": "tp1",
        "kind": "tp1",
        "trade_order_id": "to-tp1",
        "price": "110",
        "quantity": "0.05",
    }

    leg_id = store.upsert_oco_leg(
        group_id=group_id,
        trade=trade,
        order=order,
        state="active",
        exchange_status="NEW",
        verified=False,
    )
    events = [row for row in _domain_events(store) if row["event_type"] == "oco_leg.state_changed"]
    assert len(events) == 1
    assert events[0]["aggregate_id"] == leg_id
    first_payload = json.loads(events[0]["payload_json"])
    assert first_payload["previous_state"] == "absent"
    assert first_payload["new_state"] == "active"
    assert first_payload["verified"] is False

    # Identical polling upsert, including bookkeeping-only changes, is quiet.
    store.upsert_oco_leg(
        group_id=group_id,
        trade=trade,
        order=order,
        state="active",
        exchange_status="NEW",
        verified=False,
        evidence_id="evidence-2",
        not_observed_reason="still working",
    )
    assert len([row for row in _domain_events(store) if row["event_type"] == "oco_leg.state_changed"]) == 1

    store.upsert_oco_leg(
        group_id=group_id,
        trade=trade,
        order=order,
        state="filled",
        exchange_status="FILLED",
        filled_qty=Decimal("0.05"),
        filled_quote=Decimal("5.5"),
        fee_amount=Decimal("0.01"),
        fee_asset="USDT",
        verified=True,
    )
    events = [row for row in _domain_events(store) if row["event_type"] == "oco_leg.state_changed"]
    assert len(events) == 2
    fill_payload = json.loads(events[-1]["payload_json"])
    assert fill_payload["new_state"] == "filled"
    assert fill_payload["filled_qty"] == "0.05"
    assert fill_payload["filled_quote"] == "5.5"
    assert fill_payload["verified"] is True

    store.upsert_oco_leg(
        group_id=group_id,
        trade=trade,
        order=order,
        state="filled",
        exchange_status="FILLED",
        filled_qty=Decimal("0.05"),
        filled_quote=Decimal("5.5"),
        fee_amount=Decimal("0.01"),
        fee_asset="USDT",
        verified=True,
        cancel_reason="oco_sibling_filled",
        canceled_by_leg="tp2",
    )
    events = [row for row in _domain_events(store) if row["event_type"] == "oco_leg.state_changed"]
    assert len(events) == 3
    cancel_payload = json.loads(events[-1]["payload_json"])
    assert cancel_payload["cancel_reason"] == "oco_sibling_filled"
    assert cancel_payload["canceled_by_target"] == "tp2"


def test_oco_group_event_rolls_back_with_the_business_update(store, monkeypatch):
    trade = _trade(store)
    store.ensure_oco_group(trade)

    def _boom(*args, **kwargs):
        raise RuntimeError("force rollback")

    monkeypatch.setattr(store, "record_domain_event", _boom)
    with pytest.raises(RuntimeError):
        store.set_oco_group_state("trade-1", "active", protection_status="active")

    group = store.get_oco_group("trade-1")
    assert group["state"] == "intended"
    assert group["protection_status"] is None


def test_intent_transition_and_fill_insert_are_captured_once(store):
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, inserted = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="idem-1",
        client_order_id="client-1",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    assert inserted is True
    created = _domain_events(store)
    assert [row["event_type"] for row in created] == ["intent.created"]
    created_payload = json.loads(created[0]["payload_json"])
    assert created_payload["entry_price"] == "100"
    assert created_payload["tp1_price"] == "110"
    assert created_payload["tp2_price"] == "120"
    assert created_payload["stop_loss_price"] == "95"
    assert "entry_low_price" not in created_payload
    assert "entry_high_price" not in created_payload
    assert "target_prices" not in created_payload

    claimed, blocked = store.claim_intent_for_submission(intent["intent_id"], note="claim")
    assert claimed is True and blocked is None
    assert [row["event_type"] for row in _domain_events(store)] == [
        "intent.created",
        "intent.state_changed",
    ]

    store.transition_intent(
        intent["intent_id"],
        "rejected",
        note="local rejection",
        reject_reason="test",
        category="risk",
    )
    assert [row["event_type"] for row in _domain_events(store)][-1] == "intent.state_changed"

    raw_fill = {
        "price": "100",
        "qty": "0.1",
        "quoteQty": "10",
        "commission": "0.01",
        "commissionAsset": "USDT",
        "time": 1,
    }
    assert store.add_fill(exchange_order_id="ex-1", symbol="BTCUSDT", raw=raw_fill) is True
    assert store.add_fill(exchange_order_id="ex-1", symbol="BTCUSDT", raw=raw_fill) is False
    fill_events = [row for row in _domain_events(store) if row["event_type"] == "fill.recorded"]
    assert len(fill_events) == 1
    assert fill_events[0]["aggregate_type"] == "exchange_order"
    assert fill_events[0]["aggregate_id"] == "ex-1"


def test_unknown_requires_reconciliation_intent_transition_is_critical(store):
    """F5: an unresolved exchange-write outcome is CRITICAL, never INFO."""
    from tg_signal_ingest.trading.parser import SignalParser

    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="idem-unknown",
        client_order_id="client-unknown",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    store.transition_intent(
        intent["intent_id"],
        "unknown_requires_reconciliation",
        note="submission outcome uncertain",
        category="mexc_request",
    )
    events = [row for row in _domain_events(store) if row["event_type"] == "intent.state_changed"]
    assert events[-1]["severity"] == "CRITICAL"
    assert json.loads(events[-1]["payload_json"])["new_state"] == "unknown_requires_reconciliation"


def test_submission_accepted_event_is_distinct_and_precedes_open(store):
    """The exchange acknowledgement is a distinct event, written atomically with
    the ``submitted`` transition and before any later open/fill observation."""
    intent = _claimed_intent(
        store, message_id=1, idempotency_key="idem-accepted", client_order_id="client-accepted"
    )

    store.transition_intent(
        intent["intent_id"],
        "submitted",
        note="exchange accepted order ex-accepted",
        exchange_order_id="ex-accepted",
        exchange_status="NEW",
        executed_qty=Decimal("0"),
        cumulative_quote=Decimal("0"),
        category="mexc_request",
    )

    accepted = [row for row in _domain_events(store) if row["event_type"] == "intent.submission_accepted"]
    assert len(accepted) == 1
    assert accepted[0]["aggregate_type"] == "intent"
    assert accepted[0]["aggregate_id"] == intent["intent_id"]
    # Acceptance carries acceptance facts only, never fill amounts.
    assert json.loads(accepted[0]["payload_json"]) == {
        "exchange_order_id": "ex-accepted",
        "exchange_status": "NEW",
        "client_order_id": "client-accepted",
    }

    # A later independent open observation is a separate fact, never a second
    # acceptance, and it is ordered strictly after the acceptance.
    store.transition_intent(
        intent["intent_id"],
        "open",
        note="query confirms NEW",
        exchange_order_id="ex-accepted",
        exchange_status="NEW",
        category="reconcile",
    )
    assert len([row for row in _domain_events(store) if row["event_type"] == "intent.submission_accepted"]) == 1
    open_event = next(
        row
        for row in _domain_events(store)
        if row["event_type"] == "intent.state_changed" and json.loads(row["payload_json"]).get("new_state") == "open"
    )
    assert int(accepted[0]["audit_id"]) < int(open_event["audit_id"])


def test_submission_accepted_never_emitted_for_rejection_or_uncertain(store):
    """Definitive rejection and ambiguous timeout are distinct from acceptance:
    neither outcome ever produces an ``intent.submission_accepted`` event."""
    rejected = _claimed_intent(
        store, message_id=1, idempotency_key="idem-reject", client_order_id="client-reject"
    )
    store.transition_intent(
        rejected["intent_id"],
        "rejected",
        note="definitive exchange rejection",
        reject_reason="insufficient balance",
        category="mexc_request",
    )

    uncertain = _claimed_intent(
        store, message_id=2, idempotency_key="idem-uncertain", client_order_id="client-uncertain"
    )
    store.transition_intent(
        uncertain["intent_id"],
        "unknown_requires_reconciliation",
        note="ambiguous timeout; no retry will be attempted",
        category="mexc_request",
    )

    assert [row for row in _domain_events(store) if row["event_type"] == "intent.submission_accepted"] == []
    transitions = [
        (row["severity"], json.loads(row["payload_json"])["new_state"])
        for row in _domain_events(store)
        if row["event_type"] == "intent.state_changed"
    ]
    # Definitive rejection (ERROR) and ambiguous unknown (CRITICAL) stay distinct.
    assert ("ERROR", "rejected") in transitions
    assert ("CRITICAL", "unknown_requires_reconciliation") in transitions


def test_engine_emits_accepted_event_immediately_before_reconciliation(cfg, repo):
    """Engine-level: a definitive submit response emits the acceptance event
    immediately, before the later open/fill observation from reconciliation."""
    open_order = {
        "orderId": "ex-accepted",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0",
        "cumulativeQuoteQty": "0",
        "status": "NEW",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-accepted", "status": "NEW"},
        query_results=[open_order],
    )
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None
    assert client.place_calls == 1

    events = _domain_events(engine.store)
    types = [row["event_type"] for row in events]
    assert "intent.submission_accepted" in types
    accepted_idx = types.index("intent.submission_accepted")

    # The acceptance is followed only by later independent observations; the
    # earlier `submitting -> submitted` state change precedes it.
    later_states = [
        json.loads(row["payload_json"]).get("new_state")
        for row in events[accepted_idx + 1:]
        if row["event_type"] == "intent.state_changed"
    ]
    assert any(state in ("open", "filled") for state in later_states)
    assert "submitted" not in later_states

    # Acceptance facts only, never fill amounts.
    accepted = json.loads(events[accepted_idx]["payload_json"])
    assert accepted["exchange_order_id"] == "ex-accepted"
    assert "executed_qty" not in accepted and "cumulative_quote" not in accepted


def test_notifier_persists_only_and_producer_never_calls_telegram(cfg, repo):
    class ExplodingBot:
        def __init__(self):
            self.calls = 0

        def send_message(self, chat_id: int, text: str):
            self.calls += 1
            raise AssertionError("producer path must not call Telegram")

    from tg_signal_ingest.trading.notify import TradingNotifier

    engine = _engine(cfg, repo, FakeMexc())
    bot = ExplodingBot()
    notifier = TradingNotifier(engine.store, bot, owner_id=100000001, developer_id=100000002)
    engine.notifier = notifier

    def _unexpected_flush(*args, **kwargs):
        raise AssertionError("producer path must not flush the notification outbox")

    notifier.flush = _unexpected_flush  # type: ignore[method-assign]
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None
    assert bot.calls == 0
    # The execution result is now delivered from the domain-event journal;
    # the removed legacy summary must not be re-enqueued in the old outbox.
    assert notifier.pending_count() == 0
    assert engine.store.db.conn.execute(
        "SELECT COUNT(*) FROM audit_events WHERE event_class='domain'"
    ).fetchone()[0] > 0
    domain_types = {row["event_type"] for row in _domain_events(engine.store)}
    assert "intent.created" in domain_types


def test_legacy_audit_rows_stay_compatible_but_domain_rows_are_immutable(store):
    store.audit("corr-legacy", "lifecycle", "legacy_event", "legacy detail", event_id="legacy-evt-1")
    row = store.db.conn.execute(
        "SELECT event_class FROM audit_events WHERE event_id = 'legacy-evt-1'"
    ).fetchone()
    assert row["event_class"] == "legacy"

    # Legacy rows remain maintainable; the new immutable trigger is scoped.
    with store.db.transaction() as conn:
        conn.execute("UPDATE audit_events SET detail = 'updated' WHERE event_id = 'legacy-evt-1'")
    with store.db.transaction() as conn:
        conn.execute("DELETE FROM audit_events WHERE event_id = 'legacy-evt-1'")

    _trade(store)
    event = build_domain_event(
        "trade.state_changed",
        aggregate_id="trade-1",
        payload={"previous_state": "pending_entry", "new_state": "position_open"},
    )
    store.record_domain_event(event)
    with pytest.raises(sqlite3.IntegrityError), store.db.transaction() as conn:
        conn.execute("UPDATE audit_events SET severity = 'ERROR' WHERE event_id = ?", (event.event_id,))
    with pytest.raises(sqlite3.IntegrityError), store.db.transaction() as conn:
        conn.execute("DELETE FROM audit_events WHERE event_id = ?", (event.event_id,))


def test_critical_trade_protection_alert_is_persisted_as_domain_event(store, cfg):
    from tg_signal_ingest.trading.trade_manager import TradeManager

    class Notifier:
        def __init__(self):
            self.calls = []

        def notify_trade(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    notifier = Notifier()
    trade = _trade(store)
    manager = TradeManager(cfg.trading, store, notifier=notifier)
    detail = "CRITICAL UNPROTECTED: BTCUSDT quantity=0.1; OCO protection missing"

    manager._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail)
    manager._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail)

    events = [
        row for row in _domain_events(store)
        if row["event_type"] == "trade.protection_critical"
    ]
    assert len(events) == 1
    assert events[0]["severity"] == "CRITICAL"
    assert events[0]["symbol"] == "BTCUSDT"
    payload = json.loads(events[0]["payload_json"])
    assert payload == {"detail": detail, "event_key": "SL_UNPROTECTED_CRITICAL"}
    assert notifier.calls == []

    manager._notify_trade(trade, "SL_UNPROTECTED_CRITICAL", detail + "; new evidence")
    assert len([
        row for row in _domain_events(store)
        if row["event_type"] == "trade.protection_critical"
    ]) == 2


def test_new_buy_block_is_persisted_as_critical_protection_event(store, cfg):
    from tg_signal_ingest.trading.trade_manager import TradeManager

    class Notifier:
        def notify_trade(self, *args, **kwargs):
            raise AssertionError("critical buy-block alerts must use the domain-event path")

    manager = TradeManager(cfg.trading, store, notifier=Notifier())
    manager._notify_trade(
        None,
        "BUY_BLOCK_ACTIVE_deadbeef",
        "new BUYs blocked: an open position has no verified exit protection",
    )
    events = [
        row for row in _domain_events(store)
        if row["event_type"] == "trade.protection_critical"
    ]
    assert len(events) == 1
    assert events[0]["severity"] == "CRITICAL"
    payload = json.loads(events[0]["payload_json"])
    assert payload["event_key"] == "BUY_BLOCK_ACTIVE_deadbeef"
