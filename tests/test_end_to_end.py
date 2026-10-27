"""Supervised end-to-end pipeline: durable queue -> parser -> intent -> mock exchange.

No network is touched: ``testonly`` uses a fake ``place_test_order`` and the live
path uses a fake MEXC client. These tests exercise the wiring that the real
ingest service uses (``ProcessingEngine`` consumer + trading handler), not just
the engine in isolation.
"""

from __future__ import annotations

import asyncio

from tests.test_trading_engine import (
    ALERT,
    FakeMexc,
    Msg,
    _engine,
    _store_message,
)
from tg_signal_ingest.normalize import message_to_record
from tg_signal_ingest.processing import ProcessingEngine


def _wire(repo, engine) -> ProcessingEngine:
    """Mirror IngestService._register_trading_handlers without starting Telegram."""

    async def handle(item, record):
        await asyncio.to_thread(engine.process_item, item, record)

    pe = ProcessingEngine(repo, poll_interval=0.01)
    for item_type in ("message", "edit", "delete"):
        pe.register_handler(item_type, handle)
    return pe


def test_testonly_pipeline_processes_queue_exactly_once(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="testonly")
    pe = _wire(repo, engine)

    _store_message(repo, 200, ALERT, source="live")
    stats = asyncio.run(pe.run_once())

    assert stats.processed == 1
    assert client.test_calls == 1
    assert client.place_calls == 0  # test endpoint only, never a real order
    intents = engine.store.list_intents(limit=10)
    assert len(intents) == 1
    assert intents[0]["state"] == "canceled_local"
    assert intents[0]["exchange_status"] == "TEST_ONLY_ACCEPTED"
    signal = engine.store.get_signal("100:200:0")
    assert signal is not None and signal["status"] == "planned"

    again = asyncio.run(pe.run_once())
    assert again.processed == 0
    assert client.test_calls == 1
    assert len(engine.store.list_intents(limit=10)) == 1


def test_backfill_message_never_reaches_the_exchange(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client, mode="testonly")
    pe = _wire(repo, engine)

    _store_message(repo, 201, ALERT, source="backfill")
    stats = asyncio.run(pe.run_once())

    assert stats.processed == 1
    assert client.test_calls == 0
    assert engine.store.list_intents(limit=10) == []
    signal = engine.store.get_signal("100:201:0")
    assert signal is not None and signal["status"] == "rejected"
    assert "backfill" in signal["rejection_reason"]


def test_duplicate_delivery_is_durable_and_single_intent(cfg, repo):
    engine = _engine(cfg, repo, FakeMexc(), mode="testonly")
    pe = _wire(repo, engine)

    record = message_to_record(100, Msg(202, ALERT), "live")
    repo.upsert_message(record)
    repo.upsert_message(message_to_record(100, Msg(202, ALERT), "live"))  # redelivery
    stats = asyncio.run(pe.run_once())

    assert stats.processed == 1
    assert len(engine.store.list_intents(limit=10)) == 1


def test_live_pipeline_submits_reconciles_and_records_fills(cfg, repo):
    new_order = {
        "orderId": "ex-e2e", "symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT",
        "price": "100", "origQty": "0.1", "executedQty": "0",
        "cumulativeQuoteQty": "0", "status": "NEW", "time": 1,
    }
    filled = {**new_order, "executedQty": "0.1", "cumulativeQuoteQty": "10", "status": "FILLED"}
    client = FakeMexc(
        place_response={"orderId": "ex-e2e", "status": "NEW"},
        query_results=[new_order, filled],
        trades=[{
            "id": "t-e2e", "price": "100", "qty": "0.1", "quoteQty": "10",
            "commission": "0.01", "commissionAsset": "USDT", "time": 3,
            "isBuyer": True, "isMaker": False,
        }],
    )
    engine = _engine(cfg, repo, client, mode="live")
    pe = _wire(repo, engine)

    _store_message(repo, 203, ALERT, source="live")
    stats = asyncio.run(pe.run_once())
    assert stats.processed == 1
    assert client.account_calls == 1  # fail-closed balance preflight ran
    assert client.place_calls == 1

    intent = engine.store.list_intents(limit=10)[0]
    assert intent["state"] == "open"

    sweep = engine.reconcile_pending()
    assert sweep.checked == 1 and sweep.resolved == 1
    final = engine.store.get_intent(intent["intent_id"])
    assert final is not None and final["state"] == "filled"
    assert int(final["fill_sync_required"]) == 0
    assert engine.store.fills_count() == 1
    assert client.place_calls == 1  # never resubmitted during reconciliation
