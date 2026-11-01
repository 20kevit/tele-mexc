"""Exchange-backed same-symbol open-order entry rule."""

from __future__ import annotations

import threading

from tests.test_submission_gate import GatedClient
from tests.test_submission_gate import _engine as _gated_engine
from tests.test_trading_engine import ALERT, FakeMexc, _engine, _item, _store_message


def _open(symbol: str, *, status: str = "NEW", order_type: str = "LIMIT", order_id: str = "ex-open"):
    return {
        "orderId": order_id,
        "symbol": symbol,
        "side": "SELL",
        "type": order_type,
        "status": status,
        "price": "100",
        "origQty": "1",
        "executedQty": "0",
        "cumulativeQuoteQty": "0",
    }


def test_same_symbol_open_order_blocks_new_buy(cfg, repo):
    client = FakeMexc(open_orders_results=[_open("BTCUSDT")])
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] == "rejected"
    assert client.place_calls == 0
    assert "open order" in (result.get("reject_reason") or "").lower()


def test_other_symbol_open_order_does_not_block(cfg, repo):
    client = FakeMexc(open_orders_results=[_open("ETHUSDT")])
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] in ("submitted", "open", "filled", "canceled")
    assert client.place_calls == 1


def test_oco_response_counts_as_same_symbol_open_order(cfg, repo):
    client = FakeMexc(
        open_orders_results=[_open("BTCUSDT", order_type="OCO_STOP_LIMIT_ORDER", order_id="oco-1")]
    )
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] == "rejected"
    assert client.place_calls == 0


def test_filled_and_canceled_orders_do_not_block(cfg, repo):
    for mid, status in enumerate(("FILLED", "CANCELED", "PARTIALLY_CANCELED", "REJECTED", "EXPIRED"), 1):
        client = FakeMexc(open_orders_results=[_open("BTCUSDT", status=status)])
        engine = _engine(cfg, repo, client)
        result = engine.process_item(_item(100, mid), _store_message(repo, mid, ALERT))
        assert result is not None and result["state"] in ("submitted", "open", "filled", "canceled"), status
        assert client.place_calls == 1, status


def test_open_order_query_failure_fails_closed_per_symbol(cfg, repo):
    client = FakeMexc(open_orders_error=RuntimeError("exchange unavailable"))
    engine = _engine(cfg, repo, client)
    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    assert result is not None and result["state"] == "rejected"
    assert client.place_calls == 0
    assert "query failed" in (result.get("reject_reason") or "").lower()


def test_sequential_same_symbol_second_entry_is_blocked_by_first_open_order(cfg, repo):
    client = GatedClient()
    engine = _gated_engine(cfg, repo, client)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")

    first = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert first is not None and len(client.place_calls) == 1
    second = engine.process_item(_item(100, 2), _store_message(repo, 2, ALERT))
    assert second is not None and second["state"] == "rejected"
    assert len(client.place_calls) == 1


def test_concurrent_same_symbol_signals_do_not_double_submit(cfg, repo):
    client = GatedClient()
    engine = _gated_engine(cfg, repo, client)
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")
    rec1 = _store_message(repo, 1, ALERT)
    rec2 = _store_message(repo, 2, ALERT)
    barrier = threading.Barrier(2)
    results = []

    def run(mid, record):
        barrier.wait(timeout=5)
        results.append(engine.process_item(_item(100, mid), record))

    threads = [
        threading.Thread(target=run, args=(1, rec1)),
        threading.Thread(target=run, args=(2, rec2)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert len(results) == 2
    assert len(client.place_calls) == 1
    assert sum(1 for r in results if r is not None and r.get("state") == "rejected") == 1
