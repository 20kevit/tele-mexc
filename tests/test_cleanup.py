"""One-time initial open-order cleanup: threshold, OCO, filled positions, idempotency."""

from __future__ import annotations

from tg_signal_ingest.trading.cleanup import CLEANUP_KEY, INFLIGHT_KEY, run_initial_open_order_cleanup
from tg_signal_ingest.trading.store import TradingStore


class CleanupClient:
    def __init__(self, orders):
        self.orders = [dict(o) for o in orders]
        self.cancel_calls: list[str] = []
        self.fail_cancel = False
        self.fail_open_orders = False

    def open_orders(self, symbol=None):
        if self.fail_open_orders:
            raise RuntimeError("exchange unavailable")
        rows = [dict(o) for o in self.orders]
        if symbol:
            rows = [o for o in rows if str(o.get("symbol") or "").upper() == str(symbol).upper()]
        return rows

    def cancel_all_orders(self, *, symbol):
        if self.fail_cancel:
            raise RuntimeError("cancel timeout")
        self.cancel_calls.append(symbol)
        self.orders = [o for o in self.orders if str(o.get("symbol") or "").upper() != str(symbol).upper()]
        return []


def _order(symbol="BTCUSDT", order_id="o1", qty="1", price="1", status="NEW", side="SELL", otype="LIMIT"):
    return {
        "orderId": order_id,
        "symbol": symbol,
        "side": side,
        "type": otype,
        "status": status,
        "origQty": qty,
        "executedQty": "0",
        "price": price,
    }


def _store(repo, cfg) -> TradingStore:
    return TradingStore(repo.db, cfg.trading)


def test_below_threshold_cancels_exactly_five_and_above_untouched(repo, cfg):
    client = CleanupClient(
        [
            _order("AAAUSDT", "below", qty="4.99", price="1"),
            _order("BBBUSDT", "exact", qty="5", price="1"),
            _order("CCCUSDT", "above", qty="5.01", price="1"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)

    assert client.cancel_calls == ["AAAUSDT"]
    assert report.canceled_symbols == ["AAAUSDT"]
    assert set(report.left_untouched) == {"BBBUSDT", "CCCUSDT"}
    assert not report.uncertain_symbols


def test_mixed_symbol_is_left_entirely_untouched(repo, cfg):
    client = CleanupClient(
        [
            _order("AAAUSDT", "small", qty="1", price="1"),
            _order("AAAUSDT", "large", qty="10", price="1"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == []
    assert "AAAUSDT" in report.left_untouched


def test_oco_legs_are_canceled_as_one_symbol_group(repo, cfg):
    client = CleanupClient(
        [
            _order("AAAUSDT", "oco-tp", qty="1", price="1", otype="OCO_STOP_LIMIT_ORDER"),
            _order("AAAUSDT", "oco-sl", qty="1", price="1", otype="OCO_STOP_LIMIT_ORDER"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == ["AAAUSDT"]
    assert report.canceled_symbols == ["AAAUSDT"]
    assert not report.uncertain_symbols


def test_filled_orders_are_never_canceled(repo, cfg):
    client = CleanupClient([_order("AAAUSDT", "filled", qty="1", price="1", status="FILLED")])
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == []
    assert "AAAUSDT" in report.left_untouched


def test_unvalued_quote_or_missing_price_is_left_untouched(repo, cfg):
    client = CleanupClient(
        [
            _order("AAABTC", "btc-quote", qty="1", price="1"),
            {
                "orderId": "no-price",
                "symbol": "CCCUSDT",
                "side": "SELL",
                "type": "LIMIT",
                "origQty": "1",
                "executedQty": "0",
            },
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == []
    assert set(report.left_untouched) == {"AAABTC", "CCCUSDT"}


def test_dry_run_never_cancels_or_persists(repo, cfg):
    store = _store(repo, cfg)
    client = CleanupClient([_order("AAAUSDT", "small", qty="1", price="1")])
    report = run_initial_open_order_cleanup(client, store, apply=False)
    assert client.cancel_calls == []
    assert report.canceled_symbols == ["AAAUSDT"]
    assert store.get_app_state(CLEANUP_KEY) is None


def test_cleanup_is_not_repeated_after_completion(repo, cfg):
    store = _store(repo, cfg)
    client = CleanupClient([_order("AAAUSDT", "small", qty="1", price="1")])
    first = run_initial_open_order_cleanup(client, store, apply=True)
    assert not first.already_completed
    client.orders = [_order("AAAUSDT", "new-small", qty="1", price="1")]
    second = run_initial_open_order_cleanup(client, store, apply=True)
    assert second.already_completed
    assert client.cancel_calls == ["AAAUSDT"]


def test_uncertain_cancellation_is_not_blindly_repeated(repo, cfg):
    store = _store(repo, cfg)
    client = CleanupClient([_order("AAAUSDT", "small", qty="1", price="1")])
    client.fail_cancel = True
    first = run_initial_open_order_cleanup(client, store, apply=True)
    assert first.uncertain_symbols == ["AAAUSDT"]
    assert store.get_app_state(CLEANUP_KEY) is None
    assert store.get_app_state(INFLIGHT_KEY)

    client.fail_cancel = False
    second = run_initial_open_order_cleanup(client, store, apply=True)
    assert second.uncertain_symbols == ["AAAUSDT"]
    assert client.cancel_calls == []


def test_open_order_query_failure_changes_nothing(repo, cfg):
    store = _store(repo, cfg)
    client = CleanupClient([_order("AAAUSDT", "small", qty="1", price="1")])
    client.fail_open_orders = True
    report = run_initial_open_order_cleanup(client, store, apply=True)
    assert "query failed" in report.detail.lower()
    assert client.cancel_calls == []
    assert store.get_app_state(CLEANUP_KEY) is None
