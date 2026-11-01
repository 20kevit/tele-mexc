"""One-time initial open-order cleanup: threshold, OCO, filled positions, idempotency."""

from __future__ import annotations

from tg_signal_ingest.trading.cleanup import CLEANUP_KEY, run_initial_open_order_cleanup
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


def test_below_threshold_orders_are_never_cancelled(repo, cfg):
    """F-07: small notional is not a licence to cancel an order."""

    client = CleanupClient(
        [
            _order("AAAUSDT", "below", qty="4.99", price="1"),
            _order("BBBUSDT", "exact", qty="5", price="1"),
            _order("CCCUSDT", "above", qty="5.01", price="1"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)

    assert client.cancel_calls == []
    assert report.canceled_symbols == []
    assert set(report.left_untouched) == {"AAAUSDT", "BBBUSDT", "CCCUSDT"}


def test_unknown_and_oco_orders_are_left_untouched(repo, cfg):
    """F-07: unattributed / manual / OCO orders are never modified."""

    client = CleanupClient(
        [
            _order("MYXUSDT", "oco-tp2", qty="26.68", price="0.0568", otype="OCO_LIMIT_ORDER"),
            _order("GOLD(XAUT)USDT", "manual-buy", qty="2.9", price="3700", side="BUY"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)

    assert client.cancel_calls == []
    assert report.left_untouched == ["GOLD(XAUT)USDT", "MYXUSDT"]
    assert len(client.orders) == 2  # nothing removed from the exchange view


def test_filled_orders_are_ignored(repo, cfg):
    client = CleanupClient([_order("AAAUSDT", "filled", qty="1", price="1", status="FILLED")])
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == []
    assert report.left_untouched == ["AAAUSDT"]


def test_unvalued_quote_or_missing_price_is_left_untouched(repo, cfg):
    client = CleanupClient(
        [
            _order("BTCUSDT", "no-usd", qty="1", price="1"),
            _order("ETHUSDT", "no-price", qty="1", price="0"),
        ]
    )
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert client.cancel_calls == []
    assert set(report.left_untouched) == {"BTCUSDT", "ETHUSDT"}


def test_inspection_records_an_audit_trail(repo, cfg):
    client = CleanupClient([_order("AAAUSDT", "below", qty="1", price="1")])
    store = _store(repo, cfg)
    run_initial_open_order_cleanup(client, store, apply=False)
    row = repo.db.conn.execute(
        "SELECT outcome FROM audit_events WHERE event_type = 'cleanup.initial_open_orders' ORDER BY audit_id DESC"
    ).fetchone()
    assert row is not None and row[0] == "inspected"


def test_repeated_runs_stay_non_destructive(repo, cfg):
    client = CleanupClient([_order("AAAUSDT", "below", qty="1", price="1")])
    store = _store(repo, cfg)
    run_initial_open_order_cleanup(client, store, apply=True)
    run_initial_open_order_cleanup(client, store, apply=True)
    assert client.cancel_calls == []


def test_apply_flag_cannot_reenable_cancellation(repo, cfg):
    client = CleanupClient([_order("AAAUSDT", "below", qty="1", price="1")])
    report = run_initial_open_order_cleanup(client, _store(repo, cfg), apply=True)
    assert "ignored" in report.detail
    assert client.cancel_calls == []


def test_open_order_query_failure_changes_nothing(repo, cfg):
    store = _store(repo, cfg)
    client = CleanupClient([_order("AAAUSDT", "small", qty="1", price="1")])
    client.fail_open_orders = True
    report = run_initial_open_order_cleanup(client, store, apply=True)
    assert "query failed" in report.detail.lower()
    assert client.cancel_calls == []
    assert store.get_app_state(CLEANUP_KEY) is None
