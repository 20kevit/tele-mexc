"""Shared fakes for trade-manager TP/SL/conditional-entry tests.

These fakes intentionally avoid any real exchange contact.  The
:class:`FakeEngine` stands in for :class:`TradingEngine` and records the
trade-manager's outbound commands; the :class:`FakeClient` is attached to the
TradeManager so symbol metadata and quantization are testable.

The fake deliberately mirrors the production engine's important behaviours:
order lookup by ``clientOrderId``, intent reconciliation and fill persistence,
and cancellation confirmation.  Tests can therefore exercise restart and
ambiguous-submission paths without a network.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any

from tg_signal_ingest.trading.trade_store import TradeStore

LIVE_TOKEN = "I_UNDERSTAND_REAL_MONEY_AND_RISKS"


def _optional_decimal(value):
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


class FakeMeta:
    """MEXC-style symbol metadata used by the TradeManager for quantization."""

    def __init__(self, *, tick_size: str = "0.01", step_size: str = "0.0001",
                 min_qty: str = "0.0001", min_notional: str = "1.0"):
        self.price_step = Decimal(tick_size)
        self.lot_size_step = Decimal(step_size)
        self.min_qty = Decimal(min_qty)
        self.min_notional = Decimal(min_notional)
        self.min_quote = Decimal(min_notional)
        self.base_asset = "BTC"
        self.quote_asset = "USDT"
        self.status = "TRADING"

    def quantize_quantity(self, quantity: Decimal) -> Decimal:
        step = self.lot_size_step
        q = quantity.quantize(step, rounding=ROUND_DOWN)
        if q < self.min_qty:
            return Decimal("0")
        return q

    def quantize_price(self, price: Decimal) -> Decimal:
        return price.quantize(self.price_step, rounding=ROUND_DOWN)

    def validate_order(self, **kwargs: Any) -> None:
        qty = kwargs.get("quantity")
        quote = kwargs.get("quote_amount")
        if qty is not None and Decimal(str(qty)) < self.min_qty:
            raise ValueError(f"quantity below minimum {self.min_qty}")
        if quote is not None and Decimal(str(quote)) < self.min_notional:
            raise ValueError(f"quote amount below minimum {self.min_notional}")


class FakeRegistry:
    def __init__(self, meta: FakeMeta | None = None, symbols: dict[str, FakeMeta] | None = None):
        self._symbols = symbols or {"BTCUSDT": meta or FakeMeta()}

    def get(self, symbol: str) -> FakeMeta | None:
        return self._symbols.get(symbol.upper())


class FakeNotifier:
    """Records notify_trade / notify_intent calls instead of sending messages."""

    def __init__(self):
        self.trade_events: list[tuple[str, dict[str, Any] | None, str]] = []
        self.intent_events: list[tuple[str, dict[str, Any] | None, str]] = []
        self.flush_calls = 0

    def notify(self, *, event_key, text, signal_id=None, intent_id=None, roles=None) -> None:
        pass

    def notify_trade(self, trade, event_key, text, **extra) -> None:
        self.trade_events.append((event_key, trade, text))

    def notify_intent(self, intent, event_key, text) -> None:
        self.intent_events.append((event_key, intent, text))

    def enqueue(self, **kwargs) -> None:
        pass

    def flush(self) -> int:
        self.flush_calls += 1
        return 0

    def pending_count(self) -> int:
        return 0


class FakeClient:
    """Stand-in MEXC client: records orders, supports fill injection."""

    def __init__(self, registry: FakeRegistry | None = None):
        self.symbols = registry or FakeRegistry()
        # order_id -> exchange order dict
        self.orders: dict[str, dict[str, Any]] = {}
        # client_order_id -> order_id
        self.client_ids: dict[str, str] = {}
        # order_id -> synthetic myTrades rows
        self.trades: dict[str, list[dict[str, Any]]] = {}
        self.place_calls: list[dict[str, Any]] = []
        self.cancel_calls: list[dict[str, Any]] = []
        self.query_calls: list[dict[str, Any]] = []
        self._next_order_id = 1000
        self._next_trade_id = 5000

    def place_order(self, **kwargs) -> dict[str, Any]:
        self.place_calls.append(kwargs)
        oid = f"ex-{self._next_order_id}"
        self._next_order_id += 1
        cid = kwargs.get("client_order_id")
        if cid:
            self.client_ids[str(cid)] = oid
        self.orders[oid] = {
            "orderId": oid,
            "symbol": kwargs.get("symbol"),
            "side": kwargs.get("side"),
            "type": kwargs.get("order_type"),
            "status": "NEW",
            "executedQty": "0",
            "cumulativeQuoteQty": "0",
            "orig_qty": str(kwargs.get("quantity") or "0"),
            "clientOrderId": cid,
        }
        return {
            "orderId": oid, "status": "NEW",
            "executedQty": "0", "cumulativeQuoteQty": "0",
        }

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        rows = []
        wanted = str(symbol).upper() if symbol else None
        for order in self.orders.values():
            if str(order.get("status") or "").upper() in ("FILLED", "CANCELED", "PARTIALLY_CANCELED", "REJECTED"):
                continue
            if wanted and str(order.get("symbol") or "").upper() != wanted:
                continue
            rows.append(dict(order))
        return rows

    def fill_order(self, order_id: str, qty: Decimal, quote: Decimal, price: Decimal) -> None:
        """Credit a fill against an existing exchange order id and mark status."""
        order = self.orders[order_id]
        prev = Decimal(order["executedQty"])
        order["executedQty"] = str(prev + qty)
        order["cumulativeQuoteQty"] = str(Decimal(order["cumulativeQuoteQty"]) + quote)
        orig = Decimal(order.get("orig_qty") or "0")
        if orig > 0 and (prev + qty) >= orig:
            order["status"] = "FILLED"
        elif prev + qty > 0:
            order["status"] = "PARTIALLY_FILLED"
        self._next_trade_id += 1
        self.trades.setdefault(order_id, []).append(
            {
                "id": str(self._next_trade_id),
                "orderId": order_id,
                "symbol": order.get("symbol"),
                "price": str(price),
                "qty": str(qty),
                "quoteQty": str(quote),
                "commission": "0",
                "commissionAsset": "USDT",
                "isBuyer": 1 if str(order.get("side", "")).upper() == "BUY" else 0,
                "isMaker": 0,
                "time": self._next_trade_id,
            }
        )

    def cancel_order(self, symbol=None, order_id=None, orig_client_order_id=None, **kwargs) -> dict[str, Any]:
        if order_id is None and orig_client_order_id is not None:
            order_id = self.client_ids.get(str(orig_client_order_id))
        self.cancel_calls.append({"symbol": symbol, "order_id": order_id})
        order = self.orders.get(order_id)
        if order is not None and order.get("status") in ("NEW", "PARTIALLY_FILLED"):
            order["status"] = "CANCELED"
        return {"status": "CANCELED"}

    def query_order(self, **kwargs) -> dict[str, Any]:
        self.query_calls.append(kwargs)
        oid = kwargs.get("order_id")
        if oid is None and kwargs.get("orig_client_order_id") is not None:
            oid = self.client_ids.get(str(kwargs.get("orig_client_order_id")))
        if oid is None:
            raise FakeOrderNotFound("no such order")
        order = self.orders.get(oid)
        if order is None:
            raise FakeOrderNotFound("no such order")
        return dict(order)

    def my_trades(self, *, symbol=None, order_id=None, limit=1000) -> list[dict[str, Any]]:
        return list(self.trades.get(str(order_id), []))


class FakeOrderNotFound(Exception):
    """Mirror of the typed exchange not-found error."""


class FakeEngine:
    """Minimal engine stand-in for the TradeManager.

    It owns a real :class:`TradeStore` so that persistence is exercised, but it
    routes every exchange call through :class:`FakeClient` so nothing touches the
    network.
    """

    def __init__(self, cfg: Any, store: TradeStore, client: FakeClient | None = None):
        self.cfg = cfg
        self.store = store
        self.client = client or FakeClient()
        self.notifier = FakeNotifier()
        self.intent_calls: list[tuple[str, dict[str, Any]]] = []
        self.tp_orders: list[dict[str, Any]] = []
        self.sl_calls: list[Decimal] = []
        self.sl_client_ids: list[str] = []
        self.cancel_intent_calls: list[dict[str, Any]] = []
        self.cancel_trade_calls: list[dict[str, Any]] = []
        self.query_trade_calls: list[dict[str, Any]] = []

    # --- entry submission (conditional activation path) -------------------
    def _execute_intent(self, intent_id: str) -> dict[str, Any]:
        intent = self.store.get_intent(intent_id)
        assert intent is not None, f"intent {intent_id} missing"
        if intent.get("state") != "planned":
            return intent
        oid = self.client.place_order(
            symbol=intent["symbol"],
            side="BUY",
            order_type="LIMIT",
            quantity=intent.get("base_quantity"),
            price=intent.get("limit_price"),
            client_order_id=intent.get("client_order_id"),
        )
        exchange_id = oid["orderId"]
        with self.store.db.transaction() as conn:
            conn.execute(
                "UPDATE order_intents SET exchange_order_id=?, state='submitted', "
                "exchange_status='NEW', submitted_at=? WHERE intent_id=?",
                (exchange_id, "now", intent_id),
            )
        refreshed = self.store.get_intent(intent_id)
        assert refreshed is not None
        self.intent_calls.append((intent_id, dict(refreshed)))
        return refreshed

    def reconcile_intent(self, intent_id: str, *, notify: bool = True) -> dict[str, Any]:
        """Read-only-ish fake reconciliation: adopt exchange state and fills."""
        intent = self.store.get_intent(intent_id)
        if intent is None:
            raise RuntimeError(f"intent {intent_id} not found")
        state = intent.get("state")
        if state in ("filled", "canceled", "failed", "rejected", "canceled_local"):
            return intent
        exchange_id = intent.get("exchange_order_id")
        if not exchange_id:
            return intent
        try:
            raw = self.client.query_order(order_id=exchange_id)
        except FakeOrderNotFound:
            return intent
        status = str(raw.get("status") or "UNKNOWN").upper()
        mapped = {
            "NEW": "open",
            "PARTIALLY_FILLED": "open",
            "FILLED": "filled",
            "CANCELED": "canceled",
            "REJECTED": "rejected",
        }.get(status)
        if mapped and mapped != state:
            with self.store.db.transaction() as conn:
                conn.execute(
                    "UPDATE order_intents SET state=?, exchange_status=?, "
                    "executed_qty=?, cumulative_quote=? WHERE intent_id=?",
                    (
                        mapped, status, raw.get("executedQty"),
                        raw.get("cumulativeQuoteQty"), intent_id,
                    ),
                )
        existing_fills = self.store.fills_for_order(str(exchange_id))
        if not existing_fills:
            for trade in self.client.my_trades(order_id=exchange_id):
                self.store.add_fill(
                    exchange_order_id=str(exchange_id),
                    symbol=intent["symbol"],
                    raw=trade,
                )
        refreshed = self.store.get_intent(intent_id)
        assert refreshed is not None
        return refreshed

    # --- TP / SL order submission ------------------------------------------
    def submit_sell_limit(
        self, trade, client_order_id, price, quantity, kind=None, trade_order_id=None
    ):
        resp = self.client.place_order(
            symbol=trade["symbol"],
            side="SELL",
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
        )
        order = {
            "exchange_order_id": resp["orderId"],
            "state": "NEW",
            "executed_qty": Decimal("0"),
            "cumulative_quote": Decimal("0"),
        }
        self.tp_orders.append({"kind": kind, "client_order_id": client_order_id, **order})
        return order

    def submit_stop_loss_market_sell(self, trade, quantity, client_order_id=None):
        cid = client_order_id or f"tsisl-{trade['trade_id']}"
        resp = self.client.place_order(
            symbol=trade["symbol"],
            side="SELL",
            order_type="MARKET",
            quantity=quantity,
            price=None,
            client_order_id=cid,
        )
        self.client.fill_order(resp["orderId"], quantity, quantity, Decimal("1"))
        order = {
            "exchange_order_id": resp["orderId"],
            "state": "FILLED",
            "executed_qty": quantity,
            "cumulative_quote": quantity,
        }
        self.sl_calls.append(quantity)
        self.sl_client_ids.append(cid)
        return order

    def submit_oco_sell(
        self, trade, client_order_id, price, quantity, stop_price, kind=None,
        trade_order_id=None,
    ):
        resp = self.client.place_order(
            symbol=trade["symbol"],
            side="SELL",
            order_type="OCO_STOP_LIMIT_ORDER",
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
            trigger_price=stop_price,
            trigger_limit_price=stop_price,
        )
        order = {
            "orderId": resp["orderId"],
            "exchange_order_id": resp["orderId"],
            "state": "NEW",
            "executed_qty": Decimal("0"),
            "cumulative_quote": Decimal("0"),
        }
        self.tp_orders.append({"kind": kind, "client_order_id": client_order_id, **order})
        return order

    # --- cancellation ------------------------------------------------------
    def cancel_intent_order(self, intent):
        self.cancel_intent_calls.append(intent)
        oid = intent.get("exchange_order_id")
        if oid:
            self.client.cancel_order(symbol=intent["symbol"], order_id=oid)
        with self.store.db.transaction() as conn:
            conn.execute(
                "UPDATE order_intents SET state='canceled', exchange_status='CANCELED' "
                "WHERE intent_id=?",
                (intent["intent_id"],),
            )

    def cancel_trade_order(self, order):
        self.cancel_trade_calls.append(order)
        oid = order.get("exchange_order_id")
        if oid:
            self.client.cancel_order(symbol=order["symbol"], order_id=oid)
        elif order.get("client_order_id"):
            self.client.cancel_order(
                symbol=order["symbol"], orig_client_order_id=order["client_order_id"]
            )

    def query_trade_order(self, order):
        self.query_trade_calls.append(order)
        oid = order.get("exchange_order_id")
        try:
            if oid:
                raw = self.client.query_order(order_id=oid)
            elif order.get("client_order_id"):
                raw = self.client.query_order(
                    orig_client_order_id=order["client_order_id"]
                )
            else:
                return {"state": "NOT_FOUND", "executed_qty": Decimal("0")}
        except FakeOrderNotFound:
            return {"state": "NOT_FOUND", "executed_qty": Decimal("0")}
        return {
            "state": str(raw.get("status") or "UNKNOWN").upper(),
            "executed_qty": _optional_decimal(raw.get("executedQty")),
            "cumulative_quote": _optional_decimal(raw.get("cumulativeQuoteQty")),
            "exchange_order_id": str(raw.get("orderId") or ""),
        }
