"""Technical reservation ledger and notification workflow.

``trading_budget_ledger`` is an idempotency/concurrency reservation for an
intent.  It is not a portfolio budget and has no configurable spending cap.
All exchange/Telegram clients are fakes; no test can reach MEXC or Telegram.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from tests.test_trading_engine import (
    ALERT,
    FakeMeta,
    FakeMexc,
    _engine,
    _item,
    _store_message,
)
from tg_signal_ingest.mexc.errors import MexcNetworkError
from tg_signal_ingest.trading import SignalParser, TradingStore
from tg_signal_ingest.trading.notify import TradingNotifier
from tg_signal_ingest.trading.store import RiskLimitExceeded

OWNER = 100000001
DEVELOPER = 100000002


class FlakyBot:
    def __init__(self):
        self.sent: list[tuple[int, str]] = []

    def send_message(self, chat_id: int, text: str):
        self.sent.append((chat_id, text))
        return {"message_id": len(self.sent)}


def _notifier(engine, bot):
    notifier = TradingNotifier(engine.store, bot, owner_id=OWNER, developer_id=DEVELOPER)
    engine.notifier = notifier
    return notifier


def _ledger(store: TradingStore) -> list[dict]:
    return [
        dict(row) for row in store.db.conn.execute("SELECT * FROM trading_budget_ledger ORDER BY intent_id").fetchall()
    ]


def _reserved_total(store: TradingStore) -> Decimal:
    return sum(
        (Decimal(str(row["reserved_quote"] or "0")) for row in _ledger(store) if int(row["active"])),
        Decimal("0"),
    )


def _create_intent(store: TradingStore, mid: int) -> dict:
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=mid, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=mid,
        edit_version=0,
        idempotency_key=f"k{mid}",
        client_order_id=f"c{mid}",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    return intent


def test_reservations_are_tracked_per_intent_without_a_spending_cap(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    store = TradingStore(repo.db, cfg.trading)

    for mid in (1, 2, 3):
        assert _create_intent(store, mid)["state"] == "planned"

    assert len(_ledger(store)) == 3
    assert _reserved_total(store) == Decimal("30.06")
    assert not hasattr(store, "budget_summary")


def test_reservation_is_released_after_local_rejection(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    store = TradingStore(repo.db, cfg.trading)
    first = _create_intent(store, 1)
    store.transition_intent(first["intent_id"], "rejected", note="risk rejected")

    second = _create_intent(store, 2)
    assert second["state"] == "planned"
    released = next(row for row in _ledger(store) if row["intent_id"] == first["intent_id"])
    assert released["state"] == "released"
    assert int(released["active"]) == 0
    assert _reserved_total(store) == Decimal("10.02")


def test_uncertain_outcome_freezes_reservation_without_blocking_next_signal(cfg, repo):
    client = FakeMexc(place_error=MexcNetworkError("timeout after send"))
    engine = _engine(cfg, repo, client)

    first = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert first is not None and first["state"] == "unknown_requires_reconciliation"
    assert _reserved_total(engine.store) == Decimal("10.02")

    second = engine.process_item(
        _item(100, 2),
        _store_message(repo, 2, "BUY ETHUSDT\nENTRY: 100\nSL: 90\nTP1: 111\nTP2: 120"),
    )
    assert second is not None and second["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 2
    assert _reserved_total(engine.store) == Decimal("20.04")


def test_reservations_survive_engine_restart(cfg, repo):
    client = FakeMexc(place_error=MexcNetworkError("timeout"))
    engine = _engine(cfg, repo, client)
    engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    restarted = _engine(cfg, repo, client)  # new process/engine
    assert len(_ledger(restarted.store)) == 1
    assert _reserved_total(restarted.store) == Decimal("10.02")


def test_filled_order_settles_actual_spend_and_fee(cfg, repo):
    filled = {
        "orderId": "ex-fee",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0.1",
        "cumulativeQuoteQty": "10",
        "status": "FILLED",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-fee", "status": "FILLED"},
        query_results=[filled],
        trades=[
            {
                "id": "t-fee",
                "price": "100",
                "qty": "0.1",
                "quoteQty": "10",
                "commission": "0.01",
                "commissionAsset": "USDT",
                "time": 2,
            }
        ],
    )
    engine = _engine(cfg, repo, client)
    intent = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert intent is not None and intent["state"] == "filled"
    row = _ledger(engine.store)[0]
    assert row["state"] == "settled"
    assert int(row["active"]) == 0
    assert Decimal(row["spent_quote"]) == Decimal("10")
    assert Decimal(row["fee_quote"]) == Decimal("0.01")


def test_concurrent_reservations_are_atomic(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    for mid in range(1, 9):
        store.record_signal(parsed, channel_id=100, message_id=mid, edit_version=0)

    def attempt(mid: int) -> bool:
        try:
            store.create_intent(
                signal=parsed,
                signal_id=f"100:{mid}:0",
                channel_id=100,
                message_id=mid,
                edit_version=0,
                idempotency_key=f"k{mid}",
                client_order_id=f"c{mid}",
                mode="live",
                order_type="limit",
                quote_amount=Decimal("10"),
                base_quantity=Decimal("0.1"),
                limit_price=Decimal("100"),
            )
        except RiskLimitExceeded:
            return False
        return True

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(attempt, range(1, 9)))

    assert sum(results) == 8
    assert len(_ledger(store)) == 8
    assert _reserved_total(store) == Decimal("80.16")


def test_three_usdt_signal_below_symbol_minimum_is_skipped(cfg, repo):
    class MinFiveMeta(FakeMeta):
        def validate_order(self, **kwargs):
            quote = kwargs.get("quote_amount")
            if quote is not None and quote < Decimal("5"):
                raise ValueError("order value 3 is below the exchange minimum 5 USDT")

    class Registry:
        def get(self, symbol):
            return MinFiveMeta()

    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    engine.registry = Registry()
    cfg.trading.order_quote_amount = 3.0

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is None
    assert client.place_calls == 0
    signal = engine.store.get_signal("100:1:0")
    assert signal is not None and "below the exchange minimum" in signal["rejection_reason"]


def test_genuine_signal_result_notifies_both_with_original_text(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    _notifier(engine, bot)

    engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    texts = [text for _, text in bot.sent]
    joined = "\n".join(texts)
    assert len(bot.sent) == 2
    assert not any("سیگنال جدید دریافت شد" in text for text in texts)
    assert all(ALERT in text for text in texts)
    assert "exchange submission attempted: yes" in joined
    assert {chat for chat, _ in bot.sent} == {OWNER, DEVELOPER}


def test_sell_signal_is_reported_ignored_and_never_ordered(cfg, repo):
    client = FakeMexc()
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)

    result = engine.process_item(
        _item(100, 1),
        _store_message(repo, 1, "SELL BTCUSDT\nENTRY: 100\nTP1: 90\nSL: 105"),
    )
    assert result is None
    assert client.place_calls == 0
    joined = "\n".join(text for _, text in bot.sent)
    assert "سیگنال فروش نادیده گرفته می‌شود❌" in joined
    assert "سفارش واقعی" not in joined


def test_live_result_notification_contains_exchange_identity(cfg, repo):
    filled = {
        "orderId": "ex-notify",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "LIMIT",
        "price": "100",
        "origQty": "0.1",
        "executedQty": "0.1",
        "cumulativeQuoteQty": "10",
        "status": "FILLED",
        "time": 1,
    }
    client = FakeMexc(
        place_response={"orderId": "ex-notify", "status": "FILLED"},
        query_results=[filled],
    )
    engine = _engine(cfg, repo, client)
    bot = FlakyBot()
    _notifier(engine, bot)

    result = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert result is not None and result["state"] == "filled"
    joined = "\n".join(text for _, text in bot.sent)
    assert "✅ سفارش واقعی در MEXC ثبت" in joined
    assert "ex-notify" in joined
    assert "cumulative tracked spend" not in joined


def test_activation_boundary_advances_only_forward(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    first = store.ensure_activation_boundary(100)
    assert first == 0
    assert store.ensure_activation_boundary(100, minimum=19038) == 19038
    assert store.ensure_activation_boundary(100, minimum=18000) == 19038
    assert store.activation_boundary() == 19038


def test_legacy_live_intent_gets_reservation_before_execution(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    intent = _create_intent(store, 1)
    with store.db.transaction() as conn:
        conn.execute("DELETE FROM trading_budget_ledger WHERE intent_id = ?", (intent["intent_id"],))
    store.ensure_budget_reservation(intent["intent_id"])
    assert _reserved_total(store) == Decimal("10.02")


def test_receipt_is_suppressed_for_backlog_recovery(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    _notifier(engine, bot)
    with repo.db.transaction() as conn:
        conn.execute("UPDATE app_state SET value = '19038' WHERE key = 'trading.activation_boundary_message_id'")

    engine.process_item(_item(100, 19038), _store_message(repo, 19038, ALERT))
    assert bot.sent == []


def test_intent_creation_does_not_aggregate_daily_spend(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    _create_intent(store, 1)

    rows = repo.db.conn.execute("SELECT COUNT(*) AS c FROM trading_daily_spend").fetchone()
    assert int(rows["c"]) == 0
    assert not hasattr(store, "daily_totals")


def test_budget_ledger_reservation_is_idempotent(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    intent = _create_intent(store, 1)
    before = _ledger(store)

    store.ensure_budget_reservation(intent["intent_id"])
    assert _ledger(store) == before
    assert _reserved_total(store) == Decimal("10.02")
