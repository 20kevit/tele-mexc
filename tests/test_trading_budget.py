"""Aggregate test-budget ledger, 3-USDT sizing and notification workflow.

All exchange/Telegram clients are fakes.  No test can reach MEXC or Telegram.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

from tests.test_trading_engine import (
    ALERT,
    FakeMeta,
    FakeMexc,
    _engine,
    _item,
    _store_message,
)
from tg_signal_ingest.config import load_config
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


def test_per_order_amount_is_not_capped_by_aggregate_reference(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.aggregate_budget_quote = 25.0
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)

    for mid in (1, 2, 3):
        signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=mid, edit_version=0)
        intent, inserted = store.create_intent(
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
        assert inserted is True
        assert intent["state"] == "planned"

    summary = store.budget_summary()
    # 3 x (10 + 0.2% fee buffer) is tracked even though it exceeds the reference.
    assert summary["limit"] == Decimal("25.0")
    assert summary["reserved"] == Decimal("30.06")
    assert summary["remaining"] == Decimal("0")  # display only, never enforced


def test_released_reservation_frees_tracked_budget_after_rejection(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.aggregate_budget_quote = 15.0
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="k1",
        client_order_id="c1",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    store.transition_intent(intent["intent_id"], "rejected", note="risk rejected")

    signal_id2, _ = store.record_signal(parsed, channel_id=100, message_id=2, edit_version=0)
    second, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id2,
        channel_id=100,
        message_id=2,
        edit_version=0,
        idempotency_key="k2",
        client_order_id="c2",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("10"),
        base_quantity=Decimal("0.1"),
        limit_price=Decimal("100"),
    )
    assert second["state"] == "planned"
    assert store.budget_summary()["spent"] == Decimal("0")


def test_uncertain_outcome_freezes_tracking_but_does_not_block_next_signal(cfg, repo):
    client = FakeMexc(place_error=MexcNetworkError("timeout after send"))
    cfg.trading.aggregate_budget_quote = 15.0
    engine = _engine(cfg, repo, client)

    first = engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))
    assert first is not None and first["state"] == "unknown_requires_reconciliation"
    assert engine.store.budget_summary()["reserved"] == Decimal("10.02")

    second = engine.process_item(
        _item(100, 2), _store_message(repo, 2, "BUY ETHUSDT\nENTRY: 100\nSL: 90\nTP1: 111\nTP2: 120")
    )
    # The per-order amount is not capped by the aggregate reference: the second
    # signal reaches the exchange and fails on its own merits.
    assert second is not None and second["state"] == "unknown_requires_reconciliation"
    assert client.place_calls == 2
    assert engine.store.budget_summary()["reserved"] == Decimal("20.04")


def test_budget_survives_engine_restart(cfg, repo):
    client = FakeMexc(place_error=MexcNetworkError("timeout"))
    cfg.trading.aggregate_budget_quote = 50.0
    engine = _engine(cfg, repo, client)
    engine.process_item(_item(100, 1), _store_message(repo, 1, ALERT))

    restarted = _engine(cfg, repo, client)  # new process/engine
    summary = restarted.store.budget_summary()
    assert summary["reserved"] == Decimal("10.02")
    assert summary["remaining"] == Decimal("39.98")


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
    summary = engine.store.budget_summary()
    assert summary["reserved"] == Decimal("0")
    assert summary["spent"] == Decimal("10.01")
    assert summary["remaining"] == Decimal("39.99")


def test_concurrent_reservations_are_all_tracked(cfg, repo):
    cfg.trading.aggregate_budget_quote = 25.0
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
    # The reference value is not a cap; every reservation is tracked.
    assert store.budget_summary()["reserved"] == Decimal("80.16")


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
    # One consolidated result per recipient, no preliminary "received" notice.
    assert len(bot.sent) == 2
    assert not any("سیگنال جدید دریافت شد" in text for text in texts)
    assert all(ALERT in text for text in texts)  # exact original text
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
    assert "cumulative tracked spend" in joined
    assert "not an order cap" in joined


def test_live_fixed_amount_three_and_aggregate_fifty_are_explicit(tmp_path: Path):
    root = tmp_path / "cfg"
    (root / "config").mkdir(parents=True)
    (root / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h\n")
    (root / "config" / "config.toml").write_text(
        "[trading]\n"
        "execution_acknowledgement = 'I_UNDERSTAND_REAL_MONEY_AND_RISKS'\n"
        "order_quote_amount = 3\n"
        "aggregate_budget_quote = 50\n"
    )
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.order_quote_amount == 3.0
    assert cfg.trading.aggregate_budget_quote == 50.0


def test_activation_boundary_advances_only_forward(cfg, repo):
    store = TradingStore(repo.db, cfg.trading)
    first = store.ensure_activation_boundary(100)
    assert first == 0
    assert store.ensure_activation_boundary(100, minimum=19038) == 19038
    # A smaller (older) value never rolls the boundary back.
    assert store.ensure_activation_boundary(100, minimum=18000) == 19038
    assert store.activation_boundary() == 19038


def test_legacy_live_intent_gets_budget_reservation_before_execution(cfg, repo):
    cfg.trading.aggregate_budget_quote = 50.0
    store = TradingStore(repo.db, cfg.trading)
    parsed = SignalParser().parse(ALERT)
    signal_id, _ = store.record_signal(parsed, channel_id=100, message_id=1, edit_version=0)
    intent, _ = store.create_intent(
        signal=parsed,
        signal_id=signal_id,
        channel_id=100,
        message_id=1,
        edit_version=0,
        idempotency_key="legacy",
        client_order_id="legacy",
        mode="live",
        order_type="limit",
        quote_amount=Decimal("3"),
        base_quantity=Decimal("0.03"),
        limit_price=Decimal("100"),
    )
    # Simulate a row created before migration 12.
    with store.db.transaction() as conn:
        conn.execute("DELETE FROM trading_budget_ledger WHERE intent_id = ?", (intent["intent_id"],))
    store.ensure_budget_reservation(intent["intent_id"])
    assert store.budget_summary()["reserved"] == Decimal("3.006")


def test_receipt_is_suppressed_for_backlog_recovery(cfg, repo):
    engine = _engine(cfg, repo)
    bot = FlakyBot()
    _notifier(engine, bot)
    with repo.db.transaction() as conn:
        conn.execute("UPDATE app_state SET value = '19038' WHERE key = 'trading.activation_boundary_message_id'")

    engine.process_item(_item(100, 19038), _store_message(repo, 19038, ALERT))
    assert bot.sent == []


def test_aggregate_budget_accepts_any_positive_value(tmp_path: Path):
    root = tmp_path / "cfg"
    (root / "config").mkdir(parents=True)
    (root / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h\n")
    (root / "config" / "config.toml").write_text("[trading]\norder_quote_amount = 3\naggregate_budget_quote = 500\n")
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.aggregate_budget_quote == 500.0
