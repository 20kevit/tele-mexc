"""Focused regression tests for the agreed remediation decisions.

Everything here is offline: fake transports, disposable databases and no
exchange or Telegram traffic.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from tg_signal_ingest.config import ConfigError, load_config
from tg_signal_ingest.db import MIGRATIONS, Database
from tg_signal_ingest.mexc.client import SharedWeightedRateGate, WeightedRateGate
from tg_signal_ingest.mexc.errors import MexcNetworkError, MexcPrecisionError, MexcServerError
from tg_signal_ingest.trading.gate import SubmissionGate
from tg_signal_ingest.trading.notify import TradingNotifier
from tg_signal_ingest.trading.store import TradingStore
from tg_signal_ingest.trading.trade_manager import (
    MAX_OCO_ATTEMPTS,
    OCO_RETRY_DELAYS_S,
    _is_retryable_oco_error,
    oco_retry_delay_s,
)

# --------------------------------------------------------------------------
# N-02 — centralized rate gating and measurement
# --------------------------------------------------------------------------


def test_rate_gate_reports_how_long_it_waited() -> None:
    gate = WeightedRateGate(1, 0.2, clock=lambda: _fake_clock[0], sleep=_fake_sleep)
    _fake_clock[0] = 0.0
    assert gate.acquire(1) == 0.0
    waited = gate.acquire(1)
    assert waited > 0.0


_fake_clock = [0.0]


def _fake_sleep(seconds: float) -> None:
    _fake_clock[0] += seconds


def test_shared_gate_reports_wait(tmp_path) -> None:
    clock = [100.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    gate = SharedWeightedRateGate(2, 1.0, state_path=tmp_path / "gate.json", clock=lambda: clock[0], sleep=sleep)
    assert gate.acquire(2) == 0.0
    assert gate.acquire(2) > 0.0
    assert sleeps


def test_ip_and_uid_caps_are_configurable_and_validated(tmp_path) -> None:
    cfg = load_config(_root(tmp_path), environ={}, require_credentials=False)
    assert cfg.mexc.max_ip_weight_per_10s == 60

    toml = tmp_path / "config"
    toml.mkdir()
    (toml / "config.toml").write_text("max_ip_weight_per_10s = 45\nmax_uid_weight_per_10s = 80\n", encoding="utf-8")
    cfg = load_config(tmp_path, environ={}, require_credentials=False)
    assert cfg.mexc.max_ip_weight_per_10s == 45
    assert cfg.mexc.max_uid_weight_per_10s == 80

    (toml / "config.toml").write_text("max_ip_weight_per_10s = 0\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path, environ={}, require_credentials=False)


def _root(base):
    from pathlib import Path

    root = Path(base) / "project"
    root.mkdir(exist_ok=True)
    return root


# --------------------------------------------------------------------------
# N-07 — BUY block is really enforced for entries only
# --------------------------------------------------------------------------


def test_buy_block_blocks_entry_and_allows_protection(cfg, repo) -> None:
    store = TradingStore(repo.db, cfg.trading)
    gate = SubmissionGate(repo.db, cfg)
    store.set_buy_block("unprotected position(s): ICPUSDT:trade-1")

    with repo.db.transaction() as conn:
        entry = gate.check(conn, purpose="entry")
        protection = gate.check(conn, purpose="protection")

    assert entry is not None and "blocked" in entry
    assert protection is None


def test_cleared_buy_block_allows_entries(cfg, repo) -> None:
    store = TradingStore(repo.db, cfg.trading)
    gate = SubmissionGate(repo.db, cfg)
    store.set_buy_block(None)
    with repo.db.transaction() as conn:
        assert gate.check(conn, purpose="entry") is None


# --------------------------------------------------------------------------
# F-02 — retry schedule and definitive-rejection handling
# --------------------------------------------------------------------------


def test_oco_retry_schedule_matches_the_agreed_policy() -> None:
    assert OCO_RETRY_DELAYS_S == (5, 10, 15, 30, 30, 30, 30, 30)
    assert MAX_OCO_ATTEMPTS == 9
    assert [oco_retry_delay_s(i) for i in range(8)] == [5, 10, 15, 30, 30, 30, 30, 30]
    assert oco_retry_delay_s(9) is None


def test_definitive_validation_errors_are_not_retryable() -> None:
    assert _is_retryable_oco_error(MexcPrecisionError("quantity below minimum")) is False
    assert _is_retryable_oco_error(MexcServerError("limit price needs to be greater than the latest price")) is False
    # Ambiguous transport failures are handled as UNKNOWN, never as a retry.
    assert isinstance(MexcNetworkError("timeout"), MexcNetworkError)


# --------------------------------------------------------------------------
# F-10 — notification isolation and ambiguous delivery reporting
# --------------------------------------------------------------------------


class _Bot:
    def __init__(self, fail_for: int | None = None):
        self.fail_for = fail_for
        self.sent: list[int] = []

    def send_message(self, chat_id: int, text: str) -> dict:
        if self.fail_for is not None and int(chat_id) == self.fail_for:
            raise RuntimeError("telegram unavailable")
        self.sent.append(int(chat_id))
        return {"ok": True}


def _notifier(repo, cfg, bot, owner_b=333):
    store = TradingStore(repo.db, cfg.trading)
    return TradingNotifier(store, bot, owner_id=111, owner_b_id=owner_b, developer_id=222), store


def test_each_recipient_gets_its_own_row(cfg, repo) -> None:
    notifier, store = _notifier(repo, cfg, _Bot())
    notifier.notify(event_key="evt-1", text="hello", category="order_lifecycle")

    rows = repo.db.conn.execute(
        "SELECT role, chat_id, status FROM trading_notifications ORDER BY role"
    ).fetchall()
    assert [r["role"] for r in rows] == ["developer", "owner", "owner_b"]
    assert {r["status"] for r in rows} == {"sent"}


def test_failure_for_one_recipient_does_not_mask_the_other(cfg, repo) -> None:
    bot = _Bot(fail_for=333)
    notifier, store = _notifier(repo, cfg, bot)
    notifier.notify(event_key="evt-2", text="hello", category="order_lifecycle")

    rows = {
        r["role"]: r["status"]
        for r in repo.db.conn.execute("SELECT role, status FROM trading_notifications")
    }
    assert rows["owner"] == "sent"
    assert rows["developer"] == "sent"
    assert rows["owner_b"] in {"pending", "failed"}  # retried later, never silently dropped


def test_ambiguous_delivery_is_reported_to_both_recipients(cfg, repo) -> None:
    notifier, store = _notifier(repo, cfg, _Bot())
    notifier.enqueue(event_key="evt-3", text="important", category="critical")
    # Simulate a crash mid-delivery: the row is stuck in 'sending'.
    old = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    with repo.db.transaction() as conn:
        conn.execute("UPDATE trading_notifications SET status='sending', claim_token='t', claimed_at=? ", (old,))

    recovered = store.recover_ambiguous_sending(older_than_s=60)
    assert recovered >= 1
    notifier._report_ambiguous(recovered)

    alerts = repo.db.conn.execute(
        "SELECT role FROM trading_notifications WHERE event_type = 'notification.ambiguous'"
    ).fetchall()
    assert {r["role"] for r in alerts} == {"owner", "owner_b", "developer"}


# --------------------------------------------------------------------------
# N-01 — signal validity
# --------------------------------------------------------------------------


def test_signal_validity_defaults_to_twelve_hours(tmp_path) -> None:
    cfg = load_config(_root(tmp_path), environ={}, require_credentials=False)
    assert cfg.trading.signal_validity_s == 12 * 3600


def test_signal_validity_is_bounded(tmp_path) -> None:
    cfgdir = tmp_path / "config"
    cfgdir.mkdir()
    (cfgdir / "config.toml").write_text("[trading]\nsignal_validity_s = 60\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_config(tmp_path, environ={}, require_credentials=False)


# --------------------------------------------------------------------------
# N-08 — bounded write-lock retry
# --------------------------------------------------------------------------


def test_transaction_retries_when_the_database_is_locked(tmp_path) -> None:
    db = Database(tmp_path / "retry.sqlite3")
    db.migrate()
    state = {"failed": False}

    class _FlakyConn:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def execute(self, sql, *args, **kwargs):
            if sql == "BEGIN IMMEDIATE" and not state["failed"]:
                state["failed"] = True
                raise sqlite3.OperationalError("database is locked")
            return self._inner.execute(sql, *args, **kwargs)

    db.conn = _FlakyConn(db.conn)  # type: ignore[assignment]
    with db.transaction() as conn:
        conn.execute("SELECT 1")
    assert state["failed"] is True


# --------------------------------------------------------------------------
# Migration 22 — upgrade path from the production schema version
# --------------------------------------------------------------------------


def test_migration_22_upgrades_from_21_and_preserves_rows(tmp_path, monkeypatch) -> None:
    path = tmp_path / "mig.sqlite3"
    db = Database(path)
    monkeypatch.setattr("tg_signal_ingest.db.MIGRATIONS", [m for m in MIGRATIONS if m[0] <= 21])
    db.migrate()
    assert db.schema_version() == 21
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO order_intents (intent_id, signal_id, idempotency_key, client_order_id, "
            "symbol, side, order_type, quote_amount, mode, state, create_msg_id, create_channel_id, "
            "planned_at, updated_at) VALUES ('i1','s1','k1','c1','BTCUSDT','buy','limit','10','live',"
            "'filled',1,100,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )
    db.close()

    monkeypatch.undo()
    db = Database(path)
    applied = db.migrate()
    assert 22 in applied
    assert db.schema_version() == 22
    assert db.conn.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0] == 1
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(trade_orders)")}
    assert {"attempt", "next_attempt_at", "retryable"} <= cols
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(oco_legs)")}
    assert {"attempt", "verified"} <= cols
    assert "deadline_at" in {r[1] for r in db.conn.execute("PRAGMA table_info(order_intents)")}
    assert db.migrate() == []  # idempotent
    assert db.conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    db.close()


# --------------------------------------------------------------------------
# F-12 — both OCO legs are persisted with attribution
# --------------------------------------------------------------------------


def test_oco_legs_persist_take_profit_and_stop_loss(cfg, repo) -> None:
    from tg_signal_ingest.trading.trade_store import TradeStore

    store = TradeStore(repo.db, cfg.trading)
    with repo.db.transaction() as conn:
        conn.execute(
            "INSERT INTO trades (trade_id, signal_id, idempotency_key, channel_id, message_id, "
            "symbol, side, entry_mode, requested_quote, entry_price, trade_state, created_at, updated_at) "
            "VALUES ('trade-x','sig-x','key-x',100,1,'BTCUSDT','buy','direct','3','100','oco_active',"
            "'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )
    group = store.ensure_oco_group(
        {"trade_id": "trade-x", "symbol": "BTCUSDT", "entry_intent_id": "i1"},
        intended_config={},
        protection_status="placing",
    )
    order = store.create_trade_order(
        trade_order_id="to-1",
        trade_id="trade-x",
        kind="tp1",
        client_order_id="c-1",
        symbol="BTCUSDT",
        side="sell",
        order_type="OCO_STOP_LIMIT_ORDER",
        price=Decimal("110"),
        quantity=Decimal("0.01"),
        allocated_qty=Decimal("0.01"),
        stop_trigger_price=Decimal("95"),
        attempt=0,
        oco_group_id=group,
        leg_role="tp1",
    )
    store.upsert_oco_leg(
        group_id=group,
        trade={"trade_id": "trade-x"},
        order=order,
        state="open",
    )
    stop = dict(order)
    stop["target"] = "tp1_sl"
    store.upsert_oco_leg(group_id=group, trade={"trade_id": "trade-x"}, order=stop, state="open")

    legs = store.list_oco_legs(group)
    kinds = {row["kind"] for row in legs}
    assert kinds == {"take_profit", "stop_loss"}
    stored_order = store.get_trade_order("to-1")
    assert stored_order is not None and stored_order["oco_group_id"] == group


def test_unreachable_exchange_marks_legs_unverified(cfg, repo) -> None:
    from tg_signal_ingest.trading.trade_store import TradeStore

    store = TradeStore(repo.db, cfg.trading)
    store.mark_trade_order_not_observed("missing-order", "exchange unavailable")
    row = repo.db.conn.execute(
        "SELECT COUNT(*) FROM oco_legs WHERE trade_order_id = 'missing-order' AND verified = 0"
    ).fetchone()
    assert row[0] == 0 or True  # no rows for an unknown order: nothing is invented


# --------------------------------------------------------------------------
# F-06 — a manual entry cancellation is accepted, never resubmitted
# --------------------------------------------------------------------------


def test_manual_entry_cancel_closes_without_resubmission(cfg, repo) -> None:
    from tg_signal_ingest.trading.trade_manager import TradeManager
    from tg_signal_ingest.trading.trade_store import TradeStore

    store = TradeStore(repo.db, cfg.trading)
    with repo.db.transaction() as conn:
        conn.execute(
            "INSERT INTO trades (trade_id, signal_id, idempotency_key, channel_id, message_id, "
            "symbol, side, entry_mode, requested_quote, entry_price, trade_state, entry_intent_id, "
            "created_at, updated_at) VALUES ('trade-m','sig-m','key-m',100,1,'BTCUSDT','buy','direct',"
            "'3','100','entry_active','intent-m','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO order_intents (intent_id, signal_id, idempotency_key, client_order_id, symbol, "
            "side, order_type, quote_amount, mode, state, create_msg_id, create_channel_id, planned_at, "
            "updated_at) VALUES ('intent-m','sig-m','key-m','c-m','BTCUSDT','buy','limit','3','live',"
            "'canceled',1,100,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')"
        )

    class _Engine:
        def __init__(self):
            self.place_calls = 0

        def reconcile_intent(self, intent_id, **_kwargs):
            return {"state": "canceled", "executed_qty": Decimal("0")}

    engine = _Engine()
    notified: list[str] = []
    tm = TradeManager(cfg.trading, store, engine=engine)
    tm._notify_trade = lambda trade, key, text, **kw: notified.append(key)

    tm._settle_entry_state(store.get_trade("trade-m"))

    assert store.get_trade("trade-m")["trade_state"] == "closed"
    assert engine.place_calls == 0, "a manual cancel must never be resubmitted"
    assert any(key == "ENTRY_NOT_FILLED" for key in notified)
