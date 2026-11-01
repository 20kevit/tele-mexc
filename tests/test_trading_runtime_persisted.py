"""Persisted owner amount: config precedence and restart behaviour.

The Telegram control menu stores the per-order quote amount in
``control_settings``.  Environment/TOML configuration must always win, while a
built-in default may be replaced by the stored value.  These tests use a
disposable SQLite database and never touch the network.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.trading.runtime import (
    apply_persisted_trading_settings,
    build_trading_engine,
)
from tg_signal_ingest.trading.trade_store import TradeStore


def _store(repo) -> TradeStore:
    return TradeStore(repo.db, None)


def test_database_amount_replaces_builtin_default(cfg, repo):
    cfg.trading.order_quote_amount = 3.0
    cfg.trading.order_quote_amount_source = "default (3 USDT)"
    _store(repo).set_setting("order_quote_amount", "7.5")

    assert apply_persisted_trading_settings(cfg, repo) is True
    assert cfg.trading.order_quote_amount == pytest.approx(7.5)
    assert cfg.trading.order_quote_amount_source == "database (Telegram menu)"
    assert cfg.trading.amount_configured is True


def test_owner_menu_wins_over_config_file(cfg, repo):
    cfg.trading.order_quote_amount = 4.25
    cfg.trading.order_quote_amount_source = "config"
    _store(repo).set_setting("order_quote_amount", "7.5")

    assert apply_persisted_trading_settings(cfg, repo) is True
    assert cfg.trading.order_quote_amount == pytest.approx(7.5)
    assert cfg.trading.order_quote_amount_source == "database (Telegram menu)"


def test_environment_wins_over_database(cfg, repo):
    cfg.trading.order_quote_amount = 4.25
    cfg.trading.order_quote_amount_source = "env"
    _store(repo).set_setting("order_quote_amount", "99")

    assert apply_persisted_trading_settings(cfg, repo) is False
    assert cfg.trading.order_quote_amount == pytest.approx(4.25)
    assert cfg.trading.order_quote_amount_source == "env"


def test_legacy_config_amount_is_overridden_by_menu(cfg, repo):
    cfg.trading.order_quote_amount = 10.0
    cfg.trading.order_quote_amount_source = "legacy max_quote_per_order"
    _store(repo).set_setting("order_quote_amount", "3")

    assert apply_persisted_trading_settings(cfg, repo) is True
    assert cfg.trading.order_quote_amount == pytest.approx(3.0)


@pytest.mark.parametrize("stored", ["", "0", "-5", "nan", "inf", "not-a-number"])
def test_invalid_stored_amount_is_ignored(cfg, repo, stored):
    cfg.trading.order_quote_amount = 3.0
    cfg.trading.order_quote_amount_source = "default (3 USDT)"
    _store(repo).set_setting("order_quote_amount", stored)

    assert apply_persisted_trading_settings(cfg, repo) is False
    assert cfg.trading.order_quote_amount == pytest.approx(3.0)
    assert cfg.trading.order_quote_amount_source == "default (3 USDT)"


def test_missing_setting_keeps_default(cfg, repo):
    cfg.trading.order_quote_amount = 3.0
    cfg.trading.order_quote_amount_source = "default (3 USDT)"

    assert apply_persisted_trading_settings(cfg, repo) is False
    assert cfg.trading.order_quote_amount == pytest.approx(3.0)


def test_amount_survives_restart(cfg, repo):
    """A fresh config (as built on process start) picks up the persisted value."""

    _store(repo).set_setting("order_quote_amount", "6.5")

    restarted = type(cfg)(
        api_id=cfg.api_id,
        api_hash=cfg.api_hash,
        data_dir=cfg.data_dir,
        db_path=cfg.db_path,
        session_path=cfg.session_path,
    )
    assert restarted.trading.order_quote_amount_source == "default (3 USDT)"
    assert apply_persisted_trading_settings(restarted, repo) is True
    assert restarted.trading.order_quote_amount == pytest.approx(6.5)


def test_round_trip_through_store(cfg, repo):
    store = _store(repo)
    store.set_setting("order_quote_amount", "5")
    store.set_setting("order_quote_amount", "8.25")
    assert store.get_setting("order_quote_amount") == "8.25"


def test_build_engine_applies_persisted_amount(cfg, repo):
    """The production builder must apply the stored amount before sizing."""

    _store(repo).set_setting("order_quote_amount", "6.5")

    engine = build_trading_engine(cfg, repo, client=object())

    assert engine.cfg.trading.order_quote_amount == pytest.approx(6.5)
    assert engine.cfg.trading.order_quote_amount_source == "database (Telegram menu)"


def test_persisted_buy_amount_reaches_a_real_spot_intent(cfg, repo):
    from tests.test_trading_engine import FakeMexc, FakeRegistry, _item, _store_message
    from tg_signal_ingest.trading.engine import TradingEngine

    _store(repo).set_setting("order_quote_amount", "6.5")
    assert apply_persisted_trading_settings(cfg, repo) is True
    client = FakeMexc(
        place_response={"orderId": "ex-owner-amount", "status": "NEW"},
    )
    engine = TradingEngine(cfg, repo, client=client, registry=FakeRegistry())
    engine.store.set_app_state_once("trading.activation_boundary_message_id", "0")

    intent = engine.process_item(
        _item(100, 900),
        _store_message(repo, 900, "BUY BTCUSDT\nENTRY: 100\nSL: 95\nTP1: 110\nTP2: 120"),
    )
    assert intent is not None
    assert Decimal(intent["quote_amount"]) == Decimal("6.5")
    assert client.place_calls == 1


def test_legacy_buy_quote_setting_is_read_only_as_fallback(cfg, repo):
    cfg.trading.order_quote_amount = 3.0
    cfg.trading.order_quote_amount_source = "default (3 USDT)"
    _store(repo).set_setting("buy_quote_usd", "9.25")

    assert apply_persisted_trading_settings(cfg, repo) is True
    assert cfg.trading.order_quote_amount == pytest.approx(9.25)
    assert cfg.trading.order_quote_amount_source == "legacy database (buy_quote_usd)"


def test_canonical_setting_wins_over_legacy_buy_quote(cfg, repo):
    store = _store(repo)
    store.set_setting("buy_quote_usd", "9.25")
    store.set_setting("order_quote_amount", "5.5")

    assert apply_persisted_trading_settings(cfg, repo) is True
    assert cfg.trading.order_quote_amount == pytest.approx(5.5)
    assert cfg.trading.order_quote_amount_source == "database (Telegram menu)"
