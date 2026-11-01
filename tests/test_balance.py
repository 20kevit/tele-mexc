"""Fail-closed MEXC Spot balance preflight tests (no network)."""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.trading.balance import (
    BalanceError,
    parse_account,
    validate_balance,
)

GOOD = {
    "canTrade": True,
    "permissions": ["SPOT"],
    "balances": [
        {"asset": "BTC", "free": "2", "locked": "0", "available": "2"},
        {"asset": "USDT", "free": "100", "locked": "0", "available": "100"},
    ],
}


def _snapshot(raw=GOOD, fetched_at=0.0):
    return parse_account(raw, fetched_at=fetched_at)


def test_parse_valid_account_response():
    snap = _snapshot()
    assert snap.can_trade is True
    assert snap.balances["USDT"].spendable == Decimal("100")
    assert snap.balances["BTC"].spendable == Decimal("2")


def test_available_lower_than_free_is_used():
    raw = {
        "canTrade": True,
        "permissions": ["SPOT"],
        "balances": [{"asset": "USDT", "free": "100", "available": "40", "locked": "60"}],
    }
    snap = _snapshot(raw)
    assert snap.balances["USDT"].spendable == Decimal("40")


@pytest.mark.parametrize(
    "raw",
    [
        "not-a-dict",
        {"canTrade": False, "balances": []},
        {"canTrade": True, "balances": "nope"},
        {"canTrade": True, "balances": [{"asset": "", "free": "1"}]},
        {"canTrade": True, "balances": [{"asset": "USDT"}]},
        {"canTrade": True, "balances": [{"asset": "USDT", "free": "nan"}]},
        {"canTrade": True, "balances": [{"asset": "USDT", "free": "-1"}]},
        {
            "canTrade": True,
            "balances": [
                {"asset": "USDT", "free": "1"},
                {"asset": "USDT", "free": "2"},
            ],
        },
        {"canTrade": True, "permissions": ["FUTURES"], "balances": []},
    ],
)
def test_malformed_account_responses_fail_closed(raw):
    with pytest.raises(BalanceError):
        parse_account(raw, fetched_at=0.0)


def test_stale_snapshot_is_rejected(cfg):
    snap = _snapshot(fetched_at=0.0)
    with pytest.raises(BalanceError, match="stale"):
        validate_balance(
            snap,
            cfg=cfg.trading,
            side="buy",
            base_asset="BTC",
            quote_asset="USDT",
            base_quantity=None,
            quote_amount=Decimal("10"),
            now=cfg.trading.balance_max_age_s + 1,
        )


def test_buy_respects_fee_buffer(cfg):
    cfg.trading.balance_fee_buffer_pct = 0.5
    snap = _snapshot()
    # 100 free; 10 + 0.5% fee = 10.05 fits.
    validate_balance(
        snap,
        cfg=cfg.trading,
        side="buy",
        base_asset="BTC",
        quote_asset="USDT",
        base_quantity=None,
        quote_amount=Decimal("10"),
        now=0,
    )


def test_buy_rejected_by_fee_inclusive_requirement(cfg):
    cfg.trading.balance_fee_buffer_pct = 1.0
    raw = {
        "canTrade": True,
        "permissions": ["SPOT"],
        "balances": [{"asset": "USDT", "free": "10", "available": "10"}],
    }
    snap = _snapshot(raw)
    with pytest.raises(BalanceError, match="insufficient"):
        validate_balance(
            snap,
            cfg=cfg.trading,
            side="buy",
            base_asset="BTC",
            quote_asset="USDT",
            base_quantity=None,
            quote_amount=Decimal("10"),
            now=0,
        )


def test_sell_uses_base_asset(cfg):
    cfg.trading.balance_fee_buffer_pct = 0.0
    snap = _snapshot()
    validate_balance(
        snap,
        cfg=cfg.trading,
        side="sell",
        base_asset="BTC",
        quote_asset="USDT",
        base_quantity=Decimal("1"),
        quote_amount=None,
        now=0,
    )
    with pytest.raises(BalanceError, match="insufficient"):
        validate_balance(
            snap,
            cfg=cfg.trading,
            side="sell",
            base_asset="BTC",
            quote_asset="USDT",
            base_quantity=Decimal("3"),
            quote_amount=None,
            now=0,
        )


def test_outstanding_local_orders_are_subtracted(cfg):
    cfg.trading.balance_fee_buffer_pct = 0.0
    snap = _snapshot()
    with pytest.raises(BalanceError, match="insufficient"):
        validate_balance(
            snap,
            cfg=cfg.trading,
            side="buy",
            base_asset="BTC",
            quote_asset="USDT",
            base_quantity=None,
            quote_amount=Decimal("90"),
            outstanding_quote=Decimal("15"),
            now=0,
        )
