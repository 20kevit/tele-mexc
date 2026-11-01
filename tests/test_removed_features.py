"""Regression guards for the removed features and simplified configuration."""

from __future__ import annotations

import pytest

from tg_signal_ingest.config import TradingConfig
from tg_signal_ingest.main import build_parser


@pytest.mark.parametrize("command", ["manage", "analyze"])
def test_removed_cli_surfaces_are_absent(command):
    with pytest.raises(SystemExit):
        build_parser().parse_args([command])


def test_management_channel_check_command_is_absent():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["auth", "check-channel"])


def test_removed_trading_configuration_fields_are_absent():
    trading = TradingConfig()
    for name in (
        "aggregate_budget_quote",
        "remaining_balance_reserve_pct",
        "fake_price_for_tests",
        "allowed_symbols",
        "allowlist_mode",
    ):
        assert not hasattr(trading, name)


def test_balance_fee_and_freshness_settings_are_retained():
    trading = TradingConfig()
    assert trading.balance_fee_buffer_pct >= 0
    assert trading.balance_max_age_s > 0
    assert trading.entry_trigger_stale_threshold_s > 0
    assert trading.stop_loss_trigger_offset_ticks >= 0
