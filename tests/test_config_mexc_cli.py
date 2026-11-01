"""Configuration loading for the MEXC CLI execution backend."""

from __future__ import annotations

import pytest

from tg_signal_ingest.config import AppConfig, ConfigError, _load_mexc_and_trading


def _app() -> AppConfig:
    return AppConfig(api_id=1, api_hash="a" * 32)


def test_cli_defaults_are_fail_closed_and_documented():
    app = _app()
    _load_mexc_and_trading(app, {}, {})
    assert app.mexc.cli_enabled is True
    assert app.mexc.cli_binary is None
    assert app.mexc.cli_profile is None
    assert app.mexc.cli_min_version == "0.0.22"
    assert app.mexc.cli_confirm_noninteractive_writes is True


def test_cli_section_overrides_defaults():
    app = _app()
    _load_mexc_and_trading(
        app,
        {
            "mexc_cli": {
                "binary": "/opt/mexc/bin/mexc",
                "profile": "tg-ingest",
                "timeout_s": 30,
                "min_version": "0.0.23",
                "enabled": False,
                "confirm_noninteractive_writes": True,
            }
        },
        {},
    )
    assert app.mexc.cli_binary == "/opt/mexc/bin/mexc"
    assert app.mexc.cli_profile == "tg-ingest"
    assert app.mexc.cli_timeout_s == 30.0
    assert app.mexc.cli_min_version == "0.0.23"
    assert app.mexc.cli_enabled is False
    assert app.mexc.cli_confirm_noninteractive_writes is True


def test_acknowledgement_arms_noninteractive_cli_writes_by_default():
    app = _app()
    _load_mexc_and_trading(
        app,
        {
            "trading": {
                "execution_acknowledgement": "I_UNDERSTAND_REAL_MONEY_AND_RISKS",
            }
        },
        {},
    )
    assert app.trading.execution_acknowledgement == "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
    assert app.mexc.cli_confirm_noninteractive_writes is True


def test_acknowledgement_preserves_an_explicit_noninteractive_opt_out():
    app = _app()
    _load_mexc_and_trading(
        app,
        {
            "mexc_cli": {"confirm_noninteractive_writes": False},
            "trading": {
                "execution_acknowledgement": "I_UNDERSTAND_REAL_MONEY_AND_RISKS",
            },
        },
        {},
    )
    assert app.mexc.cli_confirm_noninteractive_writes is False


def test_non_bool_noninteractive_setting_is_rejected():
    app = _app()
    with pytest.raises(ConfigError, match=r"confirm_noninteractive_writes"):
        _load_mexc_and_trading(app, {"mexc_cli": {"confirm_noninteractive_writes": "yes"}}, {})


@pytest.mark.parametrize("value", [0, -1, 121, float("nan")])
def test_cli_timeout_must_be_bounded(value):
    app = _app()
    with pytest.raises(ConfigError):
        _load_mexc_and_trading(app, {"mexc_cli": {"timeout_s": value}}, {})


def test_cli_min_version_shape_is_validated():
    app = _app()
    with pytest.raises(ConfigError):
        _load_mexc_and_trading(app, {"mexc_cli": {"min_version": "latest"}}, {})


def test_cli_backend_may_be_disabled_for_diagnostics():
    app = _app()
    _load_mexc_and_trading(
        app,
        {"mexc_cli": {"enabled": False}, "trading": {"order_quote_amount": 10}},
        {},
    )
    assert app.mexc.cli_enabled is False


def test_legacy_mode_selector_is_rejected():
    app = _app()
    with pytest.raises(ConfigError, match="mode"):
        _load_mexc_and_trading(
            app,
            {
                "mexc_cli": {"enabled": False},
                "trading": {
                    "mode": "live",
                    "live_enabled_token": "I_UNDERSTAND_REAL_MONEY_AND_RISKS",
                },
            },
            {},
        )
