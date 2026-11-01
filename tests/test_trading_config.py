"""MEXC + trading configuration loading (offline, no real credentials)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tg_signal_ingest.config import DEFAULT_MEXC_BASE_URL, ConfigError, load_config


def _root_with(toml_text: str, env: dict[str, str] | None = None, tmp_path: Path | None = None):
    tmp = tmp_path or Path("/tmp/opencode/test-cfg")
    tmp.mkdir(parents=True, exist_ok=True)
    (tmp / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h\n")
    (tmp / "config").mkdir(exist_ok=True)
    (tmp / "config" / "config.toml").write_text(toml_text)
    return tmp


def test_default_execution_is_unacknowledged_and_amount_is_3_usdt(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.execution_acknowledgement is None
    assert not hasattr(cfg.trading, "mode")
    assert not hasattr(cfg.trading, "live_enabled_token")
    assert cfg.trading.order_quote_amount == 3.0
    assert cfg.trading.amount_configured is True
    assert cfg.trading.order_quote_amount_source == "default (3 USDT)"
    assert cfg.mexc.base_url == DEFAULT_MEXC_BASE_URL
    assert cfg.mexc.api_key is None and cfg.mexc.api_secret is None
    assert cfg.trading.emergency_stop is False


def test_mexc_creds_from_env_are_loaded(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(
        root=root,
        environ={"MEXC_API_KEY": "key-abc", "MEXC_API_SECRET": "secret-xyz"},
        require_credentials=False,
    )
    assert cfg.mexc.api_key == "key-abc"
    assert cfg.mexc.api_secret == "secret-xyz"


def test_env_overrides_toml_for_credentials(tmp_path):
    root = _root_with('mexc_api_key = "from-toml"', tmp_path=tmp_path)
    cfg = load_config(root=root, environ={"MEXC_API_KEY": "from-env"}, require_credentials=False)
    assert cfg.mexc.api_key == "from-env"


def test_invalid_base_url_rejected(tmp_path):
    root = _root_with('mexc_base_url = "http://insecure.example"', tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="https"):
        load_config(root=root, environ={}, require_credentials=False)


def test_legacy_mode_selector_is_rejected(tmp_path):
    for value in ("sim", "testonly", "live", "yolo"):
        root = _root_with(f"[trading]\nmode = '{value}'", tmp_path=tmp_path / value)
        with pytest.raises(ConfigError, match="mode"):
            load_config(root=root, environ={}, require_credentials=False)


def test_legacy_live_token_is_rejected(tmp_path):
    root = _root_with("[trading]\nlive_enabled_token = 'anything'", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="live_enabled_token"):
        load_config(root=root, environ={}, require_credentials=False)


def test_execution_acknowledgement_loads_from_toml_and_env(tmp_path):
    root = _root_with(
        "[trading]\nexecution_acknowledgement = 'I_UNDERSTAND_REAL_MONEY_AND_RISKS'",
        tmp_path=tmp_path,
    )
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.execution_acknowledgement == "I_UNDERSTAND_REAL_MONEY_AND_RISKS"
    assert cfg.trading.order_quote_amount == 3.0
    assert cfg.trading.order_quote_amount_source == "default (3 USDT)"

    root = _root_with("", tmp_path=tmp_path / "env")
    cfg = load_config(
        root=root,
        environ={"TRADING_EXECUTION_ACKNOWLEDGEMENT": "I_UNDERSTAND_REAL_MONEY_AND_RISKS"},
        require_credentials=False,
    )
    assert cfg.trading.execution_acknowledgement == "I_UNDERSTAND_REAL_MONEY_AND_RISKS"


def test_default_amount_is_3_usdt_and_can_be_overridden(tmp_path):
    root = _root_with("[trading]\norder_quote_amount = 7.5", tmp_path=tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.order_quote_amount == 7.5
    assert cfg.trading.order_quote_amount_source == "config"


def test_env_amount_overrides_default(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(
        root=root,
        environ={"TRADING_ORDER_QUOTE_AMOUNT": "12.5"},
        require_credentials=False,
    )
    assert cfg.trading.order_quote_amount == 12.5
    assert cfg.trading.order_quote_amount_source == "env"


def test_amount_must_be_positive(tmp_path):
    root = _root_with("[trading]\norder_quote_amount = 0", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)

    root = _root_with("[trading]\norder_quote_amount = -1", tmp_path=tmp_path / "neg")
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)


def test_amount_must_be_numeric(tmp_path):
    root = _root_with("[trading]\norder_quote_amount = 'many'", tmp_path=tmp_path / "bad")
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)


def test_removed_aggregate_budget_key_is_rejected(tmp_path):
    root = _root_with(
        "[trading]\norder_quote_amount = 60\naggregate_budget_quote = 50",
        tmp_path=tmp_path,
    )
    with pytest.raises(ConfigError, match="aggregate_budget_quote"):
        load_config(root=root, environ={}, require_credentials=False)


def test_amount_from_environment(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(
        root=root,
        environ={"TRADING_ORDER_QUOTE_AMOUNT": "12.5"},
        require_credentials=False,
    )
    assert cfg.trading.order_quote_amount == 12.5
    assert cfg.trading.order_quote_amount_source == "env"


def test_legacy_fixed_amount_key_is_rejected(tmp_path):
    root = _root_with("[trading]\nmax_quote_per_order = 42.5", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="max_quote_per_order"):
        load_config(root=root, environ={}, require_credentials=False)


def test_bad_amount_fails_closed(tmp_path):
    root = _root_with("[trading]\norder_quote_amount = -5", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)

    root = _root_with("[trading]\norder_quote_amount = 'many'", tmp_path=tmp_path / "bad")
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)


def test_non_finite_amount_fails_closed(tmp_path):
    for i, value in enumerate(("nan", "inf", "-inf")):
        root = _root_with(f"[trading]\norder_quote_amount = {value}", tmp_path=tmp_path / f"nf{i}")
        with pytest.raises(ConfigError, match="finite"):
            load_config(root=root, environ={}, require_credentials=False)


def test_removed_arbitrary_restriction_keys_are_rejected(tmp_path):
    # require_stop_loss / require_tp1 etc. are hard mandatory invariants.
    root = _root_with(
        "[trading]\norder_quote_amount = 10\nrequire_stop_loss = true",
        tmp_path=tmp_path,
    )
    with pytest.raises(ConfigError, match="not configurable"):
        load_config(root=root, environ={}, require_credentials=False)
    for obsolete in (
        "daily_max_orders",
        "max_open_orders",
        "max_signal_age_s",
        "max_price_deviation_pct",
    ):
        root2 = _root_with(f"[trading]\norder_quote_amount = 10\n{obsolete} = 2", tmp_path=tmp_path / obsolete)
        with pytest.raises(ConfigError, match=obsolete):
            load_config(root=root2, environ={}, require_credentials=False)


def test_legacy_allowlist_configuration_is_rejected(tmp_path):
    for key in ("allowed_symbols", "allowlist_mode"):
        root = _root_with(f"[trading]\n{key} = 'BTCUSDT'", tmp_path=tmp_path / key)
        with pytest.raises(ConfigError, match=key):
            load_config(root=root, environ={}, require_credentials=False)
    cfg = load_config(root=_root_with("", tmp_path=tmp_path / "clean"), environ={}, require_credentials=False)
    assert not hasattr(cfg.trading, "allowed_symbols")
    assert not hasattr(cfg.trading, "allowlist_mode")


def test_entry_trigger_offset_defaults_to_three(tmp_path):
    cfg = load_config(root=_root_with("", tmp_path=tmp_path), environ={}, require_credentials=False)
    assert cfg.trading.entry_trigger_offset_ticks == 3
    assert cfg.trading.effective_entry_trigger_offset_ticks == 3


def test_entry_trigger_offset_explicit_override(tmp_path):
    root = _root_with("[trading]\nentry_trigger_offset_ticks = 5", tmp_path=tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.entry_trigger_offset_ticks == 5
    assert cfg.trading.effective_entry_trigger_offset_ticks == 5


@pytest.mark.parametrize("value", ["0", "-1", "'3'", "true", "3.5"])
def test_entry_trigger_offset_invalid_values_are_rejected(tmp_path, value):
    root = _root_with(f"[trading]\nentry_trigger_offset_ticks = {value}", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="entry_trigger_offset_ticks"):
        load_config(root=root, environ={}, require_credentials=False)


def test_order_type_policy_market_is_rejected(tmp_path):
    root = _root_with('[trading]\norder_type_policy = "market"', tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="order_type_policy"):
        load_config(root=root, environ={}, require_credentials=False)


def test_environment_order_type_policy_cannot_enable_market(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="order_type_policy"):
        load_config(
            root=root,
            environ={"TRADING_ORDER_TYPE_POLICY": "market"},
            require_credentials=False,
        )
