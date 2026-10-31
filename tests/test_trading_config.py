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


def test_default_trading_mode_is_sim_and_default_amount_is_3_usdt(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.mode == "sim"
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


def test_trading_mode_validation(tmp_path):
    root = _root_with("[trading]\nmode = 'yolo'", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="mode"):
        load_config(root=root, environ={}, require_credentials=False)


def test_live_mode_requires_token(tmp_path):
    root = _root_with("[trading]\nmode = 'live'", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="fail closed"):
        load_config(root=root, environ={}, require_credentials=False)


def test_live_mode_with_token_uses_default_amount_when_not_explicit(tmp_path):
    root = _root_with(
        "[trading]\nmode = 'live'\nlive_enabled_token = 'I_UNDERSTAND_REAL_MONEY_AND_RISKS'",
        tmp_path=tmp_path,
    )
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.mode == "live"
    assert cfg.trading.order_quote_amount == 3.0
    assert cfg.trading.order_quote_amount_source == "default (3 USDT)"

    root = _root_with(
        "[trading]\nmode = 'live'\nlive_enabled_token = 'I_UNDERSTAND_REAL_MONEY_AND_RISKS'\n"
        "order_quote_amount = 25",
        tmp_path=tmp_path / "armed",
    )
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.mode == "live"
    assert cfg.trading.order_quote_amount == 25.0
    assert cfg.trading.order_quote_amount_source == "config"


def test_testonly_uses_default_amount_when_not_explicit(tmp_path):
    root = _root_with("[trading]\nmode = 'testonly'", tmp_path=tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.mode == "testonly"
    assert cfg.trading.order_quote_amount == 3.0
    assert cfg.trading.order_quote_amount_source == "default (3 USDT)"


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


def test_amount_is_not_coupled_to_aggregate_budget(tmp_path):
    root = _root_with(
        "[trading]\norder_quote_amount = 60\naggregate_budget_quote = 50",
        tmp_path=tmp_path,
    )
    t = load_config(root=root, environ={}, require_credentials=False).trading
    # Per-order size is owner-controlled; the aggregate value is reference-only.
    assert t.order_quote_amount == 60.0
    assert t.aggregate_budget_quote == 50.0


def test_amount_from_environment(tmp_path):
    root = _root_with("", tmp_path=tmp_path)
    cfg = load_config(
        root=root,
        environ={"TRADING_ORDER_QUOTE_AMOUNT": "12.5"},
        require_credentials=False,
    )
    assert cfg.trading.order_quote_amount == 12.5
    assert cfg.trading.order_quote_amount_source == "env"


def test_explicit_legacy_key_is_accepted_as_fixed_amount(tmp_path):
    root = _root_with("[trading]\nmax_quote_per_order = 42.5", tmp_path=tmp_path)
    t = load_config(root=root, environ={}, require_credentials=False).trading
    assert t.order_quote_amount == 42.5
    assert t.order_quote_amount_source == "legacy max_quote_per_order"


def test_bad_amount_fails_closed(tmp_path):
    root = _root_with("[trading]\norder_quote_amount = -5", tmp_path=tmp_path)
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)

    root = _root_with("[trading]\norder_quote_amount = 'many'", tmp_path=tmp_path / "bad")
    with pytest.raises(ConfigError, match="order_quote_amount"):
        load_config(root=root, environ={}, require_credentials=False)


def test_non_finite_amount_fails_closed(tmp_path):
    for i, value in enumerate(("nan", "inf", "-inf")):
        root = _root_with(
            f"[trading]\norder_quote_amount = {value}", tmp_path=tmp_path / f"nf{i}"
        )
        with pytest.raises(ConfigError, match="finite"):
            load_config(root=root, environ={}, require_credentials=False)


def test_removed_arbitrary_restriction_keys_are_ignored(tmp_path):
    # require_stop_loss / require_tp1 etc. are no longer configurable — they are
    # hard mandatory invariants.  Attempting to set them via TOML must be rejected.
    root = _root_with(
        "[trading]\norder_quote_amount = 10\ndaily_max_orders = 2\nmax_open_orders = 2\n"
        "max_signal_age_s = 900\nmax_price_deviation_pct = 1\nrequire_stop_loss = true",
        tmp_path=tmp_path,
    )
    with pytest.raises(ConfigError, match="not configurable"):
        load_config(root=root, environ={}, require_credentials=False)
    # Without the forbidden keys, config loads successfully.
    root2 = _root_with(
        "[trading]\norder_quote_amount = 10\ndaily_max_orders = 2\nmax_open_orders = 2\n"
        "max_signal_age_s = 900\nmax_price_deviation_pct = 1",
        tmp_path=tmp_path,
    )
    t = load_config(root=root2, environ={}, require_credentials=False).trading
    assert t.order_quote_amount == 10.0
    assert t.daily_max_orders == 2
    assert t.max_open_orders == 2
    assert t.max_signal_age_s == 900.0
    assert t.max_price_deviation_pct == 1.0
    assert not hasattr(t, "allowed_symbols")


def test_legacy_allowlist_configuration_is_ignored(tmp_path):
    root = _root_with(
        "[trading]\nallowed_symbols = ['BTCUSDT']\nallowlist_mode = 'explicit_allowlist'",
        tmp_path=tmp_path,
    )
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert not hasattr(cfg.trading, "allowed_symbols")
    assert not hasattr(cfg.trading, "allowlist_mode")
