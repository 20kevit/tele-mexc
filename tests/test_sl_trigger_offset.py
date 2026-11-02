"""Stop-loss trigger is exactly the configured stop price.

All tests are deterministic and offline. No real orders are placed and no live
exchange is contacted.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tg_signal_ingest.config import ConfigError, TradingConfig, load_config
from tg_signal_ingest.trading.engine import TradingEngine
from tg_signal_ingest.trading.trade_manager import TradeManager


class _FakeMeta:
    def __init__(self, tick: str):
        self.price_step = Decimal(tick)


class _FakeSymbols:
    def __init__(self):
        self._symbols = {
            "APEUSDT": _FakeMeta("0.0001"),
            "BTCUSDT": _FakeMeta("0.01"),
            "LPTUSDT": _FakeMeta("0.001"),
        }

    def get(self, symbol):
        return self._symbols.get(str(symbol).upper())


class _FakeOcoClient:
    def __init__(self):
        self.symbols = _FakeSymbols()
        self.calls: list[dict] = []

    def place_oco(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "orderId": f"oco-{len(self.calls)}",
            "status": "NEW",
            "executedQty": "0",
            "cumulativeQuoteQty": "0",
        }


def _root_with(toml_text: str, tmp_path):
    (tmp_path / ".env").write_text("TELEGRAM_API_ID=1\nTELEGRAM_API_HASH=h\n")
    (tmp_path / "config").mkdir(exist_ok=True)
    (tmp_path / "config" / "config.toml").write_text(toml_text)
    return tmp_path


@pytest.mark.parametrize(
    "symbol, stop, entry, tp, quantity, tick",
    [
        ("APEUSDT", Decimal("0.1190"), Decimal("0.1219"), Decimal("0.1227"), Decimal("24.61"), "0.0001"),
        ("BTCUSDT", Decimal("95.00"), Decimal("100.00"), Decimal("110.00"), Decimal("0.05"), "0.01"),
        ("LPTUSDT", Decimal("1.403"), Decimal("1.423"), Decimal("1.429"), Decimal("1.05"), "0.001"),
    ],
)
def test_engine_submits_exact_stop_price_for_multiple_tick_sizes(
    cfg, repo, symbol, stop, entry, tp, quantity, tick
):
    """The real engine path must send trigger == stop for every tick size."""
    cfg.trading.stop_loss_trigger_offset_ticks = 7  # legacy nonzero value must be ignored
    client = _FakeOcoClient()
    engine = TradingEngine(cfg, repo, client=client, registry=client.symbols)

    trade = {
        "trade_id": f"trade-{symbol.lower()}",
        "symbol": symbol,
        "entry_price": entry,
    }
    result = engine.submit_oco_sell(
        trade,
        "sl-cid-1",
        tp,
        quantity,
        stop,
        kind="tp1",
    )

    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["trigger_price"] == stop
    assert call["trigger_limit_price"] == stop
    assert call["trigger_price"] - call["trigger_limit_price"] == Decimal("0")
    assert call["price"] == tp
    assert call["quantity"] == quantity
    assert call["symbol"] == symbol
    assert call["side"] == "SELL"
    assert result["trigger_price"] == str(stop)
    assert result["trigger_limit_price"] == str(stop)
    # The tick-size string is intentionally present only to exercise distinct
    # symbol metadata; the trigger must not depend on it.
    assert call["trigger_price"] == stop, tick


def test_stop_loss_offset_defaults_to_zero(tmp_path):
    root = _root_with("", tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.stop_loss_trigger_offset_ticks == 0


def test_explicit_zero_stop_loss_offset_is_accepted(tmp_path):
    root = _root_with("[trading]\nstop_loss_trigger_offset_ticks = 0", tmp_path)
    cfg = load_config(root=root, environ={}, require_credentials=False)
    assert cfg.trading.stop_loss_trigger_offset_ticks == 0


@pytest.mark.parametrize("value", ["1", "3", "-1", "'0'", "true", "0.0"])
def test_nonzero_or_invalid_stop_loss_offset_is_rejected(tmp_path, value):
    root = _root_with(f"[trading]\nstop_loss_trigger_offset_ticks = {value}", tmp_path)
    with pytest.raises(ConfigError, match="stop_loss_trigger_offset_ticks"):
        load_config(root=root, environ={}, require_credentials=False)


def test_legacy_in_memory_nonzero_value_cannot_reintroduce_offset(cfg, repo):
    """Even an old object or external assignment must not move the trigger."""
    cfg.trading.stop_loss_trigger_offset_ticks = 5
    client = _FakeOcoClient()
    engine = TradingEngine(cfg, repo, client=client, registry=client.symbols)
    stop = Decimal("0.1104")
    engine.submit_oco_sell(
        {"trade_id": "trade-legacy", "symbol": "APEUSDT", "entry_price": Decimal("0.1125")},
        "sl-cid-legacy",
        Decimal("0.1131"),
        Decimal("13.33"),
        stop,
    )
    assert client.calls[0]["trigger_price"] == stop
    assert client.calls[0]["trigger_limit_price"] == stop


def test_entry_trigger_offset_is_unchanged():
    assert TradingConfig().entry_trigger_offset_ticks == 3
    tm = object.__new__(TradeManager)
    assert tm._compute_trigger(Decimal("100"), 3, Decimal("0.01")) == Decimal("99.97")
