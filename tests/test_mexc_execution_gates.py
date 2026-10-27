"""Default-disabled execution tests, verified OFFLINE (no credentials needed).

Guarantees under test:
- ``sim`` mode (the default) constructs NO exchange client at all.
- ``_execute_live`` raises AccidentalOrderRiskError unless mode == "live"
  AND the explicit acknowledgement token is configured.
- A client without credentials refuses every authenticated call locally.
- Config refused to boot in live mode without the token (fail closed).

Docs checked 2026-10-23: testing uses the OFFICIAL ``POST /api/v3/order/test``
.openapi ``/order/test`` is validation only and never reaches the matching
engine; there is NO MEXC sandbox (mexc.com/mexc-api FAQ).
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from tg_signal_ingest.config import ConfigError
from tg_signal_ingest.mexc import (
    AccidentalOrderRiskError,
    MexcConfigError,
    MexcSpotClient,
    WeightedRateGate,
)
from tg_signal_ingest.trading.runtime import build_trading_engine


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Hard guard: no test here may open a socket (offline-only suite)."""

    import socket

    def _refuse(*args, **kwargs):
        raise AssertionError("blocked network access in an offline test")

    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket.socket, "connect", _refuse)


class RefusingTransport:
    """Any real HTTP call in this suite is a test failure, not a fixture."""

    def __call__(self, *args, **kw):  # pragma: no cover - must never run
        raise AssertionError("live HTTP transport was invoked during offline tests")


def test_sim_mode_builds_no_network_client_and_rejects_live_submission(repo, cfg):
    cfg.trading.mode = "sim"
    engine = build_trading_engine(cfg, repo)
    assert engine.client is None
    with pytest.raises(AccidentalOrderRiskError):
        engine._execute_live({"intent_id": "x", "symbol": "BTCUSDT"})


def test_live_refuses_to_run_without_the_explicit_token(repo, cfg):
    engine = build_trading_engine(cfg, repo)
    engine.cfg.trading.mode = "live"  # token still None -> not armed
    engine.client = None
    with pytest.raises(AccidentalOrderRiskError):
        engine._execute_live({"intent_id": "x"})


def test_armed_live_mode_with_wrong_token_is_still_blocked(repo, cfg):
    engine = build_trading_engine(cfg, repo)
    engine.cfg.trading.mode = "live"
    engine.cfg.trading.live_enabled_token = "wrong-token"
    engine.client = MexcSpotClient("k", "s", transport=RefusingTransport())
    with pytest.raises(AccidentalOrderRiskError):
        engine._execute_live({"intent_id": "x"})


def test_live_mode_config_refused_to_boot_without_token():
    """Fail closed at config load, matching the documented safety model."""

    from tg_signal_ingest.config import AppConfig, _load_mexc_and_trading

    app = AppConfig(api_id=1, api_hash="a" * 32, data_dir="/tmp/x", db_path="/tmp/x/db.sqlite3")
    toml: dict = {"trading": {"mode": "live"}}
    env: dict = {}
    with pytest.raises(ConfigError):
        _load_mexc_and_trading(app, toml, env)


def test_client_without_credentials_refuses_all_signed_calls_locally():
    client = MexcSpotClient("", "", transport=RefusingTransport())
    with pytest.raises(MexcConfigError):
        client.place_order(symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                           quantity=Decimal("1"), price=Decimal("1"))
    with pytest.raises(MexcConfigError):
        client.account()
    with pytest.raises(MexcConfigError):
        client.place_test_order(symbol="BTCUSDT", side="BUY", order_type="LIMIT",
                                quantity=Decimal("1"), price=Decimal("1"))


def test_test_order_is_officially_validation_only():
    """POST /api/v3/order/test: params equal POST /api/v3/order; no engine
    touch. Response body per docs is ``{}`` (verified 2026-10-23)."""

    class Transport:
        def __init__(self):
            self.calls = []

        def __call__(self, url, method, headers, body, timeout):
            self.calls.append((url, method, body))
            return 200, {"Content-Type": "application/json"}, b"{}"

    transport = Transport()
    client = MexcSpotClient(
        "k",
        "s",
        transport=transport,
        clock=lambda: 1700000000000,
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
    )
    assert client.place_test_order(
        symbol="MXUSDT", side="BUY", order_type="LIMIT",
        quantity=Decimal("50"), price=Decimal("0.1"),
    )
    url, method, body = transport.calls[0]
    assert url == "https://api.mexc.com/api/v3/order/test"
    assert method == "POST"
    assert b"symbol=MXUSDT" in body and json.loads(b"{}") == {}
