"""Pipeline-construction and acknowledgement gates, verified OFFLINE.

Guarantees under test:
- The pipeline has no acknowledgement/token prerequisite.
- The MEXC CLI write backend is required for the automatic Spot pipeline.
- An engine still refuses exchange writes without a client.
- A client without credentials refuses every authenticated call locally.
- The official /order/test REST primitive remains validation-only and is not
  exposed as a product execution mode.
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
from tg_signal_ingest.trading.runtime import build_mexc_client, build_trading_engine


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


class MinimalClient:
    symbols = object()


def test_pipeline_builds_without_any_acknowledgement(repo, cfg):
    engine = build_trading_engine(cfg, repo, client=MinimalClient())
    assert engine is not None
    assert not hasattr(cfg.trading, "execution_acknowledgement")


def test_pipeline_requires_the_cli_write_backend(cfg):
    cfg.mexc.api_key = "k"
    cfg.mexc.api_secret = "s"
    cfg.mexc.cli_enabled = False
    with pytest.raises(ConfigError, match="CLI"):
        build_mexc_client(cfg, for_live_writes=True)


def test_engine_still_needs_a_client(repo, cfg):
    engine = build_trading_engine(cfg, repo, client=MinimalClient())
    engine.client = None
    with pytest.raises(AccidentalOrderRiskError):
        engine._execute_live({"intent_id": "x"})


def test_legacy_mode_config_refuses_to_load():
    from tg_signal_ingest.config import AppConfig, _load_mexc_and_trading

    app = AppConfig(api_id=1, api_hash="a" * 32, data_dir="/tmp/x", db_path="/tmp/x/db.sqlite3")
    with pytest.raises(ConfigError, match="mode"):
        _load_mexc_and_trading(app, {"trading": {"mode": "live"}}, {})


def test_client_without_credentials_refuses_all_signed_calls_locally():
    client = MexcSpotClient("", "", transport=RefusingTransport())
    with pytest.raises(MexcConfigError):
        client.place_order(symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=Decimal("1"), price=Decimal("1"))
    with pytest.raises(MexcConfigError):
        client.account()
    with pytest.raises(MexcConfigError):
        client.place_test_order(
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=Decimal("1"), price=Decimal("1")
        )


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
        symbol="MXUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("50"),
        price=Decimal("0.1"),
    )
    url, method, body = transport.calls[0]
    assert url == "https://api.mexc.com/api/v3/order/test"
    assert method == "POST"
    assert b"symbol=MXUSDT" in body and json.loads(b"{}") == {}


def test_rest_client_rejects_market_orders_before_transport():
    class RecordingTransport:
        def __init__(self):
            self.calls = []

        def __call__(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            raise AssertionError("transport must not be reached for MARKET")

    transport = RecordingTransport()
    client = MexcSpotClient(
        "k",
        "s",
        transport=transport,
        clock=lambda: 1700000000000,
        general_gate=WeightedRateGate(100, 1.0),
        uid_gate=WeightedRateGate(100, 1.0),
        order_gate=WeightedRateGate(100, 1.0),
    )
    with pytest.raises(MexcConfigError, match="only LIMIT"):
        client.place_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            quantity=Decimal("0.1"),
            price=None,
        )
    with pytest.raises(MexcConfigError, match="only LIMIT"):
        client.place_test_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            quantity=Decimal("0.1"),
            price=None,
        )
    assert transport.calls == []
