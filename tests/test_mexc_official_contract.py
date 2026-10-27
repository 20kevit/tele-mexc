"""Offline tests for the adapter against the OFFICIAL MEXC documentation.

Every documented example below is copied verbatim from the official pages
(checked 2026-10-23); the expected HMAC digests were published by MEXC, not
computed by this codebase, so a passing test proves wire-compatibility.
"""
# Docs checked 2026-10-23:
# - https://www.mexc.com/api-docs/spot-v3/introduction (signature examples)
# - https://www.mexc.com/api-docs/spot-v3/spot-account-trade/cancel-all-open-orders-on-a-symbol
# - https://www.mexc.com/api-docs/spot-v3/spot-account-trade/cancel-all-orders
# - https://www.mexc.com/api-docs/spot-v3/error-code

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from decimal import Decimal

import pytest

from tg_signal_ingest.mexc import (
    MexcAuthError,
    MexcConfigError,
    MexcSpotClient,
    MexcWafError,
    WeightedRateGate,
)

DOC_API_KEY = "mx0aBYs33eIilxBWC5"
DOC_API_SECRET = "45d0b3c26f2644f19bfb98b07741b2f5"
DOC_DOC_TIMESTAMP = "1644489390087"
# Body/query of the official POST /api/v3/order example.
DOC_TOTAL_PARAMS = (
    "symbol=BTCUSDT&side=BUY&type=LIMIT&quantity=1&price=11"
    "&recvWindow=5000&timestamp=1644489390087"
)
# Official digest published under Example 1/2 (query-only and body-only).
DOC_SIG_1 = "fd3e4e8543c5188531eb7279d68ae7d26a573d0fc5ab0d18eb692451654d837a"
# Mixed query-string + body example: totalParams concatenates both parts
# directly, with NO extra '&' between them.
DOC_QUERY_3 = "symbol=BTCUSDT&side=BUY&type=LIMIT"
# Body part of doc Example 3 (the tail of totalParams after the query part).
DOC_BODY_3 = "quantity=1&price=11&recvWindow=5000&timestamp=1644489390087"
DOC_SIG_3 = "d1a676610ceb39174c8039b3f548357994b2a34139a8addd33baadba65684592"


class Transport:
    def __init__(self, *, status: int = 200, payload=None, headers=None):
        self.status = status
        self.payload = payload if payload is not None else {}
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.calls: list[dict] = []

    def __call__(self, url, method, headers, body, timeout):
        self.calls.append({"url": url, "method": method, "headers": headers, "body": body})
        return self.status, self.headers, json.dumps(self.payload).encode()


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Hard guard: no test here may open a socket (offline-only suite)."""

    import socket

    def _refuse(*args, **kwargs):
        raise AssertionError("blocked network access in an offline test")

    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket.socket, "connect", _refuse)


def _doc_client(transport: Transport | None = None, **kw):
    return MexcSpotClient(
        DOC_API_KEY,
        DOC_API_SECRET,
        transport=transport or Transport(),
        clock=lambda: int(DOC_DOC_TIMESTAMP),
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
        **kw,
    )


# ----------------------------------------------------------- official HMAC
def test_signature_matches_official_example_1_and_2_query_only():
    """Doc Example 2: all parameters in the query string.

    https://www.mexc.com/api-docs/spot-v3/introduction#post-apiv3order-example
    """

    client = _doc_client(Transport())
    assert client._sign(DOC_TOTAL_PARAMS) == DOC_SIG_1


def test_signature_matches_official_example_3_mixed_query_and_body():
    """Doc Example 3: concatenated query + body, no separator '&'."""

    client = _doc_client(Transport())
    assert client._sign(DOC_QUERY_3 + DOC_BODY_3) == DOC_SIG_3


def test_canonicalization_transmits_the_signed_bytes_verbatim():
    """Signed canonicalisation must never reorder parameters after signing.

    openOrders officially accepts up to five comma-separated symbols; the
    comma is URL-encoded once with uppercase hex, and the digest of the
    transmitted query must equal the transmitted signature.
    """
    transport = Transport(payload=[{"orderId": "1", "status": "NEW"}])
    client = _doc_client(transport)

    client.open_orders(symbol="MXUSDT,BTCUSDT")

    url = transport.calls[0]["url"]
    assert "%2C" in url and ",MX" not in url
    unsigned = url.split("?")[1].split("&signature=")[0]
    expected = hmac.new(
        DOC_API_SECRET.encode(), unsigned.encode(), hashlib.sha256
    ).hexdigest()
    assert url.endswith(f"signature={expected}")


# ------------------------------------------------------------- recvWindow
def test_recv_window_above_documented_maximum_fails_locally():
    docs_max = 60000
    with pytest.raises(MexcConfigError):
        _doc_client(Transport(), recv_window_ms=docs_max + 1)
    assert _doc_client(Transport(), recv_window_ms=60000).recv_window_ms == 60000


# ----------------------------------------------------------- cancellation
def test_cancel_all_on_symbol_uses_delete_openorders_with_required_symbol():
    transport = Transport(payload=[{"orderId": "1", "status": "CANCELED"}])
    client = _doc_client(transport)

    client.cancel_all_orders(symbol="BTCUSDT")

    call = transport.calls[0]
    assert call["method"] == "DELETE"
    assert call["url"].startswith("https://api.mexc.com/api/v3/openOrders?")
    assert "symbol=BTCUSDT" in call["url"]


def test_cancel_across_all_symbols_uses_delete_order_all_without_symbol():
    transport = Transport(payload={"code": 200, "msg": "success"})
    client = _doc_client(transport)

    client.cancel_all_account_orders()

    call = transport.calls[0]
    assert call["method"] == "DELETE"
    assert call["url"].split("?")[0] == "https://api.mexc.com/api/v3/order/all"
    assert "symbol=" not in call["url"]


def test_order_bucket_shares_the_documented_12rps_cap():
    """Order placement + cancellation share one UID bucket: 12 requests/s.

    https://www.mexc.com/api-docs/spot-v3/spot-account-trade/new-order
    ("Rate limit: Shared with the cancel order endpoint, 12 requests/second")
    """
    client = MexcSpotClient(
        DOC_API_KEY,
        DOC_API_SECRET,
        clock=lambda: int(DOC_DOC_TIMESTAMP),
        transport=Transport(),
    )  # default gates: the documented production pacing
    assert client.order_gate.max_weight == 12
    assert client.order_gate.window_s == pytest.approx(1.0)


# -------------------------------------------------------------- redaction
def test_logs_never_contain_key_secret_or_signature(caplog):
    """A leaking log line could expose credentials; signing inputs must
    never be logged (client docstring promise)."""
    transport = Transport(status=400, payload={"code": 700002, "msg": "Signature invalid"})
    client = _doc_client(transport)

    with (
        caplog.at_level(logging.DEBUG, logger="ingest.mexc"),
        pytest.raises(MexcAuthError),
    ):
        client.place_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("1"),
            price=Decimal("11"),
        )

    text = "\n".join(str(r.getMessage()) for r in caplog.records)
    assert DOC_API_KEY not in text
    assert DOC_API_SECRET not in text
    assert DOC_SIG_1 not in text
    assert "signature=" not in text
    assert "symbol=BTCUSDT" not in text  # signed parameters never logged


def test_waf_403_stops_instead_of_hammering(caplog):
    transport = Transport(status=403, payload={"msg": "Access Denied"})
    with caplog.at_level(logging.ERROR, logger="ingest.mexc"), pytest.raises(MexcWafError):
        _doc_client(transport).ping()
    assert len(transport.calls) == 1  # single attempt: WAF must not be fought
