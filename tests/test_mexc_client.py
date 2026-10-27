"""MEXC Spot request signing, error mapping and rate-limit behaviour (offline)."""

from __future__ import annotations

import hashlib
import hmac
import json
from decimal import Decimal

import pytest

from tg_signal_ingest.mexc import (
    MexcOrderNotFound,
    MexcRateLimitError,
    MexcSpotClient,
    MexcWafError,
    WeightedRateGate,
)

API_KEY = "key-from-test"
API_SECRET = "secret-from-test"
CLOCK_MS = 1_700_000_000_000


class Transport:
    def __init__(self, *, status: int = 200, payload: dict | list | None = None, headers: dict | None = None):
        self.status = status
        self.payload = payload if payload is not None else {}
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.calls: list[dict] = []

    def __call__(self, url, method, headers, body, timeout):
        self.calls.append({"url": url, "method": method, "headers": headers, "body": body, "timeout": timeout})
        return self.status, self.headers, json.dumps(self.payload).encode()


def _client(transport: Transport) -> MexcSpotClient:
    return MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=transport,
        clock=lambda: CLOCK_MS,
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
    )


def _sig(text: str) -> str:
    return hmac.new(API_SECRET.encode(), text.encode(), hashlib.sha256).hexdigest()


def test_signed_get_signature_matches_transmitted_query():
    transport = Transport(payload={"symbol": "BTCUSDT", "orderId": "123", "status": "NEW"})
    client = _client(transport)

    client.query_order(symbol="BTCUSDT", order_id="123")

    call = transport.calls[0]
    unsigned = (
        f"orderId=123&recvWindow=5000&symbol=BTCUSDT&timestamp={CLOCK_MS}"
    )
    expected = f"{unsigned}&signature={_sig(unsigned)}"
    assert call["method"] == "GET"
    assert call["url"] == f"https://api.mexc.com/api/v3/order?{expected}"
    assert call["headers"]["X-MEXC-APIKEY"] == API_KEY
    assert call["body"] is None


def test_signed_post_body_uses_raw_parameter_string():
    transport = Transport(payload={"orderId": "abc", "status": "NEW"})
    client = _client(transport)

    client.place_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="LIMIT",
        quantity=Decimal("0.5"),
        price=Decimal("100.10"),
        client_order_id="cid-1",
    )

    call = transport.calls[0]
    unsigned = (
        f"newClientOrderId=cid-1&price=100.10&quantity=0.5&recvWindow=5000"
        f"&side=BUY&symbol=BTCUSDT&timestamp={CLOCK_MS}&type=LIMIT"
    )
    expected = f"{unsigned}&signature={_sig(unsigned)}".encode()
    assert call["method"] == "POST"
    assert call["body"] == expected
    assert call["headers"]["Content-Type"] == "application/json"
    assert b"{" not in call["body"]


def test_429_retry_after_is_exposed_never_retried():
    transport = Transport(
        status=429,
        payload={"code": 700001, "msg": "too many requests"},
        headers={"Retry-After": "7"},
    )
    client = _client(transport)

    with pytest.raises(MexcRateLimitError) as excinfo:
        client.ping()

    assert excinfo.value.retry_after_s == 7.0
    assert len(transport.calls) == 1


def test_418_waf_retry_after_and_403_are_waf_errors():
    transport = Transport(status=418, payload={"msg": "banned"}, headers={"Retry-After": "120"})
    with pytest.raises(MexcWafError) as excinfo:
        _client(transport).ping()
    assert excinfo.value.retry_after_s == 120.0

    transport = Transport(status=403, payload={"msg": "waf"})
    with pytest.raises(MexcWafError):
        _client(transport).ping()


def test_unknown_order_maps_to_typed_not_found():
    transport = Transport(status=400, payload={"code": -2011, "msg": "Unknown order sent."})
    with pytest.raises(MexcOrderNotFound):
        _client(transport).query_order(symbol="BTCUSDT", order_id="missing")


def test_weighted_gate_waits_for_window_without_busy_loop():
    now = [0.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    gate = WeightedRateGate(2, 10.0, clock=lambda: now[0], sleep=sleep)
    gate.acquire(2)
    assert sleeps == []
    gate.acquire(1)
    assert sleeps and sleeps[0] == pytest.approx(10.0, abs=0.05)


def test_non_json_429_still_preserves_retry_after():
    class RawTransport(Transport):
        def __call__(self, url, method, headers, body, timeout):
            self.calls.append({"url": url, "method": method})
            return 429, {"Retry-After": "3", "Content-Type": "text/plain"}, b"too many"

    transport = RawTransport()
    with pytest.raises(MexcRateLimitError) as excinfo:
        _client(transport).ping()
    assert excinfo.value.retry_after_s == 3.0
    assert len(transport.calls) == 1


def test_retry_after_blocks_the_next_request():
    class SequenceTransport:
        def __init__(self):
            self.calls = 0

        def __call__(self, url, method, headers, body, timeout):
            self.calls += 1
            if self.calls == 1:
                return 429, {"Retry-After": "7"}, b'{"code":429,"msg":"slow down"}'
            return 200, {"Content-Type": "application/json"}, b"{}"

    transport = SequenceTransport()
    now = [100.0]
    sleeps: list[float] = []

    def fake_sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=transport,
        clock=lambda: CLOCK_MS,
        monotonic=lambda: now[0],
        sleep=fake_sleep,
        order_gate=WeightedRateGate(100, 1.0, sleep=lambda _s: None),
        general_gate=WeightedRateGate(100, 1.0, sleep=lambda _s: None),
    )

    with pytest.raises(MexcRateLimitError):
        client.ping()
    assert sleeps == []  # the failing call itself never retries

    client.ping()
    assert sleeps and sleeps[0] >= 6.9
    assert transport.calls == 2


def test_api_key_info_sends_the_required_access_key_parameter():
    transport = Transport(payload={"status": "VALID"})
    _client(transport).api_key_info()
    url = transport.calls[0]["url"]
    assert "accessKey=key-from-test" in url
    assert "signature=" in url
