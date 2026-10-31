"""MEXC Spot request signing, error mapping and rate-limit behaviour (offline)."""

from __future__ import annotations

import hashlib
import hmac
import json
from decimal import Decimal
from urllib.parse import parse_qs, urlparse

import pytest

from tg_signal_ingest.mexc import (
    MexcOrderNotFound,
    MexcRateLimitError,
    MexcSpotClient,
    MexcTimestampError,
    MexcWafError,
    SharedWeightedRateGate,
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


def test_timestamp_error_is_classified_and_not_retried_by_the_client():
    transport = Transport(
        status=400,
        payload={"code": 700003, "msg": "Timestamp for this request is outside of the recvWindow"},
    )

    with pytest.raises(MexcTimestampError) as excinfo:
        _client(transport).account()

    assert excinfo.value.code == 700003
    assert len(transport.calls) == 1  # retry policy belongs to the caller


def test_signed_timestamp_is_stamped_after_the_rate_gate_wait():
    now = [1_700_000_000.0]

    class DelayingGate(WeightedRateGate):
        def __init__(self):
            super().__init__(1000, 10.0, sleep=lambda _s: None)

        def acquire(self, weight: int = 1) -> None:
            now[0] += 9.0  # simulate a nearly full 10-second shared window

    transport = Transport(payload={})
    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=transport,
        clock=lambda: int(now[0] * 1000),
        general_gate=DelayingGate(),
        uid_gate=WeightedRateGate(1000, 10.0, sleep=lambda _s: None),
    )

    client.account()

    query = parse_qs(urlparse(transport.calls[0]["url"]).query)
    stamped = int(query["timestamp"][0])
    assert stamped == int(now[0] * 1000)
    assert stamped >= CLOCK_MS + 9_000
    # The transmitted signature still covers the fresh timestamp.
    unsigned = "&".join(
        f"{key}={value[0]}"
        for key, value in sorted(query.items())
        if key != "signature"
    )
    assert query["signature"] == [_sig(unsigned)]


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


class RecordingGate(WeightedRateGate):
    def __init__(self):
        super().__init__(1000, 10.0, sleep=lambda _s: None)
        self.weights: list[int] = []

    def acquire(self, weight: int = 1) -> None:
        self.weights.append(weight)
        super().acquire(weight)


def test_ticker_price_charges_documented_weight_10():
    general = RecordingGate()
    client = MexcSpotClient(
        API_KEY, API_SECRET, transport=Transport(payload={"symbol": "BTCUSDT", "price": "1"}),
        clock=lambda: CLOCK_MS, general_gate=general,
    )
    client.ticker_price(symbol="BTCUSDT")
    assert general.weights == [10]


def test_exchange_info_charges_documented_weight_25():
    general = RecordingGate()
    client = MexcSpotClient(
        API_KEY, API_SECRET, transport=Transport(payload={"symbols": []}),
        clock=lambda: CLOCK_MS, general_gate=general,
    )
    client.exchange_info()
    assert general.weights == [25]


def test_signed_reads_consume_ip_and_uid_budgets():
    general = RecordingGate()
    uid = RecordingGate()
    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=Transport(payload={"balances": []}),
        clock=lambda: CLOCK_MS,
        general_gate=general,
        uid_gate=uid,
    )
    client.account()
    assert general.weights == [10]
    assert uid.weights == [1]


def test_order_test_uses_ip_weight_1_not_the_live_order_bucket():
    general = RecordingGate()
    uid = RecordingGate()
    order = RecordingGate()
    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=Transport(payload={}),
        clock=lambda: CLOCK_MS,
        general_gate=general,
        uid_gate=uid,
        order_gate=order,
    )
    client.place_test_order(
        symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=Decimal("0.1"), price=Decimal("100"),
    )
    assert general.weights == [1]
    assert uid.weights == [1]
    assert order.weights == []


def test_order_writes_use_only_the_official_12_rps_uid_bucket():
    general = RecordingGate()
    uid = RecordingGate()
    order = RecordingGate()
    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=Transport(payload={"orderId": "1", "status": "NEW"}),
        clock=lambda: CLOCK_MS,
        general_gate=general,
        uid_gate=uid,
        order_gate=order,
    )
    client.place_order(
        symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=Decimal("0.1"), price=Decimal("100"),
    )
    assert general.weights == []
    assert uid.weights == []
    assert order.weights == [1]


def test_unknown_endpoint_uses_conservative_default_weight():
    general = RecordingGate()
    client = MexcSpotClient(
        API_KEY, API_SECRET, transport=Transport(payload={}),
        clock=lambda: CLOCK_MS, general_gate=general,
    )
    client.request("GET", "/api/v3/future-unlisted-endpoint")
    assert general.weights == [25]


def test_shared_gate_enforces_one_budget_across_clients(tmp_path):
    now = [100.0]
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    path = tmp_path / "ip.json"
    first = SharedWeightedRateGate(
        2, 10.0, state_path=path, clock=lambda: now[0], sleep=sleep
    )
    second = SharedWeightedRateGate(
        2, 10.0, state_path=path, clock=lambda: now[0], sleep=sleep
    )

    first.acquire(2)
    second.acquire(1)
    assert sleeps and sleeps[0] >= 9.9


def test_shared_gate_prunes_expired_events(tmp_path):
    now = [100.0]
    gate = SharedWeightedRateGate(
        1, 10.0, state_path=tmp_path / "ip.json",
        clock=lambda: now[0], sleep=lambda s: now.__setitem__(0, now[0] + s),
    )
    gate.acquire(1)
    now[0] += 10.1
    gate.acquire(1)
    assert gate.state_path.exists()
