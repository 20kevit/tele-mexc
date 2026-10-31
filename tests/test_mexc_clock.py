"""RTT-aware MEXC server clock and timestamp retry policy (offline)."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from tg_signal_ingest.mexc import (
    MexcClockUnavailable,
    MexcSpotClient,
    MexcTimestampError,
    ServerClock,
    WeightedRateGate,
)

API_KEY = "key-from-test"
API_SECRET = "secret-from-test"
BASE_SERVER_MS = 1_700_000_000_000


class Harness:
    """Deterministic wall + monotonic clock whose fetch advances both."""

    def __init__(self, rtts_ms: list[float], server_times: list[int]):
        self.monotonic_s = 1000.0
        self.wall_ms = BASE_SERVER_MS
        self.rtts_ms = rtts_ms
        self.server_times = server_times
        self.calls = 0

    def monotonic(self) -> float:
        return self.monotonic_s

    def wall(self) -> int:
        return int(self.wall_ms)

    def fetch(self) -> int:
        index = min(self.calls, len(self.rtts_ms) - 1)
        rtt_s = self.rtts_ms[index] / 1000.0
        self.calls += 1
        self.monotonic_s += rtt_s
        self.wall_ms += rtt_s * 1000.0
        return self.server_times[index]


def _clock(harness: Harness, **kwargs) -> ServerClock:
    return ServerClock(
        harness.fetch,
        wall_clock=harness.wall,
        monotonic=harness.monotonic,
        **kwargs,
    )


def test_lowest_rtt_sample_wins_and_monotonic_elapsed_is_used():
    harness = Harness(
        rtts_ms=[400.0, 100.0, 800.0],
        server_times=[
            BASE_SERVER_MS + 200,
            BASE_SERVER_MS + 50,
            BASE_SERVER_MS + 400,
        ],
    )
    clock = _clock(harness, sample_count=3, min_samples_for_write=2, max_rtt_ms=1500)

    sample = clock.refresh(force=True)
    assert sample.rtt_ms == pytest.approx(100.0)
    assert clock.snapshot()["samples"] == 3
    assert clock.trusted is True

    anchored = clock.now_ms()
    # Move only the monotonic clock: a wall-clock step must not move it.
    harness.monotonic_s += 5.0
    harness.wall_ms -= 60_000
    assert clock.now_ms() - anchored == pytest.approx(5000, abs=2)


def test_write_fails_closed_when_samples_are_too_slow():
    harness = Harness(
        rtts_ms=[900.0, 950.0],
        server_times=[BASE_SERVER_MS, BASE_SERVER_MS],
    )
    clock = _clock(harness, sample_count=2, min_samples_for_write=2, max_rtt_ms=100)

    with pytest.raises(MexcClockUnavailable):
        clock.ensure_fresh_for_write()
    assert clock.trusted is False
    assert clock.snapshot()["last_error"] is None


def test_write_requires_multiple_low_rtt_samples():
    harness = Harness(
        rtts_ms=[10.0, 900.0, 950.0],
        server_times=[BASE_SERVER_MS] * 3,
    )
    clock = _clock(harness, sample_count=3, min_samples_for_write=2, max_rtt_ms=100)

    with pytest.raises(MexcClockUnavailable):
        clock.ensure_fresh_for_write()
    assert clock.trusted is False
    assert clock.snapshot()["low_rtt_samples"] == 1


def test_write_fails_closed_with_too_few_samples():
    harness = Harness(
        rtts_ms=[10.0],
        server_times=[BASE_SERVER_MS],
    )
    clock = _clock(harness, sample_count=1, min_samples_for_write=2, max_rtt_ms=1500)

    with pytest.raises(MexcClockUnavailable):
        clock.ensure_fresh_for_write()
    assert clock.trusted is False


def test_read_uses_bounded_extrapolation_then_fails_closed():
    harness = Harness(
        rtts_ms=[10.0],
        server_times=[BASE_SERVER_MS],
    )
    clock = _clock(
        harness,
        sample_count=1,
        min_samples_for_write=1,
        max_rtt_ms=1500,
        read_max_age_s=30,
        hard_stale_s=300,
    )
    clock.refresh(force=True)

    def broken_fetch() -> int:
        raise RuntimeError("network down")

    clock._fetch_server_time = broken_fetch  # type: ignore[assignment]
    harness.monotonic_s += 120.0  # past read_max_age, inside hard_stale
    assert clock.now_ms() > BASE_SERVER_MS

    harness.monotonic_s += 400.0  # past hard_stale
    with pytest.raises(MexcClockUnavailable):
        clock.now_ms()


def test_refresh_is_single_flight_and_recently_cached():
    harness = Harness(
        rtts_ms=[10.0, 20.0, 30.0],
        server_times=[BASE_SERVER_MS] * 3,
    )
    clock = _clock(harness, sample_count=3, min_refresh_interval_s=60)

    first = clock.refresh()
    second = clock.refresh()
    assert first is second
    assert harness.calls == 3


class FakeServerClock:
    def __init__(self, *, trusted: bool = True):
        self.trusted_for_write = trusted
        self.refresh_calls = 0

    def now_ms(self, *, for_write: bool = False) -> int:
        if for_write:
            self.ensure_fresh_for_write()
        return BASE_SERVER_MS

    def ensure_fresh_for_write(self) -> None:
        if not self.trusted_for_write:
            raise MexcClockUnavailable("clock is not trustworthy for a write")

    def refresh(self, *, force: bool = False) -> object:
        self.refresh_calls += 1
        if not self.trusted_for_write:
            raise MexcClockUnavailable("refresh failed")
        return object()


class Transport:
    def __init__(self, responses: list[tuple[int, dict]]):
        self.responses = responses
        self.calls: list[dict] = []

    def __call__(self, url, method, headers, body, timeout):
        self.calls.append({"url": url, "method": method, "body": body})
        status, payload = self.responses[min(len(self.calls) - 1, len(self.responses) - 1)]
        return status, {"Content-Type": "application/json"}, json.dumps(payload).encode()


def _client(transport: Transport, server_clock: FakeServerClock) -> MexcSpotClient:
    return MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=transport,
        server_clock=server_clock,  # type: ignore[arg-type]
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
    )


TIMESTAMP_ERROR = (400, {"code": 700003, "msg": "Timestamp outside recvWindow"})


def test_read_only_timestamp_error_refreshes_and_retries_once():
    transport = Transport([TIMESTAMP_ERROR, (200, {"balances": []})])
    clock = FakeServerClock()
    client = _client(transport, clock)

    assert client.account() == {"balances": []}
    assert len(transport.calls) == 2
    assert clock.refresh_calls == 1
    assert client.retries_timestamp_reads is True


def test_write_timestamp_error_is_never_retried():
    transport = Transport([TIMESTAMP_ERROR, (200, {"orderId": "1"})])
    clock = FakeServerClock()
    client = _client(transport, clock)

    with pytest.raises(MexcTimestampError):
        client.place_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("0.1"),
            price=Decimal("100"),
            client_order_id="cid-1",
        )
    assert len(transport.calls) == 1
    assert clock.refresh_calls == 0


def test_write_is_refused_before_transport_when_clock_is_untrusted():
    transport = Transport([(200, {"orderId": "1"})])
    clock = FakeServerClock(trusted=False)
    client = _client(transport, clock)

    with pytest.raises(MexcClockUnavailable):
        client.place_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="LIMIT",
            quantity=Decimal("0.1"),
            price=Decimal("100"),
            client_order_id="cid-1",
        )
    assert transport.calls == []


def test_legacy_injected_clock_disables_server_sampling():
    transport = Transport([(200, {"balances": []})])
    client = MexcSpotClient(
        API_KEY,
        API_SECRET,
        transport=transport,
        clock=lambda: BASE_SERVER_MS,
        order_gate=WeightedRateGate(100, 1.0),
        general_gate=WeightedRateGate(100, 1.0),
    )

    assert client.retries_timestamp_reads is False
    assert client.clock_snapshot() is None
    client.account()
    assert len(transport.calls) == 1


def test_median_of_three_rejects_offset_outlier():
    harness = Harness(
        rtts_ms=[0.0, 0.0, 0.0],
        server_times=[BASE_SERVER_MS + 100, BASE_SERVER_MS + 120, BASE_SERVER_MS + 5000],
    )
    clock = _clock(
        harness,
        sample_count=3,
        min_samples_for_write=2,
        max_rtt_ms=1500,
        max_offset_deviation_ms=1000,
    )

    clock.refresh(force=True)

    # 5000 ms is an outlier; the median of the remaining offsets is 110 ms.
    assert clock.snapshot()["offset_ms"] == 110
    assert clock.trusted is True


def test_rtt_filter_excludes_slow_samples_from_offset():
    harness = Harness(
        rtts_ms=[0.0, 2000.0, 2000.0],
        server_times=[BASE_SERVER_MS + 100, BASE_SERVER_MS + 9000, BASE_SERVER_MS + 9000],
    )
    clock = _clock(
        harness,
        sample_count=3,
        min_samples_for_write=1,
        max_rtt_ms=100,
        max_offset_deviation_ms=10_000,
    )

    clock.refresh(force=True)

    assert clock.snapshot()["offset_ms"] == 100
    assert clock.snapshot()["low_rtt_samples"] == 1


def test_offset_is_clamped_to_absolute_limit():
    harness = Harness(
        rtts_ms=[0.0],
        server_times=[BASE_SERVER_MS + 10_000_000],
    )
    clock = _clock(
        harness,
        sample_count=1,
        min_samples_for_write=1,
        max_rtt_ms=1500,
        max_offset_deviation_ms=20_000_000,
        max_offset_ms=300_000,
    )

    sample = clock.refresh(force=True)

    assert sample.offset_ms == 300_000
