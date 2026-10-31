"""RTT-aware server clock for signed MEXC Spot requests.

Official rule (verified against the MEXC Spot v3 docs on 2026-10-31):

* a signed request is accepted when
  ``timestamp < serverTime + 1000`` and
  ``serverTime - timestamp <= recvWindow``;
* ``recvWindow`` defaults to 5000 ms and may not exceed 60000 ms;
* ``GET /api/v3/time`` returns the exchange ``serverTime`` in milliseconds.

The local wall clock can jump (NTP step, VM migration, suspend/resume), and a
timestamp taken before a long rate-gate wait can be stale even when the host
clock is correct.  This module therefore anchors the exchange time to a
monotonic clock and refreshes it with several low-RTT samples.

Design constraints:

* No request is ever signed with an unverified extrapolation for a write.
  ``ensure_fresh_for_write()`` fails closed when no recent, low-RTT sample is
  available.
* Refreshes are single-flight: concurrent callers wait for one sampling round
  instead of multiplying ``/time`` traffic.
* The returned time is ``anchor_server_ms + monotonic_elapsed``.  A wall-clock
  jump after the anchor cannot move it.

This module performs no network I/O itself; the client supplies the
``fetch_server_time`` callable.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from .errors import MexcClockUnavailable

log = logging.getLogger("ingest.mexc.clock")

DEFAULT_SAMPLE_COUNT = 3
DEFAULT_MIN_SAMPLES_FOR_WRITE = 2
DEFAULT_MAX_RTT_MS = 1500.0
DEFAULT_READ_MAX_AGE_S = 30.0
DEFAULT_WRITE_MAX_AGE_S = 10.0
DEFAULT_HARD_STALE_S = 300.0
DEFAULT_MIN_REFRESH_INTERVAL_S = 1.0


@dataclass(frozen=True)
class ClockSample:
    """One ``GET /api/v3/time`` observation."""

    server_time_ms: int
    wall_midpoint_ms: int
    monotonic_midpoint_s: float
    rtt_ms: float
    offset_ms: int


class ServerClock:
    """Monotonic-anchored, RTT-aware view of the exchange clock."""

    def __init__(
        self,
        fetch_server_time: Callable[[], int],
        *,
        wall_clock: Callable[[], int] | None = None,
        monotonic: Callable[[], float] | None = None,
        sample_count: int = DEFAULT_SAMPLE_COUNT,
        min_samples_for_write: int = DEFAULT_MIN_SAMPLES_FOR_WRITE,
        max_rtt_ms: float = DEFAULT_MAX_RTT_MS,
        read_max_age_s: float = DEFAULT_READ_MAX_AGE_S,
        write_max_age_s: float = DEFAULT_WRITE_MAX_AGE_S,
        hard_stale_s: float = DEFAULT_HARD_STALE_S,
        min_refresh_interval_s: float = DEFAULT_MIN_REFRESH_INTERVAL_S,
    ):
        if sample_count < 1:
            raise ValueError("sample_count must be >= 1")
        if min_samples_for_write < 1:
            raise ValueError("min_samples_for_write must be >= 1")
        if max_rtt_ms <= 0 or read_max_age_s <= 0 or write_max_age_s <= 0:
            raise ValueError("clock age/RTT limits must be positive")
        if hard_stale_s < read_max_age_s:
            raise ValueError("hard_stale_s must be >= read_max_age_s")
        if min_refresh_interval_s < 0:
            raise ValueError("min_refresh_interval_s must be >= 0")
        self._fetch_server_time = fetch_server_time
        self._wall_clock = wall_clock or (lambda: int(time.time() * 1000))
        self._monotonic = monotonic or time.monotonic
        self.sample_count = sample_count
        self.min_samples_for_write = min_samples_for_write
        self.max_rtt_ms = float(max_rtt_ms)
        self.read_max_age_s = float(read_max_age_s)
        self.write_max_age_s = float(write_max_age_s)
        self.hard_stale_s = float(hard_stale_s)
        self.min_refresh_interval_s = float(min_refresh_interval_s)

        self._lock = threading.Lock()
        self._last_sample: ClockSample | None = None
        self._last_success_count = 0
        self._last_low_rtt_count = 0
        self._last_error: str | None = None
        self._last_refresh_monotonic: float | None = None

    # ------------------------------------------------------------------ state

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def trusted(self) -> bool:
        """True when the last sample is fresh enough and low-RTT enough for writes."""

        sample = self._last_sample
        if sample is None:
            return False
        return (
            self._last_low_rtt_count >= self.min_samples_for_write
            and sample.rtt_ms <= self.max_rtt_ms
            and self._age_s(sample) <= self.write_max_age_s
        )

    def _age_s(self, sample: ClockSample | None = None) -> float:
        sample = sample or self._last_sample
        if sample is None:
            return float("inf")
        return max(0.0, self._monotonic() - sample.monotonic_midpoint_s)

    def _anchored_now_ms(self) -> int:
        sample = self._last_sample
        assert sample is not None
        elapsed_ms = (self._monotonic() - sample.monotonic_midpoint_s) * 1000.0
        return int(round(sample.server_time_ms + elapsed_ms))

    # --------------------------------------------------------------- sampling

    def refresh(self, *, force: bool = False) -> ClockSample:
        """Take a fresh sampling round (or return the last recent sample)."""

        with self._lock:
            if (
                not force
                and self._last_sample is not None
                and self._last_refresh_monotonic is not None
                and self._monotonic() - self._last_refresh_monotonic
                < self.min_refresh_interval_s
            ):
                return self._last_sample

            samples: list[ClockSample] = []
            errors: list[str] = []
            for _ in range(self.sample_count):
                try:
                    t0_wall = int(self._wall_clock())
                    t0_mono = self._monotonic()
                    server_ms = int(self._fetch_server_time())
                    t1_mono = self._monotonic()
                    rtt_ms = max(0.0, (t1_mono - t0_mono) * 1000.0)
                    if server_ms <= 0:
                        errors.append("serverTime was not positive")
                        continue
                    midpoint_mono = t0_mono + (t1_mono - t0_mono) / 2.0
                    # Derive the wall midpoint from the monotonic interval so a
                    # wall-clock step during the sample cannot corrupt it.
                    wall_midpoint = t0_wall + int(round(rtt_ms / 2.0))
                    offset = server_ms - wall_midpoint
                    samples.append(
                        ClockSample(
                            server_time_ms=server_ms,
                            wall_midpoint_ms=wall_midpoint,
                            monotonic_midpoint_s=midpoint_mono,
                            rtt_ms=rtt_ms,
                            offset_ms=offset,
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - one failed sample is not fatal
                    errors.append(f"{exc.__class__.__name__}: {exc}")

            if not samples:
                self._last_error = "; ".join(errors)[:300] or "no clock samples"
                raise MexcClockUnavailable(
                    f"exchange clock unavailable: {self._last_error}"
                )

            best = min(samples, key=lambda sample: sample.rtt_ms)
            self._last_sample = best
            self._last_success_count = len(samples)
            self._last_low_rtt_count = sum(
                1 for sample in samples if sample.rtt_ms <= self.max_rtt_ms
            )
            self._last_refresh_monotonic = self._monotonic()
            self._last_error = "; ".join(errors)[:300] if errors else None
            if best.rtt_ms > self.max_rtt_ms:
                log.warning(
                    "MEXC clock sample RTT %.0fms exceeds the trusted limit %.0fms; "
                    "writes will fail closed",
                    best.rtt_ms,
                    self.max_rtt_ms,
                )
            return best

    # ------------------------------------------------------------------- reads

    def now_ms(self, *, for_write: bool = False) -> int:
        """Return the current exchange time in milliseconds.

        ``for_write=True`` is the fail-closed path used for signed writes: it
        requires a recent, low-RTT sample and never falls back to an
        extrapolation.  Read-only requests may use a recent anchor and, when a
        refresh fails, a bounded extrapolation from the last anchor.
        """

        if for_write:
            self.ensure_fresh_for_write()
            return self._anchored_now_ms()

        sample = self._last_sample
        if sample is None:
            self.refresh(force=True)
            return self._anchored_now_ms()

        age = self._age_s(sample)
        if age <= self.read_max_age_s:
            return self._anchored_now_ms()

        try:
            self.refresh(force=True)
        except MexcClockUnavailable:
            if age <= self.hard_stale_s:
                log.warning(
                    "exchange clock refresh failed; using %.0fs-old monotonic anchor "
                    "for a read-only request",
                    age,
                )
                return self._anchored_now_ms()
            raise
        return self._anchored_now_ms()

    def ensure_fresh_for_write(self) -> None:
        """Raise ``MexcClockUnavailable`` unless a trusted sample is recent."""

        if self.trusted:
            return
        # A stale or untrusted anchor must be refreshed before a write.
        try:
            self.refresh(force=True)
        except MexcClockUnavailable:
            raise
        if not self.trusted:
            sample = self._last_sample
            detail = (
                f"samples={self._last_success_count}, "
                f"low_rtt_samples={self._last_low_rtt_count}, "
                f"rtt_ms={sample.rtt_ms:.0f}" if sample is not None else "no sample"
            )
            raise MexcClockUnavailable(
                "exchange clock is not trustworthy for a write "
                f"({detail}, required samples={self.min_samples_for_write}, "
                f"max_rtt_ms={self.max_rtt_ms:.0f}, "
                f"max_age_s={self.write_max_age_s:.0f})"
            )

    # -------------------------------------------------------------- diagnostics

    def skew_ms(self) -> int:
        """Return ``local_wall_clock - exchange_clock`` in milliseconds."""

        return int(self._wall_clock() - self.now_ms())

    def snapshot(self) -> dict[str, object]:
        sample = self._last_sample
        return {
            "anchored_now_ms": self._anchored_now_ms() if sample else None,
            "offset_ms": sample.offset_ms if sample else None,
            "rtt_ms": round(sample.rtt_ms, 1) if sample else None,
            "samples": self._last_success_count,
            "low_rtt_samples": self._last_low_rtt_count,
            "trusted": self.trusted,
            "age_s": round(self._age_s(sample), 1) if sample else None,
            "max_rtt_ms": self.max_rtt_ms,
            "read_max_age_s": self.read_max_age_s,
            "write_max_age_s": self.write_max_age_s,
            "last_error": self._last_error,
        }


__all__ = [
    "ClockSample",
    "ServerClock",
    "DEFAULT_MAX_RTT_MS",
    "DEFAULT_MIN_SAMPLES_FOR_WRITE",
    "DEFAULT_READ_MAX_AGE_S",
    "DEFAULT_SAMPLE_COUNT",
    "DEFAULT_WRITE_MAX_AGE_S",
]
