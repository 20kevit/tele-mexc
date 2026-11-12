"""Central thresholds for latency-telemetry log levels.

Every duration here is measured with a monotonic clock.  Routine timings are
emitted at DEBUG; a stage or lock is promoted to INFO only when it crosses one
of these thresholds, so a genuinely slow signal stays visible in production
logs without drowning each signal in INFO noise.
"""

from __future__ import annotations

import logging

# One signal-pipeline stage (parse, price, metadata, risk, intent, planning,
# order execution, notification enqueue).
SLOW_SIGNAL_STAGE_MS = 1000
# Waiting for a lock is a latency signal even when short; holding it is only
# interesting when it blocks a peer or a consumer for a noticeable time.
SLOW_LOCK_WAIT_MS = 50
SLOW_LOCK_HOLD_MS = 1000
# Single-consumer queue item handling and the age of the item when dequeued.
SLOW_QUEUE_ITEM_MS = 1000
SLOW_QUEUE_AGE_MS = 1000


def level_for(*durations: tuple[int, int]) -> int:
    """Return INFO when any ``(duration_ms, threshold_ms)`` pair is slow."""

    for duration_ms, threshold_ms in durations:
        if duration_ms >= threshold_ms:
            return logging.INFO
    return logging.DEBUG
