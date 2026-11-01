"""Regression tests for execution-path safety decisions (N-01, F-13).

These tests use only fakes and local scripts; no exchange credentials or network
access are involved.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from tg_signal_ingest.mexc.cli import MexcCliAdapter
from tg_signal_ingest.mexc.errors import MexcError, MexcNetworkError, MexcOrderRejected


def _envelope(message: str, *, etype: str = "upstream_error") -> str:
    return json.dumps({"error": {"type": etype, "subtype": "failed", "message": message}})


def test_cli_price_relationship_rejection_is_definitive() -> None:
    """F-13: deterministic validation errors must not become UNKNOWN."""

    def runner(argv, timeout):  # noqa: ANN001 - test double
        return __import__("subprocess").CompletedProcess(
            list(argv), 1, _envelope("limit price needs to be greater than the latest price"), ""
        )

    adapter = MexcCliAdapter(binary="/bin/true", runner=runner)
    with pytest.raises(MexcOrderRejected):
        adapter.place_oco(
            symbol="PENDLEUSDT",
            side="SELL",
            quantity="0.76",
            price="1.99",
            trigger_price="1.91",
            trigger_limit_price="1.90",
            confirm=True,
        )


def test_cli_timeout_error_stays_uncertain() -> None:
    """A transport timeout remains ambiguous, so the caller must reconcile."""

    def runner(argv, timeout):  # noqa: ANN001 - test double
        return __import__("subprocess").CompletedProcess(
            list(argv), 1, _envelope("network timeout while contacting the exchange"), ""
        )

    adapter = MexcCliAdapter(binary="/bin/true", runner=runner)
    with pytest.raises(MexcNetworkError):
        adapter.place_oco(
            symbol="PENDLEUSDT",
            side="SELL",
            quantity="0.76",
            price="1.99",
            trigger_price="1.91",
            trigger_limit_price="1.90",
            confirm=True,
        )


def test_cli_parser_error_is_not_uncertain() -> None:
    def runner(argv, timeout):  # noqa: ANN001 - test double
        return __import__("subprocess").CompletedProcess(list(argv), 1, _envelope("invalid price precision"), "")

    adapter = MexcCliAdapter(binary="/bin/true", runner=runner)
    with pytest.raises(MexcError) as excinfo:
        adapter.place_order(
            symbol="LINKUSDT", side="BUY", order_type="LIMIT", quantity="0.26", price="11.2", confirm=True
        )
    assert not isinstance(excinfo.value, MexcNetworkError)


def test_default_runner_timeout_kills_process_tree_and_returns(tmp_path: Path) -> None:
    """N-01: the configured timeout must bound the whole call.

    The child script spawns a grandchild that inherits the stdout pipe; the old
    ``subprocess.run`` implementation could block on that pipe far beyond the
    timeout.
    """

    script = tmp_path / "hang.py"
    script.write_text(
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        "print('started', flush=True)\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    adapter = MexcCliAdapter(binary=sys.executable, timeout_s=0.5)
    started = time.monotonic()
    with pytest.raises(MexcNetworkError):
        adapter._default_runner([sys.executable, str(script)], 0.5)
    assert time.monotonic() - started < 10.0
