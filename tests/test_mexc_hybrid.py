"""Hybrid REST/CLI split: writes are gated, reads stay on REST (offline)."""

from __future__ import annotations

from typing import Any

from tg_signal_ingest.mexc.hybrid import HybridMexcClient


class _Gate:
    def __init__(self) -> None:
        self.weights: list[int] = []

    def acquire(self, weight: int = 1) -> None:
        self.weights.append(weight)


class _Rest:
    def __init__(self) -> None:
        self.order_gate = _Gate()
        self.query_calls = 0
        self.symbols = object()

    def query_order(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls += 1
        return {"source": "rest", **kwargs}


class _Cli:
    def __init__(self) -> None:
        self.place_calls = 0
        self.cancel_calls = 0
        self.query_calls = 0

    def place_order(self, **kwargs: Any) -> dict[str, Any]:
        self.place_calls += 1
        return {"source": "cli", **kwargs}

    def place_oco(self, **kwargs: Any) -> dict[str, Any]:
        self.place_calls += 1
        return {"source": "cli", **kwargs}

    def cancel_order(self, **kwargs: Any) -> dict[str, Any]:
        self.cancel_calls += 1
        return {"source": "cli", **kwargs}

    def query_order(self, **kwargs: Any) -> dict[str, Any]:
        self.query_calls += 1
        return {"source": "cli", **kwargs}


def test_cli_writes_acquire_the_shared_order_bucket():
    rest, cli = _Rest(), _Cli()
    hybrid = HybridMexcClient(rest, cli)  # type: ignore[arg-type]

    hybrid.place_order(symbol="BTCUSDT", side="BUY", order_type="LIMIT")
    hybrid.cancel_order(symbol="BTCUSDT", order_id="1")

    assert cli.place_calls == 1
    assert cli.cancel_calls == 1
    assert rest.order_gate.weights == [1, 1]


def test_query_order_uses_rest_not_the_unmetered_cli():
    rest, cli = _Rest(), _Cli()
    hybrid = HybridMexcClient(rest, cli)  # type: ignore[arg-type]

    result = hybrid.query_order(symbol="BTCUSDT", order_id="1")

    assert result["source"] == "rest"
    assert rest.query_calls == 1
    assert cli.query_calls == 0
