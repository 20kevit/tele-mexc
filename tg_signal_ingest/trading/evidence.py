"""Durable, redacted exchange evidence.

Every order/account call is written to ``exchange_operations`` *before* the
request is sent and finalized afterwards.  The recorder deliberately does not
guess: an uncertain write stays ``uncertain`` until a read-only reconciliation
proves what happened on the exchange.

Market-data reads (ticker/depth/klines) are intentionally not recorded per
call: they are high-frequency, carry no order state, and are already covered by
the existing market-data diagnostics.  Order submission, cancellation and
status queries are always recorded.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from .audit import classify_exchange_error
from .store import TradingStore

# Mapping from operation type to the correlating identifiers a caller may pass.
_READ_OPERATIONS = {"query_order", "query_trade_order", "account", "open_orders"}
_WRITE_OPERATIONS = {"place_order", "place_test_order", "cancel_order", "place_oco"}


def _exchange_order_id(response: Any) -> str | None:
    if not isinstance(response, dict):
        return None
    for key in ("orderId", "order_id", "exchange_order_id", "id"):
        value = response.get(key)
        if value not in (None, ""):
            return str(value)
    return None


class ExchangeOperation:
    """Handle returned by :meth:`ExchangeEvidence.operation`."""

    def __init__(self, evidence: ExchangeEvidence, operation_id: str):
        self.evidence = evidence
        self.operation_id = operation_id
        self.finished = False
        self.response: Any = None
        self.outcome: str | None = None

    def succeed(self, response: Any) -> None:
        if self.finished:
            return
        self.finished = True
        self.response = response
        self.outcome = "success"
        status = None
        if isinstance(response, dict):
            status = response.get("status") or response.get("state") or response.get("result")
        self.evidence.store.finish_exchange_operation(
            self.operation_id,
            success=True,
            response_payload=response if isinstance(response, dict) else {"result": response},
            response_classification="success",
            response_status=str(status) if status is not None else None,
            exchange_order_id=_exchange_order_id(response),
        )

    def fail(
        self,
        exc: BaseException,
        *,
        classification: str | None = None,
        reason_code: str | None = None,
        uncertainty: str | None = None,
        rate_limit_wait_ms: int | None = None,
    ) -> None:
        if self.finished:
            return
        self.finished = True
        self.outcome = classification or "error"
        kind = self.evidence.operation_class(self.operation_id)
        if classification is None:
            classification, reason_code, uncertainty = classify_exchange_error(exc)
        if uncertainty is None:
            uncertainty = "uncertain" if kind == "write" else "read_only_uncertain"
        self.evidence.store.finish_exchange_operation(
            self.operation_id,
            success=False,
            response_classification=classification,
            exchange_code=reason_code,
            error_class=exc.__class__.__name__,
            error_message=f"{exc.__class__.__name__}: {exc}",
            uncertainty=uncertainty,
            rate_limit_wait_ms=rate_limit_wait_ms,
        )

    def uncertain(self, reason: str, *, uncertainty: str = "uncertain") -> None:
        if self.finished:
            return
        self.finished = True
        self.outcome = "uncertain"
        self.evidence.store.finish_exchange_operation(
            self.operation_id,
            success=False,
            response_classification="uncertain",
            error_class="UncertainOutcome",
            error_message=reason,
            uncertainty=uncertainty,
        )


class ExchangeEvidence:
    def __init__(self, store: TradingStore, *, clock_health: Callable[[], str | None] | None = None):
        self.store = store
        self._clock_health = clock_health
        self._classes: dict[str, str] = {}

    def operation_class(self, operation_id: str) -> str:
        return self._classes.get(operation_id, "write")

    def _clock(self) -> str | None:
        if self._clock_health is None:
            return None
        try:
            return self._clock_health()
        except Exception:  # noqa: BLE001 - clock health is advisory
            return None

    @contextmanager
    def operation(
        self,
        *,
        operation_type: str,
        purpose: str | None = None,
        transport: str = "rest",
        endpoint: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        intent_id: str | None = None,
        trade_id: str | None = None,
        trade_order_id: str | None = None,
        signal_id: str | None = None,
        symbol: str | None = None,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
        request_payload: dict[str, Any] | None = None,
        operation_class: str | None = None,
        evidence_limitations: str | None = None,
    ) -> Iterator[ExchangeOperation]:
        kind = operation_class or (
            "write" if operation_type in _WRITE_OPERATIONS else "read"
        )
        operation_id = self.store.begin_exchange_operation(
            operation_type=operation_type,
            operation_class=kind,
            purpose=purpose,
            transport=transport,
            endpoint=endpoint,
            correlation_id=correlation_id,
            causation_id=causation_id,
            intent_id=intent_id,
            trade_id=trade_id,
            trade_order_id=trade_order_id,
            signal_id=signal_id,
            symbol=symbol,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            request_payload=request_payload,
            write_attempted=kind == "write",
            clock_health=self._clock(),
            evidence_limitations=evidence_limitations,
        )
        self._classes[operation_id] = kind
        handle = ExchangeOperation(self, operation_id)
        try:
            yield handle
        except BaseException as exc:
            handle.fail(exc)
            raise
        finally:
            if not handle.finished:
                # The wrapped call returned without an explicit outcome.  A
                # write is still uncertain if the caller forgot to confirm it.
                if kind == "write":
                    handle.uncertain("call returned without a recorded outcome")
                else:
                    handle.succeed(None)
            self._classes.pop(operation_id, None)

    def record_read(
        self,
        *,
        operation_type: str,
        call: Callable[[], Any],
        **context: Any,
    ) -> Any:
        """Run a read-only exchange call under an evidence record."""

        with self.operation(operation_type=operation_type, operation_class="read", **context) as op:
            result = call()
            op.succeed(result)
            return result
