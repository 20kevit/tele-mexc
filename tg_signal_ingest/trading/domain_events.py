"""Typed, versioned domain events for the Phase 2 notification redesign.

The Phase 2 boundary is deliberately small:

* ``audit_events`` remains the single immutable event source.  New domain
  events are inserted with ``event_class='domain'`` and are protected from
  UPDATE/DELETE by database triggers.
* A domain event is a *local* fact about a business transition.  This module
  validates it, gives it a stable machine-readable vocabulary, and serializes
  its payload deterministically.
* This module never performs network I/O and never talks to Telegram.  It is
  safe to call from inside a short SQLite transaction.

The event taxonomy below is intentionally narrow.  It covers intent, trade,
trade-order, and fill transitions whose producers already have an atomic
SQLite transaction.  Other producer paths remain on their legacy behaviour and
are documented as deferred for Phase 3.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

DOMAIN_EVENT_CLASS = "domain"
DOMAIN_SCHEMA_VERSION = 1
MAX_PAYLOAD_BYTES = 32 * 1024
MAX_PAYLOAD_DEPTH = 6
MAX_STRING_LENGTH = 2000

ALLOWED_AUDIENCE = ("owner", "owner_b", "developer")
DEFAULT_AUDIENCE = ("owner", "owner_b", "developer")
ALLOWED_SEVERITIES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

# ``category`` maps to the legacy audit_events CHECK constraint.  It is not a
# second event type: the stable semantic key is ``event_type``.
_ALL_CATEGORIES = frozenset(
    {"telegram", "parse", "validate", "risk", "mexc_request", "lifecycle", "reconcile", "config", "emergency"}
)


@dataclass(frozen=True)
class EventSpec:
    """Central registry entry for one stable event type."""

    event_type: str
    category: str
    aggregate_type: str
    default_severity: str = "INFO"
    audience: tuple[str, ...] = DEFAULT_AUDIENCE
    required_payload: tuple[str, ...] = ()
    allowed_payload: frozenset[str] = frozenset()


def _spec(
    event_type: str,
    *,
    category: str,
    aggregate_type: str,
    severity: str = "INFO",
    required: tuple[str, ...] = (),
    allowed: tuple[str, ...] = (),
) -> EventSpec:
    if category not in _ALL_CATEGORIES:
        raise ValueError(f"invalid audit category {category!r}")
    return EventSpec(
        event_type=event_type,
        category=category,
        aggregate_type=aggregate_type,
        default_severity=severity,
        required_payload=required,
        allowed_payload=frozenset(allowed),
    )


# Frozen Phase 2 taxonomy.  New types require an explicit registry entry and a
# producer that can write the event in the business transition transaction.
EVENT_REGISTRY: dict[str, EventSpec] = {
    "signal.received": _spec(
        "signal.received",
        category="parse",
        aggregate_type="signal",
        severity="INFO",
        required=("state", "signal_kind"),
        allowed=(
            "state", "signal_kind", "side", "entry_price", "tp1_price", "tp2_price",
            "stop_loss_price", "order_type",
        ),
    ),
    "signal.rejected": _spec(
        "signal.rejected",
        category="lifecycle",
        aggregate_type="signal",
        severity="WARNING",
        required=("reason", "previous_state", "new_state"),
        allowed=("reason", "previous_state", "new_state", "signal_kind"),
    ),
    "intent.created": _spec(
        "intent.created",
        category="lifecycle",
        aggregate_type="intent",
        required=("state",),
        allowed=(
            "state",
            "signal_id",
            "symbol",
            "side",
            "order_type",
            "mode",
            "quote_amount",
            "base_quantity",
            "limit_price",
            "entry_price",
            "tp1_price",
            "tp2_price",
            "stop_loss_price",
            "client_order_id",
        ),
    ),
    "intent.state_changed": _spec(
        "intent.state_changed",
        category="lifecycle",
        aggregate_type="intent",
        required=("previous_state", "new_state"),
        allowed=(
            "previous_state",
            "new_state",
            "exchange_order_id",
            "exchange_status",
            "executed_qty",
            "cumulative_quote",
            "fill_sync_required",
            "reason",
        ),
    ),
    "intent.submission_accepted": _spec(
        "intent.submission_accepted",
        category="mexc_request",
        aggregate_type="intent",
        severity="INFO",
        required=("exchange_order_id",),
        # Acceptance facts only: order/client IDs and the acknowledged exchange
        # status. Fills are separate, later evidence and never belong here.
        allowed=(
            "exchange_order_id",
            "exchange_status",
            "client_order_id",
        ),
    ),
    "trade.state_changed": _spec(
        "trade.state_changed",
        category="lifecycle",
        aggregate_type="trade",
        required=("previous_state", "new_state"),
        allowed=("previous_state", "new_state", "sl_state", "reason"),
    ),
    "trade_order.state_changed": _spec(
        "trade_order.state_changed",
        category="lifecycle",
        aggregate_type="trade_order",
        required=("kind", "previous_state", "new_state"),
        allowed=(
            "kind",
            "previous_state",
            "new_state",
            "exchange_order_id",
            "allocated_qty",
            "sold_qty",
            "attempt",
            "retryable",
            "next_attempt_at",
            "price",
            "quantity",
            "stop_trigger_price",
            "reason",
            "exchange_status",
            "client_order_id",
        ),
    ),
    "oco_group.state_changed": _spec(
        "oco_group.state_changed",
        category="lifecycle",
        aggregate_type="oco_group",
        required=("previous_state", "new_state"),
        allowed=("previous_state", "new_state", "protection_status", "closed"),
    ),
    "oco_leg.state_changed": _spec(
        "oco_leg.state_changed",
        category="lifecycle",
        aggregate_type="oco_leg",
        required=("kind", "target", "previous_state", "new_state"),
        allowed=(
            "kind",
            "target",
            "previous_state",
            "new_state",
            "exchange_status",
            "filled_qty",
            "filled_quote",
            "fee_amount",
            "fee_asset",
            "cancel_reason",
            "canceled_by_target",
            "verified",
            "attempt",
        ),
    ),
    "exchange.write_outcome": _spec(
        "exchange.write_outcome",
        category="mexc_request",
        aggregate_type="exchange_operation",
        severity="WARNING",
        required=("operation_type", "outcome"),
        allowed=("operation_type", "outcome", "response_status", "error_class", "exchange_code"),
    ),
    "reconciliation.outcome": _spec(
        "reconciliation.outcome",
        category="reconcile",
        aggregate_type="intent",
        severity="WARNING",
        required=("outcome",),
        allowed=("outcome", "local_state", "exchange_state"),
    ),
    "system.service_event": _spec(
        "system.service_event",
        category="emergency",
        aggregate_type="system",
        severity="INFO",
        required=("event_key", "message"),
        allowed=("event_key", "message"),
    ),
    "system.control_event": _spec(
        "system.control_event",
        category="emergency",
        aggregate_type="system",
        severity="INFO",
        required=("event_key", "message"),
        allowed=("event_key", "message"),
    ),
    "trade.protection_critical": _spec(
        "trade.protection_critical",
        category="risk",
        aggregate_type="trade",
        severity="CRITICAL",
        required=("event_key", "detail"),
        allowed=("event_key", "detail"),
    ),
    "fill.recorded": _spec(
        "fill.recorded",
        category="reconcile",
        aggregate_type="exchange_order",
        required=("quantity",),
        allowed=(
            "exchange_trade_id",
            "price",
            "quantity",
            "quote_amount",
            "fee_asset",
            "fee_amount",
            "source",
            "intent_id",
        ),
    ),
}


class DomainEventError(ValueError):
    """Raised when an event does not satisfy the immutable envelope contract."""


class DomainEventConflict(DomainEventError):
    """Raised when an existing event_id has a different immutable envelope."""


@dataclass(frozen=True)
class DomainEvent:
    """Immutable semantic envelope for one meaningful lifecycle transition."""

    event_id: str
    event_type: str
    schema_version: int
    occurred_at_utc: str
    aggregate_type: str
    aggregate_id: str
    correlation_id: str
    payload_json: str
    category: str
    severity: str
    audience: tuple[str, ...]
    causation_id: str | None = None
    symbol: str | None = None
    event_class: str = DOMAIN_EVENT_CLASS

    @property
    def spec(self) -> EventSpec:
        return EVENT_REGISTRY[self.event_type]

    @property
    def payload(self) -> dict[str, Any]:
        return json.loads(self.payload_json)

    @property
    def audience_json(self) -> str:
        return canonical_json(list(self.audience))

    def immutable_fields(self) -> tuple[Any, ...]:
        """Fields that must match exactly when an event_id is retried."""

        return (
            self.event_id,
            self.event_type,
            self.schema_version,
            self.occurred_at_utc,
            self.aggregate_type,
            self.aggregate_id,
            self.correlation_id,
            self.causation_id,
            self.symbol,
            self.payload_json,
            self.category,
            self.severity,
            self.audience_json,
            self.event_class,
        )


def _reject_json_value(value: Any, *, depth: int = 0) -> None:
    if depth > MAX_PAYLOAD_DEPTH:
        raise DomainEventError("payload nesting exceeds the supported depth")
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int):
        return
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise DomainEventError("payload string exceeds the supported length")
        return
    if isinstance(value, (list, tuple)):
        for item in value:
            _reject_json_value(item, depth=depth + 1)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DomainEventError("payload object keys must be strings")
            if len(key) > 120:
                raise DomainEventError("payload object key exceeds the supported length")
            _reject_json_value(item, depth=depth + 1)
        return
    # float/Decimal/bytes/custom objects are rejected rather than stringified:
    # money and quantities must be passed as canonical strings.
    raise DomainEventError(f"unsupported payload value type: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    """Return deterministic compact JSON, rejecting unsupported values."""

    _reject_json_value(value)
    try:
        text = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as exc:  # defensive: validator should have caught it
        raise DomainEventError(f"payload is not serializable: {exc}") from exc
    if len(text.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise DomainEventError("payload exceeds the supported size")
    return text


def normalize_occurred_at(value: datetime | str | None = None) -> str:
    """Normalize an aware instant to UTC ISO-8601 with ``Z``."""

    if value is None:
        instant = datetime.now(UTC)
    elif isinstance(value, datetime):
        instant = value
    elif isinstance(value, str):
        try:
            instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DomainEventError("occurred_at_utc must be an ISO-8601 instant") from exc
    else:
        raise DomainEventError("occurred_at_utc must be an ISO-8601 string or datetime")
    if instant.tzinfo is None:
        raise DomainEventError("occurred_at_utc must include a timezone")
    return instant.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_payload(spec: EventSpec, payload: Mapping[str, Any]) -> str:
    if not isinstance(payload, Mapping):
        raise DomainEventError("payload must be an object")
    unknown = set(payload) - spec.allowed_payload
    if unknown:
        raise DomainEventError(f"payload contains unsupported fields: {sorted(unknown)}")
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        normalized[key] = value
    missing = set(spec.required_payload) - set(normalized)
    if missing:
        raise DomainEventError(f"payload is missing required fields: {sorted(missing)}")
    return canonical_json(normalized)


def build_domain_event(
    event_type: str,
    *,
    aggregate_id: str,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    symbol: str | None = None,
    payload: Mapping[str, Any] | None = None,
    occurred_at: datetime | str | None = None,
    severity: str | None = None,
    event_id: str | None = None,
    schema_version: int = DOMAIN_SCHEMA_VERSION,
) -> DomainEvent:
    """Build and validate an immutable domain event.

    A new UUID is generated for every call unless the caller supplies an
    explicit ``event_id`` (used by retry/idempotency tests and by a future
    outbox replay).  Distinct transitions must always call this function
    separately; there is no dedupe/coalescing layer here.
    """

    if not isinstance(event_type, str):
        raise DomainEventError("event_type must be a string")
    spec = EVENT_REGISTRY.get(event_type)
    if spec is None:
        raise DomainEventError(f"unknown domain event_type: {event_type!r}")
    if not isinstance(aggregate_id, str):
        raise DomainEventError("aggregate_id must be a string")
    aggregate_value = aggregate_id.strip()
    if not aggregate_value:
        raise DomainEventError("aggregate_id is required")
    if event_id is not None and not isinstance(event_id, str):
        raise DomainEventError("event_id must be a string")
    event_identifier = event_id if event_id is not None else uuid.uuid4().hex
    event_identifier = event_identifier.strip()
    if not event_identifier:
        raise DomainEventError("event_id is required")
    try:
        canonical_event_id = uuid.UUID(event_identifier).hex
    except (ValueError, AttributeError) as exc:
        raise DomainEventError("event_id must be a UUID") from exc
    if canonical_event_id in EVENT_REGISTRY:
        raise DomainEventError("event_id must be distinct from event_type")
    if not isinstance(schema_version, int) or isinstance(schema_version, bool):
        raise DomainEventError("schema_version must be an integer")
    if schema_version != DOMAIN_SCHEMA_VERSION:
        raise DomainEventError(f"unsupported schema_version: {schema_version!r}")
    if severity is not None and not isinstance(severity, str):
        raise DomainEventError("severity must be a string")
    severity_value = (severity or spec.default_severity).upper()
    if severity_value not in ALLOWED_SEVERITIES:
        raise DomainEventError(f"invalid severity: {severity!r}")
    payload_json = _validate_payload(spec, {} if payload is None else payload)
    if correlation_id is not None and not isinstance(correlation_id, str):
        raise DomainEventError("correlation_id must be a string")
    correlation = (correlation_id if correlation_id is not None else aggregate_value).strip()
    if not correlation:
        raise DomainEventError("correlation_id is required")
    if len(correlation) > 200:
        raise DomainEventError("correlation_id exceeds the supported length")
    if causation_id is not None and not isinstance(causation_id, str):
        raise DomainEventError("causation_id must be a string")
    causation_value = causation_id.strip() if causation_id is not None else None
    if causation_value == "":
        causation_value = None
    if symbol is not None and not isinstance(symbol, str):
        raise DomainEventError("symbol must be a string")
    symbol_value = symbol.strip().upper() if symbol is not None else None
    if symbol_value is not None and (not symbol_value or len(symbol_value) > 40 or not symbol_value.isalnum()):
        raise DomainEventError("symbol must be a short alphanumeric identifier")
    return DomainEvent(
        event_id=canonical_event_id,
        event_type=event_type,
        schema_version=DOMAIN_SCHEMA_VERSION,
        occurred_at_utc=normalize_occurred_at(occurred_at),
        aggregate_type=spec.aggregate_type,
        aggregate_id=aggregate_value,
        correlation_id=correlation,
        causation_id=causation_value,
        symbol=symbol_value,
        payload_json=payload_json,
        category=spec.category,
        severity=severity_value,
        audience=spec.audience,
    )


def validate_domain_event(event: DomainEvent) -> DomainEvent:
    """Revalidate an existing envelope before persistence."""

    if not isinstance(event, DomainEvent):
        raise DomainEventError("event must be a DomainEvent")
    rebuilt = build_domain_event(
        event.event_type,
        aggregate_id=event.aggregate_id,
        correlation_id=event.correlation_id,
        causation_id=event.causation_id,
        symbol=event.symbol,
        payload=json.loads(event.payload_json),
        occurred_at=event.occurred_at_utc,
        severity=event.severity,
        event_id=event.event_id,
        schema_version=event.schema_version,
    )
    if rebuilt.immutable_fields() != event.immutable_fields():
        raise DomainEventError("domain event envelope is not canonical")
    return event
