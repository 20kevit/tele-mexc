"""Exchange evidence recorder: writes intent before the call, classifies outcomes.

Uses a real migrated SQLite database and the real :class:`TradingStore`.  No
network is used; the MEXC client is a deterministic fake.
"""

from __future__ import annotations

import pytest

from tg_signal_ingest.db import Database
from tg_signal_ingest.trading.audit import redact_payload, redact_text
from tg_signal_ingest.trading.evidence import ExchangeEvidence
from tg_signal_ingest.trading.store import TradingStore


class _Op:
    def __init__(self):
        self.calls = []

    def account(self):
        self.calls.append("account")
        return {"balances": [{"asset": "USDT", "free": "10.5", "locked": "0"}]}

    def place_order(self, **kw):
        self.calls.append("place_order")
        return {"orderId": "ex-1", "status": "NEW"}


@pytest.fixture()
def store(cfg, tmp_path):
    db = Database(tmp_path / "data" / "evidence.sqlite3")
    db.migrate()
    yield TradingStore(db, cfg.trading)
    db.close()


def test_operation_writes_intent_before_call(store):
    client = _Op()
    evidence = ExchangeEvidence(store)
    with evidence.operation(
        operation_type="account", purpose="preflight", symbol="BTCUSDT",
    ) as op:
        result = client.account()
        op.succeed(result)
    ops = store.list_exchange_operations()
    assert len(ops) == 1
    assert ops[0]["operation_type"] == "account"
    assert ops[0]["operation_class"] == "read"
    assert ops[0]["write_attempted"] == 0
    assert ops[0]["symbol"] == "BTCUSDT"
    assert ops[0]["response_received"] == 1


def test_uncertain_write_is_recorded_and_not_retried(store):
    def flaky(**kw):
        raise TimeoutError("timeout after send")

    client = _Op()
    client.place_order = flaky
    evidence = ExchangeEvidence(store)
    with pytest.raises(TimeoutError), evidence.operation(
        operation_type="place_order", purpose="submit", symbol="BTCUSDT",
        client_order_id="cid-1",
    ) as op:
        client.place_order(symbol="BTCUSDT")
        op.succeed({})  # never reached
    ops = store.list_exchange_operations()
    assert len(ops) == 1
    assert ops[0]["operation_type"] == "place_order"
    assert ops[0]["operation_class"] == "write"
    assert ops[0]["write_attempted"] == 1
    assert ops[0]["client_order_id"] == "cid-1"
    assert ops[0]["exchange_code"] == "TimeoutError"
    assert ops[0]["uncertainty"] == "uncertain"


def test_redaction_bounds_and_masks_secrets():
    assert redact_text("bot=123456789:ABCdefGHIjklMNOpqrsTUVwxyz") == "bot=[REDACTED_BOT_TOKEN]"
    assert (
        redact_text("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9")
        == "Authorization: [REDACTED_AUTH]"
    )
    assert redact_text("api_key=sk_live_abcdef1234567890") == "[REDACTED_SECRET]"
    assert (
        redact_payload({"api_key": "secret", "qty": "0.1"})
        == '{"api_key": "[REDACTED]", "qty": "0.1"}'
    )
    assert redact_text(None) is None
    assert redact_payload(None) is None
    long_text = "a " * 3000
    assert redact_text(long_text).endswith("…[truncated]")
    assert len(redact_text(long_text)) <= 2000 + len("…[truncated]")


def test_audit_event_type_normalization():
    from tg_signal_ingest.trading.audit import normalize_event_type

    assert normalize_event_type("mexc_request", "place_order") == "mexc_request.place_order"
    assert normalize_event_type("MEXC_Request", "Place_Order!") == "mexc_request.place_order"
    assert normalize_event_type("cat", "evt", explicit="custom.Type") == "custom.Type"
