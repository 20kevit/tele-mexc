"""Deterministic tests for the dedicated MEXC CLI adapter (no subprocess/network).

The real binary is only used as an executable path; every invocation is served
by an injected runner so these tests cannot place, cancel or query an order.
"""

from __future__ import annotations

import json
import subprocess
import sys
from decimal import Decimal

import pytest

from tg_signal_ingest.mexc.cli import (
    MexcCliAdapter,
    discover_binary,
    parse_version,
)
from tg_signal_ingest.mexc.errors import (
    MexcAuthError,
    MexcCliConfirmationRequired,
    MexcConfigError,
    MexcDuplicateOrder,
    MexcNetworkError,
    MexcPrecisionError,
)


def completed(argv, *, stdout="", stderr="", code=0):
    return subprocess.CompletedProcess(list(argv), code, stdout=stdout, stderr=stderr)


def adapter(runner, **kwargs):
    return MexcCliAdapter(binary=sys.executable, runner=runner, **kwargs)


def test_parse_version_and_discovery():
    assert parse_version("mexc version 0.0.22 [public] (commit x)") == "0.0.22"
    assert isinstance(discover_binary(sys.executable), str)


def test_local_check_reports_version_and_required_operations():
    calls: list[list[str]] = []

    def runner(argv, timeout):
        calls.append(list(argv))
        if "--version" in argv:
            return completed(argv, stdout="mexc version 0.0.22 [public]\n")
        operation = argv[-1]
        return completed(argv, stdout=json.dumps({"operation": operation}))

    health = adapter(runner).check()
    assert health.ok is True
    assert health.version == "0.0.22"
    assert any(c["name"] == "schema:spot.trade.place" for c in health.checks)
    assert all("--yes" not in argv for argv in calls)


def test_version_below_minimum_fails_closed():
    def runner(argv, timeout):
        if "--version" in argv:
            return completed(argv, stdout="mexc version 0.0.21\n")
        return completed(argv, stdout="{}")

    health = adapter(runner).check(require_operations=False)
    assert health.ok is False
    assert "older than the required 0.0.22" in health.problems[0]


def test_place_argv_is_array_only_and_never_auto_confirms():
    seen: list[list[str]] = []

    def runner(argv, timeout):
        seen.append(list(argv))
        return completed(argv, stdout=json.dumps({"code": 0, "data": {"orderId": "ex-1"}}))

    result = adapter(runner).place_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="limit",
        quantity=Decimal("0.1"),
        price=Decimal("100"),
        client_order_id="tsi-abc",
        take_profit_price=Decimal("110"),
        stop_loss_price=Decimal("95"),
    )
    argv = seen[0]
    assert "--yes" not in argv
    assert "--no-retry" in argv
    assert "--read-only" not in argv
    body = json.loads(argv[argv.index("--data") + 1])
    assert body == {
        "currency": "BTC",
        "market": "USDT",
        "price": "100",
        "quantity": "0.1",
        "tradeType": "BUY",
        "orderType": "LIMIT_ORDER",
        "orderSource": "CLI",
        "clientOrderId": "tsi-abc",
        "profitTriggerPrice": "110",
        "loseTriggerPrice": "95",
    }
    assert result["orderId"] == "ex-1"


def test_place_uses_yes_only_when_explicitly_enabled():
    seen: list[list[str]] = []

    def runner(argv, timeout):
        seen.append(list(argv))
        return completed(argv, stdout=json.dumps({"code": 0, "data": {"orderId": "ex-1"}}))

    adapter(runner, confirm_writes=True).place_order(
        symbol="BTCUSDT", side="BUY", order_type="limit",
        quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1",
    )
    assert "--yes" in seen[0]


def test_dry_run_sends_no_yes_and_no_request():
    seen: list[list[str]] = []

    def runner(argv, timeout):
        seen.append(list(argv))
        return completed(argv, stdout=json.dumps({"method": "POST"}))

    result = adapter(runner, confirm_writes=True).place_order(
        symbol="BTCUSDT", side="BUY", order_type="limit",
        quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1", dry_run=True,
    )
    assert result["dry_run"] is True
    assert "--dry-run" in seen[0]
    assert "--read-only" not in seen[0]  # read-only would block the write first
    assert "--yes" not in seen[0]
    assert "--no-retry" not in seen[0]


def test_tp_sl_direction_is_validated_locally():
    with pytest.raises(MexcPrecisionError):
        adapter(lambda a, t: completed(a)).build_place_body(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("100"),
            take_profit_price=Decimal("90"),
        )
    with pytest.raises(MexcPrecisionError):
        adapter(lambda a, t: completed(a)).build_place_body(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("100"),
            stop_loss_price=Decimal("100"),
        )


def test_market_orders_cannot_attach_tp_sl():
    with pytest.raises(MexcConfigError):
        adapter(lambda a, t: completed(a)).build_place_body(
            symbol="BTCUSDT", side="BUY", order_type="market",
            quantity=Decimal("1"), take_profit_price=Decimal("110"),
        )


def test_market_requires_exactly_one_size_field():
    with pytest.raises(MexcConfigError):
        adapter(lambda a, t: completed(a)).place_market(
            symbol="BTCUSDT", side="BUY", quantity=Decimal("1"),
            quote_order_qty=Decimal("10"),
        )


def test_client_order_id_is_validated():
    with pytest.raises(MexcConfigError):
        adapter(lambda a, t: completed(a)).place_order(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("1"), client_order_id="bad id!",
        )


def test_duplicate_response_is_typed_for_reconciliation():
    def runner(argv, timeout):
        return completed(
            argv, code=2,
            stdout=json.dumps({"error": {
                "type": "validation", "subtype": "duplicate",
                "message": "duplicate clientOrderId",
            }}),
        )

    with pytest.raises(MexcDuplicateOrder):
        adapter(runner).place_order(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1",
        )


def test_unstructured_write_failure_is_uncertain_not_rejected():
    def runner(argv, timeout):
        return completed(argv, code=3, stderr="panic: runtime error in cli")

    with pytest.raises(MexcNetworkError):
        adapter(runner).place_order(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1",
        )


def test_client_order_id_collision_is_detected_without_the_word_duplicate():
    def runner(argv, timeout):
        return completed(argv, code=2, stdout=json.dumps({"error": {
            "type": "validation", "subtype": "invalid",
            "message": "clientOrderId already exists for this account",
        }}))

    with pytest.raises(MexcDuplicateOrder):
        adapter(runner).place_order(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1",
        )


def test_missing_credentials_are_typed_and_redacted():
    secret = "A" * 64

    def runner(argv, timeout):
        return completed(
            argv, code=2,
            stdout=json.dumps({"error": {
                "type": "validation", "subtype": "failed_precondition",
                "message": f"this operation requires API credentials {secret}",
            }}),
        )

    with pytest.raises(MexcAuthError) as caught:
        adapter(runner).read_only_access()
    assert secret not in str(caught.value)
    assert "<redacted>" in str(caught.value)


def test_exchange_api_key_format_error_is_auth_not_server():
    def runner(argv, timeout):
        return completed(
            argv,
            code=6,
            stdout=json.dumps({
                "error": {
                    "type": "api",
                    "subtype": "upstream_error",
                    "message": "API-key format invalid",
                    "code": 700001,
                }
            }),
        )

    with pytest.raises(MexcAuthError) as caught:
        adapter(runner).read_only_access()
    assert caught.value.code == 700001
    assert "API-key format invalid" in str(caught.value)
    assert "server" not in str(caught.value).lower()


def test_write_without_confirmation_is_terminal_and_clear():
    def runner(argv, timeout):
        return completed(argv, code=2, stderr="write operations require --yes")

    with pytest.raises(MexcCliConfirmationRequired):
        adapter(runner).place_order(
            symbol="BTCUSDT", side="BUY", order_type="limit",
            quantity=Decimal("1"), price=Decimal("1"), client_order_id="id-1",
        )


def test_verify_protection_only_claims_active_when_echoed():
    def echoed(argv, timeout):
        if "current-orders" in argv:
            return completed(argv, stdout=json.dumps({
                "code": 0,
                "data": {"list": [{"orderId": "9", "orderType": "1", "state": "1"}]},
            }))
        return completed(argv, stdout=json.dumps({
            "code": 0,
            "data": {"orderId": "9", "state": "NEW",
                     "profitTriggerPrice": "110", "loseTriggerPrice": "95"},
        }))

    result = adapter(echoed).verify_protection(symbol="BTCUSDT", order_id="9")
    assert result["verified"] is True
    assert result["take_profit"] == "110"

    def not_echoed(argv, timeout):
        if "current-orders" in argv:
            return completed(argv, stdout=json.dumps({
                "code": 0, "data": {"list": [{"orderId": "9", "orderType": "1"}]},
            }))
        return completed(argv, stdout=json.dumps({
            "code": 0, "data": {"orderId": "9", "state": "NEW"},
        }))

    result = adapter(not_echoed).verify_protection(symbol="BTCUSDT", order_id="9")
    assert result["verified"] is False


def test_find_by_client_id_checks_open_then_history():
    calls: list[list[str]] = []

    def runner(argv, timeout):
        calls.append(list(argv))
        if "current-orders" in argv:
            return completed(argv, stdout=json.dumps({"code": 0, "data": {"list": []}}))
        return completed(argv, stdout=json.dumps({
            "code": 0,
            "data": {"list": [{
                "orderId": "77", "clientOrderId": "tsi-abc",
                "state": "1", "orderType": "1",
            }]},
        }))

    found = adapter(runner).find_by_client_id("BTCUSDT", "tsi-abc")
    assert found["orderId"] == "77"
    assert len(calls) == 2
    assert calls[0][0]  # argv is non-empty


def test_query_order_retries_the_other_order_type_when_listing_is_gone():
    detail_calls: list[str] = []

    def runner(argv, timeout):
        if "current-orders" in argv or "history-orders" in argv:
            return completed(argv, stdout=json.dumps({"code": 0, "data": {"list": []}}))
        order_type = argv[argv.index("--orderType") + 1]
        detail_calls.append(order_type)
        if order_type == "LIMIT_ORDER":
            return completed(
                argv, stdout=json.dumps({"code": 500, "msg": "order does not exist"}),
            )
        return completed(argv, stdout=json.dumps({
            "code": 0,
            "data": {"orderId": "9", "state": "FILLED", "orderType": "5",
                     "executedQty": "2", "price": "50"},
        }))

    detail = adapter(runner).query_order(symbol="BTCUSDT", order_id="9")
    assert detail["orderId"] == "9"
    assert detail["status"] == "FILLED"
    assert detail["type"] == "MARKET_ORDER"
    assert detail_calls == ["LIMIT_ORDER", "MARKET_ORDER"]


def test_query_order_does_not_invent_an_order_type():
    detail_calls: list[str] = []

    def runner(argv, timeout):
        if "current-orders" in argv:
            return completed(argv, stdout=json.dumps({
                "code": 0,
                "data": {"list": [{"orderId": "9", "orderType": "5", "state": "2"}]},
            }))
        if "history-orders" in argv:
            return completed(argv, stdout=json.dumps({"code": 0, "data": {"list": []}}))
        detail_calls.append(argv[argv.index("--orderType") + 1])
        return completed(argv, stdout=json.dumps({
            "code": 0, "data": {"orderId": "9", "state": "FILLED"},
        }))

    detail = adapter(runner).query_order(symbol="BTCUSDT", order_id="9")
    assert detail_calls == ["MARKET_ORDER"]
    assert detail["type"] is None  # never guessed when the detail omits it


def test_query_order_does_not_retry_a_credential_failure():
    detail_calls: list[list[str]] = []

    def runner(argv, timeout):
        if "current-orders" in argv or "history-orders" in argv:
            return completed(argv, stdout=json.dumps({"code": 0, "data": {"list": []}}))
        detail_calls.append(list(argv))
        return completed(
            argv, code=2,
            stdout=json.dumps({"error": {
                "type": "auth", "subtype": "unauthorized",
                "message": "authentication failed for this profile",
            }}),
        )

    with pytest.raises(MexcAuthError):
        adapter(runner).query_order(symbol="BTCUSDT", order_id="9")
    assert len(detail_calls) == 1



def test_profiles_lists_saved_names_without_selecting_a_profile():
    seen: list[list[str]] = []

    def runner(argv, timeout):
        seen.append(list(argv))
        return completed(
            argv,
            stdout=json.dumps(
                {
                    "code": 0,
                    "data": {
                        "active": "main",
                        "profiles": [{"name": "main"}, {"name": "backup"}],
                    },
                }
            ),
        )

    result = adapter(runner, profile="main").profiles()
    assert result == {"active": "main", "profiles": ["main", "backup"]}
    assert "--read-only" in seen[0]
    assert "--profile" not in seen[0]


def test_profiles_tolerates_null_and_string_rows():
    def runner(argv, timeout):
        return completed(
            argv,
            stdout=json.dumps({"code": 0, "data": {"active": "", "profiles": None}}),
        )

    assert adapter(runner).profiles() == {"active": None, "profiles": []}


def test_account_is_a_read_only_signed_operation():
    seen: list[list[str]] = []

    def runner(argv, timeout):
        seen.append(list(argv))
        return completed(
            argv,
            stdout=json.dumps(
                {
                    "code": 0,
                    "data": {
                        "canTrade": True,
                        "balances": [{"asset": "USDT", "free": "10", "locked": "0"}],
                    },
                }
            ),
        )

    payload = adapter(runner).account()
    assert payload["canTrade"] is True
    argv = seen[0]
    assert "--read-only" in argv
    assert "--yes" not in argv
    assert "--data" not in argv
    assert argv[-3:] == ["spot", "asset", "account"]
