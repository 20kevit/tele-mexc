"""Offline tests for ``tg-ingest trading preflight`` (local checks only).

The CLI adapter is replaced, so no subprocess, socket or order attempt occurs.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import tg_signal_ingest.main as main
from tg_signal_ingest.mexc.cli import CliHealth


def _cfg() -> SimpleNamespace:
    return SimpleNamespace(
        mexc=SimpleNamespace(
            cli_enabled=True,
            cli_binary=sys.executable,
            cli_profile=None,
            cli_timeout_s=5.0,
            cli_min_version="0.0.22",
            api_key=None,
            api_secret=None,
        ),
        trading=SimpleNamespace(
            execution_acknowledgement="I_UNDERSTAND_REAL_MONEY_AND_RISKS",
        ),
    )


def _healthy() -> CliHealth:
    return CliHealth(ok=True, binary="/fake/mexc", version="0.0.22")


def test_preflight_fails_when_doctor_reports_a_problem(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg())
    called: list[str] = []

    class FakeAdapter:
        binary = "/fake/mexc"

        def __init__(self, **_kwargs) -> None:
            pass

        def check(self) -> CliHealth:
            return _healthy()

        def doctor(self) -> dict:
            return {
                "ok": False,
                "checks": [
                    {"name": "credentials", "status": "fail", "message": "no profile"},
                ],
            }

        def read_only_access(self) -> dict:
            called.append("read")
            raise AssertionError("signed read must not run after a failed doctor")

    monkeypatch.setattr("tg_signal_ingest.mexc.cli.MexcCliAdapter", FakeAdapter)
    code = main.cmd_trading_preflight(SimpleNamespace(network=True))
    output = capsys.readouterr().out
    assert code == 2
    assert "RESULT: FAIL" in output
    assert called == []


def test_preflight_passes_doctor_then_runs_one_signed_read(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg())
    called: list[str] = []

    class FakeAdapter:
        binary = "/fake/mexc"

        def __init__(self, **_kwargs) -> None:
            pass

        def check(self) -> CliHealth:
            return _healthy()

        def doctor(self) -> dict:
            return {"ok": True, "checks": [{"name": "credentials", "status": "pass"}]}

        def read_only_access(self) -> dict:
            called.append("read")
            return {"ok": True, "rows": 0}

    monkeypatch.setattr("tg_signal_ingest.mexc.cli.MexcCliAdapter", FakeAdapter)
    code = main.cmd_trading_preflight(SimpleNamespace(network=True))
    output = capsys.readouterr().out
    assert code == 0
    assert called == ["read"]
    assert "RESULT: PASS" in output


def test_preflight_without_network_does_not_touch_the_exchange(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg())

    class FakeAdapter:
        binary = "/fake/mexc"

        def __init__(self, **_kwargs) -> None:
            pass

        def check(self) -> CliHealth:
            return _healthy()

        def doctor(self) -> dict:
            raise AssertionError("doctor needs network; --network was not requested")

        def read_only_access(self) -> dict:
            raise AssertionError("signed read needs network; --network was not requested")

    monkeypatch.setattr("tg_signal_ingest.mexc.cli.MexcCliAdapter", FakeAdapter)
    code = main.cmd_trading_preflight(SimpleNamespace(network=False))
    output = capsys.readouterr().out
    assert code == 0
    assert "NOT CHECKED" in output


def test_preflight_fails_closed_when_cli_writes_are_disabled(monkeypatch, capsys):
    cfg = _cfg()
    cfg.mexc.cli_enabled = False
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: cfg)
    code = main.cmd_trading_preflight(SimpleNamespace(network=False))
    output = capsys.readouterr().out
    assert code == 1
    assert "RESULT: FAIL" in output


# ---------------------------------------------------------------------------
# CLI profile / account-identity checks
# ---------------------------------------------------------------------------


def _rest_cfg(profile: str | None = None) -> SimpleNamespace:
    cfg = _cfg()
    cfg.mexc.cli_profile = profile
    cfg.mexc.api_key = "rest-key"
    cfg.mexc.api_secret = "rest-secret"
    return cfg


def _account_adapter(profiles: dict, account: dict | None = None, *, seen: list[str] | None = None):
    class FakeAdapter:
        binary = "/fake/mexc"

        def __init__(self, **_kwargs) -> None:
            pass

        def check(self) -> CliHealth:
            return _healthy()

        def profiles(self) -> dict:
            return profiles

        def doctor(self) -> dict:
            return {"ok": True, "checks": [{"name": "credentials", "status": "pass"}]}

        def read_only_access(self) -> dict:
            if seen is not None:
                seen.append("read")
            return {"ok": True, "rows": 0}

        def account(self) -> dict:
            if seen is not None:
                seen.append("account")
            return account or {"balances": []}

    return FakeAdapter


def test_preflight_fails_when_no_cli_profile_is_saved(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _cfg())
    monkeypatch.setattr(
        "tg_signal_ingest.mexc.cli.MexcCliAdapter",
        _account_adapter({"active": None, "profiles": []}),
    )
    code = main.cmd_trading_preflight(SimpleNamespace(network=False))
    output = capsys.readouterr().out
    assert code == 1
    assert "no CLI credential profile" in output
    assert "mexc profile set" in output
    assert "same MEXC website-issued Open API" in output


def test_preflight_fails_when_configured_profile_is_not_saved(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _rest_cfg("missing"))
    monkeypatch.setattr(
        "tg_signal_ingest.mexc.cli.MexcCliAdapter",
        _account_adapter({"active": "other", "profiles": ["other"]}),
    )
    code = main.cmd_trading_preflight(SimpleNamespace(network=False))
    output = capsys.readouterr().out
    assert code == 1
    assert "'missing' is not saved" in output


def test_preflight_fails_when_no_profile_is_active(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _rest_cfg())
    monkeypatch.setattr(
        "tg_signal_ingest.mexc.cli.MexcCliAdapter",
        _account_adapter({"active": None, "profiles": ["one"]}),
    )
    code = main.cmd_trading_preflight(SimpleNamespace(network=False))
    output = capsys.readouterr().out
    assert code == 1
    assert "no active CLI profile" in output


def _install_rest(monkeypatch, rest_account: dict) -> None:
    class FakeRest:
        def account(self) -> dict:
            return rest_account

    monkeypatch.setattr(
        "tg_signal_ingest.trading.runtime.build_mexc_client",
        lambda _cfg: FakeRest(),
    )
    import importlib
    from types import SimpleNamespace as NS

    credentials_module = importlib.import_module("tg_signal_ingest.mexc.credentials")
    monkeypatch.setattr(
        credentials_module,
        "validate_credentials",
        lambda _rest: NS(
            permissions=("SPOT_ACCOUNT_READ", "SPOT_DEAL_WRITE"),
            remaining_validity=-999,
            permanent=True,
        ),
    )


def test_preflight_fails_when_cli_and_rest_accounts_differ(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _rest_cfg())
    monkeypatch.setattr(
        "tg_signal_ingest.mexc.cli.MexcCliAdapter",
        _account_adapter(
            {"active": "main", "profiles": ["main"]},
            {"balances": [{"asset": "USDT", "free": "1", "locked": "0"}]},
        ),
    )
    _install_rest(monkeypatch, {"balances": [{"asset": "USDT", "free": "2", "locked": "0"}]})
    code = main.cmd_trading_preflight(SimpleNamespace(network=True))
    output = capsys.readouterr().out
    assert code == 2
    assert "MISMATCH" in output
    assert "RESULT: FAIL" in output


def test_preflight_passes_when_cli_and_rest_accounts_match(monkeypatch, capsys):
    monkeypatch.setattr(main, "_load_or_exit", lambda **_kwargs: _rest_cfg())
    account = {"balances": [{"asset": "USDT", "free": "1.5", "locked": "0.5"}]}
    seen: list[str] = []
    monkeypatch.setattr(
        "tg_signal_ingest.mexc.cli.MexcCliAdapter",
        _account_adapter({"active": "main", "profiles": ["main"]}, account, seen=seen),
    )
    _install_rest(monkeypatch, account)
    code = main.cmd_trading_preflight(SimpleNamespace(network=True))
    output = capsys.readouterr().out
    assert code == 0
    assert "account identity: MATCH" in output
    assert "REST key: VALID" in output
    assert seen == ["read", "account"]
    assert "RESULT: PASS" in output
