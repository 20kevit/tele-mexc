"""Dedicated adapter for the installed ``@mexc-ai/cli`` Spot trading binary.

Why a second MEXC integration path exists
-----------------------------------------
The REST client in :mod:`tg_signal_ingest.mexc.client` is verified and remains
the source of truth for *read-only* data (exchange metadata, account balances,
fills).  The official CLI is the only supported way to attach a take-profit
and/or stop-loss to a new Spot order.  This module therefore owns **live write
operations only**; :class:`~tg_signal_ingest.mexc.hybrid.HybridMexcClient`
delegates every read back to REST.

Safety properties enforced here
-------------------------------
* argv-array subprocess execution only (no shell, no string interpolation);
* no ``--yes`` unless the caller explicitly passes ``confirm=True``;
* write operations always run with ``--no-retry`` and are never retried here;
* ``clientOrderId`` is validated and reused verbatim; a duplicate response is
  surfaced as :class:`MexcDuplicateOrder` so the caller reconciles instead of
  placing a second order;
* stdout/stderr are parsed as a JSON envelope and reduced to sanitized,
  typed errors; raw bodies, profile contents and credentials are never logged;
* the CLI is checked for a minimum version and for the required operations
  before any order is attempted.

The adapter intentionally does not read MEXC API keys.  Credentials live in the
CLI's own encrypted ``~/.mexc/profiles`` store and are created interactively by
the operator (``mexc profile set`` then ``mexc profile use``).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .errors import (
    MexcAuthError,
    MexcCliConfirmationRequired,
    MexcCliIncompatible,
    MexcCliUnavailable,
    MexcConfigError,
    MexcDuplicateOrder,
    MexcError,
    MexcNetworkError,
    MexcOrderNotFound,
    MexcOrderRejected,
    MexcPermissionError,
    MexcPrecisionError,
    MexcRateLimitError,
    MexcServerError,
    MexcSymbolError,
    MexcUnexpectedResponse,
)

DEFAULT_MIN_VERSION = "0.0.22"
DEFAULT_TIMEOUT_S = 20.0
KNOWN_LINUX_BINARY = (
    "/usr/lib/node_modules/@mexc-ai/cli/node_modules/@mexc-ai/cli-linux-x64/bin/mexc"
)
CLIENT_ORDER_ID_RE = re.compile(r"^[0-9a-zA-Z_-]{1,32}$")
PROFILE_RE = re.compile(r"^[0-9A-Za-z._-]{1,64}$")
_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")

# Proxy variables are stripped so the exchange path obeys the repository rule
# that trading traffic is direct, never inherited from an ambient proxy.
_PROXY_ENV = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
)
_SECRET_ENV = ("MEXC_API_KEY", "MEXC_API_SECRET")

_ORDER_TYPE_NAMES = {
    "limit": "LIMIT_ORDER",
    "market": "MARKET_ORDER",
    "ioc": "IMMEDIATE_OR_CANCEL",
    "fok": "FILL_OR_KILL",
    "post_only": "POST_ONLY",
}
_ORDER_TYPE_CODES = {
    -1: "TRIGGER_ORDER",
    1: "LIMIT_ORDER",
    2: "POST_ONLY",
    3: "IMMEDIATE_OR_CANCEL",
    4: "FILL_OR_KILL",
    5: "MARKET_ORDER",
    100: "STOP_LIMIT_ORDER",
    101: "OCO_STOP_LIMIT_ORDER",
    102: "OCO_LIMIT_ORDER",
    103: "STOP_MARKET_ORDER",
    104: "TRACK_LIMIT_ORDER",
    105: "TRACK_MARKET_ORDER",
}
_SIDE_CODES = {1: "BUY", 2: "SELL"}
_STATUS_CODES = {
    0: "INACTIVE",
    1: "NEW",
    2: "FILLED",
    3: "PARTIALLY_FILLED",
    4: "CANCELED",
    5: "PARTIALLY_CANCELED",
}
_STATUS_ALIASES = {
    "CANCELLED": "CANCELED",
    "PARTIALLY_CANCELLED": "PARTIALLY_CANCELED",
    "PARTIALLYFILLED": "PARTIALLY_FILLED",
    "PARTIAL_FILLED": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "NEW": "NEW",
    "INACTIVE": "INACTIVE",
}

Runner = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]


@dataclass
class CliHealth:
    """Result of a local binary/version/schema check (no network, no secrets)."""

    ok: bool
    binary: str | None = None
    version: str | None = None
    problems: list[str] = field(default_factory=list)
    checks: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "binary": self.binary,
            "version": self.version,
            "problems": list(self.problems),
            "checks": list(self.checks),
        }


def discover_binary(explicit: str | None = None) -> str:
    """Locate the real CLI binary, preferring an explicit/known path.

    The distro wrapper named ``mexc`` in ``$PATH`` may be broken, so the known
    per-platform binary is tried before ``shutil.which``.
    """

    candidates: list[str] = []
    if explicit:
        candidates.append(explicit)
    env_bin = os.environ.get("MEXC_CLI_BINARY")
    if env_bin:
        candidates.append(env_bin)
    if Path(KNOWN_LINUX_BINARY).is_file():
        candidates.append(KNOWN_LINUX_BINARY)
    which = shutil.which("mexc")
    if which:
        candidates.append(which)

    seen: set[str] = set()
    for candidate in candidates:
        path = str(Path(candidate).expanduser())
        if path in seen:
            continue
        seen.add(path)
        try:
            if Path(path).is_file() and os.access(path, os.X_OK):
                return path
        except OSError:
            continue
    raise MexcCliUnavailable(
        "MEXC CLI binary not found or not executable; set MEXC_CLI_BINARY to the "
        "real binary path (the global wrapper may be broken)."
    )


def parse_version(text: str) -> str | None:
    match = _VERSION_RE.search(text)
    if not match:
        return None
    return ".".join(match.groups())


def _version_tuple(version: str) -> tuple[int, int, int]:
    parts = version.split(".")
    return tuple(int(p) for p in (parts + ["0", "0", "0"])[:3])  # type: ignore[return-value]


def _sanitize(text: str, *, limit: int = 400) -> str:
    """Make CLI output safe to log: no control chars, no long token-like runs."""

    cleaned = re.sub(r"[\x00-\x08\x0b-\x1f\x7f]", " ", text or "")
    cleaned = re.sub(r"\b[0-9a-fA-F]{32,}\b", "<redacted>", cleaned)
    cleaned = re.sub(r"\b[A-Za-z0-9_-]{48,}\b", "<redacted>", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:limit]


def _int_code(value: Any) -> int | None:
    """Parse an exchange/CLI error code without trusting its JSON type."""

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _looks_like_type_mismatch(exc: MexcError) -> bool:
    """True when a read error plausibly means "wrong order type", not "no access"."""

    text = str(exc).lower()
    return any(
        token in text
        for token in ("order type", "ordertype", "order_type", "not exist", "not found", "no order")
    )


def _decimal_string(value: Decimal | str | int | None, field_name: str) -> str | None:
    if value is None:
        return None
    try:
        dec = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise MexcPrecisionError(f"{field_name} is not a valid decimal") from exc
    if not dec.is_finite() or dec <= 0:
        raise MexcPrecisionError(f"{field_name} must be a positive finite decimal")
    sign, digits, exponent = dec.as_tuple()
    decimals = max(0, -int(exponent))
    integer_digits = max(0, len(digits) + int(exponent))
    if integer_digits > 35 or decimals > 30:
        raise MexcPrecisionError(
            f"{field_name} exceeds the CLI decimal bounds (35 integer / 30 decimal digits)"
        )
    return format(dec, "f")


def _normalize_status(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, int):
        return _STATUS_CODES.get(value, "UNKNOWN")
    text = str(value).strip()
    if text.isdigit():
        return _STATUS_CODES.get(int(text), "UNKNOWN")
    upper = text.upper()
    return _STATUS_ALIASES.get(upper, upper)


def _normalize_order_type(value: Any) -> str | None:
    """Map a CLI order-type code/name to the name ``deal-detail`` expects.

    Returns ``None`` when the field is absent or unrecognized, so callers never
    record a guessed type for an exchange order.
    """

    if value is None:
        return None
    if isinstance(value, int):
        return _ORDER_TYPE_CODES.get(value)
    text = str(value).strip()
    if not text:
        return None
    if text.lstrip("-").isdigit():
        return _ORDER_TYPE_CODES.get(int(text))
    return text.upper()


def _normalize_side(value: Any) -> str:
    if isinstance(value, int):
        return _SIDE_CODES.get(value, "BUY")
    text = str(value or "").strip()
    if text.isdigit():
        return _SIDE_CODES.get(int(text), "BUY")
    return text.upper() or "BUY"


def _extract_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("list", "records", "rows", "items", "orderList", "result", "data"):
        rows = data.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        if isinstance(rows, dict):
            nested = _extract_rows(rows)
            if nested:
                return nested
    return []


def _pick(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
    lowered = {str(k).lower(): v for k, v in row.items()}
    for name in names:
        key = name.lower()
        if key in lowered and lowered[key] not in (None, ""):
            return lowered[key]
    return None


class MexcCliAdapter:
    """Typed, argv-only wrapper around the MEXC trading CLI."""

    def __init__(
        self,
        *,
        binary: str | None = None,
        profile: str | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        min_version: str = DEFAULT_MIN_VERSION,
        confirm_writes: bool = False,
        runner: Runner | None = None,
        symbols: Any | None = None,
        environ: dict[str, str] | None = None,
    ):
        if timeout_s <= 0:
            raise MexcConfigError("MEXC CLI timeout must be positive")
        if profile is not None and not PROFILE_RE.match(profile):
            raise MexcConfigError(
                "MEXC CLI profile name may only contain letters, digits, '.', '_' and '-'"
            )
        self.binary = discover_binary(binary)
        self.profile = profile
        self.timeout_s = float(timeout_s)
        self.min_version = min_version
        self.confirm_writes = bool(confirm_writes)
        self.symbols = symbols
        self._runner = runner or self._default_runner
        self._environ = dict(os.environ if environ is None else environ)

    # ------------------------------------------------------------- execution

    def _default_runner(
        self, argv: Sequence[str], timeout: float
    ) -> subprocess.CompletedProcess[str]:
        env = {
            key: value
            for key, value in self._environ.items()
            if key not in _PROXY_ENV and key not in _SECRET_ENV
        }
        env.setdefault("NO_COLOR", "1")
        try:
            return subprocess.run(  # noqa: S603 - fixed argv, no shell
                list(argv),
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
                shell=False,  # explicit: never invoke a shell
                env=env,
                stdin=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired as exc:
            raise MexcNetworkError(
                f"MEXC CLI timed out after {timeout:g}s; outcome is unknown"
            ) from exc
        except OSError as exc:
            raise MexcCliUnavailable(
                f"MEXC CLI could not be started ({exc.__class__.__name__})"
            ) from exc

    def _argv(
        self,
        *operation: str,
        query: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        read_only: bool = False,
        dry_run: bool = False,
        confirm: bool = False,
        use_profile: bool = True,
    ) -> list[str]:
        argv = [self.binary]
        if read_only:
            argv.append("--read-only")
        if dry_run:
            argv.append("--dry-run")
        # Never auto-confirm a write.  The owner must have explicitly enabled
        # non-interactive writes before this method passes ``confirm=True``.
        if confirm:
            argv.append("--yes")
        if self.profile and use_profile:
            argv.extend(["--profile", self.profile])
        argv.extend(operation)
        for key, value in (query or {}).items():
            if value is None:
                continue
            argv.extend([f"--{key}", str(value)])
        if data is not None:
            argv.extend(["--data", json.dumps(data, separators=(",", ":"), sort_keys=False)])
        return argv

    def _run(
        self,
        argv: Sequence[str],
        *,
        write: bool,
        timeout: float | None = None,
    ) -> Any:
        if write and "--no-retry" not in argv:
            argv = [*argv[:1], "--no-retry", *argv[1:]]
        result = self._runner(argv, float(timeout or self.timeout_s))
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        payload = self._parse_json(stdout) or self._parse_json(stderr)
        if result.returncode != 0:
            if payload is not None:
                self._raise_envelope(payload, write=write)
            message = _sanitize(stderr or stdout) or f"exit code {result.returncode}"
            if "confirm" in message.lower() or "--yes" in message:
                raise MexcCliConfirmationRequired(
                    "MEXC CLI refused the write without explicit non-interactive "
                    "confirmation; no order was sent"
                )
            if write:
                # Writes are non-idempotent.  A non-zero exit without a
                # structured envelope can happen after the signed request left
                # the process, so it is never a terminal rejection: the caller
                # must reconcile the client order id before doing anything else.
                raise MexcNetworkError(
                    f"MEXC CLI write exited {result.returncode} without a JSON "
                    f"envelope; outcome is unknown ({message})"
                )
            raise MexcOrderRejected(f"MEXC CLI failed: {message}")
        if payload is None:
            raise MexcUnexpectedResponse("MEXC CLI returned a non-JSON response")
        if isinstance(payload, dict) and "error" in payload:
            self._raise_envelope(payload, write=write)
        if isinstance(payload, dict):
            code = payload.get("code")
            success = payload.get("success")
            if code is not None and str(code) not in ("0", "200"):
                self._raise_envelope(payload, write=write)
            if success is False:
                self._raise_envelope(payload, write=write)
            if "data" in payload:
                return payload.get("data")
            return payload
        return payload

    @staticmethod
    def _parse_json(text: str) -> Any | None:
        text = (text or "").strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start == -1 or end <= start:
                return None
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                return None

    def _raise_envelope(self, payload: dict[str, Any], *, write: bool) -> None:
        error = payload.get("error") if isinstance(payload.get("error"), dict) else None
        if error is None:
            error = payload
        subtype = str(error.get("subtype") or "").strip().lower()
        etype = str(error.get("type") or "").strip().lower()
        message = _sanitize(
            str(error.get("message") or payload.get("msg") or "MEXC CLI operation failed")
        )
        hint = _sanitize(str(error.get("hint") or ""))
        haystack = f"{etype} {subtype} {message} {hint}".lower()
        code = _int_code(error.get("code", payload.get("code")))

        # The exchange reports a missing, truncated, deleted or wrong-account
        # Access Key as 700001/10072 (and a missing header as 400).  These are
        # credential failures even though the CLI envelope labels them
        # ``subtype=upstream_error``; never misreport them as server faults.
        if code in {400, 700001, 10072}:
            raise MexcAuthError(
                f"MEXC CLI authentication failed: {message}", code=code
            )

        if "duplicate" in haystack or (
            "clientorderid" in haystack
            and any(token in haystack for token in ("exist", "already", "repeat", "used", "unique"))
        ):
            raise MexcDuplicateOrder(
                f"MEXC CLI refused a duplicate client order id: {message}"
            )
        if "confirm" in haystack or "--yes" in haystack:
            raise MexcCliConfirmationRequired(
                "MEXC CLI requires explicit non-interactive confirmation; no order was sent"
            )
        if "permission" in haystack or "authority" in haystack:
            raise MexcPermissionError(f"MEXC CLI permission denied: {message}")
        if "credential" in haystack or "auth" in haystack or "profile" in haystack:
            raise MexcAuthError(f"MEXC CLI authentication failed: {message}")
        if "rate" in haystack or "too many" in haystack or "banned" in haystack:
            raise MexcRateLimitError(f"MEXC CLI rate limited: {message}")
        if any(
            token in haystack
            for token in ("precision", "decimal", "minimum", "invalid quantity", "invalid price")
        ):
            raise MexcPrecisionError(f"MEXC CLI rejected the order values: {message}")
        if any(token in haystack for token in ("symbol", "market name", "pair")):
            raise MexcSymbolError(f"MEXC CLI rejected the symbol: {message}")
        if any(token in haystack for token in ("network", "timeout", "connection", "dns")):
            raise MexcNetworkError(f"MEXC CLI network failure: {message}")
        if "server" in haystack or "upstream" in haystack or "internal" in haystack:
            raise MexcServerError(f"MEXC CLI/server failure: {message}")
        if write:
            raise MexcOrderRejected(f"MEXC CLI rejected the order: {message}")
        raise MexcError(f"MEXC CLI request failed: {message}")

    # --------------------------------------------------------------- health

    def version(self) -> str:
        result = self._runner([self.binary, "--version"], self.timeout_s)
        text = f"{result.stdout or ''}\n{result.stderr or ''}"
        version = parse_version(text)
        if result.returncode != 0 or version is None:
            raise MexcCliIncompatible("MEXC CLI did not report a usable version")
        if _version_tuple(version) < _version_tuple(self.min_version):
            raise MexcCliIncompatible(
                f"MEXC CLI {version} is older than the required {self.min_version}"
            )
        return version

    def check(self, *, require_operations: bool = True) -> CliHealth:
        checks: list[dict[str, Any]] = []
        problems: list[str] = []
        version: str | None = None
        try:
            version = self.version()
            checks.append({"name": "version", "status": "pass", "message": version})
        except MexcError as exc:
            problems.append(str(exc))
            checks.append({"name": "version", "status": "fail", "message": str(exc)})
        if require_operations:
            for operation in (
                "spot.trade.place",
                "spot.trade.place-trigger",
                "spot.trade.current-orders",
                "spot.trade.deal-detail",
            ):
                try:
                    data = self._run(
                        self._argv("schema", operation, read_only=True),
                        write=False,
                    )
                    found = isinstance(data, dict) and data.get("operation") == operation
                    if not found:
                        raise MexcUnexpectedResponse("schema response did not match the operation")
                    checks.append(
                        {"name": f"schema:{operation}", "status": "pass", "message": "present"}
                    )
                except MexcError as exc:
                    problems.append(f"{operation}: {exc}")
                    checks.append(
                        {"name": f"schema:{operation}", "status": "fail", "message": str(exc)}
                    )
        return CliHealth(
            ok=not problems,
            binary=self.binary,
            version=version,
            problems=problems,
            checks=checks,
        )

    def doctor(self) -> dict[str, Any]:
        data = self._run(self._argv("doctor", "--json", read_only=True), write=False)
        if not isinstance(data, dict):
            raise MexcUnexpectedResponse("MEXC CLI doctor returned an unexpected payload")
        return data

    def profiles(self) -> dict[str, Any]:
        """List saved CLI profiles (no credentials, no signed request).

        The CLI stores API keys outside this application.  This method only
        exposes profile names and the active pointer so preflight can fail
        closed before a live write is attempted.
        """
        data = self._run(
            self._argv("profile", "list", read_only=True, use_profile=False),
            write=False,
        )
        rows: Any = None
        active: Any = None
        if isinstance(data, dict):
            rows = data.get("profiles")
            active = data.get("active")
        elif isinstance(data, list):
            rows = data
        names: list[str] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, str):
                    names.append(row)
                elif isinstance(row, dict):
                    name = _pick(row, "name", "profile", "id")
                    if name:
                        names.append(str(name))
        return {
            "active": str(active) if active else None,
            "profiles": names,
        }

    def account(self) -> dict[str, Any]:
        """Read the CLI profile's Spot account (signed, read-only).

        ``spot asset account`` maps to the same ``GET /api/v3/account``
        OpenAPI endpoint used by the REST client, so a successful call proves
        the saved profile is an ``openapi`` identity.  The payload is never
        printed by callers; only a sanitized fingerprint is compared.
        """
        data = self._run(self._argv("spot", "asset", "account", read_only=True), write=False)
        if not isinstance(data, dict):
            raise MexcUnexpectedResponse("MEXC CLI account read returned an unexpected payload")
        return data

    def read_only_access(self) -> dict[str, Any]:
        """Proof that the saved profile can perform a signed read.

        ``current-orders`` is read-only, does not create anything and returns an
        empty page for an account with no open orders.
        """

        data = self._run(
            self._argv(
                "spot", "trade", "current-orders",
                query={"pageSize": 1, "needPage": "false"},
                read_only=True,
            ),
            write=False,
        )
        return {"ok": True, "rows": len(_extract_rows(data))}

    # --------------------------------------------------------------- helpers

    def _pair(self, symbol: str) -> tuple[str, str]:
        if self.symbols is not None:
            try:
                meta = self.symbols.get(symbol)
                base = getattr(meta, "base_asset", None)
                quote = getattr(meta, "quote_asset", None)
                if base and quote:
                    return str(base), str(quote)
            except Exception:  # noqa: BLE001 - fall back to a deterministic split
                pass
        upper = symbol.upper()
        for quote in ("USDT", "USDC", "TUSD", "FDUSD", "BTC", "ETH", "MX", "USD"):
            if upper.endswith(quote) and len(upper) > len(quote):
                return upper[: -len(quote)], quote
        raise MexcSymbolError(f"cannot split {symbol!r} into base/quote assets")

    @staticmethod
    def _validate_client_order_id(client_order_id: str | None) -> str | None:
        if client_order_id is None:
            return None
        if not CLIENT_ORDER_ID_RE.match(client_order_id):
            raise MexcConfigError(
                "client_order_id must be 1-32 characters of [0-9a-zA-Z_-]"
            )
        return client_order_id

    def build_place_body(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        client_order_id: str | None = None,
        take_profit_price: Decimal | str | None = None,
        stop_loss_price: Decimal | str | None = None,
    ) -> dict[str, Any]:
        """Build the exact JSON body sent to ``spot trade place``.

        Kept public so tests can assert the TP/SL payload without executing a
        subprocess.
        """

        side_u = str(side).upper()
        if side_u not in ("BUY", "SELL"):
            raise MexcConfigError("side must be BUY or SELL")
        type_key = str(order_type).lower()
        if type_key not in ("limit", "market"):
            raise MexcConfigError("only LIMIT and MARKET Spot orders are supported")
        if type_key == "market":
            if take_profit_price is not None or stop_loss_price is not None:
                raise MexcConfigError(
                    "the CLI market-order operation cannot attach TP/SL; use a limit entry "
                    "or manage the exit locally"
                )
            raise MexcConfigError("use place_market for market orders")

        base, quote = self._pair(symbol)
        price_s = _decimal_string(price, "price")
        qty_s = _decimal_string(quantity, "quantity")
        if price_s is None or qty_s is None:
            raise MexcPrecisionError("a LIMIT order requires both price and quantity")

        body: dict[str, Any] = {
            "currency": base,
            "market": quote,
            "price": price_s,
            "quantity": qty_s,
            "tradeType": side_u,
            "orderType": "LIMIT_ORDER",
            "orderSource": "CLI",
        }
        if client_order_id:
            body["clientOrderId"] = self._validate_client_order_id(client_order_id)

        price_dec = Decimal(price_s)
        if take_profit_price is not None:
            tp_s = _decimal_string(take_profit_price, "take_profit_price")
            assert tp_s is not None
            tp = Decimal(tp_s)
            if side_u == "BUY" and tp <= price_dec:
                raise MexcPrecisionError("take-profit must be above the BUY limit price")
            if side_u == "SELL" and tp >= price_dec:
                raise MexcPrecisionError("take-profit must be below the SELL limit price")
            body["profitTriggerPrice"] = tp_s
        if stop_loss_price is not None:
            sl_s = _decimal_string(stop_loss_price, "stop_loss_price")
            assert sl_s is not None
            sl = Decimal(sl_s)
            if side_u == "BUY" and sl >= price_dec:
                raise MexcPrecisionError("stop-loss must be below the BUY limit price")
            if side_u == "SELL" and sl <= price_dec:
                raise MexcPrecisionError("stop-loss must be above the SELL limit price")
            body["loseTriggerPrice"] = sl_s
        return body

    # ------------------------------------------------------------- order ops

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | str | None = None,
        quote_order_qty: Decimal | str | None = None,
        price: Decimal | str | None = None,
        client_order_id: str | None = None,
        take_profit_price: Decimal | str | None = None,
        stop_loss_price: Decimal | str | None = None,
        confirm: bool | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Place one live Spot order. Never retried, never auto-confirmed."""

        type_key = str(order_type).lower()
        if type_key == "market":
            return self.place_market(
                symbol=symbol,
                side=side,
                quantity=quantity,
                quote_order_qty=quote_order_qty,
                client_order_id=client_order_id,
                confirm=confirm,
                dry_run=dry_run,
            )
        body = self.build_place_body(
            symbol=symbol,
            side=side,
            order_type=type_key,
            quantity=quantity,
            price=price,
            client_order_id=client_order_id,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
        )
        confirmed = self.confirm_writes if confirm is None else bool(confirm)
        argv = self._argv(
            "spot", "trade", "place",
            data=body,
            # ``--read-only`` would block the write before --dry-run can print
            # the request, so a dry run uses --dry-run only.
            dry_run=dry_run,
            confirm=confirmed and not dry_run,
        )
        data = self._run(argv, write=not dry_run)
        if dry_run:
            return {"dry_run": True, "body": body}
        return self._normalize_place(data, body)

    def place_market(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal | str | None = None,
        quote_order_qty: Decimal | str | None = None,
        client_order_id: str | None = None,
        confirm: bool | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        base, quote = self._pair(symbol)
        qty_s = _decimal_string(quantity, "quantity")
        quote_s = _decimal_string(quote_order_qty, "quote_order_qty")
        if (qty_s is None) == (quote_s is None):
            raise MexcConfigError(
                "a MARKET order requires exactly one of quantity or quote_order_qty"
            )
        body: dict[str, Any] = {
            "currency": base,
            "market": quote,
            "tradeType": str(side).upper(),
            "orderType": "MARKET_ORDER",
            "orderSource": "CLI",
        }
        if qty_s is not None:
            body["quantity"] = qty_s
        else:
            body["amount"] = quote_s
        if client_order_id:
            body["clientOrderId"] = self._validate_client_order_id(client_order_id)
        confirmed = self.confirm_writes if confirm is None else bool(confirm)
        argv = self._argv(
            "spot", "trade", "place-market",
            data=body,
            dry_run=dry_run,
            confirm=confirmed and not dry_run,
        )
        data = self._run(argv, write=not dry_run)
        if dry_run:
            return {"dry_run": True, "body": body}
        return self._normalize_place(data, body)

    def place_oco(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal | str | None = None,
        price: Decimal | str | None = None,
        trigger_price: Decimal | str | None = None,
        trigger_limit_price: Decimal | str | None = None,
        client_order_id: str | None = None,
        confirm: bool | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Place a native OCO pair (limit leg + stop-limit leg) via ``spot trade place-oco``.

        One leg activates and the exchange cancels the other, so the caller gets
        a true one-cancels-the-other exit without managing two independent orders.
        For a SELL OCO the limit leg is the take-profit and the stop-limit leg is
        the stop-loss; ``triggerType`` is ``LE`` (stop activates when price <=
        trigger price).
        """

        side_u = str(side).upper()
        if side_u not in ("BUY", "SELL"):
            raise MexcConfigError("side must be BUY or SELL")
        base, quote = self._pair(symbol)
        qty_s = _decimal_string(quantity, "quantity")
        price_s = _decimal_string(price, "price")
        trigger_s = _decimal_string(trigger_price, "trigger_price")
        trigger_limit_s = _decimal_string(trigger_limit_price, "trigger_limit_price")
        if qty_s is None or price_s is None or trigger_s is None or trigger_limit_s is None:
            raise MexcPrecisionError("an OCO order requires quantity, price, trigger price and trigger limit price")

        body: dict[str, Any] = {
            "currency": base,
            "market": quote,
            "quantity": qty_s,
            "tradeType": side_u,
            "orderType": "OCO_STOP_LIMIT_ORDER",
            "orderSource": "CLI",
            "price": price_s,
            "triggerType": "LE" if side_u == "SELL" else "GE",
            "triggerPrice": trigger_s,
            "triggerLimitPrice": trigger_limit_s,
        }
        if client_order_id:
            body["clientOrderId"] = self._validate_client_order_id(client_order_id)
        confirmed = self.confirm_writes if confirm is None else bool(confirm)
        argv = self._argv(
            "spot", "trade", "place-oco",
            data=body,
            dry_run=dry_run,
            confirm=confirmed and not dry_run,
        )
        data = self._run(argv, write=not dry_run)
        if dry_run:
            return {"dry_run": True, "body": body}
        return self._normalize_oco(data, body)

    def _normalize_oco(self, data: Any, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            if data in (None, ""):
                raise MexcUnexpectedResponse(
                    "MEXC CLI OCO placement returned no order id; reconcile by client order id"
                )
            data = {"orderId": data}
        order_id = _pick(data, "orderId", "order_id", "id")
        if order_id in (None, ""):
            raise MexcUnexpectedResponse(
                "MEXC CLI OCO placement returned no order id; reconcile by client order id"
            )
        status = _normalize_status(_pick(data, "state", "status"))
        if status == "UNKNOWN":
            status = "NEW"
        return {
            "orderId": str(order_id),
            "status": status,
            "executedQty": str(_pick(data, "executedQty", "dealtQty", "filledQty") or "0"),
            "cumulativeQuoteQty": str(
                _pick(data, "cumulativeQuoteQty", "cumulativeQuote", "amount") or "0"
            ),
            "clientOrderId": body.get("clientOrderId"),
            "triggerPrice": _pick(data, "triggerPrice", "trigger_price"),
            "triggerLimitPrice": _pick(
                data, "triggerLimitPrice", "trigger_limit_price"
            ),
            "raw": data,
        }

    def _normalize_place(self, data: Any, body: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(data, dict):
            if data in (None, ""):
                raise MexcUnexpectedResponse(
                    "MEXC CLI placement returned no order id; reconcile by client order id"
                )
            data = {"orderId": data}
        order_id = _pick(data, "orderId", "order_id", "id")
        if order_id in (None, ""):
            raise MexcUnexpectedResponse(
                "MEXC CLI placement returned no order id; reconcile by client order id"
            )
        status = _normalize_status(_pick(data, "state", "status"))
        if status == "UNKNOWN":
            status = "NEW"
        return {
            "orderId": str(order_id),
            "status": status,
            "executedQty": str(_pick(data, "executedQty", "dealtQty", "filledQty") or "0"),
            "cumulativeQuoteQty": str(
                _pick(data, "cumulativeQuoteQty", "cumulativeQuote", "amount") or "0"
            ),
            "clientOrderId": body.get("clientOrderId"),
            "raw": data,
        }

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not order_id and not orig_client_order_id:
            raise MexcConfigError("query_order requires order_id or orig_client_order_id")
        base, quote = self._pair(symbol)
        listing: dict[str, Any] | None = None
        if not order_id:
            listing = self._find_by_client_id(base, quote, str(orig_client_order_id))
            order_id = str(_pick(listing, "orderId", "order_id", "id") or "")
            if not order_id:
                raise MexcOrderNotFound("MEXC CLI listing had no order id for the client id")
        if listing is None:
            listing = self._find_by_order_id(base, quote, order_id)

        # ``deal-detail`` requires the order type.  Prefer what the listing
        # reported; otherwise try both Spot order types so a filled market
        # entry that has left the listings can still be reconciled.
        order_types: list[str] = []
        if listing is not None:
            listed_type = _normalize_order_type(_pick(listing, "orderType", "order_type"))
            if listed_type is not None:
                order_types.append(listed_type)
        for candidate in ("LIMIT_ORDER", "MARKET_ORDER"):
            if candidate not in order_types:
                order_types.append(candidate)

        data: Any = None
        last_error: MexcError | None = None
        for index, order_type in enumerate(order_types):
            try:
                data = self._run(
                    self._argv(
                        "spot", "trade", "deal-detail",
                        query={"orderId": order_id, "orderType": order_type},
                        read_only=True,
                    ),
                    write=False,
                )
            except MexcError as exc:
                last_error = exc
                retryable = isinstance(exc, MexcOrderNotFound) or _looks_like_type_mismatch(exc)
                if retryable and index + 1 < len(order_types):
                    continue
                raise
            break
        if not isinstance(data, dict) or not data:
            if last_error is not None:
                raise last_error
            raise MexcOrderNotFound(f"MEXC CLI has no order detail for {order_id}")
        return self._normalize_detail(data, order_id)

    def _normalize_detail(self, data: dict[str, Any], order_id: str) -> dict[str, Any]:
        return {
            "orderId": str(_pick(data, "orderId", "order_id", "id") or order_id),
            "clientOrderId": _pick(data, "clientOrderId", "client_order_id"),
            "status": _normalize_status(_pick(data, "state", "status")),
            "executedQty": str(_pick(data, "executedQty", "dealtQty", "filledQty") or "0"),
            "cumulativeQuoteQty": str(
                _pick(data, "cumulativeQuoteQty", "cumulativeQuote", "amount") or "0"
            ),
            "price": _pick(data, "price"),
            "type": _normalize_order_type(_pick(data, "orderType", "order_type")),
            # Attached-protection path echoes profitTriggerPrice/loseTriggerPrice;
            # the native OCO path echoes triggerPrice/triggerLimitPrice.  Read
            # both so a read-only detail query can confirm either trigger.
            "profitTriggerPrice": _pick(
                data, "profitTriggerPrice", "profitTriggerprice", "triggerPrice", "trigger_price"
            ),
            "loseTriggerPrice": _pick(
                data, "loseTriggerPrice", "loseTriggerprice", "triggerLimitPrice",
                "trigger_limit_price",
            ),
            "raw": data,
        }

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not order_id and not orig_client_order_id:
            raise MexcConfigError("cancel_order requires order_id or orig_client_order_id")
        if not order_id:
            base, quote = self._pair(symbol)
            row = self._find_by_client_id(base, quote, str(orig_client_order_id))
            order_id = str(_pick(row, "orderId", "order_id", "id") or "")
            if not order_id:
                raise MexcOrderNotFound("MEXC CLI listing had no order id for the client id")
        data = self._run(
            self._argv(
                "spot", "trade", "cancel",
                query={"orderId": order_id},
                confirm=self.confirm_writes,
            ),
            write=True,
        )
        return {
            "orderId": str(order_id),
            "status": "CANCELED",
            "raw": data if isinstance(data, dict) else {},
        }

    # ---------------------------------------------------------------- reads

    def _list_orders(self, *, base: str, quote: str, open_only: bool) -> list[dict[str, Any]]:
        operation = "current-orders" if open_only else "history-orders"
        data = self._run(
            self._argv(
                "spot", "trade", operation,
                query={
                    "currency": base,
                    "market": quote,
                    "pageSize": 100,
                    "needPage": "false",
                },
                read_only=True,
            ),
            write=False,
        )
        return _extract_rows(data)

    def _find_by_client_id(
        self, base: str, quote: str, client_order_id: str
    ) -> dict[str, Any]:
        for open_only in (True, False):
            for row in self._list_orders(base=base, quote=quote, open_only=open_only):
                candidate = _pick(row, "clientOrderId", "client_order_id", "clientOrderID")
                if str(candidate or "") == client_order_id:
                    return row
        raise MexcOrderNotFound(
            f"MEXC CLI listings contain no order for client id {client_order_id}"
        )

    def _find_by_order_id(
        self, base: str, quote: str, order_id: str
    ) -> dict[str, Any] | None:
        for open_only in (True, False):
            for row in self._list_orders(base=base, quote=quote, open_only=open_only):
                candidate = _pick(row, "orderId", "order_id", "id")
                if str(candidate or "") == order_id:
                    return row
        return None

    def find_by_client_id(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        base, quote = self._pair(symbol)
        row = self._find_by_client_id(base, quote, client_order_id)
        return {
            "orderId": str(_pick(row, "orderId", "order_id", "id")),
            "status": _normalize_status(_pick(row, "state", "status")),
            "executedQty": str(_pick(row, "executedQty", "dealtQty") or "0"),
            "cumulativeQuoteQty": str(_pick(row, "cumulativeQuoteQty", "amount") or "0"),
            "raw": row,
        }

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        if symbol is None:
            base = quote = None
        else:
            base, quote = self._pair(symbol)
        data = self._run(
            self._argv(
                "spot", "trade", "current-orders",
                query={"currency": base, "market": quote, "pageSize": 100},
                read_only=True,
            ),
            write=False,
        )
        return _extract_rows(data)

    def verify_protection(self, *, symbol: str, order_id: str) -> dict[str, Any]:
        """Check whether the exchange echoes the attached TP/SL for an order.

        Only a non-empty echo is treated as verified.  An absent field means
        "unverified", never "protected".
        """

        detail = self.query_order(symbol=symbol, order_id=order_id)
        tp = detail.get("profitTriggerPrice")
        sl = detail.get("loseTriggerPrice")
        return {
            "verified": bool(tp or sl),
            "take_profit": tp,
            "stop_loss": sl,
            "detail": detail,
        }


__all__ = [
    "CliHealth",
    "DEFAULT_MIN_VERSION",
    "KNOWN_LINUX_BINARY",
    "MexcCliAdapter",
    "discover_binary",
    "parse_version",
]
