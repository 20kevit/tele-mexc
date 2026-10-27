"""MEXC Spot v3 REST client — documented endpoints only, injectable transport.

Reference (verified against the official docs on 2026-10-19):
- https://www.mexc.com/api-docs/spot-v3/introduction
- https://www.mexc.com/api-docs/spot-v3/spot-account-trade/new-order
- https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-order

Transport: stdlib urllib in a worker thread (async-friendly, zero dependencies).
TLS verification is always ON. Signed requests never log parameters.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

try:  # POSIX file locking for a limiter shared by serve/CLI/control processes
    import fcntl
except ImportError:  # pragma: no cover - Linux is the supported deployment
    fcntl = None  # type: ignore[assignment]

from .errors import (
    _BALANCE_CODES,
    _CRED_CODES,
    _PERM_CODES,
    _PRECISION_CODES,
    _SYMBOL_CODES,
    _TIMESTAMP_CODES,
    MexcConfigError,
    MexcNetworkError,
    MexcRateLimitError,
    MexcServerError,
    MexcUnexpectedResponse,
    MexcWafError,
    credentials,
    insufficient_balance,
    not_found,
    order_rejected,
    permission,
    precision,
    signature,
    symbol_invalid,
    timestamp,
)
from .symbols import SymbolRegistry

log = logging.getLogger("ingest.mexc")

USER_AGENT = "tg-signal-ingest/0.1 (spot-client)"
ORDER_PLACEMENT_RPS = 12.0  # official shared place+cancel UID bucket: 12 requests/s
DEFAULT_WAF_BACKOFF_S = 60.0  # no Retry-After header: stop instead of escalating a ban
MAX_RECV_WINDOW_MS = 60000  # Introduction/docs: recvWindow max is 60000 ms

# Official Spot v3 weights (verified against mexc.com/api-docs on 2026-10-27).
# Unknown paths deliberately use the heaviest documented market-data weight so
# a future endpoint cannot silently consume a 1-weight allowance.
DEFAULT_ENDPOINT_WEIGHT = 25
ENDPOINT_RULES: dict[tuple[str, str], tuple[str, int]] = {
    ("GET", "/api/v3/ping"): ("ip", 1),
    ("GET", "/api/v3/time"): ("ip", 1),
    ("GET", "/api/v3/exchangeInfo"): ("ip", 25),
    ("GET", "/api/v3/ticker/price"): ("ip", 10),
    ("GET", "/api/v3/depth"): ("ip", 3),
    ("GET", "/api/v3/order"): ("ip", 2),
    ("GET", "/api/v3/openOrders"): ("ip", 3),
    ("GET", "/api/v3/myTrades"): ("ip", 10),
    ("GET", "/api/v3/account"): ("ip", 10),
    ("GET", "/api/v3/apiKeyInfo"): ("ip", 1),
    ("POST", "/api/v3/order/test"): ("ip", 1),
    # POST/DELETE /api/v3/order share the documented 12 requests/second UID
    # bucket. The request methods pass bucket="order" explicitly.
}

Transport = Callable[[str, str, dict[str, str], bytes | None, float], tuple[int, dict[str, str], bytes]]


class WeightedRateGate:
    """Thread-safe sliding-window weight limiter.

    Used for the documented order bucket (12 requests/second) and for the
    conservative general bucket. A 429/418 response is never retried by the
    client; callers must honour ``Retry-After`` before issuing more requests.
    """

    def __init__(
        self,
        max_weight: int,
        window_s: float = 10.0,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if max_weight < 1 or window_s <= 0:
            raise ValueError("max_weight and window_s must be positive")
        self.max_weight = max_weight
        self.window_s = window_s
        self._clock = clock
        self._sleep = sleep
        self._events: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_s
        while self._events and self._events[0][0] <= cutoff:
            self._events.popleft()

    def acquire(self, weight: int = 1) -> None:
        if weight < 1:
            raise ValueError("weight must be >= 1")
        if weight > self.max_weight:
            raise ValueError("request weight exceeds the configured bucket capacity")
        with self._lock:
            while True:
                now = self._clock()
                self._prune(now)
                used = sum(w for _ts, w in self._events)
                if used + weight <= self.max_weight:
                    self._events.append((now, weight))
                    return
                oldest = self._events[0][0]
                self._sleep(max(0.01, (oldest + self.window_s) - now))


class SharedWeightedRateGate(WeightedRateGate):
    """Weighted sliding window shared by every local process.

    ``serve``, ``trading reconcile`` and the control bot can all issue MEXC
    requests from the same IP/UID.  The ledger is protected by an OS advisory
    lock, so two independently started clients observe one budget.  State is
    only timestamps and integer weights; no credentials or request data are
    written.
    """

    def __init__(
        self,
        max_weight: int,
        window_s: float,
        *,
        state_path: str | Path,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ):
        super().__init__(max_weight, window_s, clock=clock, sleep=sleep)
        self.state_path = Path(state_path)
        self.lock_path = self.state_path.with_suffix(self.state_path.suffix + ".lock")
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self._local_fallback: WeightedRateGate | None = None
        if fcntl is None:  # pragma: no cover - Linux deployment
            self._local_fallback = WeightedRateGate(
                max_weight, window_s, clock=clock, sleep=sleep
            )

    def _read_events(self) -> deque[tuple[float, int]]:
        try:
            raw = self.state_path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw.strip() else {}
            events = data.get("events", []) if isinstance(data, dict) else []
            out: deque[tuple[float, int]] = deque()
            for item in events:
                if not isinstance(item, list) or len(item) != 2:
                    continue
                out.append((float(item[0]), int(item[1])))
            return out
        except (OSError, ValueError, TypeError):
            return deque()

    def _write_events(self, events: deque[tuple[float, int]]) -> None:
        tmp = self.state_path.with_suffix(self.state_path.suffix + f".tmp.{os.getpid()}")
        tmp.write_text(
            json.dumps({"events": [[ts, weight] for ts, weight in events]}),
            encoding="utf-8",
        )
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, self.state_path)

    def acquire(self, weight: int = 1) -> None:
        if self._local_fallback is not None:  # pragma: no cover - non-POSIX
            self._local_fallback.acquire(weight)
            return
        if weight < 1:
            raise ValueError("weight must be >= 1")
        if weight > self.max_weight:
            raise ValueError("request weight exceeds the configured bucket capacity")
        while True:
            with self.lock_path.open("a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    now = self._clock()
                    events = self._read_events()
                    self._prune_events(now, events)
                    used = sum(w for _ts, w in events)
                    if used + weight <= self.max_weight:
                        events.append((now, weight))
                        self._write_events(events)
                        return
                    wait = max(0.01, (events[0][0] + self.window_s) - now)
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
            self._sleep(wait)

    def _prune_events(self, now: float, events: deque[tuple[float, int]]) -> None:
        cutoff = now - self.window_s
        while events and events[0][0] <= cutoff:
            events.popleft()


class MexcSpotClient:
    """Thin typed client. All calls are sync; async wrappers use to_thread.

    Signature (official): HMAC-SHA256(secret, totalParams) where
    totalParams = queryString + requestBody concatenated WITHOUT separator;
    signature itself excluded; lowercase hex; timestamp in milliseconds.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = "https://api.mexc.com",
        recv_window_ms: int = 5000,
        connect_timeout_s: float = 5.0,
        request_timeout_s: float = 10.0,
        transport: Transport | None = None,
        clock: Callable[[], int] | None = None,
        order_gate: WeightedRateGate | None = None,
        general_gate: WeightedRateGate | None = None,
        uid_gate: WeightedRateGate | None = None,
        symbol_registry: SymbolRegistry | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = recv_window_ms
        self.connect_timeout_s = connect_timeout_s
        self.request_timeout_s = request_timeout_s
        if recv_window_ms > MAX_RECV_WINDOW_MS:
            # Official docs: recvWindow "Max 60000" (POST /api/v3/order param
            # table); error 700005 rejects larger values. Reject locally so a
            # misconfiguration fails before the exchange ever sees it.
            raise MexcConfigError(
                f"recvWindow larger than the documented maximum {MAX_RECV_WINDOW_MS} ms"
            )
        self._transport = transport or self._default_transport
        self._clock = clock or (lambda: int(time.time() * 1000))
        self.order_gate = order_gate or WeightedRateGate(int(ORDER_PLACEMENT_RPS), 1.0)
        self.general_gate = general_gate or WeightedRateGate(60, 10.0)
        self.uid_gate = uid_gate or WeightedRateGate(100, 10.0)
        self.symbols = symbol_registry or SymbolRegistry(self)
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._blocked_until = 0.0
        self._block_lock = threading.Lock()

    # ------------------------------------------------------------------ core

    def _default_transport(
        self, url: str, method: str, headers: dict[str, str], body: bytes | None, timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        # Explicit empty proxy map: the exchange path never inherits ambient
        # proxy environment values.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers or {}), exc.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise MexcNetworkError(
                f"network failure contacting MEXC ({exc.__class__.__name__})"
            ) from exc

    @staticmethod
    def _retry_after(headers: dict[str, str]) -> float | None:
        raw = next(
            (v for k, v in headers.items() if str(k).lower() == "retry-after"),
            None,
        )
        if raw is None:
            return None
        try:
            value = float(str(raw).strip())
        except ValueError:
            return None
        return value if value >= 0 else None

    def _block_requests(self, seconds: float) -> None:
        """Honor Retry-After globally; subsequent calls wait before sending."""

        if seconds <= 0:
            return
        with self._block_lock:
            self._blocked_until = max(self._blocked_until, self._monotonic() + seconds)

    def _await_backoff(self) -> None:
        while True:
            with self._block_lock:
                wait = self._blocked_until - self._monotonic()
            if wait <= 0:
                return
            self._sleep(wait)

    def _classify_and_raise(self, status: int, text: str, headers: dict[str, str]) -> None:
        """Map HTTP status + body to typed errors. Never includes the URL
        (which could contain signed parameters) in the message."""

        code: int | None = None
        msg = text[:200]
        retry_after = self._retry_after(headers)
        if status in (429, 418, 403):
            # Classify rate-limit/WAF by HTTP status even if the body is
            # truncated or not JSON; Retry-After must never be lost.
            if status == 429:
                raise MexcRateLimitError(
                    f"rate limited (429): {msg}", code=code, http_status=status,
                    retry_after_s=retry_after,
                )
            if status == 418:
                raise MexcWafError(
                    f"IP auto-banned (418): {msg}", code=code, http_status=status,
                    retry_after_s=retry_after,
                )
            raise MexcWafError(f"WAF access denied (403): {msg}", code=code, http_status=status)

        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                raw_code = parsed.get("code")
                code = raw_code if isinstance(raw_code, int) else code
                msg = str(parsed.get("msg") or parsed.get("message") or msg)[:200]
        except (ValueError, TypeError):
            raise MexcUnexpectedResponse(f"non-JSON response (HTTP {status})") from None

        if code in _CRED_CODES:
            raise credentials(f"invalid API credentials ({code}): {msg}", code)
        if code in _TIMESTAMP_CODES:
            raise timestamp(f"timestamp/clock rejected ({code}): {msg}", code)
        if code in _PERM_CODES or status == 401:
            raise permission(f"insufficient permissions ({code}): {msg}", code)
        if code in _SYMBOL_CODES:
            raise symbol_invalid(f"symbol rejected ({code}): {msg}", code)
        if code in _PRECISION_CODES:
            raise precision(f"amount/precision rejected ({code}): {msg}", code)
        if code in _BALANCE_CODES:
            raise insufficient_balance(f"insufficient balance ({code}): {msg}", code)
        if code == -2011:
            raise not_found(f"order not found / terminal ({code}): {msg}", code)
        if code in (700002, 602):
            raise signature(f"signature rejected ({code}): {msg}", code)
        if 400 <= status < 500:
            raise order_rejected(f"request rejected (HTTP {status}, code {code}): {msg}", code)
        if status >= 500:
            raise MexcServerError(f"MEXC server error (HTTP {status}): {msg}", code=code, http_status=status)
        raise MexcServerError(
            f"unclassified MEXC failure (HTTP {status}): {msg}", code=code, http_status=status
        )

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        bucket: str = "general",  # "general" | "order"
        timeout_override: float | None = None,
    ) -> Any:
        """Execute one REST call; returns parsed JSON. Sync; run via to_thread."""

        method = method.upper()
        out_params = {k: v for k, v in dict(params or {}).items() if v is not None}

        if signed:
            self._require_credentials()
            out_params.setdefault("recvWindow", self.recv_window_ms)
            out_params["timestamp"] = self._clock()

        if method in ("GET", "DELETE"):
            if method == "DELETE" and bucket != "order":
                bucket = "order"
            query = _encoded_pairs(out_params)
            if signed:
                sig = self._sign(query)
                query = f"{query}&signature={sig}" if query else f"signature={sig}"
            url = f"{self.base_url}{path}" + (f"?{query}" if query else "")
            body: bytes | None = None
        else:
            body_str = _encoded_pairs(out_params)
            if signed:
                sig = self._sign(body_str)
                body_str = f"{body_str}&signature={sig}" if body_str else f"signature={sig}"
            body = body_str.encode("ascii")
            url = f"{self.base_url}{path}"
            query = body_str

        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if signed:
            headers["X-MEXC-APIKEY"] = self.api_key or ""
        if body is not None:
            # Official docs: raw key=value&... body, not a JSON object.
            headers["Content-Type"] = "application/json"

        self._await_backoff()
        if bucket == "order":
            # Official POST/DELETE order bucket: 12 requests/second per UID.
            self.order_gate.acquire(1)
        else:
            limit_kind, weight = ENDPOINT_RULES.get(
                (method, path), ("ip", DEFAULT_ENDPOINT_WEIGHT)
            )
            try:
                if limit_kind == "uid":
                    self.uid_gate.acquire(weight)
                else:
                    self.general_gate.acquire(weight)
                    if signed:
                        # Signed endpoints are also account-scoped; tracking the
                        # separate UID budget is conservative against the docs'
                        # per-endpoint IP/UID wording change.
                        self.uid_gate.acquire(1)
            except ValueError as exc:
                raise MexcConfigError(
                    f"configured rate budget is too small for {method} {path} "
                    f"(weight {weight}): {exc}"
                ) from exc

        started = time.monotonic()
        timeout = (timeout_override or self.request_timeout_s) + self.connect_timeout_s
        status, response_headers, payload = self._transport(url, method, headers, body, timeout)
        text = payload.decode("utf-8", errors="replace")
        content_type = response_headers.get("Content-Type", "")
        log.debug("mexc %s %s -> %s in %.2fs", method, path, status, time.monotonic() - started)
        del query  # never log signed parameter strings

        if status != 200:
            if status in (403, 418, 429):
                retry_after = self._retry_after(response_headers)
                self._block_requests(
                    retry_after if retry_after is not None else DEFAULT_WAF_BACKOFF_S
                )
            self._classify_and_raise(status, text, response_headers)
        if text and "json" not in content_type.lower() and not text.lstrip().startswith(("{", "[")):
            raise MexcUnexpectedResponse(f"unexpected content-type {content_type!r}")
        try:
            return json.loads(text) if text else {}
        except ValueError as exc:
            raise MexcUnexpectedResponse("malformed JSON from MEXC") from exc

    def _sign(self, total_params: str) -> str:
        digest = hmac.new(
            self.api_secret.encode("utf-8"), total_params.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        return digest.lower()

    def _require_credentials(self) -> None:
        if not self.api_key or not self.api_secret:
            raise MexcConfigError(
                "MEXC API key/secret missing; set MEXC_API_KEY/MEXC_API_SECRET in the private .env"
            )

    # ------------------------------------------------------------- endpoints

    def ping(self) -> bool:
        self.request("GET", "/api/v3/ping")
        return True

    def server_time_ms(self) -> int:
        return int(self.request("GET", "/api/v3/time")["serverTime"])

    def clock_skew_ms(self) -> int:
        return int(self._clock() - self.server_time_ms())

    def exchange_info(self, symbol: str | None = None, symbols: list[str] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        if symbols:
            params["symbols"] = ",".join(symbols)
        return self.request("GET", "/api/v3/exchangeInfo", params=params)

    def ticker_price(self, symbol: str | None = None) -> dict[str, str] | list[dict[str, str]]:
        if symbol:
            out = self.request("GET", "/api/v3/ticker/price", params={"symbol": symbol})
            return {"symbol": out["symbol"], "price": str(out["price"])}
        out = self.request("GET", "/api/v3/ticker/price")
        return [{"symbol": o["symbol"], "price": str(o["price"])} for o in out]

    def account(self) -> dict[str, Any]:
        return self.request("GET", "/api/v3/account", signed=True)

    def api_key_info(self) -> dict[str, Any]:
        return self.request(
            "GET", "/api/v3/apiKeyInfo", params={"accessKey": self.api_key}, signed=True
        )

    def open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol} if symbol else {}
        return self.request("GET", "/api/v3/openOrders", params=params, signed=True)

    def place_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_qty: Decimal | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        self._require_credentials()
        p: dict[str, Any] = {"symbol": symbol, "side": side, "type": order_type}
        if quantity is not None:
            p["quantity"] = _fmt(quantity)
        if quote_order_qty is not None:
            p["quoteOrderQty"] = _fmt(quote_order_qty)
        if price is not None:
            p["price"] = _fmt(price)
        if client_order_id:
            p["newClientOrderId"] = client_order_id
        return self.request("POST", "/api/v3/order", params=p, signed=True, bucket="order")

    def place_test_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Decimal | None = None,
        quote_order_qty: Decimal | None = None,
        price: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        """``POST /api/v3/order/test`` validates without reaching the matching engine."""

        self._require_credentials()
        p: dict[str, Any] = {"symbol": symbol, "side": side, "type": order_type}
        if quantity is not None:
            p["quantity"] = _fmt(quantity)
        if quote_order_qty is not None:
            p["quoteOrderQty"] = _fmt(quote_order_qty)
        if price is not None:
            p["price"] = _fmt(price)
        if client_order_id:
            p["newClientOrderId"] = client_order_id
        # Official /order/test is Weight(IP): 1, not the live order bucket.
        self.request("POST", "/api/v3/order/test", params=p, signed=True)
        return True

    def query_order(
        self, *, symbol: str, order_id: str | None = None, orig_client_order_id: str | None = None
    ) -> dict[str, Any]:
        p: dict[str, Any] = {"symbol": symbol}
        if order_id:
            p["orderId"] = order_id
        elif orig_client_order_id:
            p["origClientOrderId"] = orig_client_order_id
        else:
            raise MexcConfigError("query_order requires orderId or origClientOrderId")
        return self.request("GET", "/api/v3/order", params=p, signed=True)

    def cancel_order(
        self, *, symbol: str, order_id: str | None = None, orig_client_order_id: str | None = None
    ) -> dict[str, Any]:
        p: dict[str, Any] = {"symbol": symbol}
        if order_id:
            p["orderId"] = order_id
        elif orig_client_order_id:
            p["origClientOrderId"] = orig_client_order_id
        else:
            raise MexcConfigError("cancel_order requires orderId or origClientOrderId")
        return self.request("DELETE", "/api/v3/order", params=p, signed=True, bucket="order")

    def cancel_all_orders(self, *, symbol: str) -> list[dict[str, Any]]:
        """Cancel every open order (incl. OCO) for ONE symbol.

        Official: ``DELETE /api/v3/openOrders`` (2026-05-22 change log splits
        this from the account-wide endpoint; ``symbol`` is mandatory here).
        """

        p: dict[str, Any] = {"symbol": symbol}
        return self.request("DELETE", "/api/v3/openOrders", params=p, signed=True, bucket="order")

    def cancel_all_account_orders(self) -> Any:
        """Cancel ALL open orders across every symbol.

        Official: ``DELETE /api/v3/order/all`` — added 2026-05-22, parameters
        only ``recvWindow``/``timestamp``. Destructive; used only as a kill
        switch, never during normal signal processing.
        """

        return self.request("DELETE", "/api/v3/order/all", signed=True, bucket="order")

    def depth(self, *, symbol: str, limit: int = 100) -> dict[str, Any]:
        """Order book snapshot, default 100 levels (max 5000). Weight 3.

        Official: GET /api/v3/depth (Market data endpoints / Order Book).
        """

        if not 1 <= limit <= 5000:
            raise MexcConfigError("depth limit must be between 1 and 5000")
        return self.request(
            "GET", "/api/v3/depth", params={"symbol": symbol, "limit": limit}
        )

    def my_trades(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1000:
            raise MexcConfigError("my_trades limit must be between 1 and 1000")
        p: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if order_id:
            p["orderId"] = order_id
        if start_time_ms is not None:
            p["startTime"] = start_time_ms
        if end_time_ms is not None:
            p["endTime"] = end_time_ms
        return self.request("GET", "/api/v3/myTrades", params=p, signed=True)

    async def run(self, method: str, path: str, **kw: Any) -> Any:
        return await asyncio.to_thread(self.request, method, path, **kw)

    async def run_async(self, fn: Callable[..., Any], *args: Any, **kw: Any) -> Any:
        return await asyncio.to_thread(fn, *args, **kw)


def _param_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _encoded_pairs(params: dict[str, Any]) -> str:
    """URL-encode sorted parameters exactly once; signing uses this same string."""

    return "&".join(
        f"{urllib.parse.quote(str(k), safe='')}={urllib.parse.quote(_param_value(params[k]), safe='')}"
        for k in sorted(params)
    )


def _fmt(d: Decimal) -> str:
    """Format a Decimal losslessly (no scientific notation)."""

    return format(d, "f")
