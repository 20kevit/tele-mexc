"""Typed errors for the MEXC Spot client.

Classification follows the official error-code documentation:
- HTTP 4XX family with documented codes (signature, timestamp, perms, symbol...)
- HTTP 403 -> WAF, HTTP 429/418 -> rate limit (Retry-After honoured)
- HTTP 5XX -> server errors; outcome may still have succeeded (for trades)
"""

from __future__ import annotations


class MexcError(Exception):
    """Base class: message is safe to log (never contains secrets)."""

    def __init__(self, message: str, *, http_status: int | None = None, code: int | None = None):
        super().__init__(message)
        self.http_status = http_status
        self.code = code


class MexcConfigError(MexcError):
    """Missing/invalid client configuration or credentials."""


class MexcAuthError(MexcError):
    """Invalid key/secret or bad signature parameters."""


class MexcPermissionError(MexcError):
    """Key lacks the required permission (700007, 401 No authority...)."""


class MexcSymbolError(MexcError):
    """Symbol invalid / not tradable / suspended (10007, 30014, 30016...)."""


class MexcPrecisionError(MexcError):
    """Quantity/price precision or min/max violation (10096, 10097, 30002...)."""


class MexcInsufficientBalance(MexcError):
    """Insufficient balance / position (10101, 30004, 30005)."""


class MexcRateLimitError(MexcError):
    """429 (with Retry-After seconds) or repeated 403 escalation."""

    def __init__(self, message: str, *, retry_after_s: float | None = None, **kw):
        super().__init__(message, **kw)
        self.retry_after_s = retry_after_s


class MexcWafError(MexcError):
    """HTTP 403 WAF / 418 auto-ban; stop and back off, never fight it."""

    def __init__(self, message: str, *, retry_after_s: float | None = None, **kw):
        super().__init__(message, **kw)
        self.retry_after_s = retry_after_s


class MexcTimestampError(MexcError):
    """Timestamp outside recvWindow (700003/10073); needs clock check."""


class MexcNetworkError(MexcError):
    """Timeout, DNS, connection reset — request MAY have been sent."""


class MexcServerError(MexcError):
    """HTTP 5xx — outcome ambiguous; for orders, reconcile before retrying."""


class MexcUnexpectedResponse(MexcError):
    """Malformed JSON / unexpected response shape."""


class MexcOrderRejected(MexcError):
    """Exchange-level order rejection (terminal, not retryable)."""


class MexcOrderNotFound(MexcError):
    """-2011 Unknown order sent — cancel/query target missing/terminal."""


class AccidentalOrderRiskError(MexcError):
    """Internal guard: a code path attempted a live order without authorization."""


class MexcCliError(MexcConfigError):
    """Local MEXC CLI problem (missing binary, bad profile/version).

    These failures happen before an order can reach the exchange and are
    therefore terminal/safe to report.
    """


class MexcCliUnavailable(MexcCliError):
    """The configured ``mexc`` binary is missing or not executable."""


class MexcCliIncompatible(MexcCliError):
    """The installed CLI version/schema does not expose the required operations."""


class MexcCliConfirmationRequired(MexcOrderRejected):
    """The CLI refused a write because non-interactive confirmation is disabled.

    No request reached the exchange; re-running is safe only after an explicit
    operator decision.
    """


class MexcDuplicateOrder(MexcOrderRejected):
    """The CLI reported a duplicate ``clientOrderId``.

    The previous placement reached the exchange, so the outcome is ambiguous
    from the caller's point of view and must be reconciled, never retried under
    a new id.
    """

    def __init__(self, message: str, *, client_order_id: str | None = None, **kw):
        super().__init__(message, **kw)
        self.client_order_id = client_order_id


# convenience classifiers -------------------------------------------------------------
# Codes verified against https://www.mexc.com/api-docs/spot-v3/error-code on
# 2026-10-23. Unlisted HTTP 4xx codes intentionally fall back to a terminal
# MexcOrderRejected (never retried, safe default).

_CRED_CODES = {400, 700001, 10072}
_PERM_CODES = {700007, 700011, 70011, 60005, 730100, 401}
_TIMESTAMP_CODES = {700003, 10073}
_SYMBOL_CODES = {
    10007, 30014, 30016, 30000, 30020, 30021, 730001,
    # 30001 side not allowed, 30018/30019 market orders disabled for symbol
    30001, 30018, 30019,
}
_PRECISION_CODES = {10096, 10097, 10095, 10102, 30002, 30003}
_BALANCE_CODES = {10101, 30004, 30005}


def credentials(msg: str, code: int | None = None) -> MexcAuthError:
    return MexcAuthError(msg, code=code)


def permission(msg: str, code: int | None = None) -> MexcPermissionError:
    return MexcPermissionError(msg, code=code)


def signature(msg: str, code: int | None = None) -> MexcAuthError:
    return MexcAuthError(msg, code=code)


def timestamp(msg: str, code: int | None = None) -> MexcTimestampError:
    return MexcTimestampError(msg, code=code)


def symbol_invalid(msg: str, code: int | None = None) -> MexcSymbolError:
    return MexcSymbolError(msg, code=code)


def precision(msg: str, code: int | None = None) -> MexcPrecisionError:
    return MexcPrecisionError(msg, code=code)


def insufficient_balance(msg: str, code: int | None = None) -> MexcInsufficientBalance:
    return MexcInsufficientBalance(msg, code=code)


def not_found(msg: str, code: int | None = -2011) -> MexcOrderNotFound:
    return MexcOrderNotFound(msg, code=code)


def order_rejected(msg: str, code: int | None = None) -> MexcOrderRejected:
    return MexcOrderRejected(msg, code=code)


def unsupported(msg: str) -> MexcOrderRejected:
    return MexcOrderRejected(msg, code=None)
