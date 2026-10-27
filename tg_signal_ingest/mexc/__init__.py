"""Official MEXC Spot v3 REST client (documented endpoints only).

References (verified against the official docs on 2026-10-19):
- https://www.mexc.com/api-docs/spot-v3/introduction (signing, buckets, limits)
- https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/
- https://www.mexc.com/api-docs/spot-v3/spot-account-trade/new-order
- https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-order
- https://www.mexc.com/api-docs/spot-v3/error-code

Transport: stdlib urllib over HTTPS with TLS verification ON; injectable for
deterministic tests. No new third-party dependencies.
"""

from .cli import (
    DEFAULT_MIN_VERSION,
    KNOWN_LINUX_BINARY,
    CliHealth,
    MexcCliAdapter,
    discover_binary,
    parse_version,
)
from .client import MexcSpotClient, WeightedRateGate
from .credentials import (
    REQUIRED_SPOT_PERMISSIONS,
    WRITE_PERMISSION,
    ApiKeyStatus,
    parse_api_key_info,
    validate_credentials,
)
from .errors import (
    AccidentalOrderRiskError,
    MexcAuthError,
    MexcCliConfirmationRequired,
    MexcCliError,
    MexcCliIncompatible,
    MexcCliUnavailable,
    MexcConfigError,
    MexcDuplicateOrder,
    MexcError,
    MexcInsufficientBalance,
    MexcNetworkError,
    MexcOrderNotFound,
    MexcOrderRejected,
    MexcPermissionError,
    MexcPrecisionError,
    MexcRateLimitError,
    MexcServerError,
    MexcSymbolError,
    MexcTimestampError,
    MexcUnexpectedResponse,
    MexcWafError,
    credentials,
    not_found,
    order_rejected,
    permission,
    signature,
    symbol_invalid,
    timestamp,
    unsupported,
)
from .hybrid import HybridMexcClient
from .markets import (
    DepthValidationError,
    OrderBookSnapshot,
    parse_depth,
)
from .symbols import SymbolMeta, SymbolRegistry, SymbolValidationError

__all__ = [
    "MexcSpotClient",
    "WeightedRateGate",
    "MexcError",
    "MexcConfigError",
    "MexcAuthError",
    "MexcPermissionError",
    "MexcSymbolError",
    "MexcPrecisionError",
    "MexcInsufficientBalance",
    "MexcTimestampError",
    "MexcNetworkError",
    "MexcRateLimitError",
    "MexcServerError",
    "MexcWafError",
    "MexcUnexpectedResponse",
    "MexcOrderRejected",
    "MexcOrderNotFound",
    "AccidentalOrderRiskError",
    "MexcCliError",
    "MexcCliUnavailable",
    "MexcCliIncompatible",
    "MexcCliConfirmationRequired",
    "MexcDuplicateOrder",
    "MexcCliAdapter",
    "HybridMexcClient",
    "CliHealth",
    "DEFAULT_MIN_VERSION",
    "KNOWN_LINUX_BINARY",
    "discover_binary",
    "parse_version",
    "SymbolValidationError",
    "credentials",
    "permission",
    "signature",
    "symbol_invalid",
    "timestamp",
    "unsupported",
    "not_found",
    "order_rejected",
    "SymbolMeta",
    "SymbolRegistry",
    "ApiKeyStatus",
    "parse_api_key_info",
    "validate_credentials",
    "REQUIRED_SPOT_PERMISSIONS",
    "WRITE_PERMISSION",
    "DepthValidationError",
    "OrderBookSnapshot",
    "parse_depth",
]
