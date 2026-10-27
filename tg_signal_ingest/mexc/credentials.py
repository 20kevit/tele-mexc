"""Read-only API-key validation against ``GET /api/v3/apiKeyInfo``.

Official reference (verified 2026-10-23):
- https://www.mexc.com/api-docs/spot-v3/spot-account-trade/query-api-key-info
  (``GET``, permission SPOT_ACCOUNT_READ, weight 1; response fields
  ``note``, ``accessKey``, ``status`` VALID/DELETE/FROZEN, ``permissions``
  as a comma-separated string, ``ipWhiteList``, ``createTime``,
  ``remainingValidity``: -999 permanent, 0 expired, >0 active days)
- https://www.mexc.com/api-docs/spot-v3/introduction#api-key-setup
  (no IP whitelist => key expires after 90 days; per-key trading-pair
  restrictions configured in the MEXC web UI)

Note the POST variant of the same path configures the IP whitelist and needs
SPOT_ACCOUNT_WRITE — deliberately NOT implemented (least privilege).

This module performs NO writes and only read-only GETs; it is safe to run at
startup and in health checks without placing orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .errors import MexcConfigError, MexcPermissionError

PERMANENT_VALIDITY = "-999"
REQUIRED_SPOT_PERMISSIONS = (
    "SPOT_ACCOUNT_READ",
    "SPOT_DEAL_READ",
)
WRITE_PERMISSION = "SPOT_DEAL_WRITE"


@dataclass(frozen=True)
class ApiKeyStatus:
    """Typed view of the documented apiKeyInfo response."""

    status: str
    permissions: tuple[str, ...]
    remaining_validity: int  # -999 permanent, 0 expired, >0 active days
    ip_whitelist: tuple[str, ...]
    note: str
    raw: dict[str, Any]

    @property
    def valid(self) -> bool:
        return self.status.upper() == "VALID" and self.remaining_validity != 0

    @property
    def permanent(self) -> bool:
        return self.remaining_validity == int(PERMANENT_VALIDITY)

    def has_write(self) -> bool:
        return WRITE_PERMISSION in self.permissions


def parse_api_key_info(raw: dict[str, Any]) -> ApiKeyStatus:
    permissions_raw = str(raw.get("permissions") or "")
    permissions = tuple(p.strip() for p in permissions_raw.split(",") if p.strip())
    whitelist_raw = str(raw.get("ipWhiteList") or "")
    ip_whitelist = tuple(ip.strip() for ip in whitelist_raw.split(",") if ip.strip())
    try:
        remaining = int(str(raw.get("remainingValidity", "0")))
    except ValueError as exc:
        raise MexcConfigError("apiKeyInfo returned a non-integer remainingValidity") from exc
    return ApiKeyStatus(
        status=str(raw.get("status") or ""),
        permissions=permissions,
        remaining_validity=remaining,
        ip_whitelist=ip_whitelist,
        note=str(raw.get("note") or ""),
        raw=dict(raw),
    )


def validate_credentials(
    client: Any,
    *,
    required_permissions: tuple[str, ...] = REQUIRED_SPOT_PERMISSIONS + (WRITE_PERMISSION,),
) -> ApiKeyStatus:
    """Verify key status, expiry and permissions read-only; fail closed.

    Raises ``MexcConfigError`` for an invalid/expired key and
    ``MexcPermissionError`` when the key exists but lacks the required Spot
    permissions. Never sends a trade request and never returns a "maybe": if a
    required permission cannot be confirmed locally, it is treated as absent.
    """

    info = parse_api_key_info(client.api_key_info())
    if info.status.upper() == "DELETE":
        raise MexcConfigError("MEXC API key has been deleted; create a new key")
    if info.status.upper() == "FROZEN":
        raise MexcConfigError("MEXC API key is FROZEN; check the account on mexc.com")
    if info.status.upper() != "VALID":
        raise MexcConfigError(f"MEXC API key status is {info.status!r}, not VALID")
    if info.remaining_validity == 0:
        raise MexcConfigError("MEXC API key is expired (remainingValidity = 0); renew it")
    missing = [p for p in required_permissions if p not in info.permissions]
    if missing:
        raise MexcPermissionError(
            f"MEXC API key lacks required permissions: {', '.join(sorted(missing))}; "
            "never request withdrawal permissions"
        )
    return info
