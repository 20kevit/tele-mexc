"""Fail-closed MEXC Spot balance preflight.

Official account response (verified 2026-10-19):
``GET /api/v3/account`` with permission ``SPOT_ACCOUNT_READ`` returns
``canTrade`` and ``balances[]`` rows of ``asset``, ``free``, ``locked`` and
``available``.

The preflight is deliberately conservative:
* a fresh account response is required before every live submission;
* ``min(free, available)`` is treated as spendable when both are present;
* outstanding local non-terminal live intents on the same symbol are
  subtracted so two pending orders cannot spend the same balance;
* a configurable fee buffer covers pre-trade exchange fees; no portfolio reserve is applied.

Exchange balances can still change externally between this read and order
acceptance. That race cannot be eliminated client-side; the exchange's own
balance check remains authoritative.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any

from ..config import TradingConfig
from .store import TradingError


class BalanceError(TradingError):
    """Balance data is stale, malformed, missing or insufficient."""


def _decimal(value: Any, field_name: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise BalanceError(f"account balance field {field_name} is missing")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise BalanceError(f"account balance field {field_name} is not numeric") from exc
    if not parsed.is_finite():
        raise BalanceError(f"account balance field {field_name} is not finite")
    if parsed < 0:
        raise BalanceError(f"account balance field {field_name} is negative")
    return parsed


@dataclass(frozen=True)
class AssetBalance:
    asset: str
    free: Decimal
    available: Decimal | None = None
    locked: Decimal | None = None

    @property
    def spendable(self) -> Decimal:
        if self.available is None:
            return self.free
        return min(self.free, self.available)


@dataclass(frozen=True)
class AccountSnapshot:
    balances: dict[str, AssetBalance]
    can_trade: bool
    fetched_at: float
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    def age_s(self, now: float) -> float:
        return max(0.0, now - self.fetched_at)


def parse_account(raw: Any, *, fetched_at: float) -> AccountSnapshot:
    """Validate and normalise an official account response."""

    if not isinstance(raw, dict):
        raise BalanceError("account response is not a JSON object")
    if raw.get("canTrade") is not True:
        raise BalanceError("account response does not grant canTrade")
    permissions = raw.get("permissions")
    if isinstance(permissions, list) and permissions and "SPOT" not in {str(p).upper() for p in permissions}:
        raise BalanceError("account has no SPOT permission")
    rows = raw.get("balances")
    if not isinstance(rows, list):
        raise BalanceError("account response has no balances list")
    balances: dict[str, AssetBalance] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise BalanceError("account balances contained a malformed row")
        asset = str(row.get("asset") or "").strip().upper()
        if not asset:
            raise BalanceError("account balance row is missing its asset")
        if asset in balances:
            raise BalanceError(f"account balance row for {asset} is duplicated")
        free = _decimal(row.get("free"), f"{asset}.free")
        available = None
        if row.get("available") not in (None, ""):
            available = _decimal(row.get("available"), f"{asset}.available")
        locked = None
        if row.get("locked") not in (None, ""):
            locked = _decimal(row.get("locked"), f"{asset}.locked")
        balances[asset] = AssetBalance(asset=asset, free=free, available=available, locked=locked)
    return AccountSnapshot(balances=balances, can_trade=True, fetched_at=fetched_at, raw=dict(raw))


def validate_balance(
    snapshot: AccountSnapshot,
    *,
    cfg: TradingConfig,
    side: str,
    base_asset: str,
    quote_asset: str,
    base_quantity: Decimal | None,
    quote_amount: Decimal | None,
    outstanding_base: Decimal = Decimal("0"),
    outstanding_quote: Decimal = Decimal("0"),
    now: float,
) -> None:
    """Raise :class:`BalanceError` unless free balance covers this exact order."""

    age = snapshot.age_s(now)
    if age > cfg.balance_max_age_s:
        raise BalanceError(f"account balance snapshot is stale ({age:.1f}s > {cfg.balance_max_age_s:.1f}s)")
    fee_factor = Decimal(1) + Decimal(str(cfg.balance_fee_buffer_pct)) / Decimal(100)

    if side.lower() == "buy":
        asset = quote_asset.upper()
        if quote_amount is None or quote_amount <= 0:
            raise BalanceError("buy order has no positive quote amount to preflight")
        required = quote_amount * fee_factor
        outstanding = outstanding_quote
    elif side.lower() == "sell":
        asset = base_asset.upper()
        if base_quantity is None or base_quantity <= 0:
            raise BalanceError("sell order has no positive base quantity to preflight")
        required = base_quantity * fee_factor
        outstanding = outstanding_base
    else:
        raise BalanceError(f"unknown order side {side!r} for balance preflight")

    balance = snapshot.balances.get(asset)
    if balance is None:
        raise BalanceError(f"account balance response is missing {asset}")
    spendable = balance.spendable
    if spendable <= outstanding:
        raise BalanceError(
            f"no free {asset} left after outstanding local orders (free={spendable}, outstanding={outstanding})"
        )
    usable = spendable - outstanding
    if usable < required:
        raise BalanceError(
            f"insufficient free {asset} for this order: "
            f"free={spendable}, outstanding={outstanding}, "
            f"required={required} (fees included at {cfg.balance_fee_buffer_pct}%)"
        )
