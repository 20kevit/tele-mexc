"""MEXC Spot symbol metadata, precision validation and caching.

Official fields (verified 2026-10-19):
``status`` (1 online / 2 paused / 3 offline), ``baseAsset``, ``quoteAsset``,
``baseAssetPrecision``, ``quoteAssetPrecision``, ``baseSizePrecision`` (minimum
order quantity), ``quoteAmountPrecision`` (minimum quote amount),
``quoteAmountPrecisionMarket``, ``maxQuoteAmount``, ``maxQuoteAmountMarket``,
``orderTypes``, ``isSpotTradingAllowed`` and ``permissions``.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_UP, Decimal, InvalidOperation
from typing import Any

from .errors import MexcSymbolError


class SymbolValidationError(MexcSymbolError):
    """Local symbol/precision validation failed before reaching the exchange."""


ONLINE_STATUSES = {"1", "ONLINE"}


def _dec(value: Any, default: Decimal | None = None) -> Decimal:
    if value is None or value == "":
        if default is not None:
            return default
        raise SymbolValidationError("missing numeric symbol constraint")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise SymbolValidationError(f"invalid numeric symbol constraint {value!r}") from exc


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class SymbolMeta:
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    base_asset_precision: int
    quote_asset_precision: int
    min_qty: Decimal
    min_quote: Decimal
    min_quote_market: Decimal
    max_quote: Decimal
    max_quote_market: Decimal
    order_types: tuple[str, ...]
    spot_allowed: bool
    permissions: tuple[str, ...]
    trade_side_type: int
    bid_multiplier_up: Decimal | None  # PERCENT_PRICE_BY_SIDE buy cap
    ask_multiplier_down: Decimal | None  # PERCENT_PRICE_BY_SIDE sell floor
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    # ------------------------------------------------------------ helpers

    @property
    def qty_step(self) -> Decimal:
        return Decimal(1).scaleb(-self.base_asset_precision)

    @property
    def price_step(self) -> Decimal:
        return Decimal(1).scaleb(-self.quote_asset_precision)

    @property
    def online(self) -> bool:
        return self.status.upper() in ONLINE_STATUSES

    @property
    def tradable(self) -> bool:
        if not self.online or not self.spot_allowed:
            return False
        return not (self.permissions and "SPOT" not in {p.upper() for p in self.permissions})

    def quantize_quantity(self, quantity: Decimal) -> Decimal:
        return quantity.quantize(self.qty_step, rounding=ROUND_DOWN)

    def quantize_price(self, price: Decimal) -> Decimal:
        return price.quantize(self.price_step, rounding=ROUND_DOWN)

    def supports(self, order_type: str, side: str | None = None) -> bool:
        if order_type.upper() not in {t.upper() for t in self.order_types}:
            return False
        # tradeSideType: 1=all, 2=buy only, 3=sell only, 4=close
        if side and self.trade_side_type in (2, 3):
            if self.trade_side_type == 2 and side.upper() != "BUY":
                return False
            if self.trade_side_type == 3 and side.upper() != "SELL":
                return False
        return True

    def validate_price_guards(
        self,
        *,
        side: str,
        price: Decimal,
        reference_price: Decimal,
    ) -> None:
        """Apply the documented PERCENT_PRICE_BY_SIDE filter, fail closed.

        Official exchangeInfo field description (verified 2026-10-23):
        buy/maker orders (`LIMIT`, `IMMEDIATE_OR_CANCEL`, `FILL_OR_KILL`)
        require ``orderPrice <= lastPrice * (1 + bidMultiplierUp)`` and sell
        orders require ``orderPrice >= lastPrice * (1 - askMultiplierDown)``.
        Only enforced when the exchange actually published the multiplier and
        the caller supplies a current reference price; otherwise a stale
        reference could wrongly reject valid orders.
        """

        if self.bid_multiplier_up is None or self.ask_multiplier_down is None:
            return
        if side.upper() == "BUY":
            ceiling = reference_price * (Decimal(1) + self.bid_multiplier_up)
            if price > ceiling:
                raise SymbolValidationError(
                    f"buy price {price} exceeds lastPrice*(1+{self.bid_multiplier_up}) "
                    f"= {ceiling} for {self.symbol}"
                )
        elif side.upper() == "SELL":
            floor = reference_price * (Decimal(1) - self.ask_multiplier_down)
            if price < floor:
                raise SymbolValidationError(
                    f"sell price {price} is below lastPrice*(1-{self.ask_multiplier_down}) "
                    f"= {floor} for {self.symbol}"
                )

    def validate_order(
        self,
        *,
        side: str,
        order_type: str,
        quantity: Decimal | None,
        quote_amount: Decimal | None,
        price: Decimal | None,
    ) -> None:
        if not self.tradable:
            raise SymbolValidationError(f"{self.symbol} is not enabled for Spot API trading")
        if not self.supports(order_type, side):
            raise SymbolValidationError(f"{self.symbol} does not support {order_type} {side}")
        kind = order_type.upper()
        if kind == "LIMIT":
            if quantity is None or price is None:
                raise SymbolValidationError("LIMIT orders require quantity and price")
            if quantity < self.min_qty:
                raise SymbolValidationError(
                    f"quantity {quantity} is below minimum {self.min_qty} for {self.symbol}"
                )
            if quote_amount is not None and quote_amount < self.min_quote:
                raise SymbolValidationError(
                    f"quote amount {quote_amount} is below minimum {self.min_quote} for {self.symbol}"
                )
            if quote_amount is not None and self.max_quote > 0 and quote_amount > self.max_quote:
                raise SymbolValidationError(
                    f"quote amount {quote_amount} exceeds maximum {self.max_quote} for {self.symbol}"
                )
        elif kind == "MARKET":
            if quantity is None and quote_amount is None:
                raise SymbolValidationError("MARKET orders require quantity or quoteOrderQty")
            min_quote = self.min_quote_market or self.min_quote
            if quote_amount is not None:
                if quote_amount < min_quote:
                    raise SymbolValidationError(
                        f"quote amount {quote_amount} is below market minimum {min_quote} for {self.symbol}"
                    )
                max_quote = self.max_quote_market or self.max_quote
                if max_quote > 0 and quote_amount > max_quote:
                    raise SymbolValidationError(
                        f"quote amount {quote_amount} exceeds market maximum {max_quote} for {self.symbol}"
                    )
            if quantity is not None and quantity < self.min_qty:
                raise SymbolValidationError(
                    f"quantity {quantity} is below minimum {self.min_qty} for {self.symbol}"
                )
        else:
            raise SymbolValidationError(f"unsupported order type {order_type!r}")


def compute_stop_loss_trigger_price(
    stop_loss_price: Decimal,
    tick_size: Decimal,
    offset_ticks: int,
) -> Decimal:
    """Stop-loss activation trigger for a SELL stop-loss on a LONG spot position.

    ``trigger = stop_loss_exit_price + offset_ticks × tick_size``, rounded UP to
    the symbol tick. Rounding up guarantees the trigger can never be rounded
    down toward the exit price (which would delay activation); a higher trigger
    activates *earlier* as the market falls, which is the protective direction.

    The configured stop-loss *exit* price (the stop-limit execution price) is
    intentionally left unchanged: only the activation trigger moves upward. For
    example, with exit price ``0.1190``, tick size ``0.0001`` and five ticks,
    the trigger is ``0.1195`` while the stop-limit order still executes at
    ``0.1190``.

    Raises :class:`SymbolValidationError` when any input is invalid so callers
    fail closed instead of guessing a tick size.
    """
    if offset_ticks < 1:
        raise SymbolValidationError("stop-loss trigger offset must be >= 1 tick")
    if (
        stop_loss_price is None
        or not stop_loss_price.is_finite()
        or stop_loss_price <= 0
    ):
        raise SymbolValidationError(
            "stop-loss exit price must be a positive finite decimal"
        )
    if tick_size is None or not tick_size.is_finite() or tick_size <= 0:
        raise SymbolValidationError("symbol tick size must be a positive finite decimal")
    raw = stop_loss_price + (Decimal(offset_ticks) * tick_size)
    try:
        return raw.quantize(tick_size, rounding=ROUND_UP)
    except InvalidOperation as exc:
        raise SymbolValidationError("cannot quantize stop-loss trigger price") from exc


class SymbolRegistry:
    """Small TTL cache over ``GET /api/v3/exchangeInfo``."""

    def __init__(
        self,
        client: Any,
        *,
        ttl_s: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.client = client
        self.ttl_s = ttl_s
        self._clock = clock
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, SymbolMeta]] = {}

    @staticmethod
    def parse_percent_price_by_side(raw: dict[str, Any]) -> tuple[Decimal | None, Decimal | None]:
        """Extract (bidMultiplierUp, askMultiplierDown) from ``filters``.

        Official sample (verified 2026-10-23):
        ``filters: [{"filterType": "PERCENT_PRICE_BY_SIDE", "bidMultiplierUp":
        "0.2", "askMultiplierDown": "0.2"}]``. Unknown filter types are
        ignored; malformed known filters fail closed (return None constraint
        removal is stricter elsewhere, so malformed known values raise).
        """

        filters = raw.get("filters")
        if not isinstance(filters, list):
            return None, None
        bid_up: Decimal | None = None
        ask_down: Decimal | None = None
        for entry in filters:
            if not isinstance(entry, dict):
                raise SymbolValidationError("exchangeInfo filters contains a non-object entry")
            if str(entry.get("filterType", "")).upper() != "PERCENT_PRICE_BY_SIDE":
                continue
            if entry.get("bidMultiplierUp") is None or entry.get("askMultiplierDown") is None:
                raise SymbolValidationError(
                    "PERCENT_PRICE_BY_SIDE filter lacks bidMultiplierUp/askMultiplierDown"
                )
            bid_up = _dec(entry.get("bidMultiplierUp"))
            ask_down = _dec(entry.get("askMultiplierDown"))
        return bid_up, ask_down

    @staticmethod
    def parse_one(raw: dict[str, Any]) -> SymbolMeta:
        symbol = str(raw.get("symbol", "")).upper()
        if not symbol:
            raise SymbolValidationError("exchangeInfo entry has no symbol")
        bid_up, ask_down = SymbolRegistry.parse_percent_price_by_side(raw)
        return SymbolMeta(
            symbol=symbol,
            status=str(raw.get("status", "")),
            base_asset=str(raw.get("baseAsset", "")),
            quote_asset=str(raw.get("quoteAsset", "")),
            base_asset_precision=_int(raw.get("baseAssetPrecision")),
            quote_asset_precision=_int(raw.get("quoteAssetPrecision")),
            min_qty=_dec(raw.get("baseSizePrecision")),
            min_quote=_dec(raw.get("quoteAmountPrecision")),
            min_quote_market=_dec(raw.get("quoteAmountPrecisionMarket")),
            max_quote=_dec(raw.get("maxQuoteAmount"), Decimal("0")),
            max_quote_market=_dec(raw.get("maxQuoteAmountMarket"), Decimal("0")),
            order_types=tuple(str(t) for t in (raw.get("orderTypes") or ())),
            spot_allowed=bool(raw.get("isSpotTradingAllowed", False)),
            permissions=tuple(str(p) for p in (raw.get("permissions") or ())),
            trade_side_type=_int(raw.get("tradeSideType"), 1),
            bid_multiplier_up=bid_up,
            ask_multiplier_down=ask_down,
            raw=dict(raw),
        )

    def _parse_response(self, response: dict[str, Any]) -> dict[str, SymbolMeta]:
        rows = response.get("symbols")
        if not isinstance(rows, list):
            raise SymbolValidationError("exchangeInfo response has no symbols list")
        parsed: dict[str, SymbolMeta] = {}
        for row in rows:
            if isinstance(row, dict):
                meta = self.parse_one(row)
                parsed[meta.symbol] = meta
        return parsed

    def _store(self, now: float, metas: dict[str, SymbolMeta]) -> None:
        for symbol, meta in metas.items():
            self._cache[symbol] = (now, meta)

    def get(self, symbol: str) -> SymbolMeta:
        wanted = symbol.strip().upper()
        if not wanted:
            raise SymbolValidationError("symbol is empty")
        now = self._clock()
        with self._lock:
            hit = self._cache.get(wanted)
            if hit is not None and now - hit[0] <= self.ttl_s:
                return hit[1]
        response = self.client.exchange_info(symbol=wanted)
        metas = self._parse_response(response)
        if wanted not in metas:
            raise SymbolValidationError(f"symbol {wanted} is not listed on MEXC Spot")
        with self._lock:
            self._store(now, metas)
        return metas[wanted]

    def load(self, symbols: list[str] | None = None) -> dict[str, SymbolMeta]:
        if symbols:
            return {s.upper(): self.get(s) for s in symbols}
        response = self.client.exchange_info()
        metas = self._parse_response(response)
        with self._lock:
            self._store(self._clock(), metas)
        return metas

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()
