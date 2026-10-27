"""Read/write split for live Spot execution.

The verified REST client remains authoritative for metadata, balances and fill
accounting.  Live order *writes* are delegated to the official CLI adapter
because it is the only supported path that can attach take-profit and stop-loss
triggers to a new order.  Keeping the split explicit avoids a second,
independent read implementation and makes the one intentional duplication
(the CLI write path) auditable.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from .cli import MexcCliAdapter
from .client import MexcSpotClient


class HybridMexcClient:
    """Thin façade: reads -> REST, writes -> CLI."""

    execution_backend = "cli"
    supports_attached_protection = False

    def __init__(self, rest: MexcSpotClient, cli: MexcCliAdapter):
        self.rest = rest
        self.cli = cli
        # Engines and symbol registries reach through these attributes.
        self.symbols = rest.symbols

    # ------------------------------------------------------------- pass-through

    def __getattr__(self, name: str) -> Any:
        return getattr(self.rest, name)

    # ---------------------------------------------------------------- writes

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
        take_profit_price: Decimal | None = None,
        stop_loss_price: Decimal | None = None,
    ) -> dict[str, Any]:
        return self.cli.place_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            quote_order_qty=quote_order_qty,
            price=price,
            client_order_id=client_order_id,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
        )

    def place_oco(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Decimal | None = None,
        price: Decimal | None = None,
        trigger_price: Decimal | None = None,
        trigger_limit_price: Decimal | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return self.cli.place_oco(
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            trigger_limit_price=trigger_limit_price,
            client_order_id=client_order_id,
        )

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return self.cli.cancel_order(
            symbol=symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )

    def query_order(
        self,
        *,
        symbol: str,
        order_id: str | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        return self.cli.query_order(
            symbol=symbol,
            order_id=order_id,
            orig_client_order_id=orig_client_order_id,
        )

    def place_test_order(self, **kwargs: Any) -> bool:
        """``testonly`` keeps using the official REST /order/test endpoint."""

        return self.rest.place_test_order(**kwargs)

    def verify_protection(self, *, symbol: str, order_id: str) -> dict[str, Any]:
        return self.cli.verify_protection(symbol=symbol, order_id=order_id)

    def find_by_client_id(self, symbol: str, client_order_id: str) -> dict[str, Any]:
        return self.cli.find_by_client_id(symbol, client_order_id)


__all__ = ["HybridMexcClient"]
