"""Price data validation."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packages.market_data.contracts import TickerData, TradeData


class PriceValidator:
    """Validates price data for sanity and consistency."""

    MAX_PRICE_CHANGE_PERCENT = Decimal("50")  # 50% max change in a tick
    MIN_SPREAD_PERCENT = Decimal("0.0001")  # Minimum 0.01% spread
    MAX_SPREAD_PERCENT = Decimal("5")  # Maximum 5% spread

    @classmethod
    def validate_ticker(cls, ticker: TickerData) -> list[str]:
        """Validate ticker data.

        Args:
            ticker: Ticker data to validate

        Returns:
            List of validation errors (empty if valid).
        """
        errors = []

        if ticker.bid_price <= 0:
            errors.append(f"Invalid bid price: {ticker.bid_price}")

        if ticker.ask_price <= 0:
            errors.append(f"Invalid ask price: {ticker.ask_price}")

        if ticker.ask_price < ticker.bid_price:
            errors.append(f"Ask ({ticker.ask_price}) < Bid ({ticker.bid_price})")

        if ticker.last_price <= 0:
            errors.append(f"Invalid last price: {ticker.last_price}")

        # Check spread
        if ticker.last_price > 0:
            spread_pct = (ticker.spread / ticker.last_price) * 100
            if spread_pct > cls.MAX_SPREAD_PERCENT:
                errors.append(f"Spread too large: {spread_pct}%")

        # Check volume
        if ticker.volume_24h < 0:
            errors.append(f"Negative volume: {ticker.volume_24h}")

        return errors

    @classmethod
    def validate_trade(cls, trade: TradeData) -> list[str]:
        """Validate trade data.

        Args:
            trade: Trade data to validate

        Returns:
            List of validation errors.
        """
        errors = []

        if trade.price <= 0:
            errors.append(f"Invalid price: {trade.price}")

        if trade.quantity <= 0:
            errors.append(f"Invalid quantity: {trade.quantity}")

        if trade.quote_quantity <= 0:
            errors.append(f"Invalid quote quantity: {trade.quote_quantity}")

        return errors

    @classmethod
    def is_price_reasonable(cls, price: Decimal, reference_price: Decimal) -> bool:
        """Check if price is reasonable compared to reference.

        Args:
            price: Price to check
            reference_price: Reference price

        Returns:
            True if price is within reasonable bounds.
        """
        if reference_price <= 0:
            return True

        change_pct = abs(price - reference_price) / reference_price * 100
        return change_pct <= cls.MAX_PRICE_CHANGE_PERCENT

    @classmethod
    def calculate_slippage(
        cls,
        order_price: Decimal,
        fill_price: Decimal,
        side: str,
    ) -> Decimal:
        """Calculate slippage for a fill.

        Args:
            order_price: Expected order price
            fill_price: Actual fill price
            side: BUY or SELL

        Returns:
            Slippage as percentage.
        """
        if order_price <= 0:
            return Decimal("0")

        if side == "BUY":
            # Higher fill price = more slippage
            slippage = (fill_price - order_price) / order_price * 100
        else:
            # Lower fill price = more slippage
            slippage = (order_price - fill_price) / order_price * 100

        return max(Decimal("0"), slippage)
