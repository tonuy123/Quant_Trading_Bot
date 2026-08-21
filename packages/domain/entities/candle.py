"""Candle entity - Represents OHLCV candle data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from packages.domain.entities.base import Entity
from packages.domain.enums import Timeframe
from packages.domain.value_objects import EntityId, Price, Quantity, Symbol


@dataclass
class Candle(Entity):
    """Represents a OHLCV (Open, High, Low, Close, Volume) candle.

    Candles are the primary data structure for technical analysis:
    - Aggregated from ticks over a time period
    - Used for indicator calculation
    - Used for pattern recognition
    """

    symbol: Symbol
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open: Price
    high: Price
    low: Price
    close: Price
    volume: Quantity
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    quote_volume: Quantity | None = None
    trades_count: int = 0
    is_closed: bool = True
    is_final: bool = True  # False if still forming

    def _validate(self) -> None:
        """Validate candle data."""
        if self.high.value < self.low.value:
            raise ValueError(f"High ({self.high.value}) cannot be less than Low ({self.low.value})")
        if self.open.value > self.high.value:
            raise ValueError(f"Open ({self.open.value}) cannot exceed High ({self.high.value})")
        if self.open.value < self.low.value:
            raise ValueError(f"Open ({self.open.value}) cannot be less than Low ({self.low.value})")
        if self.close.value > self.high.value:
            raise ValueError(f"Close ({self.close.value}) cannot exceed High ({self.high.value})")
        if self.close.value < self.low.value:
            raise ValueError(
                f"Close ({self.close.value}) cannot be less than Low ({self.low.value})"
            )
        if self.close_time <= self.open_time:
            raise ValueError("Close time must be after open time")

    @property
    def range(self) -> Decimal:
        """High - Low range."""
        return self.high.value - self.low.value

    @property
    def body(self) -> Decimal:
        """Absolute difference between open and close."""
        return abs(self.close.value - self.open.value)

    @property
    def body_percentage(self) -> Decimal:
        """Body as percentage of range."""
        if self.range == 0:
            return Decimal("0")
        return (self.body / self.range) * Decimal("100")

    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish (close > open)."""
        return self.close.value > self.open.value

    @property
    def is_bearish(self) -> bool:
        """Check if candle is bearish (close < open)."""
        return self.close.value < self.open.value

    @property
    def is_doji(self) -> bool:
        """Check if candle is a doji (open ≈ close)."""
        return self.body < (self.range * Decimal("0.1"))

    @property
    def upper_shadow(self) -> Decimal:
        """Upper shadow length."""
        if self.is_bullish:
            return self.high.value - self.close.value
        return self.high.value - self.open.value

    @property
    def lower_shadow(self) -> Decimal:
        """Lower shadow length."""
        if self.is_bullish:
            return self.open.value - self.low.value
        return self.close.value - self.low.value

    def update_high(self, price: Price) -> None:
        """Update high if price is higher."""
        if price.value > self.high.value:
            self.high = price
            self.mark_updated()

    def update_low(self, price: Price) -> None:
        """Update low if price is lower."""
        if price.value < self.low.value:
            self.low = price
            self.mark_updated()

    def update_close(self, price: Price) -> None:
        """Update close price."""
        self.close = price
        self.mark_updated()

    def add_trade(self, price: Price, quantity: Quantity) -> None:
        """Add a trade to the candle."""
        self.trades_count += 1
        self.update_high(price)
        self.update_low(price)
        self.update_close(price)
        # Update volume
        new_volume = self.volume.value + quantity.value
        self.volume = Quantity(new_volume)
        self.mark_updated()
