"""Pydantic schemas for market data validation."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class CandleSchema(BaseModel):
    """Schema for candle data validation."""

    symbol: str
    timeframe: str
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    trades_count: int = 0

    @field_validator("high")
    @classmethod
    def high_must_be_max(cls, v: Decimal, info: ValidationInfo) -> Decimal:
        """Ensure high is max."""
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("high must be >= low")
        return v

    @field_validator("close")
    @classmethod
    def close_must_be_valid(cls, v: Decimal, info: ValidationInfo) -> Decimal:
        """Ensure close is within high-low range."""
        if "high" in info.data and v > info.data["high"]:
            raise ValueError("close must be <= high")
        if "low" in info.data and v < info.data["low"]:
            raise ValueError("close must be >= low")
        return v


class TickerSchema(BaseModel):
    """Schema for ticker data validation."""

    symbol: str
    bid_price: Decimal = Field(gt=0)
    ask_price: Decimal = Field(gt=0)
    last_price: Decimal = Field(gt=0)
    volume_24h: Decimal = Field(ge=0)
    timestamp: datetime

    @field_validator("ask_price")
    @classmethod
    def ask_must_be_greater_than_bid(cls, v: Decimal, info: ValidationInfo) -> Decimal:
        """Ensure ask >= bid."""
        if "bid_price" in info.data and v < info.data["bid_price"]:
            raise ValueError("ask_price must be >= bid_price")
        return v


class TradeSchema(BaseModel):
    """Schema for trade data validation."""

    symbol: str
    trade_id: str
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    timestamp: datetime

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, v: Decimal) -> Decimal:
        """Ensure quantity is positive."""
        if v <= 0:
            raise ValueError("quantity must be positive")
        return v
