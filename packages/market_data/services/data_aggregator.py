"""Market data aggregator - Aggregates ticks into candles."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING

from packages.domain.enums import Timeframe

if TYPE_CHECKING:
    from packages.market_data.contracts import CandleData, TradeData


@dataclass
class CandleBuilder:
    """Builder for aggregating candles from trades."""

    symbol: str
    timeframe: Timeframe
    open_time: datetime
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    close: Decimal | None = None
    volume: Decimal = Decimal("0")
    quote_volume: Decimal = Decimal("0")
    trades_count: int = 0

    def add_trade(self, trade: TradeData) -> None:
        """Add a trade to the candle."""
        price = trade.price
        quantity = trade.quantity

        if self.open is None:
            self.open = price
            self.high = price
            self.low = price

        self.close = price
        if self.high is not None and self.low is not None:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
        self.volume += quantity
        self.quote_volume += trade.quote_quantity
        self.trades_count += 1

    def to_candle(self, close_time: datetime) -> CandleData:
        """Build the final candle."""
        from packages.market_data.contracts import CandleData

        return CandleData(
            symbol=self.symbol,
            timeframe=self.timeframe,
            open_time=self.open_time,
            close_time=close_time,
            open=self.open or Decimal("0"),
            high=self.high or Decimal("0"),
            low=self.low or Decimal("0"),
            close=self.close or Decimal("0"),
            volume=self.volume,
            quote_volume=self.quote_volume,
            trades_count=self.trades_count,
        )

    @property
    def is_empty(self) -> bool:
        """Check if builder has any data."""
        return self.open is None


class MarketDataAggregator:
    """Aggregates tick data into higher timeframe candles.

    Takes tick/trade data and aggregates it into OHLCV candles
    for different timeframes (1m, 5m, 1h, etc.).
    """

    def __init__(self) -> None:
        """Initialize aggregator."""
        self._candle_builders: dict[tuple[str, Timeframe, datetime], CandleBuilder] = defaultdict()

    def get_timeframe_seconds(self, timeframe: Timeframe) -> int:
        """Get seconds for a timeframe."""
        return timeframe.seconds

    def get_candle_open_time(self, timestamp: datetime, timeframe: Timeframe) -> datetime:
        """Get the candle open time for a timestamp."""
        seconds = self.get_timeframe_seconds(timeframe)
        open_time = timestamp.replace(second=0, microsecond=0)
        # Align to timeframe boundaries
        minutes_since_hour = open_time.minute
        hours_since_day = open_time.hour

        if seconds <= 3600:  # Minutes
            interval = seconds // 60
            aligned_minute = (minutes_since_hour // interval) * interval
            open_time = open_time.replace(minute=aligned_minute)
        elif seconds <= 86400:  # Hours
            interval = seconds // 3600
            aligned_hour = (hours_since_day // interval) * interval
            open_time = open_time.replace(hour=aligned_hour)

        return open_time

    def get_candle_close_time(self, open_time: datetime, timeframe: Timeframe) -> datetime:
        """Get the candle close time for an open time."""
        seconds = self.get_timeframe_seconds(timeframe)
        return open_time + timedelta(seconds=seconds)

    def add_trade(self, trade: TradeData, timeframe: Timeframe) -> CandleData | None:
        """Add a trade and return completed candle if any.

        Args:
            trade: Trade data
            timeframe: Target timeframe

        Returns:
            Completed candle if candle closed, None otherwise.
        """
        open_time = self.get_candle_open_time(trade.timestamp, timeframe)
        close_time = self.get_candle_close_time(open_time, timeframe)
        key = (trade.symbol, timeframe, open_time)

        # Get or create builder
        builder = self._candle_builders[key]
        if builder.is_empty:
            builder.symbol = trade.symbol
            builder.timeframe = timeframe
            builder.open_time = open_time

        builder.add_trade(trade)

        # Check if candle should close
        if trade.timestamp >= close_time:
            # Candle completed, build it
            candle = builder.to_candle(close_time)
            del self._candle_builders[key]
            return candle

        return None

    def get_current_candle(
        self,
        symbol: str,
        timeframe: Timeframe,
        timestamp: datetime,
    ) -> CandleBuilder | None:
        """Get current (incomplete) candle builder.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            timestamp: Current timestamp

        Returns:
            Current candle builder if exists.
        """
        open_time = self.get_candle_open_time(timestamp, timeframe)
        key = (symbol, timeframe, open_time)
        return self._candle_builders.get(key)

    def close_candle(
        self,
        symbol: str,
        timeframe: Timeframe,
        timestamp: datetime,
    ) -> CandleData | None:
        """Force close a candle at current time.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            timestamp: Force close timestamp

        Returns:
            Closed candle if builder exists.
        """
        open_time = self.get_candle_open_time(timestamp, timeframe)
        close_time = self.get_candle_close_time(open_time, timeframe)
        key = (symbol, timeframe, open_time)

        builder = self._candle_builders.get(key)
        if builder and not builder.is_empty:
            candle = builder.to_candle(close_time)
            del self._candle_builders[key]
            return candle

        return None

    def reset(self) -> None:
        """Reset all builders."""
        self._candle_builders.clear()
