"""Market data normalizer - Normalizes data from different exchanges."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    pass


class MarketDataNormalizer:
    """Normalizes market data from different exchanges to a common format.

    Each exchange has its own data format and conventions.
    This class normalizes them to a standard format.
    """

    # Symbol mapping: exchange -> standard format
    SYMBOL_FORMATS: ClassVar[dict[str, dict[str, object]]] = {
        "binance": {
            "separator": "",
            "quote_first": False,
        },
        "okx": {
            "separator": "-",
            "quote_first": False,
        },
        "coinbase": {
            "separator": "-",
            "quote_first": False,
        },
    }

    @staticmethod
    def normalize_symbol(symbol: str, exchange: str) -> str:
        """Normalize symbol to standard format (BASEQUOTE).

        Args:
            symbol: Exchange-specific symbol
            exchange: Exchange name

        Returns:
            Normalized symbol.
        """
        # Remove separators and uppercase
        normalized = symbol.upper().replace("-", "").replace("/", "")

        # Extract base and quote
        quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB", "USD", "EUR"]
        for quote in quotes:
            if normalized.endswith(quote) and len(normalized) > len(quote):
                base = normalized[: -len(quote)]
                return f"{base}{quote}"

        return normalized

    @staticmethod
    def denormalize_symbol(symbol: str, exchange: str) -> str:
        """Convert normalized symbol to exchange format.

        Args:
            symbol: Normalized symbol
            exchange: Exchange name

        Returns:
            Exchange-specific symbol.
        """
        separator = MarketDataNormalizer.SYMBOL_FORMATS.get(exchange, {}).get("separator", "-")
        base, quote = MarketDataNormalizer._split_symbol(symbol)
        return f"{base}{separator}{quote}"

    @staticmethod
    def _split_symbol(symbol: str) -> tuple[str, str]:
        """Split symbol into base and quote.

        Args:
            symbol: Normalized symbol

        Returns:
            (base, quote) tuple.
        """
        quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB", "USD", "EUR"]
        for quote in quotes:
            if symbol.endswith(quote) and len(symbol) > len(quote):
                base = symbol[: -len(quote)]
                return base, quote

        # Fallback: assume last 4 chars is quote
        return symbol[:-4], symbol[-4:]

    @staticmethod
    def normalize_timestamp(timestamp: int | datetime, exchange: str) -> datetime:
        """Normalize timestamp to datetime.

        Most exchanges return milliseconds, some return seconds.

        Args:
            timestamp: Timestamp in ms or datetime
            exchange: Exchange name

        Returns:
            Normalized datetime.
        """
        if isinstance(timestamp, datetime):
            return timestamp

        # Detect if milliseconds
        if timestamp > 10**12:
            return datetime.utcfromtimestamp(timestamp / 1000)

        return datetime.utcfromtimestamp(timestamp)

    @staticmethod
    def normalize_price(price: float | Decimal | str, decimals: int = 8) -> Decimal:
        """Normalize price to Decimal.

        Args:
            price: Price value
            decimals: Decimal precision

        Returns:
            Normalized Decimal price.
        """
        if isinstance(price, (int, float)):
            return Decimal(str(round(price, decimals)))
        return Decimal(str(price))

    @staticmethod
    def normalize_quantity(quantity: float | Decimal | str, decimals: int = 8) -> Decimal:
        """Normalize quantity to Decimal.

        Args:
            quantity: Quantity value
            decimals: Decimal precision

        Returns:
            Normalized Decimal quantity.
        """
        if isinstance(quantity, (int, float)):
            return Decimal(str(round(quantity, decimals)))
        return Decimal(str(quantity))

    @staticmethod
    def normalize_timeframe(timeframe: str, exchange: str) -> str:
        """Normalize timeframe string.

        Args:
            timeframe: Exchange timeframe string
            exchange: Exchange name

        Returns:
            Normalized timeframe.
        """
        # Convert to standard format
        tf = timeframe.lower()
        mappings = {
            "1m": "1m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "4h": "4h",
            "1d": "1d",
            "1w": "1w",
        }
        return mappings.get(tf, tf)

    def normalize_candle(
        self,
        candle: dict[str, Any],
        exchange: str,
    ) -> dict[str, Any]:
        """Normalize a candle from any exchange.

        Args:
            candle: Exchange-specific candle data
            exchange: Exchange name

        Returns:
            Normalized candle data.
        """
        # Extract common fields
        result = {
            "symbol": self.normalize_symbol(candle.get("symbol", ""), exchange),
            "open": self.normalize_price(candle.get("open", 0)),
            "high": self.normalize_price(candle.get("high", 0)),
            "low": self.normalize_price(candle.get("low", 0)),
            "close": self.normalize_price(candle.get("close", 0)),
            "volume": self.normalize_quantity(candle.get("volume", 0)),
        }

        # Handle timestamp
        timestamp = candle.get("timestamp", candle.get("open_time", 0))
        result["timestamp"] = self.normalize_timestamp(timestamp, exchange)

        # Handle optional fields
        if "quote_volume" in candle:
            result["quote_volume"] = self.normalize_quantity(candle["quote_volume"])

        if "trades" in candle:
            result["trades_count"] = int(candle["trades"])

        return result
