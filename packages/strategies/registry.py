"""Strategy registry - Manages available strategies."""

from __future__ import annotations

from typing import TYPE_CHECKING

from packages.strategies.base import BaseStrategy

if TYPE_CHECKING:
    from packages.domain.enums import Timeframe


class StrategyRegistry:
    """Registry for managing trading strategies.

    The registry:
    - Stores available strategies
    - Provides strategy lookup
    - Manages strategy lifecycle
    """

    def __init__(self) -> None:
        """Initialize registry."""
        self._strategies: dict[str, BaseStrategy] = {}

    def register(self, strategy: BaseStrategy) -> None:
        """Register a strategy.

        Args:
            strategy: Strategy to register
        """
        if strategy.id in self._strategies:
            raise ValueError(f"Strategy {strategy.id} already registered")
        self._strategies[strategy.id] = strategy

    def unregister(self, strategy_id: str) -> None:
        """Unregister a strategy.

        Args:
            strategy_id: Strategy ID to unregister
        """
        if strategy_id in self._strategies:
            del self._strategies[strategy_id]

    def get(self, strategy_id: str) -> BaseStrategy | None:
        """Get a strategy by ID.

        Args:
            strategy_id: Strategy ID

        Returns:
            Strategy if found.
        """
        return self._strategies.get(strategy_id)

    def list_strategies(self) -> list[BaseStrategy]:
        """List all registered strategies.

        Returns:
            List of strategies.
        """
        return list(self._strategies.values())

    def list_enabled(self) -> list[BaseStrategy]:
        """List all enabled strategies.

        Returns:
            List of enabled strategies.
        """
        return [s for s in self._strategies.values() if s.is_enabled]

    def list_by_symbol(self, symbol: str) -> list[BaseStrategy]:
        """List strategies that trade a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            List of strategies.
        """
        return [s for s in self._strategies.values() if symbol in s.symbols]

    def list_by_timeframe(self, timeframe: Timeframe) -> list[BaseStrategy]:
        """List strategies for a timeframe.

        Args:
            timeframe: Candle timeframe

        Returns:
            List of strategies.
        """
        return [s for s in self._strategies.values() if s.timeframe == timeframe]

    def enable(self, strategy_id: str) -> bool:
        """Enable a strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            True if enabled.
        """
        strategy = self.get(strategy_id)
        if strategy:
            strategy.enable()
            return True
        return False

    def disable(self, strategy_id: str) -> bool:
        """Disable a strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            True if disabled.
        """
        strategy = self.get(strategy_id)
        if strategy:
            strategy.disable()
            return True
        return False

    def clear(self) -> None:
        """Clear all strategies."""
        self._strategies.clear()

    def __len__(self) -> int:
        """Get number of registered strategies."""
        return len(self._strategies)
