"""Strategy service - Orchestrates strategy operations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from packages.domain.interfaces import EventPublisher, UnitOfWorkFactory
from packages.strategies.contracts import StrategyConfig
from packages.strategies.registry import StrategyRegistry

if TYPE_CHECKING:
    from packages.domain.value_objects import Symbol


@dataclass
class StrategyStatus:
    """Status of a strategy."""

    strategy_id: str
    name: str
    is_enabled: bool
    last_signal_at: datetime | None
    signals_generated: int
    signals_accepted: int
    signals_rejected: int


class StrategyService:
    """Service for managing trading strategies.

    Handles strategy registration, enabling/disabling,
    and signal generation tracking.
    """

    def __init__(
        self,
        unit_of_work_factory: UnitOfWorkFactory,
        strategy_registry: StrategyRegistry,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize strategy service.

        Args:
            unit_of_work_factory: Factory for database transactions
            strategy_registry: Registry of available strategies
            event_publisher: Event distribution
        """
        self._uow_factory = unit_of_work_factory
        self._registry = strategy_registry
        self._events = event_publisher

    async def get_available_strategies(self) -> list[StrategyConfig]:
        """Get all registered strategies.

        Returns:
            List of strategy configurations.
        """
        strategies = self._registry.list_strategies()
        return [
            StrategyConfig(
                strategy_id=s.id,
                name=s.name,
                symbols=s.symbols,
                timeframe=s.timeframe,
                parameters=s.parameters,
                enabled=s.is_enabled,
            )
            for s in strategies
        ]

    async def enable_strategy(
        self,
        strategy_id: str,
        symbols: list[Symbol] | None = None,
    ) -> bool:
        """Enable a strategy.

        Args:
            strategy_id: Strategy ID
            symbols: Symbols to trade (None = all registered)

        Returns:
            True if enabled.
        """
        strategy = self._registry.get(strategy_id)
        if not strategy:
            return False

        if symbols:
            strategy.symbols = [str(s) for s in symbols]

        strategy.enable()

        # Update persistence
        async with self._uow_factory.transaction():
            # TODO: Save enabled state
            pass

        return True

    async def disable_strategy(self, strategy_id: str, reason: str | None = None) -> bool:
        """Disable a strategy.

        Args:
            strategy_id: Strategy ID
            reason: Reason for disabling

        Returns:
            True if disabled.
        """
        strategy = self._registry.get(strategy_id)
        if not strategy:
            return False

        strategy.disable()

        return True

    async def get_strategy_status(self, strategy_id: str) -> StrategyStatus | None:
        """Get strategy status.

        Args:
            strategy_id: Strategy ID

        Returns:
            Strategy status.
        """
        strategy = self._registry.get(strategy_id)
        if not strategy:
            return None

        return StrategyStatus(
            strategy_id=strategy.id,
            name=strategy.name,
            is_enabled=strategy.is_enabled,
            last_signal_at=strategy.last_signal_at,
            signals_generated=strategy.signals_generated,
            signals_accepted=strategy.signals_accepted,
            signals_rejected=strategy.signals_rejected,
        )

    async def get_active_strategies(self) -> list[StrategyStatus]:
        """Get all active strategies.

        Returns:
            List of active strategy statuses.
        """
        strategies = self._registry.list_enabled()
        return [
            StrategyStatus(
                strategy_id=s.id,
                name=s.name,
                is_enabled=True,
                last_signal_at=s.last_signal_at,
                signals_generated=s.signals_generated,
                signals_accepted=s.signals_accepted,
                signals_rejected=s.signals_rejected,
            )
            for s in strategies
        ]
