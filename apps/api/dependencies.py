"""API dependencies - Dependency injection for FastAPI."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from packages.messaging import InMemoryEventBus
from packages.observability import get_logger

if TYPE_CHECKING:
    pass


async def get_event_bus() -> InMemoryEventBus:
    """Get event bus dependency."""
    bus = InMemoryEventBus()
    await bus.start()
    return bus


async def get_logger_dependency() -> structlog.stdlib.BoundLogger:
    """Get logger dependency."""
    return get_logger(__name__)
