"""In-memory event bus implementation."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from packages.domain.events.base import DomainEvent

if TYPE_CHECKING:
    pass


class InMemoryEventBus:
    """In-memory event bus for domain events.

    Publishes events to registered handlers.
    Used for local event distribution.
    """

    def __init__(self) -> None:
        """Initialize event bus."""
        self._handlers: dict[type[DomainEvent], list[Callable[..., Any]]] = {}
        self._running = False

    async def start(self) -> None:
        """Start the event bus."""
        self._running = True

    async def stop(self) -> None:
        """Stop the event bus."""
        self._running = False

    def subscribe(self, event_type: type[DomainEvent], handler: Callable[..., Any]) -> None:
        """Subscribe to an event type.

        Args:
            event_type: Type of event to subscribe to
            handler: Handler function
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def unsubscribe(self, event_type: type[DomainEvent], handler: Callable[..., Any]) -> None:
        """Unsubscribe from an event type.

        Args:
            event_type: Type of event
            handler: Handler to remove
        """
        if event_type in self._handlers:
            self._handlers[event_type].remove(handler)

    async def publish(self, event: DomainEvent) -> None:
        """Publish an event.

        Args:
            event: Event to publish
        """
        event_type = type(event)
        handlers = self._handlers.get(event_type, [])

        # Also notify base event handlers
        for base_type in event_type.__mro__:
            if base_type is not object and issubclass(base_type, DomainEvent):
                handlers.extend(self._handlers.get(base_type, []))

        # Remove duplicates while preserving order
        seen = set()
        unique_handlers = []
        for h in handlers:
            if id(h) not in seen:
                seen.add(id(h))
                unique_handlers.append(h)

        # Call handlers
        for handler in unique_handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception:
                # Log error but don't fail publishing
                pass

    async def wait_for_handlers(self) -> None:
        """Wait for all pending handlers to complete."""
        pass  # In-memory, handlers are synchronous
