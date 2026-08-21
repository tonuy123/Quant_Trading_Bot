"""Event publisher interface - Contract for publishing domain events."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from packages.domain.events.base import DomainEvent

if TYPE_CHECKING:
    pass


# Type alias for event handlers
DomainEventHandler = Callable[[DomainEvent], Awaitable[None]]
SyncEventHandler = Callable[[DomainEvent], None]


class EventPublisher(ABC):
    """Abstract event publisher for domain events.

    Events are published when domain state changes occur.
    Subscribers receive events asynchronously.

    This interface is intentionally minimal:
    - publish(event) - publish a single event
    - subscribe(event_type, handler) - subscribe to event type
    - unsubscribe(event_type, handler) - unsubscribe handler
    """

    @abstractmethod
    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event.

        Args:
            event: Domain event to publish.
        """
        ...

    @abstractmethod
    async def publish_many(self, events: list[DomainEvent]) -> None:
        """Publish multiple domain events.

        Args:
            events: List of domain events to publish.
        """
        ...

    @abstractmethod
    def subscribe(self, event_type: type[DomainEvent], handler: DomainEventHandler) -> None:
        """Subscribe to an event type.

        Args:
            event_type: Type of event to subscribe to.
            handler: Async handler function.
        """
        ...

    @abstractmethod
    def subscribe_sync(self, event_type: type[DomainEvent], handler: SyncEventHandler) -> None:
        """Subscribe to an event type with synchronous handler.

        Args:
            event_type: Type of event to subscribe to.
            handler: Sync handler function.
        """
        ...

    @abstractmethod
    def unsubscribe(self, event_type: type[DomainEvent], handler: DomainEventHandler) -> None:
        """Unsubscribe from an event type.

        Args:
            event_type: Type of event to unsubscribe from.
            handler: Handler to remove.
        """
        ...

    @abstractmethod
    async def wait_for_handlers(self) -> None:
        """Wait for all pending event handlers to complete."""
        ...


class DomainEventBus(EventPublisher):
    """Simple in-memory event bus implementation.

    This is the default implementation for local event distribution.
    For distributed systems, replace with message queue implementation.
    """

    @abstractmethod
    async def start(self) -> None:
        """Start the event bus."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop the event bus."""
        ...
