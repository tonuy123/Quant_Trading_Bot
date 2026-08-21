"""Base domain event class."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class DomainEvent:
    """Base class for all domain events.

    Domain events are immutable records of something that happened.
    They follow the observer pattern and can be published to subscribers.

    Key principles:
    - Events are facts, never change after creation
    - Events are named in past tense (OrderPlaced, not PlaceOrder)
    - Events contain all relevant data for subscribers
    - Events include a timestamp and unique ID
    """

    event_id: str = field(init=False, default_factory=lambda: str(datetime.utcnow().timestamp()))
    timestamp: datetime = field(init=False, default_factory=datetime.utcnow)
    event_type: ClassVar[str] = "domain_event"
    version: ClassVar[int] = 1

    def __post_init__(self) -> None:
        """Validate event data."""
        if self.timestamp > datetime.utcnow():
            raise ValueError("Event timestamp cannot be in the future")

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type this event relates to."""
        return self.__class__.__name__.replace("Event", "")

    def to_dict(self) -> dict[str, Any]:
        """Convert event to dictionary for serialization."""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "version": self.version,
            "data": self._to_data(),
        }

    def _to_data(self) -> dict[str, Any]:
        """Override in subclasses to add event-specific data."""
        return {}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DomainEvent:
        """Reconstruct event from dictionary."""
        return cls(**data)

    def __str__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}({self.event_id}, {self.timestamp.isoformat()})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"{self.__class__.__name__}(timestamp={self.timestamp!r})"
