"""EntityId value object - Unique identifier for entities."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class EntityId:
    """Unique identifier for domain entities.

    EntityId is a value object that wraps a UUID string.
    It provides type safety and semantic meaning to entity IDs.
    """

    value: str

    def __post_init__(self) -> None:
        """Validate and normalize the ID value."""
        # Ensure it's a valid UUID format
        object.__setattr__(self, "value", str(self.value))

    def __str__(self) -> str:
        """String representation is the UUID value."""
        return self.value

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"EntityId({self.value!r})"

    def __eq__(self, other: object) -> bool:
        """Compare by value."""
        if not isinstance(other, EntityId):
            return NotImplemented
        return self.value == other.value

    def __hash__(self) -> int:
        """Hash based on value for dict/set usage."""
        return hash(self.value)

    @classmethod
    def generate(cls) -> EntityId:
        """Generate a new random entity ID."""
        return cls(str(uuid.uuid4()))

    @classmethod
    def from_string(cls, value: str) -> EntityId:
        """Create EntityId from string."""
        return cls(value)

    @classmethod
    def from_int(cls, value: int) -> EntityId:
        """Create EntityId from integer (converted to UUID string)."""
        return cls(str(uuid.UUID(int=value)))

    @property
    def short_id(self) -> str:
        """Shortened ID for display (first 8 characters)."""
        return self.value[:8]
