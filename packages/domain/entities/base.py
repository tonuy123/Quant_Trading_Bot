"""Base entity class with identity and lifecycle management."""

from __future__ import annotations

from datetime import datetime

from packages.domain.value_objects import EntityId


class Entity:
    """Base class for all domain entities.

    Entities have:
    - Unique identity (id)
    - Creation timestamp
    - Optional update tracking

    Entities are compared by identity, not by value.
    Two entities with the same ID are the same entity.

    Subclasses are dataclasses. They must declare ``id``, ``created_at`` and
    ``updated_at`` after their required fields:

        @dataclass
        class Order(Entity):
            symbol: Symbol
            ...
            id: EntityId | None = None
            created_at: datetime | None = None
            updated_at: datetime | None = None
    """

    id: EntityId | None
    created_at: datetime | None
    updated_at: datetime | None

    def __post_init__(self) -> None:
        """Backfill identity fields and validate entity state."""
        if self.id is None:
            self.id = EntityId.generate()
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        self._validate()

    def _validate(self) -> None:
        """Override in subclasses to add entity-specific validation."""
        pass

    def mark_updated(self) -> None:
        """Mark the entity as updated."""
        self.updated_at = datetime.utcnow()

    def __eq__(self, other: object) -> bool:
        """Entities are equal if they have the same ID."""
        if not isinstance(other, Entity):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        """Hash by entity ID for use in sets and dicts."""
        return hash(self.id)

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"{self.__class__.__name__}(id={self.id!r})"
