"""Base value object class - Immutable objects compared by value."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class ValueObject(ABC):
    """Base class for all value objects.

    Value objects are immutable objects defined entirely by their attributes.
    They are compared by value, not by identity.
    Two value objects with the same attributes are equal.

    Examples:
        - Money(100, "USD") == Money(100, "USD")
        - Price(50000.0, "BTC", "USDT") == Price(50000.0, "BTC", "USDT")
    """

    @abstractmethod
    def __str__(self) -> str:
        """String representation."""
        ...

    @classmethod
    def from_string(cls, value: str) -> ValueObject:
        """Parse from string representation."""
        raise NotImplementedError(f"{cls.__name__} does not support parsing from string")
