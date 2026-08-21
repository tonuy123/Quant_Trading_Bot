"""Environment enum and utilities."""

from __future__ import annotations

from enum import StrEnum


class Environment(StrEnum):
    """Application environment."""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_development(self) -> bool:
        """Check if development environment."""
        return self == Environment.DEVELOPMENT

    @property
    def is_staging(self) -> bool:
        """Check if staging environment."""
        return self == Environment.STAGING

    @property
    def is_production(self) -> bool:
        """Check if production environment."""
        return self == Environment.PRODUCTION

    @classmethod
    def from_string(cls, value: str) -> Environment:
        """Parse from string."""
        value = value.lower().strip()
        for env in cls:
            if env.value == value:
                return env
        raise ValueError(f"Unknown environment: {value}")
