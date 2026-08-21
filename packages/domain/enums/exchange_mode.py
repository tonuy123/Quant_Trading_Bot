"""Exchange-mode enumeration."""

from __future__ import annotations

from enum import StrEnum


class ExchangeMode(StrEnum):
    """Operating mode for exchange connections."""

    FAKE = "fake"  # Simulated data, no real trading
    PAPER = "paper"  # Paper trading with simulated fills
    LIVE = "live"  # Real trading with real money

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_fake(self) -> bool:
        """Check if fake mode."""
        return self == ExchangeMode.FAKE

    @property
    def is_paper(self) -> bool:
        """Check if paper trading mode."""
        return self == ExchangeMode.PAPER

    @property
    def is_live(self) -> bool:
        """Check if live trading mode."""
        return self == ExchangeMode.LIVE

    @property
    def allows_trading(self) -> bool:
        """Check if actual orders can be placed."""
        return self in {ExchangeMode.PAPER, ExchangeMode.LIVE}

    @property
    def is_safe(self) -> bool:
        """Check if this mode is safe (no real money)."""
        return self in {ExchangeMode.FAKE, ExchangeMode.PAPER}

    @property
    def requires_confirmation(self) -> bool:
        """Check if trading requires extra confirmation."""
        return self == ExchangeMode.LIVE

    @classmethod
    def from_string(cls, value: str) -> ExchangeMode:
        """Parse from string."""
        value = value.lower()
        mapping = {
            "fake": cls.FAKE,
            "paper": cls.PAPER,
            "live": cls.LIVE,
        }
        if result := mapping.get(value):
            return result
        raise ValueError(f"Unknown exchange mode: {value}")
