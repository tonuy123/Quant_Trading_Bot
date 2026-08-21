"""Bounded raw market-message capture for audit and deterministic replay."""

from __future__ import annotations

from collections import deque
from typing import Protocol

from packages.market_data.adapters.value_types import RawMarketMessage


class RawCaptureSink(Protocol):
    """A non-authoritative sink for raw ingress records."""

    async def capture(self, message: RawMarketMessage) -> bool:
        """Store one raw message and report whether it was accepted."""
        ...


class BoundedRawCapture:
    """In-memory bounded sink suitable for local audit and tests.

    A full buffer drops new raw records rather than evicting older evidence or
    allowing unbounded memory growth. Canonical normalization must continue if
    this sink returns False or raises.
    """

    def __init__(self, capacity: int = 10_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._messages: deque[RawMarketMessage] = deque()
        self._dropped = 0

    async def capture(self, message: RawMarketMessage) -> bool:
        """Append a raw message without exposing payload content in logs."""
        if len(self._messages) >= self._capacity:
            self._dropped += 1
            return False
        self._messages.append(message)
        return True

    @property
    def dropped_count(self) -> int:
        """Return the number of raw messages intentionally not retained."""
        return self._dropped

    @property
    def size(self) -> int:
        """Return retained raw-message count."""
        return len(self._messages)

    def snapshot(self) -> tuple[RawMarketMessage, ...]:
        """Return a stable raw-record snapshot for controlled replay."""
        return tuple(self._messages)
