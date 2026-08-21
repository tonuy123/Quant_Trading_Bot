"""Persistence ports for deterministic local research storage (MD-011).

The port follows the persistence boundary documented in
``docs/architecture/market-data-layer.md`` section 12: candles, ingestion
watermarks, gap rows, and outbox entries carry idempotent natural keys, and a
closed candle, its watermark, and its outbox entry commit atomically.  Ticker
state is an explicit last-write-wins cache, and the raw archive is bounded.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol

from packages.market_data.adapters.value_types import ExchangeId, MarketSymbol
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    DataGapDetected,
    MarketEvent,
    TickerEvent,
)

GapRecoveryOutcome = Literal[
    "pending",
    "recovered",
    "no_missing_candle",
    "unresolved",
    "snapshot_only",
    "unrecoverable",
]


@dataclass(frozen=True)
class PersistResult:
    """Outcome of one atomic closed-candle persistence attempt."""

    candle: Literal["inserted", "duplicate"]
    watermark: Literal["advanced", "unchanged"]
    outbox: Literal["inserted", "duplicate"]


@dataclass(frozen=True)
class GapRecord:
    """A durable gap row with its observable recovery outcome."""

    event: DataGapDetected
    recovery_state: GapRecoveryOutcome
    recovered_open_times: tuple[datetime, ...] = ()
    unresolved_open_times: tuple[datetime, ...] = ()


class MarketDataRepository(Protocol):
    """Port for local research persistence of canonical Market Data events.

    Implementations must be restart-safe, must never expose credentials, and
    must reject malformed records without corrupting previously stored data.
    """

    async def get_kline_watermark(
        self,
        exchange: ExchangeId,
        symbol: MarketSymbol,
        interval: str,
    ) -> datetime | None:
        """Return the last closed-candle open time for a kline stream, if any."""
        ...

    async def persist_closed_candle_and_outbox(
        self,
        event: CandleClosedEvent,
    ) -> PersistResult:
        """Atomically insert-if-absent the candle, advance the watermark, and
        append an outbox entry for at-least-once relay."""
        ...

    async def create_or_update_gap(self, gap: DataGapDetected) -> None:
        """Upsert a gap row by its stable gap ID."""
        ...

    async def mark_gap_recovery(
        self,
        gap_id: str,
        *,
        outcome: GapRecoveryOutcome,
        recovered_open_times: Sequence[datetime] = (),
        unresolved_open_times: Sequence[datetime] = (),
    ) -> None:
        """Record the observable outcome of one bounded recovery pass."""
        ...

    async def get_gap(self, gap_id: str) -> GapRecord | None:
        """Return a stored gap row, or None when the gap ID is unknown."""
        ...

    async def list_undelivered_outbox(self) -> tuple[MarketEvent, ...]:
        """Return undelivered outbox events in commit order."""
        ...

    async def mark_outbox_relayed(self, event_id: str) -> None:
        """Mark one outbox entry as relayed (at-least-once publication)."""
        ...

    async def set_ticker_cache(self, event: TickerEvent) -> bool:
        """Upsert the current ticker cache with last-write-wins semantics.

        Returns True when the cache was updated, False when the incoming event
        is older than the stored value.
        """
        ...

    async def get_ticker_cache(self, symbol: MarketSymbol) -> TickerEvent | None:
        """Return the current cached ticker state, or None when absent."""
        ...

    async def list_candles(
        self,
        symbol: MarketSymbol,
        interval: str,
        *,
        start_inclusive: datetime | None = None,
        end_exclusive: datetime | None = None,
    ) -> tuple[CandleClosedEvent, ...]:
        """Return stored closed candles ordered by open time within an optional
        half-open UTC range."""
        ...
