"""Deterministic replay of captured raw market records (MD-013).

Replay feeds stored ``RawMarketMessage`` records through the canonical
normalization pipeline in arrival order, with a caller-provided fixed clock so
gap-detection timestamps are reproducible.  The pipeline is created fresh per
replay call, so identical records always produce identical outcomes.

Replay is purely local: it never opens a socket, never queries a REST
endpoint, never touches account data, and can never place an order.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from packages.market_data.adapters.binance_normalizer import BinanceSpotNormalizer
from packages.market_data.adapters.value_types import (
    IngestionSource,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.contracts.events import MarketEvent
from packages.market_data.services.normalization import NormalizationPipeline, ProcessResult


@dataclass(frozen=True)
class ReplayResult:
    """Outcome of one deterministic replay pass."""

    results: tuple[ProcessResult, ...]
    emitted_events: tuple[MarketEvent, ...]
    accepted_count: int
    duplicate_count: int
    quarantined_count: int
    ignored_count: int


class ReplayRunner:
    """Reproduce canonical events from stored raw records without any network."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))

    async def replay(
        self,
        records: Sequence[RawMarketMessage],
        *,
        source: IngestionSource = "websocket",
        subscriptions: Collection[StreamSubscription] | None = None,
    ) -> ReplayResult:
        """Normalize stored raw records in arrival order.

        Args:
            records: Raw records in capture order, typically from a store's
                ``raw_snapshot`` or a fixture archive.
            source: Ingestion source label applied to every record.
            subscriptions: Optional active subscription set for identity
                validation during replay.

        Returns:
            Per-record results, the emitted canonical event sequence, and
            disposition counts.
        """
        pipeline = NormalizationPipeline(BinanceSpotNormalizer(), clock=self._clock)
        if subscriptions is not None:
            pipeline.set_active_subscriptions(subscriptions)
        results: list[ProcessResult] = []
        emitted: list[MarketEvent] = []
        for record in records:
            result = await pipeline.process(record, source=source)
            results.append(result)
            emitted.extend(result.emitted_events)
        counts: dict[str, int] = {"accepted": 0, "duplicate": 0, "quarantined": 0, "ignored": 0}
        for result in results:
            counts[result.disposition] += 1
        return ReplayResult(
            results=tuple(results),
            emitted_events=tuple(emitted),
            accepted_count=counts["accepted"],
            duplicate_count=counts["duplicate"],
            quarantined_count=counts["quarantined"],
            ignored_count=counts["ignored"],
        )
