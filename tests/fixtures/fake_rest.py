"""Deterministic fake Binance public REST history adapter (MD-012).

Implements the shape of ``PublicMarketDataHistory`` (see
``packages/market_data/adapters/protocols.py``) from scripted in-memory
responses.  It never performs HTTP I/O, never accepts credentials, and reports
provider-style failures (such as HTTP 429) deterministically so gap-recovery
and rate-limit tests stay network-free.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from packages.market_data.adapters.binance_rest import PublicMarketDataRequestError
from packages.market_data.adapters.value_types import MarketSymbol, RawMarketMessage


class FakeRestAdapter:
    """Scripted public market-data history with configurable failures."""

    def __init__(
        self,
        *,
        server_time: datetime,
        klines: Mapping[tuple[str, str], Sequence[Sequence[Any]]] | None = None,
        ticker_snapshot: dict[str, Any] | None = None,
        fail_after_requests: int | None = None,
        fail_status: int = 429,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if server_time.tzinfo is None or server_time.utcoffset() is None:
            raise ValueError("fake server time must be UTC-aware")
        self._server_time = server_time
        self._klines = dict(klines or {})
        self._ticker_snapshot = ticker_snapshot
        self._fail_after = fail_after_requests
        self._fail_status = fail_status
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sequence = 0
        self.requests: list[str] = []

    async def get_server_time(self) -> datetime:
        """Return the scripted public server time."""
        await asyncio.sleep(0)
        self.requests.append("server_time")
        self._maybe_fail("/api/v3/time")
        return self._server_time

    async def get_closed_klines(
        self,
        symbol: MarketSymbol,
        interval: str,
        start_inclusive: datetime,
        end_exclusive: datetime,
        page_limit: int = 1000,
    ) -> Sequence[RawMarketMessage]:
        """Return scripted closed klines inside a bounded half-open UTC range."""
        await asyncio.sleep(0)
        self.requests.append(f"klines:{symbol.canonical}:{interval}")
        self._maybe_fail("/api/v3/klines")
        key = (symbol.canonical, interval)
        start_ms = int(start_inclusive.timestamp() * 1000)
        end_ms = int(end_exclusive.timestamp() * 1000)
        stream_name = f"{symbol.base}{symbol.quote}".lower() + f"@kline_{interval}"
        rows = [
            row
            for row in self._klines.get(key, ())
            if isinstance(row[0], int) and start_ms <= row[0] < end_ms
        ]
        return tuple(
            self._raw(stream_name, row) for row in sorted(rows, key=lambda row: row[0])[:page_limit]
        )

    async def get_ticker_snapshot(self, symbol: MarketSymbol) -> RawMarketMessage:
        """Return one scripted current-state public ticker snapshot."""
        await asyncio.sleep(0)
        self.requests.append(f"ticker:{symbol.canonical}")
        self._maybe_fail("/api/v3/ticker/24hr")
        if self._ticker_snapshot is None:
            raise PublicMarketDataRequestError(400, "/api/v3/ticker/24hr")
        stream_name = f"{symbol.base}{symbol.quote}".lower() + "@ticker"
        return self._raw(stream_name, self._ticker_snapshot)

    def _maybe_fail(self, endpoint: str) -> None:
        if self._fail_after is not None and len(self.requests) >= self._fail_after:
            raise PublicMarketDataRequestError(self._fail_status, endpoint)

    def _raw(self, stream_name: str, payload: Any) -> RawMarketMessage:
        self._sequence += 1
        return RawMarketMessage(
            exchange="binance",
            market_type="spot",
            connection_id="fake-rest",
            stream_name=stream_name,
            payload_bytes=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            received_at=self._clock().astimezone(UTC),
            received_monotonic_ns=0,
            receive_sequence=self._sequence,
            source_timestamp_unit="ms",
        )
