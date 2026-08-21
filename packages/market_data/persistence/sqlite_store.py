"""SQLite-backed local research persistence for canonical Market Data events.

MD-011 requires a deterministic, restart-safe, replaceable storage that never
touches a production database server.  SQLite with WAL provides atomic
transactions, durable restart behavior, and exact local research semantics
with no extra dependency.  Every operation opens its own connection so a
malformed record or a failed transaction can never corrupt other records.

Security boundary: only public market data is stored; credentials and provider
secrets are never part of any stored field.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from packages.market_data.adapters.value_types import (
    ExchangeId,
    MarketSymbol,
    RawMarketMessage,
)
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    DataGapDetected,
    MarketEvent,
    TickerEvent,
)
from packages.market_data.persistence.ports import (
    GapRecord,
    GapRecoveryOutcome,
    PersistResult,
)
from packages.market_data.persistence.serialization import event_to_record, record_to_event

_SCHEMA = """
CREATE TABLE IF NOT EXISTS candles (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TEXT NOT NULL,
    event_json TEXT NOT NULL,
    PRIMARY KEY (exchange, market_type, symbol, interval, open_time)
);
CREATE TABLE IF NOT EXISTS watermarks (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    last_open_time TEXT NOT NULL,
    PRIMARY KEY (exchange, market_type, symbol, interval)
);
CREATE TABLE IF NOT EXISTS gaps (
    gap_id TEXT PRIMARY KEY,
    event_json TEXT NOT NULL,
    recovery_state TEXT NOT NULL DEFAULT 'pending',
    recovered_open_times TEXT NOT NULL DEFAULT '[]',
    unresolved_open_times TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    relayed_at TEXT
);
CREATE TABLE IF NOT EXISTS tickers (
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    symbol TEXT NOT NULL,
    event_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    receive_sequence INTEGER,
    PRIMARY KEY (exchange, market_type, symbol)
);
CREATE TABLE IF NOT EXISTS raw_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL UNIQUE,
    exchange TEXT NOT NULL,
    market_type TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    stream_name TEXT NOT NULL,
    payload BLOB NOT NULL,
    received_at TEXT NOT NULL,
    received_monotonic_ns INTEGER NOT NULL,
    receive_sequence INTEGER NOT NULL,
    source_timestamp_unit TEXT NOT NULL
);
"""


class SqliteMarketDataStore:
    """Local, restart-safe implementation of MarketDataRepository and RawCaptureSink.

    The raw archive is append-only and bounded by ``raw_capacity``: once the
    capacity is reached, new raw records are intentionally dropped and counted,
    matching the ``RawCaptureSink`` contract of the normalization pipeline.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        raw_capacity: int = 100_000,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if raw_capacity < 1:
            raise ValueError("raw_capacity must be positive")
        self._path = str(path)
        self._raw_capacity = raw_capacity
        self._clock = clock or (lambda: datetime.now(UTC))
        self._dropped_raw = 0

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=10.0)
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.executescript(_SCHEMA)
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Yield one transactional connection and always release its file handle.

        ``sqlite3.Connection`` as a context manager commits or rolls back but
        does not close itself.  The explicit ``finally`` is essential on
        Windows: a local soak runner must be able to remove its temporary
        SQLite database immediately after the final measurement.
        """
        connection = self._connect()
        try:
            yield connection
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
        finally:
            connection.close()

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("persistence clock must return a UTC-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _symbol_key(symbol: MarketSymbol | None) -> str:
        return symbol.canonical if symbol is not None else ""

    @staticmethod
    def _event_json(event: MarketEvent) -> str:
        return json.dumps(event_to_record(event), separators=(",", ":"))

    # ------------------------------------------------------------------
    # MarketDataRepository port
    # ------------------------------------------------------------------

    async def get_kline_watermark(
        self,
        exchange: ExchangeId,
        symbol: MarketSymbol,
        interval: str,
    ) -> datetime | None:
        """Return the last closed-candle open time for a kline stream, if any."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT last_open_time FROM watermarks "
                "WHERE exchange = ? AND market_type = ? AND symbol = ? AND interval = ?",
                (exchange, "spot", symbol.canonical, interval),
            ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(str(row[0]))

    async def persist_closed_candle_and_outbox(
        self,
        event: CandleClosedEvent,
    ) -> PersistResult:
        """Atomically insert the candle, advance the watermark, and queue outbox."""
        event_json = self._event_json(event)
        with self._connection() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO candles "
                "(exchange, market_type, symbol, interval, open_time, event_json) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.exchange,
                    event.market_type,
                    self._symbol_key(event.symbol),
                    event.interval,
                    event.open_time.isoformat(),
                    event_json,
                ),
            )
            candle_state: Literal["inserted", "duplicate"] = (
                "inserted" if cursor.rowcount == 1 else "duplicate"
            )
            row = connection.execute(
                "SELECT last_open_time FROM watermarks "
                "WHERE exchange = ? AND market_type = ? AND symbol = ? AND interval = ?",
                (event.exchange, event.market_type, self._symbol_key(event.symbol), event.interval),
            ).fetchone()
            watermark_state: Literal["advanced", "unchanged"] = "advanced"
            if row is not None:
                current = datetime.fromisoformat(str(row[0]))
                if event.open_time <= current:
                    watermark_state = "unchanged"
            if watermark_state == "advanced":
                connection.execute(
                    "INSERT INTO watermarks "
                    "(exchange, market_type, symbol, interval, last_open_time) "
                    "VALUES (?, ?, ?, ?, ?) "
                    "ON CONFLICT(exchange, market_type, symbol, interval) "
                    "DO UPDATE SET last_open_time = excluded.last_open_time",
                    (
                        event.exchange,
                        event.market_type,
                        self._symbol_key(event.symbol),
                        event.interval,
                        event.open_time.isoformat(),
                    ),
                )
            cursor = connection.execute(
                "INSERT OR IGNORE INTO outbox (event_id, event_type, event_json) VALUES (?, ?, ?)",
                (event.event_id, event.event_type, event_json),
            )
            outbox_state: Literal["inserted", "duplicate"] = (
                "inserted" if cursor.rowcount == 1 else "duplicate"
            )
        return PersistResult(
            candle=candle_state,
            watermark=watermark_state,
            outbox=outbox_state,
        )

    async def create_or_update_gap(self, gap: DataGapDetected) -> None:
        """Upsert a gap row by its stable gap ID."""
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO gaps (gap_id, event_json) VALUES (?, ?) "
                "ON CONFLICT(gap_id) DO UPDATE SET event_json = excluded.event_json",
                (gap.gap_id, self._event_json(gap)),
            )

    async def mark_gap_recovery(
        self,
        gap_id: str,
        *,
        outcome: GapRecoveryOutcome,
        recovered_open_times: Sequence[datetime] = (),
        unresolved_open_times: Sequence[datetime] = (),
    ) -> None:
        """Record the observable outcome of one bounded recovery pass."""
        recovered = json.dumps(
            [value.isoformat() for value in recovered_open_times], separators=(",", ":")
        )
        unresolved = json.dumps(
            [value.isoformat() for value in unresolved_open_times], separators=(",", ":")
        )
        with self._connection() as connection:
            connection.execute(
                "UPDATE gaps SET recovery_state = ?, recovered_open_times = ?, "
                "unresolved_open_times = ? WHERE gap_id = ?",
                (outcome, recovered, unresolved, gap_id),
            )

    async def get_gap(self, gap_id: str) -> GapRecord | None:
        """Return a stored gap row, or None when the gap ID is unknown."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT event_json, recovery_state, recovered_open_times, "
                "unresolved_open_times FROM gaps WHERE gap_id = ?",
                (gap_id,),
            ).fetchone()
        if row is None:
            return None
        event = record_to_event(json.loads(str(row[0])))
        if not isinstance(event, DataGapDetected):
            raise RuntimeError("stored gap record is not a DataGapDetected event")
        recovery_state = str(row[1])
        if recovery_state not in {
            "pending",
            "recovered",
            "no_missing_candle",
            "unresolved",
            "snapshot_only",
            "unrecoverable",
        }:
            raise RuntimeError("stored gap record has an unknown recovery state")
        recovered = tuple(datetime.fromisoformat(value) for value in json.loads(str(row[2])))
        unresolved = tuple(datetime.fromisoformat(value) for value in json.loads(str(row[3])))
        return GapRecord(
            event=event,
            recovery_state=cast(GapRecoveryOutcome, recovery_state),
            recovered_open_times=recovered,
            unresolved_open_times=unresolved,
        )

    async def list_undelivered_outbox(self) -> tuple[MarketEvent, ...]:
        """Return undelivered outbox events in commit order."""
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT event_json FROM outbox WHERE relayed_at IS NULL ORDER BY rowid"
            ).fetchall()
        events: list[MarketEvent] = []
        for row in rows:
            events.append(record_to_event(json.loads(str(row[0]))))
        return tuple(events)

    async def mark_outbox_relayed(self, event_id: str) -> None:
        """Mark one outbox entry as relayed (at-least-once publication)."""
        with self._connection() as connection:
            connection.execute(
                "UPDATE outbox SET relayed_at = ? WHERE event_id = ?",
                (self._utc_now().isoformat(), event_id),
            )

    async def set_ticker_cache(self, event: TickerEvent) -> bool:
        """Upsert the current ticker cache with last-write-wins semantics."""
        event_json = self._event_json(event)
        occurred_at = event.occurred_at.isoformat()
        sequence = event.receive_sequence
        with self._connection() as connection:
            row = connection.execute(
                "SELECT occurred_at, receive_sequence FROM tickers "
                "WHERE exchange = ? AND market_type = ? AND symbol = ?",
                (event.exchange, event.market_type, self._symbol_key(event.symbol)),
            ).fetchone()
            if row is not None:
                existing_at = datetime.fromisoformat(str(row[0]))
                existing_sequence = row[1]
                if (existing_at, existing_sequence if existing_sequence is not None else -1) > (
                    event.occurred_at,
                    sequence if sequence is not None else -1,
                ):
                    return False
            connection.execute(
                "INSERT INTO tickers "
                "(exchange, market_type, symbol, event_json, occurred_at, receive_sequence) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(exchange, market_type, symbol) "
                "DO UPDATE SET event_json = excluded.event_json, "
                "occurred_at = excluded.occurred_at, "
                "receive_sequence = excluded.receive_sequence",
                (
                    event.exchange,
                    event.market_type,
                    self._symbol_key(event.symbol),
                    event_json,
                    occurred_at,
                    sequence,
                ),
            )
            return True

    async def get_ticker_cache(self, symbol: MarketSymbol) -> TickerEvent | None:
        """Return the current cached ticker state, or None when absent."""
        with self._connection() as connection:
            row = connection.execute(
                "SELECT event_json FROM tickers "
                "WHERE exchange = 'binance' AND market_type = 'spot' AND symbol = ?",
                (symbol.canonical,),
            ).fetchone()
        if row is None:
            return None
        event = record_to_event(json.loads(str(row[0])))
        if not isinstance(event, TickerEvent):
            raise RuntimeError("stored ticker record is not a TickerEvent")
        return event

    async def list_candles(
        self,
        symbol: MarketSymbol,
        interval: str,
        *,
        start_inclusive: datetime | None = None,
        end_exclusive: datetime | None = None,
    ) -> tuple[CandleClosedEvent, ...]:
        """Return stored closed candles ordered by open time."""
        query = (
            "SELECT event_json FROM candles "
            "WHERE exchange = 'binance' AND market_type = 'spot' "
            "AND symbol = ? AND interval = ?"
        )
        params: list[Any] = [symbol.canonical, interval]
        if start_inclusive is not None:
            query += " AND open_time >= ?"
            params.append(start_inclusive.isoformat())
        if end_exclusive is not None:
            query += " AND open_time < ?"
            params.append(end_exclusive.isoformat())
        query += " ORDER BY open_time"
        with self._connection() as connection:
            rows = connection.execute(query, params).fetchall()
        candles: list[CandleClosedEvent] = []
        for row in rows:
            event = record_to_event(json.loads(str(row[0])))
            if not isinstance(event, CandleClosedEvent):
                raise RuntimeError("stored candle record is not a CandleClosedEvent")
            candles.append(event)
        return tuple(candles)

    # ------------------------------------------------------------------
    # RawCaptureSink port (bounded raw archive for replay and audit)
    # ------------------------------------------------------------------

    async def capture(self, message: RawMarketMessage) -> bool:
        """Append one raw record, or drop it when the bounded capacity is full.

        A message whose ID is already stored is accepted silently (idempotent)
        even when the archive is at capacity: it was already persisted, so it
        is never counted as a dropped record and never duplicated.  Only a
        genuinely new message rejected because the archive is full returns
        False and increments ``dropped_count``.
        """
        with self._connection() as connection:
            exists = connection.execute(
                "SELECT 1 FROM raw_messages WHERE message_id = ?",
                (message.message_id,),
            ).fetchone()
            if exists is not None:
                return True
            count = connection.execute("SELECT COUNT(*) FROM raw_messages").fetchone()
            if int(count[0]) >= self._raw_capacity:
                self._dropped_raw += 1
                return False
            connection.execute(
                "INSERT OR IGNORE INTO raw_messages "
                "(message_id, exchange, market_type, connection_id, stream_name, payload, "
                "received_at, received_monotonic_ns, receive_sequence, source_timestamp_unit) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    message.message_id,
                    message.exchange,
                    message.market_type,
                    message.connection_id,
                    message.stream_name,
                    message.payload_bytes,
                    message.received_at.isoformat(),
                    message.received_monotonic_ns,
                    message.receive_sequence,
                    message.source_timestamp_unit,
                ),
            )
            return True

    @property
    def dropped_count(self) -> int:
        """Return raw records intentionally dropped because the archive was full."""
        return self._dropped_raw

    async def raw_snapshot(self) -> tuple[RawMarketMessage, ...]:
        """Return stored raw records in capture (arrival) order for replay."""
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT message_id, exchange, market_type, connection_id, stream_name, "
                "payload, received_at, received_monotonic_ns, receive_sequence, "
                "source_timestamp_unit FROM raw_messages ORDER BY id"
            ).fetchall()
        messages: list[RawMarketMessage] = []
        for row in rows:
            messages.append(
                RawMarketMessage(
                    exchange=cast(ExchangeId, str(row[1])),
                    market_type=cast(Any, "spot"),
                    connection_id=str(row[3]),
                    stream_name=str(row[4]),
                    payload_bytes=bytes(row[5]),
                    received_at=datetime.fromisoformat(str(row[6])),
                    received_monotonic_ns=int(row[7]),
                    receive_sequence=int(row[8]),
                    source_timestamp_unit=cast(Any, str(row[9])),
                )
            )
        return tuple(messages)
