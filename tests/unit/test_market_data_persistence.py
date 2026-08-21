"""MD-011 local research persistence: idempotency, atomicity, restart, isolation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from packages.market_data.adapters.binance_normalizer import BinanceSpotNormalizer
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    DataGapDetected,
    TickerEvent,
    TradeEvent,
)
from packages.market_data.persistence.serialization import event_to_record, record_to_event
from packages.market_data.persistence.sqlite_store import SqliteMarketDataStore
from packages.market_data.services.normalization import NormalizationPipeline
from tests.fixtures.binance_payloads import (
    BASE_EPOCH_MS,
    BTCUSDT,
    MALFORMED_JSON_BYTES,
    kline_payload,
    raw_message,
    ticker_payload,
    trade_payload,
)

KLINE_STREAM = "btcusdt@kline_1m"
TICKER_STREAM = "btcusdt@ticker"
TRADE_STREAM = "btcusdt@trade"


def normalize(payload: object, *, stream_name: str, sequence: int = 1):
    """Normalize one fixture payload into a canonical event."""
    return BinanceSpotNormalizer().normalize(
        raw_message(payload, stream_name=stream_name, sequence=sequence),
        "websocket",
    )


def candle_event(*, open_time: int = BASE_EPOCH_MS, sequence: int = 1) -> CandleClosedEvent:
    event = normalize(
        kline_payload(open_time=open_time), stream_name=KLINE_STREAM, sequence=sequence
    )
    assert isinstance(event, CandleClosedEvent)
    return event


def ticker_event(*, event_time: int = BASE_EPOCH_MS, sequence: int = 1) -> TickerEvent:
    payload = ticker_payload()
    payload = {**payload, "E": event_time}
    event = normalize(payload, stream_name=TICKER_STREAM, sequence=sequence)
    assert isinstance(event, TickerEvent)
    return event


def trade_event(*, trade_id: int = 9, sequence: int = 1) -> TradeEvent:
    event = normalize(trade_payload(trade_id=trade_id), stream_name=TRADE_STREAM, sequence=sequence)
    assert isinstance(event, TradeEvent)
    return event


@pytest.fixture
def store(tmp_path) -> SqliteMarketDataStore:
    """Create a file-backed store in a temporary directory."""
    return SqliteMarketDataStore(tmp_path / "research.db")


class TestCandlePersistence:
    """Natural-key idempotency, watermark semantics, and atomic commit."""

    @pytest.mark.asyncio
    async def test_candle_watermark_outbox_commit_atomically(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        event = candle_event()
        result = await store.persist_closed_candle_and_outbox(event)

        assert result.candle == "inserted"
        assert result.watermark == "advanced"
        assert result.outbox == "inserted"
        assert await store.get_kline_watermark("binance", BTCUSDT, "1m") == event.open_time
        candles = await store.list_candles(BTCUSDT, "1m")
        assert len(candles) == 1
        assert candles[0] == event
        outbox = await store.list_undelivered_outbox()
        assert len(outbox) == 1
        assert outbox[0] == event

    @pytest.mark.asyncio
    async def test_duplicate_persist_is_idempotent(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        event = candle_event()
        await store.persist_closed_candle_and_outbox(event)
        second = await store.persist_closed_candle_and_outbox(event)

        assert second.candle == "duplicate"
        assert second.watermark == "unchanged"
        assert second.outbox == "duplicate"
        assert len(await store.list_candles(BTCUSDT, "1m")) == 1

    @pytest.mark.asyncio
    async def test_watermark_advances_with_max_and_never_rewinds(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        first = candle_event(open_time=BASE_EPOCH_MS)
        later = candle_event(open_time=BASE_EPOCH_MS + 120_000)
        stale = candle_event(open_time=BASE_EPOCH_MS - 60_000)

        await store.persist_closed_candle_and_outbox(first)
        await store.persist_closed_candle_and_outbox(later)
        result = await store.persist_closed_candle_and_outbox(stale)

        assert result.candle == "inserted"
        assert result.watermark == "unchanged"
        assert await store.get_kline_watermark("binance", BTCUSDT, "1m") == later.open_time

    @pytest.mark.asyncio
    async def test_candle_range_query_uses_half_open_boundaries(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        earlier = candle_event(open_time=BASE_EPOCH_MS)
        later = candle_event(open_time=BASE_EPOCH_MS + 60_000)
        await store.persist_closed_candle_and_outbox(earlier)
        await store.persist_closed_candle_and_outbox(later)

        start = datetime.fromtimestamp(BASE_EPOCH_MS / 1000, UTC)
        result = await store.list_candles(
            BTCUSDT,
            "1m",
            start_inclusive=start,
            end_exclusive=start + timedelta(minutes=1),
        )
        assert result == (earlier,)

    @pytest.mark.asyncio
    async def test_restart_reload_preserves_dataset(
        self,
        tmp_path,
    ) -> None:
        path = tmp_path / "research.db"
        first_store = SqliteMarketDataStore(path)
        event = candle_event()
        await first_store.persist_closed_candle_and_outbox(event)
        await first_store.capture(raw_message(trade_payload(), stream_name=TRADE_STREAM))

        reloaded = SqliteMarketDataStore(path)
        assert await reloaded.get_kline_watermark("binance", BTCUSDT, "1m") == event.open_time
        assert await reloaded.list_candles(BTCUSDT, "1m") == (event,)
        snapshot = await reloaded.raw_snapshot()
        assert len(snapshot) == 1


class TestTickerAndGapStorage:
    """Last-write-wins ticker cache and durable gap recovery outcome."""

    @pytest.mark.asyncio
    async def test_ticker_cache_is_last_write_wins(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        fresh = ticker_event(event_time=BASE_EPOCH_MS)
        older = ticker_event(event_time=BASE_EPOCH_MS - 60_000)

        assert await store.set_ticker_cache(fresh) is True
        assert await store.set_ticker_cache(older) is False
        assert await store.get_ticker_cache(BTCUSDT) == fresh

    @pytest.mark.asyncio
    async def test_gap_row_records_recovery_outcome(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        first = await pipeline.process(
            raw_message(kline_payload(open_time=BASE_EPOCH_MS), stream_name=KLINE_STREAM)
        )
        missing = await pipeline.process(
            raw_message(
                kline_payload(open_time=BASE_EPOCH_MS + 120_000),
                stream_name=KLINE_STREAM,
                sequence=2,
            )
        )
        assert isinstance(first.event, CandleClosedEvent)
        assert missing.gap is not None
        gap = missing.gap.event
        assert isinstance(gap, DataGapDetected)

        await store.create_or_update_gap(gap)
        recorded = await store.get_gap(gap.gap_id)
        assert recorded is not None
        assert recorded.recovery_state == "pending"

        recovered = gap.gap_start_at + timedelta(minutes=1)
        await store.mark_gap_recovery(
            gap.gap_id,
            outcome="recovered",
            recovered_open_times=(recovered,),
            unresolved_open_times=(),
        )
        updated = await store.get_gap(gap.gap_id)
        assert updated is not None
        assert updated.recovery_state == "recovered"
        assert updated.recovered_open_times == (recovered,)
        assert updated.unresolved_open_times == ()


class TestRawArchive:
    """Bounded raw capture with malformed-record isolation."""

    @pytest.mark.asyncio
    async def test_raw_capture_is_bounded_and_drops_newest(
        self,
        tmp_path,
    ) -> None:
        store = SqliteMarketDataStore(tmp_path / "research.db", raw_capacity=2)
        first = raw_message(trade_payload(trade_id=1), stream_name=TRADE_STREAM, sequence=1)
        second = raw_message(trade_payload(trade_id=2), stream_name=TRADE_STREAM, sequence=2)
        third = raw_message(trade_payload(trade_id=3), stream_name=TRADE_STREAM, sequence=3)

        assert await store.capture(first) is True
        assert await store.capture(second) is True
        assert await store.capture(third) is False
        assert store.dropped_count == 1
        assert len(await store.raw_snapshot()) == 2

    @pytest.mark.asyncio
    async def test_existing_message_id_is_idempotent_even_at_capacity(
        self,
        tmp_path,
    ) -> None:
        store = SqliteMarketDataStore(tmp_path / "research.db", raw_capacity=2)
        first = raw_message(trade_payload(trade_id=1), stream_name=TRADE_STREAM, sequence=1)
        second = raw_message(trade_payload(trade_id=2), stream_name=TRADE_STREAM, sequence=2)
        third = raw_message(trade_payload(trade_id=3), stream_name=TRADE_STREAM, sequence=3)

        assert await store.capture(first) is True
        assert await store.capture(second) is True
        assert await store.capture(third) is False
        assert await store.capture(second) is True
        assert store.dropped_count == 1
        assert len(await store.raw_snapshot()) == 2

    @pytest.mark.asyncio
    async def test_malformed_raw_record_does_not_corrupt_dataset(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        malformed = raw_message(MALFORMED_JSON_BYTES, stream_name="unknown", sequence=1)
        assert await store.capture(malformed) is True
        event = candle_event()
        await store.persist_closed_candle_and_outbox(event)

        snapshot = await store.raw_snapshot()
        assert len(snapshot) == 1
        assert snapshot[0].payload_bytes == MALFORMED_JSON_BYTES
        assert await store.list_candles(BTCUSDT, "1m") == (event,)

    @pytest.mark.asyncio
    async def test_outbox_relay_flow(
        self,
        store: SqliteMarketDataStore,
    ) -> None:
        event = candle_event()
        await store.persist_closed_candle_and_outbox(event)
        assert len(await store.list_undelivered_outbox()) == 1

        await store.mark_outbox_relayed(event.event_id)
        assert await store.list_undelivered_outbox() == ()


class TestSerializationRoundTrip:
    """Exact Decimal and aware-UTC round trips across all canonical event kinds."""

    def test_trade_round_trip_is_exact(self) -> None:
        event = trade_event()
        restored = record_to_event(event_to_record(event))

        assert restored == event
        assert isinstance(restored, TradeEvent)
        assert restored.price == event.price
        assert restored.occurred_at.tzinfo is UTC

    def test_candle_round_trip_preserves_decimal_scale(self) -> None:
        event = candle_event()
        restored = record_to_event(event_to_record(event))

        assert restored == event
        assert isinstance(restored, CandleClosedEvent)
        assert str(restored.open_price) == "100"
        assert restored.quality_flags == frozenset()

    def test_unknown_event_type_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported market event type"):
            record_to_event({"event_type": "market.unknown"})
