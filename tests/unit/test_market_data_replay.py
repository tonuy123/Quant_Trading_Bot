"""MD-013 deterministic replay of captured raw market records."""

from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import pytest

from packages.market_data.adapters.value_types import MarketSymbol, StreamSubscription
from packages.market_data.contracts.events import CandleClosedEvent, DataGapDetected, TradeEvent
from packages.market_data.persistence.sqlite_store import SqliteMarketDataStore
from packages.market_data.services.replay import ReplayRunner
from tests.fixtures.binance_payloads import (
    BASE_EPOCH_MS,
    MALFORMED_JSON_BYTES,
    kline_payload,
    raw_message,
    trade_payload,
)

BTCUSDT = MarketSymbol("BTC", "USDT")
TRADE_SUB = StreamSubscription("binance", "spot", BTCUSDT, "trade")
KLINE_SUB = StreamSubscription("binance", "spot", BTCUSDT, "kline", "1m")

FIXED_CLOCK = datetime.fromtimestamp(BASE_EPOCH_MS / 1000, UTC)

TRADE_RECORD = raw_message(
    trade_payload(trade_id=1),
    stream_name="btcusdt@trade",
    sequence=1,
    received_at=FIXED_CLOCK,
)
KLINE_RECORD_1 = raw_message(
    kline_payload(open_time=BASE_EPOCH_MS),
    stream_name="btcusdt@kline_1m",
    sequence=2,
    received_at=FIXED_CLOCK,
)
KLINE_RECORD_2 = raw_message(
    kline_payload(open_time=BASE_EPOCH_MS + 60_000),
    stream_name="btcusdt@kline_1m",
    sequence=3,
    received_at=FIXED_CLOCK,
)
KLINE_RECORD_3 = raw_message(
    kline_payload(open_time=BASE_EPOCH_MS + 120_000),
    stream_name="btcusdt@kline_1m",
    sequence=4,
    received_at=FIXED_CLOCK,
)


@pytest.fixture
def subscriptions() -> frozenset[StreamSubscription]:
    return frozenset({TRADE_SUB, KLINE_SUB})


class TestReplayReproduction:
    """Events and dispositions reproduce from stored records."""

    async def test_emits_events_in_record_order(
        self,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        result = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            [TRADE_RECORD, KLINE_RECORD_1, KLINE_RECORD_2],
            subscriptions=subscriptions,
        )

        assert result.accepted_count == 3
        assert result.duplicate_count == 0
        assert result.quarantined_count == 0
        assert result.ignored_count == 0
        assert [type(event) for event in result.emitted_events] == [
            TradeEvent,
            CandleClosedEvent,
            CandleClosedEvent,
        ]

    async def test_dispositions_cover_duplicates_and_malformed(
        self,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        duplicate_trade = raw_message(
            trade_payload(trade_id=1),
            stream_name="btcusdt@trade",
            sequence=5,
            received_at=FIXED_CLOCK,
        )
        malformed = raw_message(
            MALFORMED_JSON_BYTES,
            stream_name="unknown",
            sequence=6,
            received_at=FIXED_CLOCK,
        )
        result = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            [TRADE_RECORD, duplicate_trade, malformed, KLINE_RECORD_1],
            subscriptions=subscriptions,
        )

        assert result.accepted_count == 2
        assert result.duplicate_count == 1
        assert result.quarantined_count == 1
        assert result.ignored_count == 0
        assert result.results[2].quarantine is not None
        assert result.results[2].quarantine.reason_code == "transport_decode"


class TestReplayDeterminism:
    """Identical records produce identical market events on every run."""

    async def test_repeated_replay_is_identical_including_gap_detection(
        self,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        records = [KLINE_RECORD_1, KLINE_RECORD_3]

        first = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            records,
            subscriptions=subscriptions,
        )
        second = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            records,
            subscriptions=subscriptions,
        )

        assert first.emitted_events == second.emitted_events
        assert [type(event) for event in first.emitted_events] == [
            CandleClosedEvent,
            DataGapDetected,
            CandleClosedEvent,
        ]
        gap = first.emitted_events[1]
        assert isinstance(gap, DataGapDetected)
        assert gap.detection_basis == "missing_kline_slot"
        assert gap.certainty == "confirmed"
        assert gap.recoverability == "closed_candles"
        assert gap.gap_start_at == FIXED_CLOCK + timedelta(seconds=60)
        assert gap.gap_end_at == FIXED_CLOCK + timedelta(seconds=120)
        assert first.accepted_count == second.accepted_count == 2

    async def test_receipt_metadata_does_not_change_market_events(
        self,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        later_receipt = raw_message(
            trade_payload(trade_id=1),
            stream_name="btcusdt@trade",
            sequence=99,
            connection_id="other-connection",
            received_at=datetime.fromtimestamp((BASE_EPOCH_MS + 3_600_000) / 1000, UTC),
        )

        original = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            [TRADE_RECORD],
            subscriptions=subscriptions,
        )
        replayed = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            [later_receipt],
            subscriptions=subscriptions,
        )

        assert len(original.emitted_events) == len(replayed.emitted_events) == 1
        receipt_fields = {
            "received_at",
            "receive_sequence",
            "raw_message_id",
            "connection_id",
        }
        original_dict = {
            key: value
            for key, value in asdict(original.emitted_events[0]).items()
            if key not in receipt_fields
        }
        replayed_dict = {
            key: value
            for key, value in asdict(replayed.emitted_events[0]).items()
            if key not in receipt_fields
        }
        assert original_dict == replayed_dict


class TestReplayEndToEnd:
    """Stored raw records replay without any network."""

    async def test_store_snapshot_replays_without_network(
        self,
        tmp_path,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        store = SqliteMarketDataStore(tmp_path / "research.db")
        records = [TRADE_RECORD, KLINE_RECORD_1, KLINE_RECORD_2, KLINE_RECORD_3]
        for record in records:
            assert await store.capture(record) is True

        snapshot = await store.raw_snapshot()
        assert len(snapshot) == len(records)

        result = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            snapshot,
            subscriptions=subscriptions,
        )

        assert result.accepted_count == len(records)
        assert result.duplicate_count == 0
        assert result.quarantined_count == 0
        assert [type(event) for event in result.emitted_events] == [
            TradeEvent,
            CandleClosedEvent,
            CandleClosedEvent,
            CandleClosedEvent,
        ]

    async def test_replay_of_gapped_archive_detects_recoverable_gap(
        self,
        subscriptions: frozenset[StreamSubscription],
    ) -> None:
        result = await ReplayRunner(clock=lambda: FIXED_CLOCK).replay(
            [KLINE_RECORD_1, KLINE_RECORD_3],
            subscriptions=subscriptions,
        )

        gap = next(event for event in result.emitted_events if isinstance(event, DataGapDetected))
        assert gap.interval == "1m"
        assert gap.stream_kind == "kline"
        assert gap.gap_start_at == KLINE_RECORD_1.received_at + timedelta(seconds=60)
        assert gap.gap_end_at == KLINE_RECORD_3.received_at + timedelta(seconds=120)
