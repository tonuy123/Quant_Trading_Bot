"""Shared raw-to-canonical normalization, validation, dedupe, and gap handling."""

from __future__ import annotations

import uuid
from collections import OrderedDict
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from packages.market_data.adapters.binance_normalizer import BinancePayloadError
from packages.market_data.adapters.value_types import (
    IngestionSource,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    DataGapDetected,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)
from packages.market_data.services.raw_capture import RawCaptureSink


class ProviderNormalizer(Protocol):
    """Normalize a provider raw message into one canonical event."""

    def normalize(self, message: RawMarketMessage, source: IngestionSource) -> MarketEvent | None:
        """Return a canonical event or None for intentionally ignored input."""
        ...


@dataclass(frozen=True)
class QuarantinedMessage:
    """A structured invalid-data record that never includes raw payload text."""

    raw_message_id: str
    reason_code: str
    detail: str


@dataclass(frozen=True)
class GapState:
    """Observable in-memory result for a confirmed missing candle interval."""

    event: DataGapDetected
    missing_open_times: tuple[datetime, ...]


@dataclass(frozen=True)
class ProcessResult:
    """One normalization attempt result."""

    disposition: Literal["accepted", "duplicate", "quarantined", "ignored"]
    event: MarketEvent | None = None
    emitted_events: tuple[MarketEvent, ...] = ()
    quarantine: QuarantinedMessage | None = None
    gap: GapState | None = None
    raw_capture_failed: bool = False


class NormalizationPipeline:
    """The one canonical validation path for Binance WebSocket and REST input."""

    def __init__(
        self,
        normalizer: ProviderNormalizer,
        *,
        raw_capture: RawCaptureSink | None = None,
        max_payload_bytes: int = 32_768,
        max_trade_dedupe: int = 10_000,
        max_candle_dedupe: int = 10_000,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if min(max_payload_bytes, max_trade_dedupe, max_candle_dedupe) < 1:
            raise ValueError("normalization bounds must be positive")
        self._normalizer = normalizer
        self._raw_capture = raw_capture
        self._max_payload_bytes = max_payload_bytes
        self._max_trade_dedupe = max_trade_dedupe
        self._max_candle_dedupe = max_candle_dedupe
        self._clock = clock or (lambda: datetime.now(UTC))
        self._active_subscriptions: frozenset[StreamSubscription] = frozenset()
        self._trades: OrderedDict[tuple[str, str, str], TradeEvent] = OrderedDict()
        self._candles: OrderedDict[tuple[str, str, str, str], CandleClosedEvent] = OrderedDict()
        self._last_candle: dict[tuple[str, str, str], CandleClosedEvent] = {}
        self._latest_ticker: dict[tuple[str, str], TickerEvent] = {}
        self.raw_capture_failures = 0

    def set_active_subscriptions(self, subscriptions: Collection[StreamSubscription]) -> None:
        """Set the allowed provider identities for incoming events."""
        self._active_subscriptions = frozenset(subscriptions)

    async def process(
        self,
        message: RawMarketMessage,
        *,
        source: IngestionSource = "websocket",
    ) -> ProcessResult:
        """Capture raw input first, then normalize, validate, and deduplicate."""
        raw_capture_failed = await self._capture_raw(message)
        if len(message.payload_bytes) > self._max_payload_bytes:
            return self._quarantine(
                message, "transport_size", "payload exceeds configured limit", raw_capture_failed
            )
        try:
            event = self._normalizer.normalize(message, source)
        except BinancePayloadError as error:
            return self._quarantine(message, error.code, str(error), raw_capture_failed)
        except ValueError as error:
            return self._quarantine(message, "validation", str(error), raw_capture_failed)
        except (KeyError, TypeError, IndexError) as error:
            return self._quarantine(message, "schema", type(error).__name__, raw_capture_failed)
        if event is None:
            return ProcessResult(disposition="ignored", raw_capture_failed=raw_capture_failed)
        if event.exchange != message.exchange or event.market_type != message.market_type:
            return self._quarantine(
                message,
                "transport_identity",
                "canonical event does not match transport identity",
                raw_capture_failed,
            )
        identity_error = self._validate_identity(event)
        if identity_error is not None:
            return self._quarantine(message, "identity", identity_error, raw_capture_failed)
        return self._deduplicate(message, event, raw_capture_failed)

    async def _capture_raw(self, message: RawMarketMessage) -> bool:
        if self._raw_capture is None:
            return False
        try:
            accepted = await self._raw_capture.capture(message)
        except Exception:
            self.raw_capture_failures += 1
            return True
        if not accepted:
            self.raw_capture_failures += 1
            return True
        return False

    def _validate_identity(self, event: MarketEvent) -> str | None:
        if not self._active_subscriptions or event.symbol is None:
            return None
        kind = self._event_kind(event)
        for subscription in self._active_subscriptions:
            if (
                subscription.exchange == event.exchange
                and subscription.market_type == event.market_type
                and subscription.symbol == event.symbol
                and subscription.kind == kind
                and (
                    kind != "kline"
                    or (
                        isinstance(event, CandleClosedEvent)
                        and subscription.interval == event.interval
                    )
                )
            ):
                return None
        return "event does not match an active subscription"

    @staticmethod
    def _event_kind(event: MarketEvent) -> str:
        if isinstance(event, TradeEvent):
            return "trade"
        if isinstance(event, TickerEvent):
            return "ticker"
        if isinstance(event, CandleClosedEvent):
            return "kline"
        raise ValueError("unsupported canonical event for normalization pipeline")

    def _deduplicate(
        self,
        message: RawMarketMessage,
        event: MarketEvent,
        raw_capture_failed: bool,
    ) -> ProcessResult:
        if isinstance(event, TradeEvent):
            trade_key = (
                (event.exchange, event.symbol.canonical, event.trade_id)
                if event.symbol
                else ("", "", "")
            )
            trade_previous = self._trades.get(trade_key)
            if trade_previous is not None:
                if self._trade_signature(trade_previous) != self._trade_signature(event):
                    return self._quarantine(
                        message,
                        "integrity_conflict",
                        "conflicting trade duplicate",
                        raw_capture_failed,
                    )
                return ProcessResult(
                    disposition="duplicate", event=event, raw_capture_failed=raw_capture_failed
                )
            self._remember_trade(trade_key, event)
            return ProcessResult(
                disposition="accepted",
                event=event,
                emitted_events=(event,),
                raw_capture_failed=raw_capture_failed,
            )
        if isinstance(event, TickerEvent):
            ticker_key = (event.exchange, event.symbol.canonical) if event.symbol else ("", "")
            ticker_previous = self._latest_ticker.get(ticker_key)
            if ticker_previous is not None and (
                event.occurred_at,
                event.receive_sequence or -1,
            ) <= (
                ticker_previous.occurred_at,
                ticker_previous.receive_sequence or -1,
            ):
                return ProcessResult(
                    disposition="duplicate", event=event, raw_capture_failed=raw_capture_failed
                )
            self._latest_ticker[ticker_key] = event
            return ProcessResult(
                disposition="accepted",
                event=event,
                emitted_events=(event,),
                raw_capture_failed=raw_capture_failed,
            )
        if isinstance(event, CandleClosedEvent):
            candle_key = (
                event.exchange,
                event.symbol.canonical if event.symbol else "",
                event.interval,
                event.open_time.isoformat(),
            )
            candle_previous = self._candles.get(candle_key)
            if candle_previous is not None:
                if self._candle_signature(candle_previous) == self._candle_signature(event):
                    return ProcessResult(
                        disposition="duplicate",
                        event=event,
                        raw_capture_failed=raw_capture_failed,
                    )
                return self._quarantine(
                    message,
                    "integrity_conflict",
                    "conflicting candle duplicate",
                    raw_capture_failed,
                )
            self._remember_candle(candle_key, event)
            gap = self._detect_gap(event)
            emitted: tuple[MarketEvent, ...] = (event,) if gap is None else (gap.event, event)
            return ProcessResult(
                disposition="accepted",
                event=event,
                emitted_events=emitted,
                gap=gap,
                raw_capture_failed=raw_capture_failed,
            )
        return self._quarantine(
            message,
            "schema",
            "unsupported event type",
            raw_capture_failed,
        )

    def _detect_gap(self, event: CandleClosedEvent) -> GapState | None:
        if event.symbol is None:
            return None
        key = (event.exchange, event.symbol.canonical, event.interval)
        previous = self._last_candle.get(key)
        if previous is None or event.open_time <= previous.open_time:
            self._last_candle[key] = (
                max((previous, event), key=lambda candle: candle.open_time) if previous else event
            )
            return None
        if event.open_time == previous.close_time:
            self._last_candle[key] = event
            return None
        missing: list[datetime] = []
        expected = previous.close_time
        while expected < event.open_time:
            missing.append(expected)
            try:
                expected = self._next_candle_open(expected, event.interval)
            except ValueError:
                self._last_candle[key] = event
                return None
        if expected != event.open_time:
            self._last_candle[key] = event
            return None
        detected_at = self._utc_now()
        gap_event = DataGapDetected(
            event_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"gap|{key}|{previous.close_time.isoformat()}|{event.open_time.isoformat()}",
                )
            ),
            exchange=event.exchange,
            market_type=event.market_type,
            symbol=event.symbol,
            source=event.source,
            occurred_at=detected_at,
            exchange_event_at=None,
            received_at=event.received_at,
            connection_id=event.connection_id,
            gap_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{key}|{previous.close_time.isoformat()}|{event.open_time.isoformat()}",
                )
            ),
            stream_kind="kline",
            interval=event.interval,
            gap_start_at=previous.close_time,
            gap_end_at=event.open_time,
            detection_basis="missing_kline_slot",
            certainty="confirmed",
            recoverability="closed_candles",
            last_known_cursor=previous.open_time.isoformat(),
            affected_subscription_count=1,
        )
        self._last_candle[key] = event
        return GapState(event=gap_event, missing_open_times=tuple(missing))

    @staticmethod
    def _interval(value: str) -> timedelta | None:
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        if len(value) < 2 or value[-1] not in units:
            return None
        try:
            amount = int(value[:-1])
        except ValueError:
            return None
        return timedelta(seconds=amount * units[value[-1]]) if amount > 0 else None

    @classmethod
    def _next_candle_open(cls, open_time: datetime, interval: str) -> datetime:
        delta = cls._interval(interval)
        if delta is not None:
            return open_time + delta
        if not interval.endswith("M") or not interval[:-1].isdigit() or int(interval[:-1]) < 1:
            raise ValueError("unsupported kline interval")
        months = int(interval[:-1])
        next_month = open_time.month + months
        year = open_time.year + (next_month - 1) // 12
        month = (next_month - 1) % 12 + 1
        return open_time.replace(year=year, month=month)

    @staticmethod
    def _candle_signature(event: CandleClosedEvent) -> tuple[object, ...]:
        return (
            event.open_price,
            event.high_price,
            event.low_price,
            event.close_price,
            event.base_volume,
            event.quote_volume,
            event.trade_count,
            event.taker_buy_base_volume,
            event.taker_buy_quote_volume,
            event.close_time,
        )

    @staticmethod
    def _trade_signature(event: TradeEvent) -> tuple[object, ...]:
        return (
            event.price,
            event.quantity,
            event.quote_quantity,
            event.occurred_at,
            event.is_buyer_maker,
        )

    def _remember_trade(self, key: tuple[str, str, str], event: TradeEvent) -> None:
        self._trades[key] = event
        self._trades.move_to_end(key)
        while len(self._trades) > self._max_trade_dedupe:
            self._trades.popitem(last=False)

    def _remember_candle(
        self,
        key: tuple[str, str, str, str],
        event: CandleClosedEvent,
    ) -> None:
        self._candles[key] = event
        self._candles.move_to_end(key)
        while len(self._candles) > self._max_candle_dedupe:
            self._candles.popitem(last=False)

    @staticmethod
    def _quarantine(
        message: RawMarketMessage,
        reason_code: str,
        detail: str,
        raw_capture_failed: bool,
    ) -> ProcessResult:
        return ProcessResult(
            disposition="quarantined",
            quarantine=QuarantinedMessage(
                raw_message_id=message.message_id,
                reason_code=reason_code,
                detail=detail,
            ),
            raw_capture_failed=raw_capture_failed,
        )

    def _utc_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("normalization clock must return UTC-aware datetime")
        return now.astimezone(UTC)
