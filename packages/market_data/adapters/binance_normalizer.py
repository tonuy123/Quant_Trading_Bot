"""Provider-specific Binance Spot payload mapping behind the adapter boundary."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from packages.market_data.adapters.binance_ws import BinanceSymbolMapper
from packages.market_data.adapters.value_types import (
    IngestionSource,
    MarketSymbol,
    RawMarketMessage,
)
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)


class BinancePayloadError(ValueError):
    """A provider payload cannot be normalized into a canonical event."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code


class BinanceSpotNormalizer:
    """Normalize Binance public WebSocket and REST payloads through one mapper."""

    def normalize(self, message: RawMarketMessage, source: IngestionSource) -> MarketEvent | None:
        """Normalize one raw provider message, ignoring partial klines."""
        try:
            decoded = json.loads(message.payload_text)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BinancePayloadError(
                "transport_decode", "invalid UTF-8 or JSON payload"
            ) from error
        payload = (
            decoded.get("data") if isinstance(decoded, dict) and "data" in decoded else decoded
        )
        if isinstance(payload, list):
            return self._from_rest_kline(payload, message, source)
        if not isinstance(payload, dict):
            raise BinancePayloadError("schema", "Binance payload must be an object or kline array")
        event_name = payload.get("e")
        if event_name == "trade":
            return self._from_trade(payload, message, source)
        if event_name == "24hrTicker":
            return self._from_ws_ticker(payload, message, source)
        if event_name == "kline":
            return self._from_ws_kline(payload, message, source)
        if source == "rest_snapshot":
            return self._from_rest_ticker(payload, message)
        raise BinancePayloadError("unknown_event", "unsupported Binance public event type")

    def _from_trade(
        self,
        payload: dict[str, Any],
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> TradeEvent:
        symbol = self._symbol(payload.get("s"), message.stream_name)
        occurred_at = self._timestamp(payload.get("T"), "trade time")
        exchange_event_at = self._timestamp(payload.get("E"), "event time")
        price = self._decimal(payload.get("p"), "price", positive=True)
        quantity = self._decimal(payload.get("q"), "quantity", positive=True)
        with localcontext() as context:
            context.prec = 50
            quote_quantity = price * quantity
        trade_id = self._identifier(payload.get("t"), "trade id")
        return TradeEvent(
            event_id=self._event_id("trade", symbol.canonical, trade_id),
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            source=source,
            occurred_at=occurred_at,
            exchange_event_at=exchange_event_at,
            received_at=message.received_at,
            connection_id=message.connection_id,
            receive_sequence=message.receive_sequence,
            raw_message_id=message.message_id,
            trade_id=trade_id,
            price=price,
            quantity=quantity,
            quote_quantity=quote_quantity,
            is_buyer_maker=self._boolean(payload.get("m"), "is buyer maker"),
        )

    def _from_ws_ticker(
        self,
        payload: dict[str, Any],
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> TickerEvent:
        return self._ticker(
            symbol=self._symbol(payload.get("s"), message.stream_name),
            bid_price=payload.get("b"),
            bid_quantity=payload.get("B"),
            ask_price=payload.get("a"),
            ask_quantity=payload.get("A"),
            last_price=payload.get("c"),
            last_quantity=payload.get("Q"),
            window_open=payload.get("O"),
            window_close=payload.get("C"),
            open_price=payload.get("o"),
            high_price=payload.get("h"),
            low_price=payload.get("l"),
            base_volume=payload.get("v"),
            quote_volume=payload.get("q"),
            first_trade_id=payload.get("F"),
            last_trade_id=payload.get("L"),
            trade_count=payload.get("n"),
            occurred_at=payload.get("E"),
            exchange_event_at=payload.get("E"),
            message=message,
            source=source,
        )

    def _from_rest_ticker(self, payload: dict[str, Any], message: RawMarketMessage) -> TickerEvent:
        return self._ticker(
            symbol=self._symbol(payload.get("symbol"), message.stream_name),
            bid_price=payload.get("bidPrice"),
            bid_quantity=payload.get("bidQty"),
            ask_price=payload.get("askPrice"),
            ask_quantity=payload.get("askQty"),
            last_price=payload.get("lastPrice"),
            last_quantity=payload.get("lastQty"),
            window_open=payload.get("openTime"),
            window_close=payload.get("closeTime"),
            open_price=payload.get("openPrice"),
            high_price=payload.get("highPrice"),
            low_price=payload.get("lowPrice"),
            base_volume=payload.get("volume"),
            quote_volume=payload.get("quoteVolume"),
            first_trade_id=payload.get("firstId"),
            last_trade_id=payload.get("lastId"),
            trade_count=payload.get("count"),
            occurred_at=payload.get("closeTime"),
            exchange_event_at=payload.get("closeTime"),
            message=message,
            source="rest_snapshot",
        )

    def _ticker(
        self,
        *,
        symbol: Any,
        bid_price: Any,
        bid_quantity: Any,
        ask_price: Any,
        ask_quantity: Any,
        last_price: Any,
        last_quantity: Any,
        window_open: Any,
        window_close: Any,
        open_price: Any,
        high_price: Any,
        low_price: Any,
        base_volume: Any,
        quote_volume: Any,
        first_trade_id: Any,
        last_trade_id: Any,
        trade_count: Any,
        occurred_at: Any,
        exchange_event_at: Any,
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> TickerEvent:
        market_symbol = self._symbol(symbol, message.stream_name)
        close_at = self._timestamp(window_close, "ticker close time")
        return TickerEvent(
            event_id=self._event_id(
                "ticker",
                market_symbol.canonical,
                str(close_at.timestamp()),
                self._identifier(last_trade_id, "last trade id"),
            ),
            exchange="binance",
            market_type="spot",
            symbol=market_symbol,
            source=source,
            occurred_at=self._timestamp(occurred_at, "ticker event time"),
            exchange_event_at=self._timestamp(exchange_event_at, "ticker event time"),
            received_at=message.received_at,
            connection_id=message.connection_id,
            receive_sequence=message.receive_sequence,
            raw_message_id=message.message_id,
            bid_price=self._decimal(bid_price, "bid price", positive=True),
            bid_quantity=self._decimal(bid_quantity, "bid quantity"),
            ask_price=self._decimal(ask_price, "ask price", positive=True),
            ask_quantity=self._decimal(ask_quantity, "ask quantity"),
            last_price=self._decimal(last_price, "last price", positive=True),
            last_quantity=self._decimal(last_quantity, "last quantity"),
            window_open_at=self._timestamp(window_open, "ticker open time"),
            window_close_at=close_at,
            open_price=self._decimal(open_price, "open price", positive=True),
            high_price=self._decimal(high_price, "high price", positive=True),
            low_price=self._decimal(low_price, "low price", positive=True),
            base_volume=self._decimal(base_volume, "base volume"),
            quote_volume=self._decimal(quote_volume, "quote volume"),
            first_trade_id=self._identifier(first_trade_id, "first trade id"),
            last_trade_id=self._identifier(last_trade_id, "last trade id"),
            trade_count=self._integer(trade_count, "trade count"),
        )

    def _from_ws_kline(
        self,
        payload: dict[str, Any],
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> CandleClosedEvent | None:
        kline = payload.get("k")
        if not isinstance(kline, dict):
            raise BinancePayloadError("schema", "kline event is missing kline object")
        if kline.get("x") is not True:
            return None
        return self._candle(
            symbol=self._symbol(kline.get("s"), message.stream_name),
            interval=self._text(kline.get("i"), "interval"),
            open_time=kline.get("t"),
            inclusive_close_time=kline.get("T"),
            open_price=kline.get("o"),
            high_price=kline.get("h"),
            low_price=kline.get("l"),
            close_price=kline.get("c"),
            base_volume=kline.get("v"),
            quote_volume=kline.get("q"),
            trade_count=kline.get("n"),
            taker_base=kline.get("V"),
            taker_quote=kline.get("Q"),
            event_time=payload.get("E"),
            message=message,
            source=source,
        )

    def _from_rest_kline(
        self,
        payload: list[Any],
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> CandleClosedEvent:
        if len(payload) < 11:
            raise BinancePayloadError("schema", "REST kline has too few fields")
        symbol, interval = self._stream_identity(message.stream_name)
        return self._candle(
            symbol=symbol,
            interval=interval,
            open_time=payload[0],
            inclusive_close_time=payload[6],
            open_price=payload[1],
            high_price=payload[2],
            low_price=payload[3],
            close_price=payload[4],
            base_volume=payload[5],
            quote_volume=payload[7],
            trade_count=payload[8],
            taker_base=payload[9],
            taker_quote=payload[10],
            event_time=payload[6],
            message=message,
            source=source,
        )

    def _candle(
        self,
        *,
        symbol: Any,
        interval: str,
        open_time: Any,
        inclusive_close_time: Any,
        open_price: Any,
        high_price: Any,
        low_price: Any,
        close_price: Any,
        base_volume: Any,
        quote_volume: Any,
        trade_count: Any,
        taker_base: Any,
        taker_quote: Any,
        event_time: Any,
        message: RawMarketMessage,
        source: IngestionSource,
    ) -> CandleClosedEvent:
        market_symbol = self._symbol(symbol, message.stream_name)
        opened_at = self._timestamp(open_time, "candle open time")
        inclusive_closed_at = self._timestamp(inclusive_close_time, "candle close time")
        closed_at = inclusive_closed_at + timedelta(milliseconds=1)
        if self._next_candle_open(opened_at, interval) != closed_at:
            raise BinancePayloadError(
                "candle_time",
                "candle close boundary does not match the declared interval",
            )
        return CandleClosedEvent(
            event_id=self._event_id(
                "candle", market_symbol.canonical, interval, opened_at.isoformat()
            ),
            exchange="binance",
            market_type="spot",
            symbol=market_symbol,
            source=source,
            occurred_at=inclusive_closed_at,
            exchange_event_at=self._timestamp(event_time, "candle event time"),
            received_at=message.received_at,
            connection_id=message.connection_id,
            receive_sequence=message.receive_sequence,
            raw_message_id=message.message_id,
            interval=interval,
            open_time=opened_at,
            close_time=closed_at,
            open_price=self._decimal(open_price, "open price", positive=True),
            high_price=self._decimal(high_price, "high price", positive=True),
            low_price=self._decimal(low_price, "low price", positive=True),
            close_price=self._decimal(close_price, "close price", positive=True),
            base_volume=self._decimal(base_volume, "base volume"),
            quote_volume=self._decimal(quote_volume, "quote volume"),
            trade_count=self._integer(trade_count, "trade count"),
            taker_buy_base_volume=self._decimal(taker_base, "taker base volume"),
            taker_buy_quote_volume=self._decimal(taker_quote, "taker quote volume"),
        )

    def _symbol(self, provider_symbol: Any, stream_name: str) -> MarketSymbol:
        if isinstance(provider_symbol, MarketSymbol):
            return provider_symbol
        if provider_symbol is None:
            symbol, _ = self._stream_identity(stream_name)
            return symbol
        if not isinstance(provider_symbol, str):
            raise BinancePayloadError("identity", "provider symbol must be text")
        symbol = BinanceSymbolMapper.from_binance_symbol(provider_symbol)
        stream_symbol, _ = self._stream_identity(stream_name)
        if stream_symbol.canonical != symbol.canonical:
            raise BinancePayloadError("identity_mismatch", "payload symbol does not match stream")
        return symbol

    def _stream_identity(self, stream_name: str) -> tuple[MarketSymbol, str]:
        head, separator, tail = stream_name.partition("@")
        if not separator or not head:
            raise BinancePayloadError("identity", "stream name is required")
        kind = tail
        if kind.startswith("kline_"):
            return BinanceSymbolMapper.from_binance_symbol(head.upper()), kind.removeprefix(
                "kline_"
            )
        return BinanceSymbolMapper.from_binance_symbol(head.upper()), ""

    @staticmethod
    def _next_candle_open(opened_at: datetime, interval: str) -> datetime:
        if len(interval) < 2:
            raise BinancePayloadError("interval", "candle interval is invalid")
        try:
            amount = int(interval[:-1])
        except ValueError as error:
            raise BinancePayloadError("interval", "candle interval is invalid") from error
        if amount < 1:
            raise BinancePayloadError("interval", "candle interval is invalid")
        unit = interval[-1]
        seconds_by_unit = {"s": 1, "m": 60, "h": 3_600, "d": 86_400, "w": 604_800}
        if unit in seconds_by_unit:
            return opened_at + timedelta(seconds=amount * seconds_by_unit[unit])
        if (
            unit != "M"
            or opened_at.day != 1
            or opened_at.hour
            or opened_at.minute
            or opened_at.second
        ):
            raise BinancePayloadError("interval", "unsupported candle interval boundary")
        next_month = opened_at.month + amount
        year = opened_at.year + (next_month - 1) // 12
        month = (next_month - 1) % 12 + 1
        return opened_at.replace(year=year, month=month)

    @staticmethod
    def _timestamp(value: Any, field_name: str) -> datetime:
        if isinstance(value, bool) or not isinstance(value, int):
            raise BinancePayloadError("timestamp", f"{field_name} must be integer milliseconds")
        return datetime.fromtimestamp(value / 1000, tz=UTC)

    @staticmethod
    def _decimal(value: Any, field_name: str, *, positive: bool = False) -> Decimal:
        if (
            isinstance(value, float)
            or isinstance(value, bool)
            or not isinstance(value, (str, Decimal, int))
        ):
            raise BinancePayloadError("numeric", f"{field_name} must not be a float")
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, ValueError) as error:
            raise BinancePayloadError("numeric", f"{field_name} is not decimal") from error
        if not parsed.is_finite() or (positive and parsed <= Decimal("0")) or parsed < Decimal("0"):
            raise BinancePayloadError("numeric", f"{field_name} is invalid")
        return parsed

    @staticmethod
    def _integer(value: Any, field_name: str) -> int:
        if isinstance(value, bool):
            raise BinancePayloadError("schema", f"{field_name} must be an integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as error:
            raise BinancePayloadError("schema", f"{field_name} must be an integer") from error
        if parsed < 0:
            raise BinancePayloadError("numeric", f"{field_name} must be non-negative")
        return parsed

    @staticmethod
    def _identifier(value: Any, field_name: str) -> str:
        if value is None or isinstance(value, bool):
            raise BinancePayloadError("schema", f"{field_name} is required")
        result = str(value)
        if not result:
            raise BinancePayloadError("schema", f"{field_name} is required")
        return result

    @staticmethod
    def _text(value: Any, field_name: str) -> str:
        if not isinstance(value, str) or not value:
            raise BinancePayloadError("schema", f"{field_name} is required")
        return value

    @staticmethod
    def _boolean(value: Any, field_name: str) -> bool:
        if not isinstance(value, bool):
            raise BinancePayloadError("schema", f"{field_name} must be boolean")
        return value

    @staticmethod
    def _event_id(*parts: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, "|".join(parts)))
