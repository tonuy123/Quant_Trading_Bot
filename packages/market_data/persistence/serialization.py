"""Exact JSON-safe serialization for canonical Market Data events (MD-011).

Decimal values round-trip exactly as canonical decimal strings, and aware UTC
timestamps round-trip through ISO-8601.  Floats never appear in stored records.
The stored payload never contains credentials or provider secrets.
"""

from __future__ import annotations

import types
from dataclasses import fields
from datetime import datetime
from decimal import Decimal
from typing import Any, Union, get_args, get_origin, get_type_hints

from packages.market_data.adapters.value_types import MarketSymbol
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    ConnectionStatusChanged,
    DataGapDetected,
    MarketDataStale,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)

_EVENT_TYPES: dict[str, type[MarketEvent]] = {
    "market.trade": TradeEvent,
    "market.ticker": TickerEvent,
    "market.candle.closed": CandleClosedEvent,
    "market.connection.status_changed": ConnectionStatusChanged,
    "market.data_gap.detected": DataGapDetected,
    "market.data.stale": MarketDataStale,
}


def event_to_record(event: MarketEvent) -> dict[str, Any]:
    """Convert one canonical event into a JSON-safe record dict."""
    record: dict[str, Any] = {"event_type": event.event_type}
    for field in fields(event):
        record[field.name] = _jsonable(getattr(event, field.name))
    return record


def record_to_event(record: dict[str, Any]) -> MarketEvent:
    """Restore one canonical event from a JSON-safe record dict."""
    event_type = record.get("event_type")
    if not isinstance(event_type, str):
        raise ValueError("market event record is missing event_type")
    event_cls = _EVENT_TYPES.get(event_type)
    if event_cls is None:
        raise ValueError(f"unsupported market event type: {event_type}")
    hints = get_type_hints(event_cls)
    kwargs: dict[str, Any] = {}
    for field in fields(event_cls):
        if not field.init:
            continue
        if field.name not in record:
            raise ValueError(f"market event record is missing field: {field.name}")
        kwargs[field.name] = _restore(hints[field.name], record[field.name])
    return event_cls(**kwargs)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, MarketSymbol):
        return {"base": value.base, "quote": value.quote}
    if isinstance(value, frozenset):
        return sorted(value)
    return value


def _restore(annotation: Any, value: Any) -> Any:
    if value is None:
        return None
    origin = get_origin(annotation)
    if origin in (types.UnionType, Union):
        for candidate in get_args(annotation):
            if candidate is type(None):
                continue
            try:
                return _restore(candidate, value)
            except ValueError:
                continue
        raise ValueError(f"cannot restore union {annotation} from {type(value).__name__}")
    if annotation is Decimal:
        return Decimal(str(value))
    if annotation is datetime:
        return datetime.fromisoformat(str(value))
    if annotation is MarketSymbol:
        if not isinstance(value, dict) or "base" not in value or "quote" not in value:
            raise ValueError("market symbol record is invalid")
        return MarketSymbol(str(value["base"]), str(value["quote"]))
    if origin is frozenset:
        return frozenset(str(item) for item in value)
    if isinstance(value, bool) or isinstance(value, (str, int)):
        return value
    raise ValueError(f"cannot restore {annotation} from {type(value).__name__}")
