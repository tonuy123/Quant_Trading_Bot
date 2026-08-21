"""Versioned public Binance payload fixtures for network-free tests (MD-012/MD-013).

These fixtures reproduce the exact provider JSON shapes documented in
``docs/architecture/market-data-layer.md``.  They contain public market data
only: no credentials, no account data, no order payloads.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from packages.market_data.adapters.value_types import MarketSymbol, RawMarketMessage

FIXTURE_VERSION = "2026-08-18"
BASE_EPOCH_MS = 1_786_579_200_000
BTCUSDT = MarketSymbol("BTC", "USDT")

_INTERVAL_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "1h": 3_600_000,
    "1d": 86_400_000,
}


def trade_payload(*, trade_id: int = 9, price: str = "100.25") -> dict[str, object]:
    """Return a valid Binance public aggregate-trade payload."""
    return {
        "e": "trade",
        "E": 1_786_579_200_000,
        "s": "BTCUSDT",
        "t": trade_id,
        "p": price,
        "q": "0.25",
        "T": 1_786_579_199_999,
        "m": False,
    }


def ticker_payload() -> dict[str, object]:
    """Return a valid Binance public websocket 24-hour ticker payload."""
    return {
        "e": "24hrTicker",
        "E": 1_786_579_200_000,
        "s": "BTCUSDT",
        "p": "5",
        "P": "5",
        "w": "102",
        "x": "100",
        "c": "105",
        "Q": "0.2",
        "b": "104",
        "B": "3",
        "a": "106",
        "A": "2",
        "o": "100",
        "h": "110",
        "l": "90",
        "v": "10",
        "q": "1020",
        "O": 1_786_492_800_000,
        "C": 1_786_579_199_999,
        "F": 1,
        "L": 5,
        "n": 5,
    }


def kline_payload(
    *,
    open_time: int = BASE_EPOCH_MS,
    closed: bool = True,
    interval: str = "1m",
    close_price: str = "105",
) -> dict[str, object]:
    """Return a Binance public kline frame with a configurable closure flag."""
    if interval not in _INTERVAL_MS:
        raise ValueError(f"fixture supports only {sorted(_INTERVAL_MS)} intervals")
    return {
        "e": "kline",
        "E": open_time + _INTERVAL_MS[interval],
        "s": "BTCUSDT",
        "k": {
            "t": open_time,
            "T": open_time + _INTERVAL_MS[interval] - 1,
            "s": "BTCUSDT",
            "i": interval,
            "o": "100",
            "h": "110",
            "l": "90",
            "c": close_price,
            "v": "10",
            "n": 3,
            "x": closed,
            "q": "1020",
            "V": "4",
            "Q": "408",
        },
    }


def rest_kline_row(
    *,
    open_time: int = BASE_EPOCH_MS,
    interval: str = "1m",
    close_price: str = "105",
) -> list[object]:
    """Return one Binance public REST kline row (open-time indexed)."""
    if interval not in _INTERVAL_MS:
        raise ValueError(f"fixture supports only {sorted(_INTERVAL_MS)} intervals")
    return [
        open_time,
        "100",
        "110",
        "90",
        close_price,
        "10",
        open_time + _INTERVAL_MS[interval] - 1,
        "1020",
        3,
        "4",
        "408",
        "0",
    ]


def combined_envelope(stream: str, data: dict[str, object]) -> dict[str, object]:
    """Wrap a payload in a Binance combined-stream envelope."""
    return {"stream": stream, "data": data}


def raw_message(
    payload: object,
    *,
    stream_name: str = "btcusdt@trade",
    sequence: int = 1,
    received_at: datetime | None = None,
    connection_id: str = "fixture-connection",
    monotonic_ns: int = 0,
) -> RawMarketMessage:
    """Build an ingress record with exact serialized provider bytes."""
    return RawMarketMessage(
        exchange="binance",
        market_type="spot",
        connection_id=connection_id,
        stream_name=stream_name,
        payload_bytes=(
            payload
            if isinstance(payload, bytes)
            else (
                payload.encode("utf-8")
                if isinstance(payload, str)
                else json.dumps(payload, separators=(",", ":")).encode("utf-8")
            )
        ),
        received_at=received_at or datetime.fromtimestamp(BASE_EPOCH_MS / 1000, UTC),
        received_monotonic_ns=monotonic_ns,
        receive_sequence=sequence,
        source_timestamp_unit="ms",
    )


def float_price_trade_payload(*, trade_id: int = 9) -> dict[str, object]:
    """Return a trade payload with a float price - a malformed provider frame."""
    payload = trade_payload(trade_id=trade_id)
    payload["p"] = 100.25
    return payload


MALFORMED_JSON_BYTES = b"this is not valid json"
