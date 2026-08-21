"""Public Binance Spot REST adapter for server time, klines, and ticker snapshots."""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from packages.market_data.adapters.binance_ws import BinanceSymbolMapper
from packages.market_data.adapters.value_types import MarketSymbol, RawMarketMessage
from packages.market_data.services.rate_limit import BinanceRateLimitCoordinator

BINANCE_PUBLIC_REST_URL = "https://api.binance.com"
SERVER_TIME_WEIGHT = 1
KLINES_WEIGHT = 2
TICKER_SNAPSHOT_WEIGHT = 2


class PublicMarketDataRequestError(RuntimeError):
    """A public REST request failed without an implicit retry."""

    def __init__(self, status_code: int, endpoint: str) -> None:
        super().__init__(f"public Binance request failed: {status_code} {endpoint}")
        self.status_code = status_code
        self.endpoint = endpoint


@dataclass(frozen=True)
class HttpResponse:
    """Minimal injectable public HTTP response."""

    status_code: int
    headers: Mapping[str, str]
    body: bytes


class PublicHttpTransport(Protocol):
    """A no-auth HTTP transport for public endpoints."""

    async def get(self, url: str, params: Mapping[str, str]) -> HttpResponse:
        """Send one public GET request without credentials."""
        ...


class UrllibPublicHttpTransport:
    """Standard-library public GET transport with no auth-header capability."""

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._timeout_seconds = timeout_seconds

    async def get(self, url: str, params: Mapping[str, str]) -> HttpResponse:
        """Run the blocking standard-library public request off the event loop."""
        return await asyncio.to_thread(self._get_sync, url, params)

    def _get_sync(self, url: str, params: Mapping[str, str]) -> HttpResponse:
        request_url = f"{url}?{urllib.parse.urlencode(params)}" if params else url
        request = urllib.request.Request(request_url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                return HttpResponse(
                    status_code=response.status,
                    headers=dict(response.headers.items()),
                    body=response.read(),
                )
        except urllib.error.HTTPError as error:
            return HttpResponse(
                status_code=error.code,
                headers=dict(error.headers.items()) if error.headers else {},
                body=error.read(),
            )
        except urllib.error.URLError as error:
            raise ConnectionError("public Binance request failed") from error


class BinanceSpotRestAdapter:
    """Public-only Binance Spot REST implementation of PublicMarketDataHistory."""

    def __init__(
        self,
        *,
        base_url: str = BINANCE_PUBLIC_REST_URL,
        transport: PublicHttpTransport | None = None,
        rate_limiter: BinanceRateLimitCoordinator | None = None,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], int] | None = None,
    ) -> None:
        self._validate_public_base_url(base_url)
        self._base_url = base_url.rstrip("/")
        self._transport = transport or UrllibPublicHttpTransport()
        self._limiter = rate_limiter or BinanceRateLimitCoordinator()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic_clock = monotonic_clock or time.monotonic_ns
        self._sequence = 0

    async def get_server_time(self) -> datetime:
        """Get Binance public server time as an aware UTC datetime."""
        response = await self._request("/api/v3/time", {}, SERVER_TIME_WEIGHT)
        payload = self._decode_object(response.body, "/api/v3/time")
        value = payload.get("serverTime")
        if isinstance(value, bool) or not isinstance(value, int):
            raise PublicMarketDataRequestError(response.status_code, "/api/v3/time")
        return datetime.fromtimestamp(value / 1000, tz=UTC)

    async def get_closed_klines(
        self,
        symbol: MarketSymbol,
        interval: str,
        start_inclusive: datetime,
        end_exclusive: datetime,
        page_limit: int = 1000,
    ) -> Sequence[RawMarketMessage]:
        """Fetch only fully closed public Spot klines in a bounded UTC range."""
        self._require_utc(start_inclusive)
        self._require_utc(end_exclusive)
        if end_exclusive <= start_inclusive:
            raise ValueError("end_exclusive must follow start_inclusive")
        if not interval:
            raise ValueError("interval is required")
        if not 1 <= page_limit <= 1000:
            raise ValueError("page_limit must be between 1 and 1000")
        stream_name = (
            f"{BinanceSymbolMapper.to_binance_symbol(symbol, 'spot').lower()}@kline_{interval}"
        )
        start_ms = int(start_inclusive.timestamp() * 1000)
        final_end_ms = int(end_exclusive.timestamp() * 1000) - 1
        messages: list[RawMarketMessage] = []
        while start_ms <= final_end_ms:
            response = await self._request(
                "/api/v3/klines",
                {
                    "symbol": BinanceSymbolMapper.to_binance_symbol(symbol, "spot"),
                    "interval": interval,
                    "startTime": str(start_ms),
                    "endTime": str(final_end_ms),
                    "limit": str(page_limit),
                },
                KLINES_WEIGHT,
            )
            payload = self._decode_array(response.body, "/api/v3/klines")
            if not payload:
                break
            latest_open_time: int | None = None
            for row in payload:
                if not isinstance(row, list) or len(row) < 7:
                    raise PublicMarketDataRequestError(response.status_code, "/api/v3/klines")
                open_time, inclusive_close_time = row[0], row[6]
                if (
                    isinstance(open_time, bool)
                    or not isinstance(open_time, int)
                    or isinstance(inclusive_close_time, bool)
                    or not isinstance(inclusive_close_time, int)
                ):
                    raise PublicMarketDataRequestError(response.status_code, "/api/v3/klines")
                if inclusive_close_time + 1 > int(end_exclusive.timestamp() * 1000):
                    continue
                messages.append(self._raw_message(stream_name, row))
                latest_open_time = open_time
            if len(payload) < page_limit or latest_open_time is None:
                break
            if latest_open_time < start_ms:
                raise PublicMarketDataRequestError(response.status_code, "/api/v3/klines")
            start_ms = latest_open_time + 1
        return tuple(messages)

    async def get_ticker_snapshot(self, symbol: MarketSymbol) -> RawMarketMessage:
        """Fetch one labelled current-state public ticker snapshot."""
        response = await self._request(
            "/api/v3/ticker/24hr",
            {"symbol": BinanceSymbolMapper.to_binance_symbol(symbol, "spot")},
            TICKER_SNAPSHOT_WEIGHT,
        )
        payload = self._decode_object(response.body, "/api/v3/ticker/24hr")
        stream_name = f"{BinanceSymbolMapper.to_binance_symbol(symbol, 'spot').lower()}@ticker"
        return self._raw_message(stream_name, payload)

    async def _request(
        self,
        endpoint: str,
        params: Mapping[str, str],
        declared_weight: int,
    ) -> HttpResponse:
        await self._limiter.acquire(endpoint, declared_weight)
        response = await self._transport.get(f"{self._base_url}{endpoint}", params)
        self._limiter.observe_response(status_code=response.status_code, headers=response.headers)
        if response.status_code != 200:
            raise PublicMarketDataRequestError(response.status_code, endpoint)
        return response

    def _raw_message(self, stream_name: str, payload: Any) -> RawMarketMessage:
        self._sequence += 1
        return RawMarketMessage(
            exchange="binance",
            market_type="spot",
            connection_id="binance-rest",
            stream_name=stream_name,
            payload_bytes=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            received_at=self._utc_now(),
            received_monotonic_ns=self._monotonic_clock(),
            receive_sequence=self._sequence,
            source_timestamp_unit="ms",
        )

    @staticmethod
    def _decode_object(body: bytes, endpoint: str) -> dict[str, Any]:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PublicMarketDataRequestError(200, endpoint) from error
        if not isinstance(payload, dict):
            raise PublicMarketDataRequestError(200, endpoint)
        return payload

    @staticmethod
    def _decode_array(body: bytes, endpoint: str) -> list[Any]:
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PublicMarketDataRequestError(200, endpoint) from error
        if not isinstance(payload, list):
            raise PublicMarketDataRequestError(200, endpoint)
        return payload

    @staticmethod
    def _validate_public_base_url(base_url: str) -> None:
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("Binance public REST base URL must use HTTPS")
        if any(token in base_url.lower() for token in ("apikey", "listenkey", "userdata")):
            raise ValueError("private Binance configuration is not supported")

    @staticmethod
    def _require_utc(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("REST timestamps must be UTC-aware")

    def _utc_now(self) -> datetime:
        now = self._clock()
        self._require_utc(now)
        return now.astimezone(UTC)
