"""Scripted fake public HTTP transport for network-free DATA-002 tests.

Implements the shape of ``PublicHttpTransport`` (see
``packages/market_data/adapters/binance_rest.py``) from in-memory scripted
``HttpResponse`` values. It never performs HTTP I/O and never accepts
credentials, so rate-limit, retry, and failure tests stay deterministic.
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from packages.market_data.adapters.binance_rest import HttpResponse


class FakeHttpTransport:
    """Scripted public HTTP transport: per-endpoint response queues."""

    def __init__(self, server_time: datetime) -> None:
        if server_time.tzinfo is None or server_time.utcoffset() is None:
            raise ValueError("fake server time must be UTC-aware")
        self._server_time = server_time
        self._script: dict[str, list[HttpResponse]] = {}
        self._fail_after: int | None = None
        self._fail_error: Exception | None = None
        self.requests: list[tuple[str, dict[str, str]]] = []

    def add(self, endpoint: str, response: HttpResponse) -> None:
        """Queue one scripted response for an endpoint path."""
        self._script.setdefault(endpoint, []).append(response)

    def add_json(self, endpoint: str, payload: Any, *, status: int = 200) -> None:
        """Queue one JSON response for an endpoint path."""
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.add(endpoint, HttpResponse(status_code=status, headers={}, body=body))

    def add_server_time(self) -> None:
        """Queue the scripted public server time response."""
        self.add_json("/api/v3/time", {"serverTime": _to_epoch_ms(self._server_time)})

    def fail_after(self, count: int, error: Exception) -> None:
        """Fail (raise) every request once the request counter reaches count."""
        self._fail_after = count
        self._fail_error = error

    def fail_with_status(
        self, count: int, status: int, *, headers: Mapping[str, str] | None = None
    ) -> None:
        """Return a non-200 status for every request once the counter reaches count."""
        self.add(
            "/__fail__",
            HttpResponse(
                status_code=status,
                headers=dict(headers or {}),
                body=b"{}",
            ),
        )
        self._fail_after = count
        self._fail_status = status

    async def get(self, url: str, params: Mapping[str, str]) -> HttpResponse:
        """Return the next scripted response or raise the scripted failure."""
        self.requests.append((url, dict(params)))
        if self._fail_after is not None and len(self.requests) >= self._fail_after:
            if self._fail_error is not None:
                raise self._fail_error
            return self._script.get("/__fail__", [])[0]
        path = urllib.parse.urlparse(url).path
        queue = self._script.get(path)
        if not queue:
            raise AssertionError(f"unexpected request: {url}")
        response = queue.pop(0) if len(queue) > 1 else queue[0]
        return response


def kline_row(
    open_ms: int,
    *,
    close_ms: int | None = None,
    duration_ms: int = 60_000,
) -> list[Any]:
    """Build one deterministic 12-field Binance public kline row.

    Binance field 6 is the inclusive close timestamp, so the default is one
    millisecond before the exclusive interval boundary.
    """
    return [
        open_ms,
        "30000.00",
        "30100.00",
        "29900.00",
        "30050.00",
        "1.23450000",
        close_ms if close_ms is not None else open_ms + duration_ms - 1,
        "36980.12300000",
        120,
        "0.61000000",
        "18291.00000000",
        "0",
    ]


def retry_after_response(status: int, retry_after: str) -> HttpResponse:
    """Build a 429/418 response carrying a Retry-After header."""
    return HttpResponse(
        status_code=status,
        headers={"Retry-After": retry_after, "X-MBX-Used-Weight-1M": "30"},
        body=b"{}",
    )


def ok_response(payload: Any) -> HttpResponse:
    """Build a 200 JSON response."""
    return HttpResponse(
        status_code=200,
        headers={"X-MBX-Used-Weight-1M": "2"},
        body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )


def _to_epoch_ms(value: datetime) -> int:
    delta = value.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
