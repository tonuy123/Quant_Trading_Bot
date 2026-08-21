"""Deterministic public-market-data rate-limit coordination."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime


class RateLimitBlockedError(RuntimeError):
    """Raised before a request when Binance cooldown is still active."""

    def __init__(self, blocked_until: datetime, status_code: int) -> None:
        super().__init__(f"Binance public REST blocked until {blocked_until.isoformat()}")
        self.blocked_until = blocked_until
        self.status_code = status_code


@dataclass(frozen=True)
class RateLimitSnapshot:
    """Observable limiter state without endpoint query data."""

    blocked_until: datetime | None
    blocked_status_code: int | None
    endpoint_attempts: dict[str, int]
    endpoint_declared_weight: dict[str, int]
    used_weight_headers: dict[str, int]


class BinanceRateLimitCoordinator:
    """Track public Binance REST weights and mandatory cooldowns.

    This class is deliberately request-transport agnostic. A horizontally
    scaled deployment can share one implementation through a distributed
    wrapper without changing REST-adapter behavior.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        default_retry_after_seconds: float = 1.0,
        default_ban_seconds: float = 120.0,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._default_retry_after_seconds = default_retry_after_seconds
        self._default_ban_seconds = default_ban_seconds
        self._blocked_until: datetime | None = None
        self._blocked_status_code: int | None = None
        self._endpoint_attempts: dict[str, int] = {}
        self._endpoint_declared_weight: dict[str, int] = {}
        self._used_weight_headers: dict[str, int] = {}

    async def acquire(self, endpoint: str, declared_weight: int) -> None:
        """Reserve an observable public REST request slot.

        The method never sleeps or retries implicitly. A caller receives a
        deterministic blocked error and must not send a request before the
        cooldown ends.
        """
        if declared_weight < 1:
            raise ValueError("declared_weight must be positive")
        now = self._utc_now()
        if self._blocked_until is not None and now < self._blocked_until:
            raise RateLimitBlockedError(
                blocked_until=self._blocked_until,
                status_code=self._blocked_status_code or 429,
            )
        if self._blocked_until is not None:
            self._blocked_until = None
            self._blocked_status_code = None
        self._endpoint_attempts[endpoint] = self._endpoint_attempts.get(endpoint, 0) + 1
        self._endpoint_declared_weight[endpoint] = declared_weight

    def observe_response(
        self,
        *,
        status_code: int,
        headers: Mapping[str, str],
    ) -> None:
        """Record response weights and start cooldown for 429 or 418."""
        normalized_headers = {name.lower(): value for name, value in headers.items()}
        for name, value in normalized_headers.items():
            if name.startswith("x-mbx-used-weight-"):
                try:
                    self._used_weight_headers[name] = int(value)
                except ValueError:
                    continue
        if status_code not in {429, 418}:
            return
        default_seconds = (
            self._default_ban_seconds if status_code == 418 else self._default_retry_after_seconds
        )
        cooldown = self._parse_retry_after(
            normalized_headers.get("retry-after"),
            default_seconds=default_seconds,
        )
        until = self._utc_now() + timedelta(seconds=cooldown)
        if self._blocked_until is None or until > self._blocked_until:
            self._blocked_until = until
            self._blocked_status_code = status_code

    def snapshot(self) -> RateLimitSnapshot:
        """Return safe, deterministic observability state."""
        return RateLimitSnapshot(
            blocked_until=self._blocked_until,
            blocked_status_code=self._blocked_status_code,
            endpoint_attempts=dict(self._endpoint_attempts),
            endpoint_declared_weight=dict(self._endpoint_declared_weight),
            used_weight_headers=dict(self._used_weight_headers),
        )

    def _utc_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("rate-limit clock must return UTC-aware datetime")
        return now.astimezone(UTC)

    def _parse_retry_after(self, value: str | None, *, default_seconds: float) -> float:
        if value is None:
            return default_seconds
        try:
            seconds = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return default_seconds
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            return max(0.0, (retry_at - self._utc_now()).total_seconds())
        return max(0.0, seconds)


class WebSocketRateBudget:
    """Reserve provider control and reconnect headroom with fixed windows."""

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float],
        max_control_messages_per_window: int = 4,
        control_window_seconds: float = 1.0,
        max_connection_attempts_per_window: int = 240,
        connection_window_seconds: float = 300.0,
    ) -> None:
        self._clock = monotonic_clock
        self._max_control = max_control_messages_per_window
        self._control_window = control_window_seconds
        self._max_connections = max_connection_attempts_per_window
        self._connection_window = connection_window_seconds
        self._control_attempts: deque[float] = deque()
        self._connection_attempts: deque[float] = deque()

    def reserve_control_message(self) -> bool:
        """Reserve one control message while retaining heartbeat headroom."""
        return self._reserve(
            self._control_attempts,
            maximum=self._max_control,
            window_seconds=self._control_window,
        )

    def reserve_connection_attempt(self) -> bool:
        """Reserve one connection attempt below the provider attempt ceiling."""
        return self._reserve(
            self._connection_attempts,
            maximum=self._max_connections,
            window_seconds=self._connection_window,
        )

    def _reserve(self, attempts: deque[float], *, maximum: int, window_seconds: float) -> bool:
        now = self._clock()
        while attempts and now - attempts[0] >= window_seconds:
            attempts.popleft()
        if len(attempts) >= maximum:
            return False
        attempts.append(now)
        return True
