"""Per-subscription freshness tracking with deterministic stale episodes."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from packages.market_data.adapters.value_types import ConnectionState, StreamSubscription
from packages.market_data.contracts.events import MarketDataStale


@dataclass
class _FreshnessState:
    subscription: StreamSubscription
    registered_at: datetime
    last_valid_at: datetime | None = None
    episode_started_at: datetime | None = None
    last_emitted_at: datetime | None = None
    reminder_count: int = 0


StaleReasonCode = Literal[
    "no_valid_frame",
    "connection_down",
    "recovery_pending",
    "rate_limit_cooldown",
    "clock_untrusted",
]


class FreshnessMonitor:
    """Track fresh input per subscription without misclassifying quiet trades."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        ticker_threshold_seconds: float = 10.0,
        kline_threshold_seconds: float = 10.0,
        trade_threshold_seconds: float | None = None,
        reminder_seconds: float = 60.0,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(UTC))
        self._thresholds = {
            "ticker": ticker_threshold_seconds,
            "kline": kline_threshold_seconds,
            "trade": trade_threshold_seconds,
        }
        self._reminder_seconds = reminder_seconds
        self._states: dict[str, _FreshnessState] = {}

    def register(self, subscriptions: Collection[StreamSubscription]) -> None:
        """Begin tracking configured subscriptions."""
        for subscription in subscriptions:
            self._states.setdefault(
                subscription.key,
                _FreshnessState(subscription=subscription, registered_at=self._utc_now()),
            )

    def unregister(self, subscriptions: Collection[StreamSubscription]) -> None:
        """Stop tracking subscriptions removed from desired state."""
        for subscription in subscriptions:
            self._states.pop(subscription.key, None)

    def record_valid(self, subscription: StreamSubscription, received_at: datetime) -> None:
        """Record valid input and clear the current stale episode."""
        self._require_utc(received_at)
        state = self._states.setdefault(
            subscription.key,
            _FreshnessState(subscription=subscription, registered_at=self._utc_now()),
        )
        state.last_valid_at = received_at.astimezone(UTC)
        state.episode_started_at = None
        state.last_emitted_at = None
        state.reminder_count = 0

    def evaluate(
        self,
        *,
        connection_state: ConnectionState,
        connection_id: str | None = None,
        recovery_pending: bool = False,
    ) -> tuple[MarketDataStale, ...]:
        """Emit one stale event per episode plus controlled reminders."""
        now = self._utc_now()
        stale_events: list[MarketDataStale] = []
        for state in self._states.values():
            threshold = self._thresholds[state.subscription.kind]
            if threshold is None and connection_state == "streaming":
                continue
            reason = self._reason(connection_state, recovery_pending, state.last_valid_at)
            effective_threshold = threshold if threshold is not None else 0.0
            age_seconds = (
                (now - state.last_valid_at).total_seconds()
                if state.last_valid_at is not None
                else (now - state.registered_at).total_seconds()
            )
            if connection_state == "streaming" and age_seconds <= effective_threshold:
                continue
            if state.last_emitted_at is not None:
                elapsed_since_alert = (now - state.last_emitted_at).total_seconds()
                if elapsed_since_alert < self._reminder_seconds:
                    continue
                state.reminder_count += 1
            else:
                state.episode_started_at = now
            event_id = uuid.uuid5(
                uuid.NAMESPACE_URL,
                "|".join(
                    (
                        "stale",
                        state.subscription.key,
                        state.episode_started_at.isoformat() if state.episode_started_at else "",
                        str(state.reminder_count),
                    )
                ),
            )
            stale_events.append(
                MarketDataStale(
                    event_id=str(event_id),
                    exchange=state.subscription.exchange,
                    market_type=state.subscription.market_type,
                    symbol=state.subscription.symbol,
                    source="websocket",
                    occurred_at=now,
                    exchange_event_at=None,
                    received_at=now,
                    connection_id=connection_id,
                    stream_kind=state.subscription.kind,
                    interval=state.subscription.interval,
                    last_valid_received_at=state.last_valid_at,
                    stale_for_ms=max(0, int(age_seconds * 1000)),
                    threshold_ms=max(1, int(effective_threshold * 1000)),
                    connection_state=connection_state,
                    reason_code=reason,
                )
            )
            state.last_emitted_at = now
        return tuple(stale_events)

    def _reason(
        self,
        connection_state: ConnectionState,
        recovery_pending: bool,
        last_valid_at: datetime | None,
    ) -> StaleReasonCode:
        if recovery_pending:
            return "recovery_pending"
        if connection_state != "streaming":
            return "connection_down"
        if last_valid_at is None:
            return "no_valid_frame"
        return "no_valid_frame"

    def _utc_now(self) -> datetime:
        now = self._clock()
        self._require_utc(now)
        return now.astimezone(UTC)

    @staticmethod
    def _require_utc(value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("freshness clock must return UTC-aware datetime")
