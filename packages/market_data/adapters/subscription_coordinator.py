"""Subscription coordinator - Manages desired subscriptions and stream lifecycle.

The subscription coordinator:
- Maintains desired subscriptions (what we want)
- Tracks active subscriptions (what we have)
- Handles sharding when streams exceed connection limits
- Provides exchange-neutral subscription interface
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection, Set
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from packages.market_data.adapters.value_types import StreamKind, StreamSubscription

logger = logging.getLogger(__name__)


# =============================================================================
# Subscription metadata
# =============================================================================


@dataclass(frozen=True)
class SubscriptionKey:
    """Unique key for a subscription request."""

    exchange: str
    market_type: str
    symbol: str
    kind: StreamKind
    interval: str | None

    @classmethod
    def from_subscription(cls, sub: StreamSubscription) -> SubscriptionKey:
        """Create key from subscription.

        Args:
            sub: Stream subscription.

        Returns:
            Subscription key.
        """
        return cls(
            exchange=sub.exchange,
            market_type=sub.market_type,
            symbol=str(sub.symbol),
            kind=sub.kind,
            interval=sub.interval,
        )


# =============================================================================
# Subscription coordinator
# =============================================================================


class SubscriptionCoordinator:
    """Manages subscription state and lifecycle.

    The coordinator maintains the desired subscription set and handles
    translation to provider-specific streams.

    Responsibilities:
    - Track desired subscriptions (set by user)
    - Track active subscriptions (confirmed by provider)
    - Handle subscription changes (add/remove)
    - Manage sharding when needed

    NOT responsible for:
    - Provider-specific stream name translation (delegates to adapter)
    - Raw message parsing
    - Business logic
    """

    def __init__(
        self,
        adapter: Any,
        max_streams_per_connection: int = 500,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize coordinator.

        Args:
            adapter: WebSocket adapter to manage.
            max_streams_per_connection: Maximum streams per connection.
        """
        if max_streams_per_connection < 1:
            raise ValueError("max_streams_per_connection must be positive")
        self._adapter = adapter
        self._max_streams = max_streams_per_connection
        self._clock = clock or (lambda: datetime.now(UTC))

        # Desired subscriptions (what the user wants)
        self._desired: dict[SubscriptionKey, StreamSubscription] = {}

        # Active subscriptions (confirmed by provider)
        self._active: dict[SubscriptionKey, StreamSubscription] = {}
        self._pending_unsubscribes: set[SubscriptionKey] = set()

        # Subscription metadata
        self._subscribed_at: dict[SubscriptionKey, datetime] = {}
        self._last_message_at: dict[SubscriptionKey, datetime] = {}

        # Liveness tracking
        self._stale_threshold_seconds: float = 60.0
        self._check_stale_task: asyncio.Task[None] | None = None
        self._closing = False

        # Callbacks
        self._on_subscription_change: (
            Callable[[Set[StreamSubscription], Set[StreamSubscription]], None] | None
        ) = None
        self._on_stale: Callable[[StreamSubscription, datetime], None] | None = None

    # -------------------------------------------------------------------------
    # Subscription management
    # -------------------------------------------------------------------------

    def get_desired(self) -> frozenset[StreamSubscription]:
        """Get desired subscriptions."""
        return frozenset(self._desired.values())

    def get_active(self) -> frozenset[StreamSubscription]:
        """Get active (confirmed) subscriptions."""
        return frozenset(self._active.values())

    def get_missing(self) -> frozenset[StreamSubscription]:
        """Get subscriptions that are desired but not active."""
        return self.get_desired() - self.get_active()

    def is_active(self, subscription: StreamSubscription) -> bool:
        """Check if subscription is active."""
        key = SubscriptionKey.from_subscription(subscription)
        return key in self._active

    def add_subscription(self, subscription: StreamSubscription) -> None:
        """Add a subscription to the desired set.

        Args:
            subscription: Subscription to add.
        """
        key = SubscriptionKey.from_subscription(subscription)
        if key in self._desired:
            logger.debug(
                "Subscription already desired",
                extra={"key": str(key)},
            )
            return

        self._desired[key] = subscription
        self._subscribed_at[key] = self._utc_now()
        logger.info(
            "Added subscription to desired set",
            extra={"subscription": str(subscription)},
        )

    def remove_subscription(self, subscription: StreamSubscription) -> None:
        """Remove a subscription from the desired set.

        Args:
            subscription: Subscription to remove.
        """
        key = SubscriptionKey.from_subscription(subscription)
        if key not in self._desired:
            return

        del self._desired[key]
        # Retain confirmed state until the provider acknowledges UNSUBSCRIBE.
        # A sent command is not a lifecycle fact.
        self._subscribed_at.pop(key, None)
        self._last_message_at.pop(key, None)

        logger.info(
            "Removed subscription",
            extra={"subscription": str(subscription)},
        )

    def update_subscriptions(
        self,
        add: Collection[StreamSubscription],
        remove: Collection[StreamSubscription] | None = None,
    ) -> None:
        """Batch update subscriptions.

        Args:
            add: Subscriptions to add.
            remove: Subscriptions to remove.
        """
        for sub in add:
            self.add_subscription(sub)

        if remove:
            for sub in remove:
                self.remove_subscription(sub)

    # -------------------------------------------------------------------------
    # Sync with adapter
    # -------------------------------------------------------------------------

    async def sync_with_adapter(self) -> None:
        """Sync desired subscriptions with the adapter.

        This compares desired vs active and sends subscribe/unsubscribe
        commands to the adapter.
        """
        # Find new subscriptions
        to_subscribe = self.get_missing()

        # Find subscriptions to remove
        to_unsubscribe = self._get_subscriptions_to_remove() - self._pending_unsubscribes

        if to_unsubscribe:
            await self._adapter.unsubscribe(
                self._ordered([self._active[key] for key in to_unsubscribe])
            )
            self._pending_unsubscribes.update(to_unsubscribe)

        if to_subscribe:
            await self._adapter.subscribe(self._ordered(to_subscribe))

    def mark_active(self, subscription: StreamSubscription) -> None:
        """Mark a subscription as active (confirmed by provider).

        Args:
            subscription: Subscription that is now active.
        """
        key = SubscriptionKey.from_subscription(subscription)
        self._active[key] = subscription
        logger.debug(
            "Subscription marked active",
            extra={"subscription": str(subscription)},
        )

    def mark_inactive(self, subscription: StreamSubscription) -> None:
        """Mark a subscription as inactive.

        Args:
            subscription: Subscription that is no longer active.
        """
        key = SubscriptionKey.from_subscription(subscription)
        if key in self._active:
            del self._active[key]
            self._pending_unsubscribes.discard(key)
            logger.debug(
                "Subscription marked inactive",
                extra={"subscription": str(subscription)},
            )

    def reconcile_confirmed(self, confirmed: Collection[StreamSubscription]) -> None:
        """Mirror only provider-acknowledged subscriptions into active state.

        The exchange adapter owns acknowledgement correlation.  This neutral
        coordinator never treats a sent ``SUBSCRIBE`` request as activation.
        """
        confirmed_by_key = {
            SubscriptionKey.from_subscription(subscription): subscription
            for subscription in confirmed
        }
        self._active = confirmed_by_key
        self._pending_unsubscribes.intersection_update(self._active)

    def update_liveness(self, subscription: StreamSubscription, received_at: datetime) -> None:
        """Update liveness timestamp for a subscription.

        Args:
            subscription: Subscription that received data.
            received_at: Time data was received.
        """
        key = SubscriptionKey.from_subscription(subscription)
        self._last_message_at[key] = received_at

    def _get_subscriptions_to_remove(self) -> set[SubscriptionKey]:
        """Get subscriptions that are active but no longer desired."""
        return set(self._active.keys()) - set(self._desired.keys())

    # -------------------------------------------------------------------------
    # Sharding
    # -------------------------------------------------------------------------

    def get_shards(self) -> list[list[StreamSubscription]]:
        """Get subscription shards for multiple connections.

        Returns:
            List of subscription lists, each suitable for one connection.
        """
        subs = self._ordered(self.get_desired())
        shards: list[list[StreamSubscription]] = []
        current_shard: list[StreamSubscription] = []

        for sub in subs:
            if len(current_shard) >= self._max_streams:
                shards.append(current_shard)
                current_shard = []
            current_shard.append(sub)

        if current_shard:
            shards.append(current_shard)

        return shards

    # -------------------------------------------------------------------------
    # Stale detection
    # -------------------------------------------------------------------------

    async def start_stale_detection(self, interval: float = 10.0) -> None:
        """Start background stale detection.

        Args:
            interval: Check interval in seconds.
        """
        self._closing = False
        self._check_stale_task = asyncio.create_task(self._stale_detection_loop(interval))

    async def stop_stale_detection(self) -> None:
        """Stop background stale detection."""
        self._closing = True
        if self._check_stale_task:
            self._check_stale_task.cancel()
            try:
                await self._check_stale_task
            except asyncio.CancelledError:
                pass

    async def _stale_detection_loop(self, interval: float) -> None:
        """Background loop to detect stale subscriptions."""
        try:
            while not self._closing:
                await asyncio.sleep(interval)
                await self._check_stale()
        except asyncio.CancelledError:
            pass

    async def _check_stale(self) -> None:
        """Check for stale subscriptions."""
        now = self._utc_now()
        threshold = self._stale_threshold_seconds

        for key, sub in self._active.items():
            last_msg = self._last_message_at.get(key)
            if last_msg is None:
                # No message ever received
                continue

            elapsed = (now - last_msg).total_seconds()
            if elapsed > threshold:
                logger.warning(
                    "Subscription may be stale",
                    extra={
                        "subscription": str(sub),
                        "seconds_since_last_message": elapsed,
                        "threshold": threshold,
                    },
                )
                if self._on_stale:
                    self._on_stale(sub, last_msg)

    def get_stale_subscriptions(self) -> list[tuple[StreamSubscription, datetime]]:
        """Get list of stale subscriptions.

        Returns:
            List of (subscription, last_message_time) tuples.
        """
        now = self._utc_now()
        threshold = self._stale_threshold_seconds
        stale = []

        for key, sub in self._active.items():
            last_msg = self._last_message_at.get(key)
            if last_msg is None:
                continue

            elapsed = (now - last_msg).total_seconds()
            if elapsed > threshold:
                stale.append((sub, last_msg))

        return stale

    # -------------------------------------------------------------------------
    # Snapshot
    # -------------------------------------------------------------------------

    def get_snapshot(self) -> dict[str, Any]:
        """Get coordinator state snapshot."""
        return {
            "desired_count": len(self._desired),
            "active_count": len(self._active),
            "missing_count": len(self.get_missing()),
            "stale_count": len(self.get_stale_subscriptions()),
            "shards_needed": len(self.get_shards()),
        }

    @staticmethod
    def _ordered(subscriptions: Collection[StreamSubscription]) -> list[StreamSubscription]:
        return sorted(subscriptions, key=lambda subscription: subscription.key)

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("subscription coordinator clock must return an aware datetime")
        return value.astimezone(UTC)
