"""Tests for market data adapters - MD-002 through MD-004."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from packages.market_data.adapters.binance_ws import (
    BinanceSymbolMapper,
    BinanceWebSocketAdapter,
    ConnectionStateMachine,
    calculate_backoff,
    is_retry_safe,
)
from packages.market_data.adapters.connection_supervisor import (
    CircuitBreaker,
    ConnectionSupervisor,
)
from packages.market_data.adapters.subscription_coordinator import (
    SubscriptionCoordinator,
    SubscriptionKey,
)
from packages.market_data.adapters.value_types import (
    ConnectionSnapshot,
    MarketSymbol,
    RawMarketMessage,
    StreamSubscription,
)

# =============================================================================
# Value types tests (MD-002)
# =============================================================================


class TestMarketSymbol:
    """Tests for MarketSymbol value type."""

    def test_create_market_symbol(self) -> None:
        """Test creating a market symbol."""
        symbol = MarketSymbol(base="BTC", quote="USDT")

        assert symbol.base == "BTC"
        assert symbol.quote == "USDT"
        assert symbol.canonical == "BTC/USDT"
        assert str(symbol) == "BTC/USDT"

    def test_symbol_normalization(self) -> None:
        """Test symbol normalizes to uppercase."""
        symbol = MarketSymbol(base="btc", quote="usdt")

        assert symbol.base == "BTC"
        assert symbol.quote == "USDT"

    def test_symbol_equality(self) -> None:
        """Test symbol equality."""
        s1 = MarketSymbol(base="BTC", quote="USDT")
        s2 = MarketSymbol(base="BTC", quote="USDT")

        assert s1 == s2
        assert hash(s1) == hash(s2)


class TestStreamSubscription:
    """Tests for StreamSubscription value type."""

    def test_create_trade_subscription(self) -> None:
        """Test creating a trade subscription."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        assert sub.exchange == "binance"
        assert sub.market_type == "spot"
        assert sub.kind == "trade"
        assert sub.interval is None

    def test_create_kline_subscription(self) -> None:
        """Test creating a kline subscription."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="kline",
            interval="1m",
        )

        assert sub.kind == "kline"
        assert sub.interval == "1m"

    def test_kline_requires_interval(self) -> None:
        """Test that kline subscriptions require interval."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        with pytest.raises(ValueError, match="kline subscriptions require an interval"):
            StreamSubscription(
                exchange="binance",
                market_type="spot",
                symbol=symbol,
                kind="kline",
            )

    def test_subscription_key(self) -> None:
        """Test subscription key generation."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="kline",
            interval="1m",
        )

        key = sub.key
        assert "binance" in key
        assert "BTC/USDT" in key
        assert "kline" in key
        assert "1m" in key


class TestRawMarketMessage:
    """Tests for RawMarketMessage value type."""

    def test_create_raw_message(self) -> None:
        """Test creating a raw market message."""
        now = datetime.now(UTC)
        msg = RawMarketMessage(
            exchange="binance",
            market_type="spot",
            connection_id="conn-123",
            stream_name="btcusdt@trade",
            payload_bytes=b'{"test": true}',
            received_at=now,
            received_monotonic_ns=1000000000,
            receive_sequence=1,
        )

        assert msg.exchange == "binance"
        assert msg.connection_id == "conn-123"
        assert msg.receive_sequence == 1
        assert msg.source_timestamp_unit == "unknown"

    def test_payload_text_decode(self) -> None:
        """Test decoding payload as text."""
        now = datetime.now(UTC)
        msg = RawMarketMessage(
            exchange="binance",
            market_type="spot",
            connection_id="conn-123",
            stream_name="btcusdt@trade",
            payload_bytes=b'{"test": true}',
            received_at=now,
            received_monotonic_ns=0,
            receive_sequence=1,
        )

        assert msg.payload_text == '{"test": true}'

    def test_requires_utc_timestamp(self) -> None:
        """Test that naive datetime is rejected."""
        with pytest.raises(ValueError, match="UTC-aware"):
            RawMarketMessage(
                exchange="binance",
                market_type="spot",
                connection_id="conn-123",
                stream_name="btcusdt@trade",
                payload_bytes=b"{}",
                received_at=datetime.now(),  # Naive datetime
                received_monotonic_ns=0,
                receive_sequence=1,
            )


class TestConnectionSnapshot:
    """Tests for ConnectionSnapshot value type."""

    def test_create_snapshot(self) -> None:
        """Test creating a connection snapshot."""
        now = datetime.now(UTC)
        snapshot = ConnectionSnapshot(
            connection_id="conn-123",
            state="streaming",
            connected_at=now,
            last_frame_received_at=now,
            reconnect_attempt=0,
        )

        assert snapshot.connection_id == "conn-123"
        assert snapshot.state == "streaming"
        assert snapshot.is_healthy is True

    def test_not_healthy_when_not_streaming(self) -> None:
        """Test that non-streaming state is not healthy."""
        snapshot = ConnectionSnapshot(
            connection_id="conn-123",
            state="connecting",
        )

        assert snapshot.is_healthy is False


# =============================================================================
# Binance symbol mapper tests (MD-002)
# =============================================================================


class TestBinanceSymbolMapper:
    """Tests for BinanceSymbolMapper."""

    def test_to_binance_ws_stream_trade(self) -> None:
        """Test converting trade subscription to Binance stream."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        stream = BinanceSymbolMapper.to_binance_ws_stream(sub)
        assert stream == "btcusdt@trade"

    def test_to_binance_ws_stream_ticker(self) -> None:
        """Test converting ticker subscription to Binance stream."""
        symbol = MarketSymbol(base="ETH", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="ticker",
        )

        stream = BinanceSymbolMapper.to_binance_ws_stream(sub)
        assert stream == "ethusdt@ticker"

    def test_to_binance_ws_stream_kline(self) -> None:
        """Test converting kline subscription to Binance stream."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="kline",
            interval="5m",
        )

        stream = BinanceSymbolMapper.to_binance_ws_stream(sub)
        assert stream == "btcusdt@kline_5m"

    def test_to_binance_symbol(self) -> None:
        """Test converting to Binance REST symbol."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        binance_sym = BinanceSymbolMapper.to_binance_symbol(symbol, "spot")

        assert binance_sym == "BTCUSDT"

    def test_from_binance_symbol(self) -> None:
        """Test converting from Binance symbol."""
        symbol = BinanceSymbolMapper.from_binance_symbol("BTCUSDT")

        assert symbol.base == "BTC"
        assert symbol.quote == "USDT"

    def test_from_binance_symbol_eth(self) -> None:
        """Test converting ETH symbol."""
        symbol = BinanceSymbolMapper.from_binance_symbol("ETHBTC")

        assert symbol.base == "ETH"
        assert symbol.quote == "BTC"


# =============================================================================
# Connection state machine tests (MD-004)
# =============================================================================


class TestConnectionStateMachine:
    """Tests for ConnectionStateMachine."""

    def test_initial_state(self) -> None:
        """Test initial state is stopped."""
        sm = ConnectionStateMachine()
        assert sm.state == "stopped"
        assert sm.reconnect_attempt == 0

    def test_transition(self) -> None:
        """Test state transitions."""
        sm = ConnectionStateMachine()
        sm.transition_to("connecting", "test")

        assert sm.state == "connecting"

    def test_can_reconnect(self) -> None:
        """Test reconnect eligibility."""
        sm = ConnectionStateMachine()
        assert sm.can_reconnect() is True

        sm.reconnect_attempt = 10
        assert sm.can_reconnect() is False

    def test_backoff_calculation(self) -> None:
        """Test backoff calculation with jitter."""
        sm = ConnectionStateMachine()

        # First attempt
        sm.reconnect_attempt = 0
        delay = sm.calculate_backoff()
        assert 0 <= delay <= 1.0  # Base backoff

        # Second attempt
        sm.reconnect_attempt = 1
        delay = sm.calculate_backoff()
        assert 0 <= delay <= 2.0  # 2x base


# =============================================================================
# Backoff utility tests (MD-004)
# =============================================================================


class TestCalculateBackoff:
    """Tests for backoff calculation."""

    def test_base_backoff(self) -> None:
        """Test base backoff without jitter."""
        delay = calculate_backoff(0, base=1.0, cap=10.0, jitter=False)
        assert delay == 1.0

    def test_exponential_backoff(self) -> None:
        """Test exponential backoff."""
        delay = calculate_backoff(2, base=1.0, cap=100.0, jitter=False)
        assert delay == 4.0  # 1 * 2^2 = 4

    def test_backoff_capped(self) -> None:
        """Test backoff is capped."""
        delay = calculate_backoff(10, base=1.0, cap=10.0, jitter=False)
        assert delay == 10.0

    def test_backoff_with_jitter(self) -> None:
        """Test backoff with jitter."""
        delays = [calculate_backoff(0, base=1.0, cap=10.0, jitter=True) for _ in range(100)]
        # With jitter, delays should vary
        assert min(delays) >= 0
        assert max(delays) <= 10.0
        # Should have some variation
        assert max(delays) - min(delays) > 0.1


class TestIsRetrySafe:
    """Tests for retry safety check."""

    def test_connection_error_safe(self) -> None:
        """Test connection errors are retry-safe."""
        assert is_retry_safe(ConnectionError()) is True

    def test_timeout_safe(self) -> None:
        """Test timeouts are retry-safe."""
        assert is_retry_safe(TimeoutError()) is True

    def test_value_error_not_safe(self) -> None:
        """Test ValueError is not retry-safe."""
        assert is_retry_safe(ValueError("bad request")) is False


# =============================================================================
# Subscription coordinator tests (MD-003)
# =============================================================================


class TestSubscriptionKey:
    """Tests for SubscriptionKey."""

    def test_from_subscription(self) -> None:
        """Test creating key from subscription."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="kline",
            interval="1m",
        )

        key = SubscriptionKey.from_subscription(sub)

        assert key.exchange == "binance"
        assert key.market_type == "spot"
        assert key.symbol == "BTC/USDT"
        assert key.kind == "kline"
        assert key.interval == "1m"


class TestSubscriptionCoordinator:
    """Tests for SubscriptionCoordinator."""

    @pytest.fixture
    def mock_adapter(self) -> MagicMock:
        """Create mock adapter."""
        adapter = MagicMock(spec=BinanceWebSocketAdapter)
        adapter.subscribe = AsyncMock()
        adapter.unsubscribe = AsyncMock()
        return adapter

    @pytest.fixture
    def coordinator(self, mock_adapter: MagicMock) -> SubscriptionCoordinator:
        """Create coordinator with mock adapter."""
        return SubscriptionCoordinator(mock_adapter)

    def test_add_subscription(self, coordinator: SubscriptionCoordinator) -> None:
        """Test adding a subscription."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        coordinator.add_subscription(sub)

        assert len(coordinator.get_desired()) == 1
        assert coordinator.is_active(sub) is False

    def test_remove_subscription(self, coordinator: SubscriptionCoordinator) -> None:
        """Test removing a subscription."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        coordinator.add_subscription(sub)
        coordinator.remove_subscription(sub)

        assert len(coordinator.get_desired()) == 0

    def test_get_missing(self, coordinator: SubscriptionCoordinator) -> None:
        """Test getting missing subscriptions."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        coordinator.add_subscription(sub)

        missing = coordinator.get_missing()
        assert len(missing) == 1
        assert sub in missing

    def test_mark_active(self, coordinator: SubscriptionCoordinator) -> None:
        """Test marking subscription as active."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        coordinator.add_subscription(sub)
        coordinator.mark_active(sub)

        assert len(coordinator.get_active()) == 1
        assert coordinator.is_active(sub) is True

    def test_update_liveness(self, coordinator: SubscriptionCoordinator) -> None:
        """Test updating subscription liveness."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )
        now = datetime.now(UTC)

        coordinator.add_subscription(sub)
        coordinator.mark_active(sub)
        coordinator.update_liveness(sub, now)

        stale = coordinator.get_stale_subscriptions()
        assert len(stale) == 0  # Just updated

    @pytest.mark.asyncio
    async def test_sync_with_adapter(
        self,
        coordinator: SubscriptionCoordinator,
        mock_adapter: MagicMock,
    ) -> None:
        """Test syncing with adapter."""
        symbol = MarketSymbol(base="BTC", quote="USDT")
        sub = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )

        coordinator.add_subscription(sub)
        coordinator.mark_active(sub)
        await coordinator.sync_with_adapter()

        # No missing subscriptions
        assert len(coordinator.get_missing()) == 0

    @pytest.mark.asyncio
    async def test_removed_active_subscription_waits_for_unsubscribe_ack(
        self,
        coordinator: SubscriptionCoordinator,
        mock_adapter: MagicMock,
    ) -> None:
        symbol = MarketSymbol(base="BTC", quote="USDT")
        subscription = StreamSubscription(
            exchange="binance",
            market_type="spot",
            symbol=symbol,
            kind="trade",
        )
        coordinator.add_subscription(subscription)
        coordinator.mark_active(subscription)
        coordinator.remove_subscription(subscription)

        await coordinator.sync_with_adapter()

        mock_adapter.unsubscribe.assert_awaited_once_with([subscription])
        assert coordinator.is_active(subscription) is True
        coordinator.reconcile_confirmed(set())
        assert coordinator.is_active(subscription) is False

    def test_get_shards(self, coordinator: SubscriptionCoordinator) -> None:
        """Test getting subscription shards."""
        # Add many subscriptions
        quotes = ["USDT", "BTC", "ETH", "BNB", "BUSD"]
        for base in ["BTC", "ETH", "XRP", "ADA", "DOT", "SOL", "MATIC", "LINK", "AVAX", "DOGE"]:
            for quote in quotes:
                symbol = MarketSymbol(base=base, quote=quote)
                sub = StreamSubscription(
                    exchange="binance",
                    market_type="spot",
                    symbol=symbol,
                    kind="trade",
                )
                coordinator.add_subscription(sub)

        # With max 500 streams, should fit in 1 shard
        shards = coordinator.get_shards()
        assert len(shards) == 1


# =============================================================================
# Circuit breaker tests (MD-004)
# =============================================================================


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def test_initial_state(self) -> None:
        """Test circuit starts closed."""
        cb = CircuitBreaker()
        assert cb.state == "closed"
        assert cb.is_closed is True

    def test_opens_after_failures(self) -> None:
        """Test circuit opens after threshold failures."""
        cb = CircuitBreaker(failure_threshold=3)

        cb.record_failure()
        cb.record_failure()
        assert cb.state == "closed"

        cb.record_failure()
        assert cb.state == "open"

    def test_closes_after_successes(self) -> None:
        """Test circuit closes after recovery successes."""
        cb = CircuitBreaker(failure_threshold=2, success_threshold=2)

        # Open the circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

        # Wait for recovery (simulated by time passage)
        cb._opened_at = datetime.now(UTC) - timedelta(seconds=61)

        # Allow request to enter half-open
        assert cb.allow_request() is True
        assert cb.state == "half_open"

        # Two successes closes it
        cb.record_success()
        assert cb.state == "half_open"

        cb.record_success()
        assert cb.state == "closed"

    def test_half_open_failure_reopens(self) -> None:
        """Test failure in half-open reopens circuit."""
        cb = CircuitBreaker()

        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"

        cb._opened_at = datetime.now(UTC) - timedelta(seconds=61)

        # Enter half-open
        cb.allow_request()
        assert cb.state == "half_open"

        # Failure reopens
        cb.record_failure()
        assert cb.state == "open"


# =============================================================================
# Connection supervisor tests (MD-004)
# =============================================================================


class TestConnectionSupervisor:
    """Tests for ConnectionSupervisor."""

    @pytest.fixture
    def mock_adapter(self) -> MagicMock:
        """Create mock adapter."""
        adapter = MagicMock(spec=BinanceWebSocketAdapter)
        adapter.connect = AsyncMock()
        adapter.close = AsyncMock()
        adapter.subscribe = AsyncMock()
        adapter.unsubscribe = AsyncMock()
        adapter.snapshot = AsyncMock()

        async def no_messages():
            if False:
                yield None

        adapter.raw_messages = no_messages
        return adapter

    @pytest.fixture
    def mock_coordinator(self) -> MagicMock:
        """Create mock coordinator."""
        coordinator = MagicMock(spec=SubscriptionCoordinator)
        coordinator.sync_with_adapter = AsyncMock()
        coordinator.start_stale_detection = AsyncMock()
        coordinator.stop_stale_detection = AsyncMock()
        coordinator.get_desired.return_value = frozenset()
        coordinator.get_active.return_value = frozenset()
        return coordinator

    @pytest.fixture
    def supervisor(
        self,
        mock_adapter: MagicMock,
        mock_coordinator: MagicMock,
    ) -> ConnectionSupervisor:
        """Create supervisor with mocks."""
        return ConnectionSupervisor(
            adapter=mock_adapter,
            coordinator=mock_coordinator,
            heartbeat_timeout=30.0,
            max_reconnect_attempts=3,
        )

    def test_initial_state(self, supervisor: ConnectionSupervisor) -> None:
        """Test supervisor starts in stopped state."""
        assert supervisor.state == "stopped"

    @pytest.mark.asyncio
    async def test_start_connects(
        self,
        supervisor: ConnectionSupervisor,
        mock_adapter: MagicMock,
        mock_coordinator: MagicMock,
    ) -> None:
        """Test starting the supervisor connects."""
        await supervisor.start()

        mock_adapter.connect.assert_called_once()
        mock_coordinator.sync_with_adapter.assert_called_once()
        # Socket open plus submitted control messages is not data health.  The
        # supervisor becomes streaming only after a received market frame.
        assert supervisor.state == "subscribing"
        await supervisor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes(
        self,
        supervisor: ConnectionSupervisor,
        mock_adapter: MagicMock,
    ) -> None:
        """Test stopping the supervisor closes."""
        await supervisor.start()
        await supervisor.stop()

        mock_adapter.close.assert_called()
        assert supervisor.state == "stopped"

    @pytest.mark.asyncio
    async def test_cannot_start_while_subscribing(
        self,
        supervisor: ConnectionSupervisor,
    ) -> None:
        """Test cannot start when already streaming."""
        await supervisor.start()

        with pytest.raises(RuntimeError, match="cannot start from state"):
            await supervisor.start()
        await supervisor.stop()

    def test_get_health_report(self, supervisor: ConnectionSupervisor) -> None:
        """Test getting health report."""
        report = supervisor.get_health_report()

        assert "state" in report
        assert "circuit_breaker" in report
        assert "heartbeat_timeout" in report


# =============================================================================
# Integration-style tests
# =============================================================================


class TestAdapterCoordinatorIntegration:
    """Integration tests for adapter and coordinator."""

    @pytest.fixture
    def mock_adapter(self) -> MagicMock:
        """Create mock adapter."""
        adapter = MagicMock(spec=BinanceWebSocketAdapter)
        adapter.subscribe = AsyncMock()
        adapter.unsubscribe = AsyncMock()
        return adapter

    def test_full_subscription_flow(
        self,
        mock_adapter: MagicMock,
    ) -> None:
        """Test full subscription flow."""
        coordinator = SubscriptionCoordinator(mock_adapter)

        # Add multiple subscriptions
        symbols = [
            MarketSymbol(base="BTC", quote="USDT"),
            MarketSymbol(base="ETH", quote="USDT"),
            MarketSymbol(base="XRP", quote="USDT"),
        ]

        for symbol in symbols:
            sub = StreamSubscription(
                exchange="binance",
                market_type="spot",
                symbol=symbol,
                kind="trade",
            )
            coordinator.add_subscription(sub)

        # Check desired
        assert len(coordinator.get_desired()) == 3

        # Mark all active
        for sub in coordinator.get_desired():
            coordinator.mark_active(sub)

        assert len(coordinator.get_active()) == 3
        assert len(coordinator.get_missing()) == 0
