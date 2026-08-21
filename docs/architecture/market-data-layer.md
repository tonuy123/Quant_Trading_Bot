# MD-001: Market Data Layer Architecture

**Status:** Proposed  
**Last verified:** 2026-08-18  
**Scope:** Binance Spot public market data. Private API, account data, orders, strategies, and live trading are excluded.

`KPI_PLAN.md` is the canonical source for task IDs and ownership. The numbered sections in this document are design topics, not additional task IDs. The implementation mapping is:

| KPI task | Implementation scope | Owner | Reviewer |
|---|---|---|---|
| MD-001 | Architecture, contracts, timestamp policy, failure matrix | ChatGPT | CTO/User |
| MD-002 | Exchange-neutral adapter boundary | Claude Opus | ChatGPT |
| MD-003 | Subscription management and stream lifecycle | Claude Opus | ChatGPT |
| MD-004 | Heartbeat, ping/pong, reconnect, exponential backoff | Claude Opus | ChatGPT |
| MD-005 | REST market-data adapter and closed-kline gap recovery | Claude Opus | ChatGPT |
| MD-006 | Rate-limit/request-weight tracking and backoff | Claude Opus | ChatGPT |
| MD-007 | Raw-event capture | Claude Opus | ChatGPT |
| MD-008 | Event normalization for trades, tickers, and closed candles | Claude Opus | ChatGPT |
| MD-009 | OHLCV/candle validation, duplicates, and gaps | Claude Opus | ChatGPT |
| MD-010 | Stale-data detection | Claude Opus | ChatGPT |
| MD-011 | Local research persistence adapter | DeepSeek | ChatGPT |
| MD-012 | Fake WebSocket/REST fixtures and disconnect tests | DeepSeek | ChatGPT |
| MD-013 | Replay fixture and deterministic replay tooling | DeepSeek | ChatGPT |
| MD-014 | Controlled 24-hour local data soak test | ChatGPT | ChatGPT |
| MD-015 | Final Market Data architecture and implementation review | ChatGPT | CTO/User |

No implementation task may change this mapping without an approved change request in `KPI_PLAN.md`.

## 1. Decision

Market Data is an exchange-neutral ingestion subsystem within the modular monolith. Binance Spot is an infrastructure adapter behind ports owned by the Market Data package.

~~~text
Binance public WebSocket
  -> raw frame capture
  -> provider parser
  -> normalize, validate, and dedupe
  -> canonical persistence or cache
  -> transactional outbox
  -> internal event publication

Binance public REST
  -> closed-candle gap recovery only
  -> the same parser, validation, dedupe, persistence, and outbox path
~~~

Two non-negotiable rules:

1. Exchange-confirmed closed klines are the authoritative CandleClosedEvent source. Production candles are not built by rolling up locally observed trades, because a disconnect makes that candle incomplete.
2. REST never silently impersonates a lost WebSocket stream. It can deterministically recover closed candles identified by open time. It cannot prove recovery of every ticker update or individual trade.

## 2. Boundary and components

### In scope

| Canonical stream | Binance public stream | Result |
| --- | --- | --- |
| trade | lowercase-symbol@trade | TradeEvent |
| ticker | lowercase-symbol@ticker | TickerEvent with rolling 24-hour statistics |
| kline | lowercase-symbol@kline_interval in UTC | CandleClosedEvent only once the exchange marks the kline closed |

The adapter accepts raw and combined WebSocket envelopes, retaining the outer stream name as raw provenance. Connection sharding remains below a configured provider limit with headroom.

### Explicitly excluded

- Credentials, API keys, listen keys, user streams, balances, account data, orders, positions, and execution.
- Depth-book reconstruction and order-book events.
- Continuous REST polling as a replacement for WebSocket data.
- A second exchange implementation.

### Responsibilities

| Component | Owns | Must not do |
| --- | --- | --- |
| MarketDataWorker | Composition root and worker lifecycle | Parse Binance payloads or contain trading logic |
| SubscriptionCoordinator | Desired subscriptions, metadata validation, sharding | Know Binance JSON fields |
| BinanceSpotWebSocketAdapter | Public transport, controls, heartbeat integration, raw ingress | Publish business events directly |
| NormalizationPipeline | Parse, normalize, validate, dedupe, provenance | Reinterpret invalid data as valid |
| ConnectionSupervisor | State machine, backoff, breaker, planned rotation | Reconnect without a bound |
| GapRecoveryService | Closed-kline REST repair | Fabricate lost trade/ticker history |
| FreshnessMonitor | Per-subscription liveness and stale episodes | Treat a quiet illiquid trade stream as a certain loss |
| MarketDataStore | Candles, cursors, gap rows, outbox | Persist high-rate ticker history by default |
| RawCaptureSink | Optional bounded audit and replay archive | Block canonical flow when storage is slow |

No Market Data component may obtain an order, account, private exchange, strategy, or risk-decision interface. The only outward flow is normalized market and health events.

## 3. Exchange-neutral contracts

These are design contracts for the later connectivity task. No code is implemented by this document.

### 3.1 Value types

~~~python
ExchangeId = Literal["binance"]
MarketType = Literal["spot"]
StreamKind = Literal["trade", "ticker", "kline"]
IngestionSource = Literal["websocket", "rest_gap_recovery", "rest_snapshot"]
ConnectionState = Literal[
    "stopped", "connecting", "subscribing", "streaming", "backing_off", "rotating", "stopping"
]
GapRecoverability = Literal["closed_candles", "snapshot_only", "none"]


@dataclass(frozen=True)
class MarketSymbol:
    base: str
    quote: str

    @property
    def canonical(self) -> str: ...  # BTC/USDT


@dataclass(frozen=True)
class StreamSubscription:
    exchange: ExchangeId
    market_type: MarketType
    symbol: MarketSymbol
    kind: StreamKind
    interval: str | None = None


@dataclass(frozen=True)
class RawMarketMessage:
    exchange: ExchangeId
    market_type: MarketType
    connection_id: str
    stream_name: str
    payload_bytes: bytes
    received_at: datetime
    received_monotonic_ns: int
    receive_sequence: int
    source_timestamp_unit: Literal["ms", "us", "unknown"]


@dataclass(frozen=True)
class ConnectionSnapshot:
    connection_id: str
    state: ConnectionState
    connected_at: datetime | None
    last_frame_received_at: datetime | None
    active_subscriptions: frozenset[StreamSubscription]
    reconnect_attempt: int
    is_gap_recovery_pending: bool
~~~

The canonical symbol is BASE/QUOTE such as BTC/USDT. The provider adapter alone translates it to Binance uppercase REST symbols and lowercase WebSocket stream names. Provider spelling never crosses the adapter boundary.

### 3.2 Ports

~~~python
class PublicMarketDataFeed(Protocol):
    async def connect(self) -> ConnectionSnapshot: ...
    async def subscribe(self, items: Collection[StreamSubscription]) -> None: ...
    async def unsubscribe(self, items: Collection[StreamSubscription]) -> None: ...
    async def raw_messages(self) -> AsyncIterator[RawMarketMessage]: ...
    async def snapshot(self) -> ConnectionSnapshot: ...
    async def close(self, reason: str) -> None: ...


class PublicMarketDataHistory(Protocol):
    async def get_server_time(self) -> datetime: ...
    async def get_instruments(
        self, symbols: Collection[MarketSymbol] | None = None
    ) -> Mapping[MarketSymbol, InstrumentRules]: ...
    async def get_closed_klines(
        self,
        symbol: MarketSymbol,
        interval: str,
        start_inclusive: datetime,
        end_exclusive: datetime,
        page_limit: int,
    ) -> Sequence[RawMarketMessage]: ...
    async def get_ticker_snapshot(self, symbol: MarketSymbol) -> RawMarketMessage: ...


class MarketEventPublisher(Protocol):
    async def publish(self, event: MarketEvent) -> None: ...


class MarketDataRepository(Protocol):
    async def get_kline_watermark(
        self, exchange: ExchangeId, symbol: MarketSymbol, interval: str
    ) -> datetime | None: ...
    async def persist_closed_candle_and_outbox(self, event: CandleClosedEvent) -> PersistResult: ...
    async def create_or_update_gap(self, gap: DataGapDetected) -> None: ...


class RawCaptureSink(Protocol):
    async def append(self, message: RawMarketMessage) -> None: ...


class RateLimitCoordinator(Protocol):
    async def acquire(
        self, endpoint: str, declared_weight: int, priority: RequestPriority
    ) -> None: ...
    async def observe_response(self, response: HttpResponse) -> None: ...
    async def block_until(self) -> datetime | None: ...
~~~

The feed returns raw messages rather than Binance-shaped events. This allows new adapters and replay fixtures to reuse one normalizer. The history port returns raw provider-shaped messages so REST and WebSocket share one mapping and validation pipeline.

## 4. Binance Spot public WebSocket adapter contract

| Contract | Requirement |
| --- | --- |
| Input | Validated Binance Spot StreamSubscription set only |
| Successful connect | Allocate opaque connection ID and publish connecting, subscribing, then streaming status after intended subscriptions are active |
| Raw ingress | Capture exact bytes, stream name, connection ID, receive sequence, aware UTC receive time, and monotonic receive time before parsing |
| Control queue | Support SUBSCRIBE and UNSUBSCRIBE with unique request ID and correlated acknowledgement or error; serialize and batch changes |
| Heartbeat | Use client-library ping/pong support; heartbeat frames never enter market-event flow |
| Shutdown | Stop intake, cancel tasks, close with safe reason, emit final status; a cancelled task never schedules retry |
| Bad payload | Quarantine malformed frame, mismatch, or unknown event; one bad frame does not kill a healthy socket |
| Security | Reject API key, secret, listen key, authentication header, private stream, and private URL configuration |

| Binance source | Normalized mapping |
| --- | --- |
| trade | TradeEvent with trade ID, price, quantity, trade and event time, buyer-is-maker |
| 24-hour ticker | TickerEvent with bid/ask, last, rolling OHLC, volume, trade range/count, window/event time |
| kline not closed | No business event; update liveness only |
| kline closed | CandleClosedEvent with interval, OHLCV, quote/taker volumes, trade count |
| socket close, shutdown, control error | ConnectionStatusChanged and potential DataGapDetected |

Binance documents public Spot stream names, raw/combined connection forms, payloads, and UTC kline streams in [Spot WebSocket market streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

Provider operational limits are external configuration with a verification timestamp. The official Binance testnet public-stream documentation currently describes 24-hour connection lifetime, 20-second pings, a five incoming control-message-per-second limit, a 1024-stream connection limit, and 300 connection attempts per five minutes per IP. Production applicability must be reconfirmed through a production connectivity contract test before any production integration. Source: [Binance public WebSocket stream limits](https://developers.binance.com/zh-CN/docs/products/spot/testnet/web-socket-streams).

Safe policy: cap sharding conservatively below verified maximum; reserve heartbeat control headroom; coalesce subscription changes; rotate sockets before configured maximum age with jitter; apply a shared IP-wide attempt limiter; reject releases with stale provider-limit verification.

## 5. REST gap-recovery contract

### 5.1 Recoverability rule

| Data kind | Contract |
| --- | --- |
| Closed candle | Recover through public GET /api/v3/klines over a bounded UTC range. Klines are identified by open time. |
| Ticker | Optionally request a current snapshot after reconnect with source set to rest_snapshot. This restores present state only. |
| Individual trade | Do not claim recovery. Recent-trades data is a current-memory snapshot, not durable replay of a missed WebSocket sequence. |

Binance documents kline identity by open time and public IP request weight for its market-data endpoint in [Spot REST market-data endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints).

### 5.2 Recovery algorithm

1. On interruption, persist disconnect time, last valid receipt time, active subscriptions, and durable candle watermark per symbol/interval. Publish potential DataGapDetected; do not invent missed trades.
2. Reconnect WebSocket and start receiving immediately into a bounded per-connection recovery buffer.
3. Get public server time through the limiter. Derive the last eligible closed interval from exchange time, never local wall clock alone.
4. For each kline subscription, page REST klines from watermark plus interval through the eligible close. Do not rely on an undocumented fixed page size.
5. Normalize and validate fully closed REST candles. Insert using the same candle natural key as WebSocket input.
6. Drain buffered WebSocket frames in receive order. REST/WebSocket races dedupe by natural key. A conflicting duplicate becomes an integrity incident, not a silent overwrite.
7. Persist gap result as recovered, no-missing-candle, unresolved, snapshot-only, or unrecoverable. Streaming may continue with unresolved gap state, but it remains observable.

~~~python
@dataclass(frozen=True)
class KlineGapRequest:
    gap_id: UUID
    symbol: MarketSymbol
    interval: str
    start_inclusive: datetime
    end_exclusive: datetime
    expected_open_times: tuple[datetime, ...]
    priority: Literal["recovery"]


@dataclass(frozen=True)
class KlineGapResult:
    gap_id: UUID
    requested_open_times: tuple[datetime, ...]
    recovered_open_times: tuple[datetime, ...]
    unresolved_open_times: tuple[datetime, ...]
    source: Literal["rest_gap_recovery"]
    attempts: int
    provider_weight_observed: int | None
~~~

REST is a targeted repair mechanism; it is never a continuous fallback poller.

## 6. Canonical market-event schemas

### 6.1 Common envelope

Canonical events are immutable, versioned, and owned by the Market Data contracts package. They contain raw-message reference only, never an opaque provider JSON blob.

~~~python
@dataclass(frozen=True)
class MarketEvent:
    event_id: UUID
    schema_version: Literal[1]
    event_type: str
    exchange: ExchangeId
    market_type: MarketType
    symbol: MarketSymbol
    source: IngestionSource
    occurred_at: datetime
    exchange_event_at: datetime | None
    received_at: datetime
    published_at: datetime | None
    connection_id: str | None
    receive_sequence: int | None
    raw_message_id: str | None
    quality_flags: frozenset[str]
~~~

Occurred time is provider business time and the only consumer market-time ordering key. Receive and published time are operational evidence; they never replace provider business time.

### 6.2 Event definitions

~~~python
@dataclass(frozen=True)
class TradeEvent(MarketEvent):
    event_type: Literal["market.trade"]
    trade_id: str
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    is_buyer_maker: bool
    first_aggregated_trade_id: str | None = None
    last_aggregated_trade_id: str | None = None


@dataclass(frozen=True)
class TickerEvent(MarketEvent):
    event_type: Literal["market.ticker"]
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    last_price: Decimal
    last_quantity: Decimal
    window_open_at: datetime
    window_close_at: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    first_trade_id: str
    last_trade_id: str
    trade_count: int


@dataclass(frozen=True)
class CandleClosedEvent(MarketEvent):
    event_type: Literal["market.candle.closed"]
    interval: str
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: Literal[True]


@dataclass(frozen=True)
class ConnectionStatusChanged(MarketEvent):
    event_type: Literal["market.connection.status_changed"]
    symbol: MarketSymbol | None
    previous_state: ConnectionState
    current_state: ConnectionState
    reason_code: str
    reconnect_attempt: int
    next_retry_at: datetime | None
    endpoint_label: str
    subscriptions_affected: int


@dataclass(frozen=True)
class DataGapDetected(MarketEvent):
    event_type: Literal["market.data_gap.detected"]
    gap_id: UUID
    stream_kind: StreamKind
    interval: str | None
    gap_start_at: datetime
    gap_end_at: datetime | None
    detection_basis: Literal[
        "connection_interruption",
        "missing_kline_slot",
        "sequence_discontinuity",
        "recovery_failure",
    ]
    certainty: Literal["potential", "confirmed"]
    recoverability: GapRecoverability
    last_known_cursor: str | None
    affected_subscription_count: int


@dataclass(frozen=True)
class MarketDataStale(MarketEvent):
    event_type: Literal["market.data.stale"]
    stream_kind: StreamKind
    interval: str | None
    last_valid_received_at: datetime | None
    stale_for_ms: int
    threshold_ms: int
    connection_state: ConnectionState
    reason_code: Literal[
        "no_valid_frame",
        "connection_down",
        "recovery_pending",
        "rate_limit_cooldown",
        "clock_untrusted",
    ]
~~~

### 6.3 Event semantics

| Event | Idempotency and ordering |
| --- | --- |
| TradeEvent | Deduplicate on exchange, market type, symbol, trade ID. Provider trade time is occurred time. |
| TickerEvent | Cache is last-write-wins on provider event time then receipt sequence. Coalesce exact duplicate payload; REST snapshot cannot overwrite a later WebSocket value. |
| CandleClosedEvent | Natural unique key: exchange, market type, symbol, interval, open time. A partial candle never produces this event. |
| ConnectionStatusChanged | Stable safe reason code and endpoint label only; do not leak URL query or credentials. |
| DataGapDetected | Interruption is potential until missing exact kline slots confirm it. Gap row keeps subsequent recovery outcome. |
| MarketDataStale | Emit once per stale episode plus controlled reminder cadence; clear only after valid fresh input. |

Binance kline close time is inclusive. Canonical candles use half-open UTC boundaries: open time is inclusive and close time is exclusive. The parser adds one millisecond to Binance close time or uses the validated interval boundary. It performs this conversion exactly once.

## 7. Timestamp and precision policy

| Topic | Policy |
| --- | --- |
| Canonical time | Aware UTC datetime only; naive datetime rejected at package boundary |
| Provider time | Parse millisecond/microsecond epoch directly; raw capture retains source resolution |
| Candle time | Half-open exact UTC interval |
| Ingress time | Capture UTC wall time and monotonic nanoseconds at raw ingress |
| Ordering | Provider business time plus natural key; never sort cross-symbol flow only by local receipt time |
| Clock health | Compare local clock to public server time; mark source untrusted beyond configured tolerance |
| Bounds | Quarantine timestamps beyond configured future skew or retention horizon, except requested historical recovery |

Microsecond provider mode is allowed only after a deployed-endpoint contract test verifies it. Canonical time supports microseconds and never fabricates nanosecond source precision.

Decimal rules:

- Parse price, quantity, and volume from provider strings into Decimal. Floats are forbidden at ingress, validation, persistence conversion, and serialization.
- Do not use global eight-decimal quantization. Preserve source scale and validate against public instrument rules such as tick size and step size.
- Reject NaN, Infinity, negative price/quantity, and non-positive trade price/quantity.
- Calculate trade quote quantity from exact Decimal multiplication under an explicit high-precision local context.
- Persist exact numeric values or canonical decimal strings. Every serializer must round-trip Decimal exactly.

## 8. Raw versus normalized policy

| Aspect | RawMarketMessage | Normalized event |
| --- | --- | --- |
| Form | Exact provider bytes and transport metadata | Stable exchange-neutral versioned contract |
| Validation | Frame and envelope only | Semantic, numeric, temporal, and continuity checks |
| Consumer | Replay, debug, quarantine only | Cache, storage, event bus, monitoring |
| Storage | Optional compressed short-retention archive | Candles, cursors, gaps, outbox durable; tickers cache/LWW |
| Failure | Sink may degrade without blocking normal flow | Candle persistence failure blocks that candle publication |
| Compatibility | Retains future provider fields | Newly mapped data needs intentional schema review |

Raw capture is asynchronous and bounded. If the queue fills, it drops raw capture with a metric and alert rather than permit unbounded memory growth or block canonical closed-candle persistence. Normalized consumers never inspect provider JSON.

## 9. Reconnect and backoff state machine

~~~text
STOPPED
  | start
  v
CONNECTING -- timeout or error --> BACKING_OFF
  | socket opened
  v
SUBSCRIBING -- control error or first-frame timeout --> BACKING_OFF
  | desired subscription set active
  v
STREAMING -- close, error, shutdown --> BACKING_OFF
  | scheduled rotation
  v
ROTATING --> CONNECTING

BACKING_OFF -- timer elapsed --> CONNECTING
any state -- stop --> STOPPING --> STOPPED
~~~

STALE and RECOVERY_PENDING are health overlays, not connection states. A socket can be streaming while one subscription is stale or an earlier data gap remains unresolved.

| Transition | Required behavior |
| --- | --- |
| start to connecting | Allocate connection ID, apply shared attempt limiter, enforce bounded connect timeout |
| connecting to subscribing | Start receive loop and heartbeat support before trusting subscription state |
| subscribing to streaming | Verify desired subscription set and create first-valid-frame deadlines |
| streaming to backing off | Persist interruption/watermarks, publish status, create possible gap, cleanly cancel receive tasks |
| backing off to connecting | Wait full jitter plus rate-limit cooldown; do nothing when stopped |
| rotating to connecting | Planned jittered renewal uses the same recovery path |
| reconnect success | Buffer frames, repair eligible candles, drain/dedupe buffer, then complete recovery |
| retry budget exhausted | Remain visibly degraded with sparse probe retries and alerting, never maximum-rate spin |

Backoff uses full jitter:

~~~text
cap = min(max_backoff, base_backoff * 2^consecutive_failure_count)
delay = random(0, cap)
~~~

Reset consecutive failures only after stable streaming and successful recovery, not simply after TCP open. Repeated protocol errors open a circuit breaker; it stops reconnect storming, leaves gap state visible, and permits only cooldown probes.

## 10. Rate-limit handling

### REST rules

1. One deployment/IP-wide RateLimitCoordinator owns all Binance public REST consumption. Horizontally scaled workers use shared coordination, for example Redis-backed.
2. Read and refresh public rate-limit metadata. Adapter endpoint weights are versioned metadata and provider used-weight headers are live consumption evidence.
3. Reserve capacity for closed-candle recovery and instrument validation ahead of optional ticker snapshots. Continuous REST polling is prohibited.
4. Keep configurable capacity headroom below provider maximum and expose queue latency plus projected depletion.
5. On HTTP 429, suspend non-critical work, honor Retry-After when supplied, and resume through jittered scheduling. Never immediate-retry.
6. On HTTP 418, stop all Binance REST until Retry-After expiry, emit urgent stale/degraded signal, and do not probe-bomb the ban.
7. On HTTP 403/WAF, do not blind retry. Open a short circuit, log sanitized endpoint class, and alert.
8. Retry public idempotent GET timeout or 5xx only through bounded backoff. A failed response never advances a candle watermark.

Binance documents IP-based request weights, used-weight headers, HTTP 429 behavior, Retry-After, and escalating HTTP 418 bans in [Binance Spot REST API information](https://developers.binance.com/en/docs/products/spot/rest-api).

### WebSocket controls

- Serialize and coalesce subscription controls; reserve provider message-rate headroom for heartbeat traffic.
- Enforce IP-wide connection attempt limiter before every socket creation.
- Every provider close, shutdown, control reject, or rate limit signal enters ConnectionStatusChanged and the state machine.
- A release gate requires current provider-limit verification and tests for control-rate/backoff policy.

## 11. Data validation

The pipeline returns one typed disposition: accepted, duplicate, quarantined, or suspicious_accepted. It never converts invalid input into a valid fact.

| Layer | Rules | Failure behavior |
| --- | --- | --- |
| Transport | Bounded frame size, UTF-8/JSON decode, valid combined envelope | Quarantine; retain healthy socket unless repeated threshold opens breaker |
| Identity | Active stream, provider symbol, market type, interval, and instrument rules agree | Quarantine and alert if repeated |
| Schema | Required fields/type shape exist; unknown fields stay raw only | Quarantine |
| Numeric | Decimal finite; trade price/quantity positive; volume non-negative; bid not above ask; count non-negative | Quarantine |
| Candle | Exchange closed flag true; valid OHLC; exact interval boundary; unique natural key | Quarantine; conflict is integrity incident |
| Time | UTC aware, bounded source time, ordered bounds, REST value inside requested range | Quarantine except explicitly requested history |
| Continuity | Exact closed-kline slot per exchange/symbol/interval | Persist confirmed gap; do not silently bridge |
| Ordering | Trade ID dedupe, ticker LWW, candle insert-if-absent | Drop duplicate cache mutation and count it |
| Anomaly | Symbol-aware price/spread/volume/clock checks | Flag/alert but retain unless objective invariant fails |

Generic fixed thresholds such as 50-percent price change, five-percent spread, or one-minute candle continuity tolerance are not ingestion truth rules. Illiquid crypto markets may legitimately violate them. Use exact candle slots and symbol-aware quality flags.

## 12. Persistence boundary

### Durable records

| Record | Natural key | Reason |
| --- | --- | --- |
| Closed candle | exchange, spot, symbol, interval, open time | Replayable canonical series |
| Ingestion watermark | exchange, symbol, interval | REST recovery start |
| Gap row | gap ID | Certainty, recovery state, retry/error history, unresolved slots |
| Outbox row | event ID or candle key | At-least-once post-commit publication |
| Instrument rules | exchange, symbol, metadata version | Symbol, interval, and precision validation |

### Cache and optional storage

- Ticker is current-state cache with source and freshness timestamp.
- Per-stream liveness, bounded trade dedupe, and recovery buffer are ephemeral.
- Raw frames and complete trade archive are optional compressed partitioned storage with explicit retention/capacity budget.

### Delivery guarantee

For a closed candle, validate and normalize; then in a single transaction insert-if-absent candle, update contiguous watermark and gap state, and insert outbox entry. Relay after commit. Consumers are at-least-once and idempotent by event ID or natural candle key.

The layer does not claim exactly-once network delivery. It guarantees idempotent durable candle state plus at-least-once publication. Outbox failure cannot erase a committed candle.

## 13. Failure matrix

| Failure | Detection | Immediate behavior | Recovery | Signal |
| --- | --- | --- | --- | --- |
| DNS/TLS/connect failure | Connect exception/deadline | BACKING_OFF; no fake connected state | Full-jitter retry through attempt limiter | Connection status and reconnect metric |
| Socket close/provider shutdown | Close callback/control event | Persist potential gap | Reconnect, buffer, candle repair | Warning then critical after retry budget |
| Ping/pong failure | Library close/read deadline | Treat as socket loss | Standard reconnect | Heartbeat-timeout reason |
| Subscribe reject | Correlated control error | Subscription stays inactive | Bounded correction/retry | Subscription alert |
| Malformed payload | Parser/schema failure | Quarantine one frame | Adapter update or breaker at threshold | Parse metric/sample |
| Symbol/interval mismatch | Identity check | Quarantine | None unless systemic | Integrity alert |
| Kline discontinuity | Exact slot check | Persist confirmed gap | REST repair | Gap/unresolved duration |
| REST 429 | HTTP status/header | Freeze non-critical REST | Retry-After scheduler | Rate-limit gauge |
| REST 418 | HTTP status/header | Block all REST | Wait then controlled probe | Urgent alert |
| REST timeout/5xx | Deadline/status | Keep gap unresolved | Bounded idempotent retry | Recovery failure |
| Database/outbox outage | Write/health check | Do not publish non-durable candle | Storage retry under bounded pressure | Critical persistence alert |
| Raw archive slow | Queue age/append failure | Drop raw archive before canonical flow | Background retry | Audit-degraded warning |
| Clock drift | Public server-time comparison | Mark local clock untrusted | NTP/system remediation | Offset alert |
| Event bus failure | Outbox relay failure | Preserve outbox | Retry relay | Outbox lag alert |
| Process crash | Supervisor restart | Durable cursor/gap/outbox survives | Startup recovery | Restart/recovery outcome |

## 14. Test matrix

MD-002 through MD-015 are network-free. Use frozen public-shaped fixtures, fake clock, fake transport, and test database. Live Binance contract smoke tests require separate authorization.

| Test class | Required proof |
| --- | --- |
| Value-object unit | Symbol canonicalization, interval rules, aware UTC requirement, exact Decimal round-trip |
| Parser unit | Trade/ticker/kline mapping, raw/combined envelopes, unknown event, malformed field |
| Candle unit | Partial kline emits nothing; closed kline emits once; close-boundary conversion; OHLC invariants |
| Dedupe/order unit | Duplicate trade, REST/WS duplicate candle, conflicting candle, stale ticker cannot overwrite fresh ticker |
| Validation unit | NaN/Infinity/float rejection, negative values, bid above ask, time bounds, zero-volume quality flag |
| Limiter unit | Weight accounting, shared cooldown, Retry-After, 429/418/403/5xx, request priority |
| State-machine unit | All allowed/forbidden transitions, jitter bounds, cancellation, rotation, circuit breaker |
| Freshness unit | No valid frame, recovery pending, cooldown, quiet-trade policy, one stale episode/reminder |
| Property tests | Generated Decimal, candle, and timestamp invariant coverage |
| Fixture contract | Versioned Binance public JSON for all streams and REST klines |
| Fake WebSocket integration | Connect/subscribe/receive/close/reconnect, recovery buffer, bad-frame isolation |
| Fake REST integration | Paginated repair, partial pages, retry, rate limit, snapshot provenance |
| Persistence/outbox integration | Atomic candle/watermark/outbox, crash-after-commit, replay, duplicate insert |
| Replay | Raw frames reproduce canonical event/candle sequence excluding receipt-time fields |
| Load/soak | Bounded queues, memory/CPU, reconnect stampede, high ticker rate |
| Security/config | Private paths and credentials rejected; logging/metrics keep safe cardinality |

## 15. File-level implementation plan

This task changes documentation only. The following code work belongs to a separately authorized connectivity implementation.

| Action | File | Responsibility |
| --- | --- | --- |
| Create now | docs/architecture/market-data-layer.md | This MD-002--MD-015 design |
| Update now | docs/architecture/README.md | Architecture-index link |
| Create later | packages/market_data/contracts/events.py | Immutable canonical events and provenance |
| Create later | packages/market_data/contracts/ports.py | Feed/history/publisher/repository/raw/limiter ports |
| Create later | packages/market_data/contracts/subscriptions.py | Symbols, subscriptions, capabilities, instruments, gaps |
| Create later | packages/market_data/adapters/binance_spot_websocket.py | Public transport, controls, raw envelope |
| Create later | packages/market_data/adapters/binance_spot_rest.py | Public REST recovery/snapshot client with no auth |
| Create later | packages/market_data/adapters/binance_spot_schemas.py | Provider parsers and mappings |
| Create later | packages/market_data/services/subscription_coordinator.py | Desired set, metadata, sharding |
| Create later | packages/market_data/services/normalization_pipeline.py | Normalize, validate, dedupe, route |
| Create later | packages/market_data/services/connection_supervisor.py | State machine, backoff, breaker, rotation |
| Create later | packages/market_data/services/gap_recovery.py | Buffered reconnect and closed-kline repair |
| Create later | packages/market_data/services/freshness_monitor.py | Stream liveness and stale episodes |
| Create later | packages/market_data/services/rate_limit_coordinator.py | Shared budgets and cooldowns |
| Create later | packages/market_data/persistence/repositories.py | Candles, cursors, gaps, outbox |
| Create later | packages/market_data/persistence/raw_capture.py | Bounded raw archive/replay |
| Modify later | packages/market_data/contracts/base.py | Retire/migrate legacy DTOs deliberately |
| Modify later | packages/market_data/contracts/schemas.py | Reuse/retire after normalized-schema coverage |
| Modify later | packages/domain/events/market_events.py | Compatibility projection only; do not force provider provenance into old DomainEvent |
| Modify later | packages/domain/interfaces/market_data.py | Keep cache interface separate from ingestion transport |
| Modify later | packages/config/settings.py | Public endpoints, symbols/intervals, limits, timeouts, retention, toggles |
| Modify later | apps/market_data_worker/main.py | Wire composition root; do not parse in worker |
| Create later | tests/unit/market_data/ | Unit, validator, limiter, state, freshness tests |
| Create later | tests/contract/market_data/ | Versioned Binance public fixtures |
| Create later | tests/integration/market_data/ | Fake transport, recovery, persistence/outbox/replay |
| Create later | tests/property/market_data/ | Decimal, OHLC, timestamp property tests |

The existing MarketDataAggregator remains usable for research/backtest transformations, but it must not be the authority for live-style CandleClosedEvent production.

## 16. Acceptance criteria mapped to KPI task IDs

### Implemented Phase 2 alignment (MD-002 through MD-010)

The initial file plan used aspirational names. The compatible implementation
uses the existing package layout instead of introducing a second set of ports
or schemas:

| Concern | Current implementation | Boundary rule |
| --- | --- | --- |
| Neutral values and ports | `adapters/value_types.py`, `adapters/protocols.py` | Provider names stay inside Binance adapters; consumers receive canonical symbols and raw transport records. |
| Canonical events | `contracts/events.py` | This is the sole new canonical event schema. Legacy `contracts/base.py` DTOs remain compatibility-only. |
| Public Binance transport | `adapters/binance_ws.py`, `adapters/binance_rest.py` | Public Spot endpoints only; URL validation rejects user-data, listen-key, signing, and API-key configuration. |
| Provider parsing | `adapters/binance_normalizer.py` | WebSocket raw/combined envelopes and REST payloads feed one canonical mapper. |
| Ingress correctness | `services/normalization.py`, `services/raw_capture.py` | Raw capture precedes canonical mapping; capture failure/drop never blocks canonical validation. |
| Recovery and liveness | `services/gap_recovery.py`, `services/freshness.py` | REST repairs only closed candles; live frames are bounded-buffered during recovery; trades are never reconstructed. |
| Rate and lifecycle | `services/rate_limit.py`, `adapters/connection_supervisor.py` | REST cooldowns and WebSocket control/connection headroom are observable; a socket is only `streaming` after an actual market frame. |

`SubscriptionCoordinator` remains at `adapters/subscription_coordinator.py`
for compatibility with the existing worker layout. Its active set is populated
only from provider-acknowledged subscriptions; a sent `SUBSCRIBE` command is
not treated as activation. `apps/market_data_worker/main.py` remains a
composition-root placeholder: persistence, replay, and production wiring are
reserved for MD-011 through MD-013 and are not part of this implementation.

### MD-001 — Architecture and contracts

- [x] Public-only boundary excludes account, order, execution, risk-decision, and strategy dependencies.
- [x] Feed, history, publisher, repository, raw-capture, limiter, event, timestamp, and failure contracts are documented.
- [x] Claude and DeepSeek write scopes are disjoint and approved.

### MD-002 — Exchange-neutral adapter boundary

- [x] Provider-specific code stays behind Market Data ports.
- [x] BASE/QUOTE symbols prevent Binance spelling from crossing the adapter boundary.
- [x] Worker remains composition root and contains no provider parsing or trading logic.

### MD-003 — Subscription management and stream lifecycle

- [x] Desired subscriptions, validation, control requests, acknowledgements, batching, and sharding are explicit.
- [x] Raw and combined WebSocket envelopes retain stream provenance.
- [x] No private stream or credential path is accepted.

### MD-004 — Heartbeat, reconnect, and backoff

- [x] State machine, heartbeat, cancellation, jittered retry, rotation, breaker, and bounded connection attempts are implemented and tested.
- [x] Socket loss creates observable status and potential gap state.
- [x] Reconnect does not silently discard recovery information.

### MD-005 — REST adapter and gap recovery

- [x] REST repairs closed kline slots only and uses the shared normalization/validation path.
- [x] Reconnect buffers fresh WebSocket data while bounded recovery runs.
- [x] Ticker snapshots are labelled as snapshots; missed individual trades are not falsely reconstructed.

### MD-006 — Rate-limit tracking and backoff

- [x] Shared REST budget observes endpoint weights and response headers.
- [x] HTTP 429 honors `Retry-After`; HTTP 418 halts probes; HTTP 403/5xx are not blindly retried.
- [x] WebSocket control and connection-attempt limits reserve heartbeat headroom.

### MD-007 — Raw-event capture

- [x] Raw bytes and transport metadata are captured before parsing when enabled.
- [x] Capture is bounded and cannot cause unbounded memory growth.
- [x] Raw capture failure does not corrupt canonical normalized events.

### MD-008 — Event normalization

- [x] Trade, ticker, and closed-kline mappings produce stable versioned canonical events.
- [x] Partial klines never produce `CandleClosedEvent`.
- [x] Decimal values and aware UTC timestamps are preserved without float conversion.

### MD-009 — OHLCV validation, duplicates, and gaps

- [x] Invalid transport/schema/numeric/identity/candle/time input is quarantined with a reason.
- [x] Candle natural keys make REST/WebSocket duplicates idempotent.
- [x] Exact missing kline slots create an observable confirmed gap; conflicts become incidents.

### MD-010 — Stale-data detection

- [x] Freshness is tracked per subscription/stream kind.
- [x] Quiet illiquid streams are not automatically treated as a confirmed data loss.
- [x] Stale episodes emit one alert plus controlled reminders and clear only after valid fresh input.

### MD-011 — Local research persistence

- [x] Candle, cursor, gap, and outbox records have idempotent natural keys.
- [x] Closed candle, watermark, gap state, and outbox commit atomically.
- [x] Ticker remains cache/LWW by default; raw archive has an explicit bounded retention policy.

### MD-012 — Fake adapters and disconnect tests

- [x] Fake WebSocket/REST fixtures cover connect, subscribe, receive, malformed frame, close, reconnect, timeout, and recovery.
- [x] Tests are network-free and deterministic.
- [x] Tests prove no live Binance access occurs.

### MD-013 — Replay fixture

- [x] Versioned raw-shaped fixtures reproduce canonical events deterministically.
- [x] Replay proves duplicate and REST/WebSocket race behavior.
- [x] Receipt-time fields do not make replay results nondeterministic.

### MD-014 — Controlled soak test

- [x] A 24-hour local test records event count, duplicates, gaps, reconnects, recovery attempts, stale episodes, resource usage, and storage growth.
- [x] No unbounded queue, retry, reconnect, or raw-capture growth occurs.
- [x] The report is attached to the execution log before the task is marked done.

### MD-015 — Final review

- [x] Rate-limit, timestamp, reconnect, gap-recovery, security, persistence, and scope review passes.
- [x] All previous Market Data acceptance criteria are evidenced.
- [x] No Phase 3 work was started before the Phase 2 exit gate was accepted.

## 17. Trade-offs, risk, and scale

| Decision | Trade-off | Residual risk | Scale/performance |
| --- | --- | --- | --- |
| Exchange-confirmed klines | Provider cadence replaces local instant candle control | Provider latency/error still needs monitoring | Avoids trade-buffer growth and incomplete candles |
| Raw plus normalized | Two representations and storage cost | Retention and parser evolution require governance | Bounded raw sink protects canonical flow |
| REST candle repair only | Cannot recreate tick/trade history | Some gaps are permanently unrecovered | Avoids false precision and continuous REST weight |
| Durable candle/outbox | Database writes and idempotent consumers | Outbox lag during downstream outage | Correct replayable candles/horizontal consumers |
| Shared limiter/breaker | Coordination dependency at horizontal scale | Coordinator needs conservative fail-closed fallback | Prevents IP ban and retry amplification |
| Stream-level freshness | More state and metrics | Quiet trades need configured policy | Linear in subscriptions; metric labels stay bounded |

This is appropriate for a modular-monolith worker fleet with tens to low hundreds of symbols and several intervals per symbol, after capacity testing shard count, raw retention, database writes, and outbox lag. Do not split into microservices merely because connection count increases.
