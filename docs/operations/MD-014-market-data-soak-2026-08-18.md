# MD-014 — Controlled 24-hour local Market Data soak test

**Status:** PASS  
**Scope:** deterministic accelerated local simulation; fake WebSocket + fake REST only  
**Simulated UTC window:** 2026-08-13T00:00:00+00:00 to 2026-08-14T00:01:07+00:00  
**Reproduce:** `py -3.12 -m tests.soak.market_data_soak --report docs/operations/MD-014-market-data-soak-2026-08-18.md`

## Controls

- No live Binance connection, private API, API key, account endpoint, order, strategy, or Phase 3 component was imported or invoked.
- `FakeWebSocketAdapter` drives a fixed public-shaped frame script; `FakeRestAdapter` drives all server-time and kline recovery calls.
- The market-data cadence advances exactly 1,440 one-minute slots (24 hours) without wall-clock sleep. Deliberate stale/cooldown timers add 67 simulated seconds, which are visible in the reported UTC window; SQLite is local and disposable test storage.
- The run injects a disconnect/reconnect, malformed frames, duplicates, an out-of-order candle, partial klines, three exact kline gaps, HTTP 429 cooldown, REST recovery, and a deliberate recovery-buffer overflow.

## Measured flow

| Metric | Measured value |
| --- | ---: |
| Raw frames observed | 2052 |
| Canonical events emitted | 2030 |
| Accepted | 2021 |
| Duplicate | 10 |
| Quarantined | 8 |
| Ignored | 12 |
| Pre-normalization recovery-buffer drops | 1 |
| Exact kline gaps detected | 3 |
| Intentional out-of-order frame injection | 1 |
| Disconnects / reconnects | 1 / 1 |
| WebSocket connection attempts / retries | 3 / 1 |
| REST recovery attempts / successes / failures | 4 / 3 / 1 |
| Recovery retry attempts | 1 |
| HTTP 429 / cooldown-blocked retry | 1 / 1 |
| WebSocket control / connection headroom rejections | 1 / 1 |
| Stale episodes / reminders / total stale events | 3 / 3 / 6 |
| Recovery-buffer overflows / escalations | 1 / 1 |
| Raw-capture drops / affected canonical inputs | 2019 / 2019 |

## SQLite growth

| Storage metric | Bytes / rows |
| --- | ---: |
| Initial SQLite footprint | 0 |
| Final SQLite footprint (DB + WAL + SHM) | 3186688 |
| Database / WAL / SHM bytes | 3186688 / 0 / 0 |
| SQLite `candles` rows | 1440 |
| SQLite `watermarks` rows | 1 |
| SQLite `gaps` rows | 3 |
| SQLite `outbox` rows | 1440 |
| SQLite `tickers` rows | 1 |
| SQLite `raw_messages` rows | 32 |

## CPU and memory

| Metric | Measured value |
| --- | ---: |
| Process CPU time (seconds) | 28.187500 |
| Python traced heap current (bytes) | 2569510 |
| Python traced heap peak (bytes) | 2590178 |

`tracemalloc` measures Python allocations for this process; it is reported explicitly rather than mislabelling it as system-wide RSS.

## Bounded-resource proof

| Component | Capacity | High-water | Final | Drops | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| SQLite raw capture archive | 32 | 32 | 32 | 2019 | PASS |
| Recovery WebSocket buffer | 2 | 2 | 0 | 1 | PASS |
| Soak escalation queue | 16 | 1 | 1 | 0 | PASS |
| WebSocket connect retry loop | 3 | 2 | 0 | 0 | PASS |
| REST recovery retry loop per gap | 2 | 2 | 0 | 0 | PASS |

## Recovery-overflow escalation

The overflow is a deliberate fault injection. `GapRecoveryGate` produced a typed `RecoveryOverflowIncident`; the bounded local soak monitor retained a critical, payload-free escalation. This validates observability without starting Phase 9 alert delivery work.

| Escalation | Severity | Connection | Buffer capacity | Cumulative dropped frames |
| --- | --- | --- | ---: | ---: |
| recovery-overflow:fake-ws-1:3 | critical | fake-ws-1 | 2 | 1 |

**Unexplained critical incidents:** 0

## Completion decision

MD-014 evidence is complete: all required fault classes ran, the deliberately injected critical overflow was retained and escalated, no critical incident is unexplained, and every measured bounded component stayed within its configured capacity.
