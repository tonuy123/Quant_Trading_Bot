# Quant Trading Bot — KPI & Execution Plan

> **Document status:** Active execution plan  
> **Current phase:** Phase 3 — Historical Data and Data Quality  
> **Project mode:** Research → Backtest → Paper Trading → Small Live  
> **Live trading status:** Disabled  
> **Single source of truth:** This file tracks what has been designed, implemented, verified, blocked, and accepted.

---

## 0. How to use this file

This document is the project execution contract for ChatGPT, Claude, DeepSeek, and the human CTO.

Before starting any task, the assigned AI must:

1. Find the task ID in this file.
2. Read its dependencies and acceptance criteria.
3. Confirm that the task is not already marked `[x]`.
4. Inspect the current code before editing.
5. Update the task to `IN PROGRESS` only if it owns that task.
6. Change only the files inside the task scope unless a dependency requires an explicit addition.
7. Run the verification commands listed in the task.
8. Update the task checkbox and report exactly what changed.
9. Stop if an acceptance criterion fails; do not silently move to the next task.

### Status convention

- `[ ]` **TODO** — not started.
- `[~]` **IN PROGRESS** — actively being implemented; only one owner at a time.
- `[x]` **DONE** — implementation and required verification passed.
- `[!]` **BLOCKED** — cannot continue; blocker and evidence must be recorded.
- `[?]` **REVIEW** — implementation exists but reviewer has not accepted it.
- `[r]` **REWORK** — reviewer rejected it; fix the listed findings before continuing.

### Ownership convention

- **ChatGPT:** Chief Architect, task decomposition, integration, review, security review, final verification.
- **Claude Opus:** complex implementation involving risk, execution, reconciliation, PnL, state machines, or backtest semantics.
- **DeepSeek:** bounded implementation, tests, fixtures, configuration, tooling, docs, and mechanical cleanup.
- **CTO/User:** final decisions on architecture, capital, credentials, risk limits, and live trading activation.

### Non-negotiable rules

- One task has one implementation owner.
- Two AI agents must not edit the same file concurrently.
- `risk`, `execution`, `portfolio`, `reconciliation`, and backtest-core tasks require ChatGPT review.
- No live exchange connection or real order placement before Phase 9 approval.
- A green test is not enough for trading-critical code; state transitions, failure behavior, idempotency, and recovery must also be reviewed.
- Never mark a task done based on a model claim. Mark it done only from observed verification evidence.
- Do not rewrite completed tasks unless a regression or explicit architecture decision requires it.

---

## 1. Definition of project completion

The system is considered technically complete for the first production-controlled release only when all of the following are true:

- Market data is received, validated, persisted, and recovered after disconnects.
- Backtest and live strategy interfaces share the same signal semantics.
- Backtesting includes fee, spread, slippage, latency, partial fills, and no lookahead bias.
- Every order intent passes through Risk Management.
- Execution is idempotent and unknown exchange state triggers reconciliation.
- Positions, balances, fills, fees, funding, and PnL reconcile with the exchange.
- Monitoring detects stale data, worker death, order failure, position mismatch, and drawdown breach.
- Paper trading has passed a minimum 14-day soak test.
- Small live trading has hard limits, a kill switch, and documented rollback/recovery procedures.
- Security, backup, restore, audit, and operational runbooks have been verified.

“Complete” does not mean profitable. Profitability is a strategy and market hypothesis, not a software acceptance guarantee.

---

## 2. Global KPI gates

| Gate | KPI | Target | Evidence |
|---|---|---:|---|
| G-01 | Critical imports pass | 100% | Import smoke test |
| G-02 | Unit/integration tests | 100% pass | Pytest output |
| G-03 | Lint/type checks | Pass | Ruff + Mypy output |
| G-04 | Docker foundation build | Pass | Docker build log |
| G-05 | Market data continuity | No unexplained gaps | Data quality report |
| G-06 | Risk bypass paths | 0 | Negative/security tests |
| G-07 | Duplicate orders after retry | 0 | Idempotency tests |
| G-08 | Position reconciliation mismatch | 0 unexplained | Reconciliation report |
| G-09 | Paper trading stability | ≥14 days | Soak-test report |
| G-10 | Critical alert delivery | 100% in test matrix | Alert test report |

Targets that depend on exchange limits, capital, or strategy economics must be recorded in `docs/risk/` or the relevant ADR before implementation. Do not invent final risk values inside code.

---

# Phase 0 — Scope, Architecture, and Safety Contract

**Objective:** Freeze the system boundary before implementation.  
**Owner:** ChatGPT  
**Reviewer:** CTO/User  
**Exit gate:** Scope, data flow, risk policy, and module boundaries are approved.

- [x] **P0-001** Define V1 scope: one exchange, one market type, initial symbols, timeframes, and paper mode.
- [x] **P0-002** Define modular-monolith architecture and worker boundaries.
- [x] **P0-003** Define data flow: market data → signal → risk → execution → portfolio/PnL.
- [x] **P0-004** Define failure policy: stale data, exchange timeout, worker death, unknown order state.
- [x] **P0-005** Define security policy: no secret in source/logs, least-privilege API keys, live mode disabled by default.
- [x] **P0-006** Define task ownership and review workflow.
- [ ] **P0-007** Approve final V1 scope in `docs/architecture/system-scope.md`.
- [ ] **P0-008** Approve initial risk policy in `docs/risk/risk-policy.md`.

**Required artifacts:**

- `docs/architecture/system-scope.md`
- `docs/architecture/system-design.md`
- `docs/architecture/data-flow.md`
- `docs/risk/risk-policy.md`
- `docs/risk/failure-matrix.md`

---

# Phase 1 — Repository Foundation and Remediation

**Objective:** Make the current repository importable, buildable, testable, and safe by default.  
**Status:** `[x] COMPLETED — 2026-08-18`  
**Current owner:** DeepSeek  
**Reviewer:** ChatGPT  
**Exit gate:** Imports, tests, lint, type-check, package install, and Docker foundation pass.

- [x] **FND-001** Create repository/module scaffold.
- [x] **FND-002** Add base configuration, tests, Docker Compose, Alembic, and documentation scaffold.
- [x] **FND-003** Fix Dockerfile virtual environment command.
- [x] **FND-004** Fix Docker builder source-copy and local-install order.
- [x] **FND-005** Fix development image to install the local project.
- [x] **FND-006** Remove duplicate module/package namespaces under `packages/risk` and `packages/strategies`.
- [x] **FND-007** Make `strategies.contracts` export only canonical strategy contracts.
- [x] **FND-008** Make `risk.policies`, `risk.services`, and `risk.validators` expose one canonical implementation each.
- [x] **FND-009** Fix runtime imports for enums used by domain entities.
- [x] **FND-010** Fix `pyproject.toml` package discovery and optional dependency groups.
- [x] **FND-011** Fix package `__init__.py` exports and import paths.
- [x] **FND-012** Add import smoke tests for all packages and application entrypoints.
- [x] **FND-013** Fix strict typing errors in changed foundation modules.
- [x] **FND-014** Ensure fake/paper mode is the default and no credential is required for imports/tests.
- [x] **FND-015** Update README to state that trading functionality is not operational.
- [x] **FND-016** Run `pytest`, Ruff, Mypy, editable install, `docker compose config`, and Docker build.
- [x] **FND-017** ChatGPT performs architecture/import/security review.
- [x] **FND-018** Fix review findings and rerun the full foundation gate.

### Phase 1 acceptance criteria

- `import packages.domain`, `import packages.risk`, `import packages.strategies`, and all worker/API entrypoints pass.
- `from packages.risk.policies import RiskPolicy` passes.
- `from packages.risk.services import RiskManager` passes.
- `from packages.strategies.contracts import Signal, StrategyConfig` passes.
- `pytest -q` passes.
- `ruff check .` passes.
- `ruff format --check .` passes.
- `mypy packages apps` passes or has explicitly documented narrow exclusions.
- Local editable installation succeeds.
- Docker Compose config and development image build succeed.
- No Binance connection or live order behavior exists.

---

# Phase 2 — Market Data Layer

**Objective:** Receive clean Binance public market data with reconnect, rate-limit discipline, persistence, and gap recovery.  
**Owner:** Claude Opus  
**Support:** DeepSeek for fixtures/data tools  
**Reviewer:** ChatGPT  
**Exit gate:** A controlled market-data soak test produces a validated, replayable dataset.

### Phase 2 ownership matrix

| Task IDs | Implementation owner | Support | Reviewer | Scope |
|---|---|---|---|---|
| MD-001 | ChatGPT | — | CTO/User | Freeze event contracts and timestamp policy before coding |
| MD-002–MD-005 | Claude Opus | DeepSeek fixtures | ChatGPT | Exchange-neutral adapter, WebSocket lifecycle, REST recovery |
| MD-006 | Claude Opus | DeepSeek test fixtures | ChatGPT | Rate-limit tracking and backoff |
| MD-007–MD-010 | Claude Opus | DeepSeek data fixtures | ChatGPT | Raw capture, normalization, candle validation, stale-data guard |
| MD-011 | DeepSeek | ChatGPT architecture review | ChatGPT | Local persistence adapter; no production database redesign |
| MD-012–MD-013 | DeepSeek | — | ChatGPT | Fake adapters, disconnect cases, replay fixtures |
| MD-014 | ChatGPT | DeepSeek test tooling | ChatGPT | Controlled 24-hour soak-test procedure and evidence |
| MD-015 | ChatGPT | — | CTO/User | Final rate-limit, timestamp, reconnect, and gap-recovery review |

No Market Data task may connect to a private Binance account or place orders. Phase 2 is public market data only.

- [x] **MD-001** Freeze market-data event contracts and timestamp units.
- [x] **MD-002** Implement Binance public WebSocket adapter behind an exchange-neutral interface.
- [x] **MD-003** Implement subscription management and stream lifecycle.
- [x] **MD-004** Implement heartbeat, ping/pong, reconnect, and exponential backoff.
- [x] **MD-005** Implement REST market-data adapter for startup sync and gap recovery.
- [x] **MD-006** Implement rate-limit/request-weight tracking and backoff.
- [x] **MD-007** Implement raw-event capture without secret values.
- [x] **MD-008** Implement event normalization for trades, tickers, and closed candles.
- [x] **MD-009** Implement OHLCV/candle validation: timestamp, OHLC bounds, duplicates, and gaps.
- [x] **MD-010** Implement stale-data detection.
- [x] **MD-011** Implement persistence adapter for local research storage.
- [x] **MD-012** Add fake WebSocket/REST fixtures and disconnect tests.
- [x] **MD-013** Add replay fixture from captured public data.
- [x] **MD-014** Run a controlled 24-hour local data soak test.
- [x] **MD-015** ChatGPT reviews rate limits, timestamp semantics, reconnect, and gap recovery.

### Phase 2 acceptance criteria

- No duplicate or unexplained missing events in the test fixture.
- Reconnect resumes without silently losing the gap.
- REST recovery is bounded and rate-limit aware.
- Stale data blocks downstream signal generation.
- Data can be replayed deterministically.

---

# Phase 3 — Historical Data and Data Quality

**Objective:** Build versioned, validated datasets for research and backtesting.  
**Owner:** DeepSeek  
**Reviewer:** ChatGPT  
**Exit gate:** A reproducible dataset passes quality checks.

- [x] **DATA-001** Define dataset metadata and version format.
- [x] **DATA-002** Implement historical download command for selected symbols/timeframes.
- [x] **DATA-003** Verify archive checksum where available.
- [x] **DATA-004** Normalize timestamp units and timezone to UTC.
- [ ] **DATA-005** Convert raw archives to research format.
- [ ] **DATA-006** Detect duplicates, gaps, invalid OHLC values, and outliers.
- [ ] **DATA-007** Generate data-quality report.
- [ ] **DATA-008** Add dataset fixture for tests.
- [ ] **DATA-009** Document source, coverage, limitations, and retention policy.
- [ ] **DATA-010** ChatGPT reviews lookahead and survivorship-bias risks in the data pipeline.

### Phase 3 acceptance criteria

- Dataset has source, symbol, interval, UTC range, version, and integrity metadata.
- Invalid records are reported, not silently discarded.
- Dataset can be regenerated from documented commands.

---

# Phase 4 — Backtesting Engine

**Objective:** Simulate the same strategy/risk semantics without future-data leakage.  
**Owner:** Claude Opus  
**Support:** DeepSeek for reports and fixtures  
**Reviewer:** ChatGPT  
**Exit gate:** Reproducible backtest with realistic execution assumptions.

- [ ] **BT-001** Freeze strategy/backtest event contracts.
- [ ] **BT-002** Implement chronological event loop.
- [ ] **BT-003** Implement data-window rules that prevent lookahead.
- [ ] **BT-004** Implement simulated order lifecycle.
- [ ] **BT-005** Implement fee model.
- [ ] **BT-006** Implement spread and slippage model.
- [ ] **BT-007** Implement latency assumption.
- [ ] **BT-008** Implement partial-fill behavior.
- [ ] **BT-009** Implement stop-loss/take-profit behavior and gap handling.
- [ ] **BT-010** Implement cash, position, fee, and PnL accounting.
- [ ] **BT-011** Implement metrics: return, drawdown, Sharpe, Sortino, profit factor, expectancy, turnover.
- [ ] **BT-012** Implement deterministic run metadata and report export.
- [ ] **BT-013** Add tests proving no future candle is visible to a decision.
- [ ] **BT-014** Add manually verifiable scenario tests.
- [ ] **BT-015** ChatGPT audits fee, slippage, partial fill, and lookahead semantics.

### Phase 4 acceptance criteria

- Same dataset/config produces the same result.
- Fees and slippage materially affect the report.
- Future data cannot influence a prior signal.
- Partial fills and stop gaps are represented.
- Backtest results are not described as proof of profitability.

---

# Phase 5 — Strategy and Signal Engine

**Objective:** Add pluggable, versioned, deterministic strategies without coupling them to execution.  
**Owner:** ChatGPT defines contract; DeepSeek implements bounded indicators; Claude reviews complex behavior.  
**Exit gate:** One deterministic strategy runs identically in backtest and paper mode.

- [ ] **SIG-001** Freeze Signal and Strategy interfaces.
- [ ] **SIG-002** Define strategy version/config metadata.
- [ ] **SIG-003** Define closed-candle versus in-progress-candle rules.
- [ ] **SIG-004** Implement indicator utility layer with tests.
- [ ] **SIG-005** Implement strategy registry/plugin loading.
- [ ] **SIG-006** Implement one baseline deterministic strategy.
- [ ] **SIG-007** Ensure strategy cannot call exchange adapters.
- [ ] **SIG-008** Persist signal reason, input timestamp, strategy version, and parameters.
- [ ] **SIG-009** Add signal replay tests.
- [ ] **SIG-010** Compare backtest and paper signal semantics.
- [ ] **SIG-011** ChatGPT reviews no-lookahead, versioning, and dependency boundaries.

---

# Phase 6 — Risk Management

**Objective:** Make Risk Management the mandatory, fail-closed gate before execution.  
**Owner:** Claude Opus  
**Reviewer:** ChatGPT  
**Final risk-policy decision:** CTO/User  
**Exit gate:** No invalid order intent can reach execution in tests or integration flow.

- [ ] **RISK-001** Freeze RiskMetrics, RiskCheckResult, and RiskDecision contracts.
- [ ] **RISK-002** Define policy evaluation order and fail-closed behavior.
- [ ] **RISK-003** Implement order validity and exchange-filter validation.
- [ ] **RISK-004** Implement position sizing with Decimal arithmetic.
- [ ] **RISK-005** Implement max position size.
- [ ] **RISK-006** Implement max total exposure.
- [ ] **RISK-007** Implement max leverage policy if/when futures is in scope.
- [ ] **RISK-008** Implement stop-loss/take-profit requirement.
- [ ] **RISK-009** Implement stale-data and spread/slippage guard.
- [ ] **RISK-010** Implement daily-loss guard.
- [ ] **RISK-011** Implement drawdown guard.
- [ ] **RISK-012** Implement kill switch and manual disable state.
- [ ] **RISK-013** Persist every approval/rejection with metrics and policy results.
- [ ] **RISK-014** Add negative tests proving risk cannot be bypassed.
- [ ] **RISK-015** Add boundary/property tests for position sizing and exposure.
- [ ] **RISK-016** ChatGPT performs security and failure-mode review.
- [ ] **RISK-017** CTO/User approves final risk values before any live mode.

### Phase 6 acceptance criteria

- Risk failure stops the order flow.
- Missing balance/position data fails closed.
- Kill switch blocks new orders.
- Drawdown/daily-loss breach blocks new orders.
- No float-based monetary calculation exists in the risk path.
- Every decision is auditable.

---

# Phase 7 — Order Execution and Exchange Adapter

**Objective:** Execute approved intents safely with idempotency and explicit unknown-state handling.  
**Owner:** Claude Opus  
**Reviewer:** ChatGPT  
**Exit gate:** Fake-exchange integration tests prove safe order lifecycle behavior.

- [ ] **EXEC-001** Freeze exchange-neutral adapter interface.
- [ ] **EXEC-002** Define order lifecycle state machine.
- [ ] **EXEC-003** Define deterministic client order ID/idempotency key.
- [ ] **EXEC-004** Implement fake exchange adapter.
- [ ] **EXEC-005** Implement order persistence and transition history.
- [ ] **EXEC-006** Implement submit flow for already-approved intents.
- [ ] **EXEC-007** Implement timeout behavior: query/reconcile before retry.
- [ ] **EXEC-008** Implement retry/backoff policy for safe operations only.
- [ ] **EXEC-009** Implement reject, cancel, expire, partial fill, and fill handling.
- [ ] **EXEC-010** Implement weighted-average fill price.
- [ ] **EXEC-011** Ensure unknown exchange state blocks unsafe follow-up actions.
- [ ] **EXEC-012** Add duplicate-submit and network-failure tests.
- [ ] **EXEC-013** ChatGPT reviews money movement, idempotency, and race conditions.

---

# Phase 8 — Portfolio, PnL, and Reconciliation

**Objective:** Maintain an auditable local view and continuously compare it with exchange state.  
**Owner:** Claude Opus  
**Support:** DeepSeek for fixtures/reports  
**Reviewer:** ChatGPT  
**Exit gate:** Local state reconciles against fake/exchange fixtures with zero unexplained mismatch.

- [ ] **PORT-001** Freeze balance, position, fill, fee, and funding contracts.
- [ ] **PORT-002** Implement fill-driven position updates.
- [ ] **PORT-003** Implement weighted average entry price.
- [ ] **PORT-004** Implement realized PnL.
- [ ] **PORT-005** Implement unrealized PnL.
- [ ] **PORT-006** Include commission and funding in PnL.
- [ ] **PORT-007** Implement balance/position snapshots.
- [ ] **PORT-008** Implement exchange reconciliation comparison.
- [ ] **PORT-009** Implement mismatch severity and alert event.
- [ ] **PORT-010** Implement restart recovery from persisted state.
- [ ] **PORT-011** Add fixture scenarios for partial fill, restart, duplicate fill, and mismatch.
- [ ] **PORT-012** ChatGPT reviews accounting invariants and recovery behavior.

---

# Phase 9 — Monitoring, Paper Trading, and Operations

**Objective:** Run the complete workflow safely without real capital.  
**Owner:** DeepSeek for monitoring/tooling; Claude for failure semantics  
**Reviewer:** ChatGPT  
**Exit gate:** Minimum 14-day paper-trading soak test passes.

- [ ] **OPS-001** Implement worker heartbeat.
- [ ] **OPS-002** Implement structured logs with correlation IDs.
- [ ] **OPS-003** Implement health/readiness checks.
- [ ] **OPS-004** Implement metrics for data lag, event lag, order states, errors, and PnL.
- [ ] **OPS-005** Implement Telegram alerts for critical failures.
- [ ] **OPS-006** Test alert delivery and duplicate-alert suppression.
- [ ] **OPS-007** Implement startup recovery and reconciliation.
- [ ] **OPS-008** Implement Windows restart/service instructions.
- [ ] **OPS-009** Implement backup and restore procedure.
- [ ] **OPS-010** Run paper trading end-to-end.
- [ ] **OPS-011** Run fault tests: network loss, exchange timeout, database restart, worker restart.
- [ ] **OPS-012** Run 14-day paper-trading soak test.
- [ ] **OPS-013** Produce daily operational and PnL reports.
- [ ] **OPS-014** ChatGPT reviews runbooks and alert coverage.

### Phase 9 acceptance criteria

- Any critical worker failure creates an alert.
- A restart does not create duplicate orders or lose tracked state.
- Reconciliation runs before trading resumes.
- Paper mode cannot place real orders.
- Soak test has no unexplained critical incident.

---

# Phase 10 — Small Live Trading Gate

**Objective:** Enable tightly controlled live trading only after all prior gates pass.  
**Owner:** ChatGPT coordinates technical gate  
**Final approval:** CTO/User  
**Exit gate:** Explicit written approval after evidence review.

- [ ] **LIVE-001** Confirm all prior phase gates are `[x]`.
- [ ] **LIVE-002** Create separate least-privilege API key.
- [ ] **LIVE-003** Verify withdrawal permission is disabled.
- [ ] **LIVE-004** Verify live mode cannot be enabled accidentally by default.
- [ ] **LIVE-005** Set hard capital, position, exposure, daily-loss, and drawdown limits.
- [ ] **LIVE-006** Verify kill switch manually and automatically.
- [ ] **LIVE-007** Run exchange testnet or equivalent safe validation where available.
- [ ] **LIVE-008** Deploy with documented rollback and recovery procedure.
- [ ] **LIVE-009** Start with one exchange, one strategy, and minimal capital.
- [ ] **LIVE-010** Monitor execution, reconciliation, slippage, and PnL daily.
- [ ] **LIVE-011** Stop immediately on unexplained mismatch or risk breach.
- [ ] **LIVE-012** Record live-trading postmortem and go/no-go decision.

No model may mark `LIVE-001` through `LIVE-012` complete without explicit CTO/User approval.

---

# Phase 11 — Scale and Optimization

**Objective:** Scale only after correctness and operational stability are proven.  
**Owner:** ChatGPT architecture; Claude implementation of complex changes; DeepSeek tooling/tests  
**Exit gate:** Capacity and failure evidence justify each scaling decision.

- [ ] **SCALE-001** Measure event volume, database growth, CPU, memory, and latency.
- [ ] **SCALE-002** Add symbols only after per-symbol data/risk tests pass.
- [ ] **SCALE-003** Add additional strategies through the plugin contract.
- [ ] **SCALE-004** Add correlation-aware exposure controls.
- [ ] **SCALE-005** Add second exchange only after adapter contract tests pass.
- [ ] **SCALE-006** Evaluate PostgreSQL/TimescaleDB versus local research storage.
- [ ] **SCALE-007** Evaluate Redis/event bus requirements from measured load.
- [ ] **SCALE-008** Introduce Kafka/ClickHouse/Kubernetes only with an ADR and measured need.
- [ ] **SCALE-009** Run capacity and chaos tests.
- [ ] **SCALE-010** Update architecture and operational runbooks.

---

## 3. Standard task handoff template

Every AI task must append or provide this information:

```text
Task ID:
Owner:
Reviewer:
Status before work:
Files inspected:
Files changed:
Files intentionally not changed:
Implementation summary:
Architecture decision:
Database impact:
Security impact:
Risk impact:
Commands executed:
Test results:
Lint results:
Type-check results:
Known limitations:
Follow-up task IDs:
```

## 4. Definition of done for one task

A task can be marked `[x]` only when:

- The implementation is inside the declared scope.
- Existing completed behavior is not regressed.
- Required tests exist and pass.
- Relevant lint and type checks pass.
- Documentation/contracts are updated when behavior changed.
- The reviewer has inspected the diff.
- Security and failure behavior are recorded where relevant.
- No duplicate implementation was introduced.
- The next task can start without guessing what changed.

## 5. Current execution log

| Date | Task ID | Owner | Change | Verification | Result |
|---|---|---|---|---|---|
| 2026-08-17 | FND-001/FND-002 | Claude | Initial repository scaffold created | Review only; runtime verification blocked by missing Python command | `[?]` |
| 2026-08-18 | FND-003–FND-018 | DeepSeek | Assigned foundation remediation | Pending | `[ ]` |
| 2026-08-18 | FND-003–FND-018 | DeepSeek + ChatGPT | Foundation remediation completed and reviewed | 18 pytest passed; Ruff passed; format passed; Mypy passed for 139 files; editable install dry-run passed; Compose config passed; Docker development image built; container import passed | `[x]` |
| 2026-08-18 | CR-001 | CTO/User | Phase 1 closed and Phase 2 authorized | Baseline governance approval recorded; no Market Data task marked complete | `APPROVED` |
| 2026-08-18 | MD-001–MD-010 | ChatGPT | GPT takeover completed the Market Data contract, public WS/REST adapters, lifecycle, rate-limit, normalization, validation, raw capture, gap recovery, and freshness guard | 85 pytest passed; Ruff passed; format passed; Mypy passed for 153 files; public-only/security scope reviewed; MD-011–MD-013 interfaces left for DeepSeek | `[x]` |
| 2026-08-18 | MD-011–MD-013 | DeepSeek | Added SQLite research persistence, deterministic fake WS/REST fixtures, disconnect/recovery tests, and replay runner without modifying MD-001–MD-010 | 113 pytest passed; Ruff passed; format passed; Mypy passed for 158 files; import smoke passed; residual raw-capacity idempotency and GapRecovery overflow wiring remain before soak | `[x]` |
| 2026-08-18 | MD-014 | ChatGPT | Added and executed deterministic fake-only 24-hour accelerated Market Data soak with bounded-resource evidence, overflow escalation, SQLite lifecycle fix, and reproducible report | Soak PASS; 117 pytest passed; Ruff passed; format passed; Mypy passed for 158 files; report: `docs/operations/MD-014-market-data-soak-2026-08-18.md` | `[x]` |
| 2026-08-18 | MD-015 | ChatGPT | Final Market Data review completed across rate limits, UTC/Decimal timestamp semantics, reconnect, gap recovery, persistence, security boundary, and scope | 117 pytest passed; soak PASS; Ruff/format/Mypy pass; graph coverage ready with no recorded gaps on reviewed paths; no Phase 3 code started during review | `[x]` |
| 2026-08-18 | DATA-001 | DeepSeek + ChatGPT | DATA-001 implementation reviewed; implementation exists but reviewer found two P2 input-normalization issues before acceptance | 156 pytest passed; Ruff/format/Mypy pass; rework required for direct dataset-ID canonicalization and strict typed record deserialization | `[?]` |
| 2026-08-18 | DATA-001 | DeepSeek + ChatGPT | DATA-001 hardening accepted: canonical dataset identity, UTC normalization, strict deserialization, and Binance Spot runtime membership validation | 197 pytest passed; Ruff passed; format passed; Mypy passed for 160 files; 14 hardening regression tests passed | `[x]` |
| 2026-08-18 | DATA-002 | DeepSeek + ChatGPT | Historical public Binance Spot kline downloader and CLI reviewed; write-failure manifest handling and Windows async test lifecycle hardened | 261 pytest passed; targeted DATA-002 tests 64 passed with exit 0; Ruff/format/Mypy pass; import smoke pass; `data-download --help` exit 0; atomic failure tests confirmed incomplete manifest and no `.tmp` leftovers | `[x]` |
| 2026-08-18 | DATA-003 | DeepSeek + ChatGPT | Added read-only streaming SHA-256 verification for raw dataset files, dataset directory reports, path/symlink safety, and checksum CLI; fixed `read_failure` false-pass aggregation | 306 pytest passed; checksum suite 45 passed; Ruff/format/Mypy pass; import smoke and `data-checksum --help` pass; mismatch/missing/invalid/read-failure exit 1 semantics verified | `[x]` |
| 2026-08-18 | DATA-004 | DeepSeek + ChatGPT | Added strict explicit timestamp-unit conversion, UTC normalization, integer epoch-millisecond serialization, half-open normalized ranges, and direct-constructor validation | 363 pytest passed; timestamp suite 57 passed; Ruff/format/Mypy pass; import smoke pass; no DATA-002/003 behavior or raw/checksum mutation; direct range construction hardening accepted | `[x]` |
| 2026-08-19 | DATA-005B-1 | Claude + ChatGPT | Implemented and reviewed the strict deterministic `ResearchCandle` schema, canonical decimal serialization, sanitized validation errors, strict record deserialization, and exact 12-field Binance kline factory with exclusive close-time boundaries | Research-format suite 180 passed; related DATA-001..004 suites 174 passed; full suite 543 passed; Ruff/format/Mypy pass; Python `-O` validation and malformed-input regressions pass | `[x]` |
| 2026-08-19 | DATA-005 (B-2A slice) | GPT Sol + ChatGPT | Implemented and reviewed the pure raw-archive line conversion kernel, typed failure model, exact-byte hashing, strict envelope/range/order checks, and centralized preview hardening for secret markers, JSON Unicode escapes, and Unicode control/format/surrogate characters | Converter + research-format suites 322 passed; independent adversarial preview probes passed; full suite 791 passed; Ruff passed; format check passed for 217 files; Mypy passed for 169 source files; five lazy-export smoke passed; DATA-005 remains open pending B-2B orchestration | `[x]` |

Update this table after every implementation batch. Never erase prior entries; add a new row.

## 6. Next authorized work

The only currently authorized implementation scope is:

```text
DATA-001 through DATA-010 — Historical Data and Data Quality
```

Phase 2 is closed after MD-015 acceptance. Do not start Phase 4 until the Phase 3 exit gate is reviewed and accepted.

---

## 7. Baseline freeze and change-control policy

This plan is the approved baseline for the project. No AI agent may silently change the plan while implementing code.

### 7.1 Changes that require explicit CTO/User approval

The following may not be changed without prior approval from the CTO/User:

- Phase order or phase scope.
- Task IDs, task wording, dependencies, or acceptance criteria.
- Technology choices or infrastructure topology.
- Database schema direction or storage strategy.
- Exchange, market type, symbols, or trading mode.
- Risk limits, kill-switch behavior, or fail-open/fail-closed behavior.
- Model ownership or review responsibility.
- Definition of Done or global KPI gates.
- Transition from fake mode to paper mode.
- Transition from paper mode to live mode.
- API-key permissions, capital allocation, or production deployment.

### 7.2 What an AI agent may change without prior plan approval

An AI agent may only make implementation changes that are already covered by an existing task ID and its acceptance criteria.

It may not create a new phase, expand a task, replace an architecture decision, or skip a gate without first submitting a change request.

### 7.3 Change-request process

When an agent discovers that the baseline plan is insufficient or incorrect, it must stop the affected task and submit this proposal instead of silently changing the plan:

```text
Change Request ID:
Requested by:
Related task/phase:
Current baseline:
Proposed change:
Reason and evidence:
Trade-offs:
Risks:
Performance/scalability impact:
Files and documents affected:
Tasks that must be added/removed/reordered:
Rollback plan:
Approval status: PENDING
```

The proposal remains `PENDING` until the CTO/User explicitly approves or rejects it. ChatGPT may analyze and recommend a change, but recommendation is not approval.

### 7.4 Approval states

- `PENDING` — proposal exists; do not implement the plan change.
- `APPROVED` — CTO/User explicitly approved; ChatGPT may update this file.
- `REJECTED` — proposal must not be implemented.
- `IMPLEMENTED` — the approved plan change has been applied and verified.

### 7.5 Baseline rule

The current phase, task order, ownership, architecture, KPI gates, and acceptance criteria in this document remain unchanged until an approved change request is recorded below.

### 7.6 Approved change log

| Change Request ID | Date | Requested change | Decision | Approved by | Verification |
|---|---|---|---|---|---|
| CR-000 | 2026-08-18 | Freeze the initial KPI plan and require explicit approval for all plan changes | APPROVED | CTO/User | Governance section recorded |
| CR-001 | 2026-08-18 | Close Phase 1 after verified foundation remediation and authorize Phase 2 Market Data tasks | APPROVED | CTO/User | Docker, tests, lint, type-check, install, and container import evidence recorded |

No other plan change is approved at this time.
