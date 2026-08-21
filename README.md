# Quant Trading Bot

A production-grade quant crypto trading platform built with Python 3.12+.

## Architecture

This project follows a modular monolith architecture with strict dependency boundaries:

```
┌─────────────────────────────────────────────────────────────┐
│                        API Layer                            │
│                    (FastAPI Control Plane)                  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Application Layer                        │
│              (Commands, Queries, Services)                 │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      Domain Layer                           │
│         (Entities, Value Objects, Interfaces)               │
└─────────────────────────────────────────────────────────────┘
                          │
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure Layer                      │
│   Exchange Adapters │ Persistence │ Messaging │ Observability│
└─────────────────────────────────────────────────────────────┘
```

## Modules

| Module | Purpose |
|--------|---------|
| **Market Data** | WebSocket/REST data ingestion, caching, validation |
| **Strategies** | Signal generation, technical indicators, strategy registry |
| **Risk** | Position sizing, drawdown guards, risk policies |
| **Execution** | Order management, state machine, exchange adapters |
| **Portfolio** | Position tracking, PnL calculation, reconciliation |
| **Backtesting** | Historical simulation, execution simulation |
| **Monitoring** | Health checks, metrics, alerting |

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 16+ (via Docker or local)
- Redis 7+

### Installation

```bash
# Clone repository
git clone https://github.com/example/quant_trading_bot.git
cd quant_trading_bot

# Install dependencies
make dev

# Start infrastructure
make db-up

# Run migrations
make migrate

# Start API
make dev-api
```

### Using Docker

```bash
# Build and start all services
make docker-build
make docker-up

# View logs
make docker-logs
```

## Development

### Code Quality

```bash
make lint      # Lint with Ruff
make fmt       # Format code
make typecheck # Type check with MyPy
make test      # Run tests
```

### Workers

Workers are independent processes that can be run separately:

```bash
# Market data worker
make dev-worker-market-data

# Strategy worker
make dev-worker-strategy

# Execution worker
make dev-worker-execution
```

### Backtesting

```bash
python -m apps.backtest_cli.main --help
```

## Project Structure

```
quant_trading_bot/
├── apps/              # Application entry points
│   ├── api/           # FastAPI control plane
│   ├── workers/       # Independent worker processes
│   └── backtest_cli/  # Backtesting CLI
├── packages/          # Core packages
│   ├── domain/        # Domain entities, interfaces
│   ├── application/   # Application services
│   ├── config/        # Configuration management
│   └── ...            # Feature packages
├── tests/             # Test suites
├── migrations/       # Database migrations
└── deployments/       # Deployment configurations
```

## Scaffold Status

The monorepo scaffold is bootstrapped and verified:

| Layer | Status |
|-------|--------|
| Package layout (`packages/*`, `apps/*`) | Done - all packages/apps import cleanly |
| Domain (entities, value objects, enums, events, interfaces) | Done - dataclass-based, safe defaults |
| Application services (order, portfolio, strategy) | Done - typed, wired to interfaces |
| Feature packages (risk, execution, market_data, backtesting, ...) | Done - typed, importable |
| Configuration (pydantic-settings, env-driven, safe defaults) | Done - runs without `.env` |
| Workers & API entrypoints | Done - importable, typed |
| Lint / format / typecheck gates | Passing - `ruff`, `mypy` clean |
| Tests | 18 passing - domain unit tests + import smoke tests |
| Docker (compose + image build) | Pending - requires Docker Desktop on host |

Quality gates:

```bash
py -3.12 -m ruff check .      # lint
py -3.12 -m ruff format .     # format
py -3.12 -m mypy packages apps  # typecheck
py -3.12 -m pytest -q         # tests
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Key configuration options:
- `APP_ENV` - Environment (development/staging/production)
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string
- `EXCHANGE_MODE` - Exchange mode (fake/paper/live)
- `PAPER_TRADING` - Enable paper trading mode

## Safety

> **WARNING**: This software is for educational and research purposes. Do not use with real money unless you fully understand the risks involved.

- Always use `PAPER_TRADING=true` when testing
- Review risk policies before live trading
- Never commit real API keys to version control

## License

MIT License - see [LICENSE](LICENSE) for details.
