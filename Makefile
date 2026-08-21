# =============================================================================
# Makefile - Development and Operations Commands
# =============================================================================

.PHONY: help install dev test lint typecheck clean db-up db-down db-reset \
        migrate migrate-create test-all docker-build docker-up docker-down \
        backtest

# Default target
help:
	@echo "Quant Trading Bot - Available Commands"
	@echo "========================================"
	@echo "Installation:"
	@echo "  install       - Install dependencies"
	@echo "  dev           - Install development dependencies"
	@echo ""
	@echo "Database:"
	@echo "  db-up         - Start database containers"
	@echo "  db-down       - Stop database containers"
	@echo "  db-reset      - Reset database (WARNING: destroys data)"
	@echo "  migrate       - Run database migrations"
	@echo "  migrate-create MIGRATION_NAME - Create new migration"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint          - Run Ruff linter"
	@echo "  fmt           - Format code with Ruff"
	@echo "  typecheck     - Run MyPy type checker"
	@echo "  test          - Run unit tests"
	@echo "  test-all      - Run all tests (including integration)"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build  - Build Docker images"
	@echo "  docker-up     - Start all containers"
	@echo "  docker-down   - Stop all containers"
	@echo ""
	@echo "Execution:"
	@echo "  backtest      - Run backtest CLI"

# Installation
install:
	pip install -e .

dev:
	pip install -e ".[dev,lint,backtest]"

# Database
db-up:
	docker compose up -d postgres redis

db-down:
	docker compose down

db-reset:
	docker compose down -v
	docker compose up -d postgres redis
	sleep 3
	alembic upgrade head

migrate:
	alembic upgrade head

migrate-create:
	@if [ -z "$(MIGRATION_NAME)" ]; then \
		echo "Usage: make migrate-create MIGRATION_NAME=your_migration_name"; \
		exit 1; \
	fi
	alembic revision --autogenerate -m "$(MIGRATION_NAME)"

# Code quality
lint:
	ruff check .

fmt:
	ruff format .

typecheck:
	mypy packages/

test:
	pytest tests/unit -v

test-integration:
	pytest tests/integration -v

test-all:
	pytest tests/ -v --tb=short

test-contract:
	pytest tests/contract -v

test-replay:
	pytest tests/replay -v

test-property:
	pytest tests/property -v

# Docker
docker-build:
	docker compose build

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# Execution
backtest:
	python -m apps.backtest_cli.main

# Cleanup
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build/ dist/ .coverage htmlcov/

# Security
security-check:
	ruff check . --select=SEC
	pip-audit || true

# Pre-commit hooks (install)
hooks-install:
	pre-commit install

# Full CI simulation
ci: lint typecheck test-all

# Development server
dev-api:
	python -m uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 --reload

dev-worker-market-data:
	python -m apps.market_data_worker.main

dev-worker-strategy:
	python -m apps.strategy_worker.main

dev-worker-execution:
	python -m apps.execution_worker.main
