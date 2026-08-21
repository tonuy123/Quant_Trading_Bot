"""Smoke tests - verify imports, package exports, and safe defaults.

These tests protect the scaffold against:
- Circular imports / broken re-exports
- Accidentally required runtime configuration
- Value objects that cannot be constructed with minimal input
"""

import importlib

ALL_PACKAGES = [
    "packages.application",
    "packages.backtesting",
    "packages.config",
    "packages.domain",
    "packages.execution",
    "packages.market_data",
    "packages.messaging",
    "packages.observability",
    "packages.persistence",
    "packages.portfolio",
    "packages.risk",
    "packages.shared",
    "packages.strategies",
    "packages.exchanges",
]

ALL_APPS = [
    "apps.api",
    "apps.backtest_cli",
    "apps.execution_worker",
    "apps.market_data_worker",
    "apps.monitoring_worker",
    "apps.reconciliation_worker",
    "apps.strategy_worker",
]


class TestImports:
    """Every package and app must import without errors."""

    def test_all_packages_import(self) -> None:
        for name in ALL_PACKAGES:
            importlib.import_module(name)

    def test_all_apps_import(self) -> None:
        for name in ALL_APPS:
            importlib.import_module(name)

    def test_all_app_entrypoints_import(self) -> None:
        entrypoints = [
            "apps.api.main",
            "apps.backtest_cli.main",
            "apps.execution_worker.main",
            "apps.market_data_worker.main",
            "apps.monitoring_worker.main",
            "apps.reconciliation_worker.main",
            "apps.strategy_worker.main",
        ]
        for name in entrypoints:
            importlib.import_module(name)


class TestPackageExports:
    """Canonical imports must resolve through package __init__ files."""

    def test_domain_exports(self) -> None:
        from packages.domain.entities import Order, Position
        from packages.domain.enums import OrderSide, OrderType
        from packages.domain.value_objects import Price, Quantity, Symbol

        assert all([Order, Position, OrderSide, OrderType, Price, Quantity, Symbol])

    def test_interfaces_exports(self) -> None:
        from packages.domain.interfaces import (
            EventPublisher,
            ExchangeAdapter,
            OrderRequest,
            UnitOfWorkFactory,
        )

        assert all([EventPublisher, ExchangeAdapter, OrderRequest, UnitOfWorkFactory])

    def test_observability_exports(self) -> None:
        from packages.observability import get_health, get_logger, get_metrics

        assert all([get_health, get_logger, get_metrics])

    def test_risk_exports(self) -> None:
        from packages.risk.contracts import RiskCheckResult, RiskMetrics
        from packages.risk.services import RiskManager

        assert all([RiskCheckResult, RiskMetrics, RiskManager])

    def test_backtesting_exports(self) -> None:
        from packages.backtesting import BacktestEngine
        from packages.backtesting.engine import BacktestConfig, BacktestResult

        assert all([BacktestEngine, BacktestConfig, BacktestResult])


class TestSafeDefaults:
    """The system must run without external configuration."""

    def test_settings_load_with_defaults(self) -> None:
        from packages.config import get_settings

        settings = get_settings()
        assert settings.app_env is not None
        assert settings.exchange.mode in {"fake", "paper", "live"}
        assert settings.is_safe_mode in {True, False}

    def test_symbol_auto_parses_base_quote(self) -> None:
        from packages.domain.value_objects import Symbol

        symbol = Symbol("BTCUSDT")
        assert symbol.base == "BTC"
        assert symbol.quote == "USDT"

    def test_entity_id_generates(self) -> None:
        from packages.domain.value_objects import EntityId

        entity_id = EntityId.generate()
        assert entity_id.value
        assert str(entity_id)

    def test_settings_str_contains_no_secrets(self) -> None:
        from packages.config import get_settings

        rendered = str(get_settings())
        assert "postgres" not in rendered.lower() or "://" not in rendered
