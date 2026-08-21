"""FND-CONFIG-001: flat environment contract and Compose configuration tests."""

from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from packages.config.settings import (
    _FLAT_ENVIRONMENT_MAP,
    AlertingSettings,
    BacktestSettings,
    DatabaseSettings,
    ExchangeSettings,
    LoggingSettings,
    MetricsSettings,
    RedisSettings,
    Settings,
    TradingSettings,
    WorkerSettings,
    get_settings,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DOCKER = Path(r"C:\Program Files\Docker\Docker\resources\bin\docker.exe")
_COMPOSE_SECRET_ENV = {
    "POSTGRES_PASSWORD": "config-test-postgres-password",
    "REDIS_PASSWORD": "config-test-redis-password",
}
_SECTION_MODELS = {
    "database": DatabaseSettings,
    "redis": RedisSettings,
    "exchange": ExchangeSettings,
    "worker": WorkerSettings,
    "trading": TradingSettings,
    "alerting": AlertingSettings,
    "metrics": MetricsSettings,
    "logging": LoggingSettings,
    "backtest": BacktestSettings,
}
_SETTINGS_ENVIRONMENT_NAMES = set(_FLAT_ENVIRONMENT_MAP)
_SETTINGS_ENVIRONMENT_NAMES.update(
    f"{section.upper()}__{field_name.upper()}"
    for section, model in _SECTION_MODELS.items()
    for field_name in model.model_fields
)


@pytest.fixture(autouse=True)
def _isolate_settings_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep process environment and cached settings isolated for every test."""
    get_settings.cache_clear()
    for environment_name in _SETTINGS_ENVIRONMENT_NAMES:
        monkeypatch.delenv(environment_name, raising=False)
    yield
    get_settings.cache_clear()


def _setting_value(settings: Settings, location: tuple[str | None, str]) -> object:
    section, field_name = location
    target = settings if section is None else getattr(settings, section)
    return getattr(target, field_name)


def _different_valid_value(default: object) -> tuple[str, object]:
    """Return one valid serialized value distinct from a setting's default."""
    if isinstance(default, bool):
        return ("false", False) if default else ("true", True)
    if isinstance(default, int):
        return str(default + 1), default + 1
    if isinstance(default, float):
        value = 0.5 if default < 0.5 else default + 1.0
        return str(value), value
    if default == "development":
        return "staging", "staging"
    if default == "fake":
        return "paper", "paper"
    if default == "INFO":
        return "DEBUG", "DEBUG"
    if default == "json":
        return "console", "console"
    return "configured-value", "configured-value"


@pytest.mark.parametrize(("environment_name", "location"), sorted(_FLAT_ENVIRONMENT_MAP.items()))
def test_each_documented_flat_variable_changes_its_setting(
    monkeypatch: pytest.MonkeyPatch,
    environment_name: str,
    location: tuple[str | None, str],
) -> None:
    defaults = Settings(_env_file=None)
    raw_value, expected_value = _different_valid_value(_setting_value(defaults, location))
    monkeypatch.setenv(environment_name, raw_value)

    settings = Settings(_env_file=None)

    assert _setting_value(settings, location) == expected_value


def test_env_example_documents_exactly_the_settings_contract_plus_compose_password() -> None:
    assignment = re.compile(r"^([A-Z][A-Z0-9_]*)=")
    names = {
        match.group(1)
        for line in (_PROJECT_ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if (match := assignment.match(line)) is not None
    }

    assert names - {"POSTGRES_PASSWORD"} == set(_FLAT_ENVIRONMENT_MAP)


def test_flat_name_wins_over_legacy_nested_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE__URL", "postgresql+asyncpg://nested-host:5432/quant_trading")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://flat-host:5432/quant_trading")

    settings = Settings(_env_file=None)

    assert settings.database.url == "postgresql+asyncpg://flat-host:5432/quant_trading"


def test_legacy_nested_alias_remains_supported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS__URL", "redis://nested-host:6379/0")

    settings = Settings(_env_file=None)

    assert settings.redis.url == "redis://nested-host:6379/0"


def test_dotenv_uses_the_same_flat_contract_and_precedence(tmp_path: Path) -> None:
    defaults = Settings(_env_file=None)
    expected_values: dict[tuple[str | None, str], object] = {}
    lines = ["DATABASE__URL=postgresql+asyncpg://nested-host:5432/quant_trading"]
    for environment_name, location in sorted(_FLAT_ENVIRONMENT_MAP.items()):
        if environment_name == "DATABASE_URL":
            raw_value = "postgresql+asyncpg://flat-host:5432/quant_trading"
            expected_value = raw_value
        else:
            raw_value, expected_value = _different_valid_value(_setting_value(defaults, location))
        lines.append(f"{environment_name}={raw_value}")
        expected_values[location] = expected_value
    dotenv_file = tmp_path / "settings.env"
    dotenv_file.write_text("\n".join(lines), encoding="utf-8")

    settings = Settings(_env_file=dotenv_file)

    for location, expected_value in expected_values.items():
        assert _setting_value(settings, location) == expected_value
    assert settings.database.url == "postgresql+asyncpg://flat-host:5432/quant_trading"


def test_invalid_exchange_mode_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXCHANGE_MODE", "invalid-mode")

    with pytest.raises(ValidationError):
        Settings(_env_file=None)


def test_defaults_work_without_dotenv_or_process_overrides() -> None:
    settings = Settings(_env_file=None)

    assert settings.app_env == "development"
    assert settings.exchange.mode == "fake"
    assert settings.database.url.endswith("@localhost:5432/quant_trading")
    assert settings.redis.url == "redis://localhost:6379/0"


def test_process_environment_does_not_leak_between_settings_instances(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PAPER_TRADING", "false")
    assert Settings(_env_file=None).exchange.paper_trading is False

    monkeypatch.delenv("PAPER_TRADING")
    assert Settings(_env_file=None).exchange.paper_trading is True


def _rendered_compose_config() -> dict[str, Any]:
    if not _DOCKER.is_file():
        pytest.skip("Docker Compose executable is not installed")

    environment = os.environ.copy()
    environment.update(_COMPOSE_SECRET_ENV)
    result = subprocess.run(
        [str(_DOCKER), "compose", "config", "--format", "json"],
        cwd=_PROJECT_ROOT,
        capture_output=True,
        check=False,
        env=environment,
        text=True,
    )
    assert result.returncode == 0, "docker compose config failed"
    return json.loads(result.stdout)


@pytest.fixture(scope="module")
def rendered_compose_config() -> dict[str, Any]:
    return _rendered_compose_config()


def test_compose_config_parses(rendered_compose_config: dict[str, Any]) -> None:
    assert "services" in rendered_compose_config


def test_rendered_application_services_use_compose_service_hostnames(
    rendered_compose_config: dict[str, Any],
) -> None:
    services = rendered_compose_config["services"]
    environment = services["api"]["environment"]
    assert "@postgres:5432/" in environment["DATABASE_URL"]
    assert "localhost" not in environment["DATABASE_URL"]
    assert "@redis:6379/" in environment["REDIS_URL"]
    assert "localhost" not in environment["REDIS_URL"]
    assert "@postgres:5432/" in environment["DATABASE_URL_SYNC"]


def test_postgres_and_redis_bind_only_to_loopback(rendered_compose_config: dict[str, Any]) -> None:
    services = rendered_compose_config["services"]
    for service_name, container_port in (("postgres", 5432), ("redis", 6379)):
        ports = services[service_name]["ports"]
        assert ports == [
            {
                "mode": "ingress",
                "target": container_port,
                "published": str(container_port),
                "protocol": "tcp",
                "host_ip": "127.0.0.1",
            }
        ]


def test_postgres_compose_configuration_has_no_trust_authentication(
    rendered_compose_config: dict[str, Any],
) -> None:
    postgres_environment = rendered_compose_config["services"]["postgres"]["environment"]
    assert "POSTGRES_HOST_AUTH_METHOD" not in postgres_environment
