"""Application settings using Pydantic Settings v2."""

from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
from typing import Any, Literal, cast

from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    DotEnvSettingsSource,
    EnvSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class DatabaseSettings(BaseModel):
    """Database configuration settings."""

    url: str = Field(
        "postgresql+asyncpg://postgres:postgres@localhost:5432/quant_trading",
        description="Async database URL",
    )
    url_sync: str = Field(
        "postgresql://postgres:postgres@localhost:5432/quant_trading",
        description="Sync database URL for migrations",
    )
    pool_size: int = Field(5, ge=1, le=100, description="Connection pool size")
    max_overflow: int = Field(10, ge=0, le=50, description="Max pool overflow")
    pool_timeout: int = Field(30, ge=1, description="Pool timeout seconds")
    pool_recycle: int = Field(3600, ge=0, description="Pool recycle seconds")
    echo: bool = Field(False, description="Echo SQL queries")


class RedisSettings(BaseModel):
    """Redis configuration settings."""

    url: str = Field(
        "redis://localhost:6379/0",
        description="Redis connection URL",
    )
    password: str | None = Field(None, description="Redis password")
    decode_responses: bool = Field(True, description="Decode responses")
    max_connections: int = Field(50, ge=1, description="Max connections")


class ExchangeSettings(BaseModel):
    """Exchange configuration settings."""

    mode: Literal["fake", "paper", "live"] = Field(
        "fake",
        description="Exchange operating mode",
    )
    default_exchange: str = Field("binance", description="Default exchange")
    paper_trading: bool = Field(True, description="Paper trading mode")

    # Binance
    binance_api_url: str = Field("https://api.binance.com")
    binance_ws_url: str = Field("wss://stream.binance.com:9443/ws")

    # OKX
    okx_api_url: str = Field("https://www.okx.com")
    okx_ws_url: str = Field("wss://ws.okx.com:8443/ws/v5/public")

    # Rate limiting
    max_requests_per_second: int = Field(10, ge=1, description="Max requests per second")
    burst_limit: int = Field(20, ge=1, description="Burst limit")


class WorkerSettings(BaseModel):
    """Worker configuration settings."""

    heartbeat_interval: int = Field(30, ge=1, description="Heartbeat interval seconds")
    shutdown_timeout: int = Field(60, ge=1, description="Shutdown timeout seconds")

    # Individual workers
    market_data_enabled: bool = Field(True)
    market_data_cache_ttl: int = Field(60, ge=1)

    strategy_enabled: bool = Field(True)
    strategy_interval: int = Field(60, ge=1)

    execution_enabled: bool = Field(True)
    reconciliation_enabled: bool = Field(True)
    reconciliation_interval: int = Field(300, ge=10)
    monitoring_enabled: bool = Field(True)


class TradingSettings(BaseModel):
    """Trading parameters and limits."""

    default_position_size: float = Field(1.0, ge=0, le=100, description="Default position size %")
    max_position_size: float = Field(10.0, ge=0, le=100, description="Max position size %")
    max_drawdown: float = Field(20.0, ge=0, le=100, description="Max drawdown %")
    max_daily_loss: float = Field(5.0, ge=0, le=100, description="Max daily loss %")
    max_leverage: float = Field(1.0, ge=1, le=125, description="Max leverage")


class AlertingSettings(BaseModel):
    """Alerting and notification settings."""

    enabled: bool = Field(False, description="Enable alerting")
    telegram_bot_token: str = Field("", description="Telegram bot token")
    telegram_chat_id: str = Field("", description="Telegram chat ID")
    alert_info_threshold: str = Field("INFO")
    alert_warning_threshold: str = Field("WARNING")
    alert_error_threshold: str = Field("ERROR")


class MetricsSettings(BaseModel):
    """Metrics and observability settings."""

    enabled: bool = Field(True, description="Enable metrics")
    port: int = Field(9090, ge=1024, le=65535, description="Metrics port")
    health_check_enabled: bool = Field(True)


class LoggingSettings(BaseModel):
    """Logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field("INFO")
    structlog_enabled: bool = Field(True)
    structlog_format: Literal["json", "console"] = Field("json")


class BacktestSettings(BaseModel):
    """Backtesting configuration."""

    initial_capital: float = Field(10000.0, ge=0, description="Initial backtest capital")
    commission: float = Field(0.001, ge=0, le=1, description="Commission rate")
    slippage: float = Field(0.0005, ge=0, le=1, description="Slippage rate")
    data_path: str = Field("./datasets/processed", description="Backtest data path")


# .env.example defines the public external contract.  Values use the listed
# flat name; legacy SECTION__FIELD aliases are handled by env_nested_delimiter.
# When both are present in one source, _merge_flat_environment_values applies
# the flat value last, so the documented form deterministically wins.
_FLAT_ENVIRONMENT_MAP: dict[str, tuple[str | None, str]] = {
    "ALERTS_ENABLED": ("alerting", "enabled"),
    "ALERT_ERROR_THRESHOLD": ("alerting", "alert_error_threshold"),
    "ALERT_INFO_THRESHOLD": ("alerting", "alert_info_threshold"),
    "ALERT_WARNING_THRESHOLD": ("alerting", "alert_warning_threshold"),
    "APP_ENV": (None, "app_env"),
    "APP_HOST": (None, "host"),
    "APP_NAME": (None, "app_name"),
    "APP_PORT": (None, "port"),
    "BACKTEST_COMMISSION": ("backtest", "commission"),
    "BACKTEST_DATA_PATH": ("backtest", "data_path"),
    "BACKTEST_INITIAL_CAPITAL": ("backtest", "initial_capital"),
    "BACKTEST_SLIPPAGE": ("backtest", "slippage"),
    "BINANCE_API_URL": ("exchange", "binance_api_url"),
    "BINANCE_WS_URL": ("exchange", "binance_ws_url"),
    "DATABASE_MAX_OVERFLOW": ("database", "max_overflow"),
    "DATABASE_POOL_RECYCLE": ("database", "pool_recycle"),
    "DATABASE_POOL_SIZE": ("database", "pool_size"),
    "DATABASE_POOL_TIMEOUT": ("database", "pool_timeout"),
    "DATABASE_URL": ("database", "url"),
    "DATABASE_URL_SYNC": ("database", "url_sync"),
    "DEBUG": (None, "debug"),
    "DEFAULT_EXCHANGE": ("exchange", "default_exchange"),
    "DEFAULT_POSITION_SIZE": ("trading", "default_position_size"),
    "EXCHANGE_BURST_LIMIT": ("exchange", "burst_limit"),
    "EXCHANGE_MAX_REQUESTS_PER_SECOND": ("exchange", "max_requests_per_second"),
    "EXCHANGE_MODE": ("exchange", "mode"),
    "EXECUTION_WORKER_ENABLED": ("worker", "execution_enabled"),
    "HEALTH_CHECK_ENABLED": ("metrics", "health_check_enabled"),
    "LOG_LEVEL": ("logging", "level"),
    "MARKET_DATA_CACHE_TTL": ("worker", "market_data_cache_ttl"),
    "MARKET_DATA_WORKER_ENABLED": ("worker", "market_data_enabled"),
    "MAX_DAILY_LOSS": ("trading", "max_daily_loss"),
    "MAX_DRAWDOWN": ("trading", "max_drawdown"),
    "MAX_POSITION_SIZE": ("trading", "max_position_size"),
    "METRICS_ENABLED": ("metrics", "enabled"),
    "METRICS_PORT": ("metrics", "port"),
    "MONITORING_WORKER_ENABLED": ("worker", "monitoring_enabled"),
    "OKX_API_URL": ("exchange", "okx_api_url"),
    "OKX_WS_URL": ("exchange", "okx_ws_url"),
    "PAPER_TRADING": ("exchange", "paper_trading"),
    "RECONCILIATION_WORKER_ENABLED": ("worker", "reconciliation_enabled"),
    "RECONCILIATION_WORKER_INTERVAL": ("worker", "reconciliation_interval"),
    "REDIS_DECODE_RESPONSES": ("redis", "decode_responses"),
    "REDIS_MAX_CONNECTIONS": ("redis", "max_connections"),
    "REDIS_PASSWORD": ("redis", "password"),
    "REDIS_URL": ("redis", "url"),
    "STRATEGY_WORKER_ENABLED": ("worker", "strategy_enabled"),
    "STRATEGY_WORKER_INTERVAL": ("worker", "strategy_interval"),
    "STRUCTLOG_ENABLED": ("logging", "structlog_enabled"),
    "STRUCTLOG_FORMAT": ("logging", "structlog_format"),
    "TELEGRAM_BOT_TOKEN": ("alerting", "telegram_bot_token"),
    "TELEGRAM_CHAT_ID": ("alerting", "telegram_chat_id"),
    "WORKER_HEARTBEAT_INTERVAL": ("worker", "heartbeat_interval"),
    "WORKER_SHUTDOWN_TIMEOUT": ("worker", "shutdown_timeout"),
}


def _merge_flat_environment_values(
    source_values: dict[str, Any],
    env_vars: Mapping[str, str | None],
    *,
    case_sensitive: bool,
) -> dict[str, Any]:
    """Map documented flat names to nested models after legacy nested values.

    Pydantic parses values according to the destination field type.  This
    function only routes raw text and never performs ``str``/``int``/``bool``
    coercion itself.
    """
    merged = dict(source_values)
    for env_name, (section, field_name) in _FLAT_ENVIRONMENT_MAP.items():
        lookup_name = env_name if case_sensitive else env_name.lower()
        raw_value = env_vars.get(lookup_name)
        if raw_value is None:
            continue
        if section is None:
            merged[field_name] = raw_value
            continue

        existing_section = merged.get(section)
        section_values = dict(existing_section) if isinstance(existing_section, dict) else {}
        section_values[field_name] = raw_value
        merged[section] = section_values
    return merged


class _FlatEnvironmentSource(EnvSettingsSource):
    """Process-environment source with the documented flat-name contract."""

    def __call__(self) -> dict[str, Any]:
        return _merge_flat_environment_values(
            super().__call__(),
            self.env_vars,
            case_sensitive=self.case_sensitive,
        )


class _FlatDotEnvSettingsSource(DotEnvSettingsSource):
    """Dotenv source with the same flat-name contract as process environment."""

    def __call__(self) -> dict[str, Any]:
        return _merge_flat_environment_values(
            super().__call__(),
            self.env_vars,
            case_sensitive=self.case_sensitive,
        )


def _source_options(source: EnvSettingsSource) -> dict[str, Any]:
    """Preserve Pydantic source options, including runtime `_env_file` overrides."""
    return {
        "case_sensitive": source.case_sensitive,
        "env_prefix": source.env_prefix,
        "env_prefix_target": source.env_prefix_target,
        "env_nested_delimiter": source.env_nested_delimiter,
        "env_nested_max_split": source.env_nested_max_split,
        "env_ignore_empty": source.env_ignore_empty,
        "env_parse_none_str": source.env_parse_none_str,
        "env_parse_enums": source.env_parse_enums,
    }


class Settings(BaseSettings):
    """Main application settings.

    Collects all configuration sections and environment-based overrides.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Application
    app_name: str = Field("quant_trading_bot", description="Application name")
    app_env: Literal["development", "staging", "production"] = Field(
        "development",
        description="Application environment",
    )
    debug: bool = Field(False, description="Debug mode")
    host: str = Field("0.0.0.0", description="Host to bind")
    port: int = Field(8000, ge=1024, le=65535, description="Port to bind")

    # Subsections
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)  # type: ignore[arg-type]
    redis: RedisSettings = Field(default_factory=RedisSettings)  # type: ignore[arg-type]
    exchange: ExchangeSettings = Field(default_factory=ExchangeSettings)  # type: ignore[arg-type]
    worker: WorkerSettings = Field(default_factory=WorkerSettings)  # type: ignore[arg-type]
    trading: TradingSettings = Field(default_factory=TradingSettings)  # type: ignore[arg-type]
    alerting: AlertingSettings = Field(default_factory=AlertingSettings)  # type: ignore[arg-type]
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)  # type: ignore[arg-type]
    logging: LoggingSettings = Field(default_factory=LoggingSettings)  # type: ignore[arg-type]
    backtest: BacktestSettings = Field(default_factory=BacktestSettings)  # type: ignore[arg-type]

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Load flat aliases in every environment source with flat-name precedence.

        Source order remains Pydantic's standard init > process environment >
        dotenv > file secrets.  Within process environment or dotenv, a
        documented flat name wins over its legacy ``SECTION__FIELD`` alias.
        """
        environment_source = cast(EnvSettingsSource, env_settings)
        dotenv_source = cast(DotEnvSettingsSource, dotenv_settings)
        return (
            init_settings,
            _FlatEnvironmentSource(settings_cls, **_source_options(environment_source)),
            _FlatDotEnvSettingsSource(
                settings_cls,
                env_file=dotenv_source.env_file,
                env_file_encoding=dotenv_source.env_file_encoding,
                dotenv_filtering=dotenv_source.dotenv_filtering,
                **_source_options(dotenv_source),
            ),
            file_secret_settings,
        )

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.app_env == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.app_env == "production"

    @property
    def is_safe_mode(self) -> bool:
        """Check if running in safe mode (no real money)."""
        return self.exchange.mode in {"fake", "paper"}

    def __str__(self) -> str:
        """String representation (safe, no secrets)."""
        return (
            f"Settings(app_env={self.app_env}, "
            f"exchange_mode={self.exchange.mode}, "
            f"paper_trading={self.exchange.paper_trading})"
        )


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance.

    Settings are cached to avoid repeated parsing.
    Use this function to get settings throughout the application.

    Returns:
        Cached Settings instance.
    """
    return Settings()  # type: ignore[call-arg]
