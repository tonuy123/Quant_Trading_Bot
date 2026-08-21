"""Structured logging setup using structlog."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

import structlog

from packages.config import get_settings

if TYPE_CHECKING:
    pass


def setup_logging(
    level: str = "INFO",
    format: str = "json",
) -> None:
    """Setup structured logging.

    Args:
        level: Log level
        format: Output format (json or console)
    """
    settings = get_settings()

    # Get log level from settings
    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)

    # Configure structlog
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,  # type: ignore[arg-type]
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a structured logger.

    Args:
        name: Logger name

    Returns:
        Structured logger.
    """
    return structlog.get_logger(name)  # type: ignore[no-any-return]
