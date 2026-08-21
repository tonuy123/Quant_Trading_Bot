"""ID generation utilities."""

from __future__ import annotations

import uuid
from datetime import datetime


def generate_id(prefix: str | None = None) -> str:
    """Generate a unique ID.

    Args:
        prefix: Optional prefix for the ID

    Returns:
        Unique ID string.
    """
    unique_id = str(uuid.uuid4())
    if prefix:
        return f"{prefix}_{unique_id}"
    return unique_id


def generate_short_id(prefix: str | None = None) -> str:
    """Generate a short unique ID (8 characters).

    Args:
        prefix: Optional prefix for the ID

    Returns:
        Short unique ID string.
    """
    unique_id = uuid.uuid4().hex[:8]
    if prefix:
        return f"{prefix}_{unique_id}"
    return unique_id


def generate_order_id() -> str:
    """Generate a client order ID.

    Returns:
        Order ID with timestamp prefix.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    return f"ORD_{timestamp}"


def generate_position_id() -> str:
    """Generate a position ID.

    Returns:
        Position ID.
    """
    return generate_id("POS")


def generate_signal_id(strategy_id: str | None = None) -> str:
    """Generate a signal ID.

    Args:
        strategy_id: Optional strategy ID

    Returns:
        Signal ID.
    """
    prefix = f"SIG_{strategy_id}" if strategy_id else "SIG"
    return generate_id(prefix)


def generate_portfolio_id() -> str:
    """Generate a portfolio ID.

    Returns:
        Portfolio ID.
    """
    return generate_id("PF")


def is_valid_uuid(value: str) -> bool:
    """Check if a string is a valid UUID.

    Args:
        value: String to check

    Returns:
        True if valid UUID.
    """
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False
