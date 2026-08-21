"""Market Data persistence package - MD-011 local research storage.

Exports are lazy so canonical event contracts never depend on storage
implementations at import time, matching the convention of the adapters and
services packages.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "GapRecord": "ports",
    "MarketDataRepository": "ports",
    "PersistResult": "ports",
    "SqliteMarketDataStore": "sqlite_store",
    "event_to_record": "serialization",
    "record_to_event": "serialization",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Load an exported persistence symbol only when a caller asks for it."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value
