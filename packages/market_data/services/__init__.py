"""Market Data services with lazy exports to preserve contract boundaries."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "BinanceRateLimitCoordinator": "rate_limit",
    "BoundedRawCapture": "raw_capture",
    "FreshnessMonitor": "freshness",
    "GapRecoveryBuffer": "gap_recovery",
    "GapRecoveryGate": "gap_recovery",
    "GapRecoveryResult": "gap_recovery",
    "GapRecoveryService": "gap_recovery",
    "GapState": "normalization",
    "MarketDataAggregator": "data_aggregator",
    "MarketDataNormalizer": "data_normalizer",
    "NormalizationPipeline": "normalization",
    "ProcessResult": "normalization",
    "QuarantinedMessage": "normalization",
    "RateLimitBlockedError": "rate_limit",
    "RateLimitSnapshot": "rate_limit",
    "RawCaptureSink": "raw_capture",
    "WebSocketRateBudget": "rate_limit",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Load an exported service symbol only when a caller asks for it."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value
