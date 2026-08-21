"""Exchange adapters and neutral Market Data ingress contracts.

Imports are lazy so canonical event contracts can safely refer to neutral
adapter value types without provider adapters creating a circular import.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "BinancePayloadError": "binance_normalizer",
    "BinanceSpotNormalizer": "binance_normalizer",
    "BinanceSpotRestAdapter": "binance_rest",
    "BinanceSubscriptionRejected": "binance_ws",
    "BinanceSymbolMapper": "binance_ws",
    "BinanceWebSocketAdapter": "binance_ws",
    "CircuitBreaker": "connection_supervisor",
    "ConnectionSnapshot": "value_types",
    "ConnectionState": "value_types",
    "ConnectionSupervisor": "connection_supervisor",
    "ExchangeId": "value_types",
    "GapRecoverability": "value_types",
    "HttpResponse": "binance_rest",
    "IngestionSource": "value_types",
    "MarketSymbol": "value_types",
    "MarketType": "value_types",
    "PublicHttpTransport": "binance_rest",
    "PublicMarketDataFeed": "protocols",
    "PublicMarketDataHistory": "protocols",
    "PublicMarketDataRequestError": "binance_rest",
    "RawMarketMessage": "value_types",
    "StreamKind": "value_types",
    "StreamSubscription": "value_types",
    "SubscriptionCoordinator": "subscription_coordinator",
    "SubscriptionKey": "subscription_coordinator",
    "UrllibPublicHttpTransport": "binance_rest",
    "WebSocketControlBudgetExceeded": "binance_ws",
    "calculate_backoff": "binance_ws",
    "is_retry_safe": "binance_ws",
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Load an exported adapter symbol only when a caller asks for it."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f"{__name__}.{module_name}")
    value = getattr(module, name)
    globals()[name] = value
    return value
