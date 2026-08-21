"""Strategies package - Trading signal generation."""

from packages.strategies.base import BaseStrategy
from packages.strategies.contracts import Signal, StrategyConfig
from packages.strategies.registry import StrategyRegistry

__all__ = [
    "BaseStrategy",
    "Signal",
    "StrategyConfig",
    "StrategyRegistry",
]
