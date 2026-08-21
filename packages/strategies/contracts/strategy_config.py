"""Strategy configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.enums import Timeframe


@dataclass
class StrategyConfig:
    """Configuration for a strategy instance."""

    strategy_id: str
    name: str
    symbols: list[str]
    timeframe: "Timeframe"
    enabled: bool = False
    parameters: dict[str, Any] = field(default_factory=dict)
