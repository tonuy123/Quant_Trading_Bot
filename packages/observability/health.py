"""Health check system."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


@dataclass
class HealthStatus:
    """Health check result."""

    name: str
    status: str  # healthy, degraded, unhealthy
    message: str | None = None
    timestamp: datetime | None = None
    details: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Set default timestamp."""
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class HealthCheck:
    """Health check aggregator."""

    def __init__(self) -> None:
        """Initialize health check."""
        self._checks: dict[str, Callable[[], Any]] = {}

    def register(self, name: str, check: Callable[[], Any]) -> None:
        """Register a health check.

        Args:
            name: Check name
            check: Async check function
        """
        self._checks[name] = check

    async def check(self, name: str | None = None) -> HealthStatus | list[HealthStatus]:
        """Run health checks.

        Args:
            name: Specific check to run (None = all)

        Returns:
            Health status or list of statuses.
        """
        if name:
            check = self._checks.get(name)
            if not check:
                return HealthStatus(name, "unhealthy", f"Check '{name}' not found")
            return await check()  # type: ignore[no-any-return]

        results = []
        for check_name, check_fn in self._checks.items():
            try:
                result = await check_fn()
                if isinstance(result, HealthStatus):
                    results.append(result)
                else:
                    results.append(HealthStatus(check_name, "healthy", str(result)))
            except Exception as e:
                results.append(HealthStatus(check_name, "unhealthy", str(e)))

        return results

    @property
    def checks(self) -> dict[str, Callable[[], Any]]:
        """Get registered checks."""
        return self._checks


# Global health check
_health: HealthCheck | None = None


def get_health() -> HealthCheck:
    """Get global health check."""
    global _health
    if _health is None:
        _health = HealthCheck()
    return _health
