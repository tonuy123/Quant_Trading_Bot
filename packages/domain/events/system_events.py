"""System-level events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent

if TYPE_CHECKING:
    from packages.domain.enums import WorkerStatus


@dataclass(frozen=True)
class ExchangeStatusChanged(DomainEvent):
    """Event published when exchange connection status changes."""

    event_type: ClassVar[str] = "exchange_status_changed"

    exchange_name: str
    is_connected: bool
    latency_ms: int | None = None
    error_message: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "exchange_name": self.exchange_name,
            "is_connected": self.is_connected,
            "latency_ms": self.latency_ms,
            "error_message": self.error_message,
        }


@dataclass(frozen=True)
class WorkerHeartbeat(DomainEvent):
    """Event published periodically by workers to indicate liveness."""

    event_type: ClassVar[str] = "worker_heartbeat"

    worker_name: str
    worker_status: WorkerStatus
    uptime_seconds: float
    tasks_processed: int = 0
    errors_count: int = 0

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "worker_name": self.worker_name,
            "worker_status": str(self.worker_status),
            "uptime_seconds": self.uptime_seconds,
            "tasks_processed": self.tasks_processed,
            "errors_count": self.errors_count,
        }


@dataclass(frozen=True)
class WorkerStarted(DomainEvent):
    """Event published when a worker starts."""

    event_type: ClassVar[str] = "worker_started"

    worker_name: str
    worker_type: str
    config: dict[str, Any] | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "worker_name": self.worker_name,
            "worker_type": self.worker_type,
            "config": self.config,
        }


@dataclass(frozen=True)
class WorkerStopped(DomainEvent):
    """Event published when a worker stops."""

    event_type: ClassVar[str] = "worker_stopped"

    worker_name: str
    reason: str | None = None
    uptime_seconds: float | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "worker_name": self.worker_name,
            "reason": self.reason,
            "uptime_seconds": self.uptime_seconds,
        }
