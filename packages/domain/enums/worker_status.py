"""Worker-status enumeration."""

from __future__ import annotations

from enum import StrEnum


class WorkerStatus(StrEnum):
    """Status of a worker process."""

    STARTING = "STARTING"  # Worker is initializing
    RUNNING = "RUNNING"  # Worker is actively processing
    PAUSED = "PAUSED"  # Worker is paused
    STOPPING = "STOPPING"  # Worker is shutting down
    STOPPED = "STOPPED"  # Worker has stopped
    ERROR = "ERROR"  # Worker encountered an error
    DISCONNECTED = "DISCONNECTED"  # Worker lost connection

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_running(self) -> bool:
        """Check if worker is running."""
        return self == WorkerStatus.RUNNING

    @property
    def is_terminal(self) -> bool:
        """Check if this is a terminal state."""
        return self in {WorkerStatus.STOPPED, WorkerStatus.ERROR}

    @property
    def can_start(self) -> bool:
        """Check if worker can be started."""
        return self in {WorkerStatus.STOPPED}

    @property
    def can_stop(self) -> bool:
        """Check if worker can be stopped."""
        return self in {WorkerStatus.RUNNING, WorkerStatus.PAUSED, WorkerStatus.DISCONNECTED}

    @property
    def can_pause(self) -> bool:
        """Check if worker can be paused."""
        return self == WorkerStatus.RUNNING

    @property
    def can_resume(self) -> bool:
        """Check if worker can be resumed."""
        return self == WorkerStatus.PAUSED
