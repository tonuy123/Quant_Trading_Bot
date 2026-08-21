"""Monitoring worker - Health checks, metrics, and alerting."""

from __future__ import annotations

import asyncio
import signal
from datetime import datetime

from packages.config import get_settings
from packages.observability import get_health, get_logger
from packages.observability.logging import setup_logging as _setup_logging


class MonitoringWorker:
    """Worker for system monitoring.

    Responsibilities:
    - Health checks
    - Metrics collection
    - Alerting
    - Heartbeat publishing
    """

    def __init__(self) -> None:
        """Initialize worker."""
        settings = get_settings()
        _setup_logging(level=settings.logging.level)

        self._logger = get_logger(__name__)
        self._running = False
        self._stop_tasks: set[asyncio.Task[None]] = set()
        self._start_time = datetime.utcnow()

    async def start(self) -> None:
        """Start the worker."""
        self._logger.info("Starting monitoring worker")
        self._running = True

        while self._running:
            try:
                # Health check
                health = get_health()
                results = await health.check()
                self._logger.debug("Health check", results=results)

                # Calculate uptime
                uptime = (datetime.utcnow() - self._start_time).total_seconds()
                self._logger.debug("Monitoring worker running", uptime_seconds=uptime)

                await asyncio.sleep(30)

            except asyncio.CancelledError:
                self._logger.info("Monitoring worker cancelled")
                break
            except Exception as e:
                self._logger.error("Monitoring worker error", error=str(e))
                await asyncio.sleep(30)

        self._logger.info("Monitoring worker stopped")

    async def stop(self) -> None:
        """Stop the worker."""
        self._logger.info("Stopping monitoring worker")
        self._running = False


async def main() -> None:
    """Main entry point."""
    worker = MonitoringWorker()

    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        worker._stop_tasks.add(asyncio.create_task(worker.stop()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
