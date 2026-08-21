"""Execution worker - Executes orders through exchange."""

from __future__ import annotations

import asyncio
import signal

from packages.config import get_settings
from packages.observability import get_logger
from packages.observability.logging import setup_logging as _setup_logging


class ExecutionWorker:
    """Worker for order execution.

    Responsibilities:
    - Listen for risk-approved signals
    - Create and submit orders
    - Track order state
    - Handle fills and updates
    - Update positions
    """

    def __init__(self) -> None:
        """Initialize worker."""
        settings = get_settings()
        _setup_logging(level=settings.logging.level)

        self._logger = get_logger(__name__)
        self._running = False
        self._stop_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Start the worker."""
        self._logger.info("Starting execution worker")
        self._running = True

        while self._running:
            try:
                # TODO: Subscribe to risk-approved events
                # TODO: Create orders
                # TODO: Submit to exchange
                # TODO: Track order state
                # TODO: Handle fills

                self._logger.debug("Execution worker running")
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                self._logger.info("Execution worker cancelled")
                break
            except Exception as e:
                self._logger.error("Execution worker error", error=str(e))
                await asyncio.sleep(5)

        self._logger.info("Execution worker stopped")

    async def stop(self) -> None:
        """Stop the worker."""
        self._logger.info("Stopping execution worker")
        self._running = False


async def main() -> None:
    """Main entry point."""
    worker = ExecutionWorker()

    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        worker._stop_tasks.add(asyncio.create_task(worker.stop()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
