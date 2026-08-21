"""Reconciliation worker - Reconciles positions and orders."""

from __future__ import annotations

import asyncio
import signal

from packages.config import get_settings
from packages.observability import get_logger
from packages.observability.logging import setup_logging as _setup_logging


class ReconciliationWorker:
    """Worker for position reconciliation.

    Responsibilities:
    - Compare internal positions with exchange
    - Detect discrepancies
    - Alert on mismatches
    - Update internal state
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
        self._logger.info("Starting reconciliation worker")
        self._running = True

        while self._running:
            try:
                # TODO: Fetch positions from exchange
                # TODO: Compare with internal state
                # TODO: Log discrepancies
                # TODO: Update internal state

                self._logger.debug("Reconciliation worker running")
                await asyncio.sleep(300)  # Run every 5 minutes

            except asyncio.CancelledError:
                self._logger.info("Reconciliation worker cancelled")
                break
            except Exception as e:
                self._logger.error("Reconciliation worker error", error=str(e))
                await asyncio.sleep(60)

        self._logger.info("Reconciliation worker stopped")

    async def stop(self) -> None:
        """Stop the worker."""
        self._logger.info("Stopping reconciliation worker")
        self._running = False


async def main() -> None:
    """Main entry point."""
    worker = ReconciliationWorker()

    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        worker._stop_tasks.add(asyncio.create_task(worker.stop()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
