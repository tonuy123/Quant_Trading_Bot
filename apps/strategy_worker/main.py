"""Strategy worker - Evaluates strategies and generates signals."""

from __future__ import annotations

import asyncio
import signal

from packages.config import get_settings
from packages.observability import get_logger
from packages.observability.logging import setup_logging as _setup_logging


class StrategyWorker:
    """Worker for strategy evaluation.

    Responsibilities:
    - Load registered strategies
    - Get market data from cache
    - Evaluate strategies on each candle
    - Generate trading signals
    - Publish signals to event bus
    """

    def __init__(self) -> None:
        """Initialize worker."""
        settings = get_settings()
        _setup_logging(level=settings.logging.level)

        self._logger = get_logger(__name__)
        self._running = False
        self._stop_tasks: set[asyncio.Task[None]] = set()
        self._strategies: list[str] = []  # TODO: Load from registry

    async def start(self) -> None:
        """Start the worker."""
        self._logger.info("Starting strategy worker")
        self._running = True

        while self._running:
            try:
                # TODO: Get latest candle data
                # TODO: Update strategy with candles
                # TODO: Evaluate strategies
                # TODO: Publish signals

                self._logger.debug("Strategy worker running")
                await asyncio.sleep(60)  # Evaluate every minute

            except asyncio.CancelledError:
                self._logger.info("Strategy worker cancelled")
                break
            except Exception as e:
                self._logger.error("Strategy worker error", error=str(e))
                await asyncio.sleep(5)

        self._logger.info("Strategy worker stopped")

    async def stop(self) -> None:
        """Stop the worker."""
        self._logger.info("Stopping strategy worker")
        self._running = False


async def main() -> None:
    """Main entry point."""
    worker = StrategyWorker()

    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        worker._stop_tasks.add(asyncio.create_task(worker.stop()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
