"""Market data worker - Ingests and distributes market data."""

from __future__ import annotations

import asyncio
import signal

from packages.config import get_settings
from packages.observability import get_logger
from packages.observability.logging import setup_logging as _setup_logging


class MarketDataWorker:
    """Worker for ingesting market data.

    Responsibilities:
    - Connect to exchange WebSocket
    - Receive ticker/trade/candle data
    - Validate and normalize data
    - Cache in Redis
    - Publish to event bus
    """

    def __init__(self) -> None:
        """Initialize worker."""
        settings = get_settings()
        _setup_logging(level=settings.logging.level)

        self._logger = get_logger(__name__)
        self._running = False
        self._stop_tasks: set[asyncio.Task[None]] = set()
        self._exchange = None  # TODO: Initialize exchange adapter

    async def start(self) -> None:
        """Start the worker."""
        self._logger.info("Starting market data worker")
        self._running = True

        while self._running:
            try:
                # TODO: Implement WebSocket connection
                # TODO: Subscribe to symbols
                # TODO: Process incoming data
                # TODO: Cache and publish

                self._logger.debug("Market data worker running")
                await asyncio.sleep(30)

            except asyncio.CancelledError:
                self._logger.info("Market data worker cancelled")
                break
            except Exception as e:
                self._logger.error("Market data worker error", error=str(e))
                await asyncio.sleep(5)

        self._logger.info("Market data worker stopped")

    async def stop(self) -> None:
        """Stop the worker."""
        self._logger.info("Stopping market data worker")
        self._running = False


async def main() -> None:
    """Main entry point."""
    worker = MarketDataWorker()

    # Handle shutdown signals
    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        worker._stop_tasks.add(asyncio.create_task(worker.stop()))

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
