"""FastAPI application - Control plane for the trading system."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from packages.config import get_settings
from packages.observability import get_health, get_logger, get_metrics
from packages.observability.logging import setup_logging as _setup_logging

# Setup logging on import
_settings = get_settings()
_setup_logging(level=_settings.logging.level, format=_settings.logging.structlog_format)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan handler."""
    logger = get_logger(__name__)
    logger.info("Starting Quant Trading Bot API", env=_settings.app_env)

    yield

    logger.info("Shutting down Quant Trading Bot API")


app = FastAPI(
    title="Quant Trading Bot API",
    description="Control plane for the quantitative crypto trading system",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, str]:
    """Root endpoint."""
    return {
        "name": "Quant Trading Bot",
        "version": "0.1.0",
        "status": "running",
    }


@app.get("/health")
async def health() -> dict[str, object]:
    """Health check endpoint."""
    health_check = get_health()
    results = await health_check.check()
    return {"status": "healthy", "checks": results}


@app.get("/metrics")
async def metrics() -> str:
    """Prometheus metrics endpoint."""
    metrics_collector = get_metrics()
    return metrics_collector.export()


@app.get("/api/v1/portfolios")
async def list_portfolios() -> dict[str, list[object]]:
    """List all portfolios."""
    return {"portfolios": []}


@app.get("/api/v1/positions")
async def list_positions() -> dict[str, list[object]]:
    """List all positions."""
    return {"positions": []}


@app.get("/api/v1/orders")
async def list_orders() -> dict[str, list[object]]:
    """List all orders."""
    return {"orders": []}


@app.get("/api/v1/strategies")
async def list_strategies() -> dict[str, list[object]]:
    """List all strategies."""
    return {"strategies": []}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
