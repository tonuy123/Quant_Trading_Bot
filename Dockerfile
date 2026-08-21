# =============================================================================
# Dockerfile - Production Docker Image
# =============================================================================

FROM python:3.12-slim as base

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# Builder Stage
# =============================================================================
FROM base as builder

# Install build dependencies
RUN pip install --upgrade pip

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy project files for local installation
COPY pyproject.toml README.md ./
COPY packages/ ./packages/
COPY apps/ ./apps/

# Install Python dependencies
RUN pip install --no-cache-dir .

# =============================================================================
# Development Stage
# =============================================================================
FROM base as development

# Install development dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy project files for local installation
COPY pyproject.toml README.md ./
COPY packages/ ./packages/
COPY apps/ ./apps/

# Install dependencies with dev and lint extras
RUN pip install --upgrade pip && \
    pip install --no-cache-dir ".[dev,lint]"

# Set working directory
WORKDIR /app
COPY . /app

# Default command
CMD ["python", "-m", "uvicorn", "apps.api.main", "--host", "0.0.0.0", "--port", "8000"]

# =============================================================================
# Production Stage
# =============================================================================
FROM base as production

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

# Set working directory
WORKDIR /app
COPY --chown=1000:1000 . /app

# Create non-root user
RUN groupadd --gid 1000 appgroup && \
    useradd --uid 1000 --gid appgroup --shell /bin/bash --create-home appuser
USER appuser

# Default command
CMD ["python", "-m", "uvicorn", "apps.api.main", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
