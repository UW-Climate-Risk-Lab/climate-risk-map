# Use Python 3.12+ base image
FROM ghcr.io/astral-sh/uv:0.7-python3.12-bookworm-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Configure uv - enable bytecode compilation for faster app start time
ENV UV_COMPILE_BYTECODE=1

# Disable uv trying to manage Python versions, use the one from the base image
ENV UV_PYTHON_DOWNLOADS=0

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --no-dev

# Copy application code
ADD . /app

# Expose ports for Dask dashboard (8787 is default)
EXPOSE 8787

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/.venv/bin:$PATH"

# Set the entrypoint to run the Python script
ENTRYPOINT ["python", "/app/src/pipeline.py"]