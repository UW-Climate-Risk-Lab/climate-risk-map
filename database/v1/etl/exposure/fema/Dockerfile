FROM debian:bookworm-slim

COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /uvx /bin/

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    libgdal-dev \
    libgeos-dev \
    python3 \
    python3-venv \
    python3-pip \
    --fix-missing

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    CMAKE_PREFIX_PATH='/usr/include/gdal;/usr/include/geos'

# Set working directory
WORKDIR /app

# Configure uv - enable bytecode compilation for faster app start time
ENV UV_COMPILE_BYTECODE=1

# Disable uv trying to manage Python versions, use the one from the base image
# ENV UV_PYTHON_DOWNLOADS=0

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --no-dev

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app"

# Set working directory
WORKDIR /app

COPY main.py .
    
ENTRYPOINT ["/bin/bash", "-c", "uv run main.py \"$@\"", "--"]