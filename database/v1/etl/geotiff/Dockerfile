FROM debian:bookworm-slim

COPY --from=ghcr.io/astral-sh/uv:0.7.8 /uv /uvx /bin/

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    libgdal-dev \
    --fix-missing

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal \
    CMAKE_PREFIX_PATH='/usr/include/gdal;/usr/include/geos' \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app"


# Set working directory
WORKDIR /app

# Configure uv - enable bytecode compilation for faster app start time
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --no-install-project --no-dev

COPY src/main.py  .
COPY src/regions ./regions
    
ENTRYPOINT ["/bin/bash", "-c", "source /app/.venv/bin/activate && exec python main.py \"$@\"", "--"]