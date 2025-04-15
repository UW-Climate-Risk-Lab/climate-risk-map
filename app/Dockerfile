FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

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

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
ADD . /app

# Add the virtual environment's bin directory to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Expose the port the app runs on
EXPOSE 8050

# Assumes your entrypoint is 'app:server'
CMD gunicorn -b 0.0.0.0:8050 --workers $GUNICORN_WORKERS app:server