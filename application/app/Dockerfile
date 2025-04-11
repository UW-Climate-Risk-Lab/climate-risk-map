# Stage 1: Build environment with uv
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Set desired working directory (using /app as standard, adjust if needed)
WORKDIR /app

# Install system dependencies (like build-essential for C extensions, add others like libgdal-dev if needed by your Python packages)
# Doing this early helps Docker layer caching if these don't change often.
RUN apt-get update && apt-get install -y \
    build-essential \
    # Add any other required system packages here, e.g., libgdal-dev
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Configure uv - enable bytecode compilation and copy link mode for Docker
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Disable uv trying to manage Python versions, use the one from the base image
ENV UV_PYTHON_DOWNLOADS=0

# Install dependencies using uv sync and BuildKit caching
# Requires pyproject.toml and uv.lock in the build context
# --frozen: Use uv.lock for exact versions
# --no-dev: Exclude development dependencies (like poetry's --without dev)
# --no-install-project: Don't install the local project itself (like poetry's --no-root)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-dev --no-install-project

# Copy the rest of your application code into the builder stage
# This happens *after* dependencies are installed for better caching
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Add the virtual environment's bin directory to the PATH
# This makes tools installed in the venv (like gunicorn) directly executable
ENV PATH="/app/.venv/bin:$PATH"

# Expose the port the app runs on
EXPOSE 8050

# Run the application using gunicorn (no 'poetry run' or 'uv run' needed due to PATH)
# Assumes your entrypoint is 'app:server' as in your original Dockerfile
CMD uv run gunicorn -b 0.0.0.0:8050 --workers $GUNICORN_WORKERS app:server