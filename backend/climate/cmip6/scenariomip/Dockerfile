FROM python:3.12.5-bullseye AS builder

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    --fix-missing
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal \
    C_INCLUDE_PATH=/usr/include/gdal

RUN pip install poetry==1.8.3

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Set working directory
WORKDIR /app

# Install dependencies using poetry
COPY pyproject.toml poetry.lock ./
RUN poetry install && rm -rf $POETRY_CACHE_DIR

# The runtime image, used to just run the code provided its virtual environment
FROM python:3.12.5-slim-bullseye AS runtime

# Install GDAL runtime libraries
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal28 \
    --fix-missing

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY pipeline.py process_climate.py generate_geotiff.py utils.py infra_intersection.py ./

CMD ["python", "pipeline.py"]