FROM python:3.12

# Set working directory
WORKDIR /climate-risk-map/application/app

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    --fix-missing

# Install dependencies
RUN pip install poetry==1.8.3

# Copy the dependancies to install
COPY poetry.lock pyproject.toml ./

# Install dependencies
RUN poetry install --no-root --without dev

# Copy rest of code into image
COPY . .

# Expose the port the app runs on
EXPOSE 8050

# Run the application using poetry and gunicorn
CMD poetry run gunicorn -b 0.0.0.0:8050 --workers $GUNICORN_WORKERS app:server