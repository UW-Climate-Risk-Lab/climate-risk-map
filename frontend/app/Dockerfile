FROM python:3.12

# Set working directory
WORKDIR /app

# Install gdal dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    --fix-missing
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
RUN export C_INCLUDE_PATH=/usr/include/gdal

# Install dependencies
RUN pip install GDAL==3.6.2
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy the application code
COPY assets/ ./assets
COPY app.py .
COPY app_utils.py .
COPY app_map.py .
COPY app_control_panel.py .
COPY app_config.py .
COPY infraxclimate_api.py .
COPY test. ./tests

# Expose the port the app runs on
EXPOSE 8050

# Run the application using gunicorn
CMD exec gunicorn -b 0.0.0.0:8050 --workers $GUNICORN_WORKERS app:server