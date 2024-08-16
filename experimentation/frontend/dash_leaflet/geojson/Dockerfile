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
COPY app_layers.py .
COPY app_config.py .
COPY pgosm_flex_api.py .

# Expose the port the app runs on
EXPOSE 8050
EXPOSE 80

# Run the application
CMD ["gunicorn", "-b", "0.0.0.0:80", "app:server"]