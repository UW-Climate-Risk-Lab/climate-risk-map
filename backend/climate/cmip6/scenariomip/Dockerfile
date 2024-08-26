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

COPY pipeline.py /app/pipeline.py
COPY process_climate.py /app/process_climate.py
COPY generate_geotiff.py /app/generate_geotiff.py
COPY utils.py /app/utils.py
COPY requirements.txt /app/requirements.txt

RUN pip install -r requirements.txt

CMD ["python", "/app/pipeline.py"]