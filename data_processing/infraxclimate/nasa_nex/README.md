# infraXclimate Engine NASA NEX Processing Pipeline

This repository contains a comprehensive pipeline for processing NASA NEX climate data. The pipeline downloads, processes, aggregates, and uploads climate data while performing spatial intersections with OpenStreetMap features. It generates Cloud Optimized GeoTIFFs and loads tabular results into a PostGIS database.

## Table of Contents

- [Overview](#overview)
- [Scripts](#scripts)
- [Configuration](#configuration)
- [Build & Run](#build--run)
- [Dependencies](#dependencies)

## Overview

The engine supports processing multiple Shared Socioeconomic Pathway (SSP) scenarios concurrently. It leverages parallel processing for efficient zonal aggregation, generates detailed metadata, and robustly handles database write operations using temporary staging. Key components include:
- Raw climate data processing 
- GeoTIFF generation with spatial clipping
- Infrastructure intersection and zonal aggregation (points, lines, and polygons)
- Loading processed data into a PostGIS database

## Scripts

### `run.py`
Entry point for the pipeline. Iterates over SSP scenarios and triggers the complete workflow, including climate data processing, metadata generation, GeoTIFF creation, and (optionally) database loading.

### `pipeline.py`
Orchestrates the process by:
- Establishing a database connection pool
- Invoking `process_climate.py` to load and process climate data
- Generating metadata from dataset attributes
- Running `generate_geotiff.py` to create Cloud Optimized GeoTIFFs
- Executing `infra_intersection.py` for spatial intersections and zonal aggregations
- Finally loading results into PostGIS via `infra_intersection_load.py`

### `process_climate.py`
Processes raw climate datasets (NetCDF/Zarr) using Xarray. Applies a mean-based averaging (e.g., "decade_month") and supports spatial clipping using a state bounding box.

### `generate_geotiff.py`
Converts processed climate data into Cloud Optimized GeoTIFFs using rioxarray. The process includes parallel file generation and creates a metadata JSON file with min/max values.

### `infra_intersection.py`
Handles querying of OpenStreetMap features from the PG OSM Flex schema, performs zonal aggregation using parallel processing on points, lines, and polygons, and attaches additional metadata as JSON to each record.

### `infra_intersection_load.py`
Stages and loads aggregated data into a PostGIS table using temporary tables and conflict handling to ensure robust data ingestion.

### `constants.py`
Defines key constants for coordinate dimensions, metadata keys, and SSP scenarios.

### `Dockerfile`
Provides instructions for containerizing the application with all necessary geospatial libraries (GDAL, GEOS) and using Poetry for dependency management.

## Configuration

### Command Line Arguments

The following arguments are required when running the container:

- `--s3-bucket`: S3 bucket where raw climate data is stored
- `--s3-prefix`: Base prefix for S3 paths containing raw climate data
- `--s3-prefix-geotiff`: Base prefix for S3 paths where GeoTIFFs will be stored
- `--climate-variable`: The climate variable to process
- `--crs`: Coordinate Reference System (e.g., EPSG:4326)
- `--zonal-agg-method`: Method for zonal aggregation (e.g., "mean" or "max")
- `--osm-category`: OpenStreetMap feature category
- `--osm-type`: OpenStreetMap feature type
- `--state-bbox`: (Optional) State name for spatial clipping


### Environment Variables

Required environment variables in `.env`:

- `PG_DBNAME`: PostGIS database name
- `PG_USER`: PostGIS username with proper privileges
- `PG_HOST`: Host for the PostGIS server
- `PG_PASSWORD`: Password for the PostGIS user
- `PG_PORT`: Port for the PostGIS server

Example `.env`:
```properties
PG_DBNAME=washington
PG_USER=climate_user
PG_HOST=host.docker.internal
PG_PASSWORD=mysecretpassword
PG_PORT=5432
```

## Build & Run

1. Build the Docker image (ensuring compatibility with geospatial dependencies):
   ```bash
   docker build --platform linux/amd64 -t data_processing/infraxclimate/nasa_nex .
   ```
2. Execute the Docker container:
   ```bash
    docker run --env-file .env \
    -v ~/.aws/credentials:/root/.aws/credentials:ro \
    data_processing/infraxclimate/nasa_nex \
    --s3-bucket="my-bucket" \
    --s3-prefix="raw/nasa-nex" \
    --s3-prefix-geotiff="processed/nasa-nex" \
    --climate-variable="tas" \
    --crs="EPSG:4326" \
    --zonal-agg-method="mean" \
    --osm-category="infrastructure" \
    --osm-type="power" \
    --state-bbox="washington"
    ```

## Dependencies

- Python 3.11
- [GDAL](https://gdal.org/)
- [GEOS](https://libgeos.org/)
- rioxarray, xarray, geopandas, psycopg2
- Poetry for dependency management


