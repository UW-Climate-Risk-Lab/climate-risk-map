# Exposure Engine - NASA NEX Processing Pipeline

This repository contains a pipeline for processing NASA NEX climate data and intersecting it with OpenStreetMap infrastructure features. The pipeline reads climate data from S3 Zarr stores, processes it, and performs spatial intersections with OSM features stored in PostGIS.

## Table of Contents

- [Overview](#overview)
- [Scripts](#scripts)
- [Configuration](#configuration)
- [Dependencies](#dependencies)

## Overview

The pipeline processes climate data from NASA NEX datasets stored in Zarr format and intersects it with infrastructure features from OpenStreetMap. Key operations include:
- Loading and processing climate data from Zarr stores
- Querying infrastructure features from PostGIS (using PG OSM Flex schema)
- Performing spatial intersection and zonal statistics
- Loading results back to PostGIS

## Scripts

### `run.py`
Entry point that orchestrates the complete workflow. Sets up command-line arguments and coordinates the execution of processing modules.

### `process_climate.py`
Processes climate data from Zarr stores using Xarray:
- Loads data from S3 Zarr store
- Converts longitude coordinates from 0-360 to -180-180
- Sets proper CRS and spatial dimensions

### `infra_intersection.py`
Handles the core spatial operations:
- Queries OpenStreetMap features from PostGIS
- Performs parallel zonal statistics for different geometry types:
  - Points: Direct value extraction
  - Lines: Converts to points and aggregates values
  - Polygons: Parallel zonal statistics using exactextract
- Attaches metadata to results

### `infra_intersection_load.py`
Manages loading results into PostGIS:
- Creates temporary staging tables
- Handles data type conversions
- Loads data with conflict handling

### `utils.py`
Provides utility functions for:
- S3 operations
- Database queries
- Data type conversions
- Metadata handling

### `constants.py`
Defines key constants for:
- Coordinate dimensions
- Metadata keys
- SSP scenarios

## Configuration

### Command Line Arguments

Required arguments:

- `--s3-zarr-store-uri`: S3 URI to zarr store containing climate dataset
- `--climate-variable`: Climate variable of the zarr
- `--crs`: Coordinate Reference System
- `--ssp`: SSP scenario
- `--zonal-agg-method`: Method for zonal aggregation
- `--polygon-area-threshold`: km^2, Any polygon below this threshold is converted to a point for exposure calc, for performance

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
   docker build --platform linux/amd64 -t data_processing/exposure/nasa-nex .
   ```
2. Execute the Docker container:
   ```bash
    docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm --env-file .env exposure-nasa-nex \
    --s3-zarr-store-uri=s3://my-bucket/path/to/zarr/fwi_decade_month_ssp126.zarr \
    --climate-variable=fwi \
    --ssp=126 \
    --zonal-agg-method=max \
    --polygon-area-threshold
    ```

## Dependencies

- Python 3.11
- [GDAL](https://gdal.org/)
- [GEOS](https://libgeos.org/)
- rioxarray, xarray, geopandas, psycopg2
- Poetry for dependency management


