# CMIP6 Processing Pipeline

This code provides a pipeline for processing climate data, generating GeoTIFF files, and uploading them to an S3 bucket. The pipeline consists of several scripts that handle different stages of the processing.

A single code run will process a given climate variable for all available SSP, and intersect the results with a given OSM feature type.

## Table of Contents

- [Scripts](#scripts)
    - [pipeline.py](#pipelinepy)
    - [process_climate.py](#process_climatepy)
    - [generate_geotiff.py](#generate_geotiffpy)
- [Environment Variables](#environment-variables)
- [Build](#build)

## Scripts

**pipeline.py**

This script orchestrates the entire processing pipeline. It downloads climate data from an S3 bucket, processes the data, generates GeoTIFF files, and uploads the results back to the S3 bucket.

**process_climate.py**

This script processes the climate data. It reads NetCDF files, processes the data using Xarray, and returns an Xarray dataset.

**generate_geotiff.py**

This script generates GeoTIFF files from the processed climate data. It uses the rioxarray library to convert Xarray datasets to GeoTIFF format.

**infra_intersection.py**

This script queries the database (assumes PG OSM Flex Schema) and joins the features with the processed climate data using xarray and the xvec package. This will return a tabular dataframe containing each feature at different timestamps with it's associated climate value and identifier.

**infra_intersection_load.py**

This script takes the output of infra_intersection.py and loads into the PostGIS database using a series fo SQL scripts. This currently utilizes the schema found in `climate-risk-map/backend/physical-asset/database/pgosm_flex_washington/`.

## Environment Variables
Environment variables can be set locally, in deployment, or in a .env file.

The following environment variables are required to run the pipeline:

- `S3_BUCKET`: The name of the S3 bucket.
- `S3_BASE_PREFIX`: The base prefix for S3 paths.
- `CLIMATE_VARIABLE`: The climate variable to process.
- `SSP`: The Shared Socioeconomic Pathway (SSP) scenarios as a comma delimited list.
- `XARRAY_ENGINE`: The engine to use for reading NetCDF files with Xarray.
- `CRS`: The Coordinate Reference System (CRS) for the data.
- `X_DIM`: The name of the X coordinate dimension (e.g., lon or longitude).
- `Y_DIM`: The name of the Y coordinate dimension (e.g., lat or latitude).
- `TIME_DIM`: The name of the time coordinate dimension (e.g., time)
- `CLIMATOLOGY_MEAN_METHOD`: Method to average climate variable over time. Currently, the code recoginzes "decade_month", which averages overs each decade, grouped by month. 
- `ZONAL_AGG_METHOD`: Method when zonally aggregating climate variable values to vector geometry. Common are 'mean' or 'max'
- `CONVERT_360_LON`: Whether to convert longitude values from 0-360 to -180-180.
- `STATE_BBOX`: (Optional) The bounding box for a specific state.
- `PG_DBNAME`: pgosm_flex_washington
- `PG_USER`: Postgres user with read access of the osm schema, and read-write access to climate schema
- `PG_HOST`: Postgres host
- `PG_PASSWORD`: PG_USER password
- `OSM_CATEGORY`: OpenStreetMap feature category to query for intersection
- `OSM_TYPE`: OpenStreetMap feature type to query for intersection

## Build

A Dockerfile is provided to containerize the application. We build with platform linux/amd64 due to geospatial library dependancies. To build and run the Docker container:

1. Build the Docker image:
```bash
docker build --platform linux/amd64 -t climate-data-pipeline .
```

2. Run the Docker container:
```bash
docker run --env-file .env -v ~/.aws/credentials:/root/.aws/credentials:ro climate-data-pipeline
```