# Dash Leaflet Frontend Map

## Overview

This directory contains a Dash app that visualizes both infrastructure and climate data on a single map using the dash-leaflet library.

## How to run

0. Set up a local or remote PostgreSQL instance and load data using the steps in `database/v1` and `data_processing/etl/pgosm_flex`.

1. Navigate to this directory (`/climate-risk-map/application`) and build the Docker images using Docker Compose.

*Note: If the Docker build fails due to build dependencies and an error about "Hash Sum Mismatch", try building on another network or VPN.*

```bash
docker-compose build
```

2. Create a `.env` file based on the `env.sample` file. The TiTiler endpoint should be a deployed endpoint. TiTiler deployment on AWS Lambda can be done easily using [this repo and stack](https://github.com/developmentseed/titiler-lambda-layer).

3. Run the containers using Docker Compose with the following command:

```bash
docker-compose up
```

4. Open `http://0.0.0.0:8050/` in your browser to see the map.

## Environment Variables

The `.env` file should contain the following variables:

```
TITILER_ENDPOINT=<your_titiler_endpoint>
PG_DBNAME=<your_postgres_dbname>
PG_USER=<your_postgres_user>
PG_HOST=<your_postgres_host>
PG_PASSWORD=<your_postgres_password>
PG_PORT=<your_postgres_port>
PG_MAX_CONN=<your_postgres_max_connections>
MAX_DOWNLOADS=<max_downloads_per_session>
MAX_DOWNLOAD_AREA=<max_download_area_in_sq_km>
DEBUG=<true_or_false>
GUNICORN_WORKERS=<number_of_gunicorn_workers>
```

## Limitations

When experimenting, loading the infrastructure points data as a GeoJSON layer in dash-leaflet proved to crash the browser. This is likely due to the browser trying to render >100k points at once. This shows there is an inherent limitation with using GeoJSON data in the mapping application this way.

The solution would be to use something like Tegola to serve vector tile data from the PostGIS database. Dash-Leaflet does not support vector tiles. An alternative is to enable clustering, which forces the browser not to render all the features at one time. This does not look as visually appealing as a vector tile approach would. A workaround is setting the cluster icon to transparent, giving the illusion of points generating dynamically.