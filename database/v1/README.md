# Climate Risk Map Database (v1)

Welcome to the Climate Risk Map Database! This environment is designed to organize and serve geospatial data for infrastructure and climate risk analyses across different regions. This README provides a high-level overview and explains how to manage this database in your local or production setting.

## Overview

• **Regional Segmentation**  
  Each database is bound to a specific region to keep data ingest efficient and queries focused. Migrations and ETL processes are repeated per region.

• **Data Layers**  
  We use PgOSM Flex for ingesting OpenStreetMap (OSM) content. Additional climate data is placed under a separate `climate` schema. Various materialized views assemble assets into logical groups (e.g., commercial real estate, agriculture).

• **Roles & Permissions**  
  Multiple user roles control database access:  
  - `pgosm_flex` manages OSM data ingestion.  
  - `osm_ro_user` is intended for read-only queries of OSM data.  
  - `climate_user` handles writing and reading climate data.

• **Configuration**  
  A `config.json` file defines database regions, bounding boxes, and climate dataset parameters. This centralizes configuration for consistent database creation and ETL processes.

## Quick Setup Guide

**Note** If the database specified already exists, steps 1 and 2 will be skipped.

1. **Create/Initialize Database**  
   Use the `database.sh` script in this directory to create and configure the database if it does not yet exist. When prompted, supply region info, credentials, etc.
   ```
   ./database.sh washington
   ```
   The script reads region specifications from `config.json` and environment variables from `.env`.

2. **Run Migrations**  
   After initialization, migrations ensure schema consistency. The `database.sh` script automatically executes SQL files placed in the `migrations` folder.

3. **Load OSM Data**  
   Docker-based ETL scripts build a container to ingest OSM data for your chosen region. Verify everything in your `.env` file, then let the script pull and populate OSM data.

4. **Create Asset Views**  
   Multiple materialized views represent asset categories. These are created through the `database.sh` script to organize infrastructure into logical groupings.
   
5. **Load Climate Data**  
   The `database.sh` script also handles climate data ETL using the NASA NEX processing pipeline, which calculates exposure metrics for each infrastructure asset.

## Asset Categories

The database organizes infrastructure into the following asset groups (materialized views):

• **Administrative**  
  Government boundaries, cities, towns, and other administrative entities.
  
• **Agriculture**  
  Farmland, orchards, vineyards, and agricultural buildings.
  
• **Commercial Real Estate**  
  Shops, offices, restaurants, hotels, and other commercial properties.
  
• **Data Centers**  
  Buildings and facilities tagged as data centers or telecommunications infrastructure.
  
• **Power Grid**  
  Power generation, transmission, and distribution infrastructure.
  
• **Residential Real Estate**  
  Houses, apartments, and other residential buildings.

## Climate Data Processing

The system includes an exposure processing pipeline that:

1. Retrieves climate data from S3 zarr stores (NASA NEX datasets)
2. Performs spatial intersection with OSM infrastructure 
3. Calculates zonal statistics for each asset
4. Loads results into the `climate` schema

Current supported climate variables:
- `fwi` (Fire Weather Index)
- `pr_percent_change` (Precipitation Percent Change)

Includes data for multiple climate scenarios (historical, SSP126, SSP245, SSP370, SSP585).

## Helpful Commands

• **Initialize Database**  
  ```
  ./database.sh washington
  ```

• **Refresh Materialized Views**  
  ```
  psql -U <user> -d <database_name> -c "REFRESH MATERIALIZED VIEW osm.power_grid;"
  ```

• **Track Unexposed Assets**  
  ```
  psql -U <user> -d <database_name> -c "SELECT COUNT(*) FROM osm.unexposed_ids_nasa_nex_fwi;"
  ```

• **Inspect Loaded Data**  
  ```
  psql -U <user> -d <database_name> -c "\dt osm.*"
  ```

• **Docker ETL Process**  
  Set environment variables (or `.env`), then:
  ```
  cd etl/osm
  docker build -t pgosm-flex-run .
  docker run --rm --env-file ../.env pgosm-flex-run
  ```

• **NASA NEX Climate Data ETL**  
  ```
  cd etl/exposure/nasa_nex
  docker build -t data_processing/exposure/nasa-nex .
  docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm --env-file .env data_processing/exposure/nasa-nex \
    --s3-zarr-store-uri=s3://bucket/path/to/zarr \
    --climate-variable=fwi \
    --ssp=126 \
    --zonal-agg-method=mean \
    --polygon-area-threshold=20.0 \
    --x_min=-125.0 \
    --y_min=45.5 \
    --x_max=-115.9 \
    --y_max=49.1
  ```

## Environment Setup

Create a `.env` file with the following properties:
PGUSER=super-user 
PGPASSWORD=mypassword 
PGHOST=localhost 
PGPORT=5432 
PG_DBNAME=my_database

S3_BUCKET=bucket-name

PGOSM_USER=pgosm_flex 
PGOSM_PASSWORD=mysecretpassword 
PGOSM_HOST=host.docker.internal 
PGOSM_RAM=8 
PGOSM_LAYERSET=categories 
PGOSM_LANGUAGE=en 
PGOSM_SRID=4326

PGCLIMATE_USER=climate_user 
PGCLIMATE_PASSWORD=mysecretpassword 
PGCLIMATE_HOST=host.docker.internal


## Project Structure

**migrations/**  
  Contains SQL files that create or alter schemas, permissions, and tables, ensuring each region remains consistent with the unified data model.

**materialized_views/**  
  - **asset_groups/** - Views that organize infrastructure into categories
  - **unexposed_ids/** - Views that track assets missing climate exposure data

**etl/**  
  - **osm/** - Docker-based ETL for loading OpenStreetMap data
  - **exposure/nasa_nex/** - Pipeline for processing climate data and calculating exposure

**config.json**  
  Central configuration file containing database region definitions and climate dataset parameters.

---

Feel free to customize and adapt the scripts for your workflow. For more advanced details, refer to comments in the `.sql` and `.sh` files or reach out to the maintainers.