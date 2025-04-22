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

## Quick Setup Guide

1. **Create/Initialize Database**  
   Use the `setup_database.sh` script in this directory to create and configure the database if it does not yet exist. When prompted, supply region info, credentials, etc.

2. **Run Migrations**  
   After initialization, migrations ensure schema consistency. The script automatically executes SQL files placed in the `migrations` folder.

3. **Load OSM Data**  
   Docker-based ETL scripts build a container to ingest OSM data for your chosen region. Verify everything in your `.env` file, then let the script pull and populate OSM data.

4. **View Creation**  
   Multiple materialized views represent asset categories. These are refreshed through the scripts provided (`update_views.sh`) to keep data consistent.

## Helpful Commands

• **Initialize Database**  
  ```
  ./setup_database.sh my_database north-america us/california
  ```

• **Refresh Materialized Views**  
  ```
  ./update_views.sh
  ```

• **Inspect Loaded Data**  
  ```
  psql -U <user> -d <database_name> -c "\dt osm.*"
  ```

• **Docker ETL Process**  
  Set environment variables (or `.env`), then:
  ```
  cd etl
  docker build -t database-v1-osm-etl .
  docker run --rm --env-file ../.env database-v1-osm-etl
  ```

## Further Details

• **Migrations Folder**  
  Contains SQL files that create or alter schemas, permissions, and tables, ensuring each region remains consistent with the unified data model.

• **Materialized Views**  
  Organized by category within `materialized_views` to isolate data subsets (e.g., `power_grid`, `commercial_real_estate`). Refresh these views regularly to align with newly ingested or updated data.

---

Feel free to customize and adapt the scripts for your workflow. For more advanced details, refer to comments in the `.sql` and `.sh` files or reach out to the maintainers. Good luck with your Climate Risk Map deployment!