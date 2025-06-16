#!/usr/bin/env bash
set -e

DB_NAME=$1

SQL_DIR="/home/ubuntu/climate-risk-map/database/v1/materialized_views/hazards/flood"

echo "Starting flood build at $(date)"

echo "Running 1.sql  (creates climate.latest_flood_zones_subdivided)…"
time psql -v ON_ERROR_STOP=1 \
          -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DB_NAME" \
          -f "$SQL_DIR/1.sql"

echo "Running 2.sql  (creates climate.flood materialized view)…"
time psql -v ON_ERROR_STOP=1 \
          -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DB_NAME" \
          -f "$SQL_DIR/2.sql"

echo "Completed at $(date)"
