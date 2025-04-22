#!/bin/bash

# Load environment variables
if [ -f .env ]; then
    source .env
else
    echo ".env file not found!"
    exit 1
fi

# Directories
ASSET_GROUP_DIR="materialized_views/asset_groups"
UNEXPOSED_DIR="materialized_views/unexposed_ids"

# Check required environment variables
required_vars=("PGDATABASE" "PGUSER" "PGPASSWORD" "PGHOST" "PGPORT")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: $var environment variable is not set"
        exit 1
    fi
done

# First refresh all asset group views
echo "Refreshing asset group views..."
for VIEW in administrative agriculture commercial_real_estate data_center power_grid residential_real_estate
do
    echo "Refreshing osm.$VIEW..."
    psql -U "$PGUSER" -d "$PGDATABASE" -h "$PGHOST" -p "$PGPORT" \
         -c "REFRESH MATERIALIZED VIEW osm.$VIEW;"
done

# Then refresh all unexposed_ids views
echo "Refreshing unexposed_ids views..."
for VIEW in unexposed_ids_nasa_nex_fwi unexposed_ids_nasa_nex_pr_percent_change
do
    echo "Refreshing osm.$VIEW..."
    psql -U "$PGUSER" -d "$PGDATABASE" -h "$PGHOST" -p "$PGPORT" \
         -c "REFRESH MATERIALIZED VIEW osm.$VIEW;"
done