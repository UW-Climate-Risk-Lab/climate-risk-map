#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

# Function to display usage information
show_usage() {
    echo "Usage: $0 [database_name] [osm_region] [osm_subregion]"
    echo
    echo "Arguments:"
    echo "  database_name  - Name of the PostgreSQL database to create/update"
    echo "  osm_region     - OSM Flex region (e.g., north-america, europe, asia)"
    echo "  osm_subregion  - OSM Flex subregion (e.g., us/california, france, japan)"
    echo
    echo "If arguments are not provided, values will be read from environment variables."
    echo "Required environment variables if not using command line arguments:"
    echo "  PG_DBNAME, PGOSM_REGION, PGOSM_SUBREGION"
    echo
    echo "Other required environment variables:"
    echo "  PGUSER, PGPASSWORD, PGHOST, PGPORT, PGOSM_USER, PGOSM_PASSWORD,"
    echo "  PGOSM_RAM, PGOSM_LAYERSET, PGOSM_PGOSM_LANGUAGE, PGOSM_SRID"
}

# Process command line arguments
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_usage
    exit 0
fi

# Define directory paths
ROOT_DIR=$(pwd)
MIGRATIONS_DIR="$ROOT_DIR/migrations"
OSM_ETL_DIR="$ROOT_DIR/etl/osm"
EXPOSURE_ETL_DIR="$ROOT_DIR/etl/exposure"
ASSET_GROUP_DIR="$ROOT_DIR/materialized_views/asset_groups"
UNEXPOSED_DIR="$ROOT_DIR/materialized_views/unexposed_ids"
GEOTIFF_ETL_DIR="$ROOT_DIR/etl/geotiff"
HAZARD_VIEW_DIR="$ROOT_DIR/materialized_views/hazards"
CONFIG_JSON="$ROOT_DIR/config.json"

if [ ! -f "$CONFIG_JSON" ]; then
    echo "Error: Config JSON file not found at $CONFIG_JSON"
    exit 1
fi

# Assign command line arguments to variables, if provided
CLI_DB_NAME=$1

# Load environment variables from .env file
if [ -f .env ]; then
    source .env
    echo "Environment variables loaded from .env file"
else
    echo "Warning: .env file not found. Using existing environment variables."
fi

# Override environment variables with command line arguments if provided
if [ -n "$CLI_DB_NAME" ] && [ "$CLI_DB_NAME" != "all_databases" ]; then
    PG_DBNAME=$CLI_DB_NAME
    echo "Using database name from command line: $PG_DBNAME"
    # Check if the database name exists in config.json
    if ! jq -e --arg dbname "$PG_DBNAME" '.databases | has($dbname)' "$CONFIG_JSON" > /dev/null; then
        echo "Error: Database name '$PG_DBNAME' not found in config.json."
        exit 1
    fi
    PGOSM_REGION=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].osm_region' "$CONFIG_JSON")
    PGOSM_SUBREGION=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].osm_subregion' "$CONFIG_JSON")
fi

# Set default for SRID if not defined
PGOSM_SRID=${PGOSM_SRID:-4326}

# Check required environment variables
if [ "$CLI_DB_NAME" = "all_databases" ]; then
    # When processing all databases, PG_DBNAME, PGOSM_REGION, and PGOSM_SUBREGION will be set
    # dynamically for each database inside the loop, so they are not required at this point.
    required_vars=("PGUSER" "PGPASSWORD" "PGHOST" "PGPORT" "S3_BUCKET" "PGOSM_USER" "PGOSM_PASSWORD" "PGOSM_RAM" "PGOSM_LAYERSET" "PGOSM_LANGUAGE" "PGOSM_SRID" "PGCLIMATE_USER" "PGCLIMATE_PASSWORD" "PGCLIMATE_HOST" "PG_MAINTENANCE_MEMORY" "PG_MAX_PARALLEL_MAINTENANCE_WORKERS")
else
    required_vars=("PG_DBNAME" "PGUSER" "PGPASSWORD" "PGHOST" "PGPORT" "S3_BUCKET" "PGOSM_USER" "PGOSM_PASSWORD" "PGOSM_RAM" "PGOSM_REGION" "PGOSM_SUBREGION" "PGOSM_LAYERSET" "PGOSM_LANGUAGE" "PGOSM_SRID" "PGCLIMATE_USER" "PGCLIMATE_PASSWORD" "PGCLIMATE_HOST" "PG_MAINTENANCE_MEMORY" "PG_MAX_PARALLEL_MAINTENANCE_WORKERS")
fi

missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -gt 0 ]; then
    echo "Error: The following required environment variables are not set:"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    echo
    echo "Please set these variables in the .env file or provide them via command line arguments."
    echo "Run '$0 --help' for usage information."
    exit 1
fi

# Store the superuser credentials for init_db
PG_SUPER_USER=${PG_SUPER_USER:-$PGUSER}
PG_SUPER_PASSWORD=${PG_SUPER_PASSWORD:-$PGPASSWORD}


# Check if jq is installed (needed for JSON parsing)
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed. Please install jq before continuing."
    echo "  - For MacOS: brew install jq"
    echo "  - For Ubuntu/Debian: apt-get install jq"
    echo "  - For Amazon Linux/CentOS: yum install jq"
    exit 1
fi

# Check if database already exists
database_exists() {
    local dbname=$1
    PGPASSWORD=$PG_SUPER_PASSWORD psql \
        -U "$PG_SUPER_USER" \
        -d postgres \
        -h "$PGHOST" \
        -p "$PGPORT" \
        -tAc "SELECT 1 FROM pg_database WHERE datname='$dbname'" | grep -q 1
    return $?
}

# Step 1: Initialize Database
init_database() {
    echo "===== STEP 1: INITIALIZING DATABASE ====="
    
    # Check if init_db.sql exists
    if [ ! -f "$MIGRATIONS_DIR/init_db.sql" ]; then
        echo "Error: init_db.sql not found at $MIGRATIONS_DIR/init_db.sql"
        exit 1
    fi
    
    echo "Creating database $PG_DBNAME..."
    
    # Using PGPASSWORD environment variable for authentication
    PGPASSWORD=$PG_SUPER_PASSWORD psql \
        -U "$PG_SUPER_USER" \
        -d postgres \
        -h "$PGHOST" \
        -p "$PGPORT" \
        -v region_name="$PG_DBNAME" \
        -f "$MIGRATIONS_DIR/init_db.sql"
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to initialize database"
        exit 1
    fi
    
    echo "Database $PG_DBNAME successfully initialized"
}

# Step 2: Run ETL Process
run_osm_etl() {
    echo "===== STEP 2: RUNNING ETL PROCESS ====="
    echo "Using Region: $PGOSM_REGION, Subregion: $PGOSM_SUBREGION"
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$OSM_ETL_DIR" ]; then
        echo "Error: ETL directory not found at $OSM_ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$OSM_ETL_DIR/Dockerfile" ]; then
        echo "Error: Dockerfile not found in ETL directory"
        exit 1
    fi
    
    # Navigate to ETL directory
    cd "$OSM_ETL_DIR"
    
    # Build Docker image
    echo "Building ETL Docker image..."
    sudo docker build -t database-v1-osm-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Run Docker container with environment variables
    echo "Running ETL process to load OSM data..."

    sudo docker run --rm \
        -e POSTGRES_USER=$PGOSM_USER \
        -e POSTGRES_PASSWORD=$PGOSM_PASSWORD \
        -e POSTGRES_HOST=$PGOSM_HOST \
        -e POSTGRES_DB=$PG_DBNAME \
        -e POSTGRES_PORT=$PGPORT \
        -e RAM=$PGOSM_RAM \
        -e REGION=$PGOSM_REGION \
        -e SUBREGION=$PGOSM_SUBREGION \
        -e LAYERSET=$PGOSM_LAYERSET \
        -e SRID=$PGOSM_SRID \
        -e PGOSM_LANGUAGE=$PGOSM_LANGUAGE \
        -p 5433:$PGPORT \
        database-v1-osm-etl
    
    if [ $? -ne 0 ]; then
        echo "Error: ETL process failed"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "ETL process completed successfully"
}

# Step 3: Run Migrations
run_migrations() {
    echo "===== STEP 3: RUNNING MIGRATIONS ====="
    
    # Loop over each SQL file in the directory
    for FILE in $(ls $MIGRATIONS_DIR/*.sql | sort)
    do
        if [[ $FILE == *"init_db"* ]]; then
            continue
        fi  
        # Execute the SQL file with explicit connection parameters
        echo "Executing $FILE..."
        if ! PGPASSWORD=$PG_SUPER_PASSWORD psql -U "$PGUSER" -d "$PG_DBNAME" -h "$PGHOST" -p "$PGPORT" -f "$FILE"; then
            echo "Error executing $FILE"
            exit 1
        fi
    done
    
    echo "Migrations completed successfully"
}

# Step 4: Create Views
create_views() {
    echo "===== STEP 4: CREATING VIEWS ====="
    
    # Database connection string
    DB_CONN="-U $PGUSER -d $PG_DBNAME -h $PGHOST -p $PGPORT"
    
    # First step: Create or refresh asset group views
    echo "Creating/refreshing asset group views..."
    if [ -d "$ASSET_GROUP_DIR" ]; then
        for SQL_FILE in "$ASSET_GROUP_DIR"/*.sql; do
            if [ -f "$SQL_FILE" ]; then
                echo "Processing $SQL_FILE..."
                PGPASSWORD=$PG_SUPER_PASSWORD psql $DB_CONN -f "$SQL_FILE"
                
                if [ $? -ne 0 ]; then
                    echo "Error: Failed to execute $SQL_FILE"
                    exit 1
                fi
            fi
        done
    else
        echo "Warning: Asset group directory not found at $ASSET_GROUP_DIR"
    fi
    
    # Second step: Create or refresh unexposed_ids views
    echo "Creating/refreshing unexposed_ids views..."
    if [ -d "$UNEXPOSED_DIR" ]; then
        for SQL_FILE in "$UNEXPOSED_DIR"/*.sql; do
            if [ -f "$SQL_FILE" ]; then
                echo "Processing $SQL_FILE..."
                PGPASSWORD=$PG_SUPER_PASSWORD psql $DB_CONN -f "$SQL_FILE"
                
                if [ $? -ne 0 ]; then
                    echo "Error: Failed to execute $SQL_FILE"
                    exit 1
                fi
            fi
        done
    else
        echo "Warning: Unexposed IDs directory not found at $UNEXPOSED_DIR"
    fi
    
    echo "Views created/refreshed successfully"
}

# Step 4a: Refresh Asset Views (alternative to creating views)

refresh_asset_views() {
    echo "===== STEP 4a: REFRESHING ASSET VIEWS ====="

    for VIEW in power_grid commercial_real_estate data_center agriculture administrative
    do
        echo "Refreshing osm.$VIEW..."
        PGPASSWORD=$PG_SUPER_PASSWORD psql -U "$PGUSER" -d "$PG_DBNAME" -h "$PGHOST" -p "$PGPORT" \
            -c "REFRESH MATERIALIZED VIEW osm.$VIEW;"
    done
}

# Step 4b: Refresh Unexposed ID Views (alternative to creating views)

refresh_unexposed_id_views() {
    echo "===== STEP 4b: REFRESHING UNEXPOSED ID VIEWS ====="

    # Refresh all unexposed_ids views
    # Note, does not check if exposure exists for every SSP scenario and time step

    for VIEW in unexposed_ids_nasa_nex_fwi unexposed_ids_usda_burn_probability unexposed_ids_nasa_nex_pfe_mm_day unexposed_ids_nasa_nex_pluvial_change_factor
    do
        echo "Refreshing osm.$VIEW..."
        PGPASSWORD=$PG_SUPER_PASSWORD psql -U "$PGUSER" -d "$PG_DBNAME" -h "$PGHOST" -p "$PGPORT" \
            -c "REFRESH MATERIALIZED VIEW osm.$VIEW;"
    done
}

# Step 4c: Create or Refresh Hazard Views
create_or_refresh_hazard_views() {
    echo "===== STEP 4c: CREATING/REFRESHING HAZARD VIEWS ====="

    # TODO: Need to integrate the flood directory into this. Flood is special and requires a 2 part sql exection
    # due to FEMA flood zone aggregation. This can probably be optimized into a single script later and added as flood.sql
    # by creating the subdivided table as a CTE
    
    # Database connection string
    DB_CONN="-U $PGUSER -d $PG_DBNAME -h $PGHOST -p $PGPORT"
    
    echo "Creating/refreshing hazard views..."
    if [ -d "$HAZARD_VIEW_DIR" ]; then
        for SQL_FILE in "$HAZARD_VIEW_DIR"/*.sql; do
            if [ -f "$SQL_FILE" ]; then
                echo "Processing $SQL_FILE..."
                if ! PGPASSWORD=$PG_SUPER_PASSWORD psql $DB_CONN -f "$SQL_FILE"; then
                    echo "Error executing $SQL_FILE"
                    exit 1
                fi
            fi
        done
    else
        echo "Warning: Hazard view directory not found at $HAZARD_VIEW_DIR"
    fi
    
    echo "Hazard views created/refreshed successfully"
}

# Step 5: Run Climate ETL Process
run_nasa_nex_exposure_etl() {
    echo "===== STEP 5: RUNNING NASA NEX EXPOSURE ETL PROCESS ====="
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$EXPOSURE_ETL_DIR/nasa_nex" ]; then
        echo "Error: ETL directory not found at $EXPOSURE_ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$EXPOSURE_ETL_DIR/nasa_nex/Dockerfile" ]; then
        echo "Error: Dockerfile not found in Climate ETL directory"
        exit 1
    fi
    
    
    # Navigate to NASA NEX ETL directory
    cd "$EXPOSURE_ETL_DIR/nasa_nex"
    
    # Build Docker image
    echo "Building NASA NEX ETL Docker image..."
    sudo docker build -t database-v1-nasa-nex-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build NASA NEX EXPOSURE ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Get the number of datasets
    DATASET_COUNT=$(jq '.climate_exposure_args | length' "$CONFIG_JSON")
    echo "Found $DATASET_COUNT climate datasets to process"

    # Get Bounding Box for Database
    X_MIN=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.x_min' "$CONFIG_JSON")
    Y_MIN=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.y_min' "$CONFIG_JSON")
    X_MAX=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.x_max' "$CONFIG_JSON")
    Y_MAX=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.y_max' "$CONFIG_JSON")
    
    # Loop through each dataset in the JSON file
    for ((i=0; i<$DATASET_COUNT; i++)); do
        # Extract dataset properties
        ZARR_STORE_PATH=$(jq -r ".climate_exposure_args[$i].zarr_store_path" "$CONFIG_JSON")
        S3_ZARR_STORE_URI="s3://${S3_BUCKET}/${ZARR_STORE_PATH}"

        CLIMATE_VARIABLE=$(jq -r ".climate_exposure_args[$i].climate_variable" "$CONFIG_JSON")
        SSP=$(jq -r ".climate_exposure_args[$i].ssp" "$CONFIG_JSON")
        ZONAL_AGG_METHOD=$(jq -r ".climate_exposure_args[$i].zonal_agg_method" "$CONFIG_JSON")
        POLYGON_AREA_THRESHOLD=$(jq -r ".climate_exposure_args[$i].polygon_area_threshold" "$CONFIG_JSON")
        TIME_PERIOD_TYPE=$(jq -r ".climate_exposure_args[$i].time_period_type" "$CONFIG_JSON")
        RETURN_PERIOD=$(jq -r ".climate_exposure_args[$i].return_period" "$CONFIG_JSON")
        
        echo "Processing dataset $((i+1))/$DATASET_COUNT:"
        echo "  - Zarr Store URI: $S3_ZARR_STORE_URI"
        echo "  - Climate Variable: $CLIMATE_VARIABLE"
        echo "  - SSP: $SSP"
        echo "  - Zonal Aggregation Method: $ZONAL_AGG_METHOD"
        echo "  - Polygon Area Threshold: $POLYGON_AREA_THRESHOLD"
        echo "  - X min: $X_MIN"
        echo "  - Y min: $Y_MIN"
        echo "  - X max: $X_MAX"
        echo "  - Y max: $Y_MAX"
        echo "  - Postgres maintenance memory $PG_MAINTENANCE_MEMORY"
        echo "  - Postgres maintenance workers $PG_MAX_PARALLEL_MAINTENANCE_WORKERS"

        
        # Run Docker container with environment variables and arguments
        echo "Running ETL process for climate dataset $((i+1))..."
        
        DOCKER_RUN_CMD="sudo docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm \
            -e PG_DBNAME=$PG_DBNAME \
            -e PGUSER=$PGCLIMATE_USER \
            -e PGPASSWORD=$PGCLIMATE_PASSWORD \
            -e PGHOST=$PGCLIMATE_HOST \
            -e PGPORT=$PGPORT \
            database-v1-nasa-nex-etl \
            --s3-zarr-store-uri $S3_ZARR_STORE_URI \
            --climate-variable $CLIMATE_VARIABLE \
            --ssp $SSP \
            --zonal-agg-method $ZONAL_AGG_METHOD \
            --polygon-area-threshold $POLYGON_AREA_THRESHOLD \
            --x_min $X_MIN \
            --y_min $Y_MIN \
            --x_max $X_MAX \
            --y_max $Y_MAX \
            --pg_maintenance_memory $PG_MAINTENANCE_MEMORY \
            --pg_max_parallel_workers $PG_MAX_PARALLEL_MAINTENANCE_WORKERS"

        if [ -n "$TIME_PERIOD_TYPE" ] && [ "$TIME_PERIOD_TYPE" != "null" ]; then
            DOCKER_RUN_CMD="$DOCKER_RUN_CMD --time-period-type $TIME_PERIOD_TYPE"
            echo "  - Time Period: $TIME_PERIOD_TYPE"
        fi

        if [ -n "$RETURN_PERIOD" ] && [ "$RETURN_PERIOD" != "null" ]; then
            DOCKER_RUN_CMD="$DOCKER_RUN_CMD --return-period $RETURN_PERIOD"
            echo "  - Return Period: $RETURN_PERIOD"
        fi

        eval $DOCKER_RUN_CMD
        
        if [ $? -ne 0 ]; then
            echo "Error: ETL process failed for dataset $((i+1))"
            cd "$ROOT_DIR"  # Return to root directory
            exit 1
        fi
        
        echo "Dataset $((i+1)) processed successfully"
    done
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "NASA NEX ETL process completed successfully for all datasets"
}

# Step 5a: Run USDA Burn Probability ETL Process
run_usda_wildfire_exposure_etl() {
    echo "===== STEP 5a: RUNNING USDA WILDFIRE ETL PROCESS ====="
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$EXPOSURE_ETL_DIR/usda" ]; then
        echo "Error: ETL directory not found at $EXPOSURE_ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$EXPOSURE_ETL_DIR/usda/Dockerfile" ]; then
        echo "Error: Dockerfile not found in USDA ETL directory"
        exit 1
    fi
    
    
    # Navigate to NASA NEX ETL directory
    cd "$EXPOSURE_ETL_DIR/usda"
    
    # Build Docker image
    echo "Building USDA Wildfire Exposure ETL Docker image..."
    sudo docker build -t database-v1-usda-wildfire-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build Climate ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Get the number of datasets
    DATASET_COUNT=$(jq '.usda_wildfire_expsoure_args | length' "$CONFIG_JSON")
    echo "Found $DATASET_COUNT climate datasets to process"

    # Get Bounding Box for Database
    X_MIN=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.x_min' "$CONFIG_JSON")
    Y_MIN=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.y_min' "$CONFIG_JSON")
    X_MAX=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.x_max' "$CONFIG_JSON")
    Y_MAX=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].bounding_box.y_max' "$CONFIG_JSON")
    
    # Loop through each dataset in the JSON file
    for ((i=0; i<$DATASET_COUNT; i++)); do
        # Extract dataset properties
        ZARR_STORE_PATH=$(jq -r ".usda_wildfire_expsoure_args[$i].zarr_store_path" "$CONFIG_JSON")
        S3_ZARR_STORE_URI="s3://${S3_BUCKET}/${ZARR_STORE_PATH}"

        USDA_VARIABLE=$(jq -r ".usda_wildfire_expsoure_args[$i].usda_variable" "$CONFIG_JSON")
        ZONAL_AGG_METHOD=$(jq -r ".usda_wildfire_expsoure_args[$i].zonal_agg_method" "$CONFIG_JSON")
        POLYGON_AREA_THRESHOLD=$(jq -r ".usda_wildfire_expsoure_args[$i].polygon_area_threshold" "$CONFIG_JSON")
        
        echo "Processing dataset $((i+1))/$DATASET_COUNT:"
        echo "  - Zarr Store URI: $S3_ZARR_STORE_URI"
        echo "  - Variable: $USDA_VARIABLE"
        echo "  - Zonal Aggregation Method: $ZONAL_AGG_METHOD"
        echo "  - Polygon Area Threshold: $POLYGON_AREA_THRESHOLD"
        echo "  - X min: $X_MIN"
        echo "  - Y min: $Y_MIN"
        echo "  - X max: $X_MAX"
        echo "  - Y max: $Y_MAX"
        echo "  - Postgres maintenance memory $PG_MAINTENANCE_MEMORY"
        echo "  - Postgres maintenance workers $PG_MAX_PARALLEL_MAINTENANCE_WORKERS"
        
        
        # Run Docker container with environment variables and arguments
        echo "Running ETL process for climate dataset $((i+1))..."
        
        sudo docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm \
            -e PG_DBNAME=$PG_DBNAME \
            -e PGUSER=$PGCLIMATE_USER \
            -e PGPASSWORD=$PGCLIMATE_PASSWORD \
            -e PGHOST=$PGCLIMATE_HOST \
            -e PGPORT=$PGPORT \
            database-v1-usda-wildfire-etl \
            --s3-zarr-store-uri "$S3_ZARR_STORE_URI" \
            --usda-variable "$USDA_VARIABLE" \
            --zonal-agg-method "$ZONAL_AGG_METHOD" \
            --polygon-area-threshold "$POLYGON_AREA_THRESHOLD" \
            --x_min "$X_MIN" \
            --y_min "$Y_MIN" \
            --x_max "$X_MAX" \
            --y_max "$Y_MAX" \
            --pg_maintenance_memory "$PG_MAINTENANCE_MEMORY" \
            --pg_max_parallel_workers "$PG_MAX_PARALLEL_MAINTENANCE_WORKERS"
        
        if [ $? -ne 0 ]; then
            echo "Error: ETL process failed for dataset $((i+1))"
            cd "$ROOT_DIR"  # Return to root directory
            exit 1
        fi
        
        echo "Dataset $((i+1)) processed successfully"
    done
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "USDA Wildfire process completed successfully for all datasets"
}



# Step 5b: Run FEMA Flood Exposure ETL Process (US States Only)
run_fema_exposure_etl() {
    echo "===== STEP 5b: RUNNING FEMA FLOOD EXPOSURE ETL PROCESS ====="
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$EXPOSURE_ETL_DIR/fema" ]; then
        echo "Error: FEMA ETL directory not found at $EXPOSURE_ETL_DIR/fema"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$EXPOSURE_ETL_DIR/fema/Dockerfile" ]; then
        echo "Error: Dockerfile not found in FEMA ETL directory"
        exit 1
    fi
    
    # Navigate to FEMA ETL directory
    cd "$EXPOSURE_ETL_DIR/fema"
    
    # Build Docker image
    echo "Building FEMA Flood Exposure ETL Docker image..."
    sudo docker build -t database-v1-fema-flood-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build FEMA Flood Exposure ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Get FEMA configuration from config.json
    S3_PREFIX_FEMA=$(jq -r '.fema_args.s3_prefix' "$CONFIG_JSON")
    
    # Check if this database has FEMA configuration
    STATE_FILTER=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].fema_args.state_filter // empty' "$CONFIG_JSON")
    
    if [ -z "$STATE_FILTER" ] || [ "$STATE_FILTER" = "null" ]; then
        echo "Info: Database '$PG_DBNAME' does not have FEMA configuration. Skipping FEMA ETL."
        cd "$ROOT_DIR"
        return 0
    fi
    
    echo "Processing FEMA flood data for state: $STATE_FILTER"
    echo "  - S3 Bucket: $S3_BUCKET"
    echo "  - S3 Prefix: $S3_PREFIX_FEMA"
    echo "  - State Filter: $STATE_FILTER"
    echo "  - Target Database: $PG_DBNAME"
    
    # Run Docker container with environment variables and arguments
    echo "Running FEMA flood exposure ETL process..."
    
    sudo docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm \
        -e PG_DBNAME=$PG_DBNAME \
        -e PGUSER=$PGCLIMATE_USER \
        -e PGPASSWORD=$PGCLIMATE_PASSWORD \
        -e PGHOST=$PGCLIMATE_HOST \
        -e PGPORT=$PGPORT \
        -e PG_SCHEMA=climate \
        database-v1-fema-flood-etl \
        --s3-bucket "$S3_BUCKET" \
        --s3-prefix "$S3_PREFIX_FEMA" \
        --state-filter "$STATE_FILTER"
    
    if [ $? -ne 0 ]; then
        echo "Error: FEMA flood exposure ETL process failed"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "FEMA flood exposure ETL process completed successfully"
}

# Step 6: Run Geotiff Process
run_geotiff_etl() {
    echo "===== STEP 6: RUNNING GEOTIFF ETL PROCESS ====="
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$GEOTIFF_ETL_DIR" ]; then
        echo "Error: Geotiff ETL directory not found at $GEOTIFF_ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$GEOTIFF_ETL_DIR/Dockerfile" ]; then
        echo "Error: Dockerfile not found in Climate ETL directory"
        exit 1
    fi
    
    
    # Navigate to ETL directory
    cd "$GEOTIFF_ETL_DIR"
    
    # Build Docker image
    echo "Building Geotiff ETL Docker image..."
    sudo docker build -t database-v1-geotiff-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build Geotiff ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Get the number of datasets
    GEOTIFF_DATASET_COUNT=$(jq '.geotiff_args | length' "$CONFIG_JSON")
    echo "Found $GEOTIFF_DATASET_COUNT climate datasets to process"
    
    # Loop through each dataset in the JSON file
    for ((i=0; i<$GEOTIFF_DATASET_COUNT; i++)); do
        # Extract dataset properties
        S3_PREFIX_INPUT_GEOTIFF=$(jq -r ".geotiff_args[$i].s3_prefix_input" "$CONFIG_JSON")
        S3_URI_INPUT_GEOTIFF="s3://${S3_BUCKET}/${S3_PREFIX_INPUT_GEOTIFF}"
        S3_PREFIX_GEOTIFF=$(jq -r ".geotiff_args[$i].s3_prefix_geotiff" "$CONFIG_JSON")

        CLIMATE_VARIABLE=$(jq -r ".climate_exposure_args[$i].climate_variable" "$CONFIG_JSON")
        SSP=$(jq -r ".climate_exposure_args[$i].ssp" "$CONFIG_JSON")
        ZONAL_AGG_METHOD=$(jq -r ".climate_exposure_args[$i].zonal_agg_method" "$CONFIG_JSON")
        POLYGON_AREA_THRESHOLD=$(jq -r ".climate_exposure_args[$i].polygon_area_threshold" "$CONFIG_JSON")
        
        echo "Processing geotiff $((i+1))/$GEOTIFF_DATASET_COUNT:"
        echo "  - Region: $PG_DBNAME"
        
        # Run Docker container with environment variables and arguments
        echo "Running Geotiff ETL process for climate dataset $((i+1))..."
        
        sudo docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm \
            database-v1-geotiff-etl \
            --s3-bucket "$S3_BUCKET" \
            --s3-uri-input "$S3_URI_INPUT_GEOTIFF" \
            --s3-prefix-geotiff "$S3_PREFIX_GEOTIFF" \
            --region "$PG_DBNAME"
        
        if [ $? -ne 0 ]; then
            echo "Error: Geotiff ETL process failed for dataset $((i+1))"
            cd "$ROOT_DIR"  # Return to root directory
            exit 1
        fi
        
        echo "Geotiff dataset $((i+1)) processed successfully"
    done
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "Geotiff ETL process completed successfully for all datasets"
}

# Main execution logic for a single database (previously main)
run_single_db() {
    # Ensure OSM parameters are set for the current database
    PGOSM_REGION=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].osm_region' "$CONFIG_JSON")
    PGOSM_SUBREGION=$(jq -r --arg dbname "$PG_DBNAME" '.databases[$dbname].osm_subregion' "$CONFIG_JSON")

    echo "Starting climate database setup for region: $PG_DBNAME"
    echo "OSM Region: $PGOSM_REGION"
    echo "OSM Subregion: $PGOSM_SUBREGION"
    
    # Here, we allow for two paths. If the database does not exist, we intialize a new database and load it with data,
    # run all migrations and create views from scratch. If the database does exist, we give the user the option to continue
    # with a data update, which reruns the ETL (to capture the latest OSM data), refreshes our views, and reruns exposure calcs.

    if database_exists "$PG_DBNAME"; then
        echo "Database $PG_DBNAME already exists, skipping database intialization and running data refresh"

        # Run OSM ETL process
        run_osm_etl

        # Try creating views if there are ny new ones
        create_views

        # Refresh Asset Views (capture new IDs from OSM)
        refresh_asset_views

        # Refresh Unexposed ID views (capture new IDs from OSM)
        refresh_unexposed_id_views

        # Run Climate Exposure ETL process
        run_nasa_nex_exposure_etl

        run_usda_wildfire_exposure_etl

        # Run FEMA Flood Exposure ETL process (US States only)
        run_fema_exposure_etl

        # Refresh Unexposed ID views
        refresh_unexposed_id_views

        # Create or refresh hazard views
        create_or_refresh_hazard_views

        # Run Geotiff pipeline (Currently running ad hoc for global, no need for individual regions)
        # run_geotiff_etl

    else
        # Initialize the database
        init_database

        # Run migrations
        run_migrations

        # Run OSM ETL process
        run_osm_etl

        # Create views
        create_views
        
        # Run Climate ETL process
        run_nasa_nex_exposure_etl

        run_usda_wildfire_exposure_etl

        # Run FEMA Flood Exposure ETL process (US States only)
        run_fema_exposure_etl

        # Refresh Unexposed ID views
        refresh_unexposed_id_views

        # Create or refresh hazard views
        create_or_refresh_hazard_views

        # Run Geotiff pipeline (Currently running ad hoc for global, no need for individual regions)
        # run_geotiff_etl

    fi

    echo "===== SETUP COMPLETE ====="
    echo "Climate database $PG_DBNAME is now ready for use!"
}

# New main function to handle single or multiple databases
main() {
    if [ "$CLI_DB_NAME" = "all_databases" ]; then
        echo "Running setup for ALL databases defined in $CONFIG_JSON"
        for DB in $(jq -r '.databases | keys_unsorted[]' "$CONFIG_JSON"); do
            echo "=========================================="
            echo "Processing database: $DB"
            echo "=========================================="
            PG_DBNAME=$DB
            run_single_db
        done
    else
        # Either a single database was specified via CLI or environment variable
        run_single_db
    fi
}

# Execute main function
main