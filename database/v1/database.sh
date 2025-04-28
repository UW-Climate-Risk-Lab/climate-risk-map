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
EXPOSURE_ETL_DIR="$ROOT_DIR/etl/exposure/nasa_nex"
ASSET_GROUP_DIR="$ROOT_DIR/materialized_views/asset_groups"
UNEXPOSED_DIR="$ROOT_DIR/materialized_views/unexposed_ids"
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
if [ -n "$CLI_DB_NAME" ]; then
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
required_vars=("PG_DBNAME" "PGUSER" "PGPASSWORD" "PGHOST" "PGPORT" "S3_BUCKET" "PGOSM_USER" "PGOSM_PASSWORD" "PGOSM_RAM" "PGOSM_REGION" "PGOSM_SUBREGION" "PGOSM_LAYERSET" "PGOSM_LANGUAGE" "PGOSM_SRID" "PGCLIMATE_USER" "PGCLIMATE_PASSWORD" "PGCLIMATE_HOST")
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
    docker build -t database-v1-osm-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build ETL Docker image"
        cd "$ROOT_DIR"  # Return to root directory
        exit 1
    fi
    
    # Run Docker container with environment variables
    echo "Running ETL process to load OSM data..."

    docker run --rm \
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

    for VIEW in administrative agriculture commercial_real_estate data_center power_grid residential_real_estate
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

    for VIEW in unexposed_ids_nasa_nex_fwi
    do
        echo "Refreshing osm.$VIEW..."
        PGPASSWORD=$PG_SUPER_PASSWORD psql -U "$PGUSER" -d "$PG_DBNAME" -h "$PGHOST" -p "$PGPORT" \
            -c "REFRESH MATERIALIZED VIEW osm.$VIEW;"
    done
}


# Step 5: Run Climate ETL Process
run_exposure_etl() {
    echo "===== STEP 6: RUNNING CLIMATE ETL PROCESS ====="
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$EXPOSURE_ETL_DIR" ]; then
        echo "Error: ETL directory not found at $EXPOSURE_ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$EXPOSURE_ETL_DIR/Dockerfile" ]; then
        echo "Error: Dockerfile not found in Climate ETL directory"
        exit 1
    fi
    
    
    # Navigate to ETL directory
    cd "$EXPOSURE_ETL_DIR"
    
    # Build Docker image
    echo "Building Climate ETL Docker image..."
    docker build -t database-v1-climate-etl .
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to build Climate ETL Docker image"
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
        
        # Run Docker container with environment variables and arguments
        echo "Running ETL process for climate dataset $((i+1))..."
        
        docker run -v ~/.aws/credentials:/root/.aws/credentials:ro --rm \
            -e PG_DBNAME=$PG_DBNAME \
            -e PGUSER=$PGCLIMATE_USER \
            -e PGPASSWORD=$PGCLIMATE_PASSWORD \
            -e PGHOST=$PGCLIMATE_HOST \
            -e PGPORT=$PGPORT \
            database-v1-climate-etl \
            --s3-zarr-store-uri "$S3_ZARR_STORE_URI" \
            --climate-variable "$CLIMATE_VARIABLE" \
            --ssp "$SSP" \
            --zonal-agg-method "$ZONAL_AGG_METHOD" \
            --polygon-area-threshold "$POLYGON_AREA_THRESHOLD" \
            --x_min "$X_MIN" \
            --y_min "$Y_MIN" \
            --x_max "$X_MAX" \
            --y_max "$Y_MAX" \
        
        if [ $? -ne 0 ]; then
            echo "Error: ETL process failed for dataset $((i+1))"
            cd "$ROOT_DIR"  # Return to root directory
            exit 1
        fi
        
        echo "Dataset $((i+1)) processed successfully"
    done
    
    # Return to root directory
    cd "$ROOT_DIR"
    
    echo "Climate ETL process completed successfully for all datasets"
}



# Main execution
main() {
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

        # Refresh Asset Views (capture new IDs from OSM)
        refresh_asset_views

        # Refresh Unexposed ID views (capture new IDs from OSM)
        refresh_unexposed_id_views

        # Run Climate Exposure ETL process
        run_exposure_etl

        # Refresh Unexposed ID views
        refresh_unexposed_id_views

    else
        # Initialize the database
        init_database

        # Run OSM ETL process
        run_osm_etl

        # Run migrations
        run_migrations
        
        # Create views
        create_views
        
        # Run Climate ETL process
        run_exposure_etl

        # Refresh Unexposed ID views
        refresh_unexposed_id_views

    fi

    echo "===== SETUP COMPLETE ====="
    echo "Climate database $PG_DBNAME is now ready for use!"
    
}

# Execute main function
main