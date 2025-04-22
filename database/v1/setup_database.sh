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
    echo "  PG_DBNAME, PGOSMFLEX_REGION, PGOSMFLEX_SUBREGION"
    echo
    echo "Other required environment variables:"
    echo "  PGUSER, PGPASSWORD, PGHOST, PGPORT, PGOSMFLEX_USER, PGOSMFLEX_PASSWORD,"
    echo "  PGOSMFLEX_RAM, PGOSMFLEX_LAYERSET, PGOSMFLEX_PGOSM_LANGUAGE, PGOSMFLEX_SRID"
}

# Process command line arguments
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_usage
    exit 0
fi

# Assign command line arguments to variables, if provided
CLI_DB_NAME=$1
CLI_REGION=$2
CLI_SUBREGION=$3

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
fi

if [ -n "$CLI_REGION" ]; then
    PGOSMFLEX_REGION=$CLI_REGION
    echo "Using OSM region from command line: $PGOSMFLEX_REGION"
fi

if [ -n "$CLI_SUBREGION" ]; then
    PGOSMFLEX_SUBREGION=$CLI_SUBREGION
    echo "Using OSM subregion from command line: $PGOSMFLEX_SUBREGION"
fi

# Set default for SRID if not defined
PGOSMFLEX_SRID=${PGOSMFLEX_SRID:-4326}

# Check required environment variables
required_vars=("PG_DBNAME" "PGUSER" "PGPASSWORD" "PGHOST" "PGPORT" "PGOSMFLEX_USER" "PGOSMFLEX_PASSWORD" "PGOSMFLEX_RAM" "PGOSMFLEX_REGION" "PGOSMFLEX_SUBREGION" "PGOSMFLEX_LAYERSET" "PGOSMFLEX_PGOSM_LANGUAGE" "PGOSMFLEX_SRID")
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

# Define directory paths
ROOT_DIR=$(pwd)
MIGRATIONS_DIR="$ROOT_DIR/migrations"
ETL_DIR="$ROOT_DIR/etl"
ASSET_GROUP_DIR="$ROOT_DIR/materialized_views/asset_groups"
UNEXPOSED_DIR="$ROOT_DIR/materialized_views/unexposed_ids"

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
run_etl() {
    echo "===== STEP 2: RUNNING ETL PROCESS ====="
    echo "Using Region: $PGOSMFLEX_REGION, Subregion: $PGOSMFLEX_SUBREGION"
    
    # Check if Docker is installed
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check if ETL directory exists
    if [ ! -d "$ETL_DIR" ]; then
        echo "Error: ETL directory not found at $ETL_DIR"
        exit 1
    fi
    
    # Check if Dockerfile exists in ETL directory
    if [ ! -f "$ETL_DIR/Dockerfile" ]; then
        echo "Error: Dockerfile not found in ETL directory"
        exit 1
    fi
    
    # Navigate to ETL directory
    cd "$ETL_DIR"
    
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
    echo "DEBUG: $PG_HOST"
    docker run --rm \
        -e POSTGRES_USER=$PGOSMFLEX_USER \
        -e POSTGRES_PASSWORD=$PGOSMFLEX_PASSWORD \
        -e POSTGRES_HOST=$PGOSMFLEX_HOST \
        -e POSTGRES_DB=$PG_DBNAME \
        -e POSTGRES_PORT=$PGPORT \
        -e RAM=$PGOSMFLEX_RAM \
        -e REGION=$PGOSMFLEX_REGION \
        -e SUBREGION=$PGOSMFLEX_SUBREGION \
        -e LAYERSET=$PGOSMFLEX_LAYERSET \
        -e SRID=$PGOSMFLEX_SRID \
        -e PGOSM_LANGUAGE=$PGOSMFLEX_PGOSM_LANGUAGE \
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
    
    # Set PGDATABASE for migrations if not already set
    export PGDATABASE=$PG_DBNAME
    
    # Check if run_migrations.sh exists and is executable
    if [ ! -f "$ROOT_DIR/run_migrations.sh" ]; then
        echo "Error: run_migrations.sh not found at $ROOT_DIR/run_migrations.sh"
        exit 1
    fi
    
    if [ ! -x "$ROOT_DIR/run_migrations.sh" ]; then
        chmod +x "$ROOT_DIR/run_migrations.sh"
    fi
    
    echo "Running migrations..."
    "$ROOT_DIR/run_migrations.sh"
    
    if [ $? -ne 0 ]; then
        echo "Error: Failed to run migrations"
        exit 1
    fi
    
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
                psql $DB_CONN -f "$SQL_FILE"
                
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
                psql $DB_CONN -f "$SQL_FILE"
                
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

# Main execution
main() {
    echo "Starting climate database setup for region: $PG_DBNAME"
    echo "OSM Region: $PGOSMFLEX_REGION"
    echo "OSM Subregion: $PGOSMFLEX_SUBREGION"
    
    if database_exists "$PG_DBNAME"; then
        read -p "Database $PG_DBNAME already exists. Do you want to continue with migrations and view creation? (y/n): " confirm
        if [[ $confirm != [yY] ]]; then
            echo "Setup aborted."
            exit 0
        fi
        
        # Skip database initialization and ETL if it already exists
        echo "Skipping database initialization and ETL process."
    else
        # Initialize the database
        init_database

    fi

    # Run ETL process
    run_etl

    # Run migrations
    run_migrations
    
    # Create views
    create_views
    
    echo "===== SETUP COMPLETE ====="
    echo "Climate database $PG_DBNAME is now ready for use!"
}

# Execute main function
main