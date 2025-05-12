import io
import json
import logging
import time
import random
from typing import Dict
import os # For potential temp file cleanup

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql

# --- Configuration ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CLIMATE_SCHEMA = "climate"

# Adjust this list if your input DataFrame has different names initially
DF_INPUT_COLUMNS = [
    "osm_id",
    "burn_probability",
    "metadata",
]

# Define the exact column order for the TEMP table and the COPY command
# This must match the order in the CREATE TEMP TABLE and the target table (excluding 'id')
# Ensure 'ssp' is included here.
FINAL_TABLE_COLUMNS = [
    "osm_id",
    "burn_probability",
    "metadata",
]

# --- Helper Functions ---
def generate_random_table_id():
    timestamp = int(time.time() * 1000) # Milliseconds
    random_part = random.randint(1000, 9999)
    return f"{timestamp}_{random_part}"

# --- Main Loading Function ---
def main(
    df: pd.DataFrame,
    usda_variable: str,
    conn: pg.extensions.connection,
    maintenance_work_mem: str = '4GB', # Adjust based on RDS instance RAM (64GB total)
    statement_timeout: int = 7200 * 1000 # 2 hours in milliseconds
):
    """
    Loads data efficiently using a single COPY, index dropping/recreation.

    Args:
        df: Pandas DataFrame containing the data (ensure it fits in EC2 RAM).
        usda_variable: Base name for the target table (e.g., 'fwi').
        conn: Active psycopg2 connection.
        maintenance_work_mem: Memory allocated for index creation.
        statement_timeout: Timeout for SQL statements in milliseconds.
    """
    start_time = time.time()
    num_rows = len(df)
    logger.info(f"Starting efficient bulk load for '{usda_variable}'. Rows: {num_rows}")

    # Verify all needed columns exist in the DataFrame
    if not all(col in df.columns for col in FINAL_TABLE_COLUMNS):
        missing = set(FINAL_TABLE_COLUMNS) - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")

    # Select and reorder columns for COPY command consistency
    df_for_copy = df[FINAL_TABLE_COLUMNS]
    logger.info("DataFrame prepared with correct columns and order.")


    # --- Dynamic Naming ---
    target_table_name = f"usda_{usda_variable}"
    random_id = generate_random_table_id()
    temp_table_name = f"temp_load_{target_table_name}_{random_id}"

    # Define index names dynamically (matching climate schema tables)
    idx_unique_name = f"idx_unique_{target_table_name}_record"
    idx_osm_id_name = f"idx_{target_table_name}_on_osm_id"
    all_indexes = [
        idx_unique_name,
        idx_osm_id_name,
    ]

    # --- SQL Definitions ---
    target_table_ident = sql.Identifier(target_table_name)
    temp_table_ident = sql.Identifier(temp_table_name)
    schema_ident = sql.Identifier(CLIMATE_SCHEMA)
    final_cols_sql = sql.SQL(', ').join(map(sql.Identifier, FINAL_TABLE_COLUMNS))

    create_temp_table_sql = sql.SQL(
        """
        CREATE TEMP TABLE {temp_table} (
            osm_id BIGINT, burn_probability FLOAT,  metadata JSONB
        );"""
    ).format(temp_table=temp_table_ident)

    copy_sql = sql.SQL("COPY {temp_table} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER false)").format(
        temp_table=temp_table_ident,
        columns=final_cols_sql
    )

    analyze_temp_sql = sql.SQL("ANALYZE {temp_table};").format(temp_table=temp_table_ident)

    # INSERT statement explicitly lists columns to avoid issues with 'id' SERIAL column
    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{target_table} ({columns})
        SELECT {columns} FROM {temp_table};
        """
    ).format(
        schema=schema_ident,
        target_table=target_table_ident,
        columns=final_cols_sql,
        temp_table=temp_table_ident
    )

    drop_temp_sql = sql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident)

    # --- Execution ---
    try:
        with conn.cursor() as cur:
            logger.info(f"Setting statement_timeout to {statement_timeout}ms")
            cur.execute(sql.SQL("SET statement_timeout = %s;").format(), (statement_timeout,))

            # 1. Drop Indexes on Target Table
            logger.info(f"Dropping indexes on {schema_ident}.{target_table_ident}...")
            for index_name in all_indexes:
                logger.debug(f"Dropping index: {index_name}")
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {schema}.{index};").format(
                    schema=schema_ident,
                    index=sql.Identifier(index_name))
                )
            logger.info("Indexes dropped.")

            # 2. Create TEMP Table
            logger.info(f"Creating TEMP table {temp_table_ident}...")
            cur.execute(create_temp_table_sql)

            # 3. Prepare data and Execute single COPY
            logger.info("Preparing data in memory for COPY...")
            sio = io.StringIO()
            df_for_copy.to_csv(sio, index=False, header=False, na_rep='\\N') # Use \N for NULLs
            sio.seek(0)
            logger.info(f"Executing single COPY operation for {num_rows} rows...")
            copy_start_time = time.time()
            cur.copy_expert(copy_sql, sio)
            copy_duration = time.time() - copy_start_time
            logger.info(f"COPY complete in {copy_duration:.2f} seconds.")
            del sio # Free memory
            del df_for_copy # Free memory
            del df # Free memory

            # 4. Analyze TEMP Table
            logger.info(f"Analyzing {temp_table_ident}...")
            analyze_start_time = time.time()
            cur.execute(analyze_temp_sql)
            analyze_duration = time.time() - analyze_start_time
            logger.info(f"Analyze complete in {analyze_duration:.2f} seconds.")

            # 5. Insert from TEMP to Target
            logger.info(f"Inserting data from {temp_table_ident} into {schema_ident}.{target_table_ident}...")
            insert_start_time = time.time()
            cur.execute(insert_sql)
            insert_duration = time.time() - insert_start_time
            logger.info(f"INSERT complete in {insert_duration:.2f} seconds. Rows affected: {cur.rowcount}") # rowcount useful here

            # 6. Drop TEMP Table (do this before recreating indexes to free resources)
            logger.info(f"Dropping TEMP table {temp_table_ident}...")
            cur.execute(drop_temp_sql)

            # 7. Recreate Indexes
            logger.info(f"Recreating indexes on {schema_ident}.{target_table_ident}...")
            index_start_time = time.time()
            # Set memory locally for this transaction block for index creation
            cur.execute(sql.SQL("SET local maintenance_work_mem = %s;"), (maintenance_work_mem,))
            logger.info(f"Set local maintenance_work_mem to {maintenance_work_mem}")

            # Recreate indexes using the definitions from your schema setup
            cur.execute(sql.SQL("""
                CREATE UNIQUE INDEX {index_name} ON {schema}.{target_table} (osm_id);
            """).format(index_name=sql.Identifier(idx_unique_name), schema=schema_ident, target_table=target_table_ident))
            logger.debug(f"Recreated {idx_unique_name}")

            cur.execute(sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (osm_id);")
                        .format(index_name=sql.Identifier(idx_osm_id_name), schema=schema_ident, target_table=target_table_ident))
            logger.debug(f"Recreated {idx_osm_id_name}")

            index_duration = time.time() - index_start_time
            logger.info(f"Index recreation complete in {index_duration:.2f} seconds.")

        # Transaction commited automatically by exiting the 'with' block if no errors
        conn.commit() # Explicit commit for clarity after successful block execution
        total_duration = time.time() - start_time
        logger.info(f"Successfully completed bulk load for '{usda_variable}' in {total_duration:.2f} seconds.")

    except (Exception, pg.Error) as error:
        logger.error(f"Error during bulk load for {usda_variable}: {error}", exc_info=True)
        # Rollback automatically handled by exiting 'with' block on error
        conn.rollback() # Explicit rollback for clarity
        logger.warning("Transaction rolled back due to error.")
        raise # Re-raise the exception after logging and rollback

    finally:
        # Ensure temp table is dropped even if index creation fails mid-way
        # but only if connection is still usable
        if conn.status == pg.extensions.STATUS_READY and not conn.closed:
             try:
                 with conn.cursor() as cur:
                     cur.execute(sql.SQL("SET statement_timeout = '60000';")) # Short timeout for cleanup
                     cur.execute(drop_temp_sql)
                     logger.info(f"Final cleanup: Ensured TEMP table {temp_table_name} is dropped.")
                 conn.commit()
             except pg.Error as cleanup_error:
                 logger.error(f"Error during final TEMP table cleanup: {cleanup_error}")
                 conn.rollback()
        else:
            logger.warning("Skipping final TEMP table cleanup as connection is not ready.")


# --- Example Usage ---
if __name__ == '__main__':
    # This is example setup - replace with your actual connection and DataFrame loading
    try:
        # --- Database Connection ---
        # Replace with your actual connection details
        db_params = {
            "host": "your-rds-endpoint.us-west-2.rds.amazonaws.com",
            "database": "your_database",
            "user": "climate_user",
            "password": "your_password",
            "port": 5432
        }
        connection = pg.connect(**db_params)
        connection.autocommit = False # Ensure we control transactions

        # --- Load your DataFrame ---
        # Replace this with your actual DataFrame loading logic
        # For testing, creating a large dummy DataFrame (might take time/memory)
        logger.info("Creating dummy DataFrame for testing...")
        num_test_rows = 10 * 1000 * 1000 # Smaller than 180M for quicker testing
        dummy_data = {
            "osm_id": [random.randint(1, 1000000) for _ in range(num_test_rows)]
        }
        input_df = pd.DataFrame(dummy_data)
        logger.info(f"Dummy DataFrame created with {len(input_df)} rows.")


        # --- Run the Load ---
        main(
            df=input_df,
            ssp_value='ssp126',         # Example SSP
            usda_variable='fwi',     # Matches your table name part
            conn=connection
        )

    except pg.Error as db_error:
         logger.error(f"Database connection error: {db_error}", exc_info=True)
    except ValueError as val_error:
         logger.error(f"Data validation error: {val_error}", exc_info=True)
    except Exception as e:
         logger.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if 'connection' in locals() and connection is not None and not connection.closed:
            connection.close()
            logger.info("Database connection closed.")