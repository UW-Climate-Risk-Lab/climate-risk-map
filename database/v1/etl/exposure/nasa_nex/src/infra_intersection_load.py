import io
import json
import logging
import time
import random
from typing import Dict
import os # For potential temp file cleanup
import tempfile

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql

# --- Configuration ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

CLIMATE_SCHEMA = "climate"

# --- Column Definitions based on Dataset Type ---

# For 'decade_month' type
DECADE_MONTH_COLS = [
    "osm_id", "month", "decade", "ssp", "ensemble_mean", "ensemble_median",
    "ensemble_stddev", "ensemble_min", "ensemble_max", "ensemble_q1", "ensemble_q3", "metadata"
]

# For 'year_span_month' type with no return period
YEAR_SPAN_MONTH_NO_RP_COLS = [
    "osm_id", "month", "start_year", "end_year", "ssp",
    "ensemble_mean", "ensemble_median", "ensemble_stddev", "ensemble_min",
    "ensemble_max", "ensemble_q1", "ensemble_q3", "metadata"
]

# For 'year_span_month' type with return period
YEAR_SPAN_MONTH_RP_COLS = [
    "osm_id", "month", "start_year", "end_year", "ssp", "return_period",
    "ensemble_mean", "ensemble_median", "ensemble_stddev", "ensemble_min",
    "ensemble_max", "ensemble_q1", "ensemble_q3", "metadata"
]


def get_final_table_columns(time_period_type: str, has_return_period: bool) -> list:
    if time_period_type == "decade_month":
        return DECADE_MONTH_COLS
    elif time_period_type == "year_span_month":
        if has_return_period:
            return YEAR_SPAN_MONTH_RP_COLS
        else:
            return YEAR_SPAN_MONTH_NO_RP_COLS
    else:
        raise ValueError(f"Unknown time_period_type: {time_period_type}")


# --- Helper Functions ---
def generate_random_table_id():
    timestamp = int(time.time() * 1000) # Milliseconds
    random_part = random.randint(1000, 9999)
    return f"{timestamp}_{random_part}"

# --- Main Loading Function ---
def main(
    df: pd.DataFrame,
    ssp_value: str, # Use distinct name from column name
    climate_variable: str,
    time_period_type: str,
    conn: pg.extensions.connection,
    maintenance_work_mem: str = '4GB', # Adjust based on RDS instance RAM (64GB total),
    num_parallel_workers: int = 2,
    statement_timeout: int = 7200 * 1000, # 2 hours in milliseconds
    return_period: int = None,
):
    """
    Loads data efficiently using a single COPY, index dropping/recreation.

    Args:
        df: Pandas DataFrame containing the data (ensure it fits in EC2 RAM).
        ssp_value: The SSP identifier string (e.g., 'historical', 'ssp126').
        climate_variable: Base name for the target table (e.g., 'fwi').
        time_period_type: The type of dataset ('decade_month' or 'year_span_month').
        conn: Active psycopg2 connection.
        maintenance_work_mem: Memory allocated for index creation.
        statement_timeout: Timeout for SQL statements in milliseconds.
        return_period: The return period in years, if applicable.
    """
    start_time = time.time()
    num_rows = len(df)
    logger.info(f"Starting efficient bulk load for '{climate_variable}', ssp '{ssp_value}'. Rows: {num_rows}, type: '{time_period_type}'")

    # --- Prepare Data ---
    ssp_int = -999 if ssp_value == 'historical' else int(ssp_value.replace('ssp', ''))

    df = df.copy()
    df["ssp"] = ssp_int
    logger.info("Added 'ssp' column to DataFrame.")
    
    has_return_period = return_period is not None
    if time_period_type == "year_span_month" and has_return_period:
        df["return_period"] = return_period
        logger.info(f"Added 'return_period' column with value: {return_period}")

    FINAL_TABLE_COLUMNS = get_final_table_columns(time_period_type, has_return_period)

    if not all(col in df.columns for col in FINAL_TABLE_COLUMNS):
        missing = set(FINAL_TABLE_COLUMNS) - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")

    df_for_copy = df[FINAL_TABLE_COLUMNS]
    logger.info("DataFrame prepared with correct columns and order.")

    # --- Dynamic Naming ---
    target_table_name = f"nasa_nex_{climate_variable}"
    random_id = generate_random_table_id()
    temp_table_name = f"temp_load_{target_table_name}_{random_id}"

    # --- Index and SQL Definitions ---
    # These will be adapted based on time_period_type inside the try block
    
    target_table_ident = sql.Identifier(target_table_name)
    temp_table_ident = sql.Identifier(temp_table_name)
    schema_ident = sql.Identifier(CLIMATE_SCHEMA)
    final_cols_sql = sql.SQL(', ').join(map(sql.Identifier, FINAL_TABLE_COLUMNS))

    # --- Execution ---
    try:
        with conn.cursor() as cur:
            logger.info(f"Setting statement_timeout to {statement_timeout}ms")
            cur.execute(sql.SQL("SET statement_timeout = %s;").format(), (statement_timeout,))

            # 1. Drop Indexes on Target Table
            logger.info(f"Dropping indexes on {schema_ident}.{target_table_ident}...")
            # Drop indexes dynamically based on what might exist.
            # This is safer than hardcoding indexes that might not apply to all table types.
            # Example:
            if time_period_type == "decade_month":
                all_indexes = [
                    f"idx_unique_{target_table_name}_record",
                    f"idx_{target_table_name}_on_osm_id",
                    f"idx_{target_table_name}_on_month",
                    f"idx_{target_table_name}_on_decade",
                    f"idx_{target_table_name}_on_month_decade",
                    f"idx_{target_table_name}_on_ssp",
                ]
            elif time_period_type == "year_span_month":
                base_indexes = [
                    f"idx_unique_{target_table_name}_record",
                    f"idx_{target_table_name}_on_osm_id",
                    f"idx_{target_table_name}_on_month",
                    f"idx_{target_table_name}_on_start_year",
                    f"idx_{target_table_name}_on_end_year",
                    f"idx_{target_table_name}_on_ssp",
                    f"idx_{target_table_name}_on_month_year",
                ]
                if has_return_period:
                    base_indexes.append(f"idx_{target_table_name}_on_return_period")
                all_indexes = base_indexes
            else:
                all_indexes = []

            for index_name in all_indexes:
                logger.debug(f"Dropping index: {index_name}")
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {schema}.{index};").format(
                    schema=schema_ident,
                    index=sql.Identifier(index_name))
                )
            logger.info("Indexes dropped.")

            # 2. Create TEMP Table
            logger.info(f"Creating TEMP table {temp_table_ident}...")
            if time_period_type == "decade_month":
                create_temp_table_sql = sql.SQL("""
                    CREATE TEMP TABLE {temp_table} (
                        osm_id BIGINT, month SMALLINT, decade SMALLINT, ssp SMALLINT,
                        ensemble_mean FLOAT, ensemble_median FLOAT, ensemble_stddev FLOAT,
                        ensemble_min FLOAT, ensemble_max FLOAT, ensemble_q1 FLOAT,
                        ensemble_q3 FLOAT, metadata JSONB
                    );""").format(temp_table=temp_table_ident)
            elif time_period_type == "year_span_month":
                if has_return_period:
                    create_temp_table_sql = sql.SQL("""
                        CREATE TEMP TABLE {temp_table} (
                            osm_id BIGINT, month SMALLINT, start_year SMALLINT, end_year SMALLINT,
                            ssp SMALLINT, return_period SMALLINT,
                            ensemble_mean FLOAT, ensemble_median FLOAT, ensemble_stddev FLOAT,
                            ensemble_min FLOAT, ensemble_max FLOAT, ensemble_q1 FLOAT,
                            ensemble_q3 FLOAT, metadata JSONB
                        );""").format(temp_table=temp_table_ident)
                else:
                    create_temp_table_sql = sql.SQL("""
                        CREATE TEMP TABLE {temp_table} (
                            osm_id BIGINT, month SMALLINT, start_year SMALLINT, end_year SMALLINT,
                            ssp SMALLINT,
                            ensemble_mean FLOAT, ensemble_median FLOAT, ensemble_stddev FLOAT,
                            ensemble_min FLOAT, ensemble_max FLOAT, ensemble_q1 FLOAT,
                            ensemble_q3 FLOAT, metadata JSONB
                        );""").format(temp_table=temp_table_ident)
            
            cur.execute(create_temp_table_sql)

            # 3. Prepare data and Execute single COPY
            # Using a temporary file on disk can be faster for very large DataFrames
            # as it avoids holding the entire serialized CSV in memory at once, which can be
            # slow due to memory allocation overhead. On an EC2 instance, disk I/O
            # to EBS or instance storage is generally fast.
            copy_prep_start_time = time.time()
            logger.info("Preparing data for COPY using a temporary file...")

            with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8', suffix='.csv') as tmp:
                # Write dataframe to the temporary file as CSV
                df_for_copy.to_csv(tmp, index=False, header=False, na_rep='\\N')

                prep_duration = time.time() - copy_prep_start_time
                logger.info(f"Data prepared in temporary file in {prep_duration:.2f} seconds.")

                # Rewind the file to the beginning so psycopg2 can read it
                tmp.seek(0)

                logger.info(f"Executing single COPY operation for {num_rows} rows...")
                copy_start_time = time.time()
                copy_sql = sql.SQL("COPY {temp_table} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER false)").format(
                    temp_table=temp_table_ident,
                    columns=final_cols_sql
                )
                cur.copy_expert(copy_sql, tmp)
                copy_duration = time.time() - copy_start_time
                logger.info(f"COPY complete in {copy_duration:.2f} seconds.")
                # The 'with' block will automatically close and delete the temporary file.

            del df_for_copy
            del df

            # 4. Analyze TEMP Table
            logger.info(f"Analyzing {temp_table_ident}...")
            analyze_start_time = time.time()
            cur.execute(sql.SQL("ANALYZE {temp_table};").format(temp_table=temp_table_ident))
            analyze_duration = time.time() - analyze_start_time
            logger.info(f"Analyze complete in {analyze_duration:.2f} seconds.")

            # 5. Insert from TEMP to Target
            logger.info(f"Inserting data from {temp_table_ident} into {schema_ident}.{target_table_ident}...")
            insert_start_time = time.time()
            insert_sql = sql.SQL(
                "INSERT INTO {schema}.{target_table} ({columns}) SELECT {columns} FROM {temp_table};"
            ).format(
                schema=schema_ident,
                target_table=target_table_ident,
                columns=final_cols_sql,
                temp_table=temp_table_ident
            )
            cur.execute(insert_sql)
            insert_duration = time.time() - insert_start_time
            logger.info(f"INSERT complete in {insert_duration:.2f} seconds. Rows affected: {cur.rowcount}")

            # 6. Drop TEMP Table
            logger.info(f"Dropping TEMP table {temp_table_ident}...")
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident))

            # 7. Recreate Indexes
            logger.info(f"Recreating indexes on {schema_ident}.{target_table_ident}...")
            index_start_time = time.time()

            # --- Performance Tuning for Index Creation ---
            # Set memory for maintenance tasks. On a parallel build, this is allocated PER WORKER.
            cur.execute(sql.SQL("SET local maintenance_work_mem = %s;"), (maintenance_work_mem,))
            logger.info(f"Set local maintenance_work_mem to {maintenance_work_mem} (per worker)")

            # Enable parallel index creation. For a 32 vCPU instance, using 16 workers is a good start.
            # This allows PostgreSQL to use multiple CPU cores to build the index much faster.
            # The total memory used will be maintenance_work_mem * num_parallel_workers.
            # e.g., 4GB * 16 = 64GB, which is safe on a 128GB RAM instance.
            cur.execute(sql.SQL("SET local max_parallel_maintenance_workers = %s;"), (num_parallel_workers,))
            cur.execute(sql.SQL("SET local max_parallel_workers_per_gather = %s;"), (num_parallel_workers,))
            # Ensure we have enough total parallel workers
            cur.execute(sql.SQL("SET local max_parallel_workers = %s;"), (num_parallel_workers * 2,))
            cur.execute("SET LOCAL effective_io_concurrency = 256;")
            logger.info(f"Set local max_parallel_maintenance_workers and max_parallel_workers_per_gather to {num_parallel_workers}")


            if time_period_type == "decade_month":
                # Build and execute each index creation with the full index name treated as a single identifier
                cur.execute(
                    sql.SQL("CREATE UNIQUE INDEX {index_name} ON {schema}.{target_table} (osm_id, month, decade, ssp);").format(
                        index_name=sql.Identifier(f"idx_unique_{target_table_name}_record"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (osm_id);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_osm_id"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (month);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_month"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (decade);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_decade"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (month, decade);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_month_decade"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (ssp);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_ssp"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

            elif time_period_type == "year_span_month":
                # Build list of unique columns and convert to SQL identifiers
                unique_columns = ["osm_id", "month", "start_year", "end_year", "ssp"]
                if has_return_period:
                    unique_columns.append("return_period")

                unique_cols_sql = sql.SQL(", ").join(map(sql.Identifier, unique_columns))

                # Unique composite index
                cur.execute(
                    sql.SQL("CREATE UNIQUE INDEX {index_name} ON {schema}.{target_table} ({unique_cols});").format(
                        index_name=sql.Identifier(f"idx_unique_{target_table_name}_record"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                        unique_cols=unique_cols_sql,
                    )
                )

                # Simple (non-unique) indexes
                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (osm_id);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_osm_id"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (month);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_month"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (start_year);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_start_year"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (end_year);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_end_year"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

                if has_return_period:
                    cur.execute(
                        sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (return_period);").format(
                            index_name=sql.Identifier(f"idx_{target_table_name}_on_return_period"),
                            schema=schema_ident,
                            target_table=target_table_ident,
                        )
                    )

                cur.execute(
                    sql.SQL("CREATE INDEX {index_name} ON {schema}.{target_table} (ssp);").format(
                        index_name=sql.Identifier(f"idx_{target_table_name}_on_ssp"),
                        schema=schema_ident,
                        target_table=target_table_ident,
                    )
                )

            index_duration = time.time() - index_start_time
            logger.info(f"Index recreation complete in {index_duration:.2f} seconds.")

        conn.commit()
        total_duration = time.time() - start_time
        logger.info(f"Successfully completed bulk load for '{climate_variable}' in {total_duration:.2f} seconds.")

    except (Exception, pg.Error) as error:
        logger.error(f"Error during bulk load for {climate_variable}: {error}", exc_info=True)
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
                     cur.execute(sql.SQL("DROP TABLE IF EXISTS {temp_table};").format(temp_table=temp_table_ident))
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
            "osm_id": [random.randint(1, 1000000) for _ in range(num_test_rows)],
            "month": [random.randint(1, 12) for _ in range(num_test_rows)],
            "decade": [random.choice([2020, 2030, 2040, 2050]) for _ in range(num_test_rows)],
            "ensemble_mean": [random.uniform(0, 50) for _ in range(num_test_rows)],
            "ensemble_median": [random.uniform(0, 50) for _ in range(num_test_rows)],
            "ensemble_stddev": [random.uniform(0, 10) for _ in range(num_test_rows)],
            "ensemble_min": [random.uniform(-10, 30) for _ in range(num_test_rows)],
            "ensemble_max": [random.uniform(30, 70) for _ in range(num_test_rows)],
            "ensemble_q1": [random.uniform(10, 30) for _ in range(num_test_rows)],
            "ensemble_q3": [random.uniform(30, 50) for _ in range(num_test_rows)],
            "metadata": [json.dumps({"source": "test"}) for _ in range(num_test_rows)],
        }
        input_df = pd.DataFrame(dummy_data)
        logger.info(f"Dummy DataFrame created with {len(input_df)} rows.")


        # --- Run the Load ---
        main(
            df=input_df,
            ssp_value='ssp126',
            climate_variable='fwi',
            time_period_type='decade_month',  # Example dataset type
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