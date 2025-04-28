import io
import json
import logging
import time
import random
from typing import Dict

import pandas as pd
import psycopg2 as pg
from psycopg2 import sql

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CLIMATE_SCHEMA = "climate"

TEMP_TABLE_COLUMNS = [
    "osm_id",
    "month",
    "decade",
    "ssp",
    "ensemble_mean",
    "ensemble_median",
    "ensemble_stddev",
    "ensemble_min",
    "ensemble_max",
    "ensemble_q1",
    "ensemble_q3",
    "metadata",
]

def generate_random_table_id():
    timestamp = int(time.time())  # Milliseconds since epoch
    random_part = random.randint(1000, 9999)  # Random 4-digit number
    return f"{timestamp}{random_part}"

def main(
    df: pd.DataFrame,
    ssp: str,
    climate_variable: str,
    conn: pg.extensions.connection,
    batch_size: int = 5000,  # Default batch size, adjust based on your data
):
    # Want to keep ssp in database as ints
    if ssp == 'historical':
        ssp = -999
    
    # Adds columns needed for temp table
    df["ssp"] = int(ssp)
    data_load_table = f"nasa_nex_{climate_variable}"

    # Random ID needed if multiple load processes running at once
    random_table_id = generate_random_table_id()
    temp_table_name = f"nasa_nex_temp_{random_table_id}"

    create_nasa_nex_temp_table = sql.SQL(
    """
    CREATE TEMP TABLE {temp_table} (
        osm_id BIGINT,
        month INT,
        decade INT,
        ssp int,
        ensemble_mean FLOAT NOT NULL,
        ensemble_median FLOAT NOT NULL,
        ensemble_stddev FLOAT NOT NULL,
        ensemble_min FLOAT NOT NULL,
        ensemble_max FLOAT NOT NULL,
        ensemble_q1 FLOAT NOT NULL,
        ensemble_q3 FLOAT NOT NULL,
        metadata JSONB
    );
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    copy_nasa_nex_temp = sql.SQL(
    """
    COPY {temp_table}
    FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER ',')
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    drop_nasa_nex_temp = sql.SQL(
    """
    DROP TABLE {temp_table};
    """
    ).format(temp_table=sql.Identifier(temp_table_name))

    insert_nasa_nex = sql.SQL(
    """
    INSERT INTO {climate_schema}.{table} (osm_id, month, decade, ssp, ensemble_mean, ensemble_median, ensemble_stddev, ensemble_min, ensemble_max, ensemble_q1, ensemble_q3, metadata)
            SELECT temp.osm_id, temp.month, temp.decade, temp.ssp, temp.ensemble_mean, temp.ensemble_median, temp.ensemble_stddev, temp.ensemble_min, temp.ensemble_max, temp.ensemble_q1, temp.ensemble_q3, temp.metadata 
            FROM {temp_table} temp
    ON CONFLICT DO NOTHING
    """
    ).format(
        table=sql.Identifier(data_load_table),
        temp_table=sql.Identifier(temp_table_name),
        climate_schema=sql.Identifier(CLIMATE_SCHEMA),
    )

    # Process in batches to avoid timeout issues
    num_rows = len(df)
    num_batches = (num_rows + batch_size - 1) // batch_size  # Ceiling division
    
    with conn.cursor() as cur:
        # Increase statement timeout to 1 hour
        cur.execute("SET statement_timeout = '3600000';")  # 3600000ms = 1 hour
        
        # Create the temporary table
        cur.execute(create_nasa_nex_temp_table)
        
        # Process data in batches
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, num_rows)
            
            batch_df = df.iloc[start_idx:end_idx]
            if i % 10 == 0:
                logger.info(f"Loading batch {i+1}/{num_batches} ({start_idx}:{end_idx})")
            
            # Prepare batch data for copy
            sio = io.StringIO()
            sio.write(batch_df[TEMP_TABLE_COLUMNS].to_csv(index=False, header=False))
            sio.seek(0)
            
            # Copy batch data
            cur.copy_expert(copy_nasa_nex_temp, sio)
            
        logger.info(f"{climate_variable} Temp Table Loaded (all batches)")

        # Insert data from temp table to final table
        cur.execute(insert_nasa_nex)
        logger.info(f"{climate_variable} Table Loaded")

        # Cleanup
        cur.execute(drop_nasa_nex_temp)

    conn.commit()
