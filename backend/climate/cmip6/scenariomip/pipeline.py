import os
import tempfile

from psycopg2 import pool

import utils
import process_climate
import generate_geotiff
import infra_intersection
import infra_intersection_load

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


S3_BUCKET = os.environ["S3_BUCKET"]
S3_BASE_PREFIX = os.environ["S3_BASE_PREFIX"]
CLIMATE_VARIABLE = os.environ["CLIMATE_VARIABLE"]
SSP = os.environ["SSP"]
XARRAY_ENGINE = os.environ["XARRAY_ENGINE"]
CRS = os.environ["CRS"]
X_DIM = os.environ["X_DIM"]
Y_DIM = os.environ["Y_DIM"]
TIME_DIM = os.environ["TIME_DIM"]
TIME_AGGREGATION_METHOD = os.environ["TIME_AGGREGATION_METHOD"]
CONVERT_360_LON = bool(os.environ["CONVERT_360_LON"])
STATE_BBOX = os.environ.get("STATE_BBOX", None)
OSM_CATEGORY = os.environ["OSM_CATEGORY"]
OSM_TYPE = os.environ["OSM_TYPE"]
INTERSECTION_DEBUG = (
    False if os.environ["INTERSECTION_DEBUG"].lower() == "false" else True
)  # TODO: REMOVE THIS
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]

# Centrally manage database connections for the pipeline
CONNECTION_POOL = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=3,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT,
)


def get_connection():
    """Get a connection from the pool."""
    return CONNECTION_POOL.getconn()


def release_connection(conn):
    """Return a connection to the pool."""
    CONNECTION_POOL.putconn(conn)


def close_all_connections():
    """Close all connections in the pool."""
    CONNECTION_POOL.closeall()


def run():
    """Runs a processing pipeline for a given climate variable
    and SSP
    """
    with tempfile.TemporaryDirectory() as climate_tmpdir:
        utils.download_files(
            s3_bucket=S3_BUCKET,
            s3_prefix=utils.create_s3_prefix(
                S3_BASE_PREFIX, CLIMATE_VARIABLE, SSP, "data"
            ),
            dir=climate_tmpdir,
        )
        logger.info("Climate Files Downloaded")

        ds = process_climate.main(
            file_directory=climate_tmpdir,
            xarray_engine=XARRAY_ENGINE,
            crs=CRS,
            x_dim=X_DIM,
            y_dim=Y_DIM,
            convert_360_lon=CONVERT_360_LON,
            bbox=utils.get_state_bbox(STATE_BBOX),
            time_dim=TIME_DIM,
            time_agg_method=TIME_AGGREGATION_METHOD,
        )

        logger.info("Climate Data Processed")

    with tempfile.TemporaryDirectory() as geotiff_tmpdir:
        if not INTERSECTION_DEBUG:
            generate_geotiff.main(
                ds=ds,
                output_dir=geotiff_tmpdir,
                climate_variable=CLIMATE_VARIABLE,
                state=STATE_BBOX,
                time_agg_method=TIME_AGGREGATION_METHOD,
            )
            logger.info("Geotiffs created")
            utils.upload_files(
                s3_bucket=S3_BUCKET,
                s3_prefix=utils.create_s3_prefix(
                    S3_BASE_PREFIX,
                    CLIMATE_VARIABLE,
                    SSP,
                    f"cogs/{TIME_AGGREGATION_METHOD}",
                ),
                dir=geotiff_tmpdir,
            )
            logger.info("Geotiffs uploaded")

        infra_intersection_conn = get_connection()
        df = infra_intersection.main(
            climate_ds=ds,
            climate_variable=CLIMATE_VARIABLE,
            osm_category=OSM_CATEGORY,
            osm_type=OSM_TYPE,
            crs=CRS,
            x_dim=X_DIM,
            y_dim=Y_DIM,
            time_agg_method=TIME_AGGREGATION_METHOD,
            conn=infra_intersection_conn,
        )
        release_connection(infra_intersection_conn)
        logger.info("Infrastructure Intersection Complete")

        infra_intersection_load_conn = get_connection()
        infra_intersection_load.main(
            df=df,
            ssp=int(SSP),
            climate_variable=CLIMATE_VARIABLE,
            conn=infra_intersection_load_conn,
        )


if __name__ == "__main__":
    run()
