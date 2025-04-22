import argparse
import logging
import os

from psycopg2 import pool

import src.infra_intersection as infra_intersection
import src.infra_intersection_load as infra_intersection_load
import src.process_climate as process_climate
import src.utils as utils
import src.constants as constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PGUSER"]
PG_PASSWORD = os.environ["PGPASSWORD"]
PG_HOST = os.environ["PGHOST"]
PG_PORT = os.environ["PGPORT"]


def setup_args():
    parser = argparse.ArgumentParser(description="Process climate data for a given SSP")

    parser.add_argument(
        "--s3-zarr-store-uri",
        required=True,
        help="S3 URI to zarr store containing climate dataset ",
    )
    parser.add_argument(
        "--climate-variable",
        required=True,
        help="Climate variable to process in zarr store",
    )
    parser.add_argument("--ssp", required=True, help="SSP of climate hazard data")
    parser.add_argument(
        "--zonal-agg-method", required=True, help="Zonal aggregation method"
    )
    parser.add_argument(
        "--polygon-area-threshold",
        required=True,
        help="Polygons below this threshold are converted to points for zonal aggregation. Units are Square Kilometers",
    )
    return parser.parse_args()


def main(
    s3_zarr_store_uri: str,
    climate_variable: str,
    ssp: str,
    zonal_agg_method: str,
    polygon_area_threshold: str,
):
    """Runs a processing pipeline for a given zarr store"""

    try:
        polygon_area_threshold = float(polygon_area_threshold)
    except Exception as e:
        logger.error(
            f"Could not convert '{polygon_area_threshold}' to a float, defaulting to 20 sq km: {str(e)}"
        )
        polygon_area_threshold = 20.0

    # Create connection pool with passed parameters
    connection_pool = pool.SimpleConnectionPool(
        minconn=1,
        maxconn=3,
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT,
    )

    ds = process_climate.main(
        s3_zarr_store_uri=s3_zarr_store_uri,
        crs=constants.CRS,
    )

    metadata = utils.create_metadata(ds=ds)

    metadata[constants.METADATA_KEY]["zonal_agg_method"] = zonal_agg_method

    logger.info("Climate Data Loaded & Processed")

    infra_intersection_conn = connection_pool.getconn()
    df = infra_intersection.main(
        climate_ds=ds,
        climate_variable=climate_variable,
        crs=constants.CRS,
        zonal_agg_method=zonal_agg_method,
        polygon_area_threshold=polygon_area_threshold,
        conn=infra_intersection_conn,
        metadata=metadata,
    )
    connection_pool.putconn(infra_intersection_conn)
    logger.info("Infrastructure Intersection Complete")

    infra_intersection_load_conn = connection_pool.getconn()
    infra_intersection_load.main(
        df=df,
        ssp=ssp,
        climate_variable=climate_variable,
        conn=infra_intersection_load_conn,
    )
    connection_pool.putconn(infra_intersection_load_conn)


if __name__ == "__main__":
    args = setup_args()
    logger.info(f"STARTING EXPOSURE CALCULATION FOR {args.s3_zarr_store_uri}")
    main(
        s3_zarr_store_uri=args.s3_zarr_store_uri,
        climate_variable=args.climate_variable,
        ssp=args.ssp,
        zonal_agg_method=args.zonal_agg_method,
        polygon_area_threshold=args.polygon_area_threshold,
    )
    logger.info(f"EXPOSURE SUCCEEDED FOR {args.s3_zarr_store_uri}")
