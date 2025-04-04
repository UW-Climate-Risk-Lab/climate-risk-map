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
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]

def setup_args():
    parser = argparse.ArgumentParser(description="Process climate data for a given SSP")

    parser.add_argument("--s3-zarr-store-uri", required=True, help="S3 URI to zarr store containing climate dataset ")
    parser.add_argument(
        "--climate-variable", required=True, help="Climate variable to process in zarr store"
    )
    parser.add_argument("--crs", required=True, help="Coordinate reference system")
    parser.add_argument("--ssp", required=True, help="SSP of climate hazard data")
    parser.add_argument(
        "--zonal-agg-method", required=True, help="Zonal aggregation method"
    )
    parser.add_argument("--osm-category", required=True, help="OSM category")
    parser.add_argument("--osm-type", required=True, help="OSM type")
    parser.add_argument("--osm-subtype", required=False, help="OSM subtype")
    parser.add_argument("--point-only", required=False, help="Convert all geometries to points for zonal aggregation speed up")
    return parser.parse_args()


def main(
    s3_zarr_store_uri: str,
    climate_variable: str,
    ssp: str,
    crs: str,
    point_only: str,
    zonal_agg_method: str,
    osm_category: str,
    osm_type: str,
    osm_subtype: str
):
    """Runs a processing pipeline for a given zarr store"""

    if point_only.lower() == "true":
        point_only=True
    else:
        point_only=False

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
        crs=crs,
    )

    metadata = utils.create_metadata(
        ds=ds
    )

    metadata[constants.METADATA_KEY]["zonal_agg_method"] = zonal_agg_method

    logger.info("Climate Data Loaded & Processed")

    infra_intersection_conn = connection_pool.getconn()
    df = infra_intersection.main(
        climate_ds=ds,
        osm_category=osm_category,
        osm_type=osm_type,
        osm_subtype=osm_subtype,
        crs=crs,
        point_only=point_only,
        zonal_agg_method=zonal_agg_method,
        conn=infra_intersection_conn,
        metadata=metadata
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
        crs=args.crs,
        zonal_agg_method=args.zonal_agg_method,
        osm_category=args.osm_category,
        osm_type=args.osm_type,
        osm_subtype=args.osm_subtype,
        point_only=args.point_only
    )
    logger.info(f"EXPOSURE SUCCEEDED FOR {args.s3_zarr_store_uri}")
