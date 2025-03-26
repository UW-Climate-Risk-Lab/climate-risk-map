import argparse
import logging

import pipeline
import constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
    parser.add_argument("--state-bbox", help="State bounding box", required=False)
    parser.add_argument("--osm-category", required=True, help="OSM category")
    parser.add_argument("--osm-type", required=True, help="OSM type")
    return parser.parse_args()


if __name__ == "__main__":
    args = setup_args()
    logger.info(f"STARTING PIPELINE FOR {args.s3_zarr_store_uri}")
    pipeline.main(
        s3_zarr_store_uri=args.s3_zarr_store_uri,
        climate_variable=args.climate_variable,
        ssp=args.ssp,
        crs=args.crs,
        zonal_agg_method=args.zonal_agg_method,
        state_bbox=args.state_bbox,
        osm_category=args.osm_category,
        osm_type=args.osm_type,
    )
    logger.info(f"PIPELINE SUCCEEDED FOR {args.s3_zarr_store_uri}")
