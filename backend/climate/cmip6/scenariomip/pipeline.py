import os
import tempfile

import utils
import process_climate
import generate_geotiff
import infra_intersection

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
CONVERT_360_LON = bool(os.environ["CONVERT_360_LON"])
STATE_BBOX = os.environ.get("STATE_BBOX", None)
OSM_CATEGORY = os.environ["OSM_CATEGORY"]
OSM_TYPE=os.environ["OSM_TYPE"]
INTERSECTION_DEBUG = bool(os.environ["INTERSECTION_DEBUG"]) #TODO: REMOVE THIS 


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
        logger.info("Cliimate File Downloaded")

        ds = process_climate.main(
            file_directory=climate_tmpdir,
            xarray_engine=XARRAY_ENGINE,
            crs=CRS,
            x_dim=X_DIM,
            y_dim=Y_DIM,
            convert_360_lon=CONVERT_360_LON,
            bbox=utils.get_state_bbox(STATE_BBOX),
        )

        logger.info("Climate Data Processed")

    with tempfile.TemporaryDirectory() as geotiff_tmpdir:
        if not INTERSECTION_DEBUG:
            generate_geotiff.main(
                ds=ds,
                output_dir=geotiff_tmpdir,
                climate_variable=CLIMATE_VARIABLE,
                state=STATE_BBOX,
            )
            logger.info("Geotiffs created")
            utils.upload_files(
                s3_bucket=S3_BUCKET,
                s3_prefix=utils.create_s3_prefix(
                    S3_BASE_PREFIX, CLIMATE_VARIABLE, SSP, "cogs"
                ),
                dir=geotiff_tmpdir
            )
            logger.info("Geotiffs uploaded")
        
        # TODO: Compute infra intersection
        df = infra_intersection.main(ds=ds, osm_category=OSM_CATEGORY, osm_type=OSM_TYPE, crs=CRS)


        # TODO: Load infra intersection into database


if __name__ == "__main__":
    run()
