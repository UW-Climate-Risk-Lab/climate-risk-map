import os
import tempfile

import utils
import process_climate
import generate_geotiff


S3_BUCKET = os.environ["S3_BUCKET"]
S3_BASE_PREFIX = os.environ["S3_BASE_PREFIX"]
CLIMATE_VARIABLE = os.environ["CLIMATE_VARIABLE"]
SSP = os.environ["SSP"]
XARRAY_ENGINE = os.environ["XARRAY_ENGINE"]
CRS = os.environ["CRS"]
X_DIM = os.environ["X_DIM"]
Y_DIM = os.environ["Y_DIM"]
CONVERT_360_LON = bool(os.environ["CONVERT_360_LON"])


def run():
    """Runs a processing pipeline for a given climate variable
    and SSP
    """
    with tempfile.TemporaryDirectory() as climate_tmpdir:
        utils.download_files(
            s3_bucket=S3_BUCKET,
            s3_base_prefix=S3_BASE_PREFIX,
            climate_variable=CLIMATE_VARIABLE,
            ssp=str(SSP),
            dir=climate_tmpdir,
        )

        ds = process_climate.main(
            file_directory=climate_tmpdir,
            xarray_engine=XARRAY_ENGINE,
            crs=CRS,
            x_dim=X_DIM,
            y_dim=Y_DIM,
            convert_360_lon=CONVERT_360_LON
        )
    with tempfile.TemporaryDirectory() as geotiff_tmpdir:
        # TODO: Generate COGs
        generate_geotiff.main(
            ds=ds, output_dir=geotiff_tmpdir, climate_variable=CLIMATE_VARIABLE
        )

        # TODO: Compute infra intersection

        # TODO: Load infra intersection into database


if __name__ == "__main__":
    run()
