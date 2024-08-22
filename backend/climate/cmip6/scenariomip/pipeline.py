import os
import tempfile

import utils
import process_climate


S3_BUCKET = os.environ["S3_BUCKET"]
S3_BASE_PREFIX = os.environ["S3_BASE_PREFIX"]
VARIABLE = os.environ["VARIABLE"]
SSP = os.environ["SSP"]
XARRAY_ENGINE = os.environ["XARRAY_ENGINE"]
CRS = os.environ["CRS"]


def run():
    with tempfile.TemporaryDirectory() as tmpdir:
        utils.download_files(
            s3_bucket=S3_BUCKET,
            s3_base_prefix=S3_BASE_PREFIX,
            climate_variable=VARIABLE,
            ssp=str(SSP),
            dir=tmpdir,
        )
        ds = process_climate.main(
            file_directory=tmpdir, xarray_engine=XARRAY_ENGINE, crs=CRS
        )


if __name__ == "__main__":
    run()
