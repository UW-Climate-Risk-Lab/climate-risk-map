import os

import utils
import process_climate

SSP = (126, 245, 370, 585)


VARIABLE = os.environ["VARIABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_PREFIX"]
FILE_FORMAT = os.environ["FILE_FORMAT"]
CRS = os.environ["CRS"]


def run():
    for scenario in SSP:
        prefix = S3_PREFIX + "ssp" + str(scenario)
        s3_uris = utils.get_s3_uris(s3_bucket=S3_BUCKET, s3_prefix=prefix)

        climate_data = process_climate.main(
            s3_uris=s3_uris, file_format=FILE_FORMAT, crs=CRS
        )

    pass


if __name__ == "__main__":
    run()
