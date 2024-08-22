import os
import tempfile

import utils
import process_climate


VARIABLE = os.environ["VARIABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
S3_BASE_PREFIX = os.environ["S3_PREFIX"]
FILE_FORMAT = os.environ["FILE_FORMAT"]
CRS = os.environ["CRS"]
SSP = os.environ["SSP"]

def run():
    with tempfile.TemporaryDirectory() as tmpdir:
        prefix = S3_BASE_PREFIX + "ssp" + str(SSP)
        utils.download_files(s3_bucket=S3_BUCKET, s3_prefix=prefix, dir=tmpdir)



if __name__ == "__main__":
    run()
