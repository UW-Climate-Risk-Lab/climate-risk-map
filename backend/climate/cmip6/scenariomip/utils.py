import boto3

from typing import List
from pathlib import Path


def download_files(
    s3_bucket: str, s3_base_prefix: str, climate_variable: str, ssp: str, dir: str
) -> None:
    """Downloads all files in a give prefix to the directory

    Args:
        s3_bucket (str): AWS S3 Bucket
        s3_base_prefix (str): AWS S3 Prefix (should contain 1 or more files)
        climate_variable (str): Name of climate variable to download
        ssp (str): Scenario to download
        dir (str): Directory to save files to
    """
    client = boto3.client("s3")
    ssp = f"ssp{ssp}"

    s3_prefix = Path(s3_base_prefix) / climate_variable / ssp 
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=str(s3_prefix))
    contents = response["Contents"][1:]

    for file in contents:
        prefix = file["Key"]
        download_file = (Path(dir) / prefix).name
        download_path = Path(dir) / download_file
        client.download_file(s3_bucket, prefix, str(download_path))
