import boto3

from typing import List
from pathlib import Path

def download_files(s3_bucket:str, s3_prefix: str, dir: str) -> None:
    """Downloads all files in a give prefix to the directory

    Args:
        s3_bucket (str): AWS S3 Bucket
        s3_prefix (str): AWS S3 Prefix (should contain 1 or more files)
        dir (str): Directory to save files to
    """
    client = boto3.client('s3')
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    contents = response["Contents"][1:]

    for file in contents:
        prefix = file["Key"]
        download_file = (Path(dir) / prefix).name
        download_path = (Path(dir) / download_file)
        client.download_file(s3_bucket, prefix, str(download_path))