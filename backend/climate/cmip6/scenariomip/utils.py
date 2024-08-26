import boto3

from typing import List, Dict
from pathlib import Path


def get_state_bbox(state: str) -> Dict[str, float]:
    """Returns bbox of state

    Returned keys are:
    min_lon,
    min_lat,
    max_lon,
    max_lat

    Values in WGS84


    Args:
        state (str): _description_

    Returns:
        Dict[str, float]: Dict
    """
    if state is None:
        return None

    # Hard code for now
    # Source: https://observablehq.com/@rdmurphy/u-s-state-bounding-boxes
    data = {
        "washington": {
            "min_lon": -124.73364306703067,
            "min_lat": 45.54383071539715,
            "max_lon": -116.9161607504075,
            "max_lat": 49.00240502974029,
        }
    }
    try:
        response = data[state.lower()]
    except Exception as e:
        print(f"{state} bounding box not specified!")
        response = None
    return response


def create_s3_prefix(
    s3_base_prefix: str, climate_variable: str, ssp: int, file_group: str
) -> str:
    """Creates an S3 prefix based on the directory structure for CMIP6 ScenarioMIP Data


    Args:
        s3_base_prefix (str): Base prefix in S3 for CMIP6 ScenarioMIP data
        climate_variable (str): Climate Variable of interest
        ssp (int): Emissions scenario (e.g. 126, 245, etc..)
        file_group (str): "data" or "cogs"

    Returns:
        str: S3 Prefix
    """
    ssp = f"ssp{str(ssp)}"
    s3_prefix = Path(s3_base_prefix) / climate_variable / ssp / file_group
    return str(s3_prefix)


def download_files(s3_bucket: str, s3_prefix: str, dir: str) -> None:
    """Downloads all files in a give prefix to the directory

    Args:
        s3_bucket (str): AWS S3 Bucket
        s3_base_prefix (str): AWS S3 Prefix (should contain 1 or more files)
        climate_variable (str): Name of climate variable to download
        ssp (str): Scenario to download
        dir (str): Directory to save files to
    """
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    contents = response["Contents"][1:]

    for file in contents:
        prefix = file["Key"]
        download_file = (Path(dir) / prefix).name
        download_path = Path(dir) / download_file
        client.download_file(s3_bucket, prefix, str(download_path))


def upload_files(s3_bucket: str, s3_prefix: str, dir: str) -> None:
    """Uploads all files in the specified directory to the given S3 bucket and prefix

    Args:
        s3_bucket (str): AWS S3 Bucket
        s3_prefix (str): AWS S3 Prefix to upload the files to
        dir (str): Path to the directory containing files to upload
    """
    client = boto3.client("s3")
    directory = Path(dir)

    for file_path in directory.iterdir():
        if file_path.is_file():
            file_name = file_path.name
            s3_key = str(Path(s3_prefix) / file_name)
            client.upload_file(str(file_path), s3_bucket, s3_key)
