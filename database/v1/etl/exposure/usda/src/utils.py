import io
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import boto3
import numpy as np
import pandas as pd
import psycopg2 as pg
import psycopg2.sql as sql
import xarray as xr
import geopandas as gpd

import src.constants as constants

def str_to_bool(s):
    return s.lower() in ['true', '1', 't', 'y', 'yes']

def get_state_geometry(state: str) -> gpd.GeoDataFrame:

    url = f"https://raw.githubusercontent.com/glynnbird/usstatesgeojson/master/{state}.geojson"
    gdf = gpd.read_file(url)
    return gdf


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
            "min_lon": -125.1,
            "min_lat": 45.5,
            "max_lon": -115.9,
            "max_lat": 49.1,
        }
    }
    try:
        response = data[state.lower()]
    except Exception as e:
        print(f"{state} bounding box not specified!")
        response = None
    return response


def create_s3_prefix(
    s3_base_prefix: str, usda_variable: str, ssp: int, file_group: str
) -> str:
    """Creates an S3 prefix based on the directory structure for CMIP6 ScenarioMIP Data


    Args:
        s3_base_prefix (str): Base prefix in S3 for CMIP6 ScenarioMIP data
        usda_variable (str): Climate Variable of interest
        ssp (int): Emissions scenario (e.g. 126, 245, etc..)
        file_group (str): "data" or "cogs"

    Returns:
        str: S3 Prefix
    """
    ssp = f"ssp{str(ssp)}"
    s3_prefix = Path(s3_base_prefix) / usda_variable / ssp / file_group
    return str(s3_prefix)


def download_files(s3_bucket: str, s3_prefix: str, dir: str) -> None:
    """Downloads all files in a give prefix to the directory

    Args:
        s3_bucket (str): AWS S3 Bucket
        s3_base_prefix (str): AWS S3 Prefix (should contain 1 or more files)
        usda_variable (str): Name of climate variable to download
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


def query_db(query: sql.SQL, conn: pg.extensions.connection, params: Tuple[str] = None):
    """Executs database query"""
    with conn.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchall()
    return result


def copy_df_db(query: sql.SQL, df: pd.DataFrame, conn: pg.extensions.connection):
    """Reads pandas dataframe and copies directly to table in query"""

    sio = io.StringIO()
    sio.write(df.to_csv(index=False, header=False))
    sio.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(query, sio)


def get_osm_category_tables(
    osm_category: str, conn: pg.extensions.connection
) -> List[str]:
    """
    Returns DB tables

    This assumes you are querying a database set up with PG OSM Flex
    """

    query = sql.SQL("SELECT tablename FROM pg_tables WHERE schemaname='osm'")

    all_tables = query_db(query=query, conn=conn, params=None)

    # Table name always starts with category
    tables = []
    for table in all_tables:
        tablename = table[0]
        name_match = re.findall(osm_category, tablename)
        if name_match:
            tables.append(tablename)

    return tables


def convert_to_serializable(value: Any) -> Any:
    """Converts a value to a JSON serializable type."""
    if isinstance(value, (np.integer)):
        return int(value)
    elif isinstance(value, (np.floating)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, bytes):
        return value.decode("utf-8")
    else:
        return value
    
def validate_and_convert_coords(x_min, x_max, y_min, y_max):
    x_min = float(x_min)
    x_max = float(x_max)
    y_min = float(y_min)
    y_max = float(y_max)

    if not (-180 <= x_min <= 180):
        raise ValueError("x_min must be between -180 and 180")
    if not (-180 <= x_max <= 180):
        raise ValueError("x_max must be between -180 and 180")
    if not (-90 <= y_min <= 90):
        raise ValueError("y_min must be between -90 and 90")
    if not (-90 <= y_max <= 90):
        raise ValueError("y_max must be between -90 and 90")

    return x_min, x_max, y_min, y_max


def create_metadata(
    ds: xr.Dataset
) -> Dict:
    """Creates json metadata and summary metrics for
    frontend

    Args:
        ds (xr.Dataset): Processed Climate Dataset
        derived_metadata_key (str): Metadata key for any metadata derived during the process
        usda_variable (str): Specific variable being processed in pipeline

    Returns:
        Dict: Dict with metadata
    """
    metadata = {key: convert_to_serializable(value) for key, value in ds.attrs.items()}

    metadata = {
        key: convert_to_serializable(value)
        for key, value in ds.attrs.items()
    }

    # Add any additional useful metadeta to the key UW_CRL_DERIVED
    metadata[constants.METADATA_KEY] = {}

    return metadata
