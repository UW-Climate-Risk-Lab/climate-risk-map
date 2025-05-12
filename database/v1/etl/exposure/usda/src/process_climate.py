import logging
from pathlib import Path
import re

import rioxarray
import xarray as xr
import fsspec
import s3fs

import src.constants as constants

from typing import List, Set

logger = logging.getLogger(__name__)

# Supresses info log when loading zarr data from s3:
# INFO:botocore.httpchecksum:Skipping checksum validation. Response did not contain one of the following algorithms: ['crc32', 'sha1', 'sha256']
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)

def main(
    s3_zarr_store_uri: str,
    crs: str,
    x_dim: str,
    y_dim: str,
    x_min: float,
    y_min: float,
    x_max: float,
    y_max: float,
) -> xr.Dataset:
    """Processes climate data

    Args:
        s3_zarr_store_uri (str): Directory to open file from
        crs (str): Coordinate Refernce System of climate data
        x_dim (str): Dimension name for longitude
        y_dim (str): Dimension name for latitude
        x_min (str): For bounding box, longitude minimum
        y_min (str): For bounding box, latitude minimum
        x_max (str): For bounding box, longitude maximum
        y_max (str): For bounding box, latitude maximum

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    ds = xr.open_dataset(
        s3_zarr_store_uri,
        engine="zarr"
    )


    # Converts 0-360 longitude to -180-180 longitude. In line with OpenStreetMap database
    ds = ds.assign_coords({x_dim: (((ds[x_dim] + 180) % 360) - 180)})
    ds = ds.sortby(x_dim)

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=constants.X_DIM, y_dim=constants.Y_DIM, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    ds = ds.sel({x_dim: slice(x_min, x_max), y_dim: slice(y_min, y_max)})

    ds = ds.compute()

    return ds

if __name__ == "__main__":
    main()
