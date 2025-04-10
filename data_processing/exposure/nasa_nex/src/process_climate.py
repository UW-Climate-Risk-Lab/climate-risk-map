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
) -> xr.Dataset:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        crs (str): Coordinate Refernce System of climate data
        bbox (dict): Dict with keys (min_lon, min_lat, max_lon, max_lat) to filter data
        time_dim (str): The name of the time dimension in the dataset
        climatology_mean_method (str): The method by which to average climate variable over time.
        derived_metadata_key (str): Keyname to store custom metadata in

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    ds = xr.open_dataset(
        s3_zarr_store_uri,
        engine="zarr"
    )

    # Converts 0-360 longitude to -180-180 longitude. In line with OpenStreetMap database
    ds = ds.assign_coords({constants.X_DIM: (((ds[constants.X_DIM] + 180) % 360) - 180)})
    ds = ds.sortby(constants.X_DIM)

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=constants.X_DIM, y_dim=constants.Y_DIM, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    return ds

if __name__ == "__main__":
    main()
