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

def rename_value_variables(ds: xr.Dataset) -> xr.Dataset:
    """
    Renames any data variables starting with 'value_' to start with 'ensemble_' instead.
    
    Args:
        ds (xr.Dataset): Input dataset with variables to rename
        
    Returns:
        xr.Dataset: Dataset with renamed variables
    """
    # Find all variables starting with 'value_'
    value_vars = [var for var in ds.data_vars if var.startswith('value_')]
    
    if not value_vars:
        logger.info("No variables found starting with 'value_'")
        return ds
    
    # Create a rename mapping dictionary
    rename_dict = {var: f"ensemble_{var[6:]}" for var in value_vars}
    
    # Rename the variables
    return ds.rename(rename_dict)

def main(
    s3_zarr_store_uri: str,
    crs: str,
    x_dim: str,
    y_dim: str,
    x_min: float,
    y_min: float,
    x_max: float,
    y_max: float,
    return_period: int = None,
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
        return_period (int, optional): Return period in years to filter by. Defaults to None.

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

    # ------------------------------------------------------------------
    # Early exit if the spatial subset contains no data
    # This can happen if the user supplies a bounding box that lies
    # completely outside the extent of the climate dataset. Proceeding
    # with an empty Dataset will lead to downstream errors during the
    # infrastructure intersection step (e.g., KeyError for missing
    # coordinates). Detect the situation early and return ``None`` so
    # the calling code can terminate gracefully.
    # ------------------------------------------------------------------
    if (ds[x_dim].size == 0) or (ds[y_dim].size == 0):
        logger.warning(
            "No climate data found within the provided bounding box. "
            "Skipping further processing for this dataset."
        )
        return None

    # Filter by return period if provided
    if return_period is not None and 'return_period' in ds.dims:
        logger.info(f"Filtering dataset by return period: {return_period} years.")
        ds = ds.sel(return_period=return_period)
        # Drop the now-scalar coordinate
        if 'return_period' in ds.coords:
            ds = ds.drop_vars('return_period')
    
    # Ensemble count is not currently used in the database, should be stored in metadata
    if "ensemble_count" in ds.data_vars:
        ds = ds.drop_vars("ensemble_count")

    # Rename variables from value_* to ensemble_*
    ds = rename_value_variables(ds)

    ds = ds.compute()

    return ds

if __name__ == "__main__":
    main()
