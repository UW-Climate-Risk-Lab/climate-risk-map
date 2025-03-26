import logging
from pathlib import Path
import re

import rioxarray
import xarray as xr
import fsspec
import s3fs

import constants

from typing import List, Set

logger = logging.getLogger(__name__)

# Supresses info log when loading zarr data from s3:
# INFO:botocore.httpchecksum:Skipping checksum validation. Response did not contain one of the following algorithms: ['crc32', 'sha1', 'sha256']
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)


def load_data(
    s3_zarr_store_uri: str,
    ssp: str,
    climate_variable: str,
    bbox: dict,
) -> xr.DataArray:
    """Reads all valid Zarr stores in the given S3 directory"""
    data = []

    fs = s3fs.S3FileSystem()
    pattern = f"s3://{s3_bucket}/{s3_prefix}/*"
    model_paths = fs.glob(pattern)


    for model_path in model_paths:
        model_name = model_path.rstrip("/").split("/")[-1]
        logger.info(f"Validating model: {model_name}")

        # Check if model has all required SSPs
        if not validate_model_ssp(fs, model_path, ssp):
            logger.warning(f"Skipping {model_name}: missing required SSP")
            continue

        # Get all zarr stores for this model and SSP
        if ssp == '-999':
            model_pattern = f"{model_path}/historical/*/{climate_variable}_day_*.zarr"
        else:
            model_pattern = f"{model_path}/ssp{ssp}/*/{climate_variable}_day_*.zarr"

        zarr_stores = fs.glob(model_pattern)

        # Check if model has all required years
        if not validate_model_years(fs, zarr_stores):
            logger.warning(f"Skipping {model_name}: missing required years")
            continue

        # Convert to full S3 URIs
        zarr_uris = [f"s3://{path}" for path in zarr_stores]

        logger.info(f"Loading validated model: {model_name}")
        _ds = xr.open_mfdataset(
            zarr_uris,
            engine="zarr",
            combine="by_coords",
            parallel=True,
            preprocess=decade_month_calc,
        )
        _da = _ds[climate_variable]
        _da = _da.assign_coords({constants.X_DIM: (((_da[constants.X_DIM] + 180) % 360) - 180)})
        _da = _da.sortby(constants.X_DIM)

        # Bbox currently only in -180-180 lon
        # TODO: Add better error and case handling
        if bbox:
            _da = _da.sel(
                {
                    constants.Y_DIM: slice(bbox["min_lat"], bbox["max_lat"]),
                    constants.X_DIM: slice(bbox["min_lon"], bbox["max_lon"]),
                },
            )
        _da = _da.assign_coords(model=model_name)
        _da = _da.expand_dims("model")
        data.append(_da)

        logger.info(f"{model_name} loaded")
    
    da = xr.combine_nested(data, concat_dim=["model"])
    da = da.assign_attrs(ensemble_members=da.model.values)

    chunks = {
        "decade_month": 12,  # All in one chunk since it's usually small
        constants.X_DIM: "auto",
        constants.Y_DIM: "auto",
        "model": -1  # All models in one chunk for ensemble calculations
    }

    da = da.chunk(chunks)

    return da


def main(
    s3_zarr_store_uri: str,
    climate_variable: str,
    ssp: str,
    crs: str,
    bbox: dict,
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

    ds = load_data(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        ssp=ssp,
        climate_variable=climate_variable,
        bbox=bbox,
    )

    logger.info("Xarray dataset created")

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=constants.X_DIM, y_dim=constants.Y_DIM, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    

    return ds


if __name__ == "__main__":
    main()
