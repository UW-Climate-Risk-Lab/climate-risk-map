import logging
import re
import os
import multiprocessing
from pathlib import PurePosixPath

import xarray as xr
import fsspec
import s3fs

from typing import List, Set

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supresses info log when loading zarr data from s3:
# INFO:botocore.httpchecksum:Skipping checksum validation. Response did not contain one of the following algorithms: ['crc32', 'sha1', 'sha256']
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)

# Add constants for validation. Models used must have all available scenarios and all available years
HISTORICAL_YEARS = set(range(1950, 2015))  # 1950-2014
FUTURE_YEARS = set(range(2015, 2101))  # 2015-2100


def validate_model_ssp(fs: s3fs.S3FileSystem, model_path: str, ssp: str) -> bool:
    """Check if model has required SSP"""
    
    # Handle historical case
    if ssp == '-999':
        return fs.exists(f"{model_path}/historical")
    
    # Handle regular SSP cases
    return fs.exists(f"{model_path}/ssp{ssp}")


def validate_model_years(fs: s3fs.S3FileSystem, zarr_stores: List[str]) -> bool:
    """Check if model has all required years"""
    available_years = set()

    for store in zarr_stores:
        # Extract year from zarr store path
        year_match = re.search(r"_(\d{4})\.zarr$", store)
        if year_match:
            available_years.add(int(year_match.group(1)))

    required_years = (HISTORICAL_YEARS == available_years) or (
        FUTURE_YEARS == available_years
    )

    return required_years


def decade_month_calc(ds: xr.Dataset, time_dim: str = "time") -> xr.Dataset:
    """Calculates the climatological mean by decade and month.

    This function computes the decade-by-decade average for each month in the provided dataset.
    The process involves averaging values across each decade for each month separately.
    For instance, for the 2050s, the function calculates the average values for January, February,
    March, and so on, resulting in 12 averaged values corresponding to each month of the 2050s.
    This approach preserves seasonal variability while smoothing out interannual variability
    within each decade.

    The function performs the following steps:
    1. Assigns new coordinates to the dataset:
       - `decade`: Represents the decade (e.g., 2050 for the 2050s).
       - `month`: Represents the month (1 for January, 2 for February, etc.).
    2. Creates a combined `decade_month` coordinate, formatted as "YYYY-MM",
       where "YYYY" is the starting year of the decade, and "MM" is the month.
    3. Groups the dataset by the `decade_mon
    """
    ds = ds.assign_coords(
        decade=(ds["time.year"] // 10) * 10, month=ds["time"].dt.month
    )

    ds = ds.assign_coords(
        decade_month=(
            time_dim,
            [
                f"{decade}-{month:02d}"
                for decade, month in zip(ds["decade"].values, ds["month"].values)
            ],
        )
    )

    ds = ds.groupby("decade_month").mean()

    return ds


def reduce_model_stats(da: xr.DataArray) -> xr.Dataset:
    """
    Reduces a DataArray by computing statistical metrics (mean, median, stddev, etc.)
    across the 'model' dimension. This creates the climate ensemble mean

    Args:
        da (xr.DataArray): Input DataArray with a 'model' dimension.

    Returns:
        xr.Dataset: Dataset containing statistical metrics as variables.
    """
    # Compute metrics
    mean = da.mean(dim="model")
    median = da.median(dim="model")
    stddev = da.std(dim="model")
    min_val = da.min(dim="model")
    max_val = da.max(dim="model")
    q1 = da.quantile(0.25, dim="model").drop("quantile")
    q3 = da.quantile(0.75, dim="model").drop("quantile")
    sample_size = len(
        da.attrs.get("ensemble_members", [])
    )  # Number of climate models used when calculating stats

    # Create a new Dataset with the computed statistics
    stats_ds = xr.Dataset(
        {
            "value_mean": mean,
            "value_median": median,
            "value_stddev": stddev,
            "value_min": min_val,
            "value_max": max_val,
            "value_q1": q1,#quartiles.sel(quantile=0.25).drop("quantile"),
            "value_q3": q3,#quartiles.sel(quantile=0.75).drop("quantile"),
        },
        attrs=da.attrs,  # Copy original attributes, if any
    )
    stats_ds.attrs["sample_size"] = sample_size
    return stats_ds


def load_data(
    s3_bucket: str,
    s3_prefix: str,
    ssp: str,
    climate_variable: str,
) -> xr.DataArray:
    """Reads all valid Zarr stores in the given S3 directory"""
    data = []

    fs = s3fs.S3FileSystem()
    pattern = f"s3://{s3_bucket}/{s3_prefix}/*"
    model_paths = fs.glob(pattern)


    for model_path in model_paths:
        if "ENSEMBLE" in model_path:
            continue
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
        _da = _da.assign_coords({"lon": (((_da["lon"] + 180) % 360) - 180)})
        _da = _da.sortby("lon")

        _da = _da.assign_coords(model=model_name)
        _da = _da.expand_dims("model")
        data.append(_da)

        logger.info(f"{model_name} loaded")
    
    da = xr.combine_nested(data, concat_dim=["model"])
    da = da.assign_attrs(ensemble_members=da.model.values)

    chunks = {
        "decade_month": 12,  # All in one chunk since it's usually small
        "lon": "auto",
        "lat": "auto",
        "model": -1  # All models in one chunk for ensemble calculations
    }

    da = da.chunk(chunks)

    return da


def main(
    ssp: str,
    s3_bucket: str,
    s3_prefix: str,
    climate_variable: str
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

    da = load_data(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        ssp=ssp,
        climate_variable=climate_variable,
    )

    ds = reduce_model_stats(da)

    logger.info("Xarray dataset created")

    return ds


if __name__ == "__main__":
    
    ssps = [126, 245, 370, 585, -999]
    climate_variable = 'fwi'
    s3_prefix = "climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6"
    s3_bucket = os.environ["S3_BUCKET"]

    for ssp in [str(ssp) for ssp in ssps]:
        logger.info(f"STARTING PIPELINE FOR SSP {ssp}")
        ds = main(
            ssp=ssp,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix,
            climate_variable=climate_variable,
        )
        s3_output_uri = PurePosixPath(s3_bucket,s3_prefix,"DECADE_MONTH_ENSEMBLE",'historical' if ssp==-999 else f"ssp{ssp}",)
        try:
            fs = s3fs.S3FileSystem(anon=False)
            # Let to_zarr() handle the computation
            ds.to_zarr(
                store=s3fs.S3Map(root=s3_output_uri, s3=fs),
                mode="w",
                consolidated=True,
            )

        except Exception as e:
            print(f"Error writing to s3: {str(e)}")
            raise ValueError
        logger.info(f"PIPELINE SUCCEEDED FOR SSP {ssp}")
