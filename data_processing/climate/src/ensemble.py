import logging
import re
import os
import multiprocessing
from pathlib import PurePosixPath
from distributed import Client, LocalCluster

import xarray as xr
import fsspec
import s3fs

from typing import List, Set

import src.constants as constants
from src.file_utils import s3_uri_exists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Supresses info log when loading zarr data from s3:
# INFO:botocore.httpchecksum:Skipping checksum validation. Response did not contain one of the following algorithms: ['crc32', 'sha1', 'sha256']
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)

# Add constants for validation. Models used must have all available scenarios and all available years
HISTORICAL_YEARS = set(range(1950, 2015))  # 1950-2014
FUTURE_YEARS = set(range(2015, 2101))  # 2015-2100


def validate_model_scenario(fs: s3fs.S3FileSystem, model_path: str, scenario: str) -> bool:
    """Check if model has required scenario"""

    # Handle regular SSP cases
    return fs.exists(f"{model_path}/{scenario}")


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
            "ensemble_mean": mean,
            "ensemble_median": median,
            "ensemble_stddev": stddev,
            "ensemble_min": min_val,
            "ensemble_max": max_val,
            "ensemble_q1": q1,  # quartiles.sel(quantile=0.25).drop("quantile"),
            "ensemble_q3": q3,  # quartiles.sel(quantile=0.75).drop("quantile"),
        },
        attrs=da.attrs,  # Copy original attributes, if any
    )
    stats_ds.attrs["sample_size"] = sample_size
    return stats_ds


def load_data(
    s3_bucket: str,
    s3_prefix: str,
    scenario: str,
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
        if not validate_model_scenario(fs, model_path, scenario):
            logger.warning(f"Skipping {model_name}: missing required scenario")
            continue

        model_pattern = f"{model_path}/{scenario}/*/indicators_month_*.zarr"

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

        _da = _da.assign_coords(model=model_name)
        _da = _da.expand_dims("model")
        data.append(_da)

        logger.info(f"{model_name} loaded")

    da = xr.combine_nested(data, concat_dim=["model"])
    da = da.assign_attrs(ensemble_members=da.model.values)

    chunks = {
        "decade_month": 12, 
        "lon": "auto",
        "lat": "auto",
        "model": -1,  # All models in one chunk for ensemble calculations
    }

    da = da.chunk(chunks)

    return da


def main(scenario: str, s3_bucket: str, s3_prefix: str, climate_variable: str) -> None:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        bbox (dict): Dict with keys (min_lon, min_lat, max_lon, max_lat) to filter data
        time_dim (str): The name of the time dimension in the dataset
        climatology_mean_method (str): The method by which to average climate variable over time.
        derived_metadata_key (str): Keyname to store custom metadata in

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    s3_output_path = PurePosixPath(
        s3_bucket,
        s3_prefix,
        "ENSEMBLE",
        scenario
    )
    s3_output_zarr = f"{climate_variable}_decade_month_{scenario}.zarr"
    s3_output_uri = f"s3://{s3_output_path / s3_output_zarr}"

    if s3_uri_exists(s3_output_uri):
        print(f"ENSEMBLE: {s3_output_uri} exists, skipping")
        return None

    n_workers = min(multiprocessing.cpu_count(), 24)
    memory_limit_gb = int(constants.DEFAULT_MEMORY) / n_workers
    threads_per_worker = int(constants.DEFAULT_THREADS)

    print(
        f"Setting up Dask LocalCluster: {n_workers} workers, {threads_per_worker} threads/worker, {memory_limit_gb}GB total memory limit"
    )
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=f"{memory_limit_gb}GB",  # Dask's per-worker limit interpretation varies, sometimes it's total. Check docs.
        # Consider setting interface, dashboard_address appropriately if needed
        # interface='eth0', # Example for AWS Batch multi-node networking
        # dashboard_address=':8787'
    )
    client = Client(cluster)
    print(f"Dask client ready: {client}")
    print(f"Dashboard link: {client.dashboard_link}")

    da = load_data(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        scenario=scenario,
        climate_variable=climate_variable,
    )

    ds = reduce_model_stats(da)

    logger.info("Xarray dataset created")

    logger.info(f"STARTING PIPELINE FOR {scenario}")
    ds = main(
        scenario=scenario,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        climate_variable=climate_variable,
    )

    try:
        fs = s3fs.S3FileSystem(
            anon=False,
            )
        # Let to_zarr() handle the computation
        ds.to_zarr(
            store=s3fs.S3Map(root=s3_output_uri, s3=fs),
            mode="w",
            consolidated=True,
        )

    except Exception as e:
        print(f"Error writing to s3: {str(e)}")
        raise ValueError
    logger.info(f"PIPELINE SUCCEEDED FOR SSP {scenario}")

    return None
