import xarray as xr
import numpy as np
import s3fs

from typing import Optional

# Assuming constants.py is accessible (adjust import path if needed)
import constants 
import file_utils

def get_historical_baseline(ds_hist: xr.Dataset, model: str, ensemble_member: str, bbox: Optional[dict] = None) -> xr.DataArray:
    """
    Loads and calculates the mean daily precipitation baseline for the historical period.
    
    NOTE: This is potentially inefficient if called repeatedly for each future year.
          Consider pre-calculating and storing baselines separately.
          This implementation calculates it on the fly.

    Args:
        model: Climate model name.
        ensemble_member: Ensemble member name.
        bbox: Optional bounding box dictionary {'x_min': ..., 'y_min': ...} for slicing.

    Returns:
        DataArray containing the mean daily precipitation over the baseline period.
    """
    print(f"Calculating precip historical baseline for {model}/{ensemble_member}...")

    if bbox:
         ds_hist = ds_hist.sel(lat=slice(bbox['y_min'], bbox['y_max']),
                               lon=slice(bbox['x_min'], bbox['x_max']))
                               
    # Calculate mean daily precipitation across the baseline years
    # Group by day of year to get climatology, then mean across years
    pr_baseline_daily_mean = ds_hist['pr'].groupby("time.dayofyear").mean(dim="time", skipna=True)
    
    # Add units back if lost, crucial for calculations and interpretation
    pr_baseline_daily_mean.attrs['units'] = ds_hist['pr'].attrs.get('units', 'kg m-2 s-1')
    pr_baseline_daily_mean.attrs['long_name'] = f"Mean daily precipitation ({constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]})"

    # Load into memory before closing dataset
    pr_baseline_loaded = pr_baseline_daily_mean.load()
    ds_hist.close()
    del ds_hist
    
    print("Finished calculating historical baseline.")
    return pr_baseline_loaded


def calculate_precip_percent_change(ds_input: xr.Dataset, 
                                      pr_baseline_mean: xr.DataArray,
                                      **kwargs) -> xr.Dataset:
    """
    Calculates the percentage change in daily precipitation relative to a baseline mean.

    Args:
        ds_input: Dataset containing the precipitation variable ('pr') for the target period.
                  Must be chunked appropriately.
        pr_baseline_mean: DataArray containing the mean daily precipitation baseline
                          (indexed by dayofyear).
        **kwargs: Catches unused arguments like initial_conditions.

    Returns:
        Dataset with the 'pr_change_percent' variable.
    """
    pr_target = ds_input['pr']

    # Ensure baseline has dayofyear coordinate for alignment
    if 'dayofyear' not in pr_baseline_mean.coords:
         raise ValueError("Baseline mean must have 'dayofyear' coordinate.")

    # Calculate percentage change: (target - baseline) / baseline * 100
    # Group target data by dayofyear to align with baseline
    pr_change = pr_target.groupby("time.dayofyear") - pr_baseline_mean
    
    # Avoid division by zero or near-zero baseline values (common in dry areas/seasons)
    # Where baseline is near zero, set change to NaN or a large number? NaN is safer.
    # Use a small threshold, e.g., 1e-6 mm/day equivalent (adjust based on units)
    # Convert kg m-2 s-1 to mm/day approx: value * 86400
    threshold = 1e-6 / 86400 # Approx 1e-6 mm/day in kg m-2 s-1

    pr_change_percent = xr.where(
        abs(pr_baseline_mean) > threshold,
        (pr_change / pr_baseline_mean) * 100.0,
        np.nan # Assign NaN where baseline is too small
    )

    # Ungroup to get back the original time dimension
    pr_change_percent = pr_change_percent.rename("pr_change_percent")
    
    # Set attributes
    pr_change_percent.attrs['units'] = '%'
    pr_change_percent.attrs['long_name'] = f"Percentage change in daily precipitation relative to {constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]} baseline"
    pr_change_percent.attrs['baseline_period'] = f"{constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]}"

    # Clean metadata
    pr_change_percent.attrs.pop("history", None)
    pr_change_percent.attrs.pop("cell_methods", None)
    
    ds_output = pr_change_percent.to_dataset()
    
    return ds_output