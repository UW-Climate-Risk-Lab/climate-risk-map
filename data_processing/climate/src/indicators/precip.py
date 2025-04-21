import xarray as xr
import numpy as np
import s3fs

from typing import Optional

import src.constants as constants


def get_historical_baseline(
    ds_hist: xr.Dataset, model: str, ensemble_member: str, bbox: Optional[dict] = None
) -> xr.DataArray:
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
        ds_hist = ds_hist.sel(
            lat=slice(bbox["y_min"], bbox["y_max"]),
            lon=slice(bbox["x_min"], bbox["x_max"]),
        )

    # Calculate mean daily precipitation across the baseline years
    # Group by day of year to get climatology, then mean across years
    pr_baseline_daily_mean = (
        ds_hist["pr"].groupby("time.dayofyear").mean(dim="time", skipna=True)
    )

    # Add units back if lost, crucial for calculations and interpretation
    pr_baseline_daily_mean.attrs["units"] = ds_hist["pr"].attrs.get(
        "units", "kg m-2 s-1"
    )
    pr_baseline_daily_mean.attrs["long_name"] = (
        f"Mean daily precipitation ({constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]})"
    )

    # Load into memory before closing dataset
    pr_baseline_loaded = pr_baseline_daily_mean.load()
    ds_hist.close()
    del ds_hist

    print("Finished calculating historical baseline.")
    return pr_baseline_loaded


def calculate_precip_percent_change(
    ds_input: xr.Dataset, pr_baseline_mean: xr.DataArray
) -> xr.Dataset:
    """
    Calculates the percentage change in daily precipitation relative to a baseline mean.

    Args:
        ds_input: Dataset containing the precipitation variable ('pr') for the target period.
                  Must be chunked appropriately (e.g., {'time': N, 'lat': M, 'lon': L}).
        pr_baseline_mean: DataArray containing the mean daily precipitation baseline
                          (indexed by 'dayofyear', dims typically ('dayofyear', 'lat', 'lon')).
        **kwargs: Catches unused arguments like initial_conditions.

    Returns:
        Dataset with the 'pr_percent_change' variable, retaining only
        ('time', 'lat', 'lon') dimensions from ds_input.
    """
    ds_input_pr = ds_input["pr"].copy()  # Should have dims (time, lat, lon)

    # --- Input Validation ---
    if "dayofyear" not in pr_baseline_mean.coords:
        raise ValueError("Baseline mean must have 'dayofyear' coordinate.")
    if not all(dim in ds_input_pr.dims for dim in ["time", "lat", "lon"]):
        raise ValueError(
            "Input 'ds_input_pr' must have 'time', 'lat', 'lon' dimensions."
        )
    if not all(dim in pr_baseline_mean.dims for dim in ["dayofyear", "lat", "lon"]):
        # Allow for baseline potentially lacking spatial dims if it's global mean, though unlikely here
        if not all(dim in pr_baseline_mean.dims for dim in ["dayofyear"]):
            raise ValueError(
                "Input 'pr_baseline_mean' must have at least 'dayofyear' dimension."
            )

    # --- Step 1: Align Baseline to Target Time Dimension ---
    print("Aligning baseline to target time dimension...")
    # This is the crucial step. We use .sel() with the dayofyear coordinate
    # derived from the target's time coordinate. This operation maps the
    # baseline value for the correct dayofyear onto each timestamp in the target.
    # The result, baseline_aligned_to_target, should have the same dimensions
    # and coordinates as ds_input_pr: (time, lat, lon).
    try:
        # Ensure the dayofyear coordinate exists on the target array
        if "dayofyear" not in ds_input_pr.coords:
            # If running on a dataset loaded without decoding cf times fully, dayofyear might be missing.
            # Attempt to calculate it if time is present.
            if "time" in ds_input_pr.coords and np.issubdtype(
                ds_input_pr.time.dtype, np.datetime64
            ):
                ds_input_pr["dayofyear"] = ds_input_pr["time.dayofyear"]
                print("Calculated 'dayofyear' coordinate for ds_input_pr.")
            else:
                raise ValueError(
                    "ds_input_pr is missing 'dayofyear' coordinate and 'time' is not datetime64."
                )

        baseline_aligned_to_input = pr_baseline_mean.sel(
            dayofyear=ds_input_pr["dayofyear"]
        )

        # Check dimensions explicitly after selection
        if baseline_aligned_to_input.dims != ds_input_pr.dims:
            raise ValueError(
                f"Dimension mismatch after baseline alignment. "
                f"Expected {ds_input_pr.dims}, got {baseline_aligned_to_input.dims}"
            )
        print("Baseline aligned successfully.")

    except Exception as e:
        print(f"Error during baseline alignment: {e}")
        print("Details:")
        print(
            f"ds_input_pr dims: {ds_input_pr.dims}, coords: {list(ds_input_pr.coords)}"
        )
        print(
            f"pr_baseline_mean dims: {pr_baseline_mean.dims}, coords: {list(pr_baseline_mean.coords)}"
        )
        # Optionally re-raise or handle differently
        raise

    # --- Step 2: Perform Element-wise Calculation ---
    # Now both arrays (ds_input_pr, baseline_aligned_to_target) have identical
    # dimensions and coordinates, allowing for direct element-wise operations.
    print("Calculating percentage change...")

    # Calculate percentage change where baseline is sufficiently large
    pr_percent_change = (
        (ds_input_pr - baseline_aligned_to_input) / baseline_aligned_to_input
    ) * 100
    print("Percentage change calculation complete.")

    # --- Step 3: Prepare Output Dataset ---
    # Rename the output DataArray
    pr_percent_change = pr_percent_change.rename("pr_percent_change")

    # Set standard attributes
    pr_percent_change.attrs["units"] = "%"
    pr_percent_change.attrs["long_name"] = (
        f"Percentage change in daily precipitation relative to "
        f"{constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]} baseline"
    )
    pr_percent_change.attrs["baseline_period"] = (
        f"{constants.HISTORICAL_BASELINE_YEARS[0]}-{constants.HISTORICAL_BASELINE_YEARS[-1]}"
    )

    # Preserve input variable attributes if desired (e.g., grid mapping)
    for attr_name, attr_val in ds_input_pr.attrs.items():
        if attr_name not in pr_percent_change.attrs:  # Avoid overwriting specific attrs
            pr_percent_change.attrs[attr_name] = attr_val

    pr_percent_change = pr_percent_change.reset_coords(names="dayofyear", drop=True)

    # Create the output dataset
    ds_output = pr_percent_change.to_dataset()

    # Final check on estimated size (only works well if data is loaded/computed)
    # print(f"Estimated size of output dataset in memory: {ds_output.nbytes / 1e9:.3f} GB")

    return ds_output
