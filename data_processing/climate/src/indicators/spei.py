import xarray as xr
import numpy as np
import pandas as pd
import xclim.core.units as xcu
import xclim.indices as xci

import warnings
import src.indicators.climate_indices_fork as climate_indices_fork


# --- Helper Function for Unit Conversion ---
def ensure_correct_units(ds: xr.Dataset):
    """Converts units if necessary and ensures required variables exist."""
    required_vars = ["pr", "tasmin", "tasmax"]
    for var in required_vars:
        if var not in ds:
            raise ValueError(f"Variable '{var}' not found in the dataset.")

    # Convert temperature from Kelvin to Celsius if needed
    if ds["tasmin"].attrs.get("units", "").lower() in ["k", "kelvin"]:
        ds["tasmin"] = xcu.convert_units_to(ds["tasmin"], "degC")
        ds["tasmin"].attrs["units"] = "degC"
    if ds["tasmax"].attrs.get("units", "").lower() in ["k", "kelvin"]:
        ds["tasmax"] = xcu.convert_units_to(ds["tasmax"], "degC")
        ds["tasmax"].attrs["units"] = "degC"

    # Convert precipitation from flux (kg m-2 s-1) to rate (mm/day) if needed
    # 1 kg m-2 s-1 = 86400 mm/day
    if ds["pr"].attrs.get("units", "").lower() in ["kg m-2 s-1", "kg/m2/s"]:
        ds["pr"] = xcu.convert_units_to(ds["pr"], "mm/day")
        ds["pr"].attrs["units"] = "mm/day"

    # Ensure latitude is present
    if "lat" not in ds.coords:
        raise ValueError("Latitude coordinate 'lat' not found in the dataset.")

    return ds


def calculate_spei_for_series(
    precips_mm,
    pet_mm,
    scale,
    dist,
    periodicity,
    data_start_year,
    calib_start_year,
    calib_end_year,
):
    """
    Wrapper function to calculate SPEI for a chunk of time series (potentially multi-dimensional).
    Handles potential errors and NaN propagation for use with xr.apply_ufunc.
    Loops over spatial dimensions if the input chunk is multi-dimensional.
    """
    # If the input is 1D (time only), process directly

    if precips_mm.shape != pet_mm.shape:
        raise ValueError(
            "PER and Precips array must be same shape in calculate_spei_for_series"
        )

    if (precips_mm.ndim == 1) and (pet_mm.ndim == 1):
        # Check if there's enough valid data in the calibration period for fitting
        calib_indices = np.arange(
            (calib_start_year - data_start_year) * 12,
            (calib_end_year - data_start_year + 1) * 12,
        )
        # Ensure indices are within bounds
        calib_indices = calib_indices[
            (calib_indices >= 0) & (calib_indices < len(precips_mm))
        ]

        if len(calib_indices) == 0:  # Calibration period outside data range
            warnings.warn(
                f"Calibration period {calib_start_year}-{calib_end_year} is outside the data range starting {data_start_year}."
            )
            return np.full_like(precips_mm, np.nan)

        valid_calib_data = precips_mm[calib_indices][
            ~np.isnan(precips_mm[calib_indices])
        ]

        # Require at least 5 years of valid monthly data in calibration period for robust fit
        if len(valid_calib_data) < 5 * 12:
            warnings.warn(
                f"Insufficient valid data ({len(valid_calib_data)} months) in calibration period {calib_start_year}-{calib_end_year} for robust SPEI fit. Minimum required: 60 months."
            )
            return np.full_like(precips_mm, np.nan)

        # If all values are NaN, return NaNs
        if np.all(np.isnan(precips_mm)):
            return precips_mm

        try:
            # Calculate SPEI using the climate_indices package
            spei_values = climate_indices_fork.spei(
                precips_mm=precips_mm,
                pet_mm=pet_mm,  # Input water balance series (P - PET)
                scale=scale,
                distribution=dist,
                periodicity=periodicity,
                data_start_year=data_start_year,
                calibration_year_initial=calib_start_year,
                calibration_year_final=calib_end_year,
                fitting_params=None,  # Let the function compute fitting parameters from the calibration period
            )
            # Ensure output has the same shape as input, filling potentially shorter output (due to scale) with NaNs at the start
            if len(spei_values) < len(precips_mm):
                pad_width = len(precips_mm) - len(spei_values)
                spei_values = np.pad(
                    spei_values, (pad_width, 0), constant_values=np.nan
                )

            return spei_values

        except Exception as e:
            # Handle potential errors during calculation for a single pixel/series
            warnings.warn(f"SPEI calculation failed for a series: {e}")
            # Return NaNs with the same shape as the input series
            return np.full_like(precips_mm, np.nan)

    # If the input is multi-dimensional (e.g., lat, lon, time chunk)
    elif (precips_mm.ndim == 3) and (pet_mm.ndim == 3):
        # Create an output array with the same shape, filled with NaNs
        output_chunk = np.full_like(precips_mm, np.nan)
        # Loop over the spatial dimensions (first two dimensions of the chunk)
        for i in range(precips_mm.shape[0]):
            for j in range(precips_mm.shape[1]):
                precip_series = precips_mm[i, j, :]  # Extract the 1D time series
                pet_series = pet_mm[i, j, :]
                # --- Repeat the checks and calculation for this 1D series ---
                calib_indices = np.arange(
                    (calib_start_year - data_start_year) * 12,
                    (calib_end_year - data_start_year + 1) * 12,
                )
                calib_indices = calib_indices[
                    (calib_indices >= 0) & (calib_indices < len(precip_series))
                ]

                if len(calib_indices) == 0:
                    # No need to warn again for every pixel, just skip
                    continue

                valid_calib_data = precip_series[calib_indices][
                    ~np.isnan(precip_series[calib_indices])
                ]

                if len(valid_calib_data) < 5 * 12:
                    # No need to warn again for every pixel, just skip
                    continue

                if np.all(np.isnan(precip_series)):
                    continue  # Already filled with NaN

                try:
                    spei_values = climate_indices_fork.spei(
                        pet_mm=pet_series,
                        precips_mm=precip_series,
                        scale=scale,
                        distribution=dist,
                        periodicity=periodicity,
                        data_start_year=data_start_year,
                        calibration_year_initial=calib_start_year,
                        calibration_year_final=calib_end_year
                    )
                    if len(spei_values) < len(precip_series):
                        pad_width = len(precip_series) - len(spei_values)
                        spei_values = np.pad(
                            spei_values, (pad_width, 0), constant_values=np.nan
                        )

                    # Place the calculated 1D result into the output chunk
                    output_chunk[i, j, :] = spei_values

                except Exception as e:
                    # Optionally log the specific pixel index if needed for detailed debugging
                    print(f"SPEI calculation failed for series at index ({i}, {j}): {e}")
                    # Keep the output as NaN for this pixel
                    pass

        return output_chunk  # Return the processed chunk

    else:
        # Handle unexpected dimensions
        raise ValueError(
            f"Function expected 1D or 3D array, but received {pet_mm.ndim}D."
        )


# --- Main Calculation Function ---
def calculate_spei(ds_input, ds_historical, spei_scale, baseline_years):
    """
    Calculates SPEI using historical and future CMIP6 data.

    Args:
        ds_input (xr.Dataset): Dataset containing future daily 'pr', 'tasmin', 'tasmax'.
                                Must include 'lat' coordinate. Assumes 'time' coordinate.
        ds_historical (xr.Dataset): Dataset containing historical daily 'pr', 'tasmin', 'tasmax'.
                               Must include 'lat' coordinate. Assumes 'time' coordinate.
        spei_scale (int): The timescale for SPEI calculation in months.
        baseline_years (tuple): Start and end year (inclusive) for the calibration period.

    Returns:
        xr.DataArray: DataArray containing the calculated SPEI values for the full period.
    """
    print("Starting SPEI calculation...")

    # 1. Ensure correct units and variable presence
    print("Step 1: Checking variables and converting units...")
    ds_historical = ensure_correct_units(
        ds_historical
    )  # Use copy to avoid modifying original
    ds_input = ensure_correct_units(ds_input)

    # 2. Concatenate historical and future datasets along time
    print("Step 2: Concatenating historical and future data...")
    # Ensure time coordinates are compatible before concatenating
    # This might require converting calendars if they differ, though NEX-GDDP-CMIP6 is usually consistent
    try:
        ds_full = xr.concat([ds_historical, ds_input], dim="time")
        # Sort by time just in case concatenation order wasn't chronological
        ds_full = ds_full.sortby("time")
    except Exception as e:
        raise ValueError(
            f"Could not concatenate datasets. Check time coordinates and calendars. Error: {e}"
        )

    # Check for duplicate time steps after concatenation
    unique_times, counts = np.unique(ds_full["time"], return_counts=True)
    if np.any(counts > 1):
        warnings.warn(
            "Duplicate time steps found after concatenation. Keeping the first occurrence."
        )
        ds_full = ds_full.sel(time=unique_times)  # Keep unique times

    # 3. Calculate Daily Potential Evapotranspiration (PET) using Hargreaves
    print("Step 3: Calculating daily PET using Hargreaves method (xclim)...")
    if "units" not in ds_full.lat.attrs:
        print("Adding missing 'units' attribute to latitude coordinate.")
        ds_full.lat.attrs["units"] = "degrees_north"  # Or appropriate unit string
    # xclim's potential_evapotranspiration needs latitude
    pet_daily = xci.potential_evapotranspiration(
        tasmin=ds_full["tasmin"],
        tasmax=ds_full["tasmax"],
        method="HG85",  # Hargreaves method,
        lat=ds_full.lat,
    )
    # Convert from kg m-2 s-1 to mm/day
    pet_daily_mm_day = pet_daily * 86400.0
    # Assign the correct units attribute manually after conversion
    pet_daily_mm_day.attrs["units"] = "mm/day"
    # Update the variable name if needed, or overwrite pet_daily
    pet_daily = pet_daily_mm_day

    # Add PET to the dataset for convenience
    ds_full["pet"] = pet_daily

    # 4. Resample Precipitation and PET to Monthly Water Balance
    print("Step 4: Resampling to monthly and calculating water balance (P - PET)...")
    # Resample precipitation to monthly totals (sum)
    pr_monthly = (
        ds_full["pr"].resample(time="MS").sum(skipna=False)
    )  # 'MS' for month start frequency
    pr_monthly.attrs["units"] = "mm/month"
    pet_monthly = ds_full["pet"].resample(time="MS").sum(skipna=False)
    pet_monthly.attrs["units"] = "mm/month"

    # 5. Calculate SPEI using climate_indices via xr.apply_ufunc
    print(f"Step 5: Calculating {spei_scale}-month SPEI using climate_indices...")
    print(f"   Calibration period: {baseline_years[0]}-{baseline_years[-1]}")

    # Get the start year of the combined data
    full_data_start_year = pd.Timestamp(pr_monthly["time"].min().values).year

    # Use xr.apply_ufunc to apply the calculation pixel by pixel
    # Use xr.apply_ufunc to apply the calculation pixel by pixel
    spei_monthly_da = xr.apply_ufunc(
        calculate_spei_for_series,  # Function to apply
        pr_monthly,                 # First input DataArray
        pet_monthly,                # Second input DataArray
        kwargs=dict(                # Arguments for the wrapper function (excluding the DataArrays)
            scale=spei_scale,
            dist=climate_indices_fork.Distribution.pearson, # Use Pearson Type III
            periodicity=climate_indices_fork.Periodicity.monthly,
            data_start_year=full_data_start_year,
            calib_start_year=baseline_years[0],
            calib_end_year=baseline_years[-1] # Corrected index here
        ),
        input_core_dims=[['time'], ['time']],  # Specify core dims for BOTH inputs
        output_core_dims=[['time']], # Specify core dims for the SINGLE output
        exclude_dims=set(("time",)), # Keep all other dimensions (lat, lon)
        dask="parallelized",         # Enable dask for parallel computation
        output_dtypes=[pr_monthly.dtype] # Specify dtype for the SINGLE output
    )

    spei_monthly_da.name = f"spei_{spei_scale}month"
    spei_monthly_da.attrs["long_name"] = (
        f"{spei_scale}-Month Standardized Precipitation Evapotranspiration Index (SPEI)"
    )
    spei_monthly_da.attrs["units"] = "unitless"
    spei_monthly_da.attrs["calibration_period"] = (
        f"{baseline_years}-{baseline_years[1]}"
    )
    spei_monthly_da.attrs["PET_calculation"] = "Hargreaves (xclim implementation)"
    spei_monthly_da.attrs["SPEI_package"] = "climate-indices"
    spei_monthly_da.attrs["SPEI_distribution"] = "pearson"

    # 6. Reindex monthly SPEI back to daily frequency using forward fill
    print("Step 6: Reindexing monthly SPEI to daily frequency...")

    # Reindex to the original daily time index and forward fill
    spei_monthly_da = spei_monthly_da.reindex(
        {"time": pr_monthly["time"]},
    )

    spei_daily_da = spei_monthly_da.reindex(time=ds_full['time'], method="ffill")

    # Update metadata for the daily output
    spei_daily_da.attrs["long_name"] = (
        f"{spei_scale}-Month Standardized Precipitation Evapotranspiration Index (SPEI) - Daily (Forward-filled from Monthly)"
    )
    spei_daily_da.attrs["frequency"] = "daily (monthly value forward-filled)"

    spei_daily_ds = spei_daily_da.to_dataset()
    spei_daily_ds = spei_daily_ds.where(spei_daily_ds.time.isin(ds_input.time), drop=True)
    print("SPEI calculation finished.")
    return spei_daily_ds


# --- Example Usage ---
if __name__ == "__main__":
    # This is a placeholder for loading your actual data.
    # Replace this with loading your ds_historical and ds_input xarray datasets.
    print("\n--- Running Example ---")
    print("Creating dummy data for demonstration...")

    # Create dummy coordinates
    times_hist = pd.date_range(
        "1981-01-01", "2014-12-31", freq="D"
    )  # Shorter period for quick example
    times_future = pd.date_range("2018-01-01", "2018-12-31", freq="D")
    lats = np.arange(-90, 90, 5)  # Example latitudes
    lons = np.arange(-180, 180, 5)  # Example longitudes

    # Create dummy historical data
    pr_hist_data = np.random.rand(len(times_hist), len(lats), len(lons)) * 5  # mm/day
    tasmin_hist_data = (
        273.15 + 10 + np.random.randn(len(times_hist), len(lats), len(lons)) * 5
    )  # K
    tasmax_hist_data = (
        tasmin_hist_data
        + 10
        + np.random.rand(len(times_hist), len(lats), len(lons)) * 5
    )  # K

    ds_historical = xr.Dataset(
        {
            "pr": (("time", "lat", "lon"), pr_hist_data, {"units": "mm/day"}),
            "tasmin": (("time", "lat", "lon"), tasmin_hist_data, {"units": "K"}),
            "tasmax": (("time", "lat", "lon"), tasmax_hist_data, {"units": "K"}),
        },
        coords={"time": times_hist, "lat": lats, "lon": lons},
    )

    # Create dummy future data (e.g., slightly warmer and different precipitation)
    pr_future_data = (
        np.random.rand(len(times_future), len(lats), len(lons)) * 6
    )  # mm/day
    tasmin_future_data = (
        273.15 + 12 + np.random.randn(len(times_future), len(lats), len(lons)) * 5
    )  # K (warmer)
    tasmax_future_data = (
        tasmin_future_data
        + 11
        + np.random.rand(len(times_future), len(lats), len(lons)) * 5
    )  # K

    ds_input = xr.Dataset(
        {
            "pr": (("time", "lat", "lon"), pr_future_data, {"units": "mm/day"}),
            "tasmin": (("time", "lat", "lon"), tasmin_future_data, {"units": "K"}),
            "tasmax": (("time", "lat", "lon"), tasmax_future_data, {"units": "K"}),
        },
        coords={"time": times_future, "lat": lats, "lon": lons},
    )

    # Define baseline for dummy data (adjust if using real data)
    example_baseline_years = list(range(1981, 2014))
    example_spei_scale = 3  # Use 3-month SPEI for quicker example calculation

    # Calculate SPEI
    spei_results = calculate_spei(
        ds_historical,
        ds_input,
        spei_scale=example_spei_scale,
        baseline_years=example_baseline_years,
    )

    # Display some results
    print("\n--- SPEI Results ---")
    print(spei_results)
