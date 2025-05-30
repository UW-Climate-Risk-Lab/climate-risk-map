import xarray as xr
import xclim
import boto3
import multiprocessing
from xclim import indices
from xclim.indices import stats
import dask.diagnostics
import os
import numpy as np # For np.nan
import s3fs
import fsspec
from dask.distributed import Client

from src.pipeline import find_best_file
from aws_batch import MODELS

# --- Configuration ---
MEMORY_AVAILABLE = os.getenv("MEMORY_AVAILABLE", "16") # Default to 16GB if not set
HISTORICAL_START_YEAR = 1981
HISTORICAL_END_YEAR = 1983 # Reduced for testing; use 2014 for full run
OUTPUT_ZARR_PATH_PREFIX = 'climate-risk-map/backend/climate/NEX-GDDP-CMIP6'
S3_BUCKET = os.getenv("S3_BUCKET", "your-s3-bucket-name") # Get from env or set default
CHUNKS = {'time': -1, 'lat': 120, 'lon': 288}

# Define the return periods for which to calculate precipitation depths (in years)
# As per First Street Flood Methodology for their general outputs
RETURN_PERIODS_YEARS = [2, 5, 20, 100, 500]

# --- Script ---

def calculate_monthly_return_periods_for_apply(pr_daily_data_for_one_month, return_periods_config):
    """
    Calculates return period values from annual series of daily maxima for a specific month.
    `pr_daily_data_for_one_month` is daily precipitation for ONE specific month across multiple years.
    """
    if not isinstance(pr_daily_data_for_one_month, xr.DataArray):
        print("Warning: Input to calculate_monthly_return_periods_for_apply is not an xarray.DataArray. Returning NaNs.")
        # Need to return something with the expected 'return_period' dimension
        return xr.DataArray(np.nan,
                            coords={'return_period': return_periods_config},
                            dims=['return_period'],
                            name='monthly_pfe')

    if pr_daily_data_for_one_month.size == 0 or pr_daily_data_for_one_month.isnull().all():
        print("Warning: Data for return period calculation is all NaNs or empty. Returning NaNs.")
        return xr.DataArray(np.nan,
                            coords={'return_period': return_periods_config},
                            dims=['return_period'],
                            name='monthly_pfe')

    # 1. Get the annual maximum daily values for this month's data
    # Resulting `annual_max_for_month` will have a 'year' coordinate/dimension.
    annual_max_for_month = pr_daily_data_for_one_month.groupby('time.year').max(dim='time', skipna=True)

    # 2. Prepare the `annual_max_for_month` DataArray for xclim's `fa` function
    # Ensure 'year' is a dimension and rename it to 'time'
    if 'year' in annual_max_for_month.dims:
        annual_max_for_month_renamed = annual_max_for_month.rename({'year': 'time'})
    elif 'year' in annual_max_for_month.coords:
        try:
            annual_max_for_month_renamed = annual_max_for_month.set_index(time='year').rename_dims({'year':'time'})
        except Exception as e_setindex:
            print(f"Warning: Could not set 'year' coord as 'time' dimension: {e_setindex}. Proceeding with current structure.")
            annual_max_for_month_renamed = annual_max_for_month # May fail later
    else:
        print(f"Error: 'year' dim/coord not found in annual_max_for_month. Dims: {annual_max_for_month.dims}, Coords: {list(annual_max_for_month.coords.keys())}")
        return xr.DataArray(np.nan, coords={'return_period': return_periods_config}, dims=['return_period'], name='monthly_pfe')

    # Convert integer years in 'time' coordinate to datetime objects
    if 'time' in annual_max_for_month_renamed.coords and np.issubdtype(annual_max_for_month_renamed.time.dtype, np.integer):
        try:
            years_int = annual_max_for_month_renamed.time.values
            annual_max_for_month_renamed['time'] = [np.datetime64(f'{y}-01-01') for y in years_int]
        except Exception as e_datetime:
            print(f"Warning: Could not convert integer years to datetime for 'time' coord: {e_datetime}")

    # Data sufficiency check (improved from previous version)
    if 'time' in annual_max_for_month_renamed.dims:
        valid_data_count_per_pixel = annual_max_for_month_renamed.count(dim='time')
        min_valid_points_across_all_pixels = valid_data_count_per_pixel.min().item() if not hasattr(valid_data_count_per_pixel, 'compute') else valid_data_count_per_pixel.compute().min().item()
        if min_valid_points_across_all_pixels < 5: # Threshold for warning
             print(f"Warning: At least one pixel has insufficient data points (min_count_found={min_valid_points_across_all_pixels}) for robust GEV fit this month. Resulting PFEs might be NaN.")

    # Rechunk the 'time' dimension (core dimension for fa/fit)
    if 'time' in annual_max_for_month_renamed.dims and hasattr(annual_max_for_month_renamed.data, 'chunks'):
        time_axis_num = annual_max_for_month_renamed.get_axis_num('time')
        if len(annual_max_for_month_renamed.chunks[time_axis_num]) > 1:
            print("Rechunking 'time' dimension to a single chunk for frequency analysis.")
            annual_max_for_month_renamed = annual_max_for_month_renamed.chunk(CHUNKS)
    
    # 3. Calculate return period values using xclim.indices.stats.fa [cite: 40, 44]
    # `fa` combines fit and parametric_quantile. Input `annual_max_for_month_renamed` is the series of annual (monthly) maxima.
    try:
        # mode='max' is appropriate here, as we are looking for maxima. [cite: 42]
        # dist='genextreme' uses GEV.
        # `t` is the return period in years.
        rp_values = stats.fa(annual_max_for_month_renamed, t=return_periods_config, dist='genextreme', mode='max')
    except Exception as e:
        print(f"Error during frequency analysis (stats.fa): {e}. Returning NaNs.")
        return xr.DataArray(np.nan,
                            coords={'return_period': return_periods_config},
                            dims=['return_period'],
                            name='monthly_pfe')
    print(f"Return period values month calculated")
    return rp_values


def calculate_historical_monthly_pfes(start_year,
                                      end_year,
                                      output_s3_uri_prefix,
                                      model,
                                      ensemble_member,
                                      chunks,
                                      return_periods_config):
    print(f"Starting historical monthly PFE calculation for period: {start_year}-{end_year} for {model} {ensemble_member}")
    s3_client = boto3.client('s3')
    input_uris = []
    for year_iter in range(start_year, end_year + 1):
        input_uri = find_best_file(s3_client, model, "historical", ensemble_member, year_iter, ["pr"])
        if input_uri:
            input_uris.append(input_uri)

    if not input_uris:
        print(f"Error: No input files found for {model} {ensemble_member} for years {start_year}-{end_year}.")
        return

    try:
        print(f"Loading precipitation data from {len(input_uris)} files...")
        ds_hist = xr.open_mfdataset(input_uris, combine='by_coords', parallel=True, engine='h5netcdf', use_cftime=False)
    except Exception as e:
        print(f"Error loading data for {model} {ensemble_member}: {e}")
        return

    if 'pr' not in ds_hist:
        print(f"Error: 'pr' variable not found for {model} {ensemble_member}.")
        return
    pr_hist: xr.DataArray = ds_hist.pr

    if pr_hist.attrs.get('units', '').lower() == 'kg m-2 s-1':
        pr_hist = pr_hist * 86400
        pr_hist.attrs['units'] = 'mm/day'
        print("Converted precipitation units to mm/day.")
    pr_hist = pr_hist.chunk(chunks)
    #pr_hist = pr_hist.compute() # Persist after pre-processing
    print("Data loaded and pre-processed. Grouping by month and calculating monthly PFEs...")

    # Apply the PFE calculation function to each month's data group
    # Pass RETURN_PERIODS_YEARS to the apply function using args or a wrapper
    monthly_pfes_da = pr_hist.groupby('time.month').apply(
        calculate_monthly_return_periods_for_apply,
        return_periods_config=return_periods_config # Pass as kwarg
    )
    monthly_pfes_da = monthly_pfes_da.compute()
    monthly_pfes_da = monthly_pfes_da.rename({'month': 'month_of_year'})
    monthly_pfes_da.name = "monthly_pfe_mm_day"

    # Ensure correct chunking after apply
    final_chunks = {'month_of_year': -1, 'return_period':-1}
    if 'lat' in monthly_pfes_da.dims: final_chunks['lat'] = chunks.get('lat', 50)
    if 'lon' in monthly_pfes_da.dims: final_chunks['lon'] = chunks.get('lon', 50)
    monthly_pfes_da = monthly_pfes_da.chunk(final_chunks)

    pfe_dataset = monthly_pfes_da.to_dataset() # Convert DataArray to Dataset for saving
    pfe_dataset.attrs['description'] = (
        f"Monthly Precipitation Frequency Estimates (PFEs) for daily extremes "
        f"for {model} {ensemble_member}, period {start_year}-{end_year}. "
        f"Values are precipitation depths (mm/day) for specified return periods."
    )
    pfe_dataset.attrs['xclim_version'] = xclim.__version__
    pfe_dataset.attrs['return_periods_calculated_for_years'] = str(return_periods_config)


    output_s3_full_path = f"s3://{S3_BUCKET}/{output_s3_uri_prefix}/{model}/historical/{ensemble_member}/pr_month_pfe_baseline_{str(start_year)}-{str(end_year)}.zarr"
    print(f"Saving monthly PFEs to Zarr store: {output_s3_full_path}")

    try:
        fs = s3fs.S3FileSystem(anon=False)
        # Let to_zarr() handle the computation
        pfe_dataset.to_zarr(
            store=s3fs.S3Map(root=output_s3_full_path, s3=fs),
            mode="w",
            consolidated=True,
        )

    except Exception as e:
        print(f"Error writing to s3: {str(e)}")
        raise ValueError

    print(f"Historical monthly PFE calculation complete for {model} {ensemble_member}.")

if __name__ == '__main__':
    if S3_BUCKET == 'your-s3-bucket-name':
        print("--- PLEASE CONFIGURE YOUR S3_BUCKET ---")
    else:
        n_cpus = multiprocessing.cpu_count()
        try:
            memory_available_gb = float(MEMORY_AVAILABLE)
        except ValueError:
            memory_available_gb = 16.0 # Fallback
        memory_limit_gb = int(memory_available_gb / n_cpus) if n_cpus > 0 else int(memory_available_gb)
        if memory_limit_gb < 1: memory_limit_gb = 1

        print(f"CPUs: {n_cpus}, Total Memory: {memory_available_gb}GB, Memory per worker: {memory_limit_gb}GB")
        client = Client(n_workers=int(n_cpus), threads_per_worker=2, memory_limit=f"{memory_limit_gb}GB")
        print(f"Dask dashboard link: {client.dashboard_link}")

        for model_data in MODELS:
            if model_data.get("use", False):
                model = model_data["model"]
                ensemble_member = model_data["ensemble_member"]
                calculate_historical_monthly_pfes(
                    start_year=HISTORICAL_START_YEAR,
                    end_year=HISTORICAL_END_YEAR,
                    output_s3_uri_prefix=OUTPUT_ZARR_PATH_PREFIX,
                    chunks=CHUNKS,
                    model=model,
                    ensemble_member=ensemble_member,
                    return_periods_config=RETURN_PERIODS_YEARS # Pass the global config
                )
        client.close()