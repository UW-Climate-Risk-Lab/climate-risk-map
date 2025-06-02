import xarray as xr
import xclim
import boto3
import multiprocessing
from xclim import indices
from xclim.indices import stats
import dask.diagnostics
import os
import numpy as np
import s3fs
from dask.distributed import Client

from src.pipeline import find_best_file
from models import MODELS

# --- Configuration ---
MEMORY_AVAILABLE = os.getenv("MEMORY_AVAILABLE", "112")
DASK_WORKERS_ENV = os.getenv("DASK_WORKERS", str(multiprocessing.cpu_count()))
S3_BUCKET = os.getenv("S3_BUCKET")

HISTORICAL_START_YEAR = 1981
HISTORICAL_END_YEAR = 1983 # Use 2014 for full historical run

# Example: Define future periods and scenarios you want to process
# Each item is a dictionary: {"start_year": YYYY, "end_year": YYYY, "scenario_name": "sspXXX"}
FUTURE_PERIODS_CONFIG = [
    {"start_year": 2020, "end_year": 2039},
    {"start_year": 2040, "end_year": 2059},
    {"start_year": 2060, "end_year": 2079},
    {"start_year": 2080, "end_year": 2100},
]

FUTURE_SCENARIOS = ["ssp126", "ssp245", "ssp370", "ssp585"]

OUTPUT_ZARR_PATH_PREFIX = 'climate-risk-map/tests/NEX-GDDP-CMIP6-FULL' # Example test path

CHUNKS_CONFIG = {'time': -1, 'lat': 120, 'lon': 288}
RETURN_PERIODS_YEARS = [2, 5, 20, 100, 500]

# --- Helper Function for Consistent NaN Output ---
def _create_nan_data_array_for_pfe(return_periods_cfg, template_lat_coords, template_lon_coords, name='pfe_values'):
    nan_coords = {'return_period': return_periods_cfg}
    nan_dims = ['return_period']
    if template_lat_coords is not None and template_lon_coords is not None:
        nan_coords['lat'] = template_lat_coords
        nan_coords['lon'] = template_lon_coords
        nan_dims.extend(['lat', 'lon'])
    return xr.DataArray(np.nan, coords=nan_coords, dims=nan_dims, name=name)

# --- Core Calculation Function (Applied per Month) ---
def calculate_monthly_return_periods_for_apply(pr_daily_data_for_one_month, return_periods_config,
                                             template_lat_coords, template_lon_coords):
    nan_template_result = _create_nan_data_array_for_pfe(return_periods_config, template_lat_coords, template_lon_coords)
    if not isinstance(pr_daily_data_for_one_month, xr.DataArray) or pr_daily_data_for_one_month.time.size == 0:
        return nan_template_result

    try:
        annual_max_for_month = pr_daily_data_for_one_month.groupby('time.year').max(dim='time', skipna=True)
    except Exception as e_groupmax:
        print(f"Error in groupby('time.year').max(): {e_groupmax}. Returning NaNs.")
        return nan_template_result
    if annual_max_for_month.size == 0: return nan_template_result

    if 'year' in annual_max_for_month.dims:
        annual_max_for_month_renamed = annual_max_for_month.rename({'year': 'time'})
    elif 'year' in annual_max_for_month.coords:
        try:
            annual_max_for_month_renamed = annual_max_for_month.set_index(time='year').rename_dims({'year': 'time'})
        except Exception as e_set_index:
            print(f"Error in set_index/rename_dims for year to time: {e_set_index}. Returning NaNs.")
            return nan_template_result
    else: return nan_template_result

    if 'time' in annual_max_for_month_renamed.coords and np.issubdtype(annual_max_for_month_renamed.time.dtype, np.integer):
        try:
            annual_max_for_month_renamed['time'] = [np.datetime64(f'{y}-07-01') for y in annual_max_for_month_renamed.time.values]
        except Exception as e_time_conv:
            print(f"Warning: Failed to convert integer years to datetime64 for xclim: {e_time_conv}. "
                  "Proceeding with integer years, which might cause issues with xclim.stats.fa.")
            # If this conversion is absolutely critical and xclim cannot handle integer years for fa,
            # you might consider returning nan_template_result here instead of just printing a warning.
            # For example:
            # print(f"Error: Critical failure converting integer years to datetime64: {e_time_conv}. Returning NaNs.")
            # return nan_template_result
    if 'time' not in annual_max_for_month_renamed.dims: return nan_template_result

    if hasattr(annual_max_for_month_renamed.data, 'chunks'):
        time_axis_num = annual_max_for_month_renamed.get_axis_num('time')
        if len(annual_max_for_month_renamed.chunks[time_axis_num]) > 1:
            annual_max_for_month_renamed = annual_max_for_month_renamed.chunk({'time': -1})
    try:
        rp_values = stats.fa(annual_max_for_month_renamed, t=return_periods_config, dist='genextreme', mode='max')
        if template_lat_coords is not None and 'lat' not in rp_values.coords and 'lat' in nan_template_result.coords:
            rp_values = rp_values.expand_dims(lat=template_lat_coords).transpose(*nan_template_result.dims)
        if template_lon_coords is not None and 'lon' not in rp_values.coords and 'lon' in nan_template_result.coords:
            rp_values = rp_values.expand_dims(lon=template_lon_coords).transpose(*nan_template_result.dims)
    except Exception as e:
        print(f"Error in stats.fa for a month: {e}. Returning NaNs.")
        return nan_template_result
    return rp_values

# --- Generic PFE Calculation for a given period ---
def _calculate_monthly_pfes_for_period(
    period_start_year, period_end_year, scenario_name,
    model_name, ensemble_member_id,
    initial_chunks_config, return_periods_cfg, s3_client
):
    print(f"  Calculating monthly PFEs for: {model_name} {ensemble_member_id}, Scenario: {scenario_name}, Period: {period_start_year}-{period_end_year}")
    input_uris = []
    for year_iter in range(period_start_year, period_end_year + 1):
        input_uri = find_best_file(s3_client, model_name, scenario_name, ensemble_member_id, year_iter, ["pr"])
        if input_uri: input_uris.append(input_uri)
    if not input_uris:
        print(f"  Error: No input files found for PFE calculation.")
        return None

    try:
        # Note on use_cftime=False: If input NetCDF files use non-standard calendars 
        # (e.g., 'noleap', '360_day'), np.datetime64 might not represent them accurately.
        # xclim generally handles cftime objects well, so using use_cftime=True (default) 
        # is often safer if your data's calendar might be non-standard.
        # Forcing False can lead to errors or misinterpretation of time coordinates.
        ds_period = xr.open_mfdataset(input_uris, combine='by_coords', parallel=True, engine='h5netcdf', use_cftime=False)
    except Exception as e:
        print(f"  Error loading data for PFE calculation: {e}")
        return None

    if 'pr' not in ds_period:
        print(f"  Error: 'pr' variable not found for PFE calculation.")
        return None
    pr_data = ds_period.pr.sel(time=slice(f"{period_start_year}-01-01", f"{period_end_year}-12-31"))

    if pr_data.attrs.get('units', '').lower() == 'kg m-2 s-1':
        pr_data = pr_data * 86400
        pr_data.attrs['units'] = 'mm/day'
    
    pr_data = pr_data.chunk(initial_chunks_config)
    pr_data = pr_data.persist() # Persist this period's data

    template_lat = pr_data.lat if 'lat' in pr_data.coords else None
    template_lon = pr_data.lon if 'lon' in pr_data.coords else None

    monthly_pfes = pr_data.groupby('time.month').apply(
        calculate_monthly_return_periods_for_apply,
        return_periods_config=return_periods_cfg,
        template_lat_coords=template_lat,
        template_lon_coords=template_lon
    )
    if 'month' in monthly_pfes.dims:
        monthly_pfes = monthly_pfes.rename({'month': 'month_of_year'})
    monthly_pfes.name = f"{scenario_name}_monthly_pfe_mm_day"
    
    final_chunks = {'month_of_year': 12, 'return_period': -1}
    if 'lat' in monthly_pfes.dims: final_chunks['lat'] = initial_chunks_config.get('lat', 'auto')
    if 'lon' in monthly_pfes.dims: final_chunks['lon'] = initial_chunks_config.get('lon', 'auto')
    monthly_pfes = monthly_pfes.chunk(final_chunks)
    
    print(f"  Finished PFEs for: {scenario_name} {period_start_year}-{period_end_year}")
    return monthly_pfes

# --- Main Orchestration Function per GCM ---
def process_gcm_for_full_pfe_pcf_calculation(
    model_name, ensemble_member_id,
    hist_start_year, hist_end_year,
    future_periods_list, # List of {"start_year", "end_year"},
    future_scenarios_list,
    output_s3_uri_prefix,
    initial_chunks_config,
    return_periods_cfg,
    s3_bucket_name # Pass S3_BUCKET directly
):
    print(f"\nProcessing GCM: {model_name}, Ensemble Member: {ensemble_member_id}")
    s3_client = boto3.client('s3')

    # 1. Calculate Historical Monthly PFEs (all return periods)
    print(f"Calculating historical monthly PFEs ({hist_start_year}-{hist_end_year})...")
    historical_monthly_pfes_da = _calculate_monthly_pfes_for_period(
        hist_start_year, hist_end_year, "historical",
        model_name, ensemble_member_id,
        initial_chunks_config, return_periods_cfg, s3_client
    )
    if historical_monthly_pfes_da is None:
        print(f"Failed to calculate historical PFEs for {model_name} {ensemble_member_id}. Skipping this GCM.")
        return
    
    print("Persisting historical PFEs...")
    historical_pfe = historical_monthly_pfes_da.persist() # Persist for reuse
    print("Historical PFEs calculated and persisted.")

    historical_pfe.name = "historical_monthly_pfe_for_pcf"

    for future_period in future_periods_list:
        for future_scenario in future_scenarios_list:
            fut_start = future_period["start_year"]
            fut_end = future_period["end_year"]
            ssp_scenario = future_scenario
            print(f"\n  Processing future period: ({fut_start}-{fut_end})")

            future_monthly_pfes_da = _calculate_monthly_pfes_for_period(
                fut_start, fut_end, ssp_scenario,
                model_name, ensemble_member_id,
                initial_chunks_config, return_periods_cfg, s3_client
            )
            if future_monthly_pfes_da is None:
                print(f"  Failed to calculate future PFEs for {ssp_scenario} ({fut_start}-{fut_end}). Skipping.")
                continue
            
            print(f"  Persisting future PFEs for {ssp_scenario} ({fut_start}-{fut_end})...")
            future_pfe = future_monthly_pfes_da.persist()
            future_pfe.name = "future_monthly_pfe_mm_day" # Set name for dataset var

            print("  Calculating Pluvial Change Factor (PCF)...")
            pcf = future_pfe / historical_pfe # Element-wise division
            pcf.name = "pluvial_change_factor"
            pcf_description = (f"Pluvial Change Factor: Ratio of future ({ssp_scenario} {fut_start}-{fut_end}) "
                            f"Monthly PFE to historical ({hist_start_year}-{hist_end_year}) monthly PFE.")
            pcf.attrs['description'] = pcf_description
            pcf.attrs['units'] = 'dimensionless'

            output_dataset = xr.Dataset({
                future_pfe.name: future_pfe,
                pcf.name: pcf,
                historical_pfe.name: historical_pfe
            })
            output_dataset.attrs['description'] = (
                f"Future Monthly PFEs and Pluvial Change Factor for {model_name} {ensemble_member_id}. "
                f"Future: {ssp_scenario} {fut_start}-{fut_end}. Hist Baseline: {hist_start_year}-{hist_end_year}."
            )
            output_dataset.attrs['xclim_version'] = xclim.__version__
            output_dataset.attrs['CMIP6_model'] = model_name
            output_dataset.attrs['CMIP6_ensemble_member'] = ensemble_member_id
            output_dataset.attrs['historical_period_for_pcf'] = f"{hist_start_year}-{hist_end_year}"
            output_dataset.attrs['future_period_for_pcf'] = f"{fut_start}-{fut_end}"
            output_dataset.attrs['ssp_scenario'] = ssp_scenario

            output_s3_full_path = f"s3://{s3_bucket_name}/{output_s3_uri_prefix}/{model_name}/{ssp_scenario}/{ensemble_member_id}/pfe_pcf_data_{str(fut_start)}-{str(fut_end)}.zarr"
            print(f"  Attempting to save data to: {output_s3_full_path}")
            try:
                s3_map = s3fs.S3Map(root=output_s3_full_path, s3=s3fs.S3FileSystem(anon=False), check=False)
                with dask.diagnostics.ProgressBar():
                    output_dataset.to_zarr(store=s3_map, mode="w", consolidated=True)
                print(f"  Successfully saved data to {output_s3_full_path}")
            except Exception as e:
                print(f"  Error writing data to S3 ({output_s3_full_path}): {str(e)}")

# --- Main Execution ---
if __name__ == '__main__':
    if not S3_BUCKET or S3_BUCKET == 'your-s3-bucket-name':
        print("CRITICAL ERROR: S3_BUCKET environment variable is not set or is set to placeholder.")
    else:
        try:
            n_actual_workers = int(DASK_WORKERS_ENV)
        except ValueError:
            n_actual_workers = multiprocessing.cpu_count()
        try:
            memory_available_gb = float(MEMORY_AVAILABLE)
        except ValueError:
            memory_available_gb = 16.0
        
        threads_per_worker_config = 1
        memory_per_worker_gb = int(memory_available_gb * 0.9 / n_actual_workers) if n_actual_workers > 0 else int(memory_available_gb * 0.9)
        if memory_per_worker_gb < 1: memory_per_worker_gb = 1

        print(f"Configuring Dask Client: Workers={n_actual_workers}, Threads/Worker={threads_per_worker_config}, Memory/Worker={memory_per_worker_gb}GB")
        
        try:
            client = Client(n_workers=n_actual_workers, threads_per_worker=threads_per_worker_config, memory_limit=f"{memory_per_worker_gb}GB")
            print(f"Dask dashboard link: {client.dashboard_link}")

            for model_config in MODELS:
                if model_config.get("use", False):
                    process_gcm_for_full_pfe_pcf_calculation(
                        model_name=model_config["model"],
                        ensemble_member_id=model_config["ensemble_member"],
                        hist_start_year=HISTORICAL_START_YEAR,
                        hist_end_year=HISTORICAL_END_YEAR,
                        future_periods_list=FUTURE_PERIODS_CONFIG,
                        future_scenarios_list=FUTURE_SCENARIOS,
                        output_s3_uri_prefix=OUTPUT_ZARR_PATH_PREFIX,
                        initial_chunks_config=CHUNKS_CONFIG,
                        return_periods_cfg=RETURN_PERIODS_YEARS,
                        s3_bucket_name=S3_BUCKET
                    )
            client.close()
        except Exception as e_client:
            print(f"Failed to initialize Dask client or during main processing loop: {e_client}")
            raise