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
import cftime
from dask.distributed import Client
import argparse
import re

from pathlib import PurePosixPath

import constants
from models import MODELS

# --- Configuration ---
MEMORY_AVAILABLE = os.getenv("MEMORY_AVAILABLE")
DASK_WORKERS_ENV = os.getenv("DASK_WORKERS")
S3_BUCKET = os.getenv("S3_BUCKET")

# Default historical period for PCF baseline
DEFAULT_HISTORICAL_START_YEAR = 1981
DEFAULT_HISTORICAL_END_YEAR = 2014

FIRST_FUTURE_YEAR = 2015
LAST_FUTURE_YEAR = 2100

OUTPUT_ZARR_PATH_PREFIX = 'climate-risk-map/backend/climate/NEX-GDDP-CMIP6'

CHUNKS_CONFIG = {'time': -1, 'lat': 120, 'lon': 288}
RETURN_PERIODS_YEARS = [2, 5, 20, 100, 500]

def find_best_file(s3_client, model, scenario, ensemble_member, year, var_candidates):
    """
    Finds the best matching file on S3 based on variable candidates and version priority.
    """
    for variable in var_candidates:
        # Construct the S3 prefix path
        var_prefix = PurePosixPath(constants.INPUT_PREFIX, model, scenario, ensemble_member, variable)
        
        # List objects in the S3 bucket under the given prefix
        response = s3_client.list_objects_v2(Bucket=constants.INPUT_BUCKET, Prefix=str(var_prefix))
        
        if "Contents" not in response:
            continue
        
        # Regex to match the required file pattern
        pattern = (
            rf"^{variable}_day_{re.escape(model)}_{re.escape(scenario)}_"
            rf"{re.escape(ensemble_member)}_g[^_]+_{year}(_v\d+\.\d+)?\.nc$"
        )
        file_regex = re.compile(pattern)
        
        # Filter matching files using the regex
        matching_files = [
            PurePosixPath(obj["Key"]).name
            for obj in response["Contents"]
            if file_regex.match(PurePosixPath(obj["Key"]).name)
        ]
        
        if not matching_files:
            continue
        
        # Prioritize files with v1.1 if available
        v1_1_files = [f for f in matching_files if "_v1.1.nc" in f]

        chosen_file = v1_1_files[0] if v1_1_files else matching_files[0]
        
        # Construct and return the full S3 URI
        return f"s3://{constants.INPUT_BUCKET}/{var_prefix / chosen_file}"
    
    return None

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

# --- Check if zarr file exists on S3 ---
def zarr_exists_on_s3(s3_path):
    """Check if a zarr dataset exists on S3 by looking for .zattrs file"""
    try:
        fs = s3fs.S3FileSystem(anon=False)
        # Check if .zattrs exists in the zarr directory
        zattrs_path = f"{s3_path}/.zattrs"
        if s3_path.startswith("s3://"):
            zattrs_path = zattrs_path.replace("s3://", "")
        return fs.exists(zattrs_path)
    except Exception as e:
        print(f"Error checking if zarr exists at {s3_path}: {e}")
        return False

# --- Load historical PFE from S3 ---
def load_historical_pfe_from_s3(model_name, ensemble_member_id, start_year, end_year, 
                                output_s3_uri_prefix, s3_bucket_name):
    """Load cached historical PFE data from S3"""
    historical_s3_path = f"s3://{s3_bucket_name}/{output_s3_uri_prefix}/{model_name}/historical/{ensemble_member_id}/pr_pfe_month_of_year_{model_name}_historical_{ensemble_member_id}_gn_{str(start_year)}-{str(end_year)}.zarr"
    
    try:
        print(f"Loading historical PFE from: {historical_s3_path}")
        ds = xr.open_zarr(historical_s3_path, consolidated=True)
        if 'historical_pfe' in ds:
            return ds['historical_pfe']
        else:
            print(f"Warning: 'historical_pfe' not found in dataset, checking for other variable names...")
            # Try other possible variable names
            for var in ds.data_vars:
                if 'historical' in var.lower() and 'pfe' in var.lower():
                    print(f"Found historical PFE data as: {var}")
                    return ds[var]
            raise ValueError("No historical PFE variable found in dataset")
    except Exception as e:
        print(f"Error loading historical PFE from S3: {e}")
        return None

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
        ds_period = xr.open_mfdataset(input_uris, combine='by_coords', parallel=True, engine='h5netcdf', decode_times=True)
    except Exception as e:
        print(f"  Error loading data for PFE calculation: {e}")
        return None

    if 'pr' not in ds_period:
        print(f"  Error: 'pr' variable not found for PFE calculation.")
        return None
    
    pr_data = ds_period.pr

    if pr_data.attrs.get('units', '').lower() == 'kg m-2 s-1':
        pr_data = pr_data * 86400
        pr_data.attrs['units'] = 'mm/day'
    
    pr_data = pr_data.chunk(initial_chunks_config)
    pr_data = pr_data.persist()

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

# --- Process historical scenario ---
def process_historical(model_name, ensemble_member_id, start_year, end_year, 
                      output_s3_uri_prefix, s3_bucket_name, initial_chunks_config, 
                      return_periods_cfg):
    """Process and save historical PFE data"""
    output_s3_path = f"s3://{s3_bucket_name}/{output_s3_uri_prefix}/{model_name}/historical/{ensemble_member_id}/pr_pfe_month_of_year_{model_name}_historical_{ensemble_member_id}_gn_{str(start_year)}-{str(end_year)}.zarr"
    
    # Check if already exists
    if zarr_exists_on_s3(output_s3_path):
        print(f"Historical PFE already exists at: {output_s3_path}, skipping calculation")
        return True
    
    print(f"Processing historical PFE for {model_name} {ensemble_member_id} ({start_year}-{end_year})")
    s3_client = boto3.client('s3')
    
    historical_pfe = _calculate_monthly_pfes_for_period(
        start_year, end_year, "historical",
        model_name, ensemble_member_id,
        initial_chunks_config, return_periods_cfg, s3_client
    )
    
    if historical_pfe is None:
        print(f"Failed to calculate historical PFEs")
        return False
    
    historical_pfe.name = "historical_pfe"
    
    # Create dataset and save
    output_dataset = xr.Dataset({
        "historical_pfe": historical_pfe
    })
    
    output_dataset.attrs['description'] = f"Historical Monthly PFEs for {model_name} {ensemble_member_id}"
    output_dataset.attrs['xclim_version'] = xclim.__version__
    output_dataset.attrs['CMIP6_model'] = model_name
    output_dataset.attrs['CMIP6_ensemble_member'] = ensemble_member_id
    output_dataset.attrs['period'] = f"{start_year}-{end_year}"
    output_dataset.attrs['scenario'] = 'historical'
    
    print(f"Saving historical PFE to: {output_s3_path}")
    try:
        s3_map = s3fs.S3Map(root=output_s3_path, s3=s3fs.S3FileSystem(anon=False), check=False)
        with dask.diagnostics.ProgressBar():
            output_dataset.to_zarr(store=s3_map, mode="w", consolidated=True)
        print(f"Successfully saved historical PFE")
        return True
    except Exception as e:
        print(f"Error writing historical data to S3: {str(e)}")
        return False

# --- Process future scenario ---
def process_future_scenario(model_name, ensemble_member_id, start_year, end_year, scenario,
                          output_s3_uri_prefix, s3_bucket_name, initial_chunks_config, 
                          return_periods_cfg, historical_start_year, historical_end_year):
    """Process future scenario and calculate PCF using cached historical data"""
    output_s3_path = f"s3://{s3_bucket_name}/{output_s3_uri_prefix}/{model_name}/{scenario}/{ensemble_member_id}/pr_pfe_pcf_month_of_year_{model_name}_{scenario}_{ensemble_member_id}_gn_{str(start_year)}-{str(end_year)}.zarr"
    
    # Check if already exists
    if zarr_exists_on_s3(output_s3_path):
        print(f"Future PFE/PCF already exists at: {output_s3_path}, skipping calculation")
        return True
    
    print(f"Processing future scenario {scenario} for {model_name} {ensemble_member_id} ({start_year}-{end_year})")
    
    # Load historical PFE
    historical_pfe = load_historical_pfe_from_s3(
        model_name, ensemble_member_id, 
        historical_start_year, historical_end_year,
        output_s3_uri_prefix, s3_bucket_name
    )
    
    if historical_pfe is None:
        print(f"Error: Could not load historical PFE data, cannot calculate PCF")
        return False
    
    # Calculate future PFE
    s3_client = boto3.client('s3')
    future_pfe = _calculate_monthly_pfes_for_period(
        start_year, end_year, scenario,
        model_name, ensemble_member_id,
        initial_chunks_config, return_periods_cfg, s3_client
    )
    
    if future_pfe is None:
        print(f"Failed to calculate future PFEs")
        return False
    
    future_pfe.name = "future_monthly_pfe_mm_day"
    
    # Calculate PCF
    print("Calculating Pluvial Change Factor (PCF)...")
    pcf = future_pfe / historical_pfe
    pcf.name = "pluvial_change_factor"
    pcf.attrs['description'] = (f"Pluvial Change Factor: Ratio of future ({scenario} {start_year}-{end_year}) "
                               f"to historical ({historical_start_year}-{historical_end_year}) monthly PFE")
    pcf.attrs['units'] = 'dimensionless'
    
    # Create output dataset
    output_dataset = xr.Dataset({
        future_pfe.name: future_pfe,
        pcf.name: pcf
    })
    
    output_dataset.attrs['description'] = (
        f"Future Monthly PFEs and Pluvial Change Factor for {model_name} {ensemble_member_id}. "
        f"Future: {scenario} {start_year}-{end_year}. Historical baseline: {historical_start_year}-{historical_end_year}."
    )
    output_dataset.attrs['xclim_version'] = xclim.__version__
    output_dataset.attrs['CMIP6_model'] = model_name
    output_dataset.attrs['CMIP6_ensemble_member'] = ensemble_member_id
    output_dataset.attrs['historical_period_for_pcf'] = f"{historical_start_year}-{historical_end_year}"
    output_dataset.attrs['future_period_for_pcf'] = f"{start_year}-{end_year}"
    output_dataset.attrs['ssp_scenario'] = scenario
    
    print(f"Saving future PFE/PCF to: {output_s3_path}")
    try:
        s3_map = s3fs.S3Map(root=output_s3_path, s3=s3fs.S3FileSystem(anon=False), check=False)
        with dask.diagnostics.ProgressBar():
            output_dataset.to_zarr(store=s3_map, mode="w", consolidated=True)
        print(f"Successfully saved future PFE/PCF")
        return True
    except Exception as e:
        print(f"Error writing future data to S3: {str(e)}")
        return False

# --- Main function ---
def process_single_model(model_name, ensemble_member_id, scenario, future_start_year, future_end_year, 
                        historical_start_year, historical_end_year):
    """Process a single model for the given scenario and time period"""
    print(f"\n{'='*60}")
    print(f"Processing: {model_name} ({ensemble_member_id}) - {scenario} {future_start_year}-{future_end_year}")
    print(f"{'='*60}")
    
    # Process based on scenario
    if scenario == "historical":
        success = process_historical(
            model_name, ensemble_member_id, historical_start_year, historical_end_year,
            OUTPUT_ZARR_PATH_PREFIX, S3_BUCKET, CHUNKS_CONFIG, RETURN_PERIODS_YEARS
        )
    else:
        success = process_future_scenario(
            model_name, ensemble_member_id, future_start_year, future_end_year, scenario,
            OUTPUT_ZARR_PATH_PREFIX, S3_BUCKET, CHUNKS_CONFIG, RETURN_PERIODS_YEARS,
            historical_start_year, historical_end_year
        )
    
    if success:
        print(f"✓ Successfully processed {model_name} ({ensemble_member_id}) - {scenario}")
    else:
        print(f"✗ Failed to process {model_name} ({ensemble_member_id}) - {scenario}")
    
    return success

def main():
    parser = argparse.ArgumentParser(description='Calculate PFE and PCF for climate models')
    parser.add_argument('--model', type=str, help='Model name (e.g., ACCESS-CM2). If not provided, will process all models with use=True')
    parser.add_argument('--scenario', type=str, required=True,
                       help='Scenario name (historical, ssp126, ssp245, ssp370, ssp585)')
    parser.add_argument('--future-year-period', type=int, default=30,
                       help='Number of years to process in each future period (default: 30 for proper sample size)')
    parser.add_argument('--ensemble-member', type=str, help='Ensemble member ID (only used when --model is specified)')
    parser.add_argument('--historical-start-year', type=int, default=DEFAULT_HISTORICAL_START_YEAR,
                       help=f'Historical baseline start year for PCF (default: {DEFAULT_HISTORICAL_START_YEAR})')
    parser.add_argument('--historical-end-year', type=int, default=DEFAULT_HISTORICAL_END_YEAR,
                       help=f'Historical baseline end year for PCF (default: {DEFAULT_HISTORICAL_END_YEAR})')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not S3_BUCKET or S3_BUCKET == 'your-s3-bucket-name':
        print("CRITICAL ERROR: S3_BUCKET environment variable is not set or is set to placeholder.")
        return
    
    # Setup Dask
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
        client = Client(n_workers=n_actual_workers, threads_per_worker=threads_per_worker_config,
                       memory_limit=f"{memory_per_worker_gb}GB")
        print(f"Dask dashboard link: {client.dashboard_link}")

        success_count = 0
        total_processing_attempts = 0 # Renamed for clarity

        models_to_process_configs = []
        if args.model:
            ensemble_member_id = args.ensemble_member
            model_config_entry = next((m for m in MODELS if m['model'] == args.model), None) # Used for scenario validation

            if not ensemble_member_id:
                # Look up from MODELS if not provided
                resolved_model_config = next((m for m in MODELS if m['model'] == args.model and m.get('use', False)), None)
                if not resolved_model_config:
                    print(f"Error: Model {args.model} not found in MODELS config or not marked for use, and no --ensemble-member provided.")
                    client.close()
                    return
                ensemble_member_id = resolved_model_config['ensemble_member']
                print(f"Using ensemble member {ensemble_member_id} from MODELS config for {args.model}")

            # Validate scenario is supported by this model
            if model_config_entry and args.scenario not in model_config_entry['scenario']:
                print(f"Error: Scenario {args.scenario} not supported by model {args.model}")
                print(f"Supported scenarios for {args.model}: {model_config_entry['scenario']}")
                client.close()
                return
            
            models_to_process_configs.append({'model': args.model, 'ensemble_member': ensemble_member_id})
        else:
            # Process all models with use=True that support the scenario
            models_to_process_configs = [
                m for m in MODELS if m.get('use', False) and args.scenario in m['scenario']
            ]
            if not models_to_process_configs:
                print(f"No models found with use=True that support scenario {args.scenario}")
                client.close()
                return
            print(f"\nFound {len(models_to_process_configs)} models with use=True for scenario {args.scenario}")
            for mc in models_to_process_configs:
                print(f"  - {mc['model']} ({mc['ensemble_member']})")

        if args.scenario == "historical":
            print(f"\nProcessing historical period: {args.historical_start_year}-{args.historical_end_year}")
            for model_config in models_to_process_configs:
                total_processing_attempts += 1
                success = process_single_model(
                    model_config['model'], model_config['ensemble_member'], args.scenario,
                    args.historical_start_year, # future_start_year effectively this for historical
                    args.historical_end_year,   # future_end_year effectively this for historical
                    args.historical_start_year, args.historical_end_year
                )
                if success:
                    success_count += 1
        else: # Future scenarios
            for future_start_loop_year in range(FIRST_FUTURE_YEAR, LAST_FUTURE_YEAR + 1, args.future_year_period):
                future_end_loop_year = min(future_start_loop_year + args.future_year_period - 1, LAST_FUTURE_YEAR)
                
                if future_start_loop_year > future_end_loop_year : #Handles cases where period might exceed LAST_FUTURE_YEAR
                    break

                print(f"\nProcessing future period: {future_start_loop_year}-{future_end_loop_year} for scenario {args.scenario}")
                
                for model_config in models_to_process_configs:
                    total_processing_attempts += 1
                    success = process_single_model(
                        model_config['model'], model_config['ensemble_member'], args.scenario,
                        future_start_loop_year, future_end_loop_year,
                        args.historical_start_year, args.historical_end_year
                    )
                    if success:
                        success_count += 1
        
        client.close()

        # Summary
        print(f"\n{'='*60}")
        print(f"PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"Total processing attempts: {total_processing_attempts}")
        print(f"Successful: {success_count}")
        print(f"Failed: {total_processing_attempts - success_count}")

        if total_processing_attempts == 0:
            print("No models/periods were attempted.")
        elif success_count == total_processing_attempts:
            print("✓ All processing attempts successful")
        elif success_count > 0:
            print(f"⚠ Partial success: {success_count}/{total_processing_attempts} attempts successful")
            exit(1)
        else:
            print("✗ All processing attempts failed")
            exit(1)

    except Exception as e_client:
        print(f"Failed to initialize Dask client or during processing: {e_client}")
        raise

if __name__ == '__main__':
    main()