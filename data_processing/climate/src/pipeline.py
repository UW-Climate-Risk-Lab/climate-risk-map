import gc
import re
import boto3
import argparse
import importlib
from dataclasses import dataclass, field
import multiprocessing
from typing import Optional, Dict, Any, List

import xarray as xr
import zarr
from pathlib import PurePosixPath
from distributed import Client, LocalCluster
import s3fs
import fsspec
import time

# Assuming structure src/constants.py, src/indicators/fwi.py etc.
import constants
import file_utils

# Import specific indicator calculation dataclasses if needed
from indicators.fwi import FwiInitialConditions
from indicators.precip import get_historical_baseline


@dataclass
class BoundingBox:
    x_min: float
    y_min: float
    x_max: float
    y_max: float


@dataclass
class PipelineConfig:
    # Data Identification
    model: str
    scenario: str
    ensemble_member: str
    year: int

    # Processing Parameters
    n_workers: int
    threads_per_worker: int
    memory_limit_gb: int
    time_chunk: int
    lat_chunk: int
    lon_chunk: int

    # Paths
    zarr_output_uri: str
    input_uris: Dict[str, str] = field(default_factory=dict)  # Map var name to S3 URI

    # Indicator-specific context (passed to calculation functions)
    indicator_context: Dict[str, Any] = field(default_factory=dict)
    bbox: Optional[BoundingBox] = None


# --- Utility Functions (Keep or adapt from original pipeline.py) ---


def load_historical_data(
    model: str, ensemble_member: str, required_vars: list[str], years: List[int]
):
    """Loads required historical variables into a single Dataset."""
    fs = fsspec.filesystem("s3", anon=True)  # Use anonymous access for public NEX data
    s3_client = boto3.client("s3")

    uris_to_load = []
    missing_vars = []
    for year in years:
        for var in required_vars:
            uri = file_utils.find_best_input_file(
                s3_client=s3_client,
                model=model,
                scenario="historical",
                ensemble_member=ensemble_member,
                year=year,
                variable=var,
            )
            if uri:
                uris_to_load.append(uri)
            else:
                missing_vars.append(var)

    if missing_vars:
        # This indicates an issue with input file discovery upstream
        raise FileNotFoundError(
            f"Missing required input URIs for variables: {missing_vars}"
        )

    if not uris_to_load:
        raise ValueError("No input URIs provided to load_input_data.")

    # Use open_mfdataset carefully - ensure chunks are sensible
    # Use preprocess function if needed for unit conversion, renaming coords etc.
    ds_historical = xr.open_mfdataset(
        uris_to_load,
        engine="h5netcdf",  # Assuming NetCDF4/HDF5
        decode_times=True,
        combine="by_coords",
        chunks={},  # Let xarray decide initial chunks, rechunk later
        parallel=True,  # Enable parallel reading
    )

    return ds_historical


def load_input_data(config: PipelineConfig, required_vars: list[str]) -> xr.Dataset:
    """Loads required input variables into a single Dataset."""
    fs = fsspec.filesystem("s3", anon=True)  # Use anonymous access for public NEX data

    uris_to_load = []
    missing_vars = []
    for var in required_vars:
        if var in config.input_uris:
            uris_to_load.append(config.input_uris[var])
        else:
            missing_vars.append(var)

    if missing_vars:
        # This indicates an issue with input file discovery upstream
        raise FileNotFoundError(
            f"Missing required input URIs for variables: {missing_vars}"
        )

    if not uris_to_load:
        raise ValueError("No input URIs provided to load_input_data.")

    # Use open_mfdataset carefully - ensure chunks are sensible
    # Use preprocess function if needed for unit conversion, renaming coords etc.
    ds_input = xr.open_mfdataset(
        uris_to_load,
        engine="h5netcdf",  # Assuming NetCDF4/HDF5
        decode_times=True,
        combine="by_coords",
        chunks={},  # Let xarray decide initial chunks, rechunk later
        parallel=True,  # Enable parallel reading
    )

    # Apply spatial bounding box if specified
    if config.bbox:
        print(f"Applying bounding box: {config.bbox}")
        ds_input = ds_input.sel(
            lat=slice(config.bbox.y_min, config.bbox.y_max),
            lon=slice(config.bbox.x_min, config.bbox.x_max),
        )
        # Check if selection resulted in empty dimensions
        if ds_input.dims["lat"] == 0 or ds_input.dims["lon"] == 0:
            raise ValueError(
                "Bounding box selection resulted in empty spatial dimensions."
            )

    # --- IMPORTANT: Rechunk to target chunks for calculations ---
    target_chunks = {
        "time": config.time_chunk
        if config.time_chunk > 0
        else ds_input.dims["time"],  # -1 means full dim
        "lat": config.lat_chunk,
        "lon": config.lon_chunk,
    }
    # Filter chunks for dimensions actually present
    target_chunks = {
        dim: size for dim, size in target_chunks.items() if dim in ds_input.dims
    }

    print(f"Rechunking input data to: {target_chunks}")
    ds_input = ds_input.chunk(target_chunks)

    # Optional: Load data into memory if cluster resources allow (can speed up compute)
    # print("Loading input data into memory (this may take time)...")
    # ds_input = ds_input.persist()
    # print("Input data loaded.")

    return ds_input


# --- Main Pipeline Logic ---


def run_pipeline_for_year(config: PipelineConfig):
    """Runs the indicator calculation pipeline for a single year."""

    year_start_time = time.time()
    print(f"\n--- Starting Processing for Year {config.year} ---")

    # 1. Check if output Zarr store exists and what variables are already present
    fs_s3 = s3fs.S3FileSystem(anon=False)
    output_mapper = s3fs.S3Map(root=config.zarr_output_uri, s3=fs_s3, check=False)

    existing_ds = None
    all_req_vars = set()
    indicators_to_run = {}

    # Determine which indicators need to be run
    print("Checking existing variables in Zarr store...")
    all_output_vars_needed = [
        var
        for ind_cfg in constants.INDICATOR_REGISTRY.values()
        for var in ind_cfg["output_vars"]
    ]
    vars_exist_map = file_utils.check_vars_exist(
        config.zarr_output_uri, all_output_vars_needed
    )

    for name, ind_cfg in constants.INDICATOR_REGISTRY.items():
        # Check if *all* output vars for this indicator exist
        if all(vars_exist_map.get(var, False) for var in ind_cfg["output_vars"]):
            print(f"Indicator '{name}' already fully computed. Skipping.")
            continue
        else:
            print(
                f"Indicator '{name}' needs calculation (missing: {[var for var in ind_cfg['output_vars'] if not vars_exist_map.get(var, False)]})."
            )
            indicators_to_run[name] = ind_cfg
            # Add required *input* variables for this indicator
            # NOTE: Need to define input vars per indicator in constants.py or infer them
            # For now, assume VAR_LIST in constants.py covers all inputs needed by any indicator run
            all_req_vars.update(
                var for var in constants.VAR_LIST
            )  # Simple update based on VAR_LIST

    if not indicators_to_run:
        print(f"All indicators already computed for {config.year}. Nothing to do.")
        return

    # 2. Load Existing Data (if store exists and we need to add to it)
    # We will merge results and overwrite, so load existing only if needed for context (e.g., initial conditions)
    # FWI initial conditions are handled separately below.
    # If other indicators needed prior year context, load existing_ds here.

    # --- Special Handling for FWI Initial Conditions ---
    if (
        "fwi" in indicators_to_run
        and constants.INDICATOR_REGISTRY["fwi"]["requires_initial_conditions"]
    ):
        prior_year = config.year - 1
        # Construct prior year's Zarr path (assuming same naming convention)
        prior_year_output_uri = config.zarr_output_uri.replace(
            f"_{config.year}.zarr", f"_{prior_year}.zarr"
        )

        print(
            f"Checking for prior year FWI data for initial conditions: {prior_year_output_uri}"
        )
        # Check if prior year exists *and* contains necessary FWI vars
        fwi_vars = constants.INDICATOR_REGISTRY["fwi"]["output_vars"]
        prior_vars_exist = file_utils.check_vars_exist(prior_year_output_uri, fwi_vars)

        fwi_ic = FwiInitialConditions()  # Start with None
        if all(prior_vars_exist.get(var, False) for var in ["ffmc", "dmc", "dc"]):
            try:
                print("Loading FWI initial conditions from prior year...")
                ds_prior = xr.open_zarr(
                    prior_year_output_uri, consolidated=True, chunks=None
                )
                # Select last time step and load into memory
                fwi_ic = FwiInitialConditions(
                    ffmc=ds_prior["ffmc"].isel(time=-1).load(),
                    dmc=ds_prior["dmc"].isel(time=-1).load(),
                    dc=ds_prior["dc"].isel(time=-1).load(),
                )
                ds_prior.close()
                del ds_prior
                print("FWI initial conditions loaded.")
            except Exception as e:
                print(
                    f"Warning: Failed to load initial conditions from {prior_year_output_uri}: {e}. Proceeding without them."
                )
                # FWI calculation function should handle None inputs
        else:
            print(
                f"Prior year Zarr {prior_year_output_uri} missing required FWI vars for initial conditions."
            )

        # Add to context passed to indicator functions
        config.indicator_context["fwi_initial_conditions"] = fwi_ic

    # --- Special Handling for Precipitation Baseline (if needed) ---
    # Only calculate baseline if the precip_percent_change indicator is running
    if "precip_percent_change" in indicators_to_run:
        # This assumes the indicator needs the baseline. Add flag in registry if needed.
        try:
            # Pass necessary info to get baseline function
            # Note: bbox dictionary needs x_min, y_min keys etc.
            bbox_dict = config.bbox.__dict__ if config.bbox else None
            cache_pr_baseline_prefix = PurePosixPath(
                constants.OUTPUT_BUCKET,
                constants.OUTPUT_PREFIX,  # Use updated OUTPUT_PREFIX
                constants.INPUT_PREFIX,  # Keep structure similar
                config.model,
                "historical",
                config.ensemble_member,
            )
            cache_pr_baseline_file = f"pr_mean_baseline_day_{config.model}_historical_{config.ensemble_member}_gn.zarr"
            cached_pr_baseline_uri = (
                f"s3://{cache_pr_baseline_prefix / cache_pr_baseline_file}"
            )
            if file_utils.s3_uri_exists(s3_uri=cached_pr_baseline_uri, check_zattrs=True):
                pr_baseline = xr.load_dataset(cached_pr_baseline_uri)
            else:
                ds_pr_historical = load_historical_data(
                    model=args.model,
                    ensemble_member=args.ensemble_member,
                    required_vars=["pr"],
                    years=constants.HISTORICAL_BASELINE_YEARS,
                )
                pr_baseline = get_historical_baseline(
                    ds_hist=ds_pr_historical,
                    model=config.model,
                    ensemble_member=config.ensemble_member,
                    bbox=bbox_dict,
                )
                baseline_fs_s3 = s3fs.S3FileSystem(anon=False)
                output_baseline_mapper = s3fs.S3Map(
                    root=cached_pr_baseline_uri, s3=baseline_fs_s3, check=False
                )
                pr_baseline.to_zarr(
                    store=output_baseline_mapper,
                    mode="w",  # Overwrite mode
                    consolidated=True,
                    compute=True,  # Trigger computation and writing
                )

            config.indicator_context["precip_baseline_mean"] = pr_baseline
            print("Precipitation baseline loaded/calculated.")
        except Exception as e:
            print(
                f"ERROR: Failed to get precipitation baseline: {e}. Skipping precip_percent_change."
            )
            # Remove the indicator if baseline fails
            indicators_to_run.pop("precip_percent_change", None)
            # Ensure 'pr' is still loaded if other indicators need it
            # all_req_vars = ... # Re-evaluate required vars if needed

    # Check again if any indicators are left to run
    if not indicators_to_run:
        print(
            f"No indicators remaining to run for {config.year} after checking context requirements."
        )
        return

    # 3. Load Required Input Data (only load variables needed for indicators being run)
    print(f"Required input variables for this run: {all_req_vars}")
    ds_input = load_input_data(config, list(all_req_vars))

    # 4. Run Indicator Calculations
    results_to_merge = []
    for name, ind_cfg in indicators_to_run.items():
        print(f"Calculating indicator: {name}...")
        calc_start_time = time.time()
        try:
            # Dynamically import the module and function
            module = importlib.import_module(ind_cfg["module"])
            calc_function = getattr(module, ind_cfg["function"])

            # Prepare arguments for the calculation function
            # Pass the input data, and any relevant context
            kwargs = {}
            if name == "fwi" and "fwi_initial_conditions" in config.indicator_context:
                kwargs["initial_conditions"] = config.indicator_context[
                    "fwi_initial_conditions"
                ]
            if (
                name == "precip_percent_change"
                and "precip_baseline_mean" in config.indicator_context
            ):
                kwargs["pr_baseline_mean"] = config.indicator_context[
                    "precip_baseline_mean"
                ]

            # Add other context items as needed based on indicator requirements

            # Run the calculation
            ds_result = calc_function(ds_input=ds_input, **kwargs)

            # Ensure result is chunked compatibly (should match input chunks)
            ds_result = ds_result.chunk(ds_input.chunks)

            # Clean metadata before merge (optional but recommended)
            ds_result = file_utils.clean_metadata_for_merge(ds_result)

            results_to_merge.append(ds_result)
            calc_elapsed = time.time() - calc_start_time
            print(f"Indicator '{name}' calculation finished in {calc_elapsed:.2f}s.")

        except Exception as e:
            print(f"ERROR calculating indicator '{name}': {e}")
            # Decide whether to continue with other indicators or fail the year
            # For robustness, maybe just log error and continue
            # raise # Uncomment to fail the whole job on one indicator error

    # 5. Merge Results and Write to Zarr
    if results_to_merge:
        print("Merging results...")
        # Start with an empty dataset or load existing if needed for merge base
        # Using merge-and-overwrite approach:

        # First, try loading the existing dataset if it exists
        try:
            ds_existing = xr.open_zarr(output_mapper, consolidated=True)
            # Clean existing metadata as well before merge
            ds_existing = file_utils.clean_metadata_for_merge(ds_existing)
            print("Loaded existing Zarr store for merging.")
            # Ensure chunks match what we calculated with, important for merge/write
            ds_existing = ds_existing.chunk(ds_input.chunks)
        except (FileNotFoundError, IOError, KeyError):
            ds_existing = xr.Dataset()  # Create empty dataset if Zarr doesn't exist
            print("No existing Zarr store found or failed to load. Starting fresh.")
        except Exception as e:
            print(
                f"Warning: Error loading existing Zarr {config.zarr_output_uri} for merge: {e}. Starting fresh."
            )
            ds_existing = xr.Dataset()

        # Merge existing data with newly computed results
        # xr.merge preserves coordinates and merges data variables
        # Use compat='override' or 'no_conflicts' depending on desired behavior
        # 'override' lets new results replace existing vars with same name
        # 'no_conflicts' will raise error if overlapping variables differ
        try:
            ds_final = xr.merge([ds_existing] + results_to_merge, compat="override")
        except Exception as e:
            print(f"ERROR merging datasets: {e}")
            # Fallback or re-raise
            raise

        # Clean up intermediate results to free memory before writing
        del results_to_merge, ds_existing
        if "ds_input" in locals():
            del ds_input  # Delete potentially large input ds
        gc.collect()

        print(f"Writing final merged dataset to Zarr: {config.zarr_output_uri}")
        write_start_time = time.time()
        try:
            # Write the final, merged dataset, overwriting the previous version
            # Ensure compute=True to trigger Dask execution if needed
            # Using consolidated=True is generally recommended for performance
            write_job = ds_final.to_zarr(
                store=output_mapper,
                mode="w",  # Overwrite mode
                consolidated=True,
                compute=True,  # Trigger computation and writing
            )
            # If using Dask, write_job might be a future; wait if needed
            # write_job.result() # Or if compute=True, it should block already

            write_elapsed = time.time() - write_start_time
            print(f"Finished writing Zarr store in {write_elapsed:.2f}s.")

        except Exception as e:
            print(f"ERROR writing final Zarr dataset to {config.zarr_output_uri}: {e}")
            raise  # Fail the job if writing fails
        finally:
            if "ds_final" in locals():
                del ds_final  # Final cleanup
            gc.collect()

    else:
        print("No new results were generated to write.")

    year_elapsed_time = time.time() - year_start_time
    print(
        f"--- Finished Processing for Year {config.year} in {year_elapsed_time:.2f} seconds ---"
    )


def main(args):
    """Main function to set up Dask and process years."""

    # --- Dask Client Setup ---
    # Use LocalCluster for single-node execution within the Batch job
    # Adjust memory_limit based on Batch job's allocation
    n_workers = min(multiprocessing.cpu_count(), 16)
    memory_limit_gb = int(args.memory_available or constants.DEFAULT_MEMORY) / n_workers
    memory_limit_bytes = memory_limit_gb * 1024**3
    threads_per_worker = int(args.threads or constants.DEFAULT_THREADS)

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

    s3_client = boto3.client("s3")  # Used for finding files initially

    # --- Determine Years to Process ---
    if args.scenario == "historical":
        years = constants.VALID_YEARS["historical"]
    elif args.scenario.startswith("ssp"):
        years = constants.VALID_YEARS["ssp"]
    else:
        raise ValueError(f"Invalid scenario: {args.scenario}")

    # --- Determine Bounding Box ---
    bbox = None
    if all([args.x_min, args.y_min, args.x_max, args.y_max]):
        try:
            bbox = BoundingBox(
                x_min=float(args.x_min),
                y_min=float(args.y_min),
                x_max=float(args.x_max),
                y_max=float(args.y_max),
            )
            print(f"Using bounding box: {bbox}")
        except ValueError:
            print(
                "Warning: Invalid bounding box coordinates provided. Proceeding without bounding box."
            )
    else:
        print("No bounding box provided, processing global data.")

    # --- Iterate Through Years ---
    for year in years:
        print(f"\n===== Processing Year: {year} =====")
        try:
            # 1. Discover Input Files for *all* potential variables
            input_uris = {}
            print("Discovering input files...")
            required_vars_all_indicators = set(
                var for var in constants.VAR_LIST
            )  # Get unique var names
            for var_name in required_vars_all_indicators:
                input_uri = file_utils.find_best_input_file(
                    s3_client,
                    args.model,
                    args.scenario,
                    args.ensemble_member,
                    year,
                    var_name,
                )
                if input_uri:
                    input_uris[var_name] = input_uri
                else:
                    # Handle missing critical input files - maybe skip year or raise error?
                    print(
                        f"CRITICAL WARNING: Could not find required input file for variable '{var_name}' for year {year}. Skipping year."
                    )
                    # Depending on needs, you might raise an error here instead:
                    # raise FileNotFoundError(f"Missing critical input {var_name} for {year}")
                    break  # Break inner loop (variables) to skip to next year

            else:  # Only execute if the inner loop completed without break (all vars found)
                # 2. Construct Output Path
                # Use a more generic name than 'fwi_day_...'
                output_file = f"indicators_day_{args.model}_{args.scenario}_{args.ensemble_member}_gn_{year}.zarr"
                base_s3_path = PurePosixPath(
                    constants.OUTPUT_BUCKET,
                    constants.OUTPUT_PREFIX,  # Use updated OUTPUT_PREFIX
                    constants.INPUT_PREFIX,  # Keep structure similar
                    args.model,
                    args.scenario,
                    args.ensemble_member,
                )
                zarr_output_uri = f"s3://{base_s3_path / output_file}"
                print(f"Target output Zarr store: {zarr_output_uri}")

                # 3. Create Configuration for the Year
                config = PipelineConfig(
                    model=args.model,
                    scenario=args.scenario,
                    ensemble_member=args.ensemble_member,
                    year=year,
                    n_workers=n_workers,
                    threads_per_worker=threads_per_worker,
                    memory_limit_gb=memory_limit_gb,
                    time_chunk=constants.TIME_CHUNK,  # Or adjust based on memory/needs
                    lat_chunk=int(args.lat_chunk or constants.DEFAULT_LAT_CHUNK),
                    lon_chunk=int(args.lon_chunk or constants.DEFAULT_LON_CHUNK),
                    bbox=bbox,
                    input_uris=input_uris,
                    zarr_output_uri=zarr_output_uri,
                    # indicator_context will be populated within run_pipeline_for_year
                )

                # 4. Run the Pipeline for this year
                run_pipeline_for_year(config)

        except Exception as e:
            print(f"FATAL ERROR processing year {year}: {e}")
            # Optional: Decide whether to continue to the next year or stop
            # raise # Uncomment to stop the entire batch job on one year's failure
            print("Skipping to next year due to error.")

        finally:
            # Clean up memory aggressively after each year
            print("Cleaning up memory after year processing...")
            # Restarting workers can sometimes help clear stubborn memory leaks
            # try:
            #     client.restart()
            #     print("Dask workers restarted.")
            # except Exception as e:
            #     print(f"Could not restart Dask workers: {e}")
            if "config" in locals():
                del config
            # Add explicit deletes for large objects if needed (ds_input, results etc. inside run_pipeline)
            gc.collect()

    # --- Shutdown Dask Client ---
    print("Closing Dask client...")
    client.close()
    cluster.close()
    print("Pipeline finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Calculate Climate Indicators using Dask"
    )
    parser.add_argument("--model", type=str, required=True, help="Climate model")
    parser.add_argument(
        "--scenario", type=str, required=True, help="SSP Scenario or Historical"
    )
    parser.add_argument(
        "--ensemble_member",
        type=str,
        required=True,
        help="Simulation Run e.g 'r1i1p1f1'",
    )
    # Made chunk/resource args optional, using defaults from constants if not provided
    parser.add_argument(
        "--lat_chunk",
        type=str,
        required=False,
        help=f"Size of latitude chunk (default: {constants.DEFAULT_LAT_CHUNK})",
    )
    parser.add_argument(
        "--lon_chunk",
        type=str,
        required=False,
        help=f"Size of longitude chunk (default: {constants.DEFAULT_LON_CHUNK})",
    )
    parser.add_argument(
        "--threads",
        type=str,
        required=False,
        help=f"Threads per Dask worker (default: {constants.DEFAULT_THREADS})",
    )
    parser.add_argument(
        "--memory_available",
        type=str,
        required=False,
        help=f"Total GB RAM available (default: {constants.DEFAULT_MEMORY}GB)",
    )
    parser.add_argument(
        "--x_min", type=str, required=False, help="Bounding box: minimum Longitude"
    )
    parser.add_argument(
        "--y_min", type=str, required=False, help="Bounding box: minimum Latitude"
    )
    parser.add_argument(
        "--x_max", type=str, required=False, help="Bounding box: maximum Longitude"
    )
    parser.add_argument(
        "--y_max", type=str, required=False, help="Bounding box: maximum Latitude"
    )

    args = parser.parse_args()
    main(args)
