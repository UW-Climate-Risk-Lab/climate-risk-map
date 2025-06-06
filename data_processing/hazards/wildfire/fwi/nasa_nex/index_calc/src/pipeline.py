import sys
import gc
import psutil
import boto3
import re
import argparse
from dataclasses import dataclass, fields
import multiprocessing

import xarray as xr
from urllib.parse import urlparse
from pathlib import PurePosixPath
from distributed import Client

import constants as constants
import calc as calc

@dataclass
class BoundingBox:
    x_min: float
    y_min: float
    x_max: float
    y_max: float


@dataclass
class InitialConditions:
    ffmc: xr.DataArray | None = None
    dmc: xr.DataArray | None = None
    dc: xr.DataArray | None = None

    @classmethod
    def from_zarr(cls, prior_year_zarr: str) -> "InitialConditions":
        ds = xr.open_zarr(store=prior_year_zarr, decode_times=True, consolidated=False)
        # Load the data arrays from ds so they no longer reference ds
        ffmc = ds.ffmc.isel(time=-1).load() if 'ffmc' in ds else None
        dmc = ds.dmc.isel(time=-1).load() if 'dmc' in ds else None
        dc = ds.dc.isel(time=-1).load() if 'dc' in ds else None
        ds.close()

        return cls(ffmc=ffmc, dmc=dmc, dc=dc)

@dataclass
class CalcConfig:
    n_workers: int
    threads_per_worker: int
    time_chunk: int
    lat_chunk: int
    lon_chunk: int
    bbox: BoundingBox | None
    initial_conditions: InitialConditions
    zarr_output_uri: str
    input_uris: list

def log_memory_usage():
    """Log current memory usage"""
    process = psutil.Process()
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")


def s3_uri_exists(s3_client, s3_uri: str, zarr_store: bool = False) -> bool:
    """
    Check if an S3 URI exists.

    Args:
        s3_uri (str): The S3 URI to check (e.g., "s3://bucket-name/path/to/object").
        zarr (str): If checking existence of .zarr path, pass True

    Returns:
        bool: True if the S3 object exists, False otherwise.
    """
    # Parse the S3 URI
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if zarr_store:
        # Zarr store has unique key, need actual zattrs object to check existence
        key = key + "/.zattrs"
    
    try:
        # Attempt to retrieve metadata for the object
        s3_client.head_object(Bucket=bucket, Key=key)
        return True  # Object exists
    except s3_client.exceptions.ClientError as e:
        # If a 404 error occurs, the object does not exist
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Re-raise for other errors
            raise ValueError(f"Error looking up {s3_uri}: {str(e)}")

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
    

def generate_current_year_config(s3_client, 
                                 year: str, 
                                 model: str, 
                                 scenario: str, 
                                 ensemble_member: str,
                                 lat_chunk: str,
                                 lon_chunk: str,
                                 threads: str,
                                 x_min: str,
                                 y_min: str,
                                 x_max: str,
                                 y_max: str) -> CalcConfig:
    
    # Get input files for 
    input_uris = []
    for var_candidates in constants.VAR_LIST:
        input_uri = find_best_file(s3_client, model, scenario, ensemble_member, year, var_candidates)
        if input_uri is None:
            print(f"Error: Could not find a valid file for variables: {var_candidates}")
            sys.exit(1)
        input_uris.append(input_uri)

    # Construct output Zarr file names
    current_year_file = f"fwi_month_{model}_{scenario}_{ensemble_member}_gn_{year}.zarr"
    prior_year_file = f"fwi_month_{model}_{scenario}_{ensemble_member}_gn_{year - 1}.zarr"

    base_s3_path = PurePosixPath(constants.OUTPUT_BUCKET,
                                 constants.OUTPUT_PREFIX,
                                 constants.INPUT_PREFIX,
                                 model,
                                 scenario,
                                 ensemble_member)

    # Full output URIs
    current_year_output_uri = f"s3://{base_s3_path / current_year_file}"
    prior_year_output_uri = f"s3://{base_s3_path / prior_year_file}"
    
    # We check if the prior year dataset exists to get initial conditions. 
    # Specifically checking .zattrs object because .zarr path is not a true object in S3
    if s3_uri_exists(s3_client=s3_client, s3_uri=prior_year_output_uri, zarr_store=True):
        initial_conditions = InitialConditions.from_zarr(prior_year_zarr=prior_year_output_uri)
    else:
        initial_conditions = InitialConditions(ffmc=None, dmc=None, dc=None)

    if all([x_min, y_min, x_max, y_max]):
        bbox = BoundingBox(x_min=float(x_min), y_min=float(y_min), x_max=float(x_max), y_max=float(y_max))
    else:
        bbox = None

    # Run Configuration
    config = CalcConfig(
        n_workers=multiprocessing.cpu_count(),
        threads_per_worker=int(threads),
        time_chunk=constants.TIME_CHUNK,
        lat_chunk=int(lat_chunk),
        lon_chunk=int(lon_chunk),
        bbox=bbox,
        initial_conditions=initial_conditions,
        zarr_output_uri=current_year_output_uri,
        input_uris=input_uris,
    )

    return config


def main(model: str,
         scenario: str,
         ensemble_member: str,
         lat_chunk: str,
         lon_chunk: str,
         threads: str,
         memory_available: str,
         x_min: str,
         y_min: str,
         x_max: str,
         y_max: str):


    if scenario == "historical":
        years = constants.VALID_YEARS["historical"]
    elif scenario.startswith("ssp"):
        years = constants.VALID_YEARS["ssp"]
    else:
        years = None
        raise ValueError("Invalid input scenario!")
    
    n_workers = multiprocessing.cpu_count()
    memory_limit = int(int(memory_available) / n_workers)
    
    dask_client = Client(
            n_workers=multiprocessing.cpu_count(),
            threads_per_worker=int(threads),
            memory_limit=f"{memory_limit}GB",
            )
    s3_client = boto3.client('s3')
    for year in years:
        try:
            log_memory_usage()
            print(f"Processing year {year}")
            config = None
            config = generate_current_year_config(s3_client=s3_client,
                                                year=year,
                                                model=model,
                                                scenario=scenario,
                                                ensemble_member=ensemble_member,
                                                lat_chunk=lat_chunk,
                                                lon_chunk=lon_chunk,
                                                threads=threads,
                                                x_min=x_min,
                                                y_min=y_min,
                                                x_max=x_max,
                                                y_max=y_max)
            
            if s3_uri_exists(s3_client=s3_client, s3_uri=config.zarr_output_uri, zarr_store=True):
                print(f"{config.zarr_output_uri} already exists, skipping to next year!")
                continue
            
            # Run calculation
            calc.main(config=config)
        
        except Exception as e:
            print(f"{str(e)}")

        finally:
            # Clean up after each year
            print("Cleaning up memory...")
            if config:
                del config
            gc.collect()  # Force garbage collection
            log_memory_usage()

    # Close the client
    dask_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Dask EC2 Test")
    parser.add_argument("--model", type=str, required=True, help="Climate model")
    parser.add_argument("--scenario", type=str, required=True, help="SSP Scenario or Historical")
    parser.add_argument("--ensemble_member", type=str, required=True, help="Simulation Run e.g 'r4i1p1f1'")
    parser.add_argument("--lat_chunk", type=str, required=False, help="Size of latitude chunk")
    parser.add_argument("--lon_chunk", type=str, required=False, help="Size of longitude chunk")
    parser.add_argument("--threads", type=str, required=False, help="Threads per worker")
    parser.add_argument("--memory_available", type=str, required=False, help="Total GB RAM available to the container")
    parser.add_argument("--x_min", type=str, required=False, help="For bounding box, minimum Longitude")
    parser.add_argument("--y_min", type=str, required=False, help="For bounding box, minimum Latitude")
    parser.add_argument("--x_max", type=str, required=False, help="For bounding box, maximum Longitude")
    parser.add_argument("--y_max", type=str, required=False, help="For bounding box, maximum Latitude")
    


    args = parser.parse_args()
    main(
        model=args.model,
        scenario=args.scenario,
        ensemble_member=args.ensemble_member,
        lat_chunk=args.lat_chunk,
        lon_chunk=args.lon_chunk,
        threads=args.threads,
        memory_available=args.memory_available,  # Added missing comma
        x_min=args.x_min,
        x_max=args.x_max,
        y_min=args.y_min,
        y_max=args.y_max
    )