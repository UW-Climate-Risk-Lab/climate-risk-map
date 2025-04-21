import boto3
import xarray as xr
import re
import s3fs
from urllib.parse import urlparse
from pathlib import PurePosixPath

import src.constants as constants

def find_best_input_file(s3_client, model, scenario, ensemble_member, year, variable):
    """
    Finds the best matching file on S3 based on variable candidates and version priority.
    """
    # Construct the S3 prefix path
    var_prefix = PurePosixPath(constants.INPUT_PREFIX, model, scenario, ensemble_member, variable)
    
    # List objects in the S3 bucket under the given prefix
    response = s3_client.list_objects_v2(Bucket=constants.INPUT_BUCKET, Prefix=str(var_prefix))
    
    if "Contents" not in response:
        return None
    
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
        return None
    
    # Prioritize files with v1.1 if available
    v1_1_files = [f for f in matching_files if "_v1.1.nc" in f]

    chosen_file = v1_1_files[0] if v1_1_files else matching_files[0]
    
    # Construct and return the full S3 URI
    return f"s3://{constants.INPUT_BUCKET}/{var_prefix / chosen_file}"

def check_vars_exist(zarr_uri: str, variables: list[str]) -> dict[str, bool]:
    """
    Checks if specified variables exist in a Zarr store on S3.

    Args:
        zarr_uri: The S3 URI of the Zarr store (e.g., "s3://bucket/path/to/data.zarr").
        variables: A list of variable names to check.

    Returns:
        A dictionary mapping variable names to a boolean indicating existence.
    """
    exists_map = {var: False for var in variables}
    try:
        # Use fsspec mapper for efficient access
        fs = s3fs.S3FileSystem(anon=False) # Or anon=True if reading public data
        mapper = s3fs.S3Map(root=zarr_uri, s3=fs, check=False)
        
        # Open dataset with minimal loading to check metadata
        # Ensure chunks=None to avoid loading chunk data
        ds = xr.open_zarr(mapper, consolidated=True, chunks=None, decode_times=False) 
        
        for var in variables:
            if var in ds.data_vars:
                exists_map[var] = True
        ds.close()
        del ds # Explicitly delete

    except (FileNotFoundError, IOError, KeyError):
        # If the store or .zmetadata doesn't exist, or other Zarr reading errors
        print(f"Zarr store or metadata not found at {zarr_uri}. Assuming variables don't exist.")
        pass # Return all False
    except Exception as e:
        print(f"Error checking Zarr store {zarr_uri}: {e}")
        # Depending on error, might want to raise it or return False
        pass 
        
    return exists_map


def s3_uri_exists(s3_uri: str, check_zattrs: bool = False) -> bool:
    """
    Checks if an S3 URI (object or Zarr store marker) exists.

    Args:
        s3_uri: The S3 URI.
        check_zattrs: If True, checks for the existence of '.zattrs' 
                      within the path, indicating a likely Zarr store root.

    Returns:
        True if it exists, False otherwise.
    """
    s3_client = boto3.client('s3') # Consider passing client in
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if check_zattrs:
        key = f"{key}/.zattrs" # Zarr stores need a specific object check

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            print(f"Warning: Error checking S3 URI {s3_uri}: {e}. Assuming it doesn't exist.")
            return False # Or raise error depending on desired robustness
    except Exception as e:
         print(f"Warning: Unexpected error checking S3 URI {s3_uri}: {e}. Assuming it doesn't exist.")
         return False


def clean_metadata_for_merge(ds: xr.Dataset) -> xr.Dataset:
    """Remove metadata fields that cause issues during merging or writing."""
    for var in list(ds.coords) + list(ds.data_vars):
        # Remove attributes known to cause merge conflicts or Zarr issues
        ds[var].attrs.pop("_FillValue", None) # Often inferred or causes type issues
        ds[var].attrs.pop("missing_value", None) 
        # History/cell_methods can grow indefinitely if not managed
        # ds[var].attrs.pop("history", None) 
        # ds[var].attrs.pop("cell_methods", None)
    return ds