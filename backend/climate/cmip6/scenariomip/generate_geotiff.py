import xarray as xr
import pandas as pd
import numpy as np
import rioxarray
import json
from pathlib import Path

from typing import Dict, Any


def convert_to_serializable(value: Any) -> Any:
    """Converts a value to a JSON serializable type.
    """
    if isinstance(value, (np.integer)):
        return int(value)
    elif isinstance(value, (np.floating)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, bytes):
        return value.decode('utf-8')
    else:
        return value

def create_metadata(ds: xr.Dataset, climate_variable: str) -> Dict:
    """Creates json metadata and summary metrics for
    frontend 

    Args:
        ds (xr.Dataset): Processed Climate Dataset

    Returns:
        Dict: Dict with metadata 
    """
    metadata = {key: convert_to_serializable(value) for key, value in ds.attrs.items()}

    # Add any additional useful metadeta to the key UW_CRL_DERIVED
    metadata["UW_CRL_DERIVED"] = {}

    metadata["UW_CRL_DERIVED"]["max_climate_variable_value"] = float(ds[climate_variable].max())
    metadata["UW_CRL_DERIVED"]["min_climate_variable_value"] = float(ds[climate_variable].min())

    return metadata


def main(
    ds: xr.Dataset, output_dir: str, climate_variable: str, state: str, time_agg_method: str
) -> None:
    # If a state isnt specified, assume global
    if not state:
        state = 'global'
    
    # TODO: Clean this up
    if time_agg_method == "decade_month":
        for decade_month in ds["decade_month"].data:
            _da = ds.sel(decade_month=decade_month)[climate_variable]
            file_name = f"{decade_month}-{state}.tif"
            output_path = Path(output_dir) / file_name
            _da.rio.to_raster(str(output_path), driver="COG")
    
    metadata = create_metadata(ds=ds, climate_variable=climate_variable)
    metadata_file = f"metadata-{state}.json"
    metadata_output_path = Path(output_dir) / metadata_file

    with open(str(metadata_output_path), 'w') as f:
        json.dump(metadata, f, indent=4)

