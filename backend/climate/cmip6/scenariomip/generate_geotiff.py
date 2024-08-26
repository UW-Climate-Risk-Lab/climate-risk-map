import xarray as xr
import rioxarray
from pathlib import Path

import utils


def main(
    ds: xr.Dataset, output_dir: str, climate_variable: str, state: str
) -> None:
    # If a state isnt specified, assume global
    if not state:
        state = 'global'
    
    for decade_month in ds["decade_month"].data:
        _da = ds.sel(decade_month=decade_month)[climate_variable]
        file_name = f"{decade_month}-{state}.tif"
        output_path = Path(output_dir) / file_name
        _da.rio.to_raster(str(output_path), driver="COG", compression="LZW")

