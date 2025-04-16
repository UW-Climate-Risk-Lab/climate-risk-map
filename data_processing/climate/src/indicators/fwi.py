# src/indicators/fwi.py

import xarray as xr
import xclim
from dataclasses import dataclass
from typing import Optional

@dataclass
class FwiInitialConditions:
    """Specific initial conditions needed for FWI."""
    ffmc: Optional[xr.DataArray] = None
    dmc: Optional[xr.DataArray] = None
    dc: Optional[xr.DataArray] = None

def calculate_fwi(ds_input: xr.Dataset, initial_conditions: FwiInitialConditions) -> xr.Dataset:
    """
    Calculates Canadian Forest Fire Weather Index (FWI) System indices.

    Args:
        ds_input: Dataset containing necessary input variables
                  (tasmax, pr, hurs, sfcWind, lat).
                  Must be chunked appropriately before calling.
        initial_conditions: FwiInitialConditions dataclass instance.

    Returns:
        Dataset with FWI output variables (dc, dmc, ffmc, isi, bui, fwi).
    """
    # Select DataArrays for calculation
    tas = ds_input.tasmax
    pr = ds_input.pr
    hurs = ds_input.hurs
    sfcWind = ds_input.sfcWind
    lat = ds_input.lat

    # Extract initial conditions
    ffmc0 = initial_conditions.ffmc
    dmc0 = initial_conditions.dmc
    dc0 = initial_conditions.dc

    # Calculate FWI indices using xclim
    # Note: Ensure input units are correct for xclim (temp in K, precip in kg m-2 s-1 etc.)
    # xclim handles unit conversions if units are present in attrs.
    out_fwi_indices = xclim.indicators.atmos.cffwis_indices(
        tas=tas,
        pr=pr,
        hurs=hurs,
        sfcWind=sfcWind,
        lat=lat,
        ffmc0=ffmc0,
        dmc0=dmc0,
        dc0=dc0,
        season_method=None,  # Process entire year
        overwintering=False, # Adjust if overwintering logic is needed/desired
    )

    # Package results into a Dataset
    fwi_var_names = ["dc", "dmc", "ffmc", "isi", "bui", "fwi"]
    ds_fwi_output = xr.Dataset(
        {name: da for name, da in zip(fwi_var_names, out_fwi_indices)}
        )

    # Clean metadata (optional, can also be done in pipeline.py)
    for var in ds_fwi_output.data_vars:
        ds_fwi_output[var].attrs.pop("history", None)
        ds_fwi_output[var].attrs.pop("cell_methods", None)
        
    return ds_fwi_output