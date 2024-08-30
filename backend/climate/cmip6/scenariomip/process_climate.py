import xarray as xr
import rioxarray
import os
from pathlib import Path

import logging
logger = logging.getLogger(__name__)


def main(
    file_directory: str,
    xarray_engine: str,
    crs: str,
    x_dim: str,
    y_dim: str,
    convert_360_lon: bool,
    bbox: dict,
) -> xr.Dataset:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        xarray_engine (str): Engine for Xarray to open files
        crs (str): Coordinate Refernce System of climate data
        x_dim (str): The X coordinate dimension name (typically lon or longitude)
        y_dim (str): The Y coordinate dimension name (typically lat or latitude)
        convert_360_lon (bool): If True, converts 0-360 lon values to -180-180
        bbox (dict): Dict with keys (min_lon, min_lat, max_lon, max_lat) to filter data

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    # For the initial dataset (burntFractionAll CESM2), each SSP
    # contained 2 files, with 2 chunks of years. These can be simply merged
    data = []
    for file in os.listdir(file_directory):
        path = Path(file_directory) / file
        _ds = xr.open_dataset(
            filename_or_obj=str(path),
            engine=xarray_engine,
            decode_times=True,
            use_cftime=True,
            decode_coords="all",
            mask_and_scale=True
        )
        data.append(_ds)
    
    # Dropping conflicts because the creation_date between
    # datasets was slightly different (a few mintues apart).
    # All other attributes should be the same.
    # TODO: Better handle conflicting attribute values
    ds = xr.merge(data, combine_attrs="drop_conflicts")
    logger.info("Xarray dataset created")

    if convert_360_lon:
        ds = ds.assign_coords({x_dim: (((ds[x_dim] + 180) % 360) - 180)})
        ds = ds.sortby(x_dim)

        # Bbox currently only in -180-180 lon
        # TODO: Add better error and case handling
        if bbox:
            ds = ds.sel(
                {
                    y_dim: slice(bbox["min_lat"], bbox["max_lat"]),
                    x_dim: slice(bbox["min_lon"], bbox["max_lon"]),
                },
            )

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    # TODO: Make aggreagtion more configurable
    # Current implementation (08/22/24) uses climatological mean
    # for every decade.
    # We create time derived coordinates "decade_month" to represent this
    ds = ds.assign_coords(
        decade=(ds["time.year"] // 10) * 10, month=ds["time"].dt.month
    )

    ds = ds.assign_coords(
        decade_month=(
            "time",
            [
                f"{decade}-{month:02d}"
                for decade, month in zip(ds["decade"].values, ds["month"].values)
            ],
        )
    )

    ds = ds.groupby("decade_month").mean()

    return ds


if __name__ == "__main__":
    main()
