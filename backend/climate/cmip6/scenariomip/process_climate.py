import xarray as xr
import rioxarray
import os
from pathlib import Path


def main(
    file_directory: str, xarray_engine: str, crs: str, x_dim: str, y_dim: str
) -> xr.Dataset:
    """Processes climate data

    Args:
        file_directory (str): Directory to open files from
        xarray_engine (str): Engine for Xarray to open files
        crs (str): Coordinate Refernce System of climate data
        x_dim (str): The X coordinate dimension name (typically lon or longitude)
        y_dim (str): The Y coordinate dimension name (typically lat or latitude)

    Returns:
        xr.Dataset: Xarray dataset of processed climate data
    """

    data = []
    for file in os.listdir(file_directory):
        path = Path(file_directory) / file
        _ds = xr.open_dataset(
            filename_or_obj=str(path),
            engine=xarray_engine,
            decode_times=True,
            use_cftime=True,
            decode_coords="all",
        )
        data.append(_ds)

    # Dropping conflicts because the creation_date between
    # datasets was slightly different (a few mintues apart).
    # All other attributes should be the same.
    # TODO: Better handle conflicting attribute values
    ds = xr.merge(data, combine_attrs="drop_conflicts")

    # Sets the CRS based on the provided CRS
    ds.rio.write_crs(crs, inplace=True)
    ds.rio.set_spatial_dims(x_dim=x_dim, y_dim=y_dim, inplace=True)
    ds.rio.write_coordinate_system(inplace=True)

    # TODO: Make aggreagtion more configurable
    # Current implementation (08/22/24) uses climatological mean
    # for every decade. We create a derived dimension to represent this
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
