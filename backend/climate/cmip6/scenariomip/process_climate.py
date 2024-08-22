import xarray as xr
import os
from pathlib import Path


def main(file_directory: str, xarray_engine: str, crs: str) -> xr.Dataset:

    data = []
    for file in os.listdir(file_directory):
        path = Path(file_directory) / file
        ds = xr.open_dataset(filename_or_obj=str(path), engine=xarray_engine)
        data.append(ds)
    ds = xr.merge(data)
    ds



    return ds


if __name__ == "__main__":
    main()
