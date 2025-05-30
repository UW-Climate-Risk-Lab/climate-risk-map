import os
import numpy as np 
import xarray as xr
import rioxarray
from rasterio.enums import Resampling
from pathlib import Path # Still good for path handling

import s3fs
import fsspec
import zarr

S3_BUCKET = os.environ["S3_BUCKET"]
S3_URI_ZARR_OUTPUT = f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/usda/BP_CONUS.zarr"


def transform_geographic_coordinates(data_array: xr.DataArray) -> xr.DataArray:
    """
    Renames spatial coordinates/dimensions from 'x', 'y' to 'lon', 'lat'
    respectively, and converts longitude values from the -180 to 180 range
    to the 0 to 360 range. The data along the longitude dimension is
    reordered to match the new 0-360 longitude representation.

    Args:
        data_array: An xarray.DataArray typically from rioxarray with 'x' and 'y'
                    dimensions and coordinates. 'x' is assumed to be longitude
                    in degrees (-180 to 180), and 'y' is latitude.

    Returns:
        A new xarray.DataArray with 'lon' and 'lat' coordinates/dimensions,
        where 'lon' is in the 0-360 degree range, and data is
        appropriately reordered. Original DataArray attributes (like CRS)
        are preserved. Coordinate attributes are also preserved by the
        rename operation.
    """
    # 1. Basic check for required dimension/coordinate names
    if 'x' not in data_array.coords or 'y' not in data_array.coords:
        raise ValueError("Input DataArray must have 'x' and 'y' coordinates.")
    if 'x' not in data_array.dims or 'y' not in data_array.dims:
        raise ValueError("Input DataArray must have 'x' and 'y' as dimension names for renaming.")

    # 2. Rename coordinates and associated dimensions 'x' to 'lon' and 'y' to 'lat'
    #    xarray's rename operation typically returns a new DataArray.
    renamed_da = data_array.rename({'x': 'lon', 'y': 'lat'})

    # 3. Convert 'lon' coordinate from -180 to 180 range to 0 to 360 range

    # Calculate new longitude values using modulo arithmetic.
    # We operate on the .data (or .values) to get the NumPy array of coordinates.
    new_lon_values = np.mod(renamed_da['lon'].data, 360)

    # Assign the new coordinate values. This creates a new DataArray (or modifies
    # a copy if assign_coords is used that way) with the 'lon' coordinate
    # labels updated. The underlying data order is not yet changed to match
    # a global 0-360 map.
    da_relabelled_lon = renamed_da.assign_coords({'lon': new_lon_values})

    # Sort the DataArray by the new 'lon' coordinates.
    # This is a crucial step: it reorders the data slices along the 'lon'
    # dimension to ensure that data is continuous and correctly placed in the
    # 0-360 degree representation. It also makes the 'lon' coordinate monotonic.
    # This returns a new DataArray.
    da_transformed = da_relabelled_lon.sortby('lon')

    return da_transformed

def main(target_resolution_kilometer: int = 0.5):
    # --- User Configuration ---
    # Ensure this path points to your actual input GeoTIFF file
    input_filepath = "./src/data/BP_CONUS/BP_CONUS.tif"
    # Define the output path for the final data in EPSG:4326
    output_filepath_epsg4326 = "./burn_probability_1km_max_epsg4326.tif"
    # --- End User Configuration ---

    if not Path(input_filepath).exists():
        print(f"Error: Input file not found at '{input_filepath}'. Please ensure the path is correct.")
        return

    # 1. Open the raster in its native CRS (EPSG:5070)
    try:
        da_burn_probability_native = rioxarray.open_rasterio(input_filepath)
    except Exception as e:
        print(f"Error opening raster file '{input_filepath}': {e}")
        return

    # Remove band dimension if it exists and has size 1, or select band 1 if multiple
    if "band" in da_burn_probability_native.dims:
        if len(da_burn_probability_native.band) > 1:
            print(f"Selecting band 1 from {len(da_burn_probability_native.band)} bands.")
            da_burn_probability_native = da_burn_probability_native.sel(band=1, drop=True)
        elif len(da_burn_probability_native.band) == 1:
            da_burn_probability_native = da_burn_probability_native.squeeze("band", drop=True)

    print(f"Opened raster: {input_filepath}")
    print(f"Original CRS: {da_burn_probability_native.rio.crs}")
    print(f"Original shape: {da_burn_probability_native.shape}")
    original_x_res, original_y_res = da_burn_probability_native.rio.resolution()
    print(f"Original resolution (x, y): {original_x_res:.2f}, {original_y_res:.2f} (units of original CRS, likely meters)")

    target_resolution_meters = target_resolution_kilometer * 1000

    # Preserve the y-axis direction by using the sign of the original y-resolution
    target_y_res_meters_signed = target_resolution_meters * np.sign(original_y_res)
    if target_y_res_meters_signed == 0: # Should not happen with valid spatial data
        target_y_res_meters_signed = -target_resolution_meters # Default to negative if resolution was 0

    print(f"\nAggregating to {target_resolution_meters}m x {target_resolution_meters}m in original CRS using 'max' resampling...")
    da_aggregated_native_crs = da_burn_probability_native.rio.reproject(
        da_burn_probability_native.rio.crs,  # Target CRS is the same as native for this step
        resolution=(target_resolution_meters, target_y_res_meters_signed),
        resampling=Resampling.max
    )
    
    print(f"Shape after aggregation in native CRS: {da_aggregated_native_crs.shape}")
    agg_x_res, agg_y_res = da_aggregated_native_crs.rio.resolution()
    print(f"Resolution after aggregation (x, y): {agg_x_res:.2f}, {agg_y_res:.2f} meters")

    # 3. Reproject the aggregated (1km) raster to EPSG:4326 (latitude/longitude)
    print(f"\nReprojecting aggregated data to EPSG:4326...")
    da_aggregated_epsg4326 = da_aggregated_native_crs.rio.reproject("EPSG:4326")
    da_aggregated_epsg4326.name = "burn_probability"
    da_aggregated_epsg4326.where(da_aggregated_epsg4326 != -9999, 0.0)
    da_aggregated_epsg4326.attrs['_FillValue'] = np.float32(0.0)
    da_aggregated_epsg4326 = transform_geographic_coordinates(da_aggregated_epsg4326) # Converts lon to 0-360
    da_aggregated_epsg4326 = da_aggregated_epsg4326.where(da_aggregated_epsg4326 > 0)

    print(f"Shape after reprojection to EPSG:4326: {da_aggregated_epsg4326.shape}")
    final_x_res, final_y_res = da_aggregated_epsg4326.rio.resolution()
    print(f"Final resolution in EPSG:4326 (x, y): {final_x_res:.6f}, {final_y_res:.6f} degrees (approx.)")

    # Writing results to S3 as Zarr
    try:
        fs = s3fs.S3FileSystem(anon=False)
        # Let to_zarr() handle the computation
        da_aggregated_epsg4326.to_zarr(
            store=s3fs.S3Map(root=S3_URI_ZARR_OUTPUT, s3=fs),
            mode="w",
            consolidated=True,
        )
        print("Written to S3 successfully!")
    except Exception as e:
        print(f"Error writing to s3: {str(e)}")
        raise ValueError

if __name__ == "__main__":
    main()