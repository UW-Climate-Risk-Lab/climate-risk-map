import xarray as xr
import os
import numpy as np
import rioxarray
from rasterio.enums import Resampling

S3_BUCKET = os.environ["S3_BUCKET"]

ds_burn_probabilty = xr.open_dataset("data/BP_WA.tif", engine="rasterio")
ds_flame_length_exceedance_4ft = xr.open_dataset("data/FLEP4_WA.tif", engine="rasterio")
ds_flame_length_exceedance_8ft = xr.open_dataset("data/FLEP8_WA.tif", engine="rasterio")

ds_fwi_historical = xr.open_dataset(f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr", engine="zarr")
ds_fwi_future = xr.open_dataset(f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr", engine="zarr")

da_prob_gt_4ft = ds_burn_probabilty['band_data'] * ds_flame_length_exceedance_4ft['band_data']
da_prob_gt_8ft = ds_burn_probabilty['band_data'] * ds_flame_length_exceedance_8ft['band_data']

ds_fwi_historical = xr.open_dataset(f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr", engine="zarr")
month_part = np.array([month_str.split('-')[1] for month_str in ds_fwi_historical.decade_month.values])

# Create a boolean mask for months 05 to 10
month_mask = np.isin(month_part, ['05', '06', '07', '08', '09', '10'])
ds_fwi_historical = ds_fwi_historical.isel(decade_month=month_mask)

ds_fwi_2030 = xr.open_dataset(f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr", engine="zarr")
ds_fwi_2030 = ds_fwi_2030.sel(decade_month=ds_fwi_2030.decade_month.isin(['2030-05', '2030-06', '2030-07', '2030-08', '2030-09', '2030-10']))

# da_fwi_ratio = ds_fwi_future["fwi"] / ds_fwi_historical["fwi"]
# da_fwi_ratio = da_fwi_ratio.rio.write_crs("EPSG:4326")
# da_fwi_ratio.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
# print("fwi ratio calcd")

da_fwi_ratio_reproj = da_fwi_ratio.rio.reproject_match(da_prob_gt_4ft, resampling=Resampling.bilinear)
result = (da_prob_gt_4ft * da_fwi_ratio_reproj)
result_4326 = result.sel(band=1).transpose('decade_month', 'y', 'x').rio.reproject(dst_crs="EPSG:4326")
print("Final probability in EPSG 4326 calcd")