import xarray as xr
import os
import numpy as np
import rioxarray
from rasterio.enums import Resampling
import statsmodels.api as sm
from pathlib import Path
import logging
import zarr
import fsspec
import s3fs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def load_datasets(s3_bucket):
    """Load all required datasets"""
    logger.info("Loading datasets...")
    
    # Load burn probability and flame length exceedance datasets
    ds_burn_probability = xr.open_dataset("data/BP_WA.tif", engine="rasterio")
    
    # Load historical FWI dataset
    ds_fwi_historical = xr.open_dataset(
        f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr", 
        engine="zarr"
    )
    
    # Load future FWI dataset (2030)
    ds_fwi_future = xr.open_dataset(
        f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr", 
        engine="zarr"
    )
    
    return ds_burn_probability, ds_fwi_historical, ds_fwi_future

def calculate_mean_fwi_by_month(ds):
    """Calculate mean FWI across years for each month"""
    logger.info("Calculating mean FWI by month...")
    
    # Extract month from decade_month
    ds = ds.assign_coords(
        month=("decade_month", [dm.split('-')[1] for dm in ds.decade_month.values])
    )
    
    # Calculate mean across years for each month
    monthly_mean_fwi = ds.groupby("month").mean()
    
    return monthly_mean_fwi

def calculate_future_probability(p_now, fwi_now, fwi_future):
    """Calculate future probability using relative change method"""
        
    # Calculate relative change (1.0 means no change)
    relative_change = 1.0 + (fwi_future - fwi_now) / fwi_future
    p_future = p_now * relative_change
    
    # Ensure probability is between 0 and 1
    p_future = np.clip(p_future, 0, 1)
    
    return p_future

def main():
    """Main execution function"""
    s3_bucket = os.environ.get("S3_BUCKET")
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    # Load datasets
    ds_burn_probability, ds_fwi_historical, ds_fwi_future = load_datasets(s3_bucket)

    # Calculate historical mean FWI by month
    ds_fwi_historical = calculate_mean_fwi_by_month(ds_fwi_historical)

    # Reproject FWI data to match burn probability spatial resolution and extent
    logger.info("Reprojecting FWI data to match burn probability data...")
    da_burn_probability = ds_burn_probability.sel(band=1)["band_data"]
    da_burn_probability = da_burn_probability.rio.reproject("EPSG:4326")
    
    ds_fwi_historical_reproj = ds_fwi_historical.rio.write_crs("EPSG:4326")
    ds_fwi_historical_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    ds_fwi_historical_reproj = ds_fwi_historical_reproj.rio.reproject_match(da_burn_probability, resampling=Resampling.bilinear)
    
    ds_fwi_future_reproj = ds_fwi_future.rio.write_crs("EPSG:4326")
    ds_fwi_future_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    ds_fwi_future_reproj = ds_fwi_future_reproj.rio.reproject_match(da_burn_probability, resampling=Resampling.bilinear)
    
    
    # Calculate future burn probabilities for each month
    logger.info("Calculating future burn probabilities...")
    future_burn_probability = []
    
    for decade_month in ds_fwi_future['decade_month'].values: 
        # Calculate future probabilities
        _ds_future_burn_probability = calculate_future_probability(
            da_burn_probability, 
            ds_fwi_historical_reproj.sel(month=decade_month.split('-')[1]), 
            ds_fwi_future_reproj.sel(month=decade_month),
        )
        
        _ds_future_burn_probability = _ds_future_burn_probability
        
        future_burn_probability.append(_ds_future_burn_probability)
    
    ds_burn_probability_future = (xr.concat(future_burn_probability, dim='decade_month'))

    # Save results to zarr files
    logger.info("Saving results to zarr files...")
    s3_output_uri = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/burn_probability/burn_probability.zarr"
    # Let to_zarr() handle the computation
    fs = s3fs.S3FileSystem(
                anon=False,
                )
    # Let to_zarr() handle the computation
    ds_burn_probability_future.to_zarr(
        store=s3fs.S3Map(root=s3_output_uri, s3=fs),
        mode="w",
        consolidated=True,
    )
    
    logger.info("Processing complete!")

if __name__ == "__main__":
    main()