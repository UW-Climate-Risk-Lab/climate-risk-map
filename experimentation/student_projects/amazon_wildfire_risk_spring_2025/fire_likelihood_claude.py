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
    ds_flame_length_exceedance_4ft = xr.open_dataset("data/FLEP4_WA.tif", engine="rasterio")
    ds_flame_length_exceedance_8ft = xr.open_dataset("data/FLEP8_WA.tif", engine="rasterio")
    
    # Load historical FWI dataset
    ds_fwi_historical = xr.open_dataset(
        f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr", 
        engine="zarr"
    )
    
    # Load future FWI dataset (2030)
    ds_fwi_2030 = xr.open_dataset(
        f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr", 
        engine="zarr"
    )
    
    return ds_burn_probability, ds_flame_length_exceedance_4ft, ds_flame_length_exceedance_8ft, ds_fwi_historical, ds_fwi_2030

def filter_fire_season_months(ds, months=['05', '06', '07', '08', '09', '10']):
    """Filter dataset to include only fire season months (May-October)"""
    month_part = np.array([month_str.split('-')[1] for month_str in ds.decade_month.values])
    month_mask = np.isin(month_part, months)
    return ds.isel(decade_month=month_mask)

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

def calculate_beta_1(fwi_data, prob_data):
    """Calculate Beta_1 coefficient using logistic regression"""
    logger.info("Calculating Beta_1 coefficient...")
    
    # Flatten arrays for regression
    fwi_flat = fwi_data.values.flatten()
    prob_flat = prob_data.values.flatten()
    
    # Filter valid values
    valid_indices = ~np.isnan(fwi_flat) & ~np.isnan(prob_flat) & (prob_flat > 0) & (prob_flat < 1)
    fwi_valid = fwi_flat[valid_indices]
    prob_valid = prob_flat[valid_indices]
    
    if len(fwi_valid) == 0:
        logger.warning("No valid data points for regression")
        return 0.1  # Default value
    
    # Convert probabilities to log-odds
    logodds = np.log(prob_valid / (1 - prob_valid))
    
    # Add constant for intercept
    X = sm.add_constant(fwi_valid)
    
    # Perform regression
    model = sm.OLS(logodds, X)
    results = model.fit()
    
    beta_1 = results.params[1]
    logger.info(f"Calculated Beta_1: {beta_1}")
    
    return beta_1

def calculate_future_probability(p_now, fwi_now, fwi_future, beta_1):
    """Calculate future probability using logistic function and Beta_1"""
    # Apply the equation from the image
    delta_fwi = fwi_future - fwi_now
    odds_multiplier = np.exp(beta_1 * delta_fwi)
    p_future = (p_now * odds_multiplier) / (1 + p_now * (odds_multiplier - 1))
    
    # Ensure probability is between 0 and 1
    p_future = np.clip(p_future, 0, 1)
    
    return p_future

def main():
    """Main execution function"""
    s3_bucket = os.environ.get("S3_BUCKET")
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    # Load datasets
    ds_burn_probability, ds_flame_4ft, ds_flame_8ft, ds_fwi_historical, ds_fwi_2030 = load_datasets(s3_bucket)
    
    # Filter to fire season months (May-October)
    ds_fwi_historical = filter_fire_season_months(ds_fwi_historical)
    ds_fwi_2030 = filter_fire_season_months(ds_fwi_2030)
    
    # Calculate historical mean FWI by month
    ds_fwi_historical = calculate_mean_fwi_by_month(ds_fwi_historical)
    ds_fwi_2030 = calculate_mean_fwi_by_month(ds_fwi_2030)
    
    # Calculate historical burn probabilities with different flame lengths
    logger.info("Calculating historical burn probabilities...")
    da_prob_gt_4ft = ds_burn_probability['band_data'] * ds_flame_4ft['band_data']
    da_prob_gt_8ft = ds_burn_probability['band_data'] * ds_flame_8ft['band_data']

    # Reproject FWI data to match burn probability spatial resolution and extent
    logger.info("Reprojecting FWI data to match burn probability data...")
    da_prob_gt_4ft = da_prob_gt_4ft.rio.reproject("EPSG:4326")
    da_prob_gt_8ft = da_prob_gt_8ft.rio.reproject("EPSG:4326")
    
    fwi_historical_reproj = ds_fwi_historical['value_q3'].rio.write_crs("EPSG:4326")
    fwi_historical_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    fwi_historical_reproj = fwi_historical_reproj.rio.reproject_match(da_prob_gt_4ft, resampling=Resampling.bilinear)
    
    fwi_2030_reproj = ds_fwi_2030['value_q3'].rio.write_crs("EPSG:4326")
    fwi_2030_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    fwi_2030_reproj = fwi_2030_reproj.rio.reproject_match(da_prob_gt_4ft, resampling=Resampling.bilinear)
    
    # Calculate Beta_1 for both probability thresholds
    monthly_beta_1 = {
        "4ft": {"05": None, "06": None, "07": None, "08": None, "09": None, "10": None},
        "8ft": {"05": None, "06": None, "07": None, "08": None, "09": None, "10": None}
    }

    for month in monthly_beta_1["4ft"].keys():
        beta_1_4ft = calculate_beta_1(fwi_historical_reproj.sel(month=month), da_prob_gt_4ft)
        monthly_beta_1["4ft"][month] = beta_1_4ft
    
    for month in monthly_beta_1["8ft"].keys():
        beta_1_8ft = calculate_beta_1(fwi_historical_reproj.sel(month=month), da_prob_gt_8ft)
        monthly_beta_1["8ft"][month] = beta_1_8ft
    
    
    # Calculate future burn probabilities for each month
    logger.info("Calculating future burn probabilities...")
    future_prob_4ft_by_month = []
    future_prob_8ft_by_month = []
    
    for month in ds_fwi_2030.month.values:
        beta_1_4ft = monthly_beta_1["4ft"][month]
        beta_1_8ft = monthly_beta_1["8ft"][month]

        # Calculate future probabilities
        future_prob_4ft = calculate_future_probability(
            da_prob_gt_4ft, 
            fwi_historical_reproj.sel(month=month), 
            fwi_2030_reproj.sel(month=month),
            beta_1_4ft
        )
        
        future_prob_8ft = calculate_future_probability(
            da_prob_gt_8ft,
            fwi_historical_reproj.sel(month=month),
            fwi_2030_reproj.sel(month=month),
            beta_1_8ft
        )
        
        future_prob_4ft = future_prob_4ft.assign_coords(month=month)
        future_prob_8ft = future_prob_8ft.assign_coords(month=month)
        
        future_prob_4ft_by_month.append(future_prob_4ft)
        future_prob_8ft_by_month.append(future_prob_8ft)
    
    future_2030_prob_gt_4ft = (xr.concat(future_prob_4ft_by_month, dim='month')).mean("month")
    future_2030_prob_gt_8ft = (xr.concat(future_prob_8ft_by_month, dim='month')).mean("month")

    # Combine monthly future probabilities
    ds_burn_prob = xr.Dataset({
        'future_2030_prob_gt_4ft': future_2030_prob_gt_4ft,
        'future_2030_prob_gt_8ft': future_2030_prob_gt_8ft,
        'current_prob_gt_4ft': da_prob_gt_4ft,
        'current_prob_gt_8ft': da_prob_gt_8ft
    })
    
    # Save results to zarr files
    logger.info("Saving results to zarr files...")
    s3_output_uri = f"s3://{s3_bucket}/student-projects/amazon-wildfire-risk-spring2025/data/cmip6_adjusted_burn_probability.zarr"
    # Let to_zarr() handle the computation
    fs = s3fs.S3FileSystem(
                anon=False,
                )
    # Let to_zarr() handle the computation
    ds_burn_prob.to_zarr(
        store=s3fs.S3Map(root=s3_output_uri, s3=fs),
        mode="w",
        consolidated=True,
    )
    
    logger.info("Processing complete!")

if __name__ == "__main__":
    main()