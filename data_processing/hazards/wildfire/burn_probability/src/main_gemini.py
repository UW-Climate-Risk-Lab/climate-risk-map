import xarray as xr
import os
import numpy as np
import rioxarray
from rasterio.enums import Resampling # Make sure Resampling is imported
# import statsmodels.api as sm # Not used in the original script logic provided
from pathlib import Path
import logging
import zarr
import fsspec
import s3fs
# No Dask imports needed anymore
import gc # Garbage collector

# --- Configuration ---
# User provided configuration
FWI_VARIABLE_NAMES = ['value_q1', 'value_mean', 'value_q3'] # NASA NEX CMIP6 Ensemble mean and interquartile range of climate models
FWI_FUTUE_DECADES = ['2020', '2030', '2040', '2050', '2060', '2070', '2080', '2090', '2100'] # These are treated as individual years/decades to process
FWI_FIRE_MONTHS = ['05', '06', '07', '08', '09', '10'] # Months as strings 'MM'

# Define chunk sizes for initial loading (helps with lazy loading from Zarr/TIF)
# Operations will likely load data into memory regardless.
# Ensure 'month' key exists if used later, otherwise map decade_month appropriately
CHUNKS = {'y': 2048, 'x': 2048, 'month': 6, 'decade_month': 12} # Adjusted decade_month chunking
# --- End Configuration ---

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_datasets(s3_bucket, fwi_var_names, chunks):
    """Load required datasets lazily initially."""
    logger.info("Loading datasets...")
    # Define chunks specifically for loading
    load_chunks = {k: v for k, v in chunks.items() if k in ['lat', 'lon', 'y', 'x', 'decade_month', 'month']}
    spatial_chunks = {k:v for k,v in chunks.items() if k in ['y', 'x']}

    # Load burn probability (GeoTIFF) - loads lazily with rioxarray if chunked
    try:
        da_burn_probability = rioxarray.open_rasterio(
            "data/BP_WA.tif",
            chunks=spatial_chunks
        ).sel(band=1, drop=True)
        if da_burn_probability.rio.crs is None:
             logger.warning("Burn probability GeoTIFF missing CRS, assuming EPSG:4326. Please verify.")
             da_burn_probability = da_burn_probability.rio.write_crs("EPSG:4326")
        else:
             da_burn_probability = da_burn_probability.rio.reproject("EPSG:4326")
        logger.info(f"Burn probability loaded with shape: {da_burn_probability.shape}")
        # Note: Subsequent operations will likely load this into memory.
    except Exception as e:
        logger.error(f"Failed to load burn probability data: {e}")
        raise

    # Load FWI Datasets (Zarr from S3) - loads lazily with xarray
    try:
        s3 = s3fs.S3FileSystem(anon=False)

        # Historical FWI
        hist_path = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr"
        hist_map = s3fs.S3Map(root=hist_path, s3=s3, check=False)
        ds_fwi_historical = xr.open_dataset(
            hist_map,
            engine="zarr",
            chunks=load_chunks, # Still useful for initial lazy load
            consolidated=True
        )[fwi_var_names]
        logger.info(f"Historical FWI ({', '.join(fwi_var_names)}) loaded.")

        # Future FWI (Load the whole dataset, filtering happens later per year)
        future_path = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr"
        future_map = s3fs.S3Map(root=future_path, s3=s3, check=False)
        ds_fwi_future_full = xr.open_dataset(
            future_map,
            engine="zarr",
            chunks=load_chunks, # Still useful for initial lazy load
            consolidated=True
        )[fwi_var_names]
        logger.info(f"Full Future FWI ({', '.join(fwi_var_names)}) loaded.")

    except Exception as e:
        logger.error(f"Failed to load FWI data from S3: {e}")
        raise

    return da_burn_probability, ds_fwi_historical, ds_fwi_future_full # Return full future dataset

def calculate_mean_fwi_by_month(ds, fwi_var_names, filter_months=None):
    """Calculate mean FWI across years for each month."""
    logger.info(f"Calculating mean FWI by month for variables: {', '.join(fwi_var_names)}...")

    if 'decade_month' not in ds.coords:
        logger.error("Coordinate 'decade_month' not found in dataset.")
        raise KeyError("'decade_month' coordinate missing.")
    # Ensure coord is string for splitting, handle potential non-string types
    if ds['decade_month'].dtype != object and ds['decade_month'].dtype != 'str':
        logger.warning(f"Converting decade_month coordinate from {ds['decade_month'].dtype} to string.")
        try:
            ds['decade_month'] = ds['decade_month'].astype(str)
        except Exception as e:
            logger.error(f"Failed to convert decade_month to string: {e}")
            raise

    try:
        # Extract month string 'MM'
        months = [dm.split('-')[1] for dm in ds.decade_month.values]
        ds = ds.assign_coords(month=("decade_month", months))
    except Exception as e:
        logger.error(f"Error processing 'decade_month' coordinate: {e}. Values: {ds.decade_month.values[:10]}")
        raise

    # This calculation will likely load data into memory
    monthly_mean_fwi = ds.groupby("month").mean(dim="decade_month")
    logger.info("Monthly mean FWI calculated.")

    # Filter for specific fire months if requested
    if filter_months:
        try:
            if 'month' in monthly_mean_fwi.coords:
                # Ensure filter months are strings 'MM' for comparison
                filter_months_str = [str(m).zfill(2) for m in filter_months]
                monthly_mean_fwi = monthly_mean_fwi.sel(month=filter_months_str)
                logger.info(f"Filtered monthly mean FWI to months: {filter_months_str}")
            else:
                logger.error("Coordinate 'month' not found after groupby mean calculation.")
                raise KeyError("'month' coordinate missing after groupby.")
        except KeyError as e:
            logger.error(f"Failed to select fire months. Available: {monthly_mean_fwi.get('month', 'N/A').values}. Requested: {filter_months_str}. Error: {e}")
            raise

    return monthly_mean_fwi

def reproject_fwi_data(ds_fwi, target_da, fwi_var_names):
    """Reproject FWI dataset to match target DataArray (in-memory)."""
    logger.info(f"Reprojecting FWI data for variables: {', '.join(fwi_var_names)} using NEAREST neighbor...")

    # Ensure CRS and spatial dimensions are set
    ds_fwi_reproj = ds_fwi.rio.write_crs("EPSG:4326")
    try:
        # Attempt to set spatial dims, log warning if fails but continue
        ds_fwi_reproj = ds_fwi_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=False)
    except Exception as e:
        logger.warning(f"Could not automatically set spatial dims 'lon', 'lat': {e}. Check dimension names.")

    # Reprojection will load data into memory if not already loaded
    try:
        logger.info("Starting reprojection match (this may take time and memory)...")
        # Ensure target_da is loaded if it's still lazy
        target_da_loaded = target_da.load()
        ds_fwi_reproj_matched = ds_fwi_reproj.rio.reproject_match(
            target_da_loaded, # Use loaded target
            resampling=Resampling.nearest,
        )
        del target_da_loaded # Clean up loaded target
        gc.collect()
        logger.info("Reprojection match completed.")
    except Exception as e:
        logger.error(f"Reprojection failed: {e}")
        raise

    logger.info(f"FWI data reprojected.")
    return ds_fwi_reproj_matched


def calculate_future_probability_vectorized(p_now, fwi_now_monthly_mean_ds, fwi_future_monthly_mean_ds, fwi_var_names):
    """Calculate future probability using relative change method (in-memory)."""
    logger.info("Calculating future burn probabilities (vectorized, in-memory)...")

    # Align datasets - this might load data
    logger.info("Aligning historical and future monthly mean FWI datasets...")
    # Ensure inputs are loaded before align if necessary
    p_now_loaded = p_now.load()
    fwi_now_loaded = fwi_now_monthly_mean_ds.load()
    fwi_future_loaded = fwi_future_monthly_mean_ds.load()

    aligned_hist_ds, aligned_future_ds = xr.align(
        fwi_now_loaded, fwi_future_loaded, join='inner', copy=False
    )
    # Align burn probability separately if its coords differ slightly after load/reproject
    p_now_aligned, _ = xr.align(p_now_loaded, aligned_hist_ds, join="inner", copy=False)

    logger.info("Alignment complete.")

    # Perform calculations - these operate on in-memory NumPy arrays now
    output_probabilities = {}
    epsilon = 1e-9

    for var_name in fwi_var_names:
        if var_name not in aligned_hist_ds or var_name not in aligned_future_ds:
            logger.warning(f"Variable '{var_name}' not found in aligned datasets. Skipping.")
            continue

        logger.info(f"Calculating future probability for variable: {var_name}")
        hist_var = aligned_hist_ds[var_name]
        future_var = aligned_future_ds[var_name]
        future_var_safe = future_var.where(future_var > epsilon, epsilon)
        relative_change_var = 1.0 + (future_var_safe - hist_var) / future_var_safe
        # Use aligned burn probability
        p_future_var = p_now_aligned * relative_change_var
        p_future_var = p_future_var.clip(0, 1)
        # Rename output variable slightly for clarity
        output_var_name = f"burn_probability_{var_name}" # Changed from future_burn_prob_
        p_future_var.name = output_var_name
        output_probabilities[output_var_name] = p_future_var

    # Clean up loaded versions
    del p_now_loaded, fwi_now_loaded, fwi_future_loaded, p_now_aligned, aligned_hist_ds, aligned_future_ds
    gc.collect()

    if not output_probabilities:
         logger.error("No output probabilities were calculated. Check variable names and data.")
         raise ValueError("Probability calculation resulted in an empty dataset.")

    ds_p_future = xr.Dataset(output_probabilities)
    # The dimension is still 'month' at this point
    logger.info("Future probability calculation completed (dimension is 'month').")
    return ds_p_future


def main():
    """Main execution function (No Dask, Looping through years)"""
    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        logger.error("S3_BUCKET environment variable not set.")
        return

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    # --- No Dask Client Setup ---
    logger.info("Running script without Dask cluster.")

    ds_fwi_hist_reproj = None # Initialize variables to ensure they exist in finally block
    da_burn_probability = None
    ds_fwi_future_full = None

    try:
        # --- Load Data (Once) ---
        da_burn_probability, ds_fwi_historical, ds_fwi_future_full = load_datasets(
            s3_bucket, FWI_VARIABLE_NAMES, CHUNKS
        )
        logger.info("Initial data loading complete.")

        # --- Process Historical Data (Once) ---
        logger.info("Processing historical data...")
        ds_fwi_historical_monthly_mean = calculate_mean_fwi_by_month(
            ds_fwi_historical, FWI_VARIABLE_NAMES, filter_months=FWI_FIRE_MONTHS
        )
        ds_fwi_historical.close()
        del ds_fwi_historical
        gc.collect()
        logger.info("Historical monthly mean calculated.")

        logger.info("Reprojecting historical data (this may take time and memory)...")
        ds_fwi_hist_reproj = reproject_fwi_data(
            ds_fwi_historical_monthly_mean, da_burn_probability, FWI_VARIABLE_NAMES
        )
        ds_fwi_historical_monthly_mean.close()
        del ds_fwi_historical_monthly_mean
        gc.collect()
        logger.info("Historical data reprojection complete.")
        # Load historical reprojected data into memory as it's used in every loop iteration
        logger.info("Loading historical reprojected data into memory...")
        ds_fwi_hist_reproj.load()
        logger.info("Historical reprojected data loaded.")


        # --- Loop Through Future Years ---
        for year in FWI_FUTUE_DECADES:
            logger.info(f"--- Processing year: {year} ---")

            ds_fwi_future_year = None # Initialize year-specific variables
            ds_fwi_future_monthly_mean_year = None
            ds_fwi_future_reproj_year = None
            ds_burn_probability_future_year = None

            try:
                # Filter full future dataset for the current year
                logger.info(f"Filtering future data for year {year}...")
                decade_months_year_filter = [f"{year}-{month}" for month in FWI_FIRE_MONTHS]
                if 'decade_month' in ds_fwi_future_full.coords:
                    # Use isel if decade_month is not an index, or sel if it is
                    # Check if coordinate is an index
                    if 'decade_month' in ds_fwi_future_full.indexes:
                         ds_fwi_future_year = ds_fwi_future_full.sel(decade_month=decade_months_year_filter)
                    else:
                         # If not an index, boolean indexing might be needed or ensure it's set as index
                         logger.warning("decade_month is not an index, attempting boolean selection.")
                         ds_fwi_future_year = ds_fwi_future_full.isel(decade_month=ds_fwi_future_full['decade_month'].isin(decade_months_year_filter))

                    logger.info(f"Filtered future FWI for {year}.")
                else:
                    logger.error(f"Coordinate 'decade_month' not found in full future dataset for filtering year {year}.")
                    continue # Skip to next year if coordinate missing

                # Process Future Data for the current year
                logger.info(f"Calculating monthly mean for future year {year}...")
                ds_fwi_future_monthly_mean_year = calculate_mean_fwi_by_month(
                    ds_fwi_future_year, FWI_VARIABLE_NAMES, filter_months=FWI_FIRE_MONTHS # Ensure filtering matches historical
                )
                del ds_fwi_future_year # Clean up filtered data
                gc.collect()
                logger.info(f"Future monthly mean calculated for {year}.")

                # Reproject Future Data for the current year
                logger.info(f"Reprojecting future data for {year} (this may take time and memory)...")
                ds_fwi_future_reproj_year = reproject_fwi_data(
                    ds_fwi_future_monthly_mean_year, da_burn_probability, FWI_VARIABLE_NAMES
                )
                del ds_fwi_future_monthly_mean_year # Clean up monthly mean
                gc.collect()
                logger.info(f"Future data reprojection complete for {year}.")

                # Calculate Future Burn Probability for the current year
                logger.info(f"Calculating final burn probabilities for {year}...")
                ds_burn_probability_future_year = calculate_future_probability_vectorized(
                    da_burn_probability, # Use original burn prob (loaded/reprojected once)
                    ds_fwi_hist_reproj,    # Use historical reprojected (loaded once)
                    ds_fwi_future_reproj_year, # Use future reprojected for this year
                    FWI_VARIABLE_NAMES
                )
                logger.info(f"Final burn probability calculation complete for {year}.")

                # Clean up year-specific future reprojected data
                del ds_fwi_future_reproj_year
                gc.collect()

                # *** Add decade_month coordinate back before saving ***
                logger.info(f"Reconstructing decade_month coordinate for year {year}...")
                if 'month' in ds_burn_probability_future_year.coords:
                    months_coords = ds_burn_probability_future_year['month'].values
                    decade_months_coords_year = [f"{year}-{m}" for m in months_coords]
                    # Rename dimension and coordinate
                    ds_burn_probability_future_year = ds_burn_probability_future_year.rename({'month': 'decade_month'})
                    # Assign new coordinate values
                    ds_burn_probability_future_year['decade_month'] = ('decade_month', decade_months_coords_year)
                    logger.info(f"Replaced 'month' dimension with 'decade_month' for year {year}.")
                else:
                    logger.warning(f"Could not find 'month' coordinate to reconstruct 'decade_month' for year {year}.")


                # --- Save Results for the current year ---
                logger.info(f"Saving results dataset for {year} to Zarr on S3...")
                s3_output_uri = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/burn_probability/burn_probability_{year}.zarr" # Use year in filename

                # Define encoding for compression
                encoding = {}

                for var_name in ds_burn_probability_future_year.data_vars:
                     # Check dimensions match example storage chunks length
                    encoding[var_name] = {
                        'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=zarr.Blosc.SHUFFLE),
                    }
                logger.info(f"Using encoding for saving year {year}: {encoding}")

                fs_write = s3fs.S3FileSystem(anon=False)
                s3_map_write = s3fs.S3Map(root=s3_output_uri, s3=fs_write, check=False)

                logger.info(f"Starting write to Zarr for year {year} (computes result in memory first)...")
                ds_burn_probability_future_year.to_zarr(
                    store=s3_map_write,
                    mode="w",
                    consolidated=True,
                    encoding=encoding,
                )
                logger.info(f"Results for year {year} successfully saved to {s3_output_uri}")

                # Clean up final dataset object for the year
                ds_burn_probability_future_year.close()
                del ds_burn_probability_future_year
                gc.collect()

            except Exception as e:
                logger.exception(f"An error occurred during processing for year {year}: {e}")
                # Optionally decide whether to continue to the next year or stop
                # continue

        # --- End of Loop ---
        logger.info("Finished processing all years.")

    except Exception as e:
        logger.exception(f"An error occurred during the main processing pipeline outside the year loop: {e}")
    finally:
        # Clean up objects loaded/processed outside the loop
        logger.info("Performing final cleanup...")
        if ds_fwi_hist_reproj is not None:
            ds_fwi_hist_reproj.close()
            del ds_fwi_hist_reproj
        if da_burn_probability is not None:
            da_burn_probability.close()
            del da_burn_probability
        if ds_fwi_future_full is not None:
            ds_fwi_future_full.close()
            del ds_fwi_future_full
        gc.collect()
        logger.info("Script finished.")

    logger.info("Processing complete!")

if __name__ == "__main__":
    main()
