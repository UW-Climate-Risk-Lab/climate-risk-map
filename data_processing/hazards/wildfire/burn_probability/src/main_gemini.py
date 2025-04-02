import xarray as xr
import os
import numpy as np
import rioxarray
from rasterio.enums import Resampling
# import statsmodels.api as sm # Not used in the original script logic provided
from pathlib import Path
import logging
import zarr
import fsspec
import s3fs
import dask
from dask.distributed import Client, LocalCluster
import gc # Garbage collector

# --- Configuration ---
FWI_VARIABLE_NAMES = ['value_q1', 'value_mean', 'value_q3'] # NASA NEX CMIP6 Ensemble mean and interquartile range of climate models
FWI_FUTUE_DECADE_MONTHS = ['2030-05', '2030-06', '2030-07', '2030-08', '2030-09', '2030-10',
                                    '2040-05', '2040-06', '2040-07', '2040-08', '2040-09', '2040-10',
                                    '2050-05', '2050-06', '2050-07', '2050-08', '2050-09', '2050-10']
FWI_FIRE_MONTHS = ['05', '06', '07', '08', '09', '10']

# Define chunk sizes (adjust based on your instance memory and data)
# 'auto' lets dask decide. Fine-tune if needed.
# Example: CHUNKS = {'lat': 256, 'lon': 256, 'decade_month': 12}
CHUNKS = 'auto'
# --- End Configuration ---

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_datasets(s3_bucket, fwi_var_names, chunks):
    """Load required datasets lazily with specified chunks."""
    logger.info("Loading datasets lazily...")

    # Load burn probability (GeoTIFF)
    try:
        da_burn_probability = rioxarray.open_rasterio(
            "data/BP_WA.tif",
            chunks=chunks # Chunk spatially
        ).sel(band=1, drop=True)
        if da_burn_probability.rio.crs is None:
             logger.warning("Burn probability GeoTIFF missing CRS, assuming EPSG:4326. Please verify.")
             da_burn_probability = da_burn_probability.rio.write_crs("EPSG:4326")
        else:
             # Ensure it's EPSG:4326
             da_burn_probability = da_burn_probability.rio.reproject("EPSG:4326")
        logger.info(f"Burn probability loaded with shape: {da_burn_probability.shape} and chunks: {da_burn_probability.chunks}")
    except Exception as e:
        logger.error(f"Failed to load burn probability data: {e}")
        raise

    # Load FWI Datasets (Zarr from S3)
    try:
        s3 = s3fs.S3FileSystem(anon=False)

        # Historical FWI
        hist_path = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/historical/fwi_decade_month_historical.zarr"
        hist_map = s3fs.S3Map(root=hist_path, s3=s3, check=False)
        ds_fwi_historical = xr.open_dataset(
            hist_map,
            engine="zarr",
            chunks=chunks,
            consolidated=True
        )[fwi_var_names] # Select the list of needed variables
        logger.info(f"Historical FWI ({', '.join(fwi_var_names)}) loaded with chunks: {ds_fwi_historical.chunks}")

        # Future FWI
        future_path = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr"
        future_map = s3fs.S3Map(root=future_path, s3=s3, check=False)
        ds_fwi_future = xr.open_dataset(
            future_map,
            engine="zarr",
            chunks=chunks,
            consolidated=True
        )[fwi_var_names] # Select the list of needed variables
        logger.info(f"Future FWI ({', '.join(fwi_var_names)}) loaded with chunks: {ds_fwi_future.chunks}")
        ds_fwi_future = ds_fwi_future.sel(decade_month=FWI_FUTUE_DECADE_MONTHS)
    except Exception as e:
        logger.error(f"Failed to load FWI data from S3: {e}")
        raise

    return da_burn_probability, ds_fwi_historical, ds_fwi_future

def calculate_mean_fwi_by_month(ds, fwi_var_names):
    """Calculate mean FWI across years for each month lazily for multiple variables."""
    logger.info(f"Calculating mean FWI by month for variables: {', '.join(fwi_var_names)}...")

    if 'decade_month' not in ds.coords:
        logger.error("Coordinate 'decade_month' not found in historical dataset.")
        raise KeyError("'decade_month' coordinate missing.")
    if ds['decade_month'].dtype != object and ds['decade_month'].dtype != 'str':
         ds['decade_month'] = ds['decade_month'].astype(str)

    try:
        months = [dm.split('-')[1] for dm in ds.decade_month.values]
        ds = ds.assign_coords(month=("decade_month", months))
    except Exception as e:
        logger.error(f"Error processing 'decade_month' coordinate: {e}. Values: {ds.decade_month.values[:10]}")
        raise

    # Groupby month and calculate mean. This works across all variables in the dataset.
    monthly_mean_fwi = ds.groupby("month").mean(dim="decade_month")
    logger.info(f"Monthly mean FWI calculated with chunks: {monthly_mean_fwi.chunks}")
    monthly_mean_fwi = monthly_mean_fwi.sel(month = FWI_FIRE_MONTHS)
    return monthly_mean_fwi

def reproject_fwi_data(ds_fwi, target_da, fwi_var_names, chunks):
    """Reproject FWI dataset (multi-variable) lazily to match target DataArray."""
    logger.info(f"Reprojecting FWI data for variables: {', '.join(fwi_var_names)}...")

    ds_fwi_reproj = ds_fwi.rio.write_crs("EPSG:4326")

    # Chunk before reprojection
    ds_fwi_reproj = ds_fwi_reproj.chunk(chunks)
    target_da = target_da.chunk(chunks)

    try:
        # rioxarray should automatically find spatial dims, but set explicitly if needed
        ds_fwi_reproj.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    except Exception as e:
        logger.warning(f"Could not automatically set spatial dims 'lon', 'lat': {e}. Check dimension names.")

    logger.info(f"Input FWI chunks before reproject_match: {ds_fwi_reproj.chunks}")
    logger.info(f"Target burn prob chunks before reproject_match: {target_da.chunks}")

    # reproject_match works on Datasets, applying to all DataArrays
    try:
        ds_fwi_reproj = ds_fwi_reproj.rio.reproject_match(
            target_da,
            resampling=Resampling.bilinear,
        )
    except Exception as e:
        logger.error(f"Reprojection failed: {e}")
        raise

    logger.info(f"FWI data reprojected with chunks: {ds_fwi_reproj.chunks}")
    return ds_fwi_reproj


def calculate_future_probability_vectorized(p_now, fwi_now_monthly_mean_ds, fwi_future_ds, fwi_var_names):
    """
    Calculate future probability using relative change method for multiple FWI variables.

    Args:
        p_now (xr.DataArray): Current burn probability (spatial).
        fwi_now_monthly_mean_ds (xr.Dataset): Historical mean FWI by month for multiple vars.
        fwi_future_ds (xr.Dataset): Future FWI by decade_month for multiple vars.
        fwi_var_names (list): List of FWI variable names being processed.

    Returns:
        xr.Dataset: Future burn probability (spatial, decade_month) for each input FWI var.
    """
    logger.info("Calculating future burn probabilities for multiple variables using vectorized approach...")

    # 1. Align historical and future datasets
    future_coord_months = [dm.split('-')[1] for dm in fwi_future_ds.decade_month.values]
    fwi_now_aligned_ds = fwi_now_monthly_mean_ds.sel(month=future_coord_months)
    fwi_now_aligned_ds = fwi_now_aligned_ds.rename({'month': 'decade_month'})
    fwi_now_aligned_ds['decade_month'] = fwi_future_ds['decade_month']

    logger.info("Aligning historical and future FWI datasets...")
    aligned_hist_ds, aligned_future_ds = xr.align(
        fwi_now_aligned_ds, fwi_future_ds, join='inner', copy=False
    )
    logger.info("Alignment complete.")

    # Dictionary to store results for each variable
    output_probabilities = {}
    epsilon = 1e-9 # For safe division

    # 2. Iterate through each FWI variable to calculate its probability
    for var_name in fwi_var_names:
        if var_name not in aligned_hist_ds or var_name not in aligned_future_ds:
            logger.warning(f"Variable '{var_name}' not found in aligned datasets. Skipping.")
            continue

        logger.info(f"Calculating future probability for variable: {var_name}")

        # Select the specific variable DataArrays
        hist_var = aligned_hist_ds[var_name]
        future_var = aligned_future_ds[var_name]

        # Calculate relative change for this variable
        future_var_safe = future_var.where(future_var > epsilon, epsilon)
        relative_change_var = 1.0 + (future_var_safe - hist_var) / future_var_safe
        # relative_change_var = relative_change_var.clip(min=0) # Optional clipping

        # Calculate future probability for this variable (p_now broadcasts)
        p_future_var = p_now * relative_change_var
        p_future_var = p_future_var.clip(0, 1)

        # Assign a descriptive name for the output variable
        output_var_name = f"future_burn_prob_{var_name}"
        p_future_var.name = output_var_name
        output_probabilities[output_var_name] = p_future_var

    # 3. Combine results into a single Dataset
    if not output_probabilities:
         logger.error("No output probabilities were calculated. Check variable names and data.")
         raise ValueError("Probability calculation resulted in an empty dataset.")

    ds_p_future = xr.Dataset(output_probabilities)
    logger.info(f"Future probability calculation defined lazily. Output variables: {list(ds_p_future.data_vars)}")
    logger.info(f"Output chunks: {ds_p_future.chunks}")

    return ds_p_future


def main():
    """Main execution function"""
    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        logger.error("S3_BUCKET environment variable not set.")
        return

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)

    # Setup Dask Client
    client = None # Initialize client to None
    cluster = None # Initialize cluster to None
    try:
        logger.info("Setting up Dask local cluster...")
        cluster = LocalCluster(
            n_workers=4,
            threads_per_worker=2,
            memory_limit='30GB' # Adjust as needed
        )
        client = Client(cluster)
        logger.info(f"Dask client started: {client.dashboard_link}")
    except Exception as e:
        logger.error(f"Failed to start Dask client: {e}")
        logger.warning("Proceeding without explicit Dask client setup.")

    try:
        # --- Load Data ---
        da_burn_probability, ds_fwi_historical, ds_fwi_future = load_datasets(
            s3_bucket, FWI_VARIABLE_NAMES, CHUNKS
        )

        # --- Process Historical Data ---
        ds_fwi_historical_monthly_mean = calculate_mean_fwi_by_month(ds_fwi_historical, FWI_VARIABLE_NAMES)
        ds_fwi_historical.close()
        del ds_fwi_historical
        gc.collect()

        # --- Reproject FWI Data ---
        ds_fwi_hist_reproj = reproject_fwi_data(
            ds_fwi_historical_monthly_mean, da_burn_probability, FWI_VARIABLE_NAMES, CHUNKS
        )
        ds_fwi_historical_monthly_mean.close()
        del ds_fwi_historical_monthly_mean
        gc.collect()

        ds_fwi_future_reproj = reproject_fwi_data(
            ds_fwi_future, da_burn_probability, FWI_VARIABLE_NAMES, CHUNKS
        )
        ds_fwi_future.close()
        del ds_fwi_future
        gc.collect()

        # --- Calculate Future Burn Probability ---
        da_burn_probability = da_burn_probability.chunk(CHUNKS) # Ensure consistent chunking

        ds_burn_probability_future = calculate_future_probability_vectorized(
            da_burn_probability,
            ds_fwi_hist_reproj, # Monthly mean historical data (multi-var)
            ds_fwi_future_reproj, # Future data (multi-var)
            FWI_VARIABLE_NAMES
        )

        # Clean up intermediate data
        ds_fwi_hist_reproj.close()
        del ds_fwi_hist_reproj
        ds_fwi_future_reproj.close()
        del ds_fwi_future_reproj
        da_burn_probability.close()
        del da_burn_probability
        gc.collect()

        # --- Save Results ---
        logger.info("Saving results dataset to Zarr on S3...")
        s3_output_uri = f"s3://{s3_bucket}/climate-risk-map/backend/climate/scenariomip/burn_probability/burn_probability_multi_var_optimized.zarr"

        # Define encoding for compression for each output variable
        encoding = {}
        output_chunks = ds_burn_probability_future.chunk(CHUNKS).chunksizes # Get computed chunks
        for var_name in ds_burn_probability_future.data_vars:
            encoding[var_name] = {
                'compressor': zarr.Blosc(cname='zstd', clevel=3, shuffle=zarr.Blosc.SHUFFLE),
                'chunks': output_chunks # Use same chunking for all output vars
            }

        fs_write = s3fs.S3FileSystem(anon=False)
        s3_map_write = s3fs.S3Map(root=s3_output_uri, s3=fs_write, check=False)

        # Write to Zarr (triggers Dask computation)
        delayed_write = ds_burn_probability_future.to_zarr(
            store=s3_map_write,
            mode="w",
            consolidated=True,
            encoding=encoding,
            compute=False
        )

        logger.info("Starting Dask computation and writing to Zarr...")
        dask.compute(delayed_write)
        logger.info(f"Results successfully saved to {s3_output_uri}")

        ds_burn_probability_future.close()
        del ds_burn_probability_future
        gc.collect()

    except Exception as e:
        logger.exception(f"An error occurred during the main processing pipeline: {e}")
    finally:
        # Shutdown Dask Client
        if client:
            logger.info("Shutting down Dask client and cluster...")
            client.close()
        if cluster:
             # Sometimes client closes cluster, sometimes not. Explicitly close cluster too.
             cluster.close()
             logger.info("Dask cluster shut down.")

    logger.info("Processing complete!")

if __name__ == "__main__":
    main()
