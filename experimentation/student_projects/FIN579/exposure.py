import concurrent.futures as cf
import logging
import os
import gc
import time
from typing import Dict, List, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import rioxarray # noqa - Required for xr.open_dataset with spatial data
import xarray as xr
import xvec
from shapely.geometry import Point
from shapely.ops import transform

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Use an environment variable or direct assignment for the ID column
# Ensure your input CSV has a unique identifier column with this name.
ID_COLUMN = "osm_id" # IMPORTANT: Make sure this matches your CSV header for unique IDs
GEOMETRY_COLUMN = "geometry"
S3_BUCKET = os.environ.get("S3_BUCKET", "your-default-bucket") # Use env var or replace default

# Climate data variables to process
# Separating variables for point extraction vs. radius analysis
POINT_DATA_VARS = ['value_mean']
RADIUS_DATA_VARS = ['burn_probability_value_mean'] # Only these get radius stats

# Define radius distances (in miles) and stats for radius analysis
RADIUS_MILES = [5, 10, 25]
RADIUS_STATS = ['max'] # Calculate both mean and max in one pass

# Coordinate Reference Systems
WGS84_CRS = "EPSG:4326"

YEAR = 2040

# Input/Output File Paths (adjust as needed)
FACILITY_CSV_PATH = "data/data_centers.csv"
OUTPUT_CSV_PATH = f"{FACILITY_CSV_PATH}_exposure.csv"
BURN_PROBABILITY_PATH = f"s3://{S3_BUCKET}/climate-risk-map/backend/climate/scenariomip/burn_probability/burn_probability_{YEAR}.zarr"
FWI_PATH = "s3://uw-crl/climate-risk-map/backend/climate/scenariomip/NEX-GDDP-CMIP6/DECADE_MONTH_ENSEMBLE/ssp370/fwi_decade_month_ssp370.zarr"

DS_BURN_PROBABILTY = xr.open_dataset(BURN_PROBABILITY_PATH)
DS_FWI = xr.open_dataset(FWI_PATH)
DS_FWI = DS_FWI.sel(decade_month=DS_BURN_PROBABILTY['decade_month'].values)
DS_FWI = DS_FWI.assign_coords({'lon': (((DS_FWI['lon'] + 180) % 360) - 180)})
DS_FWI = DS_FWI.sortby('lon')

# --- End Configuration ---

def miles_to_meters(miles: Union[int, float]) -> float:
    """Convert miles to meters"""
    return miles * 1609.34

def convert_ds_to_df(ds: xr.Dataset, id_column: str) -> pd.DataFrame:
    """
    Converts an xarray Dataset (typically from xvec) to a pandas DataFrame,
    resetting the index and keeping relevant columns.
    Assumes the Dataset index name matches the provided id_column.
    """
    if not isinstance(ds, xr.Dataset):
        logger.error("Input to convert_ds_to_df is not an xarray Dataset.")
        return pd.DataFrame()

    try:
        # xvec adds stat prefix (e.g., 'mean_') to data vars
        # Keep all data variables calculated
        stat_columns = list(ds.data_vars.keys())
        
        # Convert to DataFrame, assumes index name is set correctly before calling xvec
        df = ds.to_dataframe()
        
        # Check if index needs resetting (depends on how xvec was called)
        if isinstance(df.index, pd.MultiIndex):
             df = df.reset_index()
        else:
             # If single index, reset and potentially rename 'index' column
             df = df.reset_index()
             if 'index' in df.columns and id_column not in df.columns:
                 df = df.rename(columns={'index': id_column})


        # Define columns to keep: ID, month (if exists), and all calculated stats
        columns_to_keep = [id_column]
        if 'decade_month' in df.columns:
            df["decade"] = df["decade_month"].apply(lambda x: int(x[0:4]))
            df["month"] = df["decade_month"].apply(lambda x: int(x[-2:]))
            columns_to_keep.append('month')
            columns_to_keep.append('decade')
        columns_to_keep.extend(stat_columns)

        # Ensure we only select columns that actually exist in the DataFrame
        existing_columns_to_keep = [col for col in columns_to_keep if col in df.columns]
    
        return df[existing_columns_to_keep]

    except Exception as e:
        logger.exception(f"Error converting Dataset to DataFrame: {e}")
        return pd.DataFrame()


def task_xvec_zonal_stats(
    climate_subset: xr.Dataset, # Pass only the necessary variables
    geometry_chunk: gpd.GeoSeries,
    x_dim: str,
    y_dim: str,
    stats_list: List[str], # Expecting ['mean', 'max'] etc.
    index_name: str,
    method: str = "exactextract",
) -> pd.DataFrame:
    """
    Worker task for running xvec.zonal_stats in parallel.
    Takes a subset of the climate data.
    """
    try:
        # Ensure geometry chunk has the correct index name for xvec
        geometry_chunk.index.name = index_name

        ds = climate_subset.xvec.zonal_stats(
            geometry_chunk,
            x_coords=x_dim,
            y_coords=y_dim,
            stats=stats_list, # Pass the list of stats
            method=method,
            index=True, # Use the geometry index
        )
        df = convert_ds_to_df(ds=ds, id_column=index_name)
        return df
    except Exception as e:
        logger.error(f"Error in task_xvec_zonal_stats: {e}", exc_info=True)
        # Return an empty dataframe or re-raise depending on desired error handling
        return pd.DataFrame()


def zonal_aggregation_point(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    data_vars: List[str],
    id_column: str,
) -> pd.DataFrame:
    """Extract values at point locations for specified data variables."""
    logger.info(f"Extracting point values for {len(infra)} facilities for variables: {data_vars}")
    if infra.empty:
        logger.warning("Input GeoDataFrame for point extraction is empty.")
        return pd.DataFrame()
    if not data_vars:
         logger.warning("No data variables specified for point extraction.")
         return pd.DataFrame()

    # Select only the required variables
    climate_subset = climate[data_vars]

    try:
        # Ensure the GeoDataFrame index is set correctly
        if infra.index.name != id_column:
             logger.warning(f"Input GDF index name is '{infra.index.name}', expected '{id_column}'. Setting index.")
             infra = infra.set_index(id_column) # Keep column if needed elsewhere

        ds = climate_subset.xvec.extract_points(
            infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
        )
        df = convert_ds_to_df(ds=ds, id_column=id_column)
        logger.info(f"Successfully extracted point values for {len(df)} records.")
        return df
    except Exception as e:
        logger.exception(f"Error during point extraction: {e}")
        return pd.DataFrame()

def zonal_aggregation_polygon(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame, # Expecting buffered polygons
    x_dim: str,
    y_dim: str,
    zonal_agg_methods: List[str], # Expecting ['mean', 'max']
    data_vars: List[str], # Specify variables for zonal stats
    id_column: str,
) -> pd.DataFrame:
    """Perform zonal aggregation on polygon geometries using parallel processing."""
    logger.info(f"Starting zonal aggregation ({', '.join(zonal_agg_methods)}) for {len(infra)} polygons for variables: {data_vars}")
    if infra.empty:
        logger.warning("Input GeoDataFrame for polygon aggregation is empty.")
        return pd.DataFrame()
    if not data_vars:
        logger.warning("No data variables specified for polygon aggregation.")
        return pd.DataFrame()
    
    # Set index correctly before splitting
    if infra.index.name != id_column:
         logger.warning(f"Input GDF index name is '{infra.index.name}', expected '{id_column}'. Setting index.")
         infra = infra.set_index(id_column, drop=False) # Keep column if needed elsewhere

    ds = climate.xvec.zonal_stats(
            infra.geometry,
            x_coords=x_dim,
            y_coords=y_dim,
            stats=RADIUS_STATS, # Pass the list of stats
            method='exactextract',
            index=True, # Use the geometry index
        )
    df_polygon = convert_ds_to_df(ds=ds, id_column=id_column)
    # If index was lost somehow during concat, try resetting
    if id_column not in df_polygon.columns and id_column != df_polygon.index.name:
         df_polygon = df_polygon.reset_index()
         if 'index' in df_polygon.columns:
              df_polygon = df_polygon.rename(columns={'index': id_column})

    logger.info(f"Successfully aggregated polygon values for {len(df_polygon)} records.")
    return df_polygon

def create_buffer_gdfs_vectorized(infra_gdf: gpd.GeoDataFrame, radius_miles_list: List[int], id_column: str) -> Dict[int, gpd.GeoDataFrame]:
    """
    Creates GeoDataFrames with buffered polygons for each radius using vectorized operations.
    Assumes infra_gdf is in WGS84_CRS.
    """
    logger.info(f"Creating vectorized buffers for radii: {radius_miles_list} miles.")
    buffer_gdfs = {}
    original_crs = infra_gdf.crs
    projected_crs = "EPSG:2285" # NAD83 / Washington North (meters)

    if not original_crs:
         logger.warning("Input GDF CRS is not set. Assuming WGS84 (EPSG:4326).")
         infra_gdf.crs = WGS84_CRS # Set CRS if missing
         original_crs = WGS84_CRS
    elif str(original_crs).upper() != WGS84_CRS:
         logger.warning(f"Input GDF CRS is {original_crs}. Re-projecting to {WGS84_CRS} before buffering.")
         infra_gdf = infra_gdf.to_crs(WGS84_CRS)
         original_crs = WGS84_CRS


    try:
        # Project ONCE to the projected CRS for buffering
        infra_proj = infra_gdf.to_crs(projected_crs)
        logger.info(f"Projected {len(infra_proj)} facilities to {projected_crs} for buffering.")

        for radius in radius_miles_list:
            start_time = time.time()
            radius_m = miles_to_meters(radius)

            # Create a copy to store the buffered geometry for this radius
            buffer_gdf_proj = infra_proj[[id_column, GEOMETRY_COLUMN]].copy()

            # Buffer ONCE (vectorized, in meters)
            buffer_gdf_proj[GEOMETRY_COLUMN] = infra_proj.geometry.buffer(radius_m)

            # Project back ONCE to the original CRS (WGS84)
            buffer_gdf = buffer_gdf_proj.to_crs(original_crs)

            # Add radius identifier
            buffer_gdf[f'radius_miles'] = radius
            buffer_gdfs[radius] = buffer_gdf
            logger.info(f"Created {radius}-mile buffer GDF in {time.time() - start_time:.2f} seconds.")

    except Exception as e:
        logger.exception(f"Error during vectorized buffer creation: {e}")
        # Depending on requirements, could return partial results or raise error
        return {} # Return empty dict on failure

    return buffer_gdfs

def main():
    """Main function to process and analyze wildfire risk data"""
    start_time_main = time.time()
    logger.info("Starting wildfire risk analysis script.")

    # --- 1. Load Facility Data ---
    try:
        infra_df = pd.read_csv(FACILITY_CSV_PATH)
        logger.info(f"Loaded facility data from {FACILITY_CSV_PATH}")
        
        # Validate ID_COLUMN existence
        if ID_COLUMN not in infra_df.columns:
             logger.error(f"ID Column '{ID_COLUMN}' not found in {FACILITY_CSV_PATH}. Please check column names.")
             return # Stop execution
        
        # Ensure ID column is suitable as index (unique, non-null)
        if not infra_df[ID_COLUMN].is_unique:
             logger.warning(f"Values in ID column '{ID_COLUMN}' are not unique. Merging might produce unexpected results.")
        if infra_df[ID_COLUMN].isnull().any():
             logger.error(f"ID column '{ID_COLUMN}' contains null values. Cannot proceed.")
             return

        infra_gdf = gpd.GeoDataFrame(
            infra_df,
            geometry=gpd.points_from_xy(x=infra_df["longitude"], y=infra_df["latitude"]),
            crs=WGS84_CRS # Assume input lat/lon is WGS84
        )
        # Set the index to the specified ID column *early*
        #infra_gdf = infra_gdf.set_index(ID_COLUMN, drop=False) # Keep column for merging later
        logger.info(f"Created GeoDataFrame for {len(infra_gdf)} facilities with index '{ID_COLUMN}'.")

    except FileNotFoundError:
        logger.error(f"Facility data file not found: {FACILITY_CSV_PATH}")
        return
    except Exception as e:
        logger.exception(f"Error loading or processing facility data: {e}")
        return


    # --- 3. Extract Point Values (FWI, Burn Prob at Location) ---
    logger.info("Starting point value extraction for facility locations...")
    point_df = zonal_aggregation_point(
        climate=DS_FWI,
        infra=infra_gdf, # Pass GDF with correct index
        x_dim='lon',
        y_dim='lat',
        data_vars=POINT_DATA_VARS, # Extract all point vars
        id_column=ID_COLUMN
    )
    point_df = point_df.fillna(0)
    # Rename point value columns for clarity
    point_rename_map = {'value_mean': 'fire_weather_index_ensemble_mean'}
    point_df = point_df.rename(columns=point_rename_map)
    logger.info(f"Finished point extraction. Result columns: {point_df.columns.tolist()}")


    # --- 4. Create Buffers ---
    # Use the more efficient vectorized approach
    buffer_gdfs = create_buffer_gdfs_vectorized(infra_gdf, RADIUS_MILES, id_column=ID_COLUMN)
    # buffer_gdfs is a dict: {5: GeoDataFrame, 10: GeoDataFrame, 25: GeoDataFrame}


    # --- 5. Process Each Radius (Zonal Stats for Burn Probability) ---
    # Select only the required variables and compute *once* before parallelizing
    radius_results_dict = {} # Store results DataFrames for each radius
    for radius in RADIUS_MILES:
        radius_start_time = time.time()
        logger.info(f"--- Starting zonal aggregation for {radius}-mile radius ---")

        if radius not in buffer_gdfs or buffer_gdfs[radius].empty:
             logger.warning(f"Buffer GDF for radius {radius} is missing or empty. Skipping.")
             continue # Skip to next radius if buffer creation failed

        buffer_gdf_for_radius = buffer_gdfs[radius]
        # Ensure index is correct before passing to aggregation
        buffer_gdf_for_radius = buffer_gdf_for_radius.set_index(ID_COLUMN, drop=False)
        months_results_list = []
        for month in DS_BURN_PROBABILTY['decade_month'].values:
            climate_subset = DS_BURN_PROBABILTY.sel(decade_month=month)[RADIUS_DATA_VARS].compute()
            # Perform zonal aggregation for MEAN and MAX in one go, only for RADIUS_DATA_VARS
            stats_df = zonal_aggregation_polygon(
                climate=climate_subset, # Pass the full dataset, selection happens inside
                infra=buffer_gdf_for_radius,
                x_dim='x',
                y_dim='y',
                zonal_agg_methods=RADIUS_STATS, # ['mean', 'max']
                data_vars=RADIUS_DATA_VARS, # Only 'burn_probability_*'
                id_column=ID_COLUMN
            )

            stats_df = stats_df.fillna(0)
            if stats_df.empty:
                logger.warning(f"Zonal aggregation for radius {radius} returned no results. Skipping merge for this radius.")
                continue

            # --- RENAME COLUMNS HERE --- (Moved renaming outside inner loop)
            rename_map = {"burn_probability_value_mean": "burn_probability"}
            stats_df = stats_df.rename(columns=rename_map)
            months_results_list.append(stats_df)
            del climate_subset
            gc.collect()
        months_df = pd.concat(months_results_list)
        radius_results_dict[radius] = months_df
        logger.info(f"--- Finished processing {radius}-mile radius in {time.time() - radius_start_time:.2f} seconds ---")

    # --- 8. Combine All Results ---
    logger.info("Combining all results...")

    # Start with the original facility attributes (non-spatial)
    # Keep only necessary original columns + ID
    # final_df = infra_df[[ID_COLUMN, 'original_attribute1', 'original_attribute2']].copy()
    final_df = infra_df.copy() # Or select specific columns if needed
    logger.info(f"Starting merge with {len(final_df)} original facility records.")
    
    # Ensure ID_COLUMN is the index for merging distance data
    if final_df.index.name != ID_COLUMN:
        final_df = final_df.set_index(ID_COLUMN)
        logger.info(f"Merged substation distances. DataFrame shape: {final_df.shape}")

    # Merge requires resetting index if merging on columns including month
    final_df = final_df.reset_index()
    

    # Add point values (merge on ID_COLUMN and month)
    if not point_df.empty:
        merge_cols_point = [ID_COLUMN]
        
        if all(col in final_df.columns for col in merge_cols_point):
             final_df = pd.merge(final_df, point_df, on=merge_cols_point, how='left')
             logger.info(f"Merged point data. DataFrame shape: {final_df.shape}")
        else:
             logger.warning(f"Cannot merge point data. Missing one or more merge columns ({merge_cols_point}) in final_df.")
             # Add NaN columns for point data if merge fails?
             for col in point_rename_map.values(): final_df[col] = np.nan

    else:
        logger.warning("Skipping merge of point data (data was empty).")
        # Add NaN columns if data was expected
        for col in point_rename_map.values(): final_df[col] = np.nan

    # Add radius-based values (merge on ID_COLUMN and month)
    for radius, radius_df in radius_results_dict.items():
        if not radius_df.empty:
            merge_cols_radius = [ID_COLUMN]
            if 'month' in radius_df.columns:
                 merge_cols_radius.append('month')
            
            if all(col in final_df.columns for col in merge_cols_radius):    
                final_df = pd.merge(final_df, radius_df, on=merge_cols_radius, how='left')
                logger.info(f"Merged radius {radius} data. DataFrame shape: {final_df.shape}")
            else:
                 logger.warning(f"Cannot merge radius {radius} data. Missing one or more merge columns ({merge_cols_radius}) in final_df.")
                 # Add NaN columns for this radius data if merge fails?

        else:
            logger.warning(f"Skipping merge for radius {radius} (data was empty).")
            # Optionally add NaN columns for this radius

    # --- 9. Save the Results ---
  
    # Check for potential duplicate columns before saving (can happen if merges go wrong)
    final_columns = final_df.columns
    if len(final_columns) != len(set(final_columns)):
            logger.warning(f"Duplicate columns found in final DataFrame: {[col for col in final_columns if list(final_columns).count(col) > 1]}")
            # Could implement logic to drop duplicates here if needed
            # final_df = final_df.loc[:,~final_df.columns.duplicated()]


    logger.info(f"Saving final results ({final_df.shape[0]} rows, {final_df.shape[1]} columns) to CSV")
    
    
    # Save to Excel format as requested
    final_df.to_csv(OUTPUT_CSV_PATH, index=False)
    
    total_time = time.time() - start_time_main
    logger.info(f"Analysis complete. Data saved successfully in {total_time:.2f} seconds.")

if __name__ == "__main__":
    # Ensure necessary directories exist
    os.makedirs("data", exist_ok=True)
    
    # Run the main analysis
    main()