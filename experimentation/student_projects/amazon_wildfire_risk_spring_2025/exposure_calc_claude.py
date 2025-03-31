import concurrent.futures as cf
import logging
import os
from typing import Dict, List, Tuple
import numpy as np

import geopandas as gpd
import pandas as pd
import xarray as xr
import xvec
from shapely.geometry import Point
import rioxarray
from shapely.ops import transform
import pyproj
from functools import partial

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Infrastructure return data should have two columns, id and geometry
ID_COLUMN = "osm_id"
GEOMETRY_COLUMN = "geometry"
S3_BUCKET = os.environ["S3_BUCKET"]

DATA_VARS = ['burn_probability_current', 'burn_probability_future_2030', 'fwi_current', 'fwi_future_2030']

# Define radius distances (in miles)
RADIUS_MILES = [5, 10, 25]

def miles_to_meters(miles):
    """Convert miles to meters"""
    return miles * 1609.34

def create_buffer_polygon(point, radius_miles):
    """Create a circular buffer polygon around a point with radius in miles"""
    # Convert the point to a projected CRS for accurate buffering
    # Using an equal-area projection suitable for the area of interest (Washington State)
    wgs84 = pyproj.CRS('EPSG:4326')
    # NAD83 / Washington North (meters)
    wash_proj = pyproj.CRS('EPSG:2285')
    
    # Create the transformer
    project = pyproj.Transformer.from_crs(wgs84, wash_proj, always_xy=True).transform
    
    # Transform the point to the projected CRS
    point_proj = transform(project, point)
    
    # Create the buffer in the projected CRS (in meters)
    buffer_proj = point_proj.buffer(miles_to_meters(radius_miles))
    
    # Transform the buffer back to WGS84
    project_back = pyproj.Transformer.from_crs(wash_proj, wgs84, always_xy=True).transform
    buffer_wgs84 = transform(project_back, buffer_proj)
    
    return buffer_wgs84

def convert_ds_to_df(ds: xr.Dataset) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe."""
    df = (
        ds.to_dataframe()
        .reset_index()[[ID_COLUMN, "month"] + DATA_VARS]
    )
    return df

def task_xvec_zonal_stats(
    climate: xr.Dataset,
    geometry,
    x_dim,
    y_dim,
    zonal_agg_method,
    method,
    index,
) -> pd.DataFrame:
    """Used for running xvec.zonal_stats in parallel process pool."""
    ds = climate.xvec.zonal_stats(
        geometry,
        x_coords=x_dim,
        y_coords=y_dim,
        stats=zonal_agg_method,
        method=method,
        index=index,
    )
    df = convert_ds_to_df(ds=ds)
    return df

def zonal_aggregation_point(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
) -> pd.DataFrame:
    """Extract values at point locations"""
    ds = climate.xvec.extract_points(
        infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
    )
    df = convert_ds_to_df(ds=ds)
    return df

def zonal_aggregation_polygon(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str,
) -> pd.DataFrame:
    """Perform zonal aggregation on polygon geometries"""
    climate_computed = climate.compute()  # Compute data before parallel processing
    
    # Parallelize the zonal aggregation
    workers = min(os.cpu_count(), len(infra.geometry))
    futures = []
    results = []
    geometry_chunks = np.array_split(infra.geometry, workers)
    
    with cf.ProcessPoolExecutor(max_workers=workers) as executor:
        for i in range(len(geometry_chunks)):
            futures.append(
                executor.submit(
                    task_xvec_zonal_stats,
                    climate_computed,
                    geometry_chunks[i],
                    x_dim,
                    y_dim,
                    zonal_agg_method,
                    "exactextract",
                    True,
                )
            )
        cf.as_completed(futures)
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                logger.info(
                    f"Future result in zonal agg process pool could not be appended: {str(e)}"
                )

    df_polygon = pd.concat(results) if results else pd.DataFrame()
    return df_polygon

def zonal_aggregation(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str
) -> pd.DataFrame:
    """Performs zonal aggregation on climate data and infrastructure data."""
    point_geom_types = ["Point", "MultiPoint"]
    polygon_geom_types = ["Polygon", "MultiPolygon"]

    polygon_infra = infra.loc[infra.geom_type.isin(polygon_geom_types)]
    point_infra = infra.loc[infra.geom_type.isin(point_geom_types)]

    results = []
    
    # Process point geometries if any exist
    if not point_infra.empty:
        point_infra = point_infra.set_index(ID_COLUMN)
        df_point = zonal_aggregation_point(
            climate=climate,
            infra=point_infra,
            x_dim=x_dim,
            y_dim=y_dim
        )
        logger.info("Point geometries intersected successfully")
        results.append(df_point)

    # Process polygon geometries if any exist
    if not polygon_infra.empty:
        polygon_infra = polygon_infra.set_index(ID_COLUMN)
        df_polygon = zonal_aggregation_polygon(
            climate=climate,
            infra=polygon_infra,
            x_dim=x_dim,
            y_dim=y_dim,
            zonal_agg_method=zonal_agg_method,
        )
        logger.info("Polygon geometries intersected successfully")
        results.append(df_polygon)

    # Combine results if any exist
    df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    if GEOMETRY_COLUMN in df.columns:
        df.drop(GEOMETRY_COLUMN, inplace=True, axis=1)

    return df

def create_buffer_gdfs(infra_gdf, radius_miles_list):
    """Create GeoDataFrames with buffered polygons for each radius"""
    buffer_gdfs = {}
    
    for radius in radius_miles_list:
        # Create a copy of the original GDF
        buffer_gdf = infra_gdf.copy()
        
        # Create buffer polygons
        buffer_gdf['geometry'] = buffer_gdf.apply(
            lambda row: create_buffer_polygon(row.geometry, radius), 
            axis=1
        )
        
        # Add radius identifier to column names
        buffer_gdf[f'radius_miles'] = radius
        
        buffer_gdfs[radius] = buffer_gdf
    
    return buffer_gdfs

def calculate_distance_to_nearest_firestation(infra_gdf, firestations_gdf):
    """Calculate the distance from each facility to the nearest fire station in miles"""
    # Ensure both GDFs are in the same CRS
    if infra_gdf.crs != firestations_gdf.crs:
        firestations_gdf = firestations_gdf.to_crs(infra_gdf.crs)
    
    # Function to find distance to nearest fire station
    def nearest_firestation(point):
        # Calculate distances to all fire stations
        distances = firestations_gdf.geometry.apply(lambda x: point.distance(x))
        # Convert to miles (approximation for WGS84)
        min_distance_degrees = distances.min()
        # For small distances, 1 degree ≈ 69 miles at equator
        # This is an approximation; for more accuracy, use geodesic calculations
        min_distance_miles = min_distance_degrees * 69
        return min_distance_miles
    
    # Apply function to each facility
    infra_gdf['Distance_to_FireStation_mi'] = infra_gdf.geometry.apply(nearest_firestation)
    
    return infra_gdf[[ID_COLUMN, 'Distance_to_FireStation_mi']]

def main():
    """Main function to process and analyze wildfire risk data"""
    # Load facility data
    infra_df = pd.read_csv("data/amazon_facilities_eastern_washington.csv")
    infra_gdf = gpd.GeoDataFrame(
        infra_df, 
        geometry=gpd.points_from_xy(x=infra_df["longitude"], y=infra_df["latitude"]), 
        crs="EPSG:4326"
    )
    
    # Load climate data
    climate_ds = xr.load_dataset(
        f"s3://{S3_BUCKET}/student-projects/amazon-wildfire-risk-spring2025/data/cmip6_adjusted_burn_probability.zarr"
    )
    logger.info("Climate data loaded successfully")
    
    # Create buffers for different radii
    buffer_gdfs = create_buffer_gdfs(infra_gdf, RADIUS_MILES)
    
    # Dictionary to store results for each radius
    radius_results = {}
    
    # Get direct point values (no radius)
    logger.info("Starting point extraction for facility locations...")
    point_df = zonal_aggregation(
        climate=climate_ds,
        infra=infra_gdf,
        x_dim="x",
        y_dim="y",
        zonal_agg_method='max'  # Not used for points, but required parameter
    )
    
    # Rename columns to indicate these are direct point values
    for var in climate_ds.data_vars:
        point_df = point_df.rename(columns={var: f"{var}_point"})
    
    # Process each radius
    for radius in RADIUS_MILES:
        logger.info(f"Starting zonal aggregation for {radius}-mile radius...")
        
        # Calculate max values within radius
        max_df = zonal_aggregation(
            climate=climate_ds,
            infra=buffer_gdfs[radius],
            x_dim="x",
            y_dim="y",
            zonal_agg_method='max'
        )
        
        # Calculate mean values within radius
        mean_df = zonal_aggregation(
            climate=climate_ds,
            infra=buffer_gdfs[radius],
            x_dim="x",
            y_dim="y",
            zonal_agg_method='mean'
        )
        
        # Rename columns to indicate radius and statistic
        for var in climate_ds.data_vars:
            if 'burn_probability' in var:
                max_df = max_df.rename(columns={var: f"Max_{var}_{radius}mi"})
                mean_df = mean_df.rename(columns={var: f"Avg_{var}_{radius}mi"})
        
        # Merge max and mean results
        radius_df = pd.merge(max_df, mean_df, on=[ID_COLUMN, 'month'])
        radius_results[radius] = radius_df
    
    # Load fire station data and calculate distances
    try:
        firestations_gdf = gpd.read_file("data/firestations.geojson")
        logger.info("Fire station data loaded successfully")
        
        # Calculate distance to nearest fire station
        firestation_distance_df = calculate_distance_to_nearest_firestation(infra_gdf, firestations_gdf)
    except Exception as e:
        logger.warning(f"Error loading fire station data: {str(e)}")
        logger.warning("Creating placeholder for Distance_to_FireStation_mi")
        # Create placeholder if fire station data is not available
        firestation_distance_df = pd.DataFrame({
            ID_COLUMN: infra_gdf[ID_COLUMN],
            'Distance_to_FireStation_mi': np.nan
        })
    
    # Combine all results
    final_df = infra_df.copy()
    
    # Add point values
    final_df = pd.merge(final_df, point_df, on=ID_COLUMN, how='left')
    
    # Add radius-based values
    for radius in RADIUS_MILES:
        final_df = pd.merge(final_df, radius_results[radius], on=ID_COLUMN, how='left')
    
    # Add fire station distances
    final_df = pd.merge(final_df, firestation_distance_df, on=ID_COLUMN, how='left')
    
    # Save the results
    final_df.to_csv("data/amazon_facilities_with_detailed_fire_exposure.csv", index=False)
    logger.info("Analysis complete - data saved to amazon_facilities_with_detailed_fire_exposure.csv")

if __name__ == "__main__":
    main()