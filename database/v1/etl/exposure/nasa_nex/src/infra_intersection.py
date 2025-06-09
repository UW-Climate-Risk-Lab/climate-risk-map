import concurrent.futures as cf
import logging
import os
import json
from typing import Dict, List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2 as pg
import psycopg2.sql as sql
import xarray as xr
import xvec
from shapely import wkt, Point, LineString, MultiLineString

import src.utils as utils
import src.constants as constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Infrastructure return data should have two columns, id and geometry
# 'id' column refers to a given feature's unique id. This is the OpenStreetMap ID for the PG OSM Flex
ID_COLUMN = "osm_id"
GEOMETRY_COLUMN = "geometry"


def convert_small_polygons(
    gdf: gpd.GeoDataFrame, polygon_area_threshold: float
) -> gpd.GeoDataFrame:
    """
    Converts polygons with area less than the threshold to points (centroids).

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame containing geometries
        polygon_area_threshold (float): Area threshold in square kilometers

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with small polygons converted to points
    """
    # Create a copy to avoid modifying the original
    result_gdf = gdf.copy()

    # Filter only polygon geometries
    polygon_mask = result_gdf.geom_type.isin(["Polygon", "MultiPolygon"])

    if polygon_mask.any():
        # Get polygons
        polygons = result_gdf[polygon_mask]

        # Create a temporary GDF in a suitable projected CRS for area calculation
        temp_gdf = polygons.copy()
        orig_crs = temp_gdf.crs

        # Convert to appropriate CRS for area calculation if needed
        if orig_crs.is_geographic:
            # Use Equal Area projection for area calculation. This will be fairly accurate but not exact, only need estimate
            center = polygons.unary_union.centroid
            proj_crs = f"+proj=aea +lat_1=10 +lat_2=60 +lat_0={center.y} +lon_0={center.x} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
            temp_gdf = temp_gdf.to_crs(proj_crs)

        # Calculate areas in square kilometers
        # If CRS is in meters, divide by 1,000,000 to get sq km
        area_conv_factor = 1_000_000  # Default: from sq meters to sq km

        # Check for other units
        if not orig_crs.is_geographic:  # Only check if it's a projected CRS
            unit = orig_crs.axis_info[0].unit_name.lower()
            if unit in ["foot", "feet", "ft"]:
                area_conv_factor = 10_763_910.4  # from sq feet to sq km
            elif unit not in ["metre", "meter", "m"]:
                logger.warning(f"Unsupported CRS unit: {unit}. Assuming meters.")

        # Calculate areas and identify small polygons
        small_indices = temp_gdf.index[
            temp_gdf.area / area_conv_factor < polygon_area_threshold
        ]

        # Convert small polygons to centroids
        if len(small_indices) > 0:
            result_gdf.loc[small_indices, GEOMETRY_COLUMN] = polygons.loc[
                small_indices
            ].centroid
            logger.info(
                f"Converted {len(small_indices)} small polygons to points (area < {polygon_area_threshold} sq km)"
            )

    return result_gdf


def convert_ds_to_df_decade_month(ds: xr.Dataset) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe for decade_month datasets. This should be called after zonal aggregation with xvec

    Used since we ultimately want the data in tabular form for PostGIS.

    Args:
        da (xr.DataArray): Datarray
    """

    # We know that that each ID has a geometry associated with
    # We want to drop the geometry values (ran into error with converting to dataframe with geometry type that i couldnt resolve)
    # Solution was replace geometry values with id column values (we do not need geometries after this)
    ds_modified = (ds.set_index({GEOMETRY_COLUMN: ID_COLUMN})
                   .rename_dims({GEOMETRY_COLUMN: ID_COLUMN})
                   .drop_vars([GEOMETRY_COLUMN])
                   .assign_coords({ID_COLUMN: ds[ID_COLUMN].values}))

    df = (
        ds_modified.to_dataframe()
        .reset_index()[
            [ID_COLUMN, "decade_month"] + list(ds.data_vars)
        ]
    )

    df["decade"] = df["decade_month"].apply(lambda x: int(x[0:4]))
    df["month"] = df["decade_month"].apply(lambda x: int(x[-2:]))
    df.drop(columns=["decade_month"], inplace=True)

    return df


def convert_ds_to_df_year_span_month(ds: xr.Dataset) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe for year_span_month datasets. This should be called after zonal aggregation with xvec
    
    Args:
        ds (xr.Dataset): Dataset with dimensions (osm_id, month, start_year, end_year)
    
    Returns:
        pd.DataFrame: A formatted DataFrame.
    """
    ds_modified = (ds.set_index({GEOMETRY_COLUMN: ID_COLUMN})
                   .rename_dims({GEOMETRY_COLUMN: ID_COLUMN})
                   .drop_vars([GEOMETRY_COLUMN])
                   .assign_coords({ID_COLUMN: ds[ID_COLUMN].values}))
    
    df = ds_modified.to_dataframe().reset_index()
    
    df = df.rename(columns={"month_of_year", "month"})
    
    return df


def convert_ds_to_df(ds: xr.Dataset, time_period_type: str) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe. This should be called after zonal aggregation with xvec"""
    if time_period_type == "decade_month":
        return convert_ds_to_df_decade_month(ds)
    elif time_period_type == "year_span_month":
        return convert_ds_to_df_year_span_month(ds)
    else:
        raise ValueError(f"Unsupported time_period_type: {time_period_type}")


def task_xvec_zonal_stats(
    climate: xr.Dataset,
    geometry,
    x_dim,
    y_dim,
    zonal_agg_method,
    method,
    index,
    time_period_type: str
) -> pd.DataFrame:
    """Used for running xvec.zonal_stats in parallel process pool. Param types are the same
    as xvec.zonal_stats().

    Note, there may be a warning that spatial references systems between
    input features do not match. Under the hood, xvec uses exeactextract,
    which does a simple check on the CRS attribute of each dataset.
    If the attributes are not identical, it gives an error.

    In this pipeline, we set the CRS as an ENV variable and make sure all
    imported data is loaded/transformed in this CRS. From manual debugging and
    checking attributes, it seems the CRS attribute strings showed the same CRS,
    but the string values were not identical. So ignoring the warning was okay.

    Returns:
        pd.DataFrame: DataFrame in format of convert_da_to_df()
    """

    ds = climate.xvec.zonal_stats(
        geometry,
        x_coords=x_dim,
        y_coords=y_dim,
        stats=zonal_agg_method,
        method=method,
        index=index,
    )

    df = convert_ds_to_df(ds=ds, time_period_type=time_period_type)

    return df


def zonal_aggregation_point(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    time_period_type: str
) -> pd.DataFrame:
    ds = climate.xvec.extract_points(
        infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
    )

    df = convert_ds_to_df(ds=ds, time_period_type=time_period_type)
    return df


def zonal_aggregation_linestring(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    time_period_type: str,
) -> pd.DataFrame:
    """Linestring cannot be zonally aggreated, so must be broken into points"""

    sampled_points = []
    for idx, row in infra.iterrows():
        line = row[GEOMETRY_COLUMN]  # type == shapely.LineString
        points = list(line.coords)
        sampled_points.extend([(idx, Point(point)) for point in points])

    if sampled_points:
        df_sampled_points = pd.DataFrame(
            sampled_points, columns=[ID_COLUMN, GEOMETRY_COLUMN]
        )
        gdf_sampled_points = gpd.GeoDataFrame(
            df_sampled_points, geometry=GEOMETRY_COLUMN, crs=infra.crs
        ).set_index(ID_COLUMN)
        ds_linestring_points = climate.xvec.extract_points(
            gdf_sampled_points.geometry, x_coords=x_dim, y_coords=y_dim, index=True
        )
        df_linestring = convert_ds_to_df(ds=ds_linestring_points, time_period_type=time_period_type)

        # TODO: At this step, we are left with OSM ids broken out into individual points.
        # Depending on the resolution of the climate dataset, there will be different exposure measures
        # along the entire linestring. Different segments of the same line will have different climate exposures,
        # while the entire line is considered one entity. For the time being, I am taking some simple means, mins, and max
        # to move forward with development. In the future, the ideal solution is to break up each osm_id into multiple line segments.
        # We can determine the segments by seeing where the mean, median, etc... values change along the points. We can group these series of points into multiple linestrings,
        # and then store the linestring segments in the database with their individual exposure values. A single osm id may be comprised of between 1 and N line segments.
        # The downside to this is extra segment geometries will need to be stored, possibly in their own tables. This also creates the eventual output dataset to the user more complicated
        # as a single entity may now have multiple records of exposure, one for each line segment.
        
        grouping_cols = [ID_COLUMN, "decade", "month"]
        if time_period_type == "year_span_month":
            grouping_cols = [ID_COLUMN, "month", "start_year", "end_year"]

        df_linestring = (
            df_linestring.drop_duplicates()
            .groupby(grouping_cols)
            .agg(
                {
                    "ensemble_mean": "mean",
                    "ensemble_median": "mean",
                    "ensemble_stddev": "mean",
                    "ensemble_min": "min",
                    "ensemble_max": "max",
                    "ensemble_q1": "min",
                    "ensemble_q3": "max",
                }
            )
            .reset_index()
        )
    else:
        df_linestring = pd.DataFrame()
    return df_linestring


def simplify_and_extract_points_from_geom(
    geom, tolerance, id_val, id_column, geometry_column
):
    """
    Helper function to simplify a single geometry (LineString)
    and extract points. Handles potential MultiLineStrings after simplification.

    Args:
        geom (BaseGeometry): A Shapely geometry object (expected LineString).
        tolerance (float): Simplification tolerance.
        id_val (any): The ID associated with this geometry.
        id_column (str): Name for the ID column in the output.
        geometry_column (str): Name for the geometry column in the output.

    Returns:
        list: A list of tuples, each containing (id_val, Point).
              Returns empty list if geometry is invalid or becomes empty after simplification.
    """
    # Input validation for the geometry object
    if (
        geom is None
        or geom.is_empty
        or not isinstance(geom, (LineString, MultiLineString))
    ):
        # Silently ignore non-linestring geometries or handle as needed
        return []

    points_data = []

    # Handle MultiLineString by iterating through its parts
    if isinstance(geom, MultiLineString):
        geoms_to_process = list(
            geom.geoms
        )  # Process each LineString within the MultiLineString
    else:  # It's a LineString
        geoms_to_process = [geom]

    for line_geom in geoms_to_process:
        if not isinstance(line_geom, LineString) or line_geom.is_empty:
            continue  # Skip invalid parts within a MultiLineString

        # Simplify the line. preserve_topology=True prevents self-intersection.
        # Apply simplification only if tolerance > 0
        simplified_geom = (
            line_geom.simplify(tolerance, preserve_topology=True)
            if tolerance > 0
            else line_geom
        )

        if simplified_geom.is_empty:
            continue  # Skip if simplification resulted in an empty geometry

        # Extract coordinates. Check if it's still a LineString after simplification.
        if isinstance(simplified_geom, LineString):
            coords = list(simplified_geom.coords)
            points_data.extend([(id_val, Point(p)) for p in coords])
        # Handle rare case where simplification might result in MultiLineString (e.g., complex self-touching lines)
        elif isinstance(simplified_geom, MultiLineString):
            for part in simplified_geom.geoms:
                if isinstance(part, LineString) and not part.is_empty:
                    coords = list(part.coords)
                    points_data.extend([(id_val, Point(p)) for p in coords])

    return points_data


def zonal_aggregation_linestring_optimized(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    time_period_type: str,
    simplify_tolerance: float = 0.0001,  # Key parameter: Adjust based on CRS units & desired accuracy vs speed. Set to 0 to disable.
    id_column: str = "osm_id",  # Ensure this matches the ID column in 'infra'
    geometry_column: str = "geometry",  # Ensure this matches the geometry column in 'infra'
) -> pd.DataFrame:
    """
    Optimized zonal aggregation for linestrings using simplification.

    This function simplifies linestrings before extracting climate data points,
    reducing the computational load of the extraction and aggregation steps.

    Args:
        climate (xr.Dataset): Climate data (e.g., temperature, precipitation)
                              with an `.xvec` accessor enabled. Must have dimensions
                              specified by `x_dim` and `y_dim`, and temporal info
                              (e.g., 'time' coord) for `convert_ds_to_df`.
        infra (gpd.GeoDataFrame): GeoDataFrame containing linestring geometries
                                  (e.g., roads, rivers). Must have a geometry
                                  column and a unique ID column. CRS must be
                                  compatible with `climate` data or projection handled
                                  implicitly by `xarray-vectorize`.
        x_dim (str): Name of the longitude/X dimension in the `climate` Dataset.
        y_dim (str): Name of the latitude/Y dimension in the `climate` Dataset.
        simplify_tolerance (float): Tolerance for `shapely.simplify` in the units
                                   of the `infra` GeoDataFrame's CRS. Larger values
                                   result in fewer points and faster processing, but
                                   less detail. A value of 0 disables simplification.
                                   Choose based on data resolution and requirements.
        id_column (str): Name of the column in `infra` containing unique identifiers
                         for each linestring feature.
        geometry_column (str): Name of the active geometry column in `infra`.

    Returns:
        pd.DataFrame: A DataFrame with aggregated climate statistics (mean, min, max)
                      grouped by the original linestring ID (`id_column`), 'decade',
                      and 'month'. Returns an empty DataFrame if no valid data
                      can be processed.

    Raises:
        ValueError: If `id_column` or `geometry_column` is not found in `infra`,
                    or if essential columns for processing are missing.
        AttributeError: If `climate` dataset lacks the `.xvec` accessor.
        ImportError: If required libraries (xarray, geopandas, pandas, shapely)
                     are not installed.
    """
    # --- Input Validation ---
    if not isinstance(infra, gpd.GeoDataFrame) or infra.empty:
        print("Input 'infra' must be a non-empty GeoDataFrame.")
        return pd.DataFrame()
    if not isinstance(climate, xr.Dataset):
        print("Input 'climate' must be an xarray Dataset.")
        return pd.DataFrame()

    # Validate climate dimensions
    if x_dim not in climate.dims and x_dim not in climate.coords:
        raise ValueError(
            f"x_dim '{x_dim}' not found in climate dataset dimensions or coordinates."
        )
    if y_dim not in climate.dims and y_dim not in climate.coords:
        raise ValueError(
            f"y_dim '{y_dim}' not found in climate dataset dimensions or coordinates."
        )

    # Validate ID column (check both index and columns)
    if infra.index.name == id_column:
        id_series = infra.index  # Get IDs from index
        if id_series.hasnans:
            logger.warning(
                f"ID column '{id_column}' (index) contains NaN values.", UserWarning
            )
        if not id_series.is_unique:
            logger.warning(
                f"ID column '{id_column}' (index) contains duplicate values.",
                UserWarning,
            )
    elif id_column in infra.columns:
        id_series = infra[id_column]  # Get IDs from column
        if id_series.isnull().any():
            logger.warning(f"ID column '{id_column}' contains NaN values.", UserWarning)
        if not id_series.is_unique:
            logger.warning(
                f"ID column '{id_column}' contains duplicate values.", UserWarning
            )
    else:
        raise ValueError(
            f"ID column '{id_column}' not found in GeoDataFrame index or columns: {infra.columns.tolist()}"
        )

    print(f"Processing {len(infra)} LineString infrastructure features...")
    print(
        f"Simplification tolerance: {simplify_tolerance if simplify_tolerance > 0 else 'Disabled'}"
    )

    # --- Step 1: Simplify Geometries and Extract Points ---
    all_points_data = []
    geometries = infra[geometry_column]  # Get geometry series

    # Use zip for efficient iteration over IDs and geometries
    for id_val, geom in zip(id_series, geometries):
        # Skip if ID is null/NaN
        if pd.isna(id_val):
            continue
        # Use helper function to simplify and extract points
        points = simplify_and_extract_points_from_geom(
            geom, simplify_tolerance, id_val, id_column, geometry_column
        )
        all_points_data.extend(points)

    if not all_points_data:
        print(
            "No valid points could be extracted from the geometries after simplification."
        )
        return pd.DataFrame()

    print(f"Extracted {len(all_points_data)} points from geometries.")

    # --- Step 2: Create GeoDataFrame from Sampled Points ---
    try:
        df_sampled_points = pd.DataFrame(
            all_points_data,
            columns=[id_column, geometry_column],  # Use parameter names
        )
        # Important: Create GDF with correct CRS *before* setting index
        gdf_sampled_points = gpd.GeoDataFrame(
            df_sampled_points,
            geometry=geometry_column,  # Use parameter name
            crs=infra.crs,  # Inherit CRS from original data
        )
        # Set index AFTER creation for compatibility with extract_points
        # Use the specified id_column as the index name
        gdf_sampled_points = gdf_sampled_points.set_index(
            id_column, drop=True
        )  # Drop=True prevents id_column appearing twice

    except Exception as e:
        print(f"Error creating GeoDataFrame from sampled points: {e}")
        return pd.DataFrame()

    if gdf_sampled_points.empty:
        print("GeoDataFrame of sampled points is empty.")
        return pd.DataFrame()

    # --- Step 3: Extract Climate Data at Point Locations ---
    print("Extracting climate data at point locations...")
    try:
        # xarray-vectorize extracts data for each point using its index (id_column)
        ds_linestring_points = climate.xvec.extract_points(
            gdf_sampled_points.geometry,  # Pass the GeoDataFrame directly
            x_coords=x_dim,
            y_coords=y_dim,
            # index=gdf_sampled_points.index # Not needed, uses GDF index by default
        )

    except Exception as e:
        print(
            f"Error during climate data extraction (climate.xvec.extract_points): {e}"
        )
        print("Check CRS compatibility between climate data and infrastructure data.")
        print(
            f"Climate CRS (if available): {getattr(climate.rio, 'crs', 'Not spatial xarray/rio not available')}"
        )
        print(f"Infrastructure CRS: {infra.crs}")
        # Check if bounds overlap roughly (assuming similar CRS for bounds check)
        try:
            print(
                f"Climate bounds (approx): lon={climate[x_dim].min().item()}-{climate[x_dim].max().item()}, lat={climate[y_dim].min().item()}-{climate[y_dim].max().item()}"
            )
            print(f"Sample points bounds: {gdf_sampled_points.total_bounds}")
        except Exception as bounds_e:
            print(f"Could not determine bounds for comparison: {bounds_e}")
        return pd.DataFrame()

    # --- Step 4: Convert Extracted Data to DataFrame ---
    # Use the integrated and adapted function
    df_linestring = convert_ds_to_df(ds=ds_linestring_points, time_period_type=time_period_type)

    if df_linestring.empty:
        print(
            "DataFrame is empty after converting extracted climate data (convert_ds_to_df returned empty). Check logs."
        )
        return pd.DataFrame()

    # --- Step 5: Aggregate Results ---
    print("Aggregating results...")
    # Define the aggregations based on the original logic
    # Consider if 'mean' of median/stddev is appropriate for your analysis.
    agg_dict = {
        "ensemble_mean": "mean",
        "ensemble_median": "mean",  # Mean of point medians along the line
        "ensemble_stddev": "mean",  # Mean of point stddevs along the line
        "ensemble_min": "min",  # Minimum value found at any point along the line
        "ensemble_max": "max",  # Maximum value found at any point along the line
        "ensemble_q1": "min",  # Minimum lower quantile (Q1) along the line
        "ensemble_q3": "max",  # Maximum upper quantile (Q3) along the line
    }

    # Filter aggregation dict to only include columns present in the DataFrame
    agg_dict_filtered = {
        k: v for k, v in agg_dict.items() if k in df_linestring.columns
    }

    if not agg_dict_filtered:
        print(
            f"Error: None of the target ensemble columns found for aggregation in the DataFrame. "
            f"Available columns: {df_linestring.columns.tolist()}"
        )
        return pd.DataFrame()
    else:
        print(f"Aggregating columns: {list(agg_dict_filtered.keys())}")

    # Define grouping columns
    grouping_cols = [id_column, "decade", "month"]
    if time_period_type == "year_span_month":
        grouping_cols = [id_column, "month", "start_year", "end_year"]

    # We already checked these columns exist in convert_ds_to_df, but double-check
    missing_group_cols = [
        col for col in grouping_cols if col not in df_linestring.columns
    ]
    if missing_group_cols:
        # This shouldn't happen if convert_ds_to_df worked correctly
        print(
            f"Critical Error: Missing required grouping columns post-conversion: {missing_group_cols}. DataFrame columns: {df_linestring.columns.tolist()}"
        )
        return pd.DataFrame()

    try:
        # Group by the original linestring ID, decade, and month
        # `observed=True` can improve performance if grouping keys are like categories
        # `dropna=False` would include groups with NA keys, but we dropped NAs earlier
        grouped = df_linestring.groupby(grouping_cols, observed=True, dropna=True)

        df_aggregated = grouped.agg(agg_dict_filtered)

        # Optional: Add a count of unique points contributing to each aggregate
        # df_aggregated['point_count'] = grouped.size()

        df_aggregated = df_aggregated.reset_index()
        print("Aggregation complete.")

    except KeyError as e:
        # Should be less likely now with checks, but handle just in case
        print(
            f"Error during aggregation: Missing column {e}. Ensure columns {grouping_cols} and ensemble variables exist."
        )
        print(f"Columns available for aggregation: {df_linestring.columns.tolist()}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred during aggregation: {e}")
        return pd.DataFrame()

    return df_aggregated


def zonal_aggregation_polygon(
    climate: xr.DataArray,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str,
    time_period_type: str,
) -> pd.DataFrame:
    # The following parallelizes the zonal aggregation of polygon geometry features
    # Limit workers for memory considerations.
    workers = min(os.cpu_count(), len(infra.geometry), 4)
    futures = []
    results = []
    geometry_chunks = np.array_split(infra.geometry, workers)

    with cf.ProcessPoolExecutor(max_workers=workers) as executor:
        for i in range(len(geometry_chunks)):
            futures.append(
                executor.submit(
                    task_xvec_zonal_stats,
                    climate,
                    geometry_chunks[i],
                    x_dim,
                    y_dim,
                    zonal_agg_method,
                    "exactextract",
                    True,
                    time_period_type,
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

    df_polygon = pd.concat(results)
    return df_polygon


def zonal_aggregation(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    zonal_agg_method: str,
    x_dim: str,
    y_dim: str,
    time_period_type: str,
    linestring_tolerance: float = 0.0001
) -> pd.DataFrame:
    """Performs zonal aggregation on climate data and infrastructure data.

    Data needs to be split up into point and non point geometries, as xvec
    uses 2 different methods to deal with the different geometries.

    NOTE, xvec_zonal_stats can be slow. This uses a method called exactextract,
    which is based on the package exactextract, which is a C++ zonal aggregation
    implementation. This loops through each feature sequentially to calculate the value.

    Because of this, we use a ProcessPoolExecutor and split up the infrastructure data
    into "chunks" and process each one in parallel.

    Args:
        climate (xr.DataSet): Climate data
        infra (gpd.GeoDataFrame): Infrastructure data
        zonal_agg_method (str): Zonal aggregation method
        x_dim (str): X dimension name
        y_dim (str): Y dimension name

    Returns:
        pd.DataFrame: Aggregated data
    """

    point_geom_types = ["Point", "MultiPoint"]
    line_geom_types = ["LineString", "MultiLineString"]
    polygon_geom_types = ["Polygon", "MultiPolygon"]

    line_infra = infra.loc[infra.geom_type.isin(line_geom_types)]
    polygon_infra = infra.loc[infra.geom_type.isin(polygon_geom_types)]
    point_infra = infra.loc[infra.geom_type.isin(point_geom_types)]

    if len(point_infra != 0):
        df_point = zonal_aggregation_point(
            climate=climate, infra=point_infra, x_dim=x_dim, y_dim=y_dim, time_period_type=time_period_type
        )
        logger.info("Point geometries intersected successfully")
    else:
        df_point = pd.DataFrame()

    if len(line_infra != 0):
        df_linestring = zonal_aggregation_linestring_optimized(
            climate=climate,
            infra=line_infra,
            x_dim=x_dim,
            y_dim=y_dim,
            time_period_type=time_period_type,
            simplify_tolerance=linestring_tolerance
        )

        logger.info("Lines geometries intersected successfully")
    else:
        df_linestring = pd.DataFrame()

    if len(polygon_infra != 0):
        df_polygon = zonal_aggregation_polygon(
            climate=climate,
            infra=polygon_infra,
            x_dim=x_dim,
            y_dim=y_dim,
            zonal_agg_method=zonal_agg_method,
            time_period_type=time_period_type,
        )

        logger.info("Polygon geometries intersected successfully")
    else:
        df_polygon = pd.DataFrame()

    # Applies the same method for converting from DataArray to DataFrame, and
    # combines the data back together.
    df = pd.concat(
        [df_point, df_linestring, df_polygon],
        ignore_index=True,
    )

    if GEOMETRY_COLUMN in df.columns:
        df.drop(GEOMETRY_COLUMN, inplace=True, axis=1)

    return df


def create_pgosm_flex_query(
    climate_variable: str, crs: str
) -> Tuple[sql.SQL, Tuple[str]]:
    """Creates SQL query to get all features of a given type from PG OSM Flex Schema

    Args:
        climate_variable (str): Climate variable being queried, determines table name
        crs (str): Coordinate Reference System

    Returns:
        Tuple[sql.SQL, Tuple[str]]: Query in SQL object and params of given query
    """
    schema = "osm"  # Always schema name in PG OSM Flex
    table = f"unexposed_ids_nasa_nex_{climate_variable}"

    query = sql.SQL(
        "SELECT main.osm_id AS {id}, ST_AsText(ST_Transform(main.geom, %s)) AS {geometry} FROM {schema}.{table} main"
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(table),
        id=sql.Identifier(ID_COLUMN),
        geometry=sql.Identifier(GEOMETRY_COLUMN),
    )
    params = [
        int(crs),
    ]

    return query, tuple(params)


def round_ensemble_columns(df: pd.DataFrame, decimal_places: int = 2) -> pd.DataFrame:
    """
    Rounds all columns in the DataFrame that start with 'ensemble_' to the specified decimal places.

    Args:
        df (pd.DataFrame): Input DataFrame containing ensemble columns
        decimal_places (int): Number of decimal places to round to (default: 2)

    Returns:
        pd.DataFrame: DataFrame with rounded ensemble columns
    """
    # Get all columns that start with 'ensemble_'
    ensemble_columns = [col for col in df.columns if col.startswith("ensemble_")]

    # Round these columns to the specified decimal places
    for col in ensemble_columns:
        df[col] = df[col].round(decimal_places)

    return df


def main(
    climate_ds: xr.Dataset,
    climate_variable: str,
    time_period_type: str,
    crs: str,
    zonal_agg_method: List[str] | str,
    polygon_area_threshold: float,  # Changed from point_only
    conn: pg.extensions.connection,
    metadata: Dict,
) -> pd.DataFrame:
    """Main function to perform climate and infrastructure intersection.

    Args:
        climate_ds (xr.Dataset): Climate dataset
        climate_variable (str): Climate variable being queried
        time_period_type (str): Type of dataset, e.g., 'decade_month' or 'year_span_month'
        crs (str): Coordinate Reference System
        zonal_agg_method (List[str] | str): Method(s) for zonal aggregation
        polygon_area_threshold (float): Threshold in square kilometers below which
                                       polygons will be converted to points
        conn (pg.extensions.connection): Database connection
        metadata (Dict): Metadata to be included in the output

    Returns:
        pd.DataFrame: DataFrame with aggregated climate data for infrastructure
    """
    query, params = create_pgosm_flex_query(climate_variable=climate_variable, crs=crs)
    infra_data = utils.query_db(query=query, params=params, conn=conn)

    infra_df = pd.DataFrame(infra_data, columns=[ID_COLUMN, GEOMETRY_COLUMN]).set_index(
        ID_COLUMN
    )
    num_features = len(infra_df)

    if num_features <= 0:
        logger.info("No OSM features need exposure calculation")
        return pd.DataFrame()

    logger.info(f"{str(num_features)} OSM features queried successfully")
    infra_df[GEOMETRY_COLUMN] = infra_df[GEOMETRY_COLUMN].apply(wkt.loads)
    infra_gdf = gpd.GeoDataFrame(infra_df, geometry=GEOMETRY_COLUMN, crs=crs)

    # Convert small polygons to points if threshold is provided
    if polygon_area_threshold is not None and polygon_area_threshold > 0:
        infra_gdf = convert_small_polygons(infra_gdf, polygon_area_threshold)

    logger.info("Starting Zonal Aggregation...")
    df = zonal_aggregation(
        climate=climate_ds,
        infra=infra_gdf,
        zonal_agg_method=zonal_agg_method,
        x_dim=constants.X_DIM,
        y_dim=constants.Y_DIM,
        time_period_type=time_period_type,
    )
    logger.info("Zonal Aggregation Computed")

    failed_aggregations = df.loc[df["ensemble_mean"].isna(), ID_COLUMN].nunique()
    logger.warning(
        f"{str(failed_aggregations)} osm_ids were unable to be zonally aggregated"
    )
    df = df.dropna()

    # Round ensemble columns to 2 decimal places
    df = round_ensemble_columns(df)

    # Add the metadata column
    df["metadata"] = json.dumps(metadata)

    if time_period_type == "decade_month":
        df = df.drop_duplicates(subset=['month', 'decade', ID_COLUMN])
    elif time_period_type == "year_span_month":
        df = df.drop_duplicates(subset=['month', 'start_year', 'end_year', ID_COLUMN])

    return df
