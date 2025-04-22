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
from shapely import wkt, Point

import src.utils as utils
import src.constants as constants

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Infrastructure return data should have two columns, id and geometry
# 'id' column refers to a given feature's unique id. This is the OpenStreetMap ID for the PG OSM Flex
ID_COLUMN = "osm_id"
GEOMETRY_COLUMN = "geometry"

def convert_small_polygons(
    gdf: gpd.GeoDataFrame, 
    polygon_area_threshold: float
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
            if unit in ['foot', 'feet', 'ft']:
                area_conv_factor = 10_763_910.4  # from sq feet to sq km
            elif unit not in ['metre', 'meter', 'm']:
                logger.warning(f"Unsupported CRS unit: {unit}. Assuming meters.")
        
        # Calculate areas and identify small polygons
        small_indices = temp_gdf.index[temp_gdf.area / area_conv_factor < polygon_area_threshold]
        
        # Convert small polygons to centroids
        if len(small_indices) > 0:
            result_gdf.loc[small_indices, GEOMETRY_COLUMN] = polygons.loc[small_indices].centroid
            logger.info(f"Converted {len(small_indices)} small polygons to points (area < {polygon_area_threshold} sq km)")
    
    return result_gdf


def convert_ds_to_df(ds: xr.Dataset) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe.

    Used since we ultimately want the data in tabular form for PostGIS.

    Args:
        da (xr.DataArray): Datarray
    """

    df = (
        ds.stack(id_dim=(GEOMETRY_COLUMN, "decade_month"))
        .to_dataframe()
        .reset_index(drop=True)[[ID_COLUMN, "decade_month", GEOMETRY_COLUMN] + list(ds.data_vars)]
    )

    df["decade"] = df["decade_month"].apply(lambda x: int(x[0:4]))
    df["month"] = df["decade_month"].apply(lambda x: int(x[-2:]))
    df.drop(columns=["decade_month"], inplace=True)

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

    df = convert_ds_to_df(ds=ds)

    return df


def zonal_aggregation_point(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
) -> pd.DataFrame:

    ds = climate.xvec.extract_points(
        infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
    )

    df = convert_ds_to_df(ds=ds)
    return df


def zonal_aggregation_linestring(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
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
        df_linestring = convert_ds_to_df(
            ds=ds_linestring_points
        )
        
        # TODO: At this step, we are left with OSM ids broken out into individual points.
        # Depending on the resolution of the climate dataset, there will be different exposure measures
        # along the entire linestring. Different segments of the same line will have different climate exposures, 
        # while the entire line is considered one entity. For the time being, I am taking some simple means, mins, and max
        # to move forward with development. In the future, the ideal solution is to break up each osm_id into multiple line segments.
        # We can determine the segments by seeing where the mean, median, etc... values change along the points. We can group these series of points into multiple linestrings,
        # and then store the linestring segments in the database with their individual exposure values. A single osm id may be comprised of between 1 and N line segments.
        # The downside to this is extra segment geometries will need to be stored, possibly in their own tables. This also creates the eventual output dataset to the user more complicated
        # as a single entity may now have multiple records of exposure, one for each line segment.

        df_linestring = (
            df_linestring.drop_duplicates()
            .groupby([ID_COLUMN, "decade", "month"])
            .agg({"ensemble_mean": "mean",
                  "ensemble_median": "mean",
                  "ensemble_stddev": "mean",
                  "ensemble_min": "min",
                  "ensemble_max": "max",
                  "ensemble_q1": "min",
                  "ensemble_q3": "max"})
            .reset_index()
        )
    else:
        df_linestring = pd.DataFrame()
    return df_linestring


def zonal_aggregation_polygon(
    climate: xr.DataArray,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str,
) -> pd.DataFrame:

    climate_computed = climate.compute() # Parallel task did not work unless data was computed
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

    df_polygon = pd.concat(results)
    return df_polygon


def zonal_aggregation(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    zonal_agg_method: str,
    x_dim: str,
    y_dim: str,
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
            climate=climate,
            infra=point_infra,
            x_dim=x_dim,
            y_dim=y_dim
        )
        logger.info("Point geometries intersected successfully")
    else:
        df_point = pd.DataFrame()

    if len(line_infra != 0):
        df_linestring = zonal_aggregation_linestring(
            climate=climate,
            infra=line_infra,
            x_dim=x_dim,
            y_dim=y_dim,
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
    params = [int(crs),]

    return query, tuple(params)


def main(
    climate_ds: xr.Dataset,
    climate_variable: str,
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
        crs (str): Coordinate Reference System
        zonal_agg_method (List[str] | str): Method(s) for zonal aggregation
        polygon_area_threshold (float): Threshold in square kilometers below which 
                                       polygons will be converted to points
        conn (pg.extensions.connection): Database connection
        metadata (Dict): Metadata to be included in the output
        
    Returns:
        pd.DataFrame: DataFrame with aggregated climate data for infrastructure
    """
    query, params = create_pgosm_flex_query(
        climate_variable=climate_variable, crs=crs
    )
    infra_data = utils.query_db(query=query, params=params, conn=conn)
    
    infra_df = pd.DataFrame(infra_data, columns=[ID_COLUMN, GEOMETRY_COLUMN]).set_index(
        ID_COLUMN
    )
    num_features = len(infra_df)
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
        y_dim=constants.Y_DIM
    )
    logger.info("Zonal Aggregation Computed")

    failed_aggregations = df.loc[df["ensemble_mean"].isna(), ID_COLUMN].nunique()
    logger.warning(
        f"{str(failed_aggregations)} osm_ids were unable to be zonally aggregated"
    )
    df = df.dropna()
    
    # Add the metadata column
    df['metadata'] = json.dumps(metadata)
    
    return df
