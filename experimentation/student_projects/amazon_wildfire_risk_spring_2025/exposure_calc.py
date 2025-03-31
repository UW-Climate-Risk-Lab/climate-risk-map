import concurrent.futures as cf
import logging
import os
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd
import xarray as xr
import xvec

import rioxarray

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Infrastructure return data should have two columns, id and geometry
# 'id' column refers to a given feature's unique id. This is the OpenStreetMap ID for the PG OSM Flex
ID_COLUMN = "osm_id"
GEOMETRY_COLUMN = "geometry"
S3_BUCKET = os.environ["S3_BUCKET"]


def convert_ds_to_df(ds: xr.Dataset) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe.

    Used since we ultimately want the data in tabular form for PostGIS.

    Args:
        da (xr.DataArray): Datarray
    """

    df = (
        ds.stack(id_dim=(GEOMETRY_COLUMN, "month"))
        .to_dataframe()
        .reset_index(drop=True)[[ID_COLUMN, "month"] + list(ds.data_vars)]
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

def zonal_aggregation_polygon(
    climate: xr.DataArray,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str,
) -> pd.DataFrame:

    climate_computed = climate.compute() # Parallel task did not work unless data was computed
    # The following parallelizes the zonal aggregation of polygon geometry features
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

    df_polygon = pd.concat(results)
    return df_polygon


def zonal_aggregation(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
    x_dim: str,
    y_dim: str,
    zonal_agg_method: str
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
    polygon_geom_types = ["Polygon", "MultiPolygon"]

    polygon_infra = infra.loc[infra.geom_type.isin(polygon_geom_types)]
    point_infra = infra.loc[infra.geom_type.isin(point_geom_types)]

    df_point = zonal_aggregation_point(
        climate=climate,
        infra=point_infra,
        x_dim=x_dim,
        y_dim=y_dim
    )
    logger.info("Point geometries intersected successfully")

    df_polygon = zonal_aggregation_polygon(
        climate=climate,
        infra=polygon_infra,
        x_dim=x_dim,
        y_dim=y_dim,
        zonal_agg_method=zonal_agg_method,
    )

    logger.info("Polygon geometries intersected successfully")

    # Applies the same method for converting from DataArray to DataFrame, and
    # combines the data back together.
    df = pd.concat(
        [df_point, df_polygon],
        ignore_index=True,
    )

    if GEOMETRY_COLUMN in df.columns:
        df.drop(GEOMETRY_COLUMN, inplace=True, axis=1)

    return df


def main(
) -> pd.DataFrame:

    infra_df = pd.read_csv("data/amazon_facilities_eastern_washington.csv")
    infra_gdf = gpd.GeoDataFrame(infra_df, geometry=gpd.points_from_xy(x=infra_df["longitude"], y=infra_df["latitude"]), crs="4326")

    climate_ds = xr.load_dataset(f"s3://{S3_BUCKET}/student-projects/amazon-wildfire-risk-spring2025/data/cmip6_adjusted_burn_probability.zarr")
    logger.info("Starting Zonal Aggregation...")
    df = zonal_aggregation(
        climate=climate_ds,
        infra=infra_gdf,
        x_dim="x",
        y_dim="y",
        zonal_agg_method='max'
    )
    logger.info("Zonal Aggregation Computed")

    final_df = infra_df.merge(df, how='left', on=ID_COLUMN)
    
    final_df.to_csv("data/amazon_facilities_with_fire_exposure.csv", index=False)
    
    

if __name__=="__main__":
    main()