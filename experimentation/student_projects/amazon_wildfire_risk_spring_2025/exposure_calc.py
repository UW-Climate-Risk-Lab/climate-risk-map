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
        ds
        .to_dataframe()
        .reset_index(drop=True)[[ID_COLUMN] + list(ds.data_vars)]
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


def zonal_aggregation(
    climate: xr.Dataset,
    infra: gpd.GeoDataFrame,
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

    point_infra = infra.loc[infra.geom_type.isin(point_geom_types)]
    point_infra = point_infra.set_index(ID_COLUMN)
    ds = climate.xvec.extract_points(
        point_infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
    )

    df = convert_ds_to_df(ds=ds)

    logger.info("Point geometries intersected successfully")


    if GEOMETRY_COLUMN in df.columns:
        df.drop(GEOMETRY_COLUMN, inplace=True, axis=1)

    return df


def main(
) -> pd.DataFrame:

    infra_df = pd.read_csv("data/amazon_facilities_eastern_washington.csv")
    infra_gdf = gpd.GeoDataFrame(infra_df, geometry=gpd.points_from_xy(x=infra_df["longitude"], y=infra_df["latitude"]), crs="4326")

    #climate_ds = xr.load_dataset(f"s3://{S3_BUCKET}/student-projects/amazon-wildfire-risk-spring2025/data/cmip6_adjusted_burn_probability.zarr")
    climate_ds = xr.open_dataset("data/BP_WA.tif", engine="rasterio")
    logger.info("Starting Zonal Aggregation...")
    df = zonal_aggregation(
        climate=climate_ds,
        infra=infra_gdf,
        x_dim="x",
        y_dim="y"
    )
    logger.info("Zonal Aggregation Computed")

    final_df = infra_df.merge(df, how='left', on=ID_COLUMN)
    
    final_df.to_csv("data/amazon_facilities_with_fire_exposure.csv", index=False)
    
    

if __name__=="__main__":
    main()