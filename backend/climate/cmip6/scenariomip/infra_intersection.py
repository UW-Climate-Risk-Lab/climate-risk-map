import concurrent.futures as cf
import logging
import os
from typing import Dict, List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2 as pg
import psycopg2.sql as sql
import xarray as xr
import xvec
from shapely import wkt

import utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Infrastructure return data should have two columns, id and geometry
# 'id' column refers to a given feature's unique id. This is the OpenStreetMap ID for the PG OSM Flex
ID_COLUMN = "osm_id"
GEOMETRY_COLUMN = "geometry"

# Column name of final output that holds the climate variable's value
VALUE_COLUMN = "value"


def convert_da_to_df(da: xr.DataArray, climatology_mean_method: str) -> pd.DataFrame:
    """Converts a DataArray to a Dataframe.

    Used since we ultimately want the data in tabular form for PostGIS.

    Args:
        da (xr.DataArray): Datarray
        climate_variable (str): Climate Variable of interest
        climatology_mean_method (str): Mean aggregation method
    """

    # The DataArray name is needed, and will ultimately be the climate variable value column in the output
    da.name = VALUE_COLUMN

    df = (
        da.stack(id_dim=(GEOMETRY_COLUMN, climatology_mean_method))
        .to_dataframe()
        .reset_index(drop=True)[[ID_COLUMN, climatology_mean_method, VALUE_COLUMN]]
    )

    if climatology_mean_method == "decade_month":
        df["decade"] = df["decade_month"].apply(lambda x: int(x[0:4]))
        df["month"] = df["decade_month"].apply(lambda x: int(x[-2:]))
        df.drop(columns=["decade_month"], inplace=True)
    else:
        raise ValueError(f"{climatology_mean_method} climatology method not supported")

    return df


def task_xvec_zonal_stats(
    climate: xr.DataArray,
    geometry,
    x_dim,
    y_dim,
    zonal_agg_method,
    method,
    index,
    climatology_mean_method,
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

    da = climate.xvec.zonal_stats(
        geometry,
        x_coords=x_dim,
        y_coords=y_dim,
        stats=zonal_agg_method,
        method=method,
        index=index,
    )

    df = convert_da_to_df(da=da, climatology_mean_method=climatology_mean_method)

    return df


def zonal_aggregation(
    climate: xr.DataArray,
    infra: gpd.GeoDataFrame,
    zonal_agg_method: str,
    climatology_mean_method: str,
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
        climate (xr.DataArray): Climate data
        infra (gpd.GeoDataFrame): Infrastructure data
        zonal_agg_method (str): Zonal aggregation method
        climatology_mean_method (str): Climatology mean method
        x_dim (str): X dimension name
        y_dim (str): Y dimension name

    Returns:
        pd.DataFrame: Aggregated data
    """

    point_geom_types = ["Point", "MultiPoint"]

    non_point_infra = infra.loc[~infra.geom_type.isin(point_geom_types)]
    point_infra = infra.loc[infra.geom_type.isin(point_geom_types)]

    da_point = climate.xvec.extract_points(
        point_infra.geometry, x_coords=x_dim, y_coords=y_dim, index=True
    )

    # The following parallelizes the zonal aggregation of non-point geometry features
    # The 
    workers = os.cpu_count()
    futures = []
    results = []
    geometry_chunks = np.array_split(non_point_infra.geometry, workers)
    with cf.ProcessPoolExecutor(max_workers=workers) as executor:
        for i in range(workers):
            futures.append(
                executor.submit(
                    task_xvec_zonal_stats,
                    climate,
                    geometry_chunks[i],
                    x_dim,
                    y_dim,
                    zonal_agg_method,
                    'exactextract',
                    True,
                    climatology_mean_method,
                )
            )
        cf.as_completed(futures)
        for future in futures:
            try:
                results.append(future.result())
            except:
                logger.info('Future result in zonal agg process pool could not be appended')

    df_non_point = pd.concat(results)

    # Applies the same method for converting from DataArray to DataFrame, and
    # combines the data back together.
    df = pd.concat(
        [
            convert_da_to_df(
                da=da_point,
                climatology_mean_method=climatology_mean_method,
            ),
            df_non_point,
        ],
        ignore_index=True,
    )

    return df


def create_pgosm_flex_query(
    osm_tables: List[str], osm_type: str, crs: str
) -> Tuple[sql.SQL, Tuple[str]]:
    """Creates SQL query to get all features of a given type from PG OSM Flex Schema



    Example:

    SELECT osm_id AS id, ST_AsText(ST_Transform(geom, 4326)) AS geometry
        FROM osm.infrastructure_polygon
    WHERE osm_type = 'power'
    UNION ALL
    SELECT osm_id AS id, ST_AsText(ST_Transform(geom, 4326)) AS geometry
        FROM osm.infrastructure_point
    WHERE osm_type = 'power'


    Args:
        osm_category (str): OpenStreetMap Category (Will be the prefix of the tables names)
        osm_type (str): OpenStreetMap feature type

    Returns:
        Tuple[sql.SQL, Tuple[str]]: Query in SQL object and params of given query
    """
    schema = "osm"  # Always schema name in PG OSM Flex
    params = []
    union_queries = []

    for table in osm_tables:
        sub_query = sql.SQL(
            "SELECT main.osm_id AS {id}, ST_AsText(ST_Transform(main.geom, %s)) AS {geometry} FROM {schema}.{table} main WHERE osm_type = %s"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            id=sql.Identifier(ID_COLUMN),
            geometry=sql.Identifier(GEOMETRY_COLUMN),
        )
        params += [int(crs), osm_type]
        union_queries.append(sub_query)
    query = sql.SQL(" UNION ALL ").join(union_queries)

    return query, tuple(params)


def main(
    climate_ds: xr.Dataset,
    climate_variable: str,
    osm_category: str,
    osm_type: str,
    crs: str,
    x_dim: str,
    y_dim: str,
    climatology_mean_method: str,
    zonal_agg_method: List[str] | str,
    conn: pg.extensions.connection,
) -> pd.DataFrame:

    osm_tables = utils.get_osm_category_tables(osm_category=osm_category, conn=conn)

    query, params = create_pgosm_flex_query(
        osm_tables=osm_tables, osm_type=osm_type, crs=crs
    )
    infra_data = utils.query_db(query=query, params=params, conn=conn)

    infra_df = pd.DataFrame(infra_data, columns=[ID_COLUMN, GEOMETRY_COLUMN]).set_index(
        ID_COLUMN
    )
    infra_df[GEOMETRY_COLUMN] = infra_df[GEOMETRY_COLUMN].apply(wkt.loads)
    infra_gdf = gpd.GeoDataFrame(infra_df, geometry=GEOMETRY_COLUMN, crs=crs)

    logger.info("Starting Zonal Aggregation...")
    df = zonal_aggregation(
        climate=climate_ds[climate_variable],
        infra=infra_gdf,
        zonal_agg_method=zonal_agg_method,
        climatology_mean_method=climatology_mean_method,
        x_dim=x_dim,
        y_dim=y_dim,
    )
    logger.info("Zonal Aggregation Computed")

    failed_aggregations = df.loc[df["value"].isna(), ID_COLUMN].nunique()
    logger.warning(f"{str(failed_aggregations)} osm_ids were unable to be zonally aggregated")
    df = df.dropna()
    return df
