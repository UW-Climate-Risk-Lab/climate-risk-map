import xarray as xr
import pandas as pd
import geopandas as gpd
import xvec

from shapely import wkt

from typing import Tuple, List

import utils
import psycopg2.sql as sql

# Infrastructure return data should have two columns, id and geometry
# 'id' column refers to a given feature's unique id. This is the OpenStreetMap ID for the PG OSM Flex 
ID_COLUMN = 'id'
GEOMETRY_COLUMN = 'geometry'

def create_pgosm_flex_query(
    osm_category: str, osm_type: str, crs: str
) -> Tuple[sql.SQL, Tuple[str], List[str]]:
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
    tables = utils.get_osm_category_tables(osm_category=osm_category)

    for table in tables:
        sub_query = sql.SQL(
            "SELECT main.osm_id AS {id}, ST_AsText(ST_Transform(main.geom, %s)) AS {geometry} FROM {schema}.{table} main WHERE osm_type = %s"
        ).format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            id=sql.Identifier(ID_COLUMN),
            geometry=sql.Identifier(GEOMETRY_COLUMN)
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
    time_agg_method: str
) -> pd.DataFrame:

    query, params = create_pgosm_flex_query(
        osm_category=osm_category, osm_type=osm_type, crs=crs
    )
    infra_data = utils.query_db(query=query, params=params)
    
    infra_df = pd.DataFrame(infra_data, columns=[ID_COLUMN, GEOMETRY_COLUMN]).set_index(ID_COLUMN)
    infra_df[GEOMETRY_COLUMN] = infra_df[GEOMETRY_COLUMN].apply(wkt.loads)
    infra_gdf = gpd.GeoDataFrame(infra_df, geometry=GEOMETRY_COLUMN, crs=crs)

    ds = climate_ds.xvec.zonal_stats(infra_gdf.geometry, x_coords=x_dim, y_coords=y_dim)

    # For the initial use of this with 100km x 100km climate data and Washington State Power Grid (~120k features),
    # dataframe memory size is 270MB, 14million rows
    df = (
        ds[climate_variable]
        .stack(id_dim=(GEOMETRY_COLUMN, time_agg_method))
        .to_dataframe()
        .reset_index(drop=True)[[ID_COLUMN, time_agg_method, climate_variable]]
    )

    pass