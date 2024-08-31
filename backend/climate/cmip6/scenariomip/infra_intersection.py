import xarray as xr
import pandas as pd
import re

from typing import Tuple, List

import utils
import psycopg2.sql as sql


def create_pgosm_flex_query(
    osm_category: str, osm_type: str, crs: str
) -> Tuple[sql.SQL, Tuple[str], List[str]]:
    """Creates SQL query to get all features of a given type from PG OSM Flex Schema

    PG OSM Flex Schema

    Example:


    SELECT osm_id, ST_AsText(ST_Transform(geom, 4326))
        FROM osm.infrastructure_polygon
    WHERE osm_type = 'power'
    UNION ALL
    SELECT osm_id, ST_AsText(ST_Transform(geom, 4326))
        FROM osm.infrastructure_point
    WHERE osm_type = 'power'


    Args:
        osm_category (str): OpenStreetMap Category (Will be the prefix of the tables names)
        osm_type (str): OpenStreetMap feature type

    Returns:
        Tuple[sql.SQL, Tuple[str]]: Query in SQL object and params of given query
    """
    schema = "osm"  # Always schema name in PG OSM Flex
    column_names = ["osm_id", "geometry"]
    params = []
    union_queries = []
    tables = utils.get_osm_category_tables(osm_category=osm_category)

    for table in tables:
        sub_query = sql.SQL(
            "SELECT main.{osm_id}, ST_AsText(ST_Transform(main.{geom}, %s)) AS geometry FROM {schema}.{table} main WHERE osm_type = %s"
        ).format(
            osm_id=sql.Identifier("osm_id"),
            geom=sql.Identifier("geom"),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        params += [int(crs), osm_type]
        union_queries.append(sub_query)
    query = sql.SQL(" UNION ALL ").join(union_queries)

    return query, tuple(params), column_names


def main(ds: xr.Dataset, osm_category: str, osm_type: str, crs: str) -> pd.DataFrame:

    query, params, column_names = create_pgosm_flex_query(
        osm_category=osm_category, osm_type=osm_type, crs=crs
    )
    db_data = utils.query_db(query=query, params=params)
    df = pd.DataFrame(db_data, columns=column_names)
    pass
