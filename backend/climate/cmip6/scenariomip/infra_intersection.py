import xarray as xr
import pandas as pd
import re

from typing import Tuple

import utils
import psycopg2.sql as sql


def create_postgis_query(osm_category: str, osm_type: str) -> Tuple[str, Tuple[str]]:
    schema = "osm"
    tables = utils.get_osm_category_tables(osm_category=osm_category)
    params = []
    union_queries = []
    for table in tables:
        sub_query = sql.SQL(
            "SELECT main.{osm_id}, main.{geom} FROM {schema}.{table} main WHERE osm_type = %s"
        ).format(
            osm_id=sql.Identifier("osm_id"),
            geom=sql.Identifier("geom"),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
        params.append(osm_type)
        union_queries.append(sub_query)
    query = sql.SQL(" UNION ALL ").join(union_queries)

    return query, tuple(params)


def main(ds: xr.Dataset, osm_category: str, osm_type: str) -> pd.DataFrame:

    query = create_postgis_query(osm_category=osm_category, osm_type=osm_type)
    pass
