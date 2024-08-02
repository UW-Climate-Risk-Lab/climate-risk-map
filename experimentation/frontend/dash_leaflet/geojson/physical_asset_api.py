"""
Interface with PostGIS Database for getting asset data
"""

import psycopg2 as pg
from psycopg2 import sql

from typing import List, Dict, Tuple, Union, Optional


class OpenStreetMapDataAPI:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: str):
        """The following API implementation makes a direct connection
        to the database and performs SQL queries. 
        
        It is based on data loaded using PG-OSM Flex (https://pgosm-flex.com). 
        As of Aug-2024, PG-OSM Flex is used as the ETL process.

        Can refactor in future to replace SQL with REST API connection
        and calls.

        Args:
            dbname (str): Database name. Database names should be in the format pgosm_flex_{region_name}
            user (str): Database user to assume
            password (str): Password for database user
            host (str): Database host url
            port (str): Database port
        """
        self.conn = pg.connect(
            dbname=str(dbname),
            user=str(user),
            password=str(password),
            host=str(host),
            port=str(port),
        )
        
        # schema where the feature data tables are
        self.schema = 'osm'

        # As of Aug-2024, set manually for time being.
        # Keys are categories, values are lists of available geometry types of that category
        self.available_categories = {"infrastructure": ["line", "polygon", "point"]}

    def _execute_query_postgis(self, query: str, params: Tuple[str] = None):

        with self.conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall()
        return result

    def _get_table_names(self, category: str) -> List[str]:
        """Creates table names from category

        Table names format in PG OSM Flex is {category}_{geom_type}

        """
        tables = []
        for category, geom_types in self.available_categories.items():
            for geom_type in geom_types:
                tables.append(f"{self.schema}.{category}_{geom_type}")
        return tables
        

    def _get_table_columns(self, table_name):
        """
        Fetches the column names and types for the specified table.
    
        Args:
            table_name (str): The name of the table to fetch columns for.
    
        Returns:
            List[str]: A list of dictionaries containing column names and types.
        """

         # Query to fetch column names and types
        query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
        """
    
       
        result = self._execute_query_postgis(query, (table_name,))
        column_names = [row[0] for row in result]
    
        return column_names
    


    def get_geojson_data(
        self,
        categories: List[str],
        osm_types: Tuple[str],
        osm_subtypes: Tuple[str] = tuple(),
    ) -> Dict:
        """Gets GEOJSON data from provided
        
        Args:
            categories (List[str]): 1 or more categories to get data from
            osm_types (List[str]): OSM Type to Filter On
            osm_subtypes (List[str]): OSM Subtypes to filter on
        """
        if type(categories) is str:
            categories = [categories]


        for category in categories:
            if category not in self.available_categories.keys():
                raise ValueError(f'{category} is not an available category to query!')

        if len(osm_types) < 1:
            raise ValueError('Must provide at least 1 type in osm_types arg!')
        
        base_query = """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json))
            
            FROM (
        """
        tables = []
        for category in categories:
            tables.extend(self._get_table_names(category=category))
        union_queries = []
        params = []
        for table in tables:
            # Creates a query for each table in the given category
            columns = self._get_table_columns(table_name=table)
            sub_query = """
            SELECT tags as tooltip, ST_Transform(geom, 4326) AS geometry 
            FROM %s
            JOIN %s.tags ON %s.osm_id = %s.tags.osm_id
            WHERE osm_type = ANY(%s)
            """
            params.extend([table, self.schema, table, self.schema, osm_types, osm_subtypes])
            if ("osm_subtype" in columns) & (len(osm_subtypes) > 0):
                sub_query = sub_query + "AND osm_subtype = ANY(%s)"
                params.extend([osm_subtypes])
            union_queries.append(sub_query)
            

        full_query = base_query + "UNION ALL".join(union_queries) + ") AS t;"
    
        result = self._execute_query_postgis(query=full_query, params=tuple(params))
        return result


GET_INFRASTRUCTURE_LINE = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT osm_id, osm_subtype AS tooltip, ST_Transform(geom, 4326) AS geometry FROM osm.infrastructure_line
    WHERE osm_type='power'
    )
AS t;
"""

GET_GEOJSON = query = sql.SQL("select {fields} from {table}").format(
    fields=sql.SQL(",").join(
        [
            sql.Identifier("field1"),
            sql.Identifier("field2"),
            sql.Identifier("field3"),
        ]
    ),
    table=sql.Identifier("some_table"),
)

GET_INFRASTRUCTURE_POLYGON = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT osm_id, osm_subtype AS tooltip, ST_Transform(geom, 4326) AS geometry FROM osm.infrastructure_polygon
    WHERE osm_type='power'
    )
AS t;

"""

GET_INFRASTRUCTURE_POINT = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT osm_id, osm_subtype AS tooltip, ST_Transform(geom, 4326) AS geometry FROM osm.infrastructure_point
    WHERE osm_type='power'
    )
AS t;

"""
