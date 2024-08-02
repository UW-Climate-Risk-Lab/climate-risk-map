"""
SQL quereies are written using the psycopg2

"""

import psycopg2 as pg
from psycopg2 import sql


class PostGISQuery:
    def __init__(self, dbname, user, password, host, port):
        self.conn = pg.connect(
            dbname=dbname, user=user, password=password, host=host, port=port
        )

    def _execute_query_postgis(self, query: str):

        cur = self.conn.cursor()
        cur.execute(query)
        result = cur.fetchone()[0]
        cur.close()

        return result

    def _build_query():
        pass

    def get_geojson_data(
        self,
        table: str,
    ):
        """
        
        """
        query = sql.SQL("select {fields} from {table}").format(
            fields=sql.SQL(",").join(
                [
                    sql.Identifier("field1"),
                    sql.Identifier("field2"),
                    sql.Identifier("field3"),
                ]
            ),
            table=sql.Identifier("some_table"),
        )


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
