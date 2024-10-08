"""
Interface with PostGIS Database for getting asset data
"""

import re
import json
import psycopg2 as pg
from psycopg2 import OperationalError

from typing import List, Dict, Tuple


class OpenStreetMapDataAPI:
    def __init__(self, conn: pg.extensions.connection):
        """The following API implementation takes a direct connection
        to the database and performs SQL queries.

        It is based on data loaded using PG-OSM Flex (https://pgosm-flex.com).
        As of Aug-2024, PG-OSM Flex is used as the ETL process.

        Can refactor in future to replace SQL with REST API connection
        and calls.

        Args:
            conn: connection to a PG OSM Flex loaded postgres database
        """
        self.conn = conn

        # schema where the feature data tables are
        self.schema = "osm"

        # As of Aug-2024, set manually for time being.
        # Keys are categories, values are lists of available geometry types of that category
        # Possible categories here: https://pgosm-flex.com/layersets.html
        self.available_categories = "infrastructure"

    def __del__(self):
        if self.conn:
            self.conn.close()

    def __execute_postgis(self, query: str, params: Tuple[str] = None):
        """Takes query (in PostgreSQL language) and params and executes
        in the current instance connection"""
        if not self.conn:
            raise ConnectionError("Database connection is not established.")
        
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            result = cur.fetchall()
        return result

    def _get_table_names(self, categories: List[str]) -> List[str]:
        """Creates table names from category

        Table names format in PG OSM Flex is {category}_{geom_type}

        """

        tables = []

        query = """
        SELECT tablename FROM pg_tables
        WHERE schemaname='osm'
        """
        result = self.__execute_postgis(query=query, params=None)

        # Uses Regex to check whether to use the table
        # Tables always start with the category name
        # Nested loop, not worried about performance
        for table in result:
            tablename = table[0]
            for category in categories:
                name_match = re.findall(category, tablename)
                if name_match:
                    tables.append(table[0])
                else:
                    pass

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

        result = self.__execute_postgis(query, (table_name,))
        column_names = [row[0] for row in result]

        return column_names

    def _is_valid_geometry(self, geometry: Dict) -> bool:
        """Used with _is_valid_geojson() method"""
        if "type" not in geometry or "coordinates" not in geometry:
            return False
        geom_type = geometry["type"]
        coordinates = geometry["coordinates"]
        if geom_type == "Point":
            return (
                isinstance(coordinates, list)
                and len(coordinates) == 2
                and all(isinstance(coord, (int, float)) for coord in coordinates)
            )
        elif geom_type == "MultiPoint":
            return isinstance(coordinates, list) and all(
                isinstance(coord, list)
                and len(coord) == 2
                and all(isinstance(c, (int, float)) for c in coord)
                for coord in coordinates
            )
        elif geom_type == "LineString":
            return isinstance(coordinates, list) and all(
                isinstance(coord, list)
                and len(coord) == 2
                and all(isinstance(c, (int, float)) for c in coord)
                for coord in coordinates
            )
        elif geom_type == "MultiLineString":
            return isinstance(coordinates, list) and all(
                isinstance(line, list)
                and all(
                    isinstance(coord, list)
                    and len(coord) == 2
                    and all(isinstance(c, (int, float)) for c in coord)
                    for coord in line
                )
                for line in coordinates
            )
        elif geom_type == "Polygon":
            return isinstance(coordinates, list) and all(
                isinstance(ring, list)
                and all(
                    isinstance(coord, list)
                    and len(coord) == 2
                    and all(isinstance(c, (int, float)) for c in coord)
                    for coord in ring
                )
                for ring in coordinates
            )
        elif geom_type == "MultiPolygon":
            return isinstance(coordinates, list) and all(
                isinstance(polygon, list)
                and all(
                    isinstance(ring, list)
                    and all(
                        isinstance(coord, list)
                        and len(coord) == 2
                        and all(isinstance(c, (int, float)) for c in coord)
                        for coord in ring
                    )
                    for ring in polygon
                )
                for polygon in coordinates
            )
        elif geom_type == "GeometryCollection":
            return isinstance(coordinates, list) and all(
                self._is_valid_geometry(geom) for geom in coordinates
            )
        return False

    def _is_valid_feature(self, feature: Dict) -> bool:
        """Used with _is_valid_geojson() method"""
        if "type" not in feature or feature["type"] != "Feature":
            return False
        if "geometry" not in feature or not self._is_valid_geometry(
            feature["geometry"]
        ):
            return False
        if "properties" not in feature or not isinstance(feature["properties"], dict):
            return False
        return True

    def _is_valid_geojson(self, geojson: Dict) -> bool:
        """Checks if the dict is in valid GeoJSON format

        TODO: Move the GeoJSON checking logic out of class
        """
        if "type" not in geojson:
            return False
        geojson_type = geojson["type"]
        if geojson_type == "FeatureCollection":
            if geojson["features"] is None:
                print("No data returned by GeoJSON query!")
                return True
            if "features" not in geojson or not isinstance(geojson["features"], list):
                return False
            return all(
                self._is_valid_feature(feature) for feature in geojson["features"]
            )
        elif geojson_type == "Feature":
            return self._is_valid_feature(geojson)
        elif geojson_type in [
            "Point",
            "MultiPoint",
            "LineString",
            "MultiLineString",
            "Polygon",
            "MultiPolygon",
            "GeometryCollection",
        ]:
            return self._is_valid_geometry(geojson)
        return False

    def _check_args_get_osm_data(self, args: List[Dict]) -> None:
        """Used to quality check the args in get_osm_data()

        Will raise error if check fails, else returns None

        Args:
            args: List of dicts that describe each arg
        """

        for arg in args:
            if arg["value"] is None:
                if arg["required"]:
                    raise TypeError(f"The input {arg["name"]} is required!")
                continue
            else:
                if not isinstance(arg["value"], arg["type"]):
                    raise TypeError(f"The input {arg["name"]} should be of type: {str(arg["type"])}")
        
    def get_osm_data(
        self,
        categories: List[str],
        osm_types: List[str],
        osm_subtypes: List[str] = None,
        bbox: Dict[str, List] = None,
        county: bool = False,
        city: bool = False,
        srid: int = 4326,
        geom_type: str = None,
        centroid: bool = False
    ) -> Dict:
        """Gets OSM data from provided filters.

        **Return value is in a dict that is GeoJSON format**

        The tags table is always joined on the requested category tables
        as it contains all of the tags across all of the tables.

        Args:
            categories (List[str]): 1 or more categories to get data from
            osm_types (List[str]): OSM Type to Filter On
            osm_subtypes (List[str]): OSM Subtypes to filter on
            bbox (Dict[str]): A Dict in the GeoJSON format. Used for filtering
            county (bool): If True, returns the county of the feature as a property
            city (bool): If True, returns the city of the feature as a property
            srid (int): Spatial reference ID, default is EPSG:4326
            geom_type (str): If used, returns only features of the specified geom_type
            centroid (bool): If True, returns the geometry as a Point, the centroid of the features geometry
        """
        # Used for checking quality of input args
        args = [
            {
                "name": "categories",
                "required": True,
                "type": list,
                "value": categories,
            },
            {
                "name": "osm_types",
                "required": True,
                "type": list,
                "value": osm_types,
            },
            {
                "name": "osm_subtypes",
                "required": False,
                "type": list,
                "value": osm_subtypes,
            },
            {
                "name": "bbox",
                "required": False,
                "type": dict,
                "value": bbox,
            },
            {
                "name": "county",
                "required": True,
                "type": bool,
                "value": county
            },
            {
                "name": "city",
                "required": True,
                "type": bool,
                "value": city
            },
            {
                "name": "srid",
                "required": True,
                "type": int,
                "value": srid
            },
            {
                "name": "geom_type",
                "required": False,
                "type": str,
                "value": geom_type
            },
            {
                "name": "centroid",
                "required": True,
                "type": bool,
                "value": centroid
            }
        ]

        # Quality check of input args
        self._check_args_get_osm_data(args=args)

        # Check that bbox is a geojson
        if bbox:
            if not self._is_valid_geojson(geojson=bbox):
                raise TypeError("The bbox provide is not a valid GeoJSON!")

        # This builds a query to return a GeoJSON object
        # Uses the PostGIS function "ST_AsGeoJSON" in every query
        base_query = """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(t.*)::json))
            
            FROM (
        """
        tables = self._get_table_names(categories)
        union_queries = []
        params = []
        for table in tables:
            # Creates a query for each table in the given category
            columns = self._get_table_columns(table_name=table)
            # TODO: Make SRID an input param, not hardcoded as 4326

            # TODO: Implement sql.SQL strings for building better sql queries 
            # https://www.psycopg.org/docs/sql.html

            select_statement = "SELECT main.osm_id, main.osm_type, main.osm_subtype, t.tags"
            
            if centroid:
                geom_select = ", ST_Centroid(ST_Transform(main.geom, %s)) AS geometry"
                
            else:
                geom_select = ", ST_Transform(main.geom, %s) AS geometry"
            
            select_statement = select_statement + geom_select
            params.append(srid)

            from_statement = f"FROM {self.schema}.{table} main"
            join_statement = f"JOIN {self.schema}.tags t ON main.osm_id = t.osm_id"
            where_clause = "WHERE main.osm_type IN %s" # For now, always require OSM type be specified
            params.append(tuple(osm_types))

            # Add extra where clause for subtypes if they are specified
            if osm_subtypes:
                if "osm_subtype" in columns:
                    where_clause = where_clause + " " + "AND main.osm_subtype IN %s"
                    params.append(tuple(osm_subtypes))
            
            if county: 
                join_statement = join_statement + " " + f"LEFT JOIN {self.schema}.place_polygon county ON ST_Intersects(main.geom, county.geom) AND county.admin_level = 6"
                select_statement = select_statement + ", county.name AS county_name"

            if city:
                join_statement = join_statement + " " + f"LEFT JOIN {self.schema}.place_polygon city ON ST_Intersects(main.geom, city.geom) AND city.admin_level = 8 AND city.osm_type = 'city'"
                select_statement = select_statement + ", city.name AS city_name" 

            if geom_type:
                where_clause = where_clause + " " + "AND ST_GeometryType(main.geom) = %s"
                params.append('ST_' + geom_type)

            # If a bounding box GeoJSON is passed in, use as filter
            if bbox:
                where_clause = where_clause + " AND ("
                count = 0
                # Handles multiple bounding boxes drawn by user
                for feature in bbox["features"]:
                    if count == 0:
                        conditional = None
                        bbox_filter = ""
                    else:
                        conditional = " OR "
                        bbox_filter = bbox_filter + conditional

                    geojson_str = json.dumps(feature["geometry"])
                    bbox_filter = bbox_filter + "ST_Intersects(ST_Transform(main.geom, %s), ST_GeomFromGeoJSON(%s))"
                    params.append(srid)
                    params.append(geojson_str)

                    
                    count += 1
                where_clause = where_clause + bbox_filter + ")"

            sub_table_query = select_statement + "\n" + from_statement + "\n" + join_statement + "\n" + where_clause
            union_queries.append(sub_table_query)

        full_query = base_query + " UNION ALL ".join(union_queries) + ") AS t;"

        result = self.__execute_postgis(query=full_query, params=tuple(params))
        geojson = result[0][0]
        if not self._is_valid_geojson(geojson=geojson):
            raise ValueError("The returned data is not in proper geojson format")
        return geojson
