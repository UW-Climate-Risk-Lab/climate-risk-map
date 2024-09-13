"""
Interface with PostGIS Database for getting asset data
"""

import re
import json
import psycopg2 as pg
from psycopg2 import sql
from geojson_pydantic import FeatureCollection
from pydantic import ValidationError


from typing import List, Dict, Tuple


class infraXclimateAPI:
    def __init__(self, conn: pg.extensions.connection):
        """The following API class takes a direct connection
        to the database and performs SQL queries.

        It is based on data loaded using PG-OSM Flex (https://pgosm-flex.com).
        As of Aug-2024, PG-OSM Flex is used as the ETL process.

        Can create full-fledged API based on this class int.

        Args:
            conn: connection to a PG OSM Flex loaded postgres database
        """
        self.conn = conn

        # Constants for given objects in the PgOSM Flex Schema
        self.osm_schema = "osm"
        self.osm_table_tags = "tags"
        self.osm_column_geom = "geom"
        self.osm_table_places = "place_polygon"  # This table contains Administrative Boundary data (cities, counties, towns, etc...)
        self.osm_table_places_admin_levels = {"county": 6, "city": 8}

        # As of Aug-2024, set manually for time being.
        # Possible categories here: https://pgosm-flex.com/layersets.html
        self.available_categories = ["infrastructure", self.osm_table_places]

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

    def _get_table_columns(self, table_name):
        """
        Fetches the column names and types for the specified tables.
        This will get all tables that are LIKE the table_name. Since table name passed in
        is derivied from the category, all tables of that category have the same columns with PgOSM Flex Schema

        Args:
            table_name (str): The name of the table to fetch columns for.

        Returns:
            List[str]: A list of dictionaries containing column names and types.
        """

        # Query to fetch column names and types
        query = sql.SQL(
            """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name LIKE %s
        """
        )

        result = self.__execute_postgis(query, (table_name + "%",))
        column_names = set([row[0] for row in result])

        return list(column_names)

    def _check_args_get_osm_data(
        self,
        category: str,
        osm_types: List[str],
        osm_subtypes: List[str],
        bbox: FeatureCollection,
        county: bool,
        city: bool,
        epsg_code: int,
        geom_type: str,
        centroid: bool,
    ) -> None:
        """Used to quality check the args in get_osm_data()

        Will raise error if check fails, else returns None

        Args:
            args: List of dicts that describe each arg
        """
        args = [
            {
                "name": "category",
                "required": True,
                "type": str,
                "value": category,
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
            {"name": "county", "required": True, "type": bool, "value": county},
            {"name": "city", "required": True, "type": bool, "value": city},
            {"name": "epsg_code", "required": True, "type": int, "value": epsg_code},
            {"name": "geom_type", "required": False, "type": str, "value": geom_type},
            {"name": "centroid", "required": True, "type": bool, "value": centroid},
        ]

        for arg in args:
            if arg["value"] is None:
                if arg["required"]:
                    raise TypeError(f"The input {arg['name']} is required!")
                continue
            else:
                if not isinstance(arg["value"], arg["type"]):
                    raise TypeError(
                        f"The input {arg['name']} should be of type: {str(arg['type'])}"
                    )

        # Check that bbox is a geojson
        if bbox:
            if not self._is_valid_geojson_featurecollection(data=bbox):
                raise TypeError(
                    "The bounding box (bbox) provided is not a valid GeoJSON!"
                )

    def _is_valid_geojson_featurecollection(self, data: Dict) -> bool:
        try:
            features = data["features"]
        except KeyError as e:
            raise KeyError(
                "When checking GeoJSON Feature Collection, no 'features' key was found"
            )

        try:
            fc = FeatureCollection(type="FeatureCollection", features=features)
            return fc.type == "FeatureCollection"
        except ValidationError as e:
            print(e)
            return False

    def _create_admin_table_conditions(self, condition: str) -> Dict:

        admin_conditions = {
            "condition": condition,
            "level": self.osm_table_places_admin_levels[condition],
            "alias": condition,
        }

        return admin_conditions

    def _create_select_statement(
        self,
        params: List,
        primary_table: str,
        centroid: bool,
        epsg_code: int,
        osm_subtypes: List[str],
        county: bool,
        city: bool,
    ) -> sql.SQL:
        """Bulids a dynamic SQL SELECT statement for the get_osm_data method"""

        select_fields = [
            sql.Identifier(self.osm_schema, primary_table, "osm_id"),
            sql.Identifier(self.osm_schema, primary_table, "osm_type"),
            sql.Identifier(self.osm_schema, self.osm_table_tags, "tags"),
        ]

        # If the user just wants the Centroid point of the feature, we need to use PostGIS function
        # ST_Centroid. ST_Transform is always
        if centroid:
            select_fields.append(
                sql.SQL(
                    "ST_Centroid(ST_Transform({schema}.{table}.{column}, %s)) AS geometry"
                ).format(
                    schema=sql.Identifier(self.osm_schema),
                    table=sql.Identifier(primary_table),
                    column=sql.Identifier(self.osm_column_geom),
                )
            )
        else:
            select_fields.append(
                sql.SQL(
                    "ST_Transform({schema}.{table}.{column}, %s) AS geometry"
                ).format(
                    schema=sql.Identifier(self.osm_schema),
                    table=sql.Identifier(primary_table),
                    column=sql.Identifier(self.osm_column_geom),
                )
            )
        params.append(epsg_code)

        # Add extra where clause for subtypes if they are specified
        if osm_subtypes:
            select_fields.append(sql.Identifier(primary_table, "osm_subtype"))

        # County and City tables are aliased in the _create_join_method()
        if county:
            conditions = self._create_admin_table_conditions("county")
            county_field = sql.SQL("{admin_table_alias}.name AS county_name").format(
                schema=sql.Identifier(self.osm_schema),
                admin_table_alias=sql.Identifier(conditions["alias"]),
            )
            select_fields.append(county_field)

        if city:
            conditions = self._create_admin_table_conditions("city")
            city_field = sql.SQL("{admin_table_alias}.name AS city_name").format(
                schema=sql.Identifier(self.osm_schema),
                admin_table_alias=sql.Identifier(conditions["alias"]),
            )
            select_fields.append(city_field)

        select_statement = sql.SQL("SELECT {columns}").format(
            columns=sql.SQL(", ").join(select_fields)
        )

        return select_statement

    def _create_from_statement(self, primary_table: str) -> sql.SQL:

        from_statement = sql.SQL("FROM {schema}.{table}").format(
            schema=sql.Identifier(self.osm_schema), table=sql.Identifier(primary_table)
        )
        return from_statement

    def _create_join_statement(
        self, primary_table: str, params: List, county: bool, city: bool
    ) -> sql.SQL:

        # the tags table contains all of the properties of the features
        join_statement = sql.SQL(
            "JOIN {schema}.{tags_table} ON {schema}.{primary_table}.osm_id = {schema}.{tags_table}.osm_id"
        ).format(
            schema=sql.Identifier(self.osm_schema),
            tags_table=sql.Identifier(self.osm_table_tags),
            primary_table=sql.SQL(primary_table),
        )

        # Dynamically add government administrative boundaries as necessary
        admin_conditions = []
        if county:
            admin_conditions.append(self._create_admin_table_conditions("county"))
        if city:
            admin_conditions.append(self._create_admin_table_conditions("city"))

        # Iterate over the admin conditions to build the joins dynamically
        for admin in admin_conditions:
            admin_join = sql.SQL(
                "LEFT JOIN {schema}.{admin_table} {alias}"
                "ON ST_Intersects({schema}.{primary_table}.{geom_column}, {alias}.{geom_column}) "
                "AND {alias}.admin_level = %s "
            ).format(
                schema=sql.Identifier(self.osm_schema),
                admin_table=sql.Identifier(self.osm_table_places),
                primary_table=sql.Identifier(primary_table),
                geom_column=sql.Identifier(self.osm_column_geom),
                alias=sql.Identifier(admin["alias"]),
            )
            params.append(admin["level"])
            join_statement = sql.SQL(" ").join([join_statement, admin_join])

        return join_statement

    def _create_where_clause(
        self,
        primary_table: str,
        params: List,
        osm_types: List[str],
        osm_subtypes: List[str],
        geom_type: str,
        bbox: FeatureCollection,
        epsg_code: int,
    ):

        # Always filter by osm type to throttle data output!
        where_clause = sql.SQL("WHERE {schema}.{primary_table}.{column} IN %s").format(
            schema=sql.Identifier(self.osm_schema),
            primary_table=sql.Identifier(primary_table),
            column=sql.Identifier("osm_type"),
        )
        params.append(tuple(osm_types))

        if osm_subtypes:
            subtype_clause = sql.SQL(
                "AND {schema}.{primary_table}.{column} IN %s"
            ).format(
                schema=sql.Identifier(self.osm_schema),
                primary_table=sql.Identifier(primary_table),
                column=sql.Identifier("osm_subtype"),
            )
            params.append(tuple(osm_subtypes))
            where_clause = sql.SQL(" ").join([where_clause, subtype_clause])

        if geom_type:
            geom_type_clause = sql.SQL(
                "AND {schema}.{primary_table}.geom_type = %s"
            ).format(
                schema=sql.Identifier(self.osm_schema),
                primary_table=sql.Identifier(primary_table),
            )
            params.append("ST_" + geom_type)
            where_clause = sql.SQL(" ").join([where_clause, geom_type_clause])

        # If a bounding box GeoJSON is passed in, use as filter
        if bbox:
            bbox_filter = sql.SQL("AND (")
            count = 0
            # Handles multiple bounding boxes drawn by user
            for feature in bbox["features"]:
                if count == 0:
                    pass
                else:
                    conditional = sql.SQL("OR")
                    bbox_filter = sql.SQL(" ").join([bbox_filter, conditional])

                geojson_str = json.dumps(feature["geometry"])
                feature_filter = sql.SQL(
                    "ST_Intersects(ST_Transform({schema}.{primary_table}.{geom_column}, %s), ST_GeomFromGeoJSON(%s))"
                ).format(
                    schema=sql.Identifier(self.osm_schema),
                    primary_table=sql.Identifier(primary_table),
                    geom_column=sql.Identifier(self.osm_column_geom),
                )
                params.append(epsg_code)
                params.append(geojson_str)
                bbox_filter = sql.SQL(" ").join([bbox_filter, feature_filter])
                count += 1
            bbox_filter = sql.SQL(" ").join([bbox_filter, sql.SQL(")")])

        where_clause = sql.SQL(" ").join([where_clause, bbox_filter])

        return where_clause

    def get_osm_data(
        self,
        category: str,
        osm_types: List[str],
        osm_subtypes: List[str] = None,
        bbox: FeatureCollection = None,
        county: bool = False,
        city: bool = False,
        epsg_code: int = 4326,
        geom_type: str = None,
        centroid: bool = False,
    ) -> Dict:
        """Gets OSM data from provided filters.

        **Return value is in a dict that is GeoJSON format**

        The tags table is always joined on the requested category tables
        as it contains all of the tags across all of the tables.

        Args:
            category (str): OSM Category to get data from
            osm_types (List[str]): OSM Type to Filter On
            osm_subtypes (List[str]): OSM Subtypes to filter on
            bbox (FeatureCollection): A Dict in the GeoJSON Feature Collection format. Used for filtering
            county (bool): If True, returns the county of the feature as a property
            city (bool): If True, returns the city of the feature as a property
            epsg_code (int): Spatial reference ID, default is 4326 (Representing EPSG:4326)
            geom_type (str): If used, returns only features of the specified geom_type
            centroid (bool): If True, returns the geometry as a Point, the centroid of the features geometry
        """

        # Quality check of input args
        self._check_args_get_osm_data(
            category=category,
            osm_types=osm_types,
            osm_subtypes=osm_subtypes,
            bbox=bbox,
            county=county,
            city=city,
            epsg_code=epsg_code,
            geom_type=geom_type,
            centroid=centroid,
        )

        # This builds a query to return a GeoJSON object
        # Uses the PostGIS function "ST_AsGeoJSON" in every query
        sql_geojson_build = sql.SQL(
            """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(geojson.*)::json))
            
            FROM (
        """
        )

        # Primary table will actually be a materialized view of the given category
        primary_table = category if category in self.available_categories else None
        if primary_table is None:
            raise ValueError(f"The category {category} is not currently available!")

        primary_table_columns = self._get_table_columns(table_name=primary_table)

        # Some categories do not have osm_subtype (like the "places" category)
        if "osm_subtype" not in primary_table_columns:
            osm_subtypes = None

        # Params are added in order while creating SQL statements
        params = []

        select_statement = self._create_select_statement(
            params=params,
            primary_table=primary_table,
            centroid=centroid,
            epsg_code=epsg_code,
            osm_subtypes=osm_subtypes,
            county=county,
            city=city,
        )

        from_statement = self._create_from_statement(primary_table=primary_table)

        join_statement = self._create_join_statement(
            primary_table=primary_table, params=params, county=county, city=city
        )

        where_clause = self._create_where_clause(
            primary_table=primary_table,
            params=params,
            osm_types=osm_types,
            osm_subtypes=osm_subtypes,
            geom_type=geom_type,
            bbox=bbox,
            epsg_code=epsg_code,
        )

        query = sql.SQL(" ").join(
            [
                sql_geojson_build,
                select_statement,
                from_statement,
                join_statement,
                where_clause,
                sql.SQL(") AS geojson;"),
            ]
        )

        result = self.__execute_postgis(query=query, params=tuple(params))
        geojson = result[0][0]
        if not self._is_valid_geojson_featurecollection(data=geojson):
            raise ValueError("The returned data is not in proper geojson format")
        return geojson
