"""
Interface with PostGIS Database for getting asset data. 
Create class to manage queries across the same connection the class is initialized with

#TODO: Logging
#TODO: Unit Tests
#TODO: Migrate to standalone API
"""

import psycopg2 as pg
from psycopg2 import sql
from geojson_pydantic import FeatureCollection
from pydantic import BaseModel, model_validator, ValidationError
from typing import List, Dict, Tuple, Optional, Any

# TEST CHANGE, DELETE THIS COMMENT


class infraXclimateInput(BaseModel):
    """Used to validate input parameters

    category (str): OSM Category to get data from.
    osm_types (List[str]): OSM Type to filter on.
    osm_subtypes (List[str]): OSM Subtypes to filter on.
    bbox (FeatureCollection): A Dict in the GeoJSON Feature Collection format. Used for filtering.
    county (bool): If True, returns the county of the feature as a property.
    city (bool): If True, returns the city of the feature as a property.
    epsg_code (int): Spatial reference ID, default is 4326 (Representing EPSG:4326).
    geom_type (str): If used, returns only features of the specified geom_type.
    climate_variable (str): Climate variable to filter on.
    climate_ssp (int): Climate SSP (Shared Socioeconomic Pathway) to filter on.
    climate_month (List[int]): List of months to filter on.
    climate_decade (List[int]): List of decades to filter on.
    climate_metadata (bool): Returns metadata of climate variable as dict.

    """

    category: str
    osm_types: List[str]
    osm_subtypes: Optional[List[str]] = None
    bbox: Optional[FeatureCollection] = None
    county: bool = False
    city: bool = False
    epsg_code: int = 4326
    geom_type: Optional[str] = None
    climate_variable: Optional[str] = None
    climate_ssp: Optional[int] = None
    climate_month: Optional[List[int]] = None
    climate_decade: Optional[List[int]] = None
    climate_metadata: bool = False

    # Custom validator to check that if climate data is provided, all required fields are present
    @model_validator(mode="after")
    def check_climate_params(self):
        if any(
            param is not None
            for param in [
                self.climate_variable,
                self.climate_ssp,
                self.climate_month,
                self.climate_decade,
            ]
        ):
            if self.climate_variable is None:
                raise ValueError(
                    "climate_variable is required when requesting climate data"
                )
            if self.climate_ssp is None:
                raise ValueError("climate_ssp is required when requesting climate data")
            if self.climate_month is None:
                raise ValueError(
                    "climate_month is required when requesting climate data"
                )
            if self.climate_decade is None:
                raise ValueError(
                    "climate_decade is required when requesting climate data"
                )
        return self


class infraXclimateOutput(BaseModel):
    """Checks output is a GeoJSON"""

    geojson: FeatureCollection


class infraXclimateAPI:
    def __init__(self, conn: pg.extensions.connection):
        """The following API class takes a direct connection
        to the database and performs SQL queries.

        It is based on data loaded using PG-OSM Flex (https://pgosm-flex.com).
        As of Aug-2024, PG-OSM Flex is used as the ETL process for OpenStreetMap Data.

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
        self.available_categories = ["infrastructure", self.osm_table_places, "data_center", "shop"]

        self.climate_schema = "climate"
        self.climate_table_alias = "climate_data"  # Table alias from nested join between scenariomip and scenariomip variables

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn:
            self.conn.close()

    def __del__(self):
        if self.conn:
            self.conn.close()

    def _execute_postgis(self, query: str, params: Tuple[str] = None):
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

        result = self._execute_postgis(query, (table_name + "%",))
        column_names = set([row[0] for row in result])

        return list(column_names)

    def _create_admin_table_conditions(self, condition: str) -> Dict:

        admin_conditions = {
            "condition": condition,
            "level": self.osm_table_places_admin_levels[condition],
            "alias": condition,
        }

        return admin_conditions

    def _create_select_statement(
        self,
        params: List[Any],
        primary_table: str,
        epsg_code: int,
        osm_subtypes: List[str],
        county: bool,
        city: bool,
        climate_variable: str,
        climate_month: int,
        climate_decade: int,
        climate_ssp: int,
        climate_metadata: bool,
    ) -> Tuple[sql.SQL, List[Any]]:
        """Bulids a dynamic SQL SELECT statement for the get_osm_data method"""

        # NOTE, we use ST_Centroid() to get lat/lon values for non-point shapes.
        # The lat/lon returned is not guarenteed to be on the shape itself, and represents the
        # geometric center of mass of the shape. This should be fine for polygons and points,
        # but may be meaningless for long linestrings.
        # Aleternative methods may be ST_PointOnSurface() or ST_LineInterpolatePoint(), however
        # these are more computationally expensive and for now not worth the implementation.

        # Initial list of fields that are always returned
        select_fields = [
            sql.Identifier(self.osm_schema, primary_table, "osm_id"),
            sql.Identifier(self.osm_schema, primary_table, "osm_type"),
            sql.Identifier(self.osm_schema, self.osm_table_tags, "tags"),
            sql.SQL("ST_Transform({schema}.{table}.{column}, %s) AS geometry").format(
                schema=sql.Identifier(self.osm_schema),
                table=sql.Identifier(primary_table),
                column=sql.Identifier(self.osm_column_geom),
            ),
            sql.SQL(
                "ST_AsText(ST_Transform({schema}.{table}.{column}, %s), 3) AS geometry_wkt"
            ).format(
                schema=sql.Identifier(self.osm_schema),
                table=sql.Identifier(primary_table),
                column=sql.Identifier(self.osm_column_geom),
            ),
            sql.SQL(
                "ST_X(ST_Centroid(ST_Transform({schema}.{table}.{column}, %s))) AS longitude"
            ).format(
                schema=sql.Identifier(self.osm_schema),
                table=sql.Identifier(primary_table),
                column=sql.Identifier(self.osm_column_geom),
            ),
            sql.SQL(
                "ST_Y(ST_Centroid(ST_Transform({schema}.{table}.{column}, %s))) AS latitude"
            ).format(
                schema=sql.Identifier(self.osm_schema),
                table=sql.Identifier(primary_table),
                column=sql.Identifier(self.osm_column_geom),
            ),
        ]
        params.extend([epsg_code] * 4)

        # Add extra where clause for subtypes if they are specified
        if osm_subtypes:
            select_fields.append(
                sql.Identifier(self.osm_schema, primary_table, "osm_subtype")
            )

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

        if climate_variable and climate_ssp and climate_month and climate_decade:
            select_fields.append(
                sql.SQL("{climate_table_alias}.ssp").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.month").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.decade").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_mean").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_median").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_stddev").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_min").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_max").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_q1").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.ensemble_q3").format(
                    climate_schema=sql.Identifier(self.climate_schema),
                    climate_table_alias=sql.Identifier(self.climate_table_alias),
                )
            )
            if climate_metadata:
                select_fields.append(
                    sql.SQL("{climate_table_alias}.metadata").format(
                        climate_schema=sql.Identifier(self.climate_schema),
                        climate_table_alias=sql.Identifier(self.climate_table_alias),
                    )
                )

        select_statement = sql.SQL("SELECT {columns}").format(
            columns=sql.SQL(", ").join(select_fields)
        )
        return select_statement, params

    def _create_from_statement(self, primary_table: str) -> sql.SQL:

        from_statement = sql.SQL("FROM {schema}.{table}").format(
            schema=sql.Identifier(self.osm_schema), table=sql.Identifier(primary_table)
        )
        return from_statement

    def _create_join_statement(
        self,
        params: List[Any],
        primary_table: str,
        county: bool,
        city: bool,
        climate_variable: str,
        climate_month: int,
        climate_decade: int,
        climate_ssp: int,
    ) -> Tuple[sql.SQL, List[Any]]:

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

        if climate_variable and climate_ssp and climate_month and climate_decade:
            climate_table = f"nasa_nex_{climate_variable}"
            climate_join = sql.Composed(
                [
                    sql.SQL("INNER JOIN ("),
                    sql.SQL(
                        "SELECT s.osm_id, s.ssp, s.month, s.decade, s.value_mean AS ensemble_mean, s.value_median AS ensemble_median, s.value_stddev AS ensemble_stddev, s.value_min AS ensemble_min, s.value_max AS ensemble_max, s.value_q1 AS ensemble_q1, s.value_q3 AS ensemble_q3 "
                    ),
                    sql.SQL("FROM {climate_schema}.{climate_table} s ").format(
                        climate_schema=sql.Identifier(self.climate_schema),
                        climate_table=sql.Identifier(climate_table),
                    ),
                    sql.SQL("WHERE s.ssp = %s AND s.decade IN %s AND s.month IN %s"),
                    sql.SQL(") AS {climate_table_alias} ").format(
                        climate_table_alias=sql.Identifier(self.climate_table_alias)
                    ),
                    sql.SQL(
                        "ON {schema}.{primary_table}.osm_id = {climate_table_alias}.osm_id"
                    ).format(
                        schema=sql.Identifier(self.osm_schema),
                        primary_table=sql.Identifier(primary_table),
                        climate_table_alias=sql.Identifier(self.climate_table_alias),
                    ),
                ]
            )
            params += [
                climate_ssp,
                tuple(set(climate_decade)),
                tuple(set(climate_month)),
            ]

            join_statement = sql.SQL(" ").join([join_statement, climate_join])

        return join_statement, params

    def _create_where_clause(
        self,
        params: List[Any],
        primary_table: str,
        osm_types: List[str],
        osm_subtypes: List[str],
        geom_type: str,
        bbox: FeatureCollection,
        epsg_code: int,
    ) -> Tuple[sql.SQL, List[Any]]:

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
            for feature in bbox.features:
                if count == 0:
                    pass
                else:
                    conditional = sql.SQL("OR")
                    bbox_filter = sql.SQL(" ").join([bbox_filter, conditional])
                feature_filter = sql.SQL(
                    "ST_Intersects(ST_Transform({schema}.{primary_table}.{geom_column}, %s), ST_GeomFromText(%s, %s))"
                ).format(
                    schema=sql.Identifier(self.osm_schema),
                    primary_table=sql.Identifier(primary_table),
                    geom_column=sql.Identifier(self.osm_column_geom),
                )
                params.append(epsg_code)
                params.append(feature.geometry.wkt)
                params.append(epsg_code)
                bbox_filter = sql.SQL(" ").join([bbox_filter, feature_filter])
                count += 1
            bbox_filter = sql.SQL(" ").join([bbox_filter, sql.SQL(")")])

            where_clause = sql.SQL(" ").join([where_clause, bbox_filter])

        return where_clause, params

    def get_climate_metadata(self, climate_variable: str, ssp: str) -> Dict:
        """Returns climate metadata JSON blob for given climate_variable and ssp

        Args:
            climate_variable (str): climate variable name
            ssp (str): SSP number

        Returns:
            Dict: JSON blob of climate metadata
        """
        table = f"nasa_nex_{climate_variable}_metadata"

        query = sql.SQL(
            "SELECT min_value, max_value FROM {schema}.{table};"
        ).format(
            schema=sql.Identifier(self.climate_schema),
            table=sql.Identifier(table),
        )

        result = self._execute_postgis(query=query, params=(ssp,))

        metadata = {
            "UW_CRL_DERIVED": {
                "min_climate_variable_value": result[0][0],
                "max_climate_variable_value": result[0][1],
                "units": ''
            }
        }

        return metadata

    def get_data(self, input_params: infraXclimateInput) -> Dict:
        """
        Gets infrastructure and climate data from provided filters and returns a GeoJSON Dict.

        Args:
            input_params (infraXclimateInput): Instance containing query parameters

        Returns:
            Dict: A GeoJSON dictionary of the queried data.


        If multiple months/decades are passed in, a feature will be returned for each time step.

        Latitude and Longitude are returned as properties separate from the geometry. These
        represent the geometric center of mass of the given feature,
        and are not guarenteed to actually intersect with the feature itself.

        Example SQL query created from all params:
        --------------------------------------------------------------------------------
            SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(geojson.*)::json)
            )
            FROM (
            SELECT
                "osm"."infrastructure"."osm_id",
                "osm"."infrastructure"."osm_type",
                "osm"."tags"."tags",
                ST_Transform("osm"."infrastructure"."geom", %s) AS geometry,
                ST_X(ST_Centroid(ST_Transform("osm"."."infrastructure"."geom", %s))) AS longitude,
                ST_Y(ST_Centroid(ST_Transform("osm"."."infrastructure"."geom", %s))) AS latitude,
                "county".name AS county_name,
                "city".name AS city_name,
                "climate_data".ssp,
                "climate_data".month,
                "climate_data".decade,
                "climate_data".variable AS climate_variable,
                "climate_data".value AS climate_exposure,
                "climate_data".climate_metadata
            FROM
                "osm"."infrastructure"
            JOIN "osm"."tags" ON "osm"."infrastructure"."osm_id" = "osm"."tags"."osm_id"
            LEFT JOIN "osm"."place_polygon" AS "county" ON
                ST_Intersects("osm"."infrastructure"."geom", "county"."geom") AND "county".admin_level = %s
            LEFT JOIN "osm"."place_polygon" AS "city" ON
                ST_Intersects("osm"."infrastructure"."geom", "city"."geom") AND "city".admin_level = %s
            LEFT JOIN (
                SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata
                FROM "climate"."scenariomip" s
                LEFT JOIN "climate"."scenariomip_variables" v ON s.variable_id = v.id
                WHERE v.ssp = %s
                AND v.variable = %s
                AND s.decade IN %s
                AND s.month IN %s
            ) AS "climate_data"
            ON
                "osm"."infrastructure".osm_id = "climate_data".osm_id
            WHERE
                "osm"."infrastructure"."osm_type" IN %s
            AND (
                ST_Intersects(
                ST_Transform("osm"."infrastructure"."geom", %s),
                ST_GeomFromGeoJSON(%s)
                )
            OR
                ST_Intersects(
                ST_Transform("osm"."infrastructure"."geom", %s),
                ST_GeomFromGeoJSON(%s)
                )
            )
            ) AS geojson;
        --------------------------------------------------------------------------------

        """

        category = input_params.category
        osm_types = input_params.osm_types
        osm_subtypes = input_params.osm_subtypes
        bbox = input_params.bbox
        county = input_params.county
        city = input_params.city
        epsg_code = input_params.epsg_code
        geom_type = input_params.geom_type
        climate_variable = input_params.climate_variable
        climate_ssp = input_params.climate_ssp
        climate_month = input_params.climate_month
        climate_decade = input_params.climate_decade
        climate_metadata = input_params.climate_metadata

        # Primary table will actually be a materialized view of the given category
        primary_table = category if category in self.available_categories else None
        if primary_table is None:
            raise ValueError(f"The category {category} is not currently available!")

        primary_table_columns = self._get_table_columns(table_name=primary_table)

        # Some categories do not have osm_subtype (like the "places" category)
        if "osm_subtype" not in primary_table_columns:
            osm_subtypes = None

        # Params are added in order while creating SQL statements
        query_params = []

        # This builds a query to return a GeoJSON object
        # This method should always return a GeoJSON to the client
        geojson_statement = sql.SQL(
            """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(geojson.*)::json))
            
            FROM (
        """
        )

        select_statement, query_params = self._create_select_statement(
            params=query_params,
            primary_table=primary_table,
            epsg_code=epsg_code,
            osm_subtypes=osm_subtypes,
            county=county,
            city=city,
            climate_variable=climate_variable,
            climate_ssp=climate_ssp,
            climate_decade=climate_decade,
            climate_month=climate_month,
            climate_metadata=climate_metadata,
        )

        from_statement = self._create_from_statement(primary_table=primary_table)

        join_statement, query_params = self._create_join_statement(
            primary_table=primary_table,
            params=query_params,
            county=county,
            city=city,
            climate_variable=climate_variable,
            climate_ssp=climate_ssp,
            climate_decade=climate_decade,
            climate_month=climate_month,
        )

        where_clause, query_params = self._create_where_clause(
            primary_table=primary_table,
            params=query_params,
            osm_types=osm_types,
            osm_subtypes=osm_subtypes,
            geom_type=geom_type,
            bbox=bbox,
            epsg_code=epsg_code,
        )

        query = sql.SQL(" ").join(
            [
                geojson_statement,
                select_statement,
                from_statement,
                join_statement,
                where_clause,
                sql.SQL(") AS geojson;"),
            ]
        )

        result = self._execute_postgis(query=query, params=tuple(query_params))
        geojson = result[0][0]
        try:
            if geojson["features"] is None:
                geojson["features"] = []
            infraXclimateOutput(geojson=geojson)
        except ValidationError as e:
            print(e)

        return geojson
