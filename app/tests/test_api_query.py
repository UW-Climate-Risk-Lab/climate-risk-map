from unittest.mock import MagicMock, patch

import pytest
from geojson_pydantic import FeatureCollection
from psycopg2.sql import SQL, Composed, Identifier

from api.v1.app import query, schemas

TEST_BBOX = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "type": "rectangle",
                "_bounds": [
                    {"lat": 47.61402337357123, "lng": -119.32662963867189},
                    {"lat": 47.62651702078168, "lng": -119.27650451660158},
                ],
                "_leaflet_id": 11228,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-119.32662963867189, 47.61402337357123],
                        [-119.32662963867189, 47.62651702078168],
                        [-119.27650451660158, 47.62651702078168],
                        [-119.27650451660158, 47.61402337357123],
                        [-119.32662963867189, 47.61402337357123],
                    ]
                ],
            },
        },
        {
            "type": "Feature",
            "properties": {
                "type": "rectangle",
                "_bounds": [
                    {"lat": 47.49541671416695, "lng": -119.30191040039064},
                    {"lat": 47.50747495167563, "lng": -119.27444458007814},
                ],
                "_leaflet_id": 11242,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-119.30191040039064, 47.49541671416695],
                        [-119.30191040039064, 47.50747495167563],
                        [-119.27444458007814, 47.50747495167563],
                        [-119.27444458007814, 47.49541671416695],
                        [-119.30191040039064, 47.49541671416695],
                    ]
                ],
            },
        },
    ],
}


@pytest.mark.parametrize(
    "input_params, expected_select_statement, expected_params",
    [
        # Select with climate arguments
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
            ),
            Composed(
                [
                    SQL("SELECT "),
                    Composed(
                        [
                            Identifier("osm", "power_grid", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_type"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" AS tags"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s) AS geometry"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_AsText(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s), 3) AS geometry_wkt"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_X(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS longitude"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Y(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_subtype"),
                            SQL(", "),
                            Composed([Identifier("county"), SQL(".name AS county")]),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ssp")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".month")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".decade")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_mean")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_mean_historic_baseline")]),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
        # Climate query test case 1 - Any climate argument that is None will result in no climate columns returned
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
            ),
            Composed(
                [
                    SQL("SELECT "),
                    Composed(
                        [
                            Identifier("osm", "power_grid", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_type"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" AS tags"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s) AS geometry"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_AsText(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s), 3) AS geometry_wkt"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_X(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS longitude"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Y(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_subtype"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("county"),
                                    SQL(".name AS county"),
                                ]
                            ),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city")]),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
        # Select with climate_variable == "wildfire"
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="wildfire", # Test "wildfire"
                climate_decade=[2080, 2090],
                climate_month=[6, 7],
                climate_ssp=370,
            ),
            Composed(
                [
                    SQL("SELECT "),
                    Composed(
                        [
                            Identifier("osm", "power_grid", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_type"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" AS tags"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s) AS geometry"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_AsText(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s), 3) AS geometry_wkt"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_X(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS longitude"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Y(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "power_grid", "osm_subtype"),
                            SQL(", "),
                            Composed([Identifier("county"), SQL(".name AS county")]),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ssp")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".month")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".decade")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_mean")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_mean_historic_baseline")]),
                            SQL(", "), # Added for wildfire
                            Composed([Identifier("climate_table"), SQL(".burn_probability")]), # Added for wildfire
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
    ],
)
def test_create_select_statement(
    input_params, expected_select_statement, expected_params
):

    query_builder = query.GetDataQueryBuilder(input_params=input_params)

    generated_select_statement, generated_params = (
        query_builder._create_select_statement()
    )

    assert generated_select_statement == expected_select_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_from_statement",
    [
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
            ),
            Composed(
                [
                    SQL("FROM "),
                    Identifier("osm"),
                    SQL("."),
                    Identifier("power_grid"),
                ]
            ),
        )
    ],
)
def test_create_from_statement(input_params, expected_from_statement):

    query_builder = query.GetDataQueryBuilder(input_params=input_params)

    generated_from_statement = query_builder._create_from_statement()

    assert generated_from_statement == expected_from_statement


@pytest.mark.parametrize(
    "input_params, expected_join_statement, expected_params",
    [
        # Test case with all possible input params
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="fwi",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
            ),
            Composed(
                [
                    Composed(
                        [
                            Composed(
                                [
                                    Composed(
                                        [
                                            SQL("JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(" ON "),
                                            Identifier("osm"),
                                            SQL("."),
                                            SQL("power_grid"),
                                            SQL(".osm_id = "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(".osm_id"),
                                        ]
                                    ),
                                    SQL(" "),
                                    Composed(
                                        [
                                            SQL("LEFT JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("place_polygon"),
                                            SQL(" "),
                                            Identifier("county"),
                                            SQL("ON ST_Intersects("),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("power_grid"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(", "),
                                            Identifier("county"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(") AND "),
                                            Identifier("county"),
                                            SQL(".admin_level = %s "),
                                        ]
                                    ),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("place_polygon"),
                                    SQL(" "),
                                    Identifier("city"),
                                    SQL("ON ST_Intersects("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", "),
                                    Identifier("city"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(") AND "),
                                    Identifier("city"),
                                    SQL(".admin_level = %s "),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            SQL("INNER JOIN ("),
                            SQL(
                                "SELECT * "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("fwi"),
                                    SQL(" s "),
                                ]
                            ),
                            SQL("WHERE s.ssp = %s AND s.decade IN %s AND s.month IN %s"),
                            Composed([SQL(") AS "), Identifier("climate_table"), SQL(" ")]),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_table"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [6, 8, 126, (2060, 2070), (8, 9)],
        ),
        # Test case with climate_variable == "wildfire"
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="wildfire", # Test "wildfire"
                climate_decade=[2080, 2090],
                climate_month=[6, 7],
                climate_ssp=370,
            ),
            Composed(
                [
                    Composed( # Join for tags, county, city
                        [
                            Composed(
                                [
                                    Composed(
                                        [
                                            SQL("JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(" ON "),
                                            Identifier("osm"),
                                            SQL("."),
                                            SQL("power_grid"),
                                            SQL(".osm_id = "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(".osm_id"),
                                        ]
                                    ),
                                    SQL(" "),
                                    Composed(
                                        [
                                            SQL("LEFT JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("place_polygon"),
                                            SQL(" "),
                                            Identifier("county"),
                                            SQL("ON ST_Intersects("),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("power_grid"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(", "),
                                            Identifier("county"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(") AND "),
                                            Identifier("county"),
                                            SQL(".admin_level = %s "),
                                        ]
                                    ),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("place_polygon"),
                                    SQL(" "),
                                    Identifier("city"),
                                    SQL("ON ST_Intersects("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", "),
                                    Identifier("city"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(") AND "),
                                    Identifier("city"),
                                    SQL(".admin_level = %s "),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "), # Climate Join
                    Composed(
                        [
                            SQL("INNER JOIN ("),
                            SQL(
                                "SELECT * " # Subquery selects all columns
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("wildfire"), # Climate table is "wildfire"
                                    SQL(" s "),
                                ]
                            ),
                            SQL("WHERE s.ssp = %s AND s.decade IN %s AND s.month IN %s"),
                            Composed([SQL(") AS "), Identifier("climate_table"), SQL(" ")]),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_table"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [6, 8, 370, (2080, 2090), (6, 7)], # Params: county_admin_level, city_admin_level, ssp, decade_tuple, month_tuple
        ),
        # Test case no climate
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
            ),
            Composed(
                [
                    Composed(
                        [
                            Composed(
                                [
                                    SQL("JOIN "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    SQL("power_grid"),
                                    SQL(".osm_id = "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(".osm_id"),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("place_polygon"),
                                    SQL(" "),
                                    Identifier("county"),
                                    SQL("ON ST_Intersects("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", "),
                                    Identifier("county"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(") AND "),
                                    Identifier("county"),
                                    SQL(".admin_level = %s "),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            SQL("LEFT JOIN "),
                            Identifier("osm"),
                            SQL("."),
                            Identifier("place_polygon"),
                            SQL(" "),
                            Identifier("city"),
                            SQL("ON ST_Intersects("),
                            Identifier("osm"),
                            SQL("."),
                            Identifier("power_grid"),
                            SQL("."),
                            Identifier("geom"),
                            SQL(", "),
                            Identifier("city"),
                            SQL("."),
                            Identifier("geom"),
                            SQL(") AND "),
                            Identifier("city"),
                            SQL(".admin_level = %s "),
                        ]
                    ),
                ]
            ),
            [6, 8],
        ),
    ],
)
def test_create_join_statement(input_params, expected_join_statement, expected_params):

    query_builder = query.GetDataQueryBuilder(input_params=input_params)
    generated_join_statement, generated_params = query_builder._create_join_statement()

    assert generated_join_statement == expected_join_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_where_clause, expected_params",
    [
        (
            schemas.GetDataInputParameters(
                osm_category="power_grid",
                osm_types=["power"],
                osm_subtypes=["line"],
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                bbox=FeatureCollection(
                    type=TEST_BBOX["type"], features=TEST_BBOX["features"]
                ),
            ),
            Composed(
                [
                    Composed(
                        [
                            Composed(
                                [
                                    SQL("WHERE "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("osm_type"),
                                    SQL(" IN %s"),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("AND "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("power_grid"),
                                    SQL("."),
                                    Identifier("osm_subtype"),
                                    SQL(" IN %s"),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            Composed(
                                [
                                    Composed(
                                        [
                                            Composed(
                                                [
                                                    SQL("AND ("),
                                                    SQL(" "),
                                                    Composed(
                                                        [
                                                            SQL(
                                                                "ST_Intersects(ST_Transform("
                                                            ),
                                                            Identifier("osm"),
                                                            SQL("."),
                                                            Identifier(
                                                                "power_grid"
                                                            ),
                                                            SQL("."),
                                                            Identifier("geom"),
                                                            SQL(
                                                                ", %s), ST_GeomFromText(%s, %s))"
                                                            ),
                                                        ]
                                                    ),
                                                ]
                                            ),
                                            SQL(" "),
                                            SQL("OR"),
                                        ]
                                    ),
                                    SQL(" "),
                                    Composed(
                                        [
                                            SQL("ST_Intersects(ST_Transform("),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("power_grid"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(", %s), ST_GeomFromText(%s, %s))"),
                                        ]
                                    ),
                                ]
                            ),
                            SQL(" "),
                            SQL(")"),
                        ]
                    ),
                ]
            ),
            [
                ("power",),
                ("line",),
                4326,
                "POLYGON ((-119.32662963867189 47.61402337357123, -119.32662963867189 47.62651702078168, -119.27650451660158 47.62651702078168, -119.27650451660158 47.61402337357123, -119.32662963867189 47.61402337357123))",
                4326,
                4326,
                "POLYGON ((-119.30191040039064 47.49541671416695, -119.30191040039064 47.50747495167563, -119.27444458007814 47.50747495167563, -119.27444458007814 47.49541671416695, -119.30191040039064 47.49541671416695))",
                4326,
            ],
        )
    ],
)
def test_create_where_clause(input_params, expected_where_clause, expected_params):
    query_builder = query.GetDataQueryBuilder(input_params=input_params)
    generated_where_clause, generated_params = query_builder._create_where_clause()

    # print(f"Generated: {generated_where_clause.as_string(query_builder.input_params)}")
    # print(f"Expected: {expected_where_clause.as_string(query_builder.input_params)}")

    assert generated_where_clause == expected_where_clause
    assert generated_params == expected_params


def test_create_limit():
    # Set the limit value
    input_params = schemas.GetDataInputParameters(
        osm_category="power_grid",
        osm_types=["power"],
        osm_subtypes=["line"],
        epsg_code=4326,
        climate_variable="burntFractionAll",
        climate_decade=[2060, 2070],
        climate_month=[8, 9],
        climate_ssp=126,
        bbox=FeatureCollection(type=TEST_BBOX["type"], features=TEST_BBOX["features"]),
        limit=10,
    )
    query_builder = query.GetDataQueryBuilder(input_params=input_params)

    limit_statement, params = query_builder._create_limit()

    # Check the results
    assert limit_statement == SQL("LIMIT %s")
    assert params == [10]
