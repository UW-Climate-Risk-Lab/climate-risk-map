from unittest.mock import MagicMock, patch

import pytest
from geojson_pydantic import FeatureCollection
from psycopg2.sql import SQL, Composed, Identifier

from ..app import query, schemas

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
                osm_category="infrastructure",
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
                            Identifier("osm", "infrastructure", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_type"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" AS osm_tags"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_subtype"),
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
                            Composed([Identifier("climate_table"), SQL(".ensemble_median")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_stddev")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_min")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_max")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_q1")]),
                            SQL(", "),
                            Composed([Identifier("climate_table"), SQL(".ensemble_q3")]),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
        # Climate query test case 1 - Any climate argument that is None will result in no climate columns returned
        (
            schemas.GetDataInputParameters(
                osm_category="infrastructure",
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
                            Identifier("osm", "infrastructure", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_type"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL("."),
                                    Identifier("tags"),
                                    SQL(" AS osm_tags"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_subtype"),
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
                osm_category="infrastructure",
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
                    Identifier("infrastructure"),
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
                osm_category="infrastructure",
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
                                            SQL("infrastructure"),
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
                                            Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                "SELECT s.osm_id, s.ssp, s.month, s.decade, "
                                "s.value_mean AS ensemble_mean, s.value_median AS ensemble_median, "
                                "s.value_stddev AS ensemble_stddev, s.value_min AS ensemble_min, "
                                "s.value_max AS ensemble_max, s.value_q1 AS ensemble_q1, "
                                "s.value_q3 AS ensemble_q3 "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("nasa_nex_fwi"),
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
                                    Identifier("infrastructure"),
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
        # Test case no climate
        (
            schemas.GetDataInputParameters(
                osm_category="infrastructure",
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
                                    SQL("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                            Identifier("infrastructure"),
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
                osm_category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
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
                                    Identifier("infrastructure"),
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
                                    Identifier("infrastructure"),
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
                                                                "infrastructure"
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
                                            Identifier("infrastructure"),
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

    assert generated_where_clause == expected_where_clause
    assert generated_params == expected_params


def test_create_limit():
    # Set the limit value
    input_params = schemas.GetDataInputParameters(
        osm_category="infrastructure",
        osm_types=["power"],
        osm_subtypes=["line"],
        county=True,
        city=True,
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
