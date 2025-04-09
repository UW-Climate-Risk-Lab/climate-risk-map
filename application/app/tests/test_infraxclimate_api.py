"""
The general philosphy for testing our "API" is to
primarily test the generated SQL statements, since the
data returned in practice may vary significantly.

By ensuring our SQL statements are built correctly, this will
ensure our return values are going to be what we expect.

"""

import pytest
from psycopg2.sql import SQL, Composed, Identifier
from unittest.mock import MagicMock, patch
from dao.api import infraXclimateAPI, infraXclimateInput

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


@pytest.fixture
def mock_conn():
    with patch("psycopg2.connect") as mock_connect:
        mock_conn_instance = MagicMock()
        mock_connect.return_value = mock_conn_instance
        yield mock_conn_instance


def test_get_data_invalid_category(mock_conn):
    # Arrange
    input_params = infraXclimateInput(
        category="unsupported-category", osm_types=["nonexistent-type"]
    )
    api = infraXclimateAPI(conn=mock_conn)

    with pytest.raises(ValueError):
        api.get_data(input_params)


@pytest.mark.parametrize(
    "input_params, expected_select_statement, expected_params",
    [
        # Select with climate arguments
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
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
                            Identifier("osm", "tags", "tags"),
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
                                [Identifier("county"), SQL(".name AS county_name")]
                            ),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city_name")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".ssp")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".month")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".decade")]),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_mean"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_median"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_stddev"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_min"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_max"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_q1"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".ensemble_q3"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [Identifier("climate_data"), SQL(".metadata")]
                            ),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
        # Climate query test case 1 - Any climate argument that is None will result in no climate columns returned
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=True,
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
                            Identifier("osm", "tags", "tags"),
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
                                    SQL(".name AS county_name"),
                                ]
                            ),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city_name")]),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
    ],
)
def test_create_select_statement(
    input_params, expected_select_statement, expected_params, mock_conn
):

    api = infraXclimateAPI(conn=mock_conn)

    generated_select_statement, generated_params = api._create_select_statement(
        params=[],
        primary_table=input_params.category,
        osm_subtypes=input_params.osm_subtypes,
        county=input_params.county,
        city=input_params.city,
        epsg_code=input_params.epsg_code,
        climate_variable=input_params.climate_variable,
        climate_decade=input_params.climate_decade,
        climate_month=input_params.climate_month,
        climate_ssp=input_params.climate_ssp,
        climate_metadata=input_params.climate_metadata,
    )

    assert generated_select_statement == expected_select_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_from_statement",
    [
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
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
def test_create_from_statement(input_params, expected_from_statement, mock_conn):

    api = infraXclimateAPI(conn=mock_conn)

    generated_from_statement = api._create_from_statement(
        primary_table=input_params.category
    )

    assert generated_from_statement == expected_from_statement


@pytest.mark.parametrize(
    "input_params, expected_join_statement, expected_params",
    [
        # Test case with all possible input params
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
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
                                "SELECT s.osm_id, s.ssp, s.month, s.decade, s.value_mean AS ensemble_mean, s.value_median AS ensemble_median, s.value_stddev AS ensemble_stddev, s.value_min AS ensemble_min, s.value_max AS ensemble_max, s.value_q1 AS ensemble_q1, s.value_q3 AS ensemble_q3 "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("nasa_nex_burntFractionAll"),
                                    SQL(" s "),
                                ]
                            ),
                            SQL(
                                "WHERE s.ssp = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            Composed(
                                [
                                    SQL(") AS "),
                                    Identifier("climate_data"),
                                    SQL(" "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_data"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [6, 8, 126, (2060, 2070), (8, 9)],
        ),
        # Test case with no city and no count
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
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
                            SQL("INNER JOIN ("),
                            SQL(
                                "SELECT s.osm_id, s.ssp, s.month, s.decade, s.value_mean AS ensemble_mean, s.value_median AS ensemble_median, s.value_stddev AS ensemble_stddev, s.value_min AS ensemble_min, s.value_max AS ensemble_max, s.value_q1 AS ensemble_q1, s.value_q3 AS ensemble_q3 "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("nasa_nex_burntFractionAll"),
                                    SQL(" s "),
                                ]
                            ),
                            SQL(
                                "WHERE s.ssp = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            Composed(
                                [
                                    SQL(") AS "),
                                    Identifier("climate_data"),
                                    SQL(" "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_data"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [126, (2060, 2070), (8, 9)],
        ),
        # Test case no climate
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=False,
            ),
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
            [],
        ),
    ],
)
def test_create_join_statement(
    input_params, expected_join_statement, expected_params, mock_conn
):

    api = infraXclimateAPI(mock_conn)
    params = []
    generated_join_statement, generated_params = api._create_join_statement(
        params=params,
        primary_table=input_params.category,
        county=input_params.county,
        city=input_params.city,
        climate_variable=input_params.climate_variable,
        climate_decade=input_params.climate_decade,
        climate_month=input_params.climate_month,
        climate_ssp=input_params.climate_ssp,
    )
    assert generated_join_statement == expected_join_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_where_clause, expected_params",
    [
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
                bbox=TEST_BBOX,
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
def test_create_where_clause(
    input_params, expected_where_clause, expected_params, mock_conn
):
    api = infraXclimateAPI(mock_conn)
    params = []
    generated_where_clause, generated_params = api._create_where_clause(
        params=params,
        primary_table=input_params.category,
        osm_types=input_params.osm_types,
        osm_subtypes=input_params.osm_subtypes,
        geom_type=input_params.geom_type,
        bbox=input_params.bbox,
        epsg_code=input_params.epsg_code,
    )

    assert generated_where_clause == expected_where_clause
    assert generated_params == expected_params
