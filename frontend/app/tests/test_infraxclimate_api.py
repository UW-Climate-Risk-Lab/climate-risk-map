"""
The general philosphy for testing our "API" is to
primarily test the generated SQL statements, since the
data returned in practice may vary significantly.

By ensuring our SQL statements are built correctly, this will
ensure our return values are going to be what we expect.

"""

import pytest
from psycopg2 import sql
from unittest.mock import MagicMock, patch
from app.infraxclimate_api import infraXclimateAPI, infraXclimateInput

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
        # Centroid False test case
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                centroid=False,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.SQL("SELECT "),
                    sql.Composed(
                        [
                            sql.Identifier("osm", "infrastructure", "osm_id"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_type"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "tags", "tags"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.SQL("ST_Transform("),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("geom"),
                                    sql.SQL(", %s) AS geometry"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_subtype"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("county"),
                                    sql.SQL(".name AS county_name"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("city"), sql.SQL(".name AS city_name")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".ssp")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".month")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".decade")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".variable AS climate_variable"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".value AS climate_exposure"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".climate_metadata"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [4326],
        ),
        # Centroid True test case:
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                centroid=True,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.SQL("SELECT "),
                    sql.Composed(
                        [
                            sql.Identifier("osm", "infrastructure", "osm_id"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_type"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "tags", "tags"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.SQL("ST_Centroid(ST_Transform("),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("geom"),
                                    sql.SQL(", %s)) AS geometry"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_subtype"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("county"),
                                    sql.SQL(".name AS county_name"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("city"), sql.SQL(".name AS city_name")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".ssp")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".month")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("climate_data"), sql.SQL(".decade")]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".variable AS climate_variable"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".value AS climate_exposure"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".climate_metadata"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [4326],
        ),
        # Climate query test case 1 - Any climate argument that is None will result in no climate columns returned
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                centroid=False,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.SQL("SELECT "),
                    sql.Composed(
                        [
                            sql.Identifier("osm", "infrastructure", "osm_id"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_type"),
                            sql.SQL(", "),
                            sql.Identifier("osm", "tags", "tags"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.SQL("ST_Transform("),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("geom"),
                                    sql.SQL(", %s) AS geometry"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Identifier("osm", "infrastructure", "osm_subtype"),
                            sql.SQL(", "),
                            sql.Composed(
                                [
                                    sql.Identifier("county"),
                                    sql.SQL(".name AS county_name"),
                                ]
                            ),
                            sql.SQL(", "),
                            sql.Composed(
                                [sql.Identifier("city"), sql.SQL(".name AS city_name")]
                            ),
                        ]
                    ),
                ]
            ),
            [4326],
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
        centroid=input_params.centroid,
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
                centroid=False,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.SQL("FROM "),
                    sql.Identifier("osm"),
                    sql.SQL("."),
                    sql.Identifier("infrastructure"),
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
                centroid=False,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.Composed(
                        [
                            sql.Composed(
                                [
                                    sql.Composed(
                                        [
                                            sql.SQL("JOIN "),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.Identifier("tags"),
                                            sql.SQL(" ON "),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.SQL("infrastructure"),
                                            sql.SQL(".osm_id = "),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.Identifier("tags"),
                                            sql.SQL(".osm_id"),
                                        ]
                                    ),
                                    sql.SQL(" "),
                                    sql.Composed(
                                        [
                                            sql.SQL("LEFT JOIN "),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.Identifier("place_polygon"),
                                            sql.SQL(" "),
                                            sql.Identifier("county"),
                                            sql.SQL("ON ST_Intersects("),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.Identifier("infrastructure"),
                                            sql.SQL("."),
                                            sql.Identifier("geom"),
                                            sql.SQL(", "),
                                            sql.Identifier("county"),
                                            sql.SQL("."),
                                            sql.Identifier("geom"),
                                            sql.SQL(") AND "),
                                            sql.Identifier("county"),
                                            sql.SQL(".admin_level = %s "),
                                        ]
                                    ),
                                ]
                            ),
                            sql.SQL(" "),
                            sql.Composed(
                                [
                                    sql.SQL("LEFT JOIN "),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("place_polygon"),
                                    sql.SQL(" "),
                                    sql.Identifier("city"),
                                    sql.SQL("ON ST_Intersects("),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("geom"),
                                    sql.SQL(", "),
                                    sql.Identifier("city"),
                                    sql.SQL("."),
                                    sql.Identifier("geom"),
                                    sql.SQL(") AND "),
                                    sql.Identifier("city"),
                                    sql.SQL(".admin_level = %s "),
                                ]
                            ),
                        ]
                    ),
                    sql.SQL(" "),
                    sql.Composed(
                        [
                            sql.SQL("LEFT JOIN ("),
                            sql.SQL(
                                "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata "
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("FROM "),
                                    sql.Identifier("climate"),
                                    sql.SQL("."),
                                    sql.Identifier("scenariomip"),
                                    sql.SQL(" s "),
                                ]
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("LEFT JOIN "),
                                    sql.Identifier("climate"),
                                    sql.SQL("."),
                                    sql.Identifier("scenariomip_variables"),
                                    sql.SQL(" v "),
                                ]
                            ),
                            sql.SQL("ON s.variable_id = v.id "),
                            sql.SQL(
                                "WHERE v.ssp = %s AND v.variable = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            sql.Composed(
                                [
                                    sql.SQL(") AS "),
                                    sql.Identifier("climate_data"),
                                    sql.SQL(" "),
                                ]
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("ON "),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL(".osm_id = "),
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [6, 8, 126, "burntFractionAll", (2060, 2070), (8, 9)],
        ),
        # Test case with no city and no count
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                centroid=False,
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            sql.Composed(
                [
                    sql.Composed(
                        [
                            sql.SQL("JOIN "),
                            sql.Identifier("osm"),
                            sql.SQL("."),
                            sql.Identifier("tags"),
                            sql.SQL(" ON "),
                            sql.Identifier("osm"),
                            sql.SQL("."),
                            sql.SQL("infrastructure"),
                            sql.SQL(".osm_id = "),
                            sql.Identifier("osm"),
                            sql.SQL("."),
                            sql.Identifier("tags"),
                            sql.SQL(".osm_id"),
                        ]
                    ),
                    sql.SQL(" "),
                    sql.Composed(
                        [
                            sql.SQL("LEFT JOIN ("),
                            sql.SQL(
                                "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata "
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("FROM "),
                                    sql.Identifier("climate"),
                                    sql.SQL("."),
                                    sql.Identifier("scenariomip"),
                                    sql.SQL(" s "),
                                ]
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("LEFT JOIN "),
                                    sql.Identifier("climate"),
                                    sql.SQL("."),
                                    sql.Identifier("scenariomip_variables"),
                                    sql.SQL(" v "),
                                ]
                            ),
                            sql.SQL("ON s.variable_id = v.id "),
                            sql.SQL(
                                "WHERE v.ssp = %s AND v.variable = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            sql.Composed(
                                [
                                    sql.SQL(") AS "),
                                    sql.Identifier("climate_data"),
                                    sql.SQL(" "),
                                ]
                            ),
                            sql.Composed(
                                [
                                    sql.SQL("ON "),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL(".osm_id = "),
                                    sql.Identifier("climate_data"),
                                    sql.SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [126, "burntFractionAll", (2060, 2070), (8, 9)],
        ),
        # Test case no climate
        (
            infraXclimateInput(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                centroid=False,
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=False,
            ),
            sql.Composed(
                [
                    sql.SQL("JOIN "),
                    sql.Identifier("osm"),
                    sql.SQL("."),
                    sql.Identifier("tags"),
                    sql.SQL(" ON "),
                    sql.Identifier("osm"),
                    sql.SQL("."),
                    sql.SQL("infrastructure"),
                    sql.SQL(".osm_id = "),
                    sql.Identifier("osm"),
                    sql.SQL("."),
                    sql.Identifier("tags"),
                    sql.SQL(".osm_id"),
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
                centroid=False,
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
            sql.Composed(
                [
                    sql.Composed(
                        [
                            sql.Composed(
                                [
                                    sql.SQL("WHERE "),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("osm_type"),
                                    sql.SQL(" IN %s"),
                                ]
                            ),
                            sql.SQL(" "),
                            sql.Composed(
                                [
                                    sql.SQL("AND "),
                                    sql.Identifier("osm"),
                                    sql.SQL("."),
                                    sql.Identifier("infrastructure"),
                                    sql.SQL("."),
                                    sql.Identifier("osm_subtype"),
                                    sql.SQL(" IN %s"),
                                ]
                            ),
                        ]
                    ),
                    sql.SQL(" "),
                    sql.Composed(
                        [
                            sql.Composed(
                                [
                                    sql.Composed(
                                        [
                                            sql.Composed(
                                                [
                                                    sql.SQL("AND ("),
                                                    sql.SQL(" "),
                                                    sql.Composed(
                                                        [
                                                            sql.SQL(
                                                                "ST_Intersects(ST_Transform("
                                                            ),
                                                            sql.Identifier("osm"),
                                                            sql.SQL("."),
                                                            sql.Identifier(
                                                                "infrastructure"
                                                            ),
                                                            sql.SQL("."),
                                                            sql.Identifier("geom"),
                                                            sql.SQL(
                                                                ", %s), ST_GeomFromText(%s, %s))"
                                                            ),
                                                        ]
                                                    ),
                                                ]
                                            ),
                                            sql.SQL(" "),
                                            sql.SQL("OR"),
                                        ]
                                    ),
                                    sql.SQL(" "),
                                    sql.Composed(
                                        [
                                            sql.SQL("ST_Intersects(ST_Transform("),
                                            sql.Identifier("osm"),
                                            sql.SQL("."),
                                            sql.Identifier("infrastructure"),
                                            sql.SQL("."),
                                            sql.Identifier("geom"),
                                            sql.SQL(", %s), ST_GeomFromText(%s, %s))"),
                                        ]
                                    ),
                                ]
                            ),
                            sql.SQL(" "),
                            sql.SQL(")"),
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
