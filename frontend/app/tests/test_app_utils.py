import pytest
from unittest.mock import patch, MagicMock
import httpx
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon
from concurrent.futures import ThreadPoolExecutor

# Import the module functions
from app.app_utils import (
    geojson_to_geopandas,
    create_feature_toolip,
    process_output_csv,
    create_custom_icon,
    convert_geojson_feature_collection_to_points,
    calc_bbox_area,
)

# Sample data for testing
SAMPLE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "tags": {"name": "Sample Point", "type": "Point of Interest"},
                "latitude": 45.0,
                "longitude": -120.0,
            },
            "geometry": {"type": "Point", "coordinates": [-120.0, 10.0]},
        },
        {
            "type": "Feature",
            "properties": {
                "tags": {"name": "Sample LineString", "type": "Point of Interest"},
                "latitude": 45.0,
                "longitude": -120.0,
            },
            "geometry": {
                "type": "LineString",
                "coordinates": [[-120.0, 10.0], [-119.0, 11.0], [-118.0, 12.0]],
            },
        },
    ],
}

SAMPLE_GEOJSON_NO_TAGS = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {},
            "geometry": {"type": "Point", "coordinates": [20.0, 10.0]},
        }
    ],
}

SAMPLE_FEATURE = [
    {
        "type": "Feature",
        "properties": {
            "_bounds": [
                {"lat": 10.0, "lng": 20.0},
                {"lat": 15.0, "lng": 25.0},
            ]
        },
    }
]


def test_geojson_to_geopandas():
    gdf = geojson_to_geopandas(SAMPLE_GEOJSON)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert not gdf.empty
    assert "geometry" in gdf.columns


def test_geojson_to_geopandas_empty():
    empty_geojson = {"type": "FeatureCollection", "features": []}
    gdf = geojson_to_geopandas(empty_geojson)
    assert isinstance(gdf, gpd.GeoDataFrame)
    assert gdf.empty


def test_create_feature_toolip():
    geojson_with_tooltip = create_feature_toolip(SAMPLE_GEOJSON.copy())
    tooltip = geojson_with_tooltip["features"][0]["properties"]["tooltip"]
    assert "<b>name<b>: Sample Point<br><b>type<b>: Point of Interest<br>" == tooltip


def test_create_feature_toolip_no_tags():
    with pytest.raises(ValueError):
        create_feature_toolip(SAMPLE_GEOJSON_NO_TAGS.copy())


def test_process_output_csv():
    df = process_output_csv(SAMPLE_GEOJSON)
    assert isinstance(df, pd.DataFrame)
    assert "latitude" in df.columns
    assert "longitude" in df.columns


def test_process_output_csv_no_features():
    data = {"type": "FeatureCollection", "features": None}
    df = process_output_csv(data)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


@pytest.mark.parametrize(
    "geojson, preserve_types, expected_output",
    [
        (
            SAMPLE_GEOJSON.copy(),
            [],
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {
                            "tags": {
                                "name": "Sample Point",
                                "type": "Point of Interest",
                            },
                            "latitude": 40.0,
                            "longitude": -120.0,
                        },
                        "geometry": {"type": "Point", "coordinates": [-120.0, 40.0]},
                    },
                    {
                        "type": "Feature",
                        "properties": {
                            "tags": {
                                "name": "Sample LineString",
                                "type": "Point of Interest",
                            },
                            "latitude": 45.0,
                            "longitude": -120.0,
                        },
                        "geometry": {"type": "Point", "coordinates": [-120.0, 45.0]},
                    },
                ],
            },
        )
    ],
)
def test_convert_geojson_feature_collection_to_points(
    geojson, preserve_types, expected_output
):
    converted_geojson = convert_geojson_feature_collection_to_points(
        geojson=geojson, preserve_types=preserve_types
    )
    assert converted_geojson == expected_output


def test_convert_geojson_preserve_types():
    sample_line_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"latitude": 10.0, "longitude": 20.0},
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[20.0, 10.0], [21.0, 11.0]],
                },
            }
        ],
    }
    converted_geojson = convert_geojson_feature_collection_to_points(
        sample_line_geojson, preserve_types=["LineString"]
    )
    assert converted_geojson["features"][0]["geometry"]["type"] == "LineString"


def test_calc_bbox_area():
    area = calc_bbox_area(sample_features_with_bounds)
    expected_area = 6103.515  # Calculated manually for these coordinates
    assert pytest.approx(area, 0.1) == expected_area


def test_calc_bbox_area_no_features():
    area = calc_bbox_area([])
    assert area == 0


def test_calc_bbox_area_invalid_feature():
    invalid_feature = {"type": "Invalid", "properties": {}}
    area = calc_bbox_area([invalid_feature])
    assert area == 0
