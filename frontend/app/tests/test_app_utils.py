import pytest
import copy
import pandas as pd

# Import the module functions
from app.app_utils import (
    geojson_to_pandas,
    create_feature_toolip,
    process_output_csv,
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
                "latitude": 40.0,
                "longitude": -120.0,
                "geometry_wkt": "POINT(-120.0, 40.0)"
            },
            "geometry": {"type": "Point", "coordinates": [-120.0, 40.0]},
        },
        {
            "type": "Feature",
            "properties": {
                "tags": {"name": "Sample LineString", "type": "Point of Interest"},
                "latitude": 45.0,
                "longitude": -120.0,
                "geometry_wkt": "LINESTRING(-120.0 10.0, -119.0 11.0, -118.0 12.0)"
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


def test_geojson_to_pandas():
    df = geojson_to_pandas(copy.deepcopy(SAMPLE_GEOJSON))
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert "geometry_wkt" in df.columns


def test_geojson_to_pandas_empty():
    empty_geojson = {"type": "FeatureCollection", "features": []}
    df = geojson_to_pandas(empty_geojson)
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_create_feature_toolip():
    geojson_with_tooltip = create_feature_toolip(copy.deepcopy(SAMPLE_GEOJSON))
    tooltip = geojson_with_tooltip["features"][0]["properties"]["tooltip"]
    assert "<b>name<b>: Sample Point<br><b>type<b>: Point of Interest<br>" == tooltip


def test_create_feature_toolip_no_tags():
    with pytest.raises(ValueError):
        create_feature_toolip(copy.deepcopy(SAMPLE_GEOJSON_NO_TAGS))


def test_process_output_csv():
    df = process_output_csv(copy.deepcopy(SAMPLE_GEOJSON))
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
            copy.deepcopy(SAMPLE_GEOJSON),
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
                            "geometry_wkt": "POINT(-120.0, 40.0)"
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
                            "geometry_wkt": "LINESTRING(-120.0 10.0, -119.0 11.0, -118.0 12.0)"
                        },
                        "geometry": {"type": "Point", "coordinates": [-120.0, 45.0]},
                    },
                ],
            },
        ),
        (
            copy.deepcopy(SAMPLE_GEOJSON),
            ["LineString"],
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
                            "geometry_wkt": "POINT(-120.0, 40.0)"
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
                            "geometry_wkt": "LINESTRING(-120.0 10.0, -119.0 11.0, -118.0 12.0)"
                        },
                        "geometry": {
                            "type": "LineString",
                            "coordinates": [
                                [-120.0, 10.0],
                                [-119.0, 11.0],
                                [-118.0, 12.0],
                            ],
                        },
                    },
                ],
            },
        ),
    ],
)
def test_convert_geojson_feature_collection_to_points(
    geojson, preserve_types, expected_output
):
    converted_geojson = convert_geojson_feature_collection_to_points(
        geojson=geojson, preserve_types=preserve_types
    )
    assert converted_geojson == expected_output


def test_calc_bbox_area():
    leaflet_bbox = [
        {
            "type": "Feature",
            "properties": {
                "_bounds": [
                    {"lat": 10.0, "lng": 11.0},
                    {"lat": 11.0, "lng": 10.0},
                ]
            },
        }
    ]
    area = calc_bbox_area(leaflet_bbox)
    expected_area = (
        111**2
    )  # Calculated manually for sample coordinates assuming 1 deg lat ~= 111km

    assert pytest.approx(area, 0.1) == expected_area


def test_calc_bbox_area_no_features():
    area = calc_bbox_area([])
    assert area == 0


def test_calc_bbox_area_invalid_feature():
    invalid_feature = {"type": "Invalid", "properties": {}}
    area = calc_bbox_area([invalid_feature])
    assert area == 0
