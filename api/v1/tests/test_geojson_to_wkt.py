from ..app import utils

def test_point():
    geojson = {
        "type": "Point",
        "coordinates": [30.0, 10.0]
    }
    expected_wkt = "POINT (30.0 10.0)"
    assert utils.geojson_to_wkt(geojson) == expected_wkt

def test_linestring():
    geojson = {
        "type": "LineString",
        "coordinates": [[30.0, 10.0], [10.0, 30.0], [40.0, 40.0]]
    }
    expected_wkt = "LINESTRING (30.0 10.0, 10.0 30.0, 40.0 40.0)"
    assert utils.geojson_to_wkt(geojson) == expected_wkt

def test_multilinestring():
    geojson = {
        "type": "MultiLineString",
        "coordinates": [
            [[10.0, 10.0], [20.0, 20.0], [10.0, 40.0]],
            [[40.0, 40.0], [30.0, 30.0], [40.0, 20.0], [30.0, 10.0]]
        ]
    }
    expected_wkt = "MULTILINESTRING ((10.0 10.0, 20.0 20.0, 10.0 40.0), (40.0 40.0, 30.0 30.0, 40.0 20.0, 30.0 10.0))"
    assert utils.geojson_to_wkt(geojson) == expected_wkt

def test_polygon():
    geojson = {
        "type": "Polygon",
        "coordinates": [
            [[30.0, 10.0], [40.0, 40.0], [20.0, 40.0], [10.0, 20.0], [30.0, 10.0]]
        ]
    }
    expected_wkt = "POLYGON ((30.0 10.0, 40.0 40.0, 20.0 40.0, 10.0 20.0, 30.0 10.0))"
    assert utils.geojson_to_wkt(geojson) == expected_wkt

def test_multipolygon():
    geojson = {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [[30.0, 20.0], [45.0, 40.0], [10.0, 40.0], [30.0, 20.0]]
            ],
            [
                [[15.0, 5.0], [40.0, 10.0], [10.0, 20.0], [5.0, 10.0], [15.0, 5.0]]
            ]
        ]
    }
    expected_wkt = "MULTIPOLYGON (((30.0 20.0, 45.0 40.0, 10.0 40.0, 30.0 20.0)), ((15.0 5.0, 40.0 10.0, 10.0 20.0, 5.0 10.0, 15.0 5.0)))"
    assert utils.geojson_to_wkt(geojson) == expected_wkt

def test_unsupported_geometry():
    geojson = {
        "type": "UnsupportedType",
        "coordinates": []
    }
    assert utils.geojson_to_wkt(geojson) == ""