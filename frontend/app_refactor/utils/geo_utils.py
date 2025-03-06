import math

import pandas as pd

from typing import Dict, List

def convert_geojson_feature_collection_to_points(
    geojson: Dict, preserve_types: List[str] = []
) -> Dict:
    """
    Convert all features in a GeoJSON FeatureCollection to Point geometries.
    This function processes a GeoJSON FeatureCollection and converts each feature's geometry
    to a Point. It assumes that the centroid latitude and longitude are present in the
    feature's properties under the keys 'latitude' and 'longitude'. If a feature's geometry
    is already a Point, it retains the original coordinates. Other properties and keys,
    such as 'id', are preserved.

    Args:
        geojson (Dict): A dictionary representing a GeoJSON FeatureCollection.
        preserve_types (List[str], optional): Geometry types to skip converting to points, e.g, 'LineString'
    Returns:
        Dict: A new GeoJSON FeatureCollection with all features converted to Point geometries.
    """

    def convert_geojson_feature_to_point(feature: Dict, preserve_types: List[str]) -> Dict:
        """Internal helper function
        """
        new_feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": []},
                "properties": feature.get("properties", {}),
            }

        # Only update if the geometry is not already a Point.
        if (feature["geometry"]["type"] != "Point") and (
            feature["geometry"]["type"] not in preserve_types
        ):
            properties = feature["properties"]
            new_feature["geometry"]["coordinates"] = [
                properties.get("longitude", 0.0),
                properties.get("latitude", 0.0),
            ]
        else:
            # If already a Point, retain the original coordinates
            new_feature["geometry"] = feature.get("geometry", {})

        # Preserve other keys like "id" if they exist
        if "id" in feature:
            new_feature["id"] = feature["id"]

        return new_feature

    new_geojson = {"type": "FeatureCollection", "features": []}

    # If this becomes a performance bottleneck, process features in parallel using ProcessPoolExecutor
    new_geojson["features"] = [convert_geojson_feature_to_point(feature, preserve_types) for feature in geojson["features"]]

    return new_geojson


def calc_bbox_area(features: List) -> float:
    """
    Rough calc for area of rectangle bounding box(es) from leaflet drawn shape

    Calc'd manually to avoid external package dependancy
    """
    total_area_sq_km = 0
    lat_km_per_deg = 110.574
    lon_km_per_deg = lambda lat: 111.320 * math.cos(math.radians(lat))
    for feature in features:
        if feature["type"].lower() != "feature":
            continue

        bounds = feature["properties"]["_bounds"]
        min_lat = min(bounds[0]["lat"], bounds[1]["lat"])
        max_lat = max(bounds[0]["lat"], bounds[1]["lat"])
        min_lon = min(bounds[0]["lng"], bounds[1]["lng"])
        max_lon = max(bounds[0]["lng"], bounds[1]["lng"])

        width_deg = abs(min_lat - max_lat)
        length_deg = abs(min_lon - max_lon)

        avg_lat = (min_lat + max_lat) / 2
        width_km = width_deg * lat_km_per_deg
        length_km = length_deg * lon_km_per_deg(avg_lat)

        total_area_sq_km += width_km * length_km

    return total_area_sq_km

def create_feature_toolip(geojson: dict):
    """Creates a property called "tooltip"

    The tooltip property is automatically displayed
    by dash leaflet as a popup when the mouse hover over the feature

    Args:
        geojson (dict): Dict in GeoJSON Format
    """

    # TODO: Add check to confirm it is a valid geojson

    for i, feature in enumerate(geojson["features"]):

        tooltip_str = ""
        
        # "tags" is a special property that relates to OpenStreetMap features

        if "tags" in feature["properties"].keys():
            for key, value in feature["properties"]["tags"].items():
                tooltip_str = tooltip_str + f"<b>{str(key)}<b>: {str(value)}<br>"
        else:
            for key, value in feature["properties"].items():
                tooltip_str = tooltip_str + f"<b>{key}<b>: {value}<br>"

        geojson["features"][i]["properties"]["tooltip"] = tooltip_str
    return geojson


def geojson_to_pandas(data: dict) -> pd.DataFrame:

    if data["features"] is None:
        return pd.DataFrame()

    properties = [feature["properties"] for feature in data["features"]]

    df = pd.DataFrame(properties)
    
    return df