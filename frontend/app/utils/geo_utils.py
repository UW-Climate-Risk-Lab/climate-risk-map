import math

import pandas as pd

from typing import List


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


def geojson_to_pandas(data: dict) -> pd.DataFrame:

    if data["features"] is None:
        return pd.DataFrame()

    properties = [feature["properties"] for feature in data["features"]]

    df = pd.DataFrame(properties)

    return df
