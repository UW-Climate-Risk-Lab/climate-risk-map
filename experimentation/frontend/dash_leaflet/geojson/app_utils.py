import psycopg2
import httpx
import os
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
from dash_extensions.javascript import assign

import app_config

TITILER_BASE_ENDPOINT = os.environ["TITILER_BASE_ENDPOINT"]
FILE_URL = os.environ["FILE_URL"]
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]


def query_titiler(endpoint: str, params):
    try:
        r = httpx.get(url=endpoint, params=params)
    except Exception as e:
        # TODO: Add logging
        print(str(e))
        raise ConnectionError("Unable to connect to Titiler Endpoint!")

    r.raise_for_status()
    return r.json()


def get_climate_min_max(file_url: str):
    endpoint = f"{TITILER_BASE_ENDPOINT}/cog/statistics"
    params = {"url": file_url, "bidx": [1]}
    r = query_titiler(endpoint, params)

    # b1 refers to "band 1". Currently the test data is a single band
    min_climate_value = r["b1"]["min"]
    max_climate_value = r["b1"]["max"]

    return min_climate_value, max_climate_value


def get_tilejson_url(file_url: str, climate_variable: str, min_climate_value: str, max_climate_value: str, colormap: str):

    endpoint = f"{TITILER_BASE_ENDPOINT}/cog/tilejson.json"
    params = {
        "tileMatrixSetId": "WebMercatorQuad",
        "url": file_url,
        "rescale": f"{min_climate_value},{max_climate_value}",
        "colormap_name": colormap,
    }
    r = query_titiler(endpoint=endpoint, params=params)
    return r["tiles"][0]


def geojson_to_geopandas(geojson: dict) -> gpd.GeoDataFrame:
    """
    Convert a GeoJSON object to a GeoPandas DataFrame.

    Args:
        geojson (dict): GeoJSON object.

    Returns:
        gpd.GeoDataFrame: GeoPandas DataFrame.
    """
    # Convert GeoJSON features into a list of dictionaries with geometry and properties
    features = geojson["features"]
    geometries = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]

    # Create a GeoPandas DataFrame
    gdf = gpd.GeoDataFrame(properties, geometry=geometries)
    return gdf


def create_feature_toolip(geojson: dict):
    """Creates a property called "tooltip"

    The tooltip property is automatically displayed
    by dash leaflet as a popup when the mouse hover over the feature

    Args:
        geojson (dict): Dict in GeoJSON Format
    """

    # TODO: Add check to confirm it is a valid geojson

    for i, feature in enumerate(geojson["features"]):

        # For now, our tooltip will depend on the OSM tags
        if "tags" not in feature["properties"].keys():
            raise ValueError("Tags arent available in GeoJSON!")

        tooltip_str = ""

        for key, value in feature["properties"]["tags"].items():
            tooltip_str = tooltip_str + f"<b>{key}<b>: {value}<br>"

        geojson["features"][i]["properties"]["tooltip"] = tooltip_str
    return geojson


def process_output_csv(data: dict) -> pd.DataFrame:

    if data["features"] is None:
        return pd.DataFrame()

    gdf = geojson_to_geopandas(geojson=data)

    gdf["latitude"] = gdf.geometry.centroid.y
    gdf["longitude"] = gdf.geometry.centroid.x

    df = pd.DataFrame(gdf)
    return df


def create_custom_icon(icon_url: str):

    icon_func = assign(
        """function(feature, latlng){{
const custom_icon = L.icon({{iconUrl: `{}`, iconSize: [15, 15]}});
return L.marker(latlng, {{icon: custom_icon}});
}}""".format(
            icon_url
        )
    )

    return icon_func
