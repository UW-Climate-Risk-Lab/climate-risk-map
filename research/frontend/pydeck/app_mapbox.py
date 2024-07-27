import pandas as pd
import os
import plotly.express as px
from dotenv import load_dotenv
import httpx
load_dotenv()
px.set_mapbox_access_token(os.environ["MAPBOX_KEY"])
us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")

import plotly.express as px

fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=1000)

TITILER_BASE_ENDPOINT = "http://localhost:8000"
FILE_URL = "http://fileserver:8080/OutputCOG.tif"


def query_titiler(endpoint: str, params):
    r = httpx.get(url=endpoint, params=params)
    r.raise_for_status()
    return r.json()


def get_climate_min_max():
    endpoint = f"{TITILER_BASE_ENDPOINT}/cog/statistics"
    params = {"url": FILE_URL, "bidx": [1]}
    r = query_titiler(endpoint, params)

    # b1 refers to "band 1". Currently the test data is a single band
    min_climate_value = r["b1"]["min"]
    max_climate_value = r["b1"]["max"]

    return min_climate_value, max_climate_value


def get_tilejson_url():

    # Get min and max climate data variables to resecale
    min_climate_value, max_climate_value = get_climate_min_max()
    r = httpx.get(
        f"{TITILER_BASE_ENDPOINT}/cog/tilejson.json",
        params={
            "tileMatrixSetId": "WebMercatorQuad",
            "url": FILE_URL,
            "rescale": f"{min_climate_value},{max_climate_value}",
            "colormap_name": "reds",
        },
    ).json()
    return r["tiles"][0]
fig.update_layout(
    mapbox_layers=[
        {
            "below": 'traces',
            "sourcetype": "raster",
            "sourceattribution": "United States Geological Survey",
            "source": [
                "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}"
            ]
        },
        {
            "below": 'traces',
            "sourcetype": "raster",
            "sourceattribution": "United States Geological Survey",
            "opacity": 0.5,
            "source": [
                get_tilejson_url()
            ]
        },
         {
            "below": 'traces',
            "sourcetype": "vector",
            "sourcelayer": "osm_line",
            "sourceattribution": "OSM",
            "opacity": 0.5,
            "source": ["http://localhost:8070/{z}/{x}/{y}.pbf"],
            "type" : "line",
            "color" : "#3366ff",
            "opacity" : 1,
        },
      ])
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()