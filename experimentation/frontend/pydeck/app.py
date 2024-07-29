import dash
from dash import html
import dash_deck
import pydeck as pdk
import httpx
import os

from dotenv import load_dotenv

load_dotenv()


# Initialize the Dash app
app = dash.Dash(__name__)

# Define the TiTiler URL
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


# Define your Pydeck TileLayer
tile_layer = pdk.Layer(
    "TerrainLayer",
    texture=get_tilejson_url(),
    elevation_data=get_tilejson_url(),
    min_zoom=0,
    max_zoom=23,
    tile_size=256,
    opacity=0.3,
)

# Define any additional layers, e.g., a GeoJsonLayer from PostGIS
vector_layer = pdk.Layer(
    "MVTLayer",
    "http://localhost:8070/maps/osm_map/{z}/{x}/{y}.pbf",
    opacity=0.5,
    min_zoom=0,
    max_zoom=23,
    get_line_color=[0, 0, 255],  # Change line color to blue
    get_fill_color=[0, 255, 0, 128],
    get_line_width=20,
    line_width_min_pixels=1,
    pickable=True,
    auto_highlight=True,
)

# Define your deck.gl map
deck = pdk.Deck(
    map_style="light",
    initial_view_state=pdk.ViewState(
        latitude=37.0902,
        longitude=-95.7129,
        zoom=5,
        pitch=50,
    ),
    layers=[
        vector_layer,
        tile_layer,
    ],
)

# Create the Dash layout
app.layout = html.Div(
    dash_deck.DeckGL(
        deck.to_json(),
        id="deck_gl",
        style={"width": "100%", "height": "100vh"},
        mapboxKey=os.environ["MAPBOX_KEY"],
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
