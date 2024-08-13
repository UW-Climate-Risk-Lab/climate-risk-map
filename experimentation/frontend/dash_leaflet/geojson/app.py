import json
import dash_leaflet as dl
import dash_leaflet.express as dlx
import pandas as pd

from dash import Dash, Input, Output, html, dcc, no_update


import pgosm_flex_api
import app_utils
import app_layers
import app_config
from dotenv import load_dotenv
import os


load_dotenv()
PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]

icon_url = "/assets/csv_icon.css"
app = Dash()
# Assumes you are running the docker-compose.yml in the directory

min_climate_value, max_climate_value = app_utils.get_climate_min_max()

api = pgosm_flex_api.OpenStreetMapDataAPI(
    dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
)

app.layout = html.Div(
    children=[
        dl.Map(
            [
                dl.TileLayer(
                    url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
                    attribution='&copy; <a href="https://carto.com/attributions">CARTO</a>',
                ),
                dl.FeatureGroup(
                    [
                        dl.EditControl(
                            draw={
                                "rectangle": True,
                                "circle": False,
                                "polygon": False,
                                "circlemarker": False,
                                "polyline": False,
                                "marker": False,
                            },
                            edit=False,
                            id="drawn-shapes",
                        )
                    ]
                ),
                dl.LayersControl(
                    id="layers-control",
                    children=[
                        dl.BaseLayer(
                            [
                                dl.TileLayer(
                                    url=app_utils.get_tilejson_url(),
                                    opacity=app_config.CLIMATE_LAYER_OPACITY,
                                )
                            ],
                            name="Climate",
                        ),
                    ]
                    + app_layers.get_infrastucture_overlays(),
                ),
                dl.Colorbar(
                    colorscale=app_config.COLORMAP,
                    width=20,
                    height=150,
                    min=min_climate_value,
                    max=max_climate_value,
                    unit="%",
                    position="bottomleft",
                ),
                dl.EasyButton(icon="icon", title="CSV", id="csv-btn"),
                dcc.Download(id="csv-download")
            ],
            center={"lat": 37.0902, "lng": -95.7129},
            zoom=5,
            style={"height": "100vh"},
            id="map",
            preferCanvas=True,
        ),
    ]
)


@app.callback(
    [Output("csv-download", "data"), Output("csv-btn", "n_clicks")],
    [
        Input("csv-btn", "n_clicks"),
        Input("drawn-shapes", "geojson"),
        Input("layers-control", "overlays"),
    ],
)
def download_csv(n_clicks, shapes, selected_overlays):

    # Need to check shapes value for different cases
    if (shapes is None) or (len(shapes["features"]) == 0) or (n_clicks is None):
        return no_update, 0
    
    if n_clicks > 0:
        categories = []
        osm_types = []
        osm_subtypes = []
        # Use the selected overlays to get the proper types to return in the data
        for overlay in selected_overlays:
            categories = (
                categories
                + app_config.INFRASTRUCTURE_LAYERS[overlay]["GeoJSON"]["categories"]
            )
            osm_types = (
                osm_types
                + app_config.INFRASTRUCTURE_LAYERS[overlay]["GeoJSON"]["osm_types"]
            )
            osm_subtypes = (
                osm_subtypes
                + app_config.INFRASTRUCTURE_LAYERS[overlay]["GeoJSON"]["osm_subtypes"]
            )
        
        # quick fix, use list(set()) to remove duplicates from input params
        data = api.get_osm_data(
            categories=list(set(categories)),
            osm_types=list(set(osm_types)),
            osm_subtypes=list(set(osm_subtypes)),
            bbox=shapes,
        )
        gdf = app_utils.geojson_to_geopandas(geojson=data)
        df = pd.DataFrame(gdf)
        return dcc.send_data_frame(df.to_csv, "climate_risk_map_download.csv"), 0
    return no_update, 0


if __name__ == "__main__":
    app.run_server(port=8050, host="127.0.0.1", debug=True)
