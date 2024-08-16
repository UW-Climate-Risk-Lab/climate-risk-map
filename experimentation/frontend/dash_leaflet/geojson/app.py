import os

import dash_leaflet as dl

from psycopg2 import pool
from dash import Dash, Input, Output, html, dcc, no_update, State
from typing import List

import pgosm_flex_api
import app_utils
import app_layers
import app_config

PG_DBNAME = os.environ["PG_DBNAME"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]
PG_MAX_CONN = os.environ["PG_MAX_CONN"]

CONNECTION_POOL = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=PG_MAX_CONN,
    dbname=PG_DBNAME,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT,
)


def get_connection():
    """Get a connection from the pool."""
    return CONNECTION_POOL.getconn()


def release_connection(conn):
    """Return a connection to the pool."""
    CONNECTION_POOL.putconn(conn)


def close_all_connections():
    """Close all connections in the pool."""
    CONNECTION_POOL.closeall()


icon_url = "/assets/icon.css"
app = Dash(__name__)
server = app.server
# Assumes you are running the docker-compose.yml in the directory

min_climate_value, max_climate_value = app_utils.get_climate_min_max()


def get_feature_overlays() -> List[dl.Overlay]:
    """Returns overlays of Geojson features

    Returns:
        List[dl.Overlay]: List of overlays for LayersControl
    """
    try:
        conn = get_connection()
        power_grid_features = app_layers.get_power_grid_overlays(conn=conn)
    except Exception as e:
        print(str(e))
    finally:
        release_connection(conn=conn)

    # If more features needed in future, add on to this
    features = power_grid_features

    return features


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
                # TODO: Move base layer generation into a function in app_layers
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
                            checked=True,
                        ),
                    ]
                    + get_feature_overlays(),
                ),
                app_layers.get_state_overlay(state="washington", z_index=300),
                dl.Colorbar(
                    colorscale=app_config.COLORMAP,
                    width=20,
                    height=150,
                    min=min_climate_value,
                    max=max_climate_value,
                    unit="%",
                    position="bottomleft",
                ),
                dl.EasyButton(icon="csv", title="CSV", id="csv-btn"),
                dcc.Download(id="csv-download"),
            ],
            center={"lat": 47.0902, "lng": -120.7129},
            zoom=7,
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
                + app_config.POWER_GRID_LAYERS[overlay]["GeoJSON"]["categories"]
            )
            osm_types = (
                osm_types
                + app_config.POWER_GRID_LAYERS[overlay]["GeoJSON"]["osm_types"]
            )
            osm_subtypes = (
                osm_subtypes
                + app_config.POWER_GRID_LAYERS[overlay]["GeoJSON"]["osm_subtypes"]
            )

        conn = get_connection()
        api = pgosm_flex_api.OpenStreetMapDataAPI(conn=conn)
        # quick fix, use list(set()) to remove duplicates from input params
        data = api.get_osm_data(
            categories=list(set(categories)),
            osm_types=list(set(osm_types)),
            osm_subtypes=list(set(osm_subtypes)),
            bbox=shapes,
            county=True,
            city=True,
            srid=4326,
        )
        release_connection(conn=conn)
        df = app_utils.process_output_csv(data=data)
        return dcc.send_data_frame(df.to_csv, "climate_risk_map_download.csv"), 0
    return no_update, 0


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=80)
