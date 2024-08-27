import os

import dash_leaflet as dl
import dash_bootstrap_components as dbc

from psycopg2 import pool
from dash import Dash, Input, Output, html, dcc, no_update, State
from typing import List

import pgosm_flex_api
import app_utils
import app_map
import app_control_panel
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


try:
    map_conn = get_connection()
    MAP = app_map.get_map(conn=map_conn)
except Exception as e:
    raise ValueError("Could not generate map component")
finally:
    release_connection(conn=map_conn)


icon_url = "/assets/icon.css"
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

app.layout = dbc.Container(
    fluid=True,
    class_name="g-0",
    children=[
        dbc.Row(
            class_name="g-0",
            children=[
                dbc.Col(
                    id="control-panel-col",
                    children=[app_control_panel.TITLE_BAR],
                    style={"backgroundColor": "#4B2E83"},
                    width=3,
                ),
                dbc.Col(
                    id="map-col",
                    children=[
                        html.Div([MAP]),
                    ],
                ),
            ],
        )
    ],
)


@app.callback(
    [
        Output("color-bar", "min"),
        Output("color-bar", "max"),
        Output("color-bar", "colorscale"),
        Output("color-bar", "unit"),
    ],
    [Input("layers-control", "baseLayer")],
)
def update_colorbar(climate_layer: str):
    """Takes name of baselayer, and using config, updates
    color bar to reflect currentyl selected climate variable

    Args:
        climate_layer (str): Name of Climate Layer
    """
    min_climate_value, max_climate_value = app_utils.get_climate_min_max()

    colorscale = "reds"
    unit = "%"

    return min_climate_value, max_climate_value, colorscale, unit


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
    app.run_server(host="0.0.0.0", port=8050)
