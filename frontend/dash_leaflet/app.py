import os

import dash_leaflet as dl
import dash_bootstrap_components as dbc

from psycopg2 import pool
from dash import Dash, Input, Output, State, html, dcc, no_update
from dash.exceptions import PreventUpdate
from typing import List
import pandas as pd

import infraxclimate_api
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

MAX_DOWNLOADS = int(os.environ["MAX_DOWNLOADS"])  # Maximum downloads per session
MAX_DOWNLOAD_AREA = float(os.environ["MAX_DOWNLOAD_AREA"])    # Maximum area in square kilometers the user can download at once

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
                    children=[
                        app_control_panel.TITLE_BAR,
                        html.Br(),
                        app_control_panel.CLIMATE_VARIABLE_SELECTOR,
                        html.Br(),
                        app_control_panel.CLIMATE_SCENARIO_SELECTOR,
                        html.Br(),
                        app_control_panel.DOWNLOAD_DATA_BUTTONS,
                    ],
                    style={"backgroundColor": "#39275B"},
                    width=4,
                ),
                dbc.Col(
                    id="map-col",
                    children=[
                        html.Div([MAP]),
                    ],
                ),
            ],
        ),
        dcc.Store("climate-metadata-store"),
        dcc.Store(id="download-counter", data=0, storage_type="session"),
        
    ],
)


@app.callback(
    Output("climate-metadata-store", "data"),
    [
        Input("climate-variable-dropdown", "value"),
        Input("ssp-dropdown", "value"),
    ],
)
def load_climate_metadata(climate_variable, ssp):
    """Stores select metadata for use in app"""
    if (ssp is None) or (climate_variable is None):
        raise PreventUpdate

    conn = get_connection()
    api = infraxclimate_api.infraXclimateAPI(conn=conn)
    ssp = int(ssp[3:])  # Quickfix to get the ssp int value
    metadata = api.get_climate_metadata(climate_variable=climate_variable, ssp=ssp)

    min_value = metadata["UW_CRL_DERIVED"]["min_climate_variable_value"]
    max_value = metadata["UW_CRL_DERIVED"]["max_climate_variable_value"]
    unit = metadata[climate_variable]["units"]
    colormap = app_config.CLIMATE_DATA[climate_variable]["geotiff"]["colormap"]
    layer_opacity = app_config.CLIMATE_DATA[climate_variable]["geotiff"][
        "layer_opacity"
    ]

    del api
    release_connection(conn=conn)

    return {
        "min_value": min_value,
        "max_value": max_value,
        "colormap": colormap,
        "unit": unit,
        "layer_opacity": layer_opacity,
    }


@app.callback(
    [
        Output("climate-tile-layer", "url"),
        Output("climate-tile-layer", "opacity"),
        Output("color-bar", "min"),
        Output("color-bar", "max"),
        Output("color-bar", "colorscale"),
        Output("color-bar", "unit"),
    ],
    [
        Input("climate-variable-dropdown", "value"),
        Input("ssp-dropdown", "value"),
        Input("decade-slider", "value"),
        Input("month-slider", "value"),
        Input("climate-metadata-store", "data"),
    ],
)
def update_climate_tiles(climate_variable, ssp, decade, month, climate_metadata):
    # TODO: Make state selection dynamic
    state = "washington"
    if (
        (ssp is None)
        or (climate_variable is None)
        or (decade is None)
        or (month is None)
    ):
        raise PreventUpdate

    properties = app_config.CLIMATE_DATA[climate_variable]

    file = f"{decade}-{month:02d}-{state}.tif"
    bucket = properties["geotiff"]["s3_bucket"]
    prefix = properties["geotiff"]["s3_base_prefix"]
    climatological_mean = properties["climatological_mean"]

    file_url = f"s3://{bucket}/{prefix}/{str(ssp)}/cogs/{climatological_mean}/{file}"
    min_climate_value = climate_metadata["min_value"]
    max_climate_value = climate_metadata["max_value"]
    colormap = climate_metadata["colormap"]
    unit = climate_metadata["unit"]
    layer_opacity = climate_metadata["layer_opacity"]

    url = app_utils.get_tilejson_url(
        file_url=file_url,
        climate_variable=climate_variable,
        min_climate_value=min_climate_value,
        max_climate_value=max_climate_value,
        colormap=colormap,
    )

    return_values = (
        url,
        layer_opacity,
        min_climate_value,
        max_climate_value,
        colormap,
        unit,
    )
    return return_values


@app.callback(
    [Output("ssp-dropdown", "options")], [Input("climate-variable-dropdown", "value")]
)
def update_ssp_dropdown(climate_variable: str) -> List[str]:
    """Updates the available SSPs based on the dropdown

    Args:
        climate_variable (str): Name of climate variable selected

    Returns:
        List[str]: List of ssp strings
    """
    if climate_variable:
        return [app_config.CLIMATE_DATA[climate_variable]["available_ssp"]]
    else:
        return no_update


@app.callback(
    [
        Output("csv-download", "data"),
        Output("csv-btn", "n_clicks"),
        Output("download-counter", "data"),
        Output("download-message", "children"),
        Output("download-message", "is_open"),
        Output("download-message", "color"),
    ],
    [
        Input("csv-btn", "n_clicks"),
        Input("drawn-shapes", "geojson"),
        Input("layers-control", "overlays"),
        Input("climate-variable-dropdown", "value"),
        Input("ssp-dropdown", "value"),
        Input("decade-slider", "value"),
        Input("month-slider", "value"),
    ],
    State("download-counter", "data"),
)
def download_csv(
    n_clicks, shapes, selected_overlays, climate_variable, ssp, decade, month, download_counter
):
    # TODO: Create function to package return values tuple
    # TODO: Add return value checking (Pydantic)
    

    download_message = ''
    is_open = False
    download_message_color = None

    if n_clicks is None or n_clicks == 0:
        raise PreventUpdate

    if shapes is None or len(shapes["features"]) == 0:
        download_message = "Please select an area on the map."
        is_open = True
        download_message_color = "warning"
        return no_update, 0, download_counter, download_message, is_open, download_message_color

    # Initialize download counter if None
    if download_counter is None:
        download_counter = 0

    # Check download limit
    if download_counter >= MAX_DOWNLOADS:
        download_message = f"You have reached the maximum of {MAX_DOWNLOADS} downloads per session."
        is_open = True
        download_message_color = "danger"
        return no_update, 0, download_counter, download_message, is_open, download_message_color
    
    if app_utils.calc_bbox_area(features=shapes["features"]) > MAX_DOWNLOAD_AREA:
        download_message = f"Your selected area is too large to download"
        is_open = True
        download_message_color = "danger"
        return no_update, 0, download_counter, download_message, is_open, download_message_color

    # Only want to return climate data if user has selected all relevant criteria
    if None in [climate_variable, ssp, decade, month]:
        climate_variable = None
        ssp = None
        decade = None
        month = None
    else:
        if decade is not None:
            decade = [decade]
        if month is not None:
            month = [month]
        if ssp is not None:
            ssp = ssp[3:]

    if n_clicks > 0:
        osm_types = []
        osm_subtypes = []
        category = (
            "infrastructure"  # Quick fix as this is the only available category for now
        )

        # Use the selected overlays to get the proper types to return in the data
        for overlay in selected_overlays:
            osm_types = (
                osm_types
                + app_config.POWER_GRID_LAYERS[overlay]["GeoJSON"]["osm_types"]
            )
            osm_subtypes = (
                osm_subtypes
                + app_config.POWER_GRID_LAYERS[overlay]["GeoJSON"]["osm_subtypes"]
            )

        conn = get_connection()
        try:
            params = infraxclimate_api.infraXclimateInput(
                category=category,
                osm_types=list(set(osm_types)),
                osm_subtypes=list(set(osm_subtypes)),
                bbox=shapes,
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable=climate_variable,
                climate_ssp=ssp,
                climate_month=month,
                climate_decade=decade,
                climate_metadata=False,
            )

            api = infraxclimate_api.infraXclimateAPI(conn=conn)
            data = api.get_data(input_params=params)
            df = app_utils.process_output_csv(data=data)
            del api
            
        except Exception as e:
            print(e)
            df = pd.DataFrame()

        release_connection(conn=conn)

        # Increment download counter, reset error message
        download_counter += 1
        download_message = 'Download in progress!'
        is_open = True
        download_message_color = "success"
        
        return dcc.send_data_frame(df.to_csv, "climate_risk_map_download.csv"), 0, download_counter, download_message, is_open, download_message_color
    return no_update, 0


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=bool(os.environ["DEBUG"]))
