import os

import dash_leaflet as dl
import dash_bootstrap_components as dbc

import psycopg2 as pg

from psycopg2 import pool
from dash import Dash, Input, Output, State, html, dcc, no_update, callback_context
from dash.exceptions import PreventUpdate
from typing import List
import pandas as pd

import infraxclimate_api
import app_utils
import app_map
import app_control_panel
import app_config

TITILER_ENDPOINT = os.environ["TITILER_ENDPOINT"]
PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_PORT = os.environ["PG_PORT"]

MAX_DOWNLOADS = int(os.environ["MAX_DOWNLOADS"])  # Maximum downloads per session
MAX_DOWNLOAD_AREA = float(
    os.environ["MAX_DOWNLOAD_AREA"]
)  # Maximum area in square kilometers the user can download at once


def get_db_connection(dbname: str):
    """Get a database connection."""

    conn = pg.connect(
        database=dbname,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )
    return conn


MAP = app_map.get_base_map()


assets_path = os.getcwd() + "/assets"
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], assets_folder=assets_path)
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
                        app_control_panel.STATE_SELECTOR,
                        html.Br(),
                        app_control_panel.CLIMATE_VARIABLE_SELECTOR,
                        html.Br(),
                        app_control_panel.CLIMATE_SCENARIO_SELECTOR,
                        html.Br(),
                        app_control_panel.DOWNLOAD_DATA_BUTTONS,
                    ],
                    style={"backgroundColor": "#39275B"},
                    width=3,
                ),
                dbc.Col(
                    id="map-col",
                    children=[html.Div(children=[MAP], id="map-div")],
                ),
            ],
        ),
        dcc.Store(id="download-counter", data=0, storage_type="session"),
        dcc.Store("prev-selected-state-outline", storage_type="memory"),
    ],
)


@app.callback(
    [
        Output("climate-tile-layer", "url"),
        Output("climate-tile-layer", "opacity"),
        Output("color-bar-div", "children"),
    ],
    [
        Input("climate-variable-dropdown", "value"),
        Input("ssp-dropdown", "value"),
        Input("decade-slider", "value"),
        Input("month-slider", "value"),
        Input("state-select-dropdown", "value"),
    ],
)
def update_climate_tiles(climate_variable, ssp, decade, month, selected_state):
    # TODO: Make state selection dynamic
    if not selected_state:
        selected_state = "usa"
    if (
        (ssp is None)
        or (climate_variable is None)
        or (decade is None)
        or (month is None)
    ):

        return app_config.MAP_COMPONENT["base_map"]["url"], 1, []

    min_climate_value = app_config.CLIMATE_DATA[climate_variable]["min_value"]
    max_climate_value = app_config.CLIMATE_DATA[climate_variable]["max_value"]
    colormap = app_config.CLIMATE_DATA[climate_variable]["geotiff"]["colormap"]
    unit = app_config.CLIMATE_DATA[climate_variable]["unit"]
    layer_opacity = app_config.CLIMATE_DATA[climate_variable]["geotiff"]["layer_opacity"]

    # Generate Colorbar to match tiles
    color_bar = dl.Colorbar(
        id=app_config.MAP_COMPONENT["color_bar"]["id"],
        width=app_config.MAP_COMPONENT["color_bar"]["width"],
        colorscale=colormap,
        height=app_config.MAP_COMPONENT["color_bar"]["height"],
        position=app_config.MAP_COMPONENT["color_bar"]["position"],
        min=min_climate_value,
        max=max_climate_value,
        unit=unit,
    )

    # Generate S3 URI to COG File
    bucket = app_config.CLIMATE_DATA[climate_variable]["geotiff"]["s3_bucket"]
    prefix = app_config.CLIMATE_DATA[climate_variable]["geotiff"]["s3_base_prefix"]
    stat = app_config.CLIMATE_DATA[climate_variable]['statistical_measure']
    file = f"{climate_variable}_{stat}-{decade}-{month:02d}-usa.tif"
    file_url = f"s3://{bucket}/{prefix}/{str(ssp)}/cogs/{file}"
    tile_url = app_utils.get_tilejson_url(
        titiler_endpoint=TITILER_ENDPOINT,
        file_url=file_url,
        min_climate_value=min_climate_value,
        max_climate_value=max_climate_value,
        colormap=colormap,
    )

    return_values = (tile_url, layer_opacity, [color_bar])
    return return_values


@app.callback(
    Output("state-outline-geojson", "url"),
    Output(app_config.MAP_COMPONENT["id"], "viewport"),
    Input("state-select-dropdown", "value"),
    prevent_initial_call=True,
)
def handle_state_outline(selected_state):
    if not selected_state:
        return no_update
    url = f"assets/geojsons/{selected_state}.geojson"
    viewport = {
        "center": app_config.STATES["available_states"][selected_state]["map_center"],
        "zoom": app_config.STATES["available_states"][selected_state]["map_zoom"],
        "transition": "flyTo",
    }
    return url, viewport


@app.callback(
    Output("layers-control", "children"),
    Input("state-select-dropdown", "value"),
    prevent_initial_call=True,
)
def handle_state_features(selected_state):
    if not selected_state:
        return no_update
    if selected_state == "usa":
        return list()
    conn = pg.connect(
        database=selected_state,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT,
    )
    feature_overlays = app_map.get_feature_overlays(conn=conn)
    conn.close()
    return feature_overlays


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
        Input("state-select-dropdown", "value"),
    ],
    State("download-counter", "data"),
)
def download_csv(
    n_clicks,
    shapes,
    selected_overlays,
    climate_variable,
    ssp,
    decade,
    month,
    download_counter,
    selected_state
):
    # TODO: Create function to package return values tuple
    # TODO: Add return value checking (Pydantic)

    download_message = ""
    is_open = False
    download_message_color = None

    if n_clicks is None or n_clicks == 0:
        raise PreventUpdate

    if shapes is None or len(shapes["features"]) == 0:
        download_message = "Please select an area on the map (Hint: Click the black square in the upper right of the map)."
        is_open = True
        download_message_color = "warning"
        return (
            no_update,
            0,
            download_counter,
            download_message,
            is_open,
            download_message_color,
        )

    # Initialize download counter if None
    if download_counter is None:
        download_counter = 0

    # Check download limit
    if download_counter >= MAX_DOWNLOADS:
        download_message = (
            f"You have reached the maximum of {MAX_DOWNLOADS} downloads per session."
        )
        is_open = True
        download_message_color = "danger"
        return (
            no_update,
            0,
            download_counter,
            download_message,
            is_open,
            download_message_color,
        )

    if app_utils.calc_bbox_area(features=shapes["features"]) > MAX_DOWNLOAD_AREA:
        download_message = f"Your selected area is too large to download"
        is_open = True
        download_message_color = "danger"
        return (
            no_update,
            0,
            download_counter,
            download_message,
            is_open,
            download_message_color,
        )

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

        conn = get_db_connection(dbname=selected_state)
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

        conn.close()

        # Increment download counter, reset error message
        download_counter += 1
        download_message = "Download is in progress!"
        is_open = True
        download_message_color = "success"

        return (
            dcc.send_data_frame(df.to_csv, "climate_risk_map_download.csv"),
            0,
            download_counter,
            download_message,
            is_open,
            download_message_color,
        )
    return no_update, 0, download_counter, no_update, no_update, no_update


if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=True, dev_tools_hot_reload=False)
