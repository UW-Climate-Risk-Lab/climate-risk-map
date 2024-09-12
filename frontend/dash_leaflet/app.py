import os

import dash_leaflet as dl
import dash_bootstrap_components as dbc

from psycopg2 import pool
from dash import Dash, Input, Output, html, dcc, no_update
from dash.exceptions import PreventUpdate
from typing import List

import osm_api
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
                    children=[
                        app_control_panel.TITLE_BAR,
                        html.Br(),
                        app_control_panel.CLIMATE_VARIABLE_SELECTOR,
                        html.Br(),
                        app_control_panel.CLIMATE_SCENARIO_SELECTOR,
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
        )
    ],
)

@app.callback(
    [Output("climate-tile-layer", "url"),
     Output("climate-tile-layer", "opacity"),
     Output("color-bar", "min"),
     Output("color-bar", "max"),
     Output("color-bar", "colorscale"),
     Output("color-bar", "unit")],
    [
        Input("climate-variable-dropdown", "value"),
        Input("ssp-dropdown", "value"),
        Input("decade-slider", "value"),
        Input("month-slider", "value"),
        
    ],
)
def update_climate_file(climate_variable, ssp, decade, month):
    # TODO: Make state selection dynamic
    state = 'washington'
    if (ssp is None) or (climate_variable is None) or (decade is None) or (month is None):
        raise PreventUpdate
    
    properties = app_config.CLIMATE_DATA[climate_variable]
    
    file = f"{decade}-{month:02d}-{state}.tif"
    file_url = f"s3://{properties["geotiff"]["s3_bucket"]}/{properties['geotiff']["s3_base_prefix"]}/{str(ssp)}/cogs/{file}"
    min_climate_value, max_climate_value = app_utils.get_climate_min_max(file_url=file_url)
    colormap = properties["geotiff"]["colormap"]
    layer_opacity = properties["geotiff"]["layer_opacity"]
    unit = properties["unit"]

    url = app_utils.get_tilejson_url(file_url=file_url,
                                        climate_variable=climate_variable,
                                        min_climate_value=min_climate_value,
                                        max_climate_value=max_climate_value,
                                        colormap=colormap)
            
    return_values = (url, layer_opacity, min_climate_value, max_climate_value, colormap, unit)
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
        api = osm_api.OpenStreetMapDataAPI(conn=conn)
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
    app.run_server(host="0.0.0.0", port=8050, debug=bool(os.environ["DEBUG"]))
