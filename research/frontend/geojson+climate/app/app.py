from dash import Dash, Input, Output
from dash import html
import dash_leaflet as dl
from dash_extensions.javascript import arrow_function
import dash_leaflet.express as dlx

import app_sql
import app_utils
import constants

app = Dash()
  # Assumes you are running the docker-compose.yml in the directory

min_climate_value, max_climate_value = app_utils.get_climate_min_max()

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
                            draw={"rectangle": True}, edit=False, id="drawn-shapes"
                        )
                    ]
                ),
                dl.LayersControl(
                    [
                        dl.Overlay(
                            dl.LayerGroup(
                                [dl.TileLayer(url=app_utils.get_tilejson_url(), opacity=constants.CLIMATE_LAYER_OPACITY)]
                            ),
                            name="Percentage of Entire Grid cell that is Covered by Burnt Vegetation",
                            checked=True,
                        ),
                        dl.Overlay(
                            dl.LayerGroup(
                                [
                                    dl.GeoJSON(
                                        data=app_utils.query_postgis(app_sql.GET_INFRASTRUCTURE_LINE),
                                        id="infrastructure-line",
                                        hoverStyle=arrow_function(
                                            dict(weight=5, color="yellow", dashArray="")
                                        ),
                                        style={
                                            "color": "#008000",
                                            "weight": 2,
                                            "fillColor": "#008000",
                                            "fillOpacity": 0.5,
                                        },
                                    )
                                ]
                            ),
                            name="Infrastructure Lines",
                            checked=True,
                        ),
                        dl.Overlay(
                            dl.LayerGroup(
                                [
                                    dl.GeoJSON(
                                        data=app_utils.query_postgis(
                                            app_sql.GET_INFRASTRUCTURE_POLYGON
                                        ),
                                        id="infrastructure-polygon",
                                        hoverStyle=arrow_function(
                                            dict(weight=5, color="yellow", dashArray="")
                                        ),
                                        style={
                                            "color": "#FF0000",
                                            "weight": 2,
                                            "fillColor": "#FF0000",
                                            "fillOpacity": 0.5,
                                        },
                                    ),
                                ]
                            ),
                            name="Infrastructure Polygons",
                            checked=True,
                        ),
                        dl.Overlay(
                            dl.LayerGroup(
                                children=dl.GeoJSON(
                                    data=app_utils.query_postgis(app_sql.GET_INFRASTRUCTURE_POINT),
                                    id="infrastructure-point",
                                    hoverStyle=arrow_function(
                                        dict(weight=5, color="yellow", dashArray="")
                                    ),
                                    style={
                                        "color": "#0000FF",
                                        "weight": 2,
                                        "fillColor": "#0000FF",
                                        "fillOpacity": 0.5,
                                    },
                                    cluster=True
                                ),
                                id="points-group",
                            ),
                            id="points-overlay",
                            name="Infrastructure Points",
                            checked=False,
                        ),
                    ]
                ),
                dl.Colorbar(
                    colorscale=constants.COLORMAP,
                    width=20,
                    height=150,
                    min=min_climate_value,
                    max=max_climate_value,
                    unit="%",
                    position="bottomleft",
                ),
            ],
            center={"lat": 37.0902, "lng": -95.7129},
            zoom=5,
            style={"height": "100vh"},
            id="map",
        ),
    ]
)

if __name__ == "__main__":
    app.run_server(port=8050, host="127.0.0.1", debug=True)
