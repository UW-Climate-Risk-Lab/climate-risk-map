from dash import Dash
from dash import Input, Output
from dash import html
import dash_leaflet as dl
from dash_extensions.javascript import arrow_function
import psycopg2
import sql

app = Dash()


def query_postgis(query: str):
    # Connect to your PostGIS database
    conn = psycopg2.connect(
        "dbname='pgosm_flex_washington' user='osm_ro_user' host='localhost' password='mysecretpassword'"
    )
    cur = conn.cursor()

    # Execute the query
    cur.execute(query)
    result = cur.fetchone()[0]

    cur.close()
    conn.close()

    return result


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
                                [
                                    dl.GeoJSON(
                                        data=query_postgis(sql.GET_INFRASTRUCTURE_LINE),
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
                                        data=query_postgis(
                                            sql.GET_INFRASTRUCTURE_POLYGON
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
                    ]
                ),
            ],
            center={"lat": 47.7511, "lng": -120.7401},
            zoom=8,
            style={"height": "100vh"},
            id="map"
        ),
    ]
)

if __name__ == "__main__":
    app.run_server()
