from dash import Dash, Input, Output
from dash import html
import dash_leaflet as dl
import httpx
import psycopg2
from dash_extensions.javascript import arrow_function
import dash_leaflet.express as dlx

import sql

app = Dash()
COLORMAP = "reds"

titiler_endpoint = "http://localhost:8000"
file_url = "http://fileserver:8080/OutputCOG.tif"  # Assumes you are running the docker-compose.yml in the directory

r = httpx.get(
    f"{titiler_endpoint}/cog/statistics",
    params={
        "url": file_url,
    },
).json()

min = r["b1"]["min"]
max = r["b1"]["max"]

r = httpx.get(
    f"{titiler_endpoint}/cog/tilejson.json",
    params={"url": file_url, "rescale": f"{min},{max}", "colormap_name": COLORMAP},
).json()


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
                                [dl.TileLayer(url=r["tiles"][0], opacity=0.6)]
                            ),
                            name="Percentage of Entire Grid cell that is Covered by Burnt Vegetation",
                            checked=True,
                        ),
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
                        dl.Overlay(
                            dl.LayerGroup(
                                children=dl.GeoJSON(
                                    data=query_postgis(sql.GET_INFRASTRUCTURE_POINT),
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
                    colorscale=COLORMAP,
                    width=20,
                    height=150,
                    min=min,
                    max=max,
                    unit="%",
                    position="bottomleft",
                ),
            ],
            center={"lat": 37.0902, "lng": -95.7129},
            zoom=5,
            style={"height": "100vh"},
            id="map",
        )
    ]
)



if __name__ == "__main__":
    app.run_server(port=8050, host="127.0.0.1", debug=True)
