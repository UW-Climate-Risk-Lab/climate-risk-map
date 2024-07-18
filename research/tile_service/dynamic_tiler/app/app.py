from dash import Dash
from dash import html
import dash_leaflet as dl
import httpx

app = Dash()
COLORMAP = "reds"

titiler_endpoint = "http://localhost:8000"
file_url = "http://fileserver:8080/OutputCOG.tif"

r = httpx.get(
    f"{titiler_endpoint}/cog/statistics",
    params={
        "url": file_url,
    },
).json()

minv = r["b1"]["min"]
maxv = r["b1"]["max"]

r = httpx.get(
    f"{titiler_endpoint}/cog/tilejson.json",
    params={"url": file_url, "rescale": f"{minv},{maxv}", "colormap_name": COLORMAP},
).json()


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
                            name="Percentage of Entire Grid cell  that is Covered by Burnt Vegetation",
                            checked=True,
                        ),
                        html.Div(
                            id="info",
                            className="info",
                            style={
                                "position": "absolute",
                                "bottom": "10px",
                                "left": "10px",
                                "z-index": "1000",
                            },
                        ),
                    ]
                ),
                dl.Colorbar(colorscale=COLORMAP, width=20, height=150, min=minv, max=maxv,unit='%',position="bottomleft"),
            ],
            center={"lat": 37.0902, "lng": -95.7129},
            zoom=5,
            style={"height": "100vh"},
            id="map",
        )
    ]
)


if __name__ == "__main__":
    app.run_server(port=8050, host='0.0.0.0')
