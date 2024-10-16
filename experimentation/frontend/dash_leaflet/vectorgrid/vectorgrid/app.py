import dash
from dash import html, Output, Input, State
import dash_leaflet as dl
import dash_extensions.javascript as dljs

# Initialize the Dash app
app = dash.Dash(__name__)

# Define a JavaScript function to capture the clicked feature and its coordinates
eventHandlers = dict(
    click=dljs.assign(
        "function(e, ctx){ctx.setProps({clickData: e.layer.feature.properties})}"
    )
)
dl.GeoJSON
# Define style for each vector layer
vector_tile_layer_styles = {
    "osm_line": {"color": "red", "weight": 1},
}

vector_tile_layer = dl.VectorTileLayer(
    id="vector-layer",
    url="http://localhost:8070/maps/osm_map/osm_line/{z}/{x}/{y}.pbf",
    eventHandlers=eventHandlers,
    vectorTileLayerStyles=vector_tile_layer_styles,
)

# Define the layout of the app
app.layout = html.Div(
    [
        dl.Map(
            [
                dl.TileLayer(
                    url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
                ),
                dl.LayersControl(
                    id="layers-control",
                    children=[
                        dl.Overlay(dl.LayerGroup(vector_tile_layer), name="Power Line")
                    ],
                ),
                dl.Popup(id="popup"),  # Popup to display feature info
            ],
            center={"lat": 47.0902, "lng": -120.7129},
            zoom=7,
            style={"height": "80vh"},
            id="map",
        ),
        html.Div(id="feature-info", style={"whiteSpace": "pre-wrap", "padding": "10px", "backgroundColor": "#f9f9f9"})
    ]
)

# Callback to display the popup on the map
@app.callback(
    Output("feature-info", "children"),
    [Input("vector-layer", "click"), Input("vector-layer", "clickData")],  # Capture the click event data
)
def on_click(_, data):
    if data:
        return [str(data)]


# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)