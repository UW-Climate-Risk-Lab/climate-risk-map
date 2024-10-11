import dash
from dash import html, Output, Input, State
from dash_extensions.javascript import assign
import dash_leaflet as dl

# Initialize the Dash app
app = dash.Dash(__name__)




# Not working? Error: wrong listener type: object
eventHandlers = dict(click=assign("function(e){console.log(e.layer.feature);}"))

# Define style for each vector layer
vector_tile_layer_styles = {
    "osm_line": {"color": "red", "weight": 2},
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
            ],
            center={"lat": 47.0902, "lng": -120.7129},
            zoom=7,
            style={"height": "80vh"},
            id="map",
        ),
    ]
)


# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
