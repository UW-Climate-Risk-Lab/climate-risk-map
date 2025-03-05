import dash_bootstrap_components as dbc
from dash import html, dcc

from ui.components.control_panel import create_control_panel
from services.map_service import MapService

def create_main_layout():
    """Create the main application layout
    
    Returns:
        dbc.Container: Main layout container
    """
    # Get base map component
    map_component = MapService.get_base_map()
    
    # Create layout
    return dbc.Container(
        fluid=True,
        class_name="g-0",
        children=[
            dbc.Row(
                class_name="g-0",
                children=[
                    # Control panel column
                    create_control_panel(),
                    
                    # Map column
                    dbc.Col(
                        id="map-col",
                        children=[html.Div(children=[map_component], id="map-div")],
                    ),
                ],
            ),
            # State for tracking downloads
            dcc.Store(id="download-counter", data=0, storage_type="session"),
            # State for tracking previously selected state
            dcc.Store("prev-selected-state-outline", storage_type="memory"),
        ],
    )