import dash_bootstrap_components as dbc
from dash import html, dcc

from ui.components.control_panel import create_control_panel
from ui.components.legend import create_legend_bar, create_legend_toggle_button
from ui.components.chat_window import create_ai_analysis_modal 
from services.map_service import MapService
from config.ui_config import LEGEND_CONTAINER_STYLE
from config.map_config import MapConfig


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
                    # Map column with legend bar
                    dbc.Col(
                        id="map-col",
                        children=[
                            # Add legend bar above the map
                            html.Div(
                                id="legend-container",
                                children=[create_legend_bar()],
                                style=LEGEND_CONTAINER_STYLE,
                            ),
                            # Add legend toggle button
                            create_legend_toggle_button(),
                            # Map component
                            html.Div(children=[map_component], id="map-div"),
                        ],
                        style={"position": "relative"},
                    ),
                ],
            ),
            create_ai_analysis_modal(),
            # State for tracking downloads
            dcc.Store(id="download-counter", data=0, storage_type="session"),
            dcc.Store(id="download-allowed", data=False, storage_type="memory"),
            dcc.Store(id="region-features-change-signal", data=MapConfig.BASE_MAP_COMPONENT["default_region_name"], storage_type="memory"),
            dcc.Store(id="region-outline-change-signal", storage_type="memory"),
            dcc.Store(id="chat-counter", data=0, storage_type="session"),
            dcc.Store(id="chat-allowed", data=False, storage_type="memory"),
            # chat-selection-config is a hash int representing the combination of the user selection of hazard, time, bounding box, assets
            # If the user selects a new combination of dropdowns, this will create a new section in the chat window
            dcc.Store(id="chat-selection-config", data=0, storage_type="session"),
            dcc.Store(id="agent-session-id", data='', storage_type="session"),
            dcc.Store(id="trigger-ai-response-store", data=0.0, storage_type="memory")
        ],
    )
