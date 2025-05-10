import dash_bootstrap_components as dbc
from dash import html, dcc

from ui.components.control_panel import create_control_panel
from ui.components.legend import create_legend_bar, create_legend_toggle_button
from ui.components.chat_window import create_ai_analysis_modal
from ui.components.base_map_button import create_basemap_toggle_button
from ui.components.password_screen import create_password_screen
from services.map_service import MapService
from services.auth_service import AuthService
from config.ui_config import (
    LEGEND_CONTAINER_STYLE,
    PRIMARY_COLOR,
    MAP_FEATURES_LOADING_SPINNER_STYLE,
)
from config.map_config import MapConfig


def create_main_layout():
    """Create the main application layout

    Returns:
        dbc.Container: Main layout container
    """
    # Check if password protection is enabled
    password_protected = AuthService.is_password_protection_enabled()
    
    # For password-protected app, don't create the map component yet
    # We'll create it dynamically after authentication
    map_placeholder = html.Div(id="map-placeholder")
    
    # Create the main application layout
    main_app_layout = dbc.Container(
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
                            create_basemap_toggle_button(),
                            html.Div(
                                dbc.Spinner(
                                    html.Div(
                                        id="map-loading-placeholder",
                                        style={"width": "50px", "height": "50px"},
                                    ),
                                    id="map-loading-spinner",
                                    delay_show=100,
                                    color=PRIMARY_COLOR,
                                    type="border",
                                    spinner_style={
                                        "width": "3rem",
                                        "height": "3rem",
                                    },
                                ),
                                style=MAP_FEATURES_LOADING_SPINNER_STYLE,
                            ),
                            # Map component - either the actual map or a placeholder
                            html.Div(
                                children=[
                                    map_placeholder if password_protected else MapService.get_base_map(),
                                ],
                                id="map-div",
                            ),
                        ],
                        style={"position": "relative"},
                    ),
                ],
            ),
            create_ai_analysis_modal(),
            # State for tracking downloads
            dcc.Store(id="download-counter", data=0, storage_type="session"),
            dcc.Store(id="download-allowed", data=False, storage_type="memory"),
            dcc.Store(
                id="region-features-change-signal",
                data=MapConfig.BASE_MAP_COMPONENT["default_region_name"],
                storage_type="memory",
            ),
            dcc.Store(id="region-outline-change-signal", storage_type="memory"),
            dcc.Store(id="chat-counter", data=0, storage_type="session"),
            dcc.Store(id="chat-allowed", data=False, storage_type="memory"),
            # chat-selection-hash is a string hash representing the combination of the user selection of hazard, time, bounding box, assets
            # If the user selects a new combination of dropdowns or area on the map, this hash will update. Currently used in chat_callbacks in get_ai repsonse
            # TODO: Make this update any time the map changes anywhere 
            dcc.Store(id="chat-selection-hash", data="", storage_type="memory"),
            dcc.Store(id="new-user-selection", data=True, storage_type="memory"),
            dcc.Store(id="agent-session-id", data="", storage_type="session"),
            dcc.Store(id="trigger-ai-response-store", data=0.0, storage_type="memory"),
        ],
    )
    
    # Check if password protection is enabled
    if password_protected:
        # Add a store to track authentication state
        auth_store = dcc.Store(id="auth-status", data=False, storage_type="session")
        
        # Create complete layout with password screen and hidden main app
        return html.Div([
            create_password_screen(),
            html.Div(
                id="main-app-container",
                children=[main_app_layout],
                style={"display": "none"}
            ),
            auth_store
        ])
    else:
        # If password protection is not enabled, return just the main app layout
        return main_app_layout