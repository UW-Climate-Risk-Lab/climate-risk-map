import dash_bootstrap_components as dbc
from dash import html, dcc

from config.ui_config import UIConfig
from config.map_config import MapConfig
from config.hazard_config import HazardConfig

def create_title_bar():
    """Create the application title bar
    
    Returns:
        html.Div: Title bar component
    """
    return html.Div(
        [
            dbc.Row(
                align="center",
                children=[
                    dbc.Col(
                        html.Img(
                            src=UIConfig.LOGO_PATH,
                            style=UIConfig.LOGO_STYLE,
                        ),
                        width="auto",
                    ),
                    dbc.Col(
                        html.Div(
                            "UW Climate Risk Map",
                            style=UIConfig.TITLE_STYLE,
                        ),
                        width="auto",
                    ),
                ],
                justify="center",
                class_name="g-0",
                style=UIConfig.TITLE_BAR_STYLE,
            )
        ],
        style=UIConfig.TITLE_CONTAINER_STYLE,
    )

def create_region_selector():
    """Create the region selection dropdown
    
    Returns:
        html.Div: Region selector component
    """
    return html.Div(
        children=[
            dbc.Row(
                align="center",
                class_name="g-0",
                children=[
                    html.Div(
                        children=[
                            html.H6(
                                "Select a Region", style={"color": "white"}
                            ),
                            dcc.Dropdown(
                                [
                                    {
                                        "label": region.label,
                                        "value": region.name,
                                    }
                                    for region in MapConfig.REGIONS
                                ],
                                id="region-select-dropdown",
                                placeholder="Select a Region",
                                value=MapConfig.BASE_MAP_COMPONENT["default_region_name"]
                            ),
                        ]
                    )
                ],
            )
        ],
        style=UIConfig.PANEL_SECTION_STYLE,
    )

def create_hazard_indicator_selector():
    """Create the climate variable indicator selection components
    
    Returns:
        html.Div: Hazard indicator variable selector component
    """
    return html.Div(
        children=[
            dbc.Row(
                align="center",
                class_name="g-0",
                children=[
                    html.Div(
                        children=[
                            html.H6(
                                "Select a Hazard Indicator", 
                                style={"color": "white"}
                            ),
                            dcc.Dropdown(
                                [
                                    {
                                        "label": hazard.label,
                                        "value": hazard.name,
                                    }
                                    for hazard in HazardConfig.HAZARDS
                                ],
                                id="hazard-indicator-dropdown",
                                placeholder="Select a Hazard Indicator",
                            ),
                        ]
                    )
                ],
            ),
            html.Br(),
            dbc.Row(
                align="center",
                children=[
                    html.Div(
                        dcc.Dropdown(
                            id="ssp-dropdown",
                            placeholder="Select an Emissions Scenario",
                        )
                    )
                ],
            ),
        ],
        style=UIConfig.PANEL_SECTION_STYLE,
    )

def create_timeframe_selector():
    """Create the timeframe selection components (month and decade)
    
    Returns:
        html.Div: Timeframe selector component
    """
    return html.Div(
        children=[
            dbc.Row(
                align="center",
                children=[
                    html.H6("Select a Timescale", style={"color": "white"}),
                    html.Div(
                        dcc.Slider(
                            1,
                            12,
                            step=None,
                            marks={
                                1: "Jan",
                                2: "Feb",
                                3: "Mar",
                                4: "Apr",
                                5: "May",
                                6: "Jun",
                                7: "Jul",
                                8: "Aug",
                                9: "Sep",
                                10: "Oct",
                                11: "Nov",
                                12: "Dec",
                            },
                            value=8,
                            id="month-slider",
                            className="custom-slider",
                        ),
                    ),
                ],
            ),
            html.Br(),
            dbc.Row(
                align="center",
                children=[
                    dcc.Slider(
                        2020,
                        2100,
                        10,
                        marks={
                            2020: "2020s",
                            2030: "2030s",
                            2040: "2040s",
                            2050: "2050s",
                            2060: "2060s",
                            2070: "2070s",
                            2080: "2080s",
                            2090: "2090s",
                            2100: "2100s",
                        },
                        value=2030,
                        id="decade-slider",
                        className="custom-slider",
                    )
                ],
            ),
        ],
        style=UIConfig.PANEL_SECTION_STYLE,
    )

def create_download_section():
    """Create the download buttons and status message area
    
    Returns:
        html.Div: Download section component
    """
    return html.Div(
        children=[
            dbc.Row(
                align="center",
                children=[
                    dbc.Col(
                        align="center",
                        width="auto",
                        children=[
                            dbc.Button(
                                "Download Data",
                                id="download-btn",
                                className="me-1",
                                n_clicks=0,
                                style=UIConfig.BUTTON_STYLE,
                            ),
                            dcc.Download(id="data-download"),
                        ],
                    )
                ],
                justify="center",
                class_name="g-0",
                style={"border-radius": "25px"},
            ),
            html.Br(),
            dbc.Row(
                align="center",
                children=[
                    dbc.Col(
                        align="center",
                        width={"size": 8, "offset": 0},
                        children=[
                            dbc.Alert(
                                id="download-message",
                                color="danger",
                                is_open=False,
                                duration=3000,
                            )
                        ],
                    ),
                ],
                justify="center",
                class_name="g-0",
                style={"border-radius": "25px"},
            ),
        ],
        style=UIConfig.PANEL_SECTION_STYLE,
    )

def create_control_panel():
    """Create the complete control panel
    
    Returns:
        dbc.Col: Control panel column
    """
    return dbc.Col(
        id="control-panel-col",
        children=[
            create_title_bar(),
            html.Br(),
            create_region_selector(),
            html.Br(),
            create_hazard_indicator_selector(),
            html.Br(),
            create_timeframe_selector(),
            html.Br(),
            create_download_section(),
        ],
        style={"backgroundColor": UIConfig.PANEL_BACKGROUND_COLOR},
        width=3,
    )