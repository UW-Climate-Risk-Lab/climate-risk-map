import dash_bootstrap_components as dbc
from dash import html, dcc

import app_config

TITLE_BAR = html.Div(
    [
        dbc.Row(
            align="center",  # Align content in the middle vertically
            children=[
                dbc.Col(
                    html.Img(
                        src="/assets/CRL-Logo.png",
                        style={
                            "width": "10vw",  # Set the width relative to viewport width
                            "max-width": "50px",  # Limit the max size of the image
                            "height": "auto",  # Maintain aspect ratio
                            "object-fit": "contain",
                        },
                    ),
                    width="auto",  # Let the image column take just enough space
                ),
                dbc.Col(
                    html.Div(
                        "UW Climate Risk Lab",
                        style={
                            "color": "#39275B",
                            "font-size": "2vw",  # Scale text size based on viewport width
                            "white-space": "nowrap",  # Prevent wrapping to a new line
                            "text-align": "center",
                            "overflow": "hidden",
                            "padding": "5px",
                        },
                    ),
                    width="auto",  # This makes the text take the remaining space
                ),
            ],
            justify="center",
            class_name="g-0",  # Remove gap between the columns
            style={
                "backgroundColor": "white",
                "border-radius": "15px",
            },  # Make the box rounded,
        )
    ],
    style={
        "padding": "10px",
    },  # Add a subtle shadow for effect},  # Add padding to the Div
)

# TODO: As we add more climate variables, need to make this update the map baseLayer selection
CLIMATE_VARIABLE_SELECTOR = html.Div(
    children=[
        dbc.Row(
            align="center",
            class_name="g-0",
            children=[
                html.Div(
                    children=[
                        html.H6(
                            "Select a Climate Risk Measure...", style={"color": "white"}
                        ),
                        dcc.Dropdown(
                            [
                                {
                                    "label": properties["label"],
                                    "value": climate_variable,
                                }
                                for climate_variable, properties in app_config.CLIMATE_DATA.items()
                            ],
                            id="climate-variable-dropdown",
                            placeholder="Select a Climate Variable...",
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
                        placeholder="Select an Emissions Scenario...",
                    )
                )
            ],
        ),
    ],
    style={"padding": "15px"},
)

CLIMATE_SCENARIO_SELECTOR = html.Div(
    children=[
        dbc.Row(
            align="center",
            children=[
                html.H6("Select a Timescale...", style={"color": "white"}),
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
    style={"padding": "15px"},
)

DOWNLOAD_DATA_BUTTONS = html.Div(
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
                            id="csv-btn",
                            className="me-1",
                            n_clicks=0,
                            style={
                                "backgroundColor": "white",
                                "border-radius": "15px",
                                "color": "#39275B",
                            },  # Make the box rounded,
                        ),
                        dcc.Download(id="csv-download"),
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
    ]
)
