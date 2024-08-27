import dash_bootstrap_components as dbc
from dash import html, dcc

TITLE_BAR = html.Div(
    [
        dbc.Row(
            align="center",  # Align content in the middle vertically
            children=[
                dbc.Col(
                    html.Img(
                        src="/assets/CRL-Logo.png",
                        style={
                            "width": "50px",
                            "height": "50px",
                            "object-fit": "contain",
                        },
                    ),
                    width="auto",  # Let the image column take just enough space
                ),
                dbc.Col(
                    html.Div(
                        "UW Climate Risk Lab",
                        style={
                            "color": "#4B2E83",
                            "font-size": "2vw",  # Scale text size based on viewport width
                            "white-space": "nowrap",  # Prevent wrapping to a new line
                            "text-align": "center",
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
