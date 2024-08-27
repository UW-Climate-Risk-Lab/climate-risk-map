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
                            "color": "#4B2E83",
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
