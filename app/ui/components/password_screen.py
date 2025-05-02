import dash_bootstrap_components as dbc
from dash import html, dcc

from config.ui_config import PASSWORD_CONTAINER_STYLE

def create_password_screen():
    """Create a simple password input screen
    
    Returns:
        html.Div: Password screen container
    """
    return html.Div(
        id="password-screen-container",
        style=PASSWORD_CONTAINER_STYLE,
        children=[
            dbc.Card(
                [
                    dbc.CardHeader("Authentication Required"),
                    dbc.CardBody(
                        [
                            html.P("Please enter the password to access the application:"),
                            dbc.Input(
                                id="password-input",
                                type="password",
                                placeholder="Enter password",
                                autoFocus=True
                            ),
                            html.Div(id="password-feedback", style={"marginTop": "10px"}),
                            dbc.Button(
                                "Submit", 
                                id="password-submit", 
                                color="primary", 
                                style={"marginTop": "15px"}
                            ),
                        ]
                    ),
                ],
                style={"width": "400px"},
            )
        ]
    )