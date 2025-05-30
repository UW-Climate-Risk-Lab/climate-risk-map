from dash import Input, Output, State, callback
import dash_bootstrap_components as dbc
from dash import html

from config.ui_config import PASSWORD_CONTAINER_STYLE
from services.auth_service import AuthService


def register_auth_callbacks(app):
    """Register authentication-related callbacks

    Args:
        app (Dash): The Dash application instance
    """

    @callback(
        [
            Output("password-screen-container", "style"),
            Output("main-app-container", "style"),
            Output("auth-status", "data"),
            Output("password-feedback", "children"),
        ],
        [Input("password-submit", "n_clicks"), Input("password-input", "n_submit")],
        [State("password-input", "value"), State("auth-status", "data")],
        prevent_initial_call=False,
    )
    def validate_password(submit_clicks, enter_pressed, password, current_auth_status):
        """Validate the password and update the UI accordingly

        Args:
            submit_clicks: Number of clicks on the submit button
            enter_pressed: Number of times Enter was pressed in the password field
            password: The entered password
            current_auth_status: Current authentication status

        Returns:
            tuple: Updated style for password screen, main app, auth status, and feedback message
        """
        # If already authenticated, maintain the state
        if current_auth_status:
            return ({"display": "none"}, {"display": "block"}, True, "")

        # If no trigger or no password entered, keep the current state
        if (not submit_clicks and not enter_pressed) or not password:
            return (
                PASSWORD_CONTAINER_STYLE,
                {"display": "none"},
                False,
                "",
            )

        # Validate the password
        if AuthService.validate_password(password):
            # If password is correct, hide password screen and show main app
            return ({"display": "none"}, {"display": "block"}, True, "")
        else:
            # If password is incorrect, show error message
            return (
                PASSWORD_CONTAINER_STYLE,
                {"display": "none"},
                False,
                dbc.Alert("Incorrect password. Please try again.", color="danger"),
            )
