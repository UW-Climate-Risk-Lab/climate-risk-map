import logging
from dash import Input, Output, no_update


from config.ui_config import PRIMARY_COLOR, LEGEND_CONTAINER_STYLE, LEGEND_BUTTON_STYLE
from ui.components.legend import create_legend_bar
from utils.error_utils import handle_callback_error

logger = logging.getLogger(__name__)


def register_legend_callbacks(app):
    """Register all legend-related callbacks

    Args:
        app: Dash application instance
    """

    @app.callback(
        Output("legend-container", "children"),
        Input("exposure-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def update_legend(selected_exposure):
        """Update legend when region selection changes

        Args:
            selected_exposure (str): Selected exposure grouping

        Returns:
            html.Div: Updated legend component
        """

        logger.debug(f"Updating legend for exposure: {selected_exposure}")

        # Create new legend bar for selected region
        legend_bar = create_legend_bar(asset_group_name=selected_exposure)

        return [legend_bar]

    @app.callback(
        [Output("legend-container", "style"), Output("legend-toggle-btn", "style")],
        Input("legend-toggle-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def toggle_legend_visibility(n_clicks):
        """Toggle legend visibility when button is clicked

        Changes button color to give on/off visual cue to user

        Args:
            n_clicks (int): Number of button clicks

        Returns:
            dict: Updated style for legend container
        """
        if n_clicks is None:
            return no_update

        legend_container_style = LEGEND_CONTAINER_STYLE.copy()
        legend_button_style = LEGEND_BUTTON_STYLE.copy()

        # Toggle visibility based on even/odd clicks
        if n_clicks % 2 == 1:
            # Hide legend
            legend_container_style["display"] = "none"
            legend_button_style["backgroundColor"] = "white"
            legend_button_style["color"] = PRIMARY_COLOR
            return legend_container_style, legend_button_style
        else:
            return legend_container_style, legend_button_style
