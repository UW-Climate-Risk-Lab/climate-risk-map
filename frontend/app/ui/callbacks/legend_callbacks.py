import logging
from dash import Input, Output, no_update


from config.ui_config import UIConfig
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
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def update_legend(selected_region):
        """Update legend when region selection changes

        Args:
            selected_region (str): Selected region

        Returns:
            html.Div: Updated legend component
        """
        if not selected_region:
            return no_update

        logger.debug(f"Updating legend for region: {selected_region}")

        # Create new legend bar for selected region
        legend_bar = create_legend_bar(region_name=selected_region)

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

        legend_container_style = UIConfig.LEGEND_CONTAINER_STYLE.copy()
        legend_button_style = UIConfig.LEGEND_BUTTON_STYLE.copy()

        # Toggle visibility based on even/odd clicks
        if n_clicks % 2 == 1:
            # Hide legend
            legend_container_style["display"] = "none"
            legend_button_style["backgroundColor"] = "white"
            legend_button_style["color"] = UIConfig.PRIMARY_COLOR
            return legend_container_style, legend_button_style
        else:
            return legend_container_style, legend_button_style