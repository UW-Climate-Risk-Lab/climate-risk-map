import logging
import pandas as pd
from dash import Input, Output, no_update, State, dcc
from dash.exceptions import PreventUpdate

from config.map_config import MapConfig

from services.map_service import MapService
from services.hazard_service import HazardService
from services.download_service import DownloadService

from utils.error_utils import handle_callback_error

logger = logging.getLogger(__name__)


def register_download_callbacks(app):
    """Register all map-related callbacks.

    Args:
        app: Dash application instance
    """

    @app.callback(
        [
            Output("data-download", "data"),
            Output("download-btn", "n_clicks"),
            Output("download-counter", "data"),
            Output("download-message", "children"),
            Output("download-message", "is_open"),
            Output("download-message", "color"),
        ],
        [
            Input("download-btn", "n_clicks"),
            Input(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            Input(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "children"),
            Input("region-select-dropdown", "value"),
            Input("hazard-indicator-dropdown", "value"),
            Input("ssp-dropdown", "value"),
            Input("decade-slider", "value"),
            Input("month-slider", "value"),
        ],
        State("download-counter", "data"),
    )
    def download_data(
        n_clicks,
        shapes,
        selected_asset_overlays,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
        stored_download_count,
    ):

        if n_clicks is None or n_clicks == 0:
            raise PreventUpdate

        if n_clicks > 0:
            download = DownloadService.get_download(
                shapes=shapes,
                asset_overlays=selected_asset_overlays,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
                download_count=stored_download_count,
            )

            return (
                download.data_sender,
                download.n_clicks,
                download.download_count,
                download.download_message,
                download.download_message_is_open,
                download.download_message_color,
            )


        return no_update, 0, stored_download_count, no_update, no_update, no_update
