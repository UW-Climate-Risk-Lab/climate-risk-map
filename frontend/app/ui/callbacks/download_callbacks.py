import logging
from dash import Input, Output, no_update, State, dcc

from config.map_config import MapConfig

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
            Output("download-allowed", "data"),
            Output("download-btn", "n_clicks"),
            Output("download-counter", "data"),
            Output("download-message", "children"),
            Output("download-message", "is_open"),
            Output("download-message", "color"),
        ],
        [
            Input("download-btn", "n_clicks"),
            Input(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            Input(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            Input("region-select-dropdown", "value"),
            Input("hazard-indicator-dropdown", "value"),
            Input("ssp-dropdown", "value"),
            Input("decade-slider", "value"),
            Input("month-slider", "value"),
        ],
        State("download-counter", "data"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=6)
    def prep_download(
        n_clicks,
        shapes,
        selected_assets,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
        stored_download_count,
    ):

        #if n_clicks is None or n_clicks == 0:
            #raise PreventUpdate

        if n_clicks > 0:
            download = DownloadService.create_download_config(
                shapes=shapes,
                asset_overlays=selected_assets,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
                download_count=stored_download_count,
            )

            return (
                download.download_allowed,
                0,
                download.download_count,
                download.download_message,
                download.download_message_is_open,
                download.download_message_color,
            )

        return no_update, 0, stored_download_count, no_update, no_update, no_update

    @app.callback(
        [
            Output("data-download", "data", allow_duplicate=True),
            Output("download-allowed", "data", allow_duplicate=True),
            Output("download-counter", "data", allow_duplicate=True),
            Output("download-message", "children", allow_duplicate=True),
            Output("download-message", "is_open", allow_duplicate=True),
            Output("download-message", "color", allow_duplicate=True),
        ],
        [
            Input("download-allowed", "data"),
            Input(MapConfig.BASE_MAP_COMPONENT["drawn_shapes_layer"]["id"], "geojson"),
            Input(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
            Input("region-select-dropdown", "value"),
            Input("hazard-indicator-dropdown", "value"),
            Input("ssp-dropdown", "value"),
            Input("decade-slider", "value"),
            Input("month-slider", "value"),
        ],
        State("download-counter", "data"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=6)
    def download(
        download_allowed,
        shapes,
        selected_assets,
        selected_region,
        selected_hazard,
        selected_ssp,
        selected_decade,
        selected_month,
        stored_download_count,
    ):
        if download_allowed:
            df = DownloadService.get_download(
                shapes=shapes,
                asset_overlays=selected_assets,
                region_name=selected_region,
                hazard_name=selected_hazard,
                ssp=selected_ssp,
                decade=selected_decade,
                month=selected_month,
            )

            if len(df) > 0:
                stored_download_count += 1
                return (
                    dcc.send_data_frame(df.to_csv, "climate-risk-map-download.csv"),
                    False,
                    stored_download_count,
                    "Data Downloaded!",
                    True,
                    "success",
                )
            else:
                return (
                    None,
                    False,
                    stored_download_count,
                    "No data found in selected area!",
                    True,
                    "warning",
                )

        return no_update, False, stored_download_count, no_update, no_update, no_update
