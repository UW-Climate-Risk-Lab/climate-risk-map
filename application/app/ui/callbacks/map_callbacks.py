import logging
import time
from dash import Input, Output, no_update

from config.map_config import MapConfig

from services.map_service import MapService
from services.hazard_service import HazardService

from utils.error_utils import handle_callback_error

logger = logging.getLogger(__name__)


def register_map_callbacks(app):
    """Register all map-related callbacks

    Args:
        app: Dash application instance
    """

    @app.callback(
        Output("region-outline-geojson", "url"),
        Input("region-outline-change-signal", "data"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=2)
    def update_region_outline(selected_region: str):
        """Update region outline when selected region changes

        Pull selected region from the region-outline-change-signal Store.
        This allows the previous outline to be removed from the map before this one
        is updated

        Args:
            selected_region (str): Selected region

        Returns:
            str: Path to GeoJSON of region outline
        """
        if not selected_region:
            return no_update

        region = MapConfig.get_region(region_name=selected_region)

        if not region:
            logger.error(
                f"{selected_region} from region select dropdown is not configured"
            )
            return no_update

        geojson_path = region.geojson

        # Wait before returning path to allow map to fly to new region
        time.sleep(1.5)

        return geojson_path

    @app.callback(
        [
            Output("region-outline-geojson", "url", allow_duplicate=True),
            Output("region-outline-change-signal", "data"),
        ],
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=2)
    def remove_region_outline(selected_region: str):
        """Remove region outline and

        Args:
            selected_region (str): Selected region

        Returns:
            str: Path to GeoJSON of region outline
        """
        if not selected_region:
            return no_update

        return None, selected_region

    @app.callback(
        Output(MapConfig.BASE_MAP_COMPONENT["id"], "viewport"),
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=2)
    def update_region_viewport(selected_region: str):
        """Update map viewport when region selection changes

        Args:
            selected_region (str): Selected region

        Returns:
            tuple: Viewport Settings
        """
        if not selected_region:
            return no_update

        region = MapConfig.get_region(region_name=selected_region)

        if not region:
            logger.error(
                f"{selected_region} from region select dropdown is not configured"
            )
            return no_update

        viewport = {
            "center": {"lat": region.map_center_lat, "lng": region.map_center_lon},
            "zoom": region.map_zoom,
            "transition": MapConfig.BASE_MAP_COMPONENT["viewport"]["transition"],
        }

        # Get state outline URL and viewport settings
        return viewport

    @app.callback(
        Output(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "children"),
        Output(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "overlays"),
        Input("region-features-change-signal", "data"),
        Input("exposure-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def update_region_features(selected_region, selected_exposure):
        """Update map overlays when region selection changes. This queries
        database and loads vector features.

        The selected region is pulled from the region-change-signal data store (in layout.py)
        This ensures that the current asset overlays are removed before this callback fires

        Args:
            selected_region (str): Selected region
            selected_exposure(str): Selected exposure

        Returns:
            list: List of map overlays
        """
        if not selected_region:
            return no_update

        if not selected_exposure:
            return no_update

        overlays, overlay_names = MapService.get_asset_overlays(
            asset_group_name=selected_exposure, region_name=selected_region
        )

        return overlays, overlay_names

    @app.callback(
        [
            Output(
                MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"],
                "children",
                allow_duplicate=True,
            ),
            Output("region-features-change-signal", "data"),
            Output("exposure-select-dropdown", "value")
        ],
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def remove_region_features(selected_region):
        """Remove map asset overlays when region selection changes.

        We output the selected region into the store. This ensures
        that the current asset overlays are removed before the update_regions
        callback fires and adds new assets.

        Args:
            selected_region (str): Selected region
            selected_exposure(str): Selected exposure

        Returns:
            list, str: List of map overlays, selected region, and None for exposure dropdown
        """
        if not selected_region:
            return no_update, no_update, no_update

        overlays = list()

        return overlays, selected_region, None

    @app.callback(
        Output(
            MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"],
            "children",
            allow_duplicate=True,
        ),
        Input("exposure-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def remove_exposure_features(selected_exposure):
        """Remove map asset overlays when exposure dropdown is cleared.

        Similar to remove_region_features. We return an empty list if
        there is "None" for the selected exposure dropdown

        Args:
            selected_exposure(str): Selected exposure

        Returns:
            list: Em
        """
        if selected_exposure is None:
            return list()

        return no_update

    @app.callback(
        Output("region-select-message", "is_open"),
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error(output_count=1)
    def update_region_select_message(selected_region):
        """When region is selected, open pop up message to notify user region
        assets are loading

        Args:
            selected_region (str): Selected region

        Returns:
            bool: True to display message
        """
        if not selected_region:
            return no_update

        return True

    @app.callback(
        [
            Output(MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"]["id"], "url"),
            Output(MapConfig.BASE_MAP_COMPONENT["hazard_tile_layer"]["id"], "opacity"),
        ],
        [
            Input("hazard-indicator-dropdown", "value"),
            Input("ssp-dropdown", "value"),
            Input("decade-slider", "value"),
            Input("month-slider", "value"),
            Input("region-select-dropdown", "value"),
        ],
    )
    @handle_callback_error(output_count=2)
    def update_hazard_tiles(
        selected_hazard: str, ssp: int, decade: int, month: int, selected_region: str
    ):
        """Update climate tiles based on user selections

        Args:
            selected_hazard (str): Selected hazard indicator variable
            ssp (str): Selected emissions scenario
            decade (int): Selected decade
            month (int): Selected month
            selected_region (str): Selected region name

        Returns:
            tuple: Tile URL, opacity
        """
        logger.debug(
            f"Updating hazard tiles: var={selected_hazard}, ssp={ssp}, decade={str(decade)}, month={str(month)}"
        )

        # If any required inputs are missing, return default values
        if (
            (ssp is None)
            or (selected_hazard is None)
            or (decade is None)
            or (month is None)
        ):
            return no_update

        url, opacity = HazardService.get_hazard_tilejson_url(
            hazard_name=selected_hazard,
            ssp=int(ssp),
            month=int(month),
            decade=int(decade),
            region_name=selected_region,
        )

        # Get climate tile data from service
        return url, opacity

    @app.callback(
        [
            Output(
                MapConfig.BASE_MAP_COMPONENT["color_bar_layer"]["parent_div_id"],
                "children",
            )
        ],
        [
            Input("hazard-indicator-dropdown", "value"),
            Input("ssp-dropdown", "value"),
            Input("decade-slider", "value"),
            Input("month-slider", "value"),
        ],
    )
    @handle_callback_error(output_count=1)
    def update_color_bar(selected_hazard, ssp, decade, month):
        if (
            (ssp is None)
            or (selected_hazard is None)
            or (decade is None)
            or (month is None)
        ):
            return no_update

        color_bar = MapService.get_color_bar(hazard_name=selected_hazard)

        return [color_bar]

    @app.callback(
        [Output("ssp-dropdown", "options")],
        [Input("hazard-indicator-dropdown", "value")],
    )
    @handle_callback_error(output_count=1)
    def update_ssp_dropdown(hazard_name: str):
        """Update SSP dropdown options based on selected climate variable

        Args:
            hazard_name (str): Selected climate variable

        Returns:
            list: List of available SSP options
        """
        if not hazard_name:
            return no_update

        ssp_options = HazardService.get_available_ssp(hazard_name=hazard_name)

        return [ssp_options]

    @app.callback(
        [Output("exposure-select-dropdown", "options")],
        [Input("region-select-dropdown", "value")],
    )
    @handle_callback_error(output_count=1)
    def update_exposure_dropdown(region_name: str):
        """Update exposire dropdown options based on selected region

        Args:
            region_name (str): Selected climate variable

        Returns:
            list: List of available SSP options
        """
        if not region_name:
            return no_update

        region = MapConfig.get_region(region_name=region_name)
        exposure_options = [
            asset_group.label for asset_group in region.available_asset_groups
        ]

        return [exposure_options]
