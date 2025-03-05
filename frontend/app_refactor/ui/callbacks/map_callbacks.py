import logging
from dash import Input, Output, no_update
from dash.exceptions import PreventUpdate

from config.map_config import MapConfig
from services.map_service import MapService
from utils.error_utils import handle_callback_error

logger = logging.getLogger(__name__)

def register_map_callbacks(app):
    """Register all map-related callbacks
    
    Args:
        app: Dash application instance
    """
    
    @app.callback(
        Output("region-outline-geojson", "url"),
        Output(MapConfig.BASE_MAP_COMPONENT["id"], "viewport"),
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error
    def handle_region_outline(selected_region):
        """Update state outline and map viewport when state selection changes
        
        Args:
            selected_state (str): Selected state
            
        Returns:
            tuple: GeoJSON URL and viewport settings
        """
        if not selected_region:
            raise no_update
        
        region = MapConfig.get_region(name=selected_region)

        if not region:
            logger.error(f"{selected_region} from region select dropdown is not configured")
            return no_update
            
        geojson_path = region.geojson
        viewport = {
            "center": {"lat": region.map_center_lat, "lng": region.map_center_lon},
            "zoom": region.map_zoom,
            "transition": MapConfig.BASE_MAP_COMPONENT["viewport"]["transition"],
        }
            
        # Get state outline URL and viewport settings
        return geojson_path, viewport
    
    @app.callback(
        Output(MapConfig.BASE_MAP_COMPONENT["asset_layer"]["id"], "children"),
        Input("region-select-dropdown", "value"),
        prevent_initial_call=True,
    )
    @handle_callback_error
    def handle_region_features(selected_region):
        """Update map overlays when region selection changes. This queries
        database and loads vector features. 
        
        Args:
            selected_state (str): Selected state
            
        Returns:
            list: List of map overlays
        """
        if not selected_region:
            raise no_update
            
        region = MapConfig.get_region(name=selected_region)

        if region:
            return MapService.get_asset_overlays(region)
        else:
            logger.error(f"{selected_region} from region select dropdown is not configured")
            return no_update

        