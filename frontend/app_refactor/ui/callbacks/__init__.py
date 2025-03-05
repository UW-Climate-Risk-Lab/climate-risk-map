from ui.callbacks.climate_callbacks import register_climate_callbacks
from ui.callbacks.map_callbacks import register_map_callbacks
from ui.callbacks.download_callbacks import register_download_callbacks

def register_all_callbacks(app):
    """Register all application callbacks
    
    Args:
        app: Dash application instance
    """
    register_climate_callbacks(app)
    register_map_callbacks(app)
    register_download_callbacks(app)