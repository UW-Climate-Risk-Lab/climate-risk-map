from ui.callbacks.map_callbacks import register_map_callbacks
from ui.callbacks.download_callbacks import register_download_callbacks
from ui.callbacks.legend_callbacks import register_legend_callbacks
from ui.callbacks.chat_callbacks import register_chat_callbacks
from ui.callbacks.auth_callbacks import register_auth_callbacks

def register_all_callbacks(app):
    """Register all application callbacks
    
    Args:
        app: Dash application instance
    """
    
    register_map_callbacks(app)
    register_download_callbacks(app)
    register_legend_callbacks(app)
    register_chat_callbacks(app)
    register_auth_callbacks(app)