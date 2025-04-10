import logging
import atexit
import dash_bootstrap_components as dbc
from dash import Dash

from config.settings import ASSETS_PATH, DEBUG
from ui.components.layout import create_main_layout
from ui.callbacks import register_all_callbacks
from dao.database import DatabaseManager
from utils.log_utils import configure_logging

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Initialize the Dash application
logger.info("Initializing Dash application")
app = Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME], 
    assets_folder=ASSETS_PATH
)
server = app.server

# Set application layout
app.layout = create_main_layout()

# Register all callbacks
register_all_callbacks(app)

# Register shutdown function to close all database connections
def shutdown():
    """Clean up resources on application shutdown"""
    logger.info("Closing all database connections")
    DatabaseManager.close_all_pools()
    
atexit.register(shutdown)

if __name__ == "__main__":
    logger.info("Starting Dash server")
    app.run_server(
        host="0.0.0.0", 
        port=8050, 
        debug=DEBUG, 
        dev_tools_hot_reload=False
    )