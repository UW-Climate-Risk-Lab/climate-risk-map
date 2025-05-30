from dash import html
from config.ui_config import BASEMAP_BUTTON_STYLE

def create_basemap_toggle_button():
    """Create a button to toggle between basemaps"""
    return html.Button(
        children=["Toggle Satellite View"],
        id="basemap-toggle-btn",
        n_clicks=0,
        style=BASEMAP_BUTTON_STYLE
    )