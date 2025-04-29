"""
UI Configuration for the Climate Risk Application

This module centralizes all UI-related configuration settings including
color schemes, component styles, and layout parameters. By centralizing
these settings, we can maintain a consistent look and feel throughout
the application and make global style changes easily.
"""

from config.settings import ASSETS_PATH


# Color scheme
# Primary colors
PRIMARY_COLOR = "#39275B"  # UW purple
SECONDARY_COLOR = "#4B2E83"  # Deeper UW purple
ACCENT_COLOR = "#85754D"  # UW gold

# UI element colors
PANEL_BACKGROUND_COLOR = PRIMARY_COLOR
BUTTON_COLOR = "white"
TEXT_COLOR_LIGHT = "white"
TEXT_COLOR_DARK = "#333333"
ALERT_INFO_COLOR = "#5BC0DE"
ALERT_SUCCESS_COLOR = "#5CB85C"
ALERT_WARNING_COLOR = "#F0AD4E"
ALERT_DANGER_COLOR = "#D9534F"

# Logo location
LOGO_PATH = ASSETS_PATH + "/icons/CRL-Logo.png"

# Logo style
LOGO_STYLE = {
    "width": "10vw",
    "max-width": "50px",
    "height": "auto",
    "object-fit": "contain",
}

# Title style
TITLE_STYLE = {
    "color": PRIMARY_COLOR,
    "font-size": "2vw",
    "white-space": "nowrap",
    "text-align": "center",
    "overflow": "hidden",
    "padding": "5px",
}

# Title bar style (the container with logo and title)
TITLE_BAR_STYLE = {
    "backgroundColor": "white",
    "border-radius": "15px",
}

# Container for the title bar
TITLE_CONTAINER_STYLE = {
    "padding": "10px",
}

# Style for panel sections
PANEL_SECTION_STYLE = {
    "padding": "15px",
    "margin-bottom": "10px",
}

# Button styles
BUTTON_STYLE = {
    "backgroundColor": "white",
    "border-radius": "15px",
    "color": PRIMARY_COLOR,
}

BUTTON_SECONDARY_STYLE = {
    "backgroundColor": SECONDARY_COLOR,
    "border-radius": "15px",
    "color": "white",
    "font-weight": "500",
}

# Dropdown styles
DROPDOWN_STYLE = {
    "border-radius": "5px",
    "margin-bottom": "10px",
}

# Slider styles
SLIDER_STYLE = {
    "margin-top": "10px",
    "margin-bottom": "20px",
}

# Section header styles
SECTION_HEADER_STYLE = {
    "color": TEXT_COLOR_LIGHT,
    "font-size": "1.2rem",
    "margin-bottom": "10px",
}

# Alert styles
ALERT_STYLE = {
    "border-radius": "10px",
    "padding": "10px",
    "margin-top": "10px",
}

# Map container style
MAP_CONTAINER_STYLE = {
    "height": "100vh",
    "width": "100%",
}

# Map overlay control style
MAP_CONTROL_STYLE = {
    "backgroundColor": "white",
    "padding": "5px",
    "border-radius": "5px",
    "box-shadow": "0 0 10px rgba(0,0,0,0.1)",
}

# Responsive breakpoints (can be used for conditional styling)
BREAKPOINTS = {
    "xs": 576,  # Extra small devices
    "sm": 768,  # Small devices
    "md": 992,  # Medium devices
    "lg": 1200,  # Large devices
}

# CSS classes for common styling patterns
CSS_CLASSES = {
    "rounded-container": {
        "border-radius": "15px",
        "overflow": "hidden",
    },
    "shadow": {
        "box-shadow": "0 4px 6px rgba(0,0,0,0.1)",
    },
    "centered-content": {
        "display": "flex",
        "justify-content": "center",
        "align-items": "center",
    },
}

# Layout configurations
LAYOUT = {
    "control_panel_width": 3,  # Width in Bootstrap grid columns (out of 12)
    "map_width": 9,  # Width in Bootstrap grid columns (out of 12)
    "padding": "15px",
    "gap": "10px",
}

# Custom data visualization colors (for charts, etc.)
VIZ_COLORS = [
    "#4E79A7",  # Blue
    "#F28E2B",  # Orange
    "#E15759",  # Red
    "#76B7B2",  # Teal
    "#59A14F",  # Green
    "#EDC948",  # Yellow
    "#B07AA1",  # Purple
    "#FF9DA7",  # Pink
    "#9C755F",  # Brown
    "#BAB0AC",  # Grey
]

# Icon sizes
ICON_SIZES = {
    "small": "15px",
    "medium": "24px",
    "large": "32px",
}

# Animation settings
ANIMATIONS = {
    "transition_duration": "300ms",
    "transition_timing": "ease-in-out",
}

LEGEND_BAR_STYLE = {
    "backgroundColor": "white",
    "padding": "8px 15px",
    "borderRadius": "15px",
    "marginBottom": "10px",
    "display": "flex",
    "flexDirection": "column",
    "alignItems": "stretch",
    "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
    "border": f"1px solid {PRIMARY_COLOR}",
    "maxHeight": "90px",
    "overflowY": "auto",
    "width": "100%",
    "justifyContent": "flex-start",
}

LEGEND_CONTAINER_STYLE = {
    "position": "absolute",
    "top": "15px",
    "left": "50%",
    "transform": "translateX(-50%)",
    "zIndex": "1000",
    "width": "70%",
    "maxWidth": "90%",
    "pointerEvents": "auto",  # Make sure it's clickable if needed
}

LEGEND_BUTTON_STYLE = {
    "position": "absolute",
    "top": "190px",
    "right": "10px",
    "zIndex": "1001",
    "backgroundColor": PRIMARY_COLOR,
    "color": "white",
    "border": f"1px solid {PRIMARY_COLOR}",
    "borderRadius": "4px",
    "padding": "10px 10px",
    "fontSize": "12px",
    "cursor": "pointer",
    "display": "flex",
    "alignItems": "center",
    "gap": "0px",
}

MAP_FEATURES_LOADING_SPINNER_STYLE = {
    "position": "absolute",
    "top": "50%",
    "left": "50%",
    "transform": "translate(-50%, -50%)",
    "zIndex": 1500,  # Very high z-index
    "backgroundColor": "rgba(255, 255, 255, 0.5)",  # Semi-transparent background
    "borderRadius": "10px",
    "padding": "20px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "center",
}

POWER_LINE_CUSTOM_COLOR_RANGES = [
    {"min": 0, "max": 100, "color": "#5b8f22", "label": "< 100 kV"},
    {"min": 100, "max": 300, "color": "#0046AD", "label": "100-300 kV"},
    {"min": 300, "max": 500, "color": "#63B1E5", "label": "300-500 kV"},
    {"min": 500, "max": float("inf"), "color": "#C75B12", "label": "> 500 kV"},
]
