import dash_bootstrap_components as dbc
from dash import html

from config.ui_config import UIConfig
from config.map_config import MapConfig
from config.exposure.definitions import POWER_LINE_CUSTOM_COLOR_RANGES
from config.exposure.asset import AssetConfig


def create_color_legend_item(color, label):
    """Create a legend item for color-based assets (like lines)"""
    return html.Div(
        className="d-flex align-items-center",
        style={
            "marginRight": "20px",
            "marginBottom": "3px",
            "whiteSpace": "nowrap",  # Prevent wrapping within an item
        },
        children=[
            html.Div(
                style={
                    "backgroundColor": color,
                    "width": "25px",
                    "height": "4px",
                    "marginRight": "8px",
                },
            ),
            html.Span(
                label, style={"fontSize": "12px", "color": UIConfig.TEXT_COLOR_DARK}
            ),
        ],
    )


def create_icon_legend_item(icon_path, label):
    """Create a legend item for icon-based assets (like points)"""
    return html.Div(
        className="d-flex align-items-center",
        style={
            "marginRight": "20px",
            "marginBottom": "3px",
            "whiteSpace": "nowrap",  # Prevent wrapping within an item
        },
        children=[
            html.Img(
                src=icon_path,
                style={
                    "width": "18px",
                    "height": "18px",
                    "marginRight": "8px",
                },
            ),
            html.Span(
                label, style={"fontSize": "12px", "color": UIConfig.TEXT_COLOR_DARK}
            ),
        ],
    )


def create_legend_toggle_button():
    """Create a button to toggle legend visibility"""
    return html.Button(
        children=[html.I(className="fa fa-info-circle"), "Legend"],
        id="legend-toggle-btn",
        n_clicks=0,
        style=UIConfig.LEGEND_BUTTON_STYLE,
    )


def create_legend_bar(region_name=None):
    """Create the legend container component

    Args:
        region_name (str): The selected region name

    Returns:
        html.Div: Legend bar component
    """
    if not region_name:
        region_name = MapConfig.BASE_MAP_COMPONENT["default_region_name"]

    region = MapConfig.get_region(region_name=region_name)

    if not region:
        # Return empty legend if region not found
        return html.Div(id="legend-container", style={"display": "none"})

    # Create legend items for available assets in the region
    power_line_items = []
    asset_icon_items = []

    # Only show the power line legend if there are power line assets
    has_power_lines = any(
        asset.name.endswith("-line") or "transmission-line" in asset.name
        for asset in region.available_assets
    )

    if has_power_lines:
        # Add power line voltage color legend items
        power_line_items.append(
            html.Div(
                className="d-flex align-items-center me-3",
                children=[
                    html.Strong(
                        "Power Lines:",
                        style={
                            "fontSize": "12px",
                            "marginRight": "4px",
                            "color": UIConfig.TEXT_COLOR_DARK,
                        },
                    )
                ],
            )
        )

        for range_def in POWER_LINE_CUSTOM_COLOR_RANGES:
            power_line_items.append(
                create_color_legend_item(
                    color=range_def["color"], label=range_def["label"]
                )
            )

    # Add icon-based assets
    icon_assets = [asset for asset in region.available_assets if asset.icon_path]
    if icon_assets:
        asset_icon_items.append(
            html.Div(
                className="d-flex align-items-center me-3",
                children=[
                    html.Strong(
                        "Assets:",
                        style={
                            "fontSize": "12px",
                            "marginRight": "4px",
                            "color": UIConfig.TEXT_COLOR_DARK,
                        },
                    )
                ],
            )
        )

        for asset in icon_assets:
            asset_icon_items.append(
                create_icon_legend_item(icon_path=asset.icon_path, label=asset.label)
            )

    # Create the two rows
    power_line_row = (
        html.Div(
            className="d-flex align-items-center flex-wrap w-100",
            style={
                "marginBottom": "8px",
                "justifyContent": "space-between",
            },  # Add space between rows
            children=power_line_items,
        )
        if power_line_items
        else None
    )

    asset_icon_row = (
        html.Div(
            className="d-flex align-items-center flex-wrap w-100",
            children=asset_icon_items,
        )
        if asset_icon_items
        else None
    )

    # Combine rows into the legend content
    legend_content = []
    if power_line_items:
        legend_content.append(power_line_row)
    if asset_icon_row:
        legend_content.append(asset_icon_row)

    legend_bar = html.Div(
        id="legend-bar",
        className="shadow-sm",
        style=UIConfig.LEGEND_BAR_STYLE,
        children=legend_content,
    )

    return legend_bar
