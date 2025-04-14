"""
This module contains a serice layer for updating map components.
The methods in the MapService class should return dash-leaflet component objects

"""

import logging
import dash_leaflet as dl

from typing import List, Tuple
from dash import html
from dash_extensions.javascript import arrow_function

from config.hazard_config import HazardConfig
from config.ui_config import PRIMARY_COLOR
from dao.exposure_dao import ExposureDAO

from config.map_config import MapConfig
from config.exposure import (
    get_asset_group,
    TRANSPARENT_MARKER_CLUSTER,
    CREATE_FEATURE_ICON,
    CREATE_FEATURE_COLOR_STYLE,
)

logger = logging.getLogger(__name__)


class MapService:
    """Service for handling map-related operations"""

    @staticmethod
    def get_base_map() -> dl.Map:
        """Generate the base map component

        Returns:
            dl.Map: Configured map component
        """
        config = MapConfig.BASE_MAP_COMPONENT
        default_region = MapConfig.get_region(region_name=config["default_region_name"])

        # Base map layer
        base_map_layer = dl.TileLayer(
            id=config["base_map_layer"]["id"],
            url=config["base_map_layer"]["url"],
            attribution=config["base_map_layer"]["attribution"],
        )

        # Drawing tools component
        # Used for drawing bounding boxes
        drawn_shapes_layer = dl.FeatureGroup(
            [
                dl.EditControl(
                    id=config["drawn_shapes_layer"]["id"],
                    draw=config["drawn_shapes_layer"]["draw"],
                    edit=config["drawn_shapes_layer"]["edit"],
                )
            ]
        )

        # Placeholder for climate hazard raster data.
        # When hazard data is selected,, url can be updated
        hazard_tile_layer = dl.TileLayer(
            id=config["hazard_tile_layer"]["id"],
            url=config["hazard_tile_layer"]["placeholder_url"],
            opacity=config["hazard_tile_layer"]["placeholder_opacity"],
        )

        # Layer control for toggling asset/exposure features
        # default_assets, default_asset_labels = MapService.get_asset_overlays(
        #     asset_group_name=config["default_asset_group_name"],
        #     region_name=default_region.name
        # )
        asset_layer = dl.LayersControl(
            id=config["asset_layer"]["id"],
            children=list(),
            overlays=list(),
        )

        # State outline overlay
        state_outline_overlay = MapService.get_region_overlay(
            region_name=default_region.name, z_index=300
        )

        # Color bar (initially hidden)
        color_bar_layer = html.Div(
            id=config["color_bar_layer"]["parent_div_id"],
            style={"display": "none"},
            children=[],
        )

        # Assemble the map
        map_component = dl.Map(
            children=[
                base_map_layer,
                drawn_shapes_layer,
                hazard_tile_layer,
                asset_layer,
                state_outline_overlay,
                color_bar_layer,
            ],
            center={
                "lat": default_region.map_center_lat,
                "lng": default_region.map_center_lon,
            },
            zoom=default_region.map_zoom,
            style=config["style"],
            id=config["id"],
            preferCanvas=config["preferCanvas"],
        )

        return map_component

    @staticmethod
    def get_region_overlay(region_name, z_index) -> dl.Pane:
        """Get a region outline overlay from geojson

        Args:
            region (str): Region name (lowercase)
            z_index (int): Z-index for layer ordering

        Returns:
            dl.Pane: Pane component containing GeoJSON region outline
        """

        region = MapConfig.get_region(region_name=region_name)

        if not region:
            # Default to United State overlay if region is unavailable
            region = MapConfig.get_region(region_name="usa")

        layer = dl.Pane(
            dl.GeoJSON(
                url=region.geojson,
                style={
                    "color": PRIMARY_COLOR,
                    "weight": 2,
                    "fillOpacity": 0,
                },
                zoomToBoundsOnClick=True,
                id="region-outline-geojson",
            ),
            id="region-outline-pane",
            name="region_pane",
            style=dict(zIndex=z_index),
        )
        return layer

    @staticmethod
    def get_asset_overlays(asset_group_name: str, region_name: str) -> Tuple[List[dl.Overlay], List[str]]:
        """Get asset overlays for the specified region, and return the overlay geojson
        data and the labels that are checked. We default to checking all asset layers
        when assets are loaded.

        Args:
            asset_group_name (str): Name of asset group
            region_name (str): Name of region

        Returns:
            Tuple[List[dl.Overlay], List[str]]: List of dl.Overlay components, which
            contains the actual data to display. The list of strings contain the asset labels
            that are currently checked.
        """
        overlays = []
        overlay_names = []

        asset_group = get_asset_group(name=asset_group_name)
        region = MapConfig.get_region(region_name=region_name)

        if not asset_group:
            return list()

        # Iterate through the specified assets so that each asset gets its own layer
        # These are are configured manually in config/map_config.py
        for asset in asset_group.assets:
            try:
                # Get raw data from DAO
                data = ExposureDAO.get_exposure_data(region=region, assets=[asset])

                data = asset.preprocess_geojson_for_display(geojson=data)

                # Process data for display
                if asset.cluster:
                    # This logic is performed for performance. We
                    # want to "cluster" certain assets (if there are a large number of them)
                    # so they don't all display at once. We first must convert all relevant features
                    # to points so that they can be clustered.
                    cluster = asset.cluster
                    clusterToLayer = TRANSPARENT_MARKER_CLUSTER
                    superClusterOptions = asset.superClusterOptions

                else:
                    cluster = False
                    clusterToLayer = None
                    superClusterOptions = False

                # Icon refers to the picture displayed on the map for the point
                if asset.custom_icon is not None:
                    pointToLayer = CREATE_FEATURE_ICON
                else:
                    pointToLayer = None

                # Create GeoJSON layer with style function to use the embedded styles
                style_function = CREATE_FEATURE_COLOR_STYLE

                # Hard code for now
                hover_style_function = arrow_function(
                    dict(weight=5, color="yellow", dashArray="")
                )

                layergroup_child = dl.GeoJSON(
                    id=f"{asset.name}-geojson",
                    data=data,
                    hoverStyle=hover_style_function,
                    style=style_function,
                    cluster=cluster,
                    clusterToLayer=clusterToLayer,
                    superClusterOptions=superClusterOptions,
                    pointToLayer=pointToLayer,
                )

                overlay = dl.Overlay(
                    id=asset.name,
                    name=asset.label,
                    checked=True,
                    children=[dl.LayerGroup(children=[layergroup_child])],
                )

                overlays.append(overlay)
                overlay_names.append(asset.label)

            except Exception as e:
                logger.error(f"Error creating overlay for {asset.name}: {str(e)}")

        return overlays, overlay_names

    @staticmethod
    def get_color_bar(hazard_name: str) -> dl.Colorbar:

        hazard = HazardConfig.get_hazard(hazard_name=hazard_name)

        if not hazard:
            # We return the original placeholder overlay url to serve map tiles
            return None

        # Generate Colorbar to match tiles
        color_bar = dl.Colorbar(
            id=MapConfig.BASE_MAP_COMPONENT["color_bar_layer"]["id"],
            width=MapConfig.BASE_MAP_COMPONENT["color_bar_layer"]["width"],
            colorscale=hazard.geotiff.colormap,
            height=MapConfig.BASE_MAP_COMPONENT["color_bar_layer"]["height"],
            position=MapConfig.BASE_MAP_COMPONENT["color_bar_layer"]["position"],
            min=hazard.min_value,
            max=hazard.max_value,
            unit=hazard.unit,
        )

        return color_bar
