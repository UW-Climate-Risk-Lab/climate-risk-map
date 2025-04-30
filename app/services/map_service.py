"""
This module contains a serice layer for updating map components.
The methods in the MapService class should return dash-leaflet component objects

"""

import logging
import base64
import dash_leaflet as dl
import dash_leaflet.express as dlx
import time
import os

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
from utils import file_utils
from config.settings import ASSETS_PATH

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
            preferCanvas=config["preferCanvas"]
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
    def get_asset_overlays(
        asset_group_name: str, region_name: str
    ) -> Tuple[List[dl.Overlay], List[str]]:
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
        start_time_total = time.time()
        overlays = []
        overlay_names = []

        start_time = time.time()
        asset_group = get_asset_group(name=asset_group_name)
        region = MapConfig.get_region(region_name=region_name)
        logger.debug(f"Time to get asset group and region: {time.time() - start_time:.4f}s")

        if not asset_group:
            return list(), list()

        # Iterate through the specified assets so that each asset gets its own layer
        # These are are configured manually in config/map_config.py
        for asset in asset_group.assets:
            asset_start_time = time.time()
            logger.debug(f"Processing asset: {asset.name}")
            
            try:
                # Check for cached data before retrieving from DAO
                cache_check_time = time.time()
                use_cache = file_utils.cache_exists(region, asset, ASSETS_PATH)
                logger.debug(f"  Time to check cache: {time.time() - cache_check_time:.4f}s")
                
                if use_cache:
                    logger.info(f"Using cached geobuf data for {asset.name} in {region.name}")
                    data_url = file_utils.get_asset_cache_url(region, asset, ASSETS_PATH)
                    use_url = True
                    data = None
                else:
                    # Get raw data from DAO
                    dao_start_time = time.time()
                    data = ExposureDAO.get_exposure_data(region=region, assets=[asset])
                    logger.debug(f"  Time to get exposure data: {time.time() - dao_start_time:.4f}s")

                    # Process data for display
                    preprocess_start_time = time.time()
                    data = asset.preprocess_geojson_for_display(geojson=data)
                    logger.debug(f"  Time to preprocess geojson: {time.time() - preprocess_start_time:.4f}s")
                    use_url = False

                # Configure display properties
                config_start_time = time.time()
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
                logger.debug(f"  Time to configure display properties: {time.time() - config_start_time:.4f}s")

                # Create layer components
                component_start_time = time.time()
                
                if use_url:
                    # Use URL to the cached geobuf file
                    layergroup_child = dl.GeoJSON(
                        id=f"{asset.name}-geojson",
                        url=data_url,
                        format="geobuf",
                        hoverStyle=hover_style_function,
                        style=style_function,
                        cluster=cluster,
                        clusterToLayer=clusterToLayer,
                        superClusterOptions=superClusterOptions,
                        pointToLayer=pointToLayer,
                    )
                else:
                    # Convert to geobuf and cache for future use
                    geobuf_start_time = time.time()
                    geobuf = dlx._try_import_geobuf()
                    encoded_data = base64.b64encode(geobuf.encode(data, 3)).decode()
                    logger.debug(f"  Time to encode geobuf: {time.time() - geobuf_start_time:.4f}s")
                    
                    # Cache the data for future requests
                    cache_start_time = time.time()
                    file_utils.write_geobuf_to_cache(region, asset, encoded_data, ASSETS_PATH)
                    logger.debug(f"  Time to write to cache: {time.time() - cache_start_time:.4f}s")
                    
                    layergroup_child = dl.GeoJSON(
                        id=f"{asset.name}-geojson",
                        data=encoded_data,
                        format="geobuf",
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
                logger.debug(f"  Time to create components: {time.time() - component_start_time:.4f}s")

                overlays.append(overlay)
                overlay_names.append(asset.label)
                logger.debug(f"  Total time for asset {asset.name}: {time.time() - asset_start_time:.4f}s")

            except Exception as e:
                logger.error(f"Error creating overlay for {asset.name}: {str(e)}")

        logger.debug(f"Total time for get_asset_overlays: {time.time() - start_time_total:.4f}s")
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
