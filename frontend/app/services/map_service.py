"""
This module contains a serice layer for updating map components.
The methods in the MapService class should return dash-leaflet component objects

"""
import logging
import dash_leaflet as dl

from typing import List
from dash import html
from dash_extensions.javascript import arrow_function, assign

from config.hazard_config import HazardConfig
from data.exposure_dao import ExposureDAO
from utils.geo_utils import (
    convert_geojson_feature_collection_to_points,
    create_feature_toolip,
)

from config.map_config import MapConfig, Region
from config.asset_config import TRANSPARENT_MARKER_CLUSTER

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
        default_assets = MapService.get_asset_overlays(region_name=default_region.name)
        asset_layer = dl.LayersControl(
            id=config["asset_layer"]["id"],
            children=default_assets,
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
            center={"lat": default_region.map_center_lat, "lng": default_region.map_center_lon},
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
                    "color": "#000080",
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
    def get_asset_overlays(region_name: str) -> List[dl.Overlay]:
        """Get asset overlays for the specified region

        Args:
            region (str): Region name

        Returns:
            list: List of dl.Overlay components
        """
        overlays = []

        region = MapConfig.get_region(region_name=region_name)

        if not region:
            return list()

        # Iterate through the specified assets available in the region
        # These are are configured manually in config/map_config.py
        for asset in region.available_assets:
            try:
                # Get raw data from DAO
                data = ExposureDAO.get_exposure_data(
                    region=region,
                    assets=[asset]
                )

                data = create_feature_toolip(geojson=data)

                # Process data for display
                if asset.cluster:
                    # This logic is performed for performance. We 
                    # want to "cluster" certain assets (if there are a large number of them)
                    # so they don't all display at once. We first must convert all relevant features
                    # to points so that they can be clustered. 
                    data = convert_geojson_feature_collection_to_points(
                        geojson=data, preserve_types=["LineString"]
                    )
                    cluster = asset.cluster
                    clusterToLayer = TRANSPARENT_MARKER_CLUSTER
                    superClusterOptions = asset.superClusterOptions

                else:
                    cluster = False
                    clusterToLayer = None
                    superClusterOptions = False

                # Icon refers to the picture displayed on the map for the point
                if asset.icon is not None:
                    pointToLayer = asset.icon
                else:
                    pointToLayer = None

                layergroup_child = dl.GeoJSON(
                    id=f"{asset.name}-geojson",
                    data=data,
                    hoverStyle=asset.hoverStyle,
                    style={
                        "color": asset.color,
                        "weight": asset.weight,
                        "fillColor": asset.fill_color,
                        "fillOpacity": asset.fill_opacity,
                    },
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

            except Exception as e:
                logger.error(f"Error creating overlay for {asset.name}: {str(e)}")

        return overlays
    
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