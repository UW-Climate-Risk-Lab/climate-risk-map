# services/map_service.py
import logging
import dash_leaflet as dl

from typing import List
from dash import html
from dash_extensions.javascript import arrow_function, assign
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

        # Base map layer
        base_map_layer = dl.TileLayer(
            id=config["base_map"]["id"],
            url=config["base_map"]["url"],
            attribution=config["base_map"]["attribution"],
        )

        # Drawing tools component
        # Used for drawing bounding boxes
        drawn_shapes_layer = dl.FeatureGroup(
            [
                dl.EditControl(
                    id=config["drawn_shapes_component"]["id"],
                    draw=config["drawn_shapes_component"]["draw"],
                    edit=config["drawn_shapes_component"]["edit"],
                )
            ]
        )

        # Placeholder for climate hazard raster data.
        # When hazard data is selected,, url can be updated
        hazard_tile_layer = dl.TileLayer(
            id=config["hazard-tile-layer"]["id"],
            url=config["hazard-tile-layer"]["placeholder_url"],
            opacity=config["hazard-tile-layer"]["placeholder_opacity"],
        )

        # Layer control for toggling asset/exposure features
        asset_layer = dl.LayersControl(
            id=config["asset_layer"]["id"],
            children=list(),  # Empty list initially
        )

        # State outline overlay
        state_outline_overlay = MapService.get_region_overlay(
            state_geojson_path=config["default_region_overlay_path"], z_index=300
        )

        # Color bar (initially hidden)
        color_bar_layer = html.Div(
            id=config["color_bar_layer"]["id"] + "-div",
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
            center=config["center"],
            zoom=config["zoom"],
            style=config["style"],
            id=config["id"],
            preferCanvas=config["preferCanvas"],
        )

        return map_component

    @staticmethod
    def get_region_overlay(region_geojson_path, z_index) -> dl.Pane:
        """Get a region outline overlay from geojson

        Args:
            region (str): Region name (lowercase)
            z_index (int): Z-index for layer ordering

        Returns:
            dl.Pane: Pane component containing GeoJSON region outline
        """
        layer = dl.Pane(
            dl.GeoJSON(
                url=region_geojson_path,
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
    def get_asset_overlays(region: Region) -> List[dl.Overlay]:
        """Get asset overlays for the specified region

        Args:
            region (str): Region name

        Returns:
            list: List of dl.Overlay components
        """
        overlays = []

        # Iterate through the specified assets available in the region
        # These are are configured manually in config/map_config.py
        for asset in region.available_assets:
            try:
                # Get raw data from DAO
                data = ExposureDAO.get_exposure_data(
                    region=region,
                    category=asset.osm_category,
                    osm_types=asset.osm_types,
                    osm_subtypes=asset.osm_subtypes,
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
                    id=f"{asset.name}-overlay",
                    name=asset.label,
                    checked=True,
                    children=[dl.LayerGroup(children=[layergroup_child])],
                )

                overlays.append(overlay)

            except Exception as e:
                logger.error(f"Error creating overlay for {asset.name}: {str(e)}")

        return overlays
