"""
Map Configuration for the Climate Risk map

This module adds the configuration for the Map component of the dash-leaflet app.
This also defines the available regions that can be selected for the map. By centralizing
this configuration, we can add additional regions and update the map setings all in one place.

"""
import logging

from dataclasses import dataclass

from typing import List

from config.settings import ASSETS_PATH
from config.exposure import Asset, get_asset

logger = logging.getLogger(__name__)

@dataclass
class Region:
    name: str
    label: str  # Used for display in the UI
    dbname: str  # database name where region asset data lives
    map_center_lat: float
    map_center_lon: float
    map_zoom: int
    geojson: str  # path to geojson with region shape
    available_assets: List[Asset]
    available_download: bool  # flag if region should be available on the map


class MapConfig:
    """Map-specific configuration"""

    # Base map settings for dash-leaflet component
    # Decided to keep as separate config dictionary
    BASE_MAP_COMPONENT = {
        "id": "map",
        "style": {"height": "100vh"},
        "preferCanvas": True,
        "default_region_name": "usa",
        "base_map_layer": {
            "id": "base-map-layer",
            "url": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
            "attribution": '&copy; <a href="https://carto.com/attributions">CARTO</a>',
        },
        "drawn_shapes_layer": {
            "id": "drawn-shapes-layer",
            "draw": {
                "rectangle": True,
                "circle": False,
                "polygon": False,
                "circlemarker": False,
                "polyline": False,
                "marker": False,
            },
            "edit": False,
        },
        "color_bar_layer": {
            "id": "color-bar-layer",
            "parent_div_id": "color-bar-layer-div",
            "width": 20,
            "height": 150,
            "position": "bottomleft",
        },
        "hazard_tile_layer": {
            "id": "hazard-tile-layer",
            "placeholder_url": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
            "placeholder_opacity": 1,
        },
        "asset_layer": {"id": "asset-layer"},
        "viewport": {"transition": "flyTo"}
    }

    REGIONS = [
        Region(
            name="washington",
            label="Washington",
            dbname="washington",
            map_center_lat=47.0902,
            map_center_lon=-120.7129,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/washington.geojson",
            available_assets=[
                get_asset("osm-power-plant"),
                get_asset("osm-power-transmission-line"),
                get_asset("osm-power-distribution-line"),
                get_asset("osm-power-substation"),
            ],
            available_download=True
        ),
        Region(
            name="new-york",
            label="New York",
            dbname="new-york",
            map_center_lat=42.7118,
            map_center_lon=-75.0071,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/new-york.geojson",
            available_assets=[
                get_asset("osm-power-plant"),
                get_asset("osm-power-transmission-line"),
                get_asset("osm-power-distribution-line"),
                get_asset("osm-power-substation"),
            ],
            available_download=False
        ),
        Region(
            name="usa",
            label="United States",
            dbname=None,
            map_center_lon=-98.5795,
            map_center_lat=39.8283,
            map_zoom=4,
            geojson=ASSETS_PATH + "/geojsons/regions/usa.geojson",
            available_assets=[get_asset("hifld-power-transmission-line")],
            available_download=False
        )
    ]

    @classmethod
    def get_region(cls, region_name: str) -> Region:
        for region in cls.REGIONS:
            if region.name == region_name:
                return region
        logger.error(f"Region '{region_name}' that was requested is not configured")
        return None
