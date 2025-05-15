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
from config.exposure import AssetGroup, get_asset_group

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
    available_asset_groups: List[AssetGroup]
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
            "placeholder_opacity": 0,
        },
        "asset_layer": {"id": "asset-layer"},
        "viewport": {"transition": "flyTo"},
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
            available_asset_groups=[
                get_asset_group("power-grid"),
                get_asset_group("data-infrastructure"),
                get_asset_group("commercial-real-estate"),
                get_asset_group("agriculture")
            ],
            available_download=True,
        ),
        Region(
            name="new-york",
            label="New York",
            dbname="new_york",
            map_center_lat=42.7118,
            map_center_lon=-75.0071,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/new_york.geojson",
            available_asset_groups=[get_asset_group("power-grid"),
                                    get_asset_group("data-infrastructure"),
                                    get_asset_group("commercial-real-estate"),
                                    get_asset_group("agriculture")],
            available_download=True,
        ),
        Region(
            name="oregon",
            label="Oregon",
            dbname="oregon",
            map_center_lat=43.8,
            map_center_lon=-120.8,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/oregon.geojson",
            available_asset_groups=[get_asset_group("power-grid"),
                                    get_asset_group("data-infrastructure"),
                                    get_asset_group("commercial-real-estate"),
                                    get_asset_group("agriculture")],
            available_download=True,
        ),
        Region(
            name="california",
            label="California",
            dbname="california",
            map_center_lat=37.3,
            map_center_lon=-120.15,
            map_zoom=6,
            geojson=ASSETS_PATH + "/geojsons/regions/california.geojson",
            available_asset_groups=[get_asset_group("power-grid"),
                                    get_asset_group("data-infrastructure"),
                                    get_asset_group("commercial-real-estate"),
                                    get_asset_group("agriculture")],
            available_download=True,
        ),
        Region(
            name="texas",
            label="Texas",
            dbname="texas",
            map_center_lat=31.77,
            map_center_lon=-100.1,
            map_zoom=6,
            geojson=ASSETS_PATH + "/geojsons/regions/texas.geojson",
            available_asset_groups=[get_asset_group("power-grid"),
                                    get_asset_group("data-infrastructure"),
                                    get_asset_group("commercial-real-estate"),
                                    get_asset_group("agriculture")],
            available_download=True,
        ),
        Region(
            name="florida",
            label="Florida",
            dbname="florida",
            map_center_lat=28.5,
            map_center_lon=-81.7,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/florida.geojson",
            available_asset_groups=[get_asset_group("power-grid"),
                                    get_asset_group("data-infrastructure"),
                                    get_asset_group("commercial-real-estate"),
                                    get_asset_group("agriculture")],
            available_download=True,
        ),
        Region(
            name="usa",
            label="United States",
            dbname=None,
            map_center_lon=-98.5795,
            map_center_lat=39.8283,
            map_zoom=4,
            geojson=ASSETS_PATH + "/geojsons/regions/usa.geojson",
            available_asset_groups=[get_asset_group("hifld-high-voltage-power-grid")],
            available_download=False,
        ),
        Region(
            name="south-korea",
            label="South Korea",
            dbname="south_korea",
            map_center_lon=128.0,
            map_center_lat=36.5,
            map_zoom=7,
            geojson=ASSETS_PATH + "/geojsons/regions/south_korea.geojson",
            available_asset_groups=[
                get_asset_group("power-grid"),
                get_asset_group("data-infrastructure"),
                get_asset_group("commercial-real-estate"),
                get_asset_group("agriculture")
            ],
            available_download=True,
        ),
        Region(
            name="japan",
            label="Japan",
            dbname="japan",
            map_center_lon=138.25,
            map_center_lat=36.204,
            map_zoom=5,
            geojson=ASSETS_PATH + "/geojsons/regions/japan.geojson",
            available_asset_groups=[
                get_asset_group("power-grid"),
                get_asset_group("data-infrastructure"),
                get_asset_group("commercial-real-estate"),
            ],
            available_download=False,
        ),
    ]

    @classmethod
    def get_region(cls, region_name: str) -> Region:
        for region in cls.REGIONS:
            if region.name == region_name:
                return region
        logger.error(f"Region '{region_name}' that was requested is not configured")
        return None
