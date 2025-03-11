"""
The term "asset" here is used to represent anything that can give exposure in climate risk.
This can range from infrastructure, buildings, administrative areas (like counties or census blocks),
or natural features.

Assets will generally be vector features, having a geometry properties.

"""

from dataclasses import dataclass

from typing import List, Dict

from dash_extensions.javascript import arrow_function
from dash_extensions.javascript import assign

from config.settings import ASSETS_PATH


@dataclass
class Asset:
    name: str
    label: str
    default_color: str
    default_weight: float
    default_fillColor: str
    default_fillOpacity: float
    color_property: str | None
    color_ranges: List[Dict] | None
    color_categories: Dict | None
    hoverStyle: str  # Used in hoverStyle property of leaflet GeoJSON component, should be initialized with arrow_function
    cluster: bool
    superClusterOptions: dict  # Used in superClusterOptions kwarg in leaflet GeoJSON component, example "superClusterOptions": {"radius": 50}
    geom_types: List[str]
    icon: (
        Dict | None
    )  # This should be initialized with the assign funcion if you are using a custom icon

    def apply_style_to_geojson(self, geojson: Dict) -> Dict:
        """
        Preprocesses GeoJSON features to include style information for each feature based on property values.

        Args:
            geojson (Dict): GeoJSON feature collection

        Returns:
            Dict: GeoJSON with style properties added to each feature
        """
        if not self.color_property or (
            not self.color_ranges and not self.color_categories
        ):
            # No dynamic coloring configuration, return original
            for feature in geojson.get("features", []):
                if "style" not in feature:
                    feature["style"] = {}

                feature["style"]["color"] = self.default_color
                feature["style"]["fillColor"] = self.default_color

                # Setting these as default for now, may be dynamic in the future
                feature["style"]["weight"] = self.default_weight
                feature["style"]["fillOpacity"] = self.default_fillOpacity

            return geojson

        for feature in geojson.get("features", []):
            # Get property value
            prop_value = feature.get("properties", {}).get(self.color_property)
            if prop_value is None:
                continue

            # Determine color based on property value
            color = self.default_color

            # For numeric properties with ranges
            if self.color_ranges and isinstance(prop_value, (int, float)):
                for range_def in self.color_ranges:
                    min_val = range_def.get("min", float("-inf"))
                    max_val = range_def.get("max", float("inf"))
                    if min_val <= prop_value < max_val:
                        color = range_def.get("color", self.default_color)
                        break

            # For categorical properties
            elif self.color_categories and isinstance(prop_value, str):
                color = self.color_categories.get(prop_value, self.default_color)

            # Add style information to feature properties
            if "style" not in feature:
                feature["style"] = {}

            feature["style"]["color"] = color
            feature["style"]["fillColor"] = color

            # Setting these as default for now, may be dynamic in the future
            feature["style"]["weight"] = self.default_weight
            feature["style"]["fillOpacity"] = self.default_fillOpacity

        return geojson


@dataclass
class OpenStreetMapAsset(Asset):
    osm_category: str
    osm_type: str
    osm_subtype: str


@dataclass
class HifldAsset(Asset):
    geojson_path: str


class AssetConfig:

    # Default color of asset to display
    DEFAULT_COLOR = "#6a6a6a"

    ASSETS = [
        OpenStreetMapAsset(
            name="osm-power-plant",
            label="Power Plants",
            osm_category="infrastructure",
            osm_type="power",
            osm_subtype="plant",
            default_color=DEFAULT_COLOR,
            default_weight=2.0,
            default_fillColor=DEFAULT_COLOR,
            default_fillOpacity=0.8,
            color_property=None,
            color_ranges=None,
            color_categories=None,
            hoverStyle=arrow_function(dict(weight=3, color="yellow", dashArray="")),
            cluster=True,
            superClusterOptions={"radius": 50},
            geom_types=["MultiPolygon"],
            icon=assign(
                """
                function(feature, latlng){
                    const custom_icon = L.icon({iconUrl: `assets/icons/power-plant.svg`, iconSize: [15, 15]});
                    return L.marker(latlng, {icon: custom_icon});
                }
                """
            ),
        ),
        OpenStreetMapAsset(
            name="osm-power-transmission-line",
            label="Power Transmission Lines",
            osm_category="infrastructure",
            osm_type="power",
            osm_subtype="line",
            default_color=DEFAULT_COLOR,
            default_weight=1.5,
            default_fillColor=DEFAULT_COLOR,
            default_fillOpacity=0.5,
            color_property=None,
            color_ranges=None,
            color_categories=None,
            hoverStyle=arrow_function(dict(weight=5, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        ),
        OpenStreetMapAsset(
            name="osm-power-distribution-line",
            label="Power Distribution Lines",
            osm_category="infrastructure",
            osm_type="power",
            osm_subtype="minor_line",
            default_color=DEFAULT_COLOR,
            default_weight=1,
            default_fillColor=DEFAULT_COLOR,
            default_fillOpacity=0.5,
            color_property=None,
            color_ranges=None,
            color_categories=None,
            hoverStyle=arrow_function(dict(weight=3, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        ),
        OpenStreetMapAsset(
            name="osm-power-substation",
            label="Power Substations",
            osm_category="infrastructure",
            osm_type="power",
            osm_subtype="substation",
            default_color=DEFAULT_COLOR,
            default_weight=2,
            default_fillColor=DEFAULT_COLOR,
            default_fillOpacity=0.5,
            color_property=None,
            color_ranges=None,
            color_categories=None,
            hoverStyle=arrow_function(dict(weight=5, color="yellow", dashArray="")),
            cluster=True,
            superClusterOptions={"radius": 500},
            geom_types=["Multipolygon", "Point"],
            icon=assign(
                """
                function(feature, latlng){
                    const custom_icon = L.icon({iconUrl: `assets/icons/black-dot.svg`, iconSize: [15, 15]});
                    return L.marker(latlng, {icon: custom_icon});
                }
                """
            ),
        ),
        HifldAsset(
            geojson_path=ASSETS_PATH
            + "/geojsons/hifld/hifld-in-service-high-voltage-power-transmission-line.geojson",
            name="hifld-power-transmission-line",
            label="Power Transmission Lines (345+ kV)",
            default_color=DEFAULT_COLOR,
            default_weight=1.0,
            default_fillColor=DEFAULT_COLOR,
            default_fillOpacity=0.5,
            color_property="VOLTAGE",
            color_ranges=[
                {"min": 0, "max": 100, "color": "#1a9641", "label": "< 100 kV"},
                {"min": 100, "max": 300, "color": "#a6d96a", "label": "100-300 kV"},
                {"min": 300, "max": 500, "color": "#ffffbf", "label": "300-500 kV"},
                {"min": 500, "max": 700, "color": "#fdae61", "label": "500-700 kV"},
                {
                    "min": 700,
                    "max": float("inf"),
                    "color": "#d7191c",
                    "label": "> 700 kV",
                },
            ],
            color_categories=None,
            hoverStyle=arrow_function(dict(weight=5, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        ),
    ]

    @classmethod
    def get_asset(cls, name):
        """Returns asset object

        Args:
            name (str): Name or label of asset
        """
        for asset in cls.ASSETS:
            if (asset.name == name) or (asset.label == name):
                return asset
        return None


# Javascript code to create a transparent cluster icon
TRANSPARENT_MARKER_CLUSTER = assign(
    """function(feature, latlng, index, context){
    const scatterIcon = L.DivIcon.extend({
        createIcon: function(oldIcon) {
               let icon = L.DivIcon.prototype.createIcon.call(this, oldIcon);
               icon.style.backgroundColor = this.options.color;
               return icon;
        }
    })                      
    // Render a circle with the number of leaves written in the center.
    const icon = new scatterIcon({
        html: '<div style="background-color:rgba(255, 255, 255, 0);"><span>' + '</span></div>',
        className: "marker-cluster",
        iconSize: L.point(40, 40),
    });
    return L.marker(latlng, {icon : icon})
}"""
)
