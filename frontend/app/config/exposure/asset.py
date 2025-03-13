"""
The term "asset" here is used to represent anything that can give exposure in climate risk.
This can range from infrastructure, buildings, administrative areas (like counties or census blocks),
or natural features.

Assets will generally be vector features, having geometry properties.

"""

from dataclasses import dataclass, field

from typing import List, Dict, Optional

from dash_extensions.javascript import arrow_function
from dash_extensions.javascript import assign

from config.exposure.definitions import load_asset_definitions
from config.exposure.transformer import DataTransformer


class AssetRegistry:
    _asset_types = {}

    @classmethod
    def register(cls, name):
        def decorator(asset_class):
            cls._asset_types[name] = asset_class
            return asset_class

        return decorator

    @classmethod
    def create(cls, asset_type, **kwargs):
        if asset_type not in cls._asset_types:
            raise ValueError(f"Unknown asset type: {asset_type}")
        return cls._asset_types[asset_type](**kwargs)


class AssetFactory:
    @staticmethod
    def create_from_definitions():
        """Create assets from definitions."""
        asset_definitions = load_asset_definitions()
        assets = []

        for name, config in asset_definitions.items():
            asset_type = config.pop("type")
            asset = AssetRegistry.create(asset_type, name=name, **config)
            assets.append(asset)

        return assets


@dataclass
class Asset:
    name: str
    label: str
    style: Dict
    custom_color: Dict | None
    cluster: bool
    superClusterOptions: Dict | bool
    geom_types: List[str]
    icon_path: str | None
    data_transformations: List[str] | None

    def preprocess_geojson_for_display(self, geojson: Dict) -> Dict:
        """Processes GeoJSON containing data for asset type of instance

        This is done to add special properties and values used by Dash Leaflet
        in the visual display of GeoJSON data. The geojson passed in is assumed
        to contain ONLY assets of type self. This will access asset level hard coded configurations
        found in asset_definitions.py.

        """

        # OpenStreetMap data has special property 'tags',
        # which is a json of the feature properties. This must be run first

        for feature in geojson.get("features", []):
            if isinstance(self, OpenStreetMapAsset):
                feature = self._parse_tags(feature=feature)

            # Apply data transformations
            feature = DataTransformer.apply(
                data=feature, transformations=self.data_transformations
            )

            # Apply styling
            feature = self._apply_feature_colors(feature=feature)

            # Apply icon url
            feature = self._create_feature_icon_path(feature=feature)

            # Create tooltips
            feature = self._create_feature_toolip(feature=feature)

            # Converts geometry to point for clustering 
            # (this improves performance when displaying points on dash-leaflet)
            if self.cluster:
                feature = self._convert_feature_to_point(feature=feature)

        return geojson

    def _create_feature_icon_path(self, feature: Dict):
        """Adds icon path to each feature

        Args:
            feature (Dict): GeoJSON feature
        """
        if self.icon_path is None:
            return feature

        feature["icon_path"] = self.icon_path

        return feature

    def _apply_feature_colors(self, feature: Dict) -> Dict:
        """
        Preprocesses a GeoJSON feature to include style information based on property values.

        Args:
            feature (Dict): GeoJSON feature collection

        Returns:
            Dict: Feature with a style key added
        """
        # Make copy here to avoid pointing to instance-level property
        default_style = self.style.copy()
        # Add style information to feature properties
        if "style" not in feature.keys():
            feature["style"] = default_style.copy()

        if not self.custom_color:
            # No dynamic coloring configuration, return original
            return feature

        # Get property value
        prop_value = feature.get("properties", {}).get(self.custom_color["property"])
        if prop_value is None:
            return feature

        # Determine color based on property value
        custom_color_code = None

        # For numeric properties with ranges
        if self.custom_color["ranges"] and isinstance(prop_value, (int, float)):
            for range_def in self.custom_color["ranges"]:
                min_val = range_def.get("min", float("-inf"))
                max_val = range_def.get("max", float("inf"))
                if min_val <= prop_value < max_val:
                    custom_color_code = range_def.get("color", self.style["color"])
                    break

        # For categorical properties
        elif self.custom_color["categories"] and isinstance(prop_value, str):
            custom_color_code = self.custom_color["categories"].get(
                prop_value, self.style["color"]
            )
        if custom_color_code:
            feature["style"]["color"] = custom_color_code
            feature["style"]["fillColor"] = custom_color_code

        return feature

    def _create_feature_toolip(self, feature: Dict):
        """Creates a property called "tooltip". OSM data stores
        the feature specific properties in a single object called "tags".

        The tooltip property is automatically displayed
        by dash leaflet as a popup when the mouse hover over the feature

        Args:
            feature (dict): Dict of GeoJson feature
        """

        tooltip_str = ""

        # "tags" is a special property that relates to OpenStreetMap features

        if "tags" in feature["properties"].keys():
            for key, value in feature["properties"]["tags"].items():
                tooltip_str = tooltip_str + f"<b>{str(key)}<b>: {str(value)}<br>"
        else:
            for key, value in feature["properties"].items():
                tooltip_str = tooltip_str + f"<b>{key}<b>: {value}<br>"

        feature["properties"]["tooltip"] = tooltip_str
        return feature

    def _convert_feature_to_point(self, feature: Dict, preserve_types: List[str] = ["LineString"]) -> Dict:
        """
        This function processes a feature and converts each feature's geometry
        to a Point. It assumes that the centroid latitude and longitude are present in the
        feature's properties under the keys 'latitude' and 'longitude'. If a feature's geometry
        is already a Point, it retains the original coordinates. Other properties and keys,
        such as 'id', are preserved.

        Args:
            feature (Dict): A dictionary representing a GeoJSON Feature.
            preserve_types (List[str], optional): Geometry types to skip converting to points, e.g, 'LineString'
        Returns:
            Dict: A GeoJSON feature with all features converted to Point geometries.
        """

        # Only update if the geometry is not already a Point.
        if (
            (feature["geometry"]["type"] != "Point")
            and (feature["geometry"]["type"] not in preserve_types)
            and ("longitude" in feature["properties"].keys())
            and ("latitude" in feature["properties"].keys())
        ):
            feature["geometry"]["coordinates"] = [
                feature["properties"].get("longitude"),
                feature["properties"].get("latitude"),
            ]
            feature["geometry"]["type"] = "Point"

        return feature


@AssetRegistry.register("OpenStreetMap")
@dataclass
class OpenStreetMapAsset(Asset):
    osm_category: str
    osm_type: str
    osm_subtype: str

    def _parse_tags(self, feature: Dict):

        for key, value in feature["properties"].get("tags", []).items():
            feature["properties"][key] = value
        return feature


@AssetRegistry.register("HIFLD")
@dataclass
class HifldAsset(Asset):
    geojson_path: str


class AssetConfig:
    # Load assets dynamically
    ASSETS = AssetFactory.create_from_definitions()

    @classmethod
    def get_asset(cls, name):
        """Returns asset object by name or label."""
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

CREATE_FEATURE_ICON = assign(
    """
    function(feature, latlng){
    const custom_icon = L.icon({iconUrl: feature.icon_path, iconSize: [15, 15]});
    return L.marker(latlng, {icon: custom_icon})
    }
    """
)

CREATE_FEATURE_COLOR_STYLE = assign(
    """
    function(feature) {
        return feature.style || {
            color: feature.properties.style.color,
            weight: feature.properties.style.weight,
            fillColor: feature.properties.style.fillColor,
            fillOpacity: feature.properties.style.fillOpacity
        };
    }
    """
)
