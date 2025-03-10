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
    color: str
    weight: float
    fill_color: str
    fill_opacity: float
    hoverStyle: str  # Used in hoverStyle property of leaflet GeoJSON component, should be initialized with arrow_function
    cluster: bool
    superClusterOptions: dict  # Used in superClusterOptions kwarg in leaflet GeoJSON component, example "superClusterOptions": {"radius": 50}
    geom_types: List[str]
    icon: Dict | None # This should be initialized with the assign funcion if icon is available

@dataclass
class OpenStreetMapAsset(Asset):
    osm_category: str
    osm_type: str
    osm_subtype: str

@dataclass
class HifldAsset(Asset):
    geojson_path: str

class AssetConfig:
    ASSETS = [
        OpenStreetMapAsset(
            name="osm-power-plant",
            label="Power Plants",
            osm_category="infrastructure",
            osm_type="power",
            osm_subtype="plant",
            color="#B0C4DE",
            weight=2.0,
            fill_color="#D3D3D3",
            fill_opacity=0.8,
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
            color="#4682B4",
            weight=1.5,
            fill_color="#D3D3D3",
            fill_opacity=0.5,
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
            color="#4682B4",
            weight=1,
            fill_color="#D3D3D3",
            fill_opacity=0.5,
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
            color="#696969",
            weight=2,
            fill_color="#5F9EA0",
            fill_opacity=0.5,
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
            geojson_path=ASSETS_PATH + "/geojsons/hifld/hifld-in-service-high-voltage-power-transmission-line.geojson",
            name="hifld-power-transmission-line",
            label="Power Transmission Lines (345+ kV)",
            color="#4682B4",
            weight=1.0,
            fill_color="#D3D3D3",
            fill_opacity=0.5,
            hoverStyle=arrow_function(dict(weight=5, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        )
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
