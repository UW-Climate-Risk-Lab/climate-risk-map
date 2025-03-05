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


@dataclass
class Asset:
    name: str
    label: str
    osm_category: str
    osm_types: List[str]
    osm_subtypes: List[str]
    color: str
    weight: float
    fill_color: str
    fill_opacity: float
    hoverStyle: str  # Used in hoverStyle property of leaflet GeoJSON component, should be initialized with arrow_function
    cluster: bool
    superClusterOptions: dict  # Used in superClusterOptions kwarg in leaflet GeoJSON component, example "superClusterOptions": {"radius": 50}
    geom_types: List[str]
    icon: Dict | None = None  # This should be initialized with the assign funcion

class AssetConfig:
    ASSETS = [
        Asset(
            name="power-plant",
            label="Power Plants",
            osm_category="infrastructure",
            osm_types=["power"],
            osm_subtypes=["plant"],
            color="#B0C4DE",
            weight=2.0,
            fill_color="#D3D3D3",
            fill_opactity=0.8,
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
        Asset(
            name="power-transmission-line",
            label="Power Transmission Lines",
            osm_category="infrastructure",
            osm_types=["power"],
            osm_subtypes=["line"],
            color="#4682B4",
            weight=1.5,
            fill_color="#D3D3D3",
            fill_opactity=0.5,
            hoverStyle=arrow_function(dict(weight=5, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        ),
        Asset(
            name="power-distribution-line",
            label="Power Distribution Lines",
            osm_category="infrastructure",
            osm_types=["power"],
            osm_subtypes=["minor_line"],
            color="#4682B4",
            weight=1,
            fill_color="#D3D3D3",
            fill_opactity=0.5,
            hoverStyle=arrow_function(dict(weight=3, color="yellow", dashArray="")),
            cluster=False,
            superClusterOptions={"radius": 500},
            geom_types=["LineString"],
            icon=None,
        ),
        Asset(
            name="power-substation",
            label="Power Substations",
            osm_category="infrastructure",
            osm_types=["power"],
            osm_subtypes=["substation"],
            color="#696969",
            weight=2,
            fill_color="#5F9EA0",
            fill_opactity=0.5,
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
    ]

    @classmethod
    def get_asset(cls, name):
        """Returns asset object 

        Args:
            name (str): Name of asset
        """
        for asset in cls.ASSETS:
            if asset.name == name:
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
