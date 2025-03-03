from dash_extensions.javascript import arrow_function
from dash_extensions.javascript import assign

DEFAULT_CLIMATE_VARIABLE = "fwi"  # Climate data to load on app start up

CLIMATE_DATA = {
    "fwi": {
        "label": r"Fire Weather Index (NASA NEX GDDP Ensemble Mean)",
        "statistical_measure": "ensemble_mean",
        "unit": "",
        "min_value": 0,
        "max_value": 50,
        "geotiff": {
            "format": "cogs",
            "s3_bucket": "uw-crl",
            "s3_base_prefix": "climate-risk-map/frontend/NEX-GDDP-CMIP6/fwi",
            "colormap": "ylorbr",
            "layer_opacity": 0.6,
        },
        "available_ssp": ["ssp126", "ssp245", "ssp370", "ssp585"],
    }
}

STATES = {
    "available_states": {
        "washington": {
            "map_center": {"lat": 47.0902, "lng": -120.7129},
            "map_zoom": 7,
            "label": "Washington",
        },
        "new-york": {
            "map_center": {"lat": 42.7118, "lng": -75.0071},
            "map_zoom": 7,
            "label": "New York",
        },
    },
}

MAP_COMPONENT = {
    "id": "map",
    "center": {"lat": 39.8283, "lng": -98.5795},
    "zoom": 4,
    "style": {"height": "100vh"},
    "preferCanvas": True,
    "base_map": {
        "id": "base-map-layer",
        "url": "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        "attribution": '&copy; <a href="https://carto.com/attributions">CARTO</a>',
    },
    "drawn_shapes_component": {
        "id": "drawn-shapes",
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
    "color_bar": {
        "id": "color-bar",
        "width": 20,
        "height": 150,
        "position": "bottomleft",
    },
    "default_state": "usa",
}

SUPERCLUSTER = {"radius": 500}
DEFAULT_POINT_ICON_URL = "assets/icons/black-dot.svg"

# Main keys should be the same as [Overlay][name]!
POWER_GRID_LAYERS = {
    "Power Plants": {
        "Overlay": {
            "name": "Power Plants",
            "id": "power-plant-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-plant-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["plant"],
            "hoverStyle": arrow_function(dict(weight=3, color="yellow", dashArray="")),
            "style": {
                "color": "#B0C4DE",
                "weight": 2,
                "fillColor": "#D3D3D3",
                "fillOpacity": 0.8,
            },
            "cluster": True,
            "superClusterOptions": {"radius": 50},
        },
        "geom_types": ["MultiPolygon"],
        "icon": assign(
            """
            function(feature, latlng){
                const custom_icon = L.icon({iconUrl: `assets/icons/power-plant.svg`, iconSize: [15, 15]});
                return L.marker(latlng, {icon: custom_icon});
            }
            """
        ),
    },
    "Power Lines": {
        "Overlay": {
            "name": "Power Lines",
            "id": "power-line-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-line-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["line"],
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#4682B4",
                "weight": 1.5,
                "fillColor": "#A9A9A9",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["LineString"],
        "icon": None,
    },
    "Power Cables": {
        "Overlay": {
            "name": "Power Cables",
            "id": "power-cable-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-cable-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["cable"],
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#483D8B",
                "weight": 1,
                "fillColor": "#C4C3D0",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["LineString"],
        "icon": None,
    },
    "Power Generators": {
        "Overlay": {
            "name": "Power Generators",
            "id": "power-generator-overlay",
            "checked": False,
        },
        "GeoJSON": {
            "id": "power-generator-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["generator"],
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#000080",
                "weight": 2,
                "fillColor": "##000080",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["MultiPolygon", "Point"],
        "icon": assign(
            """
            function(feature, latlng){
                const custom_icon = L.icon({iconUrl: `assets/icons/black-dot.svg`, iconSize: [15, 15]});
                return L.marker(latlng, {icon: custom_icon});
            }
            """,
        )
    },
    "Power Transformers": {
        "Overlay": {
            "name": "Power Transformers",
            "id": "power-transformer-overlay",
            "checked": False,
        },
        "GeoJSON": {
            "id": "power-transformer-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["transformer"],
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#708090",
                "weight": 2,
                "fillColor": "#B0C4DE",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["MultiPolygon", "Point"],
        "icon": assign(
            """
            function(feature, latlng){
                const custom_icon = L.icon({iconUrl: `assets/icons/black-dot.svg`, iconSize: [15, 15]});
                return L.marker(latlng, {icon: custom_icon});
            }
            """,
        ),
    },
    "Power Substations": {
        "Overlay": {
            "name": "Power Substations",
            "id": "power-substation-overlay",
            "checked": False,
        },
        "GeoJSON": {
            "id": "power-substation-geojson",
            "category": "infrastructure",
            "osm_types": ["power"],
            "osm_subtypes": ["substation"],
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#696969",
                "weight": 2,
                "fillColor": "#5F9EA0",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["MultiPolygon", "Point"],
        "icon": assign(
            """
            function(feature, latlng){
                const custom_icon = L.icon({iconUrl: `assets/icons/black-dot.svg`, iconSize: [15, 15]});
                return L.marker(latlng, {icon: custom_icon});
            }
            """,
        ),
    },
}

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
