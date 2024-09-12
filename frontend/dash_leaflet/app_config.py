from dash_extensions.javascript import arrow_function
from dash_extensions.javascript import assign

DEFAULT_CLIMATE_VARIABLE = "burntFractionAll" # Climate data to load on app start up

CLIMATE_DATA = {
    "burntFractionAll": {
        "label": r"% of Area that is Covered by Burnt Vegetation",
        "geotiff": {
            "format": "cogs",
            "s3_bucket": "uw-climaterisklab",
            "s3_base_prefix": "climate/CMIP6/ScenarioMIP/burntFractionAll",
            "colormap": "reds",
            "layer_opacity": 0.6
        },
        "available_ssp": ["ssp126", "ssp245", "ssp370", "ssp585"],
        "timescale": "decade-month",
        "unit": "%"
    }

}

MAP_COMPONENT = {
    "id": "map",
    "center": {"lat": 47.0902, "lng": -120.7129},
    "zoom": 7,
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
}

# Pull from open source repo for now
WASHINGTON_STATE_BOUNDARY_GEOJSON_URL = "https://raw.githubusercontent.com/glynnbird/usstatesgeojson/master/washington.geojson"

SUPERCLUSTER = {"radius": 500}
DEFAULT_POINT_ICON_URL = "assets/black-dot.svg"

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
            "categories": ["infrastructure"],
            "osm_types": ["power"],
            "osm_subtypes": ["plant"],
            "hoverStyle": arrow_function(dict(weight=3, color="yellow", dashArray="")),
            "style": {
                "color": "#B0C4DE",
                "weight": 2,
                "fillColor": "#D3D3D3",
                "fillOpacity": 0.8,
            },
            "cluster": False,
            "superClusterOptions": SUPERCLUSTER,
        },
        "geom_types": ["MultiPolygon"],
        "icon": {"create_points": True, "url": "assets/electric.svg"},
    },
    "Power Substations": {
        "Overlay": {
            "name": "Power Substations",
            "id": "power-substation-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-substation-geojson",
            "categories": ["infrastructure"],
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
        "icon": {"create_points": False, "url": DEFAULT_POINT_ICON_URL},
    },
    "Power Lines": {
        "Overlay": {
            "name": "Power Lines",
            "id": "power-line-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-line-geojson",
            "categories": ["infrastructure"],
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
            "categories": ["infrastructure"],
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
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-generator-geojson",
            "categories": ["infrastructure"],
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
        "icon": {"create_points": False, "url": DEFAULT_POINT_ICON_URL},
    },
    "Power Transformers": {
        "Overlay": {
            "name": "Power Transformers",
            "id": "power-transformer-overlay",
            "checked": True,
        },
        "GeoJSON": {
            "id": "power-transformer-geojson",
            "categories": ["infrastructure"],
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
        "icon": {"create_points": False, "url": DEFAULT_POINT_ICON_URL},
    },
}

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

CUSTOM_ICON_TEST = assign(
    """function(feature, latlng){
const custom_icon = L.icon({iconUrl: `assets/power-plant.svg`, iconSize: [15, 15]});
return L.marker(latlng, {icon: custom_icon});
}"""
)