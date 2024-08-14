from dash_extensions.javascript import arrow_function

COLORMAP = "reds"
CLIMATE_LAYER_OPACITY = 0.6
SUPER_CLUSTER_RADIUS = 50

# Pull from open source repo for now
WASHINGTON_STATE_BOUNDARY_GEOJSON_URL = "https://raw.githubusercontent.com/glynnbird/usstatesgeojson/master/washington.geojson"

# Main keys should be the same as [Overlay][name]
INFRASTRUCTURE_LAYERS = {
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
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
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
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
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
                "weight": 1,
                "fillColor": "#A9A9A9",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
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
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
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
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS},
        },
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
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": True,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
    },
}

