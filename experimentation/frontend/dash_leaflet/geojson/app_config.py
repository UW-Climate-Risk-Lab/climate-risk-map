from dash_extensions.javascript import arrow_function

COLORMAP = "reds"
CLIMATE_LAYER_OPACITY = 0.6
SUPER_CLUSTER_RADIUS = 50

INFRASTRUCTURE_LAYERS = {
    "power_plant": {
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
            "hoverStyle": arrow_function(dict(weight=5, color="yellow", dashArray="")),
            "style": {
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
    },
    "power_substation": {
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
    "power_line": {
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
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
    },
    "power_cable": {
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
                "color": "#008000",
                "weight": 2,
                "fillColor": "#008000",
                "fillOpacity": 0.5,
            },
            "cluster": False,
            "superClusterOptions": {"radius": SUPER_CLUSTER_RADIUS}
        },
    },
    "power_generator": {
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
    "power_transformer": {
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
