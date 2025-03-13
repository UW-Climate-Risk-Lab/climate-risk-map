from config.settings import ASSETS_PATH


def load_asset_definitions():
    return ASSET_DEFINITIONS.copy()


POWER_LINE_CUSTOM_COLOR_RANGES = [
    {"min": 0, "max": 100, "color": "#5b8f22", "label": "< 100 kV"},
    {"min": 100, "max": 300, "color": "#0046AD", "label": "100-300 kV"},
    {"min": 300, "max": 500, "color": "#63B1E5", "label": "300-500 kV"},
    {"min": 500, "max": float("inf"), "color": "#C75B12", "label": "> 500 kV"},
]

ASSET_DEFINITIONS = {
    "osm-power-plant": {
        "type": "OpenStreetMap",
        "label": "Power Plants",
        "osm_category": "infrastructure",
        "osm_type": "power",
        "osm_subtype": "plant",
        "geom_types": ["MultiPolygon"],
        "style": {
            "color": "#6a6a6a",
            "weight": 2,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.8,
        },
        "cluster": True,
        "superClusterOptions": {"radius": 50},
        "custom_color": None,
        "icon_path": ASSETS_PATH + "/icons/power-plant.svg",
        "data_transformations": None,
    },
    "osm-power-transmission-line": {
        "type": "OpenStreetMap",
        "label": "Power Transmission Lines",
        "osm_category": "infrastructure",
        "osm_type": "power",
        "osm_subtype": "line",
        "geom_types": ["LineString"],
        "style": {
            "color": "#6a6a6a",
            "weight": 1.5,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": False,
        "superClusterOptions": False,
        "custom_color": {
            "property": "voltage",
            "ranges": POWER_LINE_CUSTOM_COLOR_RANGES,
            "categories": None,
        },
        "icon_path": None,
        "data_transformations": ["osm_line_voltage"],
    },
    "osm-power-distribution-line": {
        "type": "OpenStreetMap",
        "label": "Power Distribution Lines",
        "osm_category": "infrastructure",
        "osm_type": "power",
        "osm_subtype": "minor_line",
        "geom_types": ["LineString"],
        "style": {
            "color": "#6a6a6a",
            "weight": 1.5,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": False,
        "superClusterOptions": False,
        "custom_color": {
            "property": "voltage",
            "ranges": POWER_LINE_CUSTOM_COLOR_RANGES,
            "categories": None,
        },
        "icon_path": None,
        "data_transformations": ["osm_line_voltage"],
    },
    "osm-power-substation": {
        "type": "OpenStreetMap",
        "label": "Power Substations",
        "osm_category": "infrastructure",
        "osm_type": "power",
        "osm_subtype": "substation",
        "geom_types": ["MultiPolygon", "Point"],
        "style": {
            "color": "#6a6a6a",
            "weight": 2,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": True,
        "superClusterOptions": {"radius": 500},
        "custom_color": None,
        "icon_path": ASSETS_PATH + "/icons/black-dot.svg",
        "data_transformations": None,
    },
    "hifld-power-transmission-line": {
        "type": "HIFLD",
        "label": "Power Transmission Lines (345+ kV)",
        "geojson_path": ASSETS_PATH
        + "/geojsons/hifld/hifld-in-service-high-voltage-power-transmission-line.geojson",
        "geom_types": ["LineString"],
        "style": {
            "color": "#6a6a6a",
            "weight": 1.0,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": False,
        "superClusterOptions": False,
        "custom_color": {
            "property": "VOLTAGE",
            "ranges": POWER_LINE_CUSTOM_COLOR_RANGES,
            "categories": None,
        },
        "icon_path": None,
        "data_transformations": None,
    },
}
