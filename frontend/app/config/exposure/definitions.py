from config.settings import ASSETS_PATH


def load_asset_definitions():
    return ASSET_DEFINITIONS.copy()


POWER_LINE_CUSTOM_COLOR_RANGES = [
    {"min": 0, "max": 100, "color": "#5b8f22", "label": "< 100 kV"},
    {"min": 100, "max": 300, "color": "#0046AD", "label": "100-300 kV"},
    {"min": 300, "max": 500, "color": "#63B1E5", "label": "300-500 kV"},
    {"min": 500, "max": float("inf"), "color": "#C75B12", "label": "> 500 kV"},
]

DEFAULT_ICON_PATH = ASSETS_PATH + "/icons/black-dot.svg"

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
        "custom_icon": {
            "property": "plant:source",
            "categories": [
                {
                    "property_value": "wind",
                    "icon_path": ASSETS_PATH + "/icons/wind.png",
                    "label": "Wind Plant",
                },
                {
                    "property_value": "solar",
                    "icon_path": ASSETS_PATH + "/icons/solar.png",
                    "label": "Solar Plant",
                },
                {
                    "property_value": "nuclear",
                    "icon_path": ASSETS_PATH + "/icons/nuclear.png",
                    "label": "Nuclear Plant",
                },
                {
                    "property_value": "gas",
                    "icon_path": ASSETS_PATH + "/icons/gas.png",
                    "label": "Gas Plant",
                },
                {
                    "property_value": "hydro",
                    "icon_path": ASSETS_PATH + "/icons/hydro.png",
                    "label": "Hydro Plant",
                },
            ],
        },
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
        "custom_icon": None,
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
        "custom_icon": None,
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
        "custom_icon": {
            "property": "power",
            "categories": [
                {
                    "property_value": "substation",
                    "icon_path": ASSETS_PATH + "/icons/electric.svg",
                    "label": "Substation",
                },
            ],
        },
        "data_transformations": None,
    },
    "osm-data-center": {
        "type": "OpenStreetMap",
        "label": "Data Center",
        "osm_category": "data_center",
        "osm_type": "building",
        "osm_subtype": None,
        "geom_types": ["MultiPolygon", "Point"],
        "style": {
            "color": "#6a6a6a",
            "weight": 2,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": True,
        "superClusterOptions": {"radius": 1},
        "custom_color": None,
        "custom_icon": {
            "property": 'telecom',
            "categories": [
                {
                    "property_value": 'data_center',
                    "icon_path": ASSETS_PATH + "/icons/data-center.svg",
                    "label": "Data Center",
                },
            ],
        },
        "data_transformations": None,
    },
    "osm-storage-rental": {
        "type": "OpenStreetMap",
        "label": "Storage Rental Facility",
        "osm_category": "shop",
        "osm_type": "shop",
        "osm_subtype": "storage_rental",
        "geom_types": ["MultiPolygon", "Point"],
        "style": {
            "color": "#6a6a6a",
            "weight": 2,
            "fillColor": "#6a6a6a",
            "fillOpacity": 0.5,
        },
        "cluster": True,
        "superClusterOptions": {"radius": 20},
        "custom_color": None,
        "custom_icon": {
            "property": 'shop',
            "categories": [
                {
                    "property_value": 'storage_rental',
                    "icon_path": ASSETS_PATH + "/icons/storage-rental.svg",
                    "label": "Storage Rental Facility",
                },
            ],
        },
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
        "custom_icon": None,
        "data_transformations": None,
    },
}
