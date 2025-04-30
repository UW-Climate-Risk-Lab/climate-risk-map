from config.settings import ASSETS_PATH
from config.ui_config import POWER_LINE_CUSTOM_COLOR_RANGES


def load_asset_definitions():
    return ASSET_DEFINITIONS.copy()

def load_asset_group_definitions():
    return ASSET_GROUP_DEFINITIONS.copy()

DEFAULT_ICON_PATH = ASSETS_PATH + "/icons/black-dot.svg"

ASSET_GROUP_DEFINITIONS = {
    "power-grid": {
        "label": "Power Grid Infrastructure",
        "description": "Power generation, transmission, and distribution assets",
        "assets": [
            "osm-power-plant",
            "osm-power-transmission-line",
            "osm-power-distribution-line",
            "osm-power-substation",
        ],
        "icon": "assets/icons/electric.svg",
    },
    "data-infrastructure": {
        "label": "Data & Computing Infrastructure",
        "description": "Data centers and supporting power infrastructure",
        "assets": [
            "osm-data-center",
            "osm-power-substation",
            "osm-power-transmission-line"
        ],
        "icon": "assets/icons/data-center.svg",
    },
    "commercial-real-estate": {
        "label": "Commercial Real Estate",
        "description": "Retail, Office, St",
        "assets": [
            "osm-commercial-building",
            "osm-office-building",
            "osm-hotel"
        ],
        "icon": None
    },
    "hifld-high-voltage-power-grid": {
        "label": "High Voltage Transmission Lines",
        "description": "Transmission lines over 345 kV",
        "assets": ["hifld-power-transmission-line"],
        "icon": None
    }

}

ASSET_DEFINITIONS = {
    "osm-power-plant": {
        "type": "OpenStreetMap",
        "label": "Power Plants",
        "osm_category": "power_grid",
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
        "osm_category": "power_grid",
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
        "osm_category": "power_grid",
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
        "osm_category": "power_grid",
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
        "superClusterOptions": {"radius": 20},
        "custom_color": None,
        "custom_icon": {
            "property": "telecom",
            "categories": [
                {
                    "property_value": "data_center",
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
        "osm_category": "commericial_real_estate",
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
            "property": "shop",
            "categories": [
                {
                    "property_value": "storage_rental",
                    "icon_path": ASSETS_PATH + "/icons/storage-rental.svg",
                    "label": "Storage Rental Facility",
                },
            ],
        },
        "data_transformations": None,
    },
    "osm-commercial-building": {
        "type": "OpenStreetMap",
        "label": "Commercial Building",
        "osm_category": "commercial_real_estate",
        "osm_type": "building",
        "osm_subtype": "commercial",
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
            "property": "building",
            "categories": [
                {
                    "property_value": "commercial",
                    "icon_path": DEFAULT_ICON_PATH,
                    "label": "Commercial Building",
                },
            ],
        },
        "data_transformations": None,
    },
    "osm-office-building": {
        "type": "OpenStreetMap",
        "label": "Commercial Office Building",
        "osm_category": "commercial_real_estate",
        "osm_type": "building",
        "osm_subtype": "office",
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
            "property": "building",
            "categories": [
                {
                    "property_value": "office",
                    "icon_path": DEFAULT_ICON_PATH,
                    "label": "Commercial Office Building",
                },
            ],
        },
        "data_transformations": None,
    },
    "osm-hotel": {
        "type": "OpenStreetMap",
        "label": "Hotel",
        "osm_category": "commercial_real_estate",
        "osm_type": "tourism",
        "osm_subtype": "hotel",
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
            "property": "tourism",
            "categories": [
                {
                    "property_value": "hotel",
                    "icon_path": DEFAULT_ICON_PATH,
                    "label": "Hotel",
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
