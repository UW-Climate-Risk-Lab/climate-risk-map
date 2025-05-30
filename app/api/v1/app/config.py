OSM_AVAILABLE_CATEGORIES = {
    "power_grid": {"has_subtypes": True},
    "amenity": {"has_subtypes": True},
    "place": {"has_subtypes": False},
    "landuse": {"has_subtypes": False},
    "data_center": {"has_subtypes": True},
    "commercial_real_estate": {"has_subtypes": True},
    "agriculture": {"has_subtypes": True},
    "administrative": {"has_subtypes": True}
}

OSM_SCHEMA_NAME = "osm"
OSM_TABLE_TAGS = "tags"
OSM_COLUMN_GEOM = "geom"
OSM_TABLE_PLACES = "place_polygon"  # This table contains Administrative Boundary data (cities, counties, towns, etc...)
OSM_TABLE_PLACES_ADMIN_LEVELS = {"county": 6, "city": 8}

CLIMATE_SCHEMA_NAME = "climate"
CLIMATE_NASA_NEX_TABLE_PREFIX = "nasa_nex_"
CLIMATE_TABLE_ALIAS = "climate_table"
