GET_INFRASTRUCTURE_LINE = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT osm_id, osm_subtype AS tooltip, ST_Transform(geom, 4326) AS geometry FROM osm.infrastructure_line
    WHERE osm_type='power'
    )
AS t;
"""

GET_INFRASTRUCTURE_POLYGON = """
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(ST_AsGeoJSON(t.*)::json)
)
FROM (
    SELECT osm_id, osm_subtype AS tooltip, ST_Transform(geom, 4326) AS geometry FROM osm.infrastructure_polygon
    WHERE osm_type='power'
    )
AS t;

"""