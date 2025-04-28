-- This creates a view that combines all features in the OSM type "Power"
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.power_grid;
CREATE MATERIALIZED VIEW osm.power_grid AS
SELECT DISTINCT ON (osm_id) 
    osm_id,
    osm_type,
    osm_subtype,
    geom_type,
    geom,
    tags
FROM (
    SELECT
        i.osm_id,
        i.osm_type,
        i.osm_subtype,
        ST_GeometryType(i.geom) AS geom_type,
        i.geom,
        t.tags
    FROM osm.infrastructure_point i
    JOIN osm.tags t ON i.osm_id = t.osm_id
    WHERE i.osm_type = 'power' AND i.osm_subtype IN ('substation', 'line', 'plant', 'minor_line')
    UNION ALL
    SELECT
        i.osm_id,
        i.osm_type,
        i.osm_subtype,
        ST_GeometryType(i.geom) AS geom_type,
        i.geom,
        t.tags
    FROM osm.infrastructure_line i
    JOIN osm.tags t ON i.osm_id = t.osm_id
    WHERE i.osm_type = 'power' AND i.osm_subtype IN ('substation', 'line', 'plant', 'minor_line')
    UNION ALL
    SELECT
        i.osm_id,
        i.osm_type,
        i.osm_subtype,
        ST_GeometryType(i.geom) AS geom_type,
        i.geom,
        t.tags
    FROM osm.infrastructure_polygon i
    JOIN osm.tags t ON i.osm_id = t.osm_id
    WHERE i.osm_type = 'power' AND i.osm_subtype IN ('substation', 'line', 'plant', 'minor_line')
) AS combined_data;

CREATE INDEX power_grid_idx_osm_id ON osm.power_grid (osm_id);
CREATE INDEX power_grid_idx_geom ON osm.power_grid USING GIST (geom);
CREATE INDEX power_grid_idx_osm_type ON osm.power_grid (osm_type);
CREATE INDEX power_grid_idx_osm_subtype ON osm.power_grid (osm_subtype);
CREATE INDEX power_grid_idx_osm_type_subtype ON osm.power_grid (osm_type, osm_subtype);
CREATE INDEX power_grid_idx_geom_type ON osm.power_grid (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.power_grid TO osm_ro_user;
GRANT SELECT ON osm.power_grid TO climate_user;