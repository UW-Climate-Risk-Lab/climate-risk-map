-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
CREATE MATERIALIZED VIEW IF NOT EXISTS osm.infrastructure AS
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_point
UNION ALL
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_line
UNION ALL
SELECT
    osm_id,
    osm_type,
    osm_subtype,
    ST_GeometryType(geom) AS geom_type,
    geom
FROM osm.infrastructure_polygon;

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.infrastructure TO osm_ro_user;
GRANT SELECT ON osm.infrastructure TO climate_user;