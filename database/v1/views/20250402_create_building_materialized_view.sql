-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.building;
CREATE MATERIALIZED VIEW osm.building AS
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    a.operator,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.building_point a
JOIN osm.tags t ON a.osm_id = t.osm_id
UNION ALL
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    a.operator,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.building_polygon a
JOIN osm.tags t ON a.osm_id = t.osm_id;

CREATE INDEX building_idx_osm_id ON osm.building (osm_id);
CREATE INDEX building_idx_geom ON osm.building USING GIST (geom);
CREATE INDEX building_idx_osm_type ON osm.building (osm_type);
CREATE INDEX building_idx_osm_subtype ON osm.building (osm_subtype);
CREATE INDEX building_idx_osm_type_subtype ON osm.building (osm_type, osm_subtype);
CREATE INDEX building_idx_osm_operator ON osm.building (operator);
CREATE INDEX building_idx_geom_type ON osm.building (geom_type);