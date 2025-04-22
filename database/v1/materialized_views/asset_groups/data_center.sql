-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.data_center;
CREATE MATERIALIZED VIEW osm.data_center AS
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
WHERE t.tags ->> 'telecom' = 'data_center'
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
JOIN osm.tags t ON a.osm_id = t.osm_id
WHERE t.tags ->> 'telecom' = 'data_center';

CREATE INDEX data_center_idx_osm_id ON osm.data_center (osm_id);
CREATE INDEX data_center_idx_geom ON osm.data_center USING GIST (geom);
CREATE INDEX data_center_idx_osm_type ON osm.data_center (osm_type);
CREATE INDEX data_center_idx_osm_subtype ON osm.data_center (osm_subtype);
CREATE INDEX data_center_idx_osm_type_subtype ON osm.data_center (osm_type, osm_subtype);
CREATE INDEX data_center_idx_osm_operator ON osm.data_center (operator);
CREATE INDEX data_center_idx_geom_type ON osm.data_center (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.data_center TO osm_ro_user;
GRANT SELECT ON osm.data_center TO climate_user;