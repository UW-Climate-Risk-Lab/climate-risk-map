-- This creates a view that combines all features in the ifnrastructure categories
-- This will consolidate features for easier querying. All properties can be found by joining the tags table
SET ROLE pgosm_flex;
DROP MATERIALIZED VIEW IF EXISTS osm.shop;
CREATE MATERIALIZED VIEW osm.shop AS
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    a.brand,
    a.operator,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.shop_point a
JOIN osm.tags t ON a.osm_id = t.osm_id
UNION ALL
SELECT
    a.osm_id,
    a.osm_type,
    a.osm_subtype,
    a.name,
    a.brand,
    a.operator,
    ST_GeometryType(a.geom) AS geom_type,
    a.geom,
    t.tags
FROM osm.shop_polygon a
JOIN osm.tags t ON a.osm_id = t.osm_id;

CREATE INDEX shop_idx_osm_id ON osm.shop (osm_id);
CREATE INDEX shop_idx_geom ON osm.shop USING GIST (geom);
CREATE INDEX shop_idx_osm_type ON osm.shop (osm_type);
CREATE INDEX shop_idx_osm_subtype ON osm.shop (osm_subtype);
CREATE INDEX shop_idx_osm_type_subtype ON osm.shop (osm_type, osm_subtype);
CREATE INDEX shop_idx_osm_brand ON osm.shop (brand);
CREATE INDEX shop_idx_osm_operator ON osm.shop (operator);
CREATE INDEX shop_idx_geom_type ON osm.shop (geom_type);