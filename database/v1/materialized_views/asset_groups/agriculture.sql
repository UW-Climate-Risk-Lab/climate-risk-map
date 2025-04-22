-- This creates a materialized view for Agriculture features.
-- It combines relevant features from landuse and building layers.
-- Filters based on agricultural landuse and building tags.
SET ROLE pgosm_flex;

DROP MATERIALIZED VIEW IF EXISTS osm.agriculture;

CREATE MATERIALIZED VIEW osm.agriculture AS

-- Agricultural Landuse (Polygons)
SELECT
    l.osm_id,
    'landuse' AS osm_type,
    t.tags ->> 'landuse' AS osm_subtype,
    ST_GeometryType(l.geom) AS geom_type,
    l.geom,
    t.tags
FROM osm.landuse_polygon l
JOIN osm.tags t ON l.osm_id = t.osm_id
WHERE t.tags ->> 'landuse' IN ('farmland', 'farmyard', 'orchard', 'vineyard', 'meadow', 'grassland', 'plant_nursery') -- Added meadow, grassland, nursery

UNION ALL

-- Agricultural Buildings (Points and Polygons)
SELECT
    b.osm_id,
    'building' AS osm_type,
    b.osm_subtype,
    ST_GeometryType(b.geom) AS geom_type,
    b.geom,
    t.tags
FROM osm.building_point b
JOIN osm.tags t ON b.osm_id = t.osm_id
WHERE t.tags ->> 'building' IN ('farm', 'barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 'silo', 'stable', 'sty')
UNION ALL
SELECT
    b.osm_id,
    'building' AS osm_type,
    b.osm_subtype,
    ST_GeometryType(b.geom) AS geom_type,
    b.geom,
    t.tags
FROM osm.building_polygon b
JOIN osm.tags t ON b.osm_id = t.osm_id
WHERE t.tags ->> 'building' IN ('farm', 'barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 'silo', 'stable', 'sty');

-- Create Indexes
CREATE INDEX agriculture_idx_osm_id ON osm.agriculture (osm_id);
CREATE INDEX agriculture_idx_geom ON osm.agriculture USING GIST (geom);
CREATE INDEX agriculture_idx_osm_type ON osm.agriculture (osm_type);
CREATE INDEX agriculture_idx_osm_subtype ON osm.agriculture (osm_subtype);
CREATE INDEX agriculture_idx_geom_type ON osm.agriculture (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.agriculture TO osm_ro_user;
GRANT SELECT ON osm.agriculture TO climate_user;