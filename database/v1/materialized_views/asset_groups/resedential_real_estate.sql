-- This creates a materialized view for Residential Real Estate features.
-- It combines relevant features primarily from the building and landuse layers.
-- Filters based on common residential-related tags.
SET ROLE pgosm_flex;

DROP MATERIALIZED VIEW IF EXISTS osm.residential_real_estate;

CREATE MATERIALIZED VIEW osm.residential_real_estate AS

-- Residential Buildings (Points and Polygons)
SELECT
    b.osm_id,
    'building' AS osm_type,
    b.osm_subtype, -- Use the subtype from the building table (e.g., 'house', 'apartments')
    ST_GeometryType(b.geom) AS geom_type,
    b.geom,
    t.tags
FROM osm.building_point b
JOIN osm.tags t ON b.osm_id = t.osm_id
WHERE t.tags ->> 'building' IN (
    'house', 'detached', 'semidetached_house', 'terrace', -- Single/multi-family houses
    'residential', 'apartments', 'bungalow', 'cabin', 'dormitory', -- Other residential types
    'static_caravan', 'hut' -- Include potentially less permanent dwellings
)
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
WHERE t.tags ->> 'building' IN (
    'house', 'detached', 'semidetached_house', 'terrace',
    'residential', 'apartments', 'bungalow', 'cabin', 'dormitory',
    'static_caravan', 'hut'
);


-- Create Indexes
CREATE INDEX residential_real_estate_idx_osm_id ON osm.residential_real_estate (osm_id);
CREATE INDEX residential_real_estate_idx_geom ON osm.residential_real_estate USING GIST (geom);
CREATE INDEX residential_real_estate_idx_osm_type ON osm.residential_real_estate (osm_type);
CREATE INDEX residential_real_estate_idx_osm_subtype ON osm.residential_real_estate (osm_subtype);
CREATE INDEX residential_real_estate_idx_geom_type ON osm.residential_real_estate (geom_type);
CREATE INDEX residential_real_estate_idx_tags_building ON osm.residential_real_estate ((tags->>'building'));

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.commercial_real_estate TO osm_ro_user;
GRANT SELECT ON osm.commercial_real_estate TO climate_user;