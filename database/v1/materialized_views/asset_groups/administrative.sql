-- This creates a materialized view for Administrative features.
-- It combines relevant features from the place layer (points, lines, polygons).
-- Filters based on boundary=administrative tags and place tags representing administrative entities.
SET ROLE pgosm_flex;

DROP MATERIALIZED VIEW IF EXISTS osm.administrative;

CREATE MATERIALIZED VIEW osm.administrative AS
SELECT DISTINCT ON (osm_id) 
    osm_id,
    osm_type,
    osm_subtype,
    admin_level,
    name,
    geom_type,
    geom,
    tags
FROM (

    -- Administrative Boundaries (Polygons)
    SELECT
        p.osm_id,
        'place' AS osm_type,
        'boundary' AS osm_subtype,
        p.admin_level,
        p.name,
        ST_GeometryType(p.geom) AS geom_type,
        p.geom,
        t.tags
    FROM osm.place_polygon p -- Boundaries can also be polygons
    LEFT JOIN osm.tags t ON p.osm_id = t.osm_id --Must left join because official boundaries do not have tags
    WHERE p.admin_level IS NOT NULL

    UNION ALL

    -- Place Nodes/Polygons (Cities, States, Countries, etc.)
    SELECT
        p.osm_id,
        'place' AS osm_type,
        t.tags ->> 'place' AS osm_subtype, -- Extract subtype from 'place' tag,
        p.admin_level,
        p.name,
        ST_GeometryType(p.geom) AS geom_type,
        p.geom,
        t.tags
    FROM osm.place_point p
    JOIN osm.tags t ON p.osm_id = t.osm_id
    WHERE t.tags ->> 'place' IN ('country', 'state', 'region', 'province', 'district', 'county', 'municipality', 'city', 'borough', 'suburb', 'quarter', 'neighbourhood', 'town', 'village', 'hamlet')
    -- Optionally filter out boundaries if already captured above, though overlap might be acceptable
    AND t.tags ->> 'boundary' IS DISTINCT FROM 'administrative'
    UNION ALL
    SELECT
        p.osm_id,
        'place' AS osm_type,
        t.tags ->> 'place' AS osm_subtype, -- Extract subtype from 'place' tag,
        p.admin_level,
        p.name,
        ST_GeometryType(p.geom) AS geom_type,
        p.geom,
        t.tags
    FROM osm.place_polygon p -- Include polygons representing places like cities or states
    JOIN osm.tags t ON p.osm_id = t.osm_id
    WHERE t.tags ->> 'place' IN ('country', 'state', 'region', 'province', 'district', 'county', 'municipality', 'city', 'borough', 'suburb', 'quarter', 'neighbourhood', 'town', 'village', 'hamlet')
    AND t.tags ->> 'boundary' IS DISTINCT FROM 'administrative'
) AS combined_data;


-- Create Indexes
CREATE INDEX administrative_idx_osm_id ON osm.administrative (osm_id);
CREATE INDEX administrative_idx_geom ON osm.administrative USING GIST (geom);
CREATE INDEX administrative_idx_osm_type ON osm.administrative (osm_type);
CREATE INDEX administrative_idx_osm_subtype ON osm.administrative (osm_subtype);
CREATE INDEX administrative_idx_geom_type ON osm.administrative (geom_type);
CREATE INDEX administrative_idx_tags_admin_level ON osm.administrative ((tags->>'admin_level')); -- Index admin_level if frequently queried
CREATE INDEX administrative_idx_tags_place ON osm.administrative ((tags->>'place')); -- Index place tag if frequently queried

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.administrative TO osm_ro_user;
GRANT SELECT ON osm.administrative TO climate_user;