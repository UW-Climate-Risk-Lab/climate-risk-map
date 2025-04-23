-- This creates a materialized view for Commercial Real Estate features.
-- It combines relevant features from amenity, shop, office, tourism, building, poi and landuse layers.
-- Filters based on common commercial-related tags.
SET ROLE pgosm_flex;

DROP MATERIALIZED VIEW IF EXISTS osm.commercial_real_estate;

CREATE MATERIALIZED VIEW osm.commercial_real_estate AS
SELECT DISTINCT ON (osm_id) 
    osm_id,
    osm_type,
    osm_subtype,
    geom_type,
    geom,
    tags
FROM (
    -- Shops (Points and Polygons)
    SELECT
        s.osm_id,
        'shop' AS osm_type,
        s.osm_subtype,
        ST_GeometryType(s.geom) AS geom_type,
        s.geom,
        t.tags
    FROM osm.shop_point s
    JOIN osm.tags t ON s.osm_id = t.osm_id
    WHERE t.tags ? 'shop' -- Selects any feature tagged as a shop
    UNION ALL
    SELECT
        s.osm_id,
        'shop' AS osm_type,
        s.osm_subtype,
        ST_GeometryType(s.geom) AS geom_type,
        s.geom,
        t.tags
    FROM osm.shop_polygon s
    JOIN osm.tags t ON s.osm_id = t.osm_id
    WHERE t.tags ? 'shop'

    UNION ALL

    -- Offices (Points and Polygons from building layer)
    SELECT
        b.osm_id,
        'building' AS osm_type, -- Using building as base type
        'office' AS osm_subtype, -- Explicitly setting subtype
        ST_GeometryType(b.geom) AS geom_type,
        b.geom,
        t.tags
    FROM osm.building_point b
    JOIN osm.tags t ON b.osm_id = t.osm_id
    WHERE t.tags ->> 'office' IS NOT NULL -- Check if 'office' tag exists
    OR t.tags ->> 'building' = 'office'
    UNION ALL
    SELECT
        b.osm_id,
        'building' AS osm_type,
        'office' AS osm_subtype,
        ST_GeometryType(b.geom) AS geom_type,
        b.geom,
        t.tags
    FROM osm.building_polygon b
    JOIN osm.tags t ON b.osm_id = t.osm_id
    WHERE t.tags ->> 'office' IS NOT NULL
    OR t.tags ->> 'building' = 'office'

    UNION ALL

    -- Commercial Amenities (Points and Polygons)
    SELECT
        a.osm_id,
        'amenity' AS osm_type,
        a.osm_subtype,
        ST_GeometryType(a.geom) AS geom_type,
        a.geom,
        t.tags
    FROM osm.amenity_point a
    JOIN osm.tags t ON a.osm_id = t.osm_id
    WHERE t.tags ->> 'amenity' IN ('restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'bank', 'clinic', 'pharmacy', 'cinema', 'theatre', 'nightclub', 'fuel', 'car_wash', 'car_rental')
    UNION ALL
    SELECT
        a.osm_id,
        'amenity' AS osm_type,
        a.osm_subtype,
        ST_GeometryType(a.geom) AS geom_type,
        a.geom,
        t.tags
    FROM osm.amenity_polygon a
    JOIN osm.tags t ON a.osm_id = t.osm_id
    WHERE t.tags ->> 'amenity' IN ('restaurant', 'fast_food', 'cafe', 'pub', 'bar', 'bank', 'clinic', 'pharmacy', 'cinema', 'theatre', 'nightclub', 'fuel', 'car_wash', 'car_rental')

    UNION ALL

    -- Tourism (Hotels, etc. - Points and Polygons)
    SELECT
        p.osm_id,
        'tourism' AS osm_type, -- Assuming tourism is loaded or using POI
        p.osm_subtype,
        ST_GeometryType(p.geom) AS geom_type,
        p.geom,
        t.tags
    FROM osm.poi_point p -- Using POI as a potential source for tourism features
    JOIN osm.tags t ON p.osm_id = t.osm_id
    WHERE t.tags ->> 'tourism' IN ('hotel', 'motel', 'guest_house', 'hostel', 'chalet')
    UNION ALL
    SELECT
        p.osm_id,
        'tourism' AS osm_type,
        p.osm_subtype,
        ST_GeometryType(p.geom) AS geom_type,
        p.geom,
        t.tags
    FROM osm.poi_polygon p
    JOIN osm.tags t ON p.osm_id = t.osm_id
    WHERE t.tags ->> 'tourism' IN ('hotel', 'motel', 'guest_house', 'hostel', 'chalet')

    UNION ALL

    -- Commercial/Industrial/Retail Buildings (Polygons)
    SELECT
        b.osm_id,
        'building' AS osm_type,
        b.osm_subtype,
        ST_GeometryType(b.geom) AS geom_type,
        b.geom,
        t.tags
    FROM osm.building_polygon b
    JOIN osm.tags t ON b.osm_id = t.osm_id
    WHERE t.tags ->> 'building' IN ('commercial', 'industrial', 'retail', 'warehouse', 'office')
    -- Add specific commercial/industrial amenity types if needed and not covered above
    OR t.tags ->> 'amenity' IN ('marketplace')

    UNION ALL

    -- Commercial/Industrial/Retail Landuse (Polygons)
    SELECT
        l.osm_id,
        'landuse' AS osm_type,
        t.tags ->> 'landuse' AS osm_subtype,
        ST_GeometryType(l.geom) AS geom_type,
        l.geom,
        t.tags
    FROM osm.landuse_polygon l
    JOIN osm.tags t ON l.osm_id = t.osm_id
    WHERE t.tags ->> 'landuse' IN ('commercial', 'retail', 'industrial', 'logistics') -- Added logistics
) AS combined_data;




-- Create Indexes
CREATE INDEX commercial_real_estate_idx_osm_id ON osm.commercial_real_estate (osm_id);
CREATE INDEX commercial_real_estate_idx_geom ON osm.commercial_real_estate USING GIST (geom);
CREATE INDEX commercial_real_estate_idx_osm_type ON osm.commercial_real_estate (osm_type);
CREATE INDEX commercial_real_estate_idx_osm_subtype ON osm.commercial_real_estate (osm_subtype);
CREATE INDEX commercial_real_estate_idx_geom_type ON osm.commercial_real_estate (geom_type);

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON osm.commercial_real_estate TO osm_ro_user;
GRANT SELECT ON osm.commercial_real_estate TO climate_user;