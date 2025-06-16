-- This query creates a new table containing the subdivided flood zones.
-- It might take 15-45 minutes to run, but it's a one-time cost.

DROP TABLE IF EXISTS climate.latest_flood_zones_subdivided CASCADE;

CREATE TABLE climate.latest_flood_zones_subdivided AS
WITH latest_zones AS (
    -- First, get only the most recent record for each flood area ID
    SELECT DISTINCT ON (fld_ar_id)
        fld_ar_id,
        flood_zone,
        flood_zone_subtype,
        is_sfha,
        flood_depth,
        lomr_effective_date,
        geom
    FROM climate.fema_nfhl_flood_zones_county
    ORDER BY fld_ar_id, lomr_effective_date DESC NULLS LAST
)
-- Now, take those latest records and subdivide their geometries
SELECT
    fld_ar_id,
    flood_zone,
    flood_zone_subtype,
    is_sfha,
    flood_depth,
    lomr_effective_date,
    ST_Subdivide(geom, 64) AS geom
FROM
    latest_zones;

-- THIS IS THE MOST IMPORTANT PART: INDEX THE NEW TABLE
CREATE INDEX idx_latest_flood_zones_subdivided_geom ON climate.latest_flood_zones_subdivided USING GIST (geom);

-- And tell the database to analyze it
ANALYZE climate.latest_flood_zones_subdivided;