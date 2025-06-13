BEGIN;

SET ROLE climate_user;

CREATE TABLE IF NOT EXISTS climate.fema_nfhl_flood_zones_county (
    id SERIAL PRIMARY KEY,
    fld_ar_id TEXT NOT NULL,
    dfirm_id TEXT,
    version_id TEXT,
    flood_zone TEXT,
    flood_zone_subtype TEXT,
    is_sfha TEXT,
    static_bfe FLOAT,
    flood_depth FLOAT,
    source_url TEXT,
    lomr_effective_date DATE,
    geom GEOMETRY(MultiPolygon, 4326) NOT NULL -- SRID will match TARGET_CRS
);

CREATE UNIQUE INDEX idx_unique_fema_nfhl_flood_zones_county_geom
    ON climate.fema_nfhl_flood_zones_county (dfirm_id, fld_ar_id, version_id, flood_zone, flood_zone_subtype, is_sfha, static_bfe, flood_depth, source_url, lomr_effective_date);

CREATE INDEX IF NOT EXISTS idx_fema_nfhl_flood_zones_county_geom
ON climate.fema_nfhl_flood_zones_county
USING GIST (geom);

COMMIT;