SET LOCAL statement_timeout = '24h';

BEGIN;
DROP MATERIALIZED VIEW IF EXISTS climate.extreme_precip CASCADE;

CREATE MATERIALIZED VIEW climate.extreme_precip AS
SELECT
    pfe.osm_id,
    pfe.month,
    pfe.start_year,
    pfe.end_year,
    pfe.ssp,
    pfe.return_period,
    pfe.ensemble_median AS pfe_ensemble_median,
    pfe.ensemble_q3 AS pfe_ensemble_q3,
    (pfe.ensemble_median / NULLIF(pcf.ensemble_median, 0)) AS pfe_ensemble_median_historical_baseline,
    (pfe.ensemble_q3 / NULLIF(pcf.ensemble_q3, 0)) AS pfe_ensemble_q3_historical_baseline
FROM climate.nasa_nex_pfe_mm_day pfe
LEFT JOIN climate.nasa_nex_pluvial_change_factor pcf ON pfe.osm_id = pcf.osm_id AND pfe.month = pcf.month AND pfe.start_year = pcf.start_year AND pfe.end_year = pcf.end_year AND pfe.ssp = pcf.ssp AND pfe.return_period = pcf.return_period;

CREATE UNIQUE INDEX idx_unique_extreme_precip_record
    ON climate.extreme_precip (osm_id, month, start_year, end_year, ssp, return_period);

CREATE INDEX idx_extreme_precip_on_osm_id ON climate.extreme_precip (osm_id);
CREATE INDEX idx_extreme_precip_on_month ON climate.extreme_precip (month);
CREATE INDEX idx_extreme_precip_on_start_year ON climate.extreme_precip (start_year);
CREATE INDEX idx_extreme_precip_on_end_year ON climate.extreme_precip (end_year);
CREATE INDEX idx_extreme_precip_on_month_year ON climate.extreme_precip (month, start_year, end_year);
CREATE INDEX idx_extreme_precip_on_ssp ON climate.extreme_precip (ssp);
CREATE INDEX idx_extreme_precip_on_return_period ON climate.extreme_precip (return_period);

GRANT SELECT ON climate.extreme_precip TO osm_ro_user;
GRANT SELECT ON climate.extreme_precip TO climate_user;
COMMIT;


BEGIN;
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
COMMIT;

DROP MATERIALIZED VIEW IF EXISTS climate.flood;

BEGIN;

CREATE MATERIALIZED VIEW climate.flood AS
WITH
-- 1. Get the distinct assets from your existing materialized view.
distinct_assets AS (
    SELECT DISTINCT osm_id FROM climate.extreme_precip
),
-- 2. Get the geometries for ONLY those assets.
asset_geoms AS (
    SELECT DISTINCT ON (g.osm_id) g.osm_id, g.geom
    FROM distinct_assets p
    INNER JOIN (
        SELECT osm_id, geom FROM osm.power_grid
        UNION ALL
        SELECT osm_id, geom FROM osm.data_center
        UNION ALL
        SELECT osm_id, geom FROM osm.administrative
        UNION ALL
        SELECT osm_id, geom FROM osm.agriculture
        UNION ALL
        SELECT osm_id, geom FROM osm.commercial_real_estate
    ) g ON p.osm_id = g.osm_id
),
-- 3. Spatially join assets to the NEW subdivided flood zone table.
asset_flood_zone AS (
    SELECT
        ag.osm_id,
        fz.flood_zone,
        fz.flood_zone_subtype,
        fz.is_sfha,
        fz.flood_depth
    FROM asset_geoms ag
    -- NOTE: We are joining to the new table now.
    LEFT JOIN LATERAL (
        SELECT
            flood_zone,
            flood_zone_subtype,
            is_sfha,
            flood_depth
        FROM climate.latest_flood_zones_subdivided fz
        WHERE ST_Intersects(ag.geom, fz.geom)
        ORDER BY
            -- This ORDER BY is still needed to handle cases where an asset
            -- might intersect two different original flood zones.
            fz.lomr_effective_date DESC NULLS LAST
        LIMIT 1
    ) fz ON TRUE
),
-- 4. Final, simple join between your existing view and the new spatial data.
base AS (
    SELECT
        ep.osm_id,
        ep.month,
        ep.start_year,
        ep.end_year,
        ep.ssp,
        ep.return_period,
        ep.pfe_ensemble_median AS ensemble_median,
        ep.pfe_ensemble_q3 AS ensemble_q3,
        ep.pfe_ensemble_median_historical_baseline AS ensemble_median_historical_baseline,
        ep.pfe_ensemble_q3_historical_baseline AS ensemble_q3_historical_baseline,
        afz.flood_zone,
        afz.flood_zone_subtype,
        afz.is_sfha,
        afz.flood_depth
    FROM climate.extreme_precip ep
    LEFT JOIN asset_flood_zone afz ON ep.osm_id = afz.osm_id
)
SELECT b.*, gs.decade
FROM base b
-- This logic ensures that decades are only generated if they START within a period.
-- This is more efficient and robust than generating duplicates and then filtering.
JOIN LATERAL generate_series(
    (ceil(b.start_year / 10.0) * 10)::integer,
    (floor(b.end_year / 10.0) * 10)::integer,
    10
) AS gs(decade) ON TRUE;


-- Your index creation remains the same.
CREATE UNIQUE INDEX idx_unique_flood_record
    ON climate.flood
    (osm_id , month , decade , ssp , return_period ,
     flood_zone , flood_zone_subtype , is_sfha , flood_depth);

CREATE INDEX idx_flood_on_osm_id ON climate.flood (osm_id);
CREATE INDEX idx_flood_on_month ON climate.flood (month);
CREATE INDEX idx_flood_on_start_year ON climate.flood (start_year);
CREATE INDEX idx_flood_on_end_year ON climate.flood (end_year);
CREATE INDEX idx_flood_on_decade ON climate.flood (decade);
CREATE INDEX idx_flood_on_ssp ON climate.flood (ssp);
CREATE INDEX idx_flood_on_return_period ON climate.flood (return_period);

CREATE INDEX idx_flood_zones_subdivided_lomr ON
  climate.latest_flood_zones_subdivided (lomr_effective_date DESC NULLS LAST);

GRANT SELECT ON climate.flood TO osm_ro_user;
GRANT SELECT ON climate.flood TO climate_user;

COMMIT;