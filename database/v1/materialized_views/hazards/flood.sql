DROP MATERIALIZED VIEW IF EXISTS climate.flood;

BEGIN;

-- Your existing performance settings are good.
SET LOCAL work_mem = '2GB';
SET LOCAL max_parallel_workers_per_gather = 20;
SET LOCAL statement_timeout = '12h';
SET LOCAL temp_buffers = '1GB';
SET LOCAL effective_cache_size = '80GB';
SET LOCAL random_page_cost = 1.0;
SET LOCAL seq_page_cost = 0.1;
SET LOCAL cpu_tuple_cost = 0.01;

SELECT 'Starting limited flood materialized view creation at: ' || now() ||
       ' with work_mem=' || current_setting('work_mem') ||
       ', max_workers=' || current_setting('max_parallel_workers_per_gather');


CREATE MATERIALIZED VIEW climate.flood AS
WITH
-- 1. First, get the distinct set of assets that are actually present in the main data table.
distinct_assets_in_pfe AS (
    SELECT DISTINCT osm_id FROM climate.nasa_nex_pfe_mm_day
),
-- 2. Get the geometries for ONLY those assets. This is much smaller than all ~140k assets.
asset_geoms AS (
    SELECT p.osm_id, g.geom
    FROM distinct_assets_in_pfe p
    JOIN (
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
-- 3. Get the latest version of each flood zone, as before.
latest_flood_zones AS (
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
),
-- 4. Spatially join the filtered assets to flood zones using a much more efficient LATERAL join.
asset_flood_zone AS (
    SELECT
        ag.osm_id,
        fz.flood_zone,
        fz.flood_zone_subtype,
        fz.is_sfha,
        fz.flood_depth
    FROM asset_geoms ag
    -- For each asset, find the single best intersecting flood zone based on your criteria.
    LEFT JOIN LATERAL (
        SELECT
            flood_zone,
            flood_zone_subtype,
            is_sfha,
            flood_depth
        FROM latest_flood_zones fz
        WHERE ST_Intersects(ag.geom, fz.geom) AND fz.is_sfha = 'T'
        ORDER BY
            fz.lomr_effective_date DESC NULLS LAST       -- Then most recent data
        LIMIT 1 -- This is key for performance
    ) fz ON TRUE
)
-- 5. Now, perform the final join. All expensive spatial work is done on a minimal dataset.
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
    (pfe.ensemble_q3 / NULLIF(pcf.ensemble_q3, 0)) AS pfe_ensemble_q3_historical_baseline,
    afz.flood_zone,
    afz.flood_zone_subtype,
    afz.is_sfha,
    afz.flood_depth
FROM climate.nasa_nex_pfe_mm_day pfe
LEFT JOIN climate.nasa_nex_pluvial_change_factor pcf
    ON pfe.osm_id = pcf.osm_id
    AND pfe.month = pcf.month
    AND pfe.start_year = pcf.start_year
    AND pfe.end_year = pcf.end_year
    AND pfe.ssp = pcf.ssp
    AND pfe.return_period = pcf.return_period
LEFT JOIN asset_flood_zone afz ON afz.osm_id = pfe.osm_id;

SELECT 'Starting index creation at: ' || now();
-- Your indexes remain excellent for querying the final materialized view.
CREATE UNIQUE INDEX idx_unique_flood_record
    ON climate.flood (osm_id, month, start_year, end_year, ssp, return_period);

CREATE INDEX idx_flood_on_osm_id ON climate.flood (osm_id);
CREATE INDEX idx_flood_on_month ON climate.flood (month);
CREATE INDEX idx_flood_on_start_year ON climate.flood (start_year);
CREATE INDEX idx_flood_on_end_year ON climate.flood (end_year);
CREATE INDEX idx_flood_on_month_year ON climate.flood (month, start_year, end_year);
CREATE INDEX idx_flood_on_ssp ON climate.flood (ssp);
CREATE INDEX idx_flood_on_return_period ON climate.flood (return_period);

GRANT SELECT ON climate.flood TO osm_ro_user;
GRANT SELECT ON climate.flood TO climate_user;

COMMIT;