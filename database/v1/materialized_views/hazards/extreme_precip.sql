DROP MATERIALIZED VIEW IF EXISTS climate.extreme_precip;

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