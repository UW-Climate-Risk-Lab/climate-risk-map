DROP MATERIALIZED VIEW IF EXISTS climate.wildfire;

CREATE MATERIALIZED VIEW climate.wildfire AS
WITH historical_baseline AS (
    SELECT osm_id, AVG(ensemble_mean) as ensemble_mean_historic_baseline FROM climate.nasa_nex_fwi fwi WHERE fwi.ssp = -999
    GROUP BY osm_id
)
SELECT DISTINCT fwi.osm_id, fwi.month, fwi.decade, fwi.ssp, fwi.fwi_ensemble_mean, hb.ensemble_mean_historic_baseline, bp.burn_probability
FROM climate.nasa_nex_fwi fwi
LEFT JOIN climate.usda_burn_probability bp ON fwi.osm_id = bp.osm_id
LEFT JOIN historical_baseline hb ON fwi.osm_id = hb.osm_id;

CREATE INDEX idx_wildfire_on_osm_id ON climate.wildfire (osm_id);
CREATE INDEX idx_wildfire_on_month ON climate.nasa_wildfirenex_fwi (month);
CREATE INDEX idx_wildfire_on_decade ON climate.wildfire (decade);
CREATE INDEX idx_wildfire_on_month_decade ON climate.wildfire (month, decade);
CREATE INDEX idx_wildfire_on_ssp ON climate.wildfire (ssp);

GRANT SELECT ON climate.wildfire TO osm_ro_user;
GRANT SELECT ON climate.wildfire TO climate_user;