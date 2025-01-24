-- This creates a view that takes certain aggregates from the FWI table for quick access by the frontend
SET ROLE climate_user;
DROP MATERIALIZED VIEW IF EXISTS climate.nasa_nex_fwi_metadata;
CREATE MATERIALIZED VIEW climate.nasa_nex_fwi_metadata AS
SELECT
    MIN(f.value_mean) as min_value,
    MAX(f.value_max) as max_value
FROM climate.nasa_nex_fwi f;

-- Grant SELECT on the materialized view to osm_ro_user and climate_user
GRANT SELECT ON climate.nasa_nex_fwi_metadata TO osm_ro_user;
GRANT SELECT ON climate.nasa_nex_fwi_metadata TO climate_user;