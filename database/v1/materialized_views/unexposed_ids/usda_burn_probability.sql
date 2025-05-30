DROP MATERIALIZED VIEW IF EXISTS osm.unexposed_ids_usda_burn_probability;

CREATE MATERIALIZED VIEW osm.unexposed_ids_usda_burn_probability AS
WITH all_assets AS (
    SELECT osm_id, geom FROM osm.power_grid
        UNION ALL
    SELECT osm_id, geom FROM osm.data_center
        UNION ALL
    SELECT osm_id, geom FROM osm.administrative
        UNION ALL
    SELECT osm_id, geom FROM osm.agriculture
        UNION ALL
    SELECT osm_id, geom FROM osm.commercial_real_estate WHERE osm_subtype IS NOT NULL
    
    
)
SELECT DISTINCT a.osm_id, a.geom
FROM all_assets a
LEFT JOIN climate.usda_burn_probability bp ON bp.osm_id = a.osm_id
WHERE bp.osm_id IS NULL;

CREATE INDEX unexposed_ids_usda_burn_probability_idx_osm_id ON osm.unexposed_ids_usda_burn_probability (osm_id);
CREATE INDEX unexposed_ids_usda_burn_probability_idx_geom ON osm.unexposed_ids_usda_burn_probability USING GIST (geom);

GRANT SELECT ON osm.unexposed_ids_usda_burn_probability TO osm_ro_user;
GRANT SELECT ON osm.unexposed_ids_usda_burn_probability TO climate_user;