DROP MATERIALIZED VIEW IF EXISTS osm.unexposed_ids_nasa_nex_fwi;

CREATE MATERIALIZED VIEW osm.unexposed_ids_nasa_nex_fwi AS
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
LEFT JOIN climate.nasa_nex_fwi fwi ON fwi.osm_id = a.osm_id
WHERE fwi.osm_id IS NULL;

CREATE INDEX unexposed_ids_nasa_nex_fwi_idx_osm_id ON osm.unexposed_ids_nasa_nex_fwi (osm_id);
CREATE INDEX unexposed_ids_nasa_nex_fwi_idx_geom ON osm.unexposed_ids_nasa_nex_fwi USING GIST (geom);

GRANT SELECT ON osm.unexposed_ids_nasa_nex_fwi TO osm_ro_user;
GRANT SELECT ON osm.unexposed_ids_nasa_nex_fwi TO climate_user;