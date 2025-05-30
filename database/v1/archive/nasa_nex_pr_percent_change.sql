DROP MATERIALIZED VIEW IF EXISTS osm.unexposed_ids_nasa_nex_pr_percent_change;

CREATE MATERIALIZED VIEW osm.unexposed_ids_nasa_nex_pr_percent_change AS
WITH all_assets AS (
    SELECT osm_id, geom FROM osm.data_center
    UNION ALL
    SELECT osm_id, geom FROM osm.administrative
    UNION ALL
    SELECT osm_id, geom FROM osm.agriculture
    UNION ALL
    SELECT osm_id, geom FROM osm.commercial_real_estate
    UNION ALL
    SELECT osm_id, geom FROM osm.power_grid
    UNION ALL
    SELECT osm_id, geom FROM osm.residential_real_estate
)
SELECT DISTINCT a.osm_id, a.geom
FROM all_assets a
LEFT JOIN climate.nasa_nex_pr_percent_change pr_percent_change ON pr_percent_change.osm_id = a.osm_id
WHERE pr_percent_change.osm_id IS NULL;

CREATE INDEX unexposed_ids_nasa_nex_pr_percent_change_idx_osm_id ON osm.unexposed_ids_nasa_nex_pr_percent_change (osm_id);
CREATE INDEX unexposed_ids_nasa_nex_pr_percent_change_idx_geom ON osm.unexposed_ids_nasa_nex_pr_percent_change USING GIST (geom);

GRANT SELECT ON osm.unexposed_ids_nasa_nex_pr_percent_change TO osm_ro_user;
GRANT SELECT ON osm.unexposed_ids_nasa_nex_pr_percent_change TO climate_user;