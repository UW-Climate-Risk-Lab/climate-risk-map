BEGIN;

SET ROLE climate_user;

CREATE TABLE climate.nasa_nex_pr_percent_change (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    month SMALLINT NOT NULL,
    decade SMALLINT NOT NULL,
    ssp SMALLINT NOT NULL, -- ssp -999 is for 'historical' values
    ensemble_mean FLOAT NOT NULL,
    ensemble_median FLOAT NOT NULL,
    ensemble_stddev FLOAT NOT NULL,
    ensemble_min FLOAT NOT NULL,
    ensemble_max FLOAT NOT NULL,
    ensemble_q1 FLOAT NOT NULL,
    ensemble_q3 FLOAT NOT NULL,  
    metadata JSONB

);

-- Unique index to constrain possible values for a given feature (osm_id)
CREATE UNIQUE INDEX idx_unique_nasa_nex_pr_percent_change_record
    ON climate.nasa_nex_pr_percent_change (osm_id, month, decade, ssp);

CREATE INDEX idx_nasa_nex_pr_percent_change_on_osm_id ON climate.nasa_nex_pr_percent_change (osm_id);
CREATE INDEX idx_nasa_nex_pr_percent_change_on_month ON climate.nasa_nex_pr_percent_change (month);
CREATE INDEX idx_nasa_nex_pr_percent_change_on_decade ON climate.nasa_nex_pr_percent_change (decade);
CREATE INDEX idx_nasa_nex_pr_percent_change_on_month_decade ON climate.nasa_nex_pr_percent_change (month, decade);
CREATE INDEX idx_nasa_nex_pr_percent_change_on_ssp ON climate.nasa_nex_pr_percent_change (ssp);

COMMIT;