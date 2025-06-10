BEGIN;

SET ROLE climate_user;

CREATE TABLE climate.nasa_nex_pfe_mm_day (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    month SMALLINT NOT NULL,
    start_year SMALLINT NOT NULL,
    end_year SMALLINT NOT NULL,
    ssp SMALLINT NOT NULL, -- ssp -999 is for 'historical' values
    return_period SMALLINT NOT NULL, -- in years
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
CREATE UNIQUE INDEX idx_unique_nasa_nex_pfe_mm_day_record
    ON climate.nasa_nex_pfe_mm_day (osm_id, month, start_year, end_year, ssp);

CREATE INDEX idx_nasa_nex_pfe_mm_day_on_osm_id ON climate.nasa_nex_pfe_mm_day (osm_id);
CREATE INDEX idx_nasa_nex_pfe_mm_day_on_month ON climate.nasa_nex_pfe_mm_day (month);
CREATE INDEX idx_nasa_nex_pfe_mm_day_on_start_year ON climate.nasa_nex_pfe_mm_day (start_year);
CREATE INDEX idx_nasa_nex_pfe_mm_day_on_end_year ON climate.nasa_nex_pfe_mm_day (end_year);
CREATE INDEX idx_nasa_nex_pfe_mm_day_on_month_year ON climate.nasa_nex_pfe_mm_day (month, start_year, end_year);
CREATE INDEX idx_nasa_nex_pfe_mm_day_on_ssp ON climate.nasa_nex_pfe_mm_day (ssp);

COMMIT;