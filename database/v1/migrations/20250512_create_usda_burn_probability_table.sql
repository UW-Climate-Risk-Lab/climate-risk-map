BEGIN;

SET ROLE climate_user;

CREATE TABLE climate.usda_burn_probability (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT NOT NULL,
    burn_probability FLOAT NOT NULL,
    metadata JSONB

);

-- Unique index to constrain possible values for a given feature (osm_id)
CREATE UNIQUE INDEX idx_unique_usda_burn_probability_record
    ON climate.usda_burn_probability (osm_id);

CREATE INDEX idx_usda_burn_probability_on_osm_id ON climate.usda_burn_probability (osm_id);

COMMIT;